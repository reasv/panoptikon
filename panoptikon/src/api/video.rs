//! `/api/video/*`: the transcode job surface, the artifact serving path, and
//! the cache admin endpoints (docs/video-transcoding-design.md §7).
//!
//! The prefix is deliberately **not** under `/api/items/`: every shipped
//! `restricted_demo` ruleset grants `GET /api/items/`, so an endpoint that
//! spawns ffmpeg living there would be silently inherited by public profiles
//! (the `pinboard_search` precedent). POST (create work) and GET
//! (serve finished bytes) are separately rule-able for the same reason.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, Response, StatusCode},
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::api::db_params::DbQueryParams;
use crate::api::http_file::{FILE_IO_TIMEOUT, FileServeSpec, open_file_with_timeout, serve_file};
use crate::api::utils::serve_outro_metadata;
use crate::api_error::ApiError;
use crate::config::{PolicyConfig, Settings};
use crate::db::files::get_item_content_end_ms;
use crate::db::items::{FileRecord, ItemIdentifierType, ItemRecord, get_item_metadata_unchecked};
use crate::db::{DbConnection, ReadOnlyNoUserData};
use crate::media_tools::transcode::cache::{CacheStats, ResizeError};
use crate::media_tools::transcode::compose::{
    self, ComposeLimits, ComposeParams, ComposeRequest, ResolvedCompose,
};
use crate::media_tools::transcode::pool::{
    self, ArtifactRef, JobRequest, JobWeight, SubmitOutcome, SubmitRequest, TranscodeJobSnapshot,
};
use crate::media_tools::transcode::presets::{
    Channel, Container, ResolvedPreset, Surface, find_preset, resolve_presets,
};
use crate::media_tools::transcode::{TranscodeParams, path_stem, transcode_file_name};
use crate::policy::PolicyContext;
use crate::proxy::ProxyState;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// The `key=` form's URL names the exact bytes it serves: the key hashes the
/// source content *and* every setting that produced the file, so unlike the
/// item-file path there is no mtime-drift caveat to weaken this.
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// The resolvable form's URL is **not** content-addressed: the same
/// `(id, preset, bounds)` names different bytes after a profile edit or a
/// hardware-encoder flip, both of which re-key the artifact. `no-cache` keeps
/// the body cacheable but forces the revalidation the ETag (the key) answers
/// in one round trip — where `immutable` would be a year-long promise no
/// server could ever recall.
const CACHE_REVALIDATE: &str = "public, no-cache";

/// SSE keep-alive interval. Comments, not events: they exist so a relay or
/// proxy that buffers `text/event-stream` still sees traffic.
const SSE_KEEP_ALIVE: Duration = Duration::from_secs(10);

/// `[policies.client]` key restricting which presets a policy exposes.
const CLIENT_PRESETS_KEY: &str = "transcode_presets";

/// The only value `cut` accepts: the server-side outro cut.
const CUT_OUTRO: &str = "outro";

/// Lead subtracted from a detected outro boundary before it becomes an export
/// cut, mirroring `ui/lib/videoTrim.ts` `OUTRO_GUARD_MS`. The detected content
/// end leads the first card frame by up to 60 ms of audio bang (outro
/// detection design §2.3/§10), and that bang belongs to the card, not to the
/// content: an exported file must not open — or close — on it any more than
/// playback must.
///
/// The *constant* is shared with the player; the *timeline* is not, and only
/// the first of those is meant to match. Here the guard is subtracted from a
/// boundary measured in the file's own timeline and handed to ffmpeg, which
/// reads that same timeline — so this number is already the frame-exact one.
/// The player's midpoint anchor and rVFC end-probe corrections
/// (`docs/video-outro-skip-design.md` §1) exist only to compensate for the
/// browser's decoded timeline drifting from the file's, and deliberately have
/// no server analogue: reproducing them here would move the cut *off* the
/// frame the content actually ends on.
const OUTRO_EXPORT_GUARD_MS: i64 = 60;

/// Milliseconds per centisecond, the resolution every trim bound is carried
/// in (the `vt` trim codec's lattice).
const MS_PER_CS: i64 = 10;

/// Shortest trim window that is still a *clip*, in centiseconds. Mirrors
/// `ui/lib/videoTrim.ts` `FREEZE_EPS = 0.02` s, the same predicate the player
/// uses to decide a trim is a freeze frame rather than a playing range: a
/// window at or below it renders one frame and stops, so encoding it as a
/// video is never what was meant.
const FREEZE_GUARD_CS: i64 = 2;

// --- request/response shapes ----------------------------------------------

#[derive(Deserialize, ToSchema)]
pub(crate) struct TranscodeRequest {
    /// An item identifier (sha256 hash, file ID, path, item ID, ...).
    pub id: String,
    pub id_type: ItemIdentifierType,
    /// Preset id from `GET /api/video/presets`.
    pub preset: String,
    /// Trim start, in centiseconds from the start of the file.
    #[serde(default)]
    pub start_cs: Option<i64>,
    /// Trim end, in centiseconds from the start of the file.
    #[serde(default)]
    pub end_cs: Option<i64>,
    /// `"outro"` to end the clip at this item's detected outro boundary,
    /// resolved server-side. Excludes `end_cs` (the two are the same bound
    /// asked for two ways), composes with `start_cs`, and is a 404 when the
    /// item has no detected outro or the index database has detection off.
    /// Any other value is rejected rather than ignored: a client that sent one
    /// and got a full-length file would have no way to notice.
    #[serde(default)]
    pub cut: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodeSubmitResponse {
    /// `hit` | `created` | `joined` | `known_failure`.
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<TranscodeJobSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactRef>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ArtifactMissResponse {
    pub detail: String,
    /// The job already producing this artifact, when there is one. A miss is
    /// never a reason to start one — `POST` is the sole job creator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<TranscodeJobSnapshot>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ArtifactQuery {
    /// The cache key, as carried by every `ArtifactRef`. Primary form.
    pub key: Option<String>,
    /// Resolvable form: the same `(id, id_type, preset, start_cs, end_cs)`
    /// that produced the artifact.
    pub id: Option<String>,
    pub id_type: Option<ItemIdentifierType>,
    pub preset: Option<String>,
    pub start_cs: Option<i64>,
    pub end_cs: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodePresetInfo {
    pub id: String,
    pub label: String,
    pub container: Container,
    /// File extension for the container, so clients keep no lookup table.
    pub ext: String,
    pub channel: Channel,
    pub surfaces: Vec<Surface>,
    /// Cap on output height in pixels; `null` keeps the source height.
    ///
    /// Carried because it is a *rejection*: a composition whose canvas is
    /// taller than this is refused outright (`canvas_over_preset_height`)
    /// rather than rescaled, so a client that cannot see the number can only
    /// discover it by having a document turned away. Its `fps_max` twin is
    /// deliberately **not** here, for the same reason inverted: an over-cap
    /// frame rate is silently capped, never refused, so there is nothing a
    /// client could do with it but mirror a value that changes nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_height: Option<i64>,
}

/// Composition limits, carried alongside the presets so a client builder
/// clamps against what this server enforces instead of mirrored constants.
///
/// Deliberately *not* only the config values: the canvas and frame-rate bounds
/// are code constants (`compose.rs`), and a client that has to guess them is in
/// exactly the position this envelope exists to prevent. Where a limit comes
/// from is the server's business; that the client has the number is the point.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodeLimits {
    pub max_mosaic_inputs: usize,
    pub max_mosaic_loop_mb: u64,
    pub max_animated_image_seconds: u64,
    pub max_output_seconds: u64,
    pub min_canvas_side: i64,
    pub max_canvas_side: i64,
    pub max_canvas_area: i64,
    pub max_compose_fps: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodePresetsResponse {
    pub presets: Vec<TranscodePresetInfo>,
    pub limits: TranscodeLimits,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodeCacheStats {
    pub entries: i64,
    pub pinned_entries: i64,
    pub used_bytes: i64,
    pub capacity_bytes: u64,
    /// The `[transcode] cache_size_max_mb` ceiling, in bytes.
    pub limit_bytes: u64,
}

impl From<CacheStats> for TranscodeCacheStats {
    fn from(stats: CacheStats) -> Self {
        Self {
            entries: stats.entries,
            pinned_entries: stats.pinned_entries,
            used_bytes: stats.used_bytes,
            capacity_bytes: stats.budget_bytes,
            limit_bytes: stats.limit_bytes,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct TranscodeCacheResize {
    /// New byte budget in megabytes. `0` empties the cache; values above the
    /// `[transcode] cache_size_max_mb` ceiling are rejected. Not persisted.
    pub size_mb: u64,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct TranscodeCacheClearParams {
    /// Also forget the recorded encode verdicts, so files that failed twice
    /// are attempted again.
    #[serde(default)]
    pub include_failures: bool,
}

// --- handlers --------------------------------------------------------------

#[utoipa::path(
    post,
    operation_id = "video_transcode",
    path = "/api/video/transcode",
    tag = "video",
    summary = "Create or join a transcode job",
    description = "Resolves the item, validates the preset and trim bounds, and either answers \
        from the artifact cache (200, `outcome: \"hit\"`) or creates/joins a job (202). \
        `cut: \"outro\"` ends the clip at the item's detected outro boundary: it excludes \
        `end_cs`, composes with `start_cs`, and is resolved to explicit centiseconds here, so \
        it shares its cache entry with the identical explicit trim. An item with no detected \
        outro — including one whose index database has `detect_outros` off — is a 404.",
    params(DbQueryParams),
    request_body = TranscodeRequest,
    responses(
        (status = 200, description = "The rendition was already cached", body = TranscodeSubmitResponse),
        (status = 202, description = "A job was created or joined", body = TranscodeSubmitResponse),
        (status = 404, description = "No such item, no readable file for it, or no detected outro"),
        (status = 422, description = "Unknown preset, an unusable trim window (bounds that name a \
            freeze frame rather than a clip, a start bound past the end of the item, or a \
            start bound at or past the resolved outro cut), an unknown/conflicting `cut`, or an \
            animated-image preset asked for more than `max_animated_image_seconds` of output \
            (including an unbounded one on an item with no recorded duration)")
    )
)]
pub async fn video_transcode(
    State(state): State<Arc<ProxyState>>,
    axum::Extension(context): axum::Extension<PolicyContext>,
    mut db: DbConnection<ReadOnlyNoUserData>,
    Json(body): Json<TranscodeRequest>,
) -> ApiResult<Response<Body>> {
    let cut = parse_cut(body.cut.as_deref(), body.end_cs)?;
    let preset = policy_preset(&state.settings, &context, &body.preset)?;
    validate_bounds(body.start_cs, body.end_cs)?;

    let source = resolve_source(&mut db, &body.id, body.id_type).await?;
    // The outro cut is resolved *here*, never carried inward: the job and
    // cache layers only ever see explicit centiseconds, so a clip cut at the
    // outro and the same clip asked for by hand are one artifact, and a
    // re-detection mints a new key instead of quietly re-serving the old cut.
    let end_cs = match cut {
        Some(Cut::Outro) => {
            let end_cs =
                resolve_outro_end_cs(&mut db, &source.item.sha256, source.item.duration).await?;
            // Validated exactly like a bound that arrived explicitly — but the
            // two ways it can fail are two different answers.
            //
            // A cut that is not a clip even measured from zero (a boundary
            // inside the guard, or one within the freeze band of the file's
            // start) is an outro this item cannot be cut at, by anyone: the
            // same 404 as an item with no outro at all, because that is what
            // it amounts to.
            if validate_bounds(None, Some(end_cs)).is_err() {
                return Err(no_outro());
            }
            // Whereas a cut that only fails against *this* request's start
            // bound is the client's own doing, and naming that is the only way
            // it can be fixed: the freeze-frame text points at an `end_cs`
            // this request never carried, and the outro is not the pinboard's
            // still-image path.
            if validate_bounds(body.start_cs, Some(end_cs)).is_err() {
                return Err(unprocessable(
                    "start_cs is at or past the outro cut: there is no clip between them. \
                     Move the start bound back",
                ));
            }
            Some(end_cs)
        }
        None => body.end_cs,
    };
    drop(db);
    if let Some(start_cs) = body.start_cs
        && let Some(duration) = source.item.duration
        && (start_cs as f64) / 100.0 >= duration
    {
        return Err(unprocessable("start_cs is past the end of the item"));
    }

    // The animated-image cap, enforced *here* as well as in the encoder: the
    // client hides the row it would refuse (`ui/lib/videoClip.ts` `clipRows`),
    // and this is the half of that rule that does not depend on the client.
    validate_animated_duration(
        preset.container,
        body.start_cs,
        end_cs,
        source.item.duration,
        crate::config::runtime().transcode.max_animated_image_seconds,
    )?;

    let params = resolve_params(source.item.sha256.clone(), preset, body.start_cs, end_cs).await?;
    let outcome = pool::submit(SubmitRequest {
        job: JobRequest::Single {
            params: Box::new(params),
            source_path: source.file.path.clone().into(),
            source_duration_s: source.item.duration,
        },
        // Single-file jobs never need the pool to themselves; compositions do.
        weight: JobWeight::Light,
        // Not the encode input's stem: see [`download_stem`].
        download_stem: source.download_stem,
    })
    .await?;

    Ok(submit_response(outcome))
}

/// The response every submit produces: 200 for bytes that already existed,
/// 202 for a job to follow. Shared by both routes, so `transcode` and
/// `compose` are one envelope to a client (§0.1).
fn submit_response(outcome: SubmitOutcome) -> Response<Body> {
    let (status, body) = match outcome {
        SubmitOutcome::Hit(artifact) => (
            StatusCode::OK,
            TranscodeSubmitResponse {
                outcome: "hit".to_string(),
                job: None,
                artifact: Some(artifact),
            },
        ),
        ref other => (
            StatusCode::ACCEPTED,
            TranscodeSubmitResponse {
                outcome: other.as_str().to_string(),
                job: match other {
                    SubmitOutcome::Created(job)
                    | SubmitOutcome::Joined(job)
                    | SubmitOutcome::KnownFailure(job) => Some(job.clone()),
                    SubmitOutcome::Hit(_) => None,
                },
                artifact: None,
            },
        ),
    };
    (status, Json(body)).into_response()
}

#[utoipa::path(
    post,
    operation_id = "video_compose",
    path = "/api/video/compose",
    tag = "video",
    summary = "Create or join a composition job",
    description = "Renders a composition document — a canvas, a frame rate, an output length \
        policy and a list of placed items — into one animated artifact. A sibling of \
        `/api/video/transcode` rather than a variant of it: a composition is addressed by the \
        hash of its document, not by an item, and is strictly heavier work, so a policy can \
        allow one and deny the other. The response envelope, the jobs/SSE routes and the \
        artifact route are identical to the single-file path; a single-item save is simply a \
        composition with one item.",
    params(DbQueryParams),
    request_body = ComposeRequest,
    responses(
        (status = 200, description = "The composition was already cached", body = TranscodeSubmitResponse),
        (status = 202, description = "A job was created or joined", body = TranscodeSubmitResponse),
        (status = 404, description = "An item is not in this database, or has no readable file"),
        (status = 422, description = "Unknown preset, or a document the composition limits \
            refuse: too many items, a canvas that is odd/too large/taller than the preset \
            renders, a destination rectangle outside the canvas or at an odd position, a span \
            whose end is not after its start, a still frozen at or past its item's recorded \
            length, an unusable frame rate or length cap, or loop buffers over \
            `max_mosaic_loop_mb` (the message carries the estimate)")
    )
)]
pub async fn video_compose(
    State(state): State<Arc<ProxyState>>,
    axum::Extension(context): axum::Extension<PolicyContext>,
    mut db: DbConnection<ReadOnlyNoUserData>,
    Json(body): Json<ComposeRequest>,
) -> ApiResult<Response<Body>> {
    let preset = policy_preset(&state.settings, &context, &body.output.preset)?;
    let doc = compose::resolve_compose(&body, &preset, ComposeLimits::from_config())
        .map_err(compose_rejection)?;

    // Every item's own file, by the same readability rule the single-file path
    // uses: handing ffmpeg a path on a dropped mount would produce a *verdict*
    // on a composition that is perfectly fine.
    let mut sources = Vec::with_capacity(doc.items.len());
    for item in &doc.items {
        let source = resolve_item_source(&mut db, &item.sha256).await?;
        // The one admission rule the document could not decide on its own: a
        // still's timestamp is only past the end relative to a length the
        // index database holds. Refused here, by name, while the answer can
        // still say which pin to fix — at dispatch it would fail the whole
        // graph instead.
        compose::validate_still_bounds(item, source.duration).map_err(compose_rejection)?;
        sources.push(source.path);
    }
    drop(db);

    let weight = compose_weight(doc.items.len());
    let params = resolve_compose_params(doc, preset).await?;
    let outcome = pool::submit(SubmitRequest {
        job: JobRequest::Compose {
            params: Box::new(params),
            sources,
        },
        weight,
        // A composition has no source stem; its download name is fixed.
        download_stem: None,
    })
    .await?;
    Ok(submit_response(outcome))
}

#[utoipa::path(
    get,
    operation_id = "video_artifact",
    path = "/api/video/artifact",
    tag = "video",
    summary = "Serve a cached transcode artifact",
    description = "Serves a finished rendition by `key` (primary form) or by the \
        `(id, id_type, preset, start_cs, end_cs)` that produced it. Supports Range requests. \
        **Never starts a job**: a miss is a 404 whose body names the live job when one exists. \
        The `key=` form is `immutable` — that URL is content-addressed on both the source hash \
        and the resolved settings — while the resolvable form is `no-cache`, so its ETag \
        revalidates: the same parameters name different bytes after a profile edit.",
    params(DbQueryParams, ArtifactQuery),
    responses(
        (status = 200, description = "Artifact contents"),
        (status = 206, description = "Partial artifact contents (Range request)"),
        (status = 404, description = "Not cached", body = ArtifactMissResponse),
        (status = 416, description = "Requested range not satisfiable"),
        (status = 422, description = "Unknown preset, or trim bounds that name a freeze frame \
            rather than a clip (the resolvable form validates exactly as the POST does)")
    )
)]
pub async fn video_artifact(
    State(state): State<Arc<ProxyState>>,
    axum::Extension(context): axum::Extension<PolicyContext>,
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ArtifactQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let target = match query.key.clone() {
        // A key names bytes, not a request: it carries neither the source's
        // path nor whether the request that produced it was trimmed, so this
        // form's `Content-Disposition` can only ever offer the hash-prefix
        // name. That is a known limitation, not an oversight — the download
        // name a client should use rides on `ArtifactRef::filename`, computed
        // where the request is still known.
        Some(key) => ArtifactTarget {
            key,
            stem: None,
            trimmed: false,
            cache_control: CACHE_IMMUTABLE,
        },
        None => {
            let (Some(id), Some(id_type), Some(preset)) =
                (query.id.as_deref(), query.id_type, query.preset.as_deref())
            else {
                return Err(ApiError::bad_request(
                    "provide key=, or id + id_type + preset",
                ));
            };
            let preset = policy_preset(&state.settings, &context, preset)?;
            validate_bounds(query.start_cs, query.end_cs)?;
            // Deliberately not [`resolve_source`]: this handler can never
            // start a job, so whether any of the item's files is still
            // readable says nothing about whether the artifact is cached, and
            // statting a dropped network mount here would stall the response
            // for the timeout with no question it could answer.
            let identity = resolve_identity(&mut db, id, id_type).await?;
            let params =
                resolve_params(identity.sha256, preset, query.start_cs, query.end_cs).await?;
            ArtifactTarget {
                key: params.cache_key(),
                stem: identity.stem,
                trimmed: query.start_cs.is_some() || query.end_cs.is_some(),
                cache_control: CACHE_REVALIDATE,
            }
        }
    };
    // Nothing below this point touches the index database, and the file body
    // is streamed after the handler returns.
    drop(db);

    let cache = pool::transcode_cache().await?;
    let Some(artifact) = cache.lookup(&target.key).await else {
        let job = pool::job_for_key(target.key.clone()).await.unwrap_or(None);
        return Ok((
            StatusCode::NOT_FOUND,
            Json(ArtifactMissResponse {
                detail: "No cached artifact for this key".to_string(),
                job,
            }),
        )
            .into_response());
    };

    let path = artifact.path.to_string_lossy().into_owned();
    let file = open_file_with_timeout(&path, "Artifact not found").await?;
    // The size on disk is authoritative for range math, exactly as in the
    // item-file path.
    let size = match tokio::time::timeout(FILE_IO_TIMEOUT, file.metadata()).await {
        Ok(Ok(metadata)) => metadata.len(),
        Ok(Err(err)) => {
            tracing::error!(error = %err, "failed to read artifact metadata");
            return Err(ApiError::internal("Failed to read artifact metadata"));
        }
        Err(_) => return Err(ApiError::internal("Timed out reading artifact metadata")),
    };

    let ext = artifact
        .path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("bin");
    // A composition key names no source and no trim, so the single-file naming
    // rule has nothing to work with: it gets the same fixed scheme the
    // `ArtifactRef` carries, minus the item count the key does not record.
    let filename = if compose::is_compose_key(&target.key) {
        compose::compose_file_name(None, ext)
    } else {
        transcode_file_name(
            target.stem.as_deref(),
            source_sha_of(&target.key),
            target.trimmed,
            &artifact.preset,
            ext,
        )
    };
    serve_file(
        FileServeSpec {
            file,
            size,
            mime_type: artifact.mime_type.clone(),
            etag: format!("\"{}\"", artifact.key),
            cache_control: target.cache_control,
            last_modified: None,
            content_disposition_type: "inline",
            filename,
        },
        &request_headers,
    )
    .await
}

#[utoipa::path(
    get,
    operation_id = "video_job",
    path = "/api/video/jobs/{job_id}",
    tag = "video",
    summary = "Get a transcode job snapshot",
    description = "The same envelope the SSE stream carries. Exists for late joiners and as \
        the fallback when `text/event-stream` cannot get through.",
    params(("job_id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Job snapshot", body = TranscodeJobSnapshot),
        (status = 404, description = "No such job")
    )
)]
pub async fn video_job(AxumPath(job_id): AxumPath<String>) -> ApiResult<Json<TranscodeJobSnapshot>> {
    let id = parse_job_id(&job_id)?;
    pool::job_snapshot(id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("No such transcode job"))
}

#[utoipa::path(
    delete,
    operation_id = "video_job_cancel",
    path = "/api/video/jobs/{job_id}",
    tag = "video",
    summary = "Cancel a transcode job",
    description = "A queued job settles immediately; a running one is flagged and its ffmpeg \
        child killed. Cancellation is never recorded as a verdict on the file.",
    params(("job_id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Job snapshot after the cancel", body = TranscodeJobSnapshot),
        (status = 404, description = "No such job")
    )
)]
pub async fn video_job_cancel(
    AxumPath(job_id): AxumPath<String>,
) -> ApiResult<Json<TranscodeJobSnapshot>> {
    let id = parse_job_id(&job_id)?;
    pool::cancel(id).await?;
    pool::job_snapshot(id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("No such transcode job"))
}

#[utoipa::path(
    get,
    operation_id = "video_job_events",
    path = "/api/video/jobs/{job_id}/events",
    tag = "video",
    summary = "Follow a transcode job (SSE)",
    description = "A `text/event-stream` of job snapshots. The first event is always the \
        current snapshot, keep-alive comments are sent every 10 seconds, and the stream ends \
        after the terminal event — clients must close their EventSource then, or it will \
        reconnect forever.",
    params(("job_id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Stream of TranscodeJobSnapshot events (text/event-stream)"),
        (status = 404, description = "No such job")
    )
)]
pub async fn video_job_events(
    AxumPath(job_id): AxumPath<String>,
) -> ApiResult<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>> {
    let id = parse_job_id(&job_id)?;
    let receiver = pool::subscribe(id)
        .await?
        .ok_or_else(|| ApiError::not_found("No such transcode job"))?;
    Ok(Sse::new(job_event_stream(receiver)).keep_alive(KeepAlive::new().interval(SSE_KEEP_ALIVE)))
}

/// The stream half of [`video_job_events`], separated from the job lookup so
/// the contract it implements — first event is the current snapshot, stream
/// ends after the terminal one — is testable without a live pool.
///
/// `(receiver, first)`: the first poll reports the value the subscription
/// already holds, every later one waits for a change. `None` ends the stream.
fn job_event_stream(
    receiver: tokio::sync::watch::Receiver<TranscodeJobSnapshot>,
) -> impl futures_util::Stream<Item = Result<Event, Infallible>> {
    futures_util::stream::unfold(
        (Some(receiver), true),
        |(receiver, first)| async move {
            let mut receiver = receiver?;
            if !first && receiver.changed().await.is_err() {
                return None;
            }
            let snapshot = receiver.borrow_and_update().clone();
            let terminal = snapshot.event.is_terminal();
            let event = Event::default()
                .json_data(&snapshot)
                .unwrap_or_else(|_| Event::default().comment("unserializable job snapshot"));
            let next = if terminal { None } else { Some(receiver) };
            Some((Ok(event), (next, false)))
        },
    )
}

#[utoipa::path(
    get,
    operation_id = "video_presets",
    path = "/api/video/presets",
    tag = "video",
    summary = "List the available transcode presets",
    description = "The resolved preset table (built-ins merged with `[transcode.profiles]`), \
        filtered by the matched policy's `[policies.client] transcode_presets` when it is set. \
        The envelope also carries the composition limits, so clients clamp against live config.",
    responses(
        (status = 200, description = "Presets and limits", body = TranscodePresetsResponse)
    )
)]
pub async fn video_presets(
    State(state): State<Arc<ProxyState>>,
    axum::Extension(context): axum::Extension<PolicyContext>,
) -> ApiResult<Json<TranscodePresetsResponse>> {
    let policy = matched_policy(&state.settings, &context)?;
    let transcode = &crate::config::runtime().transcode;
    let presets = allowed_presets(policy);
    Ok(Json(TranscodePresetsResponse {
        presets: presets.iter().map(preset_info).collect(),
        limits: TranscodeLimits {
            max_mosaic_inputs: transcode.max_mosaic_inputs,
            max_mosaic_loop_mb: transcode.max_mosaic_loop_mb,
            max_animated_image_seconds: transcode.max_animated_image_seconds,
            max_output_seconds: transcode.max_output_seconds,
            min_canvas_side: compose::MIN_CANVAS_SIDE,
            max_canvas_side: compose::MAX_CANVAS_SIDE,
            max_canvas_area: compose::MAX_CANVAS_AREA,
            max_compose_fps: compose::MAX_COMPOSE_FPS,
        },
    }))
}

#[utoipa::path(
    get,
    operation_id = "get_transcode_cache",
    path = "/api/video/cache",
    tag = "video",
    summary = "Get transcode artifact cache stats",
    responses(
        (status = 200, description = "Artifact cache stats", body = TranscodeCacheStats)
    )
)]
pub async fn get_transcode_cache() -> ApiResult<Json<TranscodeCacheStats>> {
    let cache = pool::transcode_cache().await?;
    let stats = cache.stats().await.map_err(|err| {
        tracing::error!(error = %err, "failed to read transcode cache stats");
        ApiError::internal("Failed to read transcode cache stats")
    })?;
    Ok(Json(stats.into()))
}

#[utoipa::path(
    put,
    operation_id = "resize_transcode_cache",
    path = "/api/video/cache",
    tag = "video",
    summary = "Resize the transcode artifact cache",
    description = "Sets the live byte budget and evicts down to it. Sizes above the \
        `[transcode] cache_size_max_mb` ceiling are rejected. Not persisted — the TOML value \
        applies again at the next startup.",
    request_body = TranscodeCacheResize,
    responses(
        (status = 200, description = "Artifact cache stats after resizing", body = TranscodeCacheStats),
        (status = 422, description = "Above the configured ceiling"),
        (status = 500, description = "The eviction pass behind the resize failed")
    )
)]
pub async fn resize_transcode_cache(
    Json(body): Json<TranscodeCacheResize>,
) -> ApiResult<Json<TranscodeCacheStats>> {
    let cache = pool::transcode_cache().await?;
    cache
        .set_budget_mb(body.size_mb)
        .await
        .map_err(|err| match err {
            ResizeError::AboveCeiling(detail) => unprocessable(detail),
            // The chain names the cache directory, so it is logged rather
            // than echoed.
            ResizeError::Internal(err) => {
                tracing::error!(error = ?err, "failed to resize the transcode cache");
                ApiError::internal("Failed to resize the transcode artifact cache")
            }
        })?;
    get_transcode_cache().await
}

#[utoipa::path(
    delete,
    operation_id = "clear_transcode_cache",
    path = "/api/video/cache",
    tag = "video",
    summary = "Clear the transcode artifact cache",
    description = "Removes every unpinned artifact (pinned rows are the share-link guarantee \
        and survive). `include_failures` also forgets the recorded encode verdicts.",
    params(TranscodeCacheClearParams),
    responses(
        (status = 200, description = "Artifact cache stats after clearing", body = TranscodeCacheStats)
    )
)]
pub async fn clear_transcode_cache(
    Query(params): Query<TranscodeCacheClearParams>,
) -> ApiResult<Json<TranscodeCacheStats>> {
    let cache = pool::transcode_cache().await?;
    let removed = cache.clear(params.include_failures).await.map_err(|err| {
        tracing::error!(error = %err, "failed to clear the transcode cache");
        ApiError::internal("Failed to clear the transcode cache")
    })?;
    tracing::info!(removed, "transcode artifact cache cleared");
    get_transcode_cache().await
}

// --- helpers ---------------------------------------------------------------

struct ArtifactTarget {
    key: String,
    /// Source file stem, when the request identified an item (the `key=` form
    /// carries no path).
    stem: Option<String>,
    trimmed: bool,
    /// Whether this URL is a promise about bytes ([`CACHE_IMMUTABLE`]) or a
    /// name that can come to mean different bytes ([`CACHE_REVALIDATE`]).
    cache_control: &'static str,
}

struct ResolvedSource {
    item: ItemRecord,
    file: FileRecord,
    /// See [`download_stem`]: the item's name for downloads, which is not the
    /// name of [`Self::file`] unless that is also the item's first file.
    download_stem: Option<String>,
}

/// What one composition item resolves to: the file ffmpeg is handed, and the
/// item's recorded duration (`None` when the index has none).
struct ComposeItemSource {
    path: std::path::PathBuf,
    duration: Option<f64>,
}

/// What the artifact-serving path needs out of the index database: the hash
/// the key is built from, and a name for the download.
struct ResolvedIdentity {
    sha256: String,
    stem: Option<String>,
}

fn unprocessable(detail: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, detail)
}

fn parse_job_id(value: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(value).map_err(|_| ApiError::not_found("No such transcode job"))
}

/// What the request's `cut` asked for. One variant today; it is an enum so the
/// resolution below is matched rather than string-compared at the point of
/// use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cut {
    Outro,
}

/// The `cut` field, validated against the bound it replaces.
///
/// Deliberately parsed by hand rather than through a serde enum: an unknown
/// value must fail as a *validated* 422 with a message naming what is
/// accepted, not as a deserialization rejection of the whole body.
fn parse_cut(cut: Option<&str>, end_cs: Option<i64>) -> ApiResult<Option<Cut>> {
    let Some(cut) = cut else {
        return Ok(None);
    };
    if cut != CUT_OUTRO {
        return Err(unprocessable(format!(
            "unknown cut '{cut}'; the only supported value is \"{CUT_OUTRO}\""
        )));
    }
    if end_cs.is_some() {
        // Both name the end of the clip, so honouring one would silently
        // discard the other.
        return Err(unprocessable(
            "cut and end_cs are exclusive; cut=outro composes with start_cs only",
        ));
    }
    Ok(Some(Cut::Outro))
}

/// The one answer every "this item has no usable outro" case gets (§0.9).
///
/// They are the *same* 404 on purpose: the `detect_outros` toggle means the
/// whole outro feature is off for that database, and a client must not be able
/// to tell a database with the switch off from an item that simply has no
/// card — the same reason the item metadata is nulled rather than flagged. An
/// item whose recorded boundary yields no clip is the same answer for the same
/// reason: what the client asked for does not exist here.
fn no_outro() -> ApiError {
    ApiError::not_found("No detected outro for this item")
}

/// The clip end this item's detected outro implies, in centiseconds.
async fn resolve_outro_end_cs(
    db: &mut DbConnection<ReadOnlyNoUserData>,
    sha256: &str,
    duration: Option<f64>,
) -> ApiResult<i64> {
    let Some(content_end_ms) = get_item_content_end_ms(&mut db.conn, sha256).await? else {
        return Err(no_outro());
    };
    if !serve_outro_metadata(&db.index_db, true).await {
        return Err(no_outro());
    }
    // The player's own eligibility rule (`ui/lib/videoTrim.ts`: a card exists
    // only while `duration - contentEnd > 0`). A boundary at or past the end
    // of the item leaves nothing to cut away, and a `cut=outro` that quietly
    // returned the full length would be indistinguishable, to the client, from
    // one that trimmed a card. An item with no recorded duration cannot be
    // judged this way, so it is not — the boundary is taken at face value.
    if duration.is_some_and(|duration| (content_end_ms as f64) / 1000.0 >= duration) {
        return Err(no_outro());
    }
    Ok(outro_cut_cs(content_end_ms))
}

/// A detected content end (ms) as an export cut (cs): the audio-bang guard
/// ([`OUTRO_EXPORT_GUARD_MS`], whose doc covers why the player's playback-time
/// corrections have no counterpart here), then a *floor* to the centisecond
/// lattice every trim bound lives on.
///
/// Floor, not the round `ui/lib/videoTrim.ts` applies to its copy of this
/// arithmetic: rounding can move the cut up to 5 ms later, and later is *into*
/// the card — the one direction the guard exists to avoid. The
/// half-centisecond it can cost is invisible; a frame of end card is not.
fn outro_cut_cs(content_end_ms: i64) -> i64 {
    (content_end_ms - OUTRO_EXPORT_GUARD_MS).div_euclid(MS_PER_CS)
}

fn validate_bounds(start_cs: Option<i64>, end_cs: Option<i64>) -> ApiResult<()> {
    if start_cs.is_some_and(|start| start < 0) || end_cs.is_some_and(|end| end < 0) {
        return Err(unprocessable("trim bounds must not be negative"));
    }
    // The floor is the freeze-frame band, not equality: a window of one or two
    // centiseconds is a still that happens to be spelled as a range, and
    // encoding it as a video produces a file nobody asked for.
    //
    // Saturating, and on the *window* rather than on the start bound: both
    // bounds are attacker-supplied `i64`s, and `start + FREEZE_GUARD_CS` on a
    // start near `i64::MAX` overflows — which in release builds wraps to a
    // hugely negative number and waves the whole band through.
    if let Some(end) = end_cs
        && end.saturating_sub(start_cs.unwrap_or(0)) <= FREEZE_GUARD_CS
    {
        return Err(unprocessable(
            "the trim window is a freeze frame, not a clip: end_cs must be more than \
             0.02 s past the start bound. Save a still from the pinboard instead",
        ));
    }
    Ok(())
}

/// How long the encode this request describes would run for, in seconds, or
/// `None` when that is not knowable here.
///
/// Both bounds present is exact arithmetic. Without an end bound the answer is
/// the item's own duration less the start bound — an *upper* bound rather than
/// a measurement (`items.duration` is ffprobe's container duration, and the
/// encoder may stop a little short), which is the right direction for a cap.
/// An item with no recorded duration and no end bound is unknowable: `None`.
fn expected_output_seconds(
    start_cs: Option<i64>,
    end_cs: Option<i64>,
    duration: Option<f64>,
) -> Option<f64> {
    let start_s = start_cs.unwrap_or(0) as f64 / 100.0;
    if let Some(end_cs) = end_cs {
        return Some((end_cs as f64 / 100.0 - start_s).max(0.0));
    }
    duration.map(|duration| (duration - start_s).max(0.0))
}

/// The `max_animated_image_seconds` cap, applied to a *single-file* job.
///
/// Only the animated-image containers are capped. A long mp4 or webm is a
/// legitimate thing to ask for — playback of a whole film is the surface's
/// ordinary case — whereas an animated WebP is an all-frames-in-one-file
/// format whose size grows without the bounds a video codec puts on it, which
/// is exactly what the config value exists to bound.
///
/// This is the server half of a rule the client also enforces by *hiding* the
/// row (`ui/lib/videoClip.ts`): the pair is deliberate, since neither half can
/// be trusted alone — a hidden row is not a limit, and a 422 the user only
/// meets after pressing a visible row is not a UI.
fn validate_animated_duration(
    container: Container,
    start_cs: Option<i64>,
    end_cs: Option<i64>,
    duration: Option<f64>,
    max_seconds: u64,
) -> ApiResult<()> {
    if !container.is_animated_image() {
        return Ok(());
    }
    let Some(seconds) = expected_output_seconds(start_cs, end_cs, duration) else {
        // Unbounded *and* unmeasurable: the only honest answers are to refuse
        // or to start an encode whose length nobody can predict, and the second
        // one spends the pool on a file that may never fit the cap anyway.
        return Err(unprocessable(format!(
            "this preset writes an animated image, which is capped at {max_seconds} s of \
             output, and this item has no recorded duration to check against: give the \
             request a trim end (end_cs, or cut=outro)"
        )));
    };
    if seconds > max_seconds as f64 {
        return Err(unprocessable(format!(
            "an animated image may be at most {max_seconds} s long; this request asks for \
             {seconds:.2} s. Trim the clip shorter, or pick a video preset"
        )));
    }
    Ok(())
}

/// The item and the first of its files that can actually be read.
///
/// The existence check is not decoration: handing ffmpeg a path on an
/// unmounted share would produce a *verdict* on the file, and two of those
/// suppress the rendition until the cache is cleared.
async fn resolve_source(
    db: &mut DbConnection<ReadOnlyNoUserData>,
    id: &str,
    id_type: ItemIdentifierType,
) -> ApiResult<ResolvedSource> {
    let metadata = get_item_metadata_unchecked(&mut db.conn, id, id_type).await?;
    let Some(item) = metadata.item else {
        return Err(ApiError::not_found("Item not found"));
    };
    // Taken before the readability loop consumes the list: the name is the
    // item's, the input is whichever copy answered.
    let download_stem = download_stem(&metadata.files);
    let Some(file) = first_readable_file(metadata.files).await else {
        return Err(ApiError::not_found("No readable file found for item"));
    };
    Ok(ResolvedSource {
        item,
        file,
        download_stem,
    })
}

/// The encode input for one composition item, and the recorded length the
/// still-timestamp rule is judged against.
///
/// Its two failures name the hash, unlike the single-file path's: a mosaic of
/// a dozen pins answered with a bare "Item not found" tells the client nothing
/// about which pin to drop.
async fn resolve_item_source(
    db: &mut DbConnection<ReadOnlyNoUserData>,
    sha256: &str,
) -> ApiResult<ComposeItemSource> {
    let metadata =
        get_item_metadata_unchecked(&mut db.conn, sha256, ItemIdentifierType::Sha256).await?;
    let Some(item) = metadata.item else {
        return Err(ApiError::not_found(format!(
            "Composition item {sha256} is not in this database"
        )));
    };
    let file = first_readable_file(metadata.files).await.ok_or_else(|| {
        ApiError::not_found(format!("No readable file for composition item {sha256}"))
    })?;
    Ok(ComposeItemSource {
        path: file.path.into(),
        duration: item.duration,
    })
}

/// The first of an item's files that is on this machine right now.
async fn first_readable_file(files: Vec<FileRecord>) -> Option<FileRecord> {
    for file in files {
        let readable = tokio::time::timeout(FILE_IO_TIMEOUT, tokio::fs::metadata(&file.path))
            .await
            .ok()
            .and_then(Result::ok)
            .is_some_and(|meta| meta.is_file());
        if readable {
            return Some(file);
        }
    }
    None
}

/// A refused composition, as an HTTP answer. The rule's name is logged rather
/// than sent: the client shows the message, which already carries every number
/// needed to fix the document.
fn compose_rejection(rejection: compose::ComposeRejection) -> ApiError {
    tracing::debug!(reason = rejection.reason, "composition refused");
    unprocessable(rejection.detail)
}

/// Compositions past the light threshold take the pool to themselves: their
/// filtergraph holds every item's loop buffer at once, so pairing one with
/// anything else is how a host runs out of memory.
fn compose_weight(items: usize) -> JobWeight {
    if items > crate::config::runtime().transcode.compose_light_threshold {
        JobWeight::Exclusive
    } else {
        JobWeight::Light
    }
}

/// Builds a composition's identity, resolving the encoder against this host's
/// hardware probe. Off the async runtime, exactly like [`resolve_params`].
async fn resolve_compose_params(
    doc: ResolvedCompose,
    preset: ResolvedPreset,
) -> ApiResult<ComposeParams> {
    tokio::task::spawn_blocking(move || ComposeParams::resolve(doc, preset))
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "the encoder probe task failed");
            ApiError::internal("Failed to resolve the transcode encoder")
        })
}

/// The item's identity, with no filesystem access at all. Used by the GET
/// path, which needs the hash to compute a cache key and a file name to hang
/// on the download — never a file it could hand to ffmpeg.
async fn resolve_identity(
    db: &mut DbConnection<ReadOnlyNoUserData>,
    id: &str,
    id_type: ItemIdentifierType,
) -> ApiResult<ResolvedIdentity> {
    let metadata = get_item_metadata_unchecked(&mut db.conn, id, id_type).await?;
    let Some(item) = metadata.item else {
        return Err(ApiError::not_found("Item not found"));
    };
    Ok(ResolvedIdentity {
        sha256: item.sha256,
        stem: download_stem(&metadata.files),
    })
}

/// Builds the artifact identity, resolving the encoder against this host's
/// hardware probe. Off the async runtime: the first call may spawn ffmpeg
/// twice (an encoder listing plus a validation encode).
async fn resolve_params(
    sha256: String,
    preset: ResolvedPreset,
    start_cs: Option<i64>,
    end_cs: Option<i64>,
) -> ApiResult<TranscodeParams> {
    tokio::task::spawn_blocking(move || {
        TranscodeParams::resolve(sha256, preset, start_cs, end_cs)
    })
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "the encoder probe task failed");
        ApiError::internal("Failed to resolve the transcode encoder")
    })
}

fn matched_policy<'a>(
    settings: &'a Settings,
    context: &PolicyContext,
) -> ApiResult<&'a PolicyConfig> {
    settings
        .policies
        .iter()
        .find(|policy| policy.name == context.policy_name)
        .ok_or_else(|| {
            // Unreachable in practice: the policy layer selected this name out
            // of the same settings moments ago.
            tracing::error!(policy = %context.policy_name, "matched policy missing from config");
            ApiError::internal("matched policy missing from configuration")
        })
}

/// The presets this policy exposes. The `[policies.client] transcode_presets`
/// list is a *limit*, so it governs the POST as well as the listing —
/// otherwise a restricted client could simply name a preset it was not
/// offered.
fn allowed_presets(policy: &PolicyConfig) -> Vec<ResolvedPreset> {
    let presets = resolve_presets(crate::config::runtime().transcode.profiles.as_ref());
    filter_presets(presets, policy.client.get(CLIENT_PRESETS_KEY))
}

/// Pure half of [`allowed_presets`]: an absent (or non-array) setting means
/// no restriction; an explicit list — empty included — means exactly it.
fn filter_presets(
    presets: Vec<ResolvedPreset>,
    allowed: Option<&serde_json::Value>,
) -> Vec<ResolvedPreset> {
    let Some(allowed) = allowed.and_then(serde_json::Value::as_array) else {
        return presets;
    };
    let names: Vec<&str> = allowed.iter().filter_map(serde_json::Value::as_str).collect();
    presets
        .into_iter()
        .filter(|preset| names.contains(&preset.id.as_str()))
        .collect()
}

fn policy_preset(
    settings: &Settings,
    context: &PolicyContext,
    preset_id: &str,
) -> ApiResult<ResolvedPreset> {
    let policy = matched_policy(settings, context)?;
    let presets = allowed_presets(policy);
    find_preset(&presets, preset_id)
        .cloned()
        .ok_or_else(|| unprocessable(format!("unknown transcode preset '{preset_id}'")))
}

fn preset_info(preset: &ResolvedPreset) -> TranscodePresetInfo {
    TranscodePresetInfo {
        id: preset.id.clone(),
        label: preset.label.clone(),
        container: preset.container,
        ext: preset.container.ext().to_string(),
        channel: preset.channel,
        surfaces: preset.surfaces.clone(),
        max_height: preset.max_height,
    }
}

/// The stem every download of this item is named after: the stem of its
/// **first** file's path.
///
/// The single source for both naming paths — this handler's
/// `Content-Disposition` and the `ArtifactRef.filename` the pool computes for
/// a job or a hit — so an item names its downloads one way. Deliberately not
/// the encode input's stem: [`resolve_source`] picks that by *readability*, so
/// naming after it would let the same request produce two different download
/// names depending on which copy of identical content answered, or on whether
/// a network mount happened to be up.
fn download_stem(files: &[FileRecord]) -> Option<String> {
    files
        .first()
        .and_then(|file| path_stem(std::path::Path::new(&file.path)))
}

/// The `<source sha256>-<params hash>` key's first half.
fn source_sha_of(key: &str) -> &str {
    key.split('-').next().unwrap_or(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_tools::transcode::presets::builtin_presets;

    fn ids(presets: &[ResolvedPreset]) -> Vec<&str> {
        presets.iter().map(|preset| preset.id.as_str()).collect()
    }

    /// The policy limit: absent means everything, an explicit list means
    /// exactly it, and an explicit empty list means nothing at all (a policy
    /// that offers no presets is a coherent thing to configure).
    #[test]
    fn policy_preset_filter_is_a_whitelist_when_present() {
        let all = builtin_presets();
        assert_eq!(
            ids(&filter_presets(all.clone(), None)).len(),
            all.len(),
            "no setting means no restriction"
        );
        assert_eq!(
            ids(&filter_presets(
                all.clone(),
                Some(&serde_json::json!("not-an-array"))
            ))
            .len(),
            all.len(),
            "a malformed setting is ignored rather than fenced into nothing"
        );
        assert_eq!(
            ids(&filter_presets(
                all.clone(),
                Some(&serde_json::json!(["playback", "clip-fast", "nonexistent"]))
            )),
            ["playback", "clip-fast"]
        );
        assert!(
            filter_presets(all, Some(&serde_json::json!([]))).is_empty(),
            "an explicit empty list offers nothing"
        );
    }

    /// Trim bound validation, including the degenerate cases a client can
    /// reach by dragging both markers together — and the freeze band just
    /// above them, which is a still rather than a clip.
    #[test]
    fn trim_bounds_are_validated() {
        assert!(validate_bounds(None, None).is_ok());
        assert!(validate_bounds(Some(500), None).is_ok());
        assert!(validate_bounds(None, Some(500)).is_ok());
        assert!(validate_bounds(Some(0), Some(3)).is_ok());
        assert!(validate_bounds(Some(500), Some(503)).is_ok());
        for (start, end) in [
            (Some(-1), None),
            (None, Some(-1)),
            (Some(500), Some(500)),
            (Some(500), Some(499)),
            // The freeze band: `end - start <= FREEZE_EPS`, the player's own
            // predicate for a trim that shows one frame and stops.
            (Some(0), Some(1)),
            (Some(0), Some(2)),
            (Some(500), Some(502)),
            // An end bound alone is measured from zero, so the same band
            // applies with no start bound at all.
            (None, Some(2)),
            // Bounds are attacker-supplied i64s: `start + FREEZE_GUARD_CS`
            // overflows near the top of the range, and in a release build the
            // wrapped comparison would wave the whole band through.
            (Some(i64::MAX), Some(i64::MAX)),
            (Some(i64::MAX), Some(i64::MAX - 2)),
            (Some(i64::MAX - 5), Some(i64::MAX - 5)),
        ] {
            let err = validate_bounds(start, end).expect_err("rejected");
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{start:?}..{end:?}"
            );
        }
        assert!(validate_bounds(None, Some(3)).is_ok());
        // The same extremes, the other way round: a window that really is one
        // is still accepted, so the saturation did not simply reject the top
        // of the range.
        assert!(validate_bounds(Some(i64::MAX - 5), Some(i64::MAX)).is_ok());
        assert!(validate_bounds(Some(0), Some(i64::MAX)).is_ok());
    }

    /// The `key=` form's fallback name is built from the key's first half.
    #[test]
    fn the_key_carries_its_source_hash() {
        assert_eq!(source_sha_of("deadbeef-0011"), "deadbeef");
        assert_eq!(source_sha_of("nodash"), "nodash");
    }

    /// `cut` accepts exactly one value, and never alongside the bound it
    /// replaces: honouring one of two spellings of the clip's end would
    /// silently discard the other.
    #[test]
    fn cut_accepts_only_the_outro_and_never_with_an_end_bound() {
        assert_eq!(parse_cut(None, None).unwrap(), None);
        assert_eq!(parse_cut(None, Some(500)).unwrap(), None);
        assert_eq!(parse_cut(Some("outro"), None).unwrap(), Some(Cut::Outro));
        // Composition with a start bound is the point: the cut names the end.
        assert_eq!(parse_cut(Some("outro"), None).unwrap(), Some(Cut::Outro));
        for (cut, end_cs) in [
            (Some("intro"), None),
            (Some("Outro"), None),
            (Some(""), None),
            (Some("outro"), Some(500)),
        ] {
            let err = parse_cut(cut, end_cs).expect_err("rejected");
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{cut:?} + {end_cs:?}"
            );
        }
    }

    /// The guard-and-floor arithmetic, which is what makes an exported clip
    /// end where playback's outro skip ended.
    #[test]
    fn the_outro_cut_guards_then_floors_to_centiseconds() {
        assert_eq!(OUTRO_EXPORT_GUARD_MS, 60, "ui/lib/videoTrim.ts OUTRO_GUARD_MS");
        // 8.000 s of content, minus the 60 ms audio-bang lead.
        assert_eq!(outro_cut_cs(8000), 794);
        // Floored, never rounded: 7.945 s must not become 7.95 s, which is
        // 5 ms *into* the card the guard exists to stay out of.
        assert_eq!(outro_cut_cs(8005), 794);
        assert_eq!(outro_cut_cs(8009), 794);
        assert_eq!(outro_cut_cs(8010), 795);
        // A boundary inside the guard floors below zero rather than wrapping
        // towards it; the bound validation is what rejects it.
        assert_eq!(outro_cut_cs(60), 0);
        assert!(outro_cut_cs(50) < 0);
        assert!(validate_bounds(None, Some(outro_cut_cs(50))).is_err());
    }

    /// The whole point of resolving `cut=outro` at the API edge: the job and
    /// cache layers below see nothing but centiseconds, so the outro clip and
    /// the identical hand-trimmed one are **one** artifact — and produce one
    /// identical ffmpeg command line.
    #[test]
    fn an_outro_cut_is_indistinguishable_from_the_same_explicit_trim() {
        use crate::media_tools::transcode::run::{
            ENCODER_X264_QUALITY, EncodeJobSpec, build_args,
        };

        let presets = builtin_presets();
        let preset = find_preset(&presets, "clip").expect("the clip preset ships");
        let params = |start_cs, end_cs| {
            TranscodeParams::new(
                "a".repeat(64),
                preset.clone(),
                ENCODER_X264_QUALITY.to_string(),
                start_cs,
                end_cs,
            )
        };
        let resolved = params(Some(100), Some(outro_cut_cs(8005)));
        let explicit = params(Some(100), Some(794));
        assert_eq!(resolved.cache_key(), explicit.cache_key());
        assert_ne!(resolved.cache_key(), params(Some(100), None).cache_key());
        assert_ne!(resolved.cache_key(), params(None, None).cache_key());

        let args = |params| {
            build_args(&EncodeJobSpec {
                input: std::path::PathBuf::from("in.mp4"),
                output: std::path::PathBuf::from("out.tmp"),
                params,
                source_duration_s: Some(10.0),
            })
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
        };
        let argv = args(resolved);
        assert_eq!(argv, args(explicit));
        // The window itself: one second in, ending at the guarded outro.
        let ss = argv.iter().position(|arg| arg == "-ss").expect("-ss");
        let t = argv.iter().position(|arg| arg == "-t").expect("-t");
        assert_eq!(argv[ss + 1], "1.00");
        assert_eq!(argv[t + 1], "6.94");
    }

    /// The animated-image cap: what it measures, and what it deliberately does
    /// **not** bound. A long mp4 is a legitimate request — it is how the player
    /// gets a whole film — so the cap is a property of the container, not of
    /// the trim.
    #[test]
    fn the_animated_image_cap_bounds_only_the_animated_containers() {
        // The window itself, before any container is involved.
        assert_eq!(expected_output_seconds(Some(100), Some(600), None), Some(5.0));
        assert_eq!(expected_output_seconds(None, Some(600), Some(90.0)), Some(6.0));
        // No end bound: the item's duration, less the start bound.
        assert_eq!(expected_output_seconds(Some(1000), None, Some(45.0)), Some(35.0));
        assert_eq!(expected_output_seconds(None, None, Some(45.0)), Some(45.0));
        // ...and no way to know at all.
        assert_eq!(expected_output_seconds(None, None, None), None);
        assert_eq!(expected_output_seconds(Some(1000), None, None), None);

        let webp = |start_cs, end_cs, duration| {
            validate_animated_duration(Container::Webp, start_cs, end_cs, duration, 30)
        };
        // Avif is the other animated-image container, capped identically.
        assert!(
            validate_animated_duration(Container::Avif, None, None, Some(45.0), 30).is_err()
        );
        assert!(validate_animated_duration(Container::Avif, None, Some(1200), None, 30).is_ok());
        // Under the cap, both ways of knowing the window.
        assert!(webp(Some(100), Some(600), Some(600.0)).is_ok());
        assert!(webp(None, None, Some(12.0)).is_ok());
        assert!(webp(Some(1000), None, Some(35.0)).is_ok(), "25 s of output");
        // Exactly at it is still a clip anyone may ask for.
        assert!(webp(None, Some(3000), None).is_ok());

        // Over it, with bounds — the message names both the limit and what was
        // asked for, since the fix is to move a marker.
        let err = webp(Some(100), Some(4000), Some(600.0)).expect_err("over the cap");
        let detail = err.detail().to_string();
        assert!(detail.contains("30"), "{detail}");
        assert!(detail.contains("39.00"), "{detail}");
        assert_eq!(
            err.into_response().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        // Over it untrimmed, which is the case a client that offered the row
        // anyway would send.
        assert!(webp(None, None, Some(45.0)).is_err());
        assert!(webp(Some(100), None, Some(45.0)).is_err());
        // Unbounded *and* unmeasurable: refused rather than started blind, and
        // the message says which bound would fix it.
        let blind = webp(None, None, None).expect_err("nothing to check against");
        let detail = blind.detail().to_string();
        assert!(detail.contains("end_cs"), "{detail}");
        assert!(detail.contains("30"), "{detail}");
        assert_eq!(
            blind.into_response().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        // And the containers this cap says nothing about: a two-hour mp4 with
        // no recorded duration is an ordinary playback request.
        for container in [Container::Mp4, Container::Webm] {
            assert!(validate_animated_duration(container, None, None, None, 30).is_ok());
            assert!(
                validate_animated_duration(container, None, None, Some(7200.0), 30).is_ok(),
                "{container:?}"
            );
            assert!(
                validate_animated_duration(container, Some(0), Some(720_000), None, 30).is_ok()
            );
        }
    }

    /// A job id that is not a UUID is a 404, not a 500: the id comes straight
    /// off the URL.
    #[test]
    fn a_malformed_job_id_is_a_not_found() {
        assert!(parse_job_id(&Uuid::new_v4().to_string()).is_ok());
        assert_eq!(
            parse_job_id("not-a-uuid")
                .unwrap_err()
                .into_response()
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    // --- handler-level tests ------------------------------------------------

    use axum::Extension;
    use axum::Router;
    use axum::body::Body as AxumBody;
    use axum::http::{Request, header};
    use axum::routing::get;
    use http_body_util::BodyExt;
    use sqlx::Connection;
    use tower::ServiceExt;

    use crate::media_tools::transcode::cache::NewArtifact;

    /// Two policies: one unrestricted, one whose `[policies.client]` limits
    /// the presets it exposes.
    fn test_settings() -> Arc<Settings> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"

[[policies]]
name = "local"

[policies.match]
hosts = ["localhost"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"

[[policies]]
name = "limited"

[policies.match]
hosts = ["demo.example.com"]

[policies.index_db]
default = "default"
allow = "*"

[policies.user_data_db]
default = "default"
allow = "*"

[policies.client]
transcode_presets = ["playback"]
"#,
        )
        .unwrap();
        Arc::new(Settings::load(Some(path)).unwrap())
    }

    fn test_state(settings: &Arc<Settings>) -> Arc<ProxyState> {
        let upstream = crate::proxy::Upstream::parse("api", "http://127.0.0.1:1").unwrap();
        let client = crate::inferio_client::InferenceApiClient::new_with_metadata_cache(
            "http://127.0.0.1:1".to_string(),
            false,
        )
        .unwrap();
        Arc::new(ProxyState::new(
            upstream.clone(),
            upstream.clone(),
            upstream,
            client,
            0,
            Arc::clone(settings),
            Arc::new(crate::policy_token::TokenKey::random()),
            tokio::sync::watch::channel(false).1,
        ))
    }

    fn test_context(policy: &str) -> PolicyContext {
        PolicyContext {
            policy_name: policy.to_string(),
            db_action: crate::policy::DbAction::Skipped,
            selected_by: crate::policy::PolicySelection::ListenerHost,
            search_cache: true,
        }
    }

    async fn body_json(response: Response<Body>) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    /// A connection the handler never reads: the `key=` form of the artifact
    /// route resolves nothing from the index database.
    async fn unused_db() -> DbConnection<ReadOnlyNoUserData> {
        let conn = sqlx::SqliteConnection::connect("sqlite::memory:")
            .await
            .expect("an in-memory connection");
        DbConnection::for_tests(conn, "test", "test")
    }

    /// The presets route: the full resolved table, the DTO's `ext` (so no
    /// client keeps a container-to-extension map), the live limits, and the
    /// per-policy whitelist.
    #[tokio::test]
    async fn presets_route_lists_the_table_and_honours_the_policy_limit() {
        let settings = test_settings();
        let app = Router::new()
            .route("/api/video/presets", get(video_presets))
            .with_state(test_state(&settings))
            .layer(Extension(test_context("local")));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/video/presets")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        let ids: Vec<&str> = json["presets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|preset| preset["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            [
                "playback",
                "clip",
                "clip-fast",
                "webp-anim",
                "avif-anim",
                "mosaic-mp4",
                "mosaic-mp4-fast",
                "mosaic-webm",
            ]
        );
        assert_eq!(json["presets"][0]["ext"], "mp4");
        assert_eq!(json["presets"][0]["channel"], "fast");
        assert_eq!(json["presets"][0]["surfaces"][0], "playback");
        assert_eq!(json["presets"][3]["ext"], "webp");
        // The preset's height cap rides along because it is a *rejection*: a
        // canvas taller than it is refused rather than rescaled, so a client
        // that cannot see it discovers it only by being turned away. Presets
        // with no cap carry no key rather than a null.
        assert_eq!(json["presets"][0]["max_height"], 1080);
        assert_eq!(json["presets"][3]["max_height"], 720);
        assert!(
            json["presets"][1].get("max_height").is_none(),
            "`clip` is uncapped: {}",
            json["presets"][1]
        );
        // `fps_max` is deliberately absent from the DTO: an over-cap frame
        // rate is silently capped, never refused, so there is nothing a client
        // could do with the number.
        assert!(json["presets"][0].get("fps_max").is_none());

        // The compose limits ride along, so a client builder clamps against
        // what this server enforces rather than mirrored constants — the
        // config-backed ones and the code constants alike.
        let transcode = &crate::config::runtime().transcode;
        assert_eq!(
            json["limits"]["max_mosaic_inputs"],
            transcode.max_mosaic_inputs
        );
        assert_eq!(
            json["limits"]["max_output_seconds"],
            transcode.max_output_seconds
        );
        assert_eq!(json["limits"]["min_canvas_side"], compose::MIN_CANVAS_SIDE);
        assert_eq!(json["limits"]["max_canvas_side"], compose::MAX_CANVAS_SIDE);
        assert_eq!(json["limits"]["max_canvas_area"], compose::MAX_CANVAS_AREA);
        assert_eq!(json["limits"]["max_compose_fps"], compose::MAX_COMPOSE_FPS);

        let limited = Router::new()
            .route("/api/video/presets", get(video_presets))
            .with_state(test_state(&settings))
            .layer(Extension(test_context("limited")));
        let json = body_json(
            limited
                .oneshot(
                    Request::builder()
                        .uri("/api/video/presets")
                        .body(AxumBody::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let ids: Vec<&str> = json["presets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|preset| preset["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["playback"]);
    }

    /// The cache admin surface: stats, the ceiling on a resize (422, not a
    /// silent clamp), and a clear that reports the emptied cache.
    #[tokio::test]
    async fn cache_route_reports_stats_resizes_and_clears() {
        let _env = crate::test_utils::test_data_dir();
        let app = Router::new().route(
            "/api/video/cache",
            get(get_transcode_cache)
                .put(resize_transcode_cache)
                .delete(clear_transcode_cache),
        );

        let cache = pool::transcode_cache().await.expect("the cache opens");
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        cache
            .commit(
                NewArtifact {
                    key: "api-cache-test",
                    source_sha256: "sha",
                    params_hash: "hash",
                    preset: "clip",
                    file_name: "api-cache-test.mp4",
                    download_name: "api-cache-test.mp4",
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();

        let json = body_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/video/cache")
                        .body(AxumBody::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(json["entries"].as_i64().unwrap() >= 1);
        assert!(json["used_bytes"].as_i64().unwrap() >= 10);

        // Over the configured ceiling is a rejection, not a clamp.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/video/cache")
                    .header("content-type", "application/json")
                    .body(AxumBody::from(
                        serde_json::json!({ "size_mb": u64::MAX / (1024 * 1024) }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let json = body_json(
            app.oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/video/cache")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
        )
        .await;
        assert_eq!(json["entries"], 0);
        assert_eq!(json["used_bytes"], 0);
        assert!(cache.lookup("api-cache-test").await.is_none());
    }

    /// The artifact route: a miss is a 404 body (never a started job), and a
    /// hit carries the immutable validators the whole design leans on.
    #[tokio::test]
    async fn artifact_route_misses_with_404_and_hits_immutable() {
        let _env = crate::test_utils::test_data_dir();
        let settings = test_settings();
        let state = test_state(&settings);

        let miss = video_artifact(
            State(Arc::clone(&state)),
            Extension(test_context("local")),
            unused_db().await,
            Query(ArtifactQuery {
                key: Some("nothing-here".to_string()),
                id: None,
                id_type: None,
                preset: None,
                start_cs: None,
                end_cs: None,
            }),
            HeaderMap::new(),
        )
        .await
        .expect("a miss is a response, not an error");
        assert_eq!(miss.status(), StatusCode::NOT_FOUND);
        let json = body_json(miss).await;
        assert!(json["detail"].as_str().unwrap().contains("No cached"));
        assert!(json.get("job").is_none(), "no job is producing this key");

        let cache = pool::transcode_cache().await.unwrap();
        let key = "abcdef0123456789-2f2f2f2f";
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        cache
            .commit(
                NewArtifact {
                    key,
                    source_sha256: "abcdef0123456789",
                    params_hash: "hash",
                    preset: "clip-fast",
                    file_name: "abcdef0123456789-2f2f2f2f.mp4",
                    download_name: "abcdef01-clip.mp4",
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();

        let hit = video_artifact(
            State(state),
            Extension(test_context("local")),
            unused_db().await,
            Query(ArtifactQuery {
                key: Some(key.to_string()),
                id: None,
                id_type: None,
                preset: None,
                start_cs: None,
                end_cs: None,
            }),
            HeaderMap::new(),
        )
        .await
        .expect("a cached artifact is served");
        assert_eq!(hit.status(), StatusCode::OK);
        let headers = hit.headers();
        assert_eq!(headers[header::ETAG], format!("\"{key}\""));
        assert_eq!(headers[header::CACHE_CONTROL], CACHE_IMMUTABLE);
        assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
        assert_eq!(headers[header::CONTENT_TYPE], "video/mp4");
        assert_eq!(headers[header::CONTENT_LENGTH], "10");
        // The `key=` form carries no path, so the download falls back to the
        // source hash prefix plus the preset that produced it.
        assert_eq!(
            headers[header::CONTENT_DISPOSITION],
            "inline; filename=\"abcdef0123-clip-fast.mp4\""
        );
        cache.clear(true).await.unwrap();
    }

    /// The other artifact form, and the difference that matters: an
    /// `(id, preset, bounds)` URL is a *name*, not a promise about bytes —
    /// editing a profile or flipping the hardware probe makes the same URL
    /// resolve to a different artifact — so it revalidates instead of being
    /// `immutable`, which no server could ever recall. It also resolves the
    /// item without touching the filesystem: a GET starts no job, so the
    /// source file's existence is not its question.
    #[tokio::test]
    async fn the_resolvable_artifact_form_revalidates_and_never_stats_the_source() {
        let _env = crate::test_utils::test_data_dir();
        /// Spelled in full so the lookup takes the exact match rather than the
        /// prefix-range branch.
        const SHA: &str = "b1c2d3e4b1c2d3e4b1c2d3e4b1c2d3e4b1c2d3e4b1c2d3e4b1c2d3e4b1c2d3e4";

        let mut dbs = crate::db::migrations::setup_test_databases().await;
        sqlx::query(
            "INSERT INTO items (id, sha256, md5, type, duration, time_added) \
             VALUES (1, ?, 'md5_1', 'video/mp4', 12.0, '2024-01-01T00:00:00')",
        )
        .bind(SHA)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\gone")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        // A path that does not exist: the handler must still answer.
        sqlx::query(
            "INSERT INTO files (id, sha256, item_id, path, filename, last_modified, scan_id, available) \
             VALUES (10, ?, 1, 'C:\\gone\\holiday.mp4', 'holiday.mp4', '2024-01-02T00:00:00', 1, 1)",
        )
        .bind(SHA)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let crate::db::migrations::InMemoryDatabases {
            index_conn,
            storage_conn,
            user_data_conn,
        } = dbs;
        // Held so the shared-cache in-memory databases outlive the call.
        let _attached = (storage_conn, user_data_conn);

        let settings = test_settings();
        let state = test_state(&settings);
        let preset = policy_preset(&settings, &test_context("local"), "clip").unwrap();
        let key = TranscodeParams::resolve(SHA.to_string(), preset, None, None).cache_key();

        let cache = pool::transcode_cache().await.unwrap();
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        cache
            .commit(
                NewArtifact {
                    key: &key,
                    source_sha256: SHA,
                    params_hash: "hash",
                    preset: "clip",
                    file_name: &format!("{key}.mp4"),
                    download_name: &format!("{key}.mp4"),
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();

        let hit = video_artifact(
            State(state),
            Extension(test_context("local")),
            DbConnection::<ReadOnlyNoUserData>::for_tests(index_conn, "test", "test"),
            Query(ArtifactQuery {
                key: None,
                id: Some(SHA.to_string()),
                id_type: Some(ItemIdentifierType::Sha256),
                preset: Some("clip".to_string()),
                start_cs: None,
                end_cs: None,
            }),
            HeaderMap::new(),
        )
        .await
        .expect("the artifact is served without the source file existing");
        assert_eq!(hit.status(), StatusCode::OK);
        assert_eq!(hit.headers()[header::ETAG], format!("\"{key}\""));
        assert_eq!(hit.headers()[header::CACHE_CONTROL], CACHE_REVALIDATE);
        assert_ne!(CACHE_REVALIDATE, CACHE_IMMUTABLE);
        // The item's own file names the download, unlike the `key=` form.
        assert_eq!(
            hit.headers()[header::CONTENT_DISPOSITION],
            "inline; filename=\"holiday-clip.mp4\""
        );
        cache.clear(true).await.unwrap();
    }

    const WITH_OUTRO: &str = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
    const NO_OUTRO: &str = "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2";
    /// An item whose recorded boundary is inside the guard: the cut it implies
    /// is not a clip even measured from zero.
    const DEGENERATE_OUTRO: &str =
        "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4";
    const OUTRO_INDEX_DB: &str = "video-outro-cut";

    /// The in-memory databases live exactly as long as a connection to them
    /// does, so the two the handler never sees are held by the caller.
    type RetainedDbs = (sqlx::SqliteConnection, sqlx::SqliteConnection);

    /// A fresh index database carrying the outro fixtures, plus — for the two
    /// items a handler-level call resolves — file rows pointing at real files
    /// inside `dir`, which this creates.
    ///
    /// `WITH_OUTRO` deliberately gets **two**: an unreadable one that sorts
    /// first (`available` DESC is the query's order) and the real file behind
    /// it. That splits the two questions the naming rule keeps apart: the
    /// encode input is the file that can be read, the download name is the
    /// first file's, whichever that is.
    async fn outro_fixture_db(
        dir: &std::path::Path,
    ) -> (DbConnection<ReadOnlyNoUserData>, RetainedDbs) {
        // Each item needs its own path: `files.path` is unique.
        let readable = dir.join("readable-copy.mp4");
        let degenerate_source = dir.join("degenerate-copy.mp4");
        for path in [&readable, &degenerate_source] {
            std::fs::write(path, b"not really a video").unwrap();
        }
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        for (id, sha, content_end_ms) in [
            (1, WITH_OUTRO, Some(8005_i64)),
            (2, NO_OUTRO, None),
            (3, DEGENERATE_OUTRO, Some(70)),
        ] {
            sqlx::query(
                "INSERT INTO items (id, sha256, md5, type, duration, time_added, content_end_ms) \
                 VALUES (?, ?, ?, 'video/mp4', 12.0, '2024-01-01T00:00:00', ?)",
            )
            .bind(id)
            .bind(sha)
            .bind(format!("md5_{id}"))
            .bind(content_end_ms)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\gone")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        let row = |path: &std::path::Path| {
            (
                path.to_string_lossy().into_owned(),
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap(),
            )
        };
        let (readable_path, readable_name) = row(&readable);
        let (degenerate_path, degenerate_name) = row(&degenerate_source);
        for (id, item_id, sha, path, filename, available) in [
            (
                10,
                1,
                WITH_OUTRO,
                r"C:\gone\first-name.mp4".to_string(),
                "first-name.mp4".to_string(),
                1,
            ),
            (11, 1, WITH_OUTRO, readable_path, readable_name, 0),
            (
                12,
                3,
                DEGENERATE_OUTRO,
                degenerate_path,
                degenerate_name,
                1,
            ),
        ] {
            sqlx::query(
                "INSERT INTO files \
                 (id, sha256, item_id, path, filename, last_modified, scan_id, available) \
                 VALUES (?, ?, ?, ?, ?, '2024-01-02T00:00:00', 1, ?)",
            )
            .bind(id)
            .bind(sha)
            .bind(item_id)
            .bind(path)
            .bind(filename)
            .bind(available)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        let crate::db::migrations::InMemoryDatabases {
            index_conn,
            storage_conn,
            user_data_conn,
        } = dbs;
        (
            DbConnection::<ReadOnlyNoUserData>::for_tests(index_conn, OUTRO_INDEX_DB, "test"),
            (storage_conn, user_data_conn),
        )
    }

    /// `cut=outro` end to end against the index database: the guard-and-floor
    /// arithmetic, the ways an item can have no *usable* outro, and the
    /// `detect_outros` toggle — which withholds the cut exactly as it
    /// withholds the metadata, with the *same* 404, so no client can tell a
    /// database with the switch off from an item with no card.
    #[tokio::test]
    async fn the_outro_cut_resolves_against_the_index_and_the_detect_outros_gate() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        let (mut db, _attached) = outro_fixture_db(fixtures.path()).await;

        crate::test_utils::write_detect_outros_config(OUTRO_INDEX_DB, true);
        assert_eq!(
            resolve_outro_end_cs(&mut db, WITH_OUTRO, Some(12.0))
                .await
                .unwrap(),
            794,
            "8.005 s of content, less the 60 ms guard, floored to centiseconds"
        );
        // A source with no recorded duration cannot be judged for eligibility,
        // so the boundary is taken at face value.
        assert_eq!(
            resolve_outro_end_cs(&mut db, WITH_OUTRO, None).await.unwrap(),
            794
        );

        // The "no outro" answers, which must be one answer: no boundary, no
        // item, a boundary that leaves no card (the player's own `card > 0`
        // rule, at and past the end of the item), and a closed gate.
        let mut missing = Vec::new();
        for (sha, duration) in [
            (NO_OUTRO, Some(12.0)),
            (&"e3".repeat(32), Some(12.0)),
            (WITH_OUTRO, Some(8.005)),
            (WITH_OUTRO, Some(8.0)),
        ] {
            missing.push(
                resolve_outro_end_cs(&mut db, sha, duration)
                    .await
                    .expect_err("404"),
            );
        }
        crate::test_utils::write_detect_outros_config(OUTRO_INDEX_DB, false);
        missing.push(
            resolve_outro_end_cs(&mut db, WITH_OUTRO, Some(12.0))
                .await
                .expect_err("the gate closes over a detected outro too"),
        );
        let answers: Vec<(String, StatusCode)> = missing
            .into_iter()
            .map(|err| {
                let detail = err.detail().to_string();
                (detail, err.into_response().status())
            })
            .collect();
        assert!(answers[0].0.contains("No detected outro"), "{answers:?}");
        for answer in &answers {
            assert_eq!(answer.1, StatusCode::NOT_FOUND);
            assert_eq!(
                answer.0, answers[0].0,
                "a closed gate is indistinguishable from an item with no card"
            );
        }

        // And back: the gate is read from the file's stamp, so re-enabling it
        // takes effect on the very next request. Written by hand rather than
        // through the helper so the file's *length* differs from both earlier
        // writes: the stamp then provably moved even where mtime granularity
        // is too coarse to record a same-second rewrite, which is the case the
        // length is carried for.
        let config = crate::db::system_config::SystemConfigStore::from_env()
            .config_path(OUTRO_INDEX_DB);
        std::fs::write(&config, "detect_outros = true # re-enabled\n").unwrap();
        assert_eq!(
            resolve_outro_end_cs(&mut db, WITH_OUTRO, Some(12.0))
                .await
                .unwrap(),
            794
        );
    }

    /// The point of resolving `cut=outro` at the edge, proven through the
    /// handler rather than through the arithmetic: the request that names the
    /// cut and the request that spells out the identical `end_cs` reach the
    /// same artifact. A handler that passed `body.end_cs` (absent, here) inward
    /// would key an untrimmed rendition and miss the cache below.
    ///
    /// Also pins the download name against the *first* file while the encode
    /// input is the second — the only file that can be read.
    #[tokio::test]
    async fn the_outro_cut_and_its_explicit_trim_are_one_artifact_through_the_handler() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        crate::test_utils::write_detect_outros_config(OUTRO_INDEX_DB, true);

        let settings = test_settings();
        let preset = policy_preset(&settings, &test_context("local"), "clip").unwrap();
        // 8.005 s of content, less the guard, floored: the cut `cut=outro`
        // must resolve to, one second into the file.
        let key = TranscodeParams::resolve(WITH_OUTRO.to_string(), preset, Some(100), Some(794))
            .cache_key();

        // Pre-filled so both requests are answered from the cache: this test is
        // about the key each one computes, not about ffmpeg.
        let cache = pool::transcode_cache().await.unwrap();
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        cache
            .commit(
                NewArtifact {
                    key: &key,
                    source_sha256: WITH_OUTRO,
                    params_hash: "hash",
                    preset: "clip",
                    file_name: &format!("{key}.mp4"),
                    download_name: &format!("{key}.mp4"),
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();

        let request = |end_cs, cut: Option<&str>| TranscodeRequest {
            id: WITH_OUTRO.to_string(),
            id_type: ItemIdentifierType::Sha256,
            preset: "clip".to_string(),
            start_cs: Some(100),
            end_cs,
            cut: cut.map(str::to_string),
        };
        let mut keys = Vec::new();
        for body in [request(None, Some("outro")), request(Some(794), None)] {
            let (db, _attached) = outro_fixture_db(fixtures.path()).await;
            let response = video_transcode(
                State(test_state(&settings)),
                Extension(test_context("local")),
                db,
                Json(body),
            )
            .await
            .expect("the cached rendition answers");
            assert_eq!(response.status(), StatusCode::OK);
            let json = body_json(response).await;
            assert_eq!(json["outcome"], "hit");
            // The item's first file names the download even though the encode
            // input is the second one — the only readable copy.
            assert_eq!(json["artifact"]["filename"], "first-name-clip.mp4");
            keys.push(json["artifact"]["key"].as_str().unwrap().to_string());
        }
        assert_eq!(keys[0], key, "cut=outro resolved to the explicit cut");
        assert_eq!(
            keys[0], keys[1],
            "the two spellings of the same clip are one artifact"
        );
        cache.clear(true).await.unwrap();
    }

    /// The two ways a resolved outro cut can fail validation are two different
    /// answers, because they are two different situations.
    #[tokio::test]
    async fn a_degenerate_outro_is_a_404_and_a_late_start_bound_is_a_422() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        crate::test_utils::write_detect_outros_config(OUTRO_INDEX_DB, true);
        let settings = test_settings();

        /// One `cut=outro` POST against a freshly seeded database, which is
        /// what a handler call costs: the connection is consumed.
        async fn post(
            settings: &Arc<Settings>,
            fixtures: &std::path::Path,
            sha: &str,
            start_cs: Option<i64>,
        ) -> ApiError {
            let (db, _attached) = outro_fixture_db(fixtures).await;
            video_transcode(
                State(test_state(settings)),
                Extension(test_context("local")),
                db,
                Json(TranscodeRequest {
                    id: sha.to_string(),
                    id_type: ItemIdentifierType::Sha256,
                    preset: "clip".to_string(),
                    start_cs,
                    end_cs: None,
                    cut: Some(CUT_OUTRO.to_string()),
                }),
            )
            .await
            .expect_err("rejected")
        }

        // A boundary inside the guard: the cut is not a clip from zero, so the
        // outro is unusable for anyone — the *same* answer as an item with no
        // outro at all, down to the body, so nothing about the item leaks.
        let degenerate = post(&settings, fixtures.path(), DEGENERATE_OUTRO, None).await;
        assert_eq!(degenerate.detail(), no_outro().detail());
        assert_eq!(degenerate.into_response().status(), StatusCode::NOT_FOUND);

        // Whereas a start bound that lands at or past a perfectly good cut is
        // this request's fault, and says so: naming `end_cs` (never sent) or
        // the pinboard still (the freeze-frame text) would send the client
        // looking in the wrong place.
        for start_cs in [Some(794), Some(793), Some(1_000)] {
            let late = post(&settings, fixtures.path(), WITH_OUTRO, start_cs).await;
            let detail = late.detail().to_string();
            assert!(detail.contains("start_cs"), "{detail}");
            assert!(!detail.contains("end_cs"), "{detail}");
            assert!(!detail.contains("pinboard"), "{detail}");
            assert_eq!(
                late.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{start_cs:?}"
            );
        }
    }

    /// The request-level rejections, which all land before the item is
    /// resolved — the connection below would fail any query it was handed.
    #[tokio::test]
    async fn a_malformed_clip_request_never_reaches_the_item() {
        let settings = test_settings();
        let request = |start_cs, end_cs, cut: Option<&str>| TranscodeRequest {
            id: "a".repeat(64),
            id_type: ItemIdentifierType::Sha256,
            preset: "clip".to_string(),
            start_cs,
            end_cs,
            cut: cut.map(str::to_string),
        };
        let cases = [
            // `cut` and `end_cs` are the same bound asked for twice.
            request(Some(100), Some(500), Some("outro")),
            // The only accepted value is "outro".
            request(None, None, Some("intro")),
            // A freeze frame is a still, not a clip.
            request(Some(500), Some(501), None),
            // An unknown preset is a rejection, not a default.
            TranscodeRequest {
                preset: "nonexistent".to_string(),
                ..request(None, None, None)
            },
        ];
        for body in cases {
            let err = video_transcode(
                State(test_state(&settings)),
                Extension(test_context("local")),
                unused_db().await,
                Json(body),
            )
            .await
            .expect_err("rejected before the item is looked up");
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }

    const WEBP_ITEM: &str = "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5";

    /// One item with a readable file and the given recorded duration, which is
    /// the only thing the animated-image cap reads off the index database.
    async fn webp_fixture_db(
        dir: &std::path::Path,
        duration: Option<f64>,
    ) -> (DbConnection<ReadOnlyNoUserData>, RetainedDbs) {
        let source = dir.join("long-video.mp4");
        std::fs::write(&source, b"not really a video").unwrap();
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        sqlx::query(
            "INSERT INTO items (id, sha256, md5, type, duration, time_added) \
             VALUES (1, ?, 'md5_1', 'video/mp4', ?, '2024-01-01T00:00:00')",
        )
        .bind(WEBP_ITEM)
        .bind(duration)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\gone")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO files \
             (id, sha256, item_id, path, filename, last_modified, scan_id, available) \
             VALUES (10, ?, 1, ?, 'long-video.mp4', '2024-01-02T00:00:00', 1, 1)",
        )
        .bind(WEBP_ITEM)
        .bind(source.to_string_lossy().into_owned())
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let crate::db::migrations::InMemoryDatabases {
            index_conn,
            storage_conn,
            user_data_conn,
        } = dbs;
        (
            DbConnection::<ReadOnlyNoUserData>::for_tests(index_conn, "webp-cap", "test"),
            (storage_conn, user_data_conn),
        )
    }

    /// The animated-image cap through the handler: an over-long WebP is
    /// refused *before* a job is created, an unbounded one on an item with no
    /// recorded duration is refused rather than started blind, and neither
    /// rejection reaches an mp4 of the same length.
    #[tokio::test]
    async fn an_over_long_animated_image_is_refused_before_a_job_is_created() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        let settings = test_settings();
        let cap = crate::config::runtime().transcode.max_animated_image_seconds;

        async fn post(
            settings: &Arc<Settings>,
            fixtures: &std::path::Path,
            duration: Option<f64>,
            preset: &str,
            start_cs: Option<i64>,
            end_cs: Option<i64>,
        ) -> ApiResult<Response<Body>> {
            let (db, _attached) = webp_fixture_db(fixtures, duration).await;
            video_transcode(
                State(test_state(settings)),
                Extension(test_context("local")),
                db,
                Json(TranscodeRequest {
                    id: WEBP_ITEM.to_string(),
                    id_type: ItemIdentifierType::Sha256,
                    preset: preset.to_string(),
                    start_cs,
                    end_cs,
                    cut: None,
                }),
            )
            .await
        }

        // Untrimmed, on an item far longer than the cap.
        let long = post(&settings, fixtures.path(), Some(600.0), "webp-anim", None, None)
            .await
            .expect_err("over the cap");
        assert!(long.detail().contains(&cap.to_string()), "{}", long.detail());
        assert_eq!(
            long.into_response().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        // Trimmed, but still over it: the window is what is measured, not the
        // item.
        let trimmed = post(
            &settings,
            fixtures.path(),
            Some(600.0),
            "webp-anim",
            Some(1000),
            Some(1000 + (cap as i64 + 5) * 100),
        )
        .await
        .expect_err("over the cap");
        assert_eq!(
            trimmed.into_response().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        // Unbounded and unmeasurable: an encode nobody could predict the
        // length of is never started.
        let blind = post(&settings, fixtures.path(), None, "webp-anim", None, None)
            .await
            .expect_err("no duration to check against");
        assert!(blind.detail().contains("end_cs"), "{}", blind.detail());
        assert_eq!(
            blind.into_response().status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );

        // And the two requests that must still be accepted. Both are answered
        // from a pre-filled cache so this test is about the gate rather than
        // about ffmpeg: a WebP window under the cap, and an mp4 of the whole
        // ten-minute item — playback of a long video is the ordinary case, and
        // gains no new limit here.
        let cache = pool::transcode_cache().await.unwrap();
        let under_cap = (cap as i64).min(5) * 100;
        for (preset_id, start_cs, end_cs, ext, mime) in [
            ("webp-anim", Some(0), Some(under_cap), "webp", "image/webp"),
            ("clip", None, None, "mp4", "video/mp4"),
        ] {
            let preset = policy_preset(&settings, &test_context("local"), preset_id).unwrap();
            let key = TranscodeParams::resolve(WEBP_ITEM.to_string(), preset, start_cs, end_cs)
                .cache_key();
            let temp = cache.temp_path(ext);
            std::fs::write(&temp, b"0123456789").unwrap();
            cache
                .commit(
                    NewArtifact {
                        key: &key,
                        source_sha256: WEBP_ITEM,
                        params_hash: "hash",
                        preset: preset_id,
                        file_name: &format!("{key}.{ext}"),
                        download_name: &format!("{key}.{ext}"),
                        mime_type: mime,
                        transcoder_version: 1,
                    },
                    &temp,
                )
                .await
                .unwrap();

            let response = post(
                &settings,
                fixtures.path(),
                Some(600.0),
                preset_id,
                start_cs,
                end_cs,
            )
            .await
            .expect("accepted");
            assert_eq!(response.status(), StatusCode::OK, "{preset_id}");
            assert_eq!(body_json(response).await["outcome"], "hit");
        }
        cache.clear(true).await.unwrap();
    }

    const MOSAIC_A: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
    const MOSAIC_B: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

    /// Two items, each with a readable file: a composition resolves every one
    /// of its items before it can be keyed.
    async fn compose_fixture_db(
        dir: &std::path::Path,
    ) -> (DbConnection<ReadOnlyNoUserData>, RetainedDbs) {
        let mut dbs = crate::db::migrations::setup_test_databases().await;
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\gone")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        for (id, sha) in [(1, MOSAIC_A), (2, MOSAIC_B)] {
            let source = dir.join(format!("pin-{id}.mp4"));
            std::fs::write(&source, b"not really a video").unwrap();
            sqlx::query(
                "INSERT INTO items (id, sha256, md5, type, duration, time_added) \
                 VALUES (?, ?, ?, 'video/mp4', 12.0, '2024-01-01T00:00:00')",
            )
            .bind(id)
            .bind(sha)
            .bind(format!("md5_{id}"))
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO files \
                 (id, sha256, item_id, path, filename, last_modified, scan_id, available) \
                 VALUES (?, ?, ?, ?, ?, '2024-01-02T00:00:00', 1, 1)",
            )
            .bind(10 + id)
            .bind(sha)
            .bind(id)
            .bind(source.to_string_lossy().into_owned())
            .bind(format!("pin-{id}.mp4"))
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        }
        let crate::db::migrations::InMemoryDatabases {
            index_conn,
            storage_conn,
            user_data_conn,
        } = dbs;
        (
            DbConnection::<ReadOnlyNoUserData>::for_tests(index_conn, "compose", "test"),
            (storage_conn, user_data_conn),
        )
    }

    fn compose_body(items: Vec<serde_json::Value>) -> ComposeRequest {
        serde_json::from_value(serde_json::json!({
            "canvas": { "w": 320, "h": 240, "background": "#101820" },
            "fps": 25,
            "output": { "preset": "mosaic-mp4", "length": { "mode": "longest_loop_once" } },
            "items": items,
        }))
        .expect("the fixture document deserializes")
    }

    fn compose_item(sha: &str, dest: (i64, i64, i64, i64), time: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "sha256": sha,
            "src": { "x": 0, "y": 0, "w": 640, "h": 480 },
            "transform": { "quarter_turns": 1, "flip_h": false },
            "dest": { "x": dest.0, "y": dest.1, "w": dest.2, "h": dest.3 },
            "time": time,
            "audio": true,
        })
    }

    /// The compose route end to end against the index database: every item is
    /// resolved, the document is keyed as a whole, and the answer is the
    /// *same* envelope the single-file route returns — which is what lets one
    /// client follow both through one jobs/SSE/artifact path (§0.1).
    #[tokio::test]
    async fn the_compose_route_keys_the_document_and_shares_the_transcode_envelope() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        let settings = test_settings();
        let body = compose_body(vec![
            compose_item(
                MOSAIC_A,
                (0, 0, 160, 240),
                serde_json::json!({ "kind": "span", "start_cs": 0, "end_cs": 500 }),
            ),
            compose_item(
                MOSAIC_B,
                (160, 0, 160, 240),
                serde_json::json!({ "kind": "still", "at_cs": 50 }),
            ),
        ]);

        // The key the handler will compute, built the way the handler builds
        // it, and pre-filled so this test is about the route rather than about
        // ffmpeg.
        let preset = policy_preset(&settings, &test_context("local"), "mosaic-mp4").unwrap();
        let doc = compose::resolve_compose(&body, &preset, ComposeLimits::from_config())
            .expect("a valid document");
        assert_eq!(doc.target_cs, 500, "the longest span plays once");
        let key = ComposeParams::resolve(doc, preset).cache_key();
        assert!(key.starts_with("compose-"), "{key}");

        let cache = pool::transcode_cache().await.unwrap();
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        cache
            .commit(
                NewArtifact {
                    key: &key,
                    source_sha256: "compose",
                    params_hash: "hash",
                    preset: "mosaic-mp4",
                    file_name: &format!("{key}.mp4"),
                    download_name: &format!("{key}.mp4"),
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();

        let (db, _attached) = compose_fixture_db(fixtures.path()).await;
        let response = video_compose(
            State(test_state(&settings)),
            Extension(test_context("local")),
            db,
            Json(body),
        )
        .await
        .expect("the cached composition answers");
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        assert_eq!(json["outcome"], "hit");
        assert_eq!(json["artifact"]["key"], key);
        assert_eq!(json["artifact"]["url"], format!("/api/video/artifact?key={key}"));
        // No source stem exists for a composition, so the name is fixed.
        assert_eq!(json["artifact"]["filename"], "mosaic-2items.mp4");

        // And the artifact route serves that key like any other, naming the
        // download from the key alone.
        let served = video_artifact(
            State(test_state(&settings)),
            Extension(test_context("local")),
            unused_db().await,
            Query(ArtifactQuery {
                key: Some(key.clone()),
                id: None,
                id_type: None,
                preset: None,
                start_cs: None,
                end_cs: None,
            }),
            HeaderMap::new(),
        )
        .await
        .expect("a composition is an artifact like any other");
        assert_eq!(served.status(), StatusCode::OK);
        // The key records no item count, so the served name is the bare form.
        assert_eq!(
            served.headers()[header::CONTENT_DISPOSITION],
            "inline; filename=\"mosaic.mp4\""
        );
        assert_eq!(served.headers()[header::CACHE_CONTROL], CACHE_IMMUTABLE);
        cache.clear(true).await.unwrap();
    }

    /// A composition an item cannot be found for is a 404 that *names* the
    /// item: a mosaic of a dozen pins answered with a bare "Item not found"
    /// tells the client nothing about which pin to drop.
    #[tokio::test]
    async fn a_missing_composition_item_is_named_in_the_404() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        let settings = test_settings();
        let missing = "c3".repeat(32);
        let body = compose_body(vec![
            compose_item(
                MOSAIC_A,
                (0, 0, 160, 240),
                serde_json::json!({ "kind": "span", "start_cs": 0, "end_cs": 500 }),
            ),
            compose_item(&missing, (160, 0, 160, 240), serde_json::json!({ "kind": "image" })),
        ]);
        let (db, _attached) = compose_fixture_db(fixtures.path()).await;
        let err = video_compose(
            State(test_state(&settings)),
            Extension(test_context("local")),
            db,
            Json(body),
        )
        .await
        .expect_err("rejected");
        assert!(err.detail().contains(&missing), "{}", err.detail());
        assert_eq!(err.into_response().status(), StatusCode::NOT_FOUND);
    }

    /// A still frozen at or past its item's recorded length is refused here,
    /// naming the pin. It is the one admission rule the document alone cannot
    /// decide — the length lives in the index database — and letting it
    /// through would seek past every frame there is, failing the *whole* graph
    /// at dispatch with nothing to say which of a dozen pins caused it.
    #[tokio::test]
    async fn a_still_past_its_items_length_is_refused_by_name() {
        let _env = crate::test_utils::test_data_dir();
        let fixtures = tempfile::tempdir().unwrap();
        let settings = test_settings();

        // The fixture items are 12 s long.
        for at_cs in [1200, 1500] {
            let body = compose_body(vec![compose_item(
                MOSAIC_A,
                (0, 0, 160, 240),
                serde_json::json!({ "kind": "still", "at_cs": at_cs }),
            )]);
            let (db, _attached) = compose_fixture_db(fixtures.path()).await;
            let err = video_compose(
                State(test_state(&settings)),
                Extension(test_context("local")),
                db,
                Json(body),
            )
            .await
            .expect_err("rejected");
            assert!(err.detail().contains(MOSAIC_A), "{}", err.detail());
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "at_cs {at_cs}"
            );
        }
        // The admitted side of the rule needs no case of its own: the route
        // test above composes a still at 0.50 s of the same 12 s item.
    }

    /// The document-level rejections, which all land before any item is
    /// resolved — the connection below would fail any query it was handed.
    #[tokio::test]
    async fn a_refused_composition_never_reaches_the_items() {
        let settings = test_settings();
        let span = serde_json::json!({ "kind": "span", "start_cs": 0, "end_cs": 500 });
        let ok = compose_item(MOSAIC_A, (0, 0, 160, 240), span.clone());

        let mut odd_dest = compose_body(vec![ok.clone()]);
        odd_dest.items[0].dest.x = 15;
        let mut outside = compose_body(vec![ok.clone()]);
        outside.items[0].dest.w = 400;
        let mut frozen_span = compose_body(vec![compose_item(
            MOSAIC_A,
            (0, 0, 160, 240),
            serde_json::json!({ "kind": "span", "start_cs": 500, "end_cs": 500 }),
        )]);
        frozen_span.fps = 25;
        let mut unknown_preset = compose_body(vec![ok.clone()]);
        unknown_preset.output.preset = "nonexistent".to_string();
        let mut odd_canvas = compose_body(vec![ok.clone()]);
        odd_canvas.canvas.h = 241;
        let mut huge = compose_body(vec![ok]);
        huge.canvas.background = "not-a-colour".to_string();

        for body in [odd_dest, outside, frozen_span, unknown_preset, odd_canvas, huge] {
            let err = video_compose(
                State(test_state(&settings)),
                Extension(test_context("local")),
                unused_db().await,
                Json(body),
            )
            .await
            .expect_err("rejected before any item is looked up");
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }

    /// The pool weight: a composition past the light threshold runs alone,
    /// because its filtergraph holds every item's loop buffer at once. The
    /// exclusivity itself is the pool's own contract, tested there.
    #[test]
    fn compositions_past_the_threshold_take_the_pool_to_themselves() {
        let threshold = crate::config::runtime().transcode.compose_light_threshold;
        assert_eq!(compose_weight(1), JobWeight::Light);
        assert_eq!(compose_weight(threshold), JobWeight::Light);
        assert_eq!(compose_weight(threshold + 1), JobWeight::Exclusive);
    }

    /// The SSE contract, on the stream itself: the first event is the current
    /// snapshot (so a late joiner never has to poll), and the stream ends
    /// after the terminal one — a client whose EventSource stayed open would
    /// reconnect forever.
    #[tokio::test]
    async fn sse_stream_opens_with_a_snapshot_and_ends_after_the_terminal_event() {
        use crate::media_tools::transcode::pool::TranscodeJobEvent;

        let snapshot = |event| TranscodeJobSnapshot {
            id: "job-1".to_string(),
            event,
        };
        let (events, receiver) = tokio::sync::watch::channel(snapshot(
            TranscodeJobEvent::Queued { position: 3 },
        ));
        let response = Sse::new(job_event_stream(receiver)).into_response();
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let mut body = response.into_body();

        async fn next_event(body: &mut Body) -> Option<serde_json::Value> {
            let frame = body.frame().await?.expect("a frame");
            let text = String::from_utf8(frame.into_data().ok()?.to_vec()).expect("utf8");
            let payload = text.trim_start_matches("data: ").trim();
            Some(serde_json::from_str(payload).expect("json event"))
        }

        let first = next_event(&mut body).await.expect("a first event");
        assert_eq!(first["id"], "job-1");
        assert_eq!(first["state"], "queued");
        assert_eq!(first["position"], 3);

        events.send_replace(snapshot(TranscodeJobEvent::Running {
            progress: Some(0.5),
        }));
        let running = next_event(&mut body).await.expect("a progress event");
        assert_eq!(running["state"], "running");
        assert_eq!(running["progress"], 0.5);

        events.send_replace(snapshot(TranscodeJobEvent::Failed {
            error: "bad input".to_string(),
            cancelled: false,
        }));
        let terminal = next_event(&mut body).await.expect("the terminal event");
        assert_eq!(terminal["state"], "failed");
        assert_eq!(terminal["error"], "bad input");
        assert_eq!(terminal["cancelled"], false);
        assert!(
            next_event(&mut body).await.is_none(),
            "the stream ends after the terminal event"
        );
    }
}
