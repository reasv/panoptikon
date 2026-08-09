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
use crate::api_error::ApiError;
use crate::config::{PolicyConfig, Settings};
use crate::db::items::{FileRecord, ItemIdentifierType, ItemRecord, get_item_metadata_unchecked};
use crate::db::{DbConnection, ReadOnlyNoUserData};
use crate::media_tools::transcode::cache::{CacheStats, ResizeError};
use crate::media_tools::transcode::pool::{
    self, ArtifactRef, JobWeight, SubmitOutcome, SubmitRequest, TranscodeJobSnapshot,
};
use crate::media_tools::transcode::presets::{
    Channel, Container, ResolvedPreset, Surface, find_preset, resolve_presets,
};
use crate::media_tools::transcode::TranscodeParams;
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
    /// Reserved for the server-side outro cut (`"outro"`), which arrives with
    /// clip export. Rejected until then rather than ignored: a client that
    /// sent it and got a full-length file would have no way to notice.
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
}

/// Composition limits, carried alongside the presets so a client builder
/// clamps against live config instead of mirrored constants.
#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct TranscodeLimits {
    pub max_mosaic_inputs: usize,
    pub max_mosaic_loop_mb: u64,
    pub max_animated_image_seconds: u64,
    pub max_output_seconds: u64,
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
        `cut` is reserved for the server-side outro cut and is rejected for now.",
    params(DbQueryParams),
    request_body = TranscodeRequest,
    responses(
        (status = 200, description = "The rendition was already cached", body = TranscodeSubmitResponse),
        (status = 202, description = "A job was created or joined", body = TranscodeSubmitResponse),
        (status = 404, description = "No such item, or no readable file for it")
    )
)]
pub async fn video_transcode(
    State(state): State<Arc<ProxyState>>,
    axum::Extension(context): axum::Extension<PolicyContext>,
    mut db: DbConnection<ReadOnlyNoUserData>,
    Json(body): Json<TranscodeRequest>,
) -> ApiResult<Response<Body>> {
    if body.cut.is_some() {
        return Err(ApiError::bad_request(
            "`cut` is not supported yet; send explicit start_cs/end_cs bounds",
        ));
    }
    let preset = policy_preset(&state.settings, &context, &body.preset)?;
    validate_bounds(body.start_cs, body.end_cs)?;

    let source = resolve_source(&mut db, &body.id, body.id_type).await?;
    drop(db);
    if let Some(start_cs) = body.start_cs
        && let Some(duration) = source.item.duration
        && (start_cs as f64) / 100.0 >= duration
    {
        return Err(unprocessable("start_cs is past the end of the item"));
    }

    let params = resolve_params(
        source.item.sha256.clone(),
        preset,
        body.start_cs,
        body.end_cs,
    )
    .await?;
    let outcome = pool::submit(SubmitRequest {
        params,
        source_path: source.file.path.clone().into(),
        source_duration_s: source.item.duration,
        // Single-file jobs never need the pool to themselves; compositions do.
        weight: JobWeight::Light,
    })
    .await?;

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
    Ok((status, Json(body)).into_response())
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
        (status = 416, description = "Requested range not satisfiable")
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
    let filename = transcode_file_name(
        target.stem.as_deref(),
        source_sha_of(&target.key),
        target.trimmed,
        &artifact.preset,
        ext,
    );
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

fn validate_bounds(start_cs: Option<i64>, end_cs: Option<i64>) -> ApiResult<()> {
    if start_cs.is_some_and(|start| start < 0) || end_cs.is_some_and(|end| end < 0) {
        return Err(unprocessable("trim bounds must not be negative"));
    }
    if let (Some(start), Some(end)) = (start_cs, end_cs)
        && end <= start
    {
        return Err(unprocessable("end_cs must be greater than start_cs"));
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
    for file in metadata.files {
        let readable = tokio::time::timeout(FILE_IO_TIMEOUT, tokio::fs::metadata(&file.path))
            .await
            .ok()
            .and_then(Result::ok)
            .is_some_and(|meta| meta.is_file());
        if readable {
            return Ok(ResolvedSource { item, file });
        }
    }
    Err(ApiError::not_found("No readable file found for item"))
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
        stem: metadata.files.first().and_then(file_stem),
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
    }
}

fn file_stem(file: &FileRecord) -> Option<String> {
    std::path::Path::new(&file.filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
        .filter(|stem| !stem.is_empty())
}

/// The `<source sha256>-<params hash>` key's first half.
fn source_sha_of(key: &str) -> &str {
    key.split('-').next().unwrap_or(key)
}

/// The name a download gets (design §8 / implementation plan §3 S3): the
/// source's stem plus `-clip` for a trimmed cut or `-<preset>` for a plain
/// re-encode. A request that carried no path (the `key=` form) falls back to
/// a hash prefix, which is still stable and still identifies the source.
fn transcode_file_name(
    stem: Option<&str>,
    sha256: &str,
    trimmed: bool,
    preset_id: &str,
    ext: &str,
) -> String {
    let suffix = if trimmed {
        "-clip".to_string()
    } else {
        format!("-{preset_id}")
    };
    let base = match stem {
        Some(stem) => stem.to_string(),
        None => sha256.chars().take(10).collect(),
    };
    format!("{base}{suffix}.{ext}")
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
    /// reach by dragging both markers together.
    #[test]
    fn trim_bounds_are_validated() {
        assert!(validate_bounds(None, None).is_ok());
        assert!(validate_bounds(Some(0), Some(1)).is_ok());
        assert!(validate_bounds(Some(500), None).is_ok());
        assert!(validate_bounds(None, Some(500)).is_ok());
        for (start, end) in [
            (Some(-1), None),
            (None, Some(-1)),
            (Some(500), Some(500)),
            (Some(500), Some(499)),
        ] {
            let err = validate_bounds(start, end).expect_err("rejected");
            assert_eq!(
                err.into_response().status(),
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }

    /// The download name: a trimmed artifact is a clip, an untrimmed one is
    /// named after its preset, and a request that carried no path falls back
    /// to the source hash prefix.
    #[test]
    fn transcode_file_names_follow_the_request() {
        assert_eq!(
            transcode_file_name(Some("holiday"), "abc", true, "clip", "mp4"),
            "holiday-clip.mp4"
        );
        assert_eq!(
            transcode_file_name(Some("holiday"), "abc", false, "clip-fast", "mp4"),
            "holiday-clip-fast.mp4"
        );
        assert_eq!(
            transcode_file_name(None, "0123456789abcdef", false, "webp-anim", "webp"),
            "0123456789-webp-anim.webp"
        );
        assert_eq!(source_sha_of("deadbeef-0011"), "deadbeef");
        assert_eq!(source_sha_of("nodash"), "nodash");
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
                "mosaic-mp4",
                "mosaic-mp4-fast",
                "mosaic-webm",
            ]
        );
        assert_eq!(json["presets"][0]["ext"], "mp4");
        assert_eq!(json["presets"][0]["channel"], "fast");
        assert_eq!(json["presets"][0]["surfaces"][0], "playback");
        assert_eq!(json["presets"][3]["ext"], "webp");
        // The compose limits ride along, so a client builder clamps against
        // live config rather than mirrored constants.
        let transcode = &crate::config::runtime().transcode;
        assert_eq!(
            json["limits"]["max_mosaic_inputs"],
            transcode.max_mosaic_inputs
        );
        assert_eq!(
            json["limits"]["max_output_seconds"],
            transcode.max_output_seconds
        );

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
