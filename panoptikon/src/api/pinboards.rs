use axum::{
    Json,
    body::Body,
    extract::Path,
    http::{Response, header},
};
use axum_extra::extract::Query;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sqlx::Connection as _;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api_error::ApiError;
use crate::db::pinboard_dbs::{
    self, AssociationContext, BoardOverlap, PinboardAssociation, StampIdentity,
};
use crate::db::pinboards::{self, PinboardOrder};
use crate::db::{DbConnection, ReadOnly, UserDataWrite, open_user_data_write};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Unix seconds, the clock the activity columns are stamped in. Every db
/// helper takes the time as a parameter so tests run on fixed clocks.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

const DEFAULT_USER: &str = "user";
/// Decoded preview blobs larger than this are rejected outright.
const MAX_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
/// Serialized layouts larger than this are rejected outright.
const MAX_LAYOUT_BYTES: usize = 1024 * 1024;
/// Serialized board flags larger than this are rejected outright.
const MAX_FLAGS_BYTES: usize = 4096;
/// Upper bound for a recorded preview dimension, in pixels. Far above any
/// composite a browser canvas can produce, low enough to be obviously bogus.
const MAX_PREVIEW_DIMENSION: i64 = 100_000;
/// Databases one manual-editor request may name. A board's checklist is the
/// local index databases plus its own stamped names; a couple of dozen would
/// already be a remarkable installation, and every name costs a resolution
/// and a row write inside the write transaction.
const MAX_BOARD_DATABASES: usize = 64;
/// Longest index database name the editor will consider. Names are folder
/// names, so a real one is short; this only keeps a client from spending the
/// transaction's time on strings that could never match a folder.
const MAX_DB_NAME_BYTES: usize = 256;

fn default_user() -> String {
    DEFAULT_USER.to_string()
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PinboardUserQuery {
    /// The user the pinboard belongs to.
    #[serde(default = "default_user")]
    #[param(default = "user")]
    user: String,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PinboardListQuery {
    /// The user whose pinboards to list.
    #[serde(default = "default_user")]
    #[param(default = "user")]
    user: String,
    /// Optional name search (FTS prefix match on pinboard names).
    q: Option<String>,
    /// List ordering: `activity` (recency + decaying visit frequency, the
    /// default) or `updated` (last saved first).
    #[serde(default)]
    order: PinboardOrder,
    /// Return only the boards associated with the selected index database.
    /// The verdict is server-computed (see `associated`); the client sends
    /// its stored preference.
    #[serde(default)]
    associated_only: bool,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PinboardPreviewQuery {
    /// The user the pinboard belongs to.
    #[serde(default = "default_user")]
    #[param(default = "user")]
    user: String,
    /// Downscale the preview to at most this width (pixels), preserving
    /// aspect ratio. Omit for the stored full-resolution image.
    maxw: Option<u32>,
}

/// The saved state of a pinboard: the UI's `pinboard` URL param verbatim
/// (`layout`), the distinct full-sha256 items on the board for search
/// indexing (`items`), and an optional client-composited preview image.
#[derive(Deserialize, ToSchema)]
pub(crate) struct SaveVersionRequest {
    /// The pinboard URL param, verbatim: version token + 5-string records.
    layout: Vec<String>,
    /// Full sha256 hashes of the distinct items on the board.
    #[serde(default)]
    items: Vec<String>,
    /// Base64-encoded preview image (WebP or PNG), composited client-side.
    preview_b64: Option<String>,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    /// Height in preview-image pixels of one save-time viewport screenful.
    screenful_h: Option<i64>,
    /// Board-level editing-behavior flags (auto-layout & co.): an opaque
    /// JSON object owned by the UI, stored on the BOARD rather than the
    /// version — flag changes never create versions and never make a board
    /// "unsaved". Omitted = leave the stored flags unchanged.
    #[serde(default)]
    flags: Option<serde_json::Value>,
}

/// A replacement preview image for an existing version. Same field semantics
/// as the preview half of [`SaveVersionRequest`]; nothing else about the
/// version can be changed.
#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdatePreviewRequest {
    /// Base64-encoded preview image (WebP or PNG), composited client-side.
    preview_b64: String,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    /// Height in preview-image pixels of one save-time viewport screenful.
    screenful_h: Option<i64>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreatePinboardRequest {
    /// Optional display name; pinboards are identified by preview otherwise.
    name: Option<String>,
    #[serde(flatten)]
    version: SaveVersionRequest,
}

/// The manual editor's payload: exactly the databases the board should be
/// associated with afterwards. An empty list clears every association.
#[derive(Deserialize, ToSchema)]
pub(crate) struct SetPinboardDatabasesRequest {
    /// Index database names. Each keeps every stamp already stored under it
    /// (including one whose database no longer exists, which the server could
    /// not mint again) *and* associates the board with the live database that
    /// name refers to here. A name that is neither stamped nor local is a
    /// 400. Omitting a name removes it.
    databases: Vec<String>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct RenamePinboardRequest {
    name: Option<String>,
    /// When true, the head version's name-at-save snapshot is rewritten too.
    /// The client passes true when the current layout equals the head's
    /// ("a rename labels what you're looking at").
    #[serde(default)]
    relabel_head: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct SavePinboardResponse {
    pinboard_id: i64,
    version_id: i64,
    /// True when the layout was byte-identical to the head version and no
    /// new version was created; version_id is the existing head.
    no_op: bool,
    /// True when the board's stored flags changed as part of this save.
    /// With `no_op: true` this distinguishes a settings-only save ("Settings
    /// updated") from a save with nothing to do ("No changes to save").
    flags_updated: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardSummaryResponse {
    id: i64,
    name: Option<String>,
    head_version_id: Option<i64>,
    time_added: String,
    time_updated: String,
    /// Unix seconds of the board's last activity — opening it counts, not
    /// just saving. Null only for rows predating the activity columns.
    last_seen: Option<i64>,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
    item_count: i64,
    version_count: i64,
    /// How many of the board's items exist in the selected index database.
    /// Below `item_count` this is rot ("38/40 here"), and is reported
    /// whatever `associated_only` says — it is what tells rot apart from a
    /// board that belongs somewhere else.
    present_count: i64,
    /// Whether the board belongs to the selected index database, by the full
    /// rule: a stamp for this database (by identity, or by name for a
    /// database this instance rebuilt), or 100% of its items present here.
    associated: bool,
    /// The databases this board is stamped for, newest stamp first.
    databases: Vec<PinboardDatabaseResponse>,
}

/// One stamped database of a board. Databases are named, never identified by
/// UUID, on the wire: the UUIDs are server-side matching keys.
#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardDatabaseResponse {
    /// The index database's name as of the stamp. It may no longer resolve
    /// to a local database, in which case it is a residual label only.
    name: String,
    /// Unix seconds of the last stamp for this database.
    last_stamped: i64,
    /// Whether this row is the database currently selected.
    associated: bool,
}

/// A board's associations after the manual editor changed them — the same
/// two fields the list and detail responses carry, so the client can update
/// the card in place without re-listing.
#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardDatabasesResponse {
    /// Whether the board now belongs to the selected index database, by the
    /// full rule (so it can still be true through 100% item overlap with no
    /// stamp at all).
    associated: bool,
    /// The databases the board is stamped for, newest stamp first.
    databases: Vec<PinboardDatabaseResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardVersionResponse {
    id: i64,
    /// The pinboard URL param, verbatim, as saved.
    layout: Vec<String>,
    name_at_save: Option<String>,
    time_added: String,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
    item_count: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardListResponse {
    pinboards: Vec<PinboardSummaryResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardDetailResponse {
    id: i64,
    name: Option<String>,
    /// The board's stored editing-behavior flags, verbatim as last saved.
    /// Null for boards saved before flags existed; the UI treats that as
    /// its codec defaults.
    flags: Option<serde_json::Value>,
    time_added: String,
    time_updated: String,
    version_count: i64,
    /// The head version's items that exist in the selected index database
    /// (`head.item_count` is the total). Same field as on the list summary.
    present_count: i64,
    /// Whether the board belongs to the selected index database.
    associated: bool,
    /// The databases this board is stamped for, newest stamp first.
    databases: Vec<PinboardDatabaseResponse>,
    head: Option<PinboardVersionResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardVersionsResponse {
    /// Newest first.
    versions: Vec<PinboardVersionResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct PinboardDeleteResponse {
    message: String,
    /// True when the operation removed the board itself (deleting its last
    /// version, or DELETE on the board).
    deleted_board: bool,
    /// The board's head version after the operation, if the board remains.
    new_head_version_id: Option<i64>,
}

/// A validated preview upload: the decoded blob plus the dimensions that
/// will be recorded next to it, each range-checked and cross-checked
/// against the image itself.
struct PreviewUpload {
    bytes: Option<Vec<u8>>,
    width: Option<i64>,
    height: Option<i64>,
    screenful_h: Option<i64>,
}

/// Validates and canonicalizes the request's flags into the stored string
/// form. Keys are sorted so byte comparison in set_flags is insensitive to
/// the client's object key order. None when the request carries no flags.
fn canonical_flags(request: &SaveVersionRequest) -> ApiResult<Option<String>> {
    let Some(value) = &request.flags else {
        return Ok(None);
    };
    let Some(map) = value.as_object() else {
        return Err(ApiError::bad_request("Flags must be a JSON object"));
    };
    let sorted: std::collections::BTreeMap<&String, &serde_json::Value> = map.iter().collect();
    let serialized =
        serde_json::to_string(&sorted).map_err(|_| ApiError::bad_request("Invalid flags"))?;
    if serialized.len() > MAX_FLAGS_BYTES {
        return Err(ApiError::bad_request("Flags too large"));
    }
    Ok(Some(serialized))
}

fn parse_stored_flags(raw: Option<String>) -> Option<serde_json::Value> {
    let raw = raw?;
    match serde_json::from_str(&raw) {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::error!(error = %err, "failed to parse stored pinboard flags");
            None
        }
    }
}

fn validate_version_request(request: &SaveVersionRequest) -> ApiResult<PreviewUpload> {
    if request.layout.is_empty() {
        return Err(ApiError::bad_request("Layout must not be empty"));
    }
    let serialized_len: usize = request.layout.iter().map(|record| record.len() + 3).sum();
    if serialized_len > MAX_LAYOUT_BYTES {
        return Err(ApiError::bad_request("Layout too large"));
    }
    for sha256 in &request.items {
        if sha256.is_empty()
            || sha256.len() > 64
            || !sha256.chars().all(|ch| ch.is_ascii_hexdigit())
        {
            return Err(ApiError::bad_request("Invalid sha256 in items"));
        }
    }

    validate_preview_upload(
        request.preview_b64.as_deref(),
        request.preview_w,
        request.preview_h,
        request.screenful_h,
    )
}

/// The one place a preview blob and its declared dimensions are accepted.
///
/// Every writer goes through this — POST create, POST version, and the PUT
/// preview replacement — because the read side treats the recorded
/// `preview_w`/`preview_h` as the truth about the stored bytes: the serve
/// path answers a `maxw` request with the stored image whenever `preview_w`
/// says it is already narrow enough (so a too-small declared width makes
/// even the 160px history rail download the full master forever), and the
/// library/history cards frame their crop from the declared pair. A single
/// wrong write would poison every consumer of that version for the rest of
/// its life, so the declared dimensions are verified against the actual
/// encoded image here rather than trusted.
fn validate_preview_upload(
    preview_b64: Option<&str>,
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
) -> ApiResult<PreviewUpload> {
    let bytes = decode_preview(preview_b64)?;
    let width = validate_preview_dimension(preview_w)?;
    let height = validate_preview_dimension(preview_h)?;
    // A viewport height, not an image dimension: range-checked only.
    let screenful_h = validate_preview_dimension(screenful_h)?;

    if let Some(encoded) = bytes.as_deref()
        && (width.is_some() || height.is_some())
    {
        let (actual_w, actual_h) = probe_image_dimensions(encoded)?;
        if width.is_some_and(|declared| declared != i64::from(actual_w))
            || height.is_some_and(|declared| declared != i64::from(actual_h))
        {
            return Err(ApiError::bad_request(
                "Preview dimensions do not match the uploaded image",
            ));
        }
    }

    Ok(PreviewUpload {
        bytes,
        width,
        height,
        screenful_h,
    })
}

/// Reads an encoded image's pixel dimensions from its header, without
/// decoding the pixels — cheap enough to run on every preview write.
fn probe_image_dimensions(bytes: &[u8]) -> ApiResult<(u32, u32)> {
    image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|_| ApiError::bad_request("Unreadable preview image"))?
        .into_dimensions()
        .map_err(|_| ApiError::bad_request("Unreadable preview image"))
}

/// Decodes an uploaded preview blob. Absent or empty means "no preview",
/// which every upload path treats as a version without a picture.
fn decode_preview(preview_b64: Option<&str>) -> ApiResult<Option<Vec<u8>>> {
    match preview_b64 {
        None | Some("") => Ok(None),
        Some(encoded) => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| ApiError::bad_request("Invalid base64 preview"))?;
            if decoded.len() > MAX_PREVIEW_BYTES {
                return Err(ApiError::bad_request("Preview image too large"));
            }
            Ok(Some(decoded))
        }
    }
}

/// Rejects preview dimensions that cannot describe a real image. Absent is
/// allowed (that is what a version without recorded dimensions looks like);
/// zero, negative and absurd values are not — the serve path trusts
/// `preview_w` to decide whether a `maxw` request needs any work at all.
///
/// This is only the range half of that trust. The other half — that the
/// declared numbers are the image's ACTUAL dimensions — is enforced at
/// write time too, by [`validate_preview_upload`], which every writer of a
/// preview goes through; the serve path is therefore reading an invariant,
/// not a client's claim.
fn validate_preview_dimension(value: Option<i64>) -> ApiResult<Option<i64>> {
    match value {
        None => Ok(None),
        Some(px) if px > 0 && px <= MAX_PREVIEW_DIMENSION => Ok(Some(px)),
        Some(_) => Err(ApiError::bad_request("Invalid preview dimensions")),
    }
}

fn map_summary(
    summary: pinboards::PinboardSummary,
    association: PinboardAssociation,
) -> PinboardSummaryResponse {
    PinboardSummaryResponse {
        id: summary.id,
        name: summary.name,
        head_version_id: summary.head_version_id,
        time_added: summary.time_added,
        time_updated: summary.time_updated,
        last_seen: summary.last_seen,
        preview_w: summary.preview_w,
        preview_h: summary.preview_h,
        screenful_h: summary.screenful_h,
        item_count: summary.item_count,
        version_count: summary.version_count,
        present_count: summary.present_count,
        associated: association.associated,
        databases: map_databases(association.databases),
    }
}

/// The stamp rows of one board, in wire form.
pub(crate) fn map_databases(
    databases: Vec<pinboard_dbs::PinboardDatabase>,
) -> Vec<PinboardDatabaseResponse> {
    databases
        .into_iter()
        .map(|database| PinboardDatabaseResponse {
            name: database.name,
            last_stamped: database.last_stamped,
            associated: database.associated,
        })
        .collect()
}

/// The overlap counts clause (c) of the association rule reads, as the
/// listing queries already computed them.
fn overlap_of(summary: &pinboards::PinboardSummary) -> BoardOverlap {
    BoardOverlap {
        pinboard_id: summary.id,
        present_count: summary.present_count,
        item_count: summary.item_count,
    }
}

fn map_version(version: pinboards::PinboardVersionRecord) -> PinboardVersionResponse {
    PinboardVersionResponse {
        id: version.id,
        layout: version.layout,
        name_at_save: version.name_at_save,
        time_added: version.time_added,
        preview_w: version.preview_w,
        preview_h: version.preview_h,
        screenful_h: version.screenful_h,
        item_count: version.item_count,
    }
}

#[utoipa::path(
    get,
    operation_id = "list_pinboards",
    path = "/api/pinboards",
    tag = "pinboards",
    summary = "List saved pinboards",
    description = "Lists the user's saved pinboards with head-version metadata (preview dimensions, item and version counts) but without layouts or preview blobs.\nOrdered by `order`: `activity` (default) ranks by a recency strip followed by a decaying visit score — opening a board counts as activity, not just saving it — while `updated` is plain last-saved-first. The order applies identically under the `q` name search (FTS prefix match).\nEach board carries its association with the selected index database: `associated` (stamped for this database, or fully present in it), the stamped `databases`, and `present_count` — which is reported whether or not `associated_only` filters the list.",
    params(DbQueryParams, PinboardListQuery),
    responses(
        (status = 200, description = "Saved pinboards", body = PinboardListResponse)
    )
)]
pub async fn list_pinboards(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<PinboardListQuery>,
) -> ApiResult<Json<PinboardListResponse>> {
    let ctx = AssociationContext::load(&mut db.conn, &db.index_db).await;
    let pinboards = list_pinboard_summaries(&mut db.conn, &ctx, &query, unix_now()).await?;
    Ok(Json(PinboardListResponse { pinboards }))
}

/// The list endpoint's body: the summaries, their association verdicts, and
/// the `associated_only` filter — applied *after* the verdicts are computed,
/// so it only ever hides rows.
///
/// Split out of the handler so tests can drive it with a synthetic
/// association context; building the real one reads the process-global data
/// folder.
async fn list_pinboard_summaries(
    conn: &mut sqlx::SqliteConnection,
    ctx: &AssociationContext,
    query: &PinboardListQuery,
    now: i64,
) -> ApiResult<Vec<PinboardSummaryResponse>> {
    let summaries =
        pinboards::list_pinboards(conn, &query.user, query.q.as_deref(), query.order, now).await?;
    let overlaps: Vec<BoardOverlap> = summaries.iter().map(overlap_of).collect();
    let mut associations = pinboard_dbs::load_associations(conn, ctx, &overlaps).await?;

    let mut pinboards: Vec<PinboardSummaryResponse> = summaries
        .into_iter()
        .map(|summary| {
            let association = associations.remove(&summary.id).unwrap_or_default();
            map_summary(summary, association)
        })
        .collect();
    if query.associated_only {
        pinboards.retain(|board| board.associated);
    }
    Ok(pinboards)
}

#[utoipa::path(
    post,
    operation_id = "create_pinboard",
    path = "/api/pinboards",
    tag = "pinboards",
    summary = "Create a pinboard",
    description = "Creates a new pinboard with its first version. `layout` is the UI's pinboard URL param stored verbatim; `items` are the full sha256 hashes of the board's distinct items, used as a search index over the head version.",
    params(DbQueryParams, PinboardUserQuery),
    request_body(content = CreatePinboardRequest),
    responses(
        (status = 200, description = "Created pinboard", body = SavePinboardResponse)
    )
)]
pub async fn create_pinboard(
    mut db: DbConnection<UserDataWrite>,
    Query(query): Query<PinboardUserQuery>,
    Json(request): Json<CreatePinboardRequest>,
) -> ApiResult<Json<SavePinboardResponse>> {
    let preview = validate_version_request(&request.version)?;
    let flags = canonical_flags(&request.version)?;
    // Before the transaction on purpose: obtaining the identity reads the
    // instance file and canonicalizes the name against the index folder
    // listing, and this deployment keeps its data on a network mount — that
    // is not work to do while holding the user_data write lock.
    let identity = StampIdentity::load(&mut db.conn, &db.index_db).await;

    begin_transaction(&mut db.conn).await?;
    let result = create_board_with_version(
        &mut db.conn,
        &query.user,
        &request,
        &preview,
        flags.as_deref(),
        identity.as_ref(),
        unix_now(),
    )
    .await;

    match result {
        Ok((pinboard_id, version_id)) => {
            commit_transaction(&mut db.conn).await?;
            Ok(Json(SavePinboardResponse {
                pinboard_id,
                version_id,
                no_op: false,
                flags_updated: false,
            }))
        }
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            Err(err)
        }
    }
}

/// The create transaction's body: the board, its first version, and the stamp
/// for the database it was made in.
///
/// Split out of the handler so tests can drive it with a synthetic identity —
/// and with none at all, which is a process-global fact the real loader
/// cannot be talked out of.
async fn create_board_with_version(
    conn: &mut sqlx::SqliteConnection,
    user: &str,
    request: &CreatePinboardRequest,
    preview: &PreviewUpload,
    flags: Option<&str>,
    identity: Option<&StampIdentity>,
    now: i64,
) -> ApiResult<(i64, i64)> {
    let pinboard_id =
        pinboards::create_pinboard(conn, user, request.name.as_deref(), flags, now).await?;
    let version_id = pinboards::append_version(
        conn,
        pinboard_id,
        &request.version.layout,
        &request.version.items,
        preview.bytes.as_deref(),
        preview.width,
        preview.height,
        preview.screenful_h,
    )
    .await?;
    // Unconditional, unlike the save path's overlap test: the database a board
    // is MADE in is the strongest signal there is, whether or not its items
    // happen to be indexed here yet.
    pinboard_dbs::stamp_current_db(conn, pinboard_id, identity, now).await?;
    Ok((pinboard_id, version_id))
}

#[utoipa::path(
    get,
    operation_id = "get_pinboard",
    path = "/api/pinboards/{pinboard_id}",
    tag = "pinboards",
    summary = "Get a pinboard with its head version",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    responses(
        (status = 200, description = "Pinboard with head version", body = PinboardDetailResponse),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn get_pinboard(
    mut db: DbConnection<ReadOnly>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
) -> ApiResult<Json<PinboardDetailResponse>> {
    let Some((summary, head)) =
        pinboards::get_pinboard(&mut db.conn, pinboard_id, &query.user).await?
    else {
        return Err(ApiError::not_found("Pinboard not found"));
    };
    spawn_activity_write(&db, pinboard_id, &query.user, &summary);
    let ctx = AssociationContext::load(&mut db.conn, &db.index_db).await;
    let association = board_association(&mut db.conn, &ctx, &summary).await?;
    Ok(Json(map_detail(summary, head, association)))
}

/// The association verdict for a single board. Same computation as the list,
/// asked for one row.
async fn board_association(
    conn: &mut sqlx::SqliteConnection,
    ctx: &AssociationContext,
    summary: &pinboards::PinboardSummary,
) -> ApiResult<PinboardAssociation> {
    let pinboard_id = summary.id;
    Ok(
        pinboard_dbs::load_associations(conn, ctx, &[overlap_of(summary)])
            .await?
            .remove(&pinboard_id)
            .unwrap_or_default(),
    )
}

fn map_detail(
    summary: pinboards::PinboardSummary,
    head: Option<pinboards::PinboardVersionRecord>,
    association: PinboardAssociation,
) -> PinboardDetailResponse {
    PinboardDetailResponse {
        id: summary.id,
        name: summary.name,
        flags: parse_stored_flags(summary.flags),
        time_added: summary.time_added,
        time_updated: summary.time_updated,
        version_count: summary.version_count,
        present_count: summary.present_count,
        associated: association.associated,
        databases: map_databases(association.databases),
        head: head.map(map_version),
    }
}

/// Counts an open of `pinboard_id`, unless the last counted event is still
/// inside the debounce window — the check is free, riding on the row
/// `get_pinboard` already fetched. Every path that shows a board hits that
/// endpoint (library open, `pbid`+`pbl` links, and every page load, refresh
/// or session restore of a tab with a board open), so this is the entire
/// recording mechanism; there is no client-side beacon.
///
/// Fire-and-forget: the response never waits. The spawned task opens its own
/// short-lived write connection because `get_pinboard` runs on a pooled READ
/// connection, and that connection deliberately bypasses `DbConnection`'s
/// drop-time epoch bump — the activity columns can never affect a search
/// result, so counting an open must not invalidate the search cache.
/// Failures (notably losing a lock race with a concurrent save) are logged
/// at debug and dropped: activity data is telemetry, not content.
fn spawn_activity_write(
    db: &DbConnection<ReadOnly>,
    pinboard_id: i64,
    user: &str,
    summary: &pinboards::PinboardSummary,
) {
    let now = unix_now();
    if crate::db::readonly_mode() || !pinboards::activity_due(summary.frecency_at, now) {
        return;
    }
    let index_db = db.index_db.clone();
    let user_data_db = db.user_data_db.clone();
    let user = user.to_string();
    let frecency = summary.frecency;
    let frecency_at = summary.frecency_at;
    tokio::spawn(async move {
        let mut conn = match open_user_data_write(&index_db, &user_data_db).await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::debug!(error = ?err, "pinboard activity: failed to open database");
                return;
            }
        };
        if let Err(err) =
            pinboards::record_open(&mut conn, pinboard_id, &user, now, frecency, frecency_at).await
        {
            tracing::debug!(error = %err, "pinboard activity: failed to record open");
        }
        let _ = conn.close().await;
    });
}

#[utoipa::path(
    patch,
    operation_id = "update_pinboard",
    path = "/api/pinboards/{pinboard_id}",
    tag = "pinboards",
    summary = "Rename a pinboard",
    description = "Updates the pinboard's display name without creating a version.\nWith `relabel_head`, the head version's name-at-save snapshot is rewritten too; the client passes true when the current layout equals the head's, so the rename labels the version being looked at.",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    request_body(content = RenamePinboardRequest),
    responses(
        (status = 200, description = "Renamed", body = PinboardDeleteResponse),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn update_pinboard(
    mut db: DbConnection<UserDataWrite>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
    Json(request): Json<RenamePinboardRequest>,
) -> ApiResult<Json<PinboardDeleteResponse>> {
    begin_transaction(&mut db.conn).await?;
    let renamed = match pinboards::rename_pinboard(
        &mut db.conn,
        pinboard_id,
        &query.user,
        request.name.as_deref(),
        request.relabel_head,
    )
    .await
    {
        Ok(renamed) => renamed,
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            return Err(err);
        }
    };
    commit_transaction(&mut db.conn).await?;

    if !renamed {
        return Err(ApiError::not_found("Pinboard not found"));
    }
    Ok(Json(PinboardDeleteResponse {
        message: "Renamed pinboard".to_string(),
        deleted_board: false,
        new_head_version_id: None,
    }))
}

#[utoipa::path(
    put,
    operation_id = "set_pinboard_databases",
    path = "/api/pinboards/{pinboard_id}/databases",
    tag = "pinboards",
    summary = "Replace the databases a pinboard is associated with",
    description = "Sets the board's database associations to exactly the names given, replacing whatever was there (an empty list clears them). This is the manual fix path every automatic verdict has: associations are hints, so renames, accidental stamps and instance-identity resets all need somewhere to go.\nA name the board is already stamped for is kept exactly as stored — including one whose database no longer exists locally, which the server has no way to mint again. Every other name must resolve to a local index database; one that resolves to nothing is a 400 and nothing is written. Removing a name is expressed by omitting it.\nThe board's `time_updated` is deliberately not bumped: an association is not a content change and must not reorder the library.",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    request_body(content = SetPinboardDatabasesRequest),
    responses(
        (status = 200, description = "The board's associations after the change", body = PinboardDatabasesResponse),
        (status = 400, description = "A name that is neither already stamped nor a local index database"),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn set_pinboard_databases(
    mut db: DbConnection<UserDataWrite>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
    Json(request): Json<SetPinboardDatabasesRequest>,
) -> ApiResult<Json<PinboardDatabasesResponse>> {
    validate_database_names(&request.databases)?;
    // Before the transaction: building the context probes the local database
    // files, which is not work to do while holding the write lock. It is also
    // what resolves the names, and what computes the response's verdict.
    let ctx = AssociationContext::load(&mut db.conn, &db.index_db).await;

    begin_transaction(&mut db.conn).await?;
    let result: ApiResult<PinboardDatabasesResponse> = async {
        replace_board_databases(
            &mut db.conn,
            &ctx,
            pinboard_id,
            &query.user,
            &request.databases,
            unix_now(),
        )
        .await?;
        // Re-read rather than reason about what was written: the response is
        // the full verdict, which the overlap clause can satisfy with no stamp
        // at all. Inside the transaction, so it cannot report a board another
        // writer has since changed (or deleted) out from under it.
        let Some((summary, _)) =
            pinboards::get_pinboard(&mut db.conn, pinboard_id, &query.user).await?
        else {
            return Err(ApiError::not_found("Pinboard not found"));
        };
        let association = board_association(&mut db.conn, &ctx, &summary).await?;
        Ok(PinboardDatabasesResponse {
            associated: association.associated,
            databases: map_databases(association.databases),
        })
    }
    .await;

    match result {
        Ok(response) => {
            commit_transaction(&mut db.conn).await?;
            Ok(Json(response))
        }
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            Err(err)
        }
    }
}

/// Bounds the editor's payload before any of it reaches the write
/// transaction. Every name costs a resolution and a row write under the
/// user_data write lock, and neither limit can be reached by the shipped
/// client: the checklist is the local databases plus the board's own stamps,
/// and the names are folder names.
fn validate_database_names(names: &[String]) -> ApiResult<()> {
    if names.len() > MAX_BOARD_DATABASES {
        return Err(ApiError::bad_request("Too many databases"));
    }
    if names.iter().any(|name| name.len() > MAX_DB_NAME_BYTES) {
        return Err(ApiError::bad_request("Database name too long"));
    }
    Ok(())
}

/// The manual editor's transaction body: the ownership check, then the
/// replacement. Split out of the handler so tests can drive both.
async fn replace_board_databases(
    conn: &mut sqlx::SqliteConnection,
    ctx: &AssociationContext,
    pinboard_id: i64,
    user: &str,
    names: &[String],
    now: i64,
) -> ApiResult<()> {
    if !pinboards::pinboard_exists(&mut *conn, pinboard_id, user).await? {
        return Err(ApiError::not_found("Pinboard not found"));
    }
    pinboard_dbs::set_board_databases(conn, ctx, pinboard_id, names, now).await
}

#[utoipa::path(
    delete,
    operation_id = "delete_pinboard",
    path = "/api/pinboards/{pinboard_id}",
    tag = "pinboards",
    summary = "Delete a pinboard and its entire version history",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    responses(
        (status = 200, description = "Deleted", body = PinboardDeleteResponse),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn delete_pinboard(
    mut db: DbConnection<UserDataWrite>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
) -> ApiResult<Json<PinboardDeleteResponse>> {
    begin_transaction(&mut db.conn).await?;
    let deleted = match pinboards::delete_pinboard(&mut db.conn, pinboard_id, &query.user).await {
        Ok(deleted) => deleted,
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            return Err(err);
        }
    };
    commit_transaction(&mut db.conn).await?;

    if !deleted {
        return Err(ApiError::not_found("Pinboard not found"));
    }
    Ok(Json(PinboardDeleteResponse {
        message: "Deleted pinboard".to_string(),
        deleted_board: true,
        new_head_version_id: None,
    }))
}

#[utoipa::path(
    get,
    operation_id = "list_pinboard_versions",
    path = "/api/pinboards/{pinboard_id}/versions",
    tag = "pinboards",
    summary = "List all versions of a pinboard",
    description = "Returns every saved version, newest first, layouts included (previews are served separately by the per-version preview endpoint).",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    responses(
        (status = 200, description = "Pinboard versions", body = PinboardVersionsResponse),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn list_pinboard_versions(
    mut db: DbConnection<ReadOnly>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
) -> ApiResult<Json<PinboardVersionsResponse>> {
    if !pinboards::pinboard_exists(&mut db.conn, pinboard_id, &query.user).await? {
        return Err(ApiError::not_found("Pinboard not found"));
    }
    let versions = pinboards::list_versions(&mut db.conn, pinboard_id, &query.user).await?;
    Ok(Json(PinboardVersionsResponse {
        versions: versions.into_iter().map(map_version).collect(),
    }))
}

#[utoipa::path(
    post,
    operation_id = "save_pinboard_version",
    path = "/api/pinboards/{pinboard_id}/versions",
    tag = "pinboards",
    summary = "Save a new version of a pinboard",
    description = "Appends a new version and moves the board's head to it. If the layout is byte-identical to the current head, no version is created and the response has `no_op: true`.\nBoard-level `flags` are stored on the board itself in either case (never creating a version); `flags_updated` reports whether they changed, so a settings-only save is a flag update with `no_op: true`.\nThe version snapshots the board's current name as its name-at-save.",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        PinboardUserQuery
    ),
    request_body(content = SaveVersionRequest),
    responses(
        (status = 200, description = "Saved version", body = SavePinboardResponse),
        (status = 404, description = "Pinboard not found")
    )
)]
pub async fn save_pinboard_version(
    mut db: DbConnection<UserDataWrite>,
    Path(pinboard_id): Path<i64>,
    Query(query): Query<PinboardUserQuery>,
    Json(request): Json<SaveVersionRequest>,
) -> ApiResult<Json<SavePinboardResponse>> {
    let preview = validate_version_request(&request)?;
    let flags = canonical_flags(&request)?;
    // Outside the transaction, for the reason create_pinboard states.
    let identity = StampIdentity::load(&mut db.conn, &db.index_db).await;

    begin_transaction(&mut db.conn).await?;
    let result = save_version(
        &mut db.conn,
        pinboard_id,
        &query.user,
        SaveInputs {
            request: &request,
            preview: &preview,
            flags: flags.as_deref(),
            identity: identity.as_ref(),
        },
        unix_now(),
    )
    .await;

    match result {
        Ok(response) => {
            commit_transaction(&mut db.conn).await?;
            Ok(Json(response))
        }
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            Err(err)
        }
    }
}

/// Everything the save transaction needs besides the board it is saving.
/// Grouped so the body keeps a signature a reader can hold in their head.
struct SaveInputs<'a> {
    request: &'a SaveVersionRequest,
    preview: &'a PreviewUpload,
    flags: Option<&'a str>,
    /// The current database's stamp identity, or `None` when there is none to
    /// write (see `StampIdentity::load`).
    identity: Option<&'a StampIdentity>,
}

/// The save transaction's body, both paths of it: a new version, or the
/// settings-only no-op. Split out of the handler so tests can drive it with a
/// synthetic identity.
async fn save_version(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    user: &str,
    inputs: SaveInputs<'_>,
    now: i64,
) -> ApiResult<SavePinboardResponse> {
    let SaveInputs {
        request,
        preview,
        flags,
        identity,
    } = inputs;
    if !pinboards::pinboard_exists(&mut *conn, pinboard_id, user).await? {
        return Err(ApiError::not_found("Pinboard not found"));
    }

    if let Some((head_version_id, head_layout)) =
        pinboards::get_head_layout(&mut *conn, pinboard_id, user).await?
    {
        let incoming = serde_json::to_string(&request.layout)
            .map_err(|_| ApiError::bad_request("Invalid layout"))?;
        if incoming == head_layout {
            // Settings-only save: the layout no-ops but the board's flags
            // still advance to what the client sent.
            let flags_updated = match flags {
                Some(flags) => pinboards::set_flags(&mut *conn, pinboard_id, user, flags).await?,
                None => false,
            };
            // A save is a deliberate act even when the layout no-ops, so it
            // still counts as activity (the frecency half of it is debounced,
            // so an editing session is one visit) — and, for the same reason,
            // it still stamps. The head is unchanged here, so the overlap the
            // stamp test reads is that unchanged head's.
            pinboards::touch_saved(&mut *conn, pinboard_id, user, now).await?;
            stamp_if_present_here(conn, pinboard_id, head_version_id, identity, now).await?;
            return Ok(SavePinboardResponse {
                pinboard_id,
                version_id: head_version_id,
                no_op: true,
                flags_updated,
            });
        }
    }

    let version_id = pinboards::append_version(
        &mut *conn,
        pinboard_id,
        &request.layout,
        &request.items,
        preview.bytes.as_deref(),
        preview.width,
        preview.height,
        preview.screenful_h,
    )
    .await?;
    let flags_updated = match flags {
        Some(flags) => pinboards::set_flags(&mut *conn, pinboard_id, user, flags).await?,
        None => false,
    };
    pinboards::touch_saved(&mut *conn, pinboard_id, user, now).await?;
    stamp_if_present_here(conn, pinboard_id, version_id, identity, now).await?;
    Ok(SavePinboardResponse {
        pinboard_id,
        version_id,
        no_op: false,
        flags_updated,
    })
}

/// The save path's stamp: the current database is recorded only if some item
/// of the head version actually exists in it.
///
/// Zero overlap means the save happened under a mistakenly-selected database,
/// and recording that would hand the board to a database it has nothing to do
/// with. (Partial overlap is deliberately enough — it is a mistake guard, not
/// a membership test, which principle 2 says partial overlap can never be.)
///
/// `version_id` must be the head as it stands *after* this save, so the count
/// measures the board being left behind rather than the one being replaced.
async fn stamp_if_present_here(
    conn: &mut sqlx::SqliteConnection,
    pinboard_id: i64,
    version_id: i64,
    identity: Option<&StampIdentity>,
    now: i64,
) -> ApiResult<()> {
    if identity.is_none() {
        return Ok(());
    }
    if pinboards::version_present_count(&mut *conn, version_id).await? == 0 {
        return Ok(());
    }
    pinboard_dbs::stamp_current_db(conn, pinboard_id, identity, now).await
}

#[utoipa::path(
    delete,
    operation_id = "delete_pinboard_version",
    path = "/api/pinboards/{pinboard_id}/versions/{version_id}",
    tag = "pinboards",
    summary = "Delete one version of a pinboard",
    description = "Deletes a single version. Deleting the head moves the head to the newest remaining version; deleting the last remaining version deletes the board itself (`deleted_board: true`).",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        ("version_id" = i64, Path, description = "The version id"),
        PinboardUserQuery
    ),
    responses(
        (status = 200, description = "Delete outcome", body = PinboardDeleteResponse),
        (status = 404, description = "Version not found")
    )
)]
pub async fn delete_pinboard_version(
    mut db: DbConnection<UserDataWrite>,
    Path((pinboard_id, version_id)): Path<(i64, i64)>,
    Query(query): Query<PinboardUserQuery>,
) -> ApiResult<Json<PinboardDeleteResponse>> {
    begin_transaction(&mut db.conn).await?;
    let outcome =
        match pinboards::delete_version(&mut db.conn, pinboard_id, version_id, &query.user).await {
            Ok(outcome) => outcome,
            Err(err) => {
                let _ = rollback_transaction(&mut db.conn).await;
                return Err(err);
            }
        };
    commit_transaction(&mut db.conn).await?;

    match outcome {
        pinboards::DeleteVersionOutcome::NotFound => Err(ApiError::not_found("Version not found")),
        pinboards::DeleteVersionOutcome::Deleted {
            new_head_version_id,
        } => Ok(Json(PinboardDeleteResponse {
            message: "Deleted version".to_string(),
            deleted_board: false,
            new_head_version_id: Some(new_head_version_id),
        })),
        pinboards::DeleteVersionOutcome::DeletedBoard => Ok(Json(PinboardDeleteResponse {
            message: "Deleted last version; pinboard removed".to_string(),
            deleted_board: true,
            new_head_version_id: None,
        })),
    }
}

#[utoipa::path(
    get,
    operation_id = "pinboard_version_preview",
    path = "/api/pinboards/{pinboard_id}/versions/{version_id}/preview",
    tag = "pinboards",
    summary = "Get the stored preview image for a pinboard version",
    description = "Serves the client-composited preview for one version. Responses carry immutable cache headers.\nWith `maxw`, the image is downscaled on the fly (JPEG) to at most that width — unless the stored image is already no wider than `maxw`, in which case it is served as uploaded, exactly as it is without `maxw`. Asking for the full master therefore costs no second lossy pass.",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        ("version_id" = i64, Path, description = "The version id"),
        PinboardPreviewQuery
    ),
    responses(
        (status = 200, description = "Preview image"),
        (status = 404, description = "No preview stored for this version")
    )
)]
pub async fn pinboard_version_preview(
    mut db: DbConnection<ReadOnly>,
    Path((pinboard_id, version_id)): Path<(i64, i64)>,
    Query(query): Query<PinboardPreviewQuery>,
) -> ApiResult<Response<Body>> {
    let Some(preview) =
        pinboards::get_version_preview(&mut db.conn, pinboard_id, version_id, &query.user).await?
    else {
        return Err(ApiError::not_found("Preview not found"));
    };

    let (bytes, media_type) = match query.maxw {
        // A `maxw` at or above the stored width asks for nothing the stored
        // image doesn't already have, so it takes the same path as no `maxw`
        // at all: the bytes as uploaded. Going through downscale_preview
        // would decode and re-encode them as JPEG q85 — a second lossy pass
        // over an already-lossy composite, for zero pixels gained. The
        // recorded preview_w answers this without touching the image; rows
        // that predate it (or that stored no width) still decode to find out.
        Some(maxw) if width_at_least(preview.width, maxw) => {
            let media_type = sniff_image_media_type(&preview.bytes);
            (preview.bytes, media_type)
        }
        Some(maxw) => {
            let maxw = maxw.clamp(16, 4096);
            downscale_preview(preview.bytes, maxw).await?
        }
        None => {
            let media_type = sniff_image_media_type(&preview.bytes);
            (preview.bytes, media_type)
        }
    };

    image_response(bytes, media_type)
}

/// Whether a request for `maxw` pixels is already satisfied by an image of
/// the recorded `stored` width. Applies the same lower clamp the downscale
/// path does, so a tiny `maxw` behaves identically on both branches.
fn width_at_least(stored: Option<i64>, maxw: u32) -> bool {
    let maxw = maxw.clamp(16, 4096);
    matches!(stored, Some(width) if width > 0 && i64::from(maxw) >= width)
}

#[utoipa::path(
    put,
    operation_id = "update_pinboard_version_preview",
    path = "/api/pinboards/{pinboard_id}/versions/{version_id}/preview",
    tag = "pinboards",
    summary = "Replace the stored preview image of a pinboard version",
    description = "Overwrites one version's preview image and its recorded dimensions, leaving the layout, items and name-at-save untouched. The compositor is client-side, so this is how a board saved at an older preview resolution gets a better picture without minting a version: recomposite the head version's layout and PUT the result.\nThe board's `time_updated` is deliberately not bumped — re-rendering the picture of a version is not a content change, so it must not reorder the library.\nCaveat: version previews are served with immutable cache headers (versions were immutable until this endpoint existed), so after a refresh, already-cached sizes persist in browsers and proxies until a hard refresh. Accepted as-is: this is a one-time local operation, not a cache-busting mechanism.",
    params(
        DbQueryParams,
        ("pinboard_id" = i64, Path, description = "The pinboard id"),
        ("version_id" = i64, Path, description = "The version id"),
        PinboardUserQuery
    ),
    request_body(content = UpdatePreviewRequest),
    responses(
        (status = 200, description = "Preview replaced", body = PinboardDeleteResponse),
        (status = 404, description = "Version not found")
    )
)]
pub async fn update_pinboard_version_preview(
    mut db: DbConnection<UserDataWrite>,
    Path((pinboard_id, version_id)): Path<(i64, i64)>,
    Query(query): Query<PinboardUserQuery>,
    Json(request): Json<UpdatePreviewRequest>,
) -> ApiResult<Json<PinboardDeleteResponse>> {
    let preview = validate_preview_upload(
        Some(request.preview_b64.as_str()),
        request.preview_w,
        request.preview_h,
        request.screenful_h,
    )?;
    let Some(bytes) = preview.bytes else {
        return Err(ApiError::bad_request("Preview image required"));
    };
    let (preview_w, preview_h, screenful_h) = (preview.width, preview.height, preview.screenful_h);

    begin_transaction(&mut db.conn).await?;
    let updated = match pinboards::update_version_preview(
        &mut db.conn,
        pinboard_id,
        version_id,
        &query.user,
        &bytes,
        preview_w,
        preview_h,
        screenful_h,
    )
    .await
    {
        Ok(updated) => updated,
        Err(err) => {
            let _ = rollback_transaction(&mut db.conn).await;
            return Err(err);
        }
    };
    commit_transaction(&mut db.conn).await?;

    if !updated {
        return Err(ApiError::not_found("Version not found"));
    }
    Ok(Json(PinboardDeleteResponse {
        message: "Replaced version preview".to_string(),
        deleted_board: false,
        new_head_version_id: None,
    }))
}

/// Downscales a stored preview to `maxw` pixels wide on a blocking thread.
/// Returns the original bytes when they are already narrow enough.
async fn downscale_preview(bytes: Vec<u8>, maxw: u32) -> ApiResult<(Vec<u8>, &'static str)> {
    tokio::task::spawn_blocking(move || {
        let img = image::load_from_memory(&bytes).map_err(|err| {
            tracing::error!(error = %err, "failed to decode stored pinboard preview");
            ApiError::internal("Failed to decode preview")
        })?;
        if img.width() <= maxw {
            let media_type = sniff_image_media_type(&bytes);
            return Ok((bytes, media_type));
        }
        let height =
            ((u64::from(maxw) * u64::from(img.height())) / u64::from(img.width())).max(1) as u32;
        let scaled = img.resize_exact(maxw, height, image::imageops::FilterType::Lanczos3);

        let mut out = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut out);
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut cursor, 85);
        scaled
            .into_rgb8()
            .write_with_encoder(encoder)
            .map_err(|err| {
                tracing::error!(error = %err, "failed to encode downscaled pinboard preview");
                ApiError::internal("Failed to encode preview")
            })?;
        Ok((out, "image/jpeg"))
    })
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "preview downscale task panicked");
        ApiError::internal("Failed to process preview")
    })?
}

fn sniff_image_media_type(bytes: &[u8]) -> &'static str {
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        "image/webp"
    } else if bytes.starts_with(&[0x89, b'P', b'N', b'G']) {
        "image/png"
    } else if bytes.starts_with(&[0xFF, 0xD8]) {
        "image/jpeg"
    } else {
        "application/octet-stream"
    }
}

fn image_response(bytes: Vec<u8>, media_type: &str) -> ApiResult<Response<Body>> {
    let len = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    let headers = response.headers_mut();
    if let Ok(value) = header::HeaderValue::from_str(media_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&len.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    // Version previews are immutable: a version's preview can never change
    // after it is saved, so clients may cache each size forever.
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    Ok(response)
}

/// IMMEDIATE, not deferred, is a correctness requirement: these transactions
/// read user_data before their first write, and the background activity
/// writer (`spawn_activity_write`) commits to the same database at any time.
/// Under a deferred BEGIN the read pins a WAL snapshot, and the first write
/// after a concurrent commit fails `SQLITE_BUSY_SNAPSHOT` — which does NOT
/// invoke the busy handler, so `busy_timeout` never applies and the user's
/// save 500s. Taking the write lock up front routes the contention through
/// the busy handler instead, with the telemetry write as the loser.
async fn begin_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("BEGIN IMMEDIATE")
        .execute(conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to start transaction");
            ApiError::internal("Failed to start transaction")
        })?;
    Ok(())
}

async fn commit_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("COMMIT").execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to commit transaction");
        ApiError::internal("Failed to commit transaction")
    })?;
    Ok(())
}

async fn rollback_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("ROLLBACK").execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to rollback transaction");
        ApiError::internal("Failed to rollback transaction")
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    fn layout(records: &[&str]) -> Vec<String> {
        records.iter().map(|record| record.to_string()).collect()
    }

    fn save_request(records: &[&str], items: &[&str]) -> SaveVersionRequest {
        SaveVersionRequest {
            layout: layout(records),
            items: items.iter().map(|item| item.to_string()).collect(),
            preview_b64: None,
            preview_w: None,
            preview_h: None,
            screenful_h: None,
            flags: None,
        }
    }

    /// A fixed clock for tests that don't care about activity timestamps.
    const T0: i64 = 1_800_000_000;

    async fn create_board(
        conn: &mut sqlx::SqliteConnection,
        name: Option<&str>,
        records: &[&str],
        items: &[&str],
    ) -> (i64, i64) {
        let pinboard_id = pinboards::create_pinboard(conn, "user", name, None, T0)
            .await
            .unwrap();
        let request = save_request(records, items);
        let version_id = pinboards::append_version(
            conn,
            pinboard_id,
            &request.layout,
            &request.items,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        (pinboard_id, version_id)
    }

    // Ensures creating a board sets its head and the list reflects counts.
    #[tokio::test]
    async fn create_and_list_pinboards() {
        let mut dbs = setup_test_databases().await;
        let (board_a, head_a) = create_board(
            &mut dbs.index_conn,
            Some("poses"),
            &[
                "v2", "aaaa", "0", "0", "10", "10", "bbbb", "10", "0", "10", "10",
            ],
            &["a1", "b2"],
        )
        .await;
        create_board(
            &mut dbs.index_conn,
            None,
            &["v2", "cccc", "0", "0", "5", "5"],
            &["c3"],
        )
        .await;

        let boards = pinboards::list_pinboards(
            &mut dbs.index_conn,
            "user",
            None,
            PinboardOrder::Activity,
            T0,
        )
        .await
        .unwrap();
        assert_eq!(boards.len(), 2);
        let board = boards.iter().find(|board| board.id == board_a).unwrap();
        assert_eq!(board.name.as_deref(), Some("poses"));
        assert_eq!(board.head_version_id, Some(head_a));
        assert_eq!(board.item_count, 2);
        assert_eq!(board.version_count, 1);
    }

    /// Indexes an item in the current database — what `present_count` counts.
    async fn index_item(conn: &mut sqlx::SqliteConnection, sha256: &str) {
        sqlx::query(
            r#"
            INSERT INTO main.items (sha256, md5, type, time_added)
            VALUES (?, ?, 'image/png', '2026-01-01T00:00:00')
            "#,
        )
        .bind(sha256)
        .bind(sha256)
        .execute(conn)
        .await
        .unwrap();
    }

    fn list_query(associated_only: bool) -> PinboardListQuery {
        PinboardListQuery {
            user: DEFAULT_USER.to_string(),
            q: None,
            order: PinboardOrder::Updated,
            associated_only,
        }
    }

    /// The database being viewed: identity `uuid_here`, named `default`, and
    /// the only one that exists locally.
    fn current_db() -> AssociationContext {
        AssociationContext::for_tests(
            Some("uuid_here"),
            Some("inst"),
            "default",
            crate::db::local_dbs::LocalDbIdentities::for_tests(&[("default", "uuid_here")], false),
        )
    }

    fn ids(boards: &[PinboardSummaryResponse]) -> Vec<i64> {
        let mut ids: Vec<i64> = boards.iter().map(|board| board.id).collect();
        ids.sort_unstable();
        ids
    }

    // Ensures the list reports the association of every board and that
    // `associated_only` filters AFTER the verdicts are computed: a board
    // present here in full is admitted by overlap alone, a rotted one is
    // hidden until something stamps it, and present_count is reported either
    // way — it is what tells rot apart from a board from another database.
    #[tokio::test]
    async fn list_reports_and_filters_associations() {
        let mut dbs = setup_test_databases().await;
        index_item(&mut dbs.index_conn, "a1").await;
        let (present, _) =
            create_board(&mut dbs.index_conn, Some("here"), &["v2", "a"], &["a1"]).await;
        let (rotted, _) = create_board(
            &mut dbs.index_conn,
            Some("elsewhere"),
            &["v2", "b"],
            &["gone"],
        )
        .await;
        let ctx = current_db();

        let all = list_pinboard_summaries(&mut dbs.index_conn, &ctx, &list_query(false), T0)
            .await
            .unwrap();
        assert_eq!(ids(&all), vec![present, rotted]);
        let board = |boards: &[PinboardSummaryResponse], id: i64| {
            boards
                .iter()
                .find(|board| board.id == id)
                .map(|board| (board.present_count, board.item_count, board.associated))
                .unwrap()
        };
        assert_eq!(board(&all, present), (1, 1, true));
        assert_eq!(board(&all, rotted), (0, 1, false));
        assert!(all.iter().all(|board| board.databases.is_empty()));

        let filtered = list_pinboard_summaries(&mut dbs.index_conn, &ctx, &list_query(true), T0)
            .await
            .unwrap();
        assert_eq!(ids(&filtered), vec![present]);
        assert_eq!(filtered[0].present_count, 1);

        // Clause (a): a stamp carrying this database's identity brings the
        // rotted board back, whatever name the stamp was written under.
        crate::db::pinboard_dbs::stamp_for_tests(
            &mut dbs.index_conn,
            rotted,
            "uuid_here",
            "named-back-then",
            "inst",
            T0,
        )
        .await;
        let filtered = list_pinboard_summaries(&mut dbs.index_conn, &ctx, &list_query(true), T0)
            .await
            .unwrap();
        assert_eq!(ids(&filtered), vec![present, rotted]);
        let stamped = filtered.iter().find(|board| board.id == rotted).unwrap();
        assert!(stamped.associated);
        assert_eq!(stamped.databases.len(), 1);
        assert_eq!(stamped.databases[0].name, "named-back-then");
        assert_eq!(stamped.databases[0].last_stamped, T0);
        assert!(stamped.databases[0].associated);
    }

    // Ensures the detail response carries the same fields as the list row.
    #[tokio::test]
    async fn detail_carries_the_association_fields() {
        let mut dbs = setup_test_databases().await;
        index_item(&mut dbs.index_conn, "a1").await;
        let (pinboard_id, _) =
            create_board(&mut dbs.index_conn, None, &["v2", "a"], &["a1", "gone"]).await;
        crate::db::pinboard_dbs::stamp_for_tests(
            &mut dbs.index_conn,
            pinboard_id,
            "uuid_here",
            "default",
            "inst",
            T0,
        )
        .await;

        let ctx = current_db();
        let (summary, head) = pinboards::get_pinboard(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        let association = board_association(&mut dbs.index_conn, &ctx, &summary)
            .await
            .unwrap();
        let detail = map_detail(summary, head, association);

        // Half the board is missing here, so only the stamp associates it.
        assert_eq!(detail.present_count, 1);
        assert_eq!(detail.head.as_ref().unwrap().item_count, 2);
        assert!(detail.associated);
        assert_eq!(detail.databases.len(), 1);
        assert_eq!(detail.databases[0].name, "default");
        assert!(detail.databases[0].associated);
    }

    // Ensures FTS name search matches by prefix and ignores other boards.
    #[tokio::test]
    async fn list_pinboards_fts_name_search() {
        let mut dbs = setup_test_databases().await;
        create_board(&mut dbs.index_conn, Some("poses standing"), &["v2"], &[]).await;
        create_board(&mut dbs.index_conn, Some("landscapes"), &["v2"], &[]).await;
        create_board(&mut dbs.index_conn, None, &["v2"], &[]).await;

        let hits = pinboards::list_pinboards(
            &mut dbs.index_conn,
            "user",
            Some("pos"),
            PinboardOrder::Activity,
            T0,
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name.as_deref(), Some("poses standing"));
    }

    // Ensures appending a version moves the head and snapshots name_at_save.
    #[tokio::test]
    async fn append_version_moves_head_and_snapshots_name() {
        let mut dbs = setup_test_databases().await;
        let (board, first) =
            create_board(&mut dbs.index_conn, Some("original"), &["v2", "a"], &[]).await;

        pinboards::rename_pinboard(&mut dbs.index_conn, board, "user", Some("renamed"), false)
            .await
            .unwrap();
        let second = pinboards::append_version(
            &mut dbs.index_conn,
            board,
            &layout(&["v2", "b"]),
            &[],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let versions = pinboards::list_versions(&mut dbs.index_conn, board, "user")
            .await
            .unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].id, second);
        assert_eq!(versions[0].name_at_save.as_deref(), Some("renamed"));
        assert_eq!(versions[1].id, first);
        assert_eq!(versions[1].name_at_save.as_deref(), Some("original"));

        let (_, head) = pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.unwrap().id, second);
    }

    // Ensures relabel_head rewrites only the head version's snapshot.
    #[tokio::test]
    async fn rename_with_relabel_head_updates_snapshot() {
        let mut dbs = setup_test_databases().await;
        let (board, _) = create_board(&mut dbs.index_conn, Some("old"), &["v2", "a"], &[]).await;
        pinboards::append_version(
            &mut dbs.index_conn,
            board,
            &layout(&["v2", "b"]),
            &[],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        pinboards::rename_pinboard(&mut dbs.index_conn, board, "user", Some("new"), true)
            .await
            .unwrap();

        let versions = pinboards::list_versions(&mut dbs.index_conn, board, "user")
            .await
            .unwrap();
        assert_eq!(versions[0].name_at_save.as_deref(), Some("new"));
        assert_eq!(versions[1].name_at_save.as_deref(), Some("old"));
    }

    // Ensures deleting the head shifts it to the newest remaining version.
    #[tokio::test]
    async fn delete_head_version_shifts_head() {
        let mut dbs = setup_test_databases().await;
        let (board, first) = create_board(&mut dbs.index_conn, None, &["v2", "a"], &["a1"]).await;
        let second = pinboards::append_version(
            &mut dbs.index_conn,
            board,
            &layout(&["v2", "b"]),
            &["b2".to_string()],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let outcome = pinboards::delete_version(&mut dbs.index_conn, board, second, "user")
            .await
            .unwrap();
        match outcome {
            pinboards::DeleteVersionOutcome::Deleted {
                new_head_version_id,
            } => assert_eq!(new_head_version_id, first),
            _ => panic!("expected Deleted outcome"),
        }

        // The search index follows the head automatically.
        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.head_version_id, Some(first));
        assert_eq!(summary.item_count, 1);
    }

    // Ensures deleting the last version removes the board entirely.
    #[tokio::test]
    async fn delete_last_version_deletes_board() {
        let mut dbs = setup_test_databases().await;
        let (board, only) = create_board(&mut dbs.index_conn, None, &["v2", "a"], &[]).await;

        let outcome = pinboards::delete_version(&mut dbs.index_conn, board, only, "user")
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            pinboards::DeleteVersionOutcome::DeletedBoard
        ));
        assert!(
            pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
                .await
                .unwrap()
                .is_none()
        );
    }

    // Ensures deleting a board removes its versions and membership rows.
    #[tokio::test]
    async fn delete_pinboard_removes_history() {
        let mut dbs = setup_test_databases().await;
        let (board, _) = create_board(&mut dbs.index_conn, None, &["v2", "a"], &["a1"]).await;

        let deleted = pinboards::delete_pinboard(&mut dbs.index_conn, board, "user")
            .await
            .unwrap();
        assert!(deleted);

        let versions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM user_data.pinboard_versions WHERE pinboard_id = ?",
        )
        .bind(board)
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        assert_eq!(versions, 0);
        let items: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM user_data.pinboard_version_items")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(items, 0);
    }

    // Ensures a deleted board takes its database associations with it. Left
    // behind they would not stay harmless litter: `pinboards.id` is a plain
    // INTEGER PRIMARY KEY (no AUTOINCREMENT), so SQLite hands the highest
    // deleted id to the very next board — which would then inherit a dead
    // board's stamps and count as associated on the strength of them.
    #[tokio::test]
    async fn deleting_a_board_takes_its_database_stamps() {
        let mut dbs = setup_test_databases().await;
        let (kept, _) = create_board(&mut dbs.index_conn, Some("kept"), &["v2", "a"], &[]).await;
        let (doomed, _) =
            create_board(&mut dbs.index_conn, Some("doomed"), &["v2", "b"], &[]).await;
        for (board, name) in [(kept, "archive"), (doomed, "default")] {
            crate::db::pinboard_dbs::stamp_for_tests(
                &mut dbs.index_conn,
                board,
                "uuid_here",
                name,
                "inst",
                T0,
            )
            .await;
        }

        assert!(
            pinboards::delete_pinboard(&mut dbs.index_conn, doomed, "user")
                .await
                .unwrap()
        );
        let stamps: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM user_data.pinboard_databases WHERE pinboard_id = ?",
        )
        .bind(doomed)
        .fetch_one(&mut dbs.index_conn)
        .await
        .unwrap();
        assert_eq!(stamps, 0);

        // The next board really does get the dead one's id — that is the
        // whole reason the delete above has to be explicit.
        let (recycled, _) =
            create_board(&mut dbs.index_conn, Some("new"), &["v2", "c"], &["gone"]).await;
        assert_eq!(recycled, doomed, "SQLite recycles the highest deleted id");

        let ctx = current_db();
        let association = association_of(&mut dbs.index_conn, &ctx, recycled).await;
        assert!(
            association.databases.is_empty(),
            "a recycled id must not inherit the dead board's stamps"
        );
        assert!(!association.associated);

        // ...and the board that was not deleted keeps its own.
        let association = association_of(&mut dbs.index_conn, &ctx, kept).await;
        assert_eq!(association.databases.len(), 1);
        assert_eq!(association.databases[0].name, "archive");
    }

    /// The board's stamp rows exactly as stored, by name.
    async fn stamps(
        conn: &mut sqlx::SqliteConnection,
        pinboard_id: i64,
    ) -> Vec<(String, String, String, i64)> {
        use sqlx::Row as _;
        sqlx::query(
            r#"
            SELECT db_uuid, db_name, instance_uuid, last_stamped
            FROM user_data.pinboard_databases
            WHERE pinboard_id = ?
            ORDER BY db_name
            "#,
        )
        .bind(pinboard_id)
        .fetch_all(conn)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>("db_uuid"),
                row.get::<String, _>("db_name"),
                row.get::<String, _>("instance_uuid"),
                row.get::<i64, _>("last_stamped"),
            )
        })
        .collect()
    }

    /// A stamp row as the tests spell one out.
    fn stamp_row(
        db_uuid: &str,
        db_name: &str,
        instance_uuid: &str,
        last_stamped: i64,
    ) -> (String, String, String, i64) {
        (
            db_uuid.to_string(),
            db_name.to_string(),
            instance_uuid.to_string(),
            last_stamped,
        )
    }

    async fn time_updated_of(conn: &mut sqlx::SqliteConnection, pinboard_id: i64) -> String {
        pinboards::get_pinboard(conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap()
            .0
            .time_updated
    }

    /// Pins `time_updated` to a value the clock cannot produce, and returns
    /// it. Comparing the column against whatever it happened to hold would
    /// pass by accident whenever a bump landed inside the same millisecond;
    /// against a sentinel, "unchanged" is an assertion about the code.
    async fn pin_time_updated(conn: &mut sqlx::SqliteConnection, pinboard_id: i64) -> String {
        const SENTINEL: &str = "1999-12-31T23:59:59.999";
        sqlx::query("UPDATE user_data.pinboards SET time_updated = ? WHERE id = ?")
            .bind(SENTINEL)
            .bind(pinboard_id)
            .execute(conn)
            .await
            .unwrap();
        SENTINEL.to_string()
    }

    /// The identity of the database `current_db()` describes — what a save or
    /// a create running against it stamps.
    fn current_identity() -> StampIdentity {
        StampIdentity::for_tests("uuid_here", "default", "inst")
    }

    /// Creates a board through the create handler's transaction body, so the
    /// stamp under test is the one the endpoint writes.
    async fn create_through_handler(
        conn: &mut sqlx::SqliteConnection,
        records: &[&str],
        items: &[&str],
        identity: Option<&StampIdentity>,
        now: i64,
    ) -> i64 {
        let request = CreatePinboardRequest {
            name: None,
            version: save_request(records, items),
        };
        let preview = validate_version_request(&request.version).unwrap();
        create_board_with_version(conn, "user", &request, &preview, None, identity, now)
            .await
            .unwrap()
            .0
    }

    /// Saves through the save handler's transaction body, same reason.
    async fn save_through_handler(
        conn: &mut sqlx::SqliteConnection,
        pinboard_id: i64,
        records: &[&str],
        items: &[&str],
        identity: Option<&StampIdentity>,
        now: i64,
    ) -> SavePinboardResponse {
        let request = save_request(records, items);
        let preview = validate_version_request(&request).unwrap();
        save_version(
            conn,
            pinboard_id,
            "user",
            SaveInputs {
                request: &request,
                preview: &preview,
                flags: None,
                identity,
            },
            now,
        )
        .await
        .unwrap()
    }

    // Ensures a board is stamped for the database it was made in — the
    // strongest signal there is, so it is recorded unconditionally: this board
    // has not one item indexed here and is stamped anyway.
    #[tokio::test]
    async fn create_stamps_the_database_it_was_made_in() {
        let mut dbs = setup_test_databases().await;
        let identity = current_identity();
        let board = create_through_handler(
            &mut dbs.index_conn,
            &["v2", "a"],
            &["beef"],
            Some(&identity),
            T0,
        )
        .await;

        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0)]
        );
        // ...and it is associated on the strength of that alone, with no
        // overlap whatsoever.
        let ctx = current_db();
        assert!(
            association_of(&mut dbs.index_conn, &ctx, board)
                .await
                .associated
        );
    }

    // Ensures a deployment with no identity to sign a stamp with writes NO
    // ROW rather than a sentinel one: an empty instance_uuid would compare
    // equal across every identity-less instance, so the name fallback would
    // match any same-named database anywhere. Creation itself must still work.
    #[tokio::test]
    async fn create_without_an_identity_writes_no_row_at_all() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["a1"], None, T0).await;

        assert!(stamps(&mut dbs.index_conn, board).await.is_empty());
        let (summary, head) = pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
            .await
            .unwrap()
            .unwrap();
        assert!(head.is_some(), "the board itself must still have been made");
        assert_eq!(summary.version_count, 1);
    }

    // Ensures a save records the current database only when something of the
    // board is actually here: a save under a mistakenly-selected database
    // (zero overlap) must not hand it that board, while any overlap at all is
    // enough — this is a mistake guard, not a membership test.
    #[tokio::test]
    async fn save_stamps_only_when_something_of_the_board_is_here() {
        let mut dbs = setup_test_databases().await;
        index_item(&mut dbs.index_conn, "a1").await;
        let identity = current_identity();
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;

        // Nothing of this version exists here: no stamp.
        let saved = save_through_handler(
            &mut dbs.index_conn,
            board,
            &["v2", "b"],
            &["beef", "dead"],
            Some(&identity),
            T0 + 10,
        )
        .await;
        assert!(!saved.no_op);
        assert!(stamps(&mut dbs.index_conn, board).await.is_empty());

        // The overlap that decides is the INCOMING version's, not the one the
        // save is replacing: this payload is the first with an item here.
        save_through_handler(
            &mut dbs.index_conn,
            board,
            &["v2", "c"],
            &["a1", "beef"],
            Some(&identity),
            T0 + 20,
        )
        .await;
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0 + 20)]
        );

        // A later save refreshes the row rather than adding a second.
        save_through_handler(
            &mut dbs.index_conn,
            board,
            &["v2", "d"],
            &["a1"],
            Some(&identity),
            T0 + 30,
        )
        .await;
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0 + 30)]
        );

        // And with no identity to sign with, a save writes nothing either.
        let other =
            create_through_handler(&mut dbs.index_conn, &["v2", "e"], &["a1"], None, T0).await;
        save_through_handler(
            &mut dbs.index_conn,
            other,
            &["v2", "f"],
            &["a1"],
            None,
            T0 + 40,
        )
        .await;
        assert!(stamps(&mut dbs.index_conn, other).await.is_empty());
    }

    // Ensures a settings-only save stamps too (a save is a deliberate act,
    // same reasoning as touch_saved) — reading the overlap of the unchanged
    // head, since there is no new version — and that stamping does not bump
    // time_updated: an association is not a content change and must not
    // reorder the library.
    #[tokio::test]
    async fn a_settings_only_save_stamps_without_reordering_the_library() {
        let mut dbs = setup_test_databases().await;
        index_item(&mut dbs.index_conn, "a1").await;
        let identity = current_identity();
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["a1"], None, T0).await;
        let time_updated = pin_time_updated(&mut dbs.index_conn, board).await;

        let saved = save_through_handler(
            &mut dbs.index_conn,
            board,
            &["v2", "a"],
            &["a1"],
            Some(&identity),
            T0 + 10,
        )
        .await;
        assert!(saved.no_op, "an identical layout must not mint a version");
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0 + 10)]
        );
        assert_eq!(
            time_updated_of(&mut dbs.index_conn, board).await,
            time_updated,
            "a stamp must not reorder the library"
        );

        // The head's own overlap is what decides here: a board with nothing
        // present gets no stamp from a settings-only save either.
        let rotted =
            create_through_handler(&mut dbs.index_conn, &["v2", "b"], &["beef"], None, T0).await;
        let saved = save_through_handler(
            &mut dbs.index_conn,
            rotted,
            &["v2", "b"],
            &["beef"],
            Some(&identity),
            T0 + 10,
        )
        .await;
        assert!(saved.no_op);
        assert!(stamps(&mut dbs.index_conn, rotted).await.is_empty());
    }

    // Ensures opening a board never associates it. Opening a FOREIGN board is
    // exactly how a wrongly-selected database would collect other databases'
    // boards, so the open path (the detail read plus the activity write it
    // spawns) must leave the stamps alone — both by not adding one and by not
    // refreshing one that is there.
    #[tokio::test]
    async fn opening_a_board_never_stamps_it() {
        let mut dbs = setup_test_databases().await;
        index_item(&mut dbs.index_conn, "a1").await;
        let identity = current_identity();
        let stamped = create_through_handler(
            &mut dbs.index_conn,
            &["v2", "a"],
            &["a1"],
            Some(&identity),
            T0,
        )
        .await;
        let unstamped =
            create_through_handler(&mut dbs.index_conn, &["v2", "b"], &["a1"], None, T0).await;

        let now = T0 + 3 * 60 * 60;
        for board in [stamped, unstamped] {
            let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
                .await
                .unwrap()
                .unwrap();
            assert!(pinboards::activity_due(summary.frecency_at, now));
            pinboards::record_open(
                &mut dbs.index_conn,
                board,
                "user",
                now,
                summary.frecency,
                summary.frecency_at,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            stamps(&mut dbs.index_conn, stamped).await,
            vec![stamp_row("uuid_here", "default", "inst", T0)],
            "an open must not even refresh an existing stamp"
        );
        assert!(stamps(&mut dbs.index_conn, unstamped).await.is_empty());
        // The open itself was recorded, so this is not a test of nothing.
        assert_eq!(
            pinboards::get_pinboard(&mut dbs.index_conn, stamped, "user")
                .await
                .unwrap()
                .unwrap()
                .0
                .last_seen,
            Some(now)
        );
    }

    /// The association of one board, as the detail endpoint computes it.
    async fn association_of(
        conn: &mut sqlx::SqliteConnection,
        ctx: &AssociationContext,
        pinboard_id: i64,
    ) -> PinboardAssociation {
        let (summary, _) = pinboards::get_pinboard(conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        board_association(conn, ctx, &summary).await.unwrap()
    }

    /// The editor's payload.
    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// Drives the manual editor's transaction body — the ownership check and
    /// the replacement, exactly as the endpoint runs them.
    async fn set_databases(
        conn: &mut sqlx::SqliteConnection,
        ctx: &AssociationContext,
        pinboard_id: i64,
        user: &str,
        requested: &[&str],
        now: i64,
    ) -> ApiResult<()> {
        replace_board_databases(conn, ctx, pinboard_id, user, &names(requested), now).await
    }

    // Ensures the editor's payload is bounded before it reaches the write
    // transaction: every name costs a resolution and a row write under the
    // user_data write lock, and neither limit is reachable by the shipped
    // client (the checklist is the local databases plus the board's stamps).
    #[test]
    fn database_name_lists_are_bounded() {
        let list = |count: usize| -> Vec<String> {
            (0..count).map(|index| format!("db{index}")).collect()
        };
        assert!(validate_database_names(&[]).is_ok());
        assert!(validate_database_names(&list(MAX_BOARD_DATABASES)).is_ok());
        assert!(validate_database_names(&list(MAX_BOARD_DATABASES + 1)).is_err());

        assert!(validate_database_names(&["x".repeat(MAX_DB_NAME_BYTES)]).is_ok());
        assert!(validate_database_names(&["x".repeat(MAX_DB_NAME_BYTES + 1)]).is_err());
    }

    fn status_of(err: ApiError) -> axum::http::StatusCode {
        use axum::response::IntoResponse as _;
        err.into_response().status()
    }

    // The editor's core contract. A stamped name whose database is GONE
    // cannot be re-derived by the server, so keeping it has to mean carrying
    // the stored row through verbatim — identity, instance and last_stamped
    // alike, the last being the honest record of when the board really was
    // stamped for it. A genuinely new name resolves to a local database, and
    // removal is expressed by omission (including removing the unresolvable
    // one, which is the whole point of the editor existing).
    #[tokio::test]
    async fn the_editor_carries_stamped_names_through_and_resolves_new_ones() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;
        crate::db::pinboard_dbs::stamp_for_tests(
            &mut dbs.index_conn,
            board,
            "uuid_retired",
            "retired",
            "inst",
            T0,
        )
        .await;
        let ctx = current_db();

        set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["retired", "default"],
            T0 + 100,
        )
        .await
        .unwrap();
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![
                stamp_row("uuid_here", "default", "inst", T0 + 100),
                stamp_row("uuid_retired", "retired", "inst", T0),
            ],
            "the unresolvable row keeps its own stamp time; the new one gets now"
        );

        // Dropping the unresolvable name is simply leaving it out.
        set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["default"],
            T0 + 200,
        )
        .await
        .unwrap();
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0 + 100)],
            "a carried-through row is not re-stamped either"
        );

        // An empty list is a valid instruction: clear everything.
        set_databases(&mut dbs.index_conn, &ctx, board, "user", &[], T0 + 300)
            .await
            .unwrap();
        assert!(stamps(&mut dbs.index_conn, board).await.is_empty());
        assert!(
            !association_of(&mut dbs.index_conn, &ctx, board)
                .await
                .associated,
            "a rotted board with its stamps cleared belongs nowhere"
        );
    }

    // Ensures a new name is recorded under the FOLDER's spelling, not the
    // client's: a stamp written as `Default` would never match a request
    // running under `default`, splitting the name fallback in half.
    #[tokio::test]
    async fn the_editor_records_the_folders_own_spelling() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;
        let ctx = AssociationContext::for_tests(
            Some("uuid_here"),
            Some("inst"),
            "Photos",
            crate::db::local_dbs::LocalDbIdentities::for_tests(&[("Photos", "uuid_here")], false),
        );

        set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["photos"],
            T0 + 100,
        )
        .await
        .unwrap();
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "Photos", "inst", T0 + 100)]
        );
    }

    // Ensures a name that is neither stamped nor local is refused rather than
    // silently dropped — the editor's checklist would otherwise lose an entry
    // with no explanation — and that the refusal writes nothing.
    #[tokio::test]
    async fn the_editor_rejects_a_name_it_cannot_resolve() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;
        crate::db::pinboard_dbs::stamp_for_tests(
            &mut dbs.index_conn,
            board,
            "uuid_retired",
            "retired",
            "inst",
            T0,
        )
        .await;
        let ctx = current_db();

        let err = set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["default", "nowhere"],
            T0 + 100,
        )
        .await
        .expect_err("an unknown name must be refused");
        assert!(err.detail().contains("nowhere"), "got {}", err.detail());
        assert_eq!(status_of(err), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_retired", "retired", "inst", T0)],
            "a refused request must leave the board's stamps untouched"
        );
    }

    // Ensures the no-identity degradation is exactly what the design says:
    // no NEW stamps, but carrying stored rows through and clearing them still
    // work — otherwise an identity-less deployment could never undo a stamp.
    #[tokio::test]
    async fn without_an_instance_identity_the_editor_can_still_carry_and_clear() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;
        crate::db::pinboard_dbs::stamp_for_tests(
            &mut dbs.index_conn,
            board,
            "uuid_here",
            "default",
            "inst",
            T0,
        )
        .await;
        let ctx = AssociationContext::for_tests(
            Some("uuid_here"),
            None,
            "default",
            crate::db::local_dbs::LocalDbIdentities::for_tests(&[("default", "uuid_here")], false),
        );

        // The stored row is kept by naming it, even though nothing could mint
        // it now.
        set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["default"],
            T0 + 100,
        )
        .await
        .unwrap();
        assert_eq!(
            stamps(&mut dbs.index_conn, board).await,
            vec![stamp_row("uuid_here", "default", "inst", T0)]
        );

        // A name with no stored row cannot be signed, and says so.
        let err = set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["default", "archive"],
            T0 + 200,
        )
        .await
        .expect_err("a new name needs an instance identity");
        assert!(err.detail().contains("archive"), "got {}", err.detail());
        assert!(
            err.detail().contains("no identity"),
            "the reason must be the missing instance identity: {}",
            err.detail()
        );

        set_databases(&mut dbs.index_conn, &ctx, board, "user", &[], T0 + 300)
            .await
            .unwrap();
        assert!(stamps(&mut dbs.index_conn, board).await.is_empty());
    }

    // Ensures the editor is user-scoped like every other pinboard mutation,
    // and that it leaves time_updated alone — an association is not a content
    // change, so editing one must not reorder the library.
    #[tokio::test]
    async fn the_editor_is_user_scoped_and_does_not_reorder_the_library() {
        let mut dbs = setup_test_databases().await;
        let board =
            create_through_handler(&mut dbs.index_conn, &["v2", "a"], &["beef"], None, T0).await;
        let ctx = current_db();
        let time_updated = pin_time_updated(&mut dbs.index_conn, board).await;

        let err = set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "other",
            &["default"],
            T0 + 100,
        )
        .await
        .expect_err("another user's board must not be editable");
        assert_eq!(status_of(err), axum::http::StatusCode::NOT_FOUND);
        assert!(stamps(&mut dbs.index_conn, board).await.is_empty());

        set_databases(
            &mut dbs.index_conn,
            &ctx,
            board,
            "user",
            &["default"],
            T0 + 100,
        )
        .await
        .unwrap();
        assert_eq!(stamps(&mut dbs.index_conn, board).await.len(), 1);
        assert_eq!(
            time_updated_of(&mut dbs.index_conn, board).await,
            time_updated
        );
    }

    // Ensures user scoping hides other users' boards from every accessor.
    #[tokio::test]
    async fn user_scoping_is_enforced() {
        let mut dbs = setup_test_databases().await;
        let (board, version) = create_board(&mut dbs.index_conn, None, &["v2", "a"], &[]).await;

        assert!(
            pinboards::get_pinboard(&mut dbs.index_conn, board, "other")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            pinboards::list_versions(&mut dbs.index_conn, board, "other")
                .await
                .unwrap()
                .is_empty()
        );
        let outcome = pinboards::delete_version(&mut dbs.index_conn, board, version, "other")
            .await
            .unwrap();
        assert!(matches!(outcome, pinboards::DeleteVersionOutcome::NotFound));
        assert!(
            !pinboards::delete_pinboard(&mut dbs.index_conn, board, "other")
                .await
                .unwrap()
        );
    }

    // Ensures duplicate items collapse to set membership.
    #[tokio::test]
    async fn duplicate_items_collapse() {
        let mut dbs = setup_test_databases().await;
        let (board, _) =
            create_board(&mut dbs.index_conn, None, &["v2", "a"], &["a1", "a1", "b2"]).await;

        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, board, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.item_count, 2);
    }

    // Ensures preview blobs round-trip and are user-scoped.
    #[tokio::test]
    async fn preview_blob_round_trip() {
        let mut dbs = setup_test_databases().await;
        let pinboard_id = pinboards::create_pinboard(&mut dbs.index_conn, "user", None, None, T0)
            .await
            .unwrap();
        let version_id = pinboards::append_version(
            &mut dbs.index_conn,
            pinboard_id,
            &layout(&["v2", "a"]),
            &[],
            Some(&[1, 2, 3, 4]),
            Some(1024),
            Some(768),
            Some(500),
        )
        .await
        .unwrap();

        let preview =
            pinboards::get_version_preview(&mut dbs.index_conn, pinboard_id, version_id, "user")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(preview.bytes, vec![1, 2, 3, 4]);
        // The serve path decides on the recorded width, so it has to come
        // back with the blob.
        assert_eq!(preview.width, Some(1024));

        assert!(
            pinboards::get_version_preview(&mut dbs.index_conn, pinboard_id, version_id, "other")
                .await
                .unwrap()
                .is_none()
        );
    }

    // Ensures a preview refresh replaces the blob and its dimensions in
    // place, is user-scoped, and leaves the board's time_updated alone (a
    // re-render is not a content change — same rule as set_flags).
    #[tokio::test]
    async fn update_version_preview_replaces_in_place() {
        let mut dbs = setup_test_databases().await;
        let pinboard_id = pinboards::create_pinboard(&mut dbs.index_conn, "user", None, None, T0)
            .await
            .unwrap();
        let version_id = pinboards::append_version(
            &mut dbs.index_conn,
            pinboard_id,
            &layout(&["v2", "a"]),
            &[],
            Some(&[1, 2, 3, 4]),
            Some(1024),
            Some(768),
            Some(500),
        )
        .await
        .unwrap();
        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        let time_updated = summary.time_updated;

        assert!(
            !pinboards::update_version_preview(
                &mut dbs.index_conn,
                pinboard_id,
                version_id,
                "other",
                &[9, 9],
                Some(2048),
                Some(1536),
                Some(900),
            )
            .await
            .unwrap()
        );
        assert!(
            pinboards::update_version_preview(
                &mut dbs.index_conn,
                pinboard_id,
                version_id,
                "user",
                &[5, 6, 7],
                Some(2048),
                Some(1536),
                Some(900),
            )
            .await
            .unwrap()
        );

        let preview =
            pinboards::get_version_preview(&mut dbs.index_conn, pinboard_id, version_id, "user")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(preview.bytes, vec![5, 6, 7]);
        assert_eq!(preview.width, Some(2048));

        let versions = pinboards::list_versions(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap();
        assert_eq!(versions.len(), 1, "a refresh must not mint a version");
        assert_eq!(versions[0].preview_h, Some(1536));
        assert_eq!(versions[0].screenful_h, Some(900));
        assert_eq!(versions[0].layout, layout(&["v2", "a"]));

        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.time_updated, time_updated);
    }

    // Ensures the serve path's passthrough test matches the downscale
    // path's clamp: at or above the stored width serves the stored bytes,
    // below it downscales, and an unrecorded width always downscales.
    #[test]
    fn stored_width_decides_the_passthrough() {
        assert!(width_at_least(Some(1024), 1024));
        assert!(width_at_least(Some(1024), 4096));
        // Clamped to 4096 like the downscale branch, so a wider master
        // still takes the same branch it always did.
        assert!(!width_at_least(Some(8192), 9000));
        assert!(!width_at_least(Some(2048), 1024));
        assert!(!width_at_least(None, 4096));
        assert!(!width_at_least(Some(0), 4096));
        // The lower clamp: maxw 1 is really 16, so a 16px master passes.
        assert!(width_at_least(Some(16), 1));
        assert!(!width_at_least(Some(17), 1));
    }

    // Ensures replacement previews can't record impossible dimensions —
    // the serve path trusts preview_w to skip decoding entirely.
    #[test]
    fn preview_dimensions_are_validated() {
        assert_eq!(validate_preview_dimension(None).unwrap(), None);
        assert_eq!(validate_preview_dimension(Some(2048)).unwrap(), Some(2048));
        assert!(validate_preview_dimension(Some(0)).is_err());
        assert!(validate_preview_dimension(Some(-1)).is_err());
        assert!(validate_preview_dimension(Some(MAX_PREVIEW_DIMENSION + 1)).is_err());
    }

    /// A real encoded PNG of the given size, so the header probe has
    /// something truthful to read.
    fn encoded_png(width: u32, height: u32) -> String {
        let img = image::RgbImage::new(width, height);
        let mut bytes = Vec::new();
        img.write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .expect("encode");
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    }

    // Ensures no writer can record dimensions the uploaded image does not
    // actually have. The serve path skips all work when preview_w says the
    // stored image is already narrow enough, so a wrong width would make
    // every consumer — down to the 160px history rail — fetch the full
    // master for the rest of that version's life.
    #[test]
    fn declared_preview_dimensions_must_match_the_image() {
        let png = encoded_png(64, 32);

        let upload = validate_preview_upload(Some(&png), Some(64), Some(32), Some(16)).unwrap();
        assert_eq!(upload.width, Some(64));
        assert_eq!(upload.height, Some(32));
        assert_eq!(upload.screenful_h, Some(16));

        // Either half wrong is a rejection.
        assert!(validate_preview_upload(Some(&png), Some(16), Some(32), None).is_err());
        assert!(validate_preview_upload(Some(&png), Some(64), Some(999), None).is_err());
        // Out-of-range dimensions are still caught before the image is read.
        assert!(validate_preview_upload(Some(&png), Some(0), None, None).is_err());
        // Undeclared dimensions stay legal: that is a version with no
        // recorded size, which the serve path already handles by decoding.
        let undeclared = validate_preview_upload(Some(&png), None, None, None).unwrap();
        assert_eq!(undeclared.width, None);
        // Bytes that are not a decodable image cannot back a declared size.
        let junk = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        assert!(validate_preview_upload(Some(&junk), Some(64), None, None).is_err());
        // An empty blob is "no preview", which the PUT handler turns into a
        // 400 and the save paths accept as a version without a picture.
        assert!(
            validate_preview_upload(Some(""), None, None, None)
                .unwrap()
                .bytes
                .is_none()
        );
    }

    // Ensures flags round-trip on the board, and set_flags detects change
    // without bumping time_updated (a settings-only save must not reorder
    // the library list or touch any version).
    #[tokio::test]
    async fn flags_round_trip_without_touching_versions() {
        let mut dbs = setup_test_databases().await;
        let flags = r#"{"pba":true,"pbc":true}"#;
        let pinboard_id =
            pinboards::create_pinboard(&mut dbs.index_conn, "user", None, Some(flags), T0)
                .await
                .unwrap();
        pinboards::append_version(
            &mut dbs.index_conn,
            pinboard_id,
            &layout(&["v2", "a"]),
            &[],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.flags.as_deref(), Some(flags));
        let time_updated = summary.time_updated;

        // Identical flags: no change reported.
        assert!(
            !pinboards::set_flags(&mut dbs.index_conn, pinboard_id, "user", flags)
                .await
                .unwrap()
        );
        // Different flags: change reported, stored, user-scoped.
        let changed = r#"{"pba":false,"pbc":true}"#;
        assert!(
            pinboards::set_flags(&mut dbs.index_conn, pinboard_id, "user", changed)
                .await
                .unwrap()
        );
        assert!(
            !pinboards::set_flags(&mut dbs.index_conn, pinboard_id, "other", flags)
                .await
                .unwrap()
        );

        let (summary, _) = pinboards::get_pinboard(&mut dbs.index_conn, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.flags.as_deref(), Some(changed));
        assert_eq!(summary.time_updated, time_updated);
        assert_eq!(summary.version_count, 1);
    }

    // Ensures flags canonicalization sorts keys, rejects non-objects, and
    // passes through absent flags.
    #[test]
    fn canonical_flags_sorts_and_validates() {
        let mut request = save_request(&["v2"], &[]);
        assert_eq!(canonical_flags(&request).unwrap(), None);

        request.flags = Some(serde_json::json!({"psc": true, "pba": false}));
        assert_eq!(
            canonical_flags(&request).unwrap().as_deref(),
            Some(r#"{"pba":false,"psc":true}"#)
        );

        request.flags = Some(serde_json::json!([1, 2]));
        assert!(canonical_flags(&request).is_err());
    }

    // Ensures request validation rejects bad layouts, items, and base64.
    #[test]
    fn validate_version_request_rejects_invalid_input() {
        let empty = save_request(&[], &[]);
        assert!(validate_version_request(&empty).is_err());

        let bad_item = save_request(&["v2"], &["not-hex!"]);
        assert!(validate_version_request(&bad_item).is_err());

        let mut bad_preview = save_request(&["v2"], &[]);
        bad_preview.preview_b64 = Some("!!!not base64!!!".to_string());
        assert!(validate_version_request(&bad_preview).is_err());

        let mut ok = save_request(&["v2", "aaaa", "0", "0", "10", "10"], &["abc123"]);
        ok.preview_b64 = Some(base64::engine::general_purpose::STANDARD.encode([1, 2, 3]));
        let upload = validate_version_request(&ok).unwrap();
        assert_eq!(upload.bytes.unwrap(), vec![1, 2, 3]);
    }

    /// A user_data write connection shaped like the request-scoped
    /// `UserDataWrite` ones where it matters here: user_data attached
    /// read-write in WAL. (The real ones also hold index/storage read-only —
    /// covered by `db::connection` tests — which is irrelevant to these
    /// user_data-lock contention tests.) Short busy timeout so a losing
    /// writer fails in bounded time instead of at sqlx's five-second
    /// default. On-disk because the shared-cache in-memory databases cannot
    /// be opened twice as independent connections.
    async fn connect_user_data_write(
        index_file: &std::path::Path,
        user_data_file: &std::path::Path,
    ) -> sqlx::SqliteConnection {
        use sqlx::Connection;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(index_file)
            .create_if_missing(true)
            .busy_timeout(std::time::Duration::from_millis(250));
        let mut conn = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        sqlx::query("ATTACH DATABASE ? AS user_data")
            .bind(user_data_file.to_string_lossy().to_string())
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("PRAGMA user_data.journal_mode=WAL")
            .execute(&mut conn)
            .await
            .unwrap();
        conn
    }

    /// Two connections to one on-disk user_data database, with a board. The
    /// index database is migrated too, not left as the empty file the
    /// connection would create: the board reads count how many of a board's
    /// items are present in it, so `main.items` has to exist.
    async fn contention_fixture(
        dir: &tempfile::TempDir,
    ) -> (sqlx::SqliteConnection, sqlx::SqliteConnection, i64) {
        let index_file = dir.path().join("index.db");
        let user_data_file = dir.path().join("user_data.db");
        crate::db::migrations::migrate_index_db_file(&index_file)
            .await
            .unwrap();
        crate::db::migrations::migrate_user_data_db_file(&user_data_file)
            .await
            .unwrap();
        let mut saver = connect_user_data_write(&index_file, &user_data_file).await;
        let telemetry = connect_user_data_write(&index_file, &user_data_file).await;
        let (pinboard_id, _) = create_board(
            &mut saver,
            None,
            &["v2", "aaaa", "0", "0", "10", "10"],
            &["a1"],
        )
        .await;
        (saver, telemetry, pinboard_id)
    }

    // Ensures the user-facing write transaction beats the fire-and-forget
    // activity writer: taking the user_data write lock up front makes the
    // background record_open the contention loser (it waits out the busy
    // timeout and fails), while the save's own read, write and COMMIT all
    // succeed. Reverting begin_transaction to a deferred BEGIN flips both
    // assertions — see the deferred counterpart below.
    #[tokio::test]
    async fn save_transaction_beats_background_activity_write() {
        let dir = tempfile::tempdir().unwrap();
        let (mut saver, mut telemetry, pinboard_id) = contention_fixture(&dir).await;
        let now = T0 + 3 * 60 * 60;

        begin_transaction(&mut saver).await.unwrap();
        // The save path reads before its first write — the deferred-BEGIN trap.
        let (summary, _) = pinboards::get_pinboard(&mut saver, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();

        let recorded = pinboards::record_open(
            &mut telemetry,
            pinboard_id,
            "user",
            now,
            summary.frecency,
            summary.frecency_at,
        )
        .await;
        assert!(
            recorded.is_err(),
            "telemetry write must lose the race, got {recorded:?}"
        );

        pinboards::touch_saved(&mut saver, pinboard_id, "user", now)
            .await
            .expect("the user's save must survive the contention");
        commit_transaction(&mut saver).await.unwrap();

        let (summary, _) = pinboards::get_pinboard(&mut telemetry, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.last_seen, Some(now));
    }

    // Pins the reason begin_transaction must not be deferred: with a deferred
    // BEGIN the save's read pins a WAL snapshot, the background telemetry
    // write commits over it, and the save's *first write* then fails
    // SQLITE_BUSY_SNAPSHOT — which does not invoke the busy handler, so the
    // busy timeout above never applies and the failure is immediate.
    #[tokio::test]
    async fn deferred_transaction_loses_to_background_activity_write() {
        let dir = tempfile::tempdir().unwrap();
        let (mut saver, mut telemetry, pinboard_id) = contention_fixture(&dir).await;
        let now = T0 + 3 * 60 * 60;

        sqlx::query("BEGIN TRANSACTION")
            .execute(&mut saver)
            .await
            .unwrap();
        let (summary, _) = pinboards::get_pinboard(&mut saver, pinboard_id, "user")
            .await
            .unwrap()
            .unwrap();

        pinboards::record_open(
            &mut telemetry,
            pinboard_id,
            "user",
            now,
            summary.frecency,
            summary.frecency_at,
        )
        .await
        .expect("nothing holds the write lock, so telemetry wins instead");

        assert!(
            pinboards::touch_saved(&mut saver, pinboard_id, "user", now)
                .await
                .is_err(),
            "a deferred save must fail here — that is why BEGIN IMMEDIATE is required"
        );
        let _ = rollback_transaction(&mut saver).await;
    }

    // Ensures media type sniffing recognizes the formats browsers upload.
    #[test]
    fn sniff_media_types() {
        let mut webp = b"RIFF".to_vec();
        webp.extend_from_slice(&[0, 0, 0, 0]);
        webp.extend_from_slice(b"WEBP");
        assert_eq!(sniff_image_media_type(&webp), "image/webp");
        assert_eq!(
            sniff_image_media_type(&[0x89, b'P', b'N', b'G', 0x0D]),
            "image/png"
        );
        assert_eq!(sniff_image_media_type(&[0xFF, 0xD8, 0xFF]), "image/jpeg");
        assert_eq!(
            sniff_image_media_type(b"garbage"),
            "application/octet-stream"
        );
    }
}
