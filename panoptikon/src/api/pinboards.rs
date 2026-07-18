use axum::{
    Json,
    body::Body,
    extract::Path,
    http::{Response, header},
};
use axum_extra::extract::Query;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api_error::ApiError;
use crate::db::pinboards;
use crate::db::{DbConnection, ReadOnly, UserDataWrite};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_USER: &str = "user";
/// Decoded preview blobs larger than this are rejected outright.
const MAX_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
/// Serialized layouts larger than this are rejected outright.
const MAX_LAYOUT_BYTES: usize = 1024 * 1024;
/// Serialized board flags larger than this are rejected outright.
const MAX_FLAGS_BYTES: usize = 4096;

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

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreatePinboardRequest {
    /// Optional display name; pinboards are identified by preview otherwise.
    name: Option<String>,
    #[serde(flatten)]
    version: SaveVersionRequest,
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
    preview_w: Option<i64>,
    preview_h: Option<i64>,
    screenful_h: Option<i64>,
    item_count: i64,
    version_count: i64,
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

struct PreviewUpload {
    bytes: Option<Vec<u8>>,
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
    let serialized = serde_json::to_string(&sorted)
        .map_err(|_| ApiError::bad_request("Invalid flags"))?;
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

    let bytes = match request.preview_b64.as_deref() {
        None | Some("") => None,
        Some(encoded) => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| ApiError::bad_request("Invalid base64 preview"))?;
            if decoded.len() > MAX_PREVIEW_BYTES {
                return Err(ApiError::bad_request("Preview image too large"));
            }
            Some(decoded)
        }
    };
    Ok(PreviewUpload { bytes })
}

fn map_summary(summary: pinboards::PinboardSummary) -> PinboardSummaryResponse {
    PinboardSummaryResponse {
        id: summary.id,
        name: summary.name,
        head_version_id: summary.head_version_id,
        time_added: summary.time_added,
        time_updated: summary.time_updated,
        preview_w: summary.preview_w,
        preview_h: summary.preview_h,
        screenful_h: summary.screenful_h,
        item_count: summary.item_count,
        version_count: summary.version_count,
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
    description = "Lists the user's saved pinboards, most recently updated first, with head-version metadata (preview dimensions, item and version counts) but without layouts or preview blobs.\nThe `q` parameter matches pinboard names via FTS prefix search.",
    params(DbQueryParams, PinboardListQuery),
    responses(
        (status = 200, description = "Saved pinboards", body = PinboardListResponse)
    )
)]
pub async fn list_pinboards(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<PinboardListQuery>,
) -> ApiResult<Json<PinboardListResponse>> {
    let summaries =
        pinboards::list_pinboards(&mut db.conn, &query.user, query.q.as_deref()).await?;
    Ok(Json(PinboardListResponse {
        pinboards: summaries.into_iter().map(map_summary).collect(),
    }))
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

    begin_transaction(&mut db.conn).await?;
    let result: ApiResult<(i64, i64)> = async {
        let pinboard_id = pinboards::create_pinboard(
            &mut db.conn,
            &query.user,
            request.name.as_deref(),
            flags.as_deref(),
        )
        .await?;
        let version_id = pinboards::append_version(
            &mut db.conn,
            pinboard_id,
            &request.version.layout,
            &request.version.items,
            preview.bytes.as_deref(),
            request.version.preview_w,
            request.version.preview_h,
            request.version.screenful_h,
        )
        .await?;
        Ok((pinboard_id, version_id))
    }
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
    Ok(Json(PinboardDetailResponse {
        id: summary.id,
        name: summary.name,
        flags: parse_stored_flags(summary.flags),
        time_added: summary.time_added,
        time_updated: summary.time_updated,
        version_count: summary.version_count,
        head: head.map(map_version),
    }))
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

    begin_transaction(&mut db.conn).await?;
    let result: ApiResult<SavePinboardResponse> = async {
        if !pinboards::pinboard_exists(&mut db.conn, pinboard_id, &query.user).await? {
            return Err(ApiError::not_found("Pinboard not found"));
        }

        if let Some((head_version_id, head_layout)) =
            pinboards::get_head_layout(&mut db.conn, pinboard_id, &query.user).await?
        {
            let incoming = serde_json::to_string(&request.layout)
                .map_err(|_| ApiError::bad_request("Invalid layout"))?;
            if incoming == head_layout {
                // Settings-only save: the layout no-ops but the board's
                // flags still advance to what the client sent.
                let flags_updated = match flags.as_deref() {
                    Some(flags) => {
                        pinboards::set_flags(&mut db.conn, pinboard_id, &query.user, flags).await?
                    }
                    None => false,
                };
                return Ok(SavePinboardResponse {
                    pinboard_id,
                    version_id: head_version_id,
                    no_op: true,
                    flags_updated,
                });
            }
        }

        let version_id = pinboards::append_version(
            &mut db.conn,
            pinboard_id,
            &request.layout,
            &request.items,
            preview.bytes.as_deref(),
            request.preview_w,
            request.preview_h,
            request.screenful_h,
        )
        .await?;
        let flags_updated = match flags.as_deref() {
            Some(flags) => {
                pinboards::set_flags(&mut db.conn, pinboard_id, &query.user, flags).await?
            }
            None => false,
        };
        Ok(SavePinboardResponse {
            pinboard_id,
            version_id,
            no_op: false,
            flags_updated,
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
    description = "Serves the client-composited preview for one version. Versions are immutable, so responses carry immutable cache headers.\nWith `maxw`, the image is downscaled on the fly (JPEG) to at most that width; without it, the stored image is served as uploaded.",
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

async fn begin_transaction(conn: &mut sqlx::SqliteConnection) -> ApiResult<()> {
    sqlx::query("BEGIN TRANSACTION")
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

    async fn create_board(
        conn: &mut sqlx::SqliteConnection,
        name: Option<&str>,
        records: &[&str],
        items: &[&str],
    ) -> (i64, i64) {
        let pinboard_id = pinboards::create_pinboard(conn, "user", name, None)
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

        let boards = pinboards::list_pinboards(&mut dbs.index_conn, "user", None)
            .await
            .unwrap();
        assert_eq!(boards.len(), 2);
        let board = boards.iter().find(|board| board.id == board_a).unwrap();
        assert_eq!(board.name.as_deref(), Some("poses"));
        assert_eq!(board.head_version_id, Some(head_a));
        assert_eq!(board.item_count, 2);
        assert_eq!(board.version_count, 1);
    }

    // Ensures FTS name search matches by prefix and ignores other boards.
    #[tokio::test]
    async fn list_pinboards_fts_name_search() {
        let mut dbs = setup_test_databases().await;
        create_board(&mut dbs.index_conn, Some("poses standing"), &["v2"], &[]).await;
        create_board(&mut dbs.index_conn, Some("landscapes"), &["v2"], &[]).await;
        create_board(&mut dbs.index_conn, None, &["v2"], &[]).await;

        let hits = pinboards::list_pinboards(&mut dbs.index_conn, "user", Some("pos"))
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
        let pinboard_id = pinboards::create_pinboard(&mut dbs.index_conn, "user", None, None)
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

        assert!(
            pinboards::get_version_preview(&mut dbs.index_conn, pinboard_id, version_id, "other")
                .await
                .unwrap()
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
            pinboards::create_pinboard(&mut dbs.index_conn, "user", None, Some(flags))
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
