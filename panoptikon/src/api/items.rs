use axum::{
    Json,
    body::Body,
    http::{HeaderMap, Response, header},
};
use axum_extra::extract::Query;

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::UNIX_EPOCH;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api::http_file::{
    FILE_IO_TIMEOUT, FileServeSpec, if_none_match_matches, not_modified_response,
    open_file_with_timeout, serve_file,
};
use crate::api::utils::{
    content_disposition_value, iso_to_system_time, serve_outro_metadata, strip_non_latin1_chars,
};
use crate::api_error::ApiError;
use crate::db::items::{
    ExtractedTextRecord, FileRecord, ItemIdentifierType, ItemRecord, get_all_tags_for_item,
    get_extracted_text_for_item, get_item_metadata, get_item_metadata_unchecked, get_text_by_ids,
    get_thumbnail_bytes,
};
use crate::db::{DbConnection, ReadOnlyNoUserData};
use crate::jobs::files::format_system_time;

type ApiResult<T> = std::result::Result<T, ApiError>;

const PLACEHOLDER_PNG: &[u8] = include_bytes!("assets/placeholder.png");

/// 404 detail for a file row whose file cannot be opened.
const NO_FILE_FOUND: &str = "No file found for item";

/// Minimum sha256 prefix length (hex chars) still treated as
/// content-addressed for caching. The pinboard stores 10-char prefixes as
/// item identity, so it must be <= 10; at 40 bits the chance a given cached
/// prefix ever gains a second match is ~n/2^40 (under one in a million even
/// for multi-million-item libraries), and the pinboard already relies on
/// prefix uniqueness for identity anyway.
const MIN_IMMUTABLE_SHA256_PREFIX: usize = 10;

/// Content-addressed URL bytes can never legitimately change.
const CACHE_IMMUTABLE: &str = "public, max-age=31536000, immutable";
/// Content-addressed URL, but the on-disk mtime no longer matches the one
/// recorded at index time: the bytes may have drifted from the hash in the
/// URL, so cap the cache and let the ETag revalidate after expiry.
const CACHE_DRIFTED: &str = "public, max-age=3600";
/// Re-pointable identifier (path, file_id, ...): revalidate every time
/// (cheaply, via ETag/304).
const CACHE_REVALIDATE: &str = "public, no-cache";

/// Whether the request addressed the item by content: sha256 addressing with
/// enough of the hash to make a collision negligible. Only such URLs may
/// claim immutability; any other identifier (path, file_id, short prefix,
/// ...) can be re-pointed, so those responses must always revalidate.
fn is_content_addressed(id_type: ItemIdentifierType, id: &str) -> bool {
    matches!(id_type, ItemIdentifierType::Sha256) && id.len() >= MIN_IMMUTABLE_SHA256_PREFIX
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemTextQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default)]
    setters: Vec<String>,
    #[serde(default)]
    languages: Vec<String>,
    /// Text will be truncated to this length, if set. The `length` field will contain the original length.
    truncate_length: Option<usize>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ItemTagsQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default)]
    /// List of models that set the tags to filter by (default: all)
    setters: Vec<String>,
    #[serde(default)]
    /// List of namespaces to filter by (default: all). A namespace includes all namespaces that start with the namespace string.
    namespaces: Vec<String>,
    #[serde(default)]
    #[param(default = 0.0, minimum = 0.0, maximum = 1.0)]
    /// Minimum confidence threshold, between 0 and 1 (default: 0.0)
    confidence_threshold: f64,
    /// Maximum number of tags to return for each *setter, namespace pair* (default: all). Higher confidence tags are given priority.
    limit_per_namespace: Option<usize>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct TextAnyQuery {
    /// List of extracted text IDs
    text_ids: Vec<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ItemMetadataResponse {
    item: ItemRecordResponse,
    files: Vec<FileRecordResponse>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ItemRecordResponse {
    id: i64,
    sha256: String,
    md5: String,
    #[serde(rename = "type")]
    item_type: String,
    // Always serialized (no skip_serializing_if): required-but-nullable in
    // the schema, so generated clients don't have to treat them as absent.
    #[schema(required)]
    size: Option<i64>,
    #[schema(required)]
    width: Option<i64>,
    #[schema(required)]
    height: Option<i64>,
    #[schema(required)]
    duration: Option<f64>,
    #[schema(required)]
    audio_tracks: Option<i64>,
    #[schema(required)]
    video_tracks: Option<i64>,
    #[schema(required)]
    subtitle_tracks: Option<i64>,
    #[schema(required)]
    blurhash: Option<String>,
    /// The raw stored outro verdict, detector version included
    /// (`tiktok_card/1`, `none/1`); `null` when the item was never examined.
    /// Kind-specific checks must prefix-match (`tiktok_card/`) rather than
    /// compare the whole value — see
    /// `docs/video-outro-detection-design.md` §6.2. "Has an outro" is
    /// `content_end_ms` being non-null.
    ///
    /// Served as `null` for every item when the index database has
    /// `detect_outros` off, including items whose outro was detected while it
    /// was on: the toggle turns the whole feature off for its database.
    /// Note the deliberate asymmetry — PQL predicates (`match` filters,
    /// `order_by`) on this column keep working with the toggle off, because
    /// querying your own data is a query capability, not playback
    /// (`docs/video-outro-skip-design.md` §6). The visible edge of that
    /// asymmetry: an `order_by` on `content_end_ms` still orders the rows by
    /// the stored boundaries even though every served value is null.
    #[schema(required)]
    outro_kind: Option<String>,
    /// Where the item's real content ends, when an outro was found.
    ///
    /// Served as `null` for every item when the index database has
    /// `detect_outros` off, on the same terms (and with the same PQL
    /// asymmetry) as `outro_kind` — an `order_by` on this column still
    /// orders the rows by the stored boundaries even though every served
    /// value is null.
    #[schema(required)]
    content_end_ms: Option<i64>,
    time_added: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct FileRecordResponse {
    id: i64,
    sha256: String,
    path: String,
    last_modified: String,
    filename: String,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TextResponse {
    text: Vec<ExtractedTextRecord>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct TagResponse {
    tags: Vec<(String, String, f64, String)>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ThumbnailQuery {
    /// An item identifier (sha256 hash, file ID, path, item ID, or data ID for associated data)
    id: String,
    /// The type of the item identifier
    id_type: ItemIdentifierType,
    #[serde(default = "default_true")]
    #[param(default = true)]
    big: bool,
}

#[utoipa::path(
    get,
    operation_id = "item_file",
    path = "/api/items/item/file",
    tag = "items",
    summary = "Get actual file contents for an item",
    description = "Returns the actual file contents for a given item.\nContent type is determined by the file extension.\nSupports HTTP Range requests (single byte ranges) for seeking in media files.",
    params(DbQueryParams, ItemQuery),
    responses(
        (status = 200, description = "Item file contents"),
        (status = 206, description = "Partial item file contents (Range request)"),
        (status = 416, description = "Requested range not satisfiable")
    )
)]
pub async fn item_file(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let content_addressed = is_content_addressed(query.id_type, &query.id);
    file_response(
        &item,
        &item_data.files,
        "inline",
        &request_headers,
        content_addressed,
    )
    .await
}

#[utoipa::path(
    get,
    operation_id = "item_meta",
    path = "/api/items/item",
    tag = "items",
    summary = "Get item metadata and associated file metadata",
    description = "Returns metadata for a given item.\nThis includes the item metadata and a list of all files associated with the item.\nFiles that do not exist on disk will not be included in the response.\nThis means the file list may be empty.\n\nAn `item` is a unique file. `item`s can have multiple `file`s associated with them, but unlike `file`s, `item`s have a unique sha256 hash.\nFiles are unique by `path`. If all files associated with an `item` are deleted, the item is deleted.",
    params(DbQueryParams, ItemQuery),
    responses(
        (status = 200, description = "Item metadata", body = ItemMetadataResponse)
    )
)]
pub async fn item_meta(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemQuery>,
) -> ApiResult<Json<ItemMetadataResponse>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    // The mapping's outro gate stats a config file, which can block on a
    // network mount; the query is already done, so the pooled connection goes
    // back to the read pool before that rather than being pinned across it.
    let index_db = db.index_db.clone();
    drop(db);

    let response = ItemMetadataResponse {
        item: map_item_record(&index_db, &item).await,
        files: item_data.files.into_iter().map(map_file_record).collect(),
    };

    Ok(Json(response))
}

#[utoipa::path(
    get,
    operation_id = "item_text",
    path = "/api/items/item/text",
    tag = "items",
    summary = "Get all text extracted from an item",
    description = "Returns the text extracted from a given item",
    params(DbQueryParams, ItemTextQuery),
    responses(
        (status = 200, description = "Extracted text", body = TextResponse)
    )
)]
pub async fn item_text(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemTextQuery>,
) -> ApiResult<Json<TextResponse>> {
    // Unchecked: only the item id is needed, no reason to stat the files.
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    let mut text =
        get_extracted_text_for_item(&mut db.conn, item.id, query.truncate_length).await?;
    if !query.setters.is_empty() {
        text.retain(|entry| {
            query
                .setters
                .iter()
                .any(|setter| setter == &entry.setter_name)
        });
    }
    if !query.languages.is_empty() {
        text.retain(|entry| {
            entry
                .language
                .as_ref()
                .map(|language| query.languages.iter().any(|entry| entry == language))
                .unwrap_or(false)
        });
    }

    Ok(Json(TextResponse { text }))
}

#[utoipa::path(
    get,
    operation_id = "item_tags",
    path = "/api/items/item/tags",
    tag = "items",
    summary = "Get tags for an item",
    description = "Returns the tags associated with a given item.\nThe response contains a list of tuples, where each tuple contains\nthe tag namespace, tag name, confidence, and setter name.\nThe `setters` parameter can be used to filter tags by the setter name.\nThe `confidence_threshold` parameter can be used to filter tags based on\nthe minimum confidence threshold",
    params(DbQueryParams, ItemTagsQuery),
    responses(
        (status = 200, description = "Item tags", body = TagResponse)
    )
)]
pub async fn item_tags(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ItemTagsQuery>,
) -> ApiResult<Json<TagResponse>> {
    // Unchecked: only the item id is needed, no reason to stat the files.
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    let tags = get_all_tags_for_item(
        &mut db.conn,
        item.id,
        &query.setters,
        query.confidence_threshold,
        &query.namespaces,
        query.limit_per_namespace,
    )
    .await?;

    Ok(Json(TagResponse { tags }))
}

#[utoipa::path(
    get,
    operation_id = "texts_any",
    path = "/api/items/text/any",
    tag = "items",
    summary = "Get text from text_ids",
    description = "Returns texts given a list of text IDs",
    params(DbQueryParams, TextAnyQuery),
    responses(
        (status = 200, description = "Extracted text entries", body = TextResponse)
    )
)]
pub async fn texts_any(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<TextAnyQuery>,
) -> ApiResult<Json<TextResponse>> {
    let text = get_text_by_ids(&mut db.conn, &query.text_ids).await?;
    Ok(Json(TextResponse { text }))
}

#[utoipa::path(
    get,
    operation_id = "item_thumbnail",
    path = "/api/items/item/thumbnail",
    tag = "items",
    summary = "Get thumbnail for an item",
    description = "Returns a thumbnail for a given item.\nThe thumbnail may be a thumbnail,\nthe unmodified original image (only for images),\nor a placeholder image generated on the fly.\nGIFs are always returned as the original file.\nFor video thumbnails, the `big` parameter can be used to\nselect between the 2x2 frame grid (big=True) or the first frame from the grid (big=False).",
    params(DbQueryParams, ThumbnailQuery),
    responses(
        (status = 200, description = "Item thumbnail image")
    )
)]
pub async fn item_thumbnail(
    mut db: DbConnection<ReadOnlyNoUserData>,
    Query(query): Query<ThumbnailQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata_unchecked(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let content_addressed = is_content_addressed(query.id_type, &query.id);
    match thumbnail_response(
        &mut db.conn,
        &item,
        &item_data.files,
        query.big,
        &request_headers,
        content_addressed,
    )
    .await
    {
        Ok(response) => Ok(response),
        Err(err) => {
            tracing::error!(error = ?err, "error generating thumbnail");
            Err(ApiError::not_found("Thumbnail not found"))
        }
    }
}

fn display_filename(file: &FileRecord) -> String {
    let filename = strip_non_latin1_chars(&file.filename);
    if filename.is_empty() {
        file.filename.clone()
    } else {
        filename
    }
}

async fn thumbnail_response(
    conn: &mut sqlx::SqliteConnection,
    item: &ItemRecord,
    files: &[FileRecord],
    big: bool,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    let original_filename = display_filename(&files[0]);
    let original_filename_no_ext = Path::new(&original_filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(&original_filename);

    let mime = item.mime_type.as_str();
    if mime.is_empty() || mime.starts_with("image/gif") {
        return file_response(item, files, "inline", request_headers, content_addressed).await;
    }

    let index = if mime.starts_with("video") {
        if big { 0 } else { 1 }
    } else {
        0
    };

    // Stored thumbnails are keyed by content hash; the ETag mirrors that. If
    // thumbnail generation ever changes quality/size for existing content,
    // bust caches by versioning the URL, not by weakening this. Unlike raw
    // file serving there is no drift caveat: the stored thumbnail is derived
    // from exactly the content the URL names, however the disk file has
    // changed since, so content-addressed requests stay fully immutable.
    let sha256 = &item.sha256;
    if let Some(buffer) = get_thumbnail_bytes(conn, sha256, index).await? {
        let etag = format!("\"{sha256}-thumb{index}\"");
        let filename = format!("{original_filename_no_ext}.jpg");
        let cache_control = if content_addressed {
            CACHE_IMMUTABLE
        } else {
            CACHE_REVALIDATE
        };
        return bytes_response(
            buffer,
            "image/jpeg",
            &filename,
            &etag,
            cache_control,
            request_headers,
        );
    }

    if mime.starts_with("image") {
        return file_response(item, files, "inline", request_headers, content_addressed).await;
    }

    // The placeholder may be replaced by a real thumbnail later (e.g. after
    // the next scan), so it must not claim immutability.
    let etag = format!("\"{sha256}-placeholder\"");
    let filename = format!("{original_filename_no_ext}.png");
    bytes_response(
        PLACEHOLDER_PNG.to_vec(),
        "image/png",
        &filename,
        &etag,
        "public, max-age=300",
        request_headers,
    )
}

/// Serves the first candidate file that can actually be opened. Candidates
/// are ordered by `available` DESC in the DB; a missing or hung file falls
/// through to the next instead of failing the request (previously every
/// candidate was pre-filtered with a blocking stat on the async worker).
async fn file_response(
    item: &ItemRecord,
    files: &[FileRecord],
    content_disposition_type: &'static str,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    let mut last_error = ApiError::not_found(NO_FILE_FOUND);
    for file in files {
        match try_file_response(
            item,
            file,
            content_disposition_type,
            request_headers,
            content_addressed,
        )
        .await
        {
            Ok(response) => return Ok(response),
            Err(err) => {
                tracing::warn!(path = %file.path, "failed to serve file candidate");
                last_error = err;
            }
        }
    }
    Err(last_error)
}

async fn try_file_response(
    item: &ItemRecord,
    file: &FileRecord,
    content_disposition_type: &'static str,
    request_headers: &HeaderMap,
    content_addressed: bool,
) -> ApiResult<Response<Body>> {
    let filename = display_filename(file);
    let file_handle = open_file_with_timeout(&file.path, NO_FILE_FOUND).await?;
    // The size on disk is authoritative for range math; the DB value can be
    // stale if the file changed since the last scan.
    let metadata = match tokio::time::timeout(FILE_IO_TIMEOUT, file_handle.metadata()).await {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(err)) => {
            tracing::error!(error = %err, "failed to read file metadata");
            return Err(ApiError::internal("Failed to read file metadata"));
        }
        Err(_) => {
            tracing::error!(path = %file.path, "timed out reading file metadata");
            return Err(ApiError::internal("Timed out reading file metadata"));
        }
    };
    let size = metadata.len();

    // Strong validator: the item's content hash plus the on-disk size/mtime.
    // The disk components catch the (path/file_id-addressed) case where the
    // file changed after indexing and the recorded hash no longer matches.
    let modified = metadata.modified().ok();
    let mtime_secs = modified
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let etag = format!("\"{}-{:x}-{:x}\"", item.sha256, size, mtime_secs);

    // Immutability requires the served bytes to still be the content the URL
    // names. The on-disk mtime matching the one recorded at index time is
    // the same freshness check the scanner uses for unchanged files (via the
    // shared `format_system_time` truncation, so the strings compare equal);
    // on a mismatch the bytes may have drifted from the hash, so the cache
    // gets a bounded lifetime instead. Costs nothing: the stat already
    // happened for the ETag.
    let cache_control = if content_addressed {
        let mtime_matches = modified
            .and_then(format_system_time)
            .is_some_and(|disk_mtime| disk_mtime == file.last_modified);
        if mtime_matches {
            CACHE_IMMUTABLE
        } else {
            CACHE_DRIFTED
        }
    } else {
        CACHE_REVALIDATE
    };

    let last_modified = iso_to_system_time(&file.last_modified).map(httpdate::fmt_http_date);

    serve_file(
        FileServeSpec {
            file: file_handle,
            size,
            mime_type: item.mime_type.clone(),
            etag,
            cache_control,
            last_modified,
            content_disposition_type,
            filename,
        },
        request_headers,
    )
    .await
}

fn bytes_response(
    bytes: Vec<u8>,
    media_type: &str,
    filename: &str,
    etag: &str,
    cache_control: &str,
    request_headers: &HeaderMap,
) -> ApiResult<Response<Body>> {
    if let Some(if_none_match) = request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        if if_none_match_matches(if_none_match, etag) {
            return Ok(not_modified_response(etag, cache_control, None));
        }
    }

    let len = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    let headers = response.headers_mut();

    if let Ok(value) = header::HeaderValue::from_str(media_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&len.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(etag) {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    if let Some(value) = content_disposition_value("inline", filename) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }

    Ok(response)
}

/// Maps an item record for the response, applying the index database's outro
/// serving gate (`docs/video-outro-skip-design.md` §6): with `detect_outros`
/// off, both outro fields are served null whatever the row holds.
async fn map_item_record(index_db: &str, item: &ItemRecord) -> ItemRecordResponse {
    let serve_outro = serve_outro_metadata(
        index_db,
        item.outro_kind.is_some() || item.content_end_ms.is_some(),
    )
    .await;
    ItemRecordResponse {
        id: item.id,
        sha256: item.sha256.clone(),
        md5: item.md5.clone(),
        item_type: item.mime_type.clone(),
        size: item.size,
        width: item.width,
        height: item.height,
        duration: item.duration,
        audio_tracks: item.audio_tracks,
        video_tracks: item.video_tracks,
        subtitle_tracks: item.subtitle_tracks,
        blurhash: item.blurhash.clone(),
        outro_kind: serve_outro.then(|| item.outro_kind.clone()).flatten(),
        content_end_ms: serve_outro.then_some(item.content_end_ms).flatten(),
        time_added: item.time_added.clone(),
    }
}

fn map_file_record(file: FileRecord) -> FileRecordResponse {
    FileRecordResponse {
        id: file.id,
        sha256: file.sha256,
        path: file.path,
        last_modified: file.last_modified,
        filename: file.filename,
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("panoptikon_{label}_{stamp}"))
    }

    fn test_records(file_path: &PathBuf) -> (ItemRecord, FileRecord) {
        let item = ItemRecord {
            id: 1,
            sha256: "sha256".to_string(),
            md5: "md5".to_string(),
            mime_type: "image/png".to_string(),
            size: Some(4),
            width: None,
            height: None,
            duration: None,
            audio_tracks: None,
            video_tracks: None,
            subtitle_tracks: None,
            blurhash: None,
            outro_kind: None,
            content_end_ms: None,
            time_added: "2024-01-01T00:00:00".to_string(),
        };
        let file = FileRecord {
            id: 10,
            sha256: "sha256".to_string(),
            path: file_path.to_string_lossy().to_string(),
            last_modified: "2024-01-01T00:00:00".to_string(),
            filename: "file.png".to_string(),
        };
        (item, file)
    }

    async fn body_bytes(response: Response<Body>) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    // Ensures file responses include content headers derived from item metadata.
    #[tokio::test]
    async fn file_response_sets_headers() {
        let file_path = temp_path("file_response");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);

        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            false,
        )
        .await
        .unwrap();
        let headers = response.headers();

        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "image/png");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(headers.get(header::ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(
            headers.get(header::CONTENT_DISPOSITION).unwrap(),
            "inline; filename=\"file.png\""
        );
        assert!(headers.get(header::LAST_MODIFIED).is_some());
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn file_response_serves_byte_range() {
        let file_path = temp_path("file_range");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let headers = response.headers();
        assert_eq!(headers.get(header::CONTENT_RANGE).unwrap(), "bytes 2-5/10");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(headers.get(header::ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(body_bytes(response).await, b"2345");
    }

    #[tokio::test]
    async fn file_response_serves_open_ended_and_suffix_ranges() {
        let file_path = temp_path("file_range_open");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=7-".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 7-9/10"
        );
        assert_eq!(body_bytes(response).await, b"789");

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=-3".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 7-9/10"
        );
        assert_eq!(body_bytes(response).await, b"789");
    }

    #[tokio::test]
    async fn file_response_rejects_unsatisfiable_range() {
        let file_path = temp_path("file_range_416");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=100-".parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */10"
        );
        assert!(body_bytes(response).await.is_empty());
    }

    #[tokio::test]
    async fn file_response_ignores_range_on_stale_if_range() {
        let file_path = temp_path("file_if_range");
        std::fs::write(&file_path, b"0123456789").unwrap();
        let (item, file) = test_records(&file_path);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        request_headers.insert(
            header::IF_RANGE,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"0123456789");
    }

    // Cache validators are emitted and a matching If-None-Match short-circuits
    // to an empty 304 carrying the same validators.
    #[tokio::test]
    async fn file_response_supports_etag_and_304() {
        let file_path = temp_path("file_etag");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);

        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            false,
        )
        .await
        .unwrap();
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("ETag header present")
            .to_str()
            .unwrap()
            .to_string();
        assert!(etag.starts_with("\"sha256-"), "{etag}");
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, no-cache"
        );

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::IF_NONE_MATCH, etag.parse().unwrap());
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &request_headers,
            false,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), &etag);
        assert!(response.headers().get(header::CACHE_CONTROL).is_some());
        assert!(body_bytes(response).await.is_empty());
    }

    // A missing first candidate falls through to the next instead of 404ing.
    #[tokio::test]
    async fn file_response_falls_back_to_next_candidate() {
        let file_path = temp_path("file_fallback");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, file) = test_records(&file_path);
        let missing = FileRecord {
            id: 11,
            sha256: file.sha256.clone(),
            path: temp_path("file_fallback_missing")
                .to_string_lossy()
                .to_string(),
            last_modified: file.last_modified.clone(),
            filename: "gone.png".to_string(),
        };

        let response = file_response(&item, &[missing, file], "inline", &HeaderMap::new(), false)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"test");
    }

    #[tokio::test]
    async fn bytes_response_supports_etag_and_304() {
        let response = bytes_response(
            b"thumb".to_vec(),
            "image/jpeg",
            "file.jpg",
            "\"sha-thumb0\"",
            "public, max-age=31536000, immutable",
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            "\"sha-thumb0\""
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=31536000, immutable"
        );

        let mut request_headers = HeaderMap::new();
        request_headers.insert(header::IF_NONE_MATCH, "\"sha-thumb0\"".parse().unwrap());
        let response = bytes_response(
            b"thumb".to_vec(),
            "image/jpeg",
            "file.jpg",
            "\"sha-thumb0\"",
            "public, max-age=31536000, immutable",
            &request_headers,
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert!(body_bytes(response).await.is_empty());
    }

    #[test]
    fn content_addressing_requires_long_enough_sha256() {
        let full = "a".repeat(64);
        assert!(is_content_addressed(ItemIdentifierType::Sha256, &full));
        // Pinboard-length (10 char) prefixes are content-addressed enough.
        assert!(is_content_addressed(
            ItemIdentifierType::Sha256,
            "abc123def4"
        ));
        assert!(!is_content_addressed(
            ItemIdentifierType::Sha256,
            "abc123def"
        ));
        assert!(!is_content_addressed(
            ItemIdentifierType::Path,
            "C:/img.png"
        ));
    }

    // Content-addressed requests are only immutable while the on-disk mtime
    // still matches the one recorded at index time; a drifted mtime means the
    // bytes may no longer be the content the URL names, so the cache lifetime
    // is bounded instead.
    #[tokio::test]
    async fn content_addressed_immutability_gated_on_recorded_mtime() {
        let file_path = temp_path("file_mtime_gate");
        std::fs::write(&file_path, b"test").unwrap();
        let (item, mut file) = test_records(&file_path);

        // Fixture's stale last_modified (2024-01-01) never matches the fresh
        // temp file: drifted, bounded cache.
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            CACHE_DRIFTED
        );

        // Record the actual on-disk mtime the way the scanner does: match,
        // fully immutable.
        file.last_modified =
            format_system_time(std::fs::metadata(&file_path).unwrap().modified().unwrap()).unwrap();
        let response = file_response(
            &item,
            std::slice::from_ref(&file),
            "inline",
            &HeaderMap::new(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            CACHE_IMMUTABLE
        );
    }

    /// The item endpoint's outro fields are gated per index database: with
    /// `detect_outros` off both arrive null even though the record carries a
    /// detected boundary, and the positive control proves the values do
    /// travel when the toggle is on (the default).
    #[tokio::test]
    async fn outro_fields_gated_by_detect_outros() {
        let _env = crate::test_utils::test_data_dir();
        let file_path = temp_path("outro_gate");
        let (mut item, _file) = test_records(&file_path);
        item.outro_kind = Some("tiktok_card/1".to_string());
        item.content_end_ms = Some(8000);

        crate::test_utils::write_detect_outros_config("items-outro-on", true);
        let served = map_item_record("items-outro-on", &item).await;
        assert_eq!(served.outro_kind.as_deref(), Some("tiktok_card/1"));
        assert_eq!(served.content_end_ms, Some(8000));

        crate::test_utils::write_detect_outros_config("items-outro-off", false);
        let gated = map_item_record("items-outro-off", &item).await;
        assert_eq!(
            gated.outro_kind, None,
            "an already-detected verdict is withheld once the toggle is off"
        );
        assert_eq!(gated.content_end_ms, None);
        // Everything else is untouched by the gate.
        assert_eq!(gated.sha256, item.sha256);
        assert_eq!(gated.size, item.size);
    }

    /// The wiring, not the helper: `item_meta` must consult the request's
    /// **index** database config, not its user-data one. Both names are
    /// stamped with opposite toggles and the handler is run twice with them
    /// swapped, so reading the wrong name fails whichever way it is wrong.
    #[tokio::test]
    async fn item_meta_gates_on_the_index_db_config() {
        let _env = crate::test_utils::test_data_dir();
        crate::test_utils::write_detect_outros_config("items-handler-index-off", false);
        crate::test_utils::write_detect_outros_config("items-handler-user-on", true);
        crate::test_utils::write_detect_outros_config("items-handler-index-on", true);
        crate::test_utils::write_detect_outros_config("items-handler-user-off", false);

        /// The item's sha256, spelled in full so the lookup takes the exact
        /// match rather than the prefix-range branch.
        const SHA: &str = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";

        /// Runs the real handler over a throwaway database holding one item
        /// with a detected outro. The file row exists only because the
        /// metadata lookup joins it; it need not exist on disk.
        async fn serve(index_db: &str, user_data_db: &str) -> ItemRecordResponse {
            let mut dbs = crate::db::migrations::setup_test_databases().await;
            sqlx::query(
                r#"
                INSERT INTO items (
                    id, sha256, md5, type, duration, outro_kind, content_end_ms, time_added
                )
                VALUES (1, ?, 'md5_1', 'video/mp4', 12.0, 'tiktok_card/1', 8000,
                        '2024-01-01T00:00:00')
                "#,
            )
            .bind(SHA)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
            sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, ?, ?)")
                .bind("2024-01-01T00:00:00")
                .bind(r"C:\outro")
                .execute(&mut dbs.index_conn)
                .await
                .unwrap();
            sqlx::query(
                r#"
                INSERT INTO files (
                    id, sha256, item_id, path, filename, last_modified, scan_id, available
                )
                VALUES (10, ?, 1, 'C:\outro\a.mp4', 'a.mp4', '2024-01-02T00:00:00', 1, 1)
                "#,
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
            let db = DbConnection::<ReadOnlyNoUserData>::for_tests(
                index_conn,
                index_db,
                user_data_db,
            );
            item_meta(
                db,
                Query(ItemQuery {
                    id: SHA.to_string(),
                    id_type: ItemIdentifierType::Sha256,
                }),
            )
            .await
            .expect("item metadata")
            .0
            .item
        }

        let gated = serve("items-handler-index-off", "items-handler-user-on").await;
        assert_eq!(
            gated.outro_kind, None,
            "the index database's toggle is off, so nothing is served"
        );
        assert_eq!(gated.content_end_ms, None);

        let served = serve("items-handler-index-on", "items-handler-user-off").await;
        assert_eq!(served.outro_kind.as_deref(), Some("tiktok_card/1"));
        assert_eq!(served.content_end_ms, Some(8000));
    }

}
