use axum::{
    Json,
    body::Body,
    http::{HeaderMap, Response, StatusCode, header},
};
use axum_extra::extract::Query;

use serde::{Deserialize, Serialize};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api::utils::{content_disposition_value, iso_to_system_time, strip_non_latin1_chars};
use crate::api_error::ApiError;
use crate::db::items::{
    ExtractedTextRecord, FileRecord, ItemIdentifierType, ItemRecord, get_all_tags_for_item,
    get_extracted_text_for_item, get_item_metadata, get_text_by_ids, get_thumbnail_bytes,
};
use crate::db::{DbConnection, ReadOnly};

type ApiResult<T> = std::result::Result<T, ApiError>;

const PLACEHOLDER_PNG: &[u8] = include_bytes!("assets/placeholder.png");

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
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let file = &item_data.files[0];
    let mut filename = strip_non_latin1_chars(&file.filename);
    if filename.is_empty() {
        filename = file.filename.clone();
    }

    file_response(&item, file, &filename, "inline", &request_headers).await
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
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemQuery>,
) -> ApiResult<Json<ItemMetadataResponse>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    let response = ItemMetadataResponse {
        item: map_item_record(&item),
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
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemTextQuery>,
) -> ApiResult<Json<TextResponse>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
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
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemTagsQuery>,
) -> ApiResult<Json<TagResponse>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
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
    mut db: DbConnection<ReadOnly>,
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
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ThumbnailQuery>,
    request_headers: HeaderMap,
) -> ApiResult<Response<Body>> {
    let item_data = get_item_metadata(&mut db.conn, &query.id, query.id_type).await?;
    let Some(item) = item_data.item else {
        return Err(ApiError::not_found("Item not found"));
    };

    if item_data.files.is_empty() {
        return Err(ApiError::not_found("No file found for item"));
    }

    let file = &item_data.files[0];
    let mut original_filename = strip_non_latin1_chars(&file.filename);
    if original_filename.is_empty() {
        original_filename = file.filename.clone();
    }
    let original_filename_no_ext = Path::new(&original_filename)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or(&original_filename);

    match thumbnail_response(
        &mut db.conn,
        &item,
        file,
        &original_filename,
        original_filename_no_ext,
        query.big,
        &request_headers,
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

async fn thumbnail_response(
    conn: &mut sqlx::SqliteConnection,
    item: &ItemRecord,
    file: &FileRecord,
    original_filename: &str,
    original_filename_no_ext: &str,
    big: bool,
    request_headers: &HeaderMap,
) -> ApiResult<Response<Body>> {
    let mime = item.mime_type.as_str();
    if mime.is_empty() || mime.starts_with("image/gif") {
        return file_response(item, file, original_filename, "inline", request_headers).await;
    }

    let index = if mime.starts_with("video") {
        if big { 0 } else { 1 }
    } else {
        0
    };

    if let Some(buffer) = get_thumbnail_bytes(conn, &file.sha256, index).await? {
        let filename = format!("{original_filename_no_ext}.jpg");
        return bytes_response(buffer, "image/jpeg", &filename);
    }

    if mime.starts_with("image") {
        return file_response(item, file, original_filename, "inline", request_headers).await;
    }

    let filename = format!("{original_filename_no_ext}.png");
    bytes_response(PLACEHOLDER_PNG.to_vec(), "image/png", &filename)
}

#[derive(Debug, PartialEq, Eq)]
enum RangeOutcome {
    /// No usable Range header: serve the whole file with 200.
    Full,
    /// A single satisfiable byte range (inclusive): serve 206.
    Partial { start: u64, end: u64 },
    /// Range header was valid but no requested range overlaps the file: 416.
    Unsatisfiable,
}

/// Parses a `Range` request header against a resource of `size` bytes.
///
/// Handles all RFC 9110 byte-range forms: `start-end`, `start-`, and the
/// suffix form `-N`. Out-of-bounds ends are clamped. Multiple ranges are
/// accepted syntactically, but if more than one is satisfiable the header is
/// ignored (full 200) rather than answered with multipart/byteranges, which
/// RFC 9110 permits. Malformed headers are ignored entirely.
fn parse_range_header(value: &str, size: u64) -> RangeOutcome {
    let trimmed = value.trim();
    let Some(specs) = trimmed
        .get(..6)
        .filter(|prefix| prefix.eq_ignore_ascii_case("bytes="))
        .map(|_| &trimmed[6..])
    else {
        return RangeOutcome::Full;
    };

    let mut satisfiable = Vec::new();
    let mut any_valid = false;
    for spec in specs.split(',') {
        let spec = spec.trim();
        if spec.is_empty() {
            continue;
        }
        let Some((start_str, end_str)) = spec.split_once('-') else {
            return RangeOutcome::Full;
        };
        let start_str = start_str.trim();
        let end_str = end_str.trim();
        if start_str.is_empty() {
            // Suffix form: last N bytes.
            let Ok(suffix) = end_str.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            any_valid = true;
            if suffix == 0 || size == 0 {
                continue;
            }
            satisfiable.push((size.saturating_sub(suffix), size - 1));
        } else {
            let Ok(start) = start_str.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            let end = if end_str.is_empty() {
                size.checked_sub(1)
            } else {
                let Ok(end) = end_str.parse::<u64>() else {
                    return RangeOutcome::Full;
                };
                if end < start {
                    return RangeOutcome::Full;
                }
                size.checked_sub(1).map(|last| end.min(last))
            };
            any_valid = true;
            match end {
                Some(end) if start <= end => satisfiable.push((start, end)),
                _ => {}
            }
        }
    }

    match satisfiable.as_slice() {
        [(start, end)] => RangeOutcome::Partial {
            start: *start,
            end: *end,
        },
        [] if any_valid => RangeOutcome::Unsatisfiable,
        _ => RangeOutcome::Full,
    }
}

async fn file_response(
    item: &ItemRecord,
    file: &FileRecord,
    filename: &str,
    content_disposition_type: &str,
    request_headers: &HeaderMap,
) -> ApiResult<Response<Body>> {
    let mut file_handle = tokio::fs::File::open(&file.path).await.map_err(|err| {
        tracing::error!(error = %err, "failed to open file");
        ApiError::not_found("No file found for item")
    })?;
    // The size on disk is authoritative for range math; the DB value can be
    // stale if the file changed since the last scan.
    let size = file_handle
        .metadata()
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read file metadata");
            ApiError::internal("Failed to read file metadata")
        })?
        .len();

    let last_modified = iso_to_system_time(&file.last_modified).map(httpdate::fmt_http_date);

    let mut range = request_headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(|value| parse_range_header(value, size))
        .unwrap_or(RangeOutcome::Full);

    // If-Range: only honor the range when the validator still matches;
    // otherwise the client's partial state is stale and it needs the full
    // body. We emit no ETag, so Last-Modified is the only validator.
    if range != RangeOutcome::Full {
        if let Some(if_range) = request_headers
            .get(header::IF_RANGE)
            .and_then(|value| value.to_str().ok())
        {
            if last_modified.as_deref() != Some(if_range.trim()) {
                range = RangeOutcome::Full;
            }
        }
    }

    let (status, body, content_length, content_range) = match range {
        RangeOutcome::Full => (
            StatusCode::OK,
            Body::from_stream(ReaderStream::new(file_handle)),
            size,
            None,
        ),
        RangeOutcome::Partial { start, end } => {
            file_handle
                .seek(SeekFrom::Start(start))
                .await
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to seek file");
                    ApiError::internal("Failed to read file")
                })?;
            let length = end - start + 1;
            (
                StatusCode::PARTIAL_CONTENT,
                Body::from_stream(ReaderStream::new(file_handle.take(length))),
                length,
                Some(format!("bytes {start}-{end}/{size}")),
            )
        }
        RangeOutcome::Unsatisfiable => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            Body::empty(),
            0,
            Some(format!("bytes */{size}")),
        ),
    };

    let mut response = Response::new(body);
    *response.status_mut() = status;
    let headers = response.headers_mut();

    headers.insert(
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    );
    if let Ok(value) = header::HeaderValue::from_str(&item.mime_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&content_length.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Some(content_range) = content_range {
        if let Ok(value) = header::HeaderValue::from_str(&content_range) {
            headers.insert(header::CONTENT_RANGE, value);
        }
    }
    if let Some(last_modified) = &last_modified {
        if let Ok(value) = header::HeaderValue::from_str(last_modified) {
            headers.insert(header::LAST_MODIFIED, value);
        }
    }

    if let Some(value) = content_disposition_value(content_disposition_type, filename) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }

    Ok(response)
}

fn bytes_response(bytes: Vec<u8>, media_type: &str, filename: &str) -> ApiResult<Response<Body>> {
    let len = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    let headers = response.headers_mut();

    if let Ok(value) = header::HeaderValue::from_str(media_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&len.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Some(value) = content_disposition_value("inline", filename) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }

    Ok(response)
}

fn map_item_record(item: &ItemRecord) -> ItemRecordResponse {
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

        let response = file_response(&item, &file, "file.png", "inline", &HeaderMap::new())
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
        let response = file_response(&item, &file, "file.png", "inline", &request_headers)
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
        let response = file_response(&item, &file, "file.png", "inline", &request_headers)
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
        let response = file_response(&item, &file, "file.png", "inline", &request_headers)
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
        let response = file_response(&item, &file, "file.png", "inline", &request_headers)
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
        let response = file_response(&item, &file, "file.png", "inline", &request_headers)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"0123456789");
    }

    #[test]
    fn parse_range_header_cases() {
        use RangeOutcome::*;

        // Basic forms.
        assert_eq!(
            parse_range_header("bytes=0-499", 1000),
            Partial { start: 0, end: 499 }
        );
        assert_eq!(
            parse_range_header("bytes=500-", 1000),
            Partial {
                start: 500,
                end: 999
            }
        );
        assert_eq!(
            parse_range_header("bytes=-300", 1000),
            Partial {
                start: 700,
                end: 999
            }
        );
        // End clamped to the last byte; suffix longer than the file covers it all.
        assert_eq!(
            parse_range_header("bytes=990-2000", 1000),
            Partial {
                start: 990,
                end: 999
            }
        );
        assert_eq!(
            parse_range_header("bytes=-5000", 1000),
            Partial { start: 0, end: 999 }
        );
        // Whitespace and case tolerance.
        assert_eq!(
            parse_range_header(" BYTES= 0 - 4 ", 1000),
            Partial { start: 0, end: 4 }
        );
        // Unsatisfiable: beyond EOF, zero suffix, empty file.
        assert_eq!(parse_range_header("bytes=1000-", 1000), Unsatisfiable);
        assert_eq!(parse_range_header("bytes=-0", 1000), Unsatisfiable);
        assert_eq!(parse_range_header("bytes=0-", 0), Unsatisfiable);
        // Ignored: other units, malformed specs, inverted ranges,
        // multiple satisfiable ranges (no multipart support).
        assert_eq!(parse_range_header("items=0-4", 1000), Full);
        assert_eq!(parse_range_header("bytes=abc", 1000), Full);
        assert_eq!(parse_range_header("bytes=5-2", 1000), Full);
        assert_eq!(parse_range_header("bytes=0-4,10-14", 1000), Full);
        // One satisfiable range among unsatisfiable ones is still served.
        assert_eq!(
            parse_range_header("bytes=2000-,0-4", 1000),
            Partial { start: 0, end: 4 }
        );
    }
}
