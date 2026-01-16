use axum::{
    Json,
    body::Body,
    http::{Response, header},
};
use axum_extra::extract::Query;

use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio_util::io::ReaderStream;

use crate::api::utils::{content_disposition_value, iso_to_system_time, strip_non_latin1_chars};
use crate::api_error::ApiError;
use crate::db::items::{
    ExtractedTextRecord, FileRecord, ItemIdentifierType, ItemRecord, get_all_tags_for_item,
    get_extracted_text_for_item, get_item_metadata, get_text_by_ids, get_thumbnail_bytes,
};
use crate::db::{DbConnection, ReadOnly};

type ApiResult<T> = std::result::Result<T, ApiError>;

const PLACEHOLDER_PNG: &[u8] = include_bytes!("assets/placeholder.png");

#[derive(Deserialize)]
pub(crate) struct ItemQuery {
    id: String,
    id_type: ItemIdentifierType,
}

#[derive(Deserialize)]
pub(crate) struct ItemTextQuery {
    id: String,
    id_type: ItemIdentifierType,
    #[serde(default)]
    setters: Vec<String>,
    #[serde(default)]
    languages: Vec<String>,
    truncate_length: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct ItemTagsQuery {
    id: String,
    id_type: ItemIdentifierType,
    #[serde(default)]
    setters: Vec<String>,
    #[serde(default)]
    namespaces: Vec<String>,
    #[serde(default)]
    confidence_threshold: f64,
    limit_per_namespace: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct TextAnyQuery {
    text_ids: Vec<i64>,
}

#[derive(Serialize)]
pub(crate) struct ItemMetadataResponse {
    item: ItemRecordResponse,
    files: Vec<FileRecordResponse>,
}

#[derive(Serialize)]
pub(crate) struct ItemRecordResponse {
    id: i64,
    sha256: String,
    md5: String,
    #[serde(rename = "type")]
    item_type: String,
    size: Option<i64>,
    width: Option<i64>,
    height: Option<i64>,
    duration: Option<f64>,
    audio_tracks: Option<i64>,
    video_tracks: Option<i64>,
    subtitle_tracks: Option<i64>,
    blurhash: Option<String>,
    time_added: String,
}

#[derive(Serialize)]
pub(crate) struct FileRecordResponse {
    id: i64,
    sha256: String,
    path: String,
    last_modified: String,
    filename: String,
}

#[derive(Serialize)]
pub(crate) struct TextResponse {
    text: Vec<ExtractedTextRecord>,
}

#[derive(Serialize)]
pub(crate) struct TagResponse {
    tags: Vec<(String, String, f64, String)>,
}

#[derive(Deserialize)]
pub(crate) struct ThumbnailQuery {
    id: String,
    id_type: ItemIdentifierType,
    #[serde(default = "default_true")]
    big: bool,
}

pub async fn item_file(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemQuery>,
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

    file_response(&item, file, &filename, "inline").await
}

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

pub async fn texts_any(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TextAnyQuery>,
) -> ApiResult<Json<TextResponse>> {
    let text = get_text_by_ids(&mut db.conn, &query.text_ids).await?;
    Ok(Json(TextResponse { text }))
}

pub async fn item_thumbnail(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ThumbnailQuery>,
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
) -> ApiResult<Response<Body>> {
    let mime = item.mime_type.as_str();
    if mime.is_empty() || mime.starts_with("image/gif") {
        return file_response(item, file, original_filename, "inline").await;
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
        return file_response(item, file, original_filename, "inline").await;
    }

    let filename = format!("{original_filename_no_ext}.png");
    bytes_response(PLACEHOLDER_PNG.to_vec(), "image/png", &filename)
}

async fn file_response(
    item: &ItemRecord,
    file: &FileRecord,
    filename: &str,
    content_disposition_type: &str,
) -> ApiResult<Response<Body>> {
    let size = item
        .size
        .ok_or_else(|| ApiError::internal("Failed to get item"))?;
    let file_handle = tokio::fs::File::open(&file.path).await.map_err(|err| {
        tracing::error!(error = %err, "failed to open file");
        ApiError::not_found("No file found for item")
    })?;
    let stream = ReaderStream::new(file_handle);
    let body = Body::from_stream(stream);
    let mut response = Response::new(body);
    let headers = response.headers_mut();

    if let Ok(value) = header::HeaderValue::from_str(&item.mime_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&size.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Some(last_modified) = iso_to_system_time(&file.last_modified) {
        let formatted = httpdate::fmt_http_date(last_modified);
        if let Ok(value) = header::HeaderValue::from_str(&formatted) {
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

    // Ensures file responses include content headers derived from item metadata.
    #[tokio::test]
    async fn file_response_sets_headers() {
        let file_path = temp_path("file_response");
        std::fs::write(&file_path, b"test").unwrap();

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

        let response = file_response(&item, &file, "file.png", "inline")
            .await
            .unwrap();
        let headers = response.headers();

        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "image/png");
        assert_eq!(headers.get(header::CONTENT_LENGTH).unwrap(), "4");
        assert_eq!(
            headers.get(header::CONTENT_DISPOSITION).unwrap(),
            "inline; filename=\"file.png\""
        );
        assert!(headers.get(header::LAST_MODIFIED).is_some());
    }
}
