use axum::{
    Json,
    body::Body,
    extract::Query,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::io::ReaderStream;

use crate::api_error::ApiError;
use crate::db::{DbConnection, ReadOnly};
use crate::db::info::load_db_info;
use crate::db::items::{FileRecord, ItemIdentifierType, ItemRecord, get_item_metadata};

type ApiResult<T> = std::result::Result<T, ApiError>;

pub async fn db_info() -> impl IntoResponse {
    let info = match load_db_info() {
        Ok(info) => info,
        Err(err) => {
            tracing::error!(error = %err, "failed to load db info");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    Json(info).into_response()
}

#[derive(Deserialize)]
pub(crate) struct ItemQuery {
    id: String,
    id_type: ItemIdentifierType,
}

pub async fn item_file(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<ItemQuery>,
) -> ApiResult<Response> {
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

async fn file_response(
    item: &ItemRecord,
    file: &FileRecord,
    filename: &str,
    content_disposition_type: &str,
) -> ApiResult<Response> {
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

fn content_disposition_value(kind: &str, filename: &str) -> Option<header::HeaderValue> {
    let mut value = Vec::new();
    value.extend_from_slice(kind.as_bytes());
    value.extend_from_slice(b"; filename=\"");
    value.extend_from_slice(&latin1_bytes(filename));
    value.extend_from_slice(b"\"");
    header::HeaderValue::from_bytes(&value).ok()
}

fn latin1_bytes(value: &str) -> Vec<u8> {
    value
        .chars()
        .filter_map(|ch| {
            if (ch as u32) <= 0xFF {
                Some(ch as u8)
            } else {
                None
            }
        })
        .collect()
}

fn strip_non_latin1_chars(input: &str) -> String {
    input.chars().filter(|ch| (*ch as u32) <= 0xFF).collect()
}

fn iso_to_system_time(value: &str) -> Option<SystemTime> {
    let trimmed = value.trim_end_matches('Z');
    let mut parts = trimmed.split('T');
    let date = parts.next()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut date_parts = date.split('-');
    let year: i32 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour: u32 = time_parts.next()?.parse().ok()?;
    let minute: u32 = time_parts.next()?.parse().ok()?;
    let second: u32 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600)?
        .checked_add(i64::from(minute) * 60)?
        .checked_add(i64::from(second))?;

    if seconds < 0 {
        return None;
    }

    Some(UNIX_EPOCH + Duration::from_secs(seconds as u64))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut y = year;
    let m = month as i32;
    let d = day as i32;
    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146097 + doe - 719468) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

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
