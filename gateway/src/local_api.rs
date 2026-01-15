use anyhow::{Context, Result as AnyResult};
use axum::{
    Json,
    body::Body,
    extract::Query,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::Row;
use std::{
    env, fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::io::ReaderStream;

use crate::api_error::ApiError;
use crate::db::{DbConnection, ReadOnly};
use crate::policy::{DbInfo, SingleDbInfo};

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

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ItemIdentifierType {
    ItemId,
    FileId,
    DataId,
    Path,
    Sha256,
    Md5,
}

struct ItemRecord {
    id: i64,
    sha256: String,
    md5: String,
    mime_type: String,
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

struct FileRecord {
    id: i64,
    sha256: String,
    path: String,
    last_modified: String,
    filename: String,
}

struct ItemMetadata {
    item: Option<ItemRecord>,
    files: Vec<FileRecord>,
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

async fn get_item_metadata(
    conn: &mut sqlx::SqliteConnection,
    identifier: &str,
    identifier_type: ItemIdentifierType,
) -> ApiResult<ItemMetadata> {
    let select = r#"
    SELECT
        items.id AS item_id,
        items.sha256 AS sha256,
        items.md5 AS md5,
        items.type AS item_type,
        items.size AS size,
        items.width AS width,
        items.height AS height,
        items.duration AS duration,
        items.audio_tracks AS audio_tracks,
        items.video_tracks AS video_tracks,
        items.subtitle_tracks AS subtitle_tracks,
        items.blurhash AS blurhash,
        items.time_added AS time_added,
        files.id AS file_id,
        files.path AS path,
        files.filename AS filename,
        files.last_modified AS last_modified
    FROM items
        JOIN files ON items.id = files.item_id
    "#;

    let (query, value) = match identifier_type {
        ItemIdentifierType::Sha256 if identifier.len() < 64 => (
            format!(
                "{select}
        WHERE items.sha256 LIKE ? || '%'
        ORDER BY files.available DESC
        "
            ),
            identifier,
        ),
        ItemIdentifierType::DataId => (
            format!(
                "{select}
        JOIN item_data ON items.id = item_data.item_id
        WHERE item_data.id = ?
        ORDER BY files.available DESC
        "
            ),
            identifier,
        ),
        _ => {
            let column = match identifier_type {
                ItemIdentifierType::Sha256 => "items.sha256",
                ItemIdentifierType::ItemId => "item_id",
                ItemIdentifierType::FileId => "file_id",
                ItemIdentifierType::DataId => "data_id",
                ItemIdentifierType::Path => "path",
                ItemIdentifierType::Md5 => "md5",
            };
            (
                format!(
                    "{select}
        WHERE {column} = ?
        ORDER BY files.available DESC
        "
                ),
                identifier,
            )
        }
    };

    let rows = sqlx::query(&query)
        .bind(value)
        .fetch_all(conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to query item metadata");
            ApiError::internal("Failed to get item")
        })?;

    let mut item_record = None;
    let mut files = Vec::new();

    for row in rows {
        let item_id: i64 = row.try_get("item_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read item id");
            ApiError::internal("Failed to get item")
        })?;
        let sha256: String = row.try_get("sha256").map_err(|err| {
            tracing::error!(error = %err, "failed to read sha256");
            ApiError::internal("Failed to get item")
        })?;
        let md5: String = row.try_get("md5").map_err(|err| {
            tracing::error!(error = %err, "failed to read md5");
            ApiError::internal("Failed to get item")
        })?;
        let mime_type: String = row.try_get("item_type").map_err(|err| {
            tracing::error!(error = %err, "failed to read mime type");
            ApiError::internal("Failed to get item")
        })?;
        let size: Option<i64> = row.try_get("size").map_err(|err| {
            tracing::error!(error = %err, "failed to read size");
            ApiError::internal("Failed to get item")
        })?;
        let width: Option<i64> = row.try_get("width").map_err(|err| {
            tracing::error!(error = %err, "failed to read width");
            ApiError::internal("Failed to get item")
        })?;
        let height: Option<i64> = row.try_get("height").map_err(|err| {
            tracing::error!(error = %err, "failed to read height");
            ApiError::internal("Failed to get item")
        })?;
        let duration: Option<f64> = row.try_get("duration").map_err(|err| {
            tracing::error!(error = %err, "failed to read duration");
            ApiError::internal("Failed to get item")
        })?;
        let audio_tracks: Option<i64> = row.try_get("audio_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read audio tracks");
            ApiError::internal("Failed to get item")
        })?;
        let video_tracks: Option<i64> = row.try_get("video_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read video tracks");
            ApiError::internal("Failed to get item")
        })?;
        let subtitle_tracks: Option<i64> = row.try_get("subtitle_tracks").map_err(|err| {
            tracing::error!(error = %err, "failed to read subtitle tracks");
            ApiError::internal("Failed to get item")
        })?;
        let blurhash: Option<String> = row.try_get("blurhash").map_err(|err| {
            tracing::error!(error = %err, "failed to read blurhash");
            ApiError::internal("Failed to get item")
        })?;
        let time_added: String = row.try_get("time_added").map_err(|err| {
            tracing::error!(error = %err, "failed to read time_added");
            ApiError::internal("Failed to get item")
        })?;
        let file_id: i64 = row.try_get("file_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read file id");
            ApiError::internal("Failed to get item")
        })?;
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read path");
            ApiError::internal("Failed to get item")
        })?;
        let filename: String = row.try_get("filename").map_err(|err| {
            tracing::error!(error = %err, "failed to read filename");
            ApiError::internal("Failed to get item")
        })?;
        let last_modified: String = row.try_get("last_modified").map_err(|err| {
            tracing::error!(error = %err, "failed to read last modified");
            ApiError::internal("Failed to get item")
        })?;

        if item_record.is_none() {
            item_record = Some(ItemRecord {
                id: item_id,
                sha256: sha256.clone(),
                md5,
                mime_type,
                size,
                width,
                height,
                duration,
                audio_tracks,
                video_tracks,
                subtitle_tracks,
                blurhash,
                time_added,
            });
        }

        if PathBuf::from(&path).exists() {
            files.push(FileRecord {
                id: file_id,
                sha256,
                path,
                last_modified,
                filename,
            });
        }
    }

    Ok(ItemMetadata {
        item: item_record,
        files,
    })
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
    use sqlx::Connection;

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("panoptikon_{label}_{stamp}"))
    }

    // Ensures item metadata queries return the item plus only existing file paths.
    #[tokio::test]
    async fn item_metadata_returns_existing_file() {
        let file_path = temp_path("item_meta_file");
        std::fs::write(&file_path, b"test").unwrap();

        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                sha256 TEXT NOT NULL,
                md5 TEXT NOT NULL,
                type TEXT NOT NULL,
                size INTEGER,
                width INTEGER,
                height INTEGER,
                duration REAL,
                audio_tracks INTEGER,
                video_tracks INTEGER,
                subtitle_tracks INTEGER,
                blurhash TEXT,
                time_added TEXT NOT NULL
            )
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                sha256 TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                filename TEXT NOT NULL,
                last_modified TEXT NOT NULL,
                available BOOLEAN NOT NULL
            )
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO items (
                id, sha256, md5, type, size, width, height, duration,
                audio_tracks, video_tracks, subtitle_tracks, blurhash, time_added
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(1_i64)
        .bind("sha256")
        .bind("md5")
        .bind("image/png")
        .bind(4_i64)
        .bind(10_i64)
        .bind(20_i64)
        .bind(0.0_f64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(Option::<String>::None)
        .bind("2024-01-01T00:00:00")
        .execute(&mut conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, available
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(10_i64)
        .bind("sha256")
        .bind(1_i64)
        .bind(file_path.to_string_lossy().to_string())
        .bind("file.png")
        .bind("2024-01-01T00:00:00")
        .bind(1_i64)
        .execute(&mut conn)
        .await
        .unwrap();

        let result = get_item_metadata(&mut conn, "1", ItemIdentifierType::ItemId)
            .await
            .unwrap();

        assert!(result.item.is_some());
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].path, file_path.to_string_lossy());
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

fn load_db_info() -> AnyResult<DbInfo> {
    let (index_default, user_default) = db_defaults();
    let (index_dbs, user_data_dbs) = db_lists()?;
    Ok(DbInfo {
        index: SingleDbInfo {
            current: index_default,
            all: index_dbs,
        },
        user_data: SingleDbInfo {
            current: user_default,
            all: user_data_dbs,
        },
    })
}

fn db_defaults() -> (String, String) {
    let index_default = env::var("INDEX_DB").unwrap_or_else(|_| "default".to_string());
    let user_default = env::var("USER_DATA_DB").unwrap_or_else(|_| "default".to_string());
    (index_default, user_default)
}

fn db_lists() -> AnyResult<(Vec<String>, Vec<String>)> {
    let data_dir = PathBuf::from(env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string()));
    let index_dir = data_dir.join("index");
    let user_data_dir = data_dir.join("user_data");

    fs::create_dir_all(&index_dir)
        .with_context(|| format!("failed to create index dir {}", index_dir.display()))?;
    fs::create_dir_all(&user_data_dir)
        .with_context(|| format!("failed to create user data dir {}", user_data_dir.display()))?;

    let mut index_dbs = Vec::new();
    for entry in fs::read_dir(&index_dir)
        .with_context(|| format!("failed to read index dir {}", index_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() && path.join("index.db").exists() {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                index_dbs.push(name.to_string());
            }
        }
    }

    let mut user_data_dbs = Vec::new();
    for entry in fs::read_dir(&user_data_dir)
        .with_context(|| format!("failed to read user data dir {}", user_data_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
            if let Some(stem) = path.file_stem().and_then(|name| name.to_str()) {
                user_data_dbs.push(stem.to_string());
            }
        }
    }

    Ok((index_dbs, user_data_dbs))
}
