use serde::Deserialize;
use sqlx::Row;
use std::path::PathBuf;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

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

pub(crate) struct ItemRecord {
    pub id: i64,
    pub sha256: String,
    pub md5: String,
    pub mime_type: String,
    pub size: Option<i64>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub duration: Option<f64>,
    pub audio_tracks: Option<i64>,
    pub video_tracks: Option<i64>,
    pub subtitle_tracks: Option<i64>,
    pub blurhash: Option<String>,
    pub time_added: String,
}

pub(crate) struct FileRecord {
    pub id: i64,
    pub sha256: String,
    pub path: String,
    pub last_modified: String,
    pub filename: String,
}

pub(crate) struct ItemMetadata {
    pub item: Option<ItemRecord>,
    pub files: Vec<FileRecord>,
}

pub(crate) async fn get_item_metadata(
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

pub(crate) async fn get_thumbnail_bytes(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    idx: i64,
) -> ApiResult<Option<Vec<u8>>> {
    let row = sqlx::query(
        r#"
        SELECT thumbnail
        FROM thumbnails
        WHERE item_sha256 = ? AND idx = ?
        "#,
    )
    .bind(sha256)
    .bind(idx)
    .fetch_optional(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read thumbnail bytes");
        ApiError::internal("Failed to load thumbnail")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let bytes: Vec<u8> = row.try_get("thumbnail").map_err(|err| {
        tracing::error!(error = %err, "failed to parse thumbnail bytes");
        ApiError::internal("Failed to load thumbnail")
    })?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;
    use std::{path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

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
}
