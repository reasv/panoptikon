use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Clone)]
pub(crate) struct ItemScanMeta {
    pub md5: String,
    pub mime_type: String,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub duration: Option<f64>,
    pub audio_tracks: Option<i64>,
    pub video_tracks: Option<i64>,
    pub subtitle_tracks: Option<i64>,
}

#[derive(Clone)]
pub(crate) struct FileScanData {
    pub sha256: String,
    pub last_modified: String,
    pub path: String,
    pub new_file_hash: bool,
    pub file_size: Option<i64>,
    pub item_metadata: Option<ItemScanMeta>,
    pub blurhash: Option<String>,
}

pub(crate) struct FilePathRecord {
    pub id: i64,
    pub sha256: String,
    pub last_modified: String,
}

pub(crate) struct FileDeleteInfo {
    pub item_id: i64,
    pub scan_id: i64,
    pub sha256: String,
}

pub(crate) struct FileUpsertResult {
    pub item_inserted: bool,
    pub file_updated: bool,
    pub file_deleted: bool,
    pub file_inserted: bool,
}

pub(crate) async fn get_file_by_path(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<FilePathRecord>> {
    let row = sqlx::query(
        r#"
SELECT files.id AS file_id, files.sha256 AS sha256, files.last_modified AS last_modified
FROM files
JOIN items ON files.sha256 = items.sha256
WHERE files.path = ?1
LIMIT 1
        "#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query file by path");
        ApiError::internal("Failed to query file")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    let id: i64 = row.try_get("file_id").map_err(|err| {
        tracing::error!(error = %err, "failed to read file id");
        ApiError::internal("Failed to query file")
    })?;
    let sha256: String = row.try_get("sha256").map_err(|err| {
        tracing::error!(error = %err, "failed to read file sha256");
        ApiError::internal("Failed to query file")
    })?;
    let last_modified: String = row.try_get("last_modified").map_err(|err| {
        tracing::error!(error = %err, "failed to read file last_modified");
        ApiError::internal("Failed to query file")
    })?;

    Ok(Some(FilePathRecord {
        id,
        sha256,
        last_modified,
    }))
}

pub(crate) async fn get_file_delete_info(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<FileDeleteInfo>> {
    let row = sqlx::query(
        r#"
SELECT files.item_id AS item_id, files.scan_id AS scan_id, files.sha256 AS sha256
FROM files
WHERE files.path = ?1
LIMIT 1
        "#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query file delete info");
        ApiError::internal("Failed to query file")
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    let item_id: i64 = row.try_get("item_id").map_err(|err| {
        tracing::error!(error = %err, "failed to read file item_id");
        ApiError::internal("Failed to query file")
    })?;
    let scan_id: i64 = row.try_get("scan_id").map_err(|err| {
        tracing::error!(error = %err, "failed to read file scan_id");
        ApiError::internal("Failed to query file")
    })?;
    let sha256: String = row.try_get("sha256").map_err(|err| {
        tracing::error!(error = %err, "failed to read file sha256");
        ApiError::internal("Failed to query file")
    })?;

    Ok(Some(FileDeleteInfo {
        item_id,
        scan_id,
        sha256,
    }))
}

pub(crate) async fn count_files_for_item(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
) -> ApiResult<i64> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files WHERE item_id = ?1")
        .bind(item_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, item_id, "failed to count files for item");
            ApiError::internal("Failed to query file")
        })?;
    Ok(row.0)
}

pub(crate) async fn delete_file_by_path(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<u64> {
    let result = sqlx::query("DELETE FROM files WHERE path = ?1")
        .bind(path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %path, "failed to delete file path");
            ApiError::internal("Failed to delete file")
        })?;
    Ok(result.rows_affected())
}

pub(crate) async fn delete_item_if_orphan(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
DELETE FROM items
WHERE id = ?1
  AND NOT EXISTS (SELECT 1 FROM files WHERE item_id = ?1)
        "#,
    )
    .bind(item_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, item_id, "failed to delete orphan item");
        ApiError::internal("Failed to delete orphan item")
    })?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn rename_file_path(
    conn: &mut sqlx::SqliteConnection,
    old_path: &str,
    new_path: &str,
    scan_id: i64,
    last_modified: &str,
) -> ApiResult<bool> {
    let filename = std::path::Path::new(new_path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let result = sqlx::query(
        r#"
UPDATE files
SET path = ?1,
    filename = ?2,
    scan_id = ?3,
    available = TRUE,
    last_modified = ?4
WHERE path = ?5
        "#,
    )
    .bind(new_path)
    .bind(filename)
    .bind(scan_id)
    .bind(last_modified)
    .bind(old_path)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to rename file path");
        ApiError::internal("Failed to update file")
    })?;
    Ok(result.rows_affected() > 0)
}

pub(crate) async fn update_item_size(
    conn: &mut sqlx::SqliteConnection,
    item_id: i64,
    size: i64,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
UPDATE items
SET size = ?1
WHERE id = ?2
        "#,
    )
    .bind(size)
    .bind(item_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, item_id, "failed to update item size");
        ApiError::internal("Failed to update item")
    })?;

    Ok(result.rows_affected() > 0)
}

pub(crate) async fn get_item_id(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as("SELECT id FROM items WHERE sha256 = ?1")
        .bind(sha256)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to query item id");
            ApiError::internal("Failed to update file")
        })?;
    Ok(row.map(|(id,)| id))
}

pub(crate) async fn has_blurhash(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
) -> ApiResult<bool> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT blurhash FROM items WHERE sha256 = ?1")
            .bind(sha256)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to read blurhash");
                ApiError::internal("Failed to load blurhash")
            })?;

    Ok(row.and_then(|(value,)| value).is_some())
}

pub(crate) async fn set_blurhash(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    blurhash: &str,
) -> ApiResult<()> {
    sqlx::query("UPDATE items SET blurhash = ?1 WHERE sha256 = ?2")
        .bind(blurhash)
        .bind(sha256)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to update blurhash");
            ApiError::internal("Failed to update blurhash")
        })?;
    Ok(())
}

pub(crate) async fn update_file_data(
    conn: &mut sqlx::SqliteConnection,
    time_added: &str,
    scan_id: i64,
    data: &FileScanData,
) -> ApiResult<FileUpsertResult> {
    let mut item_id = get_item_id(conn, &data.sha256).await?;
    let mut item_inserted = false;

    if let Some(meta) = &data.item_metadata {
        if item_id.is_none() {
            let result = sqlx::query(
                r#"
INSERT INTO items (
    sha256,
    md5,
    type,
    size,
    time_added,
    width,
    height,
    duration,
    audio_tracks,
    video_tracks,
    subtitle_tracks,
    blurhash
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                "#,
            )
            .bind(&data.sha256)
            .bind(&meta.md5)
            .bind(&meta.mime_type)
            .bind(data.file_size)
            .bind(time_added)
            .bind(meta.width)
            .bind(meta.height)
            .bind(meta.duration)
            .bind(meta.audio_tracks)
            .bind(meta.video_tracks)
            .bind(meta.subtitle_tracks)
            .bind(&data.blurhash)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, sha256 = %data.sha256, "failed to insert item");
                ApiError::internal("Failed to update file")
            })?;

            let inserted_id = result.last_insert_rowid();
            item_id = Some(inserted_id);
            item_inserted = true;
        }
    }

    let item_id = item_id.ok_or_else(|| {
        tracing::error!(sha256 = %data.sha256, "item not found for file update");
        ApiError::internal("Failed to update file")
    })?;

    if let Some(size) = data.file_size {
        let _ = update_item_size(conn, item_id, size).await?;
    }

    if !data.new_file_hash {
        let result = sqlx::query(
            r#"
UPDATE files
SET scan_id = ?1, available = TRUE, last_modified = ?2
WHERE path = ?3
            "#,
        )
        .bind(scan_id)
        .bind(&data.last_modified)
        .bind(&data.path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, scan_id, path = %data.path, "failed to update existing file");
            ApiError::internal("Failed to update file")
        })?;

        return Ok(FileUpsertResult {
            item_inserted,
            file_updated: result.rows_affected() > 0,
            file_deleted: false,
            file_inserted: false,
        });
    }

    let delete_result = sqlx::query("DELETE FROM files WHERE path = ?1")
        .bind(&data.path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %data.path, "failed to delete existing file path");
            ApiError::internal("Failed to update file")
        })?;
    let file_deleted = delete_result.rows_affected() > 0;

    let filename = std::path::Path::new(&data.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();

    let insert_result = sqlx::query(
        r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, TRUE)
        "#,
    )
    .bind(&data.sha256)
    .bind(item_id)
    .bind(&data.path)
    .bind(&filename)
    .bind(&data.last_modified)
    .bind(scan_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, sha256 = %data.sha256, "failed to insert file row");
        ApiError::internal("Failed to update file")
    })?;

    Ok(FileUpsertResult {
        item_inserted,
        file_updated: false,
        file_deleted,
        file_inserted: insert_result.rows_affected() > 0,
    })
}

pub(crate) async fn delete_items_without_files(
    conn: &mut sqlx::SqliteConnection,
    batch_size: i64,
) -> ApiResult<u64> {
    let mut total_deleted = 0_u64;
    loop {
        let result = sqlx::query(
            r#"
DELETE FROM items
WHERE rowid IN (
    SELECT items.id
    FROM items
    LEFT JOIN files ON files.item_id = items.id
    WHERE files.id IS NULL
    LIMIT ?1
)
            "#,
        )
        .bind(batch_size)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to delete items without files");
            ApiError::internal("Failed to delete orphan items")
        })?;

        let deleted = result.rows_affected();
        total_deleted += deleted;
        if deleted == 0 {
            break;
        }
    }

    Ok(total_deleted)
}

pub(crate) async fn delete_files_not_allowed_stub(
    _conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    // TODO: Implement PQL-based job_filters handling.
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::file_scans::add_file_scan;
    use crate::db::migrations::setup_test_databases;

    // Ensures file lookups return basic path metadata.
    #[tokio::test]
    async fn get_file_by_path_returns_row() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let record = get_file_by_path(&mut dbs.index_conn, r"C:\data\one.png")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(record.sha256, "sha_one");
        assert_eq!(record.last_modified, "2024-01-01T00:00:00");
    }

    // Ensures update_file_data inserts items and files when new data arrives.
    #[tokio::test]
    async fn update_file_data_inserts_item_and_file() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();

        let result = update_file_data(
            &mut dbs.index_conn,
            "2024-01-01T00:00:00",
            scan_id,
            &FileScanData {
                sha256: "sha_one".to_string(),
                last_modified: "2024-01-01T00:00:00".to_string(),
                path: r"C:\data\one.png".to_string(),
                new_file_hash: true,
                file_size: Some(12),
                item_metadata: Some(ItemScanMeta {
                    md5: "md5_one".to_string(),
                    mime_type: "image/png".to_string(),
                    width: Some(10),
                    height: Some(20),
                    duration: None,
                    audio_tracks: None,
                    video_tracks: None,
                    subtitle_tracks: None,
                }),
                blurhash: Some("bh".to_string()),
            },
        )
        .await
        .unwrap();

        assert!(result.item_inserted);
        assert!(result.file_inserted);

        let item_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM items WHERE sha256 = 'sha_one'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(item_row.0, 1);

        let file_row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM files WHERE path = 'C:\\data\\one.png'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(file_row.0, 1);
    }

    // Ensures unchanged files update scan_id and last_modified without reinserting.
    #[tokio::test]
    async fn update_file_data_updates_existing_path_when_hash_unchanged() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', 1, 0)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let result = update_file_data(
            &mut dbs.index_conn,
            "2024-01-01T00:00:00",
            scan_id,
            &FileScanData {
                sha256: "sha_one".to_string(),
                last_modified: "2024-01-01T00:02:00".to_string(),
                path: r"C:\data\one.png".to_string(),
                new_file_hash: false,
                file_size: None,
                item_metadata: None,
                blurhash: None,
            },
        )
        .await
        .unwrap();

        assert!(result.file_updated);
        assert!(!result.file_deleted);
        assert!(!result.file_inserted);

        let row: (i64, String) =
            sqlx::query_as("SELECT scan_id, last_modified FROM files WHERE path = 'C:\\data\\one.png'")
                .fetch_one(&mut dbs.index_conn)
                .await
                .unwrap();
        assert_eq!(row.0, scan_id);
        assert_eq!(row.1, "2024-01-01T00:02:00");
    }

    // Ensures items without files are deleted in batches.
    #[tokio::test]
    async fn delete_items_without_files_removes_orphans() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES
    (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00'),
    (2, 'sha_two', 'md5_two', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
            .await
            .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_items_without_files(&mut dbs.index_conn, 10)
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM items")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(remaining.0, 1);
    }
}
