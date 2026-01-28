use sqlx::Row;

use crate::api_error::ApiError;
use serde::Serialize;

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Serialize)]
pub(crate) struct FileScanRecord {
    pub id: i64,
    pub start_time: String,
    pub end_time: Option<String>,
    pub path: String,
    pub total_available: i64,
    pub new_items: i64,
    pub unchanged_files: i64,
    pub new_files: i64,
    pub modified_files: i64,
    pub marked_unavailable: i64,
    pub errors: i64,
    pub false_changes: i64,
    pub metadata_time: f64,
    pub hashing_time: f64,
    pub thumbgen_time: f64,
    pub blurhash_time: f64,
}

#[derive(Clone)]
pub(crate) struct FileScanUpdate {
    pub end_time: String,
    pub new_items: i64,
    pub unchanged_files: i64,
    pub new_files: i64,
    pub modified_files: i64,
    pub marked_unavailable: i64,
    pub errors: i64,
    pub total_available: i64,
    pub false_changes: i64,
    pub metadata_time: f64,
    pub hashing_time: f64,
    pub thumbgen_time: f64,
    pub blurhash_time: f64,
}

pub(crate) async fn add_file_scan(
    conn: &mut sqlx::SqliteConnection,
    start_time: &str,
    path: &str,
) -> ApiResult<i64> {
    let result = sqlx::query(
        r#"
INSERT INTO file_scans (start_time, path)
VALUES (?1, ?2)
        "#,
    )
    .bind(start_time)
    .bind(path)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert file scan");
        ApiError::internal("Failed to create file scan")
    })?;

    Ok(result.last_insert_rowid())
}

pub(crate) async fn update_file_scan(
    conn: &mut sqlx::SqliteConnection,
    scan_id: i64,
    update: FileScanUpdate,
) -> ApiResult<()> {
    fn round_time(value: f64) -> f64 {
        if value.is_finite() {
            (value * 100.0).round() / 100.0
        } else {
            value
        }
    }

    let FileScanUpdate {
        end_time,
        new_items,
        unchanged_files,
        new_files,
        modified_files,
        marked_unavailable,
        errors,
        total_available,
        false_changes,
        metadata_time,
        hashing_time,
        thumbgen_time,
        blurhash_time,
    } = update;

    sqlx::query(
        r#"
UPDATE file_scans
SET
    end_time = ?1,
    new_items = ?2,
    unchanged_files = ?3,
    new_files = ?4,
    modified_files = ?5,
    marked_unavailable = ?6,
    errors = ?7,
    total_available = ?8,
    false_changes = ?9,
    metadata_time = ?10,
    hashing_time = ?11,
    thumbgen_time = ?12,
    blurhash_time = ?13
WHERE id = ?14
        "#,
    )
    .bind(end_time)
    .bind(new_items)
    .bind(unchanged_files)
    .bind(new_files)
    .bind(modified_files)
    .bind(marked_unavailable)
    .bind(errors)
    .bind(total_available)
    .bind(false_changes)
    .bind(round_time(metadata_time))
    .bind(round_time(hashing_time))
    .bind(round_time(thumbgen_time))
    .bind(round_time(blurhash_time))
    .bind(scan_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, scan_id, "failed to update file scan");
        ApiError::internal("Failed to update file scan")
    })?;

    Ok(())
}

pub(crate) async fn close_file_scan(
    conn: &mut sqlx::SqliteConnection,
    scan_id: i64,
    end_time: &str,
) -> ApiResult<()> {
    sqlx::query(
        r#"
UPDATE file_scans
SET end_time = ?1
WHERE id = ?2
        "#,
    )
    .bind(end_time)
    .bind(scan_id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, scan_id, "failed to close file scan");
        ApiError::internal("Failed to close file scan")
    })?;
    Ok(())
}

pub(crate) async fn get_open_file_scan_id(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"
SELECT id
FROM file_scans
WHERE end_time IS NULL
  AND path = ?1
ORDER BY start_time DESC
LIMIT 1
        "#,
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query open file scan");
        ApiError::internal("Failed to query file scan")
    })?;
    Ok(row.map(|(id,)| id))
}

pub(crate) async fn get_all_file_scans(
    conn: &mut sqlx::SqliteConnection,
    page: i64,
    page_size: Option<i64>,
) -> ApiResult<Vec<FileScanRecord>> {
    let mut query = String::from(
        r#"
SELECT
    id,
    start_time,
    end_time,
    path,
    total_available,
    new_items,
    unchanged_files,
    new_files,
    modified_files,
    marked_unavailable,
    errors,
    false_changes,
    metadata_time,
    hashing_time,
    thumbgen_time,
    blurhash_time
FROM file_scans
ORDER BY start_time DESC
        "#,
    );

    let mut offset = 0_i64;
    if let Some(page_size) = page_size {
        offset = (page.saturating_sub(1)).saturating_mul(page_size);
        query.push_str(" LIMIT ? OFFSET ?");
    }

    let mut sql = sqlx::query(&query);
    if let Some(page_size) = page_size {
        sql = sql.bind(page_size).bind(offset);
    }

    let rows = sql.fetch_all(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query file scans");
        ApiError::internal("Failed to get scan history")
    })?;

    let mut scans = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan id");
            ApiError::internal("Failed to get scan history")
        })?;
        let start_time: String = row.try_get("start_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan start_time");
            ApiError::internal("Failed to get scan history")
        })?;
        let end_time: Option<String> = row.try_get("end_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan end_time");
            ApiError::internal("Failed to get scan history")
        })?;
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan path");
            ApiError::internal("Failed to get scan history")
        })?;
        let total_available: i64 = row.try_get("total_available").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan total_available");
            ApiError::internal("Failed to get scan history")
        })?;
        let new_items: i64 = row.try_get("new_items").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan new_items");
            ApiError::internal("Failed to get scan history")
        })?;
        let unchanged_files: i64 = row.try_get("unchanged_files").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan unchanged_files");
            ApiError::internal("Failed to get scan history")
        })?;
        let new_files: i64 = row.try_get("new_files").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan new_files");
            ApiError::internal("Failed to get scan history")
        })?;
        let modified_files: i64 = row.try_get("modified_files").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan modified_files");
            ApiError::internal("Failed to get scan history")
        })?;
        let marked_unavailable: i64 = row.try_get("marked_unavailable").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan marked_unavailable");
            ApiError::internal("Failed to get scan history")
        })?;
        let errors: i64 = row.try_get("errors").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan errors");
            ApiError::internal("Failed to get scan history")
        })?;
        let false_changes: i64 = row.try_get("false_changes").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan false_changes");
            ApiError::internal("Failed to get scan history")
        })?;
        let metadata_time: f64 = row.try_get("metadata_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan metadata_time");
            ApiError::internal("Failed to get scan history")
        })?;
        let hashing_time: f64 = row.try_get("hashing_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan hashing_time");
            ApiError::internal("Failed to get scan history")
        })?;
        let thumbgen_time: f64 = row.try_get("thumbgen_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan thumbgen_time");
            ApiError::internal("Failed to get scan history")
        })?;
        let blurhash_time: f64 = row.try_get("blurhash_time").map_err(|err| {
            tracing::error!(error = %err, "failed to read file scan blurhash_time");
            ApiError::internal("Failed to get scan history")
        })?;

        scans.push(FileScanRecord {
            id,
            start_time,
            end_time,
            path,
            total_available,
            new_items,
            unchanged_files,
            new_files,
            modified_files,
            marked_unavailable,
            errors,
            false_changes,
            metadata_time,
            hashing_time,
            thumbgen_time,
            blurhash_time,
        });
    }

    Ok(scans)
}

pub(crate) async fn mark_unavailable_files(
    conn: &mut sqlx::SqliteConnection,
    scan_id: i64,
    path_prefix: &str,
) -> ApiResult<(i64, i64)> {
    let row = sqlx::query(
        r#"
SELECT COUNT(*) AS marked_unavailable
FROM files
WHERE scan_id != ?1
AND path LIKE ?2 || '%'
        "#,
    )
    .bind(scan_id)
    .bind(path_prefix)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, scan_id, "failed to count unavailable files");
        ApiError::internal("Failed to mark unavailable files")
    })?;

    let marked_unavailable: i64 = row.try_get("marked_unavailable").map_err(|err| {
        tracing::error!(error = %err, "failed to read unavailable file count");
        ApiError::internal("Failed to mark unavailable files")
    })?;

    sqlx::query(
        r#"
UPDATE files
SET available = FALSE
WHERE scan_id != ?1
AND path LIKE ?2 || '%'
        "#,
    )
    .bind(scan_id)
    .bind(path_prefix)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, scan_id, "failed to update unavailable files");
        ApiError::internal("Failed to mark unavailable files")
    })?;

    let row = sqlx::query(
        r#"
SELECT COUNT(*) AS available_files
FROM files
WHERE available = TRUE
AND path LIKE ?1 || '%'
        "#,
    )
    .bind(path_prefix)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, scan_id, "failed to count available files");
        ApiError::internal("Failed to mark unavailable files")
    })?;

    let available_files: i64 = row.try_get("available_files").map_err(|err| {
        tracing::error!(error = %err, "failed to read available file count");
        ApiError::internal("Failed to mark unavailable files")
    })?;

    Ok((marked_unavailable, available_files))
}

pub(crate) async fn delete_unavailable_files(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM files
WHERE available = 0
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete unavailable files");
        ApiError::internal("Failed to delete unavailable files")
    })?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    // Ensures a scan row can be created and updated.
    #[tokio::test]
    async fn add_and_update_file_scan() {
        let mut dbs = setup_test_databases().await;
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data")
            .await
            .unwrap();

        update_file_scan(
            &mut dbs.index_conn,
            scan_id,
            FileScanUpdate {
                end_time: "2024-01-01T00:01:00".to_string(),
                new_items: 1,
                unchanged_files: 2,
                new_files: 3,
                modified_files: 4,
                marked_unavailable: 5,
                errors: 6,
                total_available: 7,
                false_changes: 8,
                metadata_time: 1.1,
                hashing_time: 2.2,
                thumbgen_time: 3.3,
                blurhash_time: 4.4,
            },
        )
        .await
        .unwrap();

        let scans = get_all_file_scans(&mut dbs.index_conn, 1, None).await.unwrap();
        let scan = scans.into_iter().find(|scan| scan.id == scan_id).unwrap();

        assert_eq!(scan.path, r"C:\data");
        assert_eq!(scan.end_time.as_deref(), Some("2024-01-01T00:01:00"));
        assert_eq!(scan.new_files, 3);
        assert_eq!(scan.blurhash_time, 4.4);
    }

    // Ensures unavailable marking only affects files under a path prefix.
    #[tokio::test]
    async fn mark_unavailable_files_limits_to_prefix() {
        let mut dbs = setup_test_databases().await;
        let previous_scan_id =
            add_file_scan(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\")
                .await
                .unwrap();
        let scan_id = add_file_scan(&mut dbs.index_conn, "2024-01-01T00:01:00", r"C:\data\")
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
VALUES
    ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 1),
    ('sha_one', 1, 'C:\other\two.png', 'two.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(previous_scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let (marked, available) =
            mark_unavailable_files(&mut dbs.index_conn, scan_id, r"C:\data\")
                .await
                .unwrap();

        assert_eq!(marked, 1);
        assert_eq!(available, 0);

        let row: (i64,) = sqlx::query_as("SELECT available FROM files WHERE path = 'C:\\other\\two.png'")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(row.0, 1);
    }

    // Ensures unavailable deletion removes only files flagged unavailable.
    #[tokio::test]
    async fn delete_unavailable_files_removes_only_unavailable() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
INSERT INTO items (id, sha256, md5, type, time_added)
VALUES (1, 'sha_one', 'md5_one', 'image/png', '2024-01-01T00:00:00')
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
VALUES
    ('sha_one', 1, 'C:\data\one.png', 'one.png', '2024-01-01T00:00:00', ?1, 0),
    ('sha_one', 1, 'C:\data\two.png', 'two.png', '2024-01-01T00:00:00', ?1, 1)
            "#,
        )
        .bind(scan_id)
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_unavailable_files(&mut dbs.index_conn).await.unwrap();
        assert_eq!(deleted, 1);

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(remaining.0, 1);
    }
}
