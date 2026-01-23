use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) async fn add_folder_to_database(
    conn: &mut sqlx::SqliteConnection,
    time_added: &str,
    path: &str,
    included: bool,
) -> ApiResult<bool> {
    let result = sqlx::query(
        r#"
INSERT OR IGNORE INTO folders (time_added, path, included)
VALUES (?1, ?2, ?3)
        "#,
    )
    .bind(time_added)
    .bind(path)
    .bind(included)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to insert folder");
        ApiError::internal("Failed to update folders")
    })?;

    Ok(result.rows_affected() > 0)
}

pub(crate) async fn delete_folders_not_in_list(
    conn: &mut sqlx::SqliteConnection,
    folder_paths: &[String],
    included: bool,
) -> ApiResult<u64> {
    if folder_paths.is_empty() {
        let result = sqlx::query("DELETE FROM folders WHERE included = ?1")
            .bind(included)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to delete folders");
                ApiError::internal("Failed to update folders")
            })?;
        return Ok(result.rows_affected());
    }

    let placeholders = std::iter::repeat("?")
        .take(folder_paths.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "DELETE FROM folders WHERE included = ?1 AND path NOT IN ({placeholders})"
    );

    let mut query = sqlx::query(&sql).bind(included);
    for path in folder_paths {
        query = query.bind(path);
    }

    let result = query.execute(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to delete folders not in list");
        ApiError::internal("Failed to update folders")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn delete_files_under_excluded_folders(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM files
WHERE EXISTS (
    SELECT 1
    FROM folders
    WHERE folders.included = 0
    AND files.path LIKE folders.path || '%'
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete files under excluded folders");
        ApiError::internal("Failed to delete excluded files")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn delete_files_not_under_included_folders(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
DELETE FROM files
WHERE NOT EXISTS (
    SELECT 1
    FROM folders
    WHERE folders.included = 1
    AND files.path LIKE folders.path || '%'
)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete files outside included folders");
        ApiError::internal("Failed to delete orphan files")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn get_folders_from_database(
    conn: &mut sqlx::SqliteConnection,
    included: bool,
) -> ApiResult<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT path
        FROM folders
        WHERE included = ?
        "#,
    )
    .bind(included)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read folders");
        ApiError::internal("Failed to get folders")
    })?;

    let mut folders = Vec::with_capacity(rows.len());
    for row in rows {
        let path: String = row.try_get("path").map_err(|err| {
            tracing::error!(error = %err, "failed to read folder path");
            ApiError::internal("Failed to get folders")
        })?;
        folders.push(path);
    }

    Ok(folders)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    // Ensures folder queries return only included entries by default.
    #[tokio::test]
    async fn get_folders_from_database_filters_included() {
        let mut dbs = setup_test_databases().await;
        sqlx::query("INSERT INTO folders (time_added, path, included) VALUES (?, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\data")
            .bind(1_i64)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO folders (time_added, path, included) VALUES (?, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\excluded")
            .bind(0_i64)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();

        let folders = get_folders_from_database(&mut dbs.index_conn, true)
            .await
            .unwrap();

        assert_eq!(folders, vec![r"C:\data".to_string()]);
    }

    // Ensures folder inserts dedupe paths via INSERT OR IGNORE.
    #[tokio::test]
    async fn add_folder_to_database_ignores_duplicates() {
        let mut dbs = setup_test_databases().await;
        let inserted = add_folder_to_database(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\", true)
            .await
            .unwrap();
        assert!(inserted);

        let inserted_again =
            add_folder_to_database(&mut dbs.index_conn, "2024-01-01T00:00:00", r"C:\data\", true)
                .await
                .unwrap();
        assert!(!inserted_again);
    }

    // Ensures excluded-folder deletions remove matching file paths.
    #[tokio::test]
    async fn delete_files_under_excluded_folders_removes_matching_paths() {
        let mut dbs = setup_test_databases().await;
        sqlx::query("INSERT INTO folders (time_added, path, included) VALUES (?, ?, ?)")
            .bind("2024-01-01T00:00:00")
            .bind(r"C:\excluded\")
            .bind(0_i64)
            .execute(&mut dbs.index_conn)
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
INSERT INTO file_scans (id, start_time, path)
VALUES (1, '2024-01-01T00:00:00', 'C:\data\')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
VALUES
    ('sha_one', 1, 'C:\excluded\one.png', 'one.png', '2024-01-01T00:00:00', 1, 1),
    ('sha_one', 1, 'C:\data\two.png', 'two.png', '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let deleted = delete_files_under_excluded_folders(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM files")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(remaining.0, 1);
    }
}
