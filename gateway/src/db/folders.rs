use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

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
}
