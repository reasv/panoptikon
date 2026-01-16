use serde_json::Value;
use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) struct BookmarkEntry {
    pub namespace: String,
    pub metadata: Option<Value>,
}

pub(crate) async fn delete_bookmark(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
) -> ApiResult<u64> {
    let query = if namespace == "*" {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            "#,
        )
        .bind(sha256)
        .bind(user)
    } else {
        sqlx::query(
            r#"
            DELETE FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ? AND namespace = ?
            "#,
        )
        .bind(sha256)
        .bind(user)
        .bind(namespace)
    };

    let result = query.execute(conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to delete bookmark");
        ApiError::internal("Failed to delete bookmark")
    })?;

    Ok(result.rows_affected())
}

pub(crate) async fn get_bookmarks_item(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    user: &str,
) -> ApiResult<Vec<BookmarkEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT namespace, metadata
        FROM user_data.bookmarks
        WHERE sha256 = ? AND user = ?
        "#,
    )
    .bind(sha256)
    .bind(user)
    .fetch_all(conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read bookmarks for item");
        ApiError::internal("Failed to get bookmarks")
    })?;

    let mut bookmarks = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark namespace");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let metadata_raw: Option<String> = row.try_get("metadata").map_err(|err| {
            tracing::error!(error = %err, "failed to read bookmark metadata");
            ApiError::internal("Failed to get bookmarks")
        })?;
        let metadata = match metadata_raw {
            Some(raw) => {
                let parsed: Value = serde_json::from_str(&raw).map_err(|err| {
                    tracing::error!(error = %err, "failed to parse bookmark metadata");
                    ApiError::internal("Failed to get bookmarks")
                })?;
                Some(parsed)
            }
            None => None,
        };
        bookmarks.push(BookmarkEntry {
            namespace,
            metadata,
        });
    }

    Ok(bookmarks)
}
