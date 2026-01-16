use axum::{Json, extract::Path};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api_error::ApiError;
use crate::db::{DbConnection, ReadOnly, UserDataWrite};
use crate::db::bookmarks::{delete_bookmark, get_bookmarks_item};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_USER: &str = "user";

#[derive(Deserialize)]
pub(crate) struct ItemBookmarksQuery {
    #[serde(default = "default_user")]
    user: String,
}

#[derive(Deserialize)]
pub(crate) struct BookmarkUserQuery {
    #[serde(default = "default_user")]
    user: String,
}

#[derive(Serialize)]
pub(crate) struct ExistingBookmarkMetadata {
    namespace: Option<String>,
    metadata: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct ItemBookmarks {
    bookmarks: Vec<ExistingBookmarkMetadata>,
}

#[derive(Serialize)]
pub(crate) struct MessageResult {
    message: String,
}

pub async fn bookmarks_item(
    mut db: DbConnection<ReadOnly>,
    Path(sha256): Path<String>,
    Query(query): Query<ItemBookmarksQuery>,
) -> ApiResult<Json<ItemBookmarks>> {
    let response = load_item_bookmarks(&mut db.conn, &sha256, &query.user).await?;
    Ok(Json(response))
}

pub async fn delete_bookmark_by_sha256(
    mut db: DbConnection<UserDataWrite>,
    Path((namespace, sha256)): Path<(String, String)>,
    Query(query): Query<BookmarkUserQuery>,
) -> ApiResult<Json<MessageResult>> {
    let response =
        delete_bookmark_entry(&mut db.conn, &sha256, &namespace, &query.user).await?;
    Ok(Json(response))
}

async fn load_item_bookmarks(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    user: &str,
) -> ApiResult<ItemBookmarks> {
    let rows = get_bookmarks_item(conn, sha256, user).await?;
    let bookmarks = rows
        .into_iter()
        .map(|entry| ExistingBookmarkMetadata {
            namespace: Some(entry.namespace),
            metadata: entry.metadata,
        })
        .collect();
    Ok(ItemBookmarks { bookmarks })
}

async fn delete_bookmark_entry(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    namespace: &str,
    user: &str,
) -> ApiResult<MessageResult> {
    delete_bookmark(conn, sha256, namespace, user).await?;
    Ok(MessageResult {
        message: "Deleted bookmark".to_string(),
    })
}

fn default_user() -> String {
    DEFAULT_USER.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::{Connection, Row};

    async fn setup_user_data_db() -> sqlx::SqliteConnection {
        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::query("ATTACH DATABASE ':memory:' AS user_data")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE user_data.bookmarks (
                user TEXT NOT NULL,
                namespace TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                time_added TEXT NOT NULL,
                metadata TEXT
            )
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        conn
    }

    // Ensures the bookmarks response includes namespaces and parsed metadata for the item.
    #[tokio::test]
    async fn load_item_bookmarks_returns_metadata() {
        let mut conn = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(r#"{"note":"test"}"#)
        .execute(&mut conn)
        .await
        .unwrap();

        let response = load_item_bookmarks(&mut conn, "sha256", "user")
            .await
            .unwrap();

        assert_eq!(response.bookmarks.len(), 1);
        assert_eq!(
            response.bookmarks[0].namespace.as_deref(),
            Some("favorites")
        );
        assert_eq!(
            response.bookmarks[0].metadata.as_ref(),
            Some(&json!({"note": "test"}))
        );
    }

    // Ensures deleting with the wildcard namespace removes all matching bookmarks.
    #[tokio::test]
    async fn delete_bookmark_entry_removes_all_namespaces() {
        let mut conn = setup_user_data_db().await;
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("favorites")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_data.bookmarks (user, namespace, sha256, time_added, metadata)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("user")
        .bind("archived")
        .bind("sha256")
        .bind("2024-01-01T00:00:00")
        .bind(Option::<String>::None)
        .execute(&mut conn)
        .await
        .unwrap();

        let response = delete_bookmark_entry(&mut conn, "sha256", "*", "user")
            .await
            .unwrap();

        assert_eq!(response.message, "Deleted bookmark");
        let remaining = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM user_data.bookmarks
            WHERE sha256 = ? AND user = ?
            "#,
        )
        .bind("sha256")
        .bind("user")
        .fetch_one(&mut conn)
        .await
        .unwrap();
        let count: i64 = remaining.try_get("count").unwrap();
        assert_eq!(count, 0);
    }
}
