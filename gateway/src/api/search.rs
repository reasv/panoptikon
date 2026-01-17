use axum::Json;
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};

use crate::api_error::ApiError;
use crate::db::tags::find_tags;
use crate::db::{DbConnection, ReadOnly};

type ApiResult<T> = std::result::Result<T, ApiError>;

const DEFAULT_LIMIT: i64 = 10;

#[derive(Deserialize)]
pub(crate) struct TagSearchQuery {
    name: String,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagSearchResults {
    tags: Vec<(String, String, i64)>,
}

pub async fn get_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TagSearchQuery>,
) -> ApiResult<Json<TagSearchResults>> {
    let tags = load_tags(&mut db.conn, &query.name, query.limit).await?;
    Ok(Json(TagSearchResults { tags }))
}

async fn load_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let mut tags = find_tags(conn, name, limit).await?;
    tags.sort_by(|a, b| b.2.cmp(&a.2));
    Ok(tags)
}

fn default_limit() -> i64 {
    DEFAULT_LIMIT
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    async fn setup_tag_db() -> sqlx::SqliteConnection {
        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE item_data (
                id INTEGER PRIMARY KEY,
                item_id INTEGER NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE tags_items (
                id INTEGER PRIMARY KEY,
                item_data_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO tags (id, namespace, name)
            VALUES
                (1, 'ns', 'cat'),
                (2, 'ns', 'caterpillar')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id)
            VALUES
                (10, 100),
                (11, 101)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id)
            VALUES
                (10, 2),
                (10, 1),
                (11, 1)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        conn
    }

    // Ensures tags are sorted by descending count to match the FastAPI handler.
    #[tokio::test]
    async fn load_tags_sorts_by_frequency_desc() {
        let mut conn = setup_tag_db().await;
        let tags = load_tags(&mut conn, "cat", 10).await.unwrap();

        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat".to_string(), 2),
                ("ns".to_string(), "caterpillar".to_string(), 1)
            ]
        );
    }
}
