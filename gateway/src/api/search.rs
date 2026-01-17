use axum::Json;
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};

use crate::api_error::ApiError;
use crate::db::tags::{find_tags, get_most_common_tags_frequency};
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

#[derive(Deserialize)]
pub(crate) struct TopTagsQuery {
    namespace: Option<String>,
    #[serde(default)]
    setters: Vec<String>,
    confidence_threshold: Option<f64>,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Serialize)]
pub(crate) struct TagFrequency {
    tags: Vec<(String, String, i64, f64)>,
}

pub async fn get_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TagSearchQuery>,
) -> ApiResult<Json<TagSearchResults>> {
    let tags = load_tags(&mut db.conn, &query.name, query.limit).await?;
    Ok(Json(TagSearchResults { tags }))
}

pub async fn get_top_tags(
    mut db: DbConnection<ReadOnly>,
    Query(query): Query<TopTagsQuery>,
) -> ApiResult<Json<TagFrequency>> {
    if let Some(confidence) = query.confidence_threshold {
        if !(0.0..=1.0).contains(&confidence) {
            return Err(ApiError::bad_request(
                "confidence_threshold must be between 0 and 1",
            ));
        }
    }

    let tags = load_top_tags(
        &mut db.conn,
        query.namespace.as_deref(),
        &query.setters,
        query.confidence_threshold,
        query.limit,
    )
    .await?;
    Ok(Json(TagFrequency { tags }))
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

async fn load_top_tags(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64, f64)>> {
    get_most_common_tags_frequency(conn, namespace, setters, confidence_threshold, limit).await
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
                item_id INTEGER NOT NULL,
                setter_id INTEGER NOT NULL
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
                tag_id INTEGER NOT NULL,
                confidence REAL NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE setters (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
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
            INSERT INTO setters (id, name)
            VALUES
                (1, 'alpha')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id)
            VALUES
                (10, 100, 1),
                (11, 101, 1)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (10, 2, 0.6),
                (10, 1, 0.9),
                (11, 1, 0.8)
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

    // Ensures top tag results include frequency fractions based on total taggable pairs.
    #[tokio::test]
    async fn load_top_tags_returns_frequency() {
        let mut conn = setup_tag_db().await;
        let tags = load_top_tags(&mut conn, None, &[], None, 10).await.unwrap();

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].0, "ns");
        assert_eq!(tags[0].1, "cat");
        assert_eq!(tags[0].2, 2);
        assert!((tags[0].3 - 1.0).abs() < 1e-6);
        assert_eq!(tags[1].1, "caterpillar");
        assert_eq!(tags[1].2, 1);
        assert!((tags[1].3 - 0.5).abs() < 1e-6);
    }
}
