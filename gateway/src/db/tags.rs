use sqlx::Row;
use std::collections::HashMap;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) async fn find_tags(
    conn: &mut sqlx::SqliteConnection,
    name: &str,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let rows = sqlx::query(
        r#"
        SELECT id, namespace, name
        FROM tags
        WHERE name LIKE ?
        LIMIT ?
        "#,
    )
    .bind(format!("%{name}%"))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to query tags");
        ApiError::internal("Failed to get tags")
    })?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ids = Vec::with_capacity(rows.len());
    let mut id_to_tag = HashMap::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag id");
            ApiError::internal("Failed to get tags")
        })?;
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tags")
        })?;
        let tag_name: String = row.try_get("name").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag name");
            ApiError::internal("Failed to get tags")
        })?;
        ids.push(id);
        id_to_tag.insert(id, (namespace, tag_name));
    }

    let placeholders = std::iter::repeat("?")
        .take(ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT tags_items.tag_id AS tag_id, COUNT(DISTINCT item_data.item_id) AS count
        FROM tags_items
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        WHERE tags_items.tag_id IN ({placeholders})
        GROUP BY tags_items.tag_id
        "#
    );

    let mut query = sqlx::query(&sql);
    for tag_id in &ids {
        query = query.bind(tag_id);
    }

    let rows = query.fetch_all(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query tag frequencies");
        ApiError::internal("Failed to get tags")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let tag_id: i64 = row.try_get("tag_id").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag id");
            ApiError::internal("Failed to get tags")
        })?;
        let count: i64 = row.try_get("count").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag count");
            ApiError::internal("Failed to get tags")
        })?;
        if let Some((namespace, tag_name)) = id_to_tag.get(&tag_id) {
            results.push((namespace.clone(), tag_name.clone(), count));
        }
    }

    Ok(results)
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
                (2, 'ns', 'caterpillar'),
                (3, 'ns', 'dog')
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
                (11, 101),
                (12, 100)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id)
            VALUES
                (10, 1),
                (11, 1),
                (12, 1),
                (10, 2),
                (11, 3)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        conn
    }

    // Ensures tag search returns per-tag distinct item counts for matching names.
    #[tokio::test]
    async fn find_tags_returns_distinct_item_counts() {
        let mut conn = setup_tag_db().await;
        let mut tags = find_tags(&mut conn, "cat", 10).await.unwrap();
        tags.sort_by(|a, b| a.1.cmp(&b.1));

        assert_eq!(
            tags,
            vec![
                ("ns".to_string(), "cat".to_string(), 2),
                ("ns".to_string(), "caterpillar".to_string(), 1)
            ]
        );
    }
}
