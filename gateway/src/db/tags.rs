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

pub(crate) async fn get_most_common_tags_frequency(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64, f64)>> {
    let tags =
        get_most_common_tags(conn, namespace, setters, confidence_threshold, limit).await?;
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    let mut sql = String::from(
        r#"
        SELECT COUNT(DISTINCT item_data.item_id || '-' || item_data.setter_id) AS distinct_count
        FROM tags_items
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        JOIN setters
            ON item_data.setter_id = setters.id
        "#,
    );
    if !setters.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(setters.len())
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(" WHERE setters.name IN ({placeholders})"));
    }

    let mut query = sqlx::query(&sql);
    for setter in setters {
        query = query.bind(setter);
    }

    let row = query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query tag totals");
        ApiError::internal("Failed to get tags")
    })?;

    let total: i64 = row.try_get("distinct_count").map_err(|err| {
        tracing::error!(error = %err, "failed to read tag total count");
        ApiError::internal("Failed to get tags")
    })?;

    let total = total as f64;
    let results = tags
        .into_iter()
        .map(|(namespace, name, count)| {
            let frequency = if total > 0.0 {
                (count as f64) / total
            } else {
                0.0
            };
            (namespace, name, count, frequency)
        })
        .collect();

    Ok(results)
}

async fn get_most_common_tags(
    conn: &mut sqlx::SqliteConnection,
    namespace: Option<&str>,
    setters: &[String],
    confidence_threshold: Option<f64>,
    limit: i64,
) -> ApiResult<Vec<(String, String, i64)>> {
    let mut sql = String::from(
        r#"
        SELECT tags.namespace, tags.name, COUNT(*) AS count
        FROM tags
        JOIN tags_items
            ON tags.id = tags_items.tag_id
        JOIN item_data
            ON tags_items.item_data_id = item_data.id
        JOIN setters
            ON item_data.setter_id = setters.id
        "#,
    );

    let mut conditions: Vec<String> = Vec::new();
    if namespace.is_some() {
        conditions.push("tags.namespace LIKE ? || '%'".to_string());
    }
    if confidence_threshold.unwrap_or(0.0) > 0.0 {
        conditions.push("tags_items.confidence >= ?".to_string());
    }
    if !setters.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(setters.len())
            .collect::<Vec<_>>()
            .join(", ");
        conditions.push(format!("setters.name IN ({placeholders})"));
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    sql.push_str(" GROUP BY tags.namespace, tags.name");
    sql.push_str(" ORDER BY count DESC");
    sql.push_str(" LIMIT ?");

    let mut query = sqlx::query(&sql);
    if let Some(namespace) = namespace {
        query = query.bind(namespace);
    }
    if let Some(confidence_threshold) = confidence_threshold {
        if confidence_threshold > 0.0 {
            query = query.bind(confidence_threshold);
        }
    }
    for setter in setters {
        query = query.bind(setter);
    }
    query = query.bind(limit);

    let rows = query.fetch_all(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to query top tags");
        ApiError::internal("Failed to get tags")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let namespace: String = row.try_get("namespace").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag namespace");
            ApiError::internal("Failed to get tags")
        })?;
        let name: String = row.try_get("name").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag name");
            ApiError::internal("Failed to get tags")
        })?;
        let count: i64 = row.try_get("count").map_err(|err| {
            tracing::error!(error = %err, "failed to read tag count");
            ApiError::internal("Failed to get tags")
        })?;
        results.push((namespace, name, count));
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
                (2, 'ns', 'caterpillar'),
                (3, 'ns', 'dog')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO setters (id, name)
            VALUES
                (1, 'alpha'),
                (2, 'beta')
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
                (11, 101, 1),
                (12, 100, 2)
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO tags_items (item_data_id, tag_id, confidence)
            VALUES
                (10, 1, 0.9),
                (11, 1, 0.7),
                (12, 1, 0.8),
                (10, 2, 0.6),
                (11, 3, 0.5)
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

    // Ensures top tags include frequency based on distinct item-setter pairs.
    #[tokio::test]
    async fn get_most_common_tags_frequency_calculates_frequency() {
        let mut conn = setup_tag_db().await;
        let tags =
            get_most_common_tags_frequency(&mut conn, None, &[], None, 10).await.unwrap();

        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0].1, "cat");
        assert_eq!(tags[0].2, 3);
        assert!((tags[0].3 - 1.0).abs() < 1e-6);
        assert_eq!(tags[1].1, "caterpillar");
        assert_eq!(tags[1].2, 1);
        assert!((tags[1].3 - (1.0 / 3.0)).abs() < 1e-6);
        assert_eq!(tags[2].1, "dog");
        assert_eq!(tags[2].2, 1);
        assert!((tags[2].3 - (1.0 / 3.0)).abs() < 1e-6);
    }
}
