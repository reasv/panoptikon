use sqlx::Row;

use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) async fn get_existing_setters(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT item_data.data_type AS data_type, setters.name AS setter_name
        FROM item_data
        JOIN setters
            ON item_data.setter_id = setters.id
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read setters");
        ApiError::internal("Failed to get setters")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let data_type: String = row.try_get("data_type").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter data type");
            ApiError::internal("Failed to get setters")
        })?;
        let setter_name: String = row.try_get("setter_name").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter name");
            ApiError::internal("Failed to get setters")
        })?;
        results.push((data_type, setter_name));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    // Ensures distinct data_type/setter pairs are returned from the extraction log tables.
    #[tokio::test]
    async fn get_existing_setters_returns_distinct_pairs() {
        let mut conn = sqlx::SqliteConnection::connect(":memory:").await.unwrap();
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
            CREATE TABLE item_data (
                id INTEGER PRIMARY KEY,
                setter_id INTEGER NOT NULL,
                data_type TEXT NOT NULL
            );
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, setter_id, data_type)
            VALUES
                (10, 1, 'tags'),
                (11, 1, 'tags'),
                (12, 1, 'text'),
                (13, 2, 'text')
            "#,
        )
        .execute(&mut conn)
        .await
        .unwrap();

        let mut results = get_existing_setters(&mut conn).await.unwrap();
        results.sort();

        assert_eq!(
            results,
            vec![
                ("tags".to_string(), "alpha".to_string()),
                ("text".to_string(), "alpha".to_string()),
                ("text".to_string(), "beta".to_string())
            ]
        );
    }
}
