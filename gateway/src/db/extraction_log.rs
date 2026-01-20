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
    use crate::db::migrations::setup_test_databases;

    // Ensures distinct data_type/setter pairs are returned from the extraction log tables.
    #[tokio::test]
    async fn get_existing_setters_returns_distinct_pairs() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (100, 'sha_100', 'md5_100', 'image/png', '2024-01-01T00:00:00'),
                (101, 'sha_101', 'md5_101', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha'), (2, 'beta')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (10, 100, 1, 'tags', 0, 1),
                (11, 100, 1, 'tags', 1, 1),
                (12, 101, 1, 'text', 0, 1),
                (13, 101, 2, 'text', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let mut results = get_existing_setters(&mut dbs.index_conn).await.unwrap();
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
