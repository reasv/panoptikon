use serde::Serialize;
use sqlx::Row;
use utoipa::ToSchema;

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

/// The search-usable embedding setters among `(data_type, setter)` pairs:
/// text-embedding and clip setters, excluding `tclip/` (text-CLIP setters
/// are not selectable in search). This is THE selection rule shared by the
/// cron `preload_embedding_models` loop and the prewarm eager set (design
/// §8: "the same selection logic as preload_embedding_models") — change it
/// in one place or the two features drift.
pub(crate) fn filter_search_embedding_setters(
    setters: impl IntoIterator<Item = (String, String)>,
) -> Vec<String> {
    setters
        .into_iter()
        .filter(|(data_type, setter)| {
            (data_type == "text-embedding" || data_type == "clip") && !setter.starts_with("tclip/")
        })
        .map(|(_, setter)| setter)
        .collect()
}

/// Search-usable embedding setters WITH DATA in this index DB (the
/// existing-setters query + [`filter_search_embedding_setters`]).
pub(crate) async fn get_search_embedding_setters(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<String>> {
    Ok(filter_search_embedding_setters(
        get_existing_setters(conn).await?,
    ))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LogRecord {
    pub id: i64,
    pub start_time: String,
    pub end_time: String,
    pub items_in_db: i64,
    #[serde(rename = "type")]
    pub data_type: String,
    pub setter: String,
    pub threshold: Option<f64>,
    pub batch_size: i64,
    pub image_files: i64,
    pub video_files: i64,
    pub other_files: i64,
    pub total_segments: i64,
    pub errors: i64,
    pub total_remaining: i64,
    pub data_load_time: f64,
    pub inference_time: f64,
    pub failed: i64,
    pub completed: i64,
    pub status: Option<i64>,
}

pub(crate) async fn get_all_data_logs(
    conn: &mut sqlx::SqliteConnection,
    page: i64,
    page_size: Option<i64>,
) -> ApiResult<Vec<LogRecord>> {
    let page = page.max(1);
    let offset = if let Some(page_size) = page_size {
        (page - 1) * page_size
    } else {
        0
    };
    let mut query = String::from(
        r#"
        SELECT
            data_log.id,
            start_time,
            end_time,
            COALESCE(COUNT(DISTINCT item_data.id), 0) AS distinct_item_count,
            type,
            setter,
            threshold,
            batch_size,
            image_files,
            video_files,
            other_files,
            total_segments,
            errors,
            total_remaining,
            data_load_time,
            inference_time,
            CASE 
                WHEN data_log.completed = 1 THEN 0
                WHEN data_log.job_id IS NULL THEN 1
                ELSE 0
            END AS failed,
            data_log.completed,
            data_jobs.completed AS status
        FROM data_log
        LEFT JOIN item_data 
            ON item_data.job_id = data_log.job_id
            AND item_data.job_id IS NOT NULL
            AND item_data.is_placeholder = 0
        LEFT JOIN data_jobs
            ON data_log.job_id = data_jobs.id
        GROUP BY data_log.id
        ORDER BY start_time DESC
        "#,
    );
    if page_size.is_some() {
        query.push_str(" LIMIT ? OFFSET ?");
    }

    let rows = if let Some(page_size) = page_size {
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .bind(page_size)
            .bind(offset)
            .fetch_all(&mut *conn)
            .await
    } else {
        sqlx::query(sqlx::AssertSqlSafe(query.as_str()))
            .fetch_all(&mut *conn)
            .await
    }
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read data logs");
        ApiError::internal("Failed to get data logs")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        results.push(LogRecord {
            id: row.try_get("id").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log id");
                ApiError::internal("Failed to get data logs")
            })?,
            start_time: row.try_get("start_time").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log start time");
                ApiError::internal("Failed to get data logs")
            })?,
            end_time: row.try_get("end_time").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log end time");
                ApiError::internal("Failed to get data logs")
            })?,
            items_in_db: row.try_get("distinct_item_count").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log item count");
                ApiError::internal("Failed to get data logs")
            })?,
            data_type: row.try_get("type").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log type");
                ApiError::internal("Failed to get data logs")
            })?,
            setter: row.try_get("setter").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log setter");
                ApiError::internal("Failed to get data logs")
            })?,
            threshold: row.try_get("threshold").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log threshold");
                ApiError::internal("Failed to get data logs")
            })?,
            batch_size: row.try_get("batch_size").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log batch size");
                ApiError::internal("Failed to get data logs")
            })?,
            image_files: row.try_get("image_files").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log image files");
                ApiError::internal("Failed to get data logs")
            })?,
            video_files: row.try_get("video_files").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log video files");
                ApiError::internal("Failed to get data logs")
            })?,
            other_files: row.try_get("other_files").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log other files");
                ApiError::internal("Failed to get data logs")
            })?,
            total_segments: row.try_get("total_segments").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log segments");
                ApiError::internal("Failed to get data logs")
            })?,
            errors: row.try_get("errors").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log errors");
                ApiError::internal("Failed to get data logs")
            })?,
            total_remaining: row.try_get("total_remaining").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log remaining");
                ApiError::internal("Failed to get data logs")
            })?,
            data_load_time: row.try_get("data_load_time").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log load time");
                ApiError::internal("Failed to get data logs")
            })?,
            inference_time: row.try_get("inference_time").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log inference time");
                ApiError::internal("Failed to get data logs")
            })?,
            failed: row.try_get("failed").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log failed");
                ApiError::internal("Failed to get data logs")
            })?,
            completed: row.try_get("completed").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log completed");
                ApiError::internal("Failed to get data logs")
            })?,
            status: row.try_get("status").map_err(|err| {
                tracing::error!(error = %err, "failed to read data log status");
                ApiError::internal("Failed to get data logs")
            })?,
        });
    }

    Ok(results)
}

/// Returns the number of rows actually deleted, so callers can skip
/// post-deletion maintenance when nothing was removed.
pub(crate) async fn delete_data_job_by_log_id(
    conn: &mut sqlx::SqliteConnection,
    data_log_id: i64,
) -> ApiResult<u64> {
    let job_id: Option<i64> = sqlx::query("SELECT job_id FROM data_log WHERE id = ?")
        .bind(data_log_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to read data log");
            ApiError::internal("Failed to delete data job")
        })?
        .and_then(|row| row.try_get("job_id").ok());

    let mut deleted = 0;
    if let Some(job_id) = job_id {
        deleted = sqlx::query("DELETE FROM data_jobs WHERE id = ?")
            .bind(job_id)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to delete data job");
                ApiError::internal("Failed to delete data job")
            })?
            .rows_affected();
    }

    Ok(deleted)
}

/// Served to the scan page, which polls it. `idx_item_data_placeholder_setter_type`
/// is what keeps it off the item_data heap: grouping on the bare `setter_id`
/// column (rather than on the joined `setters.id`) is what lets the index
/// supply both the count and its order — see the migration that adds it.
const SETTER_TOTALS_SQL: &str = r#"
        SELECT s.name as setter_name, t.total_count as total_count
        FROM (
            SELECT setter_id, COUNT(*) as total_count
            FROM item_data
            WHERE is_placeholder = 0
            GROUP BY setter_id
        ) t
        JOIN setters s ON s.id = t.setter_id
        "#;

pub(crate) async fn get_setters_total_data(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<(String, i64)>> {
    let rows = sqlx::query(SETTER_TOTALS_SQL)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to read setter totals");
        ApiError::internal("Failed to get setters")
    })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let setter: String = row.try_get("setter_name").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter name");
            ApiError::internal("Failed to get setters")
        })?;
        let total: i64 = row.try_get("total_count").map_err(|err| {
            tracing::error!(error = %err, "failed to read setter totals");
            ApiError::internal("Failed to get setters")
        })?;
        results.push((setter, total));
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

    // The shared embedding-setter selection (cron preload + prewarm eager
    // set) against the real schema: only text-embedding and clip setters
    // with actual item_data rows are returned, tclip/-prefixed clip setters
    // are excluded, and non-embedding data types (tags) never appear — a
    // setter row without item_data has no data and is not selected.
    #[tokio::test]
    async fn search_embedding_setters_filter_against_real_schema() {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES (1, 'sha_1', 'md5_1', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO setters (id, name)
            VALUES
                (1, 'clip/model-a'),
                (2, 'tclip/model-b'),
                (3, 'embed/model-c'),
                (4, 'tags/model-d'),
                (5, 'clip/no-data')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin)
            VALUES
                (10, 1, 1, 'clip', 0, 1),
                (11, 1, 2, 'clip', 0, 1),
                (12, 1, 3, 'text-embedding', 0, 1),
                (13, 1, 4, 'tags', 0, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let mut setters = get_search_embedding_setters(&mut dbs.index_conn)
            .await
            .unwrap();
        setters.sort();
        assert_eq!(
            setters,
            vec!["clip/model-a".to_string(), "embed/model-c".to_string()],
            "clip + text-embedding with data only; tclip/, tags, and data-less setters excluded"
        );
    }

    // Ensures data logs are loaded with counts and status fields.
    #[tokio::test]
    async fn get_all_data_logs_returns_entries() {
        let mut dbs = setup_test_databases().await;
        sqlx::query("INSERT INTO data_jobs (id, completed) VALUES (1, 1)")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO data_log
                (id, job_id, start_time, end_time, type, setter, threshold, batch_size,
                 image_files, video_files, other_files, total_segments, errors, total_remaining,
                 data_load_time, inference_time, completed)
            VALUES
                (10, 1, '2024-01-01T00:00:00', '2024-01-01T00:10:00', 'tags', 'alpha', 0.5, 32,
                 1, 2, 3, 4, 5, 6, 1.5, 2.5, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let logs = get_all_data_logs(&mut dbs.index_conn, 1, None)
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].id, 10);
        assert_eq!(logs[0].items_in_db, 0);
        assert_eq!(logs[0].failed, 0);
        assert_eq!(logs[0].completed, 1);
        assert_eq!(logs[0].status, Some(1));
    }

    // Ensures setter totals return counts per setter.
    #[tokio::test]
    async fn get_setters_total_data_returns_counts() {
        let mut dbs = setup_test_databases().await;
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'alpha')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha256-1', 'md5-1', 'image/jpeg', '2024-01-01T00:00:00'),
                (2, 'sha256-2', 'md5-2', 'image/jpeg', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin, is_placeholder)
            VALUES (10, 1, 1, 'tags', 0, 1, 0), (11, 2, 1, 'tags', 1, 1, 0)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        let totals = get_setters_total_data(&mut dbs.index_conn).await.unwrap();
        assert_eq!(totals, vec![("alpha".to_string(), 2)]);

        // The count is polled by the scan page, so it must never touch the
        // item_data heap: the index has to answer it outright. Column order
        // drifting out of sync with the query would not fail anything —
        // SQLite would just quietly go back to scanning every row.
        let plan: Vec<String> =
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "EXPLAIN QUERY PLAN {SETTER_TOTALS_SQL}"
            )))
            .fetch_all(&mut dbs.index_conn)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        assert!(
            plan.iter()
                .any(|step| step.contains("idx_item_data_placeholder_setter")),
            "setter totals must be answered from the covering index: {plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.contains("TEMP B-TREE")),
            "the index already yields setter order; grouping must not sort: {plan:?}"
        );
    }

    // Ensures data job deletion removes data_jobs row for a log.
    #[tokio::test]
    async fn delete_data_job_by_log_id_removes_job() {
        let mut dbs = setup_test_databases().await;
        sqlx::query("INSERT INTO data_jobs (id, completed) VALUES (5, 0)")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO data_log
                (id, job_id, start_time, end_time, type, setter, threshold, batch_size,
                 image_files, video_files, other_files, total_segments, errors, total_remaining,
                 data_load_time, inference_time, completed)
            VALUES
                (20, 5, '2024-01-01T00:00:00', '2024-01-01T00:10:00', 'tags', 'alpha', 0.5, 32,
                 1, 2, 3, 4, 5, 6, 1.5, 2.5, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        delete_data_job_by_log_id(&mut dbs.index_conn, 20)
            .await
            .unwrap();

        let remaining: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM data_jobs WHERE id = 5")
            .fetch_one(&mut dbs.index_conn)
            .await
            .unwrap();
        assert_eq!(remaining.0, 0);
    }
}
