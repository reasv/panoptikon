//! The per-job item-failure record and the job outcomes that go with it
//! (`data_job_failures`, `data_log.outcome`, `data_log.failure_reason`).
//!
//! This is the *other* extraction failure store, and the distinction between
//! the two is the whole point:
//!
//! - [`crate::db::extraction_errors`] is the **retry ledger**. A row there is
//!   a verdict about the media — the decoder rejected it, a dependency is
//!   missing, the item alone blew a limit — and it makes the work query skip
//!   the item. Only the pipeline component that actually examined the bytes
//!   may write one.
//! - This module is the **audit record of work that did not happen**. A row
//!   here says an item a job selected was not processed and nothing knows
//!   why: the inference server was down, the worker process died mid-request
//!   (run1 finding F7), a write failed. The item is untouched and is selected
//!   again on the next run, so nothing here may ever suppress anything — no
//!   query in the pipeline joins this table.
//!
//! Before it existed those failures left no trace at all: `data_log.errors`
//! counted them as an integer and `/api/jobs/data/failures` answered
//! `{"total": 0}` for every leg of run1 (finding Q8/T8).

use serde::Serialize;
use utoipa::ToSchema;

use crate::api_error::ApiError;
use crate::db::ledger::{audit_filter_sql, clamp_list_limit, read_audit_column, truncate_error};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// `stage` value for a failure raised while storing the model's output. The
/// prepare and inference stages reuse [`crate::db::extraction_errors`]'s
/// constants, which name the same two pipeline stages the retry ledger does;
/// only this third one is specific to failures that never settle a verdict
/// (an output write is always the gateway's own DB work, never the media's
/// fault, so the retry ledger can never carry it).
pub(crate) const STAGE_OUTPUT: &str = "output";

/// The `outcome` of a job that did everything it selected.
pub(crate) const OUTCOME_COMPLETED: &str = "completed";
/// The `outcome` of a job that ran to the end with items left unprocessed and
/// unexplained.
pub(crate) const OUTCOME_PARTIAL: &str = "partial";
/// The `outcome` of a job that stopped early.
pub(crate) const OUTCOME_FAILED: &str = "failed";
/// The `outcome` of a job that was cancelled, or whose process went away.
pub(crate) const OUTCOME_CANCELLED: &str = "cancelled";

/// The outcomes that put a job on the failures surface. `partial` is on the
/// list on purpose: it is exactly the case run1 found invisible.
pub(crate) const UNSUCCESSFUL_OUTCOMES: [&str; 3] =
    [OUTCOME_PARTIAL, OUTCOME_FAILED, OUTCOME_CANCELLED];

/// One item a job could not process, as the job hands it to the writer.
#[derive(Debug, Clone)]
pub(crate) struct JobItemFailureRecord {
    pub item_sha256: String,
    pub setter_name: String,
    /// `prepare`, `inference` or [`STAGE_OUTPUT`].
    pub stage: String,
    pub error: String,
    /// Whether this item's inference was re-submitted once after a worker
    /// death before failing again.
    pub requeued: bool,
    /// When the item failed, stamped by the job at that moment.
    ///
    /// Carried on the record rather than taken by the writer, because the
    /// writer runs once at the *end* of the job: a batch stamped on write
    /// would date every failure of a four-hour job to the minute it stopped,
    /// which is precisely the question this surface exists to answer ("when
    /// did the inference server go away?").
    pub occurred_at: String,
}

/// Resolves item and setter inside the statement, like the retry ledger's
/// upsert, so recording a failure costs one round trip and cannot write a row
/// that points at nothing.
///
/// `DO UPDATE` rather than `DO NOTHING`: an item is attempted once per job, so
/// a conflict means the same item failed twice in one job (a shape no current
/// path produces, but a future one might), and the *last* failure is the one
/// worth keeping.
const INSERT_SQL: &str = r#"
    INSERT INTO data_job_failures (
        job_id, item_id, setter_id, stage, error, requeued, occurred_at
    )
    SELECT ?, items.id, setters.id, ?, ?, ?, ?
    FROM items
    JOIN setters ON setters.name = ?
    WHERE items.sha256 = ?
    ON CONFLICT(job_id, item_id, setter_id) DO UPDATE SET
        stage = excluded.stage,
        error = excluded.error,
        requeued = excluded.requeued,
        occurred_at = excluded.occurred_at
"#;

/// Records a job's unexplained item failures, in one transaction.
///
/// Written as a batch at the end of the job rather than per item: a worker
/// death fails a whole in-flight window at once (1 542 items in run1), and a
/// writer round trip each would put that burst on the critical path of a
/// failure the job is already recovering from. Each record carries its own
/// `occurred_at`, stamped when the item failed rather than now, so batching
/// the write does not backdate the whole job's failures to its last second.
///
/// Returns how many rows were written. A record whose item or setter has gone
/// away writes nothing and is *not* an error: this is bookkeeping about work
/// that did not happen, and failing the job over an audit row would turn a
/// recoverable job into a lost one — the opposite of what the record is for.
/// The shortfall is logged.
pub(crate) async fn record_job_failures(
    conn: &mut sqlx::SqliteConnection,
    job_id: i64,
    records: &[JobItemFailureRecord],
) -> ApiResult<u64> {
    let mut written = 0u64;
    for record in records {
        let error = truncate_error(&record.error);
        let result = sqlx::query(INSERT_SQL)
            .bind(job_id)
            .bind(&record.stage)
            .bind(error.as_ref())
            .bind(i64::from(record.requeued))
            .bind(&record.occurred_at)
            .bind(&record.setter_name)
            .bind(&record.item_sha256)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "failed to record a job item failure");
                ApiError::internal("Failed to record job failures")
            })?;
        written += result.rows_affected();
    }
    if written < records.len() as u64 {
        tracing::warn!(
            job_id,
            recorded = written,
            expected = records.len(),
            "some job item failures had no item or setter row left to attach to"
        );
    }
    Ok(written)
}

/// Drops the failure rows of jobs that no longer exist. Called from
/// [`crate::db::extraction_write::remove_incomplete_jobs`], which runs at the
/// start of every extraction job, so retention follows job history exactly:
/// delete a job's history and its failure rows go with it.
pub(crate) async fn prune_orphan_job_failures(conn: &mut sqlx::SqliteConnection) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM data_job_failures
        WHERE job_id NOT IN (SELECT id FROM data_jobs)
        "#,
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to prune orphan job failures");
        ApiError::internal("Failed to prune job failures")
    })?;
    Ok(result.rows_affected())
}

/// Audit filters for the per-job failure list. Deliberately fewer than the
/// retry ledger's: `error_class` and `blocker` describe a *verdict*, and a row
/// here is by definition the absence of one.
#[derive(Debug, Clone, Default)]
pub(crate) struct JobFailureFilters {
    pub setter: Option<String>,
    pub stage: Option<String>,
    pub limit: Option<i64>,
    pub offset: i64,
}

/// One audit row: the failure joined with its item and a representative file.
#[derive(Debug, Clone)]
pub(crate) struct JobItemFailureRow {
    pub id: i64,
    pub job_id: i64,
    pub item_sha256: String,
    /// One of the paths the item is stored under, chosen the same way the
    /// retry ledger's audit list chooses it (available first, then the
    /// lexicographically smallest path), or `None` when every file has gone.
    pub path: Option<String>,
    pub mime_type: String,
    pub setter_name: String,
    pub stage: String,
    pub error: String,
    pub requeued: bool,
    pub occurred_at: String,
}

fn failure_filters(filters: &JobFailureFilters) -> (String, Vec<String>) {
    audit_filter_sql(
        &[
            ("setters.name", filters.setter.as_deref()),
            ("f.stage", filters.stage.as_deref()),
        ],
        // No mime filter on this surface: a row here is about the job, not
        // about a media class. The column name is required by the shared
        // builder, and passing `None` for the value makes it a no-op.
        "items.type",
        None,
    )
}

/// How many failures match these filters, ignoring the page window. Shares the
/// `WHERE` builder and the `FROM` with the list, so the count can never
/// describe a different set than the page it labels.
pub(crate) async fn count_job_failures(
    conn: &mut sqlx::SqliteConnection,
    filters: &JobFailureFilters,
) -> ApiResult<i64> {
    let (where_clause, binds) = failure_filters(filters);
    let sql = format!(
        "SELECT COUNT(*) FROM data_job_failures AS f \
         JOIN items ON items.id = f.item_id \
         JOIN setters ON setters.id = f.setter_id {where_clause}"
    );
    let mut query = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()));
    for bind in &binds {
        query = query.bind(bind);
    }
    query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to count job item failures");
        ApiError::internal("Failed to read job failures")
    })
}

/// The list statement, paged in an inner subselect before the representative
/// path is resolved — the same shape, and for the same reason, as the retry
/// ledger's audit list: a path subquery in the outer select list of a flat
/// query runs once per *matching* row, not once per row served.
fn list_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            paged.id AS id,
            paged.job_id AS job_id,
            paged.item_sha256 AS item_sha256,
            (
                SELECT files.path FROM files
                WHERE files.item_id = paged.item_id
                ORDER BY files.available DESC, files.path
                LIMIT 1
            ) AS path,
            paged.mime_type AS mime_type,
            paged.setter_name AS setter_name,
            paged.stage AS stage,
            paged.error AS error,
            paged.requeued AS requeued,
            paged.occurred_at AS occurred_at
        FROM (
            SELECT
                f.id AS id,
                f.job_id AS job_id,
                f.item_id AS item_id,
                items.sha256 AS item_sha256,
                items.type AS mime_type,
                setters.name AS setter_name,
                f.stage AS stage,
                f.error AS error,
                f.requeued AS requeued,
                f.occurred_at AS occurred_at
            FROM data_job_failures AS f
            JOIN items ON items.id = f.item_id
            JOIN setters ON setters.id = f.setter_id
            {where_clause}
            ORDER BY f.occurred_at DESC, f.id DESC
            LIMIT ? OFFSET ?
        ) AS paged
        ORDER BY paged.occurred_at DESC, paged.id DESC
        "#
    )
}

/// Lists a job's unexplained item failures for the audit surface, newest
/// first.
pub(crate) async fn list_job_failures(
    conn: &mut sqlx::SqliteConnection,
    filters: &JobFailureFilters,
) -> ApiResult<Vec<JobItemFailureRow>> {
    let (where_clause, binds) = failure_filters(filters);
    let sql = list_sql(&where_clause);
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for bind in &binds {
        query = query.bind(bind);
    }
    let rows = query
        .bind(clamp_list_limit(filters.limit))
        .bind(filters.offset.max(0))
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to list job item failures");
            ApiError::internal("Failed to read job failures")
        })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let requeued: i64 = read_audit_column(&row, "requeued")?;
        results.push(JobItemFailureRow {
            id: read_audit_column(&row, "id")?,
            job_id: read_audit_column(&row, "job_id")?,
            item_sha256: read_audit_column(&row, "item_sha256")?,
            path: read_audit_column(&row, "path")?,
            mime_type: read_audit_column(&row, "mime_type")?,
            setter_name: read_audit_column(&row, "setter_name")?,
            stage: read_audit_column(&row, "stage")?,
            error: read_audit_column(&row, "error")?,
            requeued: requeued != 0,
            occurred_at: read_audit_column(&row, "occurred_at")?,
        });
    }
    Ok(results)
}

/// A job that did not complete cleanly, as the failures surface serves it.
///
/// Every field run1 found missing on such a record is here: a real `end_time`
/// (T8 measured `end_time == start_time`), the count of items left unprocessed
/// and unexplained, the segment count, and the reason.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct FailedJobRecord {
    /// `data_log.id`, the id the job history and the job-data deletion
    /// endpoint use.
    pub log_id: i64,
    /// `data_jobs.id`, which is what a failure row carries. Null once the job
    /// row has been deleted.
    pub job_id: Option<i64>,
    pub setter: String,
    #[serde(rename = "type")]
    pub data_type: String,
    /// `partial`, `failed` or `cancelled`.
    pub outcome: String,
    /// Why, when the job knew. Null for a job whose process went away.
    pub failure_reason: Option<String>,
    pub start_time: String,
    /// When the job actually stopped.
    ///
    /// Every path that records an ending writes this afresh. The one
    /// exception is a job whose *process* died: the later sweep that finds
    /// the row deliberately leaves `end_time` alone, because for such a row
    /// "now" is when it was noticed, not when it stopped. Note also that the
    /// timestamps have one-second resolution, so `end_time == start_time` is
    /// a legitimate reading for a short job — `outcome` is what says whether
    /// an ending was recorded.
    pub end_time: String,
    /// Items attempted whose failure nothing explains — `errors` minus the
    /// subset backed by a retry-ledger verdict. This is the count that makes
    /// a job partial.
    ///
    /// It is derived from the job's own counters, which are exact, and is
    /// therefore the authority. The `job_failures` list can be *shorter*: it
    /// is capped at 10 000 rows per job, and its rows are pruned once the job's
    /// `data_jobs` row is gone — which under `atomic_extraction_jobs` (off by
    /// default) is at the start of the next extraction job for every job that
    /// did not finish, because that mode deletes unfinished job rows outright.
    /// A shortfall therefore means the listing was truncated or aged out,
    /// never that the count is wrong.
    pub failed_items: i64,
    /// Every item failure the job counted, verdicts included.
    pub errors: i64,
    /// The subset of `errors` that is a recorded verdict about the media.
    pub input_errors: i64,
    pub total_segments: i64,
    /// Items still matching the job's work query when it stopped.
    pub total_remaining: i64,
}

const FAILED_JOBS_FROM: &str = "FROM data_log WHERE outcome IN (?, ?, ?)";

/// How many unsuccessful jobs there are, ignoring the page window.
pub(crate) async fn count_failed_jobs(conn: &mut sqlx::SqliteConnection) -> ApiResult<i64> {
    let sql = format!("SELECT COUNT(*) {FAILED_JOBS_FROM}");
    let mut query = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()));
    for outcome in UNSUCCESSFUL_OUTCOMES {
        query = query.bind(outcome);
    }
    query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to count failed jobs");
        ApiError::internal("Failed to read job failures")
    })
}

/// Lists the unsuccessful jobs, newest first.
pub(crate) async fn list_failed_jobs(
    conn: &mut sqlx::SqliteConnection,
    limit: Option<i64>,
    offset: i64,
) -> ApiResult<Vec<FailedJobRecord>> {
    let sql = format!(
        r#"
        SELECT
            id,
            job_id,
            setter,
            type,
            outcome,
            failure_reason,
            start_time,
            end_time,
            MAX(errors - input_errors, 0) AS failed_items,
            errors,
            input_errors,
            total_segments,
            total_remaining
        {FAILED_JOBS_FROM}
        ORDER BY start_time DESC, id DESC
        LIMIT ? OFFSET ?
        "#
    );
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
    for outcome in UNSUCCESSFUL_OUTCOMES {
        query = query.bind(outcome);
    }
    let rows = query
        .bind(clamp_list_limit(limit))
        .bind(offset.max(0))
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to list failed jobs");
            ApiError::internal("Failed to read job failures")
        })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        results.push(FailedJobRecord {
            log_id: read_audit_column(&row, "id")?,
            job_id: read_audit_column(&row, "job_id")?,
            setter: read_audit_column(&row, "setter")?,
            data_type: read_audit_column(&row, "type")?,
            outcome: read_audit_column(&row, "outcome")?,
            failure_reason: read_audit_column(&row, "failure_reason")?,
            start_time: read_audit_column(&row, "start_time")?,
            end_time: read_audit_column(&row, "end_time")?,
            failed_items: read_audit_column(&row, "failed_items")?,
            errors: read_audit_column(&row, "errors")?,
            input_errors: read_audit_column(&row, "input_errors")?,
            total_segments: read_audit_column(&row, "total_segments")?,
            total_remaining: read_audit_column(&row, "total_remaining")?,
        });
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::setup_test_databases;

    async fn seed(conn: &mut sqlx::SqliteConnection) {
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_one', 'md5_one', 'image/png', '2026-01-01T00:00:00'),
                (2, 'sha_two', 'md5_two', 'video/mp4', '2026-01-01T00:00:00')
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'test/clip'), (2, 'test/tagger')")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) \
             VALUES (1, '2026-01-01T00:00:00', '/media')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES (1, 'sha_one', 1, '/media/one.png', 'one.png', '2026-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO data_jobs (id, completed) VALUES (7, 1), (8, 1)")
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    /// When the job says the item failed — deliberately not "now", so the
    /// round trip proves the record's own stamp is what is stored rather than
    /// the moment of the batched write.
    const FAILED_AT: &str = "2026-09-04T11:22:33";

    fn record(sha256: &str, stage: &str, requeued: bool) -> JobItemFailureRecord {
        JobItemFailureRecord {
            item_sha256: sha256.to_string(),
            setter_name: "test/clip".to_string(),
            stage: stage.to_string(),
            error: "inferio worker test/clip failed fatally: early eof".to_string(),
            requeued,
            occurred_at: FAILED_AT.to_string(),
        }
    }

    /// The write path, the audit read and the representative-path join, on one
    /// job. `sha_two` has no file row, which is the "every file has gone away"
    /// case the retry ledger's audit surface also has to survive.
    #[tokio::test]
    async fn recorded_failures_come_back_with_their_item_and_path() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        let written = record_job_failures(
            conn,
            7,
            &[
                record("sha_one", "inference", true),
                record("sha_two", STAGE_OUTPUT, false),
            ],
        )
        .await
        .unwrap();
        assert_eq!(written, 2);

        let filters = JobFailureFilters::default();
        assert_eq!(count_job_failures(conn, &filters).await.unwrap(), 2);
        let rows = list_job_failures(conn, &filters).await.unwrap();
        assert_eq!(rows.len(), 2);
        let one = rows
            .iter()
            .find(|row| row.item_sha256 == "sha_one")
            .expect("sha_one is listed");
        assert_eq!(one.job_id, 7);
        assert_eq!(one.path.as_deref(), Some("/media/one.png"));
        assert_eq!(one.mime_type, "image/png");
        assert_eq!(one.setter_name, "test/clip");
        assert_eq!(one.stage, "inference");
        assert!(one.requeued, "the re-queue must be visible in the audit");
        assert!(one.error.contains("failed fatally"));
        assert_eq!(
            one.occurred_at, FAILED_AT,
            "the stored time is the record's own, not the moment of the batched write"
        );

        let two = rows
            .iter()
            .find(|row| row.item_sha256 == "sha_two")
            .expect("sha_two is listed");
        assert_eq!(two.path, None, "an item with no files still lists");
        assert!(!two.requeued);

        let by_stage = JobFailureFilters {
            stage: Some("inference".to_string()),
            ..Default::default()
        };
        assert_eq!(count_job_failures(conn, &by_stage).await.unwrap(), 1);
        assert_eq!(list_job_failures(conn, &by_stage).await.unwrap().len(), 1);
        let by_setter = JobFailureFilters {
            setter: Some("test/tagger".to_string()),
            ..Default::default()
        };
        assert_eq!(count_job_failures(conn, &by_setter).await.unwrap(), 0);

        // A record whose item is gone writes nothing and is not an error: an
        // audit row must never be able to fail a job.
        let missing = JobItemFailureRecord {
            item_sha256: "sha_gone".to_string(),
            ..record("sha_one", "inference", false)
        };
        assert_eq!(record_job_failures(conn, 7, &[missing]).await.unwrap(), 0);
    }

    /// A row here must never outlive the job history that explains it:
    /// deleting the job row prunes it.
    #[tokio::test]
    async fn failure_rows_are_pruned_with_their_job() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        record_job_failures(conn, 7, &[record("sha_one", "inference", false)])
            .await
            .unwrap();
        record_job_failures(conn, 8, &[record("sha_two", "prepare", false)])
            .await
            .unwrap();
        assert_eq!(prune_orphan_job_failures(conn).await.unwrap(), 0);

        sqlx::query("DELETE FROM data_jobs WHERE id = 8")
            .execute(&mut *conn)
            .await
            .unwrap();
        assert_eq!(prune_orphan_job_failures(conn).await.unwrap(), 1);
        let rows = list_job_failures(conn, &JobFailureFilters::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].job_id, 7);
    }

    /// The unsuccessful-job list, and the fields run1 found missing: a real
    /// `end_time`, the unexplained-failure count and the reason. A `completed`
    /// job must not appear at all.
    #[tokio::test]
    async fn the_failed_job_list_carries_the_counts_and_the_reason() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        sqlx::query(
            r#"
            INSERT INTO data_log (
                id, job_id, start_time, end_time, type, setter, batch_size,
                total_segments, errors, input_errors, total_remaining,
                completed, outcome, failure_reason
            )
            VALUES
                (1, 7, '2026-09-04T10:00:00', '2026-09-04T10:20:00', 'clip',
                 'test/clip', 0, 400, 12, 2, 12, 1, 'partial',
                 '10 of 400 attempted items could not be processed'),
                (2, 8, '2026-09-04T11:00:00', '2026-09-04T11:00:05', 'tags',
                 'test/tagger', 0, 0, 0, 0, 500, 0, 'failed',
                 'Inference is unavailable: test/tagger is in a load-failure cooldown'),
                (3, NULL, '2026-09-04T09:00:00', '2026-09-04T09:30:00', 'clip',
                 'test/clip', 0, 900, 0, 0, 0, 1, 'completed', NULL)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        assert_eq!(count_failed_jobs(conn).await.unwrap(), 2);
        let jobs = list_failed_jobs(conn, None, 0).await.unwrap();
        assert_eq!(jobs.len(), 2);
        // Newest first.
        assert_eq!(jobs[0].log_id, 2);
        assert_eq!(jobs[0].outcome, OUTCOME_FAILED);
        assert!(
            jobs[0]
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("load-failure cooldown")
        );
        assert_ne!(
            jobs[0].end_time, jobs[0].start_time,
            "a failed job must carry a real end_time (run1 finding T8)"
        );

        let partial = &jobs[1];
        assert_eq!(partial.outcome, OUTCOME_PARTIAL);
        assert_eq!(partial.failed_items, 10, "errors minus recorded verdicts");
        assert_eq!(partial.errors, 12);
        assert_eq!(partial.input_errors, 2);
        assert_eq!(partial.total_segments, 400);
        assert_eq!(partial.job_id, Some(7));
    }
}
