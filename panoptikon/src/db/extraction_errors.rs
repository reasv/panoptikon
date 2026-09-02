//! The extraction failure ledger (`item_extraction_errors`).
//!
//! See docs/failed-media-retry-design.md. A row means "this setter has already
//! rejected this item's media, and re-attempting it is wasted work" — never
//! anything about search results or data reads. Losing a row costs one
//! re-attempt, never correctness, so every writer here is advisory for
//! correctness and authoritative only for scheduling.
//!
//! Transient failures never reach the table: the record carries an
//! [`ApiErrorKind`], and the upsert refuses the one variant whose
//! `persisted_class()` is `None`.
//!
//! `attempts` counts *runs that saw the failure*, not verdicts: a class change
//! refreshes the classification but never resets the counter. Resetting would
//! livelock a pair whose verdict alternates between runs (an item that fails
//! `input` on one pass and `resource` on the next would never reach
//! `skip_after` and would be retried forever).

use crate::api_error::{ApiError, ApiErrorKind, Blocker};
use crate::db::extraction_write::current_iso_timestamp;
use crate::db::ledger::{
    LedgerTable, audit_filter_sql, clamp_list_limit, delete_blocked_rows,
    list_distinct_blockers_in, read_audit_column,
};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Which table the shared ledger helpers operate on for this half.
const TABLE: LedgerTable = LedgerTable::ItemExtractionErrors;

/// `stage` value for a failure raised while the gateway prepared the item's
/// bytes (file read, decode, ffmpeg, pdfium, the HTML renderer).
pub(crate) const STAGE_PREPARE: &str = "prepare";

/// `stage` value for a failure raised by the inference worker itself.
pub(crate) const STAGE_INFERENCE: &str = "inference";

/// One ledger write. Owned fields so the writer actor's message can carry it.
#[derive(Debug, Clone)]
pub(crate) struct ExtractionErrorRecord {
    pub item_sha256: String,
    pub setter_name: String,
    /// [`STAGE_PREPARE`] or [`STAGE_INFERENCE`].
    pub stage: String,
    /// The classification itself, not its persisted strings: `error_class` and
    /// `blocker` are derived in [`upsert_extraction_error`], so an
    /// inconsistent pair (a `blocked` row without a blocker) is
    /// unrepresentable.
    pub kind: ApiErrorKind,
    pub error: String,
    pub skip_after: i64,
    /// The job that saw this failure, which is what dedups `attempts` (see
    /// [`UPSERT_SQL`]). `None` is allowed but blunt: two consecutive job-less
    /// writes are indistinguishable and increment only once, so a caller with
    /// no job identity must use `skip_after = 1` or its rows will never reach
    /// their threshold.
    pub job_id: Option<i64>,
    // `mime_type` is deliberately absent: the upsert reads `items.type` in the
    // same statement, so the denormalized copy cannot drift from the item.
}

/// Resolves item and setter inside the statement (the `add_item_data`
/// pattern), so a failure write costs exactly one roundtrip. `mime_type` is
/// denormalized from the joined `items` row rather than passed in.
///
/// `attempts` increments only when the job changed: an item retried inside the
/// same job — which the isolation retry does — must not burn its confirmation
/// attempt twice. `IS NOT` is SQLite's null-safe inequality, so a row written
/// by a job-less caller still increments once a real job touches it. A class
/// change refreshes the verdict but does *not* reset the count; see the module
/// doc for why.
const UPSERT_SQL: &str = r#"
    INSERT INTO item_extraction_errors (
        item_id, setter_id, stage, error_class, blocker, mime_type, error,
        skip_after, attempts, last_job_id, first_seen, last_seen
    )
    SELECT items.id, setters.id, ?, ?, ?, items.type, ?, ?, 1, ?, ?, ?
    FROM items
    JOIN setters ON setters.name = ?
    WHERE items.sha256 = ?
    ON CONFLICT(item_id, setter_id) DO UPDATE SET
        attempts = CASE
            WHEN item_extraction_errors.last_job_id IS NOT excluded.last_job_id
                THEN item_extraction_errors.attempts + 1
            ELSE item_extraction_errors.attempts
        END,
        stage = excluded.stage,
        error_class = excluded.error_class,
        blocker = excluded.blocker,
        mime_type = excluded.mime_type,
        error = excluded.error,
        skip_after = excluded.skip_after,
        last_job_id = excluded.last_job_id,
        last_seen = excluded.last_seen
"#;

/// Records (or re-records) a failure. Returns an error when the item or the
/// setter does not exist — the row would be silently dropped otherwise, and
/// the caller counts a failed ledger write as systemic rather than
/// soft-completing the job as "all corrupt media".
pub(crate) async fn upsert_extraction_error(
    conn: &mut sqlx::SqliteConnection,
    record: &ExtractionErrorRecord,
) -> ApiResult<()> {
    let Some(class) = record.kind.persisted_class() else {
        tracing::error!(
            sha256 = %record.item_sha256,
            setter = %record.setter_name,
            "refused to persist a transient extraction failure"
        );
        return Err(ApiError::internal("transient failures are not persisted"));
    };
    let blocker = record.kind.blocker().map(|blocker| blocker.as_str());
    let error = crate::db::ledger::truncate_error(&record.error);

    let now = current_iso_timestamp();
    let result = sqlx::query(UPSERT_SQL)
        .bind(&record.stage)
        .bind(class)
        .bind(blocker)
        .bind(error.as_ref())
        .bind(record.skip_after)
        .bind(record.job_id)
        .bind(&now)
        .bind(&now)
        .bind(&record.setter_name)
        .bind(&record.item_sha256)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to upsert extraction error");
            ApiError::internal("Failed to record extraction failure")
        })?;

    if result.rows_affected() == 0 {
        tracing::error!(
            sha256 = %record.item_sha256,
            setter = %record.setter_name,
            "extraction failure has no item or setter row to attach to"
        );
        return Err(ApiError::internal("Failed to record extraction failure"));
    }
    Ok(())
}

/// The success path: an item this setter can now process owes no ledger row.
/// Returns how many rows went away (almost always zero, which is why callers
/// gate this on the job having seen any rows for the setter at all).
pub(crate) async fn delete_extraction_error(
    conn: &mut sqlx::SqliteConnection,
    item_sha256: &str,
    setter_name: &str,
) -> ApiResult<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM item_extraction_errors
        WHERE item_id = (SELECT id FROM items WHERE sha256 = ?)
          AND setter_id = (SELECT id FROM setters WHERE name = ?)
        "#,
    )
    .bind(item_sha256)
    .bind(setter_name)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to delete extraction error");
        ApiError::internal("Failed to clear extraction failure")
    })?;
    Ok(result.rows_affected())
}

/// Auto-heal: clears the `blocked` rows of every dependency that now binds,
/// across all setters, so those items become selectable in the same run. The
/// scan ledger has the same pair against its own table; both go through
/// [`crate::db::ledger`].
pub(crate) async fn delete_blocked_errors(
    conn: &mut sqlx::SqliteConnection,
    blockers: &[Blocker],
) -> ApiResult<u64> {
    delete_blocked_rows(conn, TABLE, blockers).await
}

/// The dependencies the ledger is currently waiting on — usually none. Only
/// these backends get probed at job start, so a run never loads a library it
/// has no use for.
pub(crate) async fn list_distinct_blockers(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<Blocker>> {
    list_distinct_blockers_in(conn, TABLE).await
}

/// Every item sha256 this setter currently has a ledger row for, active or
/// not. Read once at job start; the success path then deletes only for the
/// items actually in the set, so a healthy library pays no writer round-trip
/// (and no search-cache epoch bump) per successful item.
///
/// Normally tiny — the empty set is the common answer, and even a bad library
/// has orders of magnitude fewer failures than items — so materializing it is
/// cheaper than the per-item delete a plain "are there any rows?" boolean
/// would force onto every success as soon as one sub-threshold row exists.
///
/// Deliberately not restricted to active rows (`attempts >= skip_after`): the
/// work query already excludes every item whose verdict is active, so the only
/// rows a successful item can ever own are the sub-threshold ones — exactly
/// the rows an active-only query ignores. Gating the success-path delete on an
/// active set would leave an unconfirmed row (one transient SMB blip) in place
/// forever, and a second blip months later would suppress a healthy file
/// permanently.
pub(crate) async fn list_error_sha256s_for_setter(
    conn: &mut sqlx::SqliteConnection,
    setter_name: &str,
) -> ApiResult<std::collections::HashSet<String>> {
    let rows: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT items.sha256
        FROM item_extraction_errors
        JOIN setters ON setters.id = item_extraction_errors.setter_id
        JOIN items ON items.id = item_extraction_errors.item_id
        WHERE setters.name = ?
        "#,
    )
    .bind(setter_name)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, "failed to list extraction errors for setter");
        ApiError::internal("Failed to read extraction failures")
    })?;
    Ok(rows.into_iter().collect())
}

// Everything from here to the end of the module is the *audit half* — the
// list query, its count twin, their filters and their row type. It is what
// `GET /api/jobs/data/failures` serves; the pipeline half above never reads it.

/// Audit filters. Every field is independently optional, matching the retry
/// directives' targeting (class, stage, mime prefix, setter).
#[derive(Debug, Clone, Default)]
pub(crate) struct ExtractionErrorFilters {
    pub setter: Option<String>,
    pub error_class: Option<String>,
    pub stage: Option<String>,
    /// Matched as a prefix of `mime_type`, e.g. `image/`. An empty string is
    /// treated as no filter.
    pub mime_prefix: Option<String>,
    /// Clamped by [`clamp_list_limit`]; `None` means its default. The clamping
    /// is why this is not a plain `i64`: a defaulted `0` would silently return
    /// nothing.
    pub limit: Option<i64>,
    /// Negative values are clamped to zero (SQLite treats a negative OFFSET as
    /// zero anyway, but the intent is explicit here).
    pub offset: i64,
}

/// One audit row: the ledger joined with its item and a representative file.
#[derive(Debug, Clone)]
pub(crate) struct ExtractionErrorRow {
    pub id: i64,
    pub item_sha256: String,
    /// One of the paths the item is stored under, or `None` for an item whose
    /// files have all gone away. An item can have any number of `files` rows
    /// (duplicates across folders) and the ledger keys on the *item*, so the
    /// audit surface shows one representative: an available file if there is
    /// one, then the lexicographically smallest path, so the choice is stable
    /// between pages instead of depending on the plan.
    pub path: Option<String>,
    pub setter_name: String,
    pub stage: String,
    pub error_class: String,
    pub blocker: Option<String>,
    pub mime_type: String,
    pub error: String,
    pub skip_after: i64,
    pub attempts: i64,
    pub last_job_id: Option<i64>,
    pub first_seen: String,
    pub last_seen: String,
}

/// The clause both audit reads share. The column names are literals from this
/// module; only the bound values come from the request.
fn audit_filters(filters: &ExtractionErrorFilters) -> (String, Vec<String>) {
    audit_filter_sql(
        &[
            ("setters.name", filters.setter.as_deref()),
            ("e.error_class", filters.error_class.as_deref()),
            ("e.stage", filters.stage.as_deref()),
        ],
        "e.mime_type",
        filters.mime_prefix.as_deref(),
    )
}

/// How many failures match these filters, ignoring the page window — the
/// denominator the paginated audit surface needs. Kept next to
/// [`list_extraction_errors`] and sharing its `WHERE` builder, so the count
/// can never describe a different set than the page it labels.
pub(crate) async fn count_extraction_errors(
    conn: &mut sqlx::SqliteConnection,
    filters: &ExtractionErrorFilters,
) -> ApiResult<i64> {
    let (where_clause, binds) = audit_filters(filters);
    // Both joins, so the `FROM` here is literally the list's: a count whose
    // source differs from the page it labels is a bug waiting for the first
    // join that stops being row-preserving.
    let sql = format!(
        "SELECT COUNT(*) FROM item_extraction_errors AS e \
         JOIN items ON items.id = e.item_id \
         JOIN setters ON setters.id = e.setter_id {where_clause}"
    );
    let mut query = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()));
    for bind in &binds {
        query = query.bind(bind);
    }
    query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to count extraction errors");
        ApiError::internal("Failed to read extraction failures")
    })
}

/// The list statement, built around `where_clause`. Split out from the query
/// so its *shape* can be pinned by a test — the nesting below is the whole
/// point of the statement and reads like a stylistic choice otherwise.
///
/// The ledger is paged in an inner subselect that touches nothing but the
/// ledger and its setter join, and the item join plus the representative-path
/// lookup happen *outside* it, over the page.
///
/// This is not cosmetic. The `ORDER BY` needs a sorter, and SQLite fills a
/// sorter with whole result rows — every expression in the `SELECT` list is
/// evaluated *before* the `LIMIT` applies. With the path subquery in the outer
/// select list of a flat query it therefore ran once per *matching* row, not
/// once per row served: 500 matching rows cost 500 file lookups to render a
/// 50-row page. Nested, it runs at most `limit` times. The `LIMIT` inside the
/// subselect also blocks the flattening optimizer from undoing this (a
/// subquery with `LIMIT` cannot be flattened into a joining outer query).
///
/// The path stays a correlated subquery rather than a join for a separate
/// reason: an item with three files must stay one audit row.
///
/// Mixing numbered and bare placeholders misbinds parameters under sqlx, so
/// every placeholder here must stay unnumbered.
fn list_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            paged.id AS id,
            items.sha256 AS item_sha256,
            (
                SELECT files.path FROM files
                WHERE files.item_id = paged.item_id
                ORDER BY files.available DESC, files.path
                LIMIT 1
            ) AS path,
            paged.setter_name AS setter_name,
            paged.stage AS stage,
            paged.error_class AS error_class,
            paged.blocker AS blocker,
            paged.mime_type AS mime_type,
            paged.error AS error,
            paged.skip_after AS skip_after,
            paged.attempts AS attempts,
            paged.last_job_id AS last_job_id,
            paged.first_seen AS first_seen,
            paged.last_seen AS last_seen
        FROM (
            SELECT
                e.id AS id,
                e.item_id AS item_id,
                setters.name AS setter_name,
                e.stage AS stage,
                e.error_class AS error_class,
                e.blocker AS blocker,
                e.mime_type AS mime_type,
                e.error AS error,
                e.skip_after AS skip_after,
                e.attempts AS attempts,
                e.last_job_id AS last_job_id,
                e.first_seen AS first_seen,
                e.last_seen AS last_seen
            FROM item_extraction_errors AS e
            JOIN setters ON setters.id = e.setter_id
            {where_clause}
            ORDER BY e.last_seen DESC, e.id DESC
            LIMIT ? OFFSET ?
        ) AS paged
        JOIN items ON items.id = paged.item_id
        ORDER BY paged.last_seen DESC, paged.id DESC
        "#
    )
}

/// Lists failures for the audit surface, newest first.
pub(crate) async fn list_extraction_errors(
    conn: &mut sqlx::SqliteConnection,
    filters: &ExtractionErrorFilters,
) -> ApiResult<Vec<ExtractionErrorRow>> {
    let (where_clause, binds) = audit_filters(filters);
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
            tracing::error!(error = %err, "failed to list extraction errors");
            ApiError::internal("Failed to read extraction failures")
        })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        results.push(ExtractionErrorRow {
            id: read_audit_column(&row, "id")?,
            item_sha256: read_audit_column(&row, "item_sha256")?,
            path: read_audit_column(&row, "path")?,
            setter_name: read_audit_column(&row, "setter_name")?,
            stage: read_audit_column(&row, "stage")?,
            error_class: read_audit_column(&row, "error_class")?,
            blocker: read_audit_column(&row, "blocker")?,
            mime_type: read_audit_column(&row, "mime_type")?,
            error: read_audit_column(&row, "error")?,
            skip_after: read_audit_column(&row, "skip_after")?,
            attempts: read_audit_column(&row, "attempts")?,
            last_job_id: read_audit_column(&row, "last_job_id")?,
            first_seen: read_audit_column(&row, "first_seen")?,
            last_seen: read_audit_column(&row, "last_seen")?,
        });
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::extraction_write::delete_setter_by_name;
    use crate::db::ledger::{CLASS_BLOCKED, CLASS_INPUT, CLASS_RESOURCE, MAX_ERROR_BYTES};
    use crate::db::migrations::{migrate_databases_on_disk, setup_test_databases};
    use crate::test_utils::test_data_dir;

    const FFMPEG: ApiErrorKind = ApiErrorKind::Blocked {
        blocker: Blocker::Ffmpeg,
    };
    const PDFIUM: ApiErrorKind = ApiErrorKind::Blocked {
        blocker: Blocker::Pdfium,
    };

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
    }

    /// Files for the seeded items. `sha_one` deliberately has three of them —
    /// an unavailable copy, and two available ones — so the audit list has to
    /// prove it still returns one row and picks the representative path
    /// deterministically.
    async fn seed_files(conn: &mut sqlx::SqliteConnection) {
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) VALUES (1, '2026-01-01T00:00:00', 'C:/m')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (id, sha256, item_id, path, filename, last_modified, scan_id, available)
            VALUES
                (1, 'sha_one', 1, 'C:/m/aaa.png', 'aaa.png', '2026-01-01T00:00:00', 1, 0),
                (2, 'sha_one', 1, 'C:/m/zzz.png', 'zzz.png', '2026-01-01T00:00:00', 1, 1),
                (3, 'sha_one', 1, 'C:/m/mmm.png', 'mmm.png', '2026-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
    }

    fn record(sha256: &str, kind: ApiErrorKind, job_id: Option<i64>) -> ExtractionErrorRecord {
        ExtractionErrorRecord {
            item_sha256: sha256.to_string(),
            setter_name: "test/clip".to_string(),
            stage: STAGE_PREPARE.to_string(),
            kind,
            error: "decode failed".to_string(),
            skip_after: 1,
            job_id,
        }
    }

    async fn row(conn: &mut sqlx::SqliteConnection, sha256: &str) -> (i64, String, String, String) {
        sqlx::query_as(
            r#"
            SELECT e.attempts, e.error_class, e.first_seen, e.last_seen
            FROM item_extraction_errors AS e
            JOIN items ON items.id = e.item_id
            WHERE items.sha256 = ?
            "#,
        )
        .bind(sha256)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
    }

    async fn count(conn: &mut sqlx::SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM item_extraction_errors")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }

    // The migration has to land on a real file-backed database too: the
    // in-memory harness the other tests use runs the same migrator, but only
    // the on-disk path is what an upgrading user actually executes.
    #[tokio::test]
    async fn migration_creates_the_ledger_and_the_data_log_column() {
        let _test_env = test_data_dir();
        migrate_databases_on_disk(Some("ledger_migration"), Some("ledger_migration_user"))
            .await
            .expect("migrate test databases");
        let mut conn = crate::db::open_index_db_write_no_user_data("ledger_migration")
            .await
            .unwrap();

        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'item_extraction_errors' AND name IS NOT NULL \
             ORDER BY name",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(
            indexes.contains(&"idx_item_extraction_errors_setter".to_string())
                && indexes.contains(&"idx_item_extraction_errors_class".to_string()),
            "both design indexes must exist: {indexes:?}"
        );

        let input_errors: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('data_log') WHERE name = 'input_errors'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(input_errors, 1, "data_log must carry the input split");
    }

    // The table's own guards: a `blocked` row without its dependency (or any
    // other row carrying one) is a lie the auto-heal probe would act on, and a
    // `skip_after` below 1 would suppress an item that never failed twice.
    #[tokio::test]
    async fn migration_check_constraints_reject_inconsistent_rows() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        let insert = |class: &'static str, blocker: Option<&'static str>, skip_after: i64| {
            sqlx::query(
                "INSERT INTO item_extraction_errors (item_id, setter_id, stage, error_class, \
                 blocker, mime_type, error, skip_after, attempts, first_seen, last_seen) \
                 VALUES (1, 1, 'prepare', ?, ?, 'image/png', 'x', ?, 1, 'now', 'now')",
            )
            .bind(class)
            .bind(blocker)
            .bind(skip_after)
        };

        assert!(
            insert(CLASS_BLOCKED, None, 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "a blocked row must name its dependency"
        );
        assert!(
            insert(CLASS_INPUT, Some("ffmpeg"), 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "only blocked rows carry a blocker"
        );
        assert!(
            insert(CLASS_INPUT, None, 0)
                .execute(&mut *conn)
                .await
                .is_err(),
            "skip_after must be at least one attempt"
        );
        assert!(
            insert(CLASS_INPUT, None, 1)
                .execute(&mut *conn)
                .await
                .is_ok()
        );
    }

    // The attempt accounting is the whole confirmation-threshold mechanism:
    // one increment per job at most, and `first_seen` records when the file
    // first went bad.
    #[tokio::test]
    async fn upsert_counts_one_attempt_per_job() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(10)))
            .await
            .unwrap();
        let (attempts, class, first_seen, _) = row(conn, "sha_one").await;
        assert_eq!((attempts, class.as_str()), (1, CLASS_INPUT));

        // Same job: the isolation retry re-attempts an item inside one job,
        // and that must not consume its confirmation attempt.
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(10)))
            .await
            .unwrap();
        let (attempts, _, _, _) = row(conn, "sha_one").await;
        assert_eq!(
            attempts, 1,
            "a second failure in the same job is one attempt"
        );

        // A later job confirms it.
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(11)))
            .await
            .unwrap();
        let (attempts, _, unchanged_first_seen, _) = row(conn, "sha_one").await;
        assert_eq!(attempts, 2);
        assert_eq!(
            unchanged_first_seen, first_seen,
            "first_seen records when the file first failed"
        );

        // A different verdict refreshes the classification but keeps counting:
        // a pair whose verdict alternates between runs must still reach its
        // threshold instead of being retried forever.
        let mut changed = record("sha_one", ApiErrorKind::Resource, Some(12));
        changed.error = "batch-1 OOM".to_string();
        upsert_extraction_error(conn, &changed).await.unwrap();
        let (attempts, class, _, _) = row(conn, "sha_one").await;
        assert_eq!((attempts, class.as_str()), (3, CLASS_RESOURCE));

        // One row per (item, setter); a second setter gets its own attempt.
        let mut other_setter = record("sha_one", ApiErrorKind::Input, Some(12));
        other_setter.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &other_setter).await.unwrap();
        assert_eq!(count(conn).await, 2);
    }

    // `last_job_id` is nullable and the dedup comparison is `IS NOT`, SQLite's
    // null-safe inequality: a plain `<>` yields NULL against a job-less row
    // and would silently stop counting.
    #[tokio::test]
    async fn upsert_counts_across_a_null_job_id() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, None))
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(7)))
            .await
            .unwrap();
        let (attempts, _, _, _) = row(conn, "sha_one").await;
        assert_eq!(attempts, 2, "a real job after a job-less write counts");

        // Two job-less writes in a row are indistinguishable, hence the
        // skip_after = 1 requirement documented on the field.
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, None))
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, None))
            .await
            .unwrap();
        let (attempts, _, _, _) = row(conn, "sha_one").await;
        assert_eq!(attempts, 3, "consecutive job-less writes count once");
    }

    // `last_seen` moves with every write; the audit surface shows it, and the
    // list is ordered by it. The denormalized `mime_type` and the `blocker`
    // must follow the verdict too.
    #[tokio::test]
    async fn upsert_refreshes_the_mutable_columns() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE item_extraction_errors \
             SET last_seen = '2000-01-01T00:00:00', mime_type = 'stale/type'",
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        let mut updated = record("sha_one", PDFIUM, Some(2));
        updated.stage = STAGE_INFERENCE.to_string();
        updated.error = "worker rejected the payload".to_string();
        updated.skip_after = 2;
        upsert_extraction_error(conn, &updated).await.unwrap();

        let (stage, error, skip_after, last_job_id, last_seen, mime_type, blocker): (
            String,
            String,
            i64,
            Option<i64>,
            String,
            String,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT stage, error, skip_after, last_job_id, last_seen, mime_type, blocker \
             FROM item_extraction_errors",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(stage, STAGE_INFERENCE);
        assert_eq!(error, "worker rejected the payload");
        assert_eq!(skip_after, 2);
        assert_eq!(last_job_id, Some(2));
        assert_ne!(last_seen, "2000-01-01T00:00:00");
        assert_eq!(
            mime_type, "image/png",
            "mime_type is re-derived from the item"
        );
        assert_eq!(blocker.as_deref(), Some("pdfium"));
    }

    // A transient failure has no ledger row by construction, and the type
    // system cannot express "record this Generic error" without going through
    // here, so this is the single guard.
    #[tokio::test]
    async fn upsert_refuses_a_transient_verdict() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        assert!(
            upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Generic, Some(1)))
                .await
                .is_err()
        );
        assert_eq!(count(conn).await, 0);
    }

    // The `error` column is audit text and a worker traceback can be huge, so
    // it is clamped once, here, on a char boundary.
    #[tokio::test]
    async fn upsert_truncates_an_oversized_message() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        // A multi-byte char straddling the cut proves the boundary walk.
        let mut huge = record("sha_one", ApiErrorKind::Input, Some(1));
        huge.error = "é".repeat(MAX_ERROR_BYTES);
        upsert_extraction_error(conn, &huge).await.unwrap();

        let stored: String = sqlx::query_scalar("SELECT error FROM item_extraction_errors")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(stored.ends_with('…'));
        assert!(stored.len() <= MAX_ERROR_BYTES + '…'.len_utf8());
        assert!(
            stored.trim_end_matches('…').chars().all(|ch| ch == 'é'),
            "the cut must land on a char boundary"
        );

        // A message that fits is stored verbatim.
        upsert_extraction_error(conn, &record("sha_two", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let stored: String = sqlx::query_scalar(
            "SELECT e.error FROM item_extraction_errors AS e \
             JOIN items ON items.id = e.item_id WHERE items.sha256 = 'sha_two'",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(stored, "decode failed");
    }

    // A write that silently matched nothing would let a DB-level problem
    // soft-complete a job as "all corrupt media", so it must be an error.
    #[tokio::test]
    async fn upsert_fails_when_the_item_or_setter_is_missing() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        assert!(
            upsert_extraction_error(conn, &record("sha_absent", ApiErrorKind::Input, Some(1)))
                .await
                .is_err(),
            "an unknown item must not be recorded silently"
        );

        let mut unknown_setter = record("sha_one", ApiErrorKind::Input, Some(1));
        unknown_setter.setter_name = "test/absent".to_string();
        assert!(
            upsert_extraction_error(conn, &unknown_setter)
                .await
                .is_err(),
            "an unknown setter must not be recorded silently"
        );
        assert_eq!(count(conn).await, 0);
    }

    #[tokio::test]
    async fn delete_clears_only_the_pair_that_succeeded() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let mut other_setter = record("sha_one", ApiErrorKind::Input, Some(1));
        other_setter.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &other_setter).await.unwrap();

        let deleted = delete_extraction_error(conn, "sha_one", "test/clip")
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(count(conn).await, 1, "the other setter keeps its verdict");

        // The 99.99% case: nothing to clear, and no error.
        let deleted = delete_extraction_error(conn, "sha_two", "test/clip")
            .await
            .unwrap();
        assert_eq!(deleted, 0);
    }

    // Auto-heal is per dependency: installing ffmpeg must not resurrect the
    // items that are waiting on pdfium, and must not touch input verdicts.
    #[tokio::test]
    async fn blocked_clearing_is_scoped_to_the_named_blockers() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        upsert_extraction_error(conn, &record("sha_two", FFMPEG, Some(1)))
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_one", PDFIUM, Some(1)))
            .await
            .unwrap();
        let mut input = record("sha_one", ApiErrorKind::Input, Some(1));
        input.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &input).await.unwrap();

        let mut blockers = list_distinct_blockers(conn).await.unwrap();
        blockers.sort_by_key(|blocker| blocker.as_str());
        assert_eq!(blockers, vec![Blocker::Ffmpeg, Blocker::Pdfium]);

        assert_eq!(delete_blocked_errors(conn, &[]).await.unwrap(), 0);
        let cleared = delete_blocked_errors(conn, &[Blocker::Ffmpeg])
            .await
            .unwrap();
        assert_eq!(cleared, 1);
        assert_eq!(
            list_distinct_blockers(conn).await.unwrap(),
            vec![Blocker::Pdfium]
        );
        assert_eq!(count(conn).await, 2, "the input verdict is untouched");
    }

    // The success-path gate lists *every* row of the setter, not the active
    // ones. Items with an active row are already excluded from the work query
    // and can never reach the success path, so an active-only query would be
    // blind to exactly the rows a success is able to clear: the sub-threshold
    // one an SMB blip left behind. The set is per-sha256 so a success only
    // pays for a delete when its own item owes a row.
    #[tokio::test]
    async fn error_sha_set_includes_the_rows_that_do_not_suppress_yet() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;

        let mut ambiguous = record("sha_two", ApiErrorKind::Input, Some(1));
        ambiguous.skip_after = 2;
        upsert_extraction_error(conn, &ambiguous).await.unwrap();
        let shas = list_error_sha256s_for_setter(conn, "test/clip")
            .await
            .unwrap();
        assert!(
            shas.contains("sha_two"),
            "an unconfirmed row still has to be clearable on success"
        );
        assert!(
            !shas.contains("sha_one"),
            "an item with no row must not trigger a delete"
        );
        assert_eq!(shas.len(), 1);

        // Confirming it changes nothing about the set.
        ambiguous.job_id = Some(2);
        upsert_extraction_error(conn, &ambiguous).await.unwrap();
        let shas = list_error_sha256s_for_setter(conn, "test/clip")
            .await
            .unwrap();
        assert_eq!(shas.len(), 1);
        assert!(shas.contains("sha_two"));

        // Still scoped to the setter.
        assert!(
            list_error_sha256s_for_setter(conn, "test/tagger")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn audit_list_filters_and_joins_the_item() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let mut video = record("sha_two", FFMPEG, Some(1));
        video.stage = STAGE_INFERENCE.to_string();
        upsert_extraction_error(conn, &video).await.unwrap();
        // A second setter, so every filter below has to discriminate rather
        // than merely return the whole (tiny) table.
        let mut tagger = record("sha_one", ApiErrorKind::Resource, Some(1));
        tagger.setter_name = "test/tagger".to_string();
        tagger.stage = STAGE_INFERENCE.to_string();
        upsert_extraction_error(conn, &tagger).await.unwrap();

        let all = list_extraction_errors(conn, &ExtractionErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 3, "no filters and no limit lists everything");

        let images = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                mime_prefix: Some("image/".to_string()),
                setter: Some("test/clip".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].item_sha256, "sha_one");
        assert_eq!(images[0].mime_type, "image/png");
        assert_eq!(images[0].setter_name, "test/clip");
        assert_eq!(images[0].attempts, 1);
        assert_eq!(images[0].last_job_id, Some(1));

        // The setter filter discriminates in both directions.
        let clip = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                setter: Some("test/clip".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(clip.len(), 2);
        assert!(clip.iter().all(|row| row.setter_name == "test/clip"));
        let tagger_rows = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                setter: Some("test/tagger".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(tagger_rows.len(), 1);
        assert_eq!(tagger_rows[0].setter_name, "test/tagger");

        // Stage alone, and class alone.
        let inference = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                stage: Some(STAGE_INFERENCE.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(inference.len(), 2);
        assert!(inference.iter().all(|row| row.stage == STAGE_INFERENCE));
        let resources = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                error_class: Some(CLASS_RESOURCE.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].setter_name, "test/tagger");

        let blocked = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                error_class: Some(CLASS_BLOCKED.to_string()),
                stage: Some(STAGE_INFERENCE.to_string()),
                setter: Some("test/clip".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].blocker.as_deref(), Some("ffmpeg"));
        assert_eq!(blocked[0].mime_type, "video/mp4");

        // The prefix is a range bound, not a pattern: wildcards are literal
        // bytes and match nothing, and an empty prefix filters nothing.
        let escaped = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                mime_prefix: Some("%".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(escaped.is_empty(), "'%' must not match every mime type");
        let empty_prefix = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                mime_prefix: Some(String::new()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(empty_prefix.len(), 3);
        // The bound is exclusive at the *end* of the range, so a prefix that
        // is a strict prefix of a longer mime still matches it.
        let videos = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                mime_prefix: Some("video".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(videos.len(), 1);
        assert_eq!(videos[0].mime_type, "video/mp4");
    }

    // The ledger keys on the item, but the audit surface shows a path. An item
    // with several files must therefore still be one row, with a stable
    // representative path — and an item whose files are all gone must not
    // disappear from the audit list entirely (the row is exactly what explains
    // why nothing was extracted).
    #[tokio::test]
    async fn audit_list_picks_one_representative_path_per_item() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        seed_files(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        // sha_two has a ledger row but no files at all.
        upsert_extraction_error(conn, &record("sha_two", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();

        let rows = list_extraction_errors(conn, &ExtractionErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "three files must not fan out into three rows"
        );
        let by_sha = |sha: &str| {
            rows.iter()
                .find(|row| row.item_sha256 == sha)
                .unwrap()
                .clone()
        };
        assert_eq!(
            by_sha("sha_one").path.as_deref(),
            Some("C:/m/mmm.png"),
            "an available file wins over the alphabetically first unavailable one"
        );
        assert_eq!(
            by_sha("sha_two").path,
            None,
            "an item with no files still has to be auditable"
        );

        // The count is the page's denominator, so it has to see the same set:
        // the join must not drop or multiply rows here either.
        assert_eq!(
            count_extraction_errors(conn, &ExtractionErrorFilters::default())
                .await
                .unwrap(),
            2
        );
    }

    // The statement's *shape*, pinned: the paging must stay inside the
    // subselect and the path lookup outside it. A flat query put the path
    // subquery in the select list of an ORDER BY that needs a sorter, and
    // SQLite fills a sorter with whole result rows — so the lookup ran once
    // per *matching* row (500 file lookups to render a 50-row page) instead of
    // once per row served. Nothing about the returned rows changes when this
    // regresses, which is why it is asserted on the SQL.
    #[test]
    fn audit_list_sql_pages_the_ledger_before_it_resolves_paths() {
        let sql = list_sql("WHERE setters.name = ?");
        let open = sql.find("FROM (").expect("the paged subselect must exist");
        let close = sql
            .find(") AS paged")
            .expect("the paged subselect must end");
        let (outer_head, inner) = (&sql[..open], &sql[open..close]);
        let outer_tail = &sql[close..];

        assert!(
            inner.contains("LIMIT ? OFFSET ?"),
            "the page window must apply inside the subselect: {sql}"
        );
        assert!(
            inner.contains("ORDER BY e.last_seen DESC, e.id DESC"),
            "the subselect is what decides which rows the page holds: {sql}"
        );
        assert!(
            inner.contains("WHERE setters.name = ?"),
            "the filters must narrow the rows that get paged: {sql}"
        );
        assert!(
            !inner.contains("files") && !inner.contains("JOIN items"),
            "nothing but the ledger may be touched per matching row: {sql}"
        );

        assert!(
            outer_head.contains("SELECT files.path FROM files"),
            "the path is resolved over the paged rows: {sql}"
        );
        assert!(
            outer_tail.contains("JOIN items ON items.id = paged.item_id"),
            "the item join belongs outside the page window: {sql}"
        );
        assert!(
            !outer_tail.contains("LIMIT") && !outer_head.contains("OFFSET"),
            "a second page window outside the subselect would re-slice it: {sql}"
        );
    }

    // The four extraction filters are independent, but only their conjunction
    // exercises the bind ordering across both filter kinds: three equalities
    // plus the mime range's two bounds is five binds ahead of the two page
    // binds, and a single misplacement filters on the wrong column silently.
    #[tokio::test]
    async fn audit_list_applies_every_filter_at_once() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        // Each of these differs from the target row in exactly one filtered
        // dimension, so a dropped or misbound predicate lets one through.
        let mut wrong_stage = record("sha_one", ApiErrorKind::Input, Some(1));
        wrong_stage.setter_name = "test/tagger".to_string();
        wrong_stage.stage = STAGE_INFERENCE.to_string();
        upsert_extraction_error(conn, &wrong_stage).await.unwrap();
        let mut wrong_mime = record("sha_two", ApiErrorKind::Input, Some(1));
        wrong_mime.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &wrong_mime).await.unwrap();

        let filters = ExtractionErrorFilters {
            setter: Some("test/clip".to_string()),
            error_class: Some(CLASS_INPUT.to_string()),
            stage: Some(STAGE_PREPARE.to_string()),
            mime_prefix: Some("image/".to_string()),
            ..Default::default()
        };
        let (_, binds) = audit_filters(&filters);
        assert_eq!(
            binds.len(),
            5,
            "three equalities and both mime bounds: {binds:?}"
        );

        let rows = list_extraction_errors(conn, &filters).await.unwrap();
        assert_eq!(rows.len(), 1, "every filter has to narrow: {rows:?}");
        assert_eq!(rows[0].item_sha256, "sha_one");
        assert_eq!(rows[0].setter_name, "test/clip");
        assert_eq!(rows[0].stage, STAGE_PREPARE);
        assert_eq!(rows[0].mime_type, "image/png");
        assert_eq!(count_extraction_errors(conn, &filters).await.unwrap(), 1);

        // Changing any one of the four to something nothing carries empties it.
        for narrowed in [
            ExtractionErrorFilters {
                setter: Some("test/absent".to_string()),
                ..filters.clone()
            },
            ExtractionErrorFilters {
                error_class: Some(CLASS_RESOURCE.to_string()),
                ..filters.clone()
            },
            ExtractionErrorFilters {
                stage: Some(STAGE_INFERENCE.to_string()),
                ..filters.clone()
            },
            ExtractionErrorFilters {
                mime_prefix: Some("audio/".to_string()),
                ..filters.clone()
            },
        ] {
            assert!(
                list_extraction_errors(conn, &narrowed)
                    .await
                    .unwrap()
                    .is_empty(),
                "the conjunction must hold for {narrowed:?}"
            );
        }
    }

    // Paging past the end is what the UI does the moment a filter narrows the
    // set under the offset it is already on. It must be an empty page against
    // an unchanged total, never an error and never a wrapped-around page.
    #[tokio::test]
    async fn audit_offset_past_the_total_is_an_empty_page() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_two", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();

        for offset in [2, 500] {
            let filters = ExtractionErrorFilters {
                offset,
                ..Default::default()
            };
            assert!(
                list_extraction_errors(conn, &filters)
                    .await
                    .unwrap()
                    .is_empty(),
                "offset {offset} is past the end"
            );
            assert_eq!(
                count_extraction_errors(conn, &filters).await.unwrap(),
                2,
                "the total is the filtered set, not the page"
            );
        }
    }

    // `last_job_id` is deliberately not a foreign key (job rows are deleted by
    // the cleanup flows and the ledger has to outlive them), so nothing nulls
    // it: the audit row keeps a stale id. The alternative — a real FK with
    // SET NULL — would silently erase the only trace of *when* a failure was
    // last seen by a run.
    #[tokio::test]
    async fn a_deleted_job_leaves_a_dangling_last_job_id() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        sqlx::query("INSERT INTO data_jobs (id, completed) VALUES (42, 1)")
            .execute(&mut *conn)
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(42)))
            .await
            .unwrap();

        sqlx::query("DELETE FROM data_jobs WHERE id = 42")
            .execute(&mut *conn)
            .await
            .unwrap();

        let rows = list_extraction_errors(conn, &ExtractionErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "the ledger row must survive the job's row");
        assert_eq!(
            rows[0].last_job_id,
            Some(42),
            "the id dangles rather than being nulled"
        );
    }

    // Every file of the item is unavailable — an unplugged drive, not a
    // deletion. The representative still has to be a path (the audit row is
    // useless without one), picked by the same deterministic tiebreak the
    // available case uses, so it does not move between pages.
    #[tokio::test]
    async fn audit_path_falls_back_to_the_smallest_unavailable_path() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) VALUES (1, '2026-01-01T00:00:00', 'C:/m')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (id, sha256, item_id, path, filename, last_modified, scan_id, available)
            VALUES
                (1, 'sha_one', 1, 'C:/m/zzz.png', 'zzz.png', '2026-01-01T00:00:00', 1, 0),
                (2, 'sha_one', 1, 'C:/m/aaa.png', 'aaa.png', '2026-01-01T00:00:00', 1, 0)
            "#,
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();

        let rows = list_extraction_errors(conn, &ExtractionErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].path.as_deref(),
            Some("C:/m/aaa.png"),
            "an all-unavailable item still names a path, the smallest one"
        );
    }

    // The count is what the audit surface paginates against, so it has to
    // answer for the *filters*, not for the page window.
    #[tokio::test]
    async fn audit_count_matches_the_filtered_set_not_the_page() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let mut video = record("sha_two", FFMPEG, Some(1));
        video.stage = STAGE_INFERENCE.to_string();
        upsert_extraction_error(conn, &video).await.unwrap();
        let mut tagger = record("sha_one", ApiErrorKind::Input, Some(1));
        tagger.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &tagger).await.unwrap();

        assert_eq!(
            count_extraction_errors(conn, &ExtractionErrorFilters::default())
                .await
                .unwrap(),
            3
        );
        // A page window narrower than the set does not move the total.
        let paged = ExtractionErrorFilters {
            limit: Some(1),
            ..Default::default()
        };
        assert_eq!(list_extraction_errors(conn, &paged).await.unwrap().len(), 1);
        assert_eq!(count_extraction_errors(conn, &paged).await.unwrap(), 3);

        for filters in [
            ExtractionErrorFilters {
                setter: Some("test/clip".to_string()),
                ..Default::default()
            },
            ExtractionErrorFilters {
                error_class: Some(CLASS_BLOCKED.to_string()),
                ..Default::default()
            },
            ExtractionErrorFilters {
                stage: Some(STAGE_INFERENCE.to_string()),
                ..Default::default()
            },
            ExtractionErrorFilters {
                mime_prefix: Some("image/".to_string()),
                ..Default::default()
            },
            ExtractionErrorFilters {
                setter: Some("test/absent".to_string()),
                ..Default::default()
            },
        ] {
            let listed = list_extraction_errors(conn, &filters).await.unwrap().len() as i64;
            assert_eq!(
                count_extraction_errors(conn, &filters).await.unwrap(),
                listed,
                "count and list must agree for {filters:?}"
            );
        }
    }

    // Newest first, and the pages partition the result set: the audit surface
    // pages through this and a duplicated or dropped row would be invisible.
    #[tokio::test]
    async fn audit_list_orders_by_last_seen_and_pages_disjointly() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let mut second = record("sha_two", ApiErrorKind::Input, Some(1));
        second.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &second).await.unwrap();
        let mut third = record("sha_one", ApiErrorKind::Input, Some(1));
        third.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &third).await.unwrap();

        // Hand-set an older timestamp: the writes above share a clock tick.
        sqlx::query(
            "UPDATE item_extraction_errors SET last_seen = '2000-01-01T00:00:00' \
             WHERE item_id = (SELECT id FROM items WHERE sha256 = 'sha_two')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        let all = list_extraction_errors(conn, &ExtractionErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(
            all[2].item_sha256, "sha_two",
            "the oldest last_seen sorts last"
        );
        assert!(
            all[0].last_seen >= all[1].last_seen && all[1].last_seen >= all[2].last_seen,
            "rows are ordered by last_seen DESC"
        );

        let page = |offset: i64| ExtractionErrorFilters {
            limit: Some(2),
            offset,
            ..Default::default()
        };
        let first_page = list_extraction_errors(conn, &page(0)).await.unwrap();
        let second_page = list_extraction_errors(conn, &page(2)).await.unwrap();
        assert_eq!((first_page.len(), second_page.len()), (2, 1));
        let mut paged: Vec<i64> = first_page
            .iter()
            .chain(second_page.iter())
            .map(|row| row.id)
            .collect();
        let seen = paged.len();
        paged.sort_unstable();
        paged.dedup();
        assert_eq!(paged.len(), seen, "the pages must be disjoint");
        let mut every: Vec<i64> = all.iter().map(|row| row.id).collect();
        every.sort_unstable();
        assert_eq!(paged, every, "the pages must cover every row");
    }

    // A caller-supplied page size can never turn into "no rows" or "every
    // row": zero and negatives clamp up, absurd sizes clamp down.
    #[tokio::test]
    async fn audit_list_clamps_the_page_window() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        upsert_extraction_error(conn, &record("sha_two", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();

        for limit in [Some(0), Some(-1)] {
            let rows = list_extraction_errors(
                conn,
                &ExtractionErrorFilters {
                    limit,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(rows.len(), 1, "a bad limit clamps to one row, not none");
        }

        let rows = list_extraction_errors(
            conn,
            &ExtractionErrorFilters {
                limit: Some(i64::MAX),
                offset: -5,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "a negative offset starts at the beginning");
    }

    // Lifecycle: the ledger shares it with the data it describes. A new sha256
    // means a new item and the old row cascades away; deleting a setter (the
    // manual "retry everything" gesture) clears its failure history too.
    #[tokio::test]
    async fn rows_cascade_with_their_item_and_setter() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        seed(conn).await;
        upsert_extraction_error(conn, &record("sha_one", ApiErrorKind::Input, Some(1)))
            .await
            .unwrap();
        let mut second = record("sha_two", ApiErrorKind::Input, Some(1));
        second.setter_name = "test/tagger".to_string();
        upsert_extraction_error(conn, &second).await.unwrap();

        sqlx::query("DELETE FROM items WHERE sha256 = 'sha_one'")
            .execute(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count(conn).await, 1, "the item's rows cascade away");

        let deleted = delete_setter_by_name(conn, "test/tagger").await.unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(count(conn).await, 0, "the setter's rows cascade away");
    }
}
