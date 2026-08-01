//! The filescan failure ledger (`scan_errors`).
//!
//! See docs/failed-media-retry-design.md. A row means "the scan already
//! decided this path is not indexable, and re-hashing / re-probing / re-
//! decoding it is wasted work" — never anything about search results or data
//! reads. Losing a row costs one re-attempt, never correctness, so every
//! writer here is advisory for correctness and authoritative only for
//! scheduling.
//!
//! The twin of [`crate::db::extraction_errors`], and deliberately a separate
//! table: these failures happen *before* an item (or even a hash) exists, so
//! the key is the path and the invalidation key is `(last_modified,
//! file_size)` rather than the content hash. Everything the two halves do
//! share — the class strings, the audit-message clamp, the `blocked`
//! auto-heal — lives in [`crate::db::ledger`].
//!
//! Transient failures never reach the table: the record carries an
//! [`ApiErrorKind`], and the upsert refuses the one variant whose
//! `persisted_class()` is `None`.

use std::collections::HashMap;

use crate::api_error::{ApiError, ApiErrorKind, Blocker};
use crate::db::extraction_write::current_iso_timestamp;
use crate::db::ledger::{
    LedgerTable, audit_filter_sql, clamp_list_limit, delete_blocked_rows,
    list_distinct_blockers_in, read_audit_column,
};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Which table the shared ledger helpers operate on for this half.
const TABLE: LedgerTable = LedgerTable::ScanErrors;

/// `stage` value for a path whose mime type could not be guessed at all. The
/// guess is a pure function of the file name, so one failure settles it.
pub(crate) const STAGE_MIME: &str = "mime";

/// `stage` value for a metadata probe (ffprobe) that rejected the file. The
/// tool did its own file I/O, so this is the ambiguous class.
pub(crate) const STAGE_METADATA: &str = "metadata";

/// `stage` value for an image the scan could not decode.
pub(crate) const STAGE_DECODE: &str = "decode";

/// SQLite caps the number of bind variables per statement; the sweep is
/// normally a handful of paths, but chunking keeps a pathological ledger from
/// turning into a hard error instead of a slightly slower delete.
const DELETE_CHUNK: usize = 500;

/// One ledger write. Owned fields so the writer actor's message can carry it.
#[derive(Debug, Clone)]
pub(crate) struct ScanErrorRecord {
    /// Exactly the string the walker produced, which is also what a `files`
    /// row would have stored. The column is BINARY; case drift on Windows is
    /// handled at query time by [`fold_scan_path`] and the duplicate-closing
    /// delete in [`upsert_scan_error`], never by the schema.
    pub path: String,
    /// The mtime and size *at failure*. Together they are the retry key: a
    /// file whose either half moved is a new verdict, not a confirmation.
    pub last_modified: String,
    pub file_size: i64,
    /// [`STAGE_MIME`], [`STAGE_METADATA`] or [`STAGE_DECODE`].
    pub stage: String,
    /// The classification itself, not its persisted strings: `error_class` and
    /// `blocker` are derived in [`upsert_scan_error`], so an inconsistent pair
    /// (a `blocked` row without a blocker) is unrepresentable.
    pub kind: ApiErrorKind,
    /// Best effort, for retry directives that target a format. `None` when the
    /// mime guess is what failed.
    pub mime_type: Option<String>,
    pub error: String,
    pub skip_after: i64,
}

/// What the walker needs to decide whether to skip a path, and nothing else:
/// the map is held for the whole folder scan, so it stays the retry key and
/// the identity rather than the whole record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanErrorSkip {
    /// The path *as stored*, which is what a delete has to bind. Not
    /// necessarily the map key it was found under: on Windows the key is a
    /// case-folded copy (see [`fold_scan_path`]), and the walker's casing and
    /// the stored casing can differ.
    pub path: String,
    pub last_modified: String,
    pub file_size: i64,
    pub attempts: i64,
    pub skip_after: i64,
}

impl ScanErrorSkip {
    /// Whether this verdict suppresses the file as it currently sits on disk.
    ///
    /// Both halves of the retry key must still match: a file whose contents
    /// were replaced (or truncated, or re-saved by a repair tool) gets a fresh
    /// attempt without anyone having to notice. `attempts >= skip_after` is
    /// the confirmation threshold — an ambiguous verdict (an external tool
    /// that read the file itself) has to fail in two *different* runs before
    /// it counts.
    pub fn suppresses(&self, last_modified: &str, file_size: i64) -> bool {
        self.attempts >= self.skip_after
            && self.last_modified == last_modified
            && self.file_size == file_size
    }
}

/// `attempts` counts *runs that saw the failure*, not verdicts.
///
/// Two rules, in this order:
///
/// 1. A changed `(last_modified, file_size)` resets it to 1. The stored
///    confirmations were about different bytes; keeping them would let a file
///    the user just fixed inherit a suppression it never earned.
/// 2. Otherwise it increments only when the scan changed. `IS NOT` is
///    SQLite's null-safe inequality, so a row written by a scan-less caller
///    still increments once a real scan touches it.
///
/// A class change refreshes the verdict but does *not* reset the count (the
/// same starvation rationale as the extraction ledger): a path whose verdict
/// alternates between runs would otherwise never reach `skip_after` and would
/// be re-processed forever.
const UPSERT_SQL: &str = r#"
    INSERT INTO scan_errors (
        path, last_modified, file_size, stage, error_class, blocker, mime_type,
        error, skip_after, attempts, last_scan_id, first_seen, last_seen
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
        attempts = CASE
            WHEN scan_errors.last_modified IS NOT excluded.last_modified
                OR scan_errors.file_size IS NOT excluded.file_size
                THEN 1
            WHEN scan_errors.last_scan_id IS NOT excluded.last_scan_id
                THEN scan_errors.attempts + 1
            ELSE scan_errors.attempts
        END,
        last_modified = excluded.last_modified,
        file_size = excluded.file_size,
        stage = excluded.stage,
        error_class = excluded.error_class,
        blocker = excluded.blocker,
        mime_type = excluded.mime_type,
        error = excluded.error,
        skip_after = excluded.skip_after,
        last_scan_id = excluded.last_scan_id,
        last_seen = excluded.last_seen
"#;

/// Records (or re-records) a scan failure.
///
/// `scan_id` is the `file_scans.id` of the run that saw it, which is what
/// dedups `attempts`. `None` is allowed but blunt: two consecutive scan-less
/// writes are indistinguishable and increment only once, so a caller with no
/// scan identity must use `skip_after = 1` or its rows will never reach their
/// threshold.
pub(crate) async fn upsert_scan_error(
    conn: &mut sqlx::SqliteConnection,
    record: &ScanErrorRecord,
    scan_id: Option<i64>,
) -> ApiResult<()> {
    let Some(class) = record.kind.persisted_class() else {
        tracing::error!(
            path = %record.path,
            "refused to persist a transient scan failure"
        );
        return Err(ApiError::internal("transient failures are not persisted"));
    };
    let blocker = record.kind.blocker().map(|blocker| blocker.as_str());
    let error = crate::db::ledger::truncate_error(&record.error);

    // Windows only, and first, on this same connection and transaction: one
    // file can reach the ledger under different casing (a root re-registered
    // as `d:\media`, a watcher event reporting a different case), and `path`
    // is a BINARY unique key, so the upsert below would insert a *second* row
    // for one file — two verdicts, neither ever reaching its threshold, and a
    // sweep that deletes whichever casing the walk did not produce. Collapse
    // them onto the casing this write uses.
    //
    // Never on Linux/macOS, where two paths differing only in case are two
    // genuinely different files and this would delete an unrelated verdict.
    // The placeholders stay unnumbered (see below) and the path is bound
    // twice.
    #[cfg(windows)]
    sqlx::query("DELETE FROM scan_errors WHERE path <> ? AND path = ? COLLATE NOCASE")
        .bind(&record.path)
        .bind(&record.path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %record.path, "failed to close scan error duplicates");
            ApiError::internal("Failed to record scan failure")
        })?;

    let now = current_iso_timestamp();
    sqlx::query(UPSERT_SQL)
        .bind(&record.path)
        .bind(&record.last_modified)
        .bind(record.file_size)
        .bind(&record.stage)
        .bind(class)
        .bind(blocker)
        .bind(record.mime_type.as_deref())
        .bind(error.as_ref())
        .bind(record.skip_after)
        .bind(scan_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path = %record.path, "failed to upsert scan error");
            ApiError::internal("Failed to record scan failure")
        })?;
    Ok(())
}

/// The lookup key for a stored path.
///
/// On Windows a file can reach the ledger under more than one casing — a root
/// re-registered as `d:\media`, a watcher event reporting a different case —
/// and every read here compares bytes. Without folding, the preload would not
/// match its own rows, the end-of-root sweep would delete them as vanished,
/// and the upsert would add a second row per file. The stored column stays
/// BINARY: the folding is query-time only, exactly like the NOCASE exclusion
/// in `mark_unavailable_files`, and ASCII-only folding covers drive letters
/// and typical drift.
///
/// Everywhere else this is the identity: two paths differing only in case are
/// two different files.
pub(crate) fn fold_scan_path(path: &str) -> String {
    #[cfg(windows)]
    {
        path.to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        path.to_string()
    }
}

/// Every ledger row under `root`, keyed by [`fold_scan_path`] — the walker's
/// whole view of the ledger, read once per folder scan so the healthy path
/// costs one hash lookup per file and no query at all.
///
/// Deliberately not restricted to active rows: the map is also what gates the
/// success-path delete and the end-of-root sweep, and an unconfirmed row that
/// no read ever returns would sit there until a second blip years later
/// confirmed a verdict on a file that had succeeded a thousand times in
/// between.
pub(crate) async fn load_scan_errors_under(
    conn: &mut sqlx::SqliteConnection,
    root: &str,
) -> ApiResult<HashMap<String, ScanErrorSkip>> {
    // The whole table, filtered in Rust. A half-open range over the
    // UNIQUE(path) index would be tighter, but it compares bytes: on Windows a
    // root whose casing drifted from the stored rows would select none of
    // them, and the end-of-root sweep would then delete every row it failed to
    // see. The ledger holds one row per file the scan gave up on — a handful,
    // by design — so reading it whole costs far less than getting that wrong.
    let rows = sqlx::query_as::<_, (String, String, i64, i64, i64)>(
        "SELECT path, last_modified, file_size, attempts, skip_after FROM scan_errors",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, root, "failed to load scan errors for a root");
        ApiError::internal("Failed to read scan failures")
    })?;

    Ok(rows
        .into_iter()
        // A *string* prefix alone would let the root `D:\Photo` cover
        // `D:\Photos\...`, which belongs to a different root and must not be
        // swept by this one, so the separator is checked too.
        .filter(|(path, ..)| path_is_under(root, path))
        .map(|(path, last_modified, file_size, attempts, skip_after)| {
            (
                fold_scan_path(&path),
                ScanErrorSkip {
                    path,
                    last_modified,
                    file_size,
                    attempts,
                    skip_after,
                },
            )
        })
        .collect())
}

/// One path's verdict, for the continuous scan — which handles single-file
/// events and has no root to preload against, so it pays one indexed point
/// lookup per event instead. On the same connection it already opens for the
/// mtime shortcut, so an event costs no extra round-trip.
///
/// The returned row carries its *stored* path, which is what the caller has to
/// bind to clear it.
pub(crate) async fn get_scan_error(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<Option<ScanErrorSkip>> {
    let row: Option<(String, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT path, last_modified, file_size, attempts, skip_after \
         FROM scan_errors WHERE path = ?",
    )
    .bind(path)
    .fetch_optional(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, path, "failed to read a scan error");
        ApiError::internal("Failed to read scan failures")
    })?;

    let row = match row {
        Some(row) => Some(row),
        // Windows only, and only after the indexed lookup missed: the
        // UNIQUE(path) autoindex is BINARY, so a NOCASE predicate cannot seek
        // it and this scans the table. Acceptable here and nowhere else — the
        // ledger is a handful of rows by design — and it is what stops an
        // event reporting a differently-cased path from re-processing (and
        // re-recording) a file that already has a verdict.
        None if cfg!(windows) => sqlx::query_as(
            "SELECT path, last_modified, file_size, attempts, skip_after \
             FROM scan_errors WHERE path = ? COLLATE NOCASE",
        )
        .bind(path)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path, "failed to read a scan error");
            ApiError::internal("Failed to read scan failures")
        })?,
        None => None,
    };

    Ok(row.map(
        |(path, last_modified, file_size, attempts, skip_after)| ScanErrorSkip {
            path,
            last_modified,
            file_size,
            attempts,
            skip_after,
        },
    ))
}

/// Whether `path` names something inside `root`, treating both separators as
/// equivalent. Roots reach the ledger through `normalize_path`, so they always
/// carry the platform separator and a trailing one; the separator check is
/// what makes a *prefix* match a *containment* match.
///
/// A root that is a prefix of another root's *name* (`C:\media` versus
/// `C:\media-old`) is not "under" it, which is the whole reason this exists.
///
/// On Windows the comparison folds ASCII case, for the reason in
/// [`fold_scan_path`].
pub(crate) fn path_is_under(root: &str, path: &str) -> bool {
    let rest = if cfg!(windows) {
        // `str::get` yields None on a non-char-boundary split, which can only
        // happen when the two disagree inside a multi-byte char anyway.
        let Some(head) = path.get(..root.len()) else {
            return false;
        };
        if !head.eq_ignore_ascii_case(root) {
            return false;
        }
        &path[root.len()..]
    } else {
        let Some(rest) = path.strip_prefix(root) else {
            return false;
        };
        rest
    };
    rest.is_empty() || root.ends_with(['/', '\\']) || rest.starts_with(['/', '\\'])
}

/// The success path: a file the scan just processed owes no ledger row.
/// Returns how many rows went away (almost always zero, which is why callers
/// gate this on the path having been in the preloaded map at all).
pub(crate) async fn delete_scan_error(
    conn: &mut sqlx::SqliteConnection,
    path: &str,
) -> ApiResult<u64> {
    let result = sqlx::query("DELETE FROM scan_errors WHERE path = ?")
        .bind(path)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, path, "failed to delete scan error");
            ApiError::internal("Failed to clear scan failure")
        })?;
    Ok(result.rows_affected())
}

/// The end-of-root sweep: rows whose path the walk never reached (the file is
/// gone, moved, excluded, or no longer has a scanned extension). One statement
/// instead of a delete per path, because this runs inside a write transaction
/// that also bumps the search-cache epoch.
pub(crate) async fn delete_scan_errors(
    conn: &mut sqlx::SqliteConnection,
    paths: &[String],
) -> ApiResult<u64> {
    let mut deleted = 0;
    for chunk in paths.chunks(DELETE_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        // Mixing numbered and bare placeholders misbinds parameters under
        // sqlx, so every placeholder here must stay unnumbered.
        let sql = format!("DELETE FROM scan_errors WHERE path IN ({placeholders})");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for path in chunk {
            query = query.bind(path);
        }
        let result = query.execute(&mut *conn).await.map_err(|err| {
            tracing::error!(error = %err, "failed to delete scan errors");
            ApiError::internal("Failed to clear scan failures")
        })?;
        deleted += result.rows_affected();
    }
    Ok(deleted)
}

/// Auto-heal, write half. The twin of
/// [`crate::db::extraction_errors::delete_blocked_errors`].
pub(crate) async fn delete_blocked_scan_errors(
    conn: &mut sqlx::SqliteConnection,
    blockers: &[Blocker],
) -> ApiResult<u64> {
    delete_blocked_rows(conn, TABLE, blockers).await
}

/// Auto-heal, read half. The twin of
/// [`crate::db::extraction_errors::list_distinct_blockers`].
pub(crate) async fn list_distinct_scan_blockers(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<Blocker>> {
    list_distinct_blockers_in(conn, TABLE).await
}

// Everything from here to the tests is the *audit half* — the list query, its
// count twin, their filters and their row type. It is what
// `GET /api/jobs/scan/failures` serves; the walker never reads it.
//
// Deliberately no [`fold_scan_path`] anywhere below: this half never matches
// on a path, it only returns one. The folding exists so a case-drifted walk
// finds its own rows; an audit list that shows every row has nothing to find.

/// Audit filters, the twin of
/// [`crate::db::extraction_errors::ExtractionErrorFilters`] minus `setter` —
/// these failures happen before any setter is involved.
#[derive(Debug, Clone, Default)]
pub(crate) struct ScanErrorFilters {
    pub error_class: Option<String>,
    pub stage: Option<String>,
    /// Matched as a prefix of `mime_type`, e.g. `image/`. An empty string is
    /// treated as no filter. Rows whose mime guess is what failed have a NULL
    /// `mime_type` and are therefore excluded by any prefix, which is the
    /// intended reading of "show me the image failures".
    pub mime_prefix: Option<String>,
    /// Clamped by [`clamp_list_limit`]; `None` means its default.
    pub limit: Option<i64>,
    /// Negative values are clamped to zero.
    pub offset: i64,
}

/// One audit row. Unlike the extraction twin this needs no join at all: the
/// path *is* the key, and there is no item to resolve.
#[derive(Debug, Clone)]
pub(crate) struct ScanErrorRow {
    pub id: i64,
    pub path: String,
    pub stage: String,
    pub error_class: String,
    pub blocker: Option<String>,
    /// `None` when the mime guess itself is what failed (stage `mime`).
    pub mime_type: Option<String>,
    pub error: String,
    pub skip_after: i64,
    pub attempts: i64,
    pub last_scan_id: Option<i64>,
    pub first_seen: String,
    pub last_seen: String,
}

/// The clause both audit reads share. The column names are literals from this
/// module; only the bound values come from the request.
fn audit_filters(filters: &ScanErrorFilters) -> (String, Vec<String>) {
    audit_filter_sql(
        &[
            ("error_class", filters.error_class.as_deref()),
            ("stage", filters.stage.as_deref()),
        ],
        "mime_type",
        filters.mime_prefix.as_deref(),
    )
}

/// How many failures match these filters, ignoring the page window — the
/// denominator the paginated audit surface needs. Shares the `WHERE` builder
/// with [`list_scan_errors`], so the count can never describe a different set
/// than the page it labels.
pub(crate) async fn count_scan_errors(
    conn: &mut sqlx::SqliteConnection,
    filters: &ScanErrorFilters,
) -> ApiResult<i64> {
    let (where_clause, binds) = audit_filters(filters);
    let sql = format!("SELECT COUNT(*) FROM scan_errors {where_clause}");
    let mut query = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()));
    for bind in &binds {
        query = query.bind(bind);
    }
    query.fetch_one(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, "failed to count scan errors");
        ApiError::internal("Failed to read scan failures")
    })
}

/// Lists scan failures for the audit surface, newest first.
///
/// The `LIMIT` is not a concession to table size — the ledger is a handful of
/// rows by design, and every other read here takes it whole — but to the page
/// the API serves: the one pathological case (a NAS that vanished mid-scan)
/// is exactly when a user opens this list.
pub(crate) async fn list_scan_errors(
    conn: &mut sqlx::SqliteConnection,
    filters: &ScanErrorFilters,
) -> ApiResult<Vec<ScanErrorRow>> {
    let (where_clause, binds) = audit_filters(filters);
    // Mixing numbered and bare placeholders misbinds parameters under sqlx,
    // so every placeholder here must stay unnumbered.
    let sql = format!(
        r#"
        SELECT
            id, path, stage, error_class, blocker, mime_type, error,
            skip_after, attempts, last_scan_id, first_seen, last_seen
        FROM scan_errors
        {where_clause}
        ORDER BY last_seen DESC, id DESC
        LIMIT ? OFFSET ?
        "#
    );

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
            tracing::error!(error = %err, "failed to list scan errors");
            ApiError::internal("Failed to read scan failures")
        })?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        results.push(ScanErrorRow {
            id: read_audit_column(&row, "id")?,
            path: read_audit_column(&row, "path")?,
            stage: read_audit_column(&row, "stage")?,
            error_class: read_audit_column(&row, "error_class")?,
            blocker: read_audit_column(&row, "blocker")?,
            mime_type: read_audit_column(&row, "mime_type")?,
            error: read_audit_column(&row, "error")?,
            skip_after: read_audit_column(&row, "skip_after")?,
            attempts: read_audit_column(&row, "attempts")?,
            last_scan_id: read_audit_column(&row, "last_scan_id")?,
            first_seen: read_audit_column(&row, "first_seen")?,
            last_seen: read_audit_column(&row, "last_seen")?,
        });
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::ledger::{CLASS_BLOCKED, CLASS_INPUT, MAX_ERROR_BYTES};
    use crate::db::migrations::{migrate_databases_on_disk, setup_test_databases};
    use crate::test_utils::test_data_dir;

    const FFMPEG: ApiErrorKind = ApiErrorKind::Blocked {
        blocker: Blocker::Ffmpeg,
    };
    const PDFIUM: ApiErrorKind = ApiErrorKind::Blocked {
        blocker: Blocker::Pdfium,
    };

    fn record(path: &str, kind: ApiErrorKind) -> ScanErrorRecord {
        ScanErrorRecord {
            path: path.to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            file_size: 100,
            stage: STAGE_DECODE.to_string(),
            kind,
            mime_type: Some("image/png".to_string()),
            error: "decode failed".to_string(),
            skip_after: 1,
        }
    }

    async fn row(conn: &mut sqlx::SqliteConnection, path: &str) -> (i64, i64, String, String) {
        sqlx::query_as(
            "SELECT attempts, skip_after, error_class, first_seen FROM scan_errors WHERE path = ?",
        )
        .bind(path)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
    }

    async fn count(conn: &mut sqlx::SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM scan_errors")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }

    // The migration has to land on a real file-backed database too: the
    // in-memory harness the other tests use runs the same migrator, but only
    // the on-disk path is what an upgrading user actually executes.
    #[tokio::test]
    async fn migration_creates_the_scan_ledger() {
        let _test_env = test_data_dir();
        migrate_databases_on_disk(
            Some("scan_ledger_migration"),
            Some("scan_ledger_migration_user"),
        )
        .await
        .expect("migrate test databases");
        let mut conn = crate::db::open_index_db_write_no_user_data("scan_ledger_migration")
            .await
            .unwrap();

        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master \
             WHERE type = 'index' AND tbl_name = 'scan_errors' AND name IS NOT NULL \
             ORDER BY name",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(
            indexes.contains(&"idx_scan_errors_class".to_string()),
            "the class index must exist: {indexes:?}"
        );
        // The UNIQUE(path) autoindex is what the path reads seek on; it is
        // unnamed in this list only because SQLite names it itself.
        assert!(
            indexes
                .iter()
                .any(|name| name.starts_with("sqlite_autoindex_scan_errors")),
            "UNIQUE(path) must have an index to seek: {indexes:?}"
        );
    }

    // The table's own guards, matching the extraction ledger's: a `blocked`
    // row without its dependency is a lie the auto-heal probe would act on,
    // and a `skip_after` below 1 would suppress a file that never failed once.
    #[tokio::test]
    async fn migration_check_constraints_reject_inconsistent_rows() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let insert = |path: &'static str,
                      class: &'static str,
                      blocker: Option<&'static str>,
                      skip_after: i64| {
            sqlx::query(
                "INSERT INTO scan_errors (path, last_modified, file_size, stage, error_class, \
                 blocker, mime_type, error, skip_after, attempts, first_seen, last_seen) \
                 VALUES (?, 'now', 1, 'decode', ?, ?, 'image/png', 'x', ?, 1, 'now', 'now')",
            )
            .bind(path)
            .bind(class)
            .bind(blocker)
            .bind(skip_after)
        };

        assert!(
            insert("a", CLASS_BLOCKED, None, 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "a blocked row must name its dependency"
        );
        assert!(
            insert("b", CLASS_INPUT, Some("ffmpeg"), 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "only blocked rows carry a blocker"
        );
        assert!(
            insert("c", CLASS_INPUT, None, 0)
                .execute(&mut *conn)
                .await
                .is_err(),
            "skip_after must be at least one attempt"
        );
        assert!(
            insert("d", CLASS_INPUT, None, 1)
                .execute(&mut *conn)
                .await
                .is_ok()
        );
        // `path` is the key: a second row for the same path is a conflict the
        // upsert resolves, never a duplicate.
        assert!(
            insert("d", CLASS_INPUT, None, 1)
                .execute(&mut *conn)
                .await
                .is_err()
        );
    }

    // The attempt accounting is the whole confirmation-threshold mechanism:
    // one increment per scan at most, and a file whose bytes moved starts over.
    #[tokio::test]
    async fn upsert_counts_one_attempt_per_scan_and_resets_on_a_changed_file() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let mut ambiguous = record("C:/media/a.mp4", ApiErrorKind::Input);
        ambiguous.stage = STAGE_METADATA.to_string();
        ambiguous.skip_after = 2;
        upsert_scan_error(conn, &ambiguous, Some(10)).await.unwrap();
        let (attempts, skip_after, class, first_seen) = row(conn, "C:/media/a.mp4").await;
        assert_eq!((attempts, skip_after, class.as_str()), (1, 2, CLASS_INPUT));

        // Same scan again (a re-dispatched path): not a confirmation.
        upsert_scan_error(conn, &ambiguous, Some(10)).await.unwrap();
        let (attempts, ..) = row(conn, "C:/media/a.mp4").await;
        assert_eq!(attempts, 1, "a second failure in one scan is one attempt");

        // A later scan confirms it, and first_seen still records the original.
        upsert_scan_error(conn, &ambiguous, Some(11)).await.unwrap();
        let (attempts, _, _, unchanged_first_seen) = row(conn, "C:/media/a.mp4").await;
        assert_eq!(attempts, 2);
        assert_eq!(unchanged_first_seen, first_seen);

        // A modified file is a fresh verdict: the confirmations were about
        // different bytes, so the count starts over even though the scan and
        // the class are the same.
        let mut modified = ambiguous.clone();
        modified.last_modified = "2026-02-02T00:00:00".to_string();
        upsert_scan_error(conn, &modified, Some(12)).await.unwrap();
        let (attempts, ..) = row(conn, "C:/media/a.mp4").await;
        assert_eq!(attempts, 1, "a new mtime resets the confirmations");

        // So is a file whose size moved without its mtime (a truncating
        // writer, or a filesystem with coarse timestamps).
        upsert_scan_error(conn, &modified, Some(13)).await.unwrap();
        let mut resized = modified.clone();
        resized.file_size = 999;
        upsert_scan_error(conn, &resized, Some(14)).await.unwrap();
        let (attempts, ..) = row(conn, "C:/media/a.mp4").await;
        assert_eq!(attempts, 1, "a new size resets the confirmations");

        // A different verdict refreshes the classification but keeps counting:
        // a path whose verdict alternates between runs must still reach its
        // threshold instead of being re-processed forever.
        let mut changed = resized.clone();
        changed.kind = ApiErrorKind::Resource;
        upsert_scan_error(conn, &changed, Some(15)).await.unwrap();
        let (attempts, _, class, _) = row(conn, "C:/media/a.mp4").await;
        assert_eq!((attempts, class.as_str()), (2, "resource"));
        assert_eq!(count(conn).await, 1, "one row per path");
    }

    // `last_scan_id` is nullable and the dedup comparison is `IS NOT`,
    // SQLite's null-safe inequality: a plain `<>` yields NULL against a
    // scan-less row and would silently stop counting.
    #[tokio::test]
    async fn upsert_counts_across_a_null_scan_id() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        let entry = record("C:/media/a.png", ApiErrorKind::Input);

        upsert_scan_error(conn, &entry, None).await.unwrap();
        upsert_scan_error(conn, &entry, Some(7)).await.unwrap();
        let (attempts, ..) = row(conn, "C:/media/a.png").await;
        assert_eq!(attempts, 2, "a real scan after a scan-less write counts");

        // Two scan-less writes in a row are indistinguishable, hence the
        // skip_after = 1 requirement documented on the parameter.
        upsert_scan_error(conn, &entry, None).await.unwrap();
        upsert_scan_error(conn, &entry, None).await.unwrap();
        let (attempts, ..) = row(conn, "C:/media/a.png").await;
        assert_eq!(attempts, 3, "consecutive scan-less writes count once");
    }

    // A transient failure has no ledger row by construction, and the type
    // system cannot express "record this Generic error" without going through
    // here, so this is the single guard. The audit message is clamped in the
    // same place, on a char boundary.
    #[tokio::test]
    async fn upsert_refuses_a_transient_verdict_and_clamps_the_message() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        assert!(
            upsert_scan_error(
                conn,
                &record("C:/media/a.png", ApiErrorKind::Generic),
                Some(1)
            )
            .await
            .is_err()
        );
        assert_eq!(count(conn).await, 0);

        let mut huge = record("C:/media/a.png", ApiErrorKind::Input);
        huge.error = "é".repeat(MAX_ERROR_BYTES);
        upsert_scan_error(conn, &huge, Some(1)).await.unwrap();
        let stored: String = sqlx::query_scalar("SELECT error FROM scan_errors")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert!(stored.ends_with('…'));
        assert!(stored.len() <= MAX_ERROR_BYTES + '…'.len_utf8());
    }

    // The preload is what makes the healthy path free, so its scoping has to
    // be exact in both directions: everything under the root, nothing that
    // merely shares a byte prefix with it.
    #[tokio::test]
    async fn preload_is_scoped_to_the_root_by_separator() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        for path in [
            r"C:\media\a.png",
            r"C:\media\sub\b.png",
            // Same byte prefix, different root: a LIKE/range match alone would
            // pull these in and the end-of-root sweep would then delete them.
            r"C:\media-old\c.png",
            r"C:\mediax\d.png",
            r"C:\other\e.png",
        ] {
            upsert_scan_error(conn, &record(path, ApiErrorKind::Input), Some(1))
                .await
                .unwrap();
        }

        let loaded = load_scan_errors_under(conn, r"C:\media").await.unwrap();
        let mut paths: Vec<&String> = loaded.keys().collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                &fold_scan_path(r"C:\media\a.png"),
                &fold_scan_path(r"C:\media\sub\b.png")
            ]
        );

        // A root the user typed with a trailing separator, and one typed with
        // forward slashes, both still match what the walker produced.
        let trailing = load_scan_errors_under(conn, r"C:\media\").await.unwrap();
        assert_eq!(trailing.len(), 2);
        let deeper = load_scan_errors_under(conn, r"C:\media\sub").await.unwrap();
        assert_eq!(deeper.len(), 1);

        // The retry key rides along, so the walker never re-reads the table —
        // and so does the stored path, which is what a delete has to bind.
        let skip = &loaded[&fold_scan_path(r"C:\media\a.png")];
        assert_eq!(
            skip,
            &ScanErrorSkip {
                path: r"C:\media\a.png".to_string(),
                last_modified: "2026-01-01T00:00:00".to_string(),
                file_size: 100,
                attempts: 1,
                skip_after: 1,
            }
        );
        assert!(skip.suppresses("2026-01-01T00:00:00", 100));
        assert!(
            !skip.suppresses("2026-05-05T00:00:00", 100),
            "a new mtime always retries"
        );
        assert!(
            !skip.suppresses("2026-01-01T00:00:00", 101),
            "a new size always retries"
        );

        // An unconfirmed ambiguous row is loaded (the success path and the
        // sweep both need to see it) but does not suppress anything yet.
        let unconfirmed = ScanErrorSkip {
            path: r"C:\media\a.png".to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            file_size: 100,
            attempts: 1,
            skip_after: 2,
        };
        assert!(!unconfirmed.suppresses("2026-01-01T00:00:00", 100));
    }

    // Forward slashes on the root and backslashes in the stored path are the
    // normal Windows pairing: the user types the folder, the walker joins with
    // the platform separator.
    #[test]
    fn under_root_accepts_either_separator_and_rejects_prefix_siblings() {
        assert!(path_is_under("C:/media", r"C:/media\a.png"));
        assert!(path_is_under("C:/media", "C:/media/a.png"));
        assert!(path_is_under("C:/media/", "C:/media/a.png"));
        assert!(path_is_under("/srv/media", "/srv/media/a.png"));
        assert!(!path_is_under("C:/media", "C:/media-old/a.png"));
        assert!(!path_is_under("C:/media", "C:/mediax/a.png"));
        assert!(!path_is_under("C:/media", "D:/media/a.png"));
    }

    // Windows paths are case-insensitive, so a root the user re-registered
    // with different casing still contains its own files. Elsewhere the two
    // are different files and must not be conflated.
    #[test]
    fn under_root_folds_case_only_on_windows() {
        assert_eq!(
            path_is_under(r"d:\media\", r"D:\Media\a.png"),
            cfg!(windows)
        );
        let folded = fold_scan_path(r"D:\Media\A.png");
        if cfg!(windows) {
            assert_eq!(folded, r"d:\media\a.png");
        } else {
            assert_eq!(folded, r"D:\Media\A.png");
        }
    }

    // The whole Windows case-drift path, end to end at the ledger level: rows
    // written under one casing, read back under another. Before the folding,
    // the preload matched nothing, so the end-of-root sweep deleted every row
    // as "vanished" and the next failure inserted a duplicate under the new
    // casing — two verdicts for one file, neither ever confirming.
    //
    // Shaped after `file_scans::mark_unavailable_files_excludes_case_drifted_paths`,
    // the same fix in the same direction one table over.
    #[cfg(windows)]
    #[tokio::test]
    async fn case_drifted_roots_find_clear_and_do_not_duplicate_their_rows() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let stored = r"D:\Media\Photos\a.png";
        upsert_scan_error(conn, &record(stored, ApiErrorKind::Input), Some(1))
            .await
            .unwrap();

        // Preload under the drifted root: found, keyed by the folded path, and
        // carrying the casing that is actually in the table.
        let loaded = load_scan_errors_under(conn, r"d:\media\").await.unwrap();
        assert_eq!(loaded.len(), 1, "the drifted root must see its own row");
        let entry = &loaded[&fold_scan_path(r"d:\media\photos\A.PNG")];
        assert_eq!(
            entry.path, stored,
            "the stored casing is what a delete binds"
        );

        // The sweep is a set difference over the folded keys, so a walker that
        // produced the other casing marks this row seen rather than vanished.
        let seen = fold_scan_path(r"d:\media\photos\a.png");
        assert!(loaded.contains_key(&seen));

        // The continuous scan's point lookup: exact miss, NOCASE fallback hit.
        let found = get_scan_error(conn, r"d:\media\photos\a.png")
            .await
            .unwrap()
            .expect("a drifted event must find the verdict");
        assert_eq!(found.path, stored);

        // A later failure under yet another casing collapses onto one row
        // instead of inserting a second verdict for the same file.
        upsert_scan_error(
            conn,
            &record(r"D:\MEDIA\photos\a.png", ApiErrorKind::Input),
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(count(conn).await, 1, "one file owes exactly one row");
        let paths: Vec<String> = sqlx::query_scalar("SELECT path FROM scan_errors")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        assert_eq!(paths, vec![r"D:\MEDIA\photos\a.png".to_string()]);

        // And clearing by the stored path leaves no orphan behind.
        let entry = load_scan_errors_under(conn, r"d:\media\")
            .await
            .unwrap()
            .into_values()
            .next()
            .unwrap();
        assert_eq!(delete_scan_error(conn, &entry.path).await.unwrap(), 1);
        assert_eq!(count(conn).await, 0);
    }

    #[tokio::test]
    async fn deletes_clear_one_path_and_a_swept_set() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        for path in [r"C:\m\a.png", r"C:\m\b.png", r"C:\m\c.png"] {
            upsert_scan_error(conn, &record(path, ApiErrorKind::Input), Some(1))
                .await
                .unwrap();
        }

        assert_eq!(delete_scan_error(conn, r"C:\m\a.png").await.unwrap(), 1);
        // The 99.99% case: nothing to clear, and no error.
        assert_eq!(delete_scan_error(conn, r"C:\m\a.png").await.unwrap(), 0);

        // The sweep takes the vanished paths in one statement, and tolerates
        // paths that are already gone.
        let swept = delete_scan_errors(
            conn,
            &[
                r"C:\m\b.png".to_string(),
                r"C:\m\c.png".to_string(),
                r"C:\m\never.png".to_string(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(swept, 2);
        assert_eq!(count(conn).await, 0);
        assert_eq!(delete_scan_errors(conn, &[]).await.unwrap(), 0);
    }

    // The audit list's filters, shaped after the extraction twin's. Each one
    // has to discriminate rather than merely return the whole (tiny) table,
    // and the count has to answer for the same set.
    #[tokio::test]
    async fn audit_list_filters_and_counts_the_same_set() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let mut video = record(r"C:\m\a.mp4", FFMPEG);
        video.stage = STAGE_METADATA.to_string();
        video.mime_type = Some("video/mp4".to_string());
        upsert_scan_error(conn, &video, Some(1)).await.unwrap();
        upsert_scan_error(conn, &record(r"C:\m\b.png", ApiErrorKind::Input), Some(1))
            .await
            .unwrap();
        // The mime guess itself failed, so this row has no mime type at all.
        let mut unguessable = record(r"C:\m\c.bin", ApiErrorKind::Input);
        unguessable.stage = STAGE_MIME.to_string();
        unguessable.mime_type = None;
        upsert_scan_error(conn, &unguessable, Some(1))
            .await
            .unwrap();

        let all = list_scan_errors(conn, &ScanErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 3, "no filters and no limit lists everything");
        let bin = all.iter().find(|row| row.path.ends_with("c.bin")).unwrap();
        assert_eq!(bin.mime_type, None);
        assert_eq!(bin.stage, STAGE_MIME);
        assert_eq!(bin.last_scan_id, Some(1));
        assert_eq!((bin.attempts, bin.skip_after), (1, 1));

        let blocked = list_scan_errors(
            conn,
            &ScanErrorFilters {
                error_class: Some(CLASS_BLOCKED.to_string()),
                stage: Some(STAGE_METADATA.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].blocker.as_deref(), Some("ffmpeg"));
        assert_eq!(blocked[0].mime_type.as_deref(), Some("video/mp4"));

        // The prefix is a range bound, not a pattern: wildcards are literal
        // bytes, an empty prefix filters nothing, and a NULL mime type is
        // outside every range.
        let images = list_scan_errors(
            conn,
            &ScanErrorFilters {
                mime_prefix: Some("image/".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].path, r"C:\m\b.png");

        for filters in [
            ScanErrorFilters::default(),
            ScanErrorFilters {
                error_class: Some(CLASS_INPUT.to_string()),
                ..Default::default()
            },
            ScanErrorFilters {
                stage: Some(STAGE_DECODE.to_string()),
                ..Default::default()
            },
            ScanErrorFilters {
                mime_prefix: Some("%".to_string()),
                ..Default::default()
            },
            ScanErrorFilters {
                mime_prefix: Some(String::new()),
                ..Default::default()
            },
            ScanErrorFilters {
                stage: Some("nonexistent".to_string()),
                ..Default::default()
            },
        ] {
            let listed = list_scan_errors(conn, &filters).await.unwrap().len() as i64;
            assert_eq!(
                count_scan_errors(conn, &filters).await.unwrap(),
                listed,
                "count and list must agree for {filters:?}"
            );
        }
        assert!(
            list_scan_errors(
                conn,
                &ScanErrorFilters {
                    mime_prefix: Some("%".to_string()),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_empty(),
            "'%' must not match every mime type"
        );
    }

    // Newest first, and the pages partition the result set: the audit surface
    // pages through this and a duplicated or dropped row would be invisible.
    // The count stays the whole filtered set, which is what the page numbers
    // are drawn against.
    #[tokio::test]
    async fn audit_list_orders_by_last_seen_and_pages_disjointly() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        for path in [r"C:\m\a.png", r"C:\m\b.png", r"C:\m\c.png"] {
            upsert_scan_error(conn, &record(path, ApiErrorKind::Input), Some(1))
                .await
                .unwrap();
        }
        // Hand-set an older timestamp: the writes above share a clock tick.
        sqlx::query("UPDATE scan_errors SET last_seen = '2000-01-01T00:00:00' WHERE path = ?")
            .bind(r"C:\m\b.png")
            .execute(&mut *conn)
            .await
            .unwrap();

        let all = list_scan_errors(conn, &ScanErrorFilters::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(
            all[2].path, r"C:\m\b.png",
            "the oldest last_seen sorts last"
        );
        assert!(all[0].last_seen >= all[1].last_seen && all[1].last_seen >= all[2].last_seen);

        let page = |offset: i64| ScanErrorFilters {
            limit: Some(2),
            offset,
            ..Default::default()
        };
        let first_page = list_scan_errors(conn, &page(0)).await.unwrap();
        let second_page = list_scan_errors(conn, &page(2)).await.unwrap();
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
        assert_eq!(
            count_scan_errors(conn, &page(0)).await.unwrap(),
            3,
            "the page window must not move the total"
        );

        // A caller-supplied page size can never turn into "no rows" or "every
        // row": zero and negatives clamp up, absurd sizes clamp down, and a
        // negative offset starts at the beginning.
        for limit in [Some(0), Some(-1)] {
            let rows = list_scan_errors(
                conn,
                &ScanErrorFilters {
                    limit,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(rows.len(), 1, "a bad limit clamps to one row, not none");
        }
        let rows = list_scan_errors(
            conn,
            &ScanErrorFilters {
                limit: Some(i64::MAX),
                offset: -5,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 3);
    }

    // The twin of the extraction ledger's offset test. Paging past the end is
    // what the UI does the moment a filter narrows the set under the offset it
    // is already on: an empty page against an unchanged total, never an error
    // and never a wrapped-around page.
    #[tokio::test]
    async fn audit_offset_past_the_total_is_an_empty_page() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        for path in [r"C:\m\a.png", r"C:\m\b.png"] {
            upsert_scan_error(conn, &record(path, ApiErrorKind::Input), Some(1))
                .await
                .unwrap();
        }

        for offset in [2, 500] {
            let filters = ScanErrorFilters {
                offset,
                ..Default::default()
            };
            assert!(
                list_scan_errors(conn, &filters).await.unwrap().is_empty(),
                "offset {offset} is past the end"
            );
            assert_eq!(
                count_scan_errors(conn, &filters).await.unwrap(),
                2,
                "the total is the filtered set, not the page"
            );
        }
    }

    // Auto-heal is per dependency: installing ffmpeg must not resurrect the
    // files waiting on pdfium, and must not touch input verdicts. The twin of
    // the extraction ledger's `blocked_clearing_is_scoped_to_the_named_blockers`.
    #[tokio::test]
    async fn blocked_clearing_is_scoped_to_the_named_blockers() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        upsert_scan_error(conn, &record(r"C:\m\a.mp4", FFMPEG), Some(1))
            .await
            .unwrap();
        upsert_scan_error(conn, &record(r"C:\m\b.pdf", PDFIUM), Some(1))
            .await
            .unwrap();
        upsert_scan_error(conn, &record(r"C:\m\c.png", ApiErrorKind::Input), Some(1))
            .await
            .unwrap();

        let mut blockers = list_distinct_scan_blockers(conn).await.unwrap();
        blockers.sort_by_key(|blocker| blocker.as_str());
        assert_eq!(blockers, vec![Blocker::Ffmpeg, Blocker::Pdfium]);

        assert_eq!(delete_blocked_scan_errors(conn, &[]).await.unwrap(), 0);
        let cleared = delete_blocked_scan_errors(conn, &[Blocker::Ffmpeg])
            .await
            .unwrap();
        assert_eq!(cleared, 1);
        assert_eq!(
            list_distinct_scan_blockers(conn).await.unwrap(),
            vec![Blocker::Pdfium]
        );
        assert_eq!(count(conn).await, 2, "the input verdict is untouched");
    }
}
