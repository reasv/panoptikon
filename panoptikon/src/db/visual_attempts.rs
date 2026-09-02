//! The visuals negative cache (`storage.visual_attempts`).
//!
//! See docs/failed-media-retry-design.md. A row means "generating this kind of
//! visual for this content produced nothing worth storing, and running the
//! generator again would produce the same nothing" — never anything about what
//! is served. The two positive caches (`thumbnails`, `frames`) stay the only
//! authority for serving; this table is consulted *after* they miss, and only
//! to decide whether to schedule work.
//!
//! That makes every writer here advisory: a lost row costs one wasted
//! generation, a stale row costs one wasted generation the next time the
//! version moves. Neither can produce a wrong answer to a user.
//!
//! The third ledger of the failed-media design, and the only one that does not
//! live in index.db: it shares a lifecycle with the positive caches it
//! shadows, so deleting storage.db to force a visuals rebuild drops these
//! markers with it. Everything it shares with the other two — the `blocked`
//! auto-heal, the audit-message clamp — comes from [`crate::db::ledger`].

use crate::api_error::{ApiError, ApiErrorKind, Blocker};
use crate::db::extraction_write::current_iso_timestamp;
use crate::db::ledger::{LedgerTable, delete_blocked_rows, list_distinct_blockers_in};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Which table the shared ledger helpers operate on for this third half.
const TABLE: LedgerTable = LedgerTable::VisualAttempts;

/// Which positive cache a marker shadows. The stored `kind` column is a plain
/// string with no `CHECK` list — a new kind must not require a table rebuild —
/// so this enum is what actually makes an invalid value unrepresentable: every
/// write goes through [`VisualKind::as_str`] and every read through
/// [`VisualKind::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VisualKind {
    /// Shadows `storage.thumbnails`.
    Thumbnail,
    /// Shadows `storage.frames`.
    Frame,
    /// Failures of the appended-outro probe
    /// (docs/video-outro-detection-design.md §7.2).
    ///
    /// The one acknowledged semantic stretch in this table: every other kind
    /// shadows a *storage-side* positive cache, but the outro verdict lives in
    /// `items.outro_kind` over in index.db. The design accepts it because the
    /// advisory property holds in both mismatch directions — "a marker
    /// orphaned by a storage.db wipe costs one ~85ms re-probe; one surviving
    /// an index.db rebuild correctly suppresses re-probing a file that would
    /// fail again; never a wrong answer" — and the alternative is a parallel
    /// index-side ledger, i.e. duplicated machinery for what measured 0.37% of
    /// files. The positive cache this one is consulted *after* is the
    /// `outro_kind` column itself: a row that already carries a verdict is
    /// never dispatched, so the marker is only ever read for a file with none.
    Outro,
    /// Shadows the `loop` row of `storage.thumbnail_tiers` — an animated
    /// item's H.264 rendition
    /// (docs/grid-scroll-performance-implementation.md §2, step B2).
    ///
    /// Its own kind rather than a second use of [`VisualKind::Thumbnail`],
    /// for two reasons that both bite:
    ///
    /// * **Truthfulness.** A loop encode fails on a file whose pixels decoded
    ///   perfectly — its posters were built from that very decode moments
    ///   earlier. A `thumbnail` marker would assert the opposite.
    /// * **Blast radius.** A `thumbnail` marker also suppresses the *display*
    ///   rendition, so a file ffmpeg cannot encode would lose the still it is
    ///   perfectly capable of producing — including later, when the display
    ///   rule flips and starts wanting one.
    ///
    /// Keyed to `TIER_PROCESS_VERSION`, which is what gives loop failures the
    /// heal path the design asks for: bumping the tier generator retires them
    /// through this table's existing `version >= ?` consult, without touching
    /// `THUMBNAIL_PROCESS_VERSION` (which §2 forbids bumping for tier work).
    Loop,
}

impl VisualKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            VisualKind::Thumbnail => "thumbnail",
            VisualKind::Frame => "frame",
            VisualKind::Outro => "outro",
            VisualKind::Loop => "loop",
        }
    }

    /// The read half of the vocabulary. Nothing in the gateway reads `kind`
    /// back yet (the table is written and consulted by key), but the retry
    /// directives and the audit surface will, and a stored string that no
    /// variant covers must be a `None` rather than a silently-defaulted kind.
    #[allow(dead_code)] // Write-only today; see above.
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "thumbnail" => Some(VisualKind::Thumbnail),
            "frame" => Some(VisualKind::Frame),
            "outro" => Some(VisualKind::Outro),
            "loop" => Some(VisualKind::Loop),
            _ => None,
        }
    }
}

/// `outcome` of a generator that ran and correctly produced nothing.
pub(crate) const OUTCOME_NONE: &str = "none";

/// `outcome` of a generator whose backend is not installed.
pub(crate) const OUTCOME_BLOCKED: &str = "blocked";

/// `outcome` of a generator that ran and failed on this content.
pub(crate) const OUTCOME_FAILED: &str = "failed";

/// A visuals-grade failure: everything the marker needs except the item's
/// identity and the kind it belongs to.
///
/// The twin of [`crate::jobs::files::ScanFailure`], and deliberately carrying
/// the [`ApiErrorKind`] rather than a persisted string: `outcome` and `blocker`
/// are derived in [`upsert_visual_attempts`], so an inconsistent pair (a
/// `blocked` row with no blocker) is unrepresentable.
#[derive(Debug, Clone)]
pub(crate) struct VisualFailure {
    pub(crate) kind: ApiErrorKind,
    /// The confirmation threshold this *site* earned, not the class's: an
    /// in-memory encode settles at one attempt, an external tool that did its
    /// own file I/O does not.
    pub(crate) skip_after: i64,
    pub(crate) message: String,
}

/// What one generation pass concluded about one kind. Produced visuals emit no
/// verdict at all — the positive cache is their record.
#[derive(Debug, Clone)]
pub(crate) struct VisualVerdict {
    pub(crate) kind: VisualKind,
    /// `None` is the `none` outcome: the generator ran and there is genuinely
    /// nothing to store for this content.
    pub(crate) failure: Option<VisualFailure>,
}

impl VisualVerdict {
    /// The generator ran and correctly produced nothing.
    pub(crate) fn nothing(kind: VisualKind) -> Self {
        Self {
            kind,
            failure: None,
        }
    }

    pub(crate) fn failed(kind: VisualKind, failure: VisualFailure) -> Self {
        Self {
            kind,
            failure: Some(failure),
        }
    }

    /// Attaches the identity only the scan knows. `version` is the generator
    /// version that produced this verdict, which is what a later consult
    /// compares against.
    pub(crate) fn into_record(
        self,
        item_sha256: impl Into<String>,
        item_mime_type: impl Into<String>,
        version: i64,
    ) -> VisualAttemptRecord {
        VisualAttemptRecord {
            item_sha256: item_sha256.into(),
            kind: self.kind,
            item_mime_type: item_mime_type.into(),
            version,
            failure: self.failure,
        }
    }
}

/// One marker to write. Owned fields so the writer actor's message can carry
/// it.
#[derive(Debug, Clone)]
pub(crate) struct VisualAttemptRecord {
    pub(crate) item_sha256: String,
    pub(crate) kind: VisualKind,
    pub(crate) item_mime_type: String,
    pub(crate) version: i64,
    pub(crate) failure: Option<VisualFailure>,
}

/// `attempts` counts *runs that saw the same conclusion*, not conclusions.
///
/// Two rules, in this order:
///
/// 1. A changed `version` resets it to 1. The stored confirmations were about
///    a different generator, so they say nothing about this one — the same
///    reasoning as `scan_errors` resetting on a changed `(mtime, size)`.
/// 2. Otherwise it increments only when the scan changed. `IS NOT` is SQLite's
///    null-safe inequality, so a row written by a scan-less caller still
///    increments once a real scan touches it.
///
/// A changed outcome refreshes the verdict but does *not* reset the count: an
/// item whose verdict alternates between runs would otherwise never reach
/// `skip_after` and would be regenerated forever.
///
/// The target table is referenced unqualified inside `DO UPDATE` (`storage.`
/// is not accepted in a column reference there); it resolves to the row being
/// updated.
const UPSERT_SQL: &str = r#"
    INSERT INTO storage.visual_attempts (
        item_sha256, kind, item_mime_type, version, outcome, blocker,
        skip_after, attempts, last_scan_id, error, first_seen, last_attempt
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    ON CONFLICT(item_sha256, kind) DO UPDATE SET
        attempts = CASE
            WHEN visual_attempts.version IS NOT excluded.version THEN 1
            WHEN visual_attempts.last_scan_id IS NOT excluded.last_scan_id
                THEN visual_attempts.attempts + 1
            ELSE visual_attempts.attempts
        END,
        item_mime_type = excluded.item_mime_type,
        version = excluded.version,
        outcome = excluded.outcome,
        blocker = excluded.blocker,
        skip_after = excluded.skip_after,
        last_scan_id = excluded.last_scan_id,
        error = excluded.error,
        last_attempt = excluded.last_attempt
"#;

/// Records (or re-records) what a generation pass concluded, in one
/// transaction — a single file's pass can owe both a thumbnail and a frame
/// marker, and they are one conclusion.
///
/// `scan_id` is the run that saw it, which is what dedups `attempts`. `None` is
/// allowed but blunt: two consecutive scan-less writes are indistinguishable
/// and increment only once, so a caller with no scan identity must stick to
/// `skip_after = 1` verdicts or its rows will never reach their threshold.
pub(crate) async fn upsert_visual_attempts(
    conn: &mut sqlx::SqliteConnection,
    records: &[VisualAttemptRecord],
    scan_id: Option<i64>,
) -> ApiResult<()> {
    let now = current_iso_timestamp();
    for record in records {
        // Derived here rather than at the classification site, so an
        // inconsistent (outcome, blocker) pair cannot be constructed.
        let (outcome, blocker, error) = match &record.failure {
            None => (OUTCOME_NONE, None, None),
            Some(failure) => {
                let Some(class) = failure.kind.persisted_class() else {
                    // A transient failure is not a verdict on the content: the
                    // pass simply failed this run and is retried untouched.
                    // Unreachable through the classification sites, which is
                    // why this is an error rather than a silent skip.
                    tracing::error!(
                        sha256 = %record.item_sha256,
                        kind = record.kind.as_str(),
                        "refused to persist a transient visuals failure"
                    );
                    return Err(ApiError::internal("transient failures are not persisted"));
                };
                let outcome = if class == OUTCOME_BLOCKED {
                    OUTCOME_BLOCKED
                } else {
                    // `input` and `resource` both mean "the generator ran and
                    // this content did not come out"; the finer class lives in
                    // the two index-side ledgers, which are the audit surface.
                    OUTCOME_FAILED
                };
                (
                    outcome,
                    failure.kind.blocker().map(Blocker::as_str),
                    Some(crate::db::ledger::truncate_error(&failure.message)),
                )
            }
        };
        let skip_after = record
            .failure
            .as_ref()
            .map(|failure| failure.skip_after)
            .unwrap_or(1);

        sqlx::query(UPSERT_SQL)
            .bind(&record.item_sha256)
            .bind(record.kind.as_str())
            .bind(&record.item_mime_type)
            .bind(record.version)
            .bind(outcome)
            .bind(blocker)
            .bind(skip_after)
            .bind(scan_id)
            .bind(error.as_deref())
            .bind(&now)
            .bind(&now)
            .execute(&mut *conn)
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    sha256 = %record.item_sha256,
                    kind = record.kind.as_str(),
                    "failed to upsert a visual attempt"
                );
                ApiError::internal("Failed to record a visuals attempt")
            })?;
    }
    Ok(())
}

/// Whether the negative cache says this generation would be wasted work.
///
/// Consulted only after `has_thumbnail`/`has_frame` has already missed, and
/// only to skip *scheduling*. `version >= ?` is the same invalidation shape the
/// positive caches use, so bumping a `*_PROCESS_VERSION` retires every marker
/// for free. `attempts >= skip_after` is the confirmation threshold: an
/// ambiguous verdict (an external tool that read the file itself) has to repeat
/// in a *later* run before it suppresses anything, while `none` and `blocked`
/// are written at threshold 1 and therefore bite immediately.
pub(crate) async fn visuals_suppressed(
    conn: &mut sqlx::SqliteConnection,
    item_sha256: &str,
    kind: VisualKind,
    process_version: i64,
) -> ApiResult<bool> {
    let row: (i64,) = sqlx::query_as(
        r#"
SELECT EXISTS(
    SELECT 1
    FROM storage.visual_attempts
    WHERE item_sha256 = ?1
      AND kind = ?2
      AND version >= ?3
      AND attempts >= skip_after
    LIMIT 1
) AS exists_flag
        "#,
    )
    .bind(item_sha256)
    .bind(kind.as_str())
    .bind(process_version)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
        tracing::error!(error = %err, sha256 = %item_sha256, kind = kind.as_str(), "failed to read a visual attempt");
        ApiError::internal("Failed to read visuals attempts")
    })?;

    Ok(row.0 == 1)
}

/// Drops the marker for one (content, kind). Called from `store_thumbnails` /
/// `store_frames` inside their own transaction, so the negative cache can never
/// outlive the positive one.
pub(crate) async fn delete_visual_attempt(
    conn: &mut sqlx::SqliteConnection,
    item_sha256: &str,
    kind: VisualKind,
) -> ApiResult<u64> {
    let result = sqlx::query("DELETE FROM storage.visual_attempts WHERE item_sha256 = ? AND kind = ?")
        .bind(item_sha256)
        .bind(kind.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, sha256 = %item_sha256, kind = kind.as_str(), "failed to clear a visual attempt");
            ApiError::internal("Failed to clear a visuals attempt")
        })?;
    Ok(result.rows_affected())
}

/// Auto-heal, write half. The twin of
/// [`crate::db::scan_errors::delete_blocked_scan_errors`].
pub(crate) async fn delete_blocked_visual_attempts(
    conn: &mut sqlx::SqliteConnection,
    blockers: &[Blocker],
) -> ApiResult<u64> {
    delete_blocked_rows(conn, TABLE, blockers).await
}

/// Auto-heal, read half. The twin of
/// [`crate::db::scan_errors::list_distinct_scan_blockers`].
pub(crate) async fn list_distinct_visual_blockers(
    conn: &mut sqlx::SqliteConnection,
) -> ApiResult<Vec<Blocker>> {
    list_distinct_blockers_in(conn, TABLE).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::ledger::MAX_ERROR_BYTES;
    use crate::db::migrations::{migrate_databases_on_disk, setup_test_databases};
    use crate::test_utils::test_data_dir;

    const THUMB_VERSION: i64 = 1;

    fn nothing(sha: &str, kind: VisualKind) -> VisualAttemptRecord {
        VisualVerdict::nothing(kind).into_record(sha, "video/mp4", THUMB_VERSION)
    }

    fn failed(
        sha: &str,
        kind: VisualKind,
        err: ApiErrorKind,
        skip_after: i64,
    ) -> VisualAttemptRecord {
        VisualVerdict::failed(
            kind,
            VisualFailure {
                kind: err,
                skip_after,
                message: "ffmpeg failed".to_string(),
            },
        )
        .into_record(sha, "video/mp4", THUMB_VERSION)
    }

    async fn row(
        conn: &mut sqlx::SqliteConnection,
        sha: &str,
        kind: VisualKind,
    ) -> (String, Option<String>, i64, i64, Option<String>, String) {
        sqlx::query_as(
            "SELECT outcome, blocker, attempts, skip_after, error, first_seen \
             FROM storage.visual_attempts WHERE item_sha256 = ? AND kind = ?",
        )
        .bind(sha)
        .bind(kind.as_str())
        .fetch_one(&mut *conn)
        .await
        .unwrap()
    }

    async fn count(conn: &mut sqlx::SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM storage.visual_attempts")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }

    // The migration has to land on a real file-backed storage database, not
    // only through the in-memory harness: this is the first Rust migration
    // *ever* applied on top of the storage init snapshot, so the baseline gate
    // that lets Python-created databases through is exercised here for the
    // first time on this migrator.
    #[tokio::test]
    async fn migration_creates_the_visuals_cache() {
        let _test_env = test_data_dir();
        migrate_databases_on_disk(
            Some("visual_attempts_migration"),
            Some("visual_attempts_user"),
        )
        .await
        .expect("migrate test databases");
        let mut conn = crate::db::open_index_db_write_no_user_data("visual_attempts_migration")
            .await
            .unwrap();

        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM storage.sqlite_master \
             WHERE type = 'index' AND tbl_name = 'visual_attempts' AND name IS NOT NULL \
             ORDER BY name",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(
            indexes.contains(&"idx_visual_attempts_outcome".to_string()),
            "the auto-heal/directive index must exist: {indexes:?}"
        );
        // The composite PRIMARY KEY is what the point lookup seeks; on a rowid
        // table SQLite names that index itself.
        assert!(
            indexes
                .iter()
                .any(|name| name.starts_with("sqlite_autoindex_visual_attempts")),
            "the primary key must have an index to seek: {indexes:?}"
        );
        // A rowid table, deliberately (the `error` column is payload up to
        // ~2 KB): WITHOUT ROWID caps in-leaf payload at roughly page/4 and
        // would push every failed row into overflow pages, slowing the very
        // lookup this table exists to make cheap. Only a rowid table answers
        // this query.
        sqlx::query("SELECT rowid FROM storage.visual_attempts LIMIT 1")
            .fetch_optional(&mut conn)
            .await
            .expect("the marker table must be a rowid table");
    }

    // The table's own guards: a `blocked` row without its dependency is a lie
    // the auto-heal probe would act on, and a threshold below one would
    // suppress a generation that never failed once.
    #[tokio::test]
    async fn migration_check_constraints_reject_inconsistent_rows() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        let insert = |sha: &'static str,
                      outcome: &'static str,
                      blocker: Option<&'static str>,
                      skip_after: i64| {
            sqlx::query(
                "INSERT INTO storage.visual_attempts (item_sha256, kind, item_mime_type, version, \
                 outcome, blocker, skip_after, attempts, first_seen, last_attempt) \
                 VALUES (?, 'thumbnail', 'video/mp4', 1, ?, ?, ?, 1, 'now', 'now')",
            )
            .bind(sha)
            .bind(outcome)
            .bind(blocker)
            .bind(skip_after)
        };

        assert!(
            insert("a", OUTCOME_BLOCKED, None, 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "a blocked marker must name its dependency"
        );
        assert!(
            insert("b", OUTCOME_FAILED, Some("ffmpeg"), 1)
                .execute(&mut *conn)
                .await
                .is_err(),
            "only blocked markers carry a blocker"
        );
        assert!(
            insert("c", OUTCOME_NONE, None, 0)
                .execute(&mut *conn)
                .await
                .is_err(),
            "skip_after must be at least one attempt"
        );
        assert!(
            insert("d", OUTCOME_NONE, None, 1)
                .execute(&mut *conn)
                .await
                .is_ok()
        );
        // (item_sha256, kind) is the key: a second marker for the same pair is
        // a conflict the upsert resolves, never a duplicate.
        assert!(
            insert("d", OUTCOME_NONE, None, 1)
                .execute(&mut *conn)
                .await
                .is_err()
        );
    }

    // The three outcomes and their thresholds, which together are the whole
    // scheduling contract: `none` and `blocked` bite immediately, an ambiguous
    // `failed` needs a second run, and a version bump retires all of them.
    #[tokio::test]
    async fn outcomes_suppress_at_their_threshold_and_expire_on_a_version_bump() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        // A legitimate nothing: never retried at this version.
        upsert_visual_attempts(conn, &[nothing("sha_none", VisualKind::Thumbnail)], Some(1))
            .await
            .unwrap();
        let (outcome, blocker, attempts, skip_after, error, _) =
            row(conn, "sha_none", VisualKind::Thumbnail).await;
        assert_eq!(
            (outcome.as_str(), blocker, attempts, skip_after, error),
            (OUTCOME_NONE, None, 1, 1, None)
        );
        assert!(
            visuals_suppressed(conn, "sha_none", VisualKind::Thumbnail, THUMB_VERSION)
                .await
                .unwrap()
        );
        // Kinds are independent: a thumbnail verdict says nothing about frames.
        assert!(
            !visuals_suppressed(conn, "sha_none", VisualKind::Frame, THUMB_VERSION)
                .await
                .unwrap()
        );
        // And a newer generator ignores it entirely, with no cleanup pass.
        assert!(
            !visuals_suppressed(conn, "sha_none", VisualKind::Thumbnail, THUMB_VERSION + 1)
                .await
                .unwrap()
        );

        // An ambiguous failure: one run is not a verdict yet.
        upsert_visual_attempts(
            conn,
            &[failed("sha_bad", VisualKind::Frame, ApiErrorKind::Input, 2)],
            Some(1),
        )
        .await
        .unwrap();
        assert!(
            !visuals_suppressed(conn, "sha_bad", VisualKind::Frame, THUMB_VERSION)
                .await
                .unwrap(),
            "an unconfirmed failure must be re-attempted"
        );
        // The same scan again is not a confirmation.
        upsert_visual_attempts(
            conn,
            &[failed("sha_bad", VisualKind::Frame, ApiErrorKind::Input, 2)],
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(row(conn, "sha_bad", VisualKind::Frame).await.2, 1);
        // A later one is.
        upsert_visual_attempts(
            conn,
            &[failed("sha_bad", VisualKind::Frame, ApiErrorKind::Input, 2)],
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(row(conn, "sha_bad", VisualKind::Frame).await.2, 2);
        assert!(
            visuals_suppressed(conn, "sha_bad", VisualKind::Frame, THUMB_VERSION)
                .await
                .unwrap()
        );

        // A resource verdict is still "the generator ran and nothing came
        // out": `failed`, with the finer class left to the audit ledgers.
        upsert_visual_attempts(
            conn,
            &[failed(
                "sha_big",
                VisualKind::Thumbnail,
                ApiErrorKind::Resource,
                1,
            )],
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(
            row(conn, "sha_big", VisualKind::Thumbnail).await.0,
            OUTCOME_FAILED
        );

        // A missing dependency names it, so the auto-heal can find it.
        upsert_visual_attempts(
            conn,
            &[failed(
                "sha_pdf",
                VisualKind::Thumbnail,
                ApiErrorKind::Blocked {
                    blocker: Blocker::Pdfium,
                },
                1,
            )],
            Some(1),
        )
        .await
        .unwrap();
        let (outcome, blocker, ..) = row(conn, "sha_pdf", VisualKind::Thumbnail).await;
        assert_eq!(
            (outcome.as_str(), blocker.as_deref()),
            (OUTCOME_BLOCKED, Some("pdfium"))
        );
    }

    // A version bump does not just stop suppressing — it also starts the
    // confirmation count over, because the stored confirmations were about a
    // different generator. Without this an item that failed twice under v1
    // would be suppressed by its *first* failure under v2.
    #[tokio::test]
    async fn a_version_bump_restarts_the_confirmation_count() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        for scan in [1, 2] {
            upsert_visual_attempts(
                conn,
                &[failed("sha", VisualKind::Thumbnail, ApiErrorKind::Input, 2)],
                Some(scan),
            )
            .await
            .unwrap();
        }
        assert_eq!(row(conn, "sha", VisualKind::Thumbnail).await.2, 2);

        let mut next = failed("sha", VisualKind::Thumbnail, ApiErrorKind::Input, 2);
        next.version = THUMB_VERSION + 1;
        upsert_visual_attempts(conn, &[next], Some(3))
            .await
            .unwrap();
        assert_eq!(
            row(conn, "sha", VisualKind::Thumbnail).await.2,
            1,
            "a new generator earns its own confirmations"
        );
        assert!(
            !visuals_suppressed(conn, "sha", VisualKind::Thumbnail, THUMB_VERSION + 1)
                .await
                .unwrap()
        );
        assert_eq!(count(conn).await, 1, "one marker per (content, kind)");
    }

    // `last_scan_id` is nullable and the dedup comparison is `IS NOT`,
    // SQLite's null-safe inequality: a plain `<>` yields NULL against a
    // scan-less row and would silently stop counting. A changed outcome keeps
    // counting, or a verdict that alternates between runs would be regenerated
    // forever.
    #[tokio::test]
    async fn attempts_count_across_null_scans_and_changed_outcomes() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        upsert_visual_attempts(
            conn,
            &[failed("sha", VisualKind::Thumbnail, ApiErrorKind::Input, 2)],
            None,
        )
        .await
        .unwrap();
        upsert_visual_attempts(
            conn,
            &[failed("sha", VisualKind::Thumbnail, ApiErrorKind::Input, 2)],
            Some(7),
        )
        .await
        .unwrap();
        assert_eq!(row(conn, "sha", VisualKind::Thumbnail).await.2, 2);

        // A different conclusion refreshes the row without resetting it.
        upsert_visual_attempts(conn, &[nothing("sha", VisualKind::Thumbnail)], Some(8))
            .await
            .unwrap();
        let (outcome, _, attempts, skip_after, ..) = row(conn, "sha", VisualKind::Thumbnail).await;
        assert_eq!(
            (outcome.as_str(), attempts, skip_after),
            (OUTCOME_NONE, 3, 1)
        );
    }

    // Storing visuals clears the marker; the two writes are in one transaction
    // at the call site, so the negative cache can never outlive the positive
    // one. Here only the delete half is exercised — the same-transaction
    // property is `db::storage`'s test.
    #[tokio::test]
    async fn markers_are_dropped_per_kind() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        upsert_visual_attempts(
            conn,
            &[
                nothing("sha", VisualKind::Thumbnail),
                nothing("sha", VisualKind::Frame),
            ],
            Some(1),
        )
        .await
        .unwrap();

        assert_eq!(
            delete_visual_attempt(conn, "sha", VisualKind::Thumbnail)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            delete_visual_attempt(conn, "sha", VisualKind::Thumbnail)
                .await
                .unwrap(),
            0,
            "the 99.99% case: nothing to clear, and no error"
        );
        assert_eq!(count(conn).await, 1, "the frame marker is untouched");
    }

    // A transient failure has no marker by construction; the upsert is the
    // single guard, and the audit message is clamped in the same place.
    #[tokio::test]
    async fn transient_verdicts_are_refused_and_messages_clamped() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        assert!(
            upsert_visual_attempts(
                conn,
                &[failed(
                    "sha",
                    VisualKind::Thumbnail,
                    ApiErrorKind::Generic,
                    1
                )],
                Some(1)
            )
            .await
            .is_err()
        );
        assert_eq!(count(conn).await, 0);

        let mut huge = failed("sha", VisualKind::Thumbnail, ApiErrorKind::Input, 1);
        huge.failure.as_mut().unwrap().message = "é".repeat(MAX_ERROR_BYTES);
        upsert_visual_attempts(conn, &[huge], Some(1))
            .await
            .unwrap();
        let stored = row(conn, "sha", VisualKind::Thumbnail).await.4.unwrap();
        assert!(stored.ends_with('…'));
        assert!(stored.len() <= MAX_ERROR_BYTES + '…'.len_utf8());
    }

    // Auto-heal is per dependency and, unlike the other two ledgers, has to
    // reach across into storage.db: installing a browser must not resurrect
    // the PDFs waiting on pdfium, and must not touch content verdicts.
    #[tokio::test]
    async fn blocked_clearing_is_scoped_to_the_named_blockers() {
        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;

        for (sha, blocker) in [
            ("sha_pdf", Blocker::Pdfium),
            ("sha_html", Blocker::HtmlRenderer),
        ] {
            upsert_visual_attempts(
                conn,
                &[failed(
                    sha,
                    VisualKind::Thumbnail,
                    ApiErrorKind::Blocked { blocker },
                    1,
                )],
                Some(1),
            )
            .await
            .unwrap();
        }
        upsert_visual_attempts(
            conn,
            &[failed(
                "sha_bad",
                VisualKind::Thumbnail,
                ApiErrorKind::Input,
                1,
            )],
            Some(1),
        )
        .await
        .unwrap();

        let mut blockers = list_distinct_visual_blockers(conn).await.unwrap();
        blockers.sort_by_key(|blocker| blocker.as_str());
        assert_eq!(blockers, vec![Blocker::HtmlRenderer, Blocker::Pdfium]);

        assert_eq!(delete_blocked_visual_attempts(conn, &[]).await.unwrap(), 0);
        assert_eq!(
            delete_blocked_visual_attempts(conn, &[Blocker::HtmlRenderer])
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            list_distinct_visual_blockers(conn).await.unwrap(),
            vec![Blocker::Pdfium]
        );
        assert_eq!(count(conn).await, 2, "the content verdict is untouched");
    }
}
