//! Plumbing shared by the two failure ledgers of
//! docs/failed-media-retry-design.md: `item_extraction_errors` (keyed per item
//! and setter) and `scan_errors` (keyed per path).
//!
//! The tables stay separate — their keys and their lifecycles have nothing in
//! common — but the taxonomy strings they persist, the clamp on the audit
//! message, and the `blocked` auto-heal are *one* mechanism. Keeping them here
//! is what stops the two halves from drifting into two slightly different
//! answers to the same question.

use std::borrow::Cow;

use crate::api_error::{ApiError, Blocker};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// `error_class` of a payload the pipeline's own decoder rejected. Only a
/// *reader* needs the constant — writers derive the class from the
/// [`crate::api_error::ApiErrorKind`] — so it stays unused until the failures
/// API lands.
#[allow(dead_code)]
pub(crate) const CLASS_INPUT: &str = "input";

/// `error_class` of the rows the auto-heal probe clears once their dependency
/// binds.
pub(crate) const CLASS_BLOCKED: &str = "blocked";

/// `error_class` of an entry that individually blew a resource limit. Reader
/// only, like [`CLASS_INPUT`]; lands with the failures API.
#[allow(dead_code)]
pub(crate) const CLASS_RESOURCE: &str = "resource";

/// The `error` column is an audit string, never matched on. A worker traceback
/// can be megabytes, and a ledger is read whole by the audit list, so it is
/// clamped at one choke point instead of at every classification site.
pub(crate) const MAX_ERROR_BYTES: usize = 2000;

/// The ledger tables. An enum rather than a `&str` so the table name
/// interpolated into the statements below cannot come from anywhere except
/// this file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LedgerTable {
    ItemExtractionErrors,
    ScanErrors,
}

impl LedgerTable {
    fn as_str(self) -> &'static str {
        match self {
            LedgerTable::ItemExtractionErrors => "item_extraction_errors",
            LedgerTable::ScanErrors => "scan_errors",
        }
    }
}

/// Auto-heal, write half: clears the `blocked` rows of every dependency that
/// now binds, so the entries waiting on it become selectable in the same run.
pub(crate) async fn delete_blocked_rows(
    conn: &mut sqlx::SqliteConnection,
    table: LedgerTable,
    blockers: &[Blocker],
) -> ApiResult<u64> {
    if blockers.is_empty() {
        return Ok(0);
    }

    let placeholders = std::iter::repeat_n("?", blockers.len())
        .collect::<Vec<_>>()
        .join(",");
    // Mixing numbered and bare placeholders misbinds parameters under sqlx,
    // so every placeholder here must stay unnumbered.
    let sql = format!(
        "DELETE FROM {} WHERE error_class = ? AND blocker IN ({placeholders})",
        table.as_str()
    );

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(CLASS_BLOCKED);
    for blocker in blockers {
        query = query.bind(blocker.as_str());
    }

    let result = query.execute(&mut *conn).await.map_err(|err| {
        tracing::error!(error = %err, table = table.as_str(), "failed to clear blocked failures");
        ApiError::internal("Failed to clear blocked failures")
    })?;
    Ok(result.rows_affected())
}

/// Auto-heal, read half: the dependencies this ledger is currently waiting on
/// — usually none. Only these backends get probed at job/scan start, so a run
/// never loads a library it has no use for. A value this build no longer knows
/// (a blocker retired since the row was written) is logged and skipped:
/// probing is best-effort, and the row stays until a retry directive clears it.
pub(crate) async fn list_distinct_blockers_in(
    conn: &mut sqlx::SqliteConnection,
    table: LedgerTable,
) -> ApiResult<Vec<Blocker>> {
    let sql = format!(
        "SELECT DISTINCT blocker FROM {} WHERE error_class = ? AND blocker IS NOT NULL",
        table.as_str()
    );
    let raw: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(CLASS_BLOCKED)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, table = table.as_str(), "failed to list ledger blockers");
            ApiError::internal("Failed to read recorded failures")
        })?;

    Ok(raw
        .iter()
        .filter_map(|value| match Blocker::parse(value) {
            Some(blocker) => Some(blocker),
            None => {
                tracing::warn!(
                    blocker = %value,
                    table = table.as_str(),
                    "unknown blocker in the failure ledger"
                );
                None
            }
        })
        .collect())
}

/// Clamps the audit message. Truncation happens on a char boundary, so the
/// stored text is always valid UTF-8.
pub(crate) fn truncate_error(error: &str) -> Cow<'_, str> {
    if error.len() <= MAX_ERROR_BYTES {
        return Cow::Borrowed(error);
    }
    // `str::floor_char_boundary` is still unstable, so walk back by hand.
    let mut end = MAX_ERROR_BYTES;
    while end > 0 && !error.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = String::with_capacity(end + '…'.len_utf8());
    truncated.push_str(&error[..end]);
    truncated.push('…');
    Cow::Owned(truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The persisted table names are schema, not display strings: renaming one
    // here silently points the auto-heal at a table that does not exist.
    #[test]
    fn table_names_are_the_schema_names() {
        assert_eq!(
            LedgerTable::ItemExtractionErrors.as_str(),
            "item_extraction_errors"
        );
        assert_eq!(LedgerTable::ScanErrors.as_str(), "scan_errors");
    }

    // A message that fits is borrowed untouched; one that does not is cut on a
    // char boundary, so the stored text is never invalid UTF-8.
    #[test]
    fn truncation_lands_on_a_char_boundary() {
        assert!(matches!(truncate_error("short"), Cow::Borrowed("short")));

        // Multi-byte chars straddle the cut at every offset the walk-back can
        // land on, which is exactly what a byte-index slice would panic on.
        let oversized = "é".repeat(MAX_ERROR_BYTES);
        let clamped = truncate_error(&oversized);
        assert!(clamped.ends_with('…'));
        assert!(clamped.len() <= MAX_ERROR_BYTES + '…'.len_utf8());
        assert!(clamped.trim_end_matches('…').chars().all(|ch| ch == 'é'));
    }
}
