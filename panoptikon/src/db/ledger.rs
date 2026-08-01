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

use sqlx::Row;

use crate::api_error::{ApiError, Blocker};

type ApiResult<T> = std::result::Result<T, ApiError>;

/// `error_class` of a payload the pipeline's own decoder rejected. Only a
/// *reader* needs the constant — writers derive the class from the
/// [`crate::api_error::ApiErrorKind`].
pub(crate) const CLASS_INPUT: &str = "input";

/// `error_class` of the rows the auto-heal probe clears once their dependency
/// binds.
pub(crate) const CLASS_BLOCKED: &str = "blocked";

/// `error_class` of an entry that individually blew a resource limit. Reader
/// only, like [`CLASS_INPUT`].
pub(crate) const CLASS_RESOURCE: &str = "resource";

/// The whole persisted `error_class` vocabulary, in the order the audit
/// surface offers it. The tables deliberately carry no `CHECK` on the column
/// (a new class must not require a table rebuild), so this is a *reader's*
/// list: it is what the failures API validates a filter against, never what
/// constrains a write.
pub(crate) const ERROR_CLASSES: [&str; 3] = [CLASS_INPUT, CLASS_BLOCKED, CLASS_RESOURCE];

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

/// Page size an audit list serves when the caller does not ask for one.
pub(crate) const DEFAULT_LIST_LIMIT: i64 = 100;

/// Hard cap on an audit page: the lists join per row and are served to a UI,
/// so an unbounded page is never what the caller wanted.
pub(crate) const MAX_LIST_LIMIT: i64 = 1000;

/// `None` means [`DEFAULT_LIST_LIMIT`]; anything else is clamped into
/// `1..=MAX_LIST_LIMIT`. Clamping *up* is the point: a caller-defaulted `0`
/// would otherwise silently return nothing.
pub(crate) fn clamp_list_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

/// The `WHERE` clause an audit list shares with its `COUNT(*)` twin, plus the
/// values to bind, in order.
///
/// `equals` and `mime_column` are SQL fragments and must stay caller-side
/// literals — only the *values* ever come from a request. Every placeholder is
/// unnumbered: mixing numbered and bare placeholders misbinds parameters under
/// sqlx.
///
/// The mime prefix becomes a half-open range, not `LIKE ? || '%'`. LIKE would
/// only be indexable here because this repo sets `case_sensitive_like`; the
/// range form is pragma-independent, needs no escaping of `%`/`_`, and is the
/// house rule for prefix matching (see [`crate::db::prefix`]). An empty prefix
/// filters nothing, and a prefix with no representable successor degrades to
/// the lower bound alone.
pub(crate) fn audit_filter_sql(
    equals: &[(&str, Option<&str>)],
    mime_column: &str,
    mime_prefix: Option<&str>,
) -> (String, Vec<String>) {
    let mut conditions: Vec<String> = Vec::new();
    let mut binds: Vec<String> = Vec::new();
    for (column, value) in equals {
        if let Some(value) = value {
            conditions.push(format!("{column} = ?"));
            binds.push((*value).to_string());
        }
    }
    if let Some(prefix) = mime_prefix.filter(|prefix| !prefix.is_empty()) {
        conditions.push(format!("{mime_column} >= ?"));
        binds.push(prefix.to_string());
        if let Some(upper) = crate::db::prefix::prefix_upper_bound(prefix) {
            conditions.push(format!("{mime_column} < ?"));
            binds.push(upper);
        }
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    (where_clause, binds)
}

/// Reads one column of an audit row by name. Both lists build their SQL by
/// interpolation, so a column that does not decode is a bug in this crate, not
/// a bad request — hence the log line and the flat internal error.
pub(crate) fn read_audit_column<'r, T>(
    row: &'r sqlx::sqlite::SqliteRow,
    column: &str,
) -> ApiResult<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get(column).map_err(|err| {
        tracing::error!(error = %err, column, "failed to read a failure ledger column");
        ApiError::internal("Failed to read recorded failures")
    })
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

    // The clause and the bind vector are built in one pass and consumed in
    // two places (the list and its COUNT twin); a bind that does not line up
    // with its placeholder silently filters on the wrong column.
    #[test]
    fn audit_filters_pair_every_placeholder_with_its_value() {
        let (clause, binds) =
            audit_filter_sql(&[("a.x", None), ("b.y", None)], "e.mime_type", None);
        assert!(
            clause.is_empty() && binds.is_empty(),
            "no filters, no clause"
        );

        // An empty prefix is "no filter", not "matches the empty string".
        let (clause, binds) = audit_filter_sql(&[], "e.mime_type", Some(""));
        assert!(clause.is_empty() && binds.is_empty());

        let (clause, binds) = audit_filter_sql(
            &[("setters.name", Some("test/clip")), ("e.stage", None)],
            "e.mime_type",
            Some("image/"),
        );
        assert_eq!(
            clause,
            "WHERE setters.name = ? AND e.mime_type >= ? AND e.mime_type < ?"
        );
        assert_eq!(binds, vec!["test/clip", "image/", "image0"]);
        assert_eq!(
            clause.matches('?').count(),
            binds.len(),
            "one bind per placeholder"
        );

        // A prefix with no representable successor keeps the lower bound
        // alone rather than dropping the filter or binding a stray value.
        let (clause, binds) = audit_filter_sql(&[], "e.mime_type", Some("\u{10FFFF}"));
        assert_eq!(clause, "WHERE e.mime_type >= ?");
        assert_eq!(binds.len(), 1);
    }

    // A page size can never turn into "no rows" or "every row".
    #[test]
    fn list_limit_clamps_in_both_directions() {
        assert_eq!(clamp_list_limit(None), DEFAULT_LIST_LIMIT);
        assert_eq!(clamp_list_limit(Some(0)), 1);
        assert_eq!(clamp_list_limit(Some(-5)), 1);
        assert_eq!(clamp_list_limit(Some(i64::MAX)), MAX_LIST_LIMIT);
        assert_eq!(clamp_list_limit(Some(25)), 25);
    }
}
