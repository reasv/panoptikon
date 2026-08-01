use sea_query::{Alias, Expr, ExprTrait, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::preprocess::PqlError;

use super::super::{
    CteRef, ItemExtractionErrors, JoinedTables, QueryState, Setters, apply_group_by,
    get_std_group_by, select_std_from_cte, wrap_query,
};
use super::FilterCompiler;

/// The alias the setter lookup joins under. A bare `setters` join would
/// collide with the one `add_inner_joins` adds when this filter is a query's
/// root, and the alias is also what lets the filter mark nothing in
/// [`JoinedTables`]: every base table the outer query needs is still its own
/// to join.
const SETTER_ALIAS: &str = "failed_for_setters";

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct FailedFor {
    /// This Item has an active extraction-failure record for this setter name
    /// (the pipeline rejected its media, or a dependency it needs is missing)
    pub failed_for: String,
}

impl FilterCompiler for FailedFor {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let cte_name = format!("n{}_FailedFor", state.cte_counter);
        let mut query = select_std_from_cte(context, state);
        // Keyed on the item even for text-entity queries: a prepare failure
        // belongs to the (item, setter) pair, not to one extracted-text row,
        // and a text source that itself failed to extract has no data rows to
        // key on in the first place.
        //
        // The consequence constrains the worker-protocol phase (step 4): a
        // per-item worker verdict on a text-entity model is really a verdict
        // on one data row, and persisting it here would take *every* segment
        // of that item out of the work query. Worker-reported input errors
        // for text-entity models therefore stay transient until the ledger
        // gains a nullable data_id and this join learns to match on it (see
        // docs/failed-media-retry-design.md, "Granularity caveat").
        query.join(
            JoinType::InnerJoin,
            ItemExtractionErrors::Table,
            Expr::col((ItemExtractionErrors::Table, ItemExtractionErrors::ItemId))
                .equals(context.column_ref("item_id")),
        );
        query.join_as(
            JoinType::InnerJoin,
            Setters::Table,
            Alias::new(SETTER_ALIAS),
            Expr::col((Alias::new(SETTER_ALIAS), Setters::Id))
                .equals((ItemExtractionErrors::Table, ItemExtractionErrors::SetterId)),
        );
        query.and_where(
            Expr::col((Alias::new(SETTER_ALIAS), Setters::Name)).eq(self.failed_for.clone()),
        );
        // Only *active* rows suppress: an ambiguous verdict (skip_after 2) on
        // its first attempt is recorded but still selectable, which is what
        // gives it the one confirmation re-attempt.
        query.and_where(
            Expr::col((ItemExtractionErrors::Table, ItemExtractionErrors::Attempts)).gte(
                Expr::col((ItemExtractionErrors::Table, ItemExtractionErrors::SkipAfter)),
            ),
        );

        apply_group_by(&mut query, get_std_group_by(context, state));

        let cte = wrap_query(state, query, context, cte_name, &JoinedTables::default());
        state.cte_counter += 1;
        Ok(cte)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pql::model::{EntityType, QueryElement};
    use serde_json::json;

    use super::super::test_support::{
        build_base_state, build_begin_cte, render_filter_sql, run_full_pql_query,
    };

    #[test]
    fn failed_for_builds_the_active_row_predicate() {
        let filter: FailedFor = serde_json::from_value(json!({
            "failed_for": "test/clip"
        }))
        .expect("failed_for filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("item_extraction_errors"));
        assert!(sql.contains(SETTER_ALIAS));
        assert!(
            sql.contains(r#""attempts" >= "item_extraction_errors"."skip_after""#),
            "the confirmation threshold must be part of the join: {sql}"
        );
    }

    #[tokio::test]
    async fn failed_for_runs_full_query() {
        let filter: FailedFor = serde_json::from_value(json!({
            "failed_for": "test/clip"
        }))
        .expect("failed_for filter");
        run_full_pql_query(QueryElement::FailedFor(filter), EntityType::File)
            .await
            .expect("failed_for query");
    }

    // The text entity is the awkward case (the work query is data_id-driven
    // while the ledger is item-keyed), so it has to compile and run too.
    #[tokio::test]
    async fn failed_for_runs_on_the_text_entity() {
        let filter: FailedFor = serde_json::from_value(json!({
            "failed_for": "test/clip"
        }))
        .expect("failed_for filter");
        run_full_pql_query(QueryElement::FailedFor(filter), EntityType::Text)
            .await
            .expect("failed_for text query");
    }

    // The decided granularity, locked against a future data_id-keyed rewrite
    // that lands by accident: the ledger row belongs to the *item*, so on a
    // text query it matches every one of that item's data rows and none of
    // another item's. Step 4 must keep worker-reported text verdicts
    // transient precisely because this is the shape (see the comment on the
    // join above).
    #[tokio::test]
    async fn failed_for_excludes_only_the_failed_items_text_rows() {
        use sea_query::SqliteQueryBuilder;
        use sea_query_sqlx::SqlxBinder;

        use crate::db::migrations::setup_test_databases;
        use crate::pql::build_query;
        use crate::pql::model::{NotOperator, PqlQuery};

        let mut dbs = setup_test_databases().await;
        let conn = &mut dbs.index_conn;
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) \
             VALUES (1, '2026-01-01T00:00:00', 'C:/data')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO items (id, sha256, md5, type, time_added) VALUES \
             (1, 'sha_a', 'md5_a', 'application/pdf', '2026-01-01T00:00:00'), \
             (2, 'sha_b', 'md5_b', 'application/pdf', '2026-01-01T00:00:00')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO files (id, sha256, item_id, path, filename, last_modified, \
             scan_id, available) VALUES \
             (1, 'sha_a', 1, 'C:/data/a.pdf', 'a.pdf', '2026-01-01T00:00:00', 1, 1), \
             (2, 'sha_b', 2, 'C:/data/b.pdf', 'b.pdf', '2026-01-01T00:00:00', 1, 1)",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO setters (id, name) VALUES (1, 'test/ocr'), (2, 'test/clip')")
            .execute(&mut *conn)
            .await
            .unwrap();
        // Two text segments for item A, one for item B.
        sqlx::query(
            "INSERT INTO item_data (id, item_id, setter_id, data_type, idx, is_origin) VALUES \
             (1, 1, 1, 'text', 0, TRUE), \
             (2, 1, 1, 'text', 1, TRUE), \
             (3, 2, 1, 'text', 0, TRUE)",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO extracted_text (id, text, text_length) VALUES \
             (1, 'first segment', 13), (2, 'second segment', 14), (3, 'other item', 10)",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        // One active verdict, on item A only.
        sqlx::query(
            "INSERT INTO item_extraction_errors (item_id, setter_id, stage, error_class, \
             mime_type, error, skip_after, attempts, first_seen, last_seen) \
             VALUES (1, 2, 'prepare', 'input', 'application/pdf', 'boom', 1, 1, 'now', 'now')",
        )
        .execute(&mut *conn)
        .await
        .unwrap();

        let filter = || {
            serde_json::from_value::<FailedFor>(json!({
                "failed_for": "test/clip"
            }))
            .expect("failed_for filter")
        };
        let run = |element: QueryElement| {
            let built = build_query(
                PqlQuery {
                    query: Some(element),
                    entity: EntityType::Text,
                    page_size: 0,
                    check_path: false,
                    ..Default::default()
                },
                false,
            )
            .expect("build_query");
            let paginated = built.paginated_query();
            match built.with_clause {
                Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
                None => paginated.build_sqlx(SqliteQueryBuilder),
            }
        };
        let data_ids = |rows: Vec<sqlx::sqlite::SqliteRow>| {
            let mut ids: Vec<i64> = rows
                .iter()
                .map(|row| sqlx::Row::try_get::<i64, _>(row, "data_id").unwrap())
                .collect();
            ids.sort_unstable();
            ids
        };

        let (sql, values) = run(QueryElement::FailedFor(filter()));
        let selected = data_ids(
            sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&mut *conn)
                .await
                .expect("the failed_for text query must be valid SQLite"),
        );
        assert_eq!(
            selected,
            vec![1, 2],
            "an item-keyed verdict matches every one of that item's data rows, \
             and no other item's"
        );

        // The complement, which is the shape the work query actually uses: the
        // filter is negated there, so "excludes only the failed item's rows"
        // has to be asserted on the negated query, not inferred from the
        // positive one.
        let (sql, values) = run(QueryElement::Not(NotOperator {
            not_: Box::new(QueryElement::FailedFor(filter())),
        }));
        let excluded = data_ids(
            sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
                .fetch_all(&mut *conn)
                .await
                .expect("the negated failed_for text query must be valid SQLite"),
        );
        assert_eq!(
            excluded,
            vec![3],
            "negated, the same verdict removes both of item A's rows and keeps \
             item B's"
        );
    }
}
