use sea_query::{Alias, Expr, ExprTrait, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, Files, JoinedTables, PinboardVersionItems, Pinboards, QueryState,
    apply_group_by, get_std_group_by, select_std_from_cte, wrap_query,
};
use super::FilterCompiler;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct InPinboardArgs {
    /// Enable the filter
    ///
    /// Must be set to True, this option only exists to make sure the filter is not empty,
    /// given that that all fields are optional.
    #[serde(default = "default_true")]
    pub filter: bool,
    /// Pinboard IDs
    ///
    /// List of pinboard IDs to filter by. An item matches if it is pinned in the
    /// head (current) version of at least one of them.
    /// If empty, membership in *any* of the user's pinboards matches.
    #[serde(default)]
    pub pinboard_ids: Vec<i64>,
    /// The user whose pinboards are searched.
    #[serde(default = "default_pinboards_user")]
    pub user: String,
}

/// Restrict search to items pinned on a pinboard.
///
/// Not sortable: pinboard membership has no natural per-item rank, so this
/// follows the `ProcessedBy`/`FailedFor` shape (no `SortableOptions`, no
/// `order_rank` column, never an order source) rather than the `InBookmarks`
/// one.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct InPinboard {
    /// Restrict search to items pinned on a pinboard
    ///
    /// Only include items that are members of the head version of a pinboard.
    pub in_pinboard: InPinboardArgs,
}

fn default_true() -> bool {
    true
}

fn default_pinboards_user() -> String {
    "user".to_string()
}

impl FilterCompiler for InPinboard {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.in_pinboard;
        // Structural user_data dependency marker: the result cache keys and
        // invalidates on the user-data epoch only for queries that set this,
        // so without it a board edit would not invalidate cached results.
        state.uses_user_data = true;
        let cte_name = format!("n{}_InPinboard", state.cte_counter);
        let user_data = Alias::new("user_data");

        let mut query = select_std_from_cte(context, state);
        query.join(
            JoinType::InnerJoin,
            Files::Table,
            Expr::col((Files::Table, Files::Id)).equals(context.column_ref("file_id")),
        );
        query.join(
            JoinType::InnerJoin,
            (user_data.clone(), PinboardVersionItems::Table),
            Expr::col((
                user_data.clone(),
                PinboardVersionItems::Table,
                PinboardVersionItems::Sha256,
            ))
            .equals((Files::Table, Files::Sha256)),
        );
        // Joining pins to boards through head_version_id is what makes only
        // the head version searchable; older versions self-heal.
        query.join(
            JoinType::InnerJoin,
            (user_data.clone(), Pinboards::Table),
            Expr::col((
                user_data.clone(),
                Pinboards::Table,
                Pinboards::HeadVersionId,
            ))
            .equals((
                user_data.clone(),
                PinboardVersionItems::Table,
                PinboardVersionItems::VersionId,
            )),
        );
        query.and_where(
            Expr::col((user_data.clone(), Pinboards::Table, Pinboards::User)).eq(args.user.clone()),
        );
        if !args.pinboard_ids.is_empty() {
            query.and_where(
                Expr::col((user_data.clone(), Pinboards::Table, Pinboards::Id))
                    .is_in(args.pinboard_ids.iter().copied().map(Expr::val)),
            );
        }

        // An item on several of the user's boards produces one join row per
        // board; the std GROUP BY collapses them back to one row.
        apply_group_by(&mut query, get_std_group_by(context, state));

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::Files);
        let cte = wrap_query(state, query, context, cte_name, &joined_tables);
        state.cte_counter += 1;
        Ok(cte)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pql::model::{EntityType, NotOperator, PqlQuery, QueryElement};
    use sea_query::SqliteQueryBuilder;
    use sea_query_sqlx::SqlxBinder;
    use serde_json::json;

    use crate::db::migrations::{InMemoryDatabases, setup_test_databases};
    use crate::pql::build_query;

    use super::super::test_support::{
        build_base_state, build_begin_cte, render_filter_sql, run_full_pql_query,
    };

    fn filter_from(value: serde_json::Value) -> InPinboard {
        serde_json::from_value(value).expect("in_pinboard filter")
    }

    fn rendered(value: serde_json::Value) -> String {
        let filter = filter_from(value);
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        render_filter_sql(&filter, &mut state, &context)
    }

    #[test]
    fn in_pinboard_joins_only_the_head_version() {
        let sql = rendered(json!({ "in_pinboard": { "filter": true } }));
        assert!(sql.contains("pinboard_version_items"), "{sql}");
        assert!(
            sql.contains(r#""pinboards"."head_version_id" = "user_data"."pinboard_version_items"."version_id""#),
            "membership must be joined through the head version: {sql}"
        );
        assert!(sql.contains(r#""pinboards"."user" = 'user'"#), "{sql}");
        // Multi-board membership must not duplicate rows.
        assert!(sql.contains("GROUP BY"), "{sql}");
        assert!(sql.contains(r#""file_id""#), "{sql}");
    }

    #[test]
    fn in_pinboard_binds_the_user_and_omits_the_id_list_when_empty() {
        let sql = rendered(json!({ "in_pinboard": { "user": "alice" } }));
        assert!(sql.contains(r#""pinboards"."user" = 'alice'"#), "{sql}");
        assert!(
            !sql.contains(r#""pinboards"."id" IN"#),
            "an empty pinboard_ids list means *any* board, so no IN list: {sql}"
        );
    }

    #[test]
    fn in_pinboard_renders_the_id_list_when_given() {
        let sql = rendered(json!({ "in_pinboard": { "pinboard_ids": [3, 7] } }));
        assert!(sql.contains(r#""pinboards"."id" IN (3, 7)"#), "{sql}");
    }

    /// The filter is not an order source: it declares no `SortableOptions`,
    /// emits no rank column, and adds nothing to the order list.
    #[test]
    fn in_pinboard_is_not_sortable() {
        let filter = filter_from(json!({ "in_pinboard": { "filter": true } }));
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(!sql.contains("order_rank"), "{sql}");
        assert!(state.order_list.is_empty());
        assert!(state.extra_columns.is_empty());
    }

    #[test]
    fn in_pinboard_marks_user_data_dependency() {
        let filter = filter_from(json!({ "in_pinboard": { "filter": true } }));
        let mut state = build_base_state(EntityType::File, false);
        assert!(!state.uses_user_data);
        let context = build_begin_cte(&mut state);
        filter.build(&context, &mut state).expect("filter build");
        assert!(state.uses_user_data);

        // And the flag propagates through full builds, for both query kinds.
        let query = PqlQuery {
            query: Some(QueryElement::InPinboard(filter)),
            ..PqlQuery::default()
        };
        let results = build_query(query.clone(), false).expect("results build");
        assert!(results.uses_user_data);
        let count = build_query(query, true).expect("count build");
        assert!(count.uses_user_data);
    }

    #[tokio::test]
    async fn in_pinboard_runs_full_query() {
        let filter = filter_from(json!({ "in_pinboard": { "filter": true } }));
        run_full_pql_query(QueryElement::InPinboard(filter), EntityType::File)
            .await
            .expect("in_pinboard query");
    }

    /// Two items, two boards, one board with an *old* version that pins an
    /// item its head no longer does, plus another user's board.
    ///
    /// - board "one" head: sha_a
    /// - board "two": v1 pinned sha_c (superseded), head pins sha_b
    /// - board "theirs" (user `other`) head: sha_d
    /// - sha_e is pinned nowhere.
    async fn setup_boards() -> (InMemoryDatabases, i64, i64) {
        let mut dbs = setup_test_databases().await;
        sqlx::query(
            r#"
            INSERT INTO items (id, sha256, md5, type, time_added)
            VALUES
                (1, 'sha_a', 'md5_a', 'image/png', '2024-01-01T00:00:00'),
                (2, 'sha_b', 'md5_b', 'image/png', '2024-01-01T00:00:00'),
                (3, 'sha_c', 'md5_c', 'image/png', '2024-01-01T00:00:00'),
                (4, 'sha_d', 'md5_d', 'image/png', '2024-01-01T00:00:00'),
                (5, 'sha_e', 'md5_e', 'image/png', '2024-01-01T00:00:00')
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO file_scans (id, start_time, path) \
             VALUES (1, '2024-01-01T00:00:00', 'C:/boards')",
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query(
            r#"
            INSERT INTO files (
                id, sha256, item_id, path, filename, last_modified, scan_id, available
            )
            VALUES
                (10, 'sha_a', 1, 'C:/boards/a.png', 'a.png', '2024-01-05T00:00:00', 1, 1),
                (11, 'sha_b', 2, 'C:/boards/b.png', 'b.png', '2024-01-04T00:00:00', 1, 1),
                (12, 'sha_c', 3, 'C:/boards/c.png', 'c.png', '2024-01-03T00:00:00', 1, 1),
                (13, 'sha_d', 4, 'C:/boards/d.png', 'd.png', '2024-01-02T00:00:00', 1, 1),
                (14, 'sha_e', 5, 'C:/boards/e.png', 'e.png', '2024-01-01T00:00:00', 1, 1)
            "#,
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let one = make_board(&mut dbs.index_conn, "user", "one", &[&["sha_a"]]).await;
        // The second version supersedes the first: sha_c must stop matching.
        let two = make_board(
            &mut dbs.index_conn,
            "user",
            "two",
            &[&["sha_c"], &["sha_b", "sha_a"]],
        )
        .await;
        make_board(&mut dbs.index_conn, "other", "theirs", &[&["sha_d"]]).await;
        (dbs, one, two)
    }

    async fn make_board(
        conn: &mut sqlx::SqliteConnection,
        user: &str,
        name: &str,
        versions: &[&[&str]],
    ) -> i64 {
        let id = crate::db::pinboards::create_pinboard(conn, user, Some(name), None, 1_000_000)
            .await
            .expect("create pinboard");
        for items in versions {
            let items: Vec<String> = items.iter().map(|item| (*item).to_string()).collect();
            crate::db::pinboards::append_version(
                conn,
                id,
                &["v2".to_string()],
                &items,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("append version");
        }
        id
    }

    /// Runs a query element and returns the matching file paths' basenames,
    /// sorted — the fixture gives every item exactly one file.
    async fn matched(conn: &mut sqlx::SqliteConnection, element: QueryElement) -> Vec<String> {
        let built = build_query(
            PqlQuery {
                query: Some(element),
                page_size: 0,
                count: false,
                check_path: false,
                ..PqlQuery::default()
            },
            false,
        )
        .expect("build_query");
        let paginated = built.paginated_query();
        let (sql, values) = match built.with_clause {
            Some(with_clause) => paginated.with(with_clause).build_sqlx(SqliteQueryBuilder),
            None => paginated.build_sqlx(SqliteQueryBuilder),
        };
        let rows = sqlx::query_with(sqlx::AssertSqlSafe(sql.as_str()), values)
            .fetch_all(conn)
            .await
            .expect("the in_pinboard query must be valid SQLite");
        let mut names: Vec<String> = rows
            .iter()
            .map(|row| {
                let path: String = sqlx::Row::try_get(row, "path").unwrap();
                path.rsplit('/').next().unwrap().to_string()
            })
            .collect();
        names.sort();
        names
    }

    fn element(value: serde_json::Value) -> QueryElement {
        QueryElement::InPinboard(filter_from(value))
    }

    #[tokio::test]
    async fn in_pinboard_filters_to_one_boards_members() {
        let (mut dbs, one, _two) = setup_boards().await;
        assert_eq!(
            matched(
                &mut dbs.index_conn,
                element(json!({ "in_pinboard": { "pinboard_ids": [one] } })),
            )
            .await,
            vec!["a.png"]
        );
    }

    #[tokio::test]
    async fn in_pinboard_empty_ids_match_any_board_of_that_user() {
        let (mut dbs, _one, _two) = setup_boards().await;
        // sha_a (board one *and* board two — not duplicated), sha_b.
        // sha_c is only in an old version, sha_d is another user's, sha_e
        // is unpinned.
        assert_eq!(
            matched(
                &mut dbs.index_conn,
                element(json!({ "in_pinboard": { "filter": true } })),
            )
            .await,
            vec!["a.png", "b.png"]
        );
    }

    #[tokio::test]
    async fn in_pinboard_does_not_duplicate_a_multi_board_item() {
        let (mut dbs, one, two) = setup_boards().await;
        // sha_a is pinned on both listed boards; it must appear once.
        assert_eq!(
            matched(
                &mut dbs.index_conn,
                element(json!({ "in_pinboard": { "pinboard_ids": [one, two] } })),
            )
            .await,
            vec!["a.png", "b.png"]
        );
    }

    #[tokio::test]
    async fn in_pinboard_ignores_superseded_versions() {
        let (mut dbs, _one, two) = setup_boards().await;
        // sha_c was pinned in board two's first version only.
        let names = matched(
            &mut dbs.index_conn,
            element(json!({ "in_pinboard": { "pinboard_ids": [two] } })),
        )
        .await;
        assert!(!names.contains(&"c.png".to_string()), "{names:?}");
        assert_eq!(names, vec!["a.png", "b.png"]);
    }

    #[tokio::test]
    async fn in_pinboard_scopes_to_the_requested_user() {
        let (mut dbs, _one, _two) = setup_boards().await;
        // sha_d is only on the other user's board.
        assert_eq!(
            matched(
                &mut dbs.index_conn,
                element(json!({ "in_pinboard": { "user": "other" } })),
            )
            .await,
            vec!["d.png"]
        );
    }

    /// `QueryElement` is `#[serde(untagged)]`, so the only thing keeping the
    /// variants apart is their distinguishing field name and the declaration
    /// order. `in_pinboard` must reach the new variant, and — because the new
    /// variant is *appended* — every payload that decoded before must still
    /// decode to the same variant.
    #[test]
    fn in_pinboard_decodes_without_capturing_the_other_variants() {
        let decode = |value: serde_json::Value| {
            serde_json::from_value::<QueryElement>(value).expect("query element")
        };

        assert!(matches!(
            decode(json!({ "in_pinboard": { "filter": true, "pinboard_ids": [1] } })),
            QueryElement::InPinboard(_)
        ));

        for (payload, expected) in [
            (json!({ "and_": [] }), "And"),
            (json!({ "or_": [] }), "Or"),
            (json!({ "not_": { "processed_by": "ocr" } }), "Not"),
            (
                json!({ "match": { "eq": { "type": "image/png" } } }),
                "Match",
            ),
            (json!({ "match_path": { "match": "a" } }), "MatchPath"),
            (json!({ "match_text": { "match": "a" } }), "MatchText"),
            (
                json!({ "text_embeddings": { "query": "a", "model": "m" } }),
                "SemanticTextSearch",
            ),
            (
                json!({ "image_embeddings": { "query": "a", "model": "m" } }),
                "SemanticImageSearch",
            ),
            (
                json!({ "similar_to": { "target": "abc", "model": "clip/test" } }),
                "SimilarTo",
            ),
            (json!({ "match_tags": { "tags": ["cat"] } }), "MatchTags"),
            (json!({ "in_bookmarks": { "filter": true } }), "InBookmarks"),
            (json!({ "processed_by": "ocr" }), "ProcessedBy"),
            (
                json!({
                    "has_data_unprocessed": { "setter_name": "ocr", "data_types": ["text"] }
                }),
                "HasUnprocessedData",
            ),
            (json!({ "failed_for": "test/clip" }), "FailedFor"),
        ] {
            let decoded = decode(payload.clone());
            let actual = match decoded {
                QueryElement::And(_) => "And",
                QueryElement::Or(_) => "Or",
                QueryElement::Not(_) => "Not",
                QueryElement::Match(_) => "Match",
                QueryElement::MatchPath(_) => "MatchPath",
                QueryElement::MatchText(_) => "MatchText",
                QueryElement::SemanticTextSearch(_) => "SemanticTextSearch",
                QueryElement::SemanticImageSearch(_) => "SemanticImageSearch",
                QueryElement::SimilarTo(_) => "SimilarTo",
                QueryElement::MatchTags(_) => "MatchTags",
                QueryElement::InBookmarks(_) => "InBookmarks",
                QueryElement::ProcessedBy(_) => "ProcessedBy",
                QueryElement::HasUnprocessedData(_) => "HasUnprocessedData",
                QueryElement::FailedFor(_) => "FailedFor",
                QueryElement::InPinboard(_) => "InPinboard",
            };
            assert_eq!(actual, expected, "{payload} decoded as {actual}");
        }
    }

    /// The headline curation workflow: everything not pinned to any board.
    #[tokio::test]
    async fn not_in_pinboard_returns_the_unpinned_items() {
        let (mut dbs, _one, _two) = setup_boards().await;
        assert_eq!(
            matched(
                &mut dbs.index_conn,
                QueryElement::Not(NotOperator {
                    not_: Box::new(element(json!({ "in_pinboard": { "filter": true } }))),
                }),
            )
            .await,
            // sha_c's pin is superseded and sha_d belongs to another user, so
            // both count as unpinned for this user.
            vec!["c.png", "d.png", "e.png"]
        );
    }
}
