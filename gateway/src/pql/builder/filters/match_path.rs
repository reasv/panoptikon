use sea_query::{Expr, ExprTrait, JoinType};
use sea_query::extension::sqlite::SqliteBinOper;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::SortableOptions;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ExtraColumn, FilesPathFts, JoinedTables, OrderByFilter, QueryState,
    add_sortable_rank_column, apply_sort_bounds, select_std_from_cte, wrap_query,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchPathArgs {
    /// Match
    ///
    /// The query to match against file paths
    pub r#match: String,
    /// Match on filenames Only
    #[serde(default)]
    pub filename_only: bool,
    /// Allow raw FTS5 MATCH Syntax
    ///
    /// If set to False, the query will be escaped before being passed to the FTS5 MATCH function
    #[serde(default = "default_true")]
    pub raw_fts5_match: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchPath {
    #[serde(flatten, default)]
    pub sort: SortableOptions,
    /// Match Path
    ///
    /// Match a query against file paths
    pub match_path: MatchPathArgs,
}

fn default_true() -> bool {
    true
}

impl FilterCompiler for MatchPath {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let mut query = select_std_from_cte(context, state);
        let join_cond = Expr::cust("files_path_fts.rowid").equals(context.column_ref("file_id"));
        query.join(JoinType::InnerJoin, FilesPathFts::Table, join_cond);

        let match_column = if self.match_path.filename_only {
            Expr::col((FilesPathFts::Table, FilesPathFts::Filename))
        } else {
            Expr::col((FilesPathFts::Table, FilesPathFts::Path))
        };
        query.and_where(
            match_column.binary(
                SqliteBinOper::Match,
                Expr::val(self.match_path.r#match.clone()),
            ),
        );

        if !state.is_count_query {
            add_sortable_rank_column(&mut query, &self.sort)?;
        }

        let cte_name = format!("n{}_MatchPath", state.cte_counter);
        let (final_query, context_for_wrap, joined_tables) = apply_sort_bounds(
            state,
            query,
            context.clone(),
            &cte_name,
            &self.sort,
            JoinedTables::default(),
        );

        let cte = wrap_query(state, final_query, &context_for_wrap, cte_name, &joined_tables);
        state.cte_counter += 1;
        if !state.is_count_query {
            if let Some(alias) = &self.sort.select_as {
                state.extra_columns.push(ExtraColumn {
                    column: "order_rank".to_string(),
                    cte: cte.clone(),
                    alias: alias.clone(),
                });
            }
            if self.sort.order_by {
                state.order_list.push(OrderByFilter {
                    cte: cte.clone(),
                    direction: self.sort.direction,
                    priority: self.sort.priority,
                    rrf: self.sort.rrf.clone(),
                });
            }
        }
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
    fn match_path_builds_sql() {
        let filter: MatchPath = serde_json::from_value(json!({
            "match_path": { "match": "docs", "filename_only": true }
        }))
        .expect("match_path filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("files_path_fts"));
        assert!(sql.contains("SELECT"));
    }

    #[tokio::test]
    async fn match_path_runs_full_query() {
        let filter: MatchPath = serde_json::from_value(json!({
            "match_path": { "match": "docs", "filename_only": true }
        }))
        .expect("match_path filter");
        run_full_pql_query(QueryElement::MatchPath(filter), EntityType::File)
            .await
            .expect("match_path query");
    }
}
