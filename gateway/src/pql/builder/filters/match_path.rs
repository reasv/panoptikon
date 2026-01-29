use sea_query::{Alias, Expr, ExprTrait, JoinType, Query};
use sea_query::extension::sqlite::SqliteBinOper;

use crate::pql::model::MatchPath;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ExtraColumn, FilesPathFts, OrderByFilter, QueryState, add_sortable_rank_column,
    create_cte, scalar_to_expr, select_std_from_cte, wrap_query,
};

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
        let mut context_for_wrap = context.clone();
        let mut final_query = query;

        if !state.is_count_query && (self.sort.gt.is_some() || self.sort.lt.is_some()) {
            let wrapped_name = format!("wrapped_{cte_name}");
            let wrapped_cte = create_cte(state, wrapped_name.clone(), final_query.to_owned());
            context_for_wrap = wrapped_cte.clone();

            let mut wrapped_query = Query::select();
            wrapped_query
                .from(Alias::new(wrapped_name.as_str()))
                .column((Alias::new(wrapped_name.as_str()), sea_query::Asterisk));
            if let Some(gt) = &self.sort.gt {
                wrapped_query.and_where(
                    Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                        .gt(scalar_to_expr(gt)),
                );
            }
            if let Some(lt) = &self.sort.lt {
                wrapped_query.and_where(
                    Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                        .lt(scalar_to_expr(lt)),
                );
            }
            final_query = wrapped_query;
        }

        let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
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
    use crate::pql::model::EntityType;
    use serde_json::json;

    use super::super::test_support::{build_base_state, build_begin_cte, render_filter_sql};

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
}
