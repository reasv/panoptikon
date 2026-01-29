use sea_query::{Alias, Expr, ExprTrait, JoinType};

use crate::pql::model::InBookmarks;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    Bookmarks, CteRef, ExtraColumn, Files, OrderByFilter, QueryState, add_rank_column_expr,
    apply_group_by, apply_sort_bounds, get_std_group_by, select_std_from_cte, wrap_query,
};

impl FilterCompiler for InBookmarks {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.in_bookmarks;
        let cte_name = format!("n{}_InBookmarks", state.cte_counter);
        let user_data = Alias::new("user_data");

        let mut criteria = Vec::new();
        if !args.namespaces.is_empty() {
            let namespaces = args
                .namespaces
                .iter()
                .cloned()
                .map(Expr::val)
                .collect::<Vec<_>>();
            let in_condition =
                Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Namespace))
                    .is_in(namespaces);
            if args.sub_ns {
                let mut namespace_exprs = Vec::new();
                namespace_exprs.push(in_condition);
                for namespace in &args.namespaces {
                    namespace_exprs.push(
                        Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Namespace))
                            .like(format!("{namespace}.%")),
                    );
                }
                let mut namespace_cond = namespace_exprs
                    .drain(..1)
                    .next()
                    .ok_or_else(|| PqlError::invalid("No namespaces provided"))?;
                for expr in namespace_exprs {
                    namespace_cond = namespace_cond.or(expr);
                }
                criteria.push(namespace_cond);
            } else {
                criteria.push(in_condition);
            }
        }

        let mut users = Vec::new();
        users.push(Expr::val(args.user.clone()));
        if args.include_wildcard {
            users.push(Expr::val("*"));
        }
        criteria.push(Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::User)).is_in(users));

        let mut query = select_std_from_cte(context, state);
        query.join(
            JoinType::InnerJoin,
            Files::Table,
            Expr::col((Files::Table, Files::Id)).equals(context.column_ref("file_id")),
        );
        query.join(
            JoinType::InnerJoin,
            (user_data.clone(), Bookmarks::Table),
            Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Sha256))
                .equals((Files::Table, Files::Sha256)),
        );
        for criterion in criteria {
            query.and_where(criterion);
        }

        if !state.is_count_query {
            let rank_expr = if args.filter {
                Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::TimeAdded))
            } else {
                Expr::val(1)
            };
            add_rank_column_expr(&mut query, &self.sort, rank_expr)?;
        }

        apply_group_by(&mut query, get_std_group_by(context, state));

        let (query, context_for_wrap) =
            apply_sort_bounds(state, query, context.clone(), &cte_name, &self.sort);

        let cte = wrap_query(state, query, &context_for_wrap, cte_name);
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
    fn in_bookmarks_builds_sql() {
        let filter: InBookmarks = serde_json::from_value(json!({
            "in_bookmarks": { "namespaces": ["demo"], "user": "alice", "sub_ns": true }
        }))
        .expect("in_bookmarks filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("bookmarks"));
        assert!(sql.contains("SELECT"));
    }
}
