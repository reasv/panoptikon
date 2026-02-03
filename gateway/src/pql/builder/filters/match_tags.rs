use sea_query::{Alias, BinOper, Cond, Expr, ExprTrait, Func, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{OrderDirection, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ExtraColumn, ItemData, JoinedTables, OrderByFilter, QueryState, Setters, Tags,
    TagsItems, add_rank_column_expr, apply_group_by, apply_sort_bounds, create_cte,
    get_std_group_by, select_std_from_cte, wrap_query,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct TagsArgs {
    /// List of tags to match
    #[serde(default)]
    pub tags: Vec<String>,
    /// Match any tag
    ///
    /// If true, match items with at least one of the given tags.
    /// If false (default), only match items with all of the given tags.
    #[serde(default)]
    pub match_any: bool,
    /// Minimum confidence
    ///
    /// Only consider tags with a confidence greater than or equal to this value
    #[serde(default)]
    pub min_confidence: f64,
    /// Only consider tags set by these setters
    #[serde(default)]
    pub setters: Vec<String>,
    /// Only consider tags in these namespaces (includes sub-namespaces)
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Require all setters to match
    ///
    /// Only consider tags that have been set by all of the given setters.
    /// If match_any is true, and there is more than one tag, this will be ignored.
    ///
    /// If you really want to match any tag set by all of the given setters,
    /// you can combine this with a separate filter for each tag in an OrOperator.
    #[serde(default)]
    pub all_setters_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchTags {
    #[serde(flatten, default = "default_sort_desc")]
    pub sort: SortableOptions,
    pub match_tags: TagsArgs,
}

// Used by serde default attribute.
#[allow(dead_code)]
fn default_sort_desc() -> SortableOptions {
    let mut options = SortableOptions::default();
    options.direction = OrderDirection::Desc;
    options.row_n_direction = OrderDirection::Desc;
    options
}

impl FilterCompiler for MatchTags {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.match_tags;
        let cte_name = format!("n{}_MatchTags", state.cte_counter);
        let mut conditions = Vec::new();
        let tag_values = args
            .tags
            .iter()
            .cloned()
            .map(Expr::val)
            .collect::<Vec<_>>();
        conditions.push(Expr::col((Tags::Table, Tags::Name)).is_in(tag_values));
        if args.min_confidence > 0.0 {
            conditions.push(
                Expr::col((TagsItems::Table, TagsItems::Confidence)).gte(args.min_confidence),
            );
        }
        if !args.setters.is_empty() {
            let setters = args.setters.iter().cloned().map(Expr::val).collect::<Vec<_>>();
            conditions.push(Expr::col((Setters::Table, Setters::Name)).is_in(setters));
        }

        if !args.namespaces.is_empty() {
            let mut namespace_exprs = Vec::new();
            for namespace in &args.namespaces {
                namespace_exprs.push(
                    Expr::col((Tags::Table, Tags::Namespace)).like(format!("{namespace}%")),
                );
            }
            let mut namespace_cond = namespace_exprs
                .drain(..1)
                .next()
                .ok_or_else(|| PqlError::invalid("No namespaces provided"))?;
            for expr in namespace_exprs {
                namespace_cond = namespace_cond.or(expr);
            }
            conditions.push(namespace_cond);
        }

        let mut matching_items_select = select_std_from_cte(context, state);
        let join_cond = Cond::all()
            .add(
                Expr::col((ItemData::Table, ItemData::ItemId))
                    .equals(context.column_ref("item_id")),
            )
            .add(Expr::col((ItemData::Table, ItemData::DataType)).eq("tags"));
        matching_items_select.join(JoinType::InnerJoin, ItemData::Table, join_cond);
        matching_items_select.join(
            JoinType::InnerJoin,
            Setters::Table,
            Expr::col((Setters::Table, Setters::Id))
                .equals((ItemData::Table, ItemData::SetterId)),
        );
        matching_items_select.join(
            JoinType::InnerJoin,
            TagsItems::Table,
            Expr::col((TagsItems::Table, TagsItems::ItemDataId))
                .equals((ItemData::Table, ItemData::Id)),
        );
        matching_items_select.join(
            JoinType::InnerJoin,
            Tags::Table,
            Expr::col((Tags::Table, Tags::Id))
                .equals((TagsItems::Table, TagsItems::TagId)),
        );
        for condition in conditions {
            matching_items_select.and_where(condition);
        }
        apply_group_by(&mut matching_items_select, get_std_group_by(context, state));

        let mut having_clauses = Vec::new();
        if args.all_setters_required {
            let setter_tag = Expr::col((ItemData::Table, ItemData::SetterId))
                .binary(BinOper::Custom("||"), Expr::val("-"))
                .binary(BinOper::Custom("||"), Expr::col((Tags::Table, Tags::Name)));
            let expected = (args.tags.len() * args.setters.len()) as i64;
            having_clauses.push(Func::count_distinct(setter_tag).eq(expected));
        } else {
            let expected = args.tags.len() as i64;
            having_clauses.push(
                Func::count_distinct(Expr::col((Tags::Table, Tags::Name))).eq(expected),
            );
        }
        if args.match_any && args.tags.len() > 1 {
            having_clauses.clear();
        }
        for clause in having_clauses {
            matching_items_select.and_having(clause);
        }

        if !state.is_count_query {
            let avg_confidence =
                Func::avg(Expr::col((TagsItems::Table, TagsItems::Confidence))).into();
            add_rank_column_expr(&mut matching_items_select, &self.sort, avg_confidence)?;
        }

        let matching_items =
            create_cte(state, format!("match_{cte_name}"), matching_items_select.to_owned());

        let mut query = select_std_from_cte(context, state);
        if !state.is_count_query {
            query.expr_as(matching_items.column_expr("order_rank"), Alias::new("order_rank"));
        }
        let join_condition = if state.item_data_query {
            Expr::col(matching_items.column_ref("data_id")).equals(context.column_ref("data_id"))
        } else {
            Expr::col(matching_items.column_ref("file_id")).equals(context.column_ref("file_id"))
        };
        query.join(
            JoinType::InnerJoin,
            Alias::new(matching_items.name.as_str()),
            join_condition,
        );

        let (query, context_for_wrap) =
            apply_sort_bounds(state, query, context.clone(), &cte_name, &self.sort);

        let joined_tables = JoinedTables::default();
        let cte = wrap_query(state, query, &context_for_wrap, cte_name, &joined_tables);
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
    fn match_tags_builds_sql() {
        let filter: MatchTags = serde_json::from_value(json!({
            "match_tags": { "tags": ["cat"], "match_any": true }
        }))
        .expect("match_tags filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("tags"));
        assert!(sql.contains("SELECT"));
    }

    #[tokio::test]
    async fn match_tags_runs_full_query() {
        let filter: MatchTags = serde_json::from_value(json!({
            "match_tags": { "tags": ["cat"], "match_any": true }
        }))
        .expect("match_tags filter");
        run_full_pql_query(QueryElement::MatchTags(filter), EntityType::File)
            .await
            .expect("match_tags query");
    }
}
