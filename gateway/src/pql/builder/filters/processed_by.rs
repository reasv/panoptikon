use sea_query::{Expr, ExprTrait, JoinType};

use crate::pql::model::ProcessedBy;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    BaseTable, CteRef, ItemData, JoinedTables, QueryState, Setters, apply_group_by,
    get_std_group_by, select_std_from_cte, wrap_query,
};

impl FilterCompiler for ProcessedBy {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let cte_name = format!("n{}_ProcessedBy", state.cte_counter);
        let mut query = select_std_from_cte(context, state);
        let join_cond = if state.item_data_query {
            Expr::col((ItemData::Table, ItemData::SourceId)).equals(context.column_ref("data_id"))
        } else {
            Expr::col((ItemData::Table, ItemData::ItemId)).equals(context.column_ref("item_id"))
        };
        query.join(JoinType::InnerJoin, ItemData::Table, join_cond);
        query.join(
            JoinType::InnerJoin,
            Setters::Table,
            Expr::col((Setters::Table, Setters::Id)).equals((ItemData::Table, ItemData::SetterId)),
        );
        query.and_where(Expr::col((Setters::Table, Setters::Name)).eq(self.processed_by.clone()));

        apply_group_by(&mut query, get_std_group_by(context, state));

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::ItemData);
        joined_tables.mark(BaseTable::Setters);
        let cte = wrap_query(state, query, context, cte_name, &joined_tables);
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
    fn processed_by_builds_sql() {
        let filter: ProcessedBy = serde_json::from_value(json!({
            "processed_by": "file_scan"
        }))
        .expect("processed_by filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("setters"));
        assert!(sql.contains("SELECT"));
    }

    #[tokio::test]
    async fn processed_by_runs_full_query() {
        let filter: ProcessedBy = serde_json::from_value(json!({
            "processed_by": "file_scan"
        }))
        .expect("processed_by filter");
        run_full_pql_query(QueryElement::ProcessedBy(filter), EntityType::File)
            .await
            .expect("processed_by query");
    }
}
