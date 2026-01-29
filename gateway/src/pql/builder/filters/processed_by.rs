use sea_query::{Expr, ExprTrait, JoinType};

use crate::pql::model::ProcessedBy;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ItemData, QueryState, Setters, apply_group_by, get_std_group_by, select_std_from_cte,
    wrap_query,
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

        let cte = wrap_query(state, query, context, cte_name);
        state.cte_counter += 1;
        Ok(cte)
    }
}
