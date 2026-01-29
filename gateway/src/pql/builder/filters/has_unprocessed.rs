use sea_query::{Alias, Expr, ExprTrait, JoinType, Query};

use crate::pql::model::HasUnprocessedData;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ItemData, QueryState, Setters, apply_group_by, get_std_group_by, select_std_from_cte,
    wrap_query,
};

impl FilterCompiler for HasUnprocessedData {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.has_data_unprocessed;
        let cte_name = format!("n{}_HasUnprocessedData", state.cte_counter);
        let src_alias = Alias::new("src_item_data");
        let derived_alias = Alias::new("derived_data");

        let mut not_exists_subquery = Query::select();
        not_exists_subquery.expr(Expr::val(1));
        not_exists_subquery.from_as(ItemData::Table, derived_alias.clone());
        not_exists_subquery.join(
            JoinType::InnerJoin,
            Setters::Table,
            Expr::col((Setters::Table, Setters::Id))
                .equals((derived_alias.clone(), ItemData::SetterId)),
        );
        not_exists_subquery.and_where(
            Expr::col((derived_alias.clone(), ItemData::SourceId))
                .equals((src_alias.clone(), ItemData::Id)),
        );
        not_exists_subquery
            .and_where(Expr::col((Setters::Table, Setters::Name)).eq(args.setter_name.clone()));

        let mut query = select_std_from_cte(context, state);
        query.join_as(
            JoinType::InnerJoin,
            ItemData::Table,
            src_alias.clone(),
            Expr::col((src_alias.clone(), ItemData::ItemId)).equals(context.column_ref("item_id")),
        );

        let data_types = args
            .data_types
            .iter()
            .cloned()
            .map(Expr::val)
            .collect::<Vec<_>>();
        query.and_where(Expr::col((src_alias.clone(), ItemData::DataType)).is_in(data_types));
        query.and_where(Expr::col((src_alias.clone(), ItemData::IsPlaceholder)).eq(0));
        query.and_where(Expr::not_exists(not_exists_subquery.to_owned()));
        apply_group_by(&mut query, get_std_group_by(context, state));

        let cte = wrap_query(state, query, context, cte_name);
        state.cte_counter += 1;
        Ok(cte)
    }
}
