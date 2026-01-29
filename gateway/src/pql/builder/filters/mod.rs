mod has_unprocessed;
mod in_bookmarks;
mod match_filter;
mod match_path;
mod match_tags;
mod match_text;
mod processed_by;

use super::{CteRef, QueryState};
use crate::pql::preprocess::PqlError;

pub(crate) trait FilterCompiler {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;

    use sea_query::{Alias, Cond, Expr, ExprTrait, Query, SqliteQueryBuilder};
    use sea_query_sqlx::SqlxBinder;

    use crate::db::migrations::setup_test_databases;
    use crate::pql::build_query;
    use crate::pql::model::{EntityType, PqlQuery, QueryElement};

    use super::{CteRef, FilterCompiler, QueryState};
    use super::super::{
        ItemData, Files, create_cte, entity_to_data_type, build_with_clause, select_std_from_cte,
    };

    pub(crate) fn build_base_state(entity: EntityType, count_query: bool) -> QueryState {
        QueryState {
            order_list: Vec::new(),
            extra_columns: Vec::new(),
            selects: HashMap::new(),
            ctes: Vec::new(),
            cte_counter: 0,
            is_count_query: count_query,
            item_data_query: matches!(entity, EntityType::Text),
            entity,
        }
    }

    pub(crate) fn build_begin_cte(state: &mut QueryState) -> CteRef {
        let mut start = Query::select();
        start
            .expr_as(Expr::col((Files::Table, Files::Id)), Alias::new("file_id"))
            .expr_as(Expr::col((Files::Table, Files::ItemId)), Alias::new("item_id"))
            .from(Files::Table);

        if state.item_data_query {
            let join_cond = Cond::all()
                .add(
                    Expr::col((ItemData::Table, ItemData::ItemId))
                        .equals((Files::Table, Files::ItemId)),
                )
                .add(
                    Expr::col((ItemData::Table, ItemData::DataType))
                        .eq(entity_to_data_type(state.entity)),
                );
            start.join(sea_query::JoinType::InnerJoin, ItemData::Table, join_cond);
            start.expr_as(Expr::col((ItemData::Table, ItemData::Id)), Alias::new("data_id"));
        }

        create_cte(state, "begin_cte".to_string(), start.to_owned())
    }

    pub(crate) fn render_filter_sql<F: FilterCompiler>(
        filter: &F,
        state: &mut QueryState,
        context: &CteRef,
    ) -> String {
        let cte = filter.build(context, state).expect("filter build");
        let select = select_std_from_cte(&cte, state);
        let with_clause = build_with_clause(state, None, None).expect("with clause");
        select.with(with_clause).to_string(SqliteQueryBuilder)
    }

    pub(crate) async fn run_full_pql_query(
        filter: QueryElement,
        entity: EntityType,
    ) -> Result<(), sqlx::Error> {
        let mut query = PqlQuery {
            query: Some(filter),
            entity,
            ..Default::default()
        };

        let built = build_query(query, false).expect("build_query");
        let mut dbs = setup_test_databases().await;

        let (sql, values) = match built.with_clause {
            Some(with_clause) => built.query.with(with_clause).build_sqlx(SqliteQueryBuilder),
            None => built.query.build_sqlx(SqliteQueryBuilder),
        };

        let _rows = sqlx::query_with(&sql, values)
            .fetch_all(&mut dbs.index_conn)
            .await?;
        Ok(())
    }
}
