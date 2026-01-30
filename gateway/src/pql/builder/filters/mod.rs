mod embedding_types;
mod has_unprocessed;
mod image_embeddings;
mod in_bookmarks;
mod item_similarity;
mod match_filter;
mod match_path;
mod match_tags;
mod match_text;
mod processed_by;
mod text_embeddings;

use super::{CteRef, QueryState};
use crate::pql::preprocess::PqlError;

pub(crate) use embedding_types::{DistanceAggregation, DistanceFunction};
pub(crate) use has_unprocessed::{DerivedDataArgs, HasUnprocessedData};
pub(crate) use image_embeddings::{SemanticImageArgs, SemanticImageSearch};
pub(crate) use in_bookmarks::{InBookmarks, InBookmarksArgs};
pub(crate) use item_similarity::{SimilarTo, SimilarityArgs, SourceArgs};
pub(crate) use match_filter::{
    Match, MatchAnd, MatchNot, MatchOps, MatchOr, MatchValue, MatchValues, Matches, OneOrMany,
    evaluate_match,
};
pub(crate) use match_path::{MatchPath, MatchPathArgs};
pub(crate) use match_tags::{MatchTags, TagsArgs};
pub(crate) use match_text::{MatchText, MatchTextArgs};
pub(crate) use processed_by::ProcessedBy;
pub(crate) use text_embeddings::{EmbedArgs, SemanticTextArgs, SemanticTextSearch};

pub(crate) trait FilterCompiler {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::OnceLock;

    use libsqlite3_sys::{SQLITE_OK, sqlite3_auto_extension};
    use sea_query::{Alias, Cond, Expr, ExprTrait, Query, SqliteQueryBuilder};
    use sea_query_sqlx::SqlxBinder;
    use sqlite_vec::sqlite3_vec_init;

    use crate::db::migrations::setup_test_databases;
    use crate::pql::build_query;
    use crate::pql::model::{EntityType, PqlQuery, QueryElement};

    use super::super::{
        Files, ItemData, build_with_clause, create_cte, entity_to_data_type, select_std_from_cte,
    };
    use super::{CteRef, FilterCompiler, QueryState};

    fn ensure_vec_extension_loaded() {
        static EXT_LOADED: OnceLock<()> = OnceLock::new();
        if EXT_LOADED.get().is_some() {
            return;
        }
        let status = unsafe {
            sqlite3_auto_extension(Some(std::mem::transmute(sqlite3_vec_init as *const ())))
        };
        if status != SQLITE_OK {
            panic!("failed to register sqlite-vec extension for tests");
        }
        let _ = EXT_LOADED.set(());
    }

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
            .expr_as(
                Expr::col((Files::Table, Files::ItemId)),
                Alias::new("item_id"),
            )
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
            start.expr_as(
                Expr::col((ItemData::Table, ItemData::Id)),
                Alias::new("data_id"),
            );
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
        ensure_vec_extension_loaded();
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
