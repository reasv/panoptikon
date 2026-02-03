use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{EntityType, OrderDirection, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, Embeddings, ExtraColumn, ExtractedText, ItemData, JoinedTables,
    OrderByFilter, QueryState, Setters, add_rank_column_expr, apply_group_by, apply_sort_bounds,
    get_std_group_by, select_std_from_cte, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::DistanceAggregation;
use super::item_similarity::SourceArgs;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct EmbedArgs {
    /// Cache Key
    ///
    /// The cache key to use for the inference *model*
    #[serde(default = "default_cache_key")]
    pub cache_key: String,
    /// LRU Cache Size
    ///
    /// The size of the LRU cache to use for the inference *model*
    #[serde(default = "default_lru_size")]
    pub lru_size: i64,
    /// TTL Seconds
    ///
    /// The time-to-live in seconds for the inference *model* to be kept in memory
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: i64,
}

impl Default for EmbedArgs {
    fn default() -> Self {
        Self {
            cache_key: default_cache_key(),
            lru_size: default_lru_size(),
            ttl_seconds: default_ttl_seconds(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SemanticTextArgs {
    /// Query
    ///
    /// Semantic query to match against the text
    pub query: String,
    #[serde(skip)]
    pub _embedding: Option<Vec<u8>>,
    /// The text embedding model to use
    ///
    /// The text embedding model to use for the semantic search.
    /// Will search embeddings produced by this model.
    pub model: String,
    /// The method to aggregate distances when an item has multiple embeddings. Default is MIN.
    #[serde(default)]
    pub distance_aggregation: DistanceAggregation,
    /// Embed The Query
    ///
    /// Embed the query using the model already specified in `model`.
    /// This is useful when the query is a string and needs to be converted to an embedding.
    ///
    /// If this is not present, the query is assumed to be an embedding already.
    /// In that case, it must be a base64 encoded string of a numpy array.
    #[serde(default = "default_embed_args")]
    pub embed: Option<EmbedArgs>,
    /// Filters and options to apply on source text that the embeddings are derived from.
    #[serde(default)]
    pub src_text: Option<SourceArgs>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SemanticTextSearch {
    #[serde(flatten, default = "default_sort_asc")]
    pub sort: SortableOptions,
    /// Search Text Embeddings
    ///
    /// Search for text using semantic search on text embeddings.
    pub text_embeddings: SemanticTextArgs,
}

fn default_cache_key() -> String {
    "search".to_string()
}

fn default_lru_size() -> i64 {
    1
}

fn default_ttl_seconds() -> i64 {
    60
}

fn default_embed_args() -> Option<EmbedArgs> {
    Some(EmbedArgs::default())
}

// Used by serde default attribute.
#[allow(dead_code)]
fn default_sort_asc() -> SortableOptions {
    let mut options = SortableOptions::default();
    options.order_by = true;
    options.direction = OrderDirection::Asc;
    options.row_n_direction = OrderDirection::Asc;
    options
}

impl FilterCompiler for SemanticTextSearch {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.text_embeddings;
        let embedding = args
            ._embedding
            .as_ref()
            .ok_or_else(|| PqlError::invalid("text_embeddings missing embedding bytes"))?;
        let cte_name = format!("n{}_SemanticTextSearch", state.cte_counter);

        let text_data = Alias::new("text_data");
        let text_setters = Alias::new("text_setters");
        let vec_data = Alias::new("vec_data");
        let vec_setters = Alias::new("vec_setters");

        let mut criteria = Vec::new();
        let mut weights_used = false;
        if let Some(src_text) = &args.src_text {
            if src_text.min_length > 0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                        .gte(src_text.min_length),
                );
            }
            if let Some(max_length) = src_text.max_length {
                if max_length > 0 {
                    criteria.push(
                        Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                            .lte(max_length),
                    );
                }
            }
            if !src_text.setters.is_empty() {
                let setters = src_text
                    .setters
                    .iter()
                    .cloned()
                    .map(Expr::val)
                    .collect::<Vec<_>>();
                criteria.push(Expr::col((text_setters.clone(), Setters::Name)).is_in(setters));
            }
            if let Some(languages) = &src_text.languages {
                if !languages.is_empty() {
                    let values = languages.iter().cloned().map(Expr::val).collect::<Vec<_>>();
                    criteria.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Language)).is_in(values),
                    );
                }
            }
            if src_text.min_language_confidence > 0.0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
                        .gte(src_text.min_language_confidence),
                );
            }
            if let Some(min_confidence) = src_text.min_confidence {
                if min_confidence > 0.0 {
                    criteria.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Confidence))
                            .gte(min_confidence),
                    );
                }
            }
            if src_text.confidence_weight != 0.0 || src_text.language_confidence_weight != 0.0 {
                weights_used = true;
            }
        }

        let vec_distance: Expr = Func::cust("vec_distance_L2")
            .args([
                Expr::col((Embeddings::Table, Embeddings::Embedding)),
                Expr::val(embedding.clone()),
            ])
            .into();
        let mut rank_column = match args.distance_aggregation {
            DistanceAggregation::Max => vec_distance.clone().max(),
            DistanceAggregation::Avg => vec_distance.clone().avg(),
            DistanceAggregation::Min => vec_distance.clone().min(),
        };

        if let Some(src_text) = &args.src_text {
            let conf_weight_clause: Expr = Func::cust("pow")
                .args([
                    Func::coalesce([
                        Expr::col((ExtractedText::Table, ExtractedText::Confidence)),
                        Expr::val(1),
                    ])
                    .into(),
                    Expr::val(src_text.confidence_weight),
                ])
                .into();
            let lang_conf_weight_clause: Expr = Func::cust("pow")
                .args([
                    Func::coalesce([
                        Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence)),
                        Expr::val(1),
                    ])
                    .into(),
                    Expr::val(src_text.language_confidence_weight),
                ])
                .into();
            if src_text.confidence_weight != 0.0 && src_text.language_confidence_weight != 0.0 {
                let weights = conf_weight_clause
                    .clone()
                    .mul(lang_conf_weight_clause.clone());
                rank_column = vec_distance
                    .clone()
                    .mul(weights.clone())
                    .sum()
                    .div(weights.sum());
            } else if src_text.confidence_weight != 0.0 {
                rank_column = vec_distance
                    .clone()
                    .mul(conf_weight_clause.clone())
                    .sum()
                    .div(conf_weight_clause.sum());
            } else if src_text.language_confidence_weight != 0.0 {
                rank_column = vec_distance
                    .clone()
                    .mul(lang_conf_weight_clause.clone())
                    .sum()
                    .div(lang_conf_weight_clause.sum());
            }
        }

        if state.item_data_query && matches!(state.entity, EntityType::Text) {
            let mut query = select_std_from_cte(context, state);
            query.join_as(
                JoinType::InnerJoin,
                ItemData::Table,
                text_data.clone(),
                Expr::col((text_data.clone(), ItemData::Id)).equals(context.column_ref("data_id")),
            );
            query.join_as(
                JoinType::InnerJoin,
                Setters::Table,
                text_setters.clone(),
                Expr::col((text_setters.clone(), Setters::Id))
                    .equals((text_data.clone(), ItemData::SetterId)),
            );
            query.join(
                JoinType::InnerJoin,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals(context.column_ref("data_id")),
            );
            query.join_as(
                JoinType::InnerJoin,
                ItemData::Table,
                vec_data.clone(),
                Expr::col((vec_data.clone(), ItemData::SourceId))
                    .equals((ExtractedText::Table, ExtractedText::Id)),
            );
            let vec_join = Cond::all()
                .add(
                    Expr::col((vec_setters.clone(), Setters::Id))
                        .equals((vec_data.clone(), ItemData::SetterId)),
                )
                .add(Expr::col((vec_setters.clone(), Setters::Name)).eq(args.model.clone()));
            query.join_as(
                JoinType::InnerJoin,
                Setters::Table,
                vec_setters.clone(),
                vec_join,
            );
            query.join(
                JoinType::InnerJoin,
                Embeddings::Table,
                Expr::col((Embeddings::Table, Embeddings::Id))
                    .equals((vec_data.clone(), ItemData::Id)),
            );

            for condition in &criteria {
                query.and_where(condition.clone());
            }

            apply_group_by(&mut query, get_std_group_by(context, state));
            if !state.is_count_query {
                add_rank_column_expr(&mut query, &self.sort, rank_column)?;
            }

            let (query, context_for_wrap) =
                apply_sort_bounds(state, query, context.clone(), &cte_name, &self.sort);

            let mut joined_tables = JoinedTables::default();
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            joined_tables.mark(BaseTable::ExtractedText);
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
            return Ok(cte);
        }

        let mut query = select_std_from_cte(context, state);
        query.join_as(
            JoinType::InnerJoin,
            ItemData::Table,
            vec_data.clone(),
            Expr::col((vec_data.clone(), ItemData::ItemId)).equals(context.column_ref("item_id")),
        );
        let vec_join = Cond::all()
            .add(
                Expr::col((vec_setters.clone(), Setters::Id))
                    .equals((vec_data.clone(), ItemData::SetterId)),
            )
            .add(Expr::col((vec_setters.clone(), Setters::Name)).eq(args.model.clone()));
        query.join_as(
            JoinType::InnerJoin,
            Setters::Table,
            vec_setters.clone(),
            vec_join,
        );
        query.join(
            JoinType::InnerJoin,
            Embeddings::Table,
            Expr::col((Embeddings::Table, Embeddings::Id)).equals((vec_data.clone(), ItemData::Id)),
        );

        if !criteria.is_empty() || weights_used {
            query.join_as(
                JoinType::InnerJoin,
                ItemData::Table,
                text_data.clone(),
                Expr::col((text_data.clone(), ItemData::Id))
                    .equals((vec_data.clone(), ItemData::SourceId)),
            );
            query.join_as(
                JoinType::InnerJoin,
                Setters::Table,
                text_setters.clone(),
                Expr::col((text_setters.clone(), Setters::Id))
                    .equals((text_data.clone(), ItemData::SetterId)),
            );
            query.join(
                JoinType::InnerJoin,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals((text_data.clone(), ItemData::Id)),
            );
        }
        for condition in &criteria {
            query.and_where(condition.clone());
        }

        apply_group_by(&mut query, get_std_group_by(context, state));
        if !state.is_count_query {
            add_rank_column_expr(&mut query, &self.sort, rank_column)?;
        }

        let (query, context_for_wrap) =
            apply_sort_bounds(state, query, context.clone(), &cte_name, &self.sort);

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::ItemData);
        joined_tables.mark(BaseTable::Setters);
        if !criteria.is_empty() || weights_used {
            joined_tables.mark(BaseTable::ExtractedText);
        }
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
    fn semantic_text_builds_sql() {
        let mut filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": { "query": "hello", "model": "textembed/test" }
        }))
        .expect("semantic text filter");
        filter.text_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("embeddings"));
    }

    #[tokio::test]
    async fn semantic_text_runs_full_query() {
        let mut filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": { "query": "hello", "model": "textembed/test" }
        }))
        .expect("semantic text filter");
        filter.text_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        run_full_pql_query(QueryElement::SemanticTextSearch(filter), EntityType::File)
            .await
            .expect("semantic text query");
    }
}
