use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{EntityType, OrderDirection, PartialSortableOptions, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, EmbeddingQuants, Embeddings, ExtraColumn, ExtractedText, ItemData,
    JoinedTables, OrderByFilter, QueryState, Setters, add_rank_column_expr, apply_group_by,
    apply_sort_bounds, get_std_group_by, select_std_from_cte, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::{DistanceAggregation, IndexMode, QuantResolved, default_k};
use super::item_similarity::SourceArgs;
use super::quant::{COARSE_DIST, COARSE_RANK, EXACT_DIST, assemble_two_stage};

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
    /// Index mode: `auto` (default) uses the default quant profile where its
    /// coverage is ready for this model, else exact; `exact` always
    /// brute-forces full-precision vectors; `quant` demands a quant profile
    /// and errors when it isn't ready. `ann` is reserved.
    ///
    /// Under a quant profile the displayed head order is always re-scored
    /// against full-precision vectors (see `k`), and `order_rank` is a rank,
    /// not a raw distance.
    #[serde(default)]
    pub index: IndexMode,
    /// Selects a specific quant profile by name (requires quant/auto index
    /// semantics). Naming a profile that doesn't exist or isn't ready for
    /// this model is a validation error, not a silent fallback.
    #[serde(default)]
    pub variant: Option<String>,
    /// The exactness horizon: the coarse-top-k candidates re-scored with
    /// full-precision distances. Ignored by `exact`. Keep it fixed across a
    /// pagination session.
    #[serde(default = "default_k")]
    pub k: i64,
    #[serde(skip)]
    pub _quant: Option<QuantResolved>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct SemanticTextSearch {
    #[serde(flatten)]
    pub sort: SortableOptions,
    /// Search Text Embeddings
    ///
    /// Search for text using semantic search on text embeddings.
    pub text_embeddings: SemanticTextArgs,
}

// Manual impl because serde ignores `default = ...` on flattened fields;
// this filter orders results by distance (ascending, best matches first)
// by default.
impl<'de> serde::Deserialize<'de> for SemanticTextSearch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(flatten)]
            sort: PartialSortableOptions,
            text_embeddings: SemanticTextArgs,
        }
        let repr = Repr::deserialize(deserializer)?;
        Ok(Self {
            sort: repr.sort.resolve(default_sort_asc()),
            text_embeddings: repr.text_embeddings,
        })
    }
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

fn default_sort_asc() -> SortableOptions {
    let mut options = SortableOptions::default();
    options.order_by = true;
    options.direction = OrderDirection::Asc;
    options.row_n_direction = OrderDirection::Asc;
    options
}

/// Which vector payload the candidate skeletons join.
enum TextVectorJoin {
    Embeddings,
    Quants { profile_id: i64 },
}

struct TextCriteria {
    conditions: Vec<Expr>,
    weights_used: bool,
}

impl SemanticTextSearch {
    fn criteria(&self) -> TextCriteria {
        let args = &self.text_embeddings;
        let text_setters = Alias::new("text_setters");
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
        TextCriteria {
            conditions: criteria,
            weights_used,
        }
    }

    /// Adds the vector payload join keyed on the embedding's item_data row.
    fn join_vector_table(
        query: &mut sea_query::SelectStatement,
        vec_data: &Alias,
        join: &TextVectorJoin,
    ) {
        match join {
            TextVectorJoin::Embeddings => {
                query.join(
                    JoinType::InnerJoin,
                    Embeddings::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .equals((vec_data.clone(), ItemData::Id)),
                );
            }
            TextVectorJoin::Quants { profile_id } => {
                let quant_cond = Cond::all()
                    .add(
                        Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Id))
                            .equals((vec_data.clone(), ItemData::Id)),
                    )
                    .add(
                        Expr::col((EmbeddingQuants::Table, EmbeddingQuants::ProfileId))
                            .eq(*profile_id),
                    );
                query.join(JoinType::InnerJoin, EmbeddingQuants::Table, quant_cond);
            }
        }
    }

    /// Candidate skeleton for text-entity queries: the context's own text
    /// rows joined to the embeddings derived from them.
    fn text_entity_skeleton(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &TextVectorJoin,
        criteria: &TextCriteria,
    ) -> sea_query::SelectStatement {
        let args = &self.text_embeddings;
        let text_data = Alias::new("text_data");
        let text_setters = Alias::new("text_setters");
        let vec_data = Alias::new("vec_data");
        let vec_setters = Alias::new("vec_setters");

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
        Self::join_vector_table(&mut query, &vec_data, join);

        for condition in &criteria.conditions {
            query.and_where(condition.clone());
        }
        query
    }

    /// Candidate skeleton for file/item queries: the context's items joined
    /// to their text-embedding rows (and, when criteria or weights need it,
    /// the source text).
    fn file_skeleton(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &TextVectorJoin,
        criteria: &TextCriteria,
    ) -> sea_query::SelectStatement {
        let args = &self.text_embeddings;
        let text_data = Alias::new("text_data");
        let text_setters = Alias::new("text_setters");
        let vec_data = Alias::new("vec_data");
        let vec_setters = Alias::new("vec_setters");

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
        Self::join_vector_table(&mut query, &vec_data, join);

        if !criteria.conditions.is_empty() || criteria.weights_used {
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
        for condition in &criteria.conditions {
            query.and_where(condition.clone());
        }
        query
    }

    /// The full-precision rank aggregate, including confidence weighting.
    fn exact_rank_column(&self, embedding: &[u8]) -> Expr {
        let args = &self.text_embeddings;
        let vec_distance: Expr = Func::cust("vec_distance_L2")
            .args([
                Expr::col((Embeddings::Table, Embeddings::Embedding)),
                Expr::val(embedding.to_vec()),
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
        rank_column
    }

    /// The weight-free coarse proxy: plain aggregated Hamming distance over
    /// binary quants (part of the bounded approximation by design).
    /// Stored quants and the bound parameter are plain BLOBs, which
    /// sqlite-vec would read as float32 — vec_bit marks them as bit vectors.
    fn coarse_rank_column(&self, query_quant: &[u8]) -> Expr {
        let hamming: Expr = Func::cust("vec_distance_hamming")
            .args([
                Func::cust("vec_bit")
                    .arg(Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Quant)))
                    .into(),
                Func::cust("vec_bit")
                    .arg(Expr::val(query_quant.to_vec()))
                    .into(),
            ])
            .into();
        match self.text_embeddings.distance_aggregation {
            DistanceAggregation::Max => hamming.max(),
            DistanceAggregation::Avg => hamming.avg(),
            DistanceAggregation::Min => hamming.min(),
        }
    }

    fn register_outputs(&self, state: &mut QueryState, cte: &CteRef) {
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
}

impl FilterCompiler for SemanticTextSearch {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.text_embeddings;
        let embedding = args
            ._embedding
            .as_ref()
            .ok_or_else(|| PqlError::invalid("text_embeddings missing embedding bytes"))?;
        let cte_name = format!("n{}_SemanticTextSearch", state.cte_counter);
        let criteria = self.criteria();
        let text_entity = state.item_data_query && matches!(state.entity, EntityType::Text);

        // The text-entity shape only joins `extracted_text` without an alias
        // (and on the same condition add_inner_joins would use); the file
        // shape joins item_data/setters only under aliases (and its
        // extracted_text join is bound to the embedding's source text), so
        // no marks there.
        let make_joined_tables = |for_bounds: bool| {
            let mut joined_tables = JoinedTables::default();
            if text_entity && !for_bounds {
                joined_tables.mark(BaseTable::ExtractedText);
            }
            joined_tables
        };

        let skeleton = |state: &QueryState, ctx: &CteRef, join: &TextVectorJoin| {
            if text_entity {
                self.text_entity_skeleton(ctx, state, join, &criteria)
            } else {
                self.file_skeleton(ctx, state, join, &criteria)
            }
        };

        if let Some(quant) = args._quant.as_ref().filter(|_| !state.is_count_query) {
            let query_quant = quant
                .query_quant
                .as_ref()
                .ok_or_else(|| PqlError::invalid("text_embeddings missing query quant"))?;

            let mut coarse = skeleton(
                state,
                context,
                &TextVectorJoin::Quants {
                    profile_id: quant.profile_id,
                },
            );
            apply_group_by(&mut coarse, get_std_group_by(context, state));
            coarse.expr_as(self.coarse_rank_column(query_quant), Alias::new(COARSE_DIST));

            let k = args.k;
            let (merge, merge_context) =
                assemble_two_stage(state, &cte_name, coarse, &self.sort, |state, ranked| {
                    let mut head = skeleton(state, ranked, &TextVectorJoin::Embeddings);
                    head.and_where(Expr::col(ranked.column_ref(COARSE_RANK)).lte(k));
                    apply_group_by(&mut head, get_std_group_by(ranked, state));
                    head.expr_as(self.exact_rank_column(embedding), Alias::new(EXACT_DIST));
                    head
                });

            // The merge selects only from CTEs, so no base tables are
            // visible to the final query; its context is the ranked CTE in
            // its FROM scope.
            let (query, context_for_wrap, joined_tables) = apply_sort_bounds(
                state,
                merge,
                merge_context,
                &cte_name,
                &self.sort,
                JoinedTables::default(),
            );
            let cte = wrap_query(state, query, &context_for_wrap, cte_name, &joined_tables);
            state.cte_counter += 1;
            self.register_outputs(state, &cte);
            return Ok(cte);
        }

        let mut query = skeleton(state, context, &TextVectorJoin::Embeddings);
        apply_group_by(&mut query, get_std_group_by(context, state));
        if !state.is_count_query {
            add_rank_column_expr(&mut query, &self.sort, self.exact_rank_column(embedding))?;
        }

        let (query, context_for_wrap, joined_tables) = apply_sort_bounds(
            state,
            query,
            context.clone(),
            &cte_name,
            &self.sort,
            make_joined_tables(false),
        );

        let cte = wrap_query(state, query, &context_for_wrap, cte_name, &joined_tables);
        state.cte_counter += 1;
        if !state.is_count_query {
            self.register_outputs(state, &cte);
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
    fn semantic_text_defaults_to_order_by_distance() {
        use crate::pql::model::OrderDirection;
        let filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": { "query": "hello", "model": "textembed/test" }
        }))
        .expect("semantic text filter");
        assert!(filter.sort.order_by);
        assert!(matches!(filter.sort.direction, OrderDirection::Asc));

        let filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": { "query": "hello", "model": "textembed/test" },
            "order_by": false
        }))
        .expect("semantic text filter");
        assert!(!filter.sort.order_by);
    }

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

    #[tokio::test]
    async fn semantic_text_entity_text_with_text_columns_runs_full_query() {
        // Regression: this filter only joins item_data/setters under aliases,
        // so the final query must add the standard unaliased joins for the
        // selected text columns to resolve.
        use super::super::test_support::run_pql_query;
        use crate::pql::model::{Column, PqlQuery};

        let mut filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": { "query": "hello", "model": "textembed/test" }
        }))
        .expect("semantic text filter");
        filter.text_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        let query = PqlQuery {
            query: Some(QueryElement::SemanticTextSearch(filter)),
            entity: EntityType::Text,
            select: vec![Column::SetterName, Column::JobId],
            ..Default::default()
        };
        run_pql_query(query)
            .await
            .expect("semantic text entity query");
    }
}
