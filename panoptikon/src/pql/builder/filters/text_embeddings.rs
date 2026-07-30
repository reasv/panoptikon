use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{EntityType, OrderDirection, PartialSortableOptions, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, EmbeddingQuants, Embeddings, ExtraColumn, ExtractedText, ItemData,
    JoinedTables, OrderByFilter, QueryState, Setters, apply_group_by, apply_sort_bounds,
    get_std_group_by, select_std_from_cte, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::{DistanceAggregation, IndexMode, QuantResolved, default_k};
use super::exact::{
    assemble_exact_fixb, confidence_weight_expr, grouped_over_materialized_distance,
};
use super::item_similarity::SourceArgs;
use super::quant::{
    COARSE_DIST, COARSE_RANK, COARSE_ROW_DIST, EXACT_DIST, HEAD_ROW_DIST, assemble_two_stage,
};

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

/// How a candidate skeleton reaches its context.
enum Drive {
    /// Context-driven: plain INNER JOINs, planner free to pick the order.
    /// Used by the count path, the exact path, and the quant coarse pass.
    Free,
    /// Ranked-driven head: the same joins rendered as `CROSS JOIN`s with
    /// their conditions in `WHERE`, restricted to `crank <= k`.
    ///
    /// `CROSS JOIN` is SQLite's join-order pin. Without it the planner drives
    /// the head from the vector tables over the *whole* setter and probes the
    /// ranked CTE, so every full-precision vector is read even though only
    /// `k` of them are needed — the execution bug measured in
    /// docs/or-composition-penalty.md §6. sea-query drops any `ON` given to a
    /// cross join, hence the conditions move to `WHERE`; that is also what
    /// keeps the pin intact.
    Pinned { k: i64 },
}

/// Adds one link of a skeleton's join chain, as an INNER JOIN or as an
/// order-pinning CROSS JOIN with the condition restated in `WHERE`.
fn add_chain_join<R>(
    query: &mut sea_query::SelectStatement,
    drive: &Drive,
    table: R,
    alias: Option<&Alias>,
    condition: Cond,
) where
    R: sea_query::IntoTableRef,
{
    let pinned = matches!(drive, Drive::Pinned { .. });
    let join = if pinned {
        JoinType::CrossJoin
    } else {
        JoinType::InnerJoin
    };
    match alias {
        Some(alias) => {
            query.join_as(join, table, alias.clone(), condition.clone());
        }
        None => {
            query.join(join, table, condition.clone());
        }
    }
    if pinned {
        query.cond_where(condition);
    }
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
        drive: &Drive,
        vec_data: &Alias,
        join: &TextVectorJoin,
    ) {
        match join {
            TextVectorJoin::Embeddings => {
                let cond = Cond::all().add(
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .equals((vec_data.clone(), ItemData::Id)),
                );
                add_chain_join(query, drive, Embeddings::Table, None, cond);
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
                add_chain_join(query, drive, EmbeddingQuants::Table, None, quant_cond);
            }
        }
    }

    /// Candidate skeleton for text-entity queries: the context's own text
    /// rows joined to the embeddings derived from them.
    ///
    /// Under `Drive::Pinned` the chain is pinned exactly as written —
    /// context → text_data (PK) → text_setters (PK) → extracted_text (PK) →
    /// vec_data (`source_id`) → vec_setters → payload — so the head probes
    /// only the `crank <= k` candidates.
    fn text_entity_skeleton(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &TextVectorJoin,
        criteria: &TextCriteria,
        drive: &Drive,
    ) -> sea_query::SelectStatement {
        let args = &self.text_embeddings;
        let text_data = Alias::new("text_data");
        let text_setters = Alias::new("text_setters");
        let vec_data = Alias::new("vec_data");
        let vec_setters = Alias::new("vec_setters");

        let mut query = select_std_from_cte(context, state);
        if let Drive::Pinned { k } = drive {
            query.and_where(Expr::col(context.column_ref(COARSE_RANK)).lte(*k));
        }
        add_chain_join(
            &mut query,
            drive,
            ItemData::Table,
            Some(&text_data),
            Cond::all().add(
                Expr::col((text_data.clone(), ItemData::Id)).equals(context.column_ref("data_id")),
            ),
        );
        add_chain_join(
            &mut query,
            drive,
            Setters::Table,
            Some(&text_setters),
            Cond::all().add(
                Expr::col((text_setters.clone(), Setters::Id))
                    .equals((text_data.clone(), ItemData::SetterId)),
            ),
        );
        add_chain_join(
            &mut query,
            drive,
            ExtractedText::Table,
            None,
            Cond::all().add(
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals(context.column_ref("data_id")),
            ),
        );
        add_chain_join(
            &mut query,
            drive,
            ItemData::Table,
            Some(&vec_data),
            Cond::all().add(
                Expr::col((vec_data.clone(), ItemData::SourceId))
                    .equals((ExtractedText::Table, ExtractedText::Id)),
            ),
        );
        let vec_join = Cond::all()
            .add(
                Expr::col((vec_setters.clone(), Setters::Id))
                    .equals((vec_data.clone(), ItemData::SetterId)),
            )
            .add(Expr::col((vec_setters.clone(), Setters::Name)).eq(args.model.clone()));
        add_chain_join(
            &mut query,
            drive,
            Setters::Table,
            Some(&vec_setters),
            vec_join,
        );
        Self::join_vector_table(&mut query, drive, &vec_data, join);

        for condition in &criteria.conditions {
            query.and_where(condition.clone());
        }
        query
    }

    /// Candidate skeleton for file/item queries: the context's items joined
    /// to their text-embedding rows (and, when criteria or weights need it,
    /// the source text).
    ///
    /// Under `Drive::Pinned` the vector chain is reordered to resolve the
    /// setter first: `name` → `id` is one index probe, and it turns the
    /// `item_data` lookup into a covering `(item_id, setter_id)` probe instead
    /// of "every data row of the item, then filter by setter". The
    /// `vec_setters` condition may reference `vec_data`, joined after it,
    /// because a pinned link's condition lives in `WHERE`, not `ON`.
    fn file_skeleton(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &TextVectorJoin,
        criteria: &TextCriteria,
        drive: &Drive,
    ) -> sea_query::SelectStatement {
        let args = &self.text_embeddings;
        let text_data = Alias::new("text_data");
        let text_setters = Alias::new("text_setters");
        let vec_data = Alias::new("vec_data");
        let vec_setters = Alias::new("vec_setters");

        let mut query = select_std_from_cte(context, state);
        if let Drive::Pinned { k } = drive {
            query.and_where(Expr::col(context.column_ref(COARSE_RANK)).lte(*k));
        }
        let vec_data_cond = Cond::all().add(
            Expr::col((vec_data.clone(), ItemData::ItemId)).equals(context.column_ref("item_id")),
        );
        let vec_setters_cond = Cond::all()
            .add(
                Expr::col((vec_setters.clone(), Setters::Id))
                    .equals((vec_data.clone(), ItemData::SetterId)),
            )
            .add(Expr::col((vec_setters.clone(), Setters::Name)).eq(args.model.clone()));
        if matches!(drive, Drive::Pinned { .. }) {
            add_chain_join(
                &mut query,
                drive,
                Setters::Table,
                Some(&vec_setters),
                vec_setters_cond,
            );
            add_chain_join(
                &mut query,
                drive,
                ItemData::Table,
                Some(&vec_data),
                vec_data_cond,
            );
        } else {
            add_chain_join(
                &mut query,
                drive,
                ItemData::Table,
                Some(&vec_data),
                vec_data_cond,
            );
            add_chain_join(
                &mut query,
                drive,
                Setters::Table,
                Some(&vec_setters),
                vec_setters_cond,
            );
        }
        Self::join_vector_table(&mut query, drive, &vec_data, join);

        if !criteria.conditions.is_empty() || criteria.weights_used {
            add_chain_join(
                &mut query,
                drive,
                ItemData::Table,
                Some(&text_data),
                Cond::all().add(
                    Expr::col((text_data.clone(), ItemData::Id))
                        .equals((vec_data.clone(), ItemData::SourceId)),
                ),
            );
            add_chain_join(
                &mut query,
                drive,
                Setters::Table,
                Some(&text_setters),
                Cond::all().add(
                    Expr::col((text_setters.clone(), Setters::Id))
                        .equals((text_data.clone(), ItemData::SetterId)),
                ),
            );
            add_chain_join(
                &mut query,
                drive,
                ExtractedText::Table,
                None,
                Cond::all().add(
                    Expr::col((ExtractedText::Table, ExtractedText::Id))
                        .equals((text_data.clone(), ItemData::Id)),
                ),
            );
        }
        for condition in &criteria.conditions {
            query.and_where(condition.clone());
        }
        query
    }

    /// The per-row full-precision distance (references the `embeddings`
    /// join, so it is only valid over the embeddings-joined skeleton).
    fn exact_distance_expr(&self, embedding: &[u8]) -> Expr {
        Func::cust("vec_distance_L2")
            .args([
                Expr::col((Embeddings::Table, Embeddings::Embedding)),
                Expr::val(embedding.to_vec()),
            ])
            .into()
    }

    /// The per-row confidence weight, when confidence weighting applies.
    fn confidence_weight(&self) -> Option<Expr> {
        self.text_embeddings
            .src_text
            .as_ref()
            .and_then(confidence_weight_expr)
    }

    /// The per-row coarse proxy: Hamming distance over binary quants. Never
    /// confidence-weighted — the coarse ordering is a weight-free
    /// approximation by design.
    /// Stored quants and the bound parameter are plain BLOBs, which
    /// sqlite-vec would read as float32 — vec_bit marks them as bit vectors.
    fn coarse_distance_expr(&self, query_quant: &[u8]) -> Expr {
        Func::cust("vec_distance_hamming")
            .args([
                Func::cust("vec_bit")
                    .arg(Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Quant)))
                    .into(),
                Func::cust("vec_bit")
                    .arg(Expr::val(query_quant.to_vec()))
                    .into(),
            ])
            .into()
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

        let skeleton = |state: &QueryState, ctx: &CteRef, join: &TextVectorJoin, drive: &Drive| {
            if text_entity {
                self.text_entity_skeleton(ctx, state, join, &criteria, drive)
            } else {
                self.file_skeleton(ctx, state, join, &criteria, drive)
            }
        };

        if let Some(quant) = args._quant.as_ref().filter(|_| !state.is_count_query) {
            let query_quant = quant
                .query_quant
                .as_ref()
                .ok_or_else(|| PqlError::invalid("text_embeddings missing query quant"))?;

            // Fix B on the coarse pass: the per-row Hamming distance is
            // evaluated in `qdist_{cte}` and only its 8-byte result reaches
            // the coarse GROUP BY sorter, never the quant blob.
            let coarse_skeleton = skeleton(
                state,
                context,
                &TextVectorJoin::Quants {
                    profile_id: quant.profile_id,
                },
                &Drive::Free,
            );
            let coarse = {
                let grouped = grouped_over_materialized_distance(
                    state,
                    format!("qdist_{cte_name}"),
                    COARSE_ROW_DIST,
                    coarse_skeleton,
                    self.coarse_distance_expr(query_quant),
                    None,
                    args.distance_aggregation,
                );
                let mut coarse = grouped.select;
                coarse.expr_as(grouped.aggregate, Alias::new(COARSE_DIST));
                coarse
            };

            let k = args.k;
            let (merge, merge_context) =
                assemble_two_stage(state, &cte_name, coarse, &self.sort, |state, ranked| {
                    // Ranked-driven, order-pinned head with its distance (and
                    // confidence weights) materialized in `hdist_{cte}`.
                    let head_skeleton = skeleton(
                        state,
                        ranked,
                        &TextVectorJoin::Embeddings,
                        &Drive::Pinned { k },
                    );
                    let grouped = grouped_over_materialized_distance(
                        state,
                        format!("hdist_{cte_name}"),
                        HEAD_ROW_DIST,
                        head_skeleton,
                        self.exact_distance_expr(embedding),
                        self.confidence_weight(),
                        args.distance_aggregation,
                    );
                    let mut head = grouped.select;
                    head.expr_as(grouped.aggregate, Alias::new(EXACT_DIST));
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

        if state.is_count_query {
            // Membership only — no distance is computed, so the fix-B
            // restructuring below would add a pointless materialization.
            let mut query = skeleton(state, context, &TextVectorJoin::Embeddings, &Drive::Free);
            apply_group_by(&mut query, get_std_group_by(context, state));
            // The text-entity shape joins `extracted_text` without an alias
            // (and on the same condition add_inner_joins would use); the
            // file shape joins item_data/setters only under aliases (and its
            // extracted_text join is bound to the embedding's source text),
            // so no marks there.
            let mut joined_tables = JoinedTables::default();
            if text_entity {
                joined_tables.mark(BaseTable::ExtractedText);
            }
            let cte = wrap_query(state, query, context, cte_name, &joined_tables);
            state.cte_counter += 1;
            return Ok(cte);
        }

        // Fix B (docs/or-composition-penalty.md §5): the distance is
        // evaluated in a materialized CTE so the GROUP BY sorter never
        // carries the embedding blob.
        let skeleton = skeleton(state, context, &TextVectorJoin::Embeddings, &Drive::Free);
        let (query, dist_cte) = assemble_exact_fixb(
            state,
            &cte_name,
            skeleton,
            self.exact_distance_expr(embedding),
            self.confidence_weight(),
            args.distance_aggregation,
            &self.sort,
        )?;

        // The grouped select reads only the dist CTE, so no base tables are
        // visible to the final query; its context is the dist CTE.
        let (query, context_for_wrap, joined_tables) = apply_sort_bounds(
            state,
            query,
            dist_cte,
            &cte_name,
            &self.sort,
            JoinedTables::default(),
        );

        let cte = wrap_query(state, query, &context_for_wrap, cte_name, &joined_tables);
        state.cte_counter += 1;
        self.register_outputs(state, &cte);
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

    /// The rendered SQL with runs of whitespace collapsed: sea-query emits
    /// `AS  MATERIALIZED` with a double space, and the shape assertions below
    /// should read like the SQL they describe.
    fn normalized_sql(
        filter: &SemanticTextSearch,
        state: &mut QueryState,
        context: &CteRef,
    ) -> String {
        render_filter_sql(filter, state, context)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn quant_filter() -> SemanticTextSearch {
        let mut filter: SemanticTextSearch = serde_json::from_value(json!({
            "text_embeddings": {
                "query": "hello", "model": "textembed/test", "index": "quant"
            }
        }))
        .expect("semantic text filter");
        filter.text_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        filter.text_embeddings._quant = Some(QuantResolved {
            profile_id: 1,
            query_quant: Some(vec![0, 0, 0, 0]),
        });
        filter
    }

    /// Both quant passes must aggregate over a `MATERIALIZED` per-row
    /// distance CTE, and the head must be driven from `ranked` with a
    /// `CROSS JOIN`-pinned order (docs/or-composition-penalty.md §5b).
    /// Materialization is load-bearing: without it the flattener inlines the
    /// distance back into the aggregate and the blob rides the GROUP BY
    /// sorter again. The pin is load-bearing too: without it the planner
    /// drives the head from the vector tables over the whole setter.
    #[test]
    fn semantic_text_quant_materializes_distances_and_pins_head() {
        let filter = quant_filter();
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = normalized_sql(&filter, &mut state, &context);

        assert!(
            sql.contains(r#""qdist_n0_SemanticTextSearch" AS MATERIALIZED"#),
            "coarse distance is not materialized: {sql}"
        );
        assert!(
            sql.contains(r#"MIN("qdist_n0_SemanticTextSearch"."qd") AS "cdist""#),
            "coarse aggregate does not read the materialized distance: {sql}"
        );
        assert!(
            sql.contains(r#""hdist_n0_SemanticTextSearch" AS MATERIALIZED"#),
            "head distance is not materialized: {sql}"
        );
        assert!(
            sql.contains(r#"MIN("hdist_n0_SemanticTextSearch"."hd") AS "edist""#),
            "head aggregate does not read the materialized distance: {sql}"
        );
        // Setter first, then the covering (item_id, setter_id) probe.
        assert!(
            sql.contains(concat!(
                r#"FROM "ranked_n0_SemanticTextSearch""#,
                r#" CROSS JOIN "setters" AS "vec_setters""#,
                r#" CROSS JOIN "item_data" AS "vec_data""#,
                r#" CROSS JOIN "embeddings""#
            )),
            "file-shaped head is not ranked-driven with a pinned join order: {sql}"
        );
        assert!(
            sql.contains(r#""vec_data"."item_id" = "ranked_n0_SemanticTextSearch"."item_id""#),
            "head does not join vec_data straight to ranked: {sql}"
        );
    }

    /// The text-entity shape gets the same treatment, driven from the
    /// context's own `data_id`.
    #[test]
    fn semantic_text_entity_quant_materializes_distances_and_pins_head() {
        let filter = quant_filter();
        let mut state = build_base_state(EntityType::Text, false);
        let context = build_begin_cte(&mut state);
        let sql = normalized_sql(&filter, &mut state, &context);

        assert!(
            sql.contains(r#""qdist_n0_SemanticTextSearch" AS MATERIALIZED"#),
            "coarse distance is not materialized: {sql}"
        );
        assert!(
            sql.contains(r#""hdist_n0_SemanticTextSearch" AS MATERIALIZED"#),
            "head distance is not materialized: {sql}"
        );
        assert!(
            sql.contains(concat!(
                r#"FROM "ranked_n0_SemanticTextSearch""#,
                r#" CROSS JOIN "item_data" AS "text_data""#,
                r#" CROSS JOIN "setters" AS "text_setters""#,
                r#" CROSS JOIN "extracted_text""#,
                r#" CROSS JOIN "item_data" AS "vec_data""#,
                r#" CROSS JOIN "setters" AS "vec_setters""#,
                r#" CROSS JOIN "embeddings""#
            )),
            "text-entity head is not ranked-driven with a pinned join order: {sql}"
        );
        // data_id is part of the group key in the item_data shape.
        assert!(
            sql.contains(r#"GROUP BY "hdist_n0_SemanticTextSearch"."data_id""#),
            "head lost data_id from the group key: {sql}"
        );
    }

    #[tokio::test]
    async fn semantic_text_quant_runs_full_query() {
        run_full_pql_query(
            QueryElement::SemanticTextSearch(quant_filter()),
            EntityType::File,
        )
        .await
        .expect("semantic text quant query");
    }

    #[tokio::test]
    async fn semantic_text_entity_quant_runs_full_query() {
        run_full_pql_query(
            QueryElement::SemanticTextSearch(quant_filter()),
            EntityType::Text,
        )
        .await
        .expect("semantic text entity quant query");
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
