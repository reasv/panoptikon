use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType, Query};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{OrderDirection, PartialSortableOptions, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, EmbeddingQuants, Embeddings, ExtraColumn, ExtractedText, ItemData, Items,
    JoinedTables, OrderByFilter, QueryState, Setters, apply_group_by, apply_sort_bounds,
    get_std_group_by, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::{
    DistanceAggregation, DistanceFunction, IndexMode, QuantResolved, default_k,
};
use super::exact::{assemble_exact_fixb, confidence_weight_expr};
use super::item_similarity::SourceArgs;
use super::text_embeddings::EmbedArgs;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SemanticImageArgs {
    /// Query
    ///
    /// Semantic query to match against the image.
    /// Can be a string or a base64 encoded numpy array
    /// to supply an embedding directly.
    pub query: String,
    #[serde(skip)]
    pub _embedding: Option<Vec<u8>>,
    #[serde(skip)]
    pub _distance_func_override: Option<DistanceFunction>,
    /// The image embedding model to use
    ///
    /// The image embedding model to use for the semantic search.
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
    /// If true, will search among text embeddings as well as image embeddings created by the same CLIP model.
    ///
    /// Note that you must have both image and text embeddings with the same CLIP model for this setting to work.
    /// Text embeddings are derived from text which must have been already previously produced by another model, such as an OCR model or a tagger.
    /// They are generated *separately* from the image embeddings, using a different job (Under 'CLIP Text Embeddings').
    /// Run a batch job with the same clip model for both image and text embeddings to use this setting.
    #[serde(default)]
    pub clip_xmodal: bool,
    /// Filters and options to apply on source text.
    /// Can exclusively be used with `clip_xmodal` set to True.
    /// Otherwise, it will be ignored, as it only applies to text embeddings.
    #[serde(default)]
    pub src_text: Option<SourceArgs>,
    /// Index mode: `auto` (default) uses the default quant profile where its
    /// coverage is ready for this model, else exact; `exact` always
    /// brute-forces full-precision vectors; `quant` demands a quant profile
    /// and errors when it isn't ready. `ann` is reserved.
    ///
    /// A quant profile scores int8 codes in a single pass; `order_rank` has
    /// exactly the same semantics as under `exact`.
    #[serde(default)]
    pub index: IndexMode,
    /// Selects a specific quant profile by name (requires quant/auto index
    /// semantics). Naming a profile that doesn't exist or isn't ready for
    /// this model is a validation error, not a silent fallback.
    #[serde(default)]
    pub variant: Option<String>,
    /// Deprecated: ignored. Reserved for a future ANN index mode (top-k
    /// retrieval depth).
    #[serde(default = "default_k")]
    pub k: i64,
    #[serde(skip)]
    pub _quant: Option<QuantResolved>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct SemanticImageSearch {
    #[serde(flatten)]
    pub sort: SortableOptions,
    /// Search Image Embeddings
    ///
    /// Search for image using semantic search on image embeddings.
    pub image_embeddings: SemanticImageArgs,
}

// Manual impl because serde ignores `default = ...` on flattened fields;
// this filter orders results by distance (ascending, best matches first)
// by default.
impl<'de> serde::Deserialize<'de> for SemanticImageSearch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(flatten)]
            sort: PartialSortableOptions,
            image_embeddings: SemanticImageArgs,
        }
        let repr = Repr::deserialize(deserializer)?;
        Ok(Self {
            sort: repr.sort.resolve(default_sort_asc()),
            image_embeddings: repr.image_embeddings,
        })
    }
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

/// Which vector payload the candidate skeleton joins.
enum ImageVectorJoin {
    Embeddings,
    Quants { profile_id: i64 },
}

impl SemanticImageSearch {
    /// The shared candidate skeleton: model/setter joins, src_text filters,
    /// context join and standard columns. The count path and both scoring
    /// passes build on this; only the vector payload join differs, so
    /// membership is identical in every mode.
    fn candidate_skeleton(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &ImageVectorJoin,
    ) -> (sea_query::SelectStatement, bool) {
        let mut query = Query::select();
        query.from(Items::Table);
        query.join(
            JoinType::InnerJoin,
            ItemData::Table,
            Expr::col((ItemData::Table, ItemData::ItemId)).equals((Items::Table, Items::Id)),
        );
        let setter_cond = Cond::all()
            .add(
                Expr::col((Setters::Table, Setters::Id))
                    .equals((ItemData::Table, ItemData::SetterId)),
            )
            .add(self.model_cond());
        query.join(JoinType::InnerJoin, Setters::Table, setter_cond);
        match join {
            ImageVectorJoin::Embeddings => {
                query.join(
                    JoinType::InnerJoin,
                    Embeddings::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .equals((ItemData::Table, ItemData::Id)),
                );
            }
            ImageVectorJoin::Quants { profile_id } => {
                let quant_cond = Cond::all()
                    .add(
                        Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Id))
                            .equals((ItemData::Table, ItemData::Id)),
                    )
                    .add(
                        Expr::col((EmbeddingQuants::Table, EmbeddingQuants::ProfileId))
                            .eq(*profile_id),
                    );
                query.join(JoinType::InnerJoin, EmbeddingQuants::Table, quant_cond);
            }
        }

        let join_text = self.apply_src_text(&mut query);

        query.join(
            JoinType::LeftJoin,
            Alias::new(context.name.as_str()),
            Expr::col(context.column_ref("item_id")).equals((Items::Table, Items::Id)),
        );
        query.and_where(Expr::col(context.column_ref("item_id")).is_not_null());

        query.expr_as(context.column_expr("item_id"), Alias::new("item_id"));
        query.expr_as(context.column_expr("file_id"), Alias::new("file_id"));
        if state.item_data_query {
            query.expr_as(context.column_expr("data_id"), Alias::new("data_id"));
        }

        (query, join_text)
    }

    /// The setter-name condition: this filter's image model, widened to its
    /// `t`-prefixed text sibling under `clip_xmodal`.
    fn model_cond(&self) -> Expr {
        let args = &self.image_embeddings;
        let mut model_cond = Expr::col((Setters::Table, Setters::Name)).eq(args.model.clone());
        if args.clip_xmodal {
            model_cond = model_cond
                .or(Expr::col((Setters::Table, Setters::Name)).eq(format!("t{}", args.model)));
        }
        model_cond
    }

    /// The `src_text` LEFT JOINs and criteria, shared by every skeleton
    /// shape. Written against the unaliased `item_data` join, so the caller
    /// must have joined it already. Returns whether the unaliased
    /// `extracted_text` join was added (the count path has to declare it to
    /// the final query).
    fn apply_src_text(&self, query: &mut sea_query::SelectStatement) -> bool {
        let args = &self.image_embeddings;
        let src_setters = Alias::new("src_setters");
        let src_item_data = Alias::new("src_item_data");
        let mut join_text = false;
        if let Some(src_args) = &args.src_text {
            query.join_as(
                JoinType::LeftJoin,
                ItemData::Table,
                src_item_data.clone(),
                Expr::col((src_item_data.clone(), ItemData::Id))
                    .equals((ItemData::Table, ItemData::SourceId)),
            );
            if !src_args.setters.is_empty() {
                query.join_as(
                    JoinType::LeftJoin,
                    Setters::Table,
                    src_setters.clone(),
                    Expr::col((src_setters.clone(), Setters::Id))
                        .equals((src_item_data.clone(), ItemData::SetterId)),
                );
            }

            let mut conditions = Vec::new();
            if !src_args.setters.is_empty() {
                let setters = src_args
                    .setters
                    .iter()
                    .cloned()
                    .map(Expr::val)
                    .collect::<Vec<_>>();
                conditions.push(Expr::col((src_setters.clone(), Setters::Name)).is_in(setters));
            }
            if let Some(languages) = &src_args.languages {
                if !languages.is_empty() {
                    join_text = true;
                    let values = languages.iter().cloned().map(Expr::val).collect::<Vec<_>>();
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Language)).is_in(values),
                    );
                }
            }
            if let Some(min_confidence) = src_args.min_confidence {
                if min_confidence > 0.0 {
                    join_text = true;
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Confidence))
                            .gte(min_confidence),
                    );
                }
            }
            if src_args.min_language_confidence > 0.0 {
                join_text = true;
                conditions.push(
                    Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
                        .gte(src_args.min_language_confidence),
                );
            }
            if src_args.min_length > 0 {
                join_text = true;
                conditions.push(
                    Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                        .gte(src_args.min_length),
                );
            }
            if let Some(max_length) = src_args.max_length {
                if max_length > 0 {
                    join_text = true;
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                            .lte(max_length),
                    );
                }
            }
            if src_args.confidence_weight != 0.0 || src_args.language_confidence_weight != 0.0 {
                join_text = true;
            }

            if join_text {
                query.join(
                    JoinType::LeftJoin,
                    ExtractedText::Table,
                    Expr::col((ExtractedText::Table, ExtractedText::Id))
                        .equals((ItemData::Table, ItemData::SourceId)),
                );
            }
            if conditions.is_empty() {
                conditions.push(Expr::cust("1=1"));
            }

            let mut cond = Cond::any();
            cond = cond.add(Expr::col((src_item_data.clone(), ItemData::Id)).is_null());
            let mut and_cond = Cond::all();
            for condition in conditions {
                and_cond = and_cond.add(condition);
            }
            cond = cond.add(and_cond);
            query.and_where(cond.into());
        }
        join_text
    }

    /// The filter's distance function name.
    fn distance_func(&self) -> &'static str {
        match self.image_embeddings._distance_func_override {
            Some(DistanceFunction::L2) => "vec_distance_L2",
            _ => "vec_distance_cosine",
        }
    }

    /// The per-row full-precision distance (references the `embeddings`
    /// join, so it is only valid over the embeddings-joined skeleton).
    fn exact_distance_expr(&self, embedding: &[u8]) -> Expr {
        Func::cust(self.distance_func())
            .args([
                Expr::col((Embeddings::Table, Embeddings::Embedding)),
                Expr::val(embedding.to_vec()),
            ])
            .into()
    }

    /// The per-row confidence weight, when confidence weighting applies.
    fn confidence_weight(&self) -> Option<Expr> {
        self.image_embeddings
            .src_text
            .as_ref()
            .and_then(confidence_weight_expr)
    }

    /// The per-row distance over int8 codes — the same distance function as
    /// the exact path, on the quantized payload (docs/vector-int8-quant.md).
    /// Stored quants and the bound parameter are plain BLOBs, which
    /// sqlite-vec would read as float32 — `vec_int8` marks them as int8.
    fn quant_distance_expr(&self, query_quant: &[u8]) -> Expr {
        Func::cust(self.distance_func())
            .args([
                Func::cust("vec_int8")
                    .arg(Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Quant)))
                    .into(),
                Func::cust("vec_int8")
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

impl FilterCompiler for SemanticImageSearch {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.image_embeddings;
        let embedding = args
            ._embedding
            .as_ref()
            .ok_or_else(|| PqlError::invalid("image_embeddings missing embedding bytes"))?;
        let cte_name = format!("n{}_SemanticImageSearch", state.cte_counter);

        if state.is_count_query {
            // Membership only — identical in every index mode, so counts
            // never consult quants.
            let (mut query, join_text) =
                self.candidate_skeleton(context, state, &ImageVectorJoin::Embeddings);
            apply_group_by(&mut query, get_std_group_by(context, state));
            let mut joined_tables = JoinedTables::default();
            joined_tables.mark(BaseTable::Items);
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            // The unaliased extracted_text join only exists when a src_text
            // criterion (or weight) required it.
            if join_text {
                joined_tables.mark(BaseTable::ExtractedText);
            }
            let cte = wrap_query(state, query, context, cte_name, &joined_tables);
            state.cte_counter += 1;
            return Ok(cte);
        }

        // Exact and quant differ only in the vector payload: the skeleton's
        // join and the blob the distance function reads. Everything after
        // this — fix B's materialized per-row distance, the confidence
        // weights, the aggregation, the rank column — is shared, so
        // `order_rank` means the same thing in both modes
        // (docs/vector-int8-quant.md).
        let (skeleton, distance) = match &args._quant {
            Some(quant) => {
                let query_quant = quant
                    .query_quant
                    .as_ref()
                    .ok_or_else(|| PqlError::invalid("image_embeddings missing query quant"))?;
                let (skeleton, _) = self.candidate_skeleton(
                    context,
                    state,
                    &ImageVectorJoin::Quants {
                        profile_id: quant.profile_id,
                    },
                );
                (skeleton, self.quant_distance_expr(query_quant))
            }
            None => {
                let (skeleton, _) =
                    self.candidate_skeleton(context, state, &ImageVectorJoin::Embeddings);
                (skeleton, self.exact_distance_expr(embedding))
            }
        };

        // Fix B (docs/or-composition-penalty.md §5): the distance is
        // evaluated in a materialized CTE so the GROUP BY sorter never
        // carries the vector blob.
        let (query, dist_cte) = assemble_exact_fixb(
            state,
            &cte_name,
            skeleton,
            distance,
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
    fn semantic_image_defaults_to_order_by_distance() {
        let filter: SemanticImageSearch = serde_json::from_value(json!({
            "image_embeddings": { "query": "hello", "model": "clip/test" }
        }))
        .expect("semantic image filter");
        assert!(filter.sort.order_by);
        assert!(matches!(filter.sort.direction, OrderDirection::Asc));
    }

    #[test]
    fn image_embeddings_sync_preprocess_requires_distance_override() {
        use crate::pql::build_query;
        use crate::pql::model::PqlQuery;

        let mut filter: SemanticImageSearch = serde_json::from_value(json!({
            "image_embeddings": { "query": "hello", "model": "clip/test" }
        }))
        .expect("semantic image filter");
        filter.image_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        let query = PqlQuery {
            query: Some(QueryElement::SemanticImageSearch(filter)),
            ..Default::default()
        };
        assert!(build_query(query, false).is_err());
    }

    #[test]
    fn semantic_image_builds_sql() {
        let mut filter: SemanticImageSearch = serde_json::from_value(json!({
            "image_embeddings": { "query": "hello", "model": "clip/test" }
        }))
        .expect("semantic image filter");
        filter.image_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        filter.image_embeddings._distance_func_override = Some(DistanceFunction::Cosine);
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("embeddings"));
    }

    /// The rendered SQL with runs of whitespace collapsed: sea-query emits
    /// `AS  MATERIALIZED` with a double space, and the shape assertions below
    /// should read like the SQL they describe.
    fn normalized_sql(
        filter: &SemanticImageSearch,
        state: &mut QueryState,
        context: &CteRef,
    ) -> String {
        render_filter_sql(filter, state, context)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn quant_filter(extra: serde_json::Value) -> SemanticImageSearch {
        let mut args = json!({ "query": "hello", "model": "clip/test", "index": "quant" });
        let obj = args.as_object_mut().expect("args object");
        for (key, value) in extra.as_object().expect("extra object") {
            obj.insert(key.clone(), value.clone());
        }
        let mut filter: SemanticImageSearch =
            serde_json::from_value(json!({ "image_embeddings": args }))
                .expect("semantic image filter");
        filter.image_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        filter.image_embeddings._distance_func_override = Some(DistanceFunction::Cosine);
        filter.image_embeddings._quant = Some(QuantResolved {
            profile_id: 1,
            query_quant: Some(vec![0, 0, 0, 0]),
        });
        filter
    }

    /// The quant arm is the exact arm with a swapped payload: one fix-B
    /// `MATERIALIZED dist_{cte}` CTE over `embedding_quants` scored with
    /// `vec_int8`, and no trace of the deleted two-stage machinery
    /// (docs/vector-int8-quant.md).
    #[test]
    fn semantic_image_quant_is_single_pass_over_int8_codes() {
        let filter = quant_filter(json!({}));
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = normalized_sql(&filter, &mut state, &context);

        assert!(
            sql.contains(r#""dist_n0_SemanticImageSearch" AS MATERIALIZED"#),
            "quant distance is not materialized: {sql}"
        );
        assert!(
            sql.contains(r#"MIN("dist_n0_SemanticImageSearch"."d")"#),
            "quant aggregate does not read the materialized distance: {sql}"
        );
        assert!(
            sql.contains(r#"vec_distance_cosine(vec_int8("embedding_quants"."quant"), vec_int8("#),
            "quant distance is not an int8 distance over the quant payload: {sql}"
        );
        for gone in ["ranked_n0", "head_n0", "coarse_n0", "qdist_n0", "hdist_n0"] {
            assert!(!sql.contains(gone), "two-stage CTE {gone} survives: {sql}");
        }
        assert!(
            !sql.contains("CROSS JOIN"),
            "the deleted head pin chain survives: {sql}"
        );
    }

    /// The quant arm carries the confidence-weighted aggregate exactly as
    /// the exact arm does — over materialized per-row distance *and* weight
    /// columns, with the xmodal widening and src_text joins intact.
    #[test]
    fn semantic_image_quant_carries_confidence_weights() {
        let filter = quant_filter(json!({
            "clip_xmodal": true,
            "src_text": { "confidence_weight": 0.5, "language_confidence_weight": 0.5 }
        }));
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = normalized_sql(&filter, &mut state, &context);

        assert!(
            sql.contains(
                r#"SUM("dist_n0_SemanticImageSearch"."d" * "dist_n0_SemanticImageSearch"."w")"#
            ),
            "quant lost the confidence-weighted aggregate: {sql}"
        );
        assert!(
            sql.contains(r#""setters"."name" = 'tclip/test'"#),
            "quant lost the xmodal setter widening: {sql}"
        );
        assert!(
            sql.contains(r#"LEFT JOIN "extracted_text""#),
            "quant lost the src_text join: {sql}"
        );
    }

    #[tokio::test]
    async fn semantic_image_quant_runs_full_query() {
        run_full_pql_query(
            QueryElement::SemanticImageSearch(quant_filter(json!({}))),
            EntityType::File,
        )
        .await
        .expect("semantic image quant query");
    }

    #[tokio::test]
    async fn semantic_image_runs_full_query() {
        let mut filter: SemanticImageSearch = serde_json::from_value(json!({
            "image_embeddings": { "query": "hello", "model": "clip/test" }
        }))
        .expect("semantic image filter");
        filter.image_embeddings._embedding = Some(vec![0, 0, 0, 0]);
        filter.image_embeddings._distance_func_override = Some(DistanceFunction::Cosine);
        run_full_pql_query(QueryElement::SemanticImageSearch(filter), EntityType::File)
            .await
            .expect("semantic image query");
    }
}
