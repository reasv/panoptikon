use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType, Query};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{OrderDirection, PartialSortableOptions, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, EmbeddingQuants, Embeddings, ExtraColumn, ExtractedText, ItemData, Items,
    JoinedTables, OrderByFilter, QueryState, Setters, add_rank_column_expr, apply_group_by,
    apply_sort_bounds, create_cte, get_std_group_by, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::{
    DistanceAggregation, DistanceFunction, IndexMode, QuantResolved, default_k,
};
use super::quant::{COARSE_DIST, COARSE_RANK, EXACT_DIST, assemble_two_stage};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SourceArgs {
    /// Include text from these setters
    ///
    /// Filter out text that is was not set by these setters.
    /// The setters are usually the names of the models that extracted or generated the text.
    /// For example, the OCR model, the Whisper STT model, the captioning model or the tagger model.
    #[serde(default)]
    pub setters: Vec<String>,
    /// The source languages to restrict the search to.
    ///
    /// These are the languages of the text produced by the source models.
    #[serde(default)]
    pub languages: Option<Vec<String>>,
    /// Minimum Confidence for the text
    ///
    /// Filter out text that has a confidence score below this threshold.
    /// Usually a value between 0 and 1.
    /// Confidence scores are usually set by the model that extracted the text.
    #[serde(default)]
    pub min_confidence: Option<f64>,
    /// Filter out text that has a language confidence score below this threshold.
    ///
    /// Usually a value between 0 and 1.
    /// Language confidence scores are usually set by the model that extracted the text.
    /// For tagging models, it's always 1.
    #[serde(default)]
    pub min_language_confidence: f64,
    /// Filter out text that is shorter than this. Inclusive.
    #[serde(default)]
    pub min_length: i64,
    /// Maximum Length
    ///
    /// Filter out text that is longer than this. Inclusive.
    #[serde(default)]
    pub max_length: Option<i64>,
    /// The weight to apply to the confidence of the source text
    /// on the embedding distance aggregation for individual items with multiple embeddings.
    /// Default is 0.0, which means that the confidence of the source text
    /// does not affect the distance aggregation.
    /// This parameter is only relevant when the source text has a confidence value.
    /// The confidence of the source text is multiplied by the confidence of the other
    /// source text when calculating the distance between two items.
    /// The formula for the distance calculation is as follows:
    /// ```
    /// weights = POW(COALESCE(text.confidence, 1)), src_confidence_weight)
    /// distance = SUM(distance * weights) / SUM(weights)
    /// ```
    /// So this weight is the exponent to which the confidence is raised, which means that it can be greater than 1.
    /// When confidence weights are set, the distance_aggregation setting is ignored.
    #[serde(default)]
    pub confidence_weight: f64,
    /// The weight to apply to the confidence of the source text language
    /// on the embedding distance aggregation.
    /// Default is 0.0, which means that the confidence of the source text language detection
    /// does not affect the distance calculation.
    /// Totally analogous to `src_confidence_weight`, but for the language confidence.
    /// When both are present, the results of the POW() functions for both are multiplied together before being applied to the distance.
    /// ```
    /// weights = POW(..., src_confidence_weight) * POW(..., src_language_confidence_weight)
    /// ```
    #[serde(default)]
    pub language_confidence_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SimilarityArgs {
    /// Sha256 hash of the target item to find similar items for
    pub target: String,
    /// The name of the embedding model used for similarity search
    pub model: String,
    /// The distance function to use for similarity search. Default is L2.
    #[serde(default)]
    pub distance_function: DistanceFunction,
    /// Force the use of the distance function specified in the `distance_function` field.
    /// If set to True, the distance function specified in the `distance_function` field will be used,
    /// even if the model used for similarity search has a different distance function override specified in its config.
    #[serde(default)]
    pub force_distance_function: Option<bool>,
    /// The method to aggregate distances when an item has multiple embeddings. Default is AVG.
    #[serde(default = "default_distance_aggregation")]
    pub distance_aggregation: DistanceAggregation,
    /// Filters and options to apply on source text.
    /// If not provided, all text embeddings are considered.
    /// The source text is the text which was used to produce the text embeddings.
    #[serde(default)]
    pub src_text: Option<SourceArgs>,
    /// Whether to use cross-modal similarity for CLIP models.
    /// Default is False. What this means is that the similarity is calculated between image and text embeddings,
    /// rather than just between image embeddings. By default will also use text-to-text similarity.
    ///
    /// Note that you must have both image and text embeddings with the same CLIP model for this setting to work.
    /// Text embeddings are derived from text which must have been already previously produced by another model, such as an OCR model or a tagger.
    /// They are generated *separately* from the image embeddings, using a different job (Under 'CLIP Text Embeddings').
    /// Run a batch job with the same clip model for both image and text embeddings to use this setting.
    #[serde(default)]
    pub clip_xmodal: bool,
    /// When using CLIP cross-modal similarity, whether to use text-to-text similarity as well or just image-to-text and image-to-image.
    #[serde(default = "default_true")]
    pub xmodal_t2t: bool,
    /// When using CLIP cross-modal similarity, whether to use image-to-image similarity as well or just image-to-text and text-to-text.
    #[serde(default = "default_true")]
    pub xmodal_i2i: bool,
    /// Index mode: `auto` (default) uses the default quant profile where its
    /// coverage is ready for this model, else exact; `exact` always
    /// brute-forces full-precision vectors; `quant` demands a quant profile
    /// and errors when it isn't ready. `ann` is reserved.
    ///
    /// Under a quant profile both sides of the similarity self-join use
    /// binary quants for the coarse pass, and `order_rank` is a rank, not a
    /// raw distance.
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
pub(crate) struct SimilarTo {
    #[serde(flatten)]
    pub sort: SortableOptions,
    /// Item Similarity Search
    ///
    /// Search for items similar to a target item using similarity search on embeddings.
    /// The search is based on the image or text embeddings of the provided item.
    ///
    /// The setter name refers to the model that produced the embeddings.
    /// You can find a list of available values for this parameter using the /api/search/stats endpoint.
    /// Any setters of type "text-embedding" or "clip" can be used for this search.
    ///
    /// "text" embeddings are derived from text produced by another model, such as an OCR model or a tagger.
    /// You can restrict the search to embeddings derived from text that was
    /// produced by one of a list of specific models by providing the appropriate filter.
    /// You can find a list of available values for text sources using the
    /// /api/search/stats endpoint, specifically any setter of type "text" will apply.
    /// Remember that tagging models also produce text by concatenating the tags,
    ///  and are therefore also returned as "text" models by the stats endpoint.
    /// Restricting similarity to a tagger model or a set of tagger models
    ///  is recommended for item similarity search based on text embeddings.
    pub similar_to: SimilarityArgs,
}

// Manual impl because serde ignores `default = ...` on flattened fields;
// this filter orders results by distance (ascending, best matches first)
// by default.
impl<'de> serde::Deserialize<'de> for SimilarTo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Repr {
            #[serde(flatten)]
            sort: PartialSortableOptions,
            similar_to: SimilarityArgs,
        }
        let repr = Repr::deserialize(deserializer)?;
        Ok(Self {
            sort: repr.sort.resolve(default_sort_asc()),
            similar_to: repr.similar_to,
        })
    }
}

fn default_true() -> bool {
    true
}

fn default_distance_aggregation() -> DistanceAggregation {
    DistanceAggregation::Avg
}

fn default_sort_asc() -> SortableOptions {
    let mut options = SortableOptions::default();
    options.order_by = true;
    options.direction = OrderDirection::Asc;
    options.row_n_direction = OrderDirection::Asc;
    options
}

/// Which vector payload the unqualified-embeddings CTE carries.
enum SimVectorJoin {
    Embeddings,
    Quants { profile_id: i64 },
}

impl SimilarTo {
    /// The shared candidate skeleton: model/setter joins, src_text joins and
    /// filters, context left-join. Both the exact and the coarse
    /// vector-collection CTEs build on this, so membership is identical.
    fn base_skeleton(&self, context: &CteRef) -> sea_query::SelectStatement {
        let args = &self.similar_to;
        let mut model_cond = Expr::col((Setters::Table, Setters::Name)).eq(args.model.clone());
        if args.clip_xmodal {
            model_cond = model_cond
                .or(Expr::col((Setters::Table, Setters::Name)).eq(format!("t{}", args.model)));
        }

        let mut base_query = Query::select();
        base_query.from(Items::Table);
        base_query.join(
            JoinType::InnerJoin,
            ItemData::Table,
            Expr::col((ItemData::Table, ItemData::ItemId)).equals((Items::Table, Items::Id)),
        );
        let model_join = Cond::all()
            .add(
                Expr::col((Setters::Table, Setters::Id))
                    .equals((ItemData::Table, ItemData::SetterId)),
            )
            .add(model_cond);
        base_query.join(JoinType::InnerJoin, Setters::Table, model_join);

        let src_setters = Alias::new("src_setters");
        let src_item_data = Alias::new("src_item_data");
        if let Some(src_args) = &args.src_text {
            let join_type = if args.clip_xmodal {
                JoinType::LeftJoin
            } else {
                JoinType::InnerJoin
            };
            base_query.join(
                join_type,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals((ItemData::Table, ItemData::SourceId)),
            );
            base_query.join_as(
                join_type,
                ItemData::Table,
                src_item_data.clone(),
                Expr::col((src_item_data.clone(), ItemData::Id))
                    .equals((ExtractedText::Table, ExtractedText::Id)),
            );
            if !src_args.setters.is_empty() {
                base_query.join_as(
                    join_type,
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
                    let values = languages.iter().cloned().map(Expr::val).collect::<Vec<_>>();
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Language)).is_in(values),
                    );
                }
            }
            if let Some(min_confidence) = src_args.min_confidence {
                if min_confidence > 0.0 {
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::Confidence))
                            .gte(min_confidence),
                    );
                }
            }
            if src_args.min_language_confidence > 0.0 {
                conditions.push(
                    Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
                        .gte(src_args.min_language_confidence),
                );
            }
            if src_args.min_length > 0 {
                conditions.push(
                    Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                        .gte(src_args.min_length),
                );
            }
            if let Some(max_length) = src_args.max_length {
                if max_length > 0 {
                    conditions.push(
                        Expr::col((ExtractedText::Table, ExtractedText::TextLength))
                            .lte(max_length),
                    );
                }
            }

            if args.clip_xmodal {
                let mut cond = Cond::any();
                cond = cond.add(Expr::col((ExtractedText::Table, ExtractedText::Id)).is_null());
                let mut and_cond = Cond::all();
                if conditions.is_empty() {
                    and_cond = and_cond.add(Expr::cust("1=1"));
                } else {
                    for condition in conditions {
                        and_cond = and_cond.add(condition);
                    }
                }
                cond = cond.add(and_cond);
                base_query.and_where(cond.into());
            } else if !conditions.is_empty() {
                let mut cond = Cond::all();
                for condition in conditions {
                    cond = cond.add(condition);
                }
                base_query.and_where(cond.into());
            }
        }

        base_query.join(
            JoinType::LeftJoin,
            Alias::new(context.name.as_str()),
            Expr::col(context.column_ref("item_id")).equals((Items::Table, Items::Id)),
        );

        base_query
    }

    /// The per-vector collection select (both the target's and the
    /// candidates' vectors): standard columns, sha256, data_type and the
    /// vector payload.
    fn vector_collection(
        &self,
        context: &CteRef,
        state: &QueryState,
        join: &SimVectorJoin,
    ) -> sea_query::SelectStatement {
        let args = &self.similar_to;
        let mut query = self.base_skeleton(context);
        match join {
            SimVectorJoin::Embeddings => {
                query.join(
                    JoinType::InnerJoin,
                    Embeddings::Table,
                    Expr::col((Embeddings::Table, Embeddings::Id))
                        .equals((ItemData::Table, ItemData::Id)),
                );
                query.expr_as(
                    Expr::col((Embeddings::Table, Embeddings::Embedding)),
                    Alias::new("embedding"),
                );
            }
            SimVectorJoin::Quants { profile_id } => {
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
                query.expr_as(
                    Expr::col((EmbeddingQuants::Table, EmbeddingQuants::Quant)),
                    Alias::new("embedding"),
                );
            }
        }

        query.expr_as(context.column_expr("item_id"), Alias::new("item_id"));
        query.expr_as(context.column_expr("file_id"), Alias::new("file_id"));
        if state.item_data_query {
            query.expr_as(context.column_expr("data_id"), Alias::new("data_id"));
        }
        query.expr_as(
            Expr::col((Items::Table, Items::Sha256)),
            Alias::new("sha256"),
        );
        query.expr_as(
            Expr::col((ItemData::Table, ItemData::DataType)),
            Alias::new("data_type"),
        );
        if matches!(join, SimVectorJoin::Embeddings) {
            if let Some(src_text) = &args.src_text {
                if src_text.confidence_weight != 0.0 {
                    query.expr_as(
                        Expr::col((ExtractedText::Table, ExtractedText::Confidence)),
                        Alias::new("confidence"),
                    );
                }
                if src_text.language_confidence_weight != 0.0 {
                    query.expr_as(
                        Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence)),
                        Alias::new("language_confidence"),
                    );
                }
            }
        }

        let target_cond = Expr::col((Items::Table, Items::Sha256)).eq(args.target.clone());
        let context_cond = Expr::col(context.column_ref("item_id")).is_not_null();
        query.and_where(context_cond.or(target_cond));
        query
    }

    /// The self-join distance select over a vector collection CTE. The rank
    /// aggregate comes from the caller (exact weighted vs coarse Hamming);
    /// `None` leaves the rank column to the caller (the exact path
    /// materializes it through the row_n machinery).
    fn distance_select(
        &self,
        state: &QueryState,
        collection_cte: &CteRef,
        rank: Option<(Expr, &str)>,
    ) -> (sea_query::SelectStatement, CteRef) {
        let args = &self.similar_to;
        let other_alias = Alias::new("other_embeddings");
        let main_alias = Alias::new("main_embeddings");
        let other_ctx = CteRef {
            name: "other_embeddings".to_string(),
        };
        let mut select = Query::select();
        select.from_as(Alias::new(collection_cte.name.as_str()), other_alias.clone());
        select.join_as(
            JoinType::InnerJoin,
            Alias::new(collection_cte.name.as_str()),
            main_alias.clone(),
            Expr::col((main_alias.clone(), Alias::new("sha256"))).eq(args.target.clone()),
        );

        select.expr_as(
            Expr::col((other_alias.clone(), Alias::new("item_id"))),
            Alias::new("item_id"),
        );
        select.expr_as(
            Expr::col((other_alias.clone(), Alias::new("file_id"))),
            Alias::new("file_id"),
        );
        if state.item_data_query {
            select.expr_as(
                Expr::col((other_alias.clone(), Alias::new("data_id"))),
                Alias::new("data_id"),
            );
        }

        select.and_where(
            Expr::col((other_alias.clone(), Alias::new("sha256"))).ne(args.target.clone()),
        );
        apply_group_by(&mut select, get_std_group_by(&other_ctx, state));

        if args.clip_xmodal {
            if !args.xmodal_i2i {
                select.and_where(
                    Expr::col((main_alias.clone(), Alias::new("data_type")))
                        .ne("clip")
                        .or(Expr::col((other_alias.clone(), Alias::new("data_type"))).ne("clip")),
                );
            }
            if !args.xmodal_t2t {
                select.and_where(
                    Expr::col((main_alias.clone(), Alias::new("data_type")))
                        .ne("text-embedding")
                        .or(Expr::col((other_alias.clone(), Alias::new("data_type")))
                            .ne("text-embedding")),
                );
            }
        }

        if let Some((rank_column, rank_alias)) = rank {
            select.expr_as(rank_column, Alias::new(rank_alias));
        }
        (select, other_ctx)
    }

    /// The full-precision self-join rank aggregate, including confidence
    /// weighting.
    fn exact_rank_column(&self) -> Expr {
        let args = &self.similar_to;
        let other_alias = Alias::new("other_embeddings");
        let main_alias = Alias::new("main_embeddings");
        let distance_func = match args.distance_function {
            DistanceFunction::L2 => "vec_distance_L2",
            DistanceFunction::Cosine => "vec_distance_cosine",
        };
        let vec_distance: Expr = Func::cust(distance_func)
            .args([
                Expr::col((main_alias.clone(), Alias::new("embedding"))),
                Expr::col((other_alias.clone(), Alias::new("embedding"))),
            ])
            .into();
        let mut rank_column = match args.distance_aggregation {
            DistanceAggregation::Max => vec_distance.clone().max(),
            DistanceAggregation::Avg => vec_distance.clone().avg(),
            DistanceAggregation::Min => vec_distance.clone().min(),
        };

        if let Some(src_text) = &args.src_text {
            let mut conf_weight_clause = Expr::val(1);
            let mut lang_conf_weight_clause = Expr::val(1);
            if src_text.confidence_weight != 0.0 {
                let conf_mul = Func::coalesce([
                    Expr::col((main_alias.clone(), Alias::new("confidence"))),
                    Expr::val(1),
                ])
                .mul(Func::coalesce([
                    Expr::col((other_alias.clone(), Alias::new("confidence"))),
                    Expr::val(1),
                ]));
                conf_weight_clause = Func::cust("pow")
                    .args([conf_mul, Expr::val(src_text.confidence_weight)])
                    .into();
            }
            if src_text.language_confidence_weight != 0.0 {
                let lang_mul = Func::coalesce([
                    Expr::col((other_alias.clone(), Alias::new("language_confidence"))),
                    Expr::val(1),
                ])
                .mul(Func::coalesce([
                    Expr::col((main_alias.clone(), Alias::new("language_confidence"))),
                    Expr::val(1),
                ]));
                lang_conf_weight_clause = Func::cust("pow")
                    .args([lang_mul, Expr::val(src_text.language_confidence_weight)])
                    .into();
            }
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

    /// The weight-free coarse proxy: aggregated Hamming distance between
    /// both sides' binary quants. Stored quants are plain BLOBs, which
    /// sqlite-vec would read as float32 — vec_bit marks them as bit vectors.
    fn coarse_rank_column(&self) -> Expr {
        let other_alias = Alias::new("other_embeddings");
        let main_alias = Alias::new("main_embeddings");
        let hamming: Expr = Func::cust("vec_distance_hamming")
            .args([
                Func::cust("vec_bit")
                    .arg(Expr::col((main_alias.clone(), Alias::new("embedding"))))
                    .into(),
                Func::cust("vec_bit")
                    .arg(Expr::col((other_alias.clone(), Alias::new("embedding"))))
                    .into(),
            ])
            .into();
        match self.similar_to.distance_aggregation {
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

impl FilterCompiler for SimilarTo {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.similar_to;
        let cte_name = format!("n{}_SimilarTo", state.cte_counter);

        if state.is_count_query {
            // Membership only — identical in every index mode, so counts
            // never consult quants.
            let mut count_query = self.base_skeleton(context);
            count_query.join(
                JoinType::InnerJoin,
                Embeddings::Table,
                Expr::col((Embeddings::Table, Embeddings::Id))
                    .equals((ItemData::Table, ItemData::Id)),
            );
            count_query.expr_as(context.column_expr("item_id"), Alias::new("item_id"));
            count_query.expr_as(context.column_expr("file_id"), Alias::new("file_id"));
            if state.item_data_query {
                count_query.expr_as(context.column_expr("data_id"), Alias::new("data_id"));
            }
            count_query.and_where(Expr::col(context.column_ref("item_id")).is_not_null());
            count_query.and_where(Expr::col((Items::Table, Items::Sha256)).ne(args.target.clone()));
            apply_group_by(&mut count_query, get_std_group_by(context, state));

            let mut joined_tables = JoinedTables::default();
            joined_tables.mark(BaseTable::Items);
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            if args.src_text.is_some() {
                joined_tables.mark(BaseTable::ExtractedText);
            }
            let cte = wrap_query(state, count_query, context, cte_name, &joined_tables);
            state.cte_counter += 1;
            return Ok(cte);
        }

        if let Some(quant) = &args._quant {
            let coarse_collection = self.vector_collection(
                context,
                state,
                &SimVectorJoin::Quants {
                    profile_id: quant.profile_id,
                },
            );
            let unqquant_cte = create_cte(
                state,
                format!("unqquant_{cte_name}"),
                coarse_collection,
            );
            let (coarse, _) = self.distance_select(
                state,
                &unqquant_cte,
                Some((self.coarse_rank_column(), COARSE_DIST)),
            );

            let k = args.k;
            let (merge, merge_context) =
                assemble_two_stage(state, &cte_name, coarse, &self.sort, |state, ranked| {
                let exact_collection =
                    self.vector_collection(context, state, &SimVectorJoin::Embeddings);
                let unqemb_cte = create_cte(
                    state,
                    format!("unqemb_{cte_name}"),
                    exact_collection,
                );
                let (mut head, _) = self.distance_select(
                    state,
                    &unqemb_cte,
                    Some((self.exact_rank_column(), EXACT_DIST)),
                );
                let other_alias = Alias::new("other_embeddings");
                let ranked_alias = Alias::new(ranked.name.as_str());
                let mut join_cond = Expr::col((ranked_alias.clone(), Alias::new("file_id")))
                    .equals((other_alias.clone(), Alias::new("file_id")));
                if state.item_data_query {
                    join_cond = join_cond.and(
                        Expr::col((ranked_alias.clone(), Alias::new("data_id")))
                            .equals((other_alias.clone(), Alias::new("data_id"))),
                    );
                }
                head.join(JoinType::InnerJoin, ranked_alias.clone(), join_cond);
                head.and_where(Expr::col(ranked.column_ref(COARSE_RANK)).lte(k));
                head
            });

            // The merge selects only from CTEs, so no base tables are
            // visible to the final query; its context is the ranked CTE in
            // its FROM scope.
            let (merge, context_for_wrap, joined_tables) = apply_sort_bounds(
                state,
                merge,
                merge_context,
                &cte_name,
                &self.sort,
                JoinedTables::default(),
            );
            let cte = wrap_query(state, merge, &context_for_wrap, cte_name, &joined_tables);
            state.cte_counter += 1;
            self.register_outputs(state, &cte);
            return Ok(cte);
        }

        let exact_collection = self.vector_collection(context, state, &SimVectorJoin::Embeddings);
        let unqemb_cte = create_cte(
            state,
            format!("unqemb_{cte_name}"),
            exact_collection,
        );
        let (mut distance_select, other_ctx) = self.distance_select(state, &unqemb_cte, None);
        add_rank_column_expr(&mut distance_select, &self.sort, self.exact_rank_column())?;

        let (distance_select, context_for_wrap, joined_tables) = apply_sort_bounds(
            state,
            distance_select,
            other_ctx.clone(),
            &cte_name,
            &self.sort,
            JoinedTables::default(),
        );

        let cte = wrap_query(
            state,
            distance_select,
            &context_for_wrap,
            cte_name,
            &joined_tables,
        );
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
    fn similar_to_defaults_to_order_by_distance() {
        use crate::pql::model::OrderDirection;
        let filter: SimilarTo = serde_json::from_value(json!({
            "similar_to": { "target": "abc", "model": "clip/test" }
        }))
        .expect("similar_to filter");
        assert!(filter.sort.order_by);
        assert!(matches!(filter.sort.direction, OrderDirection::Asc));

        let filter: SimilarTo = serde_json::from_value(json!({
            "similar_to": { "target": "abc", "model": "clip/test" },
            "order_by": false
        }))
        .expect("similar_to filter");
        assert!(!filter.sort.order_by);
    }

    #[test]
    fn similar_to_builds_sql() {
        let filter: SimilarTo = serde_json::from_value(json!({
            "similar_to": { "target": "abc", "model": "clip/test", "force_distance_function": true }
        }))
        .expect("similar_to filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("embeddings"));
    }

    #[tokio::test]
    async fn similar_to_runs_full_query() {
        let filter: SimilarTo = serde_json::from_value(json!({
            "similar_to": { "target": "abc", "model": "clip/test", "force_distance_function": true }
        }))
        .expect("similar_to filter");
        run_full_pql_query(QueryElement::SimilarTo(filter), EntityType::File)
            .await
            .expect("similar_to query");
    }
}
