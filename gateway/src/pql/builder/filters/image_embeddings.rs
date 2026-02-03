use sea_query::{Alias, Cond, Expr, ExprTrait, Func, JoinType, Query};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{OrderDirection, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::super::{
    BaseTable, CteRef, Embeddings, ExtraColumn, ExtractedText, ItemData, Items, JoinedTables,
    OrderByFilter, QueryState, Setters, add_rank_column_expr, apply_group_by, apply_sort_bounds,
    get_std_group_by, wrap_query,
};
use super::FilterCompiler;
use super::embedding_types::{DistanceAggregation, DistanceFunction};
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SemanticImageSearch {
    #[serde(flatten, default = "default_sort_asc")]
    pub sort: SortableOptions,
    /// Search Image Embeddings
    ///
    /// Search for image using semantic search on image embeddings.
    pub image_embeddings: SemanticImageArgs,
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

impl FilterCompiler for SemanticImageSearch {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.image_embeddings;
        let embedding = args
            ._embedding
            .as_ref()
            .ok_or_else(|| PqlError::invalid("image_embeddings missing embedding bytes"))?;
        let cte_name = format!("n{}_SemanticImageSearch", state.cte_counter);

        let mut model_cond = Expr::col((Setters::Table, Setters::Name)).eq(args.model.clone());
        if args.clip_xmodal {
            model_cond = model_cond
                .or(Expr::col((Setters::Table, Setters::Name)).eq(format!("t{}", args.model)));
        }

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
            .add(model_cond);
        query.join(JoinType::InnerJoin, Setters::Table, setter_cond);
        query.join(
            JoinType::InnerJoin,
            Embeddings::Table,
            Expr::col((Embeddings::Table, Embeddings::Id)).equals((ItemData::Table, ItemData::Id)),
        );

        let src_setters = Alias::new("src_setters");
        let src_item_data = Alias::new("src_item_data");
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

            let mut join_text = false;
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

        if state.is_count_query {
            apply_group_by(&mut query, get_std_group_by(context, state));
            let mut joined_tables = JoinedTables::default();
            joined_tables.mark(BaseTable::Items);
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            if args.src_text.is_some() {
                joined_tables.mark(BaseTable::ExtractedText);
            }
            let cte = wrap_query(state, query, context, cte_name, &joined_tables);
            state.cte_counter += 1;
            return Ok(cte);
        }

        let distance_func = match args._distance_func_override {
            Some(DistanceFunction::L2) => "vec_distance_L2",
            _ => "vec_distance_cosine",
        };
        let vec_distance: Expr = Func::cust(distance_func)
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

        apply_group_by(&mut query, get_std_group_by(context, state));
        if !state.is_count_query {
            add_rank_column_expr(&mut query, &self.sort, rank_column)?;
        }

        let (query, context_for_wrap) =
            apply_sort_bounds(state, query, context.clone(), &cte_name, &self.sort);

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::Items);
        joined_tables.mark(BaseTable::ItemData);
        joined_tables.mark(BaseTable::Setters);
        if args.src_text.is_some() {
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
