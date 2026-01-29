use sea_query::{
    Alias, Expr, ExprTrait, Func, JoinType, Order, OverStatement, Query, WindowStatement,
};
use sea_query::extension::sqlite::SqliteBinOper;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::pql::model::{EntityType, SortableOptions};
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    BaseTable, CteRef, ExtraColumn, ExtractedText, ExtractedTextFts, ItemData, JoinedTables,
    OrderByFilter, QueryState, Setters, add_rank_column_expr, apply_group_by, apply_sort_bounds,
    create_cte, get_std_group_by, select_std_from_cte, wrap_query,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchTextArgs {
    /// Match
    ///
    /// The query to match against text
    pub r#match: String,
    /// Filter Only
    ///
    /// Only filter out text based on the other criteria,
    /// without actually matching the query.
    ///
    /// If set to True, the match field will be ignored.
    /// Order by, select_as, and row_n will also be ignored.
    ///
    /// If set to False (default), and the match field is empty,
    /// this filter will be skipped entirely.
    #[serde(default)]
    pub filter_only: bool,
    /// Include text from these setters
    ///
    /// Filter out text that is was not set by these setters.
    /// The setters are usually the names of the models that extracted or generated the text.
    /// For example, the OCR model, the Whisper STT model, the captioning model or the tagger model.
    #[serde(default)]
    pub setters: Vec<String>,
    /// Included languages
    ///
    /// Filter out text that is not in these languages
    #[serde(default)]
    pub languages: Vec<String>,
    /// Minimum Confidence for Language Detection
    ///
    /// Filter out text that has a language confidence score below this threshold.
    /// Must be a value between 0 and 1.
    /// Language confidence scores are usually set by the model that extracted the text.
    /// For tagging models, it's always 1.
    #[serde(default)]
    pub min_language_confidence: Option<f64>,
    /// Minimum Confidence for the text
    ///
    /// Filter out text that has a confidence score below this threshold.
    /// Must be a value between 0 and 1.
    /// Confidence scores are usually set by the model that extracted the text.
    #[serde(default)]
    pub min_confidence: Option<f64>,
    /// Allow raw FTS5 MATCH Syntax
    ///
    /// If set to False, the query will be escaped before being passed to the FTS5 MATCH function
    #[serde(default = "default_true")]
    pub raw_fts5_match: bool,
    /// Minimum Length
    ///
    /// Filter out text that is shorter than this. Inclusive.
    #[serde(default)]
    pub min_length: Option<i64>,
    /// Maximum Length
    ///
    /// Filter out text that is longer than this. Inclusive.
    #[serde(default)]
    pub max_length: Option<i64>,
    /// Return matching text snippet
    ///
    /// If set, the best matching text *snippet* will be included in the `extra` dict of each result under this key.
    /// Works with any type of query, but it's best used with text-* queries.
    ///
    /// Otherwise, it's somewhat slow because of the contortions needed to get the best snippet per file.
    #[serde(default)]
    pub select_snippet_as: Option<String>,
    /// Maximum Snippet Length
    ///
    /// The maximum length (in tokens) of the snippet returned by select_snippet_as
    #[serde(default = "default_snippet_max_len")]
    pub s_max_len: i64,
    /// Snippet Ellipsis
    ///
    /// The ellipsis to use when truncating the snippet
    #[serde(default = "default_snippet_ellipsis")]
    pub s_ellipsis: String,
    /// Snippet Start Tag
    ///
    /// The tag to use at the beginning of the snippet
    #[serde(default = "default_snippet_start_tag")]
    pub s_start_tag: String,
    /// Snippet End Tag
    ///
    /// The tag to use at the end of the snippet
    #[serde(default = "default_snippet_end_tag")]
    pub s_end_tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchText {
    #[serde(flatten, default)]
    pub sort: SortableOptions,
    /// Match Extracted Text
    ///
    /// Match a query against text extracted from files or associated with them,
    /// including tags and OCR text
    pub match_text: MatchTextArgs,
}

fn default_true() -> bool {
    true
}

fn default_snippet_max_len() -> i64 {
    30
}

fn default_snippet_ellipsis() -> String {
    "...".to_string()
}

fn default_snippet_start_tag() -> String {
    "<b>".to_string()
}

fn default_snippet_end_tag() -> String {
    "</b>".to_string()
}

impl FilterCompiler for MatchText {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let args = &self.match_text;
        let cte_name = format!("n{}_MatchText", state.cte_counter);
        let want_snippet = args.select_snippet_as.is_some() && !state.is_count_query;

        let mut criteria = Vec::new();
        if !args.filter_only {
            criteria.push(
                Expr::col((ExtractedTextFts::Table, ExtractedTextFts::Text)).binary(
                    SqliteBinOper::Match,
                    Expr::val(args.r#match.clone()),
                ),
            );
        }
        if let Some(min_length) = args.min_length {
            if min_length > 0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::TextLength)).gte(min_length),
                );
            }
        }
        if let Some(max_length) = args.max_length {
            if max_length > 0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::TextLength)).lte(max_length),
                );
            }
        }
        if !args.setters.is_empty() {
            let setters = args.setters.iter().cloned().map(Expr::val).collect::<Vec<_>>();
            criteria.push(Expr::col((Setters::Table, Setters::Name)).is_in(setters));
        }
        if !args.languages.is_empty() {
            let languages = args
                .languages
                .iter()
                .cloned()
                .map(Expr::val)
                .collect::<Vec<_>>();
            criteria.push(Expr::col((ExtractedText::Table, ExtractedText::Language)).is_in(languages));
        }
        if let Some(min_language_confidence) = args.min_language_confidence {
            if min_language_confidence > 0.0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
                        .gte(min_language_confidence),
                );
            }
        }
        if let Some(min_confidence) = args.min_confidence {
            if min_confidence > 0.0 {
                criteria.push(
                    Expr::col((ExtractedText::Table, ExtractedText::Confidence))
                        .gte(min_confidence),
                );
            }
        }

        let snippet_expr: Expr = Func::cust("snippet")
            .args([
                Expr::cust("extracted_text_fts"),
                Expr::val(-1),
                Expr::val(args.s_start_tag.clone()),
                Expr::val(args.s_end_tag.clone()),
                Expr::val(args.s_ellipsis.clone()),
                Expr::val(args.s_max_len),
            ])
            .into();

        if !(state.item_data_query && matches!(state.entity, EntityType::Text)) {
            let mut query = select_std_from_cte(context, state);
            query.join(
                JoinType::InnerJoin,
                ItemData::Table,
                Expr::col((ItemData::Table, ItemData::ItemId))
                    .equals(context.column_ref("item_id")),
            );
            query.join(
                JoinType::InnerJoin,
                Setters::Table,
                Expr::col((Setters::Table, Setters::Id))
                    .equals((ItemData::Table, ItemData::SetterId)),
            );
            query.join(
                JoinType::InnerJoin,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals((ItemData::Table, ItemData::Id)),
            );
            query.join(
                JoinType::InnerJoin,
                ExtractedTextFts::Table,
                Expr::cust("extracted_text_fts.rowid")
                    .equals((ExtractedText::Table, ExtractedText::Id)),
            );
            for condition in criteria {
                query.and_where(condition);
            }

            let mut context_for_wrap = context.clone();
            let mut final_query = query;

            if want_snippet {
                final_query.expr_as(snippet_expr, Alias::new("snip"));
                final_query.expr_as(Expr::cust("rank"), Alias::new("rank"));

                let match_cte =
                    create_cte(state, format!("matchq_{cte_name}"), final_query.to_owned());
                let mut rownum_query = Query::select();
                rownum_query
                    .from(Alias::new(match_cte.name.as_str()))
                    .column((Alias::new(match_cte.name.as_str()), sea_query::Asterisk));
                let mut window = WindowStatement::new();
                window.partition_by(match_cte.column_ref("file_id"));
                window.order_by_expr(match_cte.column_expr("rank"), Order::Asc);
                rownum_query.expr_window_as(Expr::cust("row_number()"), window, Alias::new("rn"));
                let rownum_cte =
                    create_cte(state, format!("rownum_{cte_name}"), rownum_query.to_owned());

                let mut select_query = Query::select();
                select_query
                    .from(Alias::new(rownum_cte.name.as_str()))
                    .column((Alias::new(rownum_cte.name.as_str()), sea_query::Asterisk))
                    .and_where(
                        Expr::col((Alias::new(rownum_cte.name.as_str()), Alias::new("rn"))).eq(1),
                    );
                final_query = select_query;
                context_for_wrap = rownum_cte;

                if !state.is_count_query {
                    add_rank_column_expr(&mut final_query, &self.sort, Expr::cust("rank"))?;
                }
            } else {
                apply_group_by(&mut final_query, get_std_group_by(context, state));
                if !state.is_count_query {
                    let rank_expr = if args.filter_only {
                        Expr::val(1)
                    } else {
                        Func::min(Expr::cust("rank")).into()
                    };
                    add_rank_column_expr(&mut final_query, &self.sort, rank_expr)?;
                }
            }

            let (final_query, context_for_wrap) =
                apply_sort_bounds(state, final_query, context_for_wrap, &cte_name, &self.sort);

            let mut joined_tables = JoinedTables::default();
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            joined_tables.mark(BaseTable::ExtractedText);
            let cte = wrap_query(state, final_query, &context_for_wrap, cte_name, &joined_tables);
            state.cte_counter += 1;
            if !state.is_count_query {
                if let Some(alias) = &self.sort.select_as {
                    state.extra_columns.push(ExtraColumn {
                        column: "order_rank".to_string(),
                        cte: cte.clone(),
                        alias: alias.clone(),
                    });
                }
                if let Some(alias) = &args.select_snippet_as {
                    state.extra_columns.push(ExtraColumn {
                        column: "snip".to_string(),
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
        query.join(
            JoinType::InnerJoin,
            ExtractedText::Table,
            Expr::col((ExtractedText::Table, ExtractedText::Id))
                .equals(context.column_ref("data_id")),
        );
        query.join(
            JoinType::InnerJoin,
            ItemData::Table,
            Expr::col((ItemData::Table, ItemData::Id)).equals(context.column_ref("data_id")),
        );
        query.join(
            JoinType::InnerJoin,
            Setters::Table,
            Expr::col((Setters::Table, Setters::Id))
                .equals((ItemData::Table, ItemData::SetterId)),
        );
        query.join(
            JoinType::InnerJoin,
            ExtractedTextFts::Table,
            Expr::cust("extracted_text_fts.rowid")
                .equals((ExtractedText::Table, ExtractedText::Id)),
        );
        for condition in criteria {
            query.and_where(condition);
        }

        let mut context_for_wrap = context.clone();
        let mut final_query = query;

        if want_snippet {
            final_query.expr_as(snippet_expr, Alias::new("snip"));
            final_query.expr_as(Expr::cust("rank"), Alias::new("rank"));

            let match_cte = create_cte(state, format!("matchq_{cte_name}"), final_query.to_owned());
            context_for_wrap = match_cte.clone();
            let mut select_query = Query::select();
            select_query
                .from(Alias::new(match_cte.name.as_str()))
                .column((Alias::new(match_cte.name.as_str()), sea_query::Asterisk));
            final_query = select_query;
        }

        if !state.is_count_query {
            let rank_expr = if args.filter_only {
                Expr::val(1)
            } else {
                Expr::cust("rank")
            };
            add_rank_column_expr(&mut final_query, &self.sort, rank_expr)?;
        }

        let (final_query, context_for_wrap) =
            apply_sort_bounds(state, final_query, context_for_wrap, &cte_name, &self.sort);

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::ItemData);
        joined_tables.mark(BaseTable::Setters);
        joined_tables.mark(BaseTable::ExtractedText);
        let cte = wrap_query(state, final_query, &context_for_wrap, cte_name, &joined_tables);
        state.cte_counter += 1;
        if !state.is_count_query {
            if let Some(alias) = &self.sort.select_as {
                state.extra_columns.push(ExtraColumn {
                    column: "order_rank".to_string(),
                    cte: cte.clone(),
                    alias: alias.clone(),
                });
            }
            if let Some(alias) = &args.select_snippet_as {
                state.extra_columns.push(ExtraColumn {
                    column: "snip".to_string(),
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
    fn match_text_builds_sql() {
        let filter: MatchText = serde_json::from_value(json!({
            "match_text": { "match": "hello world" }
        }))
        .expect("match_text filter");
        let mut state = build_base_state(EntityType::Text, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("extracted_text_fts"));
        assert!(sql.contains("SELECT"));
    }

    #[tokio::test]
    async fn match_text_runs_full_query() {
        let filter: MatchText = serde_json::from_value(json!({
            "match_text": { "match": "hello world" }
        }))
        .expect("match_text filter");
        run_full_pql_query(QueryElement::MatchText(filter), EntityType::Text)
            .await
            .expect("match_text query");
    }
}
