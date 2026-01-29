use sea_query::{
    Alias, Expr, ExprTrait, Func, JoinType, Order, OverStatement, Query, WindowStatement,
};
use sea_query::extension::sqlite::SqliteBinOper;

use crate::pql::model::{EntityType, MatchText};
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    CteRef, ExtraColumn, ExtractedText, ExtractedTextFts, ItemData, OrderByFilter, QueryState,
    Setters, add_rank_column_expr, apply_group_by, apply_sort_bounds, create_cte, get_std_group_by,
    select_std_from_cte, wrap_query,
};

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

            let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
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

        let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
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
