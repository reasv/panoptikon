//! Two-stage quant scorer assembly (docs/vector-index-design.md).
//!
//! Every vector filter compiles to the same shape under a quant profile:
//!
//! 1. **coarse**: the filter's own candidate joins scored by aggregated
//!    Hamming distance over binary quants — full membership, plain SQL.
//! 2. **ranked**: a complete deterministic coarse ordering
//!    (`ROW_NUMBER() OVER (ORDER BY cdist, item_id)`).
//! 3. **head**: the coarse-top-`k` groups re-scored with the filter's real
//!    full-precision aggregate (including confidence weighting).
//! 4. **merge**: head by exact distance, then tail by coarse distance,
//!    `item_id` tiebreaker throughout — materialized as a single monotone
//!    `order_rank` row number so RRF and gt/lt sort bounds see exactly what
//!    they see today. (Under a quant profile `order_rank` is therefore a
//!    rank, not a raw distance, regardless of `row_n`.)
//!
//! Nothing is truncated: membership, counts, and downstream composition are
//! untouched. If the candidate set is ≤ k items the head covers everything
//! and the result is bit-identical to exact search.

use sea_query::{
    Alias, Asterisk, Expr, ExprTrait, JoinType, NullOrdering, Order, Query, SelectStatement,
    WindowStatement,
};

use crate::pql::model::SortableOptions;

use super::super::{CteRef, QueryState, create_cte, direction_to_order};

/// Column alias for the coarse Hamming aggregate in the coarse CTE.
pub(super) const COARSE_DIST: &str = "cdist";
/// Column alias for the coarse rank in the ranked CTE.
pub(super) const COARSE_RANK: &str = "crank";
/// Column alias for the exact aggregate in the head CTE.
pub(super) const EXACT_DIST: &str = "edist";

/// Assembles the coarse → ranked → head → merge CTE chain. `coarse` must be
/// a grouped select producing the standard columns plus `cdist`;
/// `build_head` receives the ranked CTE (standard columns + `cdist` +
/// `crank`) to use as its candidate context and must produce a grouped
/// select with the standard columns plus `edist`, restricted to
/// `crank <= k`. Returns the merge select (standard columns + `order_rank`)
/// plus the ranked CTE ref, which is the merge's context (the final
/// assembly joins base tables against the context's columns, so it must be
/// a table in the merge's FROM scope) — pass it to `apply_sort_bounds` +
/// `wrap_query`.
pub(super) fn assemble_two_stage<F>(
    state: &mut QueryState,
    cte_name: &str,
    coarse: SelectStatement,
    sort: &SortableOptions,
    build_head: F,
) -> (SelectStatement, CteRef)
where
    F: FnOnce(&mut QueryState, &CteRef) -> SelectStatement,
{
    let direction = direction_to_order(sort.row_n_direction);

    let coarse_cte = create_cte(state, format!("coarse_{cte_name}"), coarse);

    let mut ranked = Query::select();
    ranked
        .from(Alias::new(coarse_cte.name.as_str()))
        .column((Alias::new(coarse_cte.name.as_str()), Asterisk));
    // The tiebreaker must be a TOTAL key over the grouped rows, or
    // row_number() assignment between tied rows is engine-defined and the
    // "deterministic function of (query, DB state, k)" contract rests on
    // luck: one item can own several files (and, in item_data queries,
    // several data rows), so item_id alone leaves genuine ties.
    let mut window = WindowStatement::new();
    window.order_by_expr(Expr::col(Alias::new(COARSE_DIST)).into(), direction.clone());
    window.order_by_expr(Expr::col(Alias::new("item_id")).into(), Order::Asc);
    window.order_by_expr(Expr::col(Alias::new("file_id")).into(), Order::Asc);
    if state.item_data_query {
        window.order_by_expr(Expr::col(Alias::new("data_id")).into(), Order::Asc);
    }
    ranked.expr_window_as(Expr::cust("row_number()"), window, Alias::new(COARSE_RANK));
    let ranked_cte = create_cte(state, format!("ranked_{cte_name}"), ranked);

    let head = build_head(state, &ranked_cte);
    let head_cte = create_cte(state, format!("head_{cte_name}"), head);

    let ranked_alias = Alias::new(ranked_cte.name.as_str());
    let head_alias = Alias::new(head_cte.name.as_str());

    let mut join_cond = Expr::col((head_alias.clone(), Alias::new("file_id")))
        .equals((ranked_alias.clone(), Alias::new("file_id")));
    if state.item_data_query {
        join_cond = join_cond.and(
            Expr::col((head_alias.clone(), Alias::new("data_id")))
                .equals((ranked_alias.clone(), Alias::new("data_id"))),
        );
    }

    let mut merge = Query::select();
    merge.from(ranked_alias.clone());
    merge.join(JoinType::LeftJoin, head_alias.clone(), join_cond);
    merge.expr_as(
        Expr::col((ranked_alias.clone(), Alias::new("item_id"))),
        Alias::new("item_id"),
    );
    merge.expr_as(
        Expr::col((ranked_alias.clone(), Alias::new("file_id"))),
        Alias::new("file_id"),
    );
    if state.item_data_query {
        merge.expr_as(
            Expr::col((ranked_alias.clone(), Alias::new("data_id"))),
            Alias::new("data_id"),
        );
    }

    // Final ordering key: head before tail, head by exact distance, tail by
    // coarse distance, item_id tiebreaker. Tail rows have NULL edist and
    // head rows share a constant CASE key, so the mixed-unit columns never
    // actually compare across categories.
    let mut window = WindowStatement::new();
    window.order_by_expr(
        Expr::case(
            Expr::col((head_alias.clone(), Alias::new("file_id"))).is_null(),
            Expr::val(1),
        )
        .finally(Expr::val(0))
        .into(),
        Order::Asc,
    );
    // A NULL exact aggregate (e.g. confidence weights that sum to zero)
    // sorts last, matching the exact path's NullOrdering::Last — otherwise
    // SQLite's ASC default would promote such a row to rank 1 and the same
    // query would differ between `exact` and `auto`.
    window.order_by_expr_with_nulls(
        Expr::col((head_alias.clone(), Alias::new(EXACT_DIST))).into(),
        direction.clone(),
        NullOrdering::Last,
    );
    window.order_by_expr_with_nulls(
        Expr::col((ranked_alias.clone(), Alias::new(COARSE_DIST))).into(),
        direction,
        NullOrdering::Last,
    );
    window.order_by_expr(
        Expr::col((ranked_alias.clone(), Alias::new("item_id"))).into(),
        Order::Asc,
    );
    window.order_by_expr(
        Expr::col((ranked_alias.clone(), Alias::new("file_id"))).into(),
        Order::Asc,
    );
    if state.item_data_query {
        window.order_by_expr(
            Expr::col((ranked_alias.clone(), Alias::new("data_id"))).into(),
            Order::Asc,
        );
    }
    merge.expr_window_as(Expr::cust("row_number()"), window, Alias::new("order_rank"));

    (merge, ranked_cte)
}
