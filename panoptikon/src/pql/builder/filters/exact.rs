//! Fix B for the exact vector scorers (docs/or-composition-penalty.md §5).
//!
//! SQLite evaluates aggregate arguments *after* the GROUP BY sorter, so an
//! aggregate over `vec_distance_*(embeddings.embedding, ?)` pushes the raw
//! embedding blob through a temp b-tree whenever the plan's join order is
//! not already GROUP-BY-ordered — ~2.3 GB of sorter traffic at 690k mpnet
//! vectors, the dominant cost of the composed default search. The fix:
//! evaluate the distance (and the per-row confidence weights) in a
//! `MATERIALIZED` inner CTE first, then aggregate over that, so the sorter
//! carries a few bytes per row instead of the blob.
//!
//! Builder-level convention (same doc, §7): never place a blob-consuming
//! expression inside an aggregate that can meet a GROUP BY sorter.

use sea_query::{Alias, Expr, ExprTrait, Func, SelectStatement};

use crate::pql::model::SortableOptions;
use crate::pql::preprocess::PqlError;

use super::super::{
    CteRef, ExtractedText, QueryState, add_rank_column_expr, apply_group_by,
    create_materialized_cte, get_std_group_by, select_std_from_cte,
};
use super::embedding_types::DistanceAggregation;
use super::item_similarity::SourceArgs;

/// Column alias for the per-row distance in the materialized CTE.
const DIST: &str = "d";
/// Column alias for the per-row confidence weight in the materialized CTE.
const WEIGHT: &str = "w";

/// The combined per-row confidence weight, when weighting applies:
/// `pow(coalesce(confidence, 1), w_c) * pow(coalesce(language_confidence, 1), w_l)`
/// with a factor dropped when its exponent is zero, `None` when both are.
/// References the unaliased `extracted_text` join, so it must be evaluated
/// inside the candidate skeleton.
pub(super) fn confidence_weight_expr(src_text: &SourceArgs) -> Option<Expr> {
    let conf = (src_text.confidence_weight != 0.0)
        .then(|| pow_expr(ExtractedText::Confidence, src_text.confidence_weight));
    let lang = (src_text.language_confidence_weight != 0.0).then(|| {
        pow_expr(
            ExtractedText::LanguageConfidence,
            src_text.language_confidence_weight,
        )
    });
    match (conf, lang) {
        (Some(conf), Some(lang)) => Some(conf.mul(lang)),
        (Some(conf), None) => Some(conf),
        (None, Some(lang)) => Some(lang),
        (None, None) => None,
    }
}

fn pow_expr(column: ExtractedText, exponent: f64) -> Expr {
    Func::cust("pow")
        .args([
            Func::coalesce([Expr::col((ExtractedText::Table, column)), Expr::val(1)]).into(),
            Expr::val(exponent),
        ])
        .into()
}

/// The rank aggregate over a per-row distance: the confidence-weighted
/// average `SUM(d * w) / SUM(w)` when weights apply (`distance_aggregation`
/// is ignored then, as documented on `SourceArgs`), plain MIN/MAX/AVG
/// otherwise.
pub(super) fn rank_aggregate(
    distance: Expr,
    weight: Option<Expr>,
    aggregation: DistanceAggregation,
) -> Expr {
    match weight {
        Some(weight) => distance.mul(weight.clone()).sum().div(weight.sum()),
        None => match aggregation {
            DistanceAggregation::Max => distance.max(),
            DistanceAggregation::Avg => distance.avg(),
            DistanceAggregation::Min => distance.min(),
        },
    }
}

/// Assembles the fixed exact scorer: the (un-grouped) candidate skeleton,
/// extended with per-row distance and weight columns, becomes a
/// `MATERIALIZED` `dist_{cte_name}` CTE, and the returned select runs the
/// GROUP BY aggregation over it. Membership and aggregate inputs are
/// identical to aggregating over the skeleton directly — only where the
/// distance is evaluated moves.
///
/// The returned select reads exclusively from the dist CTE, so no base
/// tables are visible to the final query anymore; the dist CTE ref is the
/// select's context (the table its standard columns resolve against) — pass
/// both to `apply_sort_bounds` + `wrap_query` with empty `JoinedTables`.
pub(super) fn assemble_exact_fixb(
    state: &mut QueryState,
    cte_name: &str,
    mut skeleton: SelectStatement,
    distance: Expr,
    weight: Option<Expr>,
    aggregation: DistanceAggregation,
    sort: &SortableOptions,
) -> Result<(SelectStatement, CteRef), PqlError> {
    skeleton.expr_as(distance, Alias::new(DIST));
    let weighted = weight.is_some();
    if let Some(weight) = weight {
        skeleton.expr_as(weight, Alias::new(WEIGHT));
    }
    let dist_cte = create_materialized_cte(state, format!("dist_{cte_name}"), skeleton);

    let mut query = select_std_from_cte(&dist_cte, state);
    apply_group_by(&mut query, get_std_group_by(&dist_cte, state));
    let rank = rank_aggregate(
        dist_cte.column_expr(DIST),
        weighted.then(|| dist_cte.column_expr(WEIGHT)),
        aggregation,
    );
    add_rank_column_expr(&mut query, sort, rank)?;
    Ok((query, dist_cte))
}
