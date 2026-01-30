use sea_query::{Expr, ExprTrait, JoinType};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use std::collections::HashMap;

use crate::pql::model::Column;
use crate::pql::preprocess::PqlError;

use super::FilterCompiler;
use super::super::{
    BaseTable, CteRef, ExtractedText, Files, ItemData, Items, JoinedTables, QueryState, Setters,
    get_column_expr, is_text_column, select_std_from_cte, wrap_query,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchValues {
    #[serde(default)]
    pub file_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub item_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub path: Option<OneOrMany<String>>,
    #[serde(default)]
    pub filename: Option<OneOrMany<String>>,
    #[serde(default)]
    pub sha256: Option<OneOrMany<String>>,
    #[serde(default)]
    pub last_modified: Option<OneOrMany<String>>,
    #[serde(default)]
    pub r#type: Option<OneOrMany<String>>,
    #[serde(default)]
    pub size: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub width: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub height: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub duration: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub time_added: Option<OneOrMany<String>>,
    #[serde(default)]
    pub md5: Option<OneOrMany<String>>,
    #[serde(default)]
    pub audio_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub video_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub subtitle_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub blurhash: Option<OneOrMany<String>>,
    #[serde(default)]
    pub data_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub language: Option<OneOrMany<String>>,
    #[serde(default)]
    pub language_confidence: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub text: Option<OneOrMany<String>>,
    #[serde(default)]
    pub confidence: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub text_length: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub job_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub setter_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub setter_name: Option<OneOrMany<String>>,
    #[serde(default)]
    pub data_index: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub source_id: Option<OneOrMany<i64>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchValue {
    #[serde(default)]
    pub file_id: Option<i64>,
    #[serde(default)]
    pub item_id: Option<i64>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub width: Option<i64>,
    #[serde(default)]
    pub height: Option<i64>,
    #[serde(default)]
    pub duration: Option<f64>,
    #[serde(default)]
    pub time_added: Option<String>,
    #[serde(default)]
    pub md5: Option<String>,
    #[serde(default)]
    pub audio_tracks: Option<i64>,
    #[serde(default)]
    pub video_tracks: Option<i64>,
    #[serde(default)]
    pub subtitle_tracks: Option<i64>,
    #[serde(default)]
    pub blurhash: Option<String>,
    #[serde(default)]
    pub data_id: Option<i64>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub language_confidence: Option<f64>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub text_length: Option<i64>,
    #[serde(default)]
    pub job_id: Option<i64>,
    #[serde(default)]
    pub setter_id: Option<i64>,
    #[serde(default)]
    pub setter_name: Option<String>,
    #[serde(default)]
    pub data_index: Option<i64>,
    #[serde(default)]
    pub source_id: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchOps {
    #[serde(default)]
    pub eq: Option<MatchValue>,
    #[serde(default)]
    pub neq: Option<MatchValue>,
    #[serde(rename = "in_", default)]
    pub in_: Option<MatchValues>,
    #[serde(default)]
    pub nin: Option<MatchValues>,
    #[serde(default)]
    pub gt: Option<MatchValue>,
    #[serde(default)]
    pub gte: Option<MatchValue>,
    #[serde(default)]
    pub lt: Option<MatchValue>,
    #[serde(default)]
    pub lte: Option<MatchValue>,
    #[serde(default)]
    pub startswith: Option<MatchValues>,
    #[serde(default)]
    pub not_startswith: Option<MatchValues>,
    #[serde(default)]
    pub endswith: Option<MatchValues>,
    #[serde(default)]
    pub not_endswith: Option<MatchValues>,
    #[serde(default)]
    pub contains: Option<MatchValues>,
    #[serde(default)]
    pub not_contains: Option<MatchValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchAnd {
    pub and_: Vec<MatchOps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchOr {
    pub or_: Vec<MatchOps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchNot {
    pub not_: MatchOps,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum Matches {
    Ops(MatchOps),
    And(MatchAnd),
    Or(MatchOr),
    Not(MatchNot),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct Match {
    /// The match operations to apply. Match filters operate on key-value pairs representing
    /// the primitive attributes of items, files, and extracted data.
    /// For example, a match filter can be used to filter items
    /// based on their type, size, or the path of the file they are associated with.
    #[serde(rename = "match")]
    pub match_: Matches,
}

pub(crate) fn evaluate_match(filter: &Match, obj: &MatchValue) -> bool {
    let obj_fields = collect_match_value_fields(obj)
        .into_iter()
        .collect::<HashMap<_, _>>();
    evaluate_matches(&filter.match_, &obj_fields)
}

impl FilterCompiler for Match {
    fn build(&self, context: &CteRef, state: &mut QueryState) -> Result<CteRef, PqlError> {
        let expression = build_matches_expression(&self.match_, state.item_data_query)?;
        let mut query = select_std_from_cte(context, state);
        query.join(
            JoinType::InnerJoin,
            Items::Table,
            Expr::col((Items::Table, Items::Id)).equals(context.column_ref("item_id")),
        );
        query.join(
            JoinType::InnerJoin,
            Files::Table,
            Expr::col((Files::Table, Files::Id)).equals(context.column_ref("file_id")),
        );
        if state.item_data_query {
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
        }
        query.and_where(expression);

        let mut joined_tables = JoinedTables::default();
        joined_tables.mark(BaseTable::Items);
        joined_tables.mark(BaseTable::Files);
        if state.item_data_query {
            joined_tables.mark(BaseTable::ItemData);
            joined_tables.mark(BaseTable::Setters);
            joined_tables.mark(BaseTable::ExtractedText);
        }

        let cte_name = format!("n{}_Match", state.cte_counter);
        let cte = wrap_query(state, query, context, cte_name, &joined_tables);
        state.cte_counter += 1;
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
    fn match_filter_builds_sql() {
        let filter: Match = serde_json::from_value(json!({
            "match": { "eq": { "file_id": 1 } }
        }))
        .expect("match filter");
        let mut state = build_base_state(EntityType::File, false);
        let context = build_begin_cte(&mut state);
        let sql = render_filter_sql(&filter, &mut state, &context);
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM"));
    }

    #[tokio::test]
    async fn match_filter_runs_full_query() {
        let filter: Match = serde_json::from_value(json!({
            "match": { "eq": { "file_id": 1 } }
        }))
        .expect("match filter");
        run_full_pql_query(QueryElement::Match(filter), EntityType::File)
            .await
            .expect("match query");
    }

    #[test]
    fn evaluate_match_checks_eq_and_contains() {
        let filter = Match {
            match_: Matches::Ops(MatchOps {
                eq: Some(MatchValue {
                    r#type: Some("image/png".to_string()),
                    ..Default::default()
                }),
                contains: Some(MatchValues {
                    path: Some(OneOrMany::Many(vec![
                        "media".to_string(),
                        "photos".to_string(),
                    ])),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let obj = MatchValue {
            r#type: Some("image/png".to_string()),
            path: Some("C:/media/sample.png".to_string()),
            ..Default::default()
        };
        assert!(evaluate_match(&filter, &obj));
    }

    #[test]
    fn evaluate_match_handles_or_and_not() {
        let filter = Match {
            match_: Matches::Or(MatchOr {
                or_: vec![
                    MatchOps {
                        eq: Some(MatchValue {
                            filename: Some("keep.png".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    MatchOps {
                        not_contains: Some(MatchValues {
                            path: Some(OneOrMany::One("tmp".to_string())),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
            }),
        };
        let obj = MatchValue {
            filename: Some("other.png".to_string()),
            path: Some("C:/data/other.png".to_string()),
            ..Default::default()
        };
        assert!(evaluate_match(&filter, &obj));

        let blocked = MatchValue {
            filename: Some("other.png".to_string()),
            path: Some("C:/tmp/other.png".to_string()),
            ..Default::default()
        };
        assert!(!evaluate_match(&filter, &blocked));
    }
}

fn build_matches_expression(matches: &Matches, allow_text: bool) -> Result<Expr, PqlError> {
    match matches {
        Matches::Ops(ops) => build_match_ops_expression(ops, allow_text),
        Matches::And(MatchAnd { and_ }) => {
            let mut expressions = Vec::new();
            for op in and_ {
                expressions.push(build_match_ops_expression(op, allow_text)?);
            }
            combine_and(expressions)
        }
        Matches::Or(MatchOr { or_ }) => {
            let mut expressions = Vec::new();
            for op in or_ {
                expressions.push(build_match_ops_expression(op, allow_text)?);
            }
            combine_or(expressions)
        }
        Matches::Not(MatchNot { not_ }) => {
            let expr = build_match_ops_expression(not_, allow_text)?;
            Ok(expr.not())
        }
    }
}

fn evaluate_matches(matches: &Matches, obj_fields: &HashMap<Column, FieldValue>) -> bool {
    match matches {
        Matches::Ops(ops) => evaluate_match_ops(ops, obj_fields),
        Matches::And(MatchAnd { and_ }) => and_.iter().all(|ops| evaluate_match_ops(ops, obj_fields)),
        Matches::Or(MatchOr { or_ }) => or_.iter().any(|ops| evaluate_match_ops(ops, obj_fields)),
        Matches::Not(MatchNot { not_ }) => !evaluate_match_ops(not_, obj_fields),
    }
}

fn evaluate_match_ops(ops: &MatchOps, obj_fields: &HashMap<Column, FieldValue>) -> bool {
    let mut results = Vec::new();

    if let Some(values) = &ops.eq {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Eq));
    }
    if let Some(values) = &ops.neq {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Neq));
    }
    if let Some(values) = &ops.in_ {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::In));
    }
    if let Some(values) = &ops.nin {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::NotIn));
    }
    if let Some(values) = &ops.gt {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Gt));
    }
    if let Some(values) = &ops.gte {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Gte));
    }
    if let Some(values) = &ops.lt {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Lt));
    }
    if let Some(values) = &ops.lte {
        results.extend(evaluate_match_values(values, obj_fields, MatchOp::Lte));
    }
    if let Some(values) = &ops.startswith {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::StartsWith));
    }
    if let Some(values) = &ops.not_startswith {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::NotStartsWith));
    }
    if let Some(values) = &ops.endswith {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::EndsWith));
    }
    if let Some(values) = &ops.not_endswith {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::NotEndsWith));
    }
    if let Some(values) = &ops.contains {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::Contains));
    }
    if let Some(values) = &ops.not_contains {
        results.extend(evaluate_match_value_lists(values, obj_fields, MatchOp::NotContains));
    }

    if results.is_empty() {
        return true;
    }

    results.into_iter().all(|value| value)
}

#[derive(Clone, Copy)]
enum MatchOp {
    Eq,
    Neq,
    In,
    NotIn,
    Gt,
    Gte,
    Lt,
    Lte,
    StartsWith,
    NotStartsWith,
    EndsWith,
    NotEndsWith,
    Contains,
    NotContains,
}

fn evaluate_match_values(
    values: &MatchValue,
    obj_fields: &HashMap<Column, FieldValue>,
    op: MatchOp,
) -> Vec<bool> {
    let mut results = Vec::new();
    for (column, value) in collect_match_value_fields(values) {
        let Some(field_value) = obj_fields.get(&column) else {
            continue;
        };
        results.push(compare_field(field_value, &value, op));
    }
    results
}

fn evaluate_match_value_lists(
    values: &MatchValues,
    obj_fields: &HashMap<Column, FieldValue>,
    op: MatchOp,
) -> Vec<bool> {
    let mut results = Vec::new();
    for (column, value) in collect_match_values_fields(values) {
        let Some(field_value) = obj_fields.get(&column) else {
            continue;
        };
        results.push(compare_field_list(field_value, &value, op));
    }
    results
}

fn compare_field(field_value: &FieldValue, value: &FieldValue, op: MatchOp) -> bool {
    match op {
        MatchOp::Eq => field_value == value,
        MatchOp::Neq => field_value != value,
        MatchOp::Gt => compare_ordered(field_value, value, |ord| ord.is_gt()),
        MatchOp::Gte => compare_ordered(field_value, value, |ord| ord.is_gt() || ord.is_eq()),
        MatchOp::Lt => compare_ordered(field_value, value, |ord| ord.is_lt()),
        MatchOp::Lte => compare_ordered(field_value, value, |ord| ord.is_lt() || ord.is_eq()),
        _ => false,
    }
}

fn compare_field_list(field_value: &FieldValue, values: &FieldValues, op: MatchOp) -> bool {
    match op {
        MatchOp::In => {
            let list = match values {
                FieldValues::Many(values) => values,
                FieldValues::Single(_) => return false,
            };
            list.iter().any(|value| field_value == value)
        }
        MatchOp::NotIn => {
            let list = match values {
                FieldValues::Many(values) => values,
                FieldValues::Single(_) => return false,
            };
            list.iter().all(|value| field_value != value)
        }
        MatchOp::StartsWith | MatchOp::NotStartsWith => {
            string_list_check(field_value, values, op, |value, pattern| value.starts_with(pattern))
        }
        MatchOp::EndsWith | MatchOp::NotEndsWith => {
            string_list_check(field_value, values, op, |value, pattern| value.ends_with(pattern))
        }
        MatchOp::Contains | MatchOp::NotContains => {
            string_list_check(field_value, values, op, |value, pattern| value.contains(pattern))
        }
        _ => false,
    }
}

fn string_list_check(
    field_value: &FieldValue,
    values: &FieldValues,
    op: MatchOp,
    check: impl Fn(&str, &str) -> bool,
) -> bool {
    let FieldValue::String(field_str) = field_value else {
        return false;
    };
    let list = match values {
        FieldValues::Single(value) => vec![value.clone()],
        FieldValues::Many(values) => values.clone(),
    };

    let negate = matches!(
        op,
        MatchOp::NotStartsWith | MatchOp::NotEndsWith | MatchOp::NotContains
    );
    let use_all = negate;

    let mut results = list.iter().map(|value| match value {
        FieldValue::String(pattern) => check(field_str, pattern),
        _ => false,
    });

    if use_all {
        results.all(|value| if negate { !value } else { value })
    } else {
        results.any(|value| if negate { !value } else { value })
    }
}

fn compare_ordered(
    left: &FieldValue,
    right: &FieldValue,
    check: impl Fn(std::cmp::Ordering) -> bool,
) -> bool {
    match (left, right) {
        (FieldValue::Int(lhs), FieldValue::Int(rhs)) => check(lhs.cmp(rhs)),
        (FieldValue::Float(lhs), FieldValue::Float(rhs)) => lhs.partial_cmp(rhs).map(check).unwrap_or(false),
        (FieldValue::Int(lhs), FieldValue::Float(rhs)) => {
            (*lhs as f64).partial_cmp(rhs).map(check).unwrap_or(false)
        }
        (FieldValue::Float(lhs), FieldValue::Int(rhs)) => {
            lhs.partial_cmp(&(*rhs as f64)).map(check).unwrap_or(false)
        }
        (FieldValue::String(lhs), FieldValue::String(rhs)) => check(lhs.cmp(rhs)),
        _ => false,
    }
}

fn build_match_ops_expression(ops: &MatchOps, allow_text: bool) -> Result<Expr, PqlError> {
    let mut expressions = Vec::new();

    if let Some(value) = &ops.eq {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Eq, allow_text)?);
    }
    if let Some(value) = &ops.neq {
        expressions.extend(build_match_value_expressions(
            value,
            MatchOperator::Neq,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.in_ {
        expressions.extend(build_match_values_expressions(value, MatchOperator::In, allow_text)?);
    }
    if let Some(value) = &ops.nin {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::NotIn,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.gt {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Gt, allow_text)?);
    }
    if let Some(value) = &ops.gte {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Gte, allow_text)?);
    }
    if let Some(value) = &ops.lt {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Lt, allow_text)?);
    }
    if let Some(value) = &ops.lte {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Lte, allow_text)?);
    }
    if let Some(value) = &ops.startswith {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::StartsWith,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.not_startswith {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::NotStartsWith,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.endswith {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::EndsWith,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.not_endswith {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::NotEndsWith,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.contains {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::Contains,
            allow_text,
        )?);
    }
    if let Some(value) = &ops.not_contains {
        expressions.extend(build_match_values_expressions(
            value,
            MatchOperator::NotContains,
            allow_text,
        )?);
    }

    if expressions.is_empty() {
        return Err(PqlError::invalid("No valid expressions found in MatchOps"));
    }

    combine_and(expressions)
}

#[derive(Clone, Copy)]
enum MatchOperator {
    Eq,
    Neq,
    In,
    NotIn,
    Gt,
    Gte,
    Lt,
    Lte,
    StartsWith,
    NotStartsWith,
    EndsWith,
    NotEndsWith,
    Contains,
    NotContains,
}

fn build_match_value_expressions(
    values: &MatchValue,
    operator: MatchOperator,
    allow_text: bool,
) -> Result<Vec<Expr>, PqlError> {
    let mut expressions = Vec::new();
    for (column, value) in collect_match_value_fields(values) {
        if !allow_text && is_text_column(column) {
            return Err(PqlError::invalid("Text columns are not allowed in this context"));
        }
        let col_expr = get_column_expr(column);
        let value_expr = value.to_expr();
        let expr = match operator {
            MatchOperator::Eq => col_expr.eq(value_expr),
            MatchOperator::Neq => col_expr.ne(value_expr),
            MatchOperator::Gt => col_expr.gt(value_expr),
            MatchOperator::Gte => col_expr.gte(value_expr),
            MatchOperator::Lt => col_expr.lt(value_expr),
            MatchOperator::Lte => col_expr.lte(value_expr),
            _ => {
                return Err(PqlError::invalid("Invalid operator for MatchValue"));
            }
        };
        expressions.push(expr);
    }
    Ok(expressions)
}

fn build_match_values_expressions(
    values: &MatchValues,
    operator: MatchOperator,
    allow_text: bool,
) -> Result<Vec<Expr>, PqlError> {
    let mut expressions = Vec::new();
    for (column, value) in collect_match_values_fields(values) {
        if !allow_text && is_text_column(column) {
            return Err(PqlError::invalid("Text columns are not allowed in this context"));
        }
        let col_expr = get_column_expr(column);
        let expr = match operator {
            MatchOperator::In => build_in_expression(&col_expr, value, false)?,
            MatchOperator::NotIn => build_in_expression(&col_expr, value, true)?,
            MatchOperator::StartsWith => {
                build_like_expression(&col_expr, value, LikeKind::StartsWith)?
            }
            MatchOperator::NotStartsWith => {
                build_like_expression(&col_expr, value, LikeKind::NotStartsWith)?
            }
            MatchOperator::EndsWith => build_like_expression(&col_expr, value, LikeKind::EndsWith)?,
            MatchOperator::NotEndsWith => {
                build_like_expression(&col_expr, value, LikeKind::NotEndsWith)?
            }
            MatchOperator::Contains => build_like_expression(&col_expr, value, LikeKind::Contains)?,
            MatchOperator::NotContains => {
                build_like_expression(&col_expr, value, LikeKind::NotContains)?
            }
            _ => {
                return Err(PqlError::invalid("Invalid operator for MatchValues"));
            }
        };
        expressions.push(expr);
    }
    Ok(expressions)
}

#[derive(Clone, Copy)]
enum LikeKind {
    StartsWith,
    NotStartsWith,
    EndsWith,
    NotEndsWith,
    Contains,
    NotContains,
}

fn build_in_expression(col_expr: &Expr, value: FieldValues, negate: bool) -> Result<Expr, PqlError> {
    let values = match value {
        FieldValues::Single(_) => {
            return Err(PqlError::invalid("Invalid operator for single value"));
        }
        FieldValues::Many(values) => values,
    };
    if values.is_empty() {
        return Err(PqlError::invalid("Empty list for in/nin operator"));
    }
    let expr_values = values.iter().map(|v| v.to_expr()).collect::<Vec<_>>();
    let expr = if negate {
        col_expr.clone().is_not_in(expr_values)
    } else {
        col_expr.clone().is_in(expr_values)
    };
    Ok(expr)
}

fn build_like_expression(
    col_expr: &Expr,
    value: FieldValues,
    kind: LikeKind,
) -> Result<Expr, PqlError> {
    let (values, negate, use_and) = match kind {
        LikeKind::StartsWith => (value, false, false),
        LikeKind::NotStartsWith => (value, true, true),
        LikeKind::EndsWith => (value, false, false),
        LikeKind::NotEndsWith => (value, true, true),
        LikeKind::Contains => (value, false, false),
        LikeKind::NotContains => (value, true, true),
    };

    let make_pattern = |val: &FieldValue| {
        let raw = val.to_string_value();
        match kind {
            LikeKind::StartsWith | LikeKind::NotStartsWith => format!("{raw}%"),
            LikeKind::EndsWith | LikeKind::NotEndsWith => format!("%{raw}"),
            LikeKind::Contains | LikeKind::NotContains => format!("%{raw}%"),
        }
    };

    let build_single = |val: &FieldValue| {
        let pattern = make_pattern(val);
        if negate {
            col_expr.clone().not_like(pattern)
        } else {
            col_expr.clone().like(pattern)
        }
    };

    let expr = match values {
        FieldValues::Single(value) => build_single(&value),
        FieldValues::Many(values) => {
            let mut exprs = Vec::new();
            for value in values {
                exprs.push(build_single(&value));
            }
            if use_and {
                combine_and(exprs)?
            } else {
                combine_or(exprs)?
            }
        }
    };
    Ok(expr)
}

fn combine_and(mut expressions: Vec<Expr>) -> Result<Expr, PqlError> {
    let first = expressions
        .drain(..1)
        .next()
        .ok_or_else(|| PqlError::invalid("No expressions to combine"))?;
    Ok(expressions.into_iter().fold(first, |acc, expr| acc.and(expr)))
}

fn combine_or(mut expressions: Vec<Expr>) -> Result<Expr, PqlError> {
    let first = expressions
        .drain(..1)
        .next()
        .ok_or_else(|| PqlError::invalid("No expressions to combine"))?;
    Ok(expressions.into_iter().fold(first, |acc, expr| acc.or(expr)))
}

fn collect_match_value_fields(values: &MatchValue) -> Vec<(Column, FieldValue)> {
    let mut fields = Vec::new();
    if let Some(value) = values.file_id {
        fields.push((Column::FileId, FieldValue::Int(value)));
    }
    if let Some(value) = values.item_id {
        fields.push((Column::ItemId, FieldValue::Int(value)));
    }
    if let Some(value) = values.path.clone() {
        fields.push((Column::Path, FieldValue::String(value)));
    }
    if let Some(value) = values.filename.clone() {
        fields.push((Column::Filename, FieldValue::String(value)));
    }
    if let Some(value) = values.sha256.clone() {
        fields.push((Column::Sha256, FieldValue::String(value)));
    }
    if let Some(value) = values.last_modified.clone() {
        fields.push((Column::LastModified, FieldValue::String(value)));
    }
    if let Some(value) = values.r#type.clone() {
        fields.push((Column::Type, FieldValue::String(value)));
    }
    if let Some(value) = values.size {
        fields.push((Column::Size, FieldValue::Int(value)));
    }
    if let Some(value) = values.width {
        fields.push((Column::Width, FieldValue::Int(value)));
    }
    if let Some(value) = values.height {
        fields.push((Column::Height, FieldValue::Int(value)));
    }
    if let Some(value) = values.duration {
        fields.push((Column::Duration, FieldValue::Float(value)));
    }
    if let Some(value) = values.time_added.clone() {
        fields.push((Column::TimeAdded, FieldValue::String(value)));
    }
    if let Some(value) = values.md5.clone() {
        fields.push((Column::Md5, FieldValue::String(value)));
    }
    if let Some(value) = values.audio_tracks {
        fields.push((Column::AudioTracks, FieldValue::Int(value)));
    }
    if let Some(value) = values.video_tracks {
        fields.push((Column::VideoTracks, FieldValue::Int(value)));
    }
    if let Some(value) = values.subtitle_tracks {
        fields.push((Column::SubtitleTracks, FieldValue::Int(value)));
    }
    if let Some(value) = values.blurhash.clone() {
        fields.push((Column::Blurhash, FieldValue::String(value)));
    }
    if let Some(value) = values.data_id {
        fields.push((Column::DataId, FieldValue::Int(value)));
    }
    if let Some(value) = values.language.clone() {
        fields.push((Column::Language, FieldValue::String(value)));
    }
    if let Some(value) = values.language_confidence {
        fields.push((Column::LanguageConfidence, FieldValue::Float(value)));
    }
    if let Some(value) = values.text.clone() {
        fields.push((Column::Text, FieldValue::String(value)));
    }
    if let Some(value) = values.confidence {
        fields.push((Column::Confidence, FieldValue::Float(value)));
    }
    if let Some(value) = values.text_length {
        fields.push((Column::TextLength, FieldValue::Int(value)));
    }
    if let Some(value) = values.job_id {
        fields.push((Column::JobId, FieldValue::Int(value)));
    }
    if let Some(value) = values.setter_id {
        fields.push((Column::SetterId, FieldValue::Int(value)));
    }
    if let Some(value) = values.setter_name.clone() {
        fields.push((Column::SetterName, FieldValue::String(value)));
    }
    if let Some(value) = values.data_index {
        fields.push((Column::DataIndex, FieldValue::Int(value)));
    }
    if let Some(value) = values.source_id {
        fields.push((Column::SourceId, FieldValue::Int(value)));
    }
    fields
}

fn collect_match_values_fields(values: &MatchValues) -> Vec<(Column, FieldValues)> {
    let mut fields = Vec::new();
    if let Some(value) = values.file_id.as_ref() {
        fields.push((Column::FileId, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.item_id.as_ref() {
        fields.push((Column::ItemId, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.path.as_ref() {
        fields.push((Column::Path, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.filename.as_ref() {
        fields.push((
            Column::Filename,
            convert_one_or_many(value, |v| FieldValue::String(v.clone())),
        ));
    }
    if let Some(value) = values.sha256.as_ref() {
        fields.push((Column::Sha256, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.last_modified.as_ref() {
        fields.push((
            Column::LastModified,
            convert_one_or_many(value, |v| FieldValue::String(v.clone())),
        ));
    }
    if let Some(value) = values.r#type.as_ref() {
        fields.push((Column::Type, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.size.as_ref() {
        fields.push((Column::Size, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.width.as_ref() {
        fields.push((Column::Width, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.height.as_ref() {
        fields.push((Column::Height, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.duration.as_ref() {
        fields.push((Column::Duration, convert_one_or_many(value, map_float)));
    }
    if let Some(value) = values.time_added.as_ref() {
        fields.push((
            Column::TimeAdded,
            convert_one_or_many(value, |v| FieldValue::String(v.clone())),
        ));
    }
    if let Some(value) = values.md5.as_ref() {
        fields.push((Column::Md5, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.audio_tracks.as_ref() {
        fields.push((Column::AudioTracks, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.video_tracks.as_ref() {
        fields.push((Column::VideoTracks, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.subtitle_tracks.as_ref() {
        fields.push((Column::SubtitleTracks, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.blurhash.as_ref() {
        fields.push(
            (Column::Blurhash, convert_one_or_many(value, |v| FieldValue::String(v.clone()))),
        );
    }
    if let Some(value) = values.data_id.as_ref() {
        fields.push((Column::DataId, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.language.as_ref() {
        fields.push((Column::Language, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.language_confidence.as_ref() {
        fields.push((Column::LanguageConfidence, convert_one_or_many(value, map_float)));
    }
    if let Some(value) = values.text.as_ref() {
        fields.push((Column::Text, convert_one_or_many(value, |v| FieldValue::String(v.clone()))));
    }
    if let Some(value) = values.confidence.as_ref() {
        fields.push((Column::Confidence, convert_one_or_many(value, map_float)));
    }
    if let Some(value) = values.text_length.as_ref() {
        fields.push((Column::TextLength, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.job_id.as_ref() {
        fields.push((Column::JobId, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.setter_id.as_ref() {
        fields.push((Column::SetterId, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.setter_name.as_ref() {
        fields.push(
            (Column::SetterName, convert_one_or_many(value, |v| FieldValue::String(v.clone()))),
        );
    }
    if let Some(value) = values.data_index.as_ref() {
        fields.push((Column::DataIndex, convert_one_or_many(value, map_int)));
    }
    if let Some(value) = values.source_id.as_ref() {
        fields.push((Column::SourceId, convert_one_or_many(value, map_int)));
    }
    fields
}

fn convert_one_or_many<T, F>(value: &OneOrMany<T>, mapper: F) -> FieldValues
where
    F: Fn(&T) -> FieldValue,
{
    match value {
        OneOrMany::One(inner) => FieldValues::Single(mapper(inner)),
        OneOrMany::Many(list) => FieldValues::Many(list.iter().map(mapper).collect()),
    }
}

fn map_int(value: &i64) -> FieldValue {
    FieldValue::Int(*value)
}

fn map_float(value: &f64) -> FieldValue {
    FieldValue::Float(*value)
}

#[derive(Clone, Debug, PartialEq)]
enum FieldValue {
    Int(i64),
    Float(f64),
    String(String),
}

impl FieldValue {
    fn to_expr(&self) -> Expr {
        match self {
            FieldValue::Int(value) => Expr::val(*value),
            FieldValue::Float(value) => Expr::val(*value),
            FieldValue::String(value) => Expr::val(value.clone()),
        }
    }

    fn to_string_value(&self) -> String {
        match self {
            FieldValue::Int(value) => value.to_string(),
            FieldValue::Float(value) => value.to_string(),
            FieldValue::String(value) => value.clone(),
        }
    }
}

#[derive(Clone, Debug)]
enum FieldValues {
    Single(FieldValue),
    Many(Vec<FieldValue>),
}
