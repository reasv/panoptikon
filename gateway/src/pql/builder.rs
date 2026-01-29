use std::collections::{HashMap, HashSet};

use sea_query::{
    Alias, Asterisk, BinOper, ColumnRef, CommonTableExpression, Cond, Expr, ExprTrait, Func,
    IntoColumnRef, JoinType, NullOrdering, Order, OverStatement, Query, SelectStatement, UnionType,
    WindowStatement, WithClause,
};
use sea_query::extension::sqlite::SqliteBinOper;

use crate::pql::model::{
    Column, EntityType, HasUnprocessedData, InBookmarks, Match, MatchAnd, MatchNot, MatchOps,
    MatchOr, MatchPath, MatchTags, MatchText, MatchValue, MatchValues, Matches, OneOrMany, OrderArgs,
    OrderByField, OrderDirection, PqlQuery, ProcessedBy, QueryElement, Rrf, ScalarValue,
    SortableOptions,
};
use crate::pql::preprocess::{PqlError, preprocess_query};

const VERY_LARGE_NUMBER: &str = "9223372036854775805";
const VERY_SMALL_NUMBER: &str = "-9223372036854775805";

pub(crate) struct PqlBuilderResult {
    pub(crate) query: SelectStatement,
    pub(crate) with_clause: Option<WithClause>,
    pub(crate) extra_columns: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct CteRef {
    name: String,
}

impl CteRef {
    fn column_ref(&self, column: &str) -> ColumnRef {
        (Alias::new(self.name.as_str()), Alias::new(column)).into_column_ref()
    }

    fn column_expr(&self, column: &str) -> Expr {
        Expr::col(self.column_ref(column))
    }
}

#[derive(Clone, Debug)]
struct CteDefinition {
    name: String,
    query: SelectStatement,
}

#[derive(Clone, Debug)]
struct FilterSelect {
    select: SelectStatement,
    context: CteRef,
}

#[derive(Clone, Debug)]
struct ExtraColumn {
    column: String,
    cte: CteRef,
    alias: String,
}

#[derive(Clone, Debug)]
struct OrderByFilter {
    cte: CteRef,
    direction: OrderDirection,
    priority: i32,
    rrf: Option<Rrf>,
}

#[derive(Clone, Debug)]
struct QueryState {
    order_list: Vec<OrderByFilter>,
    extra_columns: Vec<ExtraColumn>,
    selects: HashMap<String, FilterSelect>,
    ctes: Vec<CteDefinition>,
    cte_counter: i64,
    is_count_query: bool,
    item_data_query: bool,
    entity: EntityType,
}

#[derive(Clone, Debug)]
struct OrderSpec {
    expr: Expr,
    order: Order,
    nulls: NullOrdering,
}

#[derive(Clone, Debug)]
enum OrderByColumn {
    Label {
        label: String,
        order: Order,
    },
    Coalesce {
        labels: Vec<String>,
        order: Order,
        rrfs: Option<Vec<Rrf>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BaseTable {
    Files,
    Items,
    ItemData,
    ExtractedText,
    Setters,
}

#[derive(Default, Debug)]
struct JoinedTables {
    tables: HashSet<BaseTable>,
}

impl JoinedTables {
    fn has(&self, table: BaseTable) -> bool {
        self.tables.contains(&table)
    }

    fn mark(&mut self, table: BaseTable) {
        self.tables.insert(table);
    }
}

#[derive(Default, Debug)]
struct SelectedColumns {
    order: Vec<String>,
    seen: HashSet<String>,
}

impl SelectedColumns {
    fn push(&mut self, name: &str) {
        if self.seen.insert(name.to_string()) {
            self.order.push(name.to_string());
        }
    }

    fn extend<I>(&mut self, names: I)
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        for name in names {
            self.push(name.as_ref());
        }
    }
}

#[derive(Clone, Debug)]
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

pub(crate) fn build_query(
    mut input_query: PqlQuery,
    count_query: bool,
) -> Result<PqlBuilderResult, PqlError> {
    raise_if_invalid(&input_query)?;

    let mut state = QueryState {
        order_list: Vec::new(),
        extra_columns: Vec::new(),
        selects: HashMap::new(),
        ctes: Vec::new(),
        cte_counter: 0,
        is_count_query: count_query,
        item_data_query: matches!(input_query.entity, EntityType::Text),
        entity: input_query.entity,
    };

    let mut root_cte_name: Option<String> = None;
    let mut last_cte_name: Option<String> = None;

    let mut full_query: SelectStatement;
    let file_id_ref: ColumnRef;
    let item_id_ref: ColumnRef;
    let mut data_id_ref: Option<ColumnRef> = None;
    let mut joined_tables = JoinedTables::default();

    let query_root = match input_query.query.take() {
        Some(query_root) => preprocess_query(query_root)?,
        None => None,
    };
    let used_filters = query_root.is_some();

    if let Some(query_root) = query_root {
        let mut start = Query::select();
        start
            .expr_as(Expr::col((Files::Table, Files::Id)), Alias::new("file_id"))
            .expr_as(
                Expr::col((Files::Table, Files::ItemId)),
                Alias::new("item_id"),
            )
            .from(Files::Table);

        if state.item_data_query {
            let join_cond = Cond::all()
                .add(
                    Expr::col((ItemData::Table, ItemData::ItemId))
                        .equals((Files::Table, Files::ItemId)),
                )
                .add(
                    Expr::col((ItemData::Table, ItemData::DataType))
                        .eq(entity_to_data_type(state.entity)),
                );
            start.join(JoinType::InnerJoin, ItemData::Table, join_cond);
            start.expr_as(
                Expr::col((ItemData::Table, ItemData::Id)),
                Alias::new("data_id"),
            );
        }

        let begin_cte = create_cte(&mut state, "begin_cte".to_string(), start.to_owned());
        let root_cte = process_query_element(query_root, &begin_cte, &mut state)?;
        root_cte_name = Some(root_cte.name.clone());

        let root_select = state
            .selects
            .get(&root_cte.name)
            .ok_or_else(|| PqlError::invalid("Root CTE not registered"))?;

        full_query = root_select.select.clone();
        let root_context = root_select.context.clone();
        last_cte_name = Some(root_context.name.clone());

        file_id_ref = root_context.column_ref("file_id");
        item_id_ref = root_context.column_ref("item_id");
        if state.item_data_query {
            data_id_ref = Some(root_context.column_ref("data_id"));
        }

        joined_tables = JoinedTables::default();
    } else {
        let (query, file_id, item_id, data_id) =
            get_empty_query(&mut joined_tables, state.item_data_query, state.entity);
        full_query = query;
        file_id_ref = file_id;
        item_id_ref = item_id;
        data_id_ref = data_id;
    }

    full_query = add_inner_joins(
        full_query,
        state.entity,
        item_id_ref.clone(),
        file_id_ref.clone(),
        data_id_ref.clone(),
        &mut joined_tables,
    );

    if count_query {
        let (count_query, extra_columns) = if input_query.partition_by.is_none() {
            let mut count_query = Query::select();
            count_query
                .expr_as(Func::count(Expr::col(Asterisk)), Alias::new("total"))
                .from_subquery(full_query, Alias::new("wrapped_query"));
            (count_query, HashMap::new())
        } else {
            let partition_by = input_query.partition_by.clone().unwrap_or_default();
            let mut partition_columns = partition_by
                .iter()
                .map(|col| get_column_expr(*col))
                .collect::<Vec<_>>();
            let mut partition_key = partition_columns
                .drain(..1)
                .next()
                .ok_or_else(|| PqlError::invalid("partition_by is empty"))?;
            for col in partition_columns {
                partition_key = partition_key.binary(BinOper::Custom("||"), col);
            }

            let mut wrapped_query = full_query.clone();
            wrapped_query.expr_as(partition_key, Alias::new("partition_key"));

            let mut count_query = Query::select();
            count_query
                .expr_as(
                    Func::count_distinct(Expr::col(Alias::new("partition_key"))),
                    Alias::new("total"),
                )
                .from_subquery(wrapped_query, Alias::new("wrapped_query"));
            (count_query, HashMap::new())
        };

        let with_clause =
            build_with_clause(&state, root_cte_name.as_deref(), last_cte_name.as_deref());

        return Ok(PqlBuilderResult {
            query: count_query,
            with_clause,
            extra_columns,
        });
    }

    let mut selected_columns = SelectedColumns::default();
    if used_filters {
        selected_columns.push("item_id");
        selected_columns.push("file_id");
    } else {
        selected_columns.push("file_id");
        selected_columns.push("item_id");
    }
    if state.item_data_query {
        selected_columns.push("data_id");
    }

    let mut join_targets: Vec<CteRef> = state.extra_columns.iter().map(|c| c.cte.clone()).collect();
    join_targets.extend(state.order_list.iter().map(|c| c.cte.clone()));

    full_query = add_joins(
        &join_targets,
        full_query,
        file_id_ref.clone(),
        data_id_ref.clone(),
        root_cte_name.as_deref(),
        last_cte_name.as_deref(),
    );

    full_query = add_select_columns(&mut input_query, full_query, &mut selected_columns);

    let (full_query, extra_columns) = add_extra_columns(
        full_query,
        &state,
        root_cte_name.as_deref(),
        &mut selected_columns,
    );

    let (mut full_query, order_specs, order_columns) = build_order_by(
        full_query,
        root_cte_name.as_deref(),
        input_query.partition_by.is_some(),
        &state.order_list,
        &input_query.order_by,
    );

    if let Some(partition_by) = input_query.partition_by.clone() {
        full_query = apply_partition_by(
            &partition_by,
            full_query,
            &selected_columns.order,
            &order_columns,
            &mut state,
        );
    } else {
        for order_spec in order_specs {
            full_query.order_by_expr_with_nulls(
                order_spec.expr,
                order_spec.order,
                order_spec.nulls,
            );
        }
    }

    let page = std::cmp::Ord::max(input_query.page, 1);
    if input_query.page_size >= 1 {
        let offset = (page - 1) * input_query.page_size;
        full_query
            .limit(input_query.page_size as u64)
            .offset(offset as u64);
    }

    let with_clause = build_with_clause(&state, root_cte_name.as_deref(), last_cte_name.as_deref());

    Ok(PqlBuilderResult {
        query: full_query,
        with_clause,
        extra_columns,
    })
}

fn raise_if_invalid(input_query: &PqlQuery) -> Result<(), PqlError> {
    if !matches!(input_query.entity, EntityType::Text) {
        if input_query.select.iter().copied().any(is_text_column) {
            return Err(PqlError::invalid(
                "Tried to select text columns in a non-text query",
            ));
        }
        if input_query
            .order_by
            .iter()
            .any(|order| is_text_order_field(order.order_by))
        {
            return Err(PqlError::invalid(
                "Tried to order by text columns in a non-text query",
            ));
        }
        if let Some(partition_by) = &input_query.partition_by {
            if partition_by.iter().copied().any(is_text_column) {
                return Err(PqlError::invalid(
                    "Tried to partition by text columns in a non-text query",
                ));
            }
        }
    }
    Ok(())
}

fn process_query_element(
    el: QueryElement,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    match el {
        QueryElement::And(op) => {
            let mut current = context.clone();
            for sub_element in op.and_ {
                current = process_query_element(sub_element, &current, state)?;
            }
            Ok(current)
        }
        QueryElement::Or(op) => {
            let mut iter = op.or_.into_iter();
            let first = iter
                .next()
                .ok_or_else(|| PqlError::invalid("OR operator has no operands"))?;
            let first_cte = process_query_element(first, context, state)?;
            let mut union_query = select_std_from_cte(&first_cte, state);
            for sub_element in iter {
                let sub_cte = process_query_element(sub_element, context, state)?;
                union_query.union(UnionType::Distinct, select_std_from_cte(&sub_cte, state));
            }
            let cte_name = format!("n{}_or", state.cte_counter);
            state.cte_counter += 1;
            let or_cte = create_cte(state, cte_name.clone(), union_query.to_owned());
            state.selects.insert(
                cte_name,
                FilterSelect {
                    select: select_std_from_cte(&or_cte, state),
                    context: or_cte.clone(),
                },
            );
            Ok(or_cte)
        }
        QueryElement::Not(op) => {
            let sub_cte = process_query_element(*op.not_, context, state)?;
            let mut query = select_std_from_cte(context, state);
            let mut join_cond = Cond::all().add(
                Expr::col(sub_cte.column_ref("file_id")).equals(context.column_ref("file_id")),
            );
            if state.item_data_query {
                join_cond = join_cond.add(
                    Expr::col(sub_cte.column_ref("data_id")).equals(context.column_ref("data_id")),
                );
            }
            query.left_join(Alias::new(sub_cte.name.as_str()), join_cond);
            let none_cond = if state.item_data_query {
                Expr::col(sub_cte.column_ref("data_id")).is_null()
            } else {
                Expr::col(sub_cte.column_ref("file_id")).is_null()
            };
            query.and_where(none_cond);

            let cte_name = format!("n{}_not_{}", state.cte_counter, sub_cte.name);
            state.cte_counter += 1;
            let not_cte = create_cte(state, cte_name.clone(), query.to_owned());
            state.selects.insert(
                cte_name,
                FilterSelect {
                    select: select_std_from_cte(&not_cte, state),
                    context: not_cte.clone(),
                },
            );
            Ok(not_cte)
        }
        QueryElement::Match(filter) => build_match_filter(&filter, context, state),
        QueryElement::MatchPath(filter) => {
            let cte = build_match_path_filter(&filter, context, state)?;
            if filter.sort.order_by {
                state.order_list.push(OrderByFilter {
                    cte: cte.clone(),
                    direction: filter.sort.direction,
                    priority: filter.sort.priority,
                    rrf: filter.sort.rrf.clone(),
                });
            }
            Ok(cte)
        }
        QueryElement::MatchText(filter) => {
            let cte = build_match_text_filter(&filter, context, state)?;
            if filter.sort.order_by {
                state.order_list.push(OrderByFilter {
                    cte: cte.clone(),
                    direction: filter.sort.direction,
                    priority: filter.sort.priority,
                    rrf: filter.sort.rrf.clone(),
                });
            }
            Ok(cte)
        }
        QueryElement::MatchTags(filter) => {
            let cte = build_match_tags_filter(&filter, context, state)?;
            if filter.sort.order_by {
                state.order_list.push(OrderByFilter {
                    cte: cte.clone(),
                    direction: filter.sort.direction,
                    priority: filter.sort.priority,
                    rrf: filter.sort.rrf.clone(),
                });
            }
            Ok(cte)
        }
        QueryElement::InBookmarks(filter) => {
            let cte = build_in_bookmarks_filter(&filter, context, state)?;
            if filter.sort.order_by {
                state.order_list.push(OrderByFilter {
                    cte: cte.clone(),
                    direction: filter.sort.direction,
                    priority: filter.sort.priority,
                    rrf: filter.sort.rrf.clone(),
                });
            }
            Ok(cte)
        }
        QueryElement::ProcessedBy(filter) => build_processed_by_filter(&filter, context, state),
        QueryElement::HasUnprocessedData(filter) => {
            build_has_unprocessed_filter(&filter, context, state)
        }
    }
}

fn create_cte(state: &mut QueryState, name: String, query: SelectStatement) -> CteRef {
    state.ctes.push(CteDefinition {
        name: name.clone(),
        query,
    });
    CteRef { name }
}

fn wrap_query(
    state: &mut QueryState,
    query: SelectStatement,
    context: &CteRef,
    cte_name: String,
) -> CteRef {
    let cte = create_cte(state, cte_name.clone(), query.clone());
    state.selects.insert(
        cte_name,
        FilterSelect {
            select: query,
            context: context.clone(),
        },
    );
    cte
}

fn build_match_filter(
    filter: &Match,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let expression = build_matches_expression(&filter.match_, state.item_data_query)?;
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
            Expr::col((Setters::Table, Setters::Id)).equals((ItemData::Table, ItemData::SetterId)),
        );
    }
    query.and_where(expression);

    let cte_name = format!("n{}_Match", state.cte_counter);
    let cte = wrap_query(state, query, context, cte_name);
    state.cte_counter += 1;
    Ok(cte)
}

fn build_match_path_filter(
    filter: &MatchPath,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let mut query = select_std_from_cte(context, state);
    let join_cond = Expr::cust("files_path_fts.rowid").equals(context.column_ref("file_id"));
    query.join(JoinType::InnerJoin, FilesPathFts::Table, join_cond);

    let match_column = if filter.match_path.filename_only {
        Expr::col((FilesPathFts::Table, FilesPathFts::Filename))
    } else {
        Expr::col((FilesPathFts::Table, FilesPathFts::Path))
    };
    query.and_where(
        match_column.binary(
            SqliteBinOper::Match,
            Expr::val(filter.match_path.r#match.clone()),
        ),
    );

    if !state.is_count_query {
        add_sortable_rank_column(&mut query, &filter.sort)?;
    }

    let cte_name = format!("n{}_MatchPath", state.cte_counter);
    let mut context_for_wrap = context.clone();
    let mut final_query = query;

    if !state.is_count_query && (filter.sort.gt.is_some() || filter.sort.lt.is_some()) {
        let wrapped_name = format!("wrapped_{cte_name}");
        let wrapped_cte = create_cte(state, wrapped_name.clone(), final_query.to_owned());
        context_for_wrap = wrapped_cte.clone();

        let mut wrapped_query = Query::select();
        wrapped_query
            .from(Alias::new(wrapped_name.as_str()))
            .column((Alias::new(wrapped_name.as_str()), Asterisk));
        if let Some(gt) = &filter.sort.gt {
            wrapped_query.and_where(
                Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                    .gt(scalar_to_expr(gt)),
            );
        }
        if let Some(lt) = &filter.sort.lt {
            wrapped_query.and_where(
                Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                    .lt(scalar_to_expr(lt)),
            );
        }
        final_query = wrapped_query;
    }

    let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
    state.cte_counter += 1;
    if !state.is_count_query {
        if let Some(alias) = &filter.sort.select_as {
            state.extra_columns.push(ExtraColumn {
                column: "order_rank".to_string(),
                cte: cte.clone(),
                alias: alias.clone(),
            });
        }
    }
    Ok(cte)
}

fn build_match_text_filter(
    filter: &MatchText,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let args = &filter.match_text;
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
            Expr::col((ExtractedText::Table, ExtractedText::Id)).equals((ItemData::Table, ItemData::Id)),
        );
        query.join(
            JoinType::InnerJoin,
            ExtractedTextFts::Table,
            Expr::cust("extracted_text_fts.rowid").equals((ExtractedText::Table, ExtractedText::Id)),
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
            let mut rownum_query = Query::select();
            rownum_query
                .from(Alias::new(match_cte.name.as_str()))
                .column((Alias::new(match_cte.name.as_str()), Asterisk));
            let mut window = WindowStatement::new();
            window.partition_by(match_cte.column_ref("file_id"));
            window.order_by_expr(match_cte.column_expr("rank"), Order::Asc);
            rownum_query.expr_window_as(Expr::cust("row_number()"), window, Alias::new("rn"));
            let rownum_cte =
                create_cte(state, format!("rownum_{cte_name}"), rownum_query.to_owned());

            let mut select_query = Query::select();
            select_query
                .from(Alias::new(rownum_cte.name.as_str()))
                .column((Alias::new(rownum_cte.name.as_str()), Asterisk))
                .and_where(
                    Expr::col((Alias::new(rownum_cte.name.as_str()), Alias::new("rn"))).eq(1),
                );
            final_query = select_query;
            context_for_wrap = rownum_cte;

            if !state.is_count_query {
                add_rank_column_expr(&mut final_query, &filter.sort, Expr::cust("rank"))?;
            }
        } else {
            apply_group_by(&mut final_query, get_std_group_by(context, state));
            if !state.is_count_query {
                let rank_expr = if args.filter_only {
                    Expr::val(1)
                } else {
                    Func::min(Expr::cust("rank")).into()
                };
                add_rank_column_expr(&mut final_query, &filter.sort, rank_expr)?;
            }
        }

        let (final_query, context_for_wrap) =
            apply_sort_bounds(state, final_query, context_for_wrap, &cte_name, &filter.sort);

        let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
        state.cte_counter += 1;
        if !state.is_count_query {
            if let Some(alias) = &filter.sort.select_as {
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
        }
        return Ok(cte);
    }

    let mut query = select_std_from_cte(context, state);
    query.join(
        JoinType::InnerJoin,
        ItemData::Table,
        Expr::col((ItemData::Table, ItemData::Id)).equals(context.column_ref("data_id")),
    );
    query.join(
        JoinType::InnerJoin,
        Setters::Table,
        Expr::col((Setters::Table, Setters::Id)).equals((ItemData::Table, ItemData::SetterId)),
    );
    query.join(
        JoinType::InnerJoin,
        ExtractedText::Table,
        Expr::col((ExtractedText::Table, ExtractedText::Id)).equals(context.column_ref("data_id")),
    );
    query.join(
        JoinType::InnerJoin,
        ExtractedTextFts::Table,
        Expr::cust("extracted_text_fts.rowid").equals(context.column_ref("data_id")),
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
            .column((Alias::new(match_cte.name.as_str()), Asterisk));
        final_query = select_query;
    }

    if !state.is_count_query {
        let rank_expr = if args.filter_only {
            Expr::val(1)
        } else {
            Expr::cust("rank")
        };
        add_rank_column_expr(&mut final_query, &filter.sort, rank_expr)?;
    }

    let (final_query, context_for_wrap) =
        apply_sort_bounds(state, final_query, context_for_wrap, &cte_name, &filter.sort);

    let cte = wrap_query(state, final_query, &context_for_wrap, cte_name);
    state.cte_counter += 1;
    if !state.is_count_query {
        if let Some(alias) = &filter.sort.select_as {
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
    }
    Ok(cte)
}

fn build_match_tags_filter(
    filter: &MatchTags,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let args = &filter.match_tags;
    let cte_name = format!("n{}_MatchTags", state.cte_counter);

    let mut conditions = Vec::new();
    let tag_values = args
        .tags
        .iter()
        .cloned()
        .map(Expr::val)
        .collect::<Vec<_>>();
    conditions.push(Expr::col((Tags::Table, Tags::Name)).is_in(tag_values));
    if args.min_confidence > 0.0 {
        conditions.push(
            Expr::col((TagsItems::Table, TagsItems::Confidence)).gte(args.min_confidence),
        );
    }
    if !args.setters.is_empty() {
        let setters = args.setters.iter().cloned().map(Expr::val).collect::<Vec<_>>();
        conditions.push(Expr::col((Setters::Table, Setters::Name)).is_in(setters));
    }
    if !args.namespaces.is_empty() {
        let mut namespace_exprs = Vec::new();
        for namespace in &args.namespaces {
            namespace_exprs.push(
                Expr::col((Tags::Table, Tags::Namespace)).like(format!("{namespace}%")),
            );
        }
        conditions.push(combine_or(namespace_exprs)?);
    }

    let mut matching_items_select = select_std_from_cte(context, state);
    let join_cond = Cond::all()
        .add(
            Expr::col((ItemData::Table, ItemData::ItemId))
                .equals(context.column_ref("item_id")),
        )
        .add(Expr::col((ItemData::Table, ItemData::DataType)).eq("tags"));
    matching_items_select.join(JoinType::InnerJoin, ItemData::Table, join_cond);
    matching_items_select.join(
        JoinType::InnerJoin,
        Setters::Table,
        Expr::col((Setters::Table, Setters::Id)).equals((ItemData::Table, ItemData::SetterId)),
    );
    matching_items_select.join(
        JoinType::InnerJoin,
        TagsItems::Table,
        Expr::col((TagsItems::Table, TagsItems::ItemDataId)).equals((ItemData::Table, ItemData::Id)),
    );
    matching_items_select.join(
        JoinType::InnerJoin,
        Tags::Table,
        Expr::col((Tags::Table, Tags::Id)).equals((TagsItems::Table, TagsItems::TagId)),
    );
    for condition in conditions {
        matching_items_select.and_where(condition);
    }
    apply_group_by(&mut matching_items_select, get_std_group_by(context, state));

    let mut having_clauses = Vec::new();
    if args.all_setters_required {
        let setter_tag = Expr::col((ItemData::Table, ItemData::SetterId))
            .binary(BinOper::Custom("||"), Expr::val("-"))
            .binary(BinOper::Custom("||"), Expr::col((Tags::Table, Tags::Name)));
        let expected = (args.tags.len() * args.setters.len()) as i64;
        having_clauses.push(Func::count_distinct(setter_tag).eq(expected));
    } else {
        let expected = args.tags.len() as i64;
        having_clauses.push(
            Func::count_distinct(Expr::col((Tags::Table, Tags::Name))).eq(expected),
        );
    }
    if args.match_any && args.tags.len() > 1 {
        having_clauses.clear();
    }
    for clause in having_clauses {
        matching_items_select.and_having(clause);
    }

    if !state.is_count_query {
        let avg_confidence =
            Func::avg(Expr::col((TagsItems::Table, TagsItems::Confidence))).into();
        add_rank_column_expr(&mut matching_items_select, &filter.sort, avg_confidence)?;
    }

    let matching_items =
        create_cte(state, format!("match_{cte_name}"), matching_items_select.to_owned());

    let mut query = select_std_from_cte(context, state);
    if !state.is_count_query {
        query.expr_as(matching_items.column_expr("order_rank"), Alias::new("order_rank"));
    }
    let join_condition = if state.item_data_query {
        Expr::col(matching_items.column_ref("data_id")).equals(context.column_ref("data_id"))
    } else {
        Expr::col(matching_items.column_ref("file_id")).equals(context.column_ref("file_id"))
    };
    query.join(
        JoinType::InnerJoin,
        Alias::new(matching_items.name.as_str()),
        join_condition,
    );

    let (query, context_for_wrap) =
        apply_sort_bounds(state, query, context.clone(), &cte_name, &filter.sort);
    let cte = wrap_query(state, query, &context_for_wrap, cte_name);
    state.cte_counter += 1;
    if !state.is_count_query {
        if let Some(alias) = &filter.sort.select_as {
            state.extra_columns.push(ExtraColumn {
                column: "order_rank".to_string(),
                cte: cte.clone(),
                alias: alias.clone(),
            });
        }
    }
    Ok(cte)
}

fn build_in_bookmarks_filter(
    filter: &InBookmarks,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let args = &filter.in_bookmarks;
    let cte_name = format!("n{}_InBookmarks", state.cte_counter);
    let user_data = Alias::new("user_data");

    let mut criteria = Vec::new();
    if !args.namespaces.is_empty() {
        let namespaces = args
            .namespaces
            .iter()
            .cloned()
            .map(Expr::val)
            .collect::<Vec<_>>();
        let in_condition =
            Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Namespace))
                .is_in(namespaces);
        if args.sub_ns {
            let mut namespace_exprs = Vec::new();
            namespace_exprs.push(in_condition);
            for namespace in &args.namespaces {
                namespace_exprs.push(
                    Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Namespace))
                        .like(format!("{namespace}.%")),
                );
            }
            criteria.push(combine_or(namespace_exprs)?);
        } else {
            criteria.push(in_condition);
        }
    }

    if args.include_wildcard {
        let user_expr =
            Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::User)).eq(args.user.clone());
        let wildcard_expr =
            Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::User)).eq("*");
        criteria.push(user_expr.or(wildcard_expr));
    } else {
        criteria.push(
            Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::User))
                .eq(args.user.clone()),
        );
    }

    let mut query = select_std_from_cte(context, state);
    query.join(
        JoinType::InnerJoin,
        Files::Table,
        Expr::col((Files::Table, Files::Id)).equals(context.column_ref("file_id")),
    );
    query.join(
        JoinType::InnerJoin,
        (user_data.clone(), Bookmarks::Table),
        Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::Sha256))
            .equals((Files::Table, Files::Sha256)),
    );
    for condition in criteria {
        query.and_where(condition);
    }
    apply_group_by(&mut query, get_std_group_by(context, state));

    if !state.is_count_query {
        let rank_expr =
            Func::max(Expr::col((user_data.clone(), Bookmarks::Table, Bookmarks::TimeAdded)))
                .into();
        add_rank_column_expr(&mut query, &filter.sort, rank_expr)?;
    }

    let (query, context_for_wrap) =
        apply_sort_bounds(state, query, context.clone(), &cte_name, &filter.sort);
    let cte = wrap_query(state, query, &context_for_wrap, cte_name);
    state.cte_counter += 1;
    if !state.is_count_query {
        if let Some(alias) = &filter.sort.select_as {
            state.extra_columns.push(ExtraColumn {
                column: "order_rank".to_string(),
                cte: cte.clone(),
                alias: alias.clone(),
            });
        }
    }
    Ok(cte)
}

fn build_processed_by_filter(
    filter: &ProcessedBy,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let cte_name = format!("n{}_ProcessedBy", state.cte_counter);
    let mut query = select_std_from_cte(context, state);
    let join_cond = if state.item_data_query {
        Expr::col((ItemData::Table, ItemData::SourceId)).equals(context.column_ref("data_id"))
    } else {
        Expr::col((ItemData::Table, ItemData::ItemId)).equals(context.column_ref("item_id"))
    };
    query.join(JoinType::InnerJoin, ItemData::Table, join_cond);
    query.join(
        JoinType::InnerJoin,
        Setters::Table,
        Expr::col((Setters::Table, Setters::Id)).equals((ItemData::Table, ItemData::SetterId)),
    );
    query.and_where(Expr::col((Setters::Table, Setters::Name)).eq(filter.processed_by.clone()));
    apply_group_by(&mut query, get_std_group_by(context, state));

    let cte = wrap_query(state, query, context, cte_name);
    state.cte_counter += 1;
    Ok(cte)
}

fn build_has_unprocessed_filter(
    filter: &HasUnprocessedData,
    context: &CteRef,
    state: &mut QueryState,
) -> Result<CteRef, PqlError> {
    let args = &filter.has_data_unprocessed;
    let cte_name = format!("n{}_HasUnprocessedData", state.cte_counter);

    let src_alias = Alias::new("src_item_data");
    let derived_alias = Alias::new("derived_data");

    let mut not_exists_subquery = Query::select();
    not_exists_subquery.expr(Expr::val(1));
    not_exists_subquery.from_as(ItemData::Table, derived_alias.clone());
    not_exists_subquery.join(
        JoinType::InnerJoin,
        Setters::Table,
        Expr::col((Setters::Table, Setters::Id)).equals((derived_alias.clone(), ItemData::SetterId)),
    );
    not_exists_subquery.and_where(
        Expr::col((derived_alias.clone(), ItemData::SourceId))
            .equals((src_alias.clone(), ItemData::Id)),
    );
    not_exists_subquery.and_where(
        Expr::col((Setters::Table, Setters::Name)).eq(args.setter_name.clone()),
    );

    let mut query = select_std_from_cte(context, state);
    query.join_as(
        JoinType::InnerJoin,
        ItemData::Table,
        src_alias.clone(),
        Expr::col((src_alias.clone(), ItemData::ItemId)).equals(context.column_ref("item_id")),
    );
    let data_types = args
        .data_types
        .iter()
        .cloned()
        .map(Expr::val)
        .collect::<Vec<_>>();
    query.and_where(Expr::col((src_alias.clone(), ItemData::DataType)).is_in(data_types));
    query.and_where(Expr::col((src_alias.clone(), ItemData::IsPlaceholder)).eq(0));
    query.and_where(Expr::not_exists(not_exists_subquery.to_owned()));
    apply_group_by(&mut query, get_std_group_by(context, state));

    let cte = wrap_query(state, query, context, cte_name);
    state.cte_counter += 1;
    Ok(cte)
}

fn build_matches_expression(
    matches: &Matches,
    allow_text: bool,
) -> Result<Expr, PqlError> {
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

fn build_match_ops_expression(
    ops: &MatchOps,
    allow_text: bool,
) -> Result<Expr, PqlError> {
    let mut expressions = Vec::new();

    if let Some(value) = &ops.eq {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Eq, allow_text)?);
    }
    if let Some(value) = &ops.neq {
        expressions.extend(build_match_value_expressions(value, MatchOperator::Neq, allow_text)?);
    }
    if let Some(value) = &ops.in_ {
        expressions.extend(build_match_values_expressions(value, MatchOperator::In, allow_text)?);
    }
    if let Some(value) = &ops.nin {
        expressions.extend(build_match_values_expressions(value, MatchOperator::NotIn, allow_text)?);
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
            return Err(PqlError::invalid(
                "Text columns are not allowed in this context",
            ));
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
                return Err(PqlError::invalid(
                    "Invalid operator for MatchValue",
                ))
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
            return Err(PqlError::invalid(
                "Text columns are not allowed in this context",
            ));
        }
        let col_expr = get_column_expr(column);
        let expr = match operator {
            MatchOperator::In => build_in_expression(&col_expr, value, false)?,
            MatchOperator::NotIn => build_in_expression(&col_expr, value, true)?,
            MatchOperator::StartsWith => build_like_expression(&col_expr, value, LikeKind::StartsWith)?,
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
                return Err(PqlError::invalid(
                    "Invalid operator for MatchValues",
                ))
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

fn build_in_expression(
    col_expr: &Expr,
    value: FieldValues,
    negate: bool,
) -> Result<Expr, PqlError> {
    let values = match value {
        FieldValues::Single(_) => {
            return Err(PqlError::invalid(
                "Invalid operator for single value",
            ))
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
        fields.push((
            Column::SubtitleTracks,
            convert_one_or_many(value, map_int),
        ));
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
        fields.push((
            Column::LanguageConfidence,
            convert_one_or_many(value, map_float),
        ));
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

fn add_sortable_rank_column(
    query: &mut SelectStatement,
    sort: &SortableOptions,
) -> Result<(), PqlError> {
    add_rank_column_expr(query, sort, Expr::cust("rank"))
}

fn add_rank_column_expr(
    query: &mut SelectStatement,
    sort: &SortableOptions,
    rank_expr: Expr,
) -> Result<(), PqlError> {
    if sort.row_n && (sort.order_by || sort.select_as.is_some()) {
        let mut window = WindowStatement::new();
        let order = direction_to_order(sort.row_n_direction);
        window.order_by_expr(rank_expr, order);
        query.expr_window_as(Expr::cust("row_number()"), window, Alias::new("order_rank"));
    } else {
        query.expr_as(rank_expr, Alias::new("order_rank"));
    }
    Ok(())
}

fn scalar_to_expr(value: &ScalarValue) -> Expr {
    match value {
        ScalarValue::Int(v) => Expr::val(*v),
        ScalarValue::Float(v) => Expr::val(*v),
        ScalarValue::String(v) => Expr::val(v.clone()),
    }
}

fn apply_sort_bounds(
    state: &mut QueryState,
    query: SelectStatement,
    context: CteRef,
    cte_name: &str,
    sort: &SortableOptions,
) -> (SelectStatement, CteRef) {
    if state.is_count_query || (sort.gt.is_none() && sort.lt.is_none()) {
        return (query, context);
    }

    let wrapped_name = format!("wrapped_{cte_name}");
    let wrapped_cte = create_cte(state, wrapped_name.clone(), query.to_owned());
    let mut wrapped_query = Query::select();
    wrapped_query
        .from(Alias::new(wrapped_name.as_str()))
        .column((Alias::new(wrapped_name.as_str()), Asterisk));
    if let Some(gt) = &sort.gt {
        wrapped_query.and_where(
            Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                .gt(scalar_to_expr(gt)),
        );
    }
    if let Some(lt) = &sort.lt {
        wrapped_query.and_where(
            Expr::col((Alias::new(wrapped_name.as_str()), Alias::new("order_rank")))
                .lt(scalar_to_expr(lt)),
        );
    }
    (wrapped_query, wrapped_cte)
}

fn select_std_from_cte(cte: &CteRef, state: &QueryState) -> SelectStatement {
    let mut query = Query::select();
    query
        .from(Alias::new(cte.name.as_str()))
        .column(cte.column_ref("item_id"))
        .column(cte.column_ref("file_id"));
    if state.item_data_query {
        query.column(cte.column_ref("data_id"));
    }
    query
}

fn get_std_group_by(cte: &CteRef, state: &QueryState) -> Vec<ColumnRef> {
    if state.item_data_query {
        vec![cte.column_ref("data_id"), cte.column_ref("file_id")]
    } else {
        vec![cte.column_ref("file_id")]
    }
}

fn apply_group_by(query: &mut SelectStatement, columns: Vec<ColumnRef>) {
    for column in columns {
        query.group_by_col(column);
    }
}

fn add_select_columns(
    input_query: &mut PqlQuery,
    mut query: SelectStatement,
    selected_columns: &mut SelectedColumns,
) -> SelectStatement {
    let mut seen: HashSet<&'static str> = HashSet::new();
    let mut deduped = Vec::new();
    for col in input_query.select.drain(..) {
        let name = column_name(col);
        if seen.insert(name) {
            deduped.push(col);
        }
    }
    input_query.select = deduped;

    input_query
        .select
        .retain(|col| !matches!(col, Column::FileId | Column::ItemId | Column::DataId));

    for col in &input_query.select {
        let name = column_name(*col);
        query.expr_as(get_column_expr(*col), Alias::new(name));
        selected_columns.push(name);
    }
    query
}

fn add_extra_columns(
    mut query: SelectStatement,
    state: &QueryState,
    root_cte_name: Option<&str>,
    selected_columns: &mut SelectedColumns,
) -> (SelectStatement, HashMap<String, String>) {
    let mut column_aliases = HashMap::new();
    for (index, extra_column) in state.extra_columns.iter().enumerate() {
        let column_name = extra_column.column.as_str();
        if Some(extra_column.cte.name.as_str()) == root_cte_name {
            column_aliases.insert(column_name.to_string(), extra_column.alias.clone());
            selected_columns.push(column_name);
            continue;
        }
        let label = format!("extra_{index}");
        query.expr_as(
            extra_column.cte.column_expr(column_name),
            Alias::new(label.as_str()),
        );
        column_aliases.insert(label.clone(), extra_column.alias.clone());
        selected_columns.push(&label);
    }
    (query, column_aliases)
}

fn add_joins(
    targets: &[CteRef],
    mut query: SelectStatement,
    file_id_ref: ColumnRef,
    data_id_ref: Option<ColumnRef>,
    root_cte_name: Option<&str>,
    last_cte_name: Option<&str>,
) -> SelectStatement {
    let mut seen = HashSet::new();
    for target in targets {
        if !seen.insert(target.name.clone()) {
            continue;
        }
        if Some(target.name.as_str()) == root_cte_name {
            continue;
        }
        if Some(target.name.as_str()) == last_cte_name {
            continue;
        }

        let mut join_cond =
            Cond::all().add(Expr::col(target.column_ref("file_id")).equals(file_id_ref.clone()));
        if let Some(data_id_ref) = data_id_ref.clone() {
            join_cond =
                join_cond.add(Expr::col(target.column_ref("data_id")).equals(data_id_ref.clone()));
        }
        query.left_join(Alias::new(target.name.as_str()), join_cond);
    }
    query
}

fn add_inner_joins(
    mut query: SelectStatement,
    entity: EntityType,
    item_id_ref: ColumnRef,
    file_id_ref: ColumnRef,
    data_id_ref: Option<ColumnRef>,
    joined_tables: &mut JoinedTables,
) -> SelectStatement {
    if !joined_tables.has(BaseTable::Items) {
        query.join(
            JoinType::InnerJoin,
            Items::Table,
            Expr::col((Items::Table, Items::Id)).equals(item_id_ref.clone()),
        );
        joined_tables.mark(BaseTable::Items);
    }

    if !joined_tables.has(BaseTable::Files) {
        query.join(
            JoinType::InnerJoin,
            Files::Table,
            Expr::col((Files::Table, Files::Id)).equals(file_id_ref.clone()),
        );
        joined_tables.mark(BaseTable::Files);
    }

    if let Some(data_id_ref) = data_id_ref {
        if !joined_tables.has(BaseTable::ItemData) {
            query.join(
                JoinType::InnerJoin,
                ItemData::Table,
                Expr::col((ItemData::Table, ItemData::Id)).equals(data_id_ref.clone()),
            );
            joined_tables.mark(BaseTable::ItemData);
        }
        if !joined_tables.has(BaseTable::Setters) {
            query.join(
                JoinType::InnerJoin,
                Setters::Table,
                Expr::col((Setters::Table, Setters::Id))
                    .equals((ItemData::Table, ItemData::SetterId)),
            );
            joined_tables.mark(BaseTable::Setters);
        }
        if matches!(entity, EntityType::Text) && !joined_tables.has(BaseTable::ExtractedText) {
            query.join(
                JoinType::InnerJoin,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id)).equals(data_id_ref.clone()),
            );
            joined_tables.mark(BaseTable::ExtractedText);
        }
    }

    query
}

fn get_empty_query(
    joined_tables: &mut JoinedTables,
    item_data_query: bool,
    entity: EntityType,
) -> (SelectStatement, ColumnRef, ColumnRef, Option<ColumnRef>) {
    let mut query = Query::select();
    query
        .expr_as(Expr::col((Files::Table, Files::Id)), Alias::new("file_id"))
        .expr_as(
            Expr::col((Files::Table, Files::ItemId)),
            Alias::new("item_id"),
        )
        .from(Files::Table);
    joined_tables.mark(BaseTable::Files);

    if item_data_query {
        let join_cond = Cond::all()
            .add(
                Expr::col((ItemData::Table, ItemData::ItemId))
                    .equals((Files::Table, Files::ItemId)),
            )
            .add(Expr::col((ItemData::Table, ItemData::DataType)).eq(entity_to_data_type(entity)));
        query.join(JoinType::InnerJoin, ItemData::Table, join_cond);
        query.expr_as(
            Expr::col((ItemData::Table, ItemData::Id)),
            Alias::new("data_id"),
        );
        joined_tables.mark(BaseTable::ItemData);

        if matches!(entity, EntityType::Text) {
            query.join(
                JoinType::InnerJoin,
                ExtractedText::Table,
                Expr::col((ExtractedText::Table, ExtractedText::Id))
                    .equals((ItemData::Table, ItemData::Id)),
            );
            joined_tables.mark(BaseTable::ExtractedText);
        }

        return (
            query,
            (Files::Table, Files::Id).into_column_ref(),
            (Files::Table, Files::ItemId).into_column_ref(),
            Some((ItemData::Table, ItemData::Id).into_column_ref()),
        );
    }

    (
        query,
        (Files::Table, Files::Id).into_column_ref(),
        (Files::Table, Files::ItemId).into_column_ref(),
        None,
    )
}

fn build_order_by(
    mut query: SelectStatement,
    root_cte_name: Option<&str>,
    select_conds: bool,
    order_list: &[OrderByFilter],
    order_args: &[OrderArgs],
) -> (SelectStatement, Vec<OrderSpec>, Vec<OrderByColumn>) {
    let combined = combine_order_lists(order_list, order_args);
    let mut order_specs = Vec::new();
    let mut order_columns = Vec::new();

    for (index, spec) in combined.into_iter().enumerate() {
        match spec {
            OrderItem::Args(args) => {
                let (query_out, order_spec, order_column) =
                    apply_order_args(query, &args, index, select_conds);
                query = query_out;
                order_specs.push(order_spec);
                if let Some(order_column) = order_column {
                    order_columns.push(order_column);
                }
            }
            OrderItem::Filter(args) => {
                let (query_out, order_spec, order_column) =
                    apply_order_filter(query, &args, index, select_conds, root_cte_name);
                query = query_out;
                order_specs.push(order_spec);
                if let Some(order_column) = order_column {
                    order_columns.push(order_column);
                }
            }
            OrderItem::FilterGroup(group) => {
                let (query_out, order_spec, order_column) =
                    apply_coalesce_order_filters(query, &group, index, select_conds, root_cte_name);
                query = query_out;
                order_specs.push(order_spec);
                if let Some(order_column) = order_column {
                    order_columns.push(order_column);
                }
            }
        }
    }

    (query, order_specs, order_columns)
}

#[derive(Clone, Debug)]
enum OrderItem {
    Args(OrderArgs),
    Filter(OrderByFilter),
    FilterGroup(Vec<OrderByFilter>),
}

fn combine_order_lists(order_list: &[OrderByFilter], order_args: &[OrderArgs]) -> Vec<OrderItem> {
    let mut combined: Vec<(OrderItem, i32, usize, i32)> = Vec::new();
    for (idx, item) in order_list.iter().enumerate() {
        combined.push((OrderItem::Filter(item.clone()), item.priority, idx, 0));
    }
    for (idx, item) in order_args.iter().enumerate() {
        combined.push((OrderItem::Args(item.clone()), item.priority, idx, 1));
    }

    combined.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.3.cmp(&b.3))
            .then_with(|| a.2.cmp(&b.2))
    });

    let mut grouped = Vec::new();
    let mut i = 0;
    while i < combined.len() {
        match combined[i].0.clone() {
            OrderItem::Filter(filter) => {
                let priority = combined[i].1;
                let mut group = vec![filter];
                let mut j = i + 1;
                while j < combined.len() {
                    if let OrderItem::Filter(next_filter) = &combined[j].0 {
                        if combined[j].1 == priority {
                            group.push(next_filter.clone());
                            j += 1;
                            continue;
                        }
                    }
                    break;
                }
                if group.len() > 1 {
                    grouped.push(OrderItem::FilterGroup(group));
                } else {
                    grouped.push(OrderItem::Filter(group.remove(0)));
                }
                i = j;
            }
            other => {
                grouped.push(other);
                i += 1;
            }
        }
    }

    grouped
}

fn get_order_by_and_direction(args: &OrderArgs) -> (OrderByField, Order) {
    let order_by = args.order_by;
    let order = match args.order {
        Some(direction) => direction_to_order(direction),
        None => {
            if matches!(order_by, OrderByField::LastModified) {
                Order::Desc
            } else {
                Order::Asc
            }
        }
    };
    (order_by, order)
}

fn apply_order_args(
    mut query: SelectStatement,
    args: &OrderArgs,
    index: usize,
    select_conds: bool,
) -> (SelectStatement, OrderSpec, Option<OrderByColumn>) {
    let (order_by, order) = get_order_by_and_direction(args);
    let expr = get_order_by_expr(order_by);
    let order_spec = OrderSpec {
        expr: expr.clone(),
        order: order.clone(),
        nulls: NullOrdering::Last,
    };

    let order_column = if select_conds {
        let label = format!("o{index}_{}", order_by_name(order_by));
        query.expr_as(expr, Alias::new(label.as_str()));
        Some(OrderByColumn::Label { label, order })
    } else {
        None
    };

    (query, order_spec, order_column)
}

fn apply_order_filter(
    mut query: SelectStatement,
    args: &OrderByFilter,
    index: usize,
    select_conds: bool,
    root_cte_name: Option<&str>,
) -> (SelectStatement, OrderSpec, Option<OrderByColumn>) {
    let order = direction_to_order(args.direction);
    let expr = if Some(args.cte.name.as_str()) == root_cte_name {
        Expr::col(Alias::new("order_rank"))
    } else {
        Expr::col((Alias::new(args.cte.name.as_str()), Alias::new("order_rank")))
    };
    let order_spec = OrderSpec {
        expr: expr.clone(),
        order: order.clone(),
        nulls: NullOrdering::Last,
    };

    let order_column = if select_conds {
        if Some(args.cte.name.as_str()) == root_cte_name {
            Some(OrderByColumn::Label {
                label: "order_rank".to_string(),
                order,
            })
        } else {
            let label = format!("o{index}_{}_rank", args.cte.name);
            query.expr_as(expr, Alias::new(label.as_str()));
            Some(OrderByColumn::Label { label, order })
        }
    } else {
        None
    };

    (query, order_spec, order_column)
}

fn apply_coalesce_order_filters(
    mut query: SelectStatement,
    args: &[OrderByFilter],
    index: usize,
    select_conds: bool,
    root_cte_name: Option<&str>,
) -> (SelectStatement, OrderSpec, Option<OrderByColumn>) {
    let order = direction_to_order(args[0].direction);
    let enable_rrf = args[0].rrf.is_some();
    let mut columns = Vec::new();
    let mut select_labels = Vec::new();
    let mut rrfs = Vec::new();

    for spec in args {
        let expr = if Some(spec.cte.name.as_str()) == root_cte_name {
            Expr::col(Alias::new("order_rank"))
        } else {
            Expr::col((Alias::new(spec.cte.name.as_str()), Alias::new("order_rank")))
        };
        columns.push(expr.clone());
        if enable_rrf {
            rrfs.push(spec.rrf.clone().unwrap_or_default());
        }
        if select_conds {
            if Some(spec.cte.name.as_str()) == root_cte_name {
                select_labels.push("order_rank".to_string());
            } else {
                let label = format!("o{index}_{}_rank", spec.cte.name);
                query.expr_as(expr, Alias::new(label.as_str()));
                select_labels.push(label);
            }
        }
    }

    let coalesced_expr = build_coalesced_expr(
        &columns,
        order.clone(),
        if enable_rrf { Some(rrfs.clone()) } else { None },
    );

    let order_spec = OrderSpec {
        expr: coalesced_expr,
        order: order.clone(),
        nulls: NullOrdering::Last,
    };

    let order_column = if select_conds {
        Some(OrderByColumn::Coalesce {
            labels: select_labels,
            order: order.clone(),
            rrfs: if enable_rrf { Some(rrfs) } else { None },
        })
    } else {
        None
    };

    (query, order_spec, order_column)
}

fn build_coalesced_expr(columns: &[Expr], order: Order, rrfs: Option<Vec<Rrf>>) -> Expr {
    if let Some(rrfs) = rrfs {
        let mut total: Option<Expr> = None;
        for (col, rrf) in columns.iter().zip(rrfs.iter()) {
            let rank = Func::coalesce([col.clone(), Expr::cust(VERY_LARGE_NUMBER)]);
            let denom = Expr::cust(rrf.k.to_string()).add(rank);
            let term = Expr::cust("1")
                .div(denom)
                .mul(Expr::cust(rrf.weight.to_string()));
            total = Some(match total {
                Some(acc) => acc.add(term),
                None => term,
            });
        }
        return total.unwrap_or_else(|| Expr::cust("0"));
    }

    let fallback = if order == Order::Asc {
        VERY_LARGE_NUMBER
    } else {
        VERY_SMALL_NUMBER
    };
    let exprs = columns
        .iter()
        .map(|col| Func::coalesce([col.clone(), Expr::cust(fallback)]).into())
        .collect::<Vec<Expr>>();
    if order == Order::Asc {
        Func::cust("min").args(exprs).into()
    } else {
        Func::cust("max").args(exprs).into()
    }
}

fn apply_partition_by(
    partition_by: &[Column],
    mut query: SelectStatement,
    selected_columns: &[String],
    order_columns: &[OrderByColumn],
    state: &mut QueryState,
) -> SelectStatement {
    for col in partition_by {
        let name = column_name(*col);
        let label = format!("part_{name}");
        query.expr_as(get_column_expr(*col), Alias::new(label.as_str()));
    }

    let select_cte = create_cte(state, "select_cte".to_string(), query.to_owned());

    let mut window = WindowStatement::new();
    for col in partition_by {
        let label = format!("part_{}", column_name(*col));
        window.partition_by((Alias::new("select_cte"), Alias::new(label.as_str())));
    }
    for order_col in order_columns {
        let order_spec = order_spec_for_alias(order_col, "select_cte");
        window.order_by_expr_with_nulls(order_spec.expr, order_spec.order, order_spec.nulls);
    }

    let mut partition_query = Query::select();
    partition_query
        .from(Alias::new("select_cte"))
        .column((Alias::new("select_cte"), Asterisk))
        .expr_window_as(
            Expr::cust("row_number()"),
            window,
            Alias::new("partition_rownum"),
        );

    let partition_cte = create_cte(
        state,
        "partition_cte".to_string(),
        partition_query.to_owned(),
    );

    let mut outer_query = Query::select();
    outer_query.from(Alias::new("partition_cte"));
    for name in selected_columns {
        outer_query.column(partition_cte.column_ref(name));
    }
    outer_query
        .and_where(Expr::col((Alias::new("partition_cte"), Alias::new("partition_rownum"))).eq(1));
    for order_col in order_columns {
        let order_spec = order_spec_for_alias(order_col, "partition_cte");
        outer_query.order_by_expr_with_nulls(order_spec.expr, order_spec.order, order_spec.nulls);
    }
    outer_query
}

fn order_spec_for_alias(order: &OrderByColumn, alias: &str) -> OrderSpec {
    match order {
        OrderByColumn::Label { label, order } => OrderSpec {
            expr: Expr::col((Alias::new(alias), Alias::new(label.as_str()))),
            order: order.clone(),
            nulls: NullOrdering::Last,
        },
        OrderByColumn::Coalesce {
            labels,
            order,
            rrfs,
        } => {
            let columns = labels
                .iter()
                .map(|label| Expr::col((Alias::new(alias), Alias::new(label.as_str()))))
                .collect::<Vec<_>>();
            OrderSpec {
                expr: build_coalesced_expr(&columns, order.clone(), rrfs.clone()),
                order: order.clone(),
                nulls: NullOrdering::Last,
            }
        }
    }
}

fn build_with_clause(
    state: &QueryState,
    root_cte_name: Option<&str>,
    last_cte_name: Option<&str>,
) -> Option<WithClause> {
    let skip_root = match (root_cte_name, last_cte_name) {
        (Some(root), Some(last)) if root != last => Some(root),
        _ => None,
    };

    let mut with_clause = WithClause::new();
    let mut has_cte = false;
    for cte in &state.ctes {
        if Some(cte.name.as_str()) == skip_root {
            continue;
        }
        let mut cte_expr = CommonTableExpression::new();
        cte_expr
            .table_name(Alias::new(cte.name.as_str()))
            .query(cte.query.clone());
        with_clause.cte(cte_expr);
        has_cte = true;
    }

    if has_cte { Some(with_clause) } else { None }
}

fn direction_to_order(direction: OrderDirection) -> Order {
    match direction {
        OrderDirection::Asc => Order::Asc,
        OrderDirection::Desc => Order::Desc,
    }
}

fn column_name(col: Column) -> &'static str {
    match col {
        Column::FileId => "file_id",
        Column::Sha256 => "sha256",
        Column::Path => "path",
        Column::Filename => "filename",
        Column::LastModified => "last_modified",
        Column::ItemId => "item_id",
        Column::Md5 => "md5",
        Column::Type => "type",
        Column::Size => "size",
        Column::Width => "width",
        Column::Height => "height",
        Column::Duration => "duration",
        Column::TimeAdded => "time_added",
        Column::AudioTracks => "audio_tracks",
        Column::VideoTracks => "video_tracks",
        Column::SubtitleTracks => "subtitle_tracks",
        Column::Blurhash => "blurhash",
        Column::DataId => "data_id",
        Column::Language => "language",
        Column::LanguageConfidence => "language_confidence",
        Column::Text => "text",
        Column::Confidence => "confidence",
        Column::TextLength => "text_length",
        Column::JobId => "job_id",
        Column::SetterId => "setter_id",
        Column::SetterName => "setter_name",
        Column::DataIndex => "data_index",
        Column::SourceId => "source_id",
    }
}

fn order_by_name(field: OrderByField) -> &'static str {
    match field {
        OrderByField::FileId => "file_id",
        OrderByField::Sha256 => "sha256",
        OrderByField::Path => "path",
        OrderByField::Filename => "filename",
        OrderByField::LastModified => "last_modified",
        OrderByField::ItemId => "item_id",
        OrderByField::Md5 => "md5",
        OrderByField::Type => "type",
        OrderByField::Size => "size",
        OrderByField::Width => "width",
        OrderByField::Height => "height",
        OrderByField::Duration => "duration",
        OrderByField::TimeAdded => "time_added",
        OrderByField::AudioTracks => "audio_tracks",
        OrderByField::VideoTracks => "video_tracks",
        OrderByField::SubtitleTracks => "subtitle_tracks",
        OrderByField::Blurhash => "blurhash",
        OrderByField::DataId => "data_id",
        OrderByField::Language => "language",
        OrderByField::LanguageConfidence => "language_confidence",
        OrderByField::Text => "text",
        OrderByField::Confidence => "confidence",
        OrderByField::TextLength => "text_length",
        OrderByField::JobId => "job_id",
        OrderByField::SetterId => "setter_id",
        OrderByField::SetterName => "setter_name",
        OrderByField::DataIndex => "data_index",
        OrderByField::SourceId => "source_id",
        OrderByField::Random => "random",
    }
}

fn get_column_expr(column: Column) -> Expr {
    match column {
        Column::FileId => Expr::col((Files::Table, Files::Id)),
        Column::Sha256 => Expr::col((Files::Table, Files::Sha256)),
        Column::Path => Expr::col((Files::Table, Files::Path)),
        Column::Filename => Expr::col((Files::Table, Files::Filename)),
        Column::LastModified => Expr::col((Files::Table, Files::LastModified)),
        Column::ItemId => Expr::col((Files::Table, Files::ItemId)),
        Column::Md5 => Expr::col((Items::Table, Items::Md5)),
        Column::Type => Expr::col((Items::Table, Items::Type)),
        Column::Size => Expr::col((Items::Table, Items::Size)),
        Column::Width => Expr::col((Items::Table, Items::Width)),
        Column::Height => Expr::col((Items::Table, Items::Height)),
        Column::Duration => Expr::col((Items::Table, Items::Duration)),
        Column::TimeAdded => Expr::col((Items::Table, Items::TimeAdded)),
        Column::AudioTracks => Expr::col((Items::Table, Items::AudioTracks)),
        Column::VideoTracks => Expr::col((Items::Table, Items::VideoTracks)),
        Column::SubtitleTracks => Expr::col((Items::Table, Items::SubtitleTracks)),
        Column::Blurhash => Expr::col((Items::Table, Items::Blurhash)),
        Column::DataId => Expr::col((ItemData::Table, ItemData::Id)),
        Column::Language => Expr::col((ExtractedText::Table, ExtractedText::Language)),
        Column::LanguageConfidence => {
            Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
        }
        Column::Text => Expr::col((ExtractedText::Table, ExtractedText::Text)),
        Column::Confidence => Expr::col((ExtractedText::Table, ExtractedText::Confidence)),
        Column::TextLength => Expr::col((ExtractedText::Table, ExtractedText::TextLength)),
        Column::JobId => Expr::col((ItemData::Table, ItemData::JobId)),
        Column::SetterId => Expr::col((ItemData::Table, ItemData::SetterId)),
        Column::SetterName => Expr::col((Setters::Table, Setters::Name)),
        Column::DataIndex => Expr::col((ItemData::Table, ItemData::Idx)),
        Column::SourceId => Expr::col((ItemData::Table, ItemData::SourceId)),
    }
}

fn get_order_by_expr(field: OrderByField) -> Expr {
    match field {
        OrderByField::Random => Func::random().into(),
        OrderByField::FileId => Expr::col((Files::Table, Files::Id)),
        OrderByField::Sha256 => Expr::col((Files::Table, Files::Sha256)),
        OrderByField::Path => Expr::col((Files::Table, Files::Path)),
        OrderByField::Filename => Expr::col((Files::Table, Files::Filename)),
        OrderByField::LastModified => Expr::col((Files::Table, Files::LastModified)),
        OrderByField::ItemId => Expr::col((Files::Table, Files::ItemId)),
        OrderByField::Md5 => Expr::col((Items::Table, Items::Md5)),
        OrderByField::Type => Expr::col((Items::Table, Items::Type)),
        OrderByField::Size => Expr::col((Items::Table, Items::Size)),
        OrderByField::Width => Expr::col((Items::Table, Items::Width)),
        OrderByField::Height => Expr::col((Items::Table, Items::Height)),
        OrderByField::Duration => Expr::col((Items::Table, Items::Duration)),
        OrderByField::TimeAdded => Expr::col((Items::Table, Items::TimeAdded)),
        OrderByField::AudioTracks => Expr::col((Items::Table, Items::AudioTracks)),
        OrderByField::VideoTracks => Expr::col((Items::Table, Items::VideoTracks)),
        OrderByField::SubtitleTracks => Expr::col((Items::Table, Items::SubtitleTracks)),
        OrderByField::Blurhash => Expr::col((Items::Table, Items::Blurhash)),
        OrderByField::DataId => Expr::col((ItemData::Table, ItemData::Id)),
        OrderByField::Language => Expr::col((ExtractedText::Table, ExtractedText::Language)),
        OrderByField::LanguageConfidence => {
            Expr::col((ExtractedText::Table, ExtractedText::LanguageConfidence))
        }
        OrderByField::Text => Expr::col((ExtractedText::Table, ExtractedText::Text)),
        OrderByField::Confidence => Expr::col((ExtractedText::Table, ExtractedText::Confidence)),
        OrderByField::TextLength => Expr::col((ExtractedText::Table, ExtractedText::TextLength)),
        OrderByField::JobId => Expr::col((ItemData::Table, ItemData::JobId)),
        OrderByField::SetterId => Expr::col((ItemData::Table, ItemData::SetterId)),
        OrderByField::SetterName => Expr::col((Setters::Table, Setters::Name)),
        OrderByField::DataIndex => Expr::col((ItemData::Table, ItemData::Idx)),
        OrderByField::SourceId => Expr::col((ItemData::Table, ItemData::SourceId)),
    }
}

fn is_text_column(column: Column) -> bool {
    matches!(
        column,
        Column::DataId
            | Column::Language
            | Column::LanguageConfidence
            | Column::Text
            | Column::Confidence
            | Column::TextLength
            | Column::JobId
            | Column::SetterId
            | Column::SetterName
            | Column::DataIndex
            | Column::SourceId
    )
}

fn is_text_order_field(field: OrderByField) -> bool {
    matches!(
        field,
        OrderByField::DataId
            | OrderByField::Language
            | OrderByField::LanguageConfidence
            | OrderByField::Text
            | OrderByField::Confidence
            | OrderByField::TextLength
            | OrderByField::JobId
            | OrderByField::SetterId
            | OrderByField::SetterName
            | OrderByField::DataIndex
            | OrderByField::SourceId
    )
}

fn entity_to_data_type(entity: EntityType) -> &'static str {
    match entity {
        EntityType::File => "file",
        EntityType::Text => "text",
    }
}

#[derive(sea_query::Iden)]
enum Files {
    Table,
    Id,
    ItemId,
    Sha256,
    Path,
    Filename,
    LastModified,
}

#[derive(sea_query::Iden)]
enum Items {
    Table,
    Id,
    Md5,
    Type,
    Size,
    Width,
    Height,
    Duration,
    TimeAdded,
    AudioTracks,
    VideoTracks,
    SubtitleTracks,
    Blurhash,
}

#[derive(sea_query::Iden)]
enum ItemData {
    Table,
    Id,
    ItemId,
    DataType,
    JobId,
    SetterId,
    Idx,
    SourceId,
    IsPlaceholder,
}

#[derive(sea_query::Iden)]
enum FilesPathFts {
    Table,
    Path,
    Filename,
}

#[derive(sea_query::Iden)]
enum ExtractedText {
    Table,
    Id,
    Language,
    LanguageConfidence,
    Text,
    Confidence,
    TextLength,
}

#[derive(sea_query::Iden)]
enum ExtractedTextFts {
    Table,
    Text,
}

#[derive(sea_query::Iden)]
enum Setters {
    Table,
    Id,
    Name,
}

#[derive(sea_query::Iden)]
enum Tags {
    Table,
    Id,
    Namespace,
    Name,
}

#[derive(sea_query::Iden)]
enum TagsItems {
    Table,
    ItemDataId,
    TagId,
    Confidence,
}

#[derive(sea_query::Iden)]
enum Bookmarks {
    Table,
    User,
    Namespace,
    Sha256,
    TimeAdded,
}
