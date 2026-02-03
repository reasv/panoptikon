use std::collections::{HashMap, HashSet};

use crate::pql::model::{
    Column, EntityType, OrderArgs, OrderByField, OrderDirection, PqlQuery, QueryElement, Rrf,
    ScalarValue, SortableOptions,
};
use crate::pql::preprocess::{PqlError, preprocess_query};
use sea_query::{
    Alias, Asterisk, BinOper, ColumnRef, CommonTableExpression, Cond, Expr, ExprTrait, Func,
    IntoColumnRef, JoinType, NullOrdering, Order, OverStatement, Query, SelectStatement, UnionType,
    WindowStatement, WithClause,
};

pub(crate) mod filters;
use self::filters::FilterCompiler;

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
    joined_tables: JoinedTables,
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

#[derive(Default, Debug, Clone)]
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

pub(crate) fn build_query(
    mut input_query: PqlQuery,
    count_query: bool,
) -> Result<PqlBuilderResult, PqlError> {
    let query_root = match input_query.query.take() {
        Some(query_root) => preprocess_query(query_root)?,
        None => None,
    };
    build_query_with_root(input_query, count_query, query_root)
}

pub(crate) fn build_query_preprocessed(
    mut input_query: PqlQuery,
    count_query: bool,
) -> Result<PqlBuilderResult, PqlError> {
    let query_root = input_query.query.take();
    build_query_with_root(input_query, count_query, query_root)
}

fn build_query_with_root(
    mut input_query: PqlQuery,
    count_query: bool,
    query_root: Option<QueryElement>,
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

        joined_tables = root_select.joined_tables.clone();
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
                    joined_tables: JoinedTables::default(),
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
                    joined_tables: JoinedTables::default(),
                },
            );
            Ok(not_cte)
        }
        QueryElement::Match(filter) => filter.build(context, state),
        QueryElement::MatchPath(filter) => filter.build(context, state),
        QueryElement::MatchText(filter) => filter.build(context, state),
        QueryElement::SemanticTextSearch(filter) => filter.build(context, state),
        QueryElement::SemanticImageSearch(filter) => filter.build(context, state),
        QueryElement::SimilarTo(filter) => filter.build(context, state),
        QueryElement::MatchTags(filter) => filter.build(context, state),
        QueryElement::InBookmarks(filter) => filter.build(context, state),
        QueryElement::ProcessedBy(filter) => filter.build(context, state),
        QueryElement::HasUnprocessedData(filter) => filter.build(context, state),
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
    joined_tables: &JoinedTables,
) -> CteRef {
    let cte = create_cte(state, cte_name.clone(), query.clone());
    state.selects.insert(
        cte_name,
        FilterSelect {
            select: query,
            context: context.clone(),
            joined_tables: joined_tables.clone(),
        },
    );
    cte
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
        window.partition_by((
            Alias::new(select_cte.name.as_str()),
            Alias::new(label.as_str()),
        ));
    }
    for order_col in order_columns {
        let order_spec = order_spec_for_alias(order_col, select_cte.name.as_str());
        window.order_by_expr_with_nulls(order_spec.expr, order_spec.order, order_spec.nulls);
    }

    let mut partition_query = Query::select();
    partition_query
        .from(Alias::new(select_cte.name.as_str()))
        .column((Alias::new(select_cte.name.as_str()), Asterisk))
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
    Sha256,
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
enum Embeddings {
    Table,
    Id,
    Embedding,
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
