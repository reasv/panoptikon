from dataclasses import dataclass
from itertools import groupby
from typing import List, Tuple, Union

from pypika import AliasedQuery, Case, Criterion, Field, Order, QmarkParameter
from pypika import SQLLiteQuery as Query
from pypika import Table
from pypika import functions as fn
from pypika.functions import Function
from pypika.queries import QueryBuilder, Selectable
from pypika.terms import BasicCriterion, Comparator, Term

VERY_LARGE_NUMBER = 9223372036854775805
VERY_SMALL_NUMBER = -9223372036854775805


class Max(Function):
    def __init__(self, term, *default_values, **kwargs):
        super(Max, self).__init__("MAX", term, *default_values, **kwargs)


class Min(Function):
    def __init__(self, term, *default_values, **kwargs):
        super(Min, self).__init__("MIN", term, *default_values, **kwargs)


class Match(Comparator):
    match_ = " MATCH "


from panoptikon.db.pql.pql_model import (
    AndOperator,
    Filter,
    NotOperator,
    Operator,
    OrderArgs,
    OrderTypeNN,
    OrOperator,
    PathFilterModel,
    PathTextFilter,
    PathTextFilterModel,
    QueryElement,
    SearchQuery,
    SortableFilter,
    TypeFilterModel,
)

files_table = Table("files")
items_table = Table("items")
files_path_fts_table = Table("files_path_fts")


@dataclass
class CTE:
    query: Selectable
    name: str


@dataclass
class OrderByFilter:
    cte: AliasedQuery
    direction: OrderTypeNN
    priority: int = 0


class QueryState:
    def __init__(self):
        self.cte_list: List[CTE] = []  # Holds all generated CTEs
        self.order_list: List[OrderByFilter] = []  # Holds order_by clauses
        self.cte_counter = 0  # Counter to generate unique CTE names
        self.root_query = None  # The main query that uses CTE names


def wrap_select(selectable: Selectable) -> QueryBuilder:
    return Query.from_(selectable).select("file_id", "item_id", "sha256")


def path_in(filter: PathFilterModel, context: Selectable) -> Selectable:
    query = (
        wrap_select(context)
        .join(files_table)
        .on(files_table.id == context.file_id)
        .where(
            Criterion.any(
                [files_table.path.like(f"{path}%") for path in filter.in_paths]
            )
        )
    )
    return query


def type_in(filter: TypeFilterModel, context: Selectable) -> Selectable:
    query = (
        wrap_select(context)
        .join(items_table)
        .on(context.item_id == items_table.id)
        .where(
            Criterion.any(
                [
                    items_table.type.like(f"{mime}%")
                    for mime in filter.mime_types
                ]
            )
        )
    )
    return query


def path_text_filter(
    filter: PathTextFilterModel, context: Selectable
) -> Selectable:
    query = (
        wrap_select(context)
        .join(files_path_fts_table)
        .on(context.file_id == files_path_fts_table.rowid)
        .select(Field("rank").as_("order_rank"))
    )
    column = (
        files_path_fts_table.filename
        if filter.path_text.filename_only
        else files_path_fts_table.path
    )
    query = query.where(
        BasicCriterion(
            Match.match_,
            column,
            Term.wrap_constant(filter.path_text.query),  # type: ignore
        )
    )
    return query


def filter_function(filter: Filter, context: Selectable, state: QueryState):
    if isinstance(filter, PathFilterModel):
        query = path_in(filter, context)
    elif isinstance(filter, TypeFilterModel):
        query = type_in(filter, context)
    elif isinstance(filter, PathTextFilterModel):
        query = path_text_filter(filter, context)
    else:
        raise ValueError("Unknown filter type")
    filter_type = filter.__class__.__name__
    cte_name = f"n_{state.cte_counter}_{filter_type}"
    state.cte_counter += 1
    state.cte_list.append(CTE(query, cte_name))
    return AliasedQuery(cte_name)


def process_query_element(
    el: QueryElement, context: Selectable, state: QueryState
) -> AliasedQuery:
    # Process primitive filters
    if isinstance(el, Filter):
        cte = filter_function(el, context, state)
        if isinstance(el, SortableFilter):
            if el.order_by:
                state.order_list.append(
                    OrderByFilter(
                        cte=cte,
                        direction=el.order_direction,
                        priority=el.order_priority,
                    )
                )
        return cte
    elif isinstance(el, Operator):
        if isinstance(el, AndOperator):
            for sub_element in el.and_:
                context = process_query_element(sub_element, context, state)
            cte_name = f"n_{state.cte_counter}_and"
            state.cte_counter += 1
            state.cte_list.append(CTE(wrap_select(context), cte_name))
            return AliasedQuery(cte_name)
        elif isinstance(el, OrOperator):
            union_query = None
            for sub_element in el.or_:
                q = process_query_element(sub_element, context, state)
                # Combine the subqueries using UNION (OR logic)
                union_query = (
                    wrap_select(q)
                    if union_query is None
                    else union_query.union(wrap_select(q))
                )
            assert union_query is not None, "No subqueries generated"
            cte_name = f"n_{state.cte_counter}_or"
            state.cte_counter += 1
            state.cte_list.append(
                CTE(
                    wrap_select(union_query),
                    cte_name,
                )
            )
            return AliasedQuery(cte_name)
        elif isinstance(el, NotOperator):
            subquery: AliasedQuery = process_query_element(
                el.not_, context, state
            )

            not_query = wrap_select(
                wrap_select(context).except_of(wrap_select(subquery))
            )

            cte_name = f"n_{state.cte_counter}_not_{subquery.name}"
            state.cte_counter += 1
            state.cte_list.append(CTE(not_query, cte_name))

            return AliasedQuery(cte_name)
    else:
        raise ValueError("Unknown query element type")


def build_query(input_query: SearchQuery) -> QueryBuilder:
    # Initialize the state object
    state = QueryState()

    # Start the recursive processing
    initial_select = (
        Query.from_(files_table)
        .join(items_table)
        .on(files_table.item_id == items_table.id)
        .select(
            files_table.id.as_("file_id"),
            "item_id",
            files_table.sha256.as_("sha256"),
        )
    )
    if input_query.query:
        root_cte = process_query_element(
            input_query.query, AliasedQuery("root_files"), state
        )

        # Add all CTEs to the final query
        full_query: QueryBuilder = Query.with_(initial_select, "root_files")
        for cte in state.cte_list:
            full_query = full_query.with_(cte.query, cte.name)
        assert full_query is not None, "No CTEs generated"

        full_query = (
            full_query.from_(root_cte)
            .join(files_table)
            .on(files_table.id == root_cte.file_id)
            .join(items_table)
            .on(root_cte.item_id == items_table.id)
            .select(
                root_cte.file_id,
                root_cte.item_id,
                root_cte.sha256,
                files_table.path,
                files_table.last_modified,
                items_table.type,
            )
        )
    else:
        full_query = (
            Query.from_(files_table)
            .join(items_table)
            .on(files_table.item_id == items_table.id)
            .select(
                files_table.id.as_("file_id"),
                "item_id",
                files_table.sha256.as_("sha256"),
                files_table.path,
                files_table.last_modified,
                items_table.type,
            )
        )

    full_order_list = combine_order_lists(
        state.order_list, input_query.order_args
    )

    for ospec in full_order_list:
        if isinstance(ospec, OrderArgs):
            order_by, direction = get_order_by_and_direction(ospec)
            field = Field(order_by)
            full_query = full_query.orderby(
                (
                    field.isnotnull()
                    if direction == Order.desc
                    else field.isnull()
                ),
                field,
                order=direction,
            )
        elif isinstance(ospec, OrderByFilter):
            direction = Order.asc if ospec.direction == "asc" else Order.desc
            full_query = (
                full_query.left_join(ospec.cte)
                .on_field("file_id")
                .orderby(
                    (  # Ensure that NULL values are at the end
                        ospec.cte.order_rank.isnotnull()
                        if direction == Order.desc
                        else ospec.cte.order_rank.isnull()
                    ),
                    ospec.cte.order_rank,
                    order=direction,
                )
            )
        elif isinstance(ospec, list):
            # Coalesce filter order by columns with the same priority
            columns = []  # Initialize variable for coalesced column
            direction = Order.asc if ospec[0].direction == "asc" else Order.desc

            for spec in ospec:
                assert isinstance(spec, OrderByFilter), "Invalid OrderByFilter"
                full_query = full_query.left_join(spec.cte).on_field("file_id")
                columns.append(spec.cte.order_rank)

            # For ascending order, use MIN to get the smallest non-null value
            if direction == Order.asc:
                coalesced_column = Min(
                    *[
                        fn.Coalesce(column, VERY_LARGE_NUMBER)
                        for column in columns
                    ]
                )
            # For descending order, use MAX to get the largest non-null value
            else:
                coalesced_column = Max(
                    *[
                        fn.Coalesce(column, VERY_SMALL_NUMBER)
                        for column in columns
                    ]
                )

            full_query = full_query.orderby(
                coalesced_column,
                order=direction,
            )

    page = max(input_query.page, 1)
    page_size = input_query.page_size
    offset = (page - 1) * page_size
    full_query = full_query.limit(page_size).offset(offset)
    return full_query


def combine_order_lists(
    order_list: List[OrderByFilter], order_args: List[OrderArgs]
) -> List[Union[OrderArgs, OrderByFilter, List[OrderByFilter]]]:

    # order_list has priority over order_args by default
    combined = [(obj, idx, 0) for idx, obj in enumerate(order_list)] + [
        (obj, idx, 1) for idx, obj in enumerate(order_args)
    ]

    # Sort by priority, then by index, then by order_args vs order_list
    sorted_combined = sorted(
        combined, key=lambda x: (-x[0].priority, x[2], x[1])
    )
    final_order = [item[0] for item in sorted_combined]
    return group_order_list(final_order)


# Assuming OrderArgs and OrderByFilter are defined elsewhere
def group_order_list(
    final_order: list[Union[OrderArgs, OrderByFilter]]
) -> List[Union[OrderArgs, OrderByFilter, List[OrderByFilter]]]:
    # Group elements from final_order by their priority if they are OrderByFilter
    grouped_order: List[
        Union[OrderArgs, OrderByFilter, List[OrderByFilter]]
    ] = []

    for key, group in groupby(
        final_order,
        key=lambda obj: (obj.priority, isinstance(obj, OrderByFilter)),
    ):
        group_list = list(group)
        priority, is_order_by_filter = key

        if (
            is_order_by_filter
            and len(group_list) > 1
            and all(isinstance(obj, OrderByFilter) for obj in group_list)
        ):
            grouped_order.append(
                group_list  # type: ignore
            )  # group_list is List[OrderByFilter]
        else:
            grouped_order.extend(group_list)

    return grouped_order


def get_order_by_and_direction(order_args: OrderArgs) -> Tuple[str, Order]:
    order_by = order_args.order_by
    if order_by is None:
        order_by = "last_modified"
    direction = order_args.order
    if direction is None:
        if order_args.order_by == "last_modified":
            direction = Order.desc
        else:
            direction = Order.asc
    else:
        direction = Order.asc if direction == "asc" else Order.desc
    return (order_by, direction)
