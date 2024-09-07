from dataclasses import dataclass
from typing import List, final

from pypika import AliasedQuery, Criterion, Field, Order, QmarkParameter
from pypika import SQLLiteQuery as Query
from pypika import Table
from pypika.queries import QueryBuilder, Selectable
from pypika.terms import BasicCriterion, Comparator, Term


class Match(Comparator):
    match_ = " MATCH "


from panoptikon.db.pql.pql_model import (
    AndOperator,
    Filter,
    NotOperator,
    Operator,
    OrOperator,
    PathFilterModel,
    PathTextFilter,
    PathTextFilterModel,
    QueryElement,
    SearchQuery,
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
class OrderByColumn:
    column: str
    direction: str
    priority: int = 0


class QueryState:
    def __init__(self):
        self.cte_list: List[CTE] = []  # Holds all generated CTEs
        self.order_list: List[OrderByColumn] = []  # Holds order_by clauses
        self.cte_counter = 0  # Counter to generate unique CTE names
        self.root_query = None  # The main query that uses CTE names


def wrap_select(selectable: Selectable) -> QueryBuilder:
    return Query.from_(selectable).select("file_id", "item_id")


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
    )
    if filter.path_text.only_match_filename:
        query = query.where(
            BasicCriterion(
                Match.match_,
                files_path_fts_table.filename,
                Term.wrap_constant(filter.path_text.query),  # type: ignore
            )
        )
    else:
        query = query.where(
            BasicCriterion(
                Match.match_,
                files_path_fts_table.path,
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


def process_query(
    el: QueryElement, context: Selectable, state: QueryState
) -> AliasedQuery:
    # Process primitive filters
    if isinstance(el, Filter):
        return filter_function(el, context, state)
    elif isinstance(el, Operator):
        if isinstance(el, AndOperator):
            for sub_element in el.and_:
                context = process_query(sub_element, context, state)
            cte_name = f"n_{state.cte_counter}_and"
            state.cte_counter += 1
            state.cte_list.append(CTE(context, cte_name))
            return AliasedQuery(cte_name)
        elif isinstance(el, OrOperator):
            union_query = None
            for sub_element in el.or_:
                q = process_query(sub_element, context, state)
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
                    union_query,
                    cte_name,
                )
            )
            return AliasedQuery(cte_name)
        elif isinstance(el, NotOperator):
            subquery: AliasedQuery = process_query(el.not_, context, state)

            not_query = wrap_select(context).except_of(wrap_select(subquery))

            cte_name = f"n_{state.cte_counter}_not_{subquery.name}"
            state.cte_counter += 1
            state.cte_list.append(CTE(not_query, cte_name))

            return AliasedQuery(cte_name)
    else:
        raise ValueError("Unknown query element type")


def build_final_query(input_query: SearchQuery) -> QueryBuilder:
    # Initialize the state object
    state = QueryState()

    # Start the recursive processing
    initial_select = (
        Query.from_(files_table)
        .join(items_table)
        .on(files_table.item_id == items_table.id)
        .select(files_table.id.as_("file_id"), "item_id")
    )
    if input_query.query:
        root_cte = process_query(
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
                files_table.path,
                files_table.last_modified,
                items_table.type,
            )
        )

    # Apply ORDER BY if needed
    if state.order_list:
        for order in state.order_list:
            full_query = full_query.orderby(order.column, order.direction)

    if input_query.order_args.order_by:
        full_query = full_query.orderby(
            Field(input_query.order_args.order_by, table=files_table),
            order=Order.asc if input_query.order_args == "asc" else Order.desc,
        )

    offset = (
        max((input_query.order_args.page - 1), 0)
        * input_query.order_args.page_size
    )
    # Apply pagination
    full_query = full_query.limit(input_query.order_args.page_size).offset(
        offset
    )
    return full_query


# Example usage
# example_query = AndOperator(
#     and_=[
#         PathFilterModel(in_paths=["/home/user1", "/home/user2", "/home/user3"]),
#         NotOperator(
#             not_=PathTextFilterModel(path_text=PathTextFilter(query="example"))
#         ),
#         OrOperator(
#             or_=[
#                 TypeFilterModel(
#                     mime_types=["application/pdf", "image/jpeg", "image/png"]
#                 ),
#                 TypeFilterModel(mime_types=["text/plain"]),
#             ]
#         ),
#     ]
# )

# search_query = SearchQuery(query=example_query)
# parameters = QmarkParameter()
# final_query = build_final_query(search_query)
# print(final_query.get_sql(parameter=parameters))
# print(parameters.get_parameters())
