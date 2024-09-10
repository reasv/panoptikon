from typing import List, Tuple

from sqlalchemy import CTE, Select, except_, func, select, union

from panoptikon.db.pql.filters import filter_function
from panoptikon.db.pql.order_by import build_order_by
from panoptikon.db.pql.pql_model import (
    AndOperator,
    NotOperator,
    Operator,
    OrOperator,
    PQLQuery,
    QueryElement,
)
from panoptikon.db.pql.preprocess_query import preprocess_query
from panoptikon.db.pql.types import Filter, SortableFilter
from panoptikon.db.pql.utils import OrderByFilter, QueryState


def build_query(input_query: PQLQuery, count_query: bool = False) -> Select:
    from panoptikon.db.pql.tables import files, items

    # Preprocess the query to remove empty filters and validate args
    if query_root := input_query.query:
        query_root = preprocess_query(query_root)
    # Initialize the state object
    state = QueryState(is_count_query=count_query)
    root_cte_name: str | None = None
    # Start the recursive processing
    if query_root:
        root_cte = process_query_element(
            query_root,
            select(files.c.id.label("file_id"), files.c.item_id).cte(
                "files_cte"
            ),
            state,
        )
        root_cte_name = root_cte.name

        file_id, item_id = (
            root_cte.c.file_id.label("file_id"),
            root_cte.c.item_id.label("item_id"),
        )
    else:
        file_id, item_id = files.c.id.label("file_id"), files.c.item_id.label(
            "item_id"
        )

    if input_query.entity == "item":
        item_cte = (
            select(func.max(file_id).label("file_id"), item_id)
            .group_by(item_id)
            .cte("item_cte")
        )
        full_query = select(item_cte.c.file_id, item_cte.c.item_id)
        file_id, item_id = item_cte.c.file_id.label(
            "file_id"
        ), item_cte.c.item_id.label("file_id")
        root_cte_name = item_cte.name
    else:
        full_query = select(file_id, item_id)

    if count_query:
        return full_query.with_only_columns(func.count().label("total"))

    # Join the item and file tables
    full_query = full_query.join(items, items.c.id == item_id).join(
        files, files.c.id == file_id
    )

    # Add order by clauses
    full_query = build_order_by(
        full_query,
        root_cte_name,
        file_id,
        state.order_list,
        input_query.order_args,
    )

    page = max(input_query.page, 1)
    page_size = input_query.page_size
    offset = (page - 1) * page_size
    full_query = full_query.limit(page_size).offset(offset)
    return full_query


def process_query_element(
    el: QueryElement, context: CTE, state: QueryState
) -> CTE:
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
            # AND is implemeted by chaining CTEs
            for sub_element in el.and_:
                context = process_query_element(sub_element, context, state)
            return context
        elif isinstance(el, OrOperator):
            union_list: List[Select] = []
            for sub_element in el.or_:
                subq = process_query_element(sub_element, context, state)
                # Combine the subqueries using UNION (OR logic)
                union_list.append(select(subq.c.item_id, subq.c.file_id))
            cte_name = f"n_{state.cte_counter}_or"
            state.cte_counter += 1
            return union(*union_list).cte(cte_name)

        elif isinstance(el, NotOperator):
            subquery = process_query_element(el.not_, context, state)
            cte_name = f"n_{state.cte_counter}_not_{subquery.name}"
            state.cte_counter += 1

            return except_(
                select(context.c.item_id, context.c.file_id),
                select(subquery.c.item_id, subquery.c.file_id),
            ).cte(cte_name)
    else:
        raise ValueError("Unknown query element type")
