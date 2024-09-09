from typing import List

from sqlalchemy import CTE, Select, except_, select, union

from panoptikon.db.pql.filters import filter_function
from panoptikon.db.pql.order_by import build_order_by
from panoptikon.db.pql.pql_model import (
    AndOperator,
    NotOperator,
    Operator,
    OrOperator,
    QueryElement,
    SearchQuery,
)
from panoptikon.db.pql.tables import files, items
from panoptikon.db.pql.types import Filter, SortableFilter
from panoptikon.db.pql.utils import OrderByFilter, QueryState


def build_query(input_query: SearchQuery) -> Select:
    # Initialize the state object
    state = QueryState()

    root_cte_name: str | None = None
    # Start the recursive processing
    if input_query.query:
        root_cte = process_query_element(
            input_query.query,
            select(files.c.id.label("file_id"), files.c.item_id).cte(
                "files_cte"
            ),
            state,
        )
        root_cte_name = root_cte.name

        full_query = (
            select(
                root_cte.c.file_id,
                root_cte.c.item_id,
                files.c.sha256,
                files.c.path,
                files.c.last_modified,
                items.c.type,
            )
            .join(files, files.c.id == root_cte.c.file_id)
            .join(items, items.c.id == root_cte.c.item_id)
        )
    else:
        full_query = select(
            files.c.sha256,
            files.c.path,
            files.c.last_modified,
            items.c.type,
        ).join(items, items.c.id == files.c.item_id)

    # Add order by clauses
    full_query = build_order_by(
        full_query, root_cte_name, state.order_list, input_query.order_args
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
