from pypika import AliasedQuery
from pypika import SQLLiteQuery as Query
from pypika.queries import QueryBuilder, Selectable

from panoptikon.db.pql.filters import filter_function
from panoptikon.db.pql.order_by import build_order_by
from panoptikon.db.pql.pql_model import (
    AndOperator,
    Filter,
    NotOperator,
    Operator,
    OrOperator,
    QueryElement,
    SearchQuery,
    SortableFilter,
)
from panoptikon.db.pql.utils import (
    CTE,
    OrderByFilter,
    QueryState,
    files_table,
    items_table,
    wrap_select,
)


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

    # Add order by clauses
    full_query = build_order_by(
        full_query, state.order_list, input_query.order_args
    )

    page = max(input_query.page, 1)
    page_size = input_query.page_size
    offset = (page - 1) * page_size
    full_query = full_query.limit(page_size).offset(offset)
    return full_query


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
