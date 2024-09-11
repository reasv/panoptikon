import logging
from typing import List, Tuple

from sqlalchemy import CTE, Column, Label, Select, except_, func, select, union

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
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
from panoptikon.db.pql.types import (
    OrderByFilter,
    QueryState,
    contains_text_columns,
    get_column,
    get_std_cols,
)

logger = logging.getLogger(__name__)


def build_query(
    input_query: PQLQuery, count_query: bool = False
) -> Tuple[Select, List[str]]:
    from panoptikon.db.pql.tables import (
        extracted_text,
        files,
        item_data,
        items,
        setters,
    )

    # Preprocess the query to remove empty filters and validate args
    if query_root := input_query.query:
        query_root = preprocess_query(query_root)
    # Initialize the state object
    state = QueryState(is_count_query=count_query)
    if input_query.entity == "text":
        state.is_text_query = True
    root_cte_name: str | None = None
    # Start the recursive processing
    if query_root:
        start = select(files.c.id.label("file_id"), files.c.item_id)
        if input_query.entity == "text":
            start = (
                start.join(item_data, item_data.c.item_id == files.c.item_id)
                .join(extracted_text, extracted_text.c.id == item_data.c.id)
                .add_columns(extracted_text.c.id.label("text_id"))
            )

        root_cte = process_query_element(
            query_root,
            start.cte("begin_cte"),
            state,
        )
        root_cte_name = root_cte.name

        file_id, item_id = (
            root_cte.c.file_id.label("file_id"),
            root_cte.c.item_id.label("item_id"),
        )
        if input_query.entity == "text":
            text_id = root_cte.c.text_id.label("text_id")
        else:
            # Not actually used, but needed for type checking
            text_id = extracted_text.c.id.label("text_id")
    else:
        file_id, item_id = files.c.id.label("file_id"), files.c.item_id.label(
            "item_id"
        )
        # Not actually used, but needed for type checking
        text_id = extracted_text.c.id.label("text_id")
        if input_query.entity == "text":
            # We must join to get the corresponding item and files
            text_cte = (
                select(file_id, item_id, text_id)
                .join(item_data, item_data.c.item_id == files.c.item_id)
                .join(extracted_text, extracted_text.c.id == item_data.c.id)
                .cte("text_cte")
            )
            file_id, item_id, text_id = (
                text_cte.c.file_id.label("file_id"),
                text_cte.c.item_id.label("item_id"),
                text_cte.c.text_id.label("text_id"),
            )
            root_cte_name = text_cte.name

    if input_query.entity == "text":
        full_query = select(file_id, item_id, text_id)
    else:
        full_query = select(file_id, item_id)

    if count_query:
        return (
            select(func.count().label("total")).select_from(
                full_query.alias("wrapped_query")
            ),
            [],
        )

    # Join the item and file tables
    full_query = full_query.join(items, items.c.id == item_id).join(
        files, files.c.id == file_id
    )
    if input_query.entity == "text":
        full_query = (
            full_query.join(extracted_text, extracted_text.c.id == text_id)
            .join(item_data, item_data.c.id == extracted_text.c.id)
            .join(setters, setters.c.id == item_data.c.setter_id)
        )
    if not input_query.entity == "text":
        if contains_text_columns(input_query.select):
            logger.error("Tried to select text columns in a non-text query")
            raise ValueError("Tried to select text columns in a non-text query")
        order_cols = [order.order_by for order in input_query.order_args]
        if contains_text_columns(order_cols):
            logger.error("Tried to order by text columns in a non-text query")
            raise ValueError(
                "Tried to order by text columns in a non-text query"
            )

    full_query = add_select_columns(input_query, full_query)
    # Add extra columns
    full_query, extra_columns = add_extra_columns(
        full_query, state, root_cte_name, file_id, text_id
    )
    selected_columns = [
        col.key for col in full_query.selected_columns if col.key
    ]
    # Add order by clauses
    text_id = text_id if input_query.entity == "text" else None
    full_query, order_by_conds, order_fns = build_order_by(
        full_query,
        root_cte_name,
        file_id,
        text_id,
        select_conds=True if input_query.partition_by is not None else False,
        order_list=state.order_list,
        order_args=input_query.order_args,
    )

    if input_query.partition_by:
        # Add row number column for partitioning, and get the first row of each partition
        partition_by_cols = [
            get_column(col).label(col) for col in input_query.partition_by
        ]
        rownum = func.row_number().over(
            partition_by=partition_by_cols, order_by=order_by_conds
        )
        full_query = full_query.add_columns(
            rownum.label("partition_rownum")
        ).cte("partition_cte")
        outer_order_by_conds = [f(full_query) for f in order_fns]
        print([str(c) for c in outer_order_by_conds])

        # Only select explicitly requested columns
        full_query = select(*[full_query.c[k] for k in selected_columns]).where(
            full_query.c.partition_rownum == 1
        )
        full_query = full_query.order_by(*outer_order_by_conds)
    else:
        full_query = full_query.order_by(*order_by_conds)

    page = max(input_query.page, 1)
    page_size = input_query.page_size
    offset = (page - 1) * page_size
    full_query = full_query.limit(page_size).offset(offset)
    return full_query, extra_columns


def process_query_element(
    el: QueryElement, context: CTE, state: QueryState
) -> CTE:
    # Process primitive filters
    if isinstance(el, Filter):
        cte = el.build_query(context, state)
        if isinstance(el, SortableFilter):
            if el.order_by:
                state.order_list.append(
                    OrderByFilter(
                        cte=cte,
                        direction=el.direction,
                        priority=el.priority,
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
                union_list.append(select(*get_std_cols(subq, state)))
            cte_name = f"n{state.cte_counter}_or"
            state.cte_counter += 1
            return union(*union_list).cte(cte_name)

        elif isinstance(el, NotOperator):
            subquery = process_query_element(el.not_, context, state)
            cte_name = f"n{state.cte_counter}_not_{subquery.name}"
            state.cte_counter += 1

            return except_(
                select(*get_std_cols(context, state)),
                select(*get_std_cols(subquery, state)),
            ).cte(cte_name)
    else:
        raise ValueError("Unknown query element type")


def add_select_columns(input_query: PQLQuery, query: Select) -> Select:
    # Always include the path, sha256, type, and last_modified columns
    input_query.select.extend(["path", "sha256", "type", "last_modified"])
    input_query.select = list(set(input_query.select))
    # These columns are already included
    input_query.select = [
        x
        for x in input_query.select
        if x not in {"item_id", "file_id", "text_id"}
    ]

    columns = [get_column(col).label(col) for col in input_query.select]
    return query.add_columns(*columns)


def add_extra_columns(
    query: Select,
    state: QueryState,
    root_cte_name: str | None,
    file_id: Label,
    text_id: Label | None,
) -> Tuple[Select, List[str]]:
    column_aliases = []
    for i, extra_column in enumerate(state.extra_columns):
        column, cte, alias = (
            extra_column.column,
            extra_column.cte,
            extra_column.alias,
        )
        query = query.add_columns(column.label(f"extra_{i}"))
        column_aliases.append(alias)
        if extra_column.need_join and cte.name != root_cte_name:
            join_cond = cte.c.file_id == file_id
            if text_id is not None:
                # For text-based queries, we need to join on the text_id as well
                # The results are unique on file_id, text_id rather than just file_id
                join_cond = join_cond & (cte.c.text_id == text_id)
            query = query.join(
                cte,
                join_cond,
                isouter=True,
            )
    return query, column_aliases
