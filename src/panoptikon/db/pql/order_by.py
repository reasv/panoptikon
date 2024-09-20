from itertools import groupby
from typing import Callable, List, Tuple, Type, Union

from sqlalchemy import (
    CTE,
    Column,
    Label,
    Select,
    UnaryExpression,
    asc,
    desc,
    func,
    nulls_last,
)
from sqlalchemy.sql.elements import KeyedColumnElement

from panoptikon.db.pql.pql_model import OrderArgs
from panoptikon.db.pql.types import (
    RRF,
    VERY_LARGE_NUMBER,
    VERY_SMALL_NUMBER,
    OrderByFilter,
    get_column,
)


def build_order_by(
    query: Select,
    root_cte_name: str | None,
    select_conds: bool,
    order_list: List[OrderByFilter],
    order_args: List[OrderArgs],
):
    full_order_list = combine_order_lists(order_list, order_args)
    order_by_conditions: List[UnaryExpression] = []
    order_fns: List[Callable[[CTE], UnaryExpression]] = []
    for index, ospec in enumerate(full_order_list):
        if isinstance(ospec, OrderArgs):
            query, order_by_condition, order_fn = apply_order_args(
                ospec, index, query, select_conds
            )
            order_by_conditions.append(order_by_condition)
            if order_fn:
                order_fns.append(order_fn)

        elif isinstance(ospec, OrderByFilter):
            query, order_by_condition, order_fn = apply_order_filter(
                ospec,
                index,
                query,
                select_conds,
                root_cte_name,
            )
            order_by_conditions.append(order_by_condition)
            if order_fn:
                order_fns.append(order_fn)

        elif isinstance(ospec, list):
            query, order_by_condition, order_fn = coalesce_order_filters(
                ospec,
                index,
                query,
                select_conds,
                root_cte_name,
            )
            order_by_conditions.append(order_by_condition)
            if order_fn:
                order_fns.append(order_fn)
    return query, order_by_conditions, order_fns


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


def get_order_by_and_direction(
    order_args: OrderArgs,
):
    order_by = order_args.order_by
    if order_by is None:
        order_by = "last_modified"
    direction = order_args.order
    if direction is None:
        if order_args.order_by == "last_modified":
            direction = desc
        else:
            direction = asc
    else:
        direction = asc if direction == "asc" else desc
    return (order_by, direction)


def apply_order_args(
    args: OrderArgs,
    index: int,
    query: Select,
    select_conds: bool,
) -> Tuple[Select, UnaryExpression, Callable[[CTE], UnaryExpression] | None]:
    order_by, direction = get_order_by_and_direction(args)
    field = get_column(order_by)

    gen = None
    if select_conds:
        label = f"o{index}_{order_by}"
        query = query.column(field.label(label))
        gen = lambda cte: nulls_last(direction(cte.c[label]))

    return (
        query,
        nulls_last(direction(field)),
        gen,
    )


def apply_order_filter(
    args: OrderByFilter,
    index: int,
    query: Select,
    select_conds: bool,
    root_cte_name: str | None,
) -> Tuple[Select, UnaryExpression, Callable[[CTE], UnaryExpression] | None]:
    direction = asc if args.direction == "asc" else desc
    field = args.cte.c.order_rank
    if root_cte_name == args.cte.name:
        # If the order rank is in the root CTE, use the column directly
        field = Column("order_rank")
    gen = None
    if select_conds:
        if root_cte_name == args.cte.name:
            # If the order rank is in the root CTE, use the column directly
            gen = lambda cte: nulls_last(direction(cte.c.order_rank))
        else:
            label = f"o{index}_{args.cte.name}_rank"
            query = query.column(field.label(label))
            gen = lambda cte: nulls_last(direction(cte.c[label]))
    return (
        query,
        nulls_last(direction(field)),
        gen,
    )


def coalesce_order_filters(
    args: List[OrderByFilter],
    index: int,
    query: Select,
    select_conds: bool,
    root_cte_name: str | None,
) -> Tuple[Select, UnaryExpression, Callable[[CTE], UnaryExpression] | None]:
    # Coalesce filter order by columns with the same priority
    columns = []  # Initialize variable for coalesced column
    direction = asc if args[0].direction == "asc" else desc

    # Enable Reciprocal Ranked Fusion (RRF) if the first of the filters has it set
    enable_rrf = True if args[0].rrf is not None else False
    rrfs: List[RRF] = []

    select_labels: List[str] = []
    for spec in args:
        assert isinstance(spec, OrderByFilter), "Invalid OrderByFilter"
        field = spec.cte.c.order_rank
        if root_cte_name == spec.cte.name:
            # If the order rank is in the root CTE, use the column directly
            field = Column("order_rank")
        columns.append(field)
        if enable_rrf:
            rrfs.append(spec.rrf or RRF())

        if select_conds:
            if root_cte_name == spec.cte.name:
                # If the order rank is in the root CTE, use the column directly
                select_labels.append("order_rank")
            else:
                # Select the order rank column from the CTE explicitly in the root SELECT
                label = f"o{index}_{spec.cte.name}_rank"
                query = query.column(field.label(label))
                select_labels.append(label)

    def coalesce_cols(
        cols: List[KeyedColumnElement],
    ) -> UnaryExpression:
        # If RRF is enabled, combine the columns using the RRF function
        if enable_rrf:
            # Apply RRF to the coalesced columns
            coalesced_column = func.sum(
                *[
                    (1 / (rrf.k + func.coalesce(column, VERY_LARGE_NUMBER)))
                    * rrf.weight
                    for rrf, column in zip(rrfs, cols)
                ]
            )
            return direction(coalesced_column)

        # If RRF is not enabled, pick the best value from the columns
        # For ascending order, use MIN to get the smallest non-null value
        if direction == asc:
            coalesced_column = func.min(
                *[func.coalesce(column, VERY_LARGE_NUMBER) for column in cols]
            )
        # For descending order, use MAX to get the largest non-null value
        else:
            coalesced_column = func.max(
                *[func.coalesce(column, VERY_SMALL_NUMBER) for column in cols]
            )
        return direction(coalesced_column)

    gen = None
    if select_conds:
        gen = lambda cte: coalesce_cols(
            [cte.c[label] for label in select_labels]
        )
    return query, coalesce_cols(columns), gen
