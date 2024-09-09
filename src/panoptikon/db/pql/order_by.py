from itertools import groupby
from typing import List, Tuple, Type, Union

from sqlalchemy import Select, asc, desc, func, literal_column, nulls_last

from panoptikon.db.pql.pql_model import OrderArgs
from panoptikon.db.pql.utils import (
    VERY_LARGE_NUMBER,
    VERY_SMALL_NUMBER,
    OrderByFilter,
)


def build_order_by(
    query: Select,
    root_cte_name: str | None,
    order_list: List[OrderByFilter],
    order_args: List[OrderArgs],
):
    from panoptikon.db.pql.tables import files

    full_order_list = combine_order_lists(order_list, order_args)

    for ospec in full_order_list:
        if isinstance(ospec, OrderArgs):
            order_by, direction = get_order_by_and_direction(ospec)
            field = literal_column(order_by)
            query = query.order_by(nulls_last(direction(field)))
        elif isinstance(ospec, OrderByFilter):
            direction = asc if ospec.direction == "asc" else desc
            field = ospec.cte.c.order_rank
            # If this is not the last CTE in the chain, we have to LEFT JOIN it
            if ospec.cte.name != root_cte_name:
                query = query.join(
                    ospec.cte,
                    ospec.cte.c.file_id == files.c.id,
                    isouter=True,
                )
            query = query.order_by(nulls_last(direction(field)))
        elif isinstance(ospec, list):
            # Coalesce filter order by columns with the same priority
            columns = []  # Initialize variable for coalesced column
            direction = asc if ospec[0].direction == "asc" else desc

            for spec in ospec:
                assert isinstance(spec, OrderByFilter), "Invalid OrderByFilter"
                # If this is not the last CTE in the chain, we have to LEFT JOIN it
                if spec.cte.name != root_cte_name:
                    query = query.join(
                        spec.cte,
                        spec.cte.c.file_id == files.c.id,
                        isouter=True,
                    )

                columns.append(spec.cte.c.order_rank)

            # For ascending order, use MIN to get the smallest non-null value
            if direction == asc:
                coalesced_column = func.min(
                    *[
                        func.coalesce(column, VERY_LARGE_NUMBER)
                        for column in columns
                    ]
                )
            # For descending order, use MAX to get the largest non-null value
            else:
                coalesced_column = func.max(
                    *[
                        func.coalesce(column, VERY_SMALL_NUMBER)
                        for column in columns
                    ]
                )
            query = query.order_by(direction(coalesced_column))
    return query


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
) -> Tuple[str, Type[asc] | Type[desc]]:
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
