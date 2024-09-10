from typing import Union

from sqlalchemy import CTE, literal_column

from panoptikon.db.pql.filters.path_in import InPaths
from panoptikon.db.pql.filters.sortable.bookmarks import (
    InBookmarks,
    InBookmarksArgs,
)
from panoptikon.db.pql.filters.sortable.extracted_text import (
    MatchText,
    MatchTextArgs,
)
from panoptikon.db.pql.filters.sortable.path_text import (
    MatchPath,
    MatchPathArgs,
)
from panoptikon.db.pql.filters.type_in import TypeIn
from panoptikon.db.pql.types import Filter, SortableFilter
from panoptikon.db.pql.utils import ExtraColumn, QueryState

Filters = Union[InPaths, InBookmarks, TypeIn, MatchPath, MatchText]


def filter_function(filter: Filter, context: CTE, state: QueryState) -> CTE:
    if isinstance(filter, Filter):
        query = filter.build_query(context)
    else:
        raise ValueError(f"Unknown filter type: {filter.__class__.__name__}")
    if state.is_count_query:
        query = query.with_only_columns(context.c.file_id, context.c.item_id)
    else:
        if isinstance(filter, SortableFilter):
            rank_order = literal_column("rank_order")
            if rank_order is not None:
                if filter.gt:
                    query.where(rank_order > filter.gt)
                if filter.lt:
                    query.where(rank_order < filter.lt)

    filter_type = filter.__class__.__name__
    cte_name = f"n_{state.cte_counter}_{filter_type}"
    state.cte_counter += 1
    cte = query.cte(cte_name)

    if (
        isinstance(filter, SortableFilter)
        and not state.is_count_query
        and filter.select_as
        and len(filter.select_as) > 0
    ):
        state.extra_columns.append(
            ExtraColumn(
                column=cte.c.rank_order,
                cte=cte,
                alias=filter.select_as,
                need_join=not filter.order_by,
            )
        )
    return cte
