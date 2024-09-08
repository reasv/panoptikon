from pypika import AliasedQuery
from pypika.queries import Selectable

from panoptikon.db.pql.filters.bookmarks import bookmarks_filter
from panoptikon.db.pql.filters.path_in import path_in
from panoptikon.db.pql.filters.path_text import path_text_filter
from panoptikon.db.pql.filters.type_in import type_in
from panoptikon.db.pql.pql_model import (
    BookmarksFilterModel,
    Filter,
    PathFilterModel,
    PathTextFilterModel,
    TypeFilterModel,
)
from panoptikon.db.pql.utils import CTE, QueryState


def filter_function(filter: Filter, context: Selectable, state: QueryState):
    if isinstance(filter, PathFilterModel):
        query = path_in(filter, context)
    elif isinstance(filter, TypeFilterModel):
        query = type_in(filter, context)
    elif isinstance(filter, PathTextFilterModel):
        query = path_text_filter(filter, context)
    elif isinstance(filter, BookmarksFilterModel):
        query = bookmarks_filter(filter, context)
    else:
        raise ValueError("Unknown filter type")
    filter_type = filter.__class__.__name__
    cte_name = f"n_{state.cte_counter}_{filter_type}"
    state.cte_counter += 1
    state.cte_list.append(CTE(query, cte_name))
    return AliasedQuery(cte_name)
