from pypika import AliasedQuery
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import Filter
from panoptikon.db.pql.utils import CTE, QueryState


def filter_function(filter: Filter, context: Selectable, state: QueryState):
    if isinstance(filter, Filter):
        query = filter.build_query(context)
    else:
        raise ValueError(f"Unknown filter type: {filter.__class__.__name__}")
    filter_type = filter.__class__.__name__
    cte_name = f"n_{state.cte_counter}_{filter_type}"
    state.cte_counter += 1
    state.cte_list.append(CTE(query, cte_name))
    return AliasedQuery(cte_name)
