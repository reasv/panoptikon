from dataclasses import dataclass
from typing import List

from pydantic import Field
from pypika import AliasedQuery
from pypika import SQLLiteQuery as Query
from pypika import Table
from pypika.functions import Function
from pypika.queries import QueryBuilder, Selectable
from pypika.terms import Comparator

from panoptikon.db.pql.types import OrderTypeNN

VERY_LARGE_NUMBER = 9223372036854775805
VERY_SMALL_NUMBER = -9223372036854775805


class Max(Function):
    def __init__(self, term, *default_values, **kwargs):
        super(Max, self).__init__("MAX", term, *default_values, **kwargs)


class Min(Function):
    def __init__(self, term, *default_values, **kwargs):
        super(Min, self).__init__("MIN", term, *default_values, **kwargs)


class Match(Comparator):
    match_ = " MATCH "


def wrap_select(selectable: Selectable) -> QueryBuilder:
    return Query.from_(selectable).select("file_id", "item_id", "sha256")


@dataclass
class CTE:
    query: Selectable
    name: str


@dataclass
class OrderByFilter:
    cte: AliasedQuery
    direction: OrderTypeNN
    priority: int = 0


class QueryState:
    def __init__(self):
        self.cte_list: List[CTE] = []  # Holds all generated CTEs
        self.order_list: List[OrderByFilter] = []  # Holds order_by clauses
        self.cte_counter = 0  # Counter to generate unique CTE names
        self.root_query = None  # The main query that uses CTE names


def get_order_by_field(default: bool):
    return Field(
        default=default,
        title="Order by this filter's rank output",
        description="This filter generates a value that can be used for ordering.",
    )


def get_order_direction_field(default: OrderTypeNN):
    return Field(
        default=default,
        title="Order Direction",
        description="""
The order direction for this filter.
If not set, the default order direction for this field is used.
""",
    )


def get_order_priority_field(default: int):
    return Field(
        default=default,
        title="Order By Priority",
        description="""
The priority of this filter in the order by clause.
If there are multiple filters with order_by set to True,
the priority is used to determine the order.
If two filter order bys have the same priority,
their values are coalesced into a single column to order by
""",
    )
