from dataclasses import dataclass, field
from typing import List

from sqlalchemy import CTE, Column, ColumnClause, cte
from sqlalchemy.sql.elements import KeyedColumnElement

from panoptikon.db.pql.types import OrderTypeNN

VERY_LARGE_NUMBER = 9223372036854775805
VERY_SMALL_NUMBER = -9223372036854775805


@dataclass
class OrderByFilter:
    cte: CTE
    direction: OrderTypeNN
    priority: int = 0


@dataclass
class ExtraColumn:
    column: ColumnClause | Column | KeyedColumnElement
    cte: CTE
    alias: str
    need_join: bool = False


@dataclass
class QueryState:
    order_list: List[OrderByFilter] = field(default_factory=list)
    extra_columns: List[ExtraColumn] = field(default_factory=list)
    cte_counter: int = 0
    is_count_query: bool = False
