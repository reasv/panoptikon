from dataclasses import dataclass, field
from typing import List

from sqlalchemy import CTE

from panoptikon.db.pql.types import OrderTypeNN

VERY_LARGE_NUMBER = 9223372036854775805
VERY_SMALL_NUMBER = -9223372036854775805


@dataclass
class OrderByFilter:
    cte: CTE
    direction: OrderTypeNN
    priority: int = 0


@dataclass
class QueryState:
    order_list: List[OrderByFilter] = field(default_factory=list)
    cte_counter: int = 0
