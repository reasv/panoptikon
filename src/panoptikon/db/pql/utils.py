from typing import List

from sqlalchemy import CTE
from sqlalchemy.sql.elements import KeyedColumnElement

from panoptikon.db.pql.types import QueryState


def get_std_cols(cte: CTE, state: QueryState) -> List[KeyedColumnElement]:
    if state.is_text_query:
        return [cte.c.item_id, cte.c.file_id, cte.c.text_id]
    return [cte.c.item_id, cte.c.file_id]


def get_std_group_by(cte: CTE, state: QueryState) -> List[KeyedColumnElement]:
    if state.is_text_query:
        return [cte.c.text_id, cte.c.file_id]
    return [cte.c.file_id]
