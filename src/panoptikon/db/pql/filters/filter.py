import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Sequence, Union, get_args

from pydantic import BaseModel, Field
from sqlalchemy import (
    CTE,
    Column,
    ColumnClause,
    Label,
    Select,
    asc,
    desc,
    func,
    literal_column,
    over,
    select,
)
from sqlalchemy.sql.elements import KeyedColumnElement

from panoptikon.db.pql.types import QueryState, get_std_cols


class Filter(BaseModel):
    _validated: bool = False

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        raise NotImplementedError("build_query not implemented")

    def wrap_query(self, query: Select, context: CTE, state: QueryState) -> CTE:
        if state.is_count_query:
            query = query.with_only_columns(*get_std_cols(context, state))
        cte_name = self.get_cte_name(state.cte_counter)
        state.cte_counter += 1
        return query.cte(cte_name)

    def get_cte_name(self, counter: int) -> str:
        filter_type = self.__class__.__name__
        cte_name = f"n_{counter}_{filter_type}"
        return cte_name

    def is_validated(self) -> bool:
        return self._validated

    def set_validated(self, value: bool):
        self._validated = value
        return self._validated

    def raise_if_not_validated(self):
        """Raise a ValueError if validate() has not been called.
        Raises:
            ValueError: If the filter has not been validated.
        """
        if not self.is_validated():
            raise ValueError("Filter was not validated")

    def validate(self) -> bool:
        """Pre-process filter args and validate them.
        Must return True if the filter should be included, False otherwise.
        Must be called before build_query.
        Can raise a ValueError if the filter args are invalid.
        """
        raise NotImplementedError("validate not implemented")
