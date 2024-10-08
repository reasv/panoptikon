from typing import Optional, Union

from pydantic import BaseModel, PrivateAttr
from sqlalchemy import CTE, Select

from panoptikon.db.pql.types import (
    FilterSelect,
    Operator,
    QueryState,
    get_std_cols,
)


class Filter(BaseModel):
    _validated: bool = PrivateAttr(False)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        raise NotImplementedError("build_query not implemented")

    def wrap_query(self, query: Select, context: CTE, state: QueryState) -> CTE:
        if state.is_count_query:
            query = query.with_only_columns(*get_std_cols(context, state))
        cte_name = self.get_cte_name(state.cte_counter)
        state.cte_counter += 1
        state.selects[cte_name] = FilterSelect(query, context)
        return query.cte(cte_name)

    def get_cte_name(self, counter: int) -> str:
        filter_type = self.__class__.__name__
        cte_name = f"n{counter}_{filter_type}"
        return cte_name

    def is_validated(self) -> bool:
        return self._validated

    def set_validated(self, value: bool):
        self._validated = value
        return self if self._validated else None

    def raise_if_not_validated(self):
        """Raise a ValueError if get_validated() has not been called.
        Raises:
            ValueError: If the filter has not been validated.
        """
        if not self.is_validated():
            raise ValueError("Filter was not validated")

    def get_validated(self) -> Optional[Union["Filter", Operator]]:
        """Pre-process filter args and validate them.
        Should return a Filter object or None if the filter should be skipped.
        Must be called before build_query.
        Can raise a ValueError if the filter args are invalid.
        """
        if self.is_validated():
            return self
        else:
            return self._validate()

    def _validate(self) -> Optional[Union["Filter", Operator]]:
        raise NotImplementedError("_validate not implemented")
