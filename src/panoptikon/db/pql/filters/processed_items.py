from typing import List

from pydantic import Field
from sqlalchemy import Select, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import QueryState, get_std_cols, get_std_group_by


class HasDataFrom(Filter):
    has_data_from: str = Field(
        ...,
        title="Item must have item_data produced by the given setter name",
    )

    def validate(self):
        return self.set_validated(bool(self.has_data_from))

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import files, item_data, items, setters

        setter = self.has_data_from
        return self.wrap_query(
            select(*get_std_cols(context, state))
            .join(
                item_data,
                item_data.c.item_id == context.c.item_id,
            )
            .join(
                setters,
                setters.c.id == item_data.c.setter_id,
            )
            .where(setters.c.name == setter)
            .group_by(
                *get_std_group_by(context, state),
            ),
            context,
            state,
        )
