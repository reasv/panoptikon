from typing import List

from pydantic import Field
from sqlalchemy import Select, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import QueryState, get_std_cols


class TypeIn(Filter):
    type_in: List[str] = Field(
        ...,
        title="MIME Type must begin with one of the given strings",
    )

    def get_validated(self):
        return self.set_validated(bool(self.type_in))

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import items

        mime_types = self.type_in
        return self.wrap_query(
            (
                select(*get_std_cols(context, state))
                .join(items, items.c.id == context.c.item_id)
                .where(
                    or_(*[items.c.type.like(f"{mime}%") for mime in mime_types])
                )
            ),
            context,
            state,
        )
