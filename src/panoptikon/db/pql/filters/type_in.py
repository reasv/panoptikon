from typing import List

from pydantic import Field
from sqlalchemy import Select, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.types import Filter


class TypeIn(Filter):
    type_in: List[str] = Field(
        default_factory=list,
        title="MIME Type must begin with one of the given strings",
    )

    def validate(self) -> bool:
        return self.set_validated(bool(self.type_in))

    def build_query(self, context: CTE) -> Select:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import items

        mime_types = self.type_in
        return (
            select(context.c.file_id, context.c.item_id)
            .join(items, items.c.id == context.c.item_id)
            .where(or_(*[items.c.type.like(f"{mime}%") for mime in mime_types]))
        )
