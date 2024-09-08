from typing import List

from pydantic import Field
from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.tables import items
from panoptikon.db.pql.types import Filter
from panoptikon.db.pql.utils import wrap_select


class TypeIn(Filter):
    type_in: List[str] = Field(
        default_factory=list,
        title="MIME Type must begin with one of the given strings",
    )

    def build_query(self, context: Selectable) -> Selectable:
        mime_types = self.type_in
        query = (
            wrap_select(context)
            .join(items)
            .on(context.item_id == items.id)
            .where(
                Criterion.any(
                    [items.type.like(f"{mime}%") for mime in mime_types]
                )
            )
        )
        return query
