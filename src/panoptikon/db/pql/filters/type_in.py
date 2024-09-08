from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import TypeFilterModel
from panoptikon.db.pql.utils import items_table, wrap_select


def type_in(filter: TypeFilterModel, context: Selectable) -> Selectable:
    query = (
        wrap_select(context)
        .join(items_table)
        .on(context.item_id == items_table.id)
        .where(
            Criterion.any(
                [
                    items_table.type.like(f"{mime}%")
                    for mime in filter.mime_types
                ]
            )
        )
    )
    return query
