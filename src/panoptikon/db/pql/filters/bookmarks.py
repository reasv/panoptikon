from typing import List

from pydantic import BaseModel, Field
from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import OrderTypeNN, SortableFilter
from panoptikon.db.pql.tables import bookmarks
from panoptikon.db.pql.utils import (
    get_order_by_field,
    get_order_direction_field,
    wrap_select,
)


class InBookmarksArgs(BaseModel):
    require: bool = True
    namespaces: List[str] = Field(default_factory=list)
    sub_ns: bool = False
    user: str = "user"
    include_wildcard: bool = True


class InBookmarks(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    in_bookmarks: InBookmarksArgs

    def build_query(self, context: Selectable) -> Selectable:
        args = self.in_bookmarks
        criterions = []
        if args.namespaces:
            ns = args.namespaces
            in_condition = bookmarks.namespace.isin(ns)
            if args.sub_ns:
                criterions.append(
                    Criterion.any(
                        [in_condition]
                        + [
                            bookmarks.namespace.like(f"{namespace}.%")
                            for namespace in ns
                        ]
                    )
                )
            else:
                criterions.append(in_condition)

        if args.include_wildcard:
            criterions.append(
                (bookmarks.user == args.user) | (bookmarks.user == "*")
            )
        else:
            criterions.append(bookmarks.user == args.user)

        return (
            wrap_select(context)
            .inner_join(bookmarks)
            .on_field("sha256")
            .select(bookmarks.time_added.as_("order_rank"))
            .where(
                Criterion.all(criterions),
            )
        )
