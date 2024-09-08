from typing import List

from pydantic import BaseModel, Field
from pypika import Criterion
from pypika.queries import Selectable

from panoptikon.db.pql.tables import bookmarks
from panoptikon.db.pql.types import (
    OrderTypeNN,
    SortableFilter,
    get_order_by_field,
    get_order_direction_field,
)
from panoptikon.db.pql.utils import wrap_select


class InBookmarksArgs(BaseModel):
    enable: bool = Field(
        default=True,
        title="Enable the filter",
        description="""
Must be set to True, this option only exists to make sure the filter is not empty,
given that that all fields are optional.
""",
    )
    namespaces: List[str] = Field(
        default_factory=list,
        title="Bookmark Namespaces",
        description="""
List of bookmark namespaces to filter by. If sub_ns is set to True, the filter will also
include all sub-namespaces of the given namespaces (ie, namespace.*).
If empty, all bookmarks will be included.
""",
    )
    sub_ns: bool = Field(
        default=False,
        title="Include Sub-namespaces",
        description="Include all sub-namespaces of the given namespaces (namespace.*).",
    )
    user: str = "user"
    include_wildcard: bool = Field(
        default=True,
        title="Include Wildcard User",
        description="Include bookmarks set to the wildcard user ('*').",
    )


class InBookmarks(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("desc")
    in_bookmarks: InBookmarksArgs = Field(
        ...,
        title="Restrict search to Bookmarks",
        description="Only include items that are bookmarked.",
    )

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
