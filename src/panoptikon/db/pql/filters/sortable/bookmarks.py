from typing import List

from pydantic import BaseModel, Field
from sqlalchemy import Select, and_, asc, desc, func, or_
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.types import (
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_order_direction_field_rownum,
    get_std_cols,
    get_std_group_by,
)


class InBookmarksArgs(BaseModel):
    filter: bool = Field(
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
    direction: OrderTypeNN = get_order_direction_field("desc")
    row_n_direction: OrderTypeNN = get_order_direction_field_rownum("desc")
    in_bookmarks: InBookmarksArgs = Field(
        ...,
        title="Restrict search to Bookmarks",
        description="Only include items that are bookmarked.",
    )

    def _validate(self):
        return self.set_validated(self.in_bookmarks.filter)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import bookmarks, files

        args = self.in_bookmarks
        criterions = []
        if args.namespaces:
            ns = args.namespaces
            in_condition = bookmarks.c.namespace.in_(ns)
            if args.sub_ns:
                criterions.append(
                    or_(
                        *(
                            [in_condition]
                            + [
                                bookmarks.c.namespace.like(f"{namespace}.%")
                                for namespace in ns
                            ]
                        )
                    )
                )
            else:
                criterions.append(in_condition)

        if args.include_wildcard:
            criterions.append(
                (bookmarks.c.user == args.user) | (bookmarks.c.user == "*")
            )
        else:
            criterions.append(bookmarks.c.user == args.user)

        rank_column = self.derive_rank_column(func.max(bookmarks.c.time_added))
        return self.wrap_query(
            (
                select(
                    *get_std_cols(context, state),
                    rank_column,
                )
                .join(files, files.c.id == context.c.file_id)
                .join(bookmarks, bookmarks.c.sha256 == files.c.sha256)
                .where(
                    and_(*criterions),
                )
                .group_by(*get_std_group_by(context, state))
            ),
            context,
            state,
        )
