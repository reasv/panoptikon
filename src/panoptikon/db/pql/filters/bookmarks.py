from pypika import Criterion, Table
from pypika.queries import Selectable

from panoptikon.db.pql.pql_model import BookmarksFilterModel
from panoptikon.db.pql.utils import wrap_select

bookmarks = Table("bookmarks")


def bookmarks_filter(
    filter: BookmarksFilterModel, context: Selectable
) -> Selectable:
    criterions = []
    if filter.bookmarks.namespaces:
        ns = filter.bookmarks.namespaces
        in_condition = bookmarks.namespace.isin(ns)
        if filter.bookmarks.sub_ns:
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

    if filter.bookmarks.include_wildcard:
        criterions.append(
            (bookmarks.user == filter.bookmarks.user) | (bookmarks.user == "*")
        )
    else:
        criterions.append(bookmarks.user == filter.bookmarks.user)

    return (
        wrap_select(context)
        .inner_join(bookmarks)
        .on_field("sha256")
        .select(bookmarks.time_added.as_("order_rank"))
        .where(
            Criterion.all(criterions),
        )
    )
