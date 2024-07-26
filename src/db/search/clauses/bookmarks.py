from src.db.search.types import BookmarkParams


def build_bookmarks_clause(
    args: BookmarkParams | None,
):
    """
    Build a subquery to match only files that are bookmarked
    and optionally restrict to specific namespaces.
    """
    if not args or not args.restrict_to_bookmarks:
        return "", [], ""
    bookmarks_condition = """
        JOIN bookmarks
        ON files.sha256 = bookmarks.sha256
        """
    if args.namespaces:
        bookmarks_condition += " AND bookmarks.namespace IN ("
        for i, _ in enumerate(args.namespaces):
            if i == 0:
                bookmarks_condition += "?"
            else:
                bookmarks_condition += ", ?"
        bookmarks_condition += ")"

    additional_columns = ",\n bookmarks.time_added AS time_added"

    return bookmarks_condition, args.namespaces, additional_columns
