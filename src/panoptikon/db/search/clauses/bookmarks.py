from panoptikon.db.search.types import BookmarksFilter


def build_bookmarks_clause(
    args: BookmarksFilter | None,
):
    """
    Build a subquery to match only files that are bookmarked
    and optionally restrict to specific namespaces.
    """
    if not args or not args.restrict_to_bookmarks:
        return "", [], ""
    if args.include_wildcard:
        user_condition = "AND (bookmarks.user = ? OR bookmarks.user = '*')"
    else:
        user_condition = "AND bookmarks.user = ?"
    params = [args.user]
    bookmarks_condition = f"""
        JOIN bookmarks
        ON files.sha256 = bookmarks.sha256
        {user_condition}
        """

    if args.namespaces:
        bookmarks_condition += f" AND bookmarks.namespace IN ({', '.join('?' for _ in args.namespaces)})"
        params.extend(args.namespaces)

    additional_columns = ",\n bookmarks.time_added AS time_added"

    return bookmarks_condition, params, additional_columns
