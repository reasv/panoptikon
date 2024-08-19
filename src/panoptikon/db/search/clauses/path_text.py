from typing import List

from panoptikon.db.search.types import PathTextFilter


def build_path_fts_clause(
    args: PathTextFilter | None,
):
    """
    Build a subquery to match file path or filename based on the given conditions.
    """
    if not args or not args.query:
        return "", [], ""

    path_condition: str | None = None
    path_params = [args.query]
    if args.only_match_filename:
        path_condition = "path_fts.filename MATCH ?"
    else:
        path_condition = "path_fts.path MATCH ?"
    path_clause = f"""
        JOIN files_path_fts AS path_fts
        ON files.id = path_fts.rowid
        AND {path_condition}
    """

    additional_columns = ",\n path_fts.rank as rank_path_fts"
    return path_clause, path_params, additional_columns


def build_path_text_subclause(args: PathTextFilter):
    """
    Build a subquery to match file path and filename based on the given conditions.
    """

    path_subclause = ""
    path_params: List[str] = [args.query]

    if not args.only_match_filename:
        # Match on both path and filename
        path_condition = "files_path_fts.path MATCH ?"
    else:
        # Match on filename
        path_condition = "files_path_fts.filename MATCH ?"

    path_subclause = f"""
        SELECT files.item_id AS item_id, MAX(files_path_fts.rank) AS max_rank
        FROM files_path_fts
        JOIN files ON files_path_fts.rowid = files.id
        WHERE {path_condition}
        GROUP BY files.item_id
    """
    return path_subclause, path_params
