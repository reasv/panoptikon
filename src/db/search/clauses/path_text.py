from typing import List

from src.db.search.clauses.utils import should_include_subclause
from src.db.search.types import AnyTextFilter, PathTextFilter


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


def build_path_text_subclause(
    args: AnyTextFilter,
):
    """
    Build a subquery to match file path and filename based on the given conditions.
    """

    path_subclause = ""
    path_params: List[str] = []

    should_include, path_filename_targets = should_include_subclause(
        args.targets, ["path"]
    )
    if not should_include:
        return path_subclause, path_params

    path_conditions = []

    if not path_filename_targets:
        # Match on both path and filename
        path_conditions.append("files_path_fts.path MATCH ?")
        path_params.append(args.query)
    else:
        # Match on either path or filename
        # It is either-or, because the path contains the filename
        targets = set([target for _, target in path_filename_targets])
        if "path" in targets:
            path_conditions.append("files_path_fts.path MATCH ?")
            path_params.append(args.query)
        else:
            # Match on filename
            path_conditions.append("files_path_fts.filename MATCH ?")
            path_params.append(args.query)

    file_path_condition = " OR ".join(path_conditions)

    path_subclause = f"""
        SELECT files.item_id AS item_id, MAX(files_path_fts.rank) AS max_rank
        FROM files_path_fts
        JOIN files ON files_path_fts.rowid = files.id
        WHERE {file_path_condition}
        GROUP BY files.item_id
    """
    return path_subclause, path_params
