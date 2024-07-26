from typing import List

from src.db.search.clauses.extracted_text import (
    build_extracted_text_search_subclause,
)
from src.db.search.clauses.path_text import build_path_text_subclause
from src.db.search.types import AnyTextFilter, ExtractedTextParams


def build_any_text_query_clause(
    args: AnyTextFilter | None,
):
    """
    Build a subquery to match any text (from extracted text or file path/filename)
    based on the given conditions.
    """

    if not args or not args.query:
        return "", [], ""

    subqueries = []
    params: List[str | float | bytes] = []

    # Define subquery for matching extracted text
    extracted_text_subclause, extracted_text_params = (
        build_extracted_text_search_subclause(
            ExtractedTextParams(
                query=args.query,
                targets=args.targets,
            )
        )
    )
    if extracted_text_subclause:
        subqueries.append(extracted_text_subclause)
        params.extend(extracted_text_params)

    # Define subquery for matching file path and filename
    path_subclause, path_params = build_path_text_subclause(args)

    if path_subclause:
        subqueries.append(path_subclause)
        params.extend(path_params)

    if len(subqueries) == 0:
        return "", [], ""

    combined_subquery = " UNION ALL ".join(subqueries)

    final_query = f"""
        JOIN (
            WITH combined_results AS (
                {combined_subquery}
            )
            SELECT item_id, MAX(rank) AS max_rank
            FROM combined_results
            GROUP BY item_id
        ) AS text_matches
        ON files.item_id = text_matches.item_id
    """

    additional_columns = ",\n text_matches.max_rank AS rank_any_text"
    return final_query, params, additional_columns
