from typing import List

from src.db.search.types import (
    ExtractedTextEmbeddingsFilter,
    ExtractedTextFilter,
)


def build_extracted_text_search_clause(
    args: ExtractedTextFilter | ExtractedTextEmbeddingsFilter | None,
):
    """
    Build a subquery to match extracted text based on the given conditions.
    """
    # Define subquery for matching extracted text
    if not args or not args.query:
        return "", [], ""
    # Check if the text query is a vector query
    is_vector_query = isinstance(args, ExtractedTextEmbeddingsFilter)

    subclause, params = build_extracted_text_search_subclause(args)
    if len(subclause) == 0:
        return "", [], ""
    if is_vector_query:
        cond = f"""
            {subclause}
        """
        additional_columns = (
            ",\n MIN(vec_distance_L2(et_vec.embedding, ?)) AS text_vec_distance"
        )
    else:
        cond = f"""
            JOIN (
                {subclause}
            ) AS extracted_text_matches
            ON files.item_id = extracted_text_matches.item_id
        """
        additional_columns = ",\n extracted_text_matches.max_rank AS rank_fts"

    return cond, params, additional_columns


def build_extracted_text_search_subclause(
    args: ExtractedTextFilter | ExtractedTextEmbeddingsFilter,
):
    """
    Build a subquery to match extracted text based on the given conditions.
    """
    # Check if the text query is a vector query
    is_vector_query = isinstance(args, ExtractedTextEmbeddingsFilter)

    # Define subquery for matching extracted text
    extracted_text_subclause = ""
    params: List[str | float | bytes] = []

    where_conditions = []
    if is_vector_query:
        # If the query is a vector query, we need to match on the text embeddings model
        params = [args.model]
    else:
        where_conditions = ["et_fts.text MATCH ?"]
        params = [args.query]

    if args.targets:
        # Text setter names must be one of the given targets, use IN clause
        where_conditions.append(
            f"text_setters.name IN ({','.join(['?']*len(args.targets))})"
        )
        params.extend(args.targets)

    if args.languages:
        where_conditions.append(
            f"et.language IN ({','.join(['?']*len(args.languages))})"
        )
        params.extend(args.languages)
        if args.language_min_confidence:
            where_conditions.append("et.language_confidence >= ?")
            params.append(args.language_min_confidence)

    if args.min_confidence:
        where_conditions.append("et.confidence >= ?")
        params.append(args.min_confidence)

    if is_vector_query:
        join_condition = (
            "AND " + " AND ".join(where_conditions) if where_conditions else ""
        )
        extracted_text_subclause = f"""
            JOIN item_data AS vec_data
                ON vec_data.data_type = 'text-embedding'
                AND vec_data.item_id = files.item_id
            JOIN setters AS vec_setters
                ON vec_data.setter_id = vec_setters.id
                AND vec_setters.name = ?
            JOIN embeddings AS et_vec
                ON et_vec.id = vec_data.id
            JOIN extracted_text AS et
                ON vec_data.source_id = et.id
            JOIN item_data AS et_data
                ON et_data.id = et.id
            JOIN setters AS text_setters
                ON et_data.setter_id = text_setters.id
                {join_condition}
        """
    else:
        extracted_text_subclause = f"""
            SELECT
                et_data.item_id AS item_id,
                MAX(et_fts.rank) AS max_rank
            FROM extracted_text_fts AS et_fts
            JOIN extracted_text AS et
                ON et_fts.rowid = et.id
            JOIN item_data AS et_data
                ON et_data.id = et.id
            JOIN setters AS text_setters
                ON et_data.setter_id = text_setters.id
            WHERE {" AND ".join(where_conditions)}
            GROUP BY et_data.item_id
        """
    return extracted_text_subclause, params
