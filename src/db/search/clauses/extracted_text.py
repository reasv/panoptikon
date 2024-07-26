from typing import List

from src.db.search.clauses.utils import should_include_subclause
from src.db.search.types import ExtractedTextParams


def build_extracted_text_search_clause(args: ExtractedTextParams | None):
    """
    Build a subquery to match extracted text based on the given conditions.
    """
    # Define subquery for matching extracted text
    if not args or not args.query:
        return "", [], ""
    # Check if the text query is a vector query
    is_vector_query = isinstance(args.query, bytes)

    subclause, params = build_extracted_text_search_subclause(args)
    if len(subclause) == 0:
        return "", [], ""
    if is_vector_query:
        extracted_text_condition = f"""
            JOIN (
                {subclause}
            ) AS text_vector_matches
            ON files.item_id = text_vector_matches.item_id
        """
        additional_columns = (
            ",\n text_vector_matches.distance AS text_vec_distance"
        )
    else:
        extracted_text_condition = f"""
            JOIN (
                {subclause}
            ) AS extracted_text_matches
            ON files.item_id = extracted_text_matches.item_id
        """
        additional_columns = ",\n extracted_text_matches.max_rank AS rank_fts"

    return extracted_text_condition, params, additional_columns


def build_extracted_text_search_subclause(args: ExtractedTextParams):
    """
    Build a subquery to match extracted text based on the given conditions.
    """
    # Check if the text query is a vector query
    is_vector_query = isinstance(args.query, bytes)

    # Define subquery for matching extracted text
    extracted_text_subclause = ""
    extracted_text_params: List[str | float | bytes] = []

    should_include, type_setter_pairs = should_include_subclause(
        args.targets, ["ocr", "stt"]
    )
    if not should_include:
        return extracted_text_subclause, extracted_text_params

    if is_vector_query:
        where_conditions = ["et_vec.sentence_embedding MATCH ?"]
    else:
        where_conditions = ["et_fts.text MATCH ?"]

    extracted_text_params.append(args.query)

    if type_setter_pairs:
        include_pairs_conditions = " OR ".join(
            ["(log.type = ? AND log.setter = ?)"] * len(type_setter_pairs)
        )
        where_conditions.append(f"({include_pairs_conditions})")
        for type, setter in type_setter_pairs:
            extracted_text_params.extend([type, setter])

    if args.languages:
        where_conditions.append(
            "et.language IN ({','.join(['?']*len(languages))})"
        )
        extracted_text_params.extend(args.languages)
        if args.language_min_confidence:
            where_conditions.append("et.language_confidence >= ?")
            extracted_text_params.append(args.language_min_confidence)

    if args.min_confidence:
        where_conditions.append("et.confidence >= ?")
        extracted_text_params.append(args.min_confidence)

    if is_vector_query:
        extracted_text_subclause = f"""
            SELECT et.item_id AS item_id, MIN(et_vec.distance) AS distance
            FROM extracted_text_embed AS et_vec
            JOIN extracted_text AS et ON et_vec.id = et.id
            JOIN data_extraction_log AS log ON et.log_id = log.id
            WHERE {" AND ".join(where_conditions)}
            GROUP BY et.item_id
        """
    else:
        extracted_text_subclause = f"""
            SELECT et.item_id AS item_id, MAX(et_fts.rank) AS rank
            FROM extracted_text_fts AS et_fts
            JOIN extracted_text AS et ON et_fts.rowid = et.id
            JOIN data_extraction_log AS log ON et.log_id = log.id
            WHERE {" AND ".join(where_conditions)}
            GROUP BY et.item_id
        """
    return extracted_text_subclause, extracted_text_params
