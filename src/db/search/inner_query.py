from typing import List, Tuple

from src.db.search.clauses.any_text import build_any_text_query_clause
from src.db.search.clauses.bookmarks import build_bookmarks_clause
from src.db.search.clauses.extracted_text import (
    build_extracted_text_search_clause,
)
from src.db.search.clauses.image_embed import build_image_embedding_clause
from src.db.search.clauses.path_text import build_path_fts_clause
from src.db.search.types import FileFilters, InnerQueryParams


def build_inner_query(
    query_args: InnerQueryParams,
) -> Tuple[str, List[str | int | float]]:
    """
    Build a query to search for files based on the given tags,
    negative tags, and other conditions.
    """

    args = query_args.filters
    tags = query_args.tags

    # The confidence should be greater than or equal to the given confidence
    min_confidence_condition = (
        f"AND tags_items.confidence >= ?" if tags.min_confidence else ""
    )

    # The setter should match the given setter
    tag_setters_condition = (
        f" AND tags.setter IN ({','.join(['?']*len(tags.setters))})"
        if tags.setters
        else ""
    )

    # The namespace needs to *start* with the given namespace
    tag_namespace_condition = ""
    if tags.namespaces:
        or_cond = " OR ".join(
            ["tags.namespace LIKE ? || '%'"] * len(tags.namespaces)
        )
        tag_namespace_condition = f"AND ({or_cond})"

    positive_tag_params: List[float | str | None] = (
        [
            *tags.positive,
            tags.min_confidence,
            *tags.setters,
            *tags.namespaces,
        ]
        if tags.positive
        else []
    )

    # Negative tags should not be associated with the item
    negative_tags_condition = (
        f"""
        WHERE files.item_id NOT IN (
            SELECT tags_items.item_id
            FROM tags_setters as tags
            JOIN tags_items ON tags.id = tags_items.tag_id
            AND tags.name IN ({','.join(['?']*len(tags.negative))})
            {tag_setters_condition}
            {tag_namespace_condition}
            {min_confidence_condition}
        )
    """
        if tags.negative
        else ""
    )
    negative_tag_params: List[float | str | None] = (
        [
            *tags.negative,
            *tags.setters,
            *tags.namespaces,
            tags.min_confidence,
        ]
        if tags.negative
        else []
    )

    if not tags.any_positive_tags_match:
        having_clause = (
            "HAVING COUNT(DISTINCT tags.name) = ?"
            if not tags.all_setters_required
            else "HAVING COUNT(DISTINCT tags.setter || '-' || tags.name) = ?"
        )
    else:
        having_clause = ""

    additional_select_columns = ""
    # If we need to match on extracted text
    extracted_text_condition, extracted_text_params, et_columns = (
        build_extracted_text_search_clause(
            args.extracted_text,
        )
    )
    if not args.files:
        args.files = FileFilters()
    # The item mimetype should start with one of the given strings
    item_type_condition = ""
    if len(args.files.item_types) > 0:
        or_cond = " OR ".join(
            ["items.type LIKE ? || '%'"] * len(args.files.item_types)
        )
        item_type_condition = f"AND ({or_cond})"

    path_condition = ""
    if len(args.files.include_path_prefixes) > 0:
        path_condition_start = "AND"
        # If no negative or positive tags are provided,
        # this needs to start a WHERE clause
        if not tags.positive and not tags.negative:
            path_condition_start = "WHERE"
        or_cond = " OR ".join(
            ["files.path LIKE ? || '%'"] * len(args.files.include_path_prefixes)
        )
        path_condition = f"{path_condition_start} ({or_cond})"

    additional_select_columns += et_columns

    # If we need to match on text embeddings
    text_embeddings_condition, text_embeddings_params, et_emb_columns = (
        build_extracted_text_search_clause(
            args.extracted_text_embeddings,
        )
    )

    additional_select_columns += et_emb_columns
    # If we need to match on the path or filename using FTS
    path_match_condition, path_params, path_fts_column = build_path_fts_clause(
        args.path,
    )
    additional_select_columns += path_fts_column

    # If this is set, we only search for files that are bookmarked
    bookmarks_condition, bookmark_namespaces, bookmark_columns = (
        build_bookmarks_clause(args.bookmarks)
    )

    additional_select_columns += bookmark_columns

    # Generalized text query
    any_text_query_clause, any_text_query_params, any_text_columns = (
        build_any_text_query_clause(args.any_text)
    )

    additional_select_columns += any_text_columns

    (
        image_embeddings_clause,
        image_embeddings_params,
        image_embeddings_columns,
    ) = build_image_embedding_clause(args.image_embeddings)

    additional_select_columns += image_embeddings_columns

    group_by = ""
    if args.extracted_text_embeddings and text_embeddings_condition:
        group_by = "GROUP BY files.path"
    if args.image_embeddings and image_embeddings_clause:
        group_by = "GROUP BY files.path"

    main_query = (
        f"""
        SELECT
            files.path,
            files.sha256,
            files.last_modified,
            items.type
            {additional_select_columns}
        FROM tags_setters as tags
        JOIN tags_items ON tags.id = tags_items.tag_id
        AND tags.name IN ({','.join(['?']*len(tags.positive))})
        {min_confidence_condition}
        {tag_setters_condition}
        {tag_namespace_condition}
        JOIN files ON tags_items.item_id = files.item_id
        {path_condition}
        JOIN items ON files.item_id = items.id
        {item_type_condition}
        {path_match_condition}
        {extracted_text_condition}
        {text_embeddings_condition}
        {image_embeddings_clause}
        {any_text_query_clause}
        {bookmarks_condition}
        {negative_tags_condition}
        GROUP BY files.path
        {having_clause}
    """
        if tags.positive
        else f"""
        SELECT
            files.path,
            files.sha256,
            files.last_modified,
            items.type
            {additional_select_columns}
        FROM files
        JOIN items ON files.item_id = items.id
        {item_type_condition}
        {path_match_condition}
        {extracted_text_condition}
        {text_embeddings_condition}
        {image_embeddings_clause}
        {any_text_query_clause}
        {bookmarks_condition}
        {negative_tags_condition}
        {path_condition}
        {group_by}
    """
    )
    params: List[str | int | float] = [
        param
        for param in [
            (
                args.extracted_text_embeddings.query
                if args.extracted_text_embeddings and text_embeddings_condition
                else None
            ),
            *positive_tag_params,
            *(args.files.include_path_prefixes if tags.positive else []),
            *args.files.item_types,
            *path_params,
            *extracted_text_params,
            *text_embeddings_params,
            *image_embeddings_params,
            *any_text_query_params,
            *bookmark_namespaces,
            *negative_tag_params,
            *(args.files.include_path_prefixes if not tags.positive else []),
            (
                # Number of tags to match,
                (
                    len(tags.positive)
                    if not tags.all_setters_required
                    # or number of tag-setter pairs to match
                    # if we require all setters to be present for all tags
                    else len(tags.positive) * len(tags.setters)
                )
                # HAVING clause is not needed if no positive tags are provided
                # or if we are matching on *any* positive tags instead of all
                if tags.positive and not tags.any_positive_tags_match
                else None
            ),
        ]
        # Filter out None values
        if param is not None
    ]

    return main_query, params
