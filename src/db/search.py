import os
import sqlite3
from typing import List, Tuple

from typeguard import typechecked

from src.db.utils import pretty_print_SQL
from src.types import (
    AnyTextParams,
    BookmarkParams,
    ExtractedTextParams,
    FileParams,
    FileSearchResult,
    InnerQueryParams,
    InnerQueryTagParams,
    OrderParams,
    PathQueryParams,
    QueryFilters,
    QueryParams,
    QueryTagParams,
    SearchQuery,
)


@typechecked
def clean_input(args: SearchQuery) -> SearchQuery:
    args.query.tags = clean_tag_params(args.query.tags)
    return args


@typechecked
def clean_tag_params(args: QueryTagParams):
    # Normalize/clean/deduplicate the inputs
    def clean_tag_list(tag_list: List[str] | None) -> List[str]:
        if not tag_list:
            return []
        cleaned_tags = [
            tag.lower().strip() for tag in tag_list if tag.strip() != ""
        ]
        return list(set(cleaned_tags))

    tag_args = QueryTagParams(
        pos_match_all=clean_tag_list(args.pos_match_all),
        pos_match_any=clean_tag_list(args.pos_match_any),
        neg_match_any=clean_tag_list(args.neg_match_any),
        neg_match_all=clean_tag_list(args.neg_match_all),
        all_setters_required=args.all_setters_required,
        setters=args.setters,
        namespaces=args.namespaces,
        min_confidence=args.min_confidence,
    )
    if len(tag_args.pos_match_any) == 1:
        # If only one tag is provided for "match any",
        # we can just set it as a regular "match all" tag
        tag_args.pos_match_all.append(tag_args.pos_match_any[0])
        tag_args.pos_match_any = []
    if len(tag_args.neg_match_all) == 1:
        # If only one tag is provided for negative "match all",
        # we can just set it as a regular "match any" negative tag
        tag_args.neg_match_any.append(tag_args.neg_match_all[0])
        tag_args.neg_match_all = []

    return tag_args


@typechecked
def search_files(
    conn: sqlite3.Connection,
    args: SearchQuery,
):
    args = clean_input(args)

    # Build the main query
    search_query, search_query_params = build_search_query(args=args.query)
    # Debugging
    # print_search_query(count_query, params)
    cursor = conn.cursor()
    if args.count:
        # First query to get the total count of items matching the criteria
        count_query = f"""
        SELECT COUNT(*)
        FROM (
            {search_query}
        )
        """
        try:
            cursor.execute(count_query, search_query_params)
        except Exception as e:
            # Debugging
            pretty_print_SQL(count_query, search_query_params)
            raise e
        total_count: int = cursor.fetchone()[0]
    else:
        total_count = 0

    # Build the ORDER BY clause
    order_by_clause, order_by_params = build_order_by_clause(
        filters=args.query.filters, oargs=args.order_args
    )

    try:
        cursor.execute(
            (search_query + order_by_clause),
            [*search_query_params, *order_by_params],
        )
    except Exception as e:
        # Debugging
        pretty_print_SQL(
            (search_query + order_by_clause),
            [*search_query_params, *order_by_params],
        )
        raise e
    results_count = cursor.rowcount
    while row := cursor.fetchone():
        file = FileSearchResult(*row[0:4])
        if args.check_path and not os.path.exists(file.path):
            continue
        yield file, total_count
    if results_count == 0:
        return []


def build_search_query(
    args: QueryParams,
):
    tags = args.tags
    filters = args.filters
    if tags.pos_match_any and not tags.pos_match_all:
        # If "match any" tags are provided,
        # but no positive match all tags are provided
        # We need to build a query to match on *any* of them being present
        main_query, params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagParams(
                    positive=tags.pos_match_any,
                    negative=tags.neg_match_any,
                    all_setters_required=False,
                    any_positive_tags_match=True,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )
    else:
        # Basic case where we need to match all positive tags and none of the negative tags
        # There might even be no tags at all in this case
        main_query, params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagParams(
                    positive=tags.pos_match_all,
                    negative=tags.neg_match_any,
                    any_positive_tags_match=False,
                    all_setters_required=tags.all_setters_required,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )

    if tags.pos_match_any and tags.pos_match_all:
        # If tags "match any" are provided along with match all regular positive tags
        # We need to build a separate query to match on *any* of them being present
        # And then intersect the results with the main query
        any_tags_query, any_tags_params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagParams(
                    positive=tags.pos_match_any,
                    negative=[],
                    any_positive_tags_match=True,
                    all_setters_required=False,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )

        # Append the tags query to the main query
        main_query = f"""
        {main_query}
        INTERSECT
        {any_tags_query}
        """
        params += any_tags_params

    if tags.neg_match_all:
        # If negative tags "match all" are provided
        # We need to build a separate query to match on *all* of them being present
        # And then exclude the results from the main query

        negative_tags_query, negative_tags_params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagParams(
                    positive=tags.neg_match_all,
                    negative=[],
                    any_positive_tags_match=False,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                    all_setters_required=tags.all_setters_required,
                ),
                filters=filters,
            )
        )

        # Append the negative tags query to the main query
        if tags.pos_match_any and tags.pos_match_all:
            # If we already have an INTERSECT query, we need to use it as a subquery
            main_query = f"""
            SELECT *
            FROM (
                {main_query}
            )
            EXCEPT
            {negative_tags_query}
            """
        else:
            main_query = f"""
            {main_query}
            EXCEPT
            {negative_tags_query}
            """
        params += negative_tags_params

    return main_query, params


def build_order_by_clause(
    filters: QueryFilters,
    oargs: OrderParams,
) -> Tuple[str, List[str | int | float]]:
    # Determine order_by_clause and default order setting based on order_by value

    order = oargs.order
    default_order_by_clause = "last_modified"
    match oargs.order_by:
        case "rank_fts":
            if filters.extracted_text:
                order_by_clause = "rank_fts"
            else:
                order_by_clause = default_order_by_clause
        case "rank_path_fts":
            if filters.path:
                order_by_clause = "rank_path_fts"
            else:
                order_by_clause = default_order_by_clause
        case "time_added":
            if filters.bookmarks:
                order_by_clause = "time_added"
            else:
                order_by_clause = default_order_by_clause
        case "rank_any_text":
            if filters.any_text:
                order_by_clause = "rank_any_text"
            else:
                order_by_clause = default_order_by_clause
        case "path":
            order_by_clause = "path"
            # Default order for path is ascending
            if order is None:
                order = "asc"
        case "text_vec_distance":
            if filters.extracted_text_embeddings:
                order_by_clause = "text_vec_distance"
                # Default order for text_vec_distance is ascending
                if order is None:
                    order = "asc"
            else:
                order_by_clause = default_order_by_clause
        case _:
            order_by_clause = default_order_by_clause

    # Default order for all other order_by values is descending
    if order is None:
        order = "desc"
    # Determine the order clause
    order_clause = "DESC" if order == "desc" else "ASC"

    # Second query to get the items with pagination
    clause = f"""
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?
    """
    page = max(oargs.page, 1)
    page_size = oargs.page_size or 1000000  # Mostly for debugging purposes
    offset = (page - 1) * page_size

    query_params: List[str | int | float] = [page_size, offset]

    return clause, query_params


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
        args.files = FileParams()
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
        {any_text_query_clause}
        {bookmarks_condition}
        {negative_tags_condition}
        {path_condition}
    """
    )
    params: List[str | int | float] = [
        param
        for param in [
            *positive_tag_params,
            *(args.files.include_path_prefixes if tags.positive else []),
            *args.files.item_types,
            *path_params,
            *extracted_text_params,
            *text_embeddings_params,
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


def build_path_fts_clause(
    args: PathQueryParams | None,
):
    """
    Build a subquery to match file path or filename based on the given conditions.
    """
    if not args or not args.query:
        return "", [], ""

    path_condition: str | None = None
    path_params = [args.query]
    if args.only_match_filename:
        path_condition = "files_path_fts.filename MATCH ?"
    else:
        path_condition = "files_path_fts.path MATCH ?"
    path_clause = f"""
        JOIN files_path_fts AS path_fts
        ON files.id = path_fts.rowid
        AND {path_condition}
    """

    additional_columns = ",\n path_fts.rank as rank_path_fts"
    return path_clause, path_params, additional_columns


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


def build_any_text_query_clause(
    args: AnyTextParams | None,
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


def build_path_text_subclause(
    args: AnyTextParams,
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
        SELECT files.item_id AS item_id, MAX(files_path_fts.rank) AS rank
        FROM files_path_fts
        JOIN files ON files_path_fts.rowid = files.id
        WHERE {file_path_condition}
        GROUP BY files.item_id
    """
    return path_subclause, path_params


def filter_targets_by_type(
    model_types: List[str], targets: List[Tuple[str, str]]
):
    """
    Filter a list of targets based on the given model types.
    """
    return [
        (model_type, setter)
        for model_type, setter in targets
        if model_type in model_types
    ]


def should_include_subclause(
    targets: List[Tuple[str, str]] | None, own_target_types: List[str]
):
    """
    Check if a subclause should be included based on the given targets.
    """
    if targets:
        own_targets = filter_targets_by_type(own_target_types, targets)
        if not own_targets:
            # If targets were provided, but none of them are our own targets,
            # Then this subclause was specifically not requested
            return False, None
        return True, own_targets
    else:
        # If no targets are provided, it means can match on any
        # Since no specific targets were requested
        return True, None
