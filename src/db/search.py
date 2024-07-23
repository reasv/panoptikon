import os
import sqlite3
from typing import List, Tuple

from src.db.utils import pretty_print_SQL
from src.types import FileSearchResult, OrderByType, OrderType


def search_files(
    conn: sqlite3.Connection,
    tags: List[str],
    tags_match_any: List[str] | None = None,
    negative_tags: List[str] | None = None,
    negative_tags_match_all: List[str] | None = None,
    tag_namespaces: List[str] | None = None,
    min_confidence: float | None = 0.5,
    setters: List[str] | None = None,
    all_setters_required: bool | None = False,
    item_types: List[str] | None = None,
    include_path_prefixes: List[str] | None = None,
    match_path: str | None = None,
    match_filename: str | None = None,
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
    restrict_to_bookmarks: bool = False,
    restrict_to_bookmark_namespaces: List[str] | None = None,
    any_text_query: str | None = None,
    any_text_query_targets: List[Tuple[str, str]] | None = None,
    order_by: OrderByType = "last_modified",
    order: OrderType = None,
    page_size: int | None = 1000,
    page: int = 1,
    check_path_exists: bool = False,
    return_total_count: bool = True,
):
    # Normalize/clean the inputs
    def clean_tag_list(tag_list: List[str] | None) -> List[str]:
        if not tag_list:
            return []
        return [tag.lower().strip() for tag in tag_list if tag.strip() != ""]

    tags_match_any = clean_tag_list(tags_match_any)
    negative_tags_match_all = clean_tag_list(negative_tags_match_all)
    tags = clean_tag_list(tags)
    negative_tags = clean_tag_list(negative_tags)
    all_setters_required = all_setters_required or False
    if len(tags_match_any) == 1:
        # If only one tag is provided for "match any", we can just use it as a regular tag
        tags.append(tags_match_any[0])
        tags_match_any = None
    if len(negative_tags_match_all) == 1:
        # If only one tag is provided for negative "match all", we can just use it as a regular negative tag
        negative_tags.append(negative_tags_match_all[0])
        negative_tags_match_all = None

    tag_namespaces = tag_namespaces or []
    item_types = item_types or []
    include_path_prefixes = include_path_prefixes or []
    min_confidence = min_confidence or None
    setters = setters or []
    restrict_to_bookmark_namespaces = restrict_to_bookmark_namespaces or []
    if not restrict_to_bookmarks:
        restrict_to_bookmark_namespaces = []

    any_text_query_targets = any_text_query_targets or []
    if not any_text_query:
        any_text_query_targets = []

    # Build the main query
    search_query, search_query_params = build_search_query(
        tags=tags,
        tags_match_any=tags_match_any,
        negative_tags=negative_tags,
        negative_tags_match_all=negative_tags_match_all,
        tag_namespaces=tag_namespaces,
        min_confidence=min_confidence,
        setters=setters,
        all_setters_required=all_setters_required,
        item_types=item_types,
        include_path_prefixes=include_path_prefixes,
        match_path=match_path,
        match_filename=match_filename,
        match_extracted_text=match_extracted_text,
        require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
        restrict_to_bookmarks=restrict_to_bookmarks,
        restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
        any_text_query=any_text_query,
        any_text_query_targets=any_text_query_targets,
    )
    # Debugging
    # print_search_query(count_query, params)
    cursor = conn.cursor()
    if return_total_count:
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
        match_path=bool(match_path),
        match_filename=bool(match_filename),
        match_extracted_text=bool(match_extracted_text),
        restrict_to_bookmarks=restrict_to_bookmarks,
        any_text_query=bool(any_text_query),
        order_by=order_by,
        order=order,
        page=page,
        page_size=page_size,
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
        if check_path_exists and not os.path.exists(file.path):
            continue
        yield file, total_count
    if results_count == 0:
        return []


def build_search_query(
    tags: List[str],
    tags_match_any: List[str] | None,
    negative_tags: List[str],
    negative_tags_match_all: List[str] | None,
    tag_namespaces: List[str],
    min_confidence: float | None,
    setters: List[str],
    all_setters_required: bool,
    item_types: List[str],
    include_path_prefixes: List[str],
    match_path: str | None,
    match_filename: str | None,
    match_extracted_text: str | None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ),  # Pairs of (type, setter) to include
    restrict_to_bookmarks: bool,
    restrict_to_bookmark_namespaces: List[str],
    any_text_query: str | None,
    any_text_query_targets: List[Tuple[str, str]],
):
    if tags_match_any and not tags:
        # If "match any" tags are provided, but no positive tags are provided
        # We need to build a query to match on *any* of them being present
        main_query, params = build_main_query(
            tags=tags_match_any,
            negative_tags=negative_tags,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=True,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
            # Generalized text query
            any_text_query=any_text_query,
            any_text_query_targets=any_text_query_targets,
        )
    else:
        # Basic case where we need to match all positive tags and none of the negative tags
        main_query, params = build_main_query(
            tags=tags,
            negative_tags=negative_tags,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=False,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
            # Generalized text query
            any_text_query=any_text_query,
            any_text_query_targets=any_text_query_targets,
        )

    if tags_match_any and tags:
        # If tags "match any" are provided along with match all regular positive tags
        # We need to build a separate query to match on *any* of them being present
        # And then intersect the results with the main query
        tags_query, tags_params = build_main_query(
            tags=tags_match_any,
            negative_tags=None,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=True,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
            # Generalized text query
            any_text_query=any_text_query,
            any_text_query_targets=any_text_query_targets,
        )

        # Append the tags query to the main query
        main_query = f"""
        {main_query}
        INTERSECT
        {tags_query}
        """
        params += tags_params

    if negative_tags_match_all:
        # If negative tags "match all" are provided
        # We need to build a separate query to match on *all* of them being present
        # And then exclude the results from the main query
        negative_tags_query, negative_tags_params = build_main_query(
            tags=negative_tags_match_all,
            negative_tags=None,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=False,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
            # Generalized text query
            any_text_query=any_text_query,
            any_text_query_targets=any_text_query_targets,
        )

        # Append the negative tags query to the main query
        if tags_match_any and tags:
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
    match_path: bool,
    match_filename: bool,
    match_extracted_text: bool,
    restrict_to_bookmarks: bool,
    any_text_query: bool,
    order_by: OrderByType,
    order: OrderType,
    page: int,
    page_size: int | None,
) -> Tuple[str, List[str | int | float]]:
    # Determine order_by_clause and default order setting based on order_by value
    default_order_by_clause = "last_modified"
    match order_by:
        case "rank_fts":
            if match_extracted_text:
                order_by_clause = "rank_fts"
            else:
                order_by_clause = default_order_by_clause
        case "rank_path_fts":
            if match_path or match_filename:
                order_by_clause = "rank_path_fts"
            else:
                order_by_clause = default_order_by_clause
        case "time_added":
            if restrict_to_bookmarks:
                order_by_clause = "time_added"
            else:
                order_by_clause = default_order_by_clause
        case "rank_any_text":
            if any_text_query:
                order_by_clause = "rank_any_text"
            else:
                order_by_clause = default_order_by_clause
        case "path":
            order_by_clause = "path"
            # Default order for path is ascending
            if order is None:
                order = "asc"
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
    page_size = page_size or 1000000  # Mostly for debugging purposes
    offset = (page - 1) * page_size

    query_params: List[str | int | float] = [page_size, offset]

    return clause, query_params


def build_main_query(
    tags: List[str],
    negative_tags: List[str] | None = None,
    tag_namespaces: List[str] = [],
    min_confidence: float | None = 0.5,
    setters: List[str] = [],
    all_setters_required: bool = False,
    item_types: List[str] = [],
    include_path_prefixes: List[str] = [],
    any_positive_tags_match: bool = False,
    match_path: str | None = None,
    match_filename: str | None = None,
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
    restrict_to_bookmarks: bool = False,
    restrict_to_bookmark_namespaces: List[str] = [],
    # Generalized text query
    any_text_query: str | None = None,
    any_text_query_targets: List[Tuple[str, str]] = [],
) -> Tuple[str, List[str | int | float]]:
    """
    Build a query to search for files based on the given tags,
    negative tags, and other conditions.
    """
    # The items should have text extracted by the given setters matching the query
    extracted_text_condition, extracted_text_params = (
        build_extracted_text_fts_clause(
            match_extracted_text,
            require_extracted_type_setter_pairs,
        )
    )

    # The item mimetype should start with one of the given strings
    item_type_condition = ""
    if item_types:
        if len(item_types) == 1:
            item_type_condition = "AND items.type LIKE ? || '%'"
        elif len(item_types) > 1:
            item_type_condition = "AND ("
            for i, _ in enumerate(item_types):
                if i == 0:
                    item_type_condition += "items.type LIKE ? || '%'"
                else:
                    item_type_condition += " OR items.type LIKE ? || '%'"
            item_type_condition += ")"

    # The setter should match the given setter
    tag_setters_condition = (
        f" AND tags.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )

    # The namespace needs to *start* with the given namespace
    tag_namespace_condition = ""
    if len(tag_namespaces) == 1:
        tag_namespace_condition = " AND tags.namespace LIKE ? || '%'"
    elif len(tag_namespaces) > 1:
        tag_namespace_condition = " AND ("
        for i, _ in enumerate(tag_namespaces):
            if i == 0:
                tag_namespace_condition += "tags.namespace LIKE ? || '%'"
            else:
                tag_namespace_condition += " OR tags.namespace LIKE ? || '%'"
        tag_namespace_condition += ")"

    # The confidence should be greater than or equal to the given confidence
    min_confidence_condition = (
        f"AND tags_items.confidence >= ?" if min_confidence else ""
    )

    # Negative tags should not be associated with the item
    negative_tags_condition = (
        f"""
        WHERE files.item_id NOT IN (
            SELECT tags_items.item_id
            FROM tags_setters as tags
            JOIN tags_items ON tags.id = tags_items.tag_id
            AND tags.name IN ({','.join(['?']*len(negative_tags))})
            {tag_setters_condition}
            {tag_namespace_condition}
            {min_confidence_condition}
        )
    """
        if negative_tags
        else ""
    )

    path_condition = ""
    if len(include_path_prefixes) > 0:
        path_condition_start = "AND"
        # If no negative or positive tags are provided,
        # this needs to start a WHERE clause
        if not tags and not negative_tags:
            path_condition_start = "WHERE"
        if len(include_path_prefixes) == 1:
            # The path needs to *start* with the given path prefix
            path_condition = f"{path_condition_start} files.path LIKE ? || '%'"
        elif len(include_path_prefixes) > 1:
            path_condition = f"{path_condition_start} ("
            for i, _ in enumerate(include_path_prefixes):
                if i == 0:
                    path_condition += "files.path LIKE ? || '%'"
                else:
                    path_condition += " OR files.path LIKE ? || '%'"
            path_condition += ")"

    having_clause = (
        "HAVING COUNT(DISTINCT tags.name) = ?"
        if not all_setters_required
        else "HAVING COUNT(DISTINCT tags.setter || '-' || tags.name) = ?"
    )

    additional_select_columns = ""
    # If we need to match on the path or filename using FTS
    path_match_condition = ""
    if match_path or match_filename:
        additional_select_columns = ",\n path_fts.rank as rank_path_fts"
        path_match_condition = f"""
        JOIN files_path_fts AS path_fts
        ON files.id = path_fts.rowid
        """
        if match_path:
            path_match_condition += f"""
            AND path_fts.path MATCH ?
            """
        if match_filename:
            path_match_condition += f"""
            AND path_fts.filename MATCH ?
            """
    if match_extracted_text:
        additional_select_columns += (
            ",\n extracted_text_matches.max_rank AS rank_fts"
        )

    # If this is set, we only search for files that are bookmarked
    bookmarks_condition = ""
    if restrict_to_bookmarks:
        additional_select_columns += ",\n bookmarks.time_added AS time_added"
        bookmarks_condition = (
            "JOIN bookmarks ON files.sha256 = bookmarks.sha256"
        )
        if restrict_to_bookmark_namespaces:
            bookmarks_condition += " AND bookmarks.namespace IN ("
            for i, _ in enumerate(restrict_to_bookmark_namespaces):
                if i == 0:
                    bookmarks_condition += "?"
                else:
                    bookmarks_condition += ", ?"
            bookmarks_condition += ")"

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
        AND tags.name IN ({','.join(['?']*len(tags))})
        {min_confidence_condition}
        {tag_setters_condition}
        {tag_namespace_condition}
        JOIN files ON tags_items.item_id = files.item_id
        {path_condition}
        JOIN items ON files.item_id = items.id
        {item_type_condition}
        {path_match_condition}
        {extracted_text_condition}
        {bookmarks_condition}
        {negative_tags_condition}
        GROUP BY files.path
        {having_clause if not any_positive_tags_match else ""}
    """
        if tags
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
        {bookmarks_condition}
        {negative_tags_condition}
        {path_condition}
    """
    )
    params: List[str | int | float] = [
        param
        for param in [
            *(
                (
                    *tags,
                    min_confidence,
                    *setters,
                    *tag_namespaces,
                )
                if tags
                else ()
            ),
            *(include_path_prefixes if tags else []),
            *item_types,
            match_path,
            match_filename,
            *extracted_text_params,
            *restrict_to_bookmark_namespaces,
            *(
                (*negative_tags, *setters, *tag_namespaces, min_confidence)
                if negative_tags
                else ()
            ),
            *(include_path_prefixes if not tags else []),
            (
                # Number of tags to match, or number of tag-setter pairs to match if we require all setters to be present for all tags
                (
                    len(tags)
                    if not all_setters_required
                    else len(tags) * len(setters)
                )
                # HAVING clause is not needed if no positive tags are provided
                if tags and not any_positive_tags_match
                else None
            ),
        ]
        if param is not None
    ]

    return main_query, params


def build_extracted_text_fts_clause(
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
):
    """
    Build a subquery to match extracted text based on the given conditions.
    """

    # Define subquery for matching extracted text
    extracted_text_condition = ""
    extracted_text_params = []
    if match_extracted_text:
        extracted_text_conditions = ["et_fts.text MATCH ?"]
        extracted_text_params.append(match_extracted_text)

        if require_extracted_type_setter_pairs:
            include_pairs_conditions = " OR ".join(
                ["(log.type = ? AND log.setter = ?)"]
                * len(require_extracted_type_setter_pairs)
            )
            extracted_text_conditions.append(f"({include_pairs_conditions})")
            for type, setter in require_extracted_type_setter_pairs:
                extracted_text_params.extend([type, setter])

        extracted_text_condition = f"""
        JOIN (
            SELECT et.item_id, MAX(et_fts.rank) AS max_rank
            FROM extracted_text_fts AS et_fts
            JOIN extracted_text AS et ON et_fts.rowid = et.id
            JOIN data_extraction_log AS log ON et.log_id = log.id
            WHERE {" AND ".join(extracted_text_conditions)}
            GROUP BY et.item_id
        ) AS extracted_text_matches
        ON files.item_id = extracted_text_matches.item_id
        """
    return extracted_text_condition, extracted_text_params
