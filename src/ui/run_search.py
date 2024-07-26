from __future__ import annotations

import urllib.parse
from time import time
from typing import List, Tuple

import gradio as gr

from src.data_extractors.utils import get_threshold_from_env
from src.db import get_database_connection
from src.db.search import search_files
from src.db.search.types import (
    AnyTextFilter,
    BookmarksFilter,
    ExtractedTextFilter,
    FileFilters,
    OrderByType,
    OrderParams,
    OrderType,
    PathTextFilter,
    QueryFilters,
    QueryParams,
    QueryTagFilters,
    SearchQuery,
)
from src.types import FileSearchResult


def search(
    tags_str: str,
    min_tag_confidence: float | None,
    results_per_page: int,
    include_paths: List[str] | None = None,
    page: int = 1,
    order_by: OrderByType = "last_modified",
    order: OrderType | None = None,
    tag_setters: List[str] | None = None,
    all_setters_required: bool = False,
    item_types: List[str] | None = None,
    namespace_prefixes: List[str] | None = None,
    path_search: str | None = None,
    search_path_in: str = "full_path",
    path_order_by_rank: bool = True,
    extracted_text_search: str | None = None,
    require_text_extractors: List[Tuple[str, str]] | None = None,
    extracted_text_order_by_rank: bool = True,
    search_in_bookmarks: bool = False,
    bookmark_namespaces: List[str] | None = None,
    order_by_time_added_bk: bool = False,
    any_text_query: str | None = None,
    restrict_to_query_types: List[Tuple[str, str]] | None = None,
    order_by_any_text_rank: bool = False,
    search_action: str | None = None,
):
    print(f"Search action: {search_action}")
    if search_action == "search_button":
        page = 1
    elif search_action == "next_page":
        page += 1
    elif search_action == "previous_page":
        page -= 1
        page = max(1, page)
    elif search_action == "goto_page":
        pass

    if page < 1:
        page = 1
    if order not in ["asc", "desc", None]:
        order = None

    minimum_confidence_threshold = get_threshold_from_env()
    if (
        not min_tag_confidence
        or min_tag_confidence <= minimum_confidence_threshold
    ):
        min_tag_confidence = None

    include_paths = include_paths or []
    include_paths = [path.strip() for path in include_paths]

    tags = [tag.strip() for tag in tags_str.split(",") if tag.strip() != ""]

    def extract_tags_subtype(tag_list: list[str], prefix: str = "-"):
        remaining = []
        subtype = []
        for tag in tag_list:
            if tag.startswith(prefix):
                subtype.append(tag[1:])
            else:
                remaining.append(tag)
        return remaining, subtype

    tags, negative_tags = extract_tags_subtype(tags, "-")
    tags, negative_tags_match_all = extract_tags_subtype(tags, "~")
    tags, tags_match_any = extract_tags_subtype(tags, "*")
    conn = get_database_connection(write_lock=False)
    print(
        f"Searching for tags: {tags} match any: {tags_match_any} "
        + f"(negative tags: {negative_tags} match all negative tags: {negative_tags_match_all}) "
        + f"with min confidence {min_tag_confidence} under path prefix {include_paths} "
        + f"with page size {results_per_page} and page {page} and order by {order_by} {order} "
        + f"and tag setters {tag_setters} and all setters required {all_setters_required} and "
        + f"item type prefix {item_types} and namespace prefix {namespace_prefixes} "
        + f"and path search {path_search} in {search_path_in} "
        + f"and extracted text search {extracted_text_search} "
        + f"and require text extractors {require_text_extractors} "
        + f"and path order by rank {path_order_by_rank} "
        + f"and extracted text order by rank {extracted_text_order_by_rank} "
        + f"and search in bookmarks {search_in_bookmarks} and bookmark namespaces {bookmark_namespaces}"
    )
    # Full text search on filename or path, or extracted text
    if any_text_query:
        if order_by_any_text_rank:
            order_by = "rank_any_text"
    if search_in_bookmarks:
        if order_by_time_added_bk:
            order_by = "time_added"
    if path_search:
        if path_order_by_rank:
            order_by = "rank_path_fts"
    if extracted_text_search:
        if extracted_text_order_by_rank:
            order_by = "rank_fts"

    start = time()
    order_args = OrderParams(
        order_by=order_by,
        order=order,
        page=page,
        page_size=results_per_page,
    )

    tags_args = QueryTagFilters(
        pos_match_all=tags,
        pos_match_any=tags_match_any,
        neg_match_any=negative_tags,
        neg_match_all=negative_tags_match_all,
    )

    filters = QueryFilters(
        files=FileFilters(
            item_types=item_types or [],
            include_path_prefixes=include_paths,
        ),
        path=(
            PathTextFilter(
                query=path_search,
                only_match_filename=(
                    False if search_path_in == "full_path" else True
                ),
            )
            if path_search
            else None
        ),
        extracted_text=(
            ExtractedTextFilter[str](
                query=extracted_text_search,
                targets=require_text_extractors or [],
                languages=[],
                language_min_confidence=None,
                min_confidence=None,
            )
            if extracted_text_search
            else None
        ),
        bookmarks=(
            BookmarksFilter(
                restrict_to_bookmarks=search_in_bookmarks,
                namespaces=bookmark_namespaces or [],
            )
            if search_in_bookmarks
            else None
        ),
        any_text=(
            AnyTextFilter(
                query=any_text_query,
                targets=restrict_to_query_types or [],
            )
            if any_text_query
            else None
        ),
        extracted_text_embeddings=None,
    )

    query_args = QueryParams(
        tags=tags_args,
        filters=filters,
    )
    search_query = SearchQuery(
        query=query_args,
        order_args=order_args,
        count=True,
        check_path=True,
    )

    res_list: List[Tuple[FileSearchResult | None, int]] = list(
        search_files(
            conn,
            search_query,
        )
    ) or [(None, 0)]

    results, total_results = zip(*res_list) if res_list else ([], [0])

    print(f"Search took {round(time() - start, 3)} seconds")
    total_results = total_results[0]
    conn.close()
    print(f"Found {total_results} images")
    # Calculate the total number of pages, we need to round up
    total_pages = total_results // results_per_page + (
        1 if total_results % results_per_page > 0 else 0
    )
    query = build_query(
        tags,
        min_tag_confidence,
        include_paths[0] if include_paths else None,
        results_per_page,
        page,
        order_by,
        order,
    )
    return (
        results,
        total_results,
        gr.update(value=page, maximum=int(total_pages)),
        f"[View Results in Gallery]({query})",
    )


def build_query(
    tags: list,
    min_tag_confidence: float | None,
    include_path: str | None = None,
    page_size: int = 10,
    page: int = 1,
    order_by: OrderByType = "last_modified",
    order: OrderType = None,
):
    if not include_path:
        include_path = ""

    if include_path.strip() != "":
        # URL encode the path
        include_path = urllib.parse.quote(include_path)
    order_query = ""
    if order is not None:
        order_query = f"&order={order}"
    tag_str = urllib.parse.quote(",".join(tags))
    if not min_tag_confidence:
        min_tag_confidence = 0.0
    if not include_path:
        include_path = ""
    return (
        f"/search/tags?tags={tag_str}&min_confidence={min_tag_confidence}"
        + f"&include_path={include_path}&page_size={page_size}"
        + f"&page={page}&order_by={order_by}{order_query}"
    )
