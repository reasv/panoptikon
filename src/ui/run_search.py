from __future__ import annotations

import urllib.parse
from cgitb import text
from time import time
from typing import List, Tuple

import gradio as gr
import numpy as np

from src.data_extractors.text_embeddings import get_text_embedding_model
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
from src.db.search.utils import pprint_dataclass
from src.db.utils import serialize_f32
from src.types import FileSearchResult

text_embedding_model = None


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
    vec_text_search: str | None = None,
    vec_targets: List[Tuple[str, str]] | None = None,
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
    if vec_text_search:
        order_by = "text_vec_distance"

    minimum_confidence_threshold = get_threshold_from_env()
    if (
        not min_tag_confidence
        or min_tag_confidence <= minimum_confidence_threshold
    ):
        min_tag_confidence = None

    include_paths = include_paths or []
    include_paths = [path.strip() for path in include_paths]

    start = time()
    order_args = OrderParams(
        order_by=order_by,
        order=order,
        page=page,
        page_size=results_per_page,
    )

    tags_match_all, tags_match_any, negative_tags, negative_tags_match_all = (
        parse_tags(tags_str)
    )

    tags_args = QueryTagFilters(
        pos_match_all=tags_match_all,
        pos_match_any=tags_match_any,
        neg_match_any=negative_tags,
        neg_match_all=negative_tags_match_all,
        all_setters_required=all_setters_required,
        setters=tag_setters or [],
        namespaces=namespace_prefixes or [],
        min_confidence=min_tag_confidence,
    )
    if vec_text_search:
        global text_embedding_model
        if not text_embedding_model:
            text_embedding_model = get_text_embedding_model()
        text_embed = text_embedding_model.encode([vec_text_search])
        assert isinstance(text_embed, np.ndarray)
        text_embed_list = text_embed.tolist()[0]
        vec_text_search_embed = serialize_f32(text_embed_list)
    else:
        vec_text_search_embed = None

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
        extracted_text_embeddings=(
            ExtractedTextFilter[bytes](
                query=vec_text_search_embed,
                targets=vec_targets or [],
                min_confidence=None,
            )
            if vec_text_search_embed
            else None
        ),
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
    conn = get_database_connection(write_lock=False)
    print("Search query:")
    pprint_dataclass(search_query)
    res_list: List[Tuple[FileSearchResult | None, int]] = list(
        search_files(
            conn,
            search_query,
        )
    ) or [(None, 0)]
    conn.close()
    results, total_results = zip(*res_list) if res_list else ([], [0])

    print(f"Search took {round(time() - start, 3)} seconds")
    total_results = total_results[0]

    print(f"Found {total_results} images")
    # Calculate the total number of pages, we need to round up
    total_pages = total_results // results_per_page + (
        1 if total_results % results_per_page > 0 else 0
    )
    query = build_query(
        tags_match_all,
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


def parse_tags(tags_str: str):
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
    return tags, tags_match_any, negative_tags, negative_tags_match_all


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
