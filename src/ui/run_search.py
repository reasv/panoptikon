from __future__ import annotations

import urllib.parse
from time import time
from typing import List, Tuple

import gradio as gr
import numpy as np

from src.data_extractors.ai.clip import CLIPEmbedder
from src.data_extractors.ai.text_embed import TextEmbedder
from src.db import get_database_connection
from src.db.search import search_files
from src.db.search.types import (
    AnyTextFilter,
    BookmarksFilter,
    ExtractedTextFilter,
    FileFilters,
    ImageEmbeddingFilter,
    OrderByType,
    OrderParams,
    OrderType,
    PathTextFilter,
    QueryFilters,
    QueryParams,
    QueryTagFilters,
    SearchQuery,
)
from src.db.search.utils import from_dict, pprint_dataclass
from src.db.utils import serialize_f32
from src.types import FileSearchResult

last_embedded_text: str | None = None
last_embedded_text_embed: bytes | None = None


def get_embed(text: str):
    global last_embedded_text, last_embedded_text_embed
    if text == last_embedded_text:
        return last_embedded_text_embed
    # Set as persistent so that the model is not reloaded every time the function is called
    embedder = TextEmbedder(persistent=True)
    text_embed = embedder.get_text_embeddings([text])[0]
    last_embedded_text = text
    last_embedded_text_embed = serialize_f32(text_embed)
    return last_embedded_text_embed


def get_clip_embed(input: str | np.ndarray, model_name: str):

    from src.data_extractors.models import ImageEmbeddingModel

    model_opt = ImageEmbeddingModel(1, model_name)
    name = model_opt.clip_model_name()
    pretrained = model_opt.clip_model_checkpoint()
    # Set as persistent so that the model is not reloaded every time the function is called
    clip_model = CLIPEmbedder(name, pretrained, persistent=True)
    clip_model.load_model()
    if isinstance(input, str):
        embed = clip_model.get_text_embeddings([input])[0]
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())
    else:  # input is an image
        embed = clip_model.get_image_embeddings([input])[0]
        assert isinstance(embed, np.ndarray)
        return serialize_f32(embed.tolist())


def search(
    query_state_dict: dict,
    page: int = 1,
    search_action: str | None = None,
):
    query_state = from_dict(SearchQuery, query_state_dict)
    print("Query state: ")
    print(query_state)
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

    # Full text search on filename or path, or extracted text
    # if any_text_query:
    #     if order_by_any_text_rank:
    #         order_by = "rank_any_text"
    # if search_in_bookmarks:
    #     if order_by_time_added_bk:
    #         order_by = "time_added"
    # if path_search:
    #     if path_order_by_rank:
    #         order_by = "rank_path_fts"
    # if extracted_text_search:
    #     if extracted_text_order_by_rank:
    #         order_by = "rank_fts"
    # if vec_text_search:
    #     order_by = "text_vec_distance"
    # if clip_text_query:
    #     order_by = "image_vec_distance"
    # if clip_image_query is not None:
    #     order_by = "image_vec_distance"

    # if vec_text_search:
    #     vec_text_search_embed = get_embed(vec_text_search)
    # else:
    #     vec_text_search_embed = None

    # image_vec_search = None
    # if clip_text_query and clip_model:
    #     image_vec_search = get_clip_embed(clip_text_query, clip_model)
    # if clip_image_query is not None and clip_model:
    #     image_vec_search = get_clip_embed(clip_image_query, clip_model)
    # extracted_text_embeddings=(
    #         ExtractedTextFilter[bytes](
    #             query=vec_text_search_embed,
    #             targets=vec_targets or [],
    #             min_confidence=None,
    #         )
    #         if vec_text_search_embed
    #         else None

    # )
    # image_embeddings=(
    #         ImageEmbeddingFilter(
    #             query=image_vec_search, target=("clip", clip_model)
    #         )
    #         if image_vec_search and clip_model
    #         else None
    #     )
    start = time()
    query_state.order_args.page = page
    search_query = SearchQuery(
        query=query_state.query,
        order_args=query_state.order_args,
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
    total_pages = total_results // search_query.order_args.page_size + (
        1 if total_results % search_query.order_args.page_size > 0 else 0
    )
    return (
        results,
        total_results,
        gr.update(value=page, maximum=int(total_pages)),
        gr.update(),
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
