from __future__ import annotations

import urllib.parse
from time import time
from typing import List, Tuple

import gradio as gr

import src.data_extractors.models as models
from src.data_extractors.utils import get_threshold_from_env
from src.db import get_database_connection
from src.db.extraction_log import get_existing_type_setter_pairs
from src.db.folders import get_folders_from_database
from src.db.search import search_files
from src.types import OrderByType, OrderType
from src.ui.components.multi_view import create_multiview


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
    search_action: str | None = None,
):
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
        + f"and extracted text order by rank {extracted_text_order_by_rank}"
    )
    # Full text search on filename or path, or extracted text
    match_path = None
    match_filename = None
    match_extracted_text = None
    if path_search:
        if search_path_in == "full_path":
            match_path = path_search
        else:
            match_filename = path_search
        if path_order_by_rank:
            order_by = "rank_path_fts"
    if extracted_text_search:
        match_extracted_text = extracted_text_search
        if extracted_text_order_by_rank:
            order_by = "rank_fts"

    start = time()
    res_list = list(
        search_files(
            conn,
            tags,
            negative_tags=negative_tags,
            negative_tags_match_all=negative_tags_match_all,
            tags_match_any=tags_match_any,
            tag_namespaces=namespace_prefixes,
            min_confidence=min_tag_confidence,
            setters=tag_setters,
            all_setters_required=all_setters_required,
            item_types=item_types,
            include_path_prefixes=include_paths,
            match_path=match_path,
            match_filename=match_filename,
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_text_extractors,
            order_by=order_by,
            order=order,
            page=page,
            page_size=results_per_page,
            check_path_exists=True,
        )
    )
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


def get_folder_list():
    conn = get_database_connection(write_lock=False)
    folders = get_folders_from_database(conn)
    conn.close()
    return folders


def get_setters_list():
    conn = get_database_connection(write_lock=False)
    setters = get_existing_type_setter_pairs(conn)
    conn.close()
    return setters


def on_tab_load():
    full_setters_list = get_setters_list()
    setters = [
        (f"{model_type}|{setter_id}", (model_type, setter_id))
        for model_type, setter_id in full_setters_list
        if model_type != "tags"
    ]
    existing_tag_setters = [
        setter_id
        for model_type, setter_id in full_setters_list
        if model_type == "tags"
    ]
    return (
        gr.update(choices=get_folder_list()),
        gr.update(choices=setters),
        gr.update(choices=existing_tag_setters),
    )


def on_tag_select(selectData: gr.SelectData):
    return selectData.value


def create_search_UI(
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Tag Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                link = gr.Markdown("[View Results in Gallery](/search/tags)")
                number_of_results = gr.Number(
                    value=0,
                    show_label=True,
                    label="Results",
                    interactive=False,
                    scale=0,
                )
                submit_button = gr.Button("Search", scale=0)
                with gr.Column(scale=10):
                    with gr.Tabs():
                        with gr.Tab(label="Options"):
                            with gr.Group():
                                with gr.Row():
                                    tag_input = gr.Textbox(
                                        label="Enter tags separated by commas",
                                        value="",
                                        show_copy_button=True,
                                        scale=3,
                                    )
                                    min_confidence = gr.Slider(
                                        minimum=0.05,
                                        maximum=1,
                                        value=get_threshold_from_env(),
                                        step=0.05,
                                        label="Min. Confidence Level for Tags",
                                        scale=2,
                                    )
                                    max_results_per_page = gr.Slider(
                                        minimum=0,
                                        maximum=500,
                                        value=10,
                                        step=1,
                                        label="Results per page (0 for max)",
                                        scale=2,
                                    )
                                    selected_folder = gr.Dropdown(
                                        label="Restrict search to paths starting with",
                                        choices=[
                                            (folder, folder)
                                            for folder in get_folder_list()
                                        ],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        scale=2,
                                    )
                                    order_by = gr.Radio(
                                        choices=["path", "last_modified"],
                                        label="Order by",
                                        value="last_modified",
                                        scale=2,
                                    )
                        with gr.Tab(label="Advanced Options"):
                            with gr.Group():
                                with gr.Row():
                                    order = gr.Radio(
                                        choices=["asc", "desc", "default"],
                                        label="Order",
                                        value="default",
                                        scale=2,
                                    )
                                    tag_setters = gr.Dropdown(
                                        label="Only search tags set by model(s)",
                                        multiselect=True,
                                        choices=[
                                            (n, n)
                                            for n in models.TagsModel.available_models()
                                        ],
                                        value=[],
                                        scale=2,
                                    )
                                    all_setters_required = gr.Checkbox(
                                        label="Require ALL selected models to have set each tag",
                                        scale=1,
                                    )
                                    item_type = gr.Dropdown(
                                        label="Item MimeType Prefix",
                                        choices=[
                                            "image/",
                                            "video/",
                                            "image/png",
                                            "image/jpeg",
                                            "video/mp4",
                                            "video/webm",
                                        ],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        value=None,
                                        scale=2,
                                    )
                                    namespace_prefix = gr.Dropdown(
                                        label="Tag Namespace Prefix",
                                        choices=[
                                            "danbooru:",
                                            "danbooru:character",
                                            "danbooru:general",
                                        ],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        value=None,
                                        scale=2,
                                    )
                        with gr.Tab(label="Filename & Path Search"):
                            with gr.Row():
                                path_search = gr.Textbox(
                                    label="SQL MATCH query on filename or path",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                search_path_in = gr.Radio(
                                    choices=[
                                        ("Full Path", "full_path"),
                                        ("Filename", "filename"),
                                    ],
                                    interactive=True,
                                    label="Search in",
                                    value="full_path",
                                    scale=1,
                                )
                                path_order_by_rank = gr.Checkbox(
                                    label="Order results by relevance if this query is present",
                                    interactive=True,
                                    value=True,
                                    scale=1,
                                )
                        with gr.Tab(label="Extracted Text Search"):
                            with gr.Row():
                                extracted_text_search = gr.Textbox(
                                    label="SQL MATCH query on text exctracted by OCR/Whisper",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                require_text_extractors = gr.Dropdown(
                                    choices=[],
                                    interactive=True,
                                    label="Only Search In Text From These Sources",
                                    multiselect=True,
                                    scale=1,
                                )
                                extracted_text_order_by_rank = gr.Checkbox(
                                    label="Order results by relevance if this query is present",
                                    interactive=True,
                                    value=True,
                                    scale=1,
                                )
        multi_view = create_multiview(
            select_history=select_history,
            bookmarks_namespace=bookmarks_namespace,
        )

        with gr.Row(elem_classes="pagination-controls"):
            previous_page = gr.Button("Previous Page", scale=1)
            current_page = gr.Slider(
                value=1,
                label="Current Page",
                maximum=1,
                minimum=1,
                step=1,
                scale=2,
            )
            next_page = gr.Button("Next Page", scale=1)

    search_tab.select(
        fn=on_tab_load,
        outputs=[
            selected_folder,
            require_text_extractors,
            tag_setters,
        ],
    )

    search_inputs = [
        tag_input,
        min_confidence,
        max_results_per_page,
        selected_folder,
        current_page,
        order_by,
        order,
        tag_setters,
        all_setters_required,
        item_type,
        namespace_prefix,
        path_search,
        search_path_in,
        path_order_by_rank,
        extracted_text_search,
        require_text_extractors,
        extracted_text_order_by_rank,
    ]

    search_outputs = [multi_view.files, number_of_results, current_page, link]

    action_search_button = gr.State("search_button")
    action_next_page = gr.State("next_page")
    action_previous_page = gr.State("previous_page")
    action_goto_page = gr.State("goto_page")

    submit_button.click(
        fn=search,
        inputs=[*search_inputs, action_search_button],
        outputs=search_outputs,
    )

    current_page.release(
        fn=search,
        inputs=[*search_inputs, action_goto_page],
        outputs=search_outputs,
    )

    previous_page.click(
        fn=search,
        inputs=[*search_inputs, action_previous_page],
        outputs=search_outputs,
    )

    next_page.click(
        fn=search,
        inputs=[*search_inputs, action_next_page],
        outputs=search_outputs,
    )

    multi_view.list_view.tag_list.select(fn=on_tag_select, outputs=[tag_input])
