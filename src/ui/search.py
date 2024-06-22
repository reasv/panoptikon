from __future__ import annotations

import gradio as gr
import urllib.parse
from time import time

from src.db import get_database_connection, get_folders_from_database, search_files
from src.ui.components.multi_view import create_multiview
from src.wd_tagger import V3_MODELS
from src.tags import get_threshold_from_env

def build_query(tags: list, min_tag_confidence: float, include_path: str = None, page_size: int = 10, page: int = 1, order_by: str = "last_modified", order = None):
    if not include_path: include_path = ""

    if include_path.strip() != "":
        # URL encode the path
        include_path = urllib.parse.quote(include_path)
    order_query = ""
    if order is not None:
        order_query = f"&order={order}"
    tag_str = urllib.parse.quote(','.join(tags))
    return f"/search/tags?tags={tag_str}&min_confidence={min_tag_confidence}&include_path={include_path}&page_size={page_size}&page={page}&order_by={order_by}{order_query}"

def search_by_tags(
        tags_str: str,
        min_tag_confidence: float | None,
        results_per_page: int,
        include_path: str = None,
        page: int = 1,
        order_by: str = "last_modified",
        order = None,
        tag_setters = None,
        all_setters_required = False,
        item_type = None,
        namespace_prefix = None
        ):
    if page < 1: page = 1
    if order not in ["asc", "desc", None]: order = None

    minimum_confidence_threshold = get_threshold_from_env()
    if min_tag_confidence <= minimum_confidence_threshold:
        min_tag_confidence = None

    include_path = include_path.strip() if include_path is not None else None
    if include_path == "": include_path = None
    tags = [tag.strip() for tag in tags_str.split(',') if tag.strip() != ""]

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
    conn = get_database_connection()
    print(f"Searching for tags: {tags} (negative tags: {negative_tags}) with min confidence {min_tag_confidence} under path prefix {include_path} with page size {results_per_page} and page {page} and order by {order_by} {order} and tag setters {tag_setters} and all setters required {all_setters_required} and item type prefix {item_type} and namespace prefix {namespace_prefix}")
    start = time()
    res_list = list(search_files(
        conn,
        tags,
        negative_tags=negative_tags,
        negative_tags_match_all=negative_tags_match_all,
        tags_match_any=tags_match_any,
        tag_namespace=namespace_prefix,
        min_confidence=min_tag_confidence,
        setters=tag_setters,
        all_setters_required = all_setters_required,
        item_type = item_type,
        include_path_prefix = include_path,
        order_by=order_by,
        order=order,
        page=page,
        page_size=results_per_page,
        check_path_exists = True
    ))
    results, total_results = zip(*res_list) if res_list else ([], [0])

    print(f"Search took {round(time() - start, 3)} seconds")
    total_results = total_results[0]
    conn.close()
    print(f"Found {total_results} images")
    # Calculate the total number of pages, we need to round up
    total_pages = total_results // results_per_page + (1 if total_results % results_per_page > 0 else 0)
    return results, total_results, gr.update(value=page, maximum=int(total_pages)), f"[View Results in Gallery]({build_query(tags, min_tag_confidence, include_path, results_per_page, page, order_by, order)})"

def search_by_tags_search_button(
        tags_str: str,
        min_tag_confidence: float,
        results_per_page: int,
        include_path: str = None,
        page: int = 1,
        order_by: str = "last_modified",
        order = None,
        tag_setters = None,
        all_setters_required = False,
        item_type = None,
        namespace_prefix = None
        ):
    return search_by_tags(
        tags_str,
        min_tag_confidence,
        results_per_page,
        include_path,
        1,
        order_by,
        order,
        tag_setters,
        all_setters_required,
        item_type,
        namespace_prefix
        )

def search_by_tags_next_page(
        tags_str: str,
        min_tag_confidence: float,
        results_per_page: int,
        include_path: str = None,
        page: int = 1,
        order_by: str = "last_modified",
        order = None,
        tag_setters = None,
        all_setters_required = False,
        item_type = None,
        namespace_prefix = None
        ):
    return search_by_tags(
        tags_str,
        min_tag_confidence,
        results_per_page,
        include_path,
        page+1,
        order_by,
        order,
        tag_setters,
        all_setters_required,
        item_type,
        namespace_prefix
        )

def search_by_tags_previous_page(
        tags_str: str,
        min_tag_confidence: float,
        results_per_page: int,
        include_path: str = None,
        page: int = 1,
        order_by: str = "last_modified",
        order = None,
        tag_setters = None,
        all_setters_required = False,
        item_type = None,
        namespace_prefix = None
    ):
    return search_by_tags(
        tags_str,
        min_tag_confidence,
        results_per_page,
        include_path,
        page-1,
        order_by,
        order,
        tag_setters,
        all_setters_required,
        item_type,
        namespace_prefix
    )

def get_folder_list():
    conn = get_database_connection()
    folders = get_folders_from_database(conn)
    conn.close()
    return folders

def on_tab_load():
    return gr.update(choices=get_folder_list())

def on_tag_select(selectData: gr.SelectData):
    return selectData.value

def create_search_UI(select_history: gr.State = None, bookmarks_namespace: gr.State = None):
    with gr.TabItem(label="Tag Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                link = gr.Markdown('[View Results in Gallery](/search/tags)')
                number_of_results = gr.Number(value=0, show_label=True, label="Results", interactive=False, scale=0)
                submit_button = gr.Button("Search", scale=0)
                with gr.Column(scale=10):
                    with gr.Tabs():
                        with gr.Tab(label="Options"):
                            with gr.Group():
                                with gr.Row():
                                    tag_input = gr.Textbox(label="Enter tags separated by commas", value='', show_copy_button=True, scale=3)
                                    min_confidence = gr.Slider(minimum=0.05, maximum=1, value=get_threshold_from_env(), step=0.05, label="Min. Confidence Level for Tags", scale=2)
                                    max_results_per_page = gr.Slider(minimum=0, maximum=500, value=10, step=1, label="Results per page (0 for max)", scale=2)
                                    selected_folder = gr.Dropdown(label="Limit search to items under path", choices=get_folder_list(), allow_custom_value=True, scale=2)
                                    order_by = gr.Radio(choices=["path", "last_modified"], label="Order by", value="last_modified", scale=2)
                        with gr.Tab(label="Advanced Options"):
                            with gr.Group():
                                with gr.Row():
                                    order = gr.Radio(choices=["asc", "desc", "default"], label="Order", value="default", scale=2)
                                    tag_setters = gr.Dropdown(label="Only search tags set by model(s)", multiselect=True, choices=V3_MODELS, value=[], scale=2)
                                    all_setters_required = gr.Checkbox(label="Require ALL selected models to have set each tag", scale=1)
                                    item_type = gr.Dropdown(label="Item MimeType Prefix", choices=["image/", "video/", "image/png", "image/jpeg", "video/mp4"], allow_custom_value=True, multiselect=False, value=None, scale=2)
                                    namespace_prefix = gr.Dropdown(
                                        label="Tag Namespace Prefix",
                                        choices=["danbooru:", "danbooru:character", "danbooru:general"],
                                        allow_custom_value=True,
                                        multiselect=False,
                                        value=None,
                                        scale=2
                                    )

        multi_view = create_multiview(select_history=select_history, bookmarks_namespace=bookmarks_namespace)

        with gr.Row(elem_classes="pagination-controls"):
            previous_page = gr.Button("Previous Page", scale=1)
            current_page = gr.Slider(value=1, label="Current Page", maximum=1, minimum=1, step=1, scale=2)
            next_page = gr.Button("Next Page", scale=1)

    search_tab.select(
        fn=on_tab_load,
        outputs=[selected_folder]
    )

    submit_button.click(
        fn=search_by_tags_search_button,
        inputs=[
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
            namespace_prefix
            ], 
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    current_page.release(
        fn=search_by_tags,
        inputs=[
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
            namespace_prefix
            ], 
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    previous_page.click(
        fn=search_by_tags_previous_page,
        inputs=[
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
            namespace_prefix
        ],
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    next_page.click(
        fn=search_by_tags_next_page,
        inputs=[
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
            namespace_prefix
        ],
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    multi_view.list_view.tag_list.select(
        fn=on_tag_select,
        outputs=[tag_input]
    )