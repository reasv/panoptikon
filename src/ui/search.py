from __future__ import annotations

import gradio as gr
import urllib.parse

from src.db import get_database_connection, get_folders_from_database, search_files
from src.ui.components.multi_view import create_multiview

def build_query(tags: list, min_tag_confidence: float, include_path: str = None, page_size: int = 10, page: int = 1, order_by: str = "last_modified", order = None):
    if not include_path: include_path = ""

    if include_path.strip() != "":
        # URL encode the path
        include_path = urllib.parse.quote(include_path)
    order_query = ""
    if order is not None:
        order_query = f"&order={order}"
    return f"/search/tags?tags={','.join(tags)}&min_confidence={min_tag_confidence}&include_path={include_path}&page_size={page_size}&page={page}&order_by={order_by}{order_query}"

def search_by_tags(
        tags_str: str,
        min_tag_confidence: float,
        results_per_page: int,
        include_path: str = None,
        page: int = 1,
        order_by: str = "last_modified",
        order = None
        ):
    if page < 1: page = 1

    include_path = include_path.strip() if include_path is not None else None
    if include_path == "": include_path = None

    tags = tags_str.split()
    conn = get_database_connection()

    results, total_results = zip(*list(search_files(
        conn,
        tags,
        negative_tags=[],
        tag_namespace="danbooru",
        min_confidence=min_tag_confidence,
        setters=None,
        all_setters_required = False,
        item_type = None,
        include_path_prefix = include_path,
        order_by=order_by,
        order=order,
        page=page,
        page_size=results_per_page,
        check_path_exists = True
    )))
    total_results = total_results[0]
    conn.close()
    print(f"Found {total_results} images")
    # Calculate the total number of pages, we need to round up
    total_pages = total_results // results_per_page + (1 if total_results % results_per_page > 0 else 0)
    return results, total_results, gr.update(value=page, maximum=int(total_pages)), f"[View Results in Gallery]({build_query(tags, min_tag_confidence, include_path, results_per_page, page, order_by, order)})"

def search_by_tags_next_page(tags_str: str, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1, order_by: str = "last_modified", order = None):
    return search_by_tags(tags_str, min_tag_confidence, results_per_page, include_path, page+1, order_by, order)

def search_by_tags_previous_page(tags_str: str, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1, order_by: str = "last_modified", order = None):
    return search_by_tags(tags_str, min_tag_confidence, results_per_page, include_path, page-1, order_by, order)

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
                    with gr.Group():
                        with gr.Row():
                            tag_input = gr.Textbox(label="Enter tags separated by spaces", value='', show_copy_button=True, scale=3)
                            min_confidence = gr.Slider(minimum=0.05, maximum=1, value=0.25, step=0.05, label="Min. Confidence Level for Tags", scale=2)
                            max_results_per_page = gr.Slider(minimum=0, maximum=500, value=10, step=1, label="Results per page (0 for max)", scale=2)
                            selected_folder = gr.Dropdown(label="Limit search to items under path", choices=get_folder_list(), allow_custom_value=True, scale=2)
                            order_by = gr.Radio(choices=["path", "last_modified"], label="Order by", value="last_modified", scale=2)       

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
        fn=search_by_tags,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page, order_by], 
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    current_page.release(
        fn=search_by_tags,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page, order_by], 
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    previous_page.click(
        fn=search_by_tags_previous_page,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page, order_by], 
        outputs=[
            multi_view.files,
            number_of_results,
            current_page,
            link
        ]
    )

    next_page.click(
        fn=search_by_tags_next_page,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page, order_by], 
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