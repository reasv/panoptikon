from __future__ import annotations
from typing import List

import gradio as gr
import json

from src.db import find_paths_by_tags, get_database_connection, get_folders_from_database
from src.ui.components.gallery_view import create_gallery_view
from src.ui.components.list_view import create_image_list

def search_by_tags(tags_str: str, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
    if page < 1: page = 1

    include_path = include_path.strip() if include_path is not None else None
    if include_path == "": include_path = None

    tags = tags_str.split()
    conn = get_database_connection()
    results, total_results = find_paths_by_tags(conn, tags, page_size=results_per_page, min_confidence=min_tag_confidence, page=page, include_path=include_path)  
    conn.close()
    # Create a list of image paths to be displayed
    images = [(result['path'], json.dumps(result)) for result in results]
    print(f"Found {total_results} images")
    # Calculate the total number of pages, we need to round up
    total_pages = total_results // results_per_page + (1 if total_results % results_per_page > 0 else 0)
    item_list  = [[item['path'], item['path'], item["sha256"]] for item in results]
    return images, total_results, gr.update(value=page, maximum=int(total_pages)), gr.update(samples=item_list)

def search_by_tags_next_page(tags_str: str, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
    return search_by_tags(tags_str, min_tag_confidence, results_per_page, include_path, page+1)

def search_by_tags_previous_page(tags_str: str, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
    return search_by_tags(tags_str, min_tag_confidence, results_per_page, include_path, page-1)

def on_gallery_select_image(evt: gr.SelectData, select_history: List[str]):
    image_data = json.loads(evt.value['caption'])
    path = image_data['path']
    sha256 = image_data['sha256']
    select_history.append(image_data)
    return select_history, path, sha256

def on_list_select_image(dataset_data, select_history: List[str]):
    sha256 = dataset_data[2]
    pathstr = dataset_data[1]
    image_data = {'path': pathstr, 'sha256': sha256}
    select_history.append(image_data)
    return select_history

def get_folder_list():
    conn = get_database_connection()
    folders = get_folders_from_database(conn)
    conn.close()
    return folders

def on_tab_load():
    return gr.update(choices=get_folder_list())

def create_search_UI(select_history: gr.State = None, bookmarks_state: gr.State = None):
    with gr.TabItem(label="Tag Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                number_of_results = gr.Number(value=0, show_label=True, label="Results", interactive=False, scale=0)
                submit_button = gr.Button("Search", scale=0)
                with gr.Column(scale=10):
                    with gr.Group():
                        with gr.Row():
                            tag_input = gr.Textbox(label="Enter tags separated by spaces", value='rating:safe', show_copy_button=True, scale=3)
                            min_confidence = gr.Slider(minimum=0.05, maximum=1, value=0.25, step=0.05, label="Min. Confidence Level for Tags", scale=2)
                            max_results_per_page = gr.Slider(minimum=0, maximum=500, value=10, step=1, label="Results per page (0 for max)", scale=2)
                            selected_folder = gr.Dropdown(label="Limit search to items under path", choices=get_folder_list(), allow_custom_value=True, scale=2)         
        with gr.Tabs():
            with gr.TabItem(label="Gallery") as gallery_tab:
                gallery_view = create_gallery_view(parent_tab=gallery_tab, bookmarks_state=bookmarks_state)
            with gr.TabItem(label="List") as list_tab:
                list_view = create_image_list(parent_tab=list_tab, bookmarks_state=bookmarks_state, tag_input=tag_input)

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
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder], 
        outputs=[gallery_view.image_output, number_of_results, current_page, list_view.file_list]
    )

    current_page.release(
        fn=search_by_tags,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[gallery_view.image_output, number_of_results, current_page, list_view.file_list]
    )

    previous_page.click(
        fn=search_by_tags_previous_page,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[gallery_view.image_output, number_of_results, current_page, list_view.file_list]
    )

    next_page.click(
        fn=search_by_tags_next_page,
        inputs=[tag_input, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[gallery_view.image_output, number_of_results, current_page, list_view.file_list]
    )

    gallery_view.image_output.select(
        fn=on_gallery_select_image,
        inputs=[select_history],
        outputs=[select_history, list_view.selected_image_path, list_view.selected_image_sha256]
    )

    list_view.file_list.click(
        fn=on_list_select_image,
        inputs=[list_view.file_list, select_history],
        outputs=[select_history]
    )