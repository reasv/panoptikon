from __future__ import annotations

import gradio as gr
import json

from src.db import find_paths_by_tags, get_all_tags_for_item_name_confidence, get_database_connection, get_folders_from_database
from src.utils import open_file, open_in_explorer

def search_by_tags(tags_str: str, columns: int, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
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
    return gr.update(value=images, columns=columns), total_results, gr.update(value=page, maximum=int(total_results/results_per_page))

def search_by_tags_next_page(tags_str: str, columns: int, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
    return search_by_tags(tags_str, columns, min_tag_confidence, results_per_page, include_path, page+1)

def search_by_tags_previous_page(tags_str: str, columns: int, min_tag_confidence: float, results_per_page: int, include_path: str = None, page: int = 1):
    return search_by_tags(tags_str, columns, min_tag_confidence, results_per_page, include_path, page-1)

def on_select_image(evt: gr.SelectData):
    pathstr = json.loads(evt.value['caption'])['path']
    return gr.update(visible=True, value=pathstr), pathstr, gr.update(visible=True), gr.update(visible=True)

def on_select_tag(evt: gr.SelectData):
    sha256 = json.loads(evt.value['caption'])['sha256']
    conn = get_database_connection()
    tags = { t[0]: t[1] for t in get_all_tags_for_item_name_confidence(conn, sha256)}
    conn.close()
    text = ", ".join(tags.keys())
    return tags, text

def on_tag_click(evt: gr.SelectData):
    return evt.value

def get_folder_list():
    conn = get_database_connection()
    folders = get_folders_from_database(conn)
    conn.close()
    return folders

def input_folder(evt):
    return evt

def change_page(evt):
    return evt

def create_search_UI():
    with gr.Column(elem_classes="centered-content", scale=0):
        with gr.Row():
            tag_input = gr.Textbox(label="Enter tags separated by spaces", value='rating:safe', scale=3)
            min_confidence = gr.Slider(minimum=0.1, maximum=1, value=0.25, step=0.05, label="Min. Confidence Level for Tags", scale=2)
            submit_button = gr.Button("Search", scale=1)
            number_of_results = gr.Number(value=0, label="Total Results", interactive=False)
        with gr.Row():
            max_results_per_page = gr.Slider(minimum=0, maximum=500, value=10, step=5, label="Results per page (0 for max)")
            columns_slider = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Number of columns")
        with gr.Row():
            with gr.Column():
                selected_folder = gr.Dropdown(label="Limit search to items under path", choices=get_folder_list(), allow_custom_value=True, scale=2)
            with gr.Column():
                with gr.Row():
                    image_path_output = gr.Text(value="", label="Last Selected Image Path", interactive=False, visible=False)
                    with gr.Column():
                        open_file_button = gr.Button("Open File", visible=False)
                        open_file_explorer = gr.Button("Show in File Manager", visible=False)

    with gr.Tabs():
        with gr.TabItem(label="Gallery"):
            image_output = gr.Gallery(label="Results", scale=2)
            with gr.Row():
                previous_page = gr.Button("Previous Page", scale=1)
                current_page = gr.Slider(value=1, label="Current Page", maximum=1, minimum=1, step=1, scale=2)
                next_page = gr.Button("Next Page", scale=1)
        with gr.TabItem(label="Selected Image"):
            with gr.Row():
                with gr.Column(scale=2):
                    image_preview = gr.Image(value="./static/404.png", label="Selected Image")
                with gr.Column(scale=1):
                    with gr.Tabs():
                        with gr.Tab(label="Tags"):
                            tag_list = gr.Label(label="Tags", show_label=False)
                        with gr.Tab(label="Tags list"):
                            tag_text = gr.Textbox(label="Tags", interactive=False, lines=5)

    submit_button.click(
        fn=search_by_tags,
        inputs=[tag_input, columns_slider, min_confidence, max_results_per_page, selected_folder], 
        outputs=[image_output, number_of_results, current_page]
    )

    current_page.release(
        fn=search_by_tags,
        inputs=[tag_input, columns_slider, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[image_output, number_of_results, current_page]
    )

    previous_page.click(
        fn=search_by_tags_previous_page,
        inputs=[tag_input, columns_slider, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[image_output, number_of_results, current_page]
    )

    next_page.click(
        fn=search_by_tags_next_page,
        inputs=[tag_input, columns_slider, min_confidence, max_results_per_page, selected_folder, current_page], 
        outputs=[image_output, number_of_results, current_page]
    )

    image_output.select(on_select_image, None, [image_path_output, image_preview, open_file_button, open_file_explorer])
    image_output.select(on_select_tag, None, [tag_list, tag_text])
    tag_list.select(on_tag_click, None, [tag_input])

    open_file_button.click(
        fn=open_file,
        inputs=image_path_output,
    )
    
    open_file_explorer.click(
        fn=open_in_explorer,
        inputs=image_path_output,
    )