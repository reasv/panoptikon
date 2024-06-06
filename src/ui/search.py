from __future__ import annotations

import gradio as gr
import json

from src.db import find_paths_by_tags, get_all_tags_for_item_name_confidence, get_database_connection
from src.utils import open_file, open_in_explorer

def search_by_tags(tags_str: str, columns: int, min_tag_confidence: float, results_per_page: int, page: int = 1, include_path: str = None):
    tags = tags_str.split()
    conn = get_database_connection()
    results, total_results = find_paths_by_tags(conn, tags, page_size=results_per_page, min_confidence=min_tag_confidence, page=page, include_path=include_path)  
    conn.close()
    # Create a list of image paths to be displayed
    images = [(result['path'], json.dumps(result)) for result in results]
    print(f"Found {total_results} images")
    return gr.update(value=images, columns=columns), total_results

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

def create_search_UI():
    with gr.Column(elem_classes="centered-content", scale=0):
        with gr.Row():
            tag_input = gr.Textbox(label="Enter tags separated by spaces")
            min_confidence = gr.Slider(minimum=0.1, maximum=1, value=0.25, step=0.05, label="Min. Confidence Level for Tags")
        with gr.Row():
            max_results = gr.Slider(minimum=0, maximum=500, value=10, step=5, label="Limit number of results (0 for maximum)")
            columns_slider = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Number of columns")
        with gr.Row():
            with gr.Column():
                submit_button = gr.Button("Find Images")
                number_of_results = gr.Number(value=0, label="Total Results", interactive=False)
            with gr.Column():
                image_path_output = gr.Text(value="", label="Last Selected Image Path", interactive=False, visible=False)
                with gr.Row():
                    open_file_button = gr.Button("Open File", visible=False)
                    open_file_explorer = gr.Button("Show in File Manager", visible=False)

    with gr.Tabs():
        with gr.TabItem(label="Gallery"):
            image_output = gr.Gallery(label="Results", scale=2)
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
        inputs=[tag_input, columns_slider, min_confidence, max_results], 
        outputs=[image_output, number_of_results]
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