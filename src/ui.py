#!/usr/bin/env python

from __future__ import annotations

import gradio as gr
import json
import os

from src import find_paths_by_tags, get_all_tags_for_item_name_confidence
from src.utils import show_in_fm

css = """
    .gradio-container {max-width: 100% !important}
    .centered-content {
        display: flex;
        flex-direction: column;
        align-items: center;
    }
    .centered-content > * {
        width: 50%;
    }
    .grid-wrap {
        max-height: 75vh !important
    }
"""

def process_tags(tag_string, max_results, confidence):
    tags = tag_string.split()
    results = find_paths_by_tags(tags, limit=max_results, min_confidence=confidence)  
    # Create a list of image paths to be displayed
    images = [(result['path'], json.dumps(result)) for result in results]
    return images, str(len(images))

def update_gallery(tags, columns, max_results, min_confidence):
    images, counts = process_tags(tags, max_results, min_confidence)
    return gr.update(value=images, columns=columns), counts

def on_select(evt: gr.SelectData):
    pathstr = json.loads(evt.value['caption'])['path']
    return gr.update(visible=True, value=pathstr), pathstr, gr.update(visible=True), gr.update(visible=True)

def on_select_label(evt: gr.SelectData):
    return evt.value

def on_select_tags(evt: gr.SelectData): 
    sha256 = json.loads(evt.value['caption'])['sha256']
    tags = { t[0]: t[1] for t in get_all_tags_for_item_name_confidence(sha256)}
    text = ", ".join(tags.keys())
    return tags, text

def open_file(image_path):
    if os.path.exists(image_path):
        os.startfile(image_path, cwd=os.path.dirname(image_path))
        return f"Attempting to open: {image_path}"
    else:
        return "File does not exist"
    
def open_in_explorer(image_path):
    if os.path.exists(image_path):
        # subprocess.run(['explorer', '/select,', os.path.normpath(image_path)])
        show_in_fm(image_path)
        return f"Attempting to open: {image_path}"
    else:
        return "File does not exist"

def create_UI():
    with gr.Blocks(css=css, fill_height=True) as ui:
        with gr.Column(elem_classes="centered-content", scale=0):
            gr.Markdown("# Image Tag Search")
            with gr.Row():
                tag_input = gr.Textbox(label="Enter tags separated by spaces")
                min_confidence = gr.Slider(minimum=0.1, maximum=1, value=0.25, step=0.05, label="Min. Confidence Level for Tags")
            with gr.Row():
                max_results = gr.Slider(minimum=0, maximum=500, value=5, step=5, label="Limit number of results (0 for maximum)")
                columns_slider = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Number of columns")
            with gr.Row():
                with gr.Column():
                    submit_button = gr.Button("Find Images")
                    number_of_results = gr.Text(value="0", label="Results Displayed", interactive=False)
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
            fn=update_gallery,
            inputs=[tag_input, columns_slider, max_results, min_confidence], 
            outputs=[image_output, number_of_results]
        )
        image_output.select(on_select, None, [image_path_output, image_preview, open_file_button, open_file_explorer])
        image_output.select(on_select_tags, None, [tag_list, tag_text])
        tag_list.select(on_select_label, None, [tag_input])
        open_file_button.click(
            fn=open_file,
            inputs=image_path_output,
        )
        open_file_explorer.click(
            fn=open_in_explorer,
            inputs=image_path_output,
        )

    ui.launch()