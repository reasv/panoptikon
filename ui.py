#!/usr/bin/env python

from __future__ import annotations

import gradio as gr
import json
import os
import subprocess

from src import find_paths_by_tags, get_all_tags_for_item_name_confidence

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

def process_tags(tag_string):
    tags = tag_string.split()
    results = find_paths_by_tags(tags)  
    # Create a list of image paths to be displayed
    images = [(result['path'], json.dumps(result)) for result in results]
    return images

def update_gallery(tags, columns):
    images = process_tags(tags)
    return gr.update(value=images, columns=columns)

def on_select(evt: gr.SelectData):  # SelectData is a subclass of EventData
    pathstr = json.loads(evt.value['caption'])['path']
    return pathstr, pathstr

def on_select_tags(evt: gr.SelectData):  # SelectData is a subclass of EventData
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
        subprocess.run(['explorer', '/select,', os.path.normpath(image_path)])
        return f"Attempting to open: {image_path}"
    else:
        return "File does not exist"

with gr.Blocks(css=css, fill_height=True) as demo:
    with gr.Column(elem_classes="centered-content", scale=0):
        gr.Markdown("# Image Tag Search")
        tag_input = gr.Textbox(label="Enter tags separated by spaces")
        columns_slider = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Number of columns")
        submit_button = gr.Button("Find Images")

    with gr.Tabs():
        with gr.TabItem(label="Gallery"):
            image_output = gr.Gallery(label="Results", scale=2)
        with gr.TabItem(label="Selected Image"):
            with gr.Row():
                image_path_output = gr.Text(value="", label="Selected Image Path", interactive=False)
                open_file_button = gr.Button("Open File")
                open_file_explorer = gr.Button("Show in File Manager")
                open_status = gr.Textbox(label="Status", interactive=False)
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
        inputs=[tag_input, columns_slider], 
        outputs=image_output
    )
    image_output.select(on_select, None, [image_path_output, image_preview])
    image_output.select(on_select_tags, None, [tag_list, tag_text])

    open_file_button.click(
        fn=open_file,
        inputs=image_path_output,
        outputs=open_status
    )
    open_file_explorer.click(
        fn=open_in_explorer,
        inputs=image_path_output,
        outputs=open_status
    )

demo.launch()