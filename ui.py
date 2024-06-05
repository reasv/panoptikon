#!/usr/bin/env python

from __future__ import annotations

import gradio as gr
from src import find_paths_by_tags

def process_tags(tag_string):
    tags = tag_string.split()
    results = find_paths_by_tags(tags)  
    # Create a list of image paths to be displayed
    images = [result['path'] for result in results]
    return images

def update_gallery(tags, columns):
    images = process_tags(tags)
    return gr.update(value=images, columns=columns)

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

with gr.Blocks(css=css, fill_height=True) as demo:
    with gr.Column(elem_classes="centered-content", scale=0):
        gr.Markdown("# Image Tag Search")
        tag_input = gr.Textbox(label="Enter tags separated by spaces")
        columns_slider = gr.Slider(minimum=1, maximum=10, value=2, step=1, label="Number of columns")
        submit_button = gr.Button("Find Images")
    image_output = gr.Gallery(label="Results", scale=2)

    submit_button.click(
        fn=update_gallery, 
        inputs=[tag_input, columns_slider], 
        outputs=image_output
    )

demo.launch()