from __future__ import annotations
from typing import List

import gradio as gr
from dataclasses import dataclass

from src.utils import open_file, open_in_explorer
from src.ui.components.utils import process_image_selection

def on_select_image_list(dataset_data, select_history: List[str]):
    sha256 = dataset_data[2]
    pathstr = dataset_data[1]
    image_data = {'path': pathstr, 'sha256': sha256}
    return process_image_selection(image_data, select_history)

def on_tag_click(evt: gr.SelectData):
    return evt.value

# We define a dataclass to use as return value for image_list which contains all the components we want to expose
@dataclass
class ImageList:
    file_list: gr.Dataset
    image_preview: gr.Image
    tag_text: gr.Textbox
    tag_list: gr.Label
    image_path_output: gr.Textbox
    btn_open_file: gr.Button
    btn_open_file_explorer: gr.Button
    bookmark: gr.Button

def image_list(tag_input: gr.Textbox = None):
    with gr.Row():
        with gr.Column(scale=1):
            file_list = gr.Dataset(label="Results", type="values", samples_per_page=10, samples=[], components=["image", "textbox"], scale=1)
        with gr.Column(scale=2):
            image_preview = gr.Image(elem_id="largeSearchPreview", value=None, label="Selected Image")
        with gr.Column(scale=1):
            with gr.Tabs():
                with gr.Tab(label="Tags"):
                    tag_text = gr.Textbox(label="Tags", show_copy_button=True, interactive=False, lines=5)
                with gr.Tab(label="Tags Confidence"):
                    tag_list = gr.Label(label="Tags", show_label=False)
            image_path_output = gr.Textbox(value="", label="Last Selected Image", show_copy_button=True, interactive=False)
            with gr.Row():
                btn_open_file = gr.Button("Open File", interactive=False, scale=3)
                btn_open_file_explorer = gr.Button("Show in Explorer", interactive=False, scale=3)
                bookmark = gr.Button("Bookmark", interactive=False, scale=3)

    # file_list.click(
    #     fn=on_select_image_list,
    #     inputs=[file_list, select_history],
    #     outputs=[
    #         image_path_output, image_preview, btn_open_file, btn_open_file_explorer,
    #         tag_list, tag_text, select_history
    #     ]
    # )

    if tag_input:
        tag_list.select(on_tag_click, None, [tag_input])

    btn_open_file.click(
        fn=open_file,
        inputs=image_path_output,
    )
    
    btn_open_file_explorer.click(
        fn=open_in_explorer,
        inputs=image_path_output,
    )

    return ImageList(
        file_list=file_list,
        image_preview=image_preview,
        tag_text=tag_text,
        tag_list=tag_list,
        image_path_output=image_path_output,
        btn_open_file=btn_open_file,
        btn_open_file_explorer=btn_open_file_explorer,
        bookmark=bookmark
    )