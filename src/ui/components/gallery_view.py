from __future__ import annotations
from dataclasses import dataclass
from typing import List

import gradio as gr
import json

from src.utils import open_file, open_in_explorer
from src.ui.components.utils import add_bookmark

def on_select_image(evt: gr.SelectData):
    image_data = json.loads(evt.value['caption'])
    return image_data['path'], image_data['sha256']

def on_change_columns_slider(columns_slider: int):
    return gr.update(columns=columns_slider)

# We define a dataclass to use as return value for create_gallery_view which contains all the components we want to expose
@dataclass
class GalleryView:
    columns_slider: gr.Slider
    selected_image_path: gr.Textbox
    selected_image_sha256: gr.Textbox
    open_file_button: gr.Button
    open_file_explorer: gr.Button
    bookmark: gr.Button
    extra: List[gr.Button]
    image_output: gr.Gallery

def create_gallery_view(bookmarks_state: gr.State = None, extra_actions: List[str] = []):
    with gr.Row():
        columns_slider = gr.Slider(minimum=1, maximum=15, value=5, step=1, label="Number of columns")
        selected_image_path = gr.Textbox(value="", label="Last Selected Image", show_copy_button=True, interactive=False)
        selected_image_sha256 = gr.Textbox(value="", label="Last Selected Image SHA256", show_copy_button=True, interactive=False, visible=False) # Hidden
        open_file_button = gr.Button("Open File", interactive=False)
        open_file_explorer = gr.Button("Show in Explorer", interactive=False)
        bookmark = gr.Button("Bookmark", interactive=False, visible=bookmarks_state != None)
        extra: List[gr.Button] = []
        for action in extra_actions:
            extra.append(gr.Button(action, interactive=False))
    image_output = gr.Gallery(label="Results", elem_classes=["gallery-view"], columns=5, scale=2)

    def on_selected_image_path_change(path: str):
        nonlocal extra_actions
        if path.strip() == "":
            return gr.update(interactive=False), gr.update(interactive=False), gr.update(interactive=False)
        updates = gr.update(interactive=True), gr.update(interactive=True), gr.update(interactive=True)
        # Add updates to the tuple for extra actions
        for _ in extra_actions:
            updates += (gr.update(interactive=True),)
        return updates

    image_output.select(
        fn=on_select_image,
        inputs=[],
        outputs=[selected_image_path, selected_image_sha256]
    )

    selected_image_path.change(
        fn=on_selected_image_path_change,
        inputs=[selected_image_path],
        outputs=[open_file_button, open_file_explorer, bookmark, *extra]
    )

    columns_slider.release(
        fn=on_change_columns_slider,
        inputs=[columns_slider],
        outputs=[image_output]
    )

    open_file_button.click(
        fn=open_file,
        inputs=selected_image_path,
    )
    
    open_file_explorer.click(
        fn=open_in_explorer,
        inputs=selected_image_path,
    )
    if bookmarks_state != None:
        bookmark.click(
            fn=add_bookmark,
            inputs=[bookmarks_state, selected_image_sha256, selected_image_path],
            outputs=[bookmarks_state]
        )

    return GalleryView(
        columns_slider=columns_slider,
        selected_image_path=selected_image_path,
        selected_image_sha256=selected_image_sha256,
        open_file_button=open_file_button,
        open_file_explorer=open_file_explorer,
        bookmark=bookmark,
        extra=extra,
        image_output=image_output
    )