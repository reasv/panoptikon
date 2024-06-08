from __future__ import annotations
from typing import List
import json

import gradio as gr
from src.ui.components.list_view import create_image_list
from src.ui.components.gallery_view import create_gallery_view

def get_history_paths(select_history: List[str]):
    print(f"History length is {len(select_history)}")
    # Should be in reverse order
    reverse = [(item['path'], json.dumps(item)) for item in select_history[::-1]]
    reverse_list = [[item['path'], item['path'], item["sha256"]] for item in select_history[::-1]]
    return reverse, gr.update(samples=reverse_list)

def erase_history_fn(select_history: List[str], keep_last_n: int):
    if keep_last_n > 0:
        select_history = select_history[-keep_last_n:]
    else:
        select_history = []
    print("History erased")
    gallery_update, list_update = get_history_paths(select_history)
    return select_history, gallery_update, list_update, None, None, None, None

def on_gallery_select_image(selected_image_path: str, selected_image_sha256: str):
    return selected_image_path, selected_image_sha256

def create_history_UI(select_history: gr.State, bookmarks_state: gr.State):
    with gr.TabItem(label="History") as history_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                erase_history = gr.Button("Erase History")
                keep_last_n = gr.Slider(minimum=0, maximum=100, value=0, step=1, label="Keep last N items on erase")
        with gr.Tabs():
            with gr.TabItem(label="Gallery"):
                history_gallery = create_gallery_view(bookmarks_state=bookmarks_state)

            with gr.TabItem(label="List"):
                history_list = create_image_list(bookmarks_state=bookmarks_state)

    history_tab.select(
        fn=get_history_paths,
        inputs=[select_history],
        outputs=[history_gallery.image_output, history_list.file_list]
    )

    erase_history.click(
        fn=erase_history_fn,
        inputs=[select_history, keep_last_n],
        outputs=[
            select_history, history_gallery.image_output, history_list.file_list,
            history_list.selected_image_path, history_list.selected_image_sha256,
            history_gallery.selected_image_path, history_gallery.selected_image_sha256
        ]
    )

    history_gallery.selected_image_path.change(
        fn=on_gallery_select_image,
        inputs=[history_gallery.selected_image_path, history_gallery.selected_image_sha256],
        outputs=[history_list.selected_image_path, history_list.selected_image_sha256]
    )