from __future__ import annotations
from typing import List
import json

import gradio as gr
from src.ui.components.list_view import create_image_list
from src.ui.components.gallery_view import create_gallery_view
from src.ui.components.history_dict import HistoryDict

def get_bookmarks_paths(bookmarks_state: HistoryDict):
    bookmarks_state = HistoryDict(bookmarks_state)
    print(f"Bookmarks length is {len(bookmarks_state)}")
    # Should be in reverse order
    reverse = [(path, json.dumps({"sha256": sha256, "path": path})) for sha256, path in bookmarks_state.reverse_chronological_order()]
    reverse_list = [[path, path, sha256] for sha256, path in bookmarks_state.reverse_chronological_order()]
    return reverse, gr.update(samples=reverse_list)

def erase_bookmarks_fn(bookmarks_state: HistoryDict, keep_last_n: int):
    bookmarks_state = HistoryDict(bookmarks_state)
    if keep_last_n > 0:
        bookmarks_state.keep_latest_n(keep_last_n)
    else:
        bookmarks_state.reset()
    print("Bookmarks erased")
    gallery_update, list_update = get_bookmarks_paths(bookmarks_state)
    return bookmarks_state, gallery_update, list_update

def delete_bookmark_fn(bookmarks_state: HistoryDict, selected_image_sha256: str):
    bookmarks_state = HistoryDict(bookmarks_state)
    bookmarks_state.remove(selected_image_sha256)
    print("Bookmark deleted")
    gallery_update, list_update = get_bookmarks_paths(bookmarks_state)
    return bookmarks_state, gallery_update, list_update

def on_gallery_select_image(selected_image_path: str, selected_image_sha256: str):
    return selected_image_path, selected_image_sha256

def create_bookmarks_UI(bookmarks_state: gr.State):
    with gr.TabItem(label="Bookmarks") as bookmarks_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                erase_bookmarks = gr.Button("Erase bookmarks")
                keep_last_n = gr.Slider(minimum=0, maximum=100, value=0, step=1, label="Keep last N items on erase")
        with gr.Tabs():
            with gr.TabItem(label="Gallery"):
                bookmarks_gallery = create_gallery_view(extra_actions=["Remove"])

            with gr.TabItem(label="List"):
                bookmarks_list = create_image_list(extra_actions=["Remove"])

    bookmarks_tab.select(
        fn=get_bookmarks_paths,
        inputs=[bookmarks_state],
        outputs=[bookmarks_gallery.image_output, bookmarks_list.file_list]
    )

    erase_bookmarks.click(
        fn=erase_bookmarks_fn,
        inputs=[bookmarks_state, keep_last_n],
        outputs=[bookmarks_state, bookmarks_gallery.image_output, bookmarks_list.file_list]
    )

    bookmarks_gallery.selected_image_path.change(
        fn=on_gallery_select_image,
        inputs=[bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256],
        outputs=[bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256]
    )

    bookmarks_list.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_state, bookmarks_list.selected_image_sha256],
        outputs=[bookmarks_state, bookmarks_gallery.image_output, bookmarks_list.file_list]
    )

    bookmarks_gallery.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_state, bookmarks_gallery.selected_image_sha256],
        outputs=[bookmarks_state, bookmarks_gallery.image_output, bookmarks_list.file_list]
    )
