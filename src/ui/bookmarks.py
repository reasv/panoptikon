from __future__ import annotations
from typing import List
import json
from urllib.parse import quote

import gradio as gr
from src.ui.components.utils import delete_bookmarks_except_last_n, get_all_bookmarks_in_folder, delete_bookmark
from src.ui.components.bookmark_folder_selector import create_bookmark_folder_chooser # type: ignore
from src.ui.components.multi_view import create_multiview

def get_bookmarks_paths(bookmarks_namespace: str):
    bookmarks, total_bookmarks = get_all_bookmarks_in_folder(bookmarks_namespace)
    print(f"Bookmarks fetched from {bookmarks_namespace} folder. Total: {total_bookmarks}, Displayed: {len(bookmarks)}")
    return [{ "path": path, "sha256": sha256 } for sha256, path in bookmarks]

def erase_bookmarks_fn(bookmarks_namespace: str, keep_last_n: int):
    delete_bookmarks_except_last_n(bookmarks_namespace, keep_last_n)
    print("Bookmarks erased")
    bookmarks = get_bookmarks_paths(bookmarks_namespace)
    return bookmarks

def delete_bookmark_fn(bookmarks_namespace: str, selected_files: List[dict]):
    if len(selected_files) == 0:
        print("No bookmark selected")
        return
    delete_bookmark(bookmarks_namespace=bookmarks_namespace, sha256=selected_files[0]["sha256"])
    print("Bookmark deleted")
    bookmarks = get_bookmarks_paths(bookmarks_namespace)
    return bookmarks

def build_bookmark_query(bookmarks_namespace: str, page_size: int = 10, page: int = 1):
    if not include_path: include_path = ""

    if include_path.strip() != "":
        # URL encode the path
        include_path = quote(include_path)
    return f"/bookmarks/{bookmarks_namespace}&page_size={page_size}&page={page}"

def create_bookmarks_UI(bookmarks_namespace: gr.State):
    with gr.TabItem(label="Bookmarks") as bookmarks_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                create_bookmark_folder_chooser(parent_tab=bookmarks_tab, bookmarks_namespace=bookmarks_namespace)
                erase_bookmarks = gr.Button("Erase bookmarks")
                keep_last_n = gr.Slider(minimum=0, maximum=100, value=0, step=1, label="Keep last N items on erase")
        
        multi_view = create_multiview(bookmarks_namespace=bookmarks_namespace, extra_actions=["Remove"])

    bookmarks_tab.select(
        fn=get_bookmarks_paths,
        inputs=[bookmarks_namespace],
        outputs=[multi_view.files]
    )

    bookmarks_namespace.change(
        fn=get_bookmarks_paths,
        inputs=[bookmarks_namespace],
        outputs=[
            multi_view.files
        ]
    )

    erase_bookmarks.click(
        fn=erase_bookmarks_fn,
        inputs=[bookmarks_namespace, keep_last_n],
        outputs=[
            multi_view.files
        ]
    )

    multi_view.list_view.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_namespace, multi_view.selected_files],
        outputs=[
            multi_view.files
        ]
    )

    multi_view.gallery_view.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_namespace, multi_view.selected_files],
        outputs=[
            multi_view.files
        ]
    )
