from __future__ import annotations
from typing import List
import json

import gradio as gr
from src.ui.components.list_view import create_image_list
from src.ui.components.gallery_view import create_gallery_view
from src.ui.components.utils import delete_bookmarks_except_last_n, get_all_bookmarks_in_folder, delete_bookmark
from src.ui.components.bookmark_folder_selector import create_bookmark_folder_chooser # type: ignore

def get_bookmarks_paths(bookmarks_namespace: str):
    bookmarks, total_bookmarks = get_all_bookmarks_in_folder(bookmarks_namespace)
    gallery = [(path, json.dumps({"sha256": sha256, "path": path})) for sha256, path in bookmarks]
    blist = [[path, path, sha256] for sha256, path in bookmarks]
    print(f"Bookmarks fetched from {bookmarks_namespace} folder. Total: {total_bookmarks}, Displayed: {len(blist)}")
    return gallery, gr.update(samples=blist), None, None, None, None

def erase_bookmarks_fn(bookmarks_namespace: str, keep_last_n: int):
    delete_bookmarks_except_last_n(bookmarks_namespace, keep_last_n)
    print("Bookmarks erased")
    gallery_update, list_update, _, _, _, _ = get_bookmarks_paths(bookmarks_namespace)
    return gallery_update, list_update, None, None, None, None

def delete_bookmark_fn(bookmarks_namespace: str, selected_image_sha256: str):
    delete_bookmark(bookmarks_namespace=bookmarks_namespace, sha256=selected_image_sha256)
    print("Bookmark deleted")
    gallery_update, list_update, _, _, _, _ = get_bookmarks_paths(bookmarks_namespace)
    return gallery_update, list_update, None, None, None, None

def on_gallery_select_image(selected_image_path: str, selected_image_sha256: str):
    return selected_image_path, selected_image_sha256

def create_bookmarks_UI(bookmarks_namespace: gr.State):
    with gr.TabItem(label="Bookmarks") as bookmarks_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                create_bookmark_folder_chooser(parent_tab=bookmarks_tab, bookmarks_namespace=bookmarks_namespace)
                erase_bookmarks = gr.Button("Erase bookmarks")
                keep_last_n = gr.Slider(minimum=0, maximum=100, value=0, step=1, label="Keep last N items on erase")
        with gr.Tabs():
            with gr.TabItem(label="Gallery"):
                bookmarks_gallery = create_gallery_view(extra_actions=["Remove"])

            with gr.TabItem(label="List"):
                bookmarks_list = create_image_list(extra_actions=["Remove"])

    bookmarks_tab.select(
        fn=get_bookmarks_paths,
        inputs=[bookmarks_namespace],
        outputs=[bookmarks_gallery.image_output, bookmarks_list.file_list]
    )

    bookmarks_namespace.change(
        fn=get_bookmarks_paths,
        inputs=[bookmarks_namespace],
        outputs=[
            bookmarks_gallery.image_output, bookmarks_list.file_list,
            bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256,
            bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256
        ]
    )

    erase_bookmarks.click(
        fn=erase_bookmarks_fn,
        inputs=[bookmarks_namespace, keep_last_n],
        outputs=[
            bookmarks_gallery.image_output, bookmarks_list.file_list,
            bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256,
            bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256
        ]
    )

    bookmarks_gallery.selected_image_path.change(
        fn=on_gallery_select_image,
        inputs=[bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256],
        outputs=[bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256]
    )

    bookmarks_list.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_namespace, bookmarks_list.selected_image_sha256],
        outputs=[
            bookmarks_gallery.image_output, bookmarks_list.file_list,
            bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256,
            bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256
        ]
    )

    bookmarks_gallery.extra[0].click(
        fn=delete_bookmark_fn,
        inputs=[bookmarks_namespace, bookmarks_gallery.selected_image_sha256],
        outputs=[
            bookmarks_gallery.image_output, bookmarks_list.file_list,
            bookmarks_list.selected_image_path, bookmarks_list.selected_image_sha256,
            bookmarks_gallery.selected_image_path, bookmarks_gallery.selected_image_sha256
        ]
    )
