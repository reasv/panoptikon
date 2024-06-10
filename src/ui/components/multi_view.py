from __future__ import annotations
from dataclasses import dataclass
from typing import List

import gradio as gr

from src.ui.components.gallery_view import create_gallery_view, GalleryView
from src.ui.components.list_view import create_image_list, ImageList

def on_files_change():
    return []

def on_selected_files_change(selected_files: List[str], selection_history: List[str]):
    if len(selected_files) > 0:
        selection_history.append(selected_files[0])
    return selection_history

@dataclass
class Multiview:
    selected_files: gr.State
    files: gr.State
    gallery_view: GalleryView
    list_view: ImageList

def create_multiview(select_history: gr.State = None, bookmarks_namespace: gr.State = None):
    selected_files = gr.State([])
    files = gr.State([])

    with gr.Tabs():
        with gr.TabItem(label="Gallery") as gallery_tab:
            gallery_view = create_gallery_view(
                selected_files=selected_files,
                files=files,
                parent_tab=gallery_tab,
                bookmarks_namespace=bookmarks_namespace
            )
        # with gr.TabItem(label="List") as list_tab:
        #     list_view = create_image_list(
        #         selected_files=selected_files,
        #         files=files,
        #         parent_tab=list_tab,
        #         bookmarks_namespace=bookmarks_namespace
        #     )

    # Reset selected files when the list of files changes
    files.change(
        fn=on_files_change,
        inputs=[],
        outputs=[selected_files]
    )

    selected_files.change(
        fn=on_selected_files_change,
        inputs=[selected_files, select_history],
        outputs=[select_history]
    )

    return Multiview(
        selected_files=selected_files,
        gallery_view=gallery_view,
        list_view=None,
        files=files
    )