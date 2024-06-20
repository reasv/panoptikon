from __future__ import annotations
from dataclasses import dataclass
from typing import List

import gradio as gr

from src.ui.components.gallery_view import create_gallery_view, GalleryView
from src.ui.components.list_view import create_image_list, ImageList
from src.db import FileSearchResult

def on_files_change():
    return []

def on_selected_files_change(selected_files: List[FileSearchResult], selection_history: List[FileSearchResult]):
    if len(selected_files) > 0:
        selection_history.append(selected_files[0])
    return selection_history

@dataclass
class Multiview:
    selected_files: gr.State
    files: gr.State
    gallery_view: GalleryView
    list_view: ImageList

def create_multiview(select_history: gr.State = None, bookmarks_namespace: gr.State = None, extra_actions: List[str] = []):
    selected_files = gr.State([])
    files = gr.State([])

    with gr.Tabs():
        with gr.TabItem(label="Gallery") as gallery_tab:
            gallery_view = create_gallery_view(
                selected_files=selected_files,
                files=files,
                parent_tab=gallery_tab,
                bookmarks_namespace=bookmarks_namespace,
                extra_actions=extra_actions
            )
        with gr.TabItem(label="List") as list_tab:
            list_view = create_image_list(
                selected_files=selected_files,
                files=files,
                parent_tab=list_tab,
                bookmarks_namespace=bookmarks_namespace,
                extra_actions=extra_actions
            )

    # Reset selected files when the list of files changes
    files.change(
        fn=on_files_change,
        inputs=[],
        outputs=[selected_files]
    )
    if select_history is not None:
        selected_files.change(
            fn=on_selected_files_change,
            inputs=[selected_files, select_history],
            outputs=[select_history]
        )

    return Multiview(
        selected_files=selected_files,
        gallery_view=gallery_view,
        list_view=list_view,
        files=files
    )