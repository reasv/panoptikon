from __future__ import annotations

import os

import gradio as gr

from src.ui.scan import create_scan_UI
from src.ui.toptags import create_toptags_UI
from src.ui.test_model import create_dd_UI
from src.ui.search import create_search_UI
# from src.ui.history import create_history_UI
from src.ui.bookmarks import create_bookmarks_UI

def create_root_UI():
    with gr.Blocks(css="static/style.css", fill_height=True) as ui:
        select_history = gr.State(value=[])
        bookmarks_namespace = gr.State(value="default")

        with gr.Tabs():
            create_search_UI(select_history, bookmarks_namespace=bookmarks_namespace)
            create_bookmarks_UI(bookmarks_namespace=bookmarks_namespace)
            # create_history_UI(select_history, bookmarks_namespace=bookmarks_namespace)
            with gr.TabItem(label="Tag Frequency"):
                create_toptags_UI()
            with gr.TabItem(label="File Scan & Tagging"):
                create_scan_UI()
            with gr.TabItem(label="Tagging Model"):
                create_dd_UI()
    return ui