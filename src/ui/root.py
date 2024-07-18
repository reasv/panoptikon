from __future__ import annotations

import gradio as gr

from src.ui.bookmarks import create_bookmarks_UI
from src.ui.history import create_history_UI
from src.ui.scan import create_scan_UI
from src.ui.search import create_search_UI
from src.ui.semantic_search import create_semantic_search_UI
from src.ui.test_models import create_model_demo
from src.ui.text_search import create_semantic_text_search_UI
from src.ui.toptags import create_toptags_UI


def create_root_UI():
    with gr.Blocks(css="static/style.css", fill_height=True) as ui:
        select_history = gr.State(value=[])
        bookmarks_namespace = gr.State(value="default")
        with gr.Tabs():
            create_search_UI(
                select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_semantic_search_UI(
                select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_semantic_text_search_UI(
                select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_bookmarks_UI(bookmarks_namespace=bookmarks_namespace)
            create_history_UI(
                select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_toptags_UI()
            create_scan_UI()
            create_model_demo()
    return ui
