from __future__ import annotations

import gradio as gr

from src.ui.bookmarks import create_bookmarks_UI
from src.ui.history import create_history_UI
from src.ui.query_db import create_query_UI
from src.ui.rule_config import create_rule_config_UI
from src.ui.scan import create_scan_UI
from src.ui.search import create_search_UI
from src.ui.test_models import create_model_demo
from src.ui.toptags import create_toptags_UI


def create_root_UI():
    with gr.Blocks(
        css="static/style.css", fill_height=True, analytics_enabled=False
    ) as ui:
        select_history = gr.State(value=[])
        bookmarks_namespace = gr.State(value="default")
        with gr.Tabs():
            create_search_UI(
                ui, select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_bookmarks_UI(bookmarks_namespace=bookmarks_namespace)
            create_history_UI(
                select_history, bookmarks_namespace=bookmarks_namespace
            )
            create_toptags_UI()
            create_scan_UI(ui)
            create_rule_config_UI(ui)
            create_model_demo()
            create_query_UI()
    return ui
