from __future__ import annotations

import gradio as gr

from src.ui.search import create_search_UI
from src.ui.toptags import create_toptags_UI


def create_root_UI():
    with gr.Blocks(css="static/style.css", fill_height=True) as ui:
        with gr.Tabs():
            with gr.TabItem(label="Tag Search"):
                create_search_UI()
            with gr.TabItem(label="Tag Frequency"):
                create_toptags_UI()
    ui.launch()
