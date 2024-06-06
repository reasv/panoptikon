from __future__ import annotations

import gradio as gr

from src.ui.scan import create_scan_UI
from src.ui.toptags import create_toptags_UI
from src.ui.test_model import create_dd_UI
from src.ui.search import create_search_UI

def create_root_UI():
    with gr.Blocks(css="static/style.css", fill_height=True) as ui:
        with gr.Tabs():
            with gr.TabItem(label="Tag Search"):
                create_search_UI()
            with gr.TabItem(label="File Scan & Tagging"):
                create_scan_UI()
            with gr.TabItem(label="Tag Frequency"):
                create_toptags_UI()
            with gr.TabItem(label="Tagging Model"):
                create_dd_UI()
    ui.launch()