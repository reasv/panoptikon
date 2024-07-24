from __future__ import annotations

import gradio as gr

from src.ui.test_models.clip import create_CLIP_ui
from src.ui.test_models.fasterw import create_whisper_ui
from src.ui.test_models.florence2 import create_florence_2_ui
from src.ui.test_models.ocr_doctr import create_doctr_UI
from src.ui.test_models.tagging import create_wd_tagger_UI


def create_model_demo():
    with gr.TabItem(label="Models"):
        with gr.Tabs():
            with gr.Tab(label="WD Taggers"):
                create_wd_tagger_UI()
            with gr.Tab(label="docTR OCR"):
                create_doctr_UI()
            with gr.Tab(label="CLIP Semantic Search"):
                create_CLIP_ui()
            with gr.Tab(label="Whisper STT"):
                create_whisper_ui()
            with gr.Tab(label="Florence 2"):
                create_florence_2_ui()
