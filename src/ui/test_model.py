from __future__ import annotations

import gradio as gr

import src.data_extractors.models as models
from src.data_extractors.wd_tagger import Predictor
from src.ui.clip import create_CLIP_ui
from src.ui.ocr_doctr import create_doctr_UI
from src.ui.test_tag_model import create_wd_tagger_UI


def create_model_demo():
    with gr.TabItem(label="Models"):
        with gr.Tabs():
            with gr.Tab(label="WD Taggers"):
                create_wd_tagger_UI()
            with gr.Tab(label="docTR OCR"):
                create_doctr_UI()
            with gr.Tab(label="CLIP Semantic Search"):
                create_CLIP_ui()
