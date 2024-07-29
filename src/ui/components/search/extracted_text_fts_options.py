from typing import List, Tuple

import gradio as gr


def create_extracted_text_fts_opts(
    extracted_text_setters: List[Tuple[str, Tuple[str, str]]]
):
    if extracted_text_setters:
        with gr.Tab(label="MATCH Text Extracted"):
            with gr.Row():
                extracted_text_search = gr.Textbox(
                    label="SQL MATCH query on text exctracted by OCR/Whisper etc.",
                    value="",
                    show_copy_button=True,
                    scale=2,
                )
                require_text_extractors = gr.Dropdown(
                    choices=extracted_text_setters,
                    interactive=True,
                    label="Only Search In Text From These Sources",
                    multiselect=True,
                    scale=1,
                )
                extracted_text_order_by_rank = gr.Checkbox(
                    label="Order results by relevance if this query is present",
                    interactive=True,
                    value=True,
                    scale=1,
                )
