from dataclasses import asdict
from typing import List, Tuple

import gradio as gr

from src.data_extractors.utils import (
    get_ocr_threshold_from_env,
    get_whisper_avg_logprob_threshold_from_env,
)
from src.db.search.types import ExtractedTextFilter, SearchQuery
from src.db.search.utils import from_dict

threshold = min(
    get_ocr_threshold_from_env(),
    get_whisper_avg_logprob_threshold_from_env() or 0,
)


def create_extracted_text_fts_opts(
    query_state: gr.State,
    extracted_text_setters: List[Tuple[str, Tuple[str, str]]],
):

    if extracted_text_setters:
        with gr.Tab(label="MATCH Text Extracted"):
            with gr.Row():
                text_query = gr.Textbox(
                    key="extracted_text_query_fts",
                    label="SQL MATCH query on text exctracted by OCR/Whisper etc.",
                    show_copy_button=True,
                    scale=2,
                )
                targets = gr.Dropdown(
                    key="extracted_text_targets_fts",
                    choices=extracted_text_setters,  # type: ignore
                    interactive=True,
                    label="Only Search In Text From These Sources",
                    multiselect=True,
                    scale=1,
                )
                confidence = gr.Slider(
                    key="extracted_text_confidence",
                    minimum=0.05,
                    maximum=1,
                    value=threshold,
                    step=0.05,
                    label="Min. Confidence Level from Text Extraction",
                    scale=1,
                )
                languages = gr.Dropdown(
                    key="extracted_text_languages",
                    label="Languages",
                    choices=[
                        "en",
                        "fr",
                        "de",
                        "es",
                        "it",
                        "pt",
                        "nl",
                        "sv",
                        "da",
                        "no",
                    ],
                    multiselect=True,
                    value=[],
                    scale=1,
                )
                language_confidence = gr.Slider(
                    key="extracted_text_language_confidence",
                    minimum=0.05,
                    maximum=1,
                    value=threshold,
                    step=0.05,
                    label="Min. Confidence Level for Language Detection",
                    scale=1,
                )

            gr.on(
                triggers=[
                    text_query.input,
                    targets.select,
                    confidence.release,
                    languages.select,
                    language_confidence.release,
                ],
                fn=on_change_data,
                inputs=[
                    query_state,
                    text_query,
                    targets,
                    confidence,
                    languages,
                    language_confidence,
                ],
                outputs=[query_state],
            )


def on_change_data(
    query_state_dict: dict,
    text_query: str | None,
    query_targets: List[Tuple[str, str]] | None,
    confidence: float,
    languages: List[str] | None,
    language_confidence: float,
):
    print("on_change_data")
    print(query_state_dict)
    query_state = from_dict(SearchQuery, query_state_dict)

    if text_query:
        query_state.query.filters.extracted_text = ExtractedTextFilter(
            query=text_query,
            targets=query_targets or [],
            min_confidence=confidence,
        )
        if languages:
            query_state.query.filters.extracted_text.languages = languages
            query_state.query.filters.extracted_text.language_min_confidence = (
                language_confidence
            )
    else:
        query_state.query.filters.extracted_text = None

    return asdict(query_state)
