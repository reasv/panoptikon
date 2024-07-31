from dataclasses import asdict
from typing import Any, List, Tuple

import gradio as gr

from src.data_extractors.utils import (
    get_ocr_threshold_from_env,
    get_whisper_avg_logprob_threshold_from_env,
)
from src.db.search.types import ExtractedTextFilter, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats

threshold = min(
    get_ocr_threshold_from_env(),
    get_whisper_avg_logprob_threshold_from_env() or 0,
)


def create_extracted_text_fts_opts(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    with gr.Tab(label="MATCH Text Extracted") as tab:
        elements: List[Any] = [tab]
        with gr.Row():
            text_query = gr.Textbox(
                key="extracted_text_query_fts",
                label="SQL MATCH query on text exctracted by OCR/Whisper etc.",
                show_copy_button=True,
                scale=2,
            )
            elements.append(text_query)
            targets = gr.Dropdown(
                key="extracted_text_targets_fts",
                choices=[],
                label="Only Search In Text From These Sources",
                multiselect=True,
                scale=1,
            )
            elements.append(targets)
            confidence = gr.Slider(
                key="extracted_text_confidence",
                minimum=0.05,
                maximum=1,
                value=threshold,
                step=0.05,
                label="Min. Confidence Level from Text Extraction",
                scale=1,
            )
            elements.append(confidence)
            languages = gr.Dropdown(
                key="extracted_text_languages",
                label="Languages",
                choices=[
                    "en",
                ],
                multiselect=True,
                value=[],
                scale=1,
            )
            elements.append(languages)
            language_confidence = gr.Slider(
                key="extracted_text_language_confidence",
                minimum=0.05,
                maximum=1,
                value=threshold,
                step=0.05,
                label="Min. Confidence Level for Language Detection",
                scale=1,
            )
            elements.append(language_confidence)

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

    def on_stats_change(
        query_state_dict: dict,
        search_stats_dict: dict,
    ):
        query = from_dict(SearchQuery, query_state_dict)
        search_stats = from_dict(SearchStats, search_stats_dict)

        extracted_text_available = bool(search_stats.et_setters)
        if not extracted_text_available:
            query.query.filters.extracted_text = None
        return {
            query_state: asdict(query),
            targets: gr.Dropdown(choices=search_stats.et_setters),
            languages: gr.Dropdown(choices=search_stats.et_stats.languages),
            language_confidence: search_stats.et_stats.lowest_language_confidence,
            confidence: search_stats.et_stats.lowest_confidence,
            tab: gr.Tab(visible=extracted_text_available),
            text_query: (
                gr.Textbox(value="")
                if not extracted_text_available
                else gr.update()
            ),
        }

    gr.on(
        triggers=[search_stats_state.change],
        fn=on_stats_change,
        inputs=[query_state, search_stats_state],
        outputs=[query_state, *elements],
    )


def on_change_data(
    query_state_dict: dict,
    text_query: str | None,
    query_targets: List[str] | None,
    confidence: float,
    languages: List[str] | None,
    language_confidence: float,
):
    query_state = from_dict(SearchQuery, query_state_dict)

    if text_query:
        query_state.query.filters.extracted_text = ExtractedTextFilter(
            query=text_query,
            targets=[("text", target) for target in query_targets or []],
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
