from dataclasses import asdict
from typing import Any, Dict, List

import gradio as gr

from src.data_extractors.utils import (
    get_ocr_threshold_from_env,
    get_whisper_avg_logprob_threshold_from_env,
)
from src.db.search.types import ExtractedTextFilter, SearchQuery
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent, bind_event_listeners

threshold = min(
    get_ocr_threshold_from_env(),
    get_whisper_avg_logprob_threshold_from_env() or 0,
)


def create_extracted_text_fts_opts(
    query_state: gr.State,
):
    elements: List[AnyComponent] = []
    with gr.Tab(label="MATCH Text Extracted") as tab:
        elements.append(tab)
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

    def on_change_data(query: SearchQuery, args: dict[AnyComponent, Any]):
        text_query_val: str | None = args[text_query]
        query_targets: List[str] | None = args[targets]
        confidence_val: float = args[confidence]
        languages_val: List[str] | None = args[languages]
        language_confidence_val: float = args[language_confidence]

        if text_query_val:
            query.query.filters.extracted_text = ExtractedTextFilter(
                query=text_query_val,
                targets=[("text", target) for target in query_targets or []],
                min_confidence=confidence_val,
            )
            if languages_val:
                query.query.filters.extracted_text.languages = languages_val
                query.query.filters.extracted_text.language_min_confidence = (
                    language_confidence_val
                )
        else:
            query.query.filters.extracted_text = None

        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:

        extracted_text_available = bool(search_stats.et_setters)
        if not extracted_text_available:
            query.query.filters.extracted_text = None
        return {
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

    return elements, on_change_data, on_stats_change
