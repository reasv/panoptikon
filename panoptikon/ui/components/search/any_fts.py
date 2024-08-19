from typing import Any, Dict, List, Tuple

import gradio as gr

from panoptikon.db.search.types import (
    AnyTextFilter,
    ExtractedTextFilter,
    PathTextFilter,
    SearchQuery,
)
from panoptikon.types import SearchStats
from panoptikon.ui.components.search.utils import AnyComponent


def create_fts_options(
    query_state: gr.State,
):
    path_setters = [
        ("Full Path", "path"),
        ("Filename", "filename"),
    ]
    elements: List[AnyComponent] = []
    with gr.Tab(label="Full Text Search"):
        with gr.Row():
            text_query = gr.Textbox(
                key="text_query_fts",
                label="Match text in any field (supports SQLite MATCH grammar)",
                show_copy_button=True,
                scale=2,
            )
            elements.append(text_query)
            query_targets = gr.Dropdown(
                key="query_targets_fts",
                choices=path_setters,  # type: ignore
                interactive=True,
                label="(Optional) Restrict query to these targets",
                multiselect=True,
                scale=1,
            )
            elements.append(query_targets)

    def on_data_change(
        query: SearchQuery,
        args: dict[AnyComponent, Any],
        final_query_build: bool = False,
    ) -> SearchQuery:
        text_query_val: str | None = args[text_query]
        query_targets_val: List[str] | None = args[query_targets]

        if not text_query_val:
            query.query.filters.any_text = None
            return query

        if not query_targets_val:
            # Default to searching in all text fields
            query.query.filters.any_text = AnyTextFilter(
                path=PathTextFilter(
                    query=text_query_val, only_match_filename=False
                ),
                extracted_text=ExtractedTextFilter(
                    query=text_query_val,
                ),
            )
            return query

        path_filter = None
        et_filter = None
        if "path" in query_targets_val:
            path_filter = PathTextFilter(
                query=text_query_val, only_match_filename=False
            )
        elif "filename" in query_targets_val:
            path_filter = PathTextFilter(
                query=text_query_val, only_match_filename=True
            )

        # Remove path and filename from query_targets_val
        et_targets = [
            t for t in query_targets_val if t not in ["path", "filename"]
        ]
        if et_targets:
            et_filter = ExtractedTextFilter(
                query=text_query_val, targets=et_targets
            )

        query.query.filters.any_text = AnyTextFilter(
            path=path_filter, extracted_text=et_filter
        )

        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:
        all_targets = path_setters + [(s, s) for s in search_stats.et_setters]
        return {
            query_targets: gr.update(choices=all_targets),
        }

    return elements, on_data_change, on_stats_change
