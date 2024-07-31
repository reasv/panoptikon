from typing import Any, Dict, List, Tuple

import gradio as gr

from src.db.search.types import AnyTextFilter, SearchQuery
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent


def create_fts_options(
    query_state: gr.State,
):
    path_setters = [
        ("Full Path", ("path", "path")),
        ("Filename", ("path", "filename")),
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
        query_targets_val: List[Tuple[str, str]] | None = args[query_targets]

        if text_query_val:
            query.query.filters.any_text = AnyTextFilter(
                query=text_query_val, targets=query_targets_val or []
            )
        else:
            query.query.filters.any_text = None

        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:
        text_setters = [
            (name, ("text", setter_id))
            for name, setter_id in search_stats.et_setters
        ]
        all_targets = path_setters + text_setters
        return {
            query_targets: gr.update(choices=all_targets),
        }

    return elements, on_data_change, on_stats_change
