from dataclasses import asdict
from typing import Any, Dict, List, Tuple

import gradio as gr

from src.db.search.types import AnyTextFilter, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent, bind_event_listeners


def create_fts_options(
    query_state: gr.State,
    search_stats_state: gr.State,
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

    def on_change_data(
        query: SearchQuery, args: dict[AnyComponent, Any]
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
            query_state: asdict(query),
            query_targets: gr.update(choices=all_targets),
        }

    bind_event_listeners(
        query_state,
        search_stats_state,
        elements,
        on_change_data,
        on_stats_change,
    )

    return elements, on_change_data
