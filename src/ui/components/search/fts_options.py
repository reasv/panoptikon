from dataclasses import asdict
from typing import List, Tuple

import gradio as gr

from src.db.search.types import AnyTextFilter, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats


def create_fts_options(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    path_setters = [
        ("Full Path", ("path", "path")),
        ("Filename", ("path", "filename")),
    ]
    with gr.Tab(label="Full Text Search"):
        with gr.Row():
            text_query = gr.Textbox(
                key="text_query_fts",
                label="Match text in any field (supports SQLite MATCH grammar)",
                show_copy_button=True,
                scale=2,
            )
            query_targets = gr.Dropdown(
                key="query_targets_fts",
                choices=path_setters,  # type: ignore
                interactive=True,
                label="(Optional) Restrict query to these targets",
                multiselect=True,
                scale=1,
            )

    gr.on(
        triggers=[text_query.input, query_targets.select],
        fn=on_change_data,
        inputs=[query_state, text_query, query_targets],
        outputs=[query_state],
    )

    def on_stats_change(
        query_state_dict: dict,
        search_stats_dict: dict,
    ):
        query = from_dict(SearchQuery, query_state_dict)
        search_stats = from_dict(SearchStats, search_stats_dict)
        text_setters = [
            (name, ("text", setter_id))
            for name, setter_id in search_stats.et_setters
        ]
        all_targets = path_setters + text_setters
        return {
            query_state: asdict(query),
            query_targets: gr.update(choices=all_targets),
        }

    gr.on(
        triggers=[search_stats_state.change],
        fn=on_stats_change,
        inputs=[query_state, search_stats_state],
        outputs=[query_state, query_targets],
    )


def on_change_data(
    query_state_dict: dict,
    text_query: str | None,
    query_targets: List[Tuple[str, str]] | None,
):
    query_state = from_dict(SearchQuery, query_state_dict)

    if text_query:
        query_state.query.filters.any_text = AnyTextFilter(
            query=text_query, targets=query_targets or []
        )
    else:
        query_state.query.filters.any_text = None

    return asdict(query_state)
