from dataclasses import asdict
from typing import List, Tuple

import gradio as gr

from src.db.search.types import AnyTextFilter, BookmarksFilter, SearchQuery
from src.db.search.utils import from_dict


def create_fts_options(
    query_state: gr.State, text_setters: List[Tuple[str, Tuple[str, str]]]
):
    if text_setters:
        text_setters.extend(
            [
                ("Full Path", ("path", "path")),
                ("Filename", ("path", "filename")),
            ]
        )
        with gr.Tab(label="Full Text Search"):
            with gr.Row():
                text_query = gr.Textbox(
                    label="Match text in any field (supports SQLite MATCH grammar)",
                    value="",
                    show_copy_button=True,
                    scale=2,
                )
                query_targets = gr.Dropdown(
                    choices=text_setters,  # type: ignore
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
