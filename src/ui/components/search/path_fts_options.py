from dataclasses import asdict
from typing import List, Tuple

import gradio as gr

from src.db.search.types import PathTextFilter, SearchQuery
from src.db.search.utils import from_dict


def create_path_fts_opts(query_state: gr.State):
    with gr.Tab(label="MATCH Filename/Path"):
        with gr.Row():
            path_search = gr.Textbox(
                key="path_search",
                label="MATCH query on filename or path",
                show_copy_button=True,
                scale=2,
            )
            search_path_in = gr.Radio(
                key="search_path_in",
                choices=[
                    ("Full Path", "full_path"),
                    ("Filename", "filename"),
                ],
                interactive=True,
                label="Match",
                value="full_path",
                scale=1,
            )
        gr.on(
            triggers=[
                path_search.input,
                search_path_in.select,
            ],
            fn=on_change_data,
            inputs=[query_state, path_search, search_path_in],
            outputs=[query_state],
        )


def on_change_data(
    query_state_dict: dict,
    path_search: str | None,
    search_path_in: str,
):
    query_state = from_dict(SearchQuery, query_state_dict)
    only_match_filename = search_path_in == "filename"
    if path_search:
        query_state.query.filters.path = PathTextFilter(
            query=path_search, only_match_filename=only_match_filename
        )
    else:
        query_state.query.filters.path = None

    return asdict(query_state)
