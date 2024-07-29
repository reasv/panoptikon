from typing import List

import gradio as gr

from src.db.search.types import BookmarksFilter, SearchQuery


def create_bookmark_search_opts(query_state: gr.State, namespaces: List[str]):
    with gr.Tab(label="Search in Bookmarks"):
        with gr.Row():
            enable = gr.Checkbox(
                label="Restrict search to bookmarked items",
                interactive=True,
                value=False,
                scale=1,
            )
            in_namespaces = gr.Dropdown(
                choices=namespaces,
                interactive=True,
                label="Restrict to these namespaces",
                multiselect=True,
                scale=3,
            )
    gr.on(
        triggers=[enable.change, in_namespaces.change],
        inputs=[query_state, enable, in_namespaces],
        fn=on_change_data,
    )


def on_change_data(
    query_state: SearchQuery, enable: bool, in_namespaces: List[str] | None
):
    if enable:
        query_state.query.filters.bookmarks = BookmarksFilter(
            restrict_to_bookmarks=True,
            namespaces=in_namespaces or [],
        )
    else:
        query_state.query.filters.bookmarks = None
    return query_state
