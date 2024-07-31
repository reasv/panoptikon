from dataclasses import asdict
from typing import List

import gradio as gr

from src.db.search.types import BookmarksFilter, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats


def create_bookmark_search_opts(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    with gr.Tab(label="Search in Bookmarks"):
        with gr.Row():
            enable = gr.Checkbox(
                key="enable_bookmarks",
                label="Restrict search to bookmarked items",
                interactive=True,
                value=False,
                scale=1,
            )
            in_namespaces = gr.Dropdown(
                key="bookmark_namespaces",
                choices=[],
                interactive=True,
                label="Restrict to these namespaces",
                multiselect=True,
                scale=5,
            )
    gr.on(
        triggers=[enable.input, in_namespaces.select],
        fn=on_change_data,
        inputs=[query_state, enable, in_namespaces],
        outputs=[query_state],
    )

    def on_stats_change(
        query_state_dict: dict,
        search_stats_dict: dict,
    ):
        search_stats = from_dict(SearchStats, search_stats_dict)

        return {
            in_namespaces: gr.update(choices=search_stats.bookmark_namespaces),
        }

    gr.on(
        triggers=[search_stats_state.change],
        fn=on_stats_change,
        inputs=[query_state, search_stats_state],
        outputs=[in_namespaces],
    )


def on_change_data(
    query_state_dict: dict, enable: bool, in_namespaces: List[str] | None
):
    query_state = from_dict(SearchQuery, query_state_dict)
    if enable:
        query_state.query.filters.bookmarks = BookmarksFilter(
            restrict_to_bookmarks=True,
            namespaces=in_namespaces or [],
        )

    else:
        query_state.query.filters.bookmarks = None
    return asdict(query_state)
