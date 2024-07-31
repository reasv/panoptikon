from typing import Any, Dict, List

import gradio as gr

from src.db.search.types import BookmarksFilter, SearchQuery
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent


def create_bookmark_search_opts(
    query_state: gr.State,
):
    elements: List[AnyComponent] = []
    with gr.Tab(label="Search in Bookmarks"):
        with gr.Row():
            enable = gr.Checkbox(
                key="enable_bookmarks",
                label="Restrict search to bookmarked items",
                interactive=True,
                value=False,
                scale=1,
            )
            elements.append(enable)
            in_namespaces = gr.Dropdown(
                key="bookmark_namespaces",
                choices=[],
                interactive=True,
                label="Restrict to these namespaces",
                multiselect=True,
                scale=5,
            )
            elements.append(in_namespaces)

    def on_data_change(
        query: SearchQuery,
        args: dict[AnyComponent, Any],
        final_query_build: bool = False,
    ) -> SearchQuery:
        enable_val: bool = args[enable]
        in_namespaces_val: List[str] = args[in_namespaces]
        if enable_val:
            query.query.filters.bookmarks = BookmarksFilter(
                restrict_to_bookmarks=True,
                namespaces=in_namespaces_val or [],
            )
        else:
            query.query.filters.bookmarks = None
        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:
        return {
            in_namespaces: gr.update(choices=search_stats.bookmark_namespaces),
        }

    return elements, on_data_change, on_stats_change
