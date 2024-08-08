from __future__ import annotations

import logging
from time import time
from typing import Any, List, Tuple

import gradio as gr

from src.db import get_database_connection
from src.db.search import search_files
from src.db.search.utils import pprint_dataclass
from src.types import FileSearchResult
from src.ui.components.multi_view import create_multiview
from src.ui.components.search import create_search_options
from src.ui.components.search.utils import AnyComponent

logger = logging.getLogger(__name__)


def create_search_UI(
    app: gr.Blocks,
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Search") as search_tab:
        n_results = gr.State(0)
        n_pages = gr.State(1)
        with gr.Row():
            with gr.Column(scale=8):
                inputs, build_query = create_search_options(app, search_tab)
            with gr.Column(scale=1):
                with gr.Row():
                    results_str = gr.Markdown("# 0 Results")
                with gr.Row():
                    link = gr.Markdown(
                        "## [View Results in Gallery](/search/tags)"
                    )
                with gr.Row():
                    submit_button = gr.Button("Search", scale=1)

        multi_view = create_multiview(
            select_history=select_history,
            bookmarks_namespace=bookmarks_namespace,
        )

        with gr.Row(elem_classes="pagination-controls"):
            previous_page = gr.Button("Previous Page", scale=1)
            current_page = gr.Slider(
                value=1,
                label="Current Page",
                maximum=1,
                minimum=1,
                step=1,
                scale=2,
            )
            next_page = gr.Button("Next Page", scale=1)

    @n_results.change(inputs=[n_results], outputs=[results_str])
    def on_n_results_change(n_results: int):
        return f"# {n_results} Results"

    def search(
        args: dict[AnyComponent, Any],
        search_action: str | None = None,
    ):
        search_query = build_query(args)
        logger.debug(f"Search action: {search_action}")
        page: int = args[current_page]
        if search_action == "search_button":
            page = 1
        elif search_action == "next_page":
            page += 1
        elif search_action == "previous_page":
            page -= 1
            page = max(1, page)
        elif search_action == "goto_page":
            pass
        if page < 1:
            page = 1

        start = time()
        search_query.order_args.page = page
        search_query.count = True
        search_query.check_path = True

        conn = get_database_connection(write_lock=False)
        logger.debug("Search query:")
        pprint_dataclass(search_query)
        res_list: List[Tuple[FileSearchResult | None, int]] = list(
            search_files(
                conn,
                search_query,
            )
        ) or [(None, 0)]
        conn.close()
        results, total_results = zip(*res_list) if res_list else ([], [0])

        logger.debug(f"Search took {round(time() - start, 3)} seconds")
        total_results = total_results[0]

        logger.debug(f"Found {total_results} images")
        # Calculate the total number of pages, we need to round up
        total_pages = total_results // search_query.order_args.page_size + (
            1 if total_results % search_query.order_args.page_size > 0 else 0
        )
        return {
            multi_view.files: results,
            n_results: total_results,
            current_page: gr.update(value=page, maximum=int(total_pages)),
            n_pages: total_pages,
        }

    search_inputs = {*inputs, n_pages, current_page}
    search_outputs = [multi_view.files, n_results, n_pages, current_page]

    submit_button.click(
        fn=lambda args: search(args, search_action="search_button"),
        inputs=search_inputs,
        outputs=search_outputs,
    )

    current_page.release(
        fn=lambda args: search(args, search_action="goto_page"),
        inputs=search_inputs,
        outputs=search_outputs,
    )

    previous_page.click(
        fn=lambda args: search(args, search_action="previous_page"),
        inputs=search_inputs,
        outputs=search_outputs,
    )

    next_page.click(
        fn=lambda args: search(args, search_action="next_page"),
        inputs=search_inputs,
        outputs=search_outputs,
    )

    def on_total_pages_change(total_pages_val: int, current_page_val: int):
        if total_pages_val == 1:
            return {
                next_page: gr.update(interactive=False),
                previous_page: gr.update(interactive=False),
            }
        if current_page_val == 1:
            return {
                next_page: gr.update(interactive=True),
                previous_page: gr.update(interactive=False),
            }
        if current_page_val == total_pages_val:
            return {
                next_page: gr.update(interactive=False),
                previous_page: gr.update(interactive=True),
            }
        return {
            next_page: gr.update(interactive=True),
            previous_page: gr.update(interactive=True),
        }

    gr.on(
        triggers=[n_pages.change, current_page.change],
        inputs=[n_pages, current_page],
        outputs=[next_page, previous_page],
        fn=on_total_pages_change,
    )
