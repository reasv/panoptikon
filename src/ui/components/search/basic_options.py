from dataclasses import asdict
from typing import Any, List, Literal

import gradio as gr

from src.db.search.types import FileFilters, OrderByType, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats


def create_basic_search_opts(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    elements: List[Any] = []
    default_order_by_choices: List[OrderByType] = ["last_modified", "path"]
    with gr.Tab(label="Options"):
        with gr.Group():
            with gr.Row():
                use_paths = gr.Dropdown(
                    key="use_paths",
                    label="Restrict search to paths starting with",
                    choices=[],
                    allow_custom_value=True,
                    multiselect=True,
                    scale=2,
                )
                elements.append(use_paths)
                use_file_types = gr.Dropdown(
                    label="Restrict search to these MIME types",
                    key="use_file_types",
                    choices=[],
                    allow_custom_value=True,
                    multiselect=True,
                    value=None,
                    scale=2,
                )
                elements.append(use_file_types)
                res_per_page = gr.Slider(
                    key="res_per_page",
                    minimum=0,
                    maximum=500,
                    value=10,
                    step=1,
                    label="Results per page (0 for max)",
                    scale=2,
                )
                elements.append(res_per_page)
                order_by = gr.Radio(
                    key="orderby",
                    choices=default_order_by_choices,  # type: ignore
                    label="Order by",
                    value="last_modified",
                    scale=2,
                    interactive=True,
                )
                elements.append(order_by)
                order = gr.Radio(
                    key="order",
                    choices=["asc", "desc", "default"],
                    label="Order",
                    value="default",
                    scale=2,
                )
                elements.append(order)

    def on_stats_change(
        query_state_dict: dict,
        search_stats_dict: dict,
    ):
        query = from_dict(SearchQuery, query_state_dict)
        search_stats = from_dict(SearchStats, search_stats_dict)

        return {
            query_state: asdict(query),
            use_paths: gr.update(choices=search_stats.folders),
            use_file_types: gr.update(choices=search_stats.file_types),
        }

    gr.on(
        triggers=[search_stats_state.change],
        fn=on_stats_change,
        inputs=[query_state, search_stats_state],
        outputs=[query_state, *elements],
    )

    def on_query_change(query_state_dict: dict, order_by_current: str):
        query = from_dict(SearchQuery, query_state_dict)
        order_by_opts: List[OrderByType] = [x for x in default_order_by_choices]
        vec = False
        if query.query.filters.any_text:
            order_by_opts.append("rank_any_text")
        if query.query.filters.extracted_text:
            order_by_opts.append("rank_fts")
        if query.query.filters.bookmarks:
            order_by_opts.append("time_added")
        if query.query.filters.path:
            order_by_opts.append("rank_path_fts")
        if query.query.filters.extracted_text_embeddings:
            order_by_opts = ["text_vec_distance"]
            vec = True
        if query.query.filters.image_embeddings:
            order_by_opts = ["image_vec_distance"]
            vec = True

        order_by_update = gr.update(choices=order_by_opts)
        if vec:
            query.order_args.order_by = order_by_opts[0]

        if order_by_current not in order_by_opts:
            order_by_update = gr.update(
                choices=order_by_opts, value=order_by_opts[0]
            )

        return {
            order_by: order_by_update,
            query_state: asdict(query),
        }

    query_state.change(
        inputs=[query_state, order_by],
        outputs=[query_state, order_by],
        fn=on_query_change,
    )
    gr.on(
        triggers=[
            use_paths.select,
            use_file_types.select,
            res_per_page.release,
            order_by.select,
            order.select,
        ],
        fn=on_change_data,
        inputs=[
            query_state,
            use_paths,
            use_file_types,
            res_per_page,
            order_by,
            order,
        ],
        outputs=[query_state],
    )


def on_change_data(
    query_state_dict: dict,
    use_paths: List[str] | None,
    use_file_types: List[str] | None,
    res_per_page: int,
    order_by: OrderByType,
    order: Literal["asc", "desc", "default"],
):
    query_state = from_dict(SearchQuery, query_state_dict)

    if use_paths or use_file_types:
        use_paths = [path.strip() for path in use_paths] if use_paths else None
        use_file_types = (
            [file_type.strip() for file_type in use_file_types]
            if use_file_types
            else None
        )
        query_state.query.filters.files = FileFilters(
            item_types=use_file_types or [],
            include_path_prefixes=use_paths or [],
        )
    else:
        query_state.query.filters.files = None

    query_state.order_args.page_size = res_per_page
    query_state.order_args.order = None if order == "default" else order
    query_state.order_args.order_by = order_by

    return asdict(query_state)
