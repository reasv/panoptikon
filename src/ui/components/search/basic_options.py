from dataclasses import asdict
from typing import Any, Dict, List, Literal

import gradio as gr

from src.db.search.types import FileFilters, OrderByType, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent, bind_event_listeners


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

    def on_data_change(query: SearchQuery, args: dict[AnyComponent, Any]):
        use_paths_val: List[str] | None = args[use_paths]
        use_file_types_val: List[str] | None = args[use_file_types]
        res_per_page_val: int = args[res_per_page]
        order_by_val: OrderByType = args[order_by]
        order_val: Literal["asc", "desc", "default"] = args[order]

        if use_paths_val or use_file_types_val:
            use_paths_val = (
                [path.strip() for path in use_paths_val]
                if use_paths_val
                else None
            )
            use_file_types_val = (
                [file_type.strip() for file_type in use_file_types_val]
                if use_file_types_val
                else None
            )
            query.query.filters.files = FileFilters(
                item_types=use_file_types_val or [],
                include_path_prefixes=use_paths_val or [],
            )
        else:
            query.query.filters.files = None

        query.order_args.page_size = res_per_page_val
        query.order_args.order = None if order_val == "default" else order_val
        query.order_args.order_by = order_by_val

        return query

    def on_stats_change(
        query: SearchQuery,
        search_stats: SearchStats,
    ) -> Dict[AnyComponent, Any]:
        return {
            query_state: asdict(query),
            use_paths: gr.update(choices=search_stats.folders),
            use_file_types: gr.update(choices=search_stats.file_types),
        }

    bind_event_listeners(
        query_state,
        search_stats_state,
        elements,
        on_data_change,
        on_stats_change,
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

    return elements, on_data_change
