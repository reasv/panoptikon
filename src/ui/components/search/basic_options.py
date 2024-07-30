from dataclasses import asdict
from typing import List, Literal

import gradio as gr

from src.db.search.types import FileFilters, SearchQuery
from src.db.search.utils import from_dict


def create_basic_search_opts(
    query_state: gr.State, folders: List[str], file_types: List[str]
):
    with gr.Tab(label="Options"):
        with gr.Group():
            with gr.Row():
                use_paths = gr.Dropdown(
                    key="use_paths",
                    label="Restrict search to paths starting with",
                    choices=folders,
                    allow_custom_value=True,
                    multiselect=True,
                    scale=2,
                )
                use_file_types = gr.Dropdown(
                    label="Restrict search to these MIME types",
                    key="use_file_types",
                    choices=file_types,
                    allow_custom_value=True,
                    multiselect=True,
                    value=None,
                    scale=2,
                )
                res_per_page = gr.Slider(
                    key="res_per_page",
                    minimum=0,
                    maximum=500,
                    value=10,
                    step=1,
                    label="Results per page (0 for max)",
                    scale=2,
                )
                order_by = gr.Radio(
                    key="orderby",
                    choices=["path", "last_modified"],
                    label="Order by",
                    value="last_modified",
                    scale=2,
                    interactive=True,
                )
                order = gr.Radio(
                    key="order",
                    choices=["asc", "desc", "default"],
                    label="Order",
                    value="default",
                    scale=2,
                )
    gr.on(
        triggers=[
            use_paths.select,
            use_file_types.select,
            res_per_page.release,
            order.select,
        ],
        fn=on_change_data,
        inputs=[
            query_state,
            use_paths,
            use_file_types,
            res_per_page,
            order,
        ],
        outputs=[query_state],
    )


def on_change_data(
    query_state_dict: dict,
    use_paths: List[str] | None,
    use_file_types: List[str] | None,
    res_per_page: int,
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

    return asdict(query_state)
