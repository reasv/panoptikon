from typing import List, Tuple

import gradio as gr

from src.db.search.types import SearchQuery
from src.ui.components.search.bookmarks import create_bookmark_search_opts
from src.ui.components.search.extracted_text_fts_options import (
    create_extracted_text_fts_opts,
)
from src.ui.components.search.fts_options import create_fts_options
from src.ui.components.search.path_fts_options import create_path_fts_opts
from src.ui.components.search.tag_options import create_tags_opts
from src.ui.components.search.vector_options import create_vector_search_opts


def create_search_options():
    query_state = gr.State(SearchQuery())
    setters_state = gr.State([])
    tag_namespaces_state = gr.State([])
    bookmark_namespaces_state = gr.State([])

    @gr.render(
        inputs=[setters_state, tag_namespaces_state, bookmark_namespaces_state]
    )
    def render_options(
        setters: List[Tuple[str, str]],
        tag_namespaces: List[Tuple[str, str]],
        bookmark_namespaces: List[Tuple[str, str]],
    ):
        extracted_text_setters = [
            (f"{model_type}|{setter_id}", (model_type, setter_id))
            for model_type, setter_id in setters
            if model_type == "text"
        ]
        tag_setters = [
            setter_id
            for model_type, setter_id in setters
            if model_type == "tags"
        ]
        with gr.Column(scale=10):
            with gr.Tabs():
                with gr.Tab(label="Options"):
                    with gr.Group():
                        with gr.Row():
                            restrict_to_paths = gr.Dropdown(
                                label="Restrict search to paths starting with",
                                choices=[],
                                allow_custom_value=True,
                                multiselect=True,
                                scale=2,
                            )
                            allowed_item_type_prefixes = gr.Dropdown(
                                label="Restrict search to these MIME types",
                                choices=[],
                                allow_custom_value=True,
                                multiselect=True,
                                value=None,
                                scale=2,
                            )
                            max_results_per_page = gr.Slider(
                                minimum=0,
                                maximum=500,
                                value=10,
                                step=1,
                                label="Results per page (0 for max)",
                                scale=2,
                            )
                            order_by = gr.Radio(
                                choices=["path", "last_modified"],
                                label="Order by",
                                value="last_modified",
                                scale=2,
                            )
                            order = gr.Radio(
                                choices=["asc", "desc", "default"],
                                label="Order",
                                value="default",
                                scale=2,
                            )

                create_bookmark_search_opts(query_state, bookmark_namespaces)
                create_vector_search_opts(setters)
                create_fts_options(extracted_text_setters)
                create_tags_opts(tag_namespaces, tag_setters)
                create_path_fts_opts()
                create_extracted_text_fts_opts(extracted_text_setters)

    return query_state
