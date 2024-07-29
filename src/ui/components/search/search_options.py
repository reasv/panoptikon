from dataclasses import asdict
from typing import List, Tuple

import gradio as gr

from src.db import get_database_connection
from src.db.bookmarks import get_all_bookmark_namespaces
from src.db.extraction_log import get_existing_setters
from src.db.files import get_all_mime_types
from src.db.folders import get_folders_from_database
from src.db.search.types import SearchQuery
from src.db.tags import get_all_tag_namespaces
from src.ui.components.search.bookmarks import create_bookmark_search_opts
from src.ui.components.search.extracted_text_fts_options import (
    create_extracted_text_fts_opts,
)
from src.ui.components.search.fts_options import create_fts_options
from src.ui.components.search.path_fts_options import create_path_fts_opts
from src.ui.components.search.tag_options import create_tags_opts
from src.ui.components.search.vector_options import create_vector_search_opts


def create_search_options(app: gr.Blocks, search_tab: gr.Tab):
    query_state = gr.State(asdict(SearchQuery()))
    setters_state = gr.State([])
    tag_namespaces_state = gr.State([])
    bookmark_namespaces_state = gr.State([])
    file_types_state = gr.State([])
    folders_state = gr.State([])
    gr.on(
        triggers=[search_tab.select, app.load],
        fn=on_tab_load,
        outputs=[
            setters_state,
            tag_namespaces_state,
            bookmark_namespaces_state,
            file_types_state,
            folders_state,
        ],
        api_name=False,
    )

    @gr.render(
        inputs=[
            setters_state,
            tag_namespaces_state,
            bookmark_namespaces_state,
            file_types_state,
            folders_state,
        ]
    )
    def render_options(
        setters: List[Tuple[str, str]],
        tag_namespaces: List[str],
        bookmark_namespaces: List[str],
        file_types: List[str],
        folders: List[str],
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
        with gr.Tabs():
            with gr.Tab(label="Options"):
                with gr.Group():
                    with gr.Row():
                        use_paths = gr.Dropdown(
                            label="Restrict search to paths starting with",
                            choices=folders,
                            allow_custom_value=True,
                            multiselect=True,
                            scale=2,
                        )
                        use_file_types = gr.Dropdown(
                            label="Restrict search to these MIME types",
                            choices=file_types,
                            allow_custom_value=True,
                            multiselect=True,
                            value=None,
                            scale=2,
                        )
                        res_per_page = gr.Slider(
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


def on_tab_load():
    conn = get_database_connection(write_lock=False)
    setters = get_existing_setters(conn)
    bookmark_namespaces = get_all_bookmark_namespaces(conn)
    file_types = get_all_mime_types(conn)
    tag_namespaces = get_all_tag_namespaces(conn)
    folders = get_folders_from_database(conn)
    conn.close()

    return (
        setters,
        tag_namespaces,
        bookmark_namespaces,
        file_types,
        folders,
    )
