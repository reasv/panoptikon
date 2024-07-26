from __future__ import annotations

import gradio as gr

from src.data_extractors.utils import get_threshold_from_env
from src.db import get_database_connection
from src.db.bookmarks import get_all_bookmark_namespaces
from src.db.extraction_log import get_existing_type_setter_pairs
from src.db.files import get_all_mime_types
from src.db.folders import get_folders_from_database
from src.db.tags import get_all_tag_namespaces
from src.ui.components.multi_view import create_multiview
from src.ui.run_search import search


def on_tab_load():
    conn = get_database_connection(write_lock=False)
    full_setters_list = get_existing_type_setter_pairs(conn)
    bookmark_namespaces = get_all_bookmark_namespaces(conn)
    file_types = get_all_mime_types(conn)
    tag_namespaces = get_all_tag_namespaces(conn)
    folders = get_folders_from_database(conn)
    conn.close()

    extracted_text_setters = [
        (f"{model_type}|{setter_id}", (model_type, setter_id))
        for model_type, setter_id in full_setters_list
        if model_type != "tags" and model_type != "clip"
    ]
    tag_setters = [
        setter_id
        for model_type, setter_id in full_setters_list
        if model_type == "tags"
    ]

    setters_except_tags = [
        (f"{model_type}|{setter_id}", (model_type, setter_id))
        for model_type, setter_id in full_setters_list
        if model_type != "tags"
    ]

    general_text_sources = [
        *extracted_text_setters,
        ("Full Path", ("path", "path")),
        ("Filename", ("path", "filename")),
    ]
    return (
        gr.update(choices=folders),
        gr.update(choices=extracted_text_setters),
        gr.update(choices=extracted_text_setters),
        gr.update(choices=tag_setters),
        gr.update(choices=tag_namespaces),
        gr.update(choices=bookmark_namespaces),
        gr.update(choices=file_types),
        gr.update(choices=general_text_sources),
    )


def on_tag_select(selectData: gr.SelectData):
    return selectData.value


def create_search_UI(
    app: gr.Blocks,
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Tag Search") as search_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                link = gr.Markdown("[View Results in Gallery](/search/tags)")
                number_of_results = gr.Number(
                    value=0,
                    show_label=True,
                    label="Results",
                    interactive=False,
                    scale=0,
                )
                submit_button = gr.Button("Search", scale=0)
                with gr.Column(scale=10):
                    with gr.Tabs():
                        with gr.Tab(label="Options"):
                            with gr.Group():
                                with gr.Row():
                                    tag_input = gr.Textbox(
                                        label="Enter tags separated by commas",
                                        value="",
                                        show_copy_button=True,
                                        scale=3,
                                    )
                                    min_confidence = gr.Slider(
                                        minimum=0.05,
                                        maximum=1,
                                        value=get_threshold_from_env(),
                                        step=0.05,
                                        label="Min. Confidence Level for Tags",
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
                                    restrict_to_paths = gr.Dropdown(
                                        label="Restrict search to paths starting with",
                                        choices=[],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        scale=2,
                                    )
                                    order_by = gr.Radio(
                                        choices=["path", "last_modified"],
                                        label="Order by",
                                        value="last_modified",
                                        scale=2,
                                    )
                        with gr.Tab(label="Advanced Options"):
                            with gr.Group():
                                with gr.Row():
                                    order = gr.Radio(
                                        choices=["asc", "desc", "default"],
                                        label="Order",
                                        value="default",
                                        scale=2,
                                    )
                                    tag_setters = gr.Dropdown(
                                        label="Only search tags set by model(s)",
                                        multiselect=True,
                                        choices=[],
                                        value=[],
                                        scale=2,
                                    )
                                    all_setters_required = gr.Checkbox(
                                        label="Require ALL selected models to have set each tag",
                                        scale=1,
                                    )
                                    allowed_item_type_prefixes = gr.Dropdown(
                                        label="Item MimeType Prefixes",
                                        choices=[],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        value=None,
                                        scale=2,
                                    )
                                    tag_namespace_prefixes = gr.Dropdown(
                                        label="Tag Namespace Prefix",
                                        choices=[],
                                        allow_custom_value=True,
                                        multiselect=True,
                                        value=None,
                                        scale=2,
                                    )
                        with gr.Tab(label="Filename & Path Search"):
                            with gr.Row():
                                path_search = gr.Textbox(
                                    label="SQL MATCH query on filename or path",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                search_path_in = gr.Radio(
                                    choices=[
                                        ("Full Path", "full_path"),
                                        ("Filename", "filename"),
                                    ],
                                    interactive=True,
                                    label="Search in",
                                    value="full_path",
                                    scale=1,
                                )
                                path_order_by_rank = gr.Checkbox(
                                    label="Order results by relevance if this query is present",
                                    interactive=True,
                                    value=True,
                                    scale=1,
                                )
                        with gr.Tab(label="Extracted Text Search"):
                            with gr.Row():
                                extracted_text_search = gr.Textbox(
                                    label="SQL MATCH query on text exctracted by OCR/Whisper",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                require_text_extractors = gr.Dropdown(
                                    choices=[],
                                    interactive=True,
                                    label="Only Search In Text From These Sources",
                                    multiselect=True,
                                    scale=1,
                                )
                                extracted_text_order_by_rank = gr.Checkbox(
                                    label="Order results by relevance if this query is present",
                                    interactive=True,
                                    value=True,
                                    scale=1,
                                )
                        with gr.Tab(label="Search in Bookmarks"):
                            with gr.Row():
                                restrict_search_to_bookmarks = gr.Checkbox(
                                    label="Restrict search to bookmarked items",
                                    interactive=True,
                                    value=False,
                                    scale=1,
                                )
                                restrict_to_bk_namespaces = gr.Dropdown(
                                    choices=[],
                                    interactive=True,
                                    label="Restrict to these namespaces",
                                    multiselect=True,
                                    scale=1,
                                )
                                order_by_time_added_bk = gr.Checkbox(
                                    label="Order by Time Added",
                                    interactive=True,
                                    value=False,
                                    scale=1,
                                )
                        with gr.Tab(label="Text Query"):
                            with gr.Row():
                                any_text_search = gr.Textbox(
                                    label="General Text Query",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                restrict_to_query_types = gr.Dropdown(
                                    choices=[],
                                    interactive=True,
                                    label="Restrict query to these targets",
                                    multiselect=True,
                                    scale=1,
                                )
                                order_by_any_text_rank = gr.Checkbox(
                                    label="Order by relevance if this query is present",
                                    interactive=True,
                                    value=False,
                                    scale=1,
                                )
                        with gr.Tab(label="Semantic Text Search"):
                            with gr.Row():
                                vec_text_search = gr.Textbox(
                                    label="Semantic Text Query",
                                    value="",
                                    show_copy_button=True,
                                    scale=2,
                                )
                                vec_targets = gr.Dropdown(
                                    choices=[],
                                    interactive=True,
                                    label="Restrict query to these targets",
                                    multiselect=True,
                                    scale=1,
                                )

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

    onload_outputs = [
        restrict_to_paths,
        require_text_extractors,
        vec_targets,
        tag_setters,
        tag_namespace_prefixes,
        restrict_to_bk_namespaces,
        allowed_item_type_prefixes,
        restrict_to_query_types,
    ]

    search_tab.select(
        fn=on_tab_load,
        outputs=onload_outputs,
    )
    app.load(
        fn=on_tab_load,
        outputs=onload_outputs,
    )

    search_inputs = [
        tag_input,
        min_confidence,
        max_results_per_page,
        restrict_to_paths,
        current_page,
        order_by,
        order,
        tag_setters,
        all_setters_required,
        allowed_item_type_prefixes,
        tag_namespace_prefixes,
        path_search,
        search_path_in,
        path_order_by_rank,
        extracted_text_search,
        require_text_extractors,
        extracted_text_order_by_rank,
        restrict_search_to_bookmarks,
        restrict_to_bk_namespaces,
        order_by_time_added_bk,
        any_text_search,
        restrict_to_query_types,
        order_by_any_text_rank,
        vec_text_search,
        vec_targets,
    ]

    search_outputs = [multi_view.files, number_of_results, current_page, link]

    action_search_button = gr.State("search_button")
    action_next_page = gr.State("next_page")
    action_previous_page = gr.State("previous_page")
    action_goto_page = gr.State("goto_page")

    submit_button.click(
        fn=search,
        inputs=[*search_inputs, action_search_button],
        outputs=search_outputs,
    )

    current_page.release(
        fn=search,
        inputs=[*search_inputs, action_goto_page],
        outputs=search_outputs,
    )

    previous_page.click(
        fn=search,
        inputs=[*search_inputs, action_previous_page],
        outputs=search_outputs,
    )

    next_page.click(
        fn=search,
        inputs=[*search_inputs, action_next_page],
        outputs=search_outputs,
    )

    multi_view.list_view.tag_list.select(fn=on_tag_select, outputs=[tag_input])
