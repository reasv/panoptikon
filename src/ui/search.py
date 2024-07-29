from __future__ import annotations

import gradio as gr

from src.db.search.types import SearchQuery
from src.db.search.utils import from_dict, pprint_dataclass
from src.ui.components.multi_view import create_multiview
from src.ui.components.search import create_search_options
from src.ui.run_search import search


def create_search_UI(
    app: gr.Blocks,
    select_history: gr.State | None = None,
    bookmarks_namespace: gr.State | None = None,
):
    with gr.TabItem(label="Search") as search_tab:
        n_results = gr.State(0)
        with gr.Row():
            with gr.Column(scale=8):
                query_state = create_search_options(app, search_tab)
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

    query_state.change(
        fn=on_query_change,
        inputs=[query_state],
    )

    n_results.change(
        fn=on_n_results_change,
        inputs=[n_results],
        outputs=[results_str],
    )

    search_inputs = [query_state, current_page]
    search_outputs = [multi_view.files, n_results, current_page, link]
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


def on_n_results_change(n_results: int):
    return f"# {n_results} Results"


def on_query_change(query_state: dict):
    pprint_dataclass(from_dict(SearchQuery, query_state))


def on_tag_select(selectData: gr.SelectData):
    return selectData.value

    # onload_outputs = [
    #     restrict_to_paths,
    #     require_text_extractors,
    #     vec_targets,
    #     tag_setters,
    #     tag_namespace_prefixes,
    #     restrict_to_bk_namespaces,
    #     allowed_item_type_prefixes,
    #     restrict_to_query_types,
    #     clip_model,
    # ]

    # search_tab.select(
    #     fn=on_tab_load,
    #     outputs=onload_outputs,
    # )
    # app.load(
    #     fn=on_tab_load,
    #     outputs=onload_outputs,
    # )

    # search_inputs = [
    #     tag_input,
    #     min_confidence,
    #     max_results_per_page,
    #     restrict_to_paths,
    #     current_page,
    #     order_by,
    #     order,
    #     tag_setters,
    #     all_setters_required,
    #     allowed_item_type_prefixes,
    #     tag_namespace_prefixes,
    #     path_search,
    #     search_path_in,
    #     path_order_by_rank,
    #     extracted_text_search,
    #     require_text_extractors,
    #     extracted_text_order_by_rank,
    #     restrict_search_to_bookmarks,
    #     restrict_to_bk_namespaces,
    #     order_by_time_added_bk,
    #     any_text_search,
    #     restrict_to_query_types,
    #     order_by_any_text_rank,
    #     vec_text_search,
    #     vec_targets,
    #     clip_model,
    #     clip_text_search,
    #     clip_image_search,
    # ]

    # search_outputs = [multi_view.files, number_of_results, current_page, link]

    # action_search_button = gr.State("search_button")
    # action_next_page = gr.State("next_page")
    # action_previous_page = gr.State("previous_page")
    # action_goto_page = gr.State("goto_page")

    # submit_button.click(
    #     fn=search,
    #     inputs=[*search_inputs, action_search_button],
    #     outputs=search_outputs,
    # )

    # current_page.release(
    #     fn=search,
    #     inputs=[*search_inputs, action_goto_page],
    #     outputs=search_outputs,
    # )

    # previous_page.click(
    #     fn=search,
    #     inputs=[*search_inputs, action_previous_page],
    #     outputs=search_outputs,
    # )

    # next_page.click(
    #     fn=search,
    #     inputs=[*search_inputs, action_next_page],
    #     outputs=search_outputs,
    # )

    # multi_view.list_view.tag_list.select(fn=on_tag_select, outputs=[tag_input])

    # def switch_vec_query_type(value: str):
    #     update_list = [{"visible": False} for _ in range(5)]
    #     (
    #         vec_text_update,
    #         vec_targets_update,
    #         clip_text_update,
    #         clip_image_update,
    #         clip_model_update,
    #     ) = update_list
    #     if value == "CLIP Text Query":
    #         clip_text_update["visible"] = True
    #         clip_model_update["visible"] = True
    #     elif value == "CLIP Reverse Image Search":
    #         clip_image_update["visible"] = True
    #         clip_model_update["visible"] = True
    #     elif value == "Text Embeddings Search":
    #         vec_text_update["visible"] = True
    #         vec_targets_update["visible"] = True
    #     return [gr.update(**update) for update in update_list]

    # vec_query_type.change(
    #     fn=switch_vec_query_type,
    #     inputs=[vec_query_type],
    #     outputs=[
    #         vec_text_search,
    #         vec_targets,
    #         clip_text_search,
    #         clip_image_search,
    #         clip_model,
    #     ],
    # )
