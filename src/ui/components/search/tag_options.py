from dataclasses import asdict

import gradio as gr

from src.data_extractors.utils import get_threshold_from_env
from src.db.search.types import QueryParams, QueryTagFilters, SearchQuery
from src.db.search.utils import from_dict
from src.utils import parse_tags


def create_tags_opts(
    query_state: gr.State, namespaces: list[str], tag_setters: list[str]
):
    if tag_setters:
        with gr.Tab(label="Tag Filters"):
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
                    chosen_tag_setters = gr.Dropdown(
                        label="Only search tags set by model(s)",
                        multiselect=True,
                        choices=tag_setters,
                        value=[],
                        scale=2,
                    )
                    all_setters_required = gr.Checkbox(
                        label="Require each tag to have been set by ALL selected models",
                        scale=1,
                    )
                    tag_namespace_prefixes = gr.Dropdown(
                        label="Tag Namespace Prefixes",
                        choices=namespaces,
                        allow_custom_value=True,
                        multiselect=True,
                        value=None,
                        scale=2,
                    )
        gr.on(
            triggers=[
                tag_input.input,
                min_confidence.release,
                chosen_tag_setters.select,
                all_setters_required.input,
                tag_namespace_prefixes.select,
            ],
            fn=on_change_data,
            inputs=[
                query_state,
                tag_input,
                min_confidence,
                chosen_tag_setters,
                all_setters_required,
                tag_namespace_prefixes,
            ],
            outputs=[query_state],
        )


def on_change_data(
    query_state_dict: dict,
    tag_input: str | None,
    min_confidence: float | None,
    chosen_tag_setters: list[str] | None,
    all_setters_required: bool,
    tag_namespace_prefixes: list[str] | None,
):
    query_state = from_dict(SearchQuery, query_state_dict)
    if tag_input:
        pos_match_all, pos_match_any, neg_match_any, neg_match_all = parse_tags(
            tag_input
        )
        minimum_confidence_threshold = get_threshold_from_env()
        if not min_confidence or min_confidence <= minimum_confidence_threshold:
            min_confidence = None

        query_state.query.tags = QueryTagFilters(
            pos_match_all=pos_match_all,
            pos_match_any=pos_match_any,
            neg_match_any=neg_match_any,
            neg_match_all=neg_match_all,
            min_confidence=min_confidence,
            setters=chosen_tag_setters or [],
            all_setters_required=all_setters_required,
            namespaces=tag_namespace_prefixes or [],
        )
    else:
        query_state.query.tags = QueryTagFilters()
    return asdict(query_state)
