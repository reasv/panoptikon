from dataclasses import asdict
from typing import Any, List

import gradio as gr

from src.data_extractors.utils import get_threshold_from_env
from src.db.search.types import QueryTagFilters, SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats
from src.utils import parse_tags


def create_tags_opts(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    elements: List[Any] = []
    with gr.Tab(label="Tag Filters") as tab:
        elements.append(tab)
        with gr.Group():
            with gr.Row():
                tag_input = gr.Textbox(
                    key="tag_input",
                    label="Enter tags separated by commas",
                    show_copy_button=True,
                    scale=3,
                )
                elements.append(tag_input)
                min_confidence = gr.Slider(
                    key="min_confidence_tags",
                    minimum=0.05,
                    maximum=1,
                    value=get_threshold_from_env(),
                    step=0.05,
                    label="Min. Confidence Level for Tags",
                    scale=2,
                )
                elements.append(min_confidence)
                chosen_tag_setters = gr.Dropdown(
                    key="chosen_tag_setters",
                    label="Only search tags set by model(s)",
                    multiselect=True,
                    choices=[],
                    scale=2,
                )
                elements.append(chosen_tag_setters)
                all_setters_required = gr.Checkbox(
                    key="all_setters_required",
                    label="Require each tag to have been set by ALL selected models",
                    scale=1,
                )
                elements.append(all_setters_required)
                tag_namespace_prefixes = gr.Dropdown(
                    key="tag_namespace_prefixes",
                    label="Tag Namespace Prefixes",
                    choices=[],
                    allow_custom_value=True,
                    multiselect=True,
                    value=None,
                    scale=2,
                )
                elements.append(tag_namespace_prefixes)

        def on_stats_change(
            query_state_dict: dict,
            search_stats_dict: dict,
        ):
            query = from_dict(SearchQuery, query_state_dict)
            search_stats = from_dict(SearchStats, search_stats_dict)
            tags_available = bool(search_stats.tag_setters)
            if not tags_available:
                query.query.tags = QueryTagFilters()
            return {
                query_state: asdict(query),
                tab: gr.Tab(visible=tags_available),
                chosen_tag_setters: gr.Dropdown(
                    choices=search_stats.tag_setters
                ),
                tag_namespace_prefixes: gr.Dropdown(
                    choices=search_stats.tag_namespaces
                ),
            }

        gr.on(
            triggers=[search_stats_state.change],
            fn=on_stats_change,
            inputs=[query_state, search_stats_state],
            outputs=[query_state, *elements],
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
