from dataclasses import asdict
from typing import Any, Dict, List

import gradio as gr

from src.data_extractors.utils import get_threshold_from_env
from src.db.search.types import QueryTagFilters, SearchQuery
from src.types import SearchStats
from src.ui.components.search.utils import AnyComponent, bind_event_listeners
from src.utils import parse_tags


def create_tags_opts(
    query_state: gr.State,
    search_stats_state: gr.State,
):
    elements: List[AnyComponent] = []
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

        def on_data_change(
            query: SearchQuery, args: dict[AnyComponent, Any]
        ) -> SearchQuery:
            tag_input_val: str | None = args[tag_input]
            min_confidence_val: float | None = args[min_confidence]
            chosen_tag_setters_val: list[str] | None = args[chosen_tag_setters]
            all_setters_required_val: bool = args[all_setters_required]
            tag_namespace_prefixes_val: list[str] | None = args[
                tag_namespace_prefixes
            ]

            if tag_input_val:
                pos_match_all, pos_match_any, neg_match_any, neg_match_all = (
                    parse_tags(tag_input_val)
                )
                minimum_confidence_threshold = get_threshold_from_env()
                if (
                    not min_confidence_val
                    or min_confidence_val <= minimum_confidence_threshold
                ):
                    min_confidence_val = None
                query.query.tags = QueryTagFilters(
                    pos_match_all=pos_match_all,
                    pos_match_any=pos_match_any,
                    neg_match_any=neg_match_any,
                    neg_match_all=neg_match_all,
                    min_confidence=min_confidence_val,
                    setters=chosen_tag_setters_val or [],
                    all_setters_required=all_setters_required_val,
                    namespaces=tag_namespace_prefixes_val or [],
                )
            else:
                query.query.tags = QueryTagFilters()
            return query

        def on_stats_change(
            query: SearchQuery,
            search_stats: SearchStats,
        ) -> Dict[AnyComponent, Any]:
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

    bind_event_listeners(
        query_state,
        search_stats_state,
        elements,
        on_data_change,
        on_stats_change,
    )

    return elements, on_data_change
