from dataclasses import asdict
from typing import Any, Callable, Dict, List

import gradio as gr
from gradio.components import Component

from src.db.search.types import SearchQuery
from src.db.search.utils import from_dict
from src.types import SearchStats

AnyComponent = Component | gr.Tab


def get_triggers(components: list[AnyComponent]) -> list[Callable]:
    triggers = []
    for component in components:
        if isinstance(component, gr.Slider):
            triggers.append(component.release)
        elif isinstance(component, gr.Checkbox):
            triggers.append(component.input)
        elif isinstance(component, gr.Dropdown):
            triggers.append(component.select)
        elif isinstance(component, gr.Radio):
            triggers.append(component.select)
        elif isinstance(component, gr.Textbox):
            triggers.append(component.input)
    return triggers


def filter_inputs(inputs: list[AnyComponent]) -> list[Component]:
    return [input for input in inputs if not isinstance(input, gr.Tab)]


def bind_event_listeners(
    query_state: gr.State,
    search_stats_state: gr.State,
    elements: List[AnyComponent],
    on_data_change: Callable[
        [SearchQuery, Dict[AnyComponent, Any]], SearchQuery
    ],
    on_stats_change: (
        Callable[[SearchQuery, SearchStats], Dict[AnyComponent, Any]] | None
    ) = None,
):

    def on_data_change_wrapper(args: dict[AnyComponent, Any]) -> dict[str, Any]:
        query = from_dict(SearchQuery, args[query_state])
        return asdict(on_data_change(query, args))

    gr.on(
        triggers=get_triggers(elements),
        fn=on_data_change_wrapper,
        inputs={query_state, *filter_inputs(elements)},
        outputs=[query_state],
    )

    if on_stats_change is None:
        return

    def on_stats_change_wrapper(
        query_state_dict: dict,
        search_stats_dict: dict,
    ):
        query = from_dict(SearchQuery, query_state_dict)
        search_stats = from_dict(SearchStats, search_stats_dict)
        return on_stats_change(query, search_stats)

    gr.on(
        triggers=[search_stats_state.change],
        fn=on_stats_change_wrapper,
        inputs=[query_state, search_stats_state],
        outputs=[query_state, *elements],
    )
