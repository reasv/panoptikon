from dataclasses import asdict
from typing import Any, Callable, Dict, List

import gradio as gr

from src.db import get_database_connection
from src.db.bookmarks import get_all_bookmark_namespaces
from src.db.extracted_text import get_text_stats
from src.db.extraction_log import get_existing_setters
from src.db.files import get_all_mime_types
from src.db.folders import get_folders_from_database
from src.db.search.types import SearchQuery
from src.db.search.utils import from_dict, pprint_dataclass
from src.db.tags import get_all_tag_namespaces
from src.types import SearchStats
from src.ui.components.search.any_fts import create_fts_options
from src.ui.components.search.base import create_basic_search_opts
from src.ui.components.search.bookmarks import create_bookmark_search_opts
from src.ui.components.search.extracted_text_fts import (
    create_extracted_text_fts_opts,
)
from src.ui.components.search.path_fts import create_path_fts_opts
from src.ui.components.search.tags import create_tags_opts
from src.ui.components.search.utils import (
    AnyComponent,
    bind_event_listeners,
    filter_inputs,
)
from src.ui.components.search.vector import create_vector_search_opts


def create_search_options(app: gr.Blocks, search_tab: gr.Tab):
    query_state = gr.State(asdict(SearchQuery()))
    search_stats_state = gr.State(asdict(SearchStats()))

    gr.on(
        triggers=[search_tab.select, app.load],
        fn=on_tab_load,
        outputs=[search_stats_state],
        api_name=False,
    )

    search_option_modules = [
        create_basic_search_opts,
        create_bookmark_search_opts,
        create_vector_search_opts,
        create_fts_options,
        create_tags_opts,
        create_path_fts_opts,
        create_extracted_text_fts_opts,
    ]
    all_inputs: List[AnyComponent] = []
    process_functions: List[
        Callable[[SearchQuery, Dict[AnyComponent, Any], bool], SearchQuery]
    ] = []
    with gr.Tabs():
        for module in search_option_modules:
            inputs, process_inputs, on_stats_change = module(query_state)
            all_inputs.extend(inputs)
            process_functions.append(process_inputs)
            bind_event_listeners(
                query_state,
                search_stats_state,
                inputs,
                process_inputs,
                on_stats_change,
            )
    process_functions.reverse()  # Reverse the order of processing functions
    # This is because the function that controls sorting of results should be the last one to run

    def build_full_query(args: dict[AnyComponent, Any]) -> SearchQuery:
        """Build the full search query object from the search modules' output components"""
        query = SearchQuery()
        for process in process_functions:
            query = process(query, args, True)
        return query

    def on_query_change(query_state: dict):
        pprint_dataclass(from_dict(SearchQuery, query_state))

    query_state.change(
        fn=on_query_change,
        inputs=[query_state],
    )

    return filter_inputs(all_inputs), build_full_query


def on_tab_load():
    conn = get_database_connection(write_lock=False)
    setters = get_existing_setters(conn)
    bookmark_namespaces = get_all_bookmark_namespaces(conn)
    file_types = get_all_mime_types(conn)
    tag_namespaces = get_all_tag_namespaces(conn)
    folders = get_folders_from_database(conn)
    text_stats = get_text_stats(conn)
    conn.close()

    extracted_text_setters = [
        (f"{setter_id}", setter_id)
        for model_type, setter_id in setters
        if model_type == "text"
    ]
    tag_setters = [s for t, s in setters if t == "tags"]

    clip_setters = [s for t, s in setters if t == "clip"]
    te_setters = [s for t, s in setters if t == "text-embedding"]
    stats = SearchStats(
        tag_namespaces=tag_namespaces,
        bookmark_namespaces=bookmark_namespaces,
        all_setters=setters,
        clip_setters=clip_setters,
        te_setters=te_setters,
        file_types=file_types,
        folders=folders,
        et_stats=text_stats,
        tag_setters=tag_setters,
        et_setters=extracted_text_setters,
    )
    return asdict(stats)
