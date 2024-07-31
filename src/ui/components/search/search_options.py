from dataclasses import asdict
from typing import List, Literal, Tuple

import gradio as gr
from sqlalchemy import all_

from src.db import get_database_connection
from src.db.bookmarks import get_all_bookmark_namespaces
from src.db.extracted_text import get_text_stats
from src.db.extraction_log import get_existing_setters
from src.db.files import get_all_mime_types
from src.db.folders import get_folders_from_database
from src.db.search.types import FileFilters, SearchQuery
from src.db.tags import get_all_tag_namespaces
from src.types import ExtractedTextStats, SearchStats
from src.ui.components.search.basic_options import create_basic_search_opts
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
    search_stats_state = gr.State(asdict(SearchStats()))

    gr.on(
        triggers=[search_tab.select, app.load],
        fn=on_tab_load,
        outputs=[search_stats_state],
        api_name=False,
    )

    with gr.Tabs():
        create_basic_search_opts(query_state, search_stats_state)
        create_bookmark_search_opts(query_state, search_stats_state)
        create_vector_search_opts(query_state, search_stats_state)
        create_fts_options(query_state, search_stats_state)
        create_tags_opts(query_state, search_stats_state)
        create_path_fts_opts(query_state)
        create_extracted_text_fts_opts(query_state, search_stats_state)

    return query_state


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
