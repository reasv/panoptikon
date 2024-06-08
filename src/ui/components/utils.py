from __future__ import annotations
from typing import List

import gradio as gr

from src.db import get_all_tags_for_item_name_confidence, get_database_connection
from src.ui.components.history_dict import HistoryDict

def process_image_selection(image_data: dict, select_history: List[str]):
    select_history.append(image_data)
    # Get the path of the image
    pathstr = image_data['path']

    # Get the tags for the image
    sha256 = image_data['sha256']
    conn = get_database_connection()
    tags = { t[0]: t[1] for t in get_all_tags_for_item_name_confidence(conn, sha256)}
    conn.close()
    # Tags in the format "tag1, tag2, tag3"
    text = ", ".join(tags.keys())
    return gr.update(value=pathstr), pathstr, gr.update(interactive=True), gr.update(interactive=True), tags, text, select_history

def add_bookmark(bookmarks_state: HistoryDict, selected_image_sha256: str, selected_image_path: dict):
    bookmarks_state = HistoryDict(bookmarks_state)
    bookmarks_state.add(selected_image_sha256, selected_image_path)
    return bookmarks_state