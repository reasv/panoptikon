from __future__ import annotations
from typing import List

import gradio as gr

from src.db import get_all_tags_for_item_name_confidence, get_database_connection
from src.ui.components.history_dict import HistoryDict

def add_bookmark(bookmarks_state: HistoryDict, selected_image_sha256: str, selected_image_path: dict):
    bookmarks_state = HistoryDict(bookmarks_state)
    bookmarks_state.add(selected_image_sha256, selected_image_path)
    return bookmarks_state