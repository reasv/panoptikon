from __future__ import annotations
from typing import List
import json

import gradio as gr

from src.db import get_all_tags_for_item_name_confidence, get_database_connection
from src.ui.components.history_dict import HistoryDict

def toggle_bookmark(bookmarks_state: HistoryDict, selected_image_sha256: str, selected_image_path: dict):
    bookmarks_state = HistoryDict(bookmarks_state)
    # Remove the old bookmark if it exists
    if selected_image_sha256 in bookmarks_state:
        bookmarks_state.remove(selected_image_sha256)
        print("Bookmark deleted")
    else:
        bookmarks_state.add(selected_image_sha256, selected_image_path)
        print("Bookmark added")
    return bookmarks_state, on_selected_image_get_bookmark_state(bookmarks_state, selected_image_sha256)

def on_selected_image_get_bookmark_state(bookmarks_state: HistoryDict, sha256: str):
    is_bookmarked = sha256 in bookmarks_state
    # If the image is bookmarked, we want to show the "Remove Bookmark" button
    return gr.update(value="Remove Bookmark" if is_bookmarked else "Bookmark")

def save_bookmarks(bookmarks: HistoryDict):
    bookmarks = HistoryDict(bookmarks)
    # Turn the HistoryDict into a list of tuples
    bookmarks_list = list(bookmarks.items())
    json.dump({"bookmarks": bookmarks_list}, open("bookmarks.json", "w"))

def load_bookmarks():
    try:
        bookmarks_store = json.load(open("bookmarks.json"))
        bookmarks = HistoryDict(bookmarks_store["bookmarks"])
    except FileNotFoundError:
        bookmarks = HistoryDict()
    return bookmarks