from __future__ import annotations
from dataclasses import dataclass
from typing import List

import gradio as gr

from src.ui.components.utils import get_all_bookmark_folders

def on_bookmark_folder_change(bookmarks_namespace: str):
    return bookmarks_namespace

def on_input(namespace_chosen: str, bookmarks_namespace: str):
    print(f"Input namespace {namespace_chosen}")
    new_value = namespace_chosen if len(namespace_chosen.strip()) > 0 else bookmarks_namespace
    return new_value, new_value

def on_tab_load():
    return gr.update(choices=get_all_bookmark_folders())

@dataclass
class BookmarkFolderChooser:
    bookmark_folder_choice: gr.Dropdown

def create_bookmark_folder_chooser(parent_tab: gr.TabItem=None, bookmarks_namespace: gr.State = None):
    bookmark_folder_choice = gr.Dropdown(choices=get_all_bookmark_folders(), allow_custom_value=True, visible=bookmarks_namespace != None, label="Bookmark group name")

    parent_tab.select(
        fn=on_tab_load,
        outputs=[bookmark_folder_choice]
    )

    bookmarks_namespace.change(
        fn=on_bookmark_folder_change,
        inputs=[bookmark_folder_choice]
    )

    bookmark_folder_choice.input(
        fn=on_input,
        inputs=[bookmark_folder_choice, bookmarks_namespace],
        outputs=[bookmark_folder_choice, bookmarks_namespace]
    )

    return BookmarkFolderChooser(
        bookmark_folder_choice = bookmark_folder_choice
    )