#!/usr/bin/env python

from __future__ import annotations

import gradio as gr
import json
import os
from src.folders import add_folders, remove_folders, execute_folder_scan
from src.db import get_folders_from_database
from src import find_paths_by_tags, get_all_tags_for_item_name_confidence
from src.utils import show_in_fm

def create_scan_UI():
    with gr.Column(elem_classes="centered-content", scale=0):
        gr.Markdown("## Scan for Images by Tags")