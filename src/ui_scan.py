from __future__ import annotations

import os
import gradio as gr
from src.folders import add_folders, remove_folders, execute_folder_scan
from src.db import get_folders_from_database, get_database_connection
from src.tags import scan_and_predict_tags

def get_folders():
    conn = get_database_connection()
    folders = get_folders_from_database(conn)
    conn.close()
    return "\n".join(folders)

def get_excluded_folders():
    conn = get_database_connection()
    folders = get_folders_from_database(conn, included=False)
    conn.close()
    return "\n".join(folders)

def update_folders(included_folders_text: str, excluded_folders_text: str):
    new_included_folders = [os.path.abspath(p.strip()) for p in included_folders_text.split("\n")]
    new_excluded_folders = [os.path.abspath(p.strip()) for p in excluded_folders_text.split("\n")]
    conn = get_database_connection()
    current_included_folders = get_folders_from_database(conn, included=True)
    current_excluded_folders = get_folders_from_database(conn, included=False)
    included_folders_to_add = list(set(new_included_folders) - set(current_included_folders))
    included_folders_to_remove = list(set(current_included_folders) - set(new_included_folders))
    excluded_folders_to_add = list(set(new_excluded_folders) - set(current_excluded_folders))
    excluded_folders_to_remove = list(set(current_excluded_folders) - set(new_excluded_folders))

    if len(included_folders_to_remove) > 0 or len(excluded_folders_to_remove) > 0:
        success, msg = remove_folders(conn, included=included_folders_to_remove, excluded=excluded_folders_to_remove)
        if not success:
            conn.close()
            return msg

    if len(included_folders_to_add) > 0 or len(excluded_folders_to_add) > 0:
        success, msg = add_folders(conn, included=included_folders_to_add, excluded=excluded_folders_to_add)
        if not success:
            conn.close()
            return msg
    conn.close()
    scan_and_predict_tags()
    return f"Added {len(included_folders_to_add)} folders, removed {len(included_folders_to_remove)} folders"

def rescan_folders():
    conn = get_database_connection()
    execute_folder_scan(conn, commit=True)
    conn.close()
    return "Rescanned all folders"

def regenerate_tags():
    scan_and_predict_tags()
    return "Generated tags for all files with missing tags"

def create_scan_UI():
    with gr.Column(elem_classes="centered-content", scale=0):
        with gr.Row():
            included_directory_list = gr.Textbox(label="Include Directories", value=get_folders, lines=20, interactive=True)
            excluded_directory_list = gr.Textbox(label="Exclude Directories", value=get_excluded_folders, lines=20, interactive=True)

        with gr.Row():
            update_button = gr.Button("Update Directories and Scan New")

        with gr.Row():
            scan_button = gr.Button("Rescan all Directories")
            regenerate_tags_button = gr.Button("Generate Tags for files with no tags")

        with gr.Row():
            results = gr.Label(label="Output", show_label=False)

    update_button.click(
        fn=update_folders,
        inputs=[included_directory_list, excluded_directory_list],
        outputs=[results],
        api_name="update_folder_lists",
    )

    scan_button.click(
        fn=rescan_folders,
        outputs=[results],
        api_name="rescan_folders",
    )

    regenerate_tags_button.click(
        fn=regenerate_tags,
        outputs=[results],
        api_name="regenerate_tags",
    )