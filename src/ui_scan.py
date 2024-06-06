from __future__ import annotations

import os
import gradio as gr
from src.folders import add_folders, remove_folders, execute_folder_scan
from src.db import get_folders_from_database, get_database_connection
from src.tags import scan_and_predict_tags
from src.utils import normalize_path

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
    new_included_folders = [normalize_path(p) for p in included_folders_text.strip().split("\n")]
    new_excluded_folders = [normalize_path(p) for p in excluded_folders_text.strip().split("\n")]
    print(new_included_folders, new_excluded_folders)
    conn = get_database_connection()
    current_included_folders = get_folders_from_database(conn, included=True)
    current_excluded_folders = get_folders_from_database(conn, included=False)
    included_folders_to_add = list(set(new_included_folders) - set(current_included_folders))
    included_folders_to_remove = list(set(current_included_folders) - set(new_included_folders))
    excluded_folders_to_add = list(set(new_excluded_folders) - set(current_excluded_folders))
    excluded_folders_to_remove = list(set(current_excluded_folders) - set(new_excluded_folders))
    
    cursor = conn.cursor()
    # Begin a transaction
    cursor.execute('BEGIN')
    try:
        if len(included_folders_to_remove) > 0 or len(excluded_folders_to_remove) > 0:
            remove_folders(conn, included=included_folders_to_remove, excluded=excluded_folders_to_remove)

        if len(included_folders_to_add) > 0 or len(excluded_folders_to_add) > 0:
            add_folders(conn, included=included_folders_to_add, excluded=excluded_folders_to_add)
        
        scan_and_predict_tags(conn)
        conn.commit()
        conn.close()
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
        conn.close()
        return f"Error: {e}"

    return f"Added {len(included_folders_to_add)} folders, removed {len(included_folders_to_remove)} folders"

def rescan_folders():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    execute_folder_scan(conn)
    conn.commit()
    conn.close()
    return "Rescanned all folders"

def regenerate_tags():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    scan_and_predict_tags(conn)
    conn.commit()
    conn.close()
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