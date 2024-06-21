from __future__ import annotations
from datetime import datetime
from typing import List

import gradio as gr

from src.folders import update_folder_lists, rescan_all_folders
from src.db import get_folders_from_database, get_database_connection, get_all_file_scans, get_all_tag_scans, delete_tags_from_setter, vacuum_database
from src.tags import scan_and_predict_tags
from src.wd_tagger import V3_MODELS

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

def parse_iso_date(date: str):
    return datetime.fromisoformat(date).strftime("%Y-%m-%d %H:%M:%S")

def update_folders(included_folders_text: str, excluded_folders_text: str, delete_unavailable_files: bool = True):
    new_included_folders = [p for p in included_folders_text.strip().split("\n")] if len(included_folders_text.strip()) > 0 else []
    new_excluded_folders = [p for p in excluded_folders_text.strip().split("\n")] if len(excluded_folders_text.strip()) > 0 else []
    conn = get_database_connection()
    try:
        cursor = conn.cursor()
        # Begin a transaction
        cursor.execute('BEGIN')
        update_result = update_folder_lists(conn, new_included_folders, new_excluded_folders, delete_unavailable_files)
        update_result_text = f"""
        Removed {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;
        Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
        Excluded folders added: {", ".join(update_result.excluded_added)};
        Removed {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem {"(enabled)" if delete_unavailable_files else "(disabled)"};
        Removed {update_result.excluded_folder_files_deleted} files from the database that were inside excluded folders;
        Removed {update_result.orphan_files_deleted} files from the database that were no longer inside included folders;
        Removed {update_result.orphan_items_deleted} orphaned items (with no corresponding files) from the database. Any bookmarks on these items were also removed.
        """
        conn.commit()
        vacuum_database(conn)
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
        conn.close()
        return f"Error: {e}", included_folders_text, excluded_folders_text
    
    current_included_folders = get_folders_from_database(conn, included=True)
    current_excluded_folders = get_folders_from_database(conn, included=False)
    conn.close()

    return f"{update_result_text}", "\n".join(current_included_folders), "\n".join(current_excluded_folders), fetch_scan_history(), fetch_tagging_history()

def rescan_folders(delete_unavailable_files: bool = True):
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    ids, files_deleted, items_deleted = rescan_all_folders(conn, delete_unavailable=delete_unavailable_files)
    conn.commit()
    vacuum_database(conn)
    conn.close()
    return f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items.", fetch_scan_history(), fetch_tagging_history()

def regenerate_tags(tag_models: List[str] = [V3_MODELS[0]]):
    print(f"Regenerating tags for models: {tag_models}")
    conn = get_database_connection()
    full_report = ""
    for model in tag_models:
        cursor = conn.cursor()
        cursor.execute('BEGIN')
        images, videos, failed, timed_out = scan_and_predict_tags(conn, setter=model)
        conn.commit()
        vacuum_database(conn)
        failed_str = "\n".join(failed)
        timed_out_str = "\n".join(timed_out)
        report_str = f"""
        Tag Generation completed for model {model}.
        Successfully processed {images} images and {videos} videos.
        {len(failed)} files failed to process due to errors.
        {len(timed_out)} files took too long to process and timed out.
        Failed files:
        {failed_str}
        Timed out files:
        {timed_out_str}
        """
        full_report += report_str
    conn.close()
    return full_report, fetch_scan_history(), fetch_tagging_history()

def delete_tags(tag_models: List[str] = []):
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    message = ""
    for model in tag_models:
        tags_removed, items_tags_removed = delete_tags_from_setter(conn, model)
        message += f"Removed {tags_removed} tags from {items_tags_removed} items tagged by model {model}.\n"
    conn.commit()
    vacuum_database(conn)
    conn.close()
    return message, fetch_scan_history(), fetch_tagging_history()

def fetch_scan_history():
    conn = get_database_connection()
    file_scans = get_all_file_scans(conn)
    conn.close()
    # Convert the file scans to a list of tuples
    file_scans = [(f.id, parse_iso_date(f.start_time), parse_iso_date(f.end_time), 
                   f.path, 
                   f.total_available, f.marked_unavailable, f.errors, f.new_items, f.new_files, f.unchanged_files, f.modified_files) for f in file_scans]
    return file_scans

def fetch_tagging_history():
    conn = get_database_connection()
    tag_scans = get_all_tag_scans(conn)
    conn.close()
    # Convert the tag scans to a list of tuples
    tag_scans = [(
        t.id,
        parse_iso_date(t.start_time),
        parse_iso_date(t.end_time),
        t.setter,
        t.threshold,
        t.image_files,
        t.video_files,
        t.other_files,
        t.video_frames,
        t.total_frames,
        t.errors,
        t.timeouts,
        t.total_remaining
        ) for t in tag_scans]
    return tag_scans

def fetch_all_history():
    return fetch_scan_history(), fetch_tagging_history()

def create_scan_UI():
    with gr.TabItem(label="File Scan & Tagging") as scan_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                included_directory_list = gr.Textbox(label="Include Directories", value=get_folders, lines=20, interactive=True)
                excluded_directory_list = gr.Textbox(label="Exclude Directories", value=get_excluded_folders, lines=20, interactive=True)
            with gr.Row():
                with gr.Column():
                    with gr.Row():
                        update_button = gr.Button("Update Directory Lists and Scan New Entries")
                        scan_button = gr.Button("Rescan all Directories")
                with gr.Column():
                    delete_unavailable_files = gr.Checkbox(label="Remove files from the database if they are no longer found on the filesystem", value=True, interactive=True)
            with gr.Row():
                with gr.Column():
                    model_choice = gr.Dropdown(label="Tagging Model(s) to Use", multiselect=True, choices=V3_MODELS, value=[V3_MODELS[0]])
                    with gr.Row():
                        regenerate_tags_button = gr.Button("Generate Tags for Files Missing Tags")
                        delete_tags_button = gr.Button("Delete ALL Tags set by selected Model(s)")
                with gr.Column():
                    gr.Markdown("""
                        ## Notes
                        The directory lists are newline-separated lists of directories to include or exclude. The directories must be absolute paths. The included directories will be scanned for files, and the excluded directories will have their files excluded/removed from the database.
                        
                        The 'Update Directory Lists and Scan New Entries' button will update the directory lists, scan newly included directories, and generate tags for files that don't have them.
                        
                        The 'Rescan all Directories' button will rescan all directories. But it will not update the directory lists or generate tags.

                        The 'Generate Tags for Files Missing Tags' button will generate tags for all items that don't have tags set by the selected model(s).

                        The 'Delete ALL Tags set by selected Model(s)' button will delete all tags set by the selected model(s) for all items from the database.                        
                        """)
            with gr.Row():
                results = gr.Textbox(label="Scan Report", interactive=False, lines=8, value="")

            with gr.Row():
                with gr.Tabs():
                    with gr.TabItem(label="Scan History"):
                        scan_history = gr.Dataset(
                            label="File Scan History",
                            type="index",
                            samples_per_page=25,
                            samples=[],
                            headers=["ID", "Start Time", "End Time", "Path", "Total Available", "Marked Unavailable", "Errors", "New Items", "New Files", "Unchanged Files", "Modified Files"],
                            components=["number", "textbox", "textbox", "textbox", "number", "number", "number", "number", "number", "number", "number"],
                            scale=1
                        )
                    with gr.TabItem(label="Tagging History"):
                        tagging_history = gr.Dataset(
                            label="Tagging History",
                            type="index",
                            samples_per_page=25,
                            samples=[],
                            headers=["ID", "Start Time", "End Time", "Tag Model", "Threshold", "Image Files", "Video Files", "Other Files", "Video Frames", "Total Frames", "Errors", "Timeouts", "Remaining Untagged"],
                            components=["number", "textbox", "textbox", "textbox", "number", "number", "number", "number", "number", "number", "number", "number", "number"],
                            scale=1
                        )
                

        scan_tab.select(
            fn=fetch_all_history,
            outputs=[scan_history, tagging_history],
            api_name="fetch_history",
        )

        update_button.click(
            fn=update_folders,
            inputs=[included_directory_list, excluded_directory_list, delete_unavailable_files],
            outputs=[results, included_directory_list, excluded_directory_list, scan_history, tagging_history],
            api_name="update_folder_lists",
        )

        scan_button.click(
            fn=rescan_folders,
            inputs=[delete_unavailable_files],
            outputs=[results, scan_history, tagging_history],
            api_name="rescan_folders",
        )

        regenerate_tags_button.click(
            fn=regenerate_tags,
            inputs=[model_choice],
            outputs=[results, scan_history, tagging_history],
            api_name="regenerate_tags",
        )

        delete_tags_button.click(
            fn=delete_tags,
            inputs=[model_choice],
            outputs=[results, scan_history, tagging_history],
            api_name="delete_tags",
        )