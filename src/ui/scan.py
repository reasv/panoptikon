from __future__ import annotations

import gradio as gr

from src.folders import update_folder_lists, rescan_all_folders
from src.db import get_folders_from_database, get_database_connection, get_all_file_scans
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
    except Exception as e:
        # Rollback the transaction on error
        conn.rollback()
        conn.close()
        return f"Error: {e}", included_folders_text, excluded_folders_text

    cursor.execute('BEGIN')
    try: 
        scan_and_predict_tags(conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return f"Error: {e}", included_folders_text, excluded_folders_text
    
    current_included_folders = get_folders_from_database(conn, included=True)
    current_excluded_folders = get_folders_from_database(conn, included=False)
    file_scans = get_all_file_scans(conn)

    conn.close()

    return f"{update_result_text}\nScanned and generated tags for all files that didn't have them.", "\n".join(current_included_folders), "\n".join(current_excluded_folders)

def rescan_folders(delete_unavailable_files: bool = True):
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    ids, files_deleted, items_deleted = rescan_all_folders(conn, delete_unavailable=delete_unavailable_files)
    conn.commit()
    conn.close()
    return f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items."

def regenerate_tags():
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    scan_and_predict_tags(conn)
    conn.commit()
    conn.close()
    return "Generated tags for all files with missing tags"

def scan_and_predict_tags(delete_unavailable_files=True):
    return rescan_folders(delete_unavailable_files) + regenerate_tags()

def fetch_scan_history():
    conn = get_database_connection()
    file_scans = get_all_file_scans(conn)
    conn.close()
    # Convert the file scans to a list of tuples
    file_scans = [(f.id, f.start_time, f.end_time, f.path, f.total_available, f.marked_unavailable, f.errors, f.new_items, f.new_files, f.unchanged_files, f.modified_files) for f in file_scans]
    return file_scans

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
                    regenerate_tags_button = gr.Button("Generate Tags for files with no tags")
                    regenerate_and_scan = gr.Button("Scan All and Generate Missing Tags")
                with gr.Column():
                    gr.Markdown("""
                        ## Notes
                        The directory lists are newline-separated lists of directories to include or exclude. The directories must be absolute paths. The included directories will be scanned for files, and the excluded directories will have their files excluded/removed from the database.
                        
                        The 'Update Directory Lists and Scan New Entries' button will update the directory lists, scan newly included directories, and generate tags for files that don't have them.
                        
                        The 'Rescan all Directories' button will rescan all directories. But it will not update the directory lists or generate tags.
                        
                        The 'Scan All and Generate missing Tags' button will rescan all directories, update the directory lists, and generate tags for files that don't have them.
                        """)
            with gr.Row():
                results = gr.Textbox(label="Scan Report", interactive=False, lines=8, value="")

            with gr.Row():
                scan_history = gr.Dataset(
                    label="File Scan History",
                    type="index",
                    samples_per_page=25,
                    samples=[],
                    components=["textbox", "textbox"],
                    scale=1
                )

        scan_tab.select(
            fn=fetch_scan_history,
            outputs=[scan_history],
            api_name="fetch_scan_history",
        )
        update_button.click(
            fn=update_folders,
            inputs=[included_directory_list, excluded_directory_list, delete_unavailable_files],
            outputs=[results, included_directory_list, excluded_directory_list,],
            api_name="update_folder_lists",
        )

        scan_button.click(
            fn=rescan_folders,
            inputs=[delete_unavailable_files],
            outputs=[results],
            api_name="rescan_folders",
        )

        regenerate_tags_button.click(
            fn=regenerate_tags,
            outputs=[results],
            api_name="regenerate_tags",
        )

        regenerate_and_scan.click(
            fn=scan_and_predict_tags,
            inputs=[delete_unavailable_files],
            outputs=[results],
            api_name="scan_and_predict_tags",
        )