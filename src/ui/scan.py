from __future__ import annotations

import gradio as gr

from src.db import (
    get_database_connection,
    get_folders_from_database,
    vacuum_database,
)
from src.folders import rescan_all_folders, update_folder_lists
from src.ui.components.extractor_ui import create_extractor_UI
from src.ui.components.scan_tables import (
    create_job_dataset,
    create_scan_dataset,
    fetch_extraction_logs,
    fetch_scan_history,
)


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


def update_folders(
    included_folders_text: str,
    excluded_folders_text: str,
    delete_unavailable_files: bool = True,
):
    new_included_folders = (
        [p for p in included_folders_text.strip().split("\n")]
        if len(included_folders_text.strip()) > 0
        else []
    )
    new_excluded_folders = (
        [p for p in excluded_folders_text.strip().split("\n")]
        if len(excluded_folders_text.strip()) > 0
        else []
    )
    conn = get_database_connection()
    try:
        cursor = conn.cursor()
        # Begin a transaction
        cursor.execute("BEGIN")
        update_result = update_folder_lists(
            conn,
            new_included_folders,
            new_excluded_folders,
            delete_unavailable_files,
        )
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

    return (
        f"{update_result_text}",
        "\n".join(current_included_folders),
        "\n".join(current_excluded_folders),
        fetch_scan_history(),
        fetch_extraction_logs(),
    )


def rescan_folders(delete_unavailable_files: bool = True):
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    ids, files_deleted, items_deleted = rescan_all_folders(
        conn, delete_unavailable=delete_unavailable_files
    )
    conn.commit()
    vacuum_database(conn)
    conn.close()
    return (
        f"Rescanned all folders. Removed {files_deleted} files and {items_deleted} orphaned items.",
        fetch_scan_history(),
        fetch_extraction_logs(),
    )


def fetch_all_history():
    return fetch_scan_history(), fetch_extraction_logs()


def create_scan_UI():
    with gr.TabItem(label="File Scan & Tagging") as scan_tab:
        with gr.Column(elem_classes="centered-content", scale=0):
            with gr.Row():
                included_directory_list = gr.Textbox(
                    label="Include Directories",
                    value=get_folders,
                    lines=15,
                    interactive=True,
                )
                excluded_directory_list = gr.Textbox(
                    label="Exclude Directories",
                    value=get_excluded_folders,
                    lines=15,
                    interactive=True,
                )
            with gr.Row():
                with gr.Column():
                    with gr.Row():
                        gr.Markdown(
                            """
                        ## Notes
                        The directory lists are newline-separated lists of directories to include or exclude. The directories must be absolute paths. The included directories will be scanned for files, and the excluded directories will have their files excluded/removed from the database.
                        
                        The 'Update Directory Lists and Scan New Entries' button will update the directory lists and only scan newly included directories.
                        
                        The 'Rescan all Directories' button will rescan all directories. But it will not apply changes to the directory lists or generate tags.                      
                        """
                        )
                    with gr.Row():
                        delete_unavailable_files = gr.Checkbox(
                            label="Remove files from the database if they are no longer found on the filesystem",
                            value=True,
                            interactive=True,
                        )
                    with gr.Row():
                        update_button = gr.Button(
                            "Update Directory Lists and Scan New Entries"
                        )
                        scan_button = gr.Button("Rescan all Directories")
                with gr.Column():
                    with gr.Row():
                        report_textbox = gr.Textbox(
                            label="Scan Report",
                            interactive=False,
                            lines=8,
                            value="",
                        )
            with gr.Row():
                create_extractor_UI()
                with gr.Column():
                    pass

            with gr.Row():
                with gr.Tabs():
                    with gr.TabItem(label="Scan History"):
                        scan_history = create_scan_dataset()
                    with gr.TabItem(
                        label="Data Extraction History"
                    ) as extractor_tab:
                        tagging_history = create_job_dataset()

        scan_tab.select(
            fn=fetch_all_history,
            outputs=[scan_history, tagging_history],
            api_name="fetch_history",
        )

        update_button.click(
            fn=update_folders,
            inputs=[
                included_directory_list,
                excluded_directory_list,
                delete_unavailable_files,
            ],
            outputs=[
                report_textbox,
                included_directory_list,
                excluded_directory_list,
                scan_history,
                tagging_history,
            ],
            api_name="update_folder_lists",
        )

        scan_button.click(
            fn=rescan_folders,
            inputs=[delete_unavailable_files],
            outputs=[report_textbox, scan_history, tagging_history],
            api_name="rescan_folders",
        )

        extractor_tab.select(
            fn=fetch_all_history,
            outputs=[scan_history, tagging_history],
            api_name="fetch_history",
        )
