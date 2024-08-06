from __future__ import annotations

from typing import Any, Dict

import gradio as gr
from croniter import croniter

from src.db import get_database_connection
from src.db.config import persist_system_config, retrieve_system_config
from src.db.folders import get_folders_from_database
from src.db.rules.rules import get_rules
from src.db.utils import vacuum_database
from src.folders import rescan_all_folders, update_folder_lists
from src.ui.components.extractor_ui import create_data_extraction_UI
from src.ui.components.scan_tables import (
    create_job_dataset,
    create_scan_dataset,
    fetch_extraction_logs,
    fetch_scan_history,
)


def get_folders():
    conn = get_database_connection(write_lock=False)
    folders = get_folders_from_database(conn)
    conn.close()
    return "\n".join(folders)


def get_excluded_folders():
    conn = get_database_connection(write_lock=False)
    folders = get_folders_from_database(conn, included=False)
    conn.close()
    return "\n".join(folders)


def update_folders(
    included_folders_text: str,
    excluded_folders_text: str,
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
    conn = get_database_connection(write_lock=True)
    try:
        cursor = conn.cursor()
        # Begin a transaction
        cursor.execute("BEGIN")
        update_result = update_folder_lists(
            conn,
            new_included_folders,
            new_excluded_folders,
        )
        update_result_text = f"""
        Removed {update_result.included_deleted} included folders, {update_result.excluded_deleted} excluded folders;
        Included folders added (and scanned): {", ".join(update_result.included_added)} ({len(update_result.scan_ids)});
        Excluded folders added: {", ".join(update_result.excluded_added)};
        Removed {update_result.unavailable_files_deleted} files from the database which were no longer available on the filesystem;
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
        return (
            f"Error: {e}",
            included_folders_text,
            excluded_folders_text,
            fetch_scan_history(),
            fetch_extraction_logs(),
        )

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


def rescan_folders():
    conn = get_database_connection(write_lock=True)
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    ids, files_deleted, items_deleted = rescan_all_folders(conn)
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


def create_scan_UI(app: gr.Blocks):
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

                    create_scan_settings_configurator(scan_tab, app)

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
                create_data_extraction_UI(app, scan_tab)
                with gr.Column():
                    create_cron_settings(scan_tab, app)

            with gr.Row():
                with gr.Tabs():
                    with gr.TabItem(label="Scan History"):
                        scan_history = create_scan_dataset()
                    with gr.TabItem(
                        label="Data Extraction History"
                    ) as extractor_tab:
                        extraction_log = create_job_dataset()

        scan_tab.select(
            fn=fetch_all_history,
            outputs=[scan_history, extraction_log],
            api_name="fetch_history",
        )

        update_button.click(
            fn=update_folders,
            inputs=[
                included_directory_list,
                excluded_directory_list,
            ],
            outputs=[
                report_textbox,
                included_directory_list,
                excluded_directory_list,
                scan_history,
                extraction_log,
            ],
            api_name="update_folder_lists",
        )

        scan_button.click(
            fn=rescan_folders,
            outputs=[report_textbox, scan_history, extraction_log],
            api_name="rescan_folders",
        )

        extractor_tab.select(
            fn=fetch_all_history,
            outputs=[scan_history, extraction_log],
            api_name="fetch_history",
        )


def create_scan_settings_configurator(tab: gr.Tab, app: gr.Blocks):
    elements = []
    with gr.Row():
        gr.Markdown("### Scan Settings (Persisted to Database)")
    with gr.Row():
        delete_unavailable = gr.Checkbox(
            label="Remove files from the database if they are no longer found on the filesystem",
            value=True,
            interactive=True,
        )
        elements.append(delete_unavailable)
    with gr.Row():
        gr.Markdown("### Search for these file types:")
        scan_images = gr.Checkbox(
            label="Images",
            value=True,
            interactive=True,
        )
        elements.append(scan_images)
        scan_videos = gr.Checkbox(
            label="Videos",
            value=True,
            interactive=True,
        )
        elements.append(scan_videos)
        scan_audio = gr.Checkbox(
            label="Audio",
            value=False,
            interactive=True,
        )
        elements.append(scan_audio)
        scan_html = gr.Checkbox(
            label="HTML Files",
            value=False,
            interactive=True,
        )
        elements.append(scan_html)
        scan_pdf = gr.Checkbox(
            label="PDF",
            value=False,
            interactive=True,
        )
        elements.append(scan_pdf)

    def update_file_types(args: Dict[gr.Checkbox, bool]):
        conn = get_database_connection(write_lock=True)
        config = retrieve_system_config(conn)
        conn.execute("BEGIN")
        config.remove_unavailable_files = args[delete_unavailable]
        config.scan_images = args[scan_images]
        config.scan_video = args[scan_videos]
        config.scan_audio = args[scan_audio]
        config.scan_html = args[scan_html]
        config.scan_pdf = args[scan_pdf]
        print(config)
        persist_system_config(conn, config)
        conn.commit()
        conn.close()
        return load_settings()

    gr.on(
        triggers=[
            scan_images.input,
            scan_videos.input,
            scan_audio.input,
            scan_html.input,
            scan_pdf.input,
            delete_unavailable.input,
        ],
        fn=update_file_types,
        inputs={*elements},
        outputs=[*elements],
    )

    def load_settings():
        conn = get_database_connection(write_lock=False)
        config = retrieve_system_config(conn)
        conn.close()
        return {
            scan_images: config.scan_images,
            scan_videos: config.scan_video,
            scan_audio: config.scan_audio,
            scan_html: config.scan_html,
            scan_pdf: config.scan_pdf,
            delete_unavailable: config.remove_unavailable_files,
        }

    gr.on(
        triggers=[tab.select, app.load],
        fn=load_settings,
        outputs=[*elements],
        api_name=False,
    )


def create_cron_settings(tab: gr.Tab, app: gr.Blocks):
    elements = []
    with gr.Row():
        gr.Markdown("## Scan And Data Extraction Cron Job")
    with gr.Row():
        gr.Markdown(
            """
The cron job will run the file scan and data extraction job regularly at the specified schedule.

The scheduler will only select models that have rules associated with them. If a model has no rules, it will not be scheduled.
Go to the rules configuration tab and create rules for the models you want to schedule.
An empty rule with no filters is sufficient, and it will match all files.

Currently, these models would be scheduled:
        """
        )
    with gr.Row():
        model_list = gr.Markdown("")
        elements.append(model_list)
    with gr.Row():
        gr.Markdown("### Cron Schedule")
    with gr.Row():
        enable_cronjob = gr.Checkbox(
            label="Enable",
            value=False,
            interactive=True,
        )
        elements.append(enable_cronjob)
        cron_string = gr.Textbox(
            label="Cron string",
            value="* * * * *",
            interactive=True,
        )
        elements.append(cron_string)
    with gr.Row():
        save_button = gr.Button("Save", scale=0)
        elements.append(save_button)
    with gr.Row():
        gr.Markdown(
            """
The cron schedule is a string that defines the frequency of the scan and data extraction job.
The format is as follows:
```
* * * * *
| | | | +-- Day of the Week (range: 0-6, 0 is Sunday)
| | | +---- Month (range: 1-12)
| | +------ Day of the Month (range: 1-31)
| +-------- Hour (range: 0-23)
+---------- Minute (range: 0-59)
```
For example, the default value `0 3 * * *` means the job will run at 3 AM every day.
Or, in order to run it every hour, use `0 * * * *`, every four hours: `0 */4 * * *`.
        """
        )

    def update_file_types(args: Dict[gr.Checkbox | gr.Textbox, Any]):
        conn = get_database_connection(write_lock=True)
        config = retrieve_system_config(conn)
        conn.execute("BEGIN")
        assert isinstance(args[enable_cronjob], bool)
        assert isinstance(args[cron_string], str)
        config.enable_cron_job = args[enable_cronjob]
        if croniter.is_valid(args[cron_string]):
            config.cron_schedule = args[cron_string]
        print(config)
        persist_system_config(conn, config)
        conn.commit()
        conn.close()
        return load_settings()

    gr.on(
        triggers=[
            save_button.click,
        ],
        fn=update_file_types,
        inputs={*elements},
        outputs=[*elements],
    )

    def load_settings():
        conn = get_database_connection(write_lock=False)
        config = retrieve_system_config(conn)
        rules = get_rules(conn)
        conn.close()
        models = []
        for rule in rules:
            if rule.enabled:
                models.extend(rule.setters)
        if models:
            model_list_str = ", ".join([f"{m[1]}" for m in models])
        else:
            model_list_str = "No models scheduled. Go to the rules tab and create rules for the models you want to schedule."
        return {
            enable_cronjob: config.enable_cron_job,
            cron_string: config.cron_schedule,
            model_list: model_list_str,
        }

    gr.on(
        triggers=[tab.select, app.load],
        fn=load_settings,
        outputs=[*elements],
        api_name=False,
    )
