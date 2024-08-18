import datetime

import gradio as gr

from src.db import get_database_connection
from src.db.extraction_log import get_all_data_logs
from src.db.files import get_all_file_scans
from src.utils import (
    estimate_eta,
    isodate_minutes_diff,
    pretty_print_isodate,
    seconds_to_hms,
)


def create_scan_dataset(samples=[]):
    scan_history = gr.Dataset(
        label="File Scan History",
        type="index",
        samples_per_page=25,
        samples=samples,
        headers=[
            "ID",
            "Start Time",
            "End Time",
            "Duration",
            "Path",
            "Total Available",
            "Marked Unavailable",
            "Errors",
            "New Items",
            "New Files",
            "Unchanged Files",
            "Modified Files",
            "Wrongly Detected Changes",
            "Metadata Scan Time",
            "File Hashing Time",
            "Thumb Gen Time",
        ],
        components=[
            "number",
            "textbox",
            "textbox",
            "textbox",
            "textbox",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "textbox",
            "textbox",
            "textbox",
        ],
        scale=1,
    )
    return scan_history


def create_job_dataset(samples=[]):
    job_history = gr.Dataset(
        label="Data Extraction Log",
        type="index",
        samples_per_page=25,
        samples=samples,
        headers=[
            "ID",
            "Start Time",
            "End Time",
            "Duration",
            "Data Load Time",
            "Inference Time",
            "Type",
            "Model",
            "DB Data",
            "Batch Size",
            "Threshold",
            "Image Files",
            "Video Files",
            "Other Files",
            "Data Segments",
            "Errors",
            "Remaining Unprocessed",
        ],
        components=[
            "number",
            "textbox",
            "textbox",
            "textbox",
            "textbox",
            "textbox",
            "textbox",
            "textbox",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
            "number",
        ],
        scale=1,
    )
    return job_history


def fetch_scan_history():
    conn = get_database_connection(write_lock=False)
    file_scans = get_all_file_scans(conn)
    conn.close()
    file_scans = [
        [
            f.id,
            pretty_print_isodate(f.start_time),
            pretty_print_isodate(f.end_time),
            isodate_minutes_diff(f.end_time, f.start_time),
            f.path,
            f.total_available,
            f.marked_unavailable,
            f.errors,
            f.new_items,
            f.new_files,
            f.unchanged_files,
            f.modified_files,
            f.false_changes,
            seconds_to_hms(f.metadata_time),
            seconds_to_hms(f.hashing_time),
            seconds_to_hms(f.thumbgen_time),
        ]
        for f in file_scans
    ]

    return gr.Dataset(samples=file_scans)


def fetch_extraction_logs():
    conn = get_database_connection(write_lock=False)
    log_records = get_all_data_logs(conn)
    conn.close()
    now_str = datetime.datetime.now().isoformat()
    log_rows = [
        [
            t.id,
            pretty_print_isodate(t.start_time),
            (
                (
                    pretty_print_isodate(t.end_time)
                    if t.completed
                    else "ETA: "
                    + estimate_eta(
                        t.start_time,
                        items_processed=(
                            t.video_files + t.image_files + t.other_files
                        ),
                        remaining_items=t.total_remaining,
                    )
                )
                if not t.failed
                else "(Failed) " + pretty_print_isodate(t.end_time)
            ),
            (
                isodate_minutes_diff(t.end_time, t.start_time)
                if t.end_time
                else isodate_minutes_diff(now_str, t.start_time)
            ),
            seconds_to_hms(t.data_load_time),
            seconds_to_hms(t.inference_time),
            t.type,
            t.setter,
            t.items_in_db,
            t.batch_size,
            t.threshold,
            t.image_files,
            t.video_files,
            t.other_files,
            t.total_segments,
            t.errors,
            t.total_remaining,
        ]
        for t in log_records
    ]
    return gr.Dataset(samples=log_rows)
