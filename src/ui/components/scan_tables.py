import gradio as gr

from src.db import (
    get_all_file_scans,
    get_all_tag_scans,
    get_database_connection,
)
from src.utils import isodate_minutes_diff, pretty_print_isodate


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
        ],
        scale=1,
    )
    return scan_history


def create_job_dataset(samples=[]):
    tagging_history = gr.Dataset(
        label="Tagging History",
        type="index",
        samples_per_page=25,
        samples=samples,
        headers=[
            "ID",
            "Start Time",
            "End Time",
            "Duration",
            "Tag Model",
            "Threshold",
            "Image Files",
            "Video Files",
            "Other Files",
            "Video Frames",
            "Total Frames",
            "Errors",
            "Timeouts",
            "Remaining Untagged",
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
            "number",
        ],
        scale=1,
    )
    return tagging_history


def fetch_scan_history():
    conn = get_database_connection()
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
        ]
        for f in file_scans
    ]

    return gr.Dataset(samples=file_scans)


def fetch_tagging_history():
    conn = get_database_connection()
    tag_scans = get_all_tag_scans(conn)
    conn.close()
    tag_scans = [
        [
            t.id,
            pretty_print_isodate(t.start_time),
            pretty_print_isodate(t.end_time),
            isodate_minutes_diff(t.end_time, t.start_time),
            t.setter,
            t.threshold,
            t.image_files,
            t.video_files,
            t.other_files,
            t.video_frames,
            t.total_frames,
            t.errors,
            t.timeouts,
            t.total_remaining,
        ]
        for t in tag_scans
    ]
    return gr.Dataset(samples=tag_scans)
