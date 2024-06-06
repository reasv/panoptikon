import sqlite3
from datetime import datetime
from typing import List, Tuple
from src.db import add_folder_to_database, get_folders_from_database, add_folder_to_database, save_items_to_database
from src.files import scan_files, get_image_extensions, get_video_extensions, get_audio_extensions

def add_new_included_folders_and_scan(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    add_time = datetime.now().isoformat()
    try:
        for folder in paths:
            add_folder_to_database(conn, add_time, folder, included=True)
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "Cannot add an excluded folder to the included folders list"
    
    execute_folder_scan(conn, paths)

    conn.commit()
    return True, "Folders added and scanned successfully"


def execute_folder_scan(
        conn: sqlite3.Connection,
        included_folders: None | List[str] = None,
        include_images = True,
        include_video = False,
        include_audio = False
    ) -> Tuple[bool, str]:

    if included_folders is None:
        included_folders = get_folders_from_database(conn, included=True)

    excluded_folders = get_folders_from_database(conn, included=False)
    starting_points = included_folders
    extensions = (
        include_images * get_image_extensions()
        + include_video * get_video_extensions()
        + include_audio * get_audio_extensions()
    )
    hashes_info = scan_files(starting_points, excluded_folders, extensions)
    save_items_to_database(hashes_info, starting_points)
    return True, "Scan completed successfully"

