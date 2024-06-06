import sqlite3
from datetime import datetime
from typing import List, Tuple
from src.db import add_folder_to_database, get_folders_from_database, add_folder_to_database, save_items_to_database, delete_files_under_excluded_folders, delete_items_without_files, delete_files_not_under_included_folders
from src.files import scan_files, get_image_extensions, get_video_extensions, get_audio_extensions

def add_new_included_folders_and_scan(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    add_time = datetime.now().isoformat()

    for folder in paths:
        add_folder_to_database(conn, add_time, folder, included=True)

    execute_folder_scan(conn, paths)
    return True, "Folders added and scanned successfully"

def execute_folder_scan(
        conn: sqlite3.Connection,
        included_folders: None | List[str] = None,
        include_images = True,
        include_video = False,
        include_audio = False,
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
    save_items_to_database(conn, hashes_info, starting_points)
    return True, "Scan completed successfully"

def add_new_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:

    for folder in paths:
        add_folder_to_database(conn, datetime.now().isoformat(), folder, included=False)
    
    delete_files_under_excluded_folders(conn)
    delete_items_without_files(conn)
    return True, "Folders added successfully"

def remove_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()

    cursor.execute('DELETE FROM folders WHERE path = ? AND included = 0', (folder,))
    return True, "Folders removed successfully"

def remove_included_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()
    for folder in paths:
        cursor.execute('DELETE FROM folders WHERE path = ? AND included = 1', (folder,))
    delete_files_not_under_included_folders(conn)
    delete_items_without_files(conn)
    return True, "Folders removed successfully"

# Public functions

def add_folders(conn: sqlite3.Connection, included: List[str] = [], excluded: List[str] = []) -> Tuple[bool, str]:
    if len(included) == 0 and len(excluded) == 0:
        return False, "No folders provided"

    if len(excluded) > 0:
        success, message = add_new_excluded_folders(conn, excluded)
        if not success:
            return False, message
        
    if len(included) > 0:
        success, message = add_new_included_folders_and_scan(conn, included)
        if not success:
            return False, message
    return True, "Folders added successfully"

def remove_folders(conn: sqlite3.Connection, included: List[str] = [], excluded: List[str] = []) -> Tuple[bool, str]:
    if len(included) == 0 and len(excluded) == 0:
        return False, "No folders provided"
        
    if len(excluded) > 0:
        success, message = remove_excluded_folders(conn, excluded)
        if not success:
            return False, message
        
    if len(included) > 0:
        success, message = remove_included_folders(conn, included)
        if not success:
            return False, message
    return True, "Folders removed successfully"