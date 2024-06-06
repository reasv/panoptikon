import sqlite3
from datetime import datetime
from typing import List, Tuple
from src.db import add_folder_to_database, get_folders_from_database, add_folder_to_database, save_items_to_database, delete_files_under_excluded_folders, delete_items_without_files, delete_files_not_under_included_folders
from src.files import scan_files, get_image_extensions, get_video_extensions, get_audio_extensions

def add_new_included_folders_and_scan(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    add_time = datetime.now().isoformat()
    try:
        for folder in paths:
            add_folder_to_database(conn, add_time, folder, included=True)
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "Cannot add an excluded folder to the included folders list"
    
    execute_folder_scan(conn, paths)
    return True, "Folders added and scanned successfully"

def execute_folder_scan(
        conn: sqlite3.Connection,
        included_folders: None | List[str] = None,
        include_images = True,
        include_video = False,
        include_audio = False,
        commit = False
    ) -> Tuple[bool, str]:

    cursor = conn.cursor()
    if commit:
        cursor.execute('BEGIN')
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
    if commit:
        conn.commit()
    return True, "Scan completed successfully"

def add_new_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    try:
        for folder in paths:
            add_folder_to_database(conn, datetime.now().isoformat(), folder, included=False)
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "Cannot add an included folder to the excluded folders list"
    
    delete_files_under_excluded_folders(conn)
    delete_items_without_files(conn)
    return True, "Folders added successfully"

def remove_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()
    try:
        for folder in paths:
            cursor.execute('DELETE FROM folders WHERE path = ? AND included = 0', (folder,))
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "Failed to remove excluded folder"
    return True, "Folders removed successfully"

def remove_included_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()
    try:
        for folder in paths:
            cursor.execute('DELETE FROM folders WHERE path = ? AND included = 1', (folder,))
    except sqlite3.IntegrityError:
        conn.rollback()
        return False, "Failed to remove included folder"
    
    delete_files_not_under_included_folders(conn)
    delete_items_without_files(conn)
    return True, "Folders removed successfully"

# Public functions

def add_folders(conn: sqlite3.Connection, included: List[str] = [], excluded: List[str] = []) -> Tuple[bool, str]:
    if len(included) == 0 and len(excluded) == 0:
        return False, "No folders provided"
    cursor = conn.cursor()
    cursor.execute('BEGIN')

    if len(excluded) > 0:
        success, message = add_new_excluded_folders(conn, excluded)
        if not success:
            return False, message
        
    if len(included) > 0:
        success, message = add_new_included_folders_and_scan(conn, included)
        if not success:
            return False, message
    conn.commit()
    return True, "Folders added successfully"

def remove_folders(conn: sqlite3.Connection, included: List[str] = [], excluded: List[str] = []) -> Tuple[bool, str]:
    if len(included) == 0 and len(excluded) == 0:
        return False, "No folders provided"
    
    cursor = conn.cursor()
    cursor.execute('BEGIN')
    
    if len(excluded) > 0:
        success, message = remove_excluded_folders(conn, excluded)
        if not success:
            return False, message
        
    if len(included) > 0:
        success, message = remove_included_folders(conn, included)
        if not success:
            return False, message
    conn.commit()
    return True, "Folders removed successfully"