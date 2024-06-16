import sqlite3
from datetime import datetime
from typing import List, Tuple

from src.db import (
    update_file_data,
    add_file_scan,
    mark_unavailable_files,
    add_folder_to_database,
    get_folders_from_database,
    add_folder_to_database,
    delete_files_under_excluded_folders,
    delete_items_without_files,
    delete_files_not_under_included_folders
)
from src.files import scan_files, deduplicate_paths

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
    ) -> list[int]:

    if included_folders is None:
        included_folders = get_folders_from_database(conn, included=True)
    excluded_folders = get_folders_from_database(conn, included=False)
    starting_points = deduplicate_paths(included_folders)
    scan_time = datetime.now().isoformat()

    scan_ids = []
    for folder in starting_points:
        new_items, unchanged_files, new_files, modified_files = 0, 0, 0, 0
        for file_data in scan_files(
            folder,
            excluded_folders,
            include_images,
            include_video,
            include_audio
        ):
            (
                item_inserted, 
                file_updated,
                file_deleted,
                file_inserted
            ) = update_file_data(
                conn,
                scan_time=scan_time,
                file_data=file_data
            )
            if item_inserted:
                new_items += 1
            if file_updated:
                # File was already in the database and has NOT been modified on disk
                unchanged_files += 1
            elif file_deleted:
                # File was in the database but has changed on disk,
                # therefore it was deleted and reinserted as a new file
                modified_files += 1
            elif file_inserted:
                # File was not in the database and has been inserted
                new_files += 1
        # Mark files that were not found in the scan but are present in the db as `unavailable`
        marked_unavailable = mark_unavailable_files(conn, scan_time=scan_time, path=folder)

        end_time = datetime.now().isoformat()
        scan_ids.append(
            add_file_scan(
                conn,
                scan_time=scan_time,
                end_time=end_time,
                folder=folder,
                new_items=new_items,
                unchanged_files=unchanged_files,
                new_files=new_files,
                modified_files=modified_files,
                marked_unavailable=marked_unavailable
        ))

    return scan_ids

def add_new_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:

    for folder in paths:
        add_folder_to_database(conn, datetime.now().isoformat(), folder, included=False)
    
    delete_files_under_excluded_folders(conn)
    delete_items_without_files(conn)
    return True, "Folders added successfully"

def remove_excluded_folders(conn: sqlite3.Connection, paths: list[str]) -> Tuple[bool, str]:
    cursor = conn.cursor()
    for folder in paths:
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