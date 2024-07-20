import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from src.db import (
    add_file_scan,
    add_folder_to_database,
    delete_files_not_under_included_folders,
    delete_files_under_excluded_folders,
    delete_folders_not_in_list,
    delete_items_without_files,
    delete_log_items_without_item,
    delete_tags_without_items,
    delete_unavailable_files,
    get_folders_from_database,
    mark_unavailable_files,
    update_file_data,
)
from src.files import deduplicate_paths, scan_files
from src.utils import normalize_path


def execute_folder_scan(
    conn: sqlite3.Connection,
    included_folders: None | List[str] = None,
    include_images=True,
    include_video=True,
    include_audio=False,
) -> list[int]:
    """
    Execute a scan of the files in the given `included_folders`, or all folders marked as `included` within the db, and update the database with the results.
    Marks files that were not found in the scan but are present in the db as `unavailable`.
    Will never scan folders not marked as `included` in the database.
    """
    all_included_folders = get_folders_from_database(conn, included=True)
    if included_folders is None:
        included_folders = all_included_folders

    # Ensure that all included_folders are also marked as included in the database
    included_folders = [
        folder for folder in included_folders if folder in all_included_folders
    ]

    excluded_folders = get_folders_from_database(conn, included=False)
    starting_points = deduplicate_paths(included_folders)
    scan_time = datetime.now().isoformat()

    print(f"Scanning folders: {included_folders}")
    scan_ids = []
    for folder in starting_points:
        new_items, unchanged_files, new_files, modified_files, errors = (
            0,
            0,
            0,
            0,
            0,
        )
        for file_data in scan_files(
            conn,
            starting_points=[folder],
            excluded_paths=excluded_folders,
            include_images=include_images,
            include_video=include_video,
            include_audio=include_audio,
        ):
            if file_data is None:
                errors += 1
                continue
            # Update the file data in the database
            (item_inserted, file_updated, file_deleted, file_inserted) = (
                update_file_data(conn, scan_time=scan_time, file_data=file_data)
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
        marked_unavailable, total_available = mark_unavailable_files(
            conn, scan_time=scan_time, path=folder
        )
        print(
            f"Scan of {folder} complete. New items: {new_items}, Unchanged files: {unchanged_files}, New files: {new_files}, Modified files: {modified_files}, Marked unavailable: {marked_unavailable}, Errors: {errors}, Total available: {total_available}"
        )
        end_time = datetime.now().isoformat()
        scan_ids.append(
            add_file_scan(
                conn,
                scan_time=scan_time,
                end_time=end_time,
                path=folder,
                new_items=new_items,
                unchanged_files=unchanged_files,
                new_files=new_files,
                modified_files=modified_files,
                marked_unavailable=marked_unavailable,
                errors=errors,
                total_available=total_available,
            )
        )

    return scan_ids


# Public API
@dataclass
class UpdateFoldersResult:
    included_deleted: int
    excluded_deleted: int
    included_added: List[str]
    excluded_added: List[str]
    unavailable_files_deleted: int
    excluded_folder_files_deleted: int
    orphan_files_deleted: int
    orphan_items_deleted: int
    scan_ids: List[int]


def update_folder_lists(
    conn: sqlite3.Connection,
    included_folders: List[str],
    excluded_folders: List[str],
    delete_unavailable: bool = True,
):
    """
    Update the database with the new `included_folders` and `excluded_folders` lists.
    Any folders that are in the database but not in the new lists will be DELETED.
    Any files under the `excluded_folders` will be DELETED.
    Any files not under the `included_folders` will be DELETED.
    Any orphaned items without files will be DELETED.
    Bookmarks on orhpaned items will be DELETED.
    """
    new_included_folders = [
        normalize_path(p) for p in included_folders if len(p.strip()) > 0
    ]
    new_excluded_folders = [
        normalize_path(p) for p in excluded_folders if len(p.strip()) > 0
    ]

    included_deleted = delete_folders_not_in_list(
        conn=conn, folder_paths=new_included_folders, included=True
    )
    excluded_deleted = delete_folders_not_in_list(
        conn=conn, folder_paths=new_excluded_folders, included=False
    )

    scan_time = datetime.now().isoformat()
    included_added = []
    for folder in new_included_folders:
        added = add_folder_to_database(conn, scan_time, folder, included=True)
        if added:
            included_added.append(folder)

    excluded_added = []
    for folder in new_excluded_folders:
        added = add_folder_to_database(conn, scan_time, folder, included=False)
        if added:
            excluded_added.append(folder)

    scan_ids = execute_folder_scan(conn, included_folders=included_added)

    if delete_unavailable:
        unavailable_files_deleted = delete_unavailable_files(conn)
    else:
        unavailable_files_deleted = 0

    excluded_folder_files_deleted = delete_files_under_excluded_folders(conn)
    orphan_files_deleted = delete_files_not_under_included_folders(conn)
    orphan_items_deleted = delete_items_without_files(conn)
    delete_tags_without_items(conn)
    delete_log_items_without_item(conn)

    return UpdateFoldersResult(
        included_deleted=included_deleted,
        excluded_deleted=excluded_deleted,
        included_added=included_added,
        excluded_added=excluded_added,
        excluded_folder_files_deleted=excluded_folder_files_deleted,
        unavailable_files_deleted=unavailable_files_deleted,
        orphan_files_deleted=orphan_files_deleted,
        orphan_items_deleted=orphan_items_deleted,
        scan_ids=scan_ids,
    )


def rescan_all_folders(
    conn: sqlite3.Connection, delete_unavailable: bool = True
):
    """
    Rescan all included folders in the database and update the database with the results.
    Executes the related cleanup operations.

    """
    scan_ids = execute_folder_scan(conn)

    if delete_unavailable:
        unavailable_files_deleted = delete_unavailable_files(conn)
        orphan_items_deleted = delete_items_without_files(conn)
        delete_tags_without_items(conn)
        delete_log_items_without_item(conn)
    else:
        unavailable_files_deleted = 0
        orphan_items_deleted = 0

    return scan_ids, unavailable_files_deleted, orphan_items_deleted
