import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List

from src.db.config import retrieve_system_config
from src.db.files import (
    add_file_scan,
    delete_files_not_allowed,
    delete_items_without_files,
    delete_unavailable_files,
    mark_unavailable_files,
    update_file_data,
    update_file_scan,
)
from src.db.folders import (
    add_folder_to_database,
    delete_files_not_under_included_folders,
    delete_files_under_excluded_folders,
    delete_folders_not_in_list,
    get_folders_from_database,
)
from src.files import deduplicate_paths, scan_files
from src.utils import normalize_path

logger = logging.getLogger(__name__)


def execute_folder_scan(
    conn: sqlite3.Connection, included_folders: None | List[str] = None
) -> list[int]:
    """
    Execute a scan of the files in the given `included_folders`,
    or all folders marked as `included` within the db, and update the database with the results.
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
    for folder in included_folders:
        if not os.path.exists(folder):
            raise FileNotFoundError(
                f"Folder {folder} does not exist. Aborting scan."
            )
        if not os.path.isdir(folder):
            raise NotADirectoryError(
                f"Path {folder} is not a directory. Aborting scan."
            )
        if not os.listdir(folder):
            raise FileNotFoundError(f"Folder {folder} is empty. Aborting scan.")

    excluded_folders = get_folders_from_database(conn, included=False)
    starting_points = deduplicate_paths(included_folders)
    scan_time = datetime.now().isoformat()
    system_config = retrieve_system_config(conn)
    logger.info(f"Scanning folders: {included_folders}")
    scan_ids = []
    for folder in starting_points:
        (
            new_items,
            unchanged_files,
            new_files,
            modified_files,
            errors,
            false_mod_timestamps,
        ) = (
            0,
            0,
            0,
            0,
            0,
            0,
        )
        time_hashing, time_metadata = 0.0, 0.0
        scan_id = add_file_scan(conn, scan_time, folder)
        scan_ids.append(scan_id)
        for file_data, hash_time, metadata_time in scan_files(
            conn,
            starting_points=[folder],
            excluded_paths=excluded_folders,
            include_images=system_config.scan_images,
            include_video=system_config.scan_video,
            include_audio=system_config.scan_audio,
            include_html=system_config.scan_html,
            include_pdf=system_config.scan_pdf,
        ):
            time_hashing += hash_time
            time_metadata += metadata_time
            if file_data is None:
                errors += 1
                continue
            if (
                file_data.new_file_timestamp == True
                and file_data.new_file_hash == False
            ):
                # File timestamp changed but hash is the same
                false_mod_timestamps += 1

            # Update the file data in the database
            (item_inserted, file_updated, file_deleted, file_inserted) = (
                update_file_data(
                    conn, time_added=scan_time, scan_id=scan_id, data=file_data
                )
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
            conn, scan_id=scan_id, path=folder
        )
        logger.info(
            f"Scan of {folder} complete. New items: {new_items}, Unchanged files: {unchanged_files}, New files: {new_files}, Modified files: {modified_files}, Marked unavailable: {marked_unavailable}, Errors: {errors}, Total available: {total_available}"
        )
        end_time = datetime.now().isoformat()
        update_file_scan(
            conn,
            scan_id=scan_id,
            end_time=end_time,
            new_items=new_items,
            unchanged_files=unchanged_files,
            new_files=new_files,
            modified_files=modified_files,
            marked_unavailable=marked_unavailable,
            errors=errors,
            total_available=total_available,
            false_changes=false_mod_timestamps,
            metadata_time=time_metadata,
            hashing_time=time_hashing,
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
    rule_files_deleted: int
    scan_ids: List[int]


def update_folder_lists(
    conn: sqlite3.Connection,
    included_folders: List[str],
    excluded_folders: List[str],
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

    system_config = retrieve_system_config(conn)

    if system_config.remove_unavailable_files:
        unavailable_files_deleted = delete_unavailable_files(conn)
    else:
        unavailable_files_deleted = 0

    excluded_folder_files_deleted = delete_files_under_excluded_folders(conn)
    orphan_files_deleted = delete_files_not_under_included_folders(conn)
    rule_files_deleted = delete_files_not_allowed(conn)
    orphan_items_deleted = delete_items_without_files(conn)

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
        rule_files_deleted=rule_files_deleted,
    )


def rescan_all_folders(conn: sqlite3.Connection):
    """
    Rescan all included folders in the database and update the database with the results.
    Executes the related cleanup operations.

    """
    scan_ids = execute_folder_scan(conn)
    system_config = retrieve_system_config(conn)
    if system_config.remove_unavailable_files:
        unavailable_files_deleted = delete_unavailable_files(conn)
    else:
        unavailable_files_deleted = 0

    rule_files_deleted = delete_files_not_allowed(conn)
    orphan_items_deleted = delete_items_without_files(conn)

    return (
        scan_ids,
        unavailable_files_deleted,
        orphan_items_deleted,
        rule_files_deleted,
    )
