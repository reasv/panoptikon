import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List

from panoptikon.config_type import SystemConfig
from panoptikon.db.files import (
    add_file_scan,
    delete_files_not_allowed,
    delete_items_without_files,
    delete_unavailable_files,
    mark_unavailable_files,
    update_file_data,
    update_file_scan,
)
from panoptikon.db.folders import (
    add_folder_to_database,
    delete_files_not_under_included_folders,
    delete_files_under_excluded_folders,
    delete_folders_not_in_list,
    get_folders_from_database,
)
from panoptikon.db.storage import (
    delete_orphaned_frames,
    delete_orphaned_thumbnails,
)
from panoptikon.files import (
    deduplicate_paths,
    ensure_thumbnail_exists,
    scan_files,
)
from panoptikon.utils import normalize_path

logger = logging.getLogger(__name__)


def check_folder_validity(folder: str) -> bool:
    """
    Check if the given folder is valid.
    """
    if not os.path.exists(folder):
        logger.error(f"Path {folder} does not exist. Skipping...")
        return False
    if not os.path.isdir(folder):
        logger.error(f"Path {folder} is not a directory. Skipping...")
        return False
    if not os.listdir(folder):
        logger.error(f"Folder {folder} is empty. Skipping...")
        return False
    return True


def execute_folder_scan(
    conn: sqlite3.Connection,
    system_config: SystemConfig,
    included_folders: None | List[str] = None,
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
    # Ensure that all included_folders are valid
    included_folders = [
        folder for folder in included_folders if check_folder_validity(folder)
    ]
    excluded_folders = get_folders_from_database(conn, included=False)
    starting_points = deduplicate_paths(included_folders)
    scan_time = datetime.now().isoformat()
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
        time_hashing, time_metadata, time_thumbgen = 0.0, 0.0, 0.0
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
            thumbgen_start = time.time()
            try:
                ensure_thumbnail_exists(conn, file_data.sha256, file_data.path)
            except Exception as e:
                logger.error(
                    f"Error generating thumbnail for {file_data.path}: {e}"
                )
            time_thumbgen += time.time() - thumbgen_start
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
            thumbgen_time=time_thumbgen,
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
    system_config: SystemConfig,
):
    """
    Update the database with the new `included_folders` and `excluded_folders` lists.
    Any folders that are in the database but not in the new lists will be DELETED.
    Any files under the `excluded_folders` will be DELETED.
    Any files not under the `included_folders` will be DELETED.
    Any orphaned items without files will be DELETED.
    """
    new_included_folders = clean_folder_list(system_config.included_folders)
    new_excluded_folders = clean_folder_list(system_config.excluded_folders)

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

    scan_ids = execute_folder_scan(
        conn, system_config=system_config, included_folders=included_added
    )

    if system_config.remove_unavailable_files:
        unavailable_files_deleted = delete_unavailable_files(conn)
    else:
        unavailable_files_deleted = 0

    excluded_folder_files_deleted = delete_files_under_excluded_folders(conn)
    orphan_files_deleted = delete_files_not_under_included_folders(conn)
    rule_files_deleted = delete_files_not_allowed(conn, system_config)
    orphan_items_deleted = delete_items_without_files(conn)
    delete_orphaned_frames(conn)
    delete_orphaned_thumbnails(conn)

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


def is_resync_needed(
    conn: sqlite3.Connection,
    system_config: SystemConfig,
):
    """
    Check if the database needs to be updated with the new `included_folders` and `excluded_folders` lists.
    """
    new_included_folders = clean_folder_list(system_config.included_folders)
    new_excluded_folders = clean_folder_list(system_config.excluded_folders)

    current_included_folders = get_folders_from_database(conn, included=True)
    current_excluded_folders = get_folders_from_database(conn, included=False)

    # Sort the lists to make comparison easier
    new_included_folders.sort()
    new_excluded_folders.sort()
    current_included_folders.sort()
    current_excluded_folders.sort()
    # Check lengths first
    if len(new_included_folders) != len(current_included_folders):
        return True
    if len(new_excluded_folders) != len(current_excluded_folders):
        return True
    # Check each element
    for i, folder in enumerate(new_included_folders):
        if folder != current_included_folders[i]:
            return True
    for i, folder in enumerate(new_excluded_folders):
        if folder != current_excluded_folders[i]:
            return True


def clean_folder_list(folder_list: List[str]) -> List[str]:
    """
    Clean up the folder list by removing any empty strings and normalizing the paths.
    """
    return [normalize_path(p) for p in folder_list if len(p.strip()) > 0]


def rescan_all_folders(conn: sqlite3.Connection, system_config: SystemConfig):
    """
    Rescan all included folders in the database and update the database with the results.
    Executes the related cleanup operations.

    """
    scan_ids = execute_folder_scan(conn, system_config=system_config)
    if system_config.remove_unavailable_files:
        unavailable_files_deleted = delete_unavailable_files(conn)
    else:
        unavailable_files_deleted = 0

    rule_files_deleted = delete_files_not_allowed(conn, system_config)
    orphan_items_deleted = delete_items_without_files(conn)

    delete_orphaned_frames(conn)
    delete_orphaned_thumbnails(conn)

    return (
        scan_ids,
        unavailable_files_deleted,
        orphan_items_deleted,
        rule_files_deleted,
    )
