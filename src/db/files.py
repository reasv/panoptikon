import logging
import os
import sqlite3
from typing import List

from src.db import get_item_id
from src.db.rules.build_filters import build_multirule_query
from src.db.rules.rules import get_rules_for_setter
from src.db.utils import pretty_print_SQL
from src.types import FileRecord, FileScanData, FileScanRecord

logger = logging.getLogger(__name__)


def update_file_data(
    conn: sqlite3.Connection, time_added: str, scan_id: int, data: FileScanData
):

    sha256 = data.sha256
    meta = data.item_metadata

    cursor = conn.cursor()
    item_id = get_item_id(conn, sha256)
    if meta and item_id is None:
        # Insert the item into the database
        cursor.execute(
            """
        INSERT INTO items
        (sha256, md5, type, size, time_added, width, height, duration, audio_tracks, video_tracks, subtitle_tracks)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                sha256,
                meta.md5,
                meta.mime_type,
                meta.size,
                time_added,
                meta.width,
                meta.height,
                meta.duration,
                meta.audio_tracks,
                meta.video_tracks,
                meta.subtitle_tracks,
            ),
        )
        assert (
            cursor.lastrowid is not None
        ), f"Item not inserted for sha256: {sha256} ({data.path})"
        # Get the rowid of the inserted item, if it was inserted
        item_id = cursor.lastrowid
        item_inserted = True
    else:
        assert (
            item_id is not None
        ), f"Item not found and no meta given for sha256: {sha256} ({data.path})"
        item_inserted = False

    if not data.new_file_hash:
        # Path exists and hash has not changed, update scan_id and available
        # Potentially, the last_modified time has changed, update it
        # Though this is a weird special case,
        # given that the hash is the same
        file_update_result = cursor.execute(
            """
        UPDATE files
        SET scan_id = ?, available = TRUE, last_modified = ?
        WHERE path = ?
        """,
            (
                scan_id,
                data.last_modified,
                data.path,
            ),
        )

        file_updated = file_update_result.rowcount > 0
        file_deleted = False
        file_inserted = False
        return item_inserted, file_updated, file_deleted, file_inserted

    # The file is either new or has a new hash
    # Files are immutable, therefore,
    # when their hashes change,
    # they must be utterly destroyed,
    # and then reborn from the ashes
    file_delete_result = cursor.execute(
        "DELETE FROM files WHERE path = ?", (data.path,)
    )
    # If the file was not in the database, it was not deleted
    file_deleted = file_delete_result.rowcount > 0

    filename = os.path.basename(data.path)
    # Path does not exist or has been modified, insert new
    file_insert_result = cursor.execute(
        """
    INSERT INTO files
    (sha256, item_id, path, filename, last_modified, scan_id, available)
    VALUES (?, ?, ?, ?, ?, ?, TRUE)
    """,
        (sha256, item_id, data.path, filename, data.last_modified, scan_id),
    )
    file_inserted = file_insert_result.rowcount > 0

    file_updated = False
    return item_inserted, file_updated, file_deleted, file_inserted


def add_file_scan(conn: sqlite3.Connection, scan_time: str, path: str) -> int:
    """
    Logs a file scan into the database
    """
    cursor = conn.cursor()
    insert_result = cursor.execute(
        """
    INSERT INTO file_scans (start_time, path)
    VALUES (?, ?)
    """,
        (
            scan_time,
            path,
        ),
    )
    # Return the row id of the inserted record
    assert insert_result.lastrowid is not None, "No row id returned"
    return insert_result.lastrowid


def update_file_scan(
    conn: sqlite3.Connection,
    scan_id: int,
    end_time: str,
    new_items: int,
    unchanged_files: int,
    new_files: int,
    modified_files: int,
    marked_unavailable: int,
    errors: int,
    total_available: int,
    false_changes: int,
    metadata_time: float,
    hashing_time: float,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE file_scans
    SET 
        end_time = ?,
        new_items = ?,
        unchanged_files = ?, 
        new_files = ?,
        modified_files = ?,
        marked_unavailable = ?, 
        errors = ?,
        total_available = ?,
        false_changes = ?,
        metadata_time = ?,
        hashing_time = ?
    WHERE id = ?
    """,
        (
            end_time,
            new_items,
            unchanged_files,
            new_files,
            modified_files,
            marked_unavailable,
            errors,
            total_available,
            false_changes,
            metadata_time,
            hashing_time,
            scan_id,
        ),
    )


def get_file_scan_by_id(
    conn: sqlite3.Connection, scan_id: int
) -> FileScanRecord | None:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT *
    FROM file_scans
    WHERE id = ?
    """,
        (scan_id,),
    )
    scan_record = cursor.fetchone()
    if scan_record:
        return FileScanRecord(*scan_record)
    return None


def get_all_file_scans(conn: sqlite3.Connection) -> List[FileScanRecord]:
    cursor = conn.cursor()
    # Order by start_time in descending order
    cursor.execute("SELECT * FROM file_scans ORDER BY start_time DESC")
    scan_records = cursor.fetchall()
    return [FileScanRecord(*scan_record) for scan_record in scan_records]


def mark_unavailable_files(conn: sqlite3.Connection, scan_id: int, path: str):
    """
    Mark files as unavailable if their path is a subpath of `path`
    and they were not seen during the scan `scan_id`
    """
    cursor = conn.cursor()

    # Count files to be marked as unavailable
    precount_result = cursor.execute(
        """
    SELECT COUNT(*)
    FROM files
    WHERE scan_id != ?
    AND path LIKE ? || '%'
    """,
        (scan_id, path),
    )

    marked_unavailable = precount_result.fetchone()[0]

    # If a file has not been seen in the scan `scan_id` mark it as unavailable
    cursor.execute(
        """
        UPDATE files
        SET available = FALSE
        WHERE scan_id != ?
        AND path LIKE ? || '%'
    """,
        (scan_id, path),
    )

    # Count available files
    result_available = cursor.execute(
        """
        SELECT COUNT(*)
        FROM files
        WHERE available = TRUE
        AND path LIKE ? || '%'
    """,
        (path,),
    )
    available_files: int = result_available.fetchone()[0]

    return marked_unavailable, available_files


def get_file_by_path(conn: sqlite3.Connection, path: str):
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT files.sha256, files.last_modified
    FROM files
    JOIN items ON files.sha256 = items.sha256
    WHERE files.path = ?
    """,
        (path,),
    )

    row = cursor.fetchone()

    if not row:
        return None
    else:
        sha256, last_modified = row
        return FileRecord(sha256=sha256, path=path, last_modified=last_modified)


def get_existing_file_for_sha256(
    conn: sqlite3.Connection, sha256: str
) -> FileRecord | None:
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT path, last_modified
    FROM files
    WHERE sha256 = ?
    ORDER BY available DESC
    """,
        (sha256,),
    )

    while row := cursor.fetchone():
        path, last_modified = row
        if os.path.exists(path):
            return FileRecord(sha256, path, last_modified)

    return None


def delete_unavailable_files(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    DELETE FROM files
    WHERE available = 0
    """
    )
    return result.rowcount


def delete_items_without_files(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM items
        WHERE rowid IN (
            SELECT items.id
            FROM items
            LEFT JOIN files ON files.item_id = items.id
            WHERE files.id IS NULL
            LIMIT ?
        )
        """,
            (batch_size,),
        )

        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows

        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break

    return total_deleted


def get_all_mime_types(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT type FROM items")
    mime_types = [row[0] for row in cursor.fetchall()]
    general_types = set()
    for mime_type in mime_types:
        general_types.add(mime_type.split("/")[0] + "/")

    mime_types.extend(general_types)
    mime_types.sort()
    return mime_types


def delete_files_not_allowed(conn: sqlite3.Connection):
    user_rules = get_rules_for_setter(conn, "files", "file_scan")
    if not user_rules:
        logger.debug("No rules for files, skipping deletion")
        return 0
    filters = [rule.filters for rule in user_rules]
    query, params = build_multirule_query(
        filters,
    )
    count_query = f"""
        WITH
        {query}
        SELECT COUNT(*)
        FROM multirule_results
    """
    count_all_items = f"""
        SELECT COUNT(*)
        FROM items
    """
    pretty_print_SQL(count_query, params)
    cursor = conn.cursor()
    cursor.execute(count_query, params)
    count: int = cursor.fetchone()[0]
    cursor.execute(count_all_items)
    total_items: int = cursor.fetchone()[0]
    logger.debug(f"{count} items out of {total_items} items match the rules")

    items_deleted = 0
    while True:
        cursor.execute(
            f"""
            WITH
            {query}
            DELETE FROM files
            WHERE item_id IN (
                SELECT item_id
                FROM files
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM multirule_results
                    WHERE multirule_results.id = files.item_id
                )
                LIMIT ?
            );
        """,
            params + [1000],
        )
        items_deleted += cursor.rowcount
        if cursor.rowcount == 0:
            break

    if items_deleted > 0:
        logger.warning(f"Deleted {items_deleted} files")
    else:
        logger.debug("No items deleted")

    return items_deleted
