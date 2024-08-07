import os
import sqlite3
from typing import List

from src.types import FileRecord, FileScanData, FileScanRecord


def update_file_data(
    conn: sqlite3.Connection, time_added: str, scan_id: int, meta: FileScanData
):
    cursor = conn.cursor()
    sha256 = meta.sha256
    path = meta.path
    last_modified = meta.last_modified
    path_in_db = meta.path_in_db
    file_modified = meta.modified

    item_insert_result = cursor.execute(
        """
    INSERT INTO items (sha256, md5, type, size, time_added, width, height, duration, audio_tracks, video_tracks, subtitle_tracks)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO NOTHING
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

    # We need to check if the item was inserted
    item_inserted = item_insert_result.rowcount > 0

    # Get the rowid of the inserted item, if it was inserted
    item_rowid: int | None = cursor.lastrowid if item_inserted else None

    file_updated = False
    if path_in_db and not file_modified:
        # Path exists and has not changed, update scan_id and available
        file_update_result = cursor.execute(
            """
        UPDATE files
        SET scan_id = ?, available = TRUE
        WHERE path = ?
        """,
            (scan_id, path),
        )

        file_updated = file_update_result.rowcount > 0

    file_deleted = False
    file_inserted = False
    if not path_in_db or file_modified:
        # If the path already exists, delete the old entry
        file_delete_result = cursor.execute(
            "DELETE FROM files WHERE path = ?", (path,)
        )
        file_deleted = file_delete_result.rowcount > 0

        if not item_rowid:
            # If the item was not inserted, get the rowid from the database
            item_rowid = cursor.execute(
                "SELECT id FROM items WHERE sha256 = ?", (sha256,)
            ).fetchone()[0]

        filename = os.path.basename(path)
        # Path does not exist or has been modified, insert new
        file_insert_result = cursor.execute(
            """
        INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available)
        VALUES (?, ?, ?, ?, ?, ?, TRUE)
        """,
            (sha256, item_rowid, path, filename, last_modified, scan_id),
        )
        file_inserted = file_insert_result.rowcount > 0

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
):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE file_scans
    SET end_time = ?, new_items = ?, unchanged_files = ?, new_files = ?, modified_files = ?, marked_unavailable = ?, errors = ?, total_available = ?
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
    SELECT files.*, items.md5, items.size
    FROM files
    JOIN items ON files.sha256 = items.sha256
    WHERE files.path = ?
    """,
        (path,),
    )

    file_record = cursor.fetchone()

    if file_record:
        # Get column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]
        # Construct a dictionary using column names and file record
        file_dict = dict(zip(column_names, file_record))
    else:
        file_dict = None

    return file_dict


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
