import os
import sqlite3
from typing import List

from src.types import FileRecord, FileScanData, FileScanRecord


def update_file_data(
    conn: sqlite3.Connection, scan_time: str, file_data: FileScanData
):
    cursor = conn.cursor()
    sha256 = file_data.sha256
    md5 = file_data.md5
    mime_type = file_data.mime_type
    file_size = file_data.size
    path = file_data.path
    last_modified = file_data.last_modified
    path_in_db = file_data.path_in_db
    file_modified = file_data.modified

    item_insert_result = cursor.execute(
        """
    INSERT INTO items (sha256, md5, type, size, time_added)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO NOTHING
    """,
        (sha256, md5, mime_type, file_size, scan_time),
    )

    # We need to check if the item was inserted
    item_inserted = item_insert_result.rowcount > 0

    # Get the rowid of the inserted item, if it was inserted
    item_rowid: int | None = cursor.lastrowid if item_inserted else None

    file_updated = False
    if path_in_db and not file_modified:
        # Path exists and has not changed, update last_seen and available
        file_update_result = cursor.execute(
            """
        UPDATE files
        SET last_seen = ?, available = TRUE
        WHERE path = ?
        """,
            (scan_time, path),
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
        INSERT INTO files (sha256, item_id, path, filename, last_modified, last_seen, available)
        VALUES (?, ?, ?, ?, ?, ?, TRUE)
        """,
            (sha256, item_rowid, path, filename, last_modified, scan_time),
        )
        file_inserted = file_insert_result.rowcount > 0

    return item_inserted, file_updated, file_deleted, file_inserted


def add_file_scan(
    conn: sqlite3.Connection,
    scan_time: str,
    end_time: str,
    path: str,
    new_items: int,
    unchanged_files: int,
    new_files: int,
    modified_files: int,
    marked_unavailable: int,
    errors: int,
    total_available: int,
):
    """
    Logs a file scan into the database
    """
    cursor = conn.cursor()
    insert_result = cursor.execute(
        """
    INSERT INTO file_scans (start_time, end_time, path, total_available, new_items, unchanged_files, new_files, modified_files, marked_unavailable, errors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            end_time,
            path,
            total_available,
            new_items,
            unchanged_files,
            new_files,
            modified_files,
            marked_unavailable,
            errors,
        ),
    )
    # Return the row id of the inserted record
    return insert_result.lastrowid


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


def mark_unavailable_files(conn: sqlite3.Connection, scan_time: str, path: str):
    """
    Mark files as unavailable if their path is a subpath of `path`
    and they were not seen during the scan at `scan_time`
    """
    cursor = conn.cursor()

    # Count files to be marked as unavailable
    precount_result = cursor.execute(
        """
    SELECT COUNT(*)
    FROM files
    WHERE last_seen != ?
    AND path LIKE ?
    """,
        (scan_time, path + "%"),
    )

    marked_unavailable = precount_result.fetchone()[0]

    # If a file has not been seen in scan that happened at scan_time, mark it as unavailable
    cursor.execute(
        """
        UPDATE files
        SET available = FALSE
        WHERE last_seen != ?
        AND path LIKE ?
    """,
        (scan_time, path + "%"),
    )

    # Count available files
    result_available = cursor.execute(
        """
        SELECT COUNT(*)
        FROM files
        WHERE available = TRUE
        AND path LIKE ?
    """,
        (path + "%",),
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


def hard_update_items_available(conn: sqlite3.Connection):
    # This function is used to update the availability of files in the database
    cursor = conn.cursor()

    cursor.execute("SELECT path FROM files")
    files = cursor.fetchall()

    for (path,) in files:
        available = os.path.exists(path)
        cursor.execute(
            """
        UPDATE files
        SET Available = ?
        WHERE path = ?
        """,
            (available, path),
        )


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
            LEFT JOIN files ON files.id = items.id
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
