import logging
import os
import sqlite3
from typing import List, Tuple

from panoptikon.config_type import SystemConfig
from panoptikon.db import get_item_id
from panoptikon.types import (
    FileRecord,
    FileScanData,
    FileScanRecord,
    ItemRecord,
)

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
        (sha256, md5, type, size, time_added, width, height, duration, audio_tracks, video_tracks, subtitle_tracks, blurhash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                sha256,
                meta.md5,
                meta.mime_type,
                data.file_size,
                time_added,
                meta.width,
                meta.height,
                meta.duration,
                meta.audio_tracks,
                meta.video_tracks,
                meta.subtitle_tracks,
                data.blurhash,
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
        if data.file_size is not None:
            if update_item_size(conn, item_id, data.file_size):
                logger.debug(f"Updated size for item {item_id} ({sha256})")
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
    thumbgen_time: float,
    blurhash_time: float,
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
        hashing_time = ?,
        thumbgen_time = ?,
        blurhash_time = ?
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
            round(metadata_time, 2),
            round(hashing_time, 2),
            round(thumbgen_time, 2),
            round(blurhash_time, 2),
            scan_id,
        ),
    )


def get_all_file_scans(
    conn: sqlite3.Connection,
    page: int | None = None,
    page_size: int | None = None,
) -> List[FileScanRecord]:
    page = page if page is not None else 1
    page = max(1, page)
    offset = (page - 1) * page_size if page_size is not None else 0
    cursor = conn.cursor()
    # Order by start_time in descending order
    cursor.execute(
        f"""
        SELECT
        id,
        start_time,        
        end_time,
        path,
        total_available,
        new_items,
        unchanged_files,
        new_files,
        modified_files,
        marked_unavailable,
        errors,
        false_changes,
        metadata_time,
        hashing_time,
        thumbgen_time,
        blurhash_time
        FROM file_scans
        ORDER BY start_time
        DESC
        {"LIMIT ? OFFSET ?" if page_size is not None else ""}
        """,
        (page_size, offset) if page_size is not None else (),
    )
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
    SELECT files.id, files.sha256, files.last_modified
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
        file_id, sha256, last_modified = row
        return FileRecord(
            id=file_id, sha256=sha256, path=path, last_modified=last_modified
        )


def get_existing_file_for_sha256(
    conn: sqlite3.Connection, sha256: str
) -> FileRecord | None:
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT id, path, last_modified
    FROM files
    WHERE sha256 = ?
    ORDER BY available DESC
    """,
        (sha256,),
    )

    while row := cursor.fetchone():
        file_id, path, last_modified = row
        if os.path.exists(path):
            return FileRecord(
                id=file_id,
                sha256=sha256,
                path=path,
                last_modified=last_modified,
            )

    return None


def get_existing_file_for_item_id(
    conn: sqlite3.Connection, item_id: int
) -> FileRecord | None:
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT id, sha256, path, last_modified, filename
    FROM files
    WHERE item_id = ?
    ORDER BY available DESC
    """,
        (item_id,),
    )

    while row := cursor.fetchone():
        file_id, sha256, path, last_modified, filename = row
        if os.path.exists(path):
            return FileRecord(file_id, sha256, path, last_modified, filename)

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


def delete_files_not_allowed(conn: sqlite3.Connection, config: SystemConfig):
    from panoptikon.data_extractors.types import JobInputData
    from panoptikon.db.pql.pql_model import (
        AndOperator,
        NotOperator,
        PQLQuery,
        QueryElement,
    )
    from panoptikon.db.pql.search import search_pql

    user_filters = [
        f.pql_query
        for f in config.job_filters
        if f.setter_names.index("file_scan") != -1
    ]
    # Flatten AND operators into a list of filters
    flattened_user_filters: List[QueryElement] = []
    for f in user_filters:
        if isinstance(f, AndOperator):
            flattened_user_filters.extend(f.and_)
        else:
            flattened_user_filters.append(f)
    if not flattened_user_filters:
        logger.debug("No rules for files, skipping deletion")
        return 0

    query = PQLQuery(
        query=NotOperator(
            not_=AndOperator(
                and_=flattened_user_filters,
            ),
        ),
        page_size=0,
        check_path=False,
    )
    logger.debug(
        f"File Scan Item Query: {(query.query or query).model_dump(exclude_defaults=True)}"
    )

    results_generator, result_count, rm, cm = search_pql(conn, query)
    cursor = conn.cursor()
    cursor.execute("""SELECT COUNT(*) FROM files""")
    total_files: int = cursor.fetchone()[0]
    if result_count > 0:
        logger.warning(
            f"{result_count} files out of {total_files} files do not match the rules"
        )
    else:
        logger.debug(f"All {total_files} items match the rules")

    assert result_count <= total_files, "Too many files violate the rules"
    logger.debug(f"{total_files} files in the database before deletion")
    # Delete files that do not match the rules
    for file in results_generator:
        cursor.execute(
            """
        DELETE FROM files
        WHERE id = ?
        """,
            (file.file_id,),
        )
        logger.debug(f"Deleted file {file.file_id} ({file.path})")

    cursor.execute("""SELECT COUNT(*) FROM files""")
    total_files_after: int = cursor.fetchone()[0]
    logger.debug(f"{total_files_after} files in the database after deletion")

    files_deleted = total_files - total_files_after
    if files_deleted > 0:
        logger.warning(
            f"Deleted {files_deleted} files due to file scan rules set by the user"
        )
    else:
        logger.debug("No items deleted")

    return files_deleted


def get_file_stats(
    conn: sqlite3.Connection,
) -> tuple[int, int]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM files
        """
    )
    total_files = cursor.fetchone()[0]
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM items
        """
    )
    total_items = cursor.fetchone()[0]

    return total_files, total_items


def get_item_metadata_by_sha256(
    conn: sqlite3.Connection, sha256: str
) -> Tuple[ItemRecord, List[FileRecord]] | Tuple[None, None]:
    cursor = conn.cursor()
    # SQL query to retrieve the item by sha256
    query = """
    SELECT
        id,
        sha256,
        md5,
        type,
        size,
        width,
        height,
        duration,
        audio_tracks,
        video_tracks,
        subtitle_tracks,
        blurhash,
        time_added
    FROM items
    WHERE sha256 = ?
    """
    cursor.execute(query, (sha256,))
    row = cursor.fetchone()

    # If the row exists, convert it to a dataclass
    if not row:
        return None, None

    item_record = ItemRecord(
        id=row[0],
        sha256=row[1],
        md5=row[2],
        type=row[3],
        size=row[4],
        width=row[5],
        height=row[6],
        duration=row[7],
        audio_tracks=row[8],
        video_tracks=row[9],
        subtitle_tracks=row[10],
        blurhash=row[11],
        time_added=row[12],
    )

    cursor.execute(
        """
    SELECT id, path, last_modified
    FROM files
    WHERE sha256 = ?
    ORDER BY available DESC
    """,
        (sha256,),
    )
    files: List[FileRecord] = []
    while row := cursor.fetchone():
        file_id, path, last_modified = row
        if os.path.exists(path):
            files.append(FileRecord(file_id, sha256, path, last_modified))

    return item_record, files


def get_sha256_for_item_id(
    conn: sqlite3.Connection, item_id: int
) -> str | None:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT sha256
    FROM items
    WHERE id = ?
    """,
        (item_id,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def get_sha256_for_file_id(
    conn: sqlite3.Connection, file_id: int
) -> str | None:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT sha256
    FROM files
    WHERE id = ?
    """,
        (file_id,),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def has_blurhash(conn: sqlite3.Connection, sha256: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT blurhash
    FROM items
    WHERE sha256 = ?
    """,
        (sha256,),
    )
    row = cursor.fetchone()
    if row:
        return row[0] is not None
    return False


def set_blurhash(conn: sqlite3.Connection, sha256: str, blurhash: str):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE items
    SET blurhash = ?
    WHERE sha256 = ?
    """,
        (blurhash, sha256),
    )


def update_item_size(conn: sqlite3.Connection, item_id: int, size: int):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE items
    SET size = ?
    WHERE id = ?
    """,
        (size, item_id),
    )
    return cursor.rowcount > 0
