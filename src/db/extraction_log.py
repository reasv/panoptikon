import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from src.db import get_item_id
from src.db.files import get_existing_file_for_sha256
from src.types import ItemWithPath


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str
    type: str
    setter: str
    threshold: float | None
    batch_size: int
    image_files: int
    video_files: int
    other_files: int
    total_segments: int
    errors: int
    total_remaining: int


def add_data_extraction_log(
    conn: sqlite3.Connection,
    scan_time: str,
    type: str,
    setter: str,
    threshold: float | None,
    batch_size: int,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO data_extraction_log (
        start_time,
        type,
        setter,
        threshold,
        batch_size
    )
    VALUES (?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            type,
            setter,
            threshold,
            batch_size,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def update_log(
    conn: sqlite3.Connection,
    log_id: int,
    image_files: int,
    video_files: int,
    other_files: int,
    total_segments: int,
    errors: int,
    total_remaining: int,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE data_extraction_log
    SET end_time = ?,
    image_files = ?,
    video_files = ?,
    other_files = ?,
    total_segments = ?,
    errors = ?,
    total_remaining = ?
    WHERE id = ?
    """,
        (
            datetime.now().isoformat(),
            image_files,
            video_files,
            other_files,
            total_segments,
            errors,
            total_remaining,
            log_id,
        ),
    )


def get_all_data_extraction_logs(conn: sqlite3.Connection) -> List[LogRecord]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        id,
        start_time,
        end_time,
        type,
        setter,
        threshold,
        batch_size,
        image_files,
        video_files,
        other_files,
        total_segments,
        errors,
        total_remaining
        FROM data_extraction_log
        ORDER BY start_time DESC"""
    )
    log_records = cursor.fetchall()
    return [LogRecord(*log_record) for log_record in log_records]


def add_item_to_log(
    conn: sqlite3.Connection,
    item: str,
    log_id: int,
):
    cursor = conn.cursor()
    item_id = get_item_id(conn, item)
    cursor.execute(
        """
    INSERT INTO extraction_log_items (item_id, log_id)
    VALUES (?, ?)
    """,
        (item_id, log_id),
    )


def get_items_missing_data_extraction(
    conn: sqlite3.Connection,
    model_type: str,
    setter: str,
    mime_type_filter: List[str] | None = None,
):
    """
    Get all items that have not been scanned by the given setter.
    More efficient than get_items_missing_tags as it does not require
    a join with the tags table.
    It also avoids joining with the files table to get the path,
    instead getting paths one by one.
    """
    clauses = f"""
    FROM items
    WHERE NOT EXISTS (
        SELECT 1
        FROM extraction_log_items
        JOIN data_extraction_log
        ON extraction_log_items.log_id = data_extraction_log.id
        WHERE items.id = extraction_log_items.item_id
        AND data_extraction_log.type = ?
        AND data_extraction_log.setter = ?
    )
    """
    if mime_type_filter:
        clauses += "AND ("
        for i, _ in enumerate(mime_type_filter):
            if i == 0:
                clauses += "items.type LIKE ? || '%'"
            else:
                clauses += f" OR items.type LIKE ? || '%'"
        clauses += ")"

    params = [
        model_type,
        setter,
        *(mime_type_filter if mime_type_filter else ()),
    ]

    count_query = f"""
    SELECT COUNT(*)
    {clauses}
    """
    cursor = conn.cursor()

    cursor.execute(
        count_query,
        params,
    )
    total_count = cursor.fetchone()[0]

    cursor.execute(
        f"""
    SELECT
    items.sha256,
    items.md5,
    items.type,
    items.size,
    items.time_added
    {clauses}
    """,
        params,
    )

    remaining_count: int = total_count
    while row := cursor.fetchone():
        item = ItemWithPath(*row, "")  # type: ignore
        remaining_count -= 1
        if file := get_existing_file_for_sha256(conn, item.sha256):
            item.path = file.path
            yield item, remaining_count
        else:
            # If no working path is found, skip this item
            continue


def remove_setter_from_items(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()

    result = cursor.execute(
        """
    DELETE FROM extraction_log_items
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = ?
    )
    """,
        (setter, model_type),
    )

    items_setter_removed = result.rowcount
    return items_setter_removed


def get_existing_type_setter_pairs(
    conn: sqlite3.Connection,
) -> List[Tuple[str, str]]:
    """
    Returns all the currently existing (type, setter) pairs from the data_extraction_log table.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing (type, setter) pairs.
    """
    query = """
    SELECT DISTINCT type, setter
    FROM data_extraction_log
    """

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results


def delete_log_items_without_item(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM extraction_log_items
        WHERE rowid IN (
            SELECT extraction_log_items.rowid
            FROM extraction_log_items
            LEFT JOIN items ON items.id = extraction_log_items.item_id
            WHERE items.id IS NULL
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
