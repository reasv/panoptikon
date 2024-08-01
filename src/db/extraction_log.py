import sqlite3
from datetime import datetime
from time import time
from typing import List, Tuple

from src.data_extractors.models import ModelOpts
from src.db import get_item_id
from src.db.files import get_existing_file_for_sha256
from src.db.rules.build_filters import build_query
from src.db.rules.types import MimeFilter, ProcessedItemsFilter
from src.db.setters import upsert_setter
from src.db.utils import pretty_print_SQL
from src.types import ItemWithPath, LogRecord


def add_data_extraction_log(
    conn: sqlite3.Connection,
    scan_time: str,
    type: str,
    setter: str,
    threshold: float | None,
    batch_size: int,
):
    setter_id = upsert_setter(conn, type, setter)
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO data_extraction_log (
        start_time,
        setter_id,
        type,
        setter,
        threshold,
        batch_size
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            setter_id,
            type,
            setter,
            threshold,
            batch_size,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid, setter_id


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
        setter_id,
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
        ORDER BY start_time DESC
        """
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
    INSERT INTO items_extractions (item_id, log_id, setter_id)
    SELECT ?, ?, log.setter_id
    FROM data_extraction_log AS log
    WHERE log.id = ?
    """,
        (item_id, log_id, log_id),
    )


def get_items_missing_data_extraction(
    conn: sqlite3.Connection, setter_id: int, model_opts: ModelOpts
):
    """
    Get all items that should be processed by the given setter.
    More efficient than get_items_missing_tags as it does not require
    a join with the tags table.
    It also avoids joining with the files table to get the path,
    instead getting paths one by one.
    """
    rules = model_opts.item_extraction_rules(setter_id)
    result_query, count_query, params = build_query(
        positive=rules.positive,
        negative=rules.negative,
    )
    pretty_print_SQL(result_query, params)
    start_time = time()
    cursor = conn.cursor()

    cursor.execute(
        count_query,
        params,
    )
    total_count = cursor.fetchone()[0]

    cursor.execute(
        result_query,
        params,
    )

    print(f"Query took {time() - start_time:.2f}s")

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


def get_existing_setters(
    conn: sqlite3.Connection,
) -> List[Tuple[str, str]]:
    """
    Returns all the currently existing (type, setter) pairs

    Args:
        conn (sqlite3.Connection): The SQLite database connection.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing (type, setter) pairs.
    """
    query = """
    SELECT DISTINCT type, name
    FROM setters
    JOIN items_extractions
    ON setters.id = items_extractions.setter_id
    UNION
    SELECT DISTINCT type, name
    FROM setters
    JOIN text_embeddings
    ON setters.id = text_embeddings.setter_id
    """

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results
