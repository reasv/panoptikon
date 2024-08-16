import sqlite3
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, List, Sequence, Tuple

if TYPE_CHECKING:
    import src.data_extractors.models as models

import logging

from src.db import get_item_id
from src.db.files import get_existing_file_for_sha256
from src.db.rules.build_filters import build_multirule_query
from src.db.rules.rules import get_rules_for_setter, get_rules_for_setter_id
from src.db.rules.types import combine_rule_item_filters
from src.db.setters import upsert_setter
from src.db.utils import pretty_print_SQL
from src.types import ItemWithPath, LogRecord

logger = logging.getLogger(__name__)


def add_data_extraction_log(
    conn: sqlite3.Connection,
    scan_time: str,
    type: str,
    setter: str,
    threshold: float | None,
    batch_size: int,
):
    # Remove any incomplete logs before starting a new one
    remove_incomplete_logs(conn)
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
    data_load_time: float,
    inference_time: float,
    finished: bool = False,
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
    total_remaining = ?,
    data_load_time = ?,
    inference_time = ?
    WHERE id = ?
    """,
        (
            datetime.now().isoformat() if finished else None,
            image_files,
            video_files,
            other_files,
            total_segments,
            errors,
            total_remaining,
            data_load_time,
            inference_time,
            log_id,
        ),
    )


def remove_incomplete_logs(conn: sqlite3.Connection):
    """
    Remove any logs that have a start time but no end time.
    This is done to ensure that the database does not contain
    any incomplete logs. As a result of foreign key constraints,
    any data extracted for these logs will also be deleted.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM data_extraction_log
        WHERE end_time IS NULL
    """
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
        total_remaining,
        data_load_time,
        inference_time
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
    data_type: str,
    previous_extraction_id: int | None = None,
):
    cursor = conn.cursor()
    item_id = get_item_id(conn, item)
    if previous_extraction_id is None:
        is_origin = True
    else:
        is_origin = None

    cursor.execute(
        """
    INSERT INTO items_extractions
    (item_id, log_id, setter_id, data_type, is_origin, source_extraction_id)
    SELECT ?, ?, log.setter_id, ?, ?, ?
    FROM data_extraction_log AS log
    WHERE log.id = ?
    """,
        (item_id, log_id, data_type, is_origin, previous_extraction_id, log_id),
    )
    # Return the ID of the new extraction
    assert cursor.lastrowid is not None, "No extraction was inserted"
    return cursor.lastrowid


def get_items_missing_data_extraction(
    conn: sqlite3.Connection, model_opts: "models.ModelOpts"
):
    """
    Get all items that should be processed by the given setter.
    More efficient than get_items_missing_tags as it does not require
    a join with the tags table.
    It also avoids joining with the files table to get the path,
    instead getting paths one by one.
    """
    model_filters = model_opts.item_extraction_rules()
    user_rules = get_rules_for_setter(
        conn, model_opts.data_type(), model_opts.setter_name()
    )
    # Merge each user rule with the model's buit-in filters
    combined_filters = [
        combine_rule_item_filters(model_filters, user_rule.filters)
        for user_rule in user_rules
    ]
    # If no user rules are present, use only the model's built-in filters
    if not combined_filters:
        combined_filters = [model_filters]

    query, params = build_multirule_query(
        combined_filters,
    )
    result_query = f"""
        WITH
        {query}
        SELECT
            items.sha256,
            items.md5,
            items.type,
            items.size,
            items.time_added
        FROM items JOIN multirule_results
        ON items.id = multirule_results.id
    """
    count_query = f"""
        WITH
        {query}
        SELECT COUNT(*)
        FROM multirule_results
    """
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

    logger.debug(f"Query took {time() - start_time:.2f}s")

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


def get_unprocessed_extractions_for_item(
    conn: sqlite3.Connection,
    item: str,
    input_type: Sequence[str],
    setter_id: int,
) -> List[int]:
    """
    Find all extractions of the specified input_type for this item
    that have not yet been processed by the specified setter.
    """
    item_id = get_item_id(conn, item)
    input_type_condition = ", ".join(["?" for _ in input_type])
    query = f"""
        SELECT ie.id
        FROM items_extractions AS ie
        JOIN setters AS s ON ie.setter_id = s.id
        WHERE ie.item_id = ?
        AND s.type IN ({input_type_condition})
        AND NOT EXISTS (
            SELECT 1
            FROM items_extractions AS ie2
            WHERE ie2.source_extraction_id = ie.id
            AND ie2.setter_id = ?
        )
    """
    params = (item_id, *input_type, setter_id)

    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()

    return [result[0] for result in results]
