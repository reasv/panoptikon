import sqlite3
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, List, Sequence, Tuple

from src.db.setters import get_setter_id

if TYPE_CHECKING:
    import src.data_extractors.models as models

import logging

from src.db import get_item_id
from src.db.files import get_existing_file_for_sha256
from src.db.rules.build_filters import build_multirule_query
from src.db.rules.rules import get_rules_for_setter
from src.db.rules.types import combine_rule_item_filters
from src.db.utils import pretty_print_SQL
from src.types import ItemData, LogRecord, OutputDataType

logger = logging.getLogger(__name__)


def add_data_log(
    conn: sqlite3.Connection,
    scan_time: str,
    threshold: float | None,
    types: List[str],
    setter: str,
    batch_size: int,
):
    # Remove any incomplete logs before starting a new one
    remove_incomplete_jobs(conn)
    cursor = conn.cursor()
    cursor.execute(
        """
            INSERT INTO data_jobs (completed)
            VALUES (0)
        """
    )
    # get job id
    job_id = cursor.lastrowid
    assert job_id is not None, "No job was inserted"

    cursor.execute(
        """
    INSERT INTO data_log (
        start_time,
        end_time,
        type,
        setter,
        threshold,
        batch_size,
        job_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            datetime.now().isoformat(),
            ", ".join(types),
            setter,
            threshold,
            batch_size,
            job_id,
        ),
    )
    assert cursor.lastrowid is not None, "No log was inserted"
    # We refer to the job id instead of the log id
    # That way, we can delete the job, and rely on foreign key constraints
    # to delete all associated data, while keeping the log entry
    # as the log entry has SET NULL constraints on the job_id
    return job_id


def update_log(
    conn: sqlite3.Connection,
    job_id: int,
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
        UPDATE data_log
        SET end_time = ?,
        image_files = ?,
        video_files = ?,
        other_files = ?,
        total_segments = ?,
        errors = ?,
        total_remaining = ?,
        data_load_time = ?,
        inference_time = ?
        WHERE job_id = ?
    """,
        (
            datetime.now().isoformat(),
            image_files,
            video_files,
            other_files,
            total_segments,
            errors,
            total_remaining,
            data_load_time,
            inference_time,
            job_id,
        ),
    )
    if finished:
        cursor.execute(
            """
            UPDATE data_jobs
            SET completed = 1
            WHERE id = ?
        """,
            (job_id,),
        )


def remove_incomplete_jobs(conn: sqlite3.Connection):
    """
    Remove any jobs that are incomplete.
    This is done to ensure that the database does not contain
    any incomplete jobs. As a result of foreign key constraints,
    any data extracted for these jobs will also be deleted.
    This ensures that jobs are atomic.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM data_jobs
        WHERE completed = 0
    """
    )


def get_all_data_logs(conn: sqlite3.Connection) -> List[LogRecord]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            data_log.id,
            start_time,
            end_time,
            COALESCE(COUNT(DISTINCT item_data.id), 0) AS distinct_item_count,
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
            inference_time,
            CASE 
                WHEN data_log.job_id IS NULL THEN 1
                ELSE 0
            END AS failed,
            CASE
                WHEN data_jobs.completed = 1 THEN 1
                ELSE 0
            END AS completed
        FROM data_log
        LEFT JOIN item_data 
            ON item_data.job_id = data_log.job_id
            AND item_data.job_id IS NOT NULL
            AND item_data.is_placeholder = 0
        LEFT JOIN data_jobs
            ON data_log.job_id = data_jobs.id
        GROUP BY data_log.id
        ORDER BY start_time DESC;
        """
    )
    log_records = cursor.fetchall()
    return [LogRecord(*log_record) for log_record in log_records]


def add_item_data(
    conn: sqlite3.Connection,
    item: str,
    setter_name: str,
    job_id: int,
    data_type: OutputDataType,
    index: int,
    src_data_id: int | None = None,
    is_placeholder: bool = False,
):
    cursor = conn.cursor()
    item_id = get_item_id(conn, item)
    assert item_id is not None, "Item does not exist in the database"
    if src_data_id is None:
        is_origin = True
    else:
        is_origin = None

    cursor.execute(
        """
    INSERT INTO item_data
    (job_id, item_id, setter_id, data_type, idx, is_origin, source_id, is_placeholder)
    SELECT ?, ?, setters.id, ?, ?, ?, ?, ?
    FROM setters
    WHERE setters.name = ?;
    """,
        (
            job_id,
            item_id,
            data_type,
            index,
            is_origin,
            src_data_id,
            is_placeholder,
            setter_name,
        ),
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
    user_rules = get_rules_for_setter(conn, model_opts.setter_name())
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
        item = ItemData(*row, path="", item_data_ids=[])
        remaining_count -= 1
        if file := get_existing_file_for_sha256(conn, item.sha256):
            item.path = file.path

            if model_opts.target_entities() != ["items"]:
                # This model operates on derived data, not the items themselves
                item.item_data_ids = get_unprocessed_item_data_for_item(
                    conn,
                    item=item.sha256,
                    data_types=model_opts.target_entities(),
                    setter_name=model_opts.setter_name(),
                )
                item.item_data_ids.sort()
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
    SELECT DISTINCT ie.data_type, s.name
    FROM item_data ie
    JOIN setters s ON ie.setter_id = s.id;
    """

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results


def get_unprocessed_item_data_for_item(
    conn: sqlite3.Connection,
    item: str,
    data_types: Sequence[str],
    setter_name: str,
) -> List[int]:
    """
    Find all item associated data of the specified data_types for this item
    that have not yet been processed by the specified setter.
    """
    item_id = get_item_id(conn, item)
    setter_id = get_setter_id(conn, setter_name)
    data_type_condition = ", ".join(["?" for _ in data_types])
    query = f"""
        SELECT data_src.id
        FROM item_data AS data_src
        WHERE data_src.item_id = ?
        AND data_src.data_type IN ({data_type_condition})
        AND data_src.is_placeholder = 0
        AND NOT EXISTS (
            SELECT 1
            FROM item_data AS data_derived
            WHERE data_derived.source_id = data_src.id
            AND data_derived.setter_id = ?
        )
    """
    params = (item_id, *data_types, setter_id)

    cursor = conn.cursor()
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()

    return [result[0] for result in results]
