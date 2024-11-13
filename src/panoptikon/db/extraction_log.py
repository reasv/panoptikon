import os
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    import panoptikon.data_extractors.models as models

import logging

from panoptikon.config_type import SystemConfig
from panoptikon.db import get_item_id
from panoptikon.db.files import get_existing_file_for_sha256
from panoptikon.types import LogRecord, OutputDataType

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
    finished_value = 1 if finished else 0
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
        inference_time = ?,
        completed = ?
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
            finished_value,
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


def get_all_data_logs(
    conn: sqlite3.Connection,
    page: int | None = None,
    page_size: int | None = None,
) -> List[LogRecord]:
    page = page if page is not None else 1
    page = max(1, page)
    offset = (page - 1) * page_size if page_size is not None else 0
    cursor = conn.cursor()
    cursor.execute(
        f"""
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
                WHEN data_log.completed = 1 THEN 0
                WHEN data_log.job_id IS NULL THEN 1
                ELSE 0
            END AS failed,
            data_log.completed
            data_jobs.completed AS status
        FROM data_log
        LEFT JOIN item_data 
            ON item_data.job_id = data_log.job_id
            AND item_data.job_id IS NOT NULL
            AND item_data.is_placeholder = 0
        LEFT JOIN data_jobs
            ON data_log.job_id = data_jobs.id
        GROUP BY data_log.id
        ORDER BY start_time DESC
        {"LIMIT ? OFFSET ?" if page_size is not None else ""}
        """,
        (page_size, offset) if page_size is not None else (),
    )
    log_records = cursor.fetchall()
    return [LogRecord(*log_record) for log_record in log_records]


def delete_data_job_by_log_id(
    conn: sqlite3.Connection, data_log_id: int
) -> None:
    cursor = conn.cursor()

    # Fetch the corresponding job_id from data_log
    cursor.execute(
        """
        SELECT job_id
        FROM data_log
        WHERE id = ?
        """,
        (data_log_id,),
    )

    job_id_row = cursor.fetchone()

    # Check if a job_id was found
    if job_id_row and job_id_row[0] is not None:
        job_id = job_id_row[0]

        # Delete the corresponding row from data_jobs
        cursor.execute(
            """
            DELETE FROM data_jobs
            WHERE id = ?
            """,
            (job_id,),
        )


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
    conn: sqlite3.Connection,
    config: SystemConfig,
    model_opts: "models.ModelOpts",
):
    """
    Get all items that should be processed by the given setter.
    More efficient than get_items_missing_tags as it does not require
    a join with the tags table.
    It also avoids joining with the files table to get the path,
    instead getting paths one by one.
    """
    from panoptikon.data_extractors.types import JobInputData
    from panoptikon.db.pql.pql_model import AndOperator, PQLQuery, QueryElement
    from panoptikon.db.pql.search import search_pql

    user_filters = [
        f.pql_query
        for f in config.job_filters
        if (
            f.setter_names.index(model_opts.setter_name()) != -1
            or f.setter_names.index("*") != -1
        )
    ]
    # Flatten AND operators into a list of filters
    flattened_user_filters: List[QueryElement] = []
    for f in user_filters:
        if isinstance(f, AndOperator):
            flattened_user_filters.extend(f.and_)
        else:
            flattened_user_filters.append(f)

    model_filters = model_opts.item_extraction_rules()
    model_filters.and_.extend(flattened_user_filters)

    query = PQLQuery(
        query=model_filters,
        page_size=0,
        check_path=False,
    )
    logger.debug(
        f"Job Item Query: {(query.query or query).model_dump(exclude_defaults=True)}"
    )
    if model_opts.target_entities() == ["items"]:
        query.entity = "file"
        query.partition_by = ["item_id"]
        query.select = [
            "sha256",
            "path",
            "last_modified",
            "type",
            "md5",
            "duration",
            "audio_tracks",
            "video_tracks",
            "subtitle_tracks",
        ]
    elif model_opts.target_entities() == ["text"]:
        query.entity = "text"
        query.partition_by = ["data_id"]
        query.select = [
            "sha256",
            "path",
            "last_modified",
            "md5",
            "type",
            "data_id",
            "text",
        ]
    else:
        raise ValueError("Only Items and Text target entities are supported")

    results_generator, total_count, rm, cm = search_pql(conn, query)

    remaining_count: int = total_count
    for result in results_generator:
        item = JobInputData(**result.model_dump())
        remaining_count -= 1
        if os.path.exists(item.path):
            yield item, remaining_count
            continue
        if file := get_existing_file_for_sha256(conn, item.sha256):
            item.path = file.path
            item.file_id = file.id
            item.last_modified = file.last_modified
            yield item, remaining_count
        else:
            # If no working path is found, skip this item
            continue


def get_existing_setters(
    conn: sqlite3.Connection,
) -> List[Tuple[OutputDataType, str]]:
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


def get_setters_total_data(
    conn: sqlite3.Connection,
) -> List[Tuple[str, int]]:
    """
    Returns tuples containing (setter, total_data_count) pairs.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.

    Returns:
        List[Tuple[str, str]]: A list tuples containing (setter, total_data_count) pairs.
    """
    query = """
    SELECT s.name, COUNT(ie.id)
    FROM item_data ie
    JOIN setters s ON ie.setter_id = s.id
    WHERE ie.is_placeholder = 0
    GROUP BY s.id, s.name
    """

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results
