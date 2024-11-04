import logging
import os
import sqlite3
import time
from calendar import c
from typing import Any, Generator, List, Tuple

from pydantic import BaseModel, Field
from sqlalchemy import Select
from sqlalchemy.dialects import sqlite

from panoptikon.db.files import get_existing_file_for_item_id
from panoptikon.db.pql.pql_model import PQLQuery
from panoptikon.db.pql.query_builder import build_query
from panoptikon.db.pql.types import (
    SearchResult,
    get_extra_columns,
    map_row_to_class,
)
from panoptikon.db.pql.utils import clean_params

logger = logging.getLogger(__name__)


def get_sql(stmt: Select, binds: bool = False) -> Tuple[str, List[Any]]:
    compiled_sql = stmt.compile(
        dialect=sqlite.dialect(),
        compile_kwargs={"literal_binds": binds, "render_postcompile": True},
    )
    sql_string = str(compiled_sql)
    if compiled_sql.positiontup is not None:
        params_ordered = [
            compiled_sql.params[key] for key in compiled_sql.positiontup
        ]
    else:
        params_ordered = []

    return sql_string, params_ordered


class SearchMetrics(BaseModel):
    build: float = Field(
        default=0,
        title="Build time",
        description="Time taken to process the query into an SQLAlchemy Core statement",
    )
    compile: float = Field(
        default=0,
        title="Compile time",
        description="Time taken to compile the SQLAlchemy Core statement into an SQL string",
    )
    execute: float = Field(
        default=0,
        title="Execution time",
        description="Time taken to execute the SQL query",
    )


def td_rounded(start_time: float) -> float:
    return round(time.time() - start_time, 3)


def search_pql(
    conn: sqlite3.Connection,
    query: PQLQuery,
):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row  # type: ignore
    count_query_metrics = SearchMetrics(build=0, compile=0, execute=0)
    result_query_metrics = SearchMetrics(build=0, compile=0, execute=0)
    if query.count:
        start_time = time.time()
        count_stmt, _ = build_query(query, count_query=True)
        count_query_metrics.build = td_rounded(start_time)
        start_time = time.time()
        count_sql_string, count_params_ordered = get_sql(count_stmt)
        count_query_metrics.compile = td_rounded(start_time)
        cleaned_params = clean_params(count_params_ordered)
        try:
            start_time = time.time()
            cursor.execute(count_sql_string, count_params_ordered)
            count_query_metrics.execute = td_rounded(start_time)
            logger.debug(f"Executing query: {count_sql_string}")
            logger.debug(f"Params: {cleaned_params}")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            try:
                debug_string, _ = get_sql(count_stmt, binds=True)
                logger.error(debug_string)
                logger.error(cleaned_params)
                raise e
            except Exception as e:
                logger.error(f"Error getting debug string: {e}")
                debug_string, _ = get_sql(count_stmt, binds=False)
                logger.error(debug_string)
                logger.error(cleaned_params)
                raise e
        total_count: int = cursor.fetchone()[0]
    else:
        total_count = 0

    if not query.results:

        def empty_generator() -> Generator[SearchResult, Any, None]:
            yield from []

        return (
            empty_generator(),
            total_count,
            result_query_metrics,
            count_query_metrics,
        )
    start_time = time.time()
    stmt, extra_columns = build_query(query, count_query=False)
    result_query_metrics.build = td_rounded(start_time)
    start_time = time.time()
    sql_string, params_ordered = get_sql(stmt)
    result_query_metrics.compile = td_rounded(start_time)
    cleaned_params = clean_params(params_ordered)
    try:
        start_time = time.time()
        cursor.execute(sql_string, params_ordered)
        result_query_metrics.execute = td_rounded(start_time)
        logger.debug(f"Executed query: {sql_string}")
        logger.debug(f"Params: {cleaned_params}")
        logger.debug(
            f"Query execution took {result_query_metrics.execute} seconds (build: {result_query_metrics.build}, compile: {result_query_metrics.compile})"
        )
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        debug_string, _ = get_sql(stmt, binds=True)
        logger.error(debug_string)
        logger.error(cleaned_params)
        raise e

    def results_generator() -> Generator[SearchResult, Any, None]:
        while row := cursor.fetchone():
            result = SearchResult(file_id=0, item_id=0)
            map_row_to_class(row, result)
            result.extra = get_extra_columns(row, extra_columns)
            if (
                query.check_path
                and result.path
                and not os.path.exists(result.path)
            ):
                if query.entity == "file" and not query.partition_by:
                    logger.warning(f"File not found: {result.path}")
                    continue
                else:
                    logger.warning(f"Result path not found: {result.path}")
                if file := get_existing_file_for_item_id(conn, result.item_id):
                    result.path = file.path
                    # Only set if not already set
                    result.last_modified = (
                        file.last_modified if result.last_modified else None
                    )
                    result.filename = file.filename if result.filename else None
                else:
                    logger.warning(
                        f"File not found in database: {result.sha256}"
                    )
                    continue
            yield result

    return (
        results_generator(),
        total_count,
        result_query_metrics,
        count_query_metrics,
    )
