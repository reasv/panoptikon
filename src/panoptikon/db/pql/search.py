import logging
import os
import sqlite3
from typing import Any, List, Tuple

from sqlalchemy import Select
from sqlalchemy.dialects import sqlite

from panoptikon.db.files import get_existing_file_for_sha256
from panoptikon.db.pql.pql_model import PQLQuery
from panoptikon.db.pql.query_builder import build_query
from panoptikon.db.pql.types import SearchResult, map_row_to_class

logger = logging.getLogger(__name__)


def get_sql(stmt: Select, binds: bool = False) -> Tuple[str, List[Any]]:
    compiled_sql = stmt.compile(
        dialect=sqlite.dialect(), compile_kwargs={"literal_binds": binds}
    )
    sql_string = str(compiled_sql)
    if compiled_sql.positiontup is not None:
        params_ordered = [
            compiled_sql.params[key] for key in compiled_sql.positiontup
        ]
    else:
        params_ordered = []

    return sql_string, params_ordered


def search_pql(
    conn: sqlite3.Connection,
    query: PQLQuery,
):
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    if query.count:
        count_stmt = build_query(query, count_query=True)
        count_sql_string, count_params_ordered = get_sql(count_stmt)
        try:
            cursor.execute(count_sql_string, count_params_ordered)
            logger.debug(f"Executing query: {count_sql_string}")
            logger.debug(f"Params: {count_params_ordered}")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            debug_string, _ = get_sql(count_stmt, binds=True)
            logger.error(debug_string)
            logger.error(count_params_ordered)
            raise e
        total_count: int = cursor.fetchone()[0]
    else:
        total_count = 0

    stmt = build_query(query, count_query=False)
    sql_string, params_ordered = get_sql(stmt)
    try:
        cursor.execute(sql_string, params_ordered)
        logger.debug(f"Executing query: {sql_string}")
        logger.debug(f"Params: {params_ordered}")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        debug_string, _ = get_sql(stmt, binds=True)
        logger.error(debug_string)
        logger.error(params_ordered)
        raise e

    def results_generator():
        while row := cursor.fetchone():
            result = SearchResult(file_id=row[0], item_id=row[1])
            map_row_to_class(row, result)
            if query.check_path and not os.path.exists(result.path):
                if query.entity == "file":
                    logger.warning(f"File not found: {result.path}")
                    continue
                else:
                    logger.warning(f"Item path not found: {result.path}")
                if file := get_existing_file_for_sha256(conn, result.sha256):
                    result.path = file.path
                    result.last_modified = file.last_modified
            yield result

    return results_generator(), total_count
