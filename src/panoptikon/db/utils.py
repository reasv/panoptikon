import logging
import sqlite3
import struct
from typing import List

logger = logging.getLogger(__name__)


def trigger_exists(conn: sqlite3.Connection, trigger_name: str) -> bool:
    """
    Check if a trigger with the given name exists in the SQLite database.

    Args:
    cursor (sqlite3.Cursor): The SQLite database cursor
    trigger_name (str): The name of the trigger to check

    Returns:
    bool: True if the trigger exists, False otherwise
    """
    query = """
    SELECT COUNT(*) 
    FROM sqlite_master 
    WHERE type = 'trigger' AND name = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, (trigger_name,))
    count = cursor.fetchone()[0]
    return count > 0


def is_column_in_table(
    conn: sqlite3.Connection, table: str, column: str
) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    return any(column[1] == column for column in columns)


def vacuum_database(conn: sqlite3.Connection):
    """
    Run VACUUM and ANALYZE on the database to optimize it
    """
    conn.execute("VACUUM")
    conn.execute("ANALYZE")
    conn.execute("PRAGMA optimize")


def pretty_print_SQL(query_str: str, params: List[str | float | int]):
    try:
        # Quote strings in params
        quoted_params = [
            f"'{param}'" if isinstance(param, str) else param
            for param in params
        ]
        formatted_query = query_str.replace("?", "{}").format(*quoted_params)
        # Remove empty lines
        formatted_query = "\n".join(
            [line for line in formatted_query.split("\n") if line.strip() != ""]
        )
        logger.debug(formatted_query)
    except Exception as e:
        logger.error(f"Error formatting query: {e}")
        logger.error(query_str, params)


def serialize_f32(vector: List[float]) -> bytes:
    """serializes a list of floats into a compact "raw bytes" format"""
    return struct.pack("%sf" % len(vector), *vector)
