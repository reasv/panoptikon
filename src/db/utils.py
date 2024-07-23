import sqlite3


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
