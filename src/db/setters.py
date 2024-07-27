import sqlite3


def upsert_setter(
    conn: sqlite3.Connection,
    setter_type: str,
    setter_name: str,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO setters (type, name)
        VALUES (?, ?)
        ON CONFLICT(type, name) DO UPDATE SET type = type
        RETURNING id
        """,
        (setter_type, setter_name),
    )
    return cursor.fetchone()[0]


def get_setter(
    conn: sqlite3.Connection,
    setter_type: str,
    setter_name: str,
) -> int | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id
        FROM setters
        WHERE type = ?
        AND name = ?
        """,
        (setter_type, setter_name),
    )
    # Handle case where setter does not exist
    if res := cursor.fetchone():
        return res[0]
    return None


def delete_setter_by_name(
    conn: sqlite3.Connection,
    setter_type: str,
    setter_name: str,
):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM setters
        WHERE type = ?
        AND name = ?
        """,
        (setter_type, setter_name),
    )
    return cursor.rowcount


def delete_setter_by_id(
    conn: sqlite3.Connection,
    setter_id: int,
):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM setters
        WHERE id = ?
        """,
        (setter_id,),
    )
    return cursor.rowcount


def get_all_setters_with_id(
    conn: sqlite3.Connection,
) -> list[tuple[int, str, str]]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, type, name
        FROM setters
        """
    )
    return cursor.fetchall()


def get_all_setters(
    conn: sqlite3.Connection,
) -> list[tuple[str, str]]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT type, name
        FROM setters
        """
    )
    return cursor.fetchall()
