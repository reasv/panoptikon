import sqlite3


def upsert_setter(
    conn: sqlite3.Connection,
    setter_name: str,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO setters (name)
        VALUES (?)
        ON CONFLICT(name)
        DO UPDATE SET name = excluded.name
        RETURNING id
        """,
        (setter_name,),
    )
    return cursor.fetchone()[0]


def get_setter_id(
    conn: sqlite3.Connection,
    setter_name: str,
) -> int | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id
        FROM setters
        WHERE name = ?
        """,
        (setter_name,),
    )
    # Handle case where setter does not exist
    if res := cursor.fetchone():
        return res[0]
    return None


def delete_setter_by_name(
    conn: sqlite3.Connection,
    setter_name: str,
):
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM setters
        WHERE name = ?
        """,
        (setter_name,),
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
