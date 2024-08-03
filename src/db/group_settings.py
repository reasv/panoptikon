import sqlite3
from typing import Optional, Tuple


def save_model_group_settings(
    conn: sqlite3.Connection,
    group_name: str,
    batch_size: int,
    threshold: Optional[float],
):
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO model_group_settings (name, batch_size, threshold)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            batch_size = excluded.batch_size,
            threshold = excluded.threshold
    """,
        (group_name, batch_size, threshold),
    )


def retrieve_model_group_settings(
    conn: sqlite3.Connection, group_name: str
) -> Optional[Tuple[int, Optional[float]]]:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT batch_size, threshold
        FROM model_group_settings
        WHERE name = ?
    """,
        (group_name,),
    )

    result = cursor.fetchone()

    if result:
        return result[0], result[1]  # batch_size, threshold
    else:
        return None
