import sqlite3
from typing import List

from src.utils import normalize_path


def add_folder_to_database(
    conn: sqlite3.Connection, time: str, folder_path: str, included=True
):
    cursor = conn.cursor()
    folder_path = normalize_path(folder_path)
    # Attempt to insert the folder
    cursor.execute(
        """
        INSERT OR IGNORE INTO folders (time_added, path, included)
        VALUES (?, ?, ?)
    """,
        (time, folder_path, included),
    )

    if cursor.rowcount == 0:
        return False
    else:
        return True


def delete_folders_not_in_list(
    conn: sqlite3.Connection, folder_paths: List[str], included=True
):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    DELETE FROM folders
    WHERE included = ?
    AND path NOT IN ({})
    """.format(
            ",".join(["?"] * len(folder_paths))
        ),
        [included] + folder_paths,
    )
    return result.rowcount


def remove_folder_from_database(conn: sqlite3.Connection, folder_path: str):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM folders WHERE path = ?", (folder_path,))


def get_folders_from_database(
    conn: sqlite3.Connection, included=True
) -> List[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT path FROM folders WHERE included = ?", (included,))
    folders = cursor.fetchall()
    return [folder[0] for folder in folders]


def delete_files_under_excluded_folders(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    DELETE FROM files
    WHERE EXISTS (
        SELECT 1
        FROM folders
        WHERE folders.included = 0
        AND files.path LIKE folders.path || '%'
    );
    """
    )
    return result.rowcount


def delete_files_not_under_included_folders(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    DELETE FROM files
    WHERE NOT EXISTS (
        SELECT 1
        FROM folders
        WHERE folders.included = 1
        AND files.path LIKE folders.path || '%'
    );
    """
    )
    return result.rowcount
