import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Literal, Tuple

from src.types import FileScanData, ItemWithPath
from src.utils import get_mime_type, normalize_path


def get_database_connection(force_readonly=False) -> sqlite3.Connection:
    # Check if we are in read-only mode
    db_file = os.getenv("DB_FILE", "./db/sqlite.db")
    if force_readonly or os.environ.get("READONLY", "false").lower() == "true":
        # Use a read-only connection
        conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_database(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        sha256 TEXT UNIQUE NOT NULL,
        md5 TEXT NOT NULL,
        type TEXT,
        size INTEGER,          -- Size of the file in bytes
        time_added TEXT NOT NULL         -- Using TEXT to store ISO-8601 formatted datetime
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        sha256 TEXT NOT NULL,
        item_id INTEGER NOT NULL,         -- Foreign key to items table
        path TEXT UNIQUE NOT NULL,        -- Ensuring path is unique
        filename TEXT NOT NULL,           -- Filename extracted from path
        last_modified TEXT NOT NULL,      -- Using TEXT to store ISO-8601 formatted datetime
        last_seen TEXT NOT NULL,          -- Using TEXT to store ISO-8601 formatted datetime
        available BOOLEAN NOT NULL,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(item_id) REFERENCES items(id)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS file_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,         -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT NOT NULL,           -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        total_available INTEGER NOT NULL,
        new_items INTEGER NOT NULL,
        unchanged_files INTEGER NOT NULL,
        new_files INTEGER NOT NULL,
        modified_files INTEGER NOT NULL,
        marked_unavailable INTEGER NOT NULL,
        errors INTEGER NOT NULL,
        UNIQUE(start_time, path)       -- Unique constraint on time and path
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS tags_setters (
        id INTEGER PRIMARY KEY,
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        setter TEXT NOT NULL,
        UNIQUE(namespace, name, setter)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS tags_items (
        item_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        confidence REAL DEFAULT 1.0,
        UNIQUE(item_id, tag_id),
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
        FOREIGN KEY(tag_id) REFERENCES tags_setters(id) ON DELETE CASCADE
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS data_extraction_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT DEFAULT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        type TEXT NOT NULL,
        setter TEXT NOT NULL,
        threshold REAL DEFAULT NULL,
        batch_size INTEGER NOT NULL,
        image_files INTEGER NOT NULL DEFAULT 0,
        video_files INTEGER NOT NULL DEFAULT 0,
        other_files INTEGER NOT NULL DEFAULT 0,
        total_segments INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        total_remaining INTEGER NOT NULL DEFAULT 0,
        UNIQUE(start_time, type, setter)       -- Unique constraint on start_time, type and setter
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time_added TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        included BOOLEAN NOT NULL,       -- BOOLEAN to indicate if folder is included or specifically excluded
        UNIQUE(path)  -- Unique constraint on path
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS bookmarks (
        namespace TEXT NOT NULL, -- Namespace for the bookmark
        sha256 TEXT NOT NULL, -- SHA256 of the item
        time_added TEXT NOT NULL, -- Using TEXT to store ISO-8601 formatted datetime
        metadata TEXT, -- JSON string to store additional metadata
        FOREIGN KEY(sha256) REFERENCES items(sha256) ON DELETE CASCADE,
        PRIMARY KEY(namespace, sha256)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS extraction_log_items (
        item_id INTEGER NOT NULL,
        log_id INTEGER NOT NULL,
        UNIQUE(item_id, log_id), -- Unique constraint on item and log_id
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
        FOREIGN KEY(log_id) REFERENCES data_extraction_log(id) ON DELETE CASCADE
    )
    """
    )
    # Create table for extracted text
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS extracted_text (
        id INTEGER PRIMARY KEY,
        item_id INTEGER NOT NULL,
        log_id INTEGER NOT NULL,
        language TEXT,
        confidence REAL,
        text TEXT NOT NULL,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
        FOREIGN KEY(log_id) REFERENCES data_extraction_log(id) ON DELETE CASCADE
    )
    """
    )

    cursor.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS extracted_text_fts
        USING fts5(
            text,
            content="extracted_text",
            content_rowid="id",
            tokenize="porter unicode61 remove_diacritics 2"
        )
        """
    )
    if trigger_exists(conn, "extracted_text_ai"):
        cursor.execute("DROP TRIGGER extracted_text_ai")
    # Triggers to keep the FTS index up to date.
    cursor.execute(
        """
        CREATE TRIGGER extracted_text_ai AFTER INSERT ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(rowid, text)
            VALUES (new.id, new.text);
        END;
    """
    )

    if trigger_exists(conn, "extracted_text_ad"):
        cursor.execute("DROP TRIGGER extracted_text_ad")

    cursor.execute(
        """
        CREATE TRIGGER extracted_text_ad AFTER DELETE ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(extracted_text_fts, rowid, text)
            VALUES('delete', old.id, old.text);
        END;
    """
    )

    if trigger_exists(conn, "extracted_text_au"):
        cursor.execute("DROP TRIGGER extracted_text_au")

    cursor.execute(
        """
        CREATE TRIGGER extracted_text_au AFTER UPDATE ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(extracted_text_fts, rowid, text)
            VALUES('delete', old.id, old.text);
            INSERT INTO extracted_text_fts(rowid, text)
            VALUES (new.id, new.text);
        END;
    """
    )

    # Create FTS table for files
    cursor.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS files_path_fts
        USING fts5(
            path,
            filename,
            content='files',
            content_rowid='id',
            tokenize='trigram case_sensitive 0'
        );
    """
    )

    if trigger_exists(conn, "files_path_ai"):
        cursor.execute("DROP TRIGGER files_path_ai")

    cursor.execute(
        """ 
        CREATE TRIGGER files_path_ai AFTER INSERT ON files BEGIN
            INSERT INTO files_path_fts(rowid, path, filename) 
            VALUES (new.id, new.path, new.filename);
        END;
    """
    )

    if trigger_exists(conn, "files_path_ad"):
        cursor.execute("DROP TRIGGER files_path_ad")

    cursor.execute(
        """
        CREATE TRIGGER files_path_ad AFTER DELETE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename) 
            VALUES('delete', old.id, old.path, old.filename);
        END;
        """
    )

    if trigger_exists(conn, "files_path_au"):
        cursor.execute("DROP TRIGGER files_path_au")
    cursor.execute(
        """
        CREATE TRIGGER files_path_au AFTER UPDATE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename) 
            VALUES('delete', old.id, old.path, old.filename);
            INSERT INTO files_path_fts(rowid, path, filename) 
            VALUES (new.id, new.path, new.filename);
        END;
    """
    )
    # Create indexes
    # Tuples are table name, followed by a list of columns
    indices = [
        ("items", ["md5"]),
        ("items", ["type"]),
        ("items", ["size"]),
        ("items", ["time_added"]),
        ("files", ["sha256"]),
        ("files", ["last_modified"]),
        ("files", ["available"]),
        ("files", ["path"]),
        ("files", ["last_seen"]),
        ("files", ["filename"]),
        ("file_scans", ["start_time"]),
        ("file_scans", ["end_time"]),
        ("file_scans", ["path"]),
        ("data_extraction_log", ["start_time"]),
        ("data_extraction_log", ["end_time"]),
        ("data_extraction_log", ["setter"]),
        ("data_extraction_log", ["type"]),
        ("data_extraction_log", ["threshold"]),
        ("folders", ["time_added"]),
        ("folders", ["path"]),
        ("folders", ["included"]),
        ("bookmarks", ["time_added"]),
        ("bookmarks", ["sha256"]),
        ("bookmarks", ["metadata"]),
        ("bookmarks", ["namespace"]),
        ("extraction_log_items", ["item_id"]),
        ("extraction_log_items", ["log_id"]),
        ("tags_items", ["item_id"]),
        ("tags_items", ["tag_id"]),
        ("tags_items", ["confidence"]),
        ("tags_items", ["item_id", "tag_id"]),
        ("tags_items", ["tag_id", "item_id"]),
        ("tags_setters", ["namespace"]),
        ("tags_setters", ["name"]),
        ("tags_setters", ["setter"]),
        ("tags_setters", ["namespace", "name"]),
        ("tags_setters", ["namespace", "setter"]),
        ("tags_setters", ["name", "setter"]),
        ("tags_setters", ["namespace", "name", "setter"]),
        ("extracted_text", ["item_id"]),
        ("extracted_text", ["log_id"]),
        ("extracted_text", ["language"]),
        ("extracted_text", ["confidence"]),
    ]

    for table, columns in indices:
        columns_str = ", ".join(columns)
        index_name = f"idx_{table}_{'_'.join(columns)}"
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            {index_name} ON {table}({columns_str})
            """
        )

    if is_column_in_table(conn, "files", "item_id"):
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_files_item ON files(item_id)"
        )


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


def create_tag_setter(conn: sqlite3.Connection, namespace, name, setter):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    INSERT INTO tags_setters (namespace, name, setter)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, name, setter) DO NOTHING
    """,
        (namespace, name, setter),
    )

    tag_setter_inserted = result.rowcount > 0
    if tag_setter_inserted and cursor.lastrowid is not None:
        rowid: int = cursor.lastrowid
    else:
        rowid: int = cursor.execute(
            "SELECT id FROM tags_setters WHERE namespace = ? AND name = ? AND setter = ?",
            (namespace, name, setter),
        ).fetchone()[0]
    return rowid


def insert_tag_item(
    conn: sqlite3.Connection, item_id: int, tag_id: int, confidence=1.0
):
    # Round confidence to 3 decimal places
    confidence_float = round(float(confidence), 4)
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO tags_items (item_id, tag_id, confidence)
    VALUES (?, ?, ? )
    ON CONFLICT(item_id, tag_id) DO UPDATE SET confidence=excluded.confidence
    """,
        (item_id, tag_id, confidence_float),
    )


def add_tag_to_item(
    conn: sqlite3.Connection,
    namespace: str,
    name: str,
    setter: str,
    sha256: str,
    confidence: float = 1.0,
):
    item_id = get_item_id(conn, sha256)
    assert item_id is not None, f"Item with sha256 {sha256} not found"
    tag_rowid = create_tag_setter(conn, namespace, name, setter)
    insert_tag_item(conn, item_id, tag_rowid, confidence)


def get_item_id(conn: sqlite3.Connection, sha256: str) -> int | None:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM items WHERE sha256 = ?", (sha256,))
    item_id = cursor.fetchone()
    if item_id:
        return item_id[0]
    return None


def update_file_data(
    conn: sqlite3.Connection, scan_time: str, file_data: FileScanData
):
    cursor = conn.cursor()
    sha256 = file_data.sha256
    md5 = file_data.md5
    mime_type = file_data.mime_type
    file_size = file_data.size
    path = file_data.path
    last_modified = file_data.last_modified
    path_in_db = file_data.path_in_db
    file_modified = file_data.modified

    item_insert_result = cursor.execute(
        """
    INSERT INTO items (sha256, md5, type, size, time_added)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO NOTHING
    """,
        (sha256, md5, mime_type, file_size, scan_time),
    )

    # We need to check if the item was inserted
    item_inserted = item_insert_result.rowcount > 0

    # Get the rowid of the inserted item, if it was inserted
    item_rowid: int | None = cursor.lastrowid if item_inserted else None

    file_updated = False
    if path_in_db and not file_modified:
        # Path exists and has not changed, update last_seen and available
        file_update_result = cursor.execute(
            """
        UPDATE files
        SET last_seen = ?, available = TRUE
        WHERE path = ?
        """,
            (scan_time, path),
        )

        file_updated = file_update_result.rowcount > 0

    file_deleted = False
    file_inserted = False
    if not path_in_db or file_modified:
        # If the path already exists, delete the old entry
        file_delete_result = cursor.execute(
            "DELETE FROM files WHERE path = ?", (path,)
        )
        file_deleted = file_delete_result.rowcount > 0

        if not item_rowid:
            # If the item was not inserted, get the rowid from the database
            item_rowid = cursor.execute(
                "SELECT id FROM items WHERE sha256 = ?", (sha256,)
            ).fetchone()[0]

        filename = os.path.basename(path)
        # Path does not exist or has been modified, insert new
        file_insert_result = cursor.execute(
            """
        INSERT INTO files (sha256, item_id, path, filename, last_modified, last_seen, available)
        VALUES (?, ?, ?, ?, ?, ?, TRUE)
        """,
            (sha256, item_rowid, path, filename, last_modified, scan_time),
        )
        file_inserted = file_insert_result.rowcount > 0

    return item_inserted, file_updated, file_deleted, file_inserted


def add_file_scan(
    conn: sqlite3.Connection,
    scan_time: str,
    end_time: str,
    path: str,
    new_items: int,
    unchanged_files: int,
    new_files: int,
    modified_files: int,
    marked_unavailable: int,
    errors: int,
    total_available: int,
):
    """
    Logs a file scan into the database
    """
    cursor = conn.cursor()
    insert_result = cursor.execute(
        """
    INSERT INTO file_scans (start_time, end_time, path, total_available, new_items, unchanged_files, new_files, modified_files, marked_unavailable, errors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            end_time,
            path,
            total_available,
            new_items,
            unchanged_files,
            new_files,
            modified_files,
            marked_unavailable,
            errors,
        ),
    )
    # Return the row id of the inserted record
    return insert_result.lastrowid


@dataclass
class FileScanRecord:
    id: int
    start_time: str
    end_time: str
    path: str
    total_available: int
    new_items: int
    unchanged_files: int
    new_files: int
    modified_files: int
    marked_unavailable: int
    errors: int


def get_file_scan_by_id(
    conn: sqlite3.Connection, scan_id: int
) -> FileScanRecord | None:
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT *
    FROM file_scans
    WHERE id = ?
    """,
        (scan_id,),
    )
    scan_record = cursor.fetchone()
    if scan_record:
        return FileScanRecord(*scan_record)
    return None


def get_all_file_scans(conn: sqlite3.Connection) -> List[FileScanRecord]:
    cursor = conn.cursor()
    # Order by start_time in descending order
    cursor.execute("SELECT * FROM file_scans ORDER BY start_time DESC")
    scan_records = cursor.fetchall()
    return [FileScanRecord(*scan_record) for scan_record in scan_records]


def is_column_in_table(
    conn: sqlite3.Connection, table: str, column: str
) -> bool:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    return any(column[1] == column for column in columns)


def mark_unavailable_files(conn: sqlite3.Connection, scan_time: str, path: str):
    """
    Mark files as unavailable if their path is a subpath of `path`
    and they were not seen during the scan at `scan_time`
    """
    cursor = conn.cursor()

    # Count files to be marked as unavailable
    precount_result = cursor.execute(
        """
    SELECT COUNT(*)
    FROM files
    WHERE last_seen != ?
    AND path LIKE ?
    """,
        (scan_time, path + "%"),
    )

    marked_unavailable = precount_result.fetchone()[0]

    # If a file has not been seen in scan that happened at scan_time, mark it as unavailable
    cursor.execute(
        """
        UPDATE files
        SET available = FALSE
        WHERE last_seen != ?
        AND path LIKE ?
    """,
        (scan_time, path + "%"),
    )

    # Count available files
    result_available = cursor.execute(
        """
        SELECT COUNT(*)
        FROM files
        WHERE available = TRUE
        AND path LIKE ?
    """,
        (path + "%",),
    )
    available_files: int = result_available.fetchone()[0]

    return marked_unavailable, available_files


def get_file_by_path(conn: sqlite3.Connection, path: str):
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT files.*, items.md5, items.size
    FROM files
    JOIN items ON files.sha256 = items.sha256
    WHERE files.path = ?
    """,
        (path,),
    )

    file_record = cursor.fetchone()

    if file_record:
        # Get column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]
        # Construct a dictionary using column names and file record
        file_dict = dict(zip(column_names, file_record))
    else:
        file_dict = None

    return file_dict


def hard_update_items_available(conn: sqlite3.Connection):
    # This function is used to update the availability of files in the database
    cursor = conn.cursor()

    cursor.execute("SELECT path FROM files")
    files = cursor.fetchall()

    for (path,) in files:
        available = os.path.exists(path)
        cursor.execute(
            """
        UPDATE files
        SET Available = ?
        WHERE path = ?
        """,
            (available, path),
        )


def create_data_extraction_log(
    conn: sqlite3.Connection,
    scan_time: str,
    type: str,
    setter: str,
    threshold: float | None,
    batch_size: int,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO data_extraction_log (
        start_time,
        type,
        setter,
        threshold,
        batch_size
    )
    VALUES (?, ?, ?, ?, ?)
    """,
        (
            scan_time,
            type,
            setter,
            threshold,
            batch_size,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def update_log(
    conn: sqlite3.Connection,
    log_id: int,
    image_files: int,
    video_files: int,
    other_files: int,
    total_segments: int,
    errors: int,
    total_remaining: int,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    UPDATE data_extraction_log
    SET end_time = ?,
    image_files = ?,
    video_files = ?,
    other_files = ?,
    total_segments = ?,
    errors = ?,
    total_remaining = ?
    WHERE id = ?
    """,
        (
            datetime.now().isoformat(),
            image_files,
            video_files,
            other_files,
            total_segments,
            errors,
            total_remaining,
            log_id,
        ),
    )


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str
    type: str
    setter: str
    threshold: float | None
    batch_size: int
    image_files: int
    video_files: int
    other_files: int
    total_segments: int
    errors: int
    total_remaining: int


def get_all_data_extraction_logs(conn: sqlite3.Connection) -> List[LogRecord]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT
        id,
        start_time,
        end_time,
        type,
        setter,
        threshold,
        batch_size,
        image_files,
        video_files,
        other_files,
        total_segments,
        errors,
        total_remaining
        FROM data_extraction_log
        ORDER BY start_time DESC"""
    )
    log_records = cursor.fetchall()
    return [LogRecord(*log_record) for log_record in log_records]


def add_item_to_log(
    conn: sqlite3.Connection,
    item: str,
    log_id: int,
):
    cursor = conn.cursor()
    item_id = get_item_id(conn, item)
    cursor.execute(
        """
    INSERT INTO extraction_log_items (item_id, log_id)
    VALUES (?, ?)
    """,
        (item_id, log_id),
    )


def get_items_missing_data_extraction(
    conn: sqlite3.Connection,
    model_type: str,
    setter: str,
    mime_type_filter: List[str] | None = None,
):
    """
    Get all items that have not been scanned by the given setter.
    More efficient than get_items_missing_tags as it does not require
    a join with the tags table.
    It also avoids joining with the files table to get the path,
    instead getting paths one by one.
    """
    clauses = f"""
    FROM items
    WHERE NOT EXISTS (
        SELECT 1
        FROM extraction_log_items
        JOIN data_extraction_log
        ON extraction_log_items.log_id = data_extraction_log.id
        WHERE items.id = extraction_log_items.item_id
        AND data_extraction_log.type = ?
        AND data_extraction_log.setter = ?
        AND data_extraction_log.end_time IS NOT NULL
    )
    """
    if mime_type_filter:
        clauses += "AND ("
        for i, _ in enumerate(mime_type_filter):
            if i == 0:
                clauses += "items.type LIKE ? || '%'"
            else:
                clauses += f" OR items.type LIKE ? || '%'"
        clauses += ")"

    params = [
        model_type,
        setter,
        *(mime_type_filter if mime_type_filter else ()),
    ]

    count_query = f"""
    SELECT COUNT(*)
    {clauses}
    """
    cursor = conn.cursor()

    cursor.execute(
        count_query,
        params,
    )
    total_count = cursor.fetchone()[0]

    cursor.execute(
        f"""
    SELECT
    items.sha256,
    items.md5,
    items.type,
    items.size,
    items.time_added
    {clauses}
    """,
        params,
    )

    remaining_count: int = total_count
    while row := cursor.fetchone():
        item = ItemWithPath(*row, "")  # type: ignore
        remaining_count -= 1
        if file := get_existing_file_for_sha256(conn, item.sha256):
            item.path = file.path
            yield item, remaining_count
        else:
            # If no working path is found, skip this item
            continue


def delete_tags_from_setter(conn: sqlite3.Connection, setter: str):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM tags_items
    WHERE rowid IN (
        SELECT tags_items.rowid
        FROM tags_items
        JOIN tags_setters as tags
        ON tags_items.tag_id = tags.id
        AND tags.setter = ?
    )
    """,
        (setter,),
    )

    result = cursor.execute(
        """
    DELETE FROM tags_setters
    WHERE setter = ?
    """,
        (setter,),
    )

    tags_removed = result.rowcount

    result_items = cursor.execute(
        """
    DELETE FROM extraction_log_items
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = 'tags'
    )
    """,
        (setter,),
    )

    items_tags_removed = result_items.rowcount
    return tags_removed, items_tags_removed


def remove_setter_from_items(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()

    result = cursor.execute(
        """
    DELETE FROM extraction_log_items
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = ?
    )
    """,
        (setter, model_type),
    )

    items_setter_removed = result.rowcount
    return items_setter_removed


@dataclass
class FileRecord:
    sha256: str
    path: str
    last_modified: str


def get_existing_file_for_sha256(
    conn: sqlite3.Connection, sha256: str
) -> FileRecord | None:
    cursor = conn.cursor()

    cursor.execute(
        """
    SELECT path, last_modified
    FROM files
    WHERE sha256 = ?
    ORDER BY available DESC
    """,
        (sha256,),
    )

    while row := cursor.fetchone():
        path, last_modified = row
        if os.path.exists(path):
            return FileRecord(sha256, path, last_modified)

    return None


def get_all_tags_for_item(conn: sqlite3.Connection, sha256):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT tags.namespace, tags.name, tags_items.confidence, tags.setter
    FROM items
    JOIN tags_items ON items.id = tags_items.item_id
    AND items.sha256 = ?
    JOIN tags_setters as tags ON tags_items.tag_id = tags.id
    """,
        (sha256,),
    )
    tags = cursor.fetchall()
    return tags


def get_all_tags_for_item_name_confidence(conn: sqlite3.Connection, sha256):
    tags = get_all_tags_for_item(conn, sha256)
    return [(row[1], row[2]) for row in tags]


def get_tag_names_list(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT name FROM tags_setters")
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]


@dataclass
class FileSearchResult:
    path: str
    sha256: str
    last_modified: str
    type: str


def build_extracted_text_fts_clause(
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
):
    """
    Build a subquery to match extracted text based on the given conditions.
    """

    # Define subquery for matching extracted text
    extracted_text_condition = ""
    extracted_text_params = []
    if match_extracted_text:
        extracted_text_conditions = ["et_fts.text MATCH ?"]
        extracted_text_params.append(match_extracted_text)

        if require_extracted_type_setter_pairs:
            include_pairs_conditions = " OR ".join(
                ["(log.type = ? AND log.setter = ?)"]
                * len(require_extracted_type_setter_pairs)
            )
            extracted_text_conditions.append(f"({include_pairs_conditions})")
            for type, setter in require_extracted_type_setter_pairs:
                extracted_text_params.extend([type, setter])

        extracted_text_condition = f"""
        JOIN (
            SELECT et.item_id, MAX(et_fts.rank) AS max_rank
            FROM extracted_text_fts AS et_fts
            JOIN extracted_text AS et ON et_fts.rowid = et.id
            JOIN data_extraction_log AS log ON et.log_id = log.id
            WHERE {" AND ".join(extracted_text_conditions)}
            GROUP BY et.item_id
        ) AS extracted_text_matches
        ON files.item_id = extracted_text_matches.item_id
        """
    return extracted_text_condition, extracted_text_params


def build_search_query(
    tags: List[str],
    negative_tags: List[str] | None = None,
    tag_namespaces: List[str] = [],
    min_confidence: float | None = 0.5,
    setters: List[str] = [],
    all_setters_required: bool = False,
    item_types: List[str] = [],
    include_path_prefixes: List[str] = [],
    any_positive_tags_match: bool = False,
    match_path: str | None = None,
    match_filename: str | None = None,
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
    restrict_to_bookmarks: bool = False,
    restrict_to_bookmark_namespaces: List[str] = [],
) -> Tuple[str, List[str | int | float]]:
    """
    Build a query to search for files based on the given tags,
    negative tags, and other conditions.
    """
    # The items should have text extracted by the given setters matching the query
    extracted_text_condition, extracted_text_params = (
        build_extracted_text_fts_clause(
            match_extracted_text,
            require_extracted_type_setter_pairs,
        )
    )

    # The item mimetype should start with one of the given strings
    item_type_condition = ""
    if item_types:
        if len(item_types) == 1:
            item_type_condition = "AND items.type LIKE ? || '%'"
        elif len(item_types) > 1:
            item_type_condition = "AND ("
            for i, _ in enumerate(item_types):
                if i == 0:
                    item_type_condition += "items.type LIKE ? || '%'"
                else:
                    item_type_condition += " OR items.type LIKE ? || '%'"
            item_type_condition += ")"

    # The setter should match the given setter
    tag_setters_condition = (
        f" AND tags.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )

    # The namespace needs to *start* with the given namespace
    tag_namespace_condition = ""
    if len(tag_namespaces) == 1:
        tag_namespace_condition = " AND tags.namespace LIKE ? || '%'"
    elif len(tag_namespaces) > 1:
        tag_namespace_condition = " AND ("
        for i, _ in enumerate(tag_namespaces):
            if i == 0:
                tag_namespace_condition += "tags.namespace LIKE ? || '%'"
            else:
                tag_namespace_condition += " OR tags.namespace LIKE ? || '%'"
        tag_namespace_condition += ")"

    # The confidence should be greater than or equal to the given confidence
    min_confidence_condition = (
        f"AND tags_items.confidence >= ?" if min_confidence else ""
    )

    # Negative tags should not be associated with the item
    negative_tags_condition = (
        f"""
        WHERE files.item_id NOT IN (
            SELECT tags_items.item_id
            FROM tags_setters as tags
            JOIN tags_items ON tags.id = tags_items.tag_id
            AND tags.name IN ({','.join(['?']*len(negative_tags))})
            {tag_setters_condition}
            {tag_namespace_condition}
            {min_confidence_condition}
        )
    """
        if negative_tags
        else ""
    )

    path_condition = ""
    if len(include_path_prefixes) > 0:
        path_condition_start = "AND"
        # If no negative or positive tags are provided,
        # this needs to start a WHERE clause
        if not tags and not negative_tags:
            path_condition_start = "WHERE"
        if len(include_path_prefixes) == 1:
            # The path needs to *start* with the given path prefix
            path_condition = f"{path_condition_start} files.path LIKE ? || '%'"
        elif len(include_path_prefixes) > 1:
            path_condition = f"{path_condition_start} ("
            for i, _ in enumerate(include_path_prefixes):
                if i == 0:
                    path_condition += "files.path LIKE ? || '%'"
                else:
                    path_condition += " OR files.path LIKE ? || '%'"
            path_condition += ")"

    having_clause = (
        "HAVING COUNT(DISTINCT tags.name) = ?"
        if not all_setters_required
        else "HAVING COUNT(DISTINCT tags.setter || '-' || tags.name) = ?"
    )

    additional_select_columns = ""
    # If we need to match on the path or filename using FTS
    path_match_condition = ""
    if match_path or match_filename:
        additional_select_columns = ",\n path_fts.rank as rank_path_fts"
        path_match_condition = f"""
        JOIN files_path_fts AS path_fts
        ON files.id = path_fts.rowid
        """
        if match_path:
            path_match_condition += f"""
            AND path_fts.path MATCH ?
            """
        if match_filename:
            path_match_condition += f"""
            AND path_fts.filename MATCH ?
            """
    if match_extracted_text:
        additional_select_columns += (
            ",\n extracted_text_matches.max_rank AS rank_fts"
        )

    # If this is set, we only search for files that are bookmarked
    bookmarks_condition = ""
    if restrict_to_bookmarks:
        bookmarks_condition = (
            "JOIN bookmarks ON files.sha256 = bookmarks.sha256"
        )
        if restrict_to_bookmark_namespaces:
            bookmarks_condition += " AND bookmarks.namespace IN ("
            for i, _ in enumerate(restrict_to_bookmark_namespaces):
                if i == 0:
                    bookmarks_condition += "?"
                else:
                    bookmarks_condition += ", ?"
            bookmarks_condition += ")"

    main_query = (
        f"""
        SELECT
            files.path,
            files.sha256,
            files.last_modified,
            items.type
            {additional_select_columns}
        FROM tags_setters as tags
        JOIN tags_items ON tags.id = tags_items.tag_id
        AND tags.name IN ({','.join(['?']*len(tags))})
        {min_confidence_condition}
        {tag_setters_condition}
        {tag_namespace_condition}
        JOIN files ON tags_items.item_id = files.item_id
        {path_condition}
        JOIN items ON files.item_id = items.id
        {item_type_condition}
        {path_match_condition}
        {extracted_text_condition}
        {bookmarks_condition}
        {negative_tags_condition}
        GROUP BY files.path
        {having_clause if not any_positive_tags_match else ""}
    """
        if tags
        else f"""
        SELECT
            files.path,
            files.sha256,
            files.last_modified,
            items.type
            {additional_select_columns}
        FROM files
        JOIN items ON files.item_id = items.id
        {item_type_condition}
        {path_match_condition}
        {extracted_text_condition}
        {bookmarks_condition}
        {negative_tags_condition}
        {path_condition}
    """
    )
    params: List[str | int | float] = [
        param
        for param in [
            *(
                (
                    *tags,
                    min_confidence,
                    *setters,
                    *tag_namespaces,
                )
                if tags
                else ()
            ),
            *(include_path_prefixes if tags else []),
            *item_types,
            match_path,
            match_filename,
            *extracted_text_params,
            *restrict_to_bookmark_namespaces,
            *(
                (*negative_tags, *setters, *tag_namespaces, min_confidence)
                if negative_tags
                else ()
            ),
            *(include_path_prefixes if not tags else []),
            (
                # Number of tags to match, or number of tag-setter pairs to match if we require all setters to be present for all tags
                (
                    len(tags)
                    if not all_setters_required
                    else len(tags) * len(setters)
                )
                # HAVING clause is not needed if no positive tags are provided
                if tags and not any_positive_tags_match
                else None
            ),
        ]
        if param is not None
    ]

    return main_query, params


def print_search_query(query_str: str, params: List[str | float | int]):
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
        print(formatted_query)
    except Exception as e:
        print(f"Error formatting query: {e}")
        print(query_str, params)


OrderByType = (
    Literal["last_modified", "path", "rank_fts", "rank_path_fts"] | None
)

OrderType = Literal["asc", "desc"] | None


def search_files(
    conn: sqlite3.Connection,
    tags: List[str],
    tags_match_any: List[str] | None = None,
    negative_tags: List[str] | None = None,
    negative_tags_match_all: List[str] | None = None,
    tag_namespaces: List[str] | None = None,
    min_confidence: float | None = 0.5,
    setters: List[str] | None = None,
    all_setters_required: bool | None = False,
    item_types: List[str] | None = None,
    include_path_prefixes: List[str] | None = None,
    match_path: str | None = None,
    match_filename: str | None = None,
    match_extracted_text: str | None = None,
    require_extracted_type_setter_pairs: (
        List[Tuple[str, str]] | None
    ) = None,  # Pairs of (type, setter) to include
    restrict_to_bookmarks: bool = False,
    restrict_to_bookmark_namespaces: List[str] | None = None,
    order_by: OrderByType = "last_modified",
    order: OrderType = None,
    page_size: int | None = 1000,
    page: int = 1,
    check_path_exists: bool = False,
    return_total_count: bool = True,
):
    # Normalize/clean the inputs
    def clean_tag_list(tag_list: List[str] | None) -> List[str]:
        if not tag_list:
            return []
        return [tag.lower().strip() for tag in tag_list if tag.strip() != ""]

    tags_match_any = clean_tag_list(tags_match_any)
    negative_tags_match_all = clean_tag_list(negative_tags_match_all)
    tags = clean_tag_list(tags)
    negative_tags = clean_tag_list(negative_tags)
    all_setters_required = all_setters_required or False
    if len(tags_match_any) == 1:
        # If only one tag is provided for "match any", we can just use it as a regular tag
        tags.append(tags_match_any[0])
        tags_match_any = None
    if len(negative_tags_match_all) == 1:
        # If only one tag is provided for negative "match all", we can just use it as a regular negative tag
        negative_tags.append(negative_tags_match_all[0])
        negative_tags_match_all = None

    tag_namespaces = tag_namespaces or []
    item_types = item_types or []
    include_path_prefixes = include_path_prefixes or []
    min_confidence = min_confidence or None
    setters = setters or []
    restrict_to_bookmark_namespaces = restrict_to_bookmark_namespaces or []

    page_size = page_size or 1000000  # Mostly for debugging purposes
    offset = (page - 1) * page_size

    if tags_match_any and not tags:
        # If "match any" tags are provided, but no positive tags are provided
        # We need to build a query to match on *any* of them being present
        main_query, params = build_search_query(
            tags=tags_match_any,
            negative_tags=negative_tags,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=True,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
        )
    else:
        # Basic case where we need to match all positive tags and none of the negative tags
        main_query, params = build_search_query(
            tags=tags,
            negative_tags=negative_tags,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=False,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
        )

    if tags_match_any and tags:
        # If tags "match any" are provided along with match all regular positive tags
        # We need to build a separate query to match on *any* of them being present
        # And then intersect the results with the main query
        tags_query, tags_params = build_search_query(
            tags=tags_match_any,
            negative_tags=None,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=True,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
        )

        # Append the tags query to the main query
        main_query = f"""
        {main_query}
        INTERSECT
        {tags_query}
        """
        params += tags_params

    if negative_tags_match_all:
        # If negative tags "match all" are provided
        # We need to build a separate query to match on *all* of them being present
        # And then exclude the results from the main query
        negative_tags_query, negative_tags_params = build_search_query(
            tags=negative_tags_match_all,
            negative_tags=None,
            tag_namespaces=tag_namespaces,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_types=item_types,
            include_path_prefixes=include_path_prefixes,
            any_positive_tags_match=False,
            # FTS match on path and filename
            match_path=match_path,
            match_filename=match_filename,
            # FTS match on extracted text
            match_extracted_text=match_extracted_text,
            require_extracted_type_setter_pairs=require_extracted_type_setter_pairs,
            # Restrict to bookmarks
            restrict_to_bookmarks=restrict_to_bookmarks,
            restrict_to_bookmark_namespaces=restrict_to_bookmark_namespaces,
        )

        # Append the negative tags query to the main query
        if tags_match_any and tags:
            # If we already have an INTERSECT query, we need to use it as a subquery
            main_query = f"""
            SELECT *
            FROM (
                {main_query}
            )
            EXCEPT
            {negative_tags_query}
            """
        else:
            main_query = f"""
            {main_query}
            EXCEPT
            {negative_tags_query}
            """
        params += negative_tags_params

    # First query to get the total count of items matching the criteria
    count_query = f"""
    SELECT COUNT(*)
    FROM (
        {main_query}
    )
    """
    # Debugging
    # print_search_query(count_query, params)
    cursor = conn.cursor()
    if return_total_count:
        try:
            cursor.execute(count_query, params)
        except Exception as e:
            # Debugging
            print_search_query(count_query, params)
            raise e
        total_count: int = cursor.fetchone()[0]
    else:
        total_count = 0

    # Determine order_by_clause and default order setting based on order_by value
    match order_by:
        case "rank_fts":
            if match_extracted_text:
                order_by_clause = "rank_fts"
            else:
                order_by_clause = "last_modified"
        case "rank_path_fts":
            if match_path or match_filename:
                order_by_clause = "rank_path_fts"
            else:
                order_by_clause = "last_modified"
        case "path":
            order_by_clause = "path"
            # Default order for path is ascending
            if order is None:
                order = "asc"
        case _:
            order_by_clause = "last_modified"

    # Default order for all other order_by values is descending
    if order is None:
        order = "desc"
    # Determine the order clause
    order_clause = "DESC" if order == "desc" else "ASC"

    # Second query to get the items with pagination
    query = f"""
    {main_query}
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?
    """
    query_params: List[str | int | float] = [*params, page_size, offset]
    try:
        cursor.execute(query, query_params)
    except Exception as e:
        # Debugging
        print_search_query(query, query_params)
        raise e
    results_count = cursor.rowcount
    while row := cursor.fetchone():
        file = FileSearchResult(*row[0:4])
        if check_path_exists and not os.path.exists(file.path):
            continue
        yield file, total_count
    if results_count == 0:
        return []


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


def delete_unavailable_files(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute(
        """
    DELETE FROM files
    WHERE available = 0
    """
    )
    return result.rowcount


def delete_items_without_files(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM items
        WHERE rowid IN (
            SELECT items.id
            FROM items
            LEFT JOIN files ON files.id = items.id
            WHERE files.id IS NULL
            LIMIT ?
        )
        """,
            (batch_size,),
        )

        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows

        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break

    return total_deleted


def delete_tags_without_items(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0
    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM tags_items
        WHERE rowid IN (
            SELECT tags_items.rowid
            FROM tags_items
            LEFT JOIN items ON items.id = tags_items.item_id
            WHERE items.id IS NULL
            LIMIT ?
        )
        """,
            (batch_size,),
        )

        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows

        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break

    return total_deleted


def delete_log_items_without_item(
    conn: sqlite3.Connection, batch_size: int = 10000
):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute(
            """
        DELETE FROM extraction_log_items
        WHERE rowid IN (
            SELECT extraction_log_items.rowid
            FROM extraction_log_items
            LEFT JOIN items ON items.id = extraction_log_items.item_id
            WHERE items.id IS NULL
            LIMIT ?
        )
        """,
            (batch_size,),
        )

        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows

        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break

    return total_deleted


def vacuum_database(conn: sqlite3.Connection):
    """
    Run VACUUM and ANALYZE on the database to optimize it
    """
    conn.execute("VACUUM")
    conn.execute("ANALYZE")


def get_most_common_tags(
    conn: sqlite3.Connection,
    namespace: str | None = None,
    setters: List[str] | None = [],
    confidence_threshold: float | None = None,
    limit=10,
):
    cursor = conn.cursor()
    namespace_clause = "AND tags.namespace LIKE ? || '%'" if namespace else ""
    setters_clause = (
        f"AND tags.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    confidence_clause = (
        f"AND tags_items.confidence >= ?" if confidence_threshold else ""
    )
    setters = setters or []
    query_args = [
        arg
        for arg in [namespace, *setters, confidence_threshold, limit]
        if arg is not None
    ]

    query = f"""
    SELECT namespace, name, COUNT(*) as count
    FROM tags_setters as tags
    JOIN tags_items ON tags.id = tags_items.tag_id
    {namespace_clause}
    {setters_clause}
    {confidence_clause}
    GROUP BY namespace, name
    ORDER BY count DESC
    LIMIT ?
    """
    cursor.execute(query, query_args)

    tags = cursor.fetchall()
    return tags


def get_most_common_tags_frequency(
    conn: sqlite3.Connection,
    namespace=None,
    setters: List[str] | None = [],
    confidence_threshold=None,
    limit=10,
):
    tags = get_most_common_tags(
        conn,
        namespace=namespace,
        setters=setters,
        confidence_threshold=confidence_threshold,
        limit=limit,
    )
    # Get the total number of item_setter pairs
    cursor = conn.cursor()
    setters_clause = (
        f"WHERE data_extraction_log.setter IN ({','.join(['?']*len(setters))})"
        if setters
        else ""
    )
    cursor.execute(
        f"""
        SELECT COUNT(
            DISTINCT extraction_log_items.item_id || '-' || data_extraction_log.setter
        ) AS distinct_count
        FROM extraction_log_items
        JOIN data_extraction_log
        ON extraction_log_items.log_id = data_extraction_log.id
        AND data_extraction_log.type = 'tags'
        {setters_clause}""",
        setters if setters else (),
    )
    total_items_setters = cursor.fetchone()[0]
    # Calculate the frequency
    tags = [
        (tag[0], tag[1], tag[2], tag[2] / (total_items_setters)) for tag in tags
    ]
    return tags


def update_bookmarks(
    conn: sqlite3.Connection,
    items_sha256: List[str],
    namespace: str = "default",
):
    cursor = conn.cursor()
    # Add all items as bookmarks, if they don't already exist, in a single query
    cursor.executemany(
        """
    INSERT INTO bookmarks (namespace, sha256, time_added)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    """,
        [
            (namespace, sha256, datetime.now().isoformat())
            for sha256 in items_sha256
        ],
    )

    # Remove all items that are not in the list
    cursor.execute(
        """
    DELETE FROM bookmarks
    WHERE sha256 NOT IN ({}) AND namespace = ?
    """.format(
            ",".join(["?"] * len(items_sha256)), items_sha256, namespace
        )
    )


def add_bookmark(
    conn: sqlite3.Connection,
    sha256: str,
    namespace: str = "default",
    metadata: str | None = None,
):
    cursor = conn.cursor()
    cursor.execute(
        """
    INSERT INTO bookmarks (namespace, sha256, time_added, metadata)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    """,
        (namespace, sha256, datetime.now().isoformat(), metadata),
    )


def remove_bookmark(
    conn: sqlite3.Connection, sha256: str, namespace: str = "default"
):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    """,
        (sha256, namespace),
    )


def get_bookmark_metadata(
    conn: sqlite3.Connection, sha256: str, namespace: str = "default"
):
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT metadata
    FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    """,
        (sha256, namespace),
    )
    metadata = cursor.fetchone()

    return (True, metadata[0]) if metadata else (False, None)


def delete_bookmarks_exclude_last_n(
    conn: sqlite3.Connection, n: int, namespace: str = "default"
):
    cursor = conn.cursor()
    # Delete all bookmarks except the last n based on time_added
    cursor.execute(
        """
        DELETE FROM bookmarks
        WHERE namespace = ?
        AND sha256 NOT IN (
            SELECT sha256
            FROM bookmarks
            WHERE namespace = ?
            ORDER BY time_added DESC
            LIMIT ?
        )
    """,
        (namespace, namespace, n),
    )

    conn.commit()


def get_all_bookmark_namespaces(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    # Get all bookmark namespaces, order by namespace name
    cursor.execute(
        """
        SELECT DISTINCT namespace
        FROM bookmarks
        ORDER BY namespace
    """
    )
    namespaces = cursor.fetchall()
    return [namespace[0] for namespace in namespaces]


def get_bookmarks(
    conn: sqlite3.Connection,
    namespace: str = "default",
    page_size=1000,
    page=1,
    order_by="time_added",
    order=None,
) -> Tuple[List[FileSearchResult], int]:

    if page_size < 1:
        page_size = 1000000
    offset = (page - 1) * page_size

    # Fetch bookmarks with their paths, prioritizing available files
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(DISTINCT bookmarks.sha256)
        FROM bookmarks
        JOIN files
        ON bookmarks.sha256 = files.sha256
        WHERE bookmarks.namespace = ?
    """,
        (namespace,),
    )
    total_results = cursor.fetchone()[0]
    # Can order by time_added, path, or last_modified

    if order_by == "path":
        order_by_clause = "path"
        if order == None:
            order = "asc"
    elif order_by == "last_modified":
        order_by_clause = "MAX(any_files.last_modified)"
        if order == None:
            order = "desc"
    else:
        order_by_clause = "bookmarks.time_added"
        if order == None:
            order = "desc"

    order_clause = "DESC" if order == "desc" else "ASC"
    cursor.execute(
        f"""
        SELECT 
        COALESCE(available_files.path, any_files.path) as path,
        bookmarks.sha256,
        COALESCE(MAX(available_files.last_modified), MAX(any_files.last_modified)) as last_modified,
        items.type
        FROM bookmarks
        LEFT JOIN files AS available_files 
               ON bookmarks.sha256 = available_files.sha256 
               AND available_files.available = 1
        JOIN files AS any_files 
               ON bookmarks.sha256 = any_files.sha256
        JOIN items ON any_files.item_id = items.id
        WHERE bookmarks.namespace = ?
        GROUP BY bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """,
        (namespace, page_size, offset),
    )

    bookmarks: List[FileSearchResult] = []
    for row in cursor.fetchall():
        item = FileSearchResult(*row)
        if not os.path.exists(item.path):
            if file := get_existing_file_for_sha256(conn, item.sha256):
                item.path = file.path
                bookmarks.append(item)
            # If the path does not exist and no working path is found, skip this item
            continue
        bookmarks.append(item)

    return bookmarks, total_results


def insert_extracted_text(
    conn: sqlite3.Connection,
    item_sha256: str,
    log_id: int,
    text: str,
    language: str | None,
    confidence: float | None,
) -> int:
    """
    Insert extracted text into the database
    """
    text = text.strip()
    if len(text) < 3:
        return -1

    item_id = get_item_id(conn, item_sha256)
    assert item_id is not None, f"Item with SHA256 {item_sha256} not found"

    cursor = conn.cursor()

    sql = """
    INSERT INTO extracted_text (item_id, log_id, language, confidence, text)
    VALUES (?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (item_id, log_id, language, confidence, text))
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def delete_text_extracted_by_setter(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM extracted_text
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = ?
    )
    """,
        (model_type, setter),
    )


def get_existing_type_setter_pairs(
    conn: sqlite3.Connection,
) -> List[Tuple[str, str]]:
    """
    Returns all the currently existing (type, setter) pairs from the data_extraction_log table.

    Args:
        conn (sqlite3.Connection): The SQLite database connection.

    Returns:
        List[Tuple[str, str]]: A list of tuples containing (type, setter) pairs.
    """
    query = """
    SELECT DISTINCT type, setter
    FROM data_extraction_log
    """

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results
