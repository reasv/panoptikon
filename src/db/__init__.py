import os
import sqlite3

from src.db.utils import is_column_in_table, trigger_exists


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


def get_item_id(conn: sqlite3.Connection, sha256: str) -> int | None:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM items WHERE sha256 = ?", (sha256,))
    item_id = cursor.fetchone()
    if item_id:
        return item_id[0]
    return None
