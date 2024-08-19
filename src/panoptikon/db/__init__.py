import logging
import os
import sqlite3

import sqlite_vec
from alembic import command
from alembic.config import Config

from panoptikon.db.utils import trigger_exists

logger = logging.getLogger(__name__)


def get_database_connection(
    write_lock: bool, user_data_wl: bool = False
) -> sqlite3.Connection:
    db_file, user_db_file, storage_db_file = get_db_paths()

    readonly_mode = os.environ.get("READONLY", "false").lower() in ["true", "1"]
    # Attach index database
    if write_lock and not readonly_mode:
        write_lock = True
        # Acquire a write lock
        logger.debug(f"Opening index database in write mode")
        conn = sqlite3.connect(db_file)
        logger.debug(f"Attaching storage database in write mode")
        conn.execute(f"ATTACH DATABASE '{storage_db_file}' AS storage")
        cursor = conn.cursor()
        # Enable Write-Ahead Logging (WAL) mode
        cursor.execute("PRAGMA journal_mode=WAL")
    else:
        write_lock = False
        # Read-only connection
        conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)
        # Attach storage database
        conn.execute(
            f"ATTACH DATABASE 'file:{storage_db_file}?mode=ro' AS storage"
        )

    # Attach user data database
    if user_data_wl and not readonly_mode:
        logger.debug(f"Opening user data database in write mode")
        conn.execute(f"ATTACH DATABASE '{user_db_file}' AS user_data")
        # Enable Write-Ahead Logging (WAL) mode
        cursor = conn.cursor()
        cursor.execute("PRAGMA user_data.journal_mode=WAL")
    elif not write_lock:
        conn.execute(
            f"ATTACH DATABASE 'file:{user_db_file}?mode=ro' AS user_data"
        )
    # Enable foreign key constraints
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    load_sqlite_vec(conn)
    return conn


def get_db_paths():
    data_dir = os.getenv("DATA_FOLDER", "data")
    index_db_dir = os.path.join(data_dir, "index")
    user_data_db_dir = os.path.join(data_dir, "user_data")
    storage_db_dir = os.path.join(data_dir, "storage")
    # Ensure the directory exists
    os.makedirs(index_db_dir, exist_ok=True)
    os.makedirs(user_data_db_dir, exist_ok=True)
    os.makedirs(storage_db_dir, exist_ok=True)

    index, user_data, storage = get_db_names()

    db_file = os.path.join(index_db_dir, f"{index}.db")
    user_db_file = os.path.join(user_data_db_dir, f"{user_data}.db")
    storage_db_file = os.path.join(storage_db_dir, f"{storage}.db")
    return db_file, user_db_file, storage_db_file


def get_db_names():
    index = os.getenv("INDEX_DB", "default")
    user_data = os.getenv("USER_DATA_DB", "default")
    storage = os.getenv("STORAGE_DB", index)  # Default to same name as index
    return index, user_data, storage


def load_sqlite_vec(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn


def run_migrations():
    alembic_cfg = Config(os.path.join(os.path.dirname(__file__), "alembic.ini"))
    command.upgrade(alembic_cfg, "head")


def initialize_database(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        sha256 TEXT UNIQUE NOT NULL,
        md5 TEXT NOT NULL,
        type TEXT NOT NULL,              -- Mime type of the file (e.g. image/jpeg)
        size INTEGER,                    -- Size of the file in bytes
        width INTEGER,                   -- Width of the frame in pixels
        height INTEGER,                  -- Height of the frame in pixels
        duration REAL,                   -- Duration of the video/audio in seconds
        audio_tracks INTEGER,            -- Number of audio tracks
        video_tracks INTEGER,            -- Number of video tracks
        subtitle_tracks INTEGER,         -- Number of subtitle tracks
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
        scan_id INTEGER NOT NULL,
        available BOOLEAN NOT NULL,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(item_id) REFERENCES items(id)
        FOREIGN KEY(scan_id) REFERENCES file_scans(id) ON DELETE CASCADE
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS file_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,         -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT,           -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        total_available INTEGER NOT NULL DEFAULT 0,
        new_items INTEGER NOT NULL DEFAULT 0,
        unchanged_files INTEGER NOT NULL DEFAULT 0,
        new_files INTEGER NOT NULL DEFAULT 0,
        modified_files INTEGER NOT NULL DEFAULT 0,
        marked_unavailable INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        false_changes INTEGER NOT NULL DEFAULT 0,
        metadata_time REAL DEFAULT 0,
        hashing_time REAL DEFAULT 0,
        thumbgen_time REAL DEFAULT 0
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY,
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        UNIQUE(namespace, name)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS tags_items (
        item_data_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        confidence REAL DEFAULT 1.0,
        UNIQUE(item_data_id, tag_id),
        FOREIGN KEY(item_data_id) REFERENCES item_data(id) ON DELETE CASCADE
        FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS data_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        completed BOOLEAN NOT NULL DEFAULT 0
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS data_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER,                 -- Foreign key to data_jobs table
        start_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT NOT NULL,                 -- Using TEXT to store ISO-8601 formatted datetime
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
        data_load_time REAL DEFAULT 0,
        inference_time REAL DEFAULT 0,
        FOREIGN KEY(job_id) REFERENCES data_jobs(id) ON DELETE SET NULL
    )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS setters (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time_added TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        included BOOLEAN NOT NULL,              -- BOOLEAN to indicate if folder is included or specifically excluded
        UNIQUE(path)  -- Unique constraint on path
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS user_data.bookmarks (
        user TEXT NOT NULL, -- User who created the bookmark
        namespace TEXT NOT NULL, -- Namespace for the bookmark
        sha256 TEXT NOT NULL, -- SHA256 of the item
        time_added TEXT NOT NULL, -- Using TEXT to store ISO-8601 formatted datetime
        metadata TEXT, -- JSON string to store additional metadata
        PRIMARY KEY(user, namespace, sha256)
    )
    """
    )

    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS item_data (
        id INTEGER PRIMARY KEY,
        item_id INTEGER NOT NULL,         -- Reference to the item that the data is extracted from
        job_id INTEGER,                   -- Reference to the job that extracted the data
        setter_id INTEGER NOT NULL,       -- Reference to the setter that extracted the data from the item
        data_type TEXT NOT NULL,          -- Type of data extracted (e.g. text, image, etc.)
        idx INTEGER NOT NULL,             -- Index of the data in the item (page number, frame number, etc.)
        source_id INTEGER,                -- Reference to a previous item_data from which data was further processed
        is_origin BOOLEAN,                -- Whether the data is from the item directly or derived from other data. True if it is, NULL if not
        is_placeholder BOOLEAN,           -- Whether the data is a placeholder (e.g. no data extracted) Needed to mark an item as processed
        UNIQUE(item_id, setter_id, data_type, idx, is_origin),            -- Origin data should be unique per item, setter, datatype and index
        UNIQUE(item_id, setter_id, data_type, idx, source_id),            -- Derived extractions should be unique per data they are derived from (and setter, datatype, index)
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
        FOREIGN KEY(job_id) REFERENCES data_jobs(id) ON DELETE CASCADE,
        FOREIGN KEY(setter_id) REFERENCES setters(id) ON DELETE CASCADE,
        FOREIGN KEY(source_id) REFERENCES item_data(id) ON DELETE CASCADE,
        CHECK ((is_origin = TRUE AND source_id IS NULL) OR (is_origin IS NULL AND source_id IS NOT NULL))
    )
    """
    )
    # Create table for extracted text
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS extracted_text (
        id INTEGER PRIMARY KEY,
        language TEXT,
        language_confidence REAL,
        confidence REAL,
        text TEXT NOT NULL,
        FOREIGN KEY(id) REFERENCES item_data(id) ON DELETE CASCADE
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
            tokenize="trigram case_sensitive 0"
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
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS embeddings (
            id INTEGER PRIMARY KEY,
            embedding float[],
            FOREIGN KEY(id) REFERENCES item_data(id) ON DELETE CASCADE
        );
        """
    )

    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS extraction_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enabled BOOLEAN NOT NULL DEFAULT 1,
                rule TEXT NOT NULL
            );
        """
    )
    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS extraction_rules_setters (
                rule_id INTEGER NOT NULL,
                setter_name TEXT NOT NULL,
                FOREIGN KEY(rule_id) REFERENCES extraction_rules(id) ON DELETE CASCADE
                UNIQUE(rule_id, setter_name)
            );
        """
    )

    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS system_config (
                k string NOT NULL UNIQUE,
                v
            );
        """
    )

    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS model_group_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                batch_size INTEGER NOT NULL,
                threshold REAL
            );
        """
    )

    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS storage.thumbnails (
                id INTEGER PRIMARY KEY,
                item_sha256 TEXT NOT NULL,
                idx INTEGER NOT NULL,
                item_mime_type TEXT NOT NULL,        -- MIME type of the source file
                width INTEGER NOT NULL,              -- Width of the thumbnail in pixels
                height INTEGER NOT NULL,             -- Height of the thumbnail in pixels
                version INTEGER NOT NULL,            -- Version of the thumbnail creation process
                thumbnail BLOB NOT NULL,             -- The thumbnail image data (stored as a BLOB)
                UNIQUE(item_sha256, idx)
            );
        """
    )

    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS storage.frames (
                id INTEGER PRIMARY KEY,
                item_sha256 TEXT NOT NULL,
                idx INTEGER NOT NULL,
                item_mime_type TEXT NOT NULL,        -- MIME type of the source file
                width INTEGER NOT NULL,              -- Width of the frame in pixels
                height INTEGER NOT NULL,             -- Height of the frame in pixels
                version INTEGER NOT NULL,            -- Version of the frame extraction process
                frame BLOB NOT NULL,                 -- The extracted frame image data (stored as a BLOB)
                UNIQUE(item_sha256, idx)
            );
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
        ("files", ["scan_id"]),
        ("files", ["filename"]),
        ("files", ["item_id"]),
        ("file_scans", ["start_time"]),
        ("file_scans", ["end_time"]),
        ("file_scans", ["path"]),
        ("data_log", ["start_time"]),
        ("data_log", ["end_time"]),
        ("data_log", ["threshold"]),
        ("data_log", ["setter"]),
        ("data_log", ["type"]),
        ("data_log", ["batch_size"]),
        ("data_log", ["image_files"]),
        ("data_log", ["video_files"]),
        ("data_log", ["other_files"]),
        ("data_log", ["total_segments"]),
        ("data_log", ["errors"]),
        ("data_log", ["total_remaining"]),
        ("data_log", ["data_load_time"]),
        ("data_log", ["inference_time"]),
        ("data_log", ["job_id"]),
        ("folders", ["time_added"]),
        ("folders", ["path"]),
        ("folders", ["included"]),
        ("user_data.bookmarks", ["time_added"]),
        ("user_data.bookmarks", ["sha256"]),
        ("user_data.bookmarks", ["metadata"]),
        ("user_data.bookmarks", ["namespace"]),
        ("user_data.bookmarks", ["user"]),
        ("item_data", ["item_id"]),
        ("item_data", ["job_id"]),
        ("item_data", ["setter_id"]),
        ("item_data", ["source_id"]),
        ("item_data", ["is_origin"]),
        ("item_data", ["data_type"]),
        ("tags_items", ["tag_id"]),
        ("tags_items", ["item_data_id"]),
        ("tags_items", ["confidence"]),
        ("tags", ["namespace"]),
        ("tags", ["name"]),
        ("tags", ["namespace", "name"]),
        ("extracted_text", ["language"]),
        ("extracted_text", ["confidence"]),
        ("extracted_text", ["language_confidence"]),
        ("extraction_rules_setters", ["rule_id"]),
        ("extraction_rules_setters", ["setter_name"]),
        ("setters", ["name"]),
        ("items", ["width"]),
        ("items", ["height"]),
        ("items", ["duration"]),
        ("items", ["audio_tracks"]),
        ("items", ["video_tracks"]),
        ("items", ["subtitle_tracks"]),
        ("extraction_rules", ["enabled"]),
        ("system_config", ["k"]),
        ("model_group_settings", ["name"]),
        ("model_group_settings", ["batch_size"]),
        ("model_group_settings", ["threshold"]),
        ("storage.thumbnails", ["item_sha256"]),
        ("storage.thumbnails", ["idx"]),
        ("storage.thumbnails", ["item_mime_type"]),
        ("storage.thumbnails", ["width"]),
        ("storage.thumbnails", ["height"]),
        ("storage.thumbnails", ["version"]),
        ("storage.frames", ["item_sha256"]),
        ("storage.frames", ["idx"]),
        ("storage.frames", ["item_mime_type"]),
        ("storage.frames", ["width"]),
        ("storage.frames", ["height"]),
        ("storage.frames", ["version"]),
    ]

    for table, columns in indices:
        if "." in table:
            database, table = table.split(".")
            index_name = f"{database}.idx_{table}_{'_'.join(columns)}"
        else:
            index_name = f"idx_{table}_{'_'.join(columns)}"

        sql = f"""
            CREATE INDEX IF NOT EXISTS
            {index_name} ON {table}({', '.join(columns)})
            """
        try:
            cursor.execute(sql)
        except sqlite3.OperationalError as e:
            logger.error(sql)
            raise e


def get_item_id(conn: sqlite3.Connection, sha256: str) -> int | None:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM items WHERE sha256 = ?", (sha256,))
    item_id = cursor.fetchone()
    if item_id:
        return item_id[0]
    return None
