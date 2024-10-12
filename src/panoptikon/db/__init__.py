import logging
import os
import sqlite3
from typing import List, Literal, Tuple

import sqlite_vec
from alembic import command
from alembic.config import Config

from panoptikon.types import FileRecord, ItemRecord

logger = logging.getLogger(__name__)


def get_database_connection(
    write_lock: bool,
    user_data_wl: bool = False,
    index_db: str | None = None,
    user_data_db: str | None = None,
) -> sqlite3.Connection:
    db_file, user_db_file, storage_db_file = get_db_paths(
        index_db=index_db,
        user_data_db=user_data_db,  # Overwrite the default database names
    )

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
    cursor.execute("PRAGMA case_sensitive_like = ON")
    load_sqlite_vec(conn)
    return conn


def get_db_paths(index_db: str | None = None, user_data_db: str | None = None):
    data_dir = os.getenv("DATA_FOLDER", "data")
    index_db_dir = os.path.join(data_dir, "index")
    user_data_db_dir = os.path.join(data_dir, "user_data")
    # Ensure the directory exists
    os.makedirs(index_db_dir, exist_ok=True)
    os.makedirs(user_data_db_dir, exist_ok=True)

    index, user_data = get_db_default_names()

    # Override the database names if provided
    if index_db:
        index = index_db
    if user_data_db:
        user_data = user_data_db

    index_dir = os.path.join(index_db_dir, index)
    os.makedirs(index_dir, exist_ok=True)
    index_db_file = os.path.join(index_dir, "index.db")
    storage_db_file = os.path.join(index_dir, "storage.db")
    user_db_file = os.path.join(user_data_db_dir, f"{user_data}.db")
    return index_db_file, user_db_file, storage_db_file


def get_db_default_names():
    index = os.getenv("INDEX_DB", "default")
    user_data = os.getenv("USER_DATA_DB", "default")
    return index, user_data


def set_db_names(index_db: str, user_data_db: str):
    os.environ["INDEX_DB"] = index_db
    os.environ["USER_DATA_DB"] = user_data_db


def get_db_lists():
    user_db_file = get_db_paths()[1]
    # Get the folders containing the databases
    data_dir = os.getenv("DATA_FOLDER", "data")
    index_db_dir = os.path.join(data_dir, "index")
    user_data_db_dir = os.path.dirname(user_db_file)
    # Get the list of databases in the folders
    index_dbs = [
        f
        for f in os.listdir(index_db_dir)
        if os.path.exists(
            os.path.join(index_db_dir, f, "index.db"),
        )
    ]
    user_data_dbs = [
        os.path.splitext(f)[0]
        for f in os.listdir(user_data_db_dir)
        if f.endswith(".db")
    ]
    return index_dbs, user_data_dbs


def load_sqlite_vec(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    return conn


def run_migrations():
    for db in ["index", "user_data", "storage"]:
        run_migrations_for_db(db)


def run_migrations_for_db(db: str):
    alembic_path = os.path.join(os.path.dirname(__file__), "migrations", db)
    logger.debug(f"Running migrations using {alembic_path}")
    alembic_cfg = Config(os.path.join(alembic_path, "alembic.ini"))
    alembic_cfg.set_main_option("script_location", alembic_path)
    command.upgrade(alembic_cfg, "head")


def get_item_id(conn: sqlite3.Connection, sha256: str) -> int | None:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM items WHERE sha256 = ?", (sha256,))
    item_id = cursor.fetchone()
    if item_id:
        return item_id[0]
    return None


ItemIdentifierType = Literal[
    "item_id", "file_id", "data_id", "path", "sha256", "md5"
]


def get_item_metadata(
    conn: sqlite3.Connection,
    identifier: str | int,
    identifier_type: ItemIdentifierType,
) -> Tuple[ItemRecord | None, List[FileRecord]]:
    cursor = conn.cursor()
    select = """
    SELECT
        items.id AS item_id,
        sha256,
        md5,
        type,
        size,
        width,
        height,
        duration,
        audio_tracks,
        video_tracks,
        subtitle_tracks,
        time_added,
        files.id AS file_id,
        files.path AS path,
        files.filename,
        files.last_modified
    FROM items
        JOIN files ON items.id = files.item_id
    """
    if identifier_type in ["item_id", "file_id", "path", "sha256", "md5"]:
        query = f"""
        {select}
        WHERE {identifier_type} = ?
        ORDER BY files.available DESC
        """
    elif identifier_type == "data_id":
        query = f"""
        {select}
        JOIN item_data ON items.id = item_data.item_id
        WHERE item_data.id = ?
        ORDER BY files.available DESC
        """
    else:
        raise ValueError(f"Invalid identifier type: {identifier_type}")

    cursor.execute(query, (identifier,))
    item_record = None
    files: List[FileRecord] = []
    while row := cursor.fetchone():
        # destructuring the row
        (
            item_id,
            sha256,
            md5,
            type,
            size,
            width,
            height,
            duration,
            audio_tracks,
            video_tracks,
            subtitle_tracks,
            time_added,
            file_id,
            path,
            filename,
            last_modified,
        ) = row
        if not item_record:
            item_record = ItemRecord(
                id=item_id,
                sha256=sha256,
                md5=md5,
                type=type,
                size=size,
                width=width,
                height=height,
                duration=duration,
                audio_tracks=audio_tracks,
                video_tracks=video_tracks,
                subtitle_tracks=subtitle_tracks,
                time_added=time_added,
            )
        if os.path.exists(path):
            files.append(
                FileRecord(
                    file_id, sha256, path, last_modified, filename=filename
                )
            )
    return item_record, files
