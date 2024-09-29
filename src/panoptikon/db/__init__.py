import logging
import os
import sqlite3
from typing import Literal

import sqlite_vec
from alembic import command
from alembic.config import Config

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
    load_sqlite_vec(conn)
    return conn


def get_db_paths(index_db: str | None = None, user_data_db: str | None = None):
    data_dir = os.getenv("DATA_FOLDER", "data")
    index_db_dir = os.path.join(data_dir, "index")
    user_data_db_dir = os.path.join(data_dir, "user_data")
    storage_db_dir = os.path.join(data_dir, "storage")
    # Ensure the directory exists
    os.makedirs(index_db_dir, exist_ok=True)
    os.makedirs(user_data_db_dir, exist_ok=True)
    os.makedirs(storage_db_dir, exist_ok=True)

    index, user_data, storage = get_db_names()

    # Override the database names if provided
    if index_db:
        index = index_db
        storage = index_db
    if user_data_db:
        user_data = user_data_db

    db_file = os.path.join(index_db_dir, f"{index}.db")
    user_db_file = os.path.join(user_data_db_dir, f"{user_data}.db")
    storage_db_file = os.path.join(storage_db_dir, f"{storage}.db")
    return db_file, user_db_file, storage_db_file


def get_db_names():
    index = os.getenv("INDEX_DB", "default")
    user_data = os.getenv("USER_DATA_DB", "default")
    storage = os.getenv("STORAGE_DB", index)  # Default to same name as index
    return index, user_data, storage


def set_db_names(
    index_db: str, user_data_db: str, storage_db: str | None = None
):
    os.environ["INDEX_DB"] = index_db
    os.environ["USER_DATA_DB"] = user_data_db
    if storage_db:
        os.environ["STORAGE_DB"] = storage_db
    else:
        os.environ["STORAGE_DB"] = index_db


def get_db_lists():
    db_file, user_db_file, _ = get_db_paths()
    # Get the folders containing the databases
    index_db_dir = os.path.dirname(db_file)
    user_data_db_dir = os.path.dirname(user_db_file)
    # Get the list of databases in the folders
    index_dbs = [
        os.path.splitext(f)[0]
        for f in os.listdir(index_db_dir)
        if f.endswith(".db")
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
