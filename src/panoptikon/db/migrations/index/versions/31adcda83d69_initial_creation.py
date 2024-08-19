"""Initial creation

Revision ID: 31adcda83d69
Revises: 
Create Date: 2024-08-19 23:03:26.259523

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "31adcda83d69"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
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

    op.execute(
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

    op.execute(
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

    op.execute(
        """
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY,
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        UNIQUE(namespace, name)
    )
    """
    )

    op.execute(
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

    op.execute(
        """
    CREATE TABLE IF NOT EXISTS data_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        completed BOOLEAN NOT NULL DEFAULT 0
    )
    """
    )

    op.execute(
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

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS setters (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )

    op.execute(
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

    op.execute(
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
    op.execute(
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

    op.execute(
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
    # Triggers to keep the FTS index up to date.
    op.execute(
        """
        CREATE TRIGGER extracted_text_ai AFTER INSERT ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(rowid, text)
            VALUES (new.id, new.text);
        END;
    """
    )
    op.execute(
        """
        CREATE TRIGGER extracted_text_ad AFTER DELETE ON extracted_text BEGIN
            INSERT INTO extracted_text_fts(extracted_text_fts, rowid, text)
            VALUES('delete', old.id, old.text);
        END;
    """
    )

    op.execute(
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
    op.execute(
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

    op.execute(
        """
        CREATE TRIGGER files_path_ai AFTER INSERT ON files BEGIN
            INSERT INTO files_path_fts(rowid, path, filename)
            VALUES (new.id, new.path, new.filename);
        END;
    """
    )

    op.execute(
        """
        CREATE TRIGGER files_path_ad AFTER DELETE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename)
            VALUES('delete', old.id, old.path, old.filename);
        END;
        """
    )

    op.execute(
        """
        CREATE TRIGGER files_path_au AFTER UPDATE ON files BEGIN
            INSERT INTO files_path_fts(files_path_fts, rowid, path, filename)
            VALUES('delete', old.id, old.path, old.filename);
            INSERT INTO files_path_fts(rowid, path, filename)
            VALUES (new.id, new.path, new.filename);
        END;
    """
    )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS embeddings (
            id INTEGER PRIMARY KEY,
            embedding float[],
            FOREIGN KEY(id) REFERENCES item_data(id) ON DELETE CASCADE
        );
        """
    )

    op.execute(
        f"""
            CREATE TABLE IF NOT EXISTS extraction_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                enabled BOOLEAN NOT NULL DEFAULT 1,
                rule TEXT NOT NULL
            );
        """
    )
    op.execute(
        f"""
            CREATE TABLE IF NOT EXISTS extraction_rules_setters (
                rule_id INTEGER NOT NULL,
                setter_name TEXT NOT NULL,
                FOREIGN KEY(rule_id) REFERENCES extraction_rules(id) ON DELETE CASCADE
                UNIQUE(rule_id, setter_name)
            );
        """
    )

    op.execute(
        f"""
            CREATE TABLE IF NOT EXISTS system_config (
                k string NOT NULL UNIQUE,
                v
            );
        """
    )

    op.execute(
        f"""
            CREATE TABLE IF NOT EXISTS model_group_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                batch_size INTEGER NOT NULL,
                threshold REAL
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
        ("item_data", ["item_id"]),
        ("item_data", ["job_id"]),
        ("item_data", ["setter_id"]),
        ("item_data", ["source_id"]),
        ("item_data", ["is_origin"]),
        ("item_data", ["data_type"]),
        ("item_data", ["idx"]),
        ("item_data", ["is_placeholder"]),
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
        op.execute(sql)
