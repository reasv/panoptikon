from dataclasses import dataclass
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple

from src.utils import normalize_path, get_mime_type
from src.types import FileScanData, ItemWithPath

def get_database_connection(force_readonly=False) -> sqlite3.Connection:
    # Check if we are in read-only mode
    db_file = os.getenv('DB_FILE', './db/sqlite.db')
    if force_readonly or os.environ.get('READONLY', 'false').lower() == 'true':
        # Use a read-only connection
        conn = sqlite3.connect(f'file:{db_file}?mode=ro', uri=True)
    else:
        conn = sqlite3.connect(db_file)
    return conn

def initialize_database(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys = ON')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS items (
        sha256 TEXT PRIMARY KEY,
        md5 TEXT NOT NULL,
        type TEXT,
        size INTEGER,          -- Size of the file in bytes
        time_added TEXT NOT NULL         -- Using TEXT to store ISO-8601 formatted datetime
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        sha256 TEXT NOT NULL,
        item INTEGER NOT NULL,            -- Foreign key to items table
        path TEXT UNIQUE NOT NULL,        -- Ensuring path is unique
        last_modified TEXT NOT NULL,      -- Using TEXT to store ISO-8601 formatted datetime
        last_seen TEXT NOT NULL,          -- Using TEXT to store ISO-8601 formatted datetime
        available BOOLEAN NOT NULL,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(sha256) REFERENCES items(sha256)
        FOREIGN KEY(item) REFERENCES items(rowid)
    )
    ''')

    cursor.execute('''
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
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tags_setters (
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        setter TEXT NOT NULL,
        PRIMARY KEY(namespace, name, setter)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tags_items (
        item INTEGER NOT NULL,
        tag INTEGER NOT NULL,
        confidence REAL DEFAULT 1.0,
        UNIQUE(item, tag),
        FOREIGN KEY(item) REFERENCES items(rowid) ON DELETE CASCADE
        FOREIGN KEY(tag) REFERENCES tags_setters(rowid) ON DELETE CASCADE
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tag_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        setter TEXT NOT NULL,
        threshold REAL NOT NULL,
        image_files INTEGER NOT NULL,
        video_files INTEGER NOT NULL,
        other_files INTEGER NOT NULL DEFAULT 0,
        video_frames INTEGER NOT NULL,
        total_frames INTEGER NOT NULL,
        errors INTEGER NOT NULL,
        timeouts INTEGER NOT NULL,
        total_remaining INTEGER NOT NULL,
        UNIQUE(start_time)       -- Unique constraint on time
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS folders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time_added TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        included BOOLEAN NOT NULL,       -- BOOLEAN to indicate if folder is included or specifically excluded
        UNIQUE(path)  -- Unique constraint on path
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bookmarks (
        namespace TEXT NOT NULL, -- Namespace for the bookmark
        sha256 TEXT NOT NULL, -- SHA256 of the item
        time_added TEXT NOT NULL, -- Using TEXT to store ISO-8601 formatted datetime
        metadata TEXT, -- JSON string to store additional metadata
        FOREIGN KEY(sha256) REFERENCES items(sha256) ON DELETE CASCADE,
        PRIMARY KEY(namespace, sha256)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS item_tag_scans (
        item TEXT NOT NULL,
        setter TEXT NOT NULL,
        last_scan TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        tags_set INTEGER NOT NULL,
        tags_removed INTEGER NOT NULL,
        UNIQUE(item, setter)                   -- Unique constraint on item and setter
        FOREIGN KEY(item) REFERENCES items(sha256) ON DELETE CASCADE
    )
    ''')

    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_md5 ON items(md5)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_type ON items(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_time_added ON items(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_modified ON files(last_modified)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_available ON files(available)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)')  # Explicit index on path
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    if is_column_in_table(conn, 'files', 'item'):
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_item ON files(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_start_time ON file_scans(start_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_end_time ON file_scans(end_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_path ON file_scans(path)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_start_time ON tag_scans(start_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_end_time ON tag_scans(end_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_setter ON tag_scans(setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_time_added ON folders(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_path ON folders(path)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_included ON folders(included)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_time_added ON bookmarks(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_sha256 ON bookmarks(sha256)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_metadata ON bookmarks(metadata)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_namespace ON bookmarks(namespace)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_item_tag_scans_item ON item_tag_scans(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_item_tag_scans_setter ON item_tag_scans(setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_item_tag_scans_last_scan ON item_tag_scans(last_scan)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_item_tag_scans_tags_set ON item_tag_scans(tags_set)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_item_tag_scans_tags_removed ON item_tag_scans(tags_removed)')
    # Create indexes for tags_items
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_items_item ON tags_items(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_items_tag ON tags_items(tag)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_items_confidence ON tags_items(confidence)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_items_item_tag ON tags_items(item, tag)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_items_tag_item ON tags_items(tag, item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_namespace ON tags_setters(namespace)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_name ON tags_setters(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_setter ON tags_setters(setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_namespace_name ON tags_setters(namespace, name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_namespace_setter ON tags_setters(namespace, setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setters_name_setter ON tags_setters(name, setter)')

def create_tag_setter(conn: sqlite3.Connection, namespace, name, setter):
    cursor = conn.cursor()
    result = cursor.execute('''
    INSERT INTO tags_setters (namespace, name, setter)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, name, setter) DO NOTHING
    ''', (namespace, name, setter))

    tag_setter_inserted = result.rowcount > 0
    if tag_setter_inserted and cursor.lastrowid is not None:
        rowid: int = cursor.lastrowid
    else:
        rowid: int = cursor.execute('SELECT rowid FROM tags_setters WHERE namespace = ? AND name = ? AND setter = ?', (namespace, name, setter)).fetchone()[0]
    return rowid

def insert_tag_item(conn: sqlite3.Connection, item_rowid: int, tag_rowid: int, confidence = 1.0):
    # Round confidence to 3 decimal places
    confidence_float = round(float(confidence), 4)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tags_items (item, tag, confidence)
    VALUES (?, ?, ? )
    ON CONFLICT(item, tag) DO UPDATE SET confidence=excluded.confidence
    ''', (item_rowid, tag_rowid, confidence_float))

def get_item_rowid(conn: sqlite3.Connection, sha256: str) -> int | None:
    cursor = conn.cursor()
    cursor.execute('SELECT rowid FROM items WHERE sha256 = ?', (sha256,))
    rowid = cursor.fetchone()
    if rowid:
        return rowid[0]
    return None

def update_file_data(conn: sqlite3.Connection, scan_time: str, file_data: FileScanData):
    cursor = conn.cursor()
    sha256 = file_data.sha256
    md5 = file_data.md5
    mime_type = file_data.mime_type
    file_size = file_data.size
    path = file_data.path
    last_modified = file_data.last_modified
    path_in_db = file_data.path_in_db
    file_modified = file_data.modified

    item_insert_result = cursor.execute('''
    INSERT INTO items (sha256, md5, type, size, time_added)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO NOTHING
    ''', (sha256, md5, mime_type, file_size, scan_time))

    # We need to check if the item was inserted
    item_inserted = item_insert_result.rowcount > 0

    # Get the rowid of the inserted item, if it was inserted
    item_rowid: int | None = cursor.lastrowid if item_inserted else None
    
    file_updated = False
    if path_in_db and not file_modified:
        # Path exists and has not changed, update last_seen and available
        file_update_result = cursor.execute('''
        UPDATE files
        SET last_seen = ?, available = TRUE
        WHERE path = ?
        ''', (scan_time, path))

        file_updated = file_update_result.rowcount > 0

    file_deleted = False
    file_inserted = False
    if not path_in_db or file_modified:
        # If the path already exists, delete the old entry
        file_delete_result = cursor.execute('DELETE FROM files WHERE path = ?', (path,))
        file_deleted = file_delete_result.rowcount > 0

        if not item_rowid:
            # If the item was not inserted, get the rowid from the database
            item_rowid = cursor.execute('SELECT rowid FROM items WHERE sha256 = ?', (sha256,)).fetchone()[0]

        # Path does not exist or has been modified, insert new
        file_insert_result = cursor.execute('''
        INSERT INTO files (sha256, item, path, last_modified, last_seen, available)
        VALUES (?, ?, ?, ?, ?, TRUE)
        ''', (sha256, item_rowid, path, last_modified, scan_time))
        file_inserted = file_insert_result.rowcount > 0

    return item_inserted, file_updated, file_deleted, file_inserted

def add_file_scan(conn: sqlite3.Connection, scan_time: str, end_time: str, path: str, new_items: int, unchanged_files: int, new_files: int, modified_files: int, marked_unavailable: int, errors: int, total_available: int):
    """
    Logs a file scan into the database
    """
    cursor = conn.cursor()
    insert_result = cursor.execute('''
    INSERT INTO file_scans (start_time, end_time, path, total_available, new_items, unchanged_files, new_files, modified_files, marked_unavailable, errors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (scan_time, end_time, path, total_available, new_items, unchanged_files, new_files, modified_files, marked_unavailable, errors))
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

def get_file_scan_by_id(conn: sqlite3.Connection, scan_id: int) -> FileScanRecord | None:
    cursor = conn.cursor()
    cursor.execute('''
    SELECT *
    FROM file_scans
    WHERE id = ?
    ''', (scan_id,))
    scan_record = cursor.fetchone()
    if scan_record:
        return FileScanRecord(*scan_record)
    return None

def get_all_file_scans(conn: sqlite3.Connection) -> List[FileScanRecord]:
    cursor = conn.cursor()
    # Order by start_time in descending order
    cursor.execute('SELECT * FROM file_scans ORDER BY start_time DESC')
    scan_records = cursor.fetchall()
    return [FileScanRecord(*scan_record) for scan_record in scan_records]

def is_column_in_table(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info({table})')
    columns = cursor.fetchall()
    return any(column[1] == column for column in columns)

def mark_unavailable_files(conn: sqlite3.Connection, scan_time: str, path: str):
    """
    Mark files as unavailable if their path is a subpath of `path` and they were not seen during the scan at `scan_time`
    """
    cursor = conn.cursor()

    # Count files to be marked as unavailable
    precount_result = cursor.execute('''
    SELECT COUNT(*)
    FROM files
    WHERE last_seen != ?
    AND path LIKE ?
    ''', (scan_time, path + '%'))

    marked_unavailable = precount_result.fetchone()[0]

    # If a file has not been seen in scan that happened at scan_time, mark it as unavailable
    cursor.execute('''
        UPDATE files
        SET available = FALSE
        WHERE last_seen != ?
        AND path LIKE ?
    ''', (scan_time, path + '%'))

    # Count available files
    result_available = cursor.execute('''
        SELECT COUNT(*)
        FROM files
        WHERE available = TRUE
        AND path LIKE ?
    ''', (path + '%',))
    available_files: int = result_available.fetchone()[0]

    
    return marked_unavailable, available_files
        

def get_file_by_path(conn: sqlite3.Connection, path: str):
    cursor = conn.cursor()

    cursor.execute('''
    SELECT files.*, items.md5, items.size
    FROM files
    JOIN items ON files.sha256 = items.sha256
    WHERE files.path = ?
    ''', (path,))
    
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
    
    cursor.execute('SELECT path FROM files')
    files = cursor.fetchall()
    
    for (path,) in files:
        available = os.path.exists(path)
        cursor.execute('''
        UPDATE files
        SET Available = ?
        WHERE path = ?
        ''', (available, path))

def add_tag_scan(
        conn: sqlite3.Connection,
        scan_time: str,
        end_time: str,
        setter: str,
        threshold: float,
        image_files: int,
        video_files: int,
        other_files: int,
        video_frames: int,
        total_frames: int,
        errors: int,
        timeouts: int,
        total_remaining: int
    ):
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tag_scans (start_time, end_time, setter, threshold, image_files, video_files, other_files, video_frames, total_frames, errors, timeouts, total_remaining)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (scan_time, end_time, setter, threshold, image_files, video_files, other_files, video_frames, total_frames, errors, timeouts, total_remaining))

@dataclass
class TagScanRecord:
    id: int
    start_time: str
    end_time: str
    setter: str
    threshold: float
    image_files: int
    video_files: int
    other_files: int
    video_frames: int
    total_frames: int
    errors: int
    timeouts: int
    total_remaining: int

def get_all_tag_scans(conn: sqlite3.Connection) -> List[TagScanRecord]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tag_scans ORDER BY start_time DESC')
    scan_records = cursor.fetchall()
    return [TagScanRecord(*scan_record) for scan_record in scan_records]

def add_item_tag_scan(conn: sqlite3.Connection, item: str, setter: str, last_scan: str, tags_set: int = 0, tags_removed: int = 0):
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO item_tag_scans (item, setter, last_scan, tags_set, tags_removed)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(item, setter) DO UPDATE SET last_scan=excluded.last_scan, tags_set=excluded.tags_set, tags_removed=excluded.tags_removed
    ''', (item, setter, last_scan, tags_set, tags_removed))

def get_items_missing_tag_scan(conn: sqlite3.Connection, setter: str):
    """
    Get all items that have not been scanned by the given tag setter.
    More efficient than get_items_missing_tags as it does not require a join with the tags table.
    It also avoids joining with the files table to get the path, instead getting paths one by one.
    """
    clauses = f"""
    FROM items
    LEFT JOIN item_tag_scans
    ON items.sha256 = item_tag_scans.item
    AND item_tag_scans.setter = ?
    WHERE item_tag_scans.item IS NULL
    """

    count_query = f'''
    SELECT COUNT(*)
    {clauses}
    '''
    cursor = conn.cursor()

    cursor.execute(count_query, (setter,))
    total_count = cursor.fetchone()[0]

    cursor.execute(f'''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added
    {clauses}
    ''', (setter,))

    remaining_count: int = total_count
    while row := cursor.fetchone():
        item = ItemWithPath(*row, "") # type: ignore
        remaining_count -= 1
        if file := get_existing_file_for_sha256(conn, item.sha256):
            item.path = file.path
            yield item, remaining_count
        else:
            # If no working path is found, skip this item
            continue

def delete_tags_from_setter(conn: sqlite3.Connection, setter: str):
    cursor = conn.cursor()
    cursor.execute('''
    DELETE FROM tags_items
    WHERE rowid IN (
        SELECT tags_items.rowid
        FROM tags_items
        JOIN tags_setters as tags
        ON tags_items.tag = tags.rowid
        AND tags.setter = ?
    )
    ''', (setter,))

    result = cursor.execute('''
    DELETE FROM tags_setters
    WHERE setter = ?
    ''', (setter,))

    tags_removed = result.rowcount

    result_items = cursor.execute('''
    DELETE FROM item_tag_scans
    WHERE setter = ?
    ''', (setter,))

    items_tags_removed = result_items.rowcount
    return tags_removed, items_tags_removed

@dataclass
class FileRecord:
    sha256: str
    path: str
    last_modified: str

def get_existing_file_for_sha256(conn: sqlite3.Connection, sha256: str) -> FileRecord | None:
    cursor = conn.cursor()

    cursor.execute('''
    SELECT path, last_modified
    FROM files
    WHERE sha256 = ?
    ORDER BY available DESC
    ''', (sha256,))

    while row := cursor.fetchone():
        path, last_modified = row
        if os.path.exists(path):
            return FileRecord(sha256, path, last_modified)
    
    return None

def get_all_tags_for_item(conn: sqlite3.Connection, sha256):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT tags.namespace, tags.name, tags_items.confidence, tags.setter
    FROM items
    JOIN tags_items ON items.rowid = tags_items.item
    AND items.sha256 = ?
    JOIN tags_setters as tags ON tags_items.tag = tags.rowid
    ''', (sha256,))
    tags = cursor.fetchall()
    return tags

def get_all_tags_for_item_name_confidence(conn: sqlite3.Connection, sha256):
    tags = get_all_tags_for_item(conn, sha256)
    return [(row[1], row[2]) for row in tags]

def get_tag_names_list(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT name FROM tags_setters')
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]

@dataclass
class FileSearchResult:
    path: str
    sha256: str
    last_modified: str
    type: str

def build_search_query(
        tags: List[str],
        negative_tags: List[str] | None = None,
        tag_namespace: str | None = None,
        min_confidence: float | None = 0.5,
        setters: List[str] = [],
        all_setters_required: bool = False,
        item_type: str | None = None,
        include_path_prefix: str | None = None,
        any_positive_tags_match: bool = False,
    ) -> Tuple[str, List[str | int | float]]:
    """
    Build a query to search for files based on the given tags, negative tags, and other conditions.
    """

    # The item mimetype should start with the given item_type
    item_type_condition = f"""
        JOIN items ON files.item = items.rowid
        AND items.type LIKE ? || '%'
    """ if item_type else ""
    # The setter should match the given setter
    tag_setters_condition = f" AND tags.setter IN ({','.join(['?']*len(setters))})" if setters else ""
    # The namespace needs to *start* with the given namespace
    tag_namespace_condition = " AND tags.namespace LIKE ? || '%'" if tag_namespace else ""

    # The confidence should be greater than or equal to the given confidence
    min_confidence_condition = f"AND tags_items.confidence >= ?" if min_confidence else ""

    # Negative tags should not be associated with the item
    negative_tags_condition = f"""
        WHERE files.item NOT IN (
            SELECT tags_items.item
            FROM tags_setters as tags
            JOIN tags_items ON tags.rowid = tags_items.tag
            AND tags.name IN ({','.join(['?']*len(negative_tags))})
            {tag_setters_condition}
            {tag_namespace_condition}
            {min_confidence_condition}
        )
    """ if negative_tags else ""
    # The path needs to *start* with the given path prefix
    path_condition = f" AND files.path LIKE ? || '%'" if include_path_prefix else ""
    # If tags are not provided, and no negative tags are provided, this needs to start a WHERE clause
    if include_path_prefix and not tags and not negative_tags:
        path_condition = f" WHERE files.path LIKE ? || '%'"

    having_clause = "HAVING COUNT(DISTINCT tags.name) = ?" if not all_setters_required else "HAVING COUNT(DISTINCT tags.setter || '-' || tags.name) = ?"

    main_query = f"""
        SELECT files.path, files.sha256, files.last_modified
        FROM tags_setters as tags
        JOIN tags_items ON tags.rowid = tags_items.tag
        AND tags.name IN ({','.join(['?']*len(tags))})
        {min_confidence_condition}
        {tag_setters_condition}
        {tag_namespace_condition}
        JOIN files ON tags_items.item = files.item
        {path_condition}
        {item_type_condition}
        {negative_tags_condition}
        GROUP BY files.path
        {having_clause if not any_positive_tags_match else ""}
    """ if tags else f"""
        SELECT files.path, files.sha256, files.last_modified
        FROM files
        {item_type_condition}
        {negative_tags_condition}
        {path_condition}
    """
    params: List[str | int | float] = [
        param for param in [
            *((*tags,
            min_confidence,
            *setters,
            tag_namespace,) if tags else ()),
            (include_path_prefix if tags else None),
            item_type,
            *((*negative_tags,
            *setters,
            tag_namespace,
            min_confidence
            ) if negative_tags else ()),
            (include_path_prefix if not tags else None),
            (   
                # Number of tags to match, or number of tag-setter pairs to match if we require all setters to be present for all tags
                (len(tags) if not all_setters_required else len(tags) * len(setters))
                # HAVING clause is not needed if no positive tags are provided
                if tags and not any_positive_tags_match else None
            )
    ] if param is not None]

    return main_query, params

def print_search_query(query_str: str, params: List[str | float | int]):
    # Quote strings in params
    quoted_params = [f"'{param}'" if isinstance(param, str) else param for param in params]
    formatted_query = query_str.replace('?', '{}').format(*quoted_params)
    # Remove empty lines
    formatted_query = '\n'.join([line for line in formatted_query.split('\n') if line.strip() != ''])
    print(formatted_query)

def search_files(
        conn: sqlite3.Connection,
        tags: List[str],
        tags_match_any: List[str] | None = None,
        negative_tags: List[str] | None = None,
        negative_tags_match_all: List[str] | None = None,
        tag_namespace: str | None = None,
        min_confidence: float | None = 0.5,
        setters: List[str] | None = None,
        all_setters_required: bool | None = False,
        item_type: str | None = None,
        include_path_prefix: str | None = None,
        order_by: str | None = "last_modified",
        order: str | None = None,
        page_size: int | None = 1000,
        page: int = 1,
        check_path_exists: bool = False,
        return_total_count: bool = True
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
    if len(negative_tags_match_all) == 1:
        # If only one tag is provided for negative "match all", we can just use it as a regular negative tag
        negative_tags.append(negative_tags_match_all[0])

    tag_namespace = tag_namespace or None
    item_type = item_type or None
    include_path_prefix = include_path_prefix or None
    min_confidence = min_confidence or None
    setters = setters or []

    page_size = page_size or 1000000 # Mostly for debugging purposes
    offset = (page - 1) * page_size

    if tags_match_any and not tags:
        # If "match any" tags are provided, but no positive tags are provided
        # We need to build a query to match on *any* of them being present
        main_query, params = build_search_query(
            tags=tags_match_any,
            negative_tags=negative_tags,
            tag_namespace=tag_namespace,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_type=item_type,
            include_path_prefix=include_path_prefix,
            any_positive_tags_match=True
        )
    else:
        # Basic case where we need to match all positive tags and none of the negative tags
        main_query, params = build_search_query(
            tags=tags,
            negative_tags=negative_tags,
            tag_namespace=tag_namespace,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_type=item_type,
            include_path_prefix=include_path_prefix,
            any_positive_tags_match=False
        )

    if tags_match_any and tags:
        # If tags "match any" are provided along with match all regular positive tags
        # We need to build a separate query to match on *any* of them being present
        # And then intersect the results with the main query
        tags_query, tags_params = build_search_query(
            tags=tags_match_any,
            negative_tags=None,
            tag_namespace=tag_namespace,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=False,
            item_type=item_type,
            include_path_prefix=include_path_prefix,
            any_positive_tags_match=True
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
            tag_namespace=tag_namespace,
            min_confidence=min_confidence,
            setters=setters,
            all_setters_required=all_setters_required,
            item_type=item_type,
            include_path_prefix=include_path_prefix,
            any_positive_tags_match=False
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

    if order_by == "path":
        order_by_clause = "path"
        # Default order differs for path and last_modified
        if order == None:
            order = "asc"
    else:
        order_by_clause = "last_modified"
        if order == None:
            order = "desc"
    
    order_clause = "DESC" if order == "desc" else "ASC"

    # Second query to get the items with pagination
    query = f"""
    {main_query}
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?
    """
    query_params: List[str | int | float] = [
        *params,
        page_size,
        offset
    ]

    cursor.execute(query, query_params)
    results_count = cursor.rowcount
    while row := cursor.fetchone():
        file = FileSearchResult(*row, get_mime_type(row[0])) # type: ignore
        if check_path_exists and not os.path.exists(file.path):
            continue
        yield file, total_count
    if results_count == 0:
        return []

def add_folder_to_database(conn: sqlite3.Connection, time: str, folder_path: str, included=True):
    cursor = conn.cursor()
    folder_path = normalize_path(folder_path)
    # Attempt to insert the folder
    cursor.execute('''
        INSERT OR IGNORE INTO folders (time_added, path, included)
        VALUES (?, ?, ?)
    ''', (time, folder_path, included))
    
    if cursor.rowcount == 0: 
        return False
    else:
        return True
    
def delete_folders_not_in_list(conn: sqlite3.Connection, folder_paths: List[str], included=True):
    cursor = conn.cursor()
    result = cursor.execute('''
    DELETE FROM folders
    WHERE included = ?
    AND path NOT IN ({})
    '''.format(','.join(['?']*len(folder_paths))), [included] + folder_paths)
    return result.rowcount

def remove_folder_from_database(conn: sqlite3.Connection, folder_path: str):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM folders WHERE path = ?', (folder_path,))

def get_folders_from_database(conn: sqlite3.Connection, included=True) -> List[str]:
    cursor = conn.cursor()
    cursor.execute('SELECT path FROM folders WHERE included = ?', (included,))
    folders = cursor.fetchall()
    return [folder[0] for folder in folders]

def delete_files_under_excluded_folders(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute('''
    DELETE FROM files
    WHERE EXISTS (
        SELECT 1
        FROM folders
        WHERE folders.included = 0
        AND files.path LIKE folders.path || '%'
    );
    ''')
    return result.rowcount

def delete_files_not_under_included_folders(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute('''
    DELETE FROM files
    WHERE NOT EXISTS (
        SELECT 1
        FROM folders
        WHERE folders.included = 1
        AND files.path LIKE folders.path || '%'
    );
    ''')
    return result.rowcount

def delete_unavailable_files(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute('''
    DELETE FROM files
    WHERE available = 0
    ''')
    return result.rowcount

def delete_items_without_files(conn: sqlite3.Connection, batch_size: int = 10000):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute('''
        DELETE FROM items
        WHERE rowid IN (
            SELECT items.rowid
            FROM items
            LEFT JOIN files ON files.sha256 = items.sha256
            WHERE files.sha256 IS NULL
            LIMIT ?
        )
        ''', (batch_size,))
        
        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows
        
        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break
    
    return total_deleted

def delete_tags_without_items(conn: sqlite3.Connection, batch_size: int = 10000):
    cursor = conn.cursor()
    total_deleted = 0
    while True:
        # Perform the deletion in batches
        cursor.execute('''
        DELETE FROM tags_items
        WHERE rowid IN (
            SELECT tags_items.rowid
            FROM tags_items
            LEFT JOIN items ON items.rowid = tags_items.item
            WHERE items.rowid IS NULL
            LIMIT ?
        )
        ''', (batch_size,))
        
        # Check the number of rows affected in this batch
        deleted_rows = cursor.rowcount
        total_deleted += deleted_rows
        
        # If no rows were deleted, we are done
        if deleted_rows == 0:
            break
    
    return total_deleted

def delete_item_tag_scans_without_items(conn: sqlite3.Connection, batch_size: int = 10000):
    cursor = conn.cursor()
    total_deleted = 0

    while True:
        # Perform the deletion in batches
        cursor.execute('''
        DELETE FROM item_tag_scans
        WHERE rowid IN (
            SELECT item_tag_scans.rowid
            FROM item_tag_scans
            LEFT JOIN items ON items.sha256 = item_tag_scans.item
            WHERE items.sha256 IS NULL
            LIMIT ?
        )
        ''', (batch_size,))
        
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
    conn.execute('VACUUM')
    conn.execute('ANALYZE')

def get_most_common_tags(conn: sqlite3.Connection, namespace: str | None = None, setters: List[str] | None = [], confidence_threshold: float | None = None, limit=10):
    cursor = conn.cursor()
    namespace_clause = "AND tags.namespace LIKE ? || '%'" if namespace else ""
    setters_clause = f"AND tags.setter IN ({','.join(['?']*len(setters))})" if setters else ""
    confidence_clause = f"AND tags_items.confidence >= ?" if confidence_threshold else ""
    setters = setters or []
    query_args = [arg for arg in [
        namespace,
        *setters,
        confidence_threshold,
        limit
    ] if arg is not None]

    query = f'''
    SELECT namespace, name, COUNT(*) as count
    FROM tags_setters as tags
    JOIN tags_items ON tags.rowid = tags_items.tag
    {namespace_clause}
    {setters_clause}
    {confidence_clause}
    GROUP BY namespace, name
    ORDER BY count DESC
    LIMIT ?
    '''
    cursor.execute(query, query_args)

    tags = cursor.fetchall()
    return tags

def get_most_common_tags_frequency(conn: sqlite3.Connection, namespace=None, setters: List[str] | None = [], confidence_threshold=None, limit=10):
    tags = get_most_common_tags(conn, namespace=namespace, setters=setters, confidence_threshold=confidence_threshold, limit=limit)
    # Get the total number of item_setter pairs
    cursor = conn.cursor()
    setters_clause = f"WHERE setter IN ({','.join(['?']*len(setters))})" if setters else ""
    cursor.execute(f"""
                   SELECT COUNT(DISTINCT item || '-' || setter) AS distinct_count
                   FROM item_tag_scans
                   {setters_clause}
                """, setters if setters else ())
    total_items_setters = cursor.fetchone()[0]
    # Calculate the frequency
    tags = [(tag[0], tag[1], tag[2], tag[2]/(total_items_setters)) for tag in tags]
    return tags

def update_bookmarks(conn: sqlite3.Connection, items_sha256: List[str], namespace: str='default'):
    cursor = conn.cursor()
    # Add all items as bookmarks, if they don't already exist, in a single query
    cursor.executemany('''
    INSERT INTO bookmarks (namespace, sha256, time_added)
    VALUES (?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    ''', [(namespace, sha256, datetime.now().isoformat()) for sha256 in items_sha256])

    # Remove all items that are not in the list
    cursor.execute('''
    DELETE FROM bookmarks
    WHERE sha256 NOT IN ({}) AND namespace = ?
    '''.format(','.join(['?']*len(items_sha256)), items_sha256, namespace))

def add_bookmark(conn: sqlite3.Connection, sha256: str, namespace: str='default', metadata: str | None = None):
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO bookmarks (namespace, sha256, time_added, metadata)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(namespace, sha256) DO NOTHING
    ''', (namespace, sha256, datetime.now().isoformat(), metadata))

def remove_bookmark(conn: sqlite3.Connection, sha256: str, namespace: str='default'):
    cursor = conn.cursor()
    cursor.execute('''
    DELETE FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    ''', (sha256, namespace))

def get_bookmark_metadata(conn: sqlite3.Connection, sha256: str, namespace: str='default'):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT metadata
    FROM bookmarks
    WHERE sha256 = ? AND namespace = ?
    ''', (sha256, namespace))
    metadata = cursor.fetchone()
    
    return (True, metadata[0]) if metadata else (False, None)

def delete_bookmarks_exclude_last_n(conn: sqlite3.Connection, n: int, namespace: str = 'default'):
    cursor = conn.cursor()
    # Delete all bookmarks except the last n based on time_added
    cursor.execute('''
        DELETE FROM bookmarks
        WHERE namespace = ?
        AND sha256 NOT IN (
            SELECT sha256
            FROM bookmarks
            WHERE namespace = ?
            ORDER BY time_added DESC
            LIMIT ?
        )
    ''', (namespace, namespace, n))
    
    conn.commit()

def get_all_bookmark_namespaces(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.cursor()
    # Get all bookmark namespaces, order by namespace name
    cursor.execute('''
        SELECT DISTINCT namespace
        FROM bookmarks
        ORDER BY namespace
    ''')
    namespaces = cursor.fetchall()
    return [namespace[0] for namespace in namespaces]

def get_bookmarks(
        conn: sqlite3.Connection,
        namespace: str = 'default',
        page_size=1000,
        page=1,
        order_by="time_added",
        order=None
    ) -> Tuple[List[FileSearchResult], int]:

    if page_size < 1:
        page_size = 1000000
    offset = (page - 1) * page_size

    # Fetch bookmarks with their paths, prioritizing available files
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(DISTINCT bookmarks.sha256)
        FROM bookmarks
        JOIN files
        ON bookmarks.sha256 = files.sha256
        WHERE bookmarks.namespace = ?
    ''', (namespace,))
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
    cursor.execute(f'''
        SELECT 
        COALESCE(available_files.path, any_files.path) as path,
        bookmarks.sha256,
        COALESCE(MAX(available_files.last_modified), MAX(any_files.last_modified)) as last_modified
        FROM bookmarks
        LEFT JOIN files AS available_files 
               ON bookmarks.sha256 = available_files.sha256 
               AND available_files.available = 1
        JOIN files AS any_files 
               ON bookmarks.sha256 = any_files.sha256
        WHERE bookmarks.namespace = ?
        GROUP BY bookmarks.sha256
        ORDER BY {order_by_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    ''', (namespace, page_size, offset))
    
    bookmarks: List[FileSearchResult] = []
    for row in cursor.fetchall():
        item = FileSearchResult(*row, get_mime_type(row[0])) # type: ignore
        if not os.path.exists(item.path):
            if file := get_existing_file_for_sha256(conn, item.sha256):
                item.path = file.path
                bookmarks.append(item)
            # If the path does not exist and no working path is found, skip this item
            continue
        bookmarks.append(item)
    
    return bookmarks, total_results