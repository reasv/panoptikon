from dataclasses import dataclass
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple

from src.utils import normalize_path, get_mime_type
from src.types import FileScanData

def get_database_connection(force_readonly=False) -> sqlite3.Connection:
    # Check if we are in read-only mode
    db_file = os.getenv('DB_FILE', 'sqlite.db')
    if force_readonly or os.environ.get('READONLY', 'false').lower() == 'true':
        # Use a read-only connection
        conn = sqlite3.connect(f'file:{db_file}?mode=ro', uri=True)
    else:
        conn = sqlite3.connect(db_file)
    return conn

def initialize_database(conn: sqlite3.Connection):
    cursor = conn.cursor()
    
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sha256 TEXT NOT NULL,
        path TEXT UNIQUE NOT NULL,        -- Ensuring path is unique
        last_modified TEXT NOT NULL,      -- Using TEXT to store ISO-8601 formatted datetime
        last_seen TEXT NOT NULL,          -- Using TEXT to store ISO-8601 formatted datetime
        available BOOLEAN NOT NULL,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(sha256) REFERENCES items(sha256)
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
    CREATE TABLE IF NOT EXISTS tags (
        namespace TEXT NOT NULL,
        name TEXT NOT NULL,
        value TEXT,
        confidence REAL DEFAULT 1.0,
        item TEXT NOT NULL,
        setter TEXT NOT NULL,
        time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        last_set TEXT NOT NULL,           -- Using TEXT to store ISO-8601 formatted datetime
        PRIMARY KEY(namespace, name, item, setter),
        FOREIGN KEY(item) REFERENCES items(sha256) ON DELETE CASCADE
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_start_time ON file_scans(start_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_end_time ON file_scans(end_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_path ON file_scans(path)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_namespace ON tags(namespace)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_value ON tags(value)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_confidence ON tags(confidence)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_item ON tags(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_setter ON tags(setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_time ON tags(time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_last_set ON tags(last_set)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_start_time ON tag_scans(start_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_end_time ON tag_scans(end_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_scans_setter ON tag_scans(setter)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_time_added ON folders(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_path ON folders(path)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_folders_included ON folders(included)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_time_added ON bookmarks(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_sha256 ON bookmarks(sha256)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bookmarks_metadata ON bookmarks(metadata)')

def insert_tag(conn: sqlite3.Connection, scan_time, namespace, name, item, setter, confidence = 1.0, value = None):
    time = scan_time
    last_set = scan_time
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tags (namespace, name, value, confidence, item, setter, time, last_set)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(namespace, name, item, setter) DO UPDATE SET value=excluded.value, confidence=excluded.confidence, last_set=excluded.last_set
    ''', (namespace, name, value, confidence, item, setter, time, last_set))

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

        # Path does not exist or has been modified, insert new
        file_insert_result = cursor.execute('''
        INSERT INTO files (sha256, path, last_modified, last_seen, available)
        VALUES (?, ?, ?, ?, TRUE)
        ''', (sha256, path, last_modified, scan_time))
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

@dataclass
class ItemWithPath:
    sha256: str
    md5: str
    type: str
    size: int
    time_added: str
    path: str

def get_items_missing_tags(conn: sqlite3.Connection, tag_setter=None):
    """
    Get all items that do not have any tags set by the given tag setter
    If tag_setter is None, get all items that do not have any tags set by any tag setter
    """
    cursor = conn.cursor()
    tag_setter_clause = "AND tags.setter = ?" if tag_setter else ""
    cursor.execute(f'''
    SELECT 
        items.sha256,
        items.md5,
        items.type,
        items.size,
        items.time_added,
        (
            SELECT files.path
            FROM files 
            WHERE files.sha256 = items.sha256 
            ORDER BY files.available DESC
            LIMIT 1
        ) as path
    FROM items
    WHERE NOT EXISTS (
        SELECT 1
        FROM tags
        WHERE tags.item = items.sha256 {tag_setter_clause}
    )
    ''', (tag_setter,) if tag_setter else ())

    # Yield each item
    while row := cursor.fetchone():
        item = ItemWithPath(*row)
        if os.path.exists(item.path):
            yield item
        else:
            # If the path does not exist, try to find a working path
            if file := get_existing_file_for_sha256(conn, item.sha256):
                item.path = file.path
                yield item
            else:
                # If no working path is found, skip this item
                continue

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
    SELECT namespace, name, value, confidence, setter, time, last_set
    FROM tags
    WHERE item = ?
    ''', (sha256,))
    tags = cursor.fetchall()
    return tags

def get_all_tags_for_item_name_confidence(conn: sqlite3.Connection, sha256):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT name, confidence
    FROM tags
    WHERE item = ?
    ''', (sha256,))
    tags = cursor.fetchall()
    return tags

def get_tag_names_list(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT name FROM tags')
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]

def get_items_by_tag_name(conn: sqlite3.Connection, tag_name):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added
    FROM items
    JOIN tags ON items.sha256 = tags.item
    WHERE tags.name = ?
    ''', (tag_name,))
    items = cursor.fetchall()
    return items

@dataclass
class FileSearchResult:
    path: str
    sha256: str
    last_modified: str
    type: str

def search_files(
        conn: sqlite3.Connection,
        tags: List[str],
        negative_tags: List[str] | None = None,
        tag_namespace: str | None = None,
        min_confidence: float | None = 0.5,
        setters: List[str] | None = None,
        all_setters_required: bool = False,
        item_type: str | None = None,
        include_path_prefix: str | None = None,
        order_by: str | None = "last_modified",
        order: str | None = None,
        page_size: int | None = 1000,
        page: int = 1,
        check_path_exists: bool = False
    ):
    negative_tags = negative_tags or []
    tags = tags or []
    tags = [tag.lower().strip() for tag in tags if tag.strip() != ""]
    negative_tags = [tag.lower().strip() for tag in negative_tags if tag.strip() != ""]
    tag_namespace = tag_namespace or None
    item_type = item_type or None
    include_path_prefix = include_path_prefix or None
    setters = setters or []
    page_size = page_size or 1000000 # Mostly for debugging purposes
    offset = (page - 1) * page_size

    # The item mimetype should start with the given item_type
    item_type_condition = f"""
        JOIN items ON files.sha256 = items.sha256
        AND items.type LIKE ? || '%'
    """ if item_type else ""
    # The setter should match the given setter
    tag_setters_condition = f" AND tags.setter IN ({','.join(['?']*len(setters))})" if setters else ""
    # The namespace needs to *start* with the given namespace
    tag_namespace_condition = " AND tags.namespace LIKE ? || '%'" if tag_namespace else ""
    # Negative tags should not be associated with the item
    negative_tags_condition = f"""
        WHERE files.sha256 NOT IN (
            SELECT item
            FROM tags
            WHERE name IN ({','.join(['?']*len(negative_tags))})
            AND confidence >= ?
            {tag_setters_condition}
            {tag_namespace_condition}
        )
    """ if negative_tags else ""
    # The path needs to *start* with the given path prefix
    path_condition = f" {' WHERE' if not negative_tags else ' AND'} files.path LIKE ? || '%'" if include_path_prefix else ""

    having_clause = "HAVING COUNT(DISTINCT tags.name) = ?" if not all_setters_required else "HAVING COUNT(DISTINCT tags.setter || '-' || tags.name) = ?"
    # First query to get the total count of items matching the criteria
    count_query = f"""
    SELECT COUNT(*)
    FROM (
        SELECT files.path
        FROM files
        JOIN tags ON tags.item = files.sha256
        AND tags.name IN ({','.join(['?']*len(tags))})
        AND tags.confidence >= ?
        {tag_setters_condition}
        {tag_namespace_condition}

        {item_type_condition}

        {negative_tags_condition}
        {path_condition}
        GROUP BY files.path
        {having_clause}
    )
    """ if tags else f"""

    SELECT COUNT(*)
    FROM (
        SELECT files.path
        FROM files
        {item_type_condition}
        {negative_tags_condition}
        {path_condition}
    )
    """
    count_params: List[str] = [
        *((*tags,
        min_confidence,
        *setters,
        tag_namespace,) if tags else ()),
        item_type,
        *((*negative_tags,
           min_confidence,
           *setters,
           tag_namespace,) if negative_tags else ()),
        include_path_prefix,
        (   
            # Number of tags to match, or number of tag-setter pairs to match if we require all setters to be present for all tags
            (len(tags) if not all_setters_required else len(tags) * len(setters))
            # HAVING clause is not needed if no positive tags are provided
            if tags else None
        )
    ]
    # Remove None values from the count_params
    count_params = [param for param in count_params if param is not None]
    
    cursor = conn.cursor()
    try:
        cursor.execute(count_query, count_params)
    except Exception as e:
        print(item_type, include_path_prefix)
        print(count_query)
        print(count_params)
        raise e
    total_count: int = cursor.fetchone()[0]

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
    SELECT files.path, files.sha256, files.last_modified
    FROM files
    JOIN tags ON tags.item = files.sha256
    AND tags.name IN ({','.join(['?']*len(tags))})
    AND tags.confidence >= ?
    {tag_setters_condition}
    {tag_namespace_condition}

    {item_type_condition}

    {negative_tags_condition}
    {path_condition}
    GROUP BY files.path
    {having_clause}
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?

    """ if tags else f"""
    SELECT files.path, files.sha256, files.last_modified
    FROM files
    {item_type_condition}
    {negative_tags_condition}
    {path_condition}
    ORDER BY {order_by_clause} {order_clause}
    LIMIT ? OFFSET ?
    """
    query_params: List[str] = [
        *count_params,
        page_size,
        offset
    ]

    cursor.execute(query, query_params)
    while row := cursor.fetchone():
        file = FileSearchResult(*row, get_mime_type(row[0]))
        if check_path_exists and not os.path.exists(file.path):
            continue
        yield file, total_count

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

def delete_items_without_files(conn: sqlite3.Connection):
    cursor = conn.cursor()
    result = cursor.execute('''
    DELETE FROM items
    WHERE NOT EXISTS (
        SELECT 1
        FROM files
        WHERE files.sha256 = items.sha256
    )
    ''')
    return result.rowcount

def get_most_common_tags(conn: sqlite3.Connection, limit=10):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT namespace, name, COUNT(*) as count
    FROM tags
    GROUP BY namespace, name
    ORDER BY count DESC
    LIMIT ?
    ''', (limit,))
    tags = cursor.fetchall()
    return tags

def get_most_common_tags_frequency(conn: sqlite3.Connection, limit=10):
    tags = get_most_common_tags(conn, limit)
    # Get the total count of items that have tags
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT item) FROM tags')
    total_count = cursor.fetchone()[0]
    # Calculate the frequency
    tags = [(tag[0], tag[1], tag[2], tag[2]/total_count) for tag in tags]
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

def add_bookmark(conn: sqlite3.Connection, sha256: str, namespace: str='default', metadata: str=None):
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
        item = FileSearchResult(*row, get_mime_type(row[0]))
        if not os.path.exists(item.path):
            if file := get_existing_file_for_sha256(conn, item.sha256):
                item.path = file.path
                bookmarks.append(item)
            # If the path does not exist and no working path is found, skip this item
            continue
        bookmarks.append(item)
    
    return bookmarks, total_results