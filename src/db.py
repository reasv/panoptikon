import os
import sqlite3
from datetime import datetime
import time
from typing import Dict, List, Tuple

def get_database_connection() -> sqlite3.Connection:
    db_file = os.getenv('DB_FILE', 'sqlite.db')
    conn = sqlite3.connect(db_file)
    return conn

def initialize_database():
    conn = get_database_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS items (
        sha256 TEXT PRIMARY KEY,
        md5 TEXT NOT NULL,
        type TEXT,
        size INTEGER,          -- Size of the file in bytes
        time_added TEXT NOT NULL,         -- Using TEXT to store ISO-8601 formatted datetime
        time_last_seen TEXT NOT NULL      -- Using TEXT to store ISO-8601 formatted datetime
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
        time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT NOT NULL,
        UNIQUE(time, path)       -- Unique constraint on time and path
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
        end_time TEXT,               -- Using TEXT to store ISO-8601 formatted datetime
        setter TEXT NOT NULL,
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
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_md5 ON items(md5)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_type ON items(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_time_added ON items(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_time_last_seen ON items(time_last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_modified ON files(last_modified)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_available ON files(available)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)')  # Explicit index on path
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_time ON file_scans(time)')
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

    conn.commit()
    conn.close()

def insert_tag(conn: sqlite3.Connection, scan_time, namespace, name, item, setter, confidence = 1.0, value = None):
    time = scan_time
    last_set = scan_time
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tags (namespace, name, value, confidence, item, setter, time, last_set)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(namespace, name, item, setter) DO UPDATE SET value=excluded.value, confidence=excluded.confidence, last_set=excluded.last_set
    ''', (namespace, name, value, confidence, item, setter, time, last_set))


def insert_or_update_file_data(conn: sqlite3.Connection, image_data, scan_time):
    cursor = conn.cursor()
    sha256 = image_data['sha256']
    md5 = image_data['MD5']
    mime_type = image_data['mime_type']
    paths = image_data['paths']
    file_size = image_data['size']

    cursor.execute('''
    INSERT INTO items (sha256, md5, type, size, time_added, time_last_seen)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO UPDATE SET time_last_seen=excluded.time_last_seen
    ''', (sha256, md5, mime_type, file_size, scan_time, scan_time))
    
    for path_data in paths:
        path = path_data['path']
        last_modified = path_data['last_modified']
        
        # Check if the path already exists
        cursor.execute('SELECT sha256 FROM files WHERE path = ?', (path,))
        existing_path = cursor.fetchone()
        
        if existing_path:
            if existing_path[0] == sha256:
                # Path exists with the same sha256, update last_modified, last_seen, and available
                cursor.execute('''
                UPDATE files
                SET last_modified = ?, last_seen = ?, available = TRUE
                WHERE path = ?
                ''', (last_modified, scan_time, path))
            else:
                # Path exists with a different sha256, delete the old entry and insert new
                cursor.execute('DELETE FROM files WHERE path = ?', (path,))
                cursor.execute('''
                INSERT INTO files (sha256, path, last_modified, last_seen, available)
                VALUES (?, ?, ?, ?, TRUE)
                ''', (sha256, path, last_modified, scan_time))
        else:
            # Path does not exist, insert new
            cursor.execute('''
            INSERT INTO files (sha256, path, last_modified, last_seen, available)
            VALUES (?, ?, ?, ?, TRUE)
            ''', (sha256, path, last_modified, scan_time))

def save_items_to_database(conn: sqlite3.Connection, files_data: Dict[str, Dict[str, str]], paths: List[str]):
    while True:
        try:
            scan_time = datetime.now().isoformat()

            # Start a transaction
            cursor = conn.cursor()
            cursor.execute('BEGIN')

            # Insert a scan entry for each parent folder path
            for path in paths:
                cursor.execute('''
                INSERT INTO file_scans (time, path)
                VALUES (?, ?)
                ''', (scan_time, path))
            break

        except sqlite3.IntegrityError:
            # Rollback the transaction on failure and wait before retrying
            conn.rollback()
            time.sleep(1)

    for _, image_data in files_data.items():
        insert_or_update_file_data(conn, image_data, scan_time)
    
    mark_unavailable_files(conn, scan_time, paths)

def mark_unavailable_files(conn: sqlite3.Connection, scan_time: str, paths: List[str]):
    cursor = conn.cursor()
    
    # Mark files as unavailable if they haven't been seen in the current scan
    # and their path starts with one of the paths provided
    for path in paths:
        cursor.execute('''
        UPDATE files
        SET available = FALSE
        WHERE last_seen != ?
        AND path LIKE ?
        ''', (scan_time, path + '%'))

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

def hard_update_items_available():
    # This function is used to update the availability of files in the database
    conn = get_database_connection()
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
    
    conn.commit()
    conn.close()


def find_working_paths(conn: sqlite3.Connection, excluded_tag_setter=None):
    cursor = conn.cursor()
    if excluded_tag_setter:
        # Get all unique sha256 hashes excluding those with a tag set by the given author
        cursor.execute('''
        SELECT DISTINCT files.sha256
        FROM files
        LEFT JOIN tags ON files.sha256 = tags.item AND tags.setter = ?
        WHERE tags.item IS NULL
        ''', (excluded_tag_setter,))
        sha256_hashes = cursor.fetchall()
    else:
        # Get all unique sha256 hashes
        cursor.execute('SELECT DISTINCT sha256 FROM files')
        sha256_hashes = cursor.fetchall()

    working_paths = {}

    for sha256_tuple in sha256_hashes:
        sha256 = sha256_tuple[0]
        
        # First, try to find a path with available = 1
        cursor.execute('SELECT path FROM files WHERE sha256 = ? AND available = 1', (sha256,))
        paths = cursor.fetchall()
        
        found = False
        for path_tuple in paths:
            path = path_tuple[0]
            if os.path.exists(path):
                working_paths[sha256] = path
                found = True
                break
        
        # If no available paths are found, try other paths
        if not found:
            cursor.execute('SELECT path FROM files WHERE sha256 = ?', (sha256,))
            paths = cursor.fetchall()
            
            for path_tuple in paths:
                path = path_tuple[0]
                if os.path.exists(path):
                    working_paths[sha256] = path
                    break
    return working_paths

def get_working_path_by_sha256(conn: sqlite3.Connection, sha256: str) -> Tuple[str, str] | None:
    cursor = conn.cursor()
    # First, try to find a path with available = 1
    cursor.execute('SELECT path, last_modified FROM files WHERE sha256 = ? AND available = 1', (sha256,))
    paths = cursor.fetchall()
    
    found = False
    for path_tuple in paths:
        path = path_tuple[0]
        if os.path.exists(path):
            return path, path_tuple[1]
    
    # If no available paths are found, try other paths
    if not found:
        cursor.execute('SELECT path, last_modified FROM files WHERE sha256 = ?', (sha256,))
        paths = cursor.fetchall()
        
        for path_tuple in paths:
            path = path_tuple[0]
            if os.path.exists(path):
                return path, path_tuple[1]
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

def get_all_tags_for_item_name_confidence(sha256):
    conn = get_database_connection()
    cursor = conn.cursor()
    cursor.execute('''
    SELECT name, confidence
    FROM tags
    WHERE item = ?
    ''', (sha256,))
    tags = cursor.fetchall()
    conn.close()
    return tags

def get_tag_names_list(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT name FROM tags')
    tag_names = cursor.fetchall()
    return [tag[0] for tag in tag_names]

def get_items_by_tag_name(conn: sqlite3.Connection, tag_name):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added, items.time_last_seen
    FROM items
    JOIN tags ON items.sha256 = tags.item
    WHERE tags.name = ?
    ''', (tag_name,))
    items = cursor.fetchall()
    return items

def find_items_by_tags(conn: sqlite3.Connection, tags, min_confidence=0.5, page_size=1000, page=1):
    cursor = conn.cursor()
    if page_size < 1:
        page_size = 1000000

    offset = (page - 1) * page_size
    query = '''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added, items.time_last_seen,
    MAX(files.last_modified) as last_modified, MIN(files.path) as path
    FROM items
    JOIN tags ON items.sha256 = tags.item
    JOIN files ON items.sha256 = files.sha256
    WHERE tags.name IN ({}) AND tags.confidence >= ?
    GROUP BY items.sha256
    HAVING COUNT(DISTINCT tags.name) = ?
    ORDER BY last_modified DESC
    LIMIT ? OFFSET ?
    '''.format(','.join(['?']*len(tags)))
    cursor.execute(query, tags + [min_confidence, len(tags), page_size, offset])
    items = cursor.fetchall()
    return items

def find_paths_by_tags(tags, min_confidence=0.5, page_size=1000, page=1):
    conn = get_database_connection()
    results = []
    for item in find_items_by_tags(conn, tags, min_confidence=min_confidence, page_size=page_size, page=page):
        if os.path.exists(item[7]):
            results.append({
                'sha256': item[0],
                'type': item[2],
                'last_modified': item[6],
                'path': item[7]
            })
        elif result := get_working_path_by_sha256(conn, item[0]):
            results.append({
                'sha256': item[0],
                'type': item[2],
                'path': result[0],
                'last_modified': result[1]
            })
    conn.close()
    return results

def add_folder_to_database(conn: sqlite3.Connection, time: str, folder_path: str, included=True):
    cursor = conn.cursor()
    # Check if the folder already exists and has the same included status
    cursor.execute('SELECT included FROM folders WHERE path = ?', (folder_path,))
    existing_folder = cursor.fetchone()
    if existing_folder and existing_folder[0] == included:
        return

    cursor.execute('''
    INSERT INTO folders (time_added, path, included)
    VALUES (?, ?, ?)
    ''', (time, folder_path, included))

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
    cursor.execute('''
    DELETE FROM files
    WHERE path IN (
        SELECT files.path
        FROM files
        JOIN folders ON files.path LIKE folders.path || '%'
        WHERE folders.included = 0
    )
    ''')

def delete_items_without_files(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('''
    DELETE FROM items
    WHERE sha256 NOT IN (
        SELECT sha256
        FROM files
    )
    ''')

def delete_files_not_under_included_folders(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('''
    DELETE FROM files
    WHERE path NOT IN (
        SELECT files.path
        FROM files
        JOIN folders ON files.path LIKE folders.path || '%'
        WHERE folders.included = 1
    )
    ''')

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