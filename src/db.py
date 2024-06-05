import os
import sqlite3
from datetime import datetime
import time
from typing import Tuple

def get_database_connection():
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
        FOREIGN KEY(item) REFERENCES items(sha256)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tag_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT NOT NULL,               -- Using TEXT to store ISO-8601 formatted datetime
        end_time TEXT,               -- Using TEXT to store ISO-8601 formatted datetime
        setter TEXT NOT NULL,
        UNIQUE(start_time)       -- Unique constraint on time and path
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

    conn.commit()
    conn.close()

def insert_tag(conn, scan_time, namespace, name, item, setter, confidence = 1.0, value = None):
    time = scan_time
    last_set = scan_time
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tags (namespace, name, value, confidence, item, setter, time, last_set)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(namespace, name, item, setter) DO UPDATE SET value=excluded.value, confidence=excluded.confidence, last_set=excluded.last_set
    ''', (namespace, name, value, confidence, item, setter, time, last_set))


def insert_or_update_file_data(conn, image_data, scan_time):
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

def save_items_to_database(images_data, paths):
    initialize_database()
    conn = get_database_connection()

    successful_insert = False
    while not successful_insert:
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
            successful_insert = True

        except sqlite3.IntegrityError:
            # Rollback the transaction on failure and wait before retrying
            conn.rollback()
            time.sleep(1)

    for sha256, image_data in images_data.items():
        insert_or_update_file_data(conn, image_data, scan_time)
    
    mark_unavailable_files(conn, scan_time, paths)

    # Only commit if the entire transaction is successful
    conn.commit()
    conn.close()

def mark_unavailable_files(conn, scan_time, paths):
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

def get_file_by_path(conn, path):
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


def find_working_paths(conn, excluded_tag_setter=None):
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

def get_working_path_by_sha256(conn, sha256: str) -> Tuple[str, str] | None:
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

def get_all_tags_for_item(conn, sha256):
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

def get_items_by_tag_name(conn, tag_name):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added, items.time_last_seen
    FROM items
    JOIN tags ON items.sha256 = tags.item
    WHERE tags.name = ?
    ''', (tag_name,))
    items = cursor.fetchall()
    return items

def find_items_by_tags(conn, tags):
    cursor = conn.cursor()
    query = '''
    SELECT items.sha256, items.md5, items.type, items.size, items.time_added, items.time_last_seen
    FROM items
    JOIN tags ON items.sha256 = tags.item
    WHERE tags.name IN ({})
    GROUP BY items.sha256
    HAVING COUNT(DISTINCT tags.name) = ?
    '''.format(','.join(['?']*len(tags)))
    cursor.execute(query, tags + [len(tags)])
    items = cursor.fetchall()
    return items

def find_paths_by_tags(tags):
    conn = get_database_connection()
    results = []
    for item in find_items_by_tags(conn, tags):
        if result := get_working_path_by_sha256(conn, item[0]):
            results.append({
                'sha256': item[0],
                'type': item[2],
                'path': result[0],
                'last_modified': result[1]
            })
    conn.close()
    return results