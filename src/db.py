import os
import sqlite3
from datetime import datetime
from .files import find_images_and_hashes, load_paths_from_file
import time

def get_database_connection():
    db_file = os.getenv('DB_FILE', 'sqlite.db')
    conn = sqlite3.connect(db_file)
    return conn

def initialize_database(conn):
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS items (
        sha256 TEXT PRIMARY KEY,
        md5 TEXT,
        type TEXT,
        time_added TEXT,         -- Using TEXT to store ISO-8601 formatted datetime
        time_last_seen TEXT      -- Using TEXT to store ISO-8601 formatted datetime
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item TEXT,
        path TEXT UNIQUE,        -- Ensuring path is unique
        last_modified TEXT,      -- Using TEXT to store ISO-8601 formatted datetime
        last_seen TEXT,          -- Using TEXT to store ISO-8601 formatted datetime
        available BOOLEAN,       -- BOOLEAN to indicate if the path is available
        FOREIGN KEY(item) REFERENCES items(sha256)
    )
    ''')

    # New table for file scans
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        time TEXT,               -- Using TEXT to store ISO-8601 formatted datetime
        path TEXT,
        UNIQUE(time, path)       -- Unique constraint on time and path
    )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_md5 ON items(md5)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_type ON items(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_time_added ON items(time_added)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_time_last_seen ON items(time_last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_item ON files(item)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_modified ON files(last_modified)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_available ON files(available)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)')  # Explicit index on path
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_last_seen ON files(last_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_time ON file_scans(time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_scans_path ON file_scans(path)')

    conn.commit()

def insert_or_update_file_data(conn, image_data, scan_time):
    cursor = conn.cursor()
    sha256 = image_data['sha256']
    md5 = image_data['MD5']
    mime_type = image_data['mime_type']
    paths = image_data['paths']
    
    cursor.execute('''
    INSERT INTO items (sha256, md5, type, time_added, time_last_seen)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(sha256) DO UPDATE SET time_last_seen=excluded.time_last_seen
    ''', (sha256, md5, mime_type, scan_time, scan_time))
    
    for path_data in paths:
        path = path_data['path']
        last_modified = path_data['last_modified']
        
        # Check if the path already exists
        cursor.execute('SELECT item FROM files WHERE path = ?', (path,))
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
                INSERT INTO files (item, path, last_modified, last_seen, available)
                VALUES (?, ?, ?, ?, TRUE)
                ''', (sha256, path, last_modified, scan_time))
        else:
            # Path does not exist, insert new
            cursor.execute('''
            INSERT INTO files (item, path, last_modified, last_seen, available)
            VALUES (?, ?, ?, ?, TRUE)
            ''', (sha256, path, last_modified, scan_time))
    
    conn.commit()


def update_items_available():
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

def save_items_to_database(images_data, paths):
    conn = get_database_connection()
    initialize_database(conn)

    successful_insert = False
    while not successful_insert:
        try:
            scan_time = datetime.now().isoformat()

            # Start a transaction
            with conn:
                cursor = conn.cursor()

                # Insert a scan entry for each parent folder path
                for path in paths:
                    cursor.execute('''
                    INSERT INTO file_scans (time, path)
                    VALUES (?, ?)
                    ''', (scan_time, path))
                
                # Commit the transaction if all inserts succeed
                conn.commit()
                successful_insert = True

        except sqlite3.IntegrityError:
            # Rollback the transaction on failure and wait before retrying
            conn.rollback()
            time.sleep(1)

    for sha256, image_data in images_data.items():
        insert_or_update_file_data(conn, image_data, scan_time)
    
    mark_unavailable_files(conn, scan_time, paths)

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
    
    conn.commit()

if __name__ == '__main__':
    file_path = 'paths.txt'
    starting_points = load_paths_from_file(file_path)
    hashes_info = find_images_and_hashes(starting_points)
    save_items_to_database(hashes_info, starting_points)