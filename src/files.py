import os
import hashlib
from collections import defaultdict
from datetime import datetime
import mimetypes
from typing import List

from .db import get_file_by_path, initialize_database, get_database_connection

def get_files_by_extension(starting_points: List[str], excluded_paths: List[str], extensions: List[str]):
    excluded_paths = [os.path.abspath(excluded_path) for excluded_path in excluded_paths]
    
    for starting_point in starting_points:
        for root, dirs, files in os.walk(starting_point):
            # Skip directories that are in the excluded paths
            dirs[:] = [d for d in dirs if os.path.abspath(os.path.join(root, d)) not in excluded_paths]

            for file in files:
                if any(file.lower().endswith(ext) for ext in extensions):
                    yield os.path.join(root, file)


def calculate_hashes(file_path):
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            hash_sha256.update(chunk)

    return hash_md5.hexdigest(), hash_sha256.hexdigest()

def get_last_modified_time(file_path):
    mtime = os.path.getmtime(file_path)
    return datetime.fromtimestamp(mtime).isoformat()

def get_file_size(file_path):
    return os.path.getsize(file_path)

def get_mime_type(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type

def scan_files(starting_points, allowed_extensions):
    initialize_database()
    result = defaultdict(lambda: {
        'sha256': '',
        'MD5': '',
        'mime_type': '',
        'size': 0,
        'paths': []
    })
    conn = get_database_connection()
    for file_path in get_files_by_extension(starting_points, allowed_extensions):
        mime_type = get_mime_type(file_path)
        file_size = get_file_size(file_path)
        last_modified = get_last_modified_time(file_path)
        md5, sha256 = None, None
        # Check if the file is already in the database
        if file_data := get_file_by_path(conn, file_path):
            # Check if the file has been modified since the last scan
            if file_data["last_modified"] == last_modified and file_data["size"] == file_size:
                # Reuse the existing hash and mime type
                md5 = file_data["md5"]
                sha256 = file_data["sha256"]

        if not sha256:
            md5, sha256 = calculate_hashes(file_path)

        if not result[sha256]['paths']:
            result[sha256]['sha256'] = sha256
            result[sha256]['MD5'] = md5
            result[sha256]['mime_type'] = mime_type
            result[sha256]['size'] = file_size
        result[sha256]['paths'].append({
            'path': file_path,
            'last_modified': last_modified
        })
    conn.close()
    return dict(result)

def load_paths_from_file(file_path):
    with open(file_path, 'r') as f:
        paths = [line.strip() for line in f if line.strip()]
    return paths

def scan_images(starting_points):
    # List of supported image file extensions
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp'}
    return scan_files(starting_points, image_extensions)