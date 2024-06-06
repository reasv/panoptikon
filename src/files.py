import os
import hashlib
from collections import defaultdict
from datetime import datetime
import mimetypes
from typing import List

from .db import get_file_by_path, get_database_connection
from src.utils import normalize_path

def get_files_by_extension(starting_points: List[str], excluded_paths: List[str], extensions: List[str]):
    excluded_paths = [normalize_path(excluded_path) for excluded_path in excluded_paths]
    starting_points = [normalize_path(starting_point) for starting_point in starting_points]
    for starting_point in starting_points:
        for root, dirs, files in os.walk(starting_point):
            # Normalize root path with trailing slash
            root_with_slash = normalize_path(root)
            
            # Filter directories: exclude if they start with any excluded path
            dirs[:] = [
                d for d in dirs 
                if not any(root_with_slash.startswith(excluded) for excluded in excluded_paths)
            ]
            for file in files:
                if any(file.lower().endswith(ext) for ext in extensions):
                    yield os.path.join(root, file)

def scan_files(
        starting_points: List[str],
        excluded_paths: List[str],
        allowed_extensions: List[str]
    ) -> dict[str, dict[str, None | str | int | dict[str, str]]]:

    result = defaultdict(lambda: {
        'sha256': '',
        'MD5': '',
        'mime_type': '',
        'size': 0,
        'paths': dict()
    })
    conn = get_database_connection()
    for file_path in get_files_by_extension(starting_points, excluded_paths, allowed_extensions):
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
            print(f"Calculating hashes for {file_path}")
            md5, sha256 = calculate_hashes(file_path)

        if result[sha256]['size'] == 0:
            result[sha256]['sha256'] = sha256
            result[sha256]['MD5'] = md5
            result[sha256]['mime_type'] = mime_type
            result[sha256]['size'] = file_size
        result[sha256]['paths'][file_path] = last_modified
    conn.close()
    return dict(result)

def get_image_extensions():
    return ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp']

def get_video_extensions():
    return ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm']

def get_audio_extensions():
    return ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a']

def calculate_hashes(file_path: str):
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            hash_sha256.update(chunk)

    return hash_md5.hexdigest(), hash_sha256.hexdigest()

def get_last_modified_time(file_path: str):
    mtime = os.path.getmtime(file_path)
    return datetime.fromtimestamp(mtime).isoformat()

def get_file_size(file_path: str):
    return os.path.getsize(file_path)

def get_mime_type(file_path: str):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type