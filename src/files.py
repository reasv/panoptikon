import os
import hashlib
from collections import defaultdict
from datetime import datetime
import mimetypes

from .db import get_file_by_path

def get_image_files(starting_points):
    # List of supported image file extensions
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff', '.webp'}

    for starting_point in starting_points:
        for root, _, files in os.walk(starting_point):
            for file in files:
                if any(file.lower().endswith(ext) for ext in image_extensions):
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

def get_mime_type(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type

def find_images_and_hashes(starting_points):
    result = defaultdict(lambda: {
        'sha256': '',
        'MD5': '',
        'mime_type': '',
        'paths': []
    })

    for file_path in get_image_files(starting_points):
        mime_type = get_mime_type(file_path)
        last_modified = get_last_modified_time(file_path)
        # Check if the file is already in the database
        if file_data := get_file_by_path(file_path):
            # Check if the file has been modified since the last scan
            if file_data["last_modified"] == last_modified:
                # Reuse the existing hash and mime type
                md5 = file_data["md5"]
                sha256 = file_data["item"]
            else:
                md5, sha256 = calculate_hashes(file_path)

        if not result[sha256]['paths']:
            result[sha256]['sha256'] = sha256
            result[sha256]['MD5'] = md5
            result[sha256]['mime_type'] = mime_type
        result[sha256]['paths'].append({
            'path': file_path,
            'last_modified': last_modified
        })

    return dict(result)

def load_paths_from_file(file_path):
    with open(file_path, 'r') as f:
        paths = [line.strip() for line in f if line.strip()]
    return paths

if __name__ == '__main__':
    file_path = 'paths.txt'
    starting_points = load_paths_from_file(file_path)
    hashes_info = find_images_and_hashes(starting_points)
    print(hashes_info)