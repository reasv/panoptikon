import hashlib
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from src.data_extractors.data_loaders.audio import extract_media_info
from src.data_extractors.data_loaders.video import video_to_frames
from src.db import get_item_id
from src.db.files import get_file_by_path
from src.types import FileRecord, FileScanData, ItemScanMeta
from src.utils import get_mime_type, make_video_thumbnails, normalize_path


def get_files_by_extension(
    starting_points: List[str], excluded_paths: List[str], extensions: List[str]
):
    """
    Get all files with the given extensions in the given starting points and their entire directory trees, excluding the given excluded paths.
    """
    print(
        f"Scanning for files with extensions {extensions} in {starting_points} excluding {excluded_paths}"
    )
    excluded_paths = [
        normalize_path(excluded_path) for excluded_path in excluded_paths
    ]
    starting_points = [
        normalize_path(starting_point) for starting_point in starting_points
    ]
    for starting_point in starting_points:
        for root, dirs, files in os.walk(starting_point):
            # Normalize root path with trailing slash
            root_with_slash = normalize_path(root)

            # Filter directories: exclude if they start with any excluded path
            dirs[:] = [
                d
                for d in dirs
                if not any(
                    root_with_slash.startswith(excluded)
                    for excluded in excluded_paths
                )
            ]
            for file in files:
                if any(file.lower().endswith(ext) for ext in extensions):
                    yield os.path.join(root, file)


# Convert ISO string to epoch time number
def parse_iso_date(date: str) -> int:
    return int(datetime.fromisoformat(date).timestamp())


def scan_files(
    conn: sqlite3.Connection,
    starting_points: List[str],
    excluded_paths: List[str],
    include_images=True,
    include_video=True,
    include_audio=False,
    include_html=False,
    include_pdf=False,
    allowed_extensions: List[str] = [],
):
    """
    Scan files in the given starting points and their entire directory trees, excluding the given excluded paths, and including images, video, and/or audio files.
    """
    allowed_extensions.extend(
        include_images * get_image_extensions()
        + include_video * get_video_extensions()
        + include_audio * get_audio_extensions()
        + include_html * get_html_extensions()
        + include_pdf * get_pdf_extensions()
    )
    for file_path in get_files_by_extension(
        starting_points=starting_points,
        excluded_paths=excluded_paths,
        extensions=allowed_extensions,
    ):
        try:
            last_modified, file_size = get_last_modified_time_and_size(
                file_path
            )
        except Exception as e:
            print(f"Error getting last modified time for {file_path}: {e}")
            yield None, 0.0, 0.0
            continue

        # Assume file is new or has changed
        new_or_new_timestamp = True
        # Check if the file is already in the database
        if file_record := get_file_by_path(conn, file_path):
            # Check if the file has been modified since the last scan
            if last_modified == file_record.last_modified:
                # File has not been modified
                new_or_new_timestamp = False

        if new_or_new_timestamp:
            try:
                yield extract_file_metadata(
                    conn, file_path, last_modified, file_size, file_record
                )
            except Exception as e:
                print(f"Error extracting metadata for {file_path}: {e}")
                yield None, 0.0, 0.0
        else:
            assert file_record is not None
            yield FileScanData(
                sha256=file_record.sha256,
                last_modified=file_record.last_modified,
                path=file_record.path,
                new_file_timestamp=False,
                new_file_hash=False,
            ), 0.0, 0.0


def extract_file_metadata(
    conn: sqlite3.Connection,
    file_path: str,
    last_modified: str,
    size: int,
    file_record: FileRecord | None,
) -> Tuple[FileScanData, float, float]:
    """
    Extract metadata from a file.
    """
    hash_start = datetime.now()
    md5, sha256 = calculate_hashes(file_path)
    hash_time_seconds = (datetime.now() - hash_start).total_seconds()
    if file_record is not None and file_record.sha256 == sha256:
        print(
            f"File has a different timestamp "
            + f"but the same hash (P: {file_record.last_modified}, "
            + f"N: {last_modified}): {file_path}"
        )
        return (
            FileScanData(
                sha256=sha256,
                last_modified=last_modified,
                path=file_path,
                new_file_timestamp=True,
                new_file_hash=False,
            ),
            hash_time_seconds,
            0.0,
        )
    if get_item_id(conn, sha256):
        print(f"Item already exists: {file_path}")
        return (
            FileScanData(
                sha256=sha256,
                last_modified=last_modified,
                path=file_path,
                new_file_timestamp=True,
                new_file_hash=True,
            ),
            hash_time_seconds,
            0.0,
        )
    print(f"Extracting metadata for {file_path}")
    meta_start = datetime.now()
    mime_type = get_mime_type(file_path)
    item_meta = ItemScanMeta(
        md5=md5,
        mime_type=mime_type,
        size=size,
    )
    if mime_type.startswith("image"):
        from PIL import Image

        with Image.open(file_path) as img:
            width, height = img.size
        item_meta.width = width
        item_meta.height = height
    elif mime_type.startswith("video"):
        media_info = extract_media_info(file_path)
        if media_info.video_track:
            item_meta.width = media_info.video_track.width
            item_meta.height = media_info.video_track.height
            item_meta.duration = media_info.video_track.duration
            item_meta.audio_tracks = len(media_info.audio_tracks)
            item_meta.video_tracks = 1
            item_meta.subtitle_tracks = len(media_info.subtitle_tracks)
            frames = video_to_frames(file_path, num_frames=4)
            make_video_thumbnails(frames, sha256, mime_type)

    elif mime_type.startswith("audio"):
        media_info = extract_media_info(file_path)
        item_meta.duration = sum(
            track.duration for track in media_info.audio_tracks
        )
        item_meta.audio_tracks = len(media_info.audio_tracks)
        item_meta.video_tracks = 0
        item_meta.subtitle_tracks = len(media_info.subtitle_tracks)

    meta_time_seconds = (datetime.now() - meta_start).total_seconds()
    return (
        FileScanData(
            sha256=sha256,
            last_modified=last_modified,
            path=file_path,
            new_file_timestamp=True,
            new_file_hash=True,
            item_metadata=item_meta,
        ),
        hash_time_seconds,
        meta_time_seconds,
    )


def get_image_extensions():
    return [".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"]


def get_video_extensions():
    return [".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm"]


def get_audio_extensions():
    return [".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a"]


def get_html_extensions():
    return [".html", ".htm"]


def get_pdf_extensions():
    return [".pdf"]


def calculate_hashes(file_path: str):
    """
    Calculate the MD5 and SHA-256 hashes of the file at the given path.
    """
    hash_md5 = hashlib.md5()
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
                hash_sha256.update(chunk)
        return hash_md5.hexdigest(), hash_sha256.hexdigest()
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
    except PermissionError:
        print(
            f"Error: You do not have permission to access the file '{file_path}'."
        )
    except IsADirectoryError:
        print(f"Error: The path '{file_path}' is a directory, not a file.")
    except NotADirectoryError:
        print(
            f"Error: A component of the path '{file_path}' is not a directory."
        )
    except OSError as e:
        print(
            f"Error: An OS error occurred while accessing the file '{file_path}': {e}"
        )
    raise Exception("Error calculating hashes")


def get_os_stat(path: str):
    """
    Get the os.stat() information for the file at the given path.
    """
    try:
        info = os.stat(path)
        return info
    except FileNotFoundError:
        print(f"Error: The path '{path}' does not exist.")
    except PermissionError:
        print(f"Error: You do not have permission to access the path '{path}'.")
    except NotADirectoryError:
        print(f"Error: A component of the path '{path}' is not a directory.")
    except OSError as e:
        print(
            f"Error: An OS error occurred while accessing the path '{path}': {e}"
        )

    raise Exception("Error getting os.stat() information")


def get_last_modified_time_and_size(file_path: str):
    """
    Get the last modified time and the size of the file at the given path.
    """
    stat = get_os_stat(file_path)
    size = stat.st_size
    mtime_ns = stat.st_mtime_ns
    # Avoid floating point arithmetic by using integer division
    timestamp_s = mtime_ns // 1_000_000_000
    nanoseconds = mtime_ns % 1_000_000_000
    dt = datetime.fromtimestamp(timestamp_s, tz=timezone.utc)
    # Format the datetime to ISO 8601 string without microseconds
    iso_format = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return iso_format, size


def get_file_size(file_path: str):
    """
    Get the size of the file at the given path.
    """
    return get_last_modified_time_and_size(file_path)[1]


def deduplicate_paths(paths: List[str]):
    """
    Deduplicate paths by normalizing them and removing subpaths of other paths.
    """
    normalized_paths = [normalize_path(path) for path in paths]
    # Remove duplicates
    normalized_paths = list(set(normalized_paths))
    # Sort
    normalized_paths.sort()

    # Remove subpaths of other paths
    deduplicated_paths = []
    for path in normalized_paths:
        # Check if path is a subpath of the last added path
        if not deduplicated_paths or not path.startswith(
            deduplicated_paths[-1]
        ):
            deduplicated_paths.append(path)
    return deduplicated_paths
