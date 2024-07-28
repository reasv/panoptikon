import hashlib
import os
import sqlite3
from datetime import datetime
from typing import List

from click import File

from src.data_extractors.data_loaders.audio import extract_media_info
from src.data_extractors.data_loaders.video import video_to_frames
from src.db.files import get_file_by_path
from src.types import FileScanData
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
    allowed_extensions: List[str] = [],
):
    """
    Scan files in the given starting points and their entire directory trees, excluding the given excluded paths, and including images, video, and/or audio files.
    """
    allowed_extensions.extend(
        include_images * get_image_extensions()
        + include_video * get_video_extensions()
        + include_audio * get_audio_extensions()
    )
    for file_path in get_files_by_extension(
        starting_points=starting_points,
        excluded_paths=excluded_paths,
        extensions=allowed_extensions,
    ):
        mime_type = get_mime_type(file_path)
        try:
            last_modified, file_size = get_last_modified_time_and_size(
                file_path
            )
        except Exception as e:
            yield None
            continue

        md5: str | None = None
        sha256: str | None = None

        file_modified = True  # Assume the file has been modified

        # Check if the file is already in the database
        if file_data := get_file_by_path(conn, file_path):
            # Check if the file has been modified since the last scan
            if parse_iso_date(file_data["last_modified"]) == int(
                os.stat(file_path).st_mtime
            ):
                # Reuse the existing hash and mime type
                md5 = file_data["md5"]
                sha256 = file_data["sha256"]
                file_modified = False
            else:
                # File has been modified since the last scan
                file_modified = True

        if sha256 is None or md5 is None or file_modified:
            print(f"Extracting metadata for {file_path}")
            try:
                yield extract_file_metadata(
                    file_path, last_modified, file_size, bool(file_data)
                )
                # if file_data:
                #     if file_data["sha256"] == sha256:
                # print(f"File {file_path} has the same SHA-256 hash as the last scan, despite looking like it has been modified. Previous size: {file_data['size']}, current size: {file_size} bytes. Previous mtime: {file_data['last_modified']}, current mtime: {last_modified}.")
            except Exception as e:
                print(f"Error extracting metadata for {file_path}: {e}")
                yield None
        else:
            yield FileScanData(
                sha256=sha256,
                md5=md5,
                mime_type=mime_type,
                last_modified=last_modified,
                size=file_size,
                path=file_path,
                path_in_db=bool(file_data),
                modified=False,
            )


def extract_file_metadata(
    file_path: str, last_modified: str, size: int, path_in_db: bool
) -> FileScanData:
    """
    Extract metadata from a file.
    """
    md5, sha256 = calculate_hashes(file_path)
    mime_type = get_mime_type(file_path)
    file_scan_data = FileScanData(
        sha256=sha256,
        md5=md5,
        mime_type=mime_type,
        last_modified=last_modified,
        size=size,
        path=file_path,
        path_in_db=path_in_db,
        modified=True,
    )
    if mime_type.startswith("image"):
        from PIL import Image

        with Image.open(file_path) as img:
            width, height = img.size
        file_scan_data.width = width
        file_scan_data.height = height
    elif mime_type.startswith("video"):
        media_info = extract_media_info(file_path)
        if media_info.video_track:
            file_scan_data.width = media_info.video_track.width
            file_scan_data.height = media_info.video_track.height
            file_scan_data.duration = media_info.video_track.duration
            file_scan_data.audio_tracks = len(media_info.audio_tracks)
            file_scan_data.video_tracks = 1
            file_scan_data.subtitle_tracks = len(media_info.subtitle_tracks)
            frames = video_to_frames(file_path, num_frames=4)
            make_video_thumbnails(frames, sha256, mime_type)

    elif mime_type.startswith("audio"):
        media_info = extract_media_info(file_path)
        file_scan_data.duration = sum(
            track.duration for track in media_info.audio_tracks
        )
        file_scan_data.audio_tracks = len(media_info.audio_tracks)
        file_scan_data.video_tracks = 0
        file_scan_data.subtitle_tracks = len(media_info.subtitle_tracks)

    return file_scan_data


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
    mtime = stat.st_mtime
    size = stat.st_size
    return datetime.fromtimestamp(mtime).isoformat(), size


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
