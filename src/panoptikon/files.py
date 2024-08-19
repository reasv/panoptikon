import hashlib
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import List, Tuple

from panoptikon.data_extractors.data_loaders.audio import (
    extract_media_info,
    get_audio_thumbnail,
)
from panoptikon.data_extractors.data_loaders.images import (
    generate_thumbnail,
    get_html_image,
    get_pdf_image,
)
from panoptikon.data_extractors.data_loaders.video import video_to_frames
from panoptikon.db import get_item_id
from panoptikon.db.files import get_file_by_path
from panoptikon.db.rules.rules import get_rules_for_setter
from panoptikon.db.rules.types import (
    MimeFilter,
    MinMaxFilter,
    NotInPathFilter,
    PathFilter,
    StoredRule,
)
from panoptikon.db.storage import (
    get_frames,
    has_frame,
    has_thumbnail,
    store_frames,
    store_thumbnails,
)
from panoptikon.types import FileRecord, FileScanData, ItemScanMeta
from panoptikon.utils import (
    get_mime_type,
    make_video_thumbnails,
    normalize_path,
)

logger = logging.getLogger(__name__)


def get_files_by_extension(
    starting_points: List[str], excluded_paths: List[str], extensions: List[str]
):
    """
    Get all files with the given extensions in the given starting points and their entire directory trees, excluding the given excluded paths.
    """
    logger.info(
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
):
    """
    Scan files in the given starting points and their entire directory trees, excluding the given excluded paths, and including images, video, and/or audio files.
    """
    extensions = (
        include_images * get_image_extensions()
        + include_video * get_video_extensions()
        + include_audio * get_audio_extensions()
        + include_html * get_html_extensions()
        + include_pdf * get_pdf_extensions()
    )
    user_rules = get_rules_for_setter(conn, "file_scan")
    for file_path in get_files_by_extension(
        starting_points=starting_points,
        excluded_paths=excluded_paths,
        extensions=extensions,
    ):
        try:
            last_modified, file_size = get_last_modified_time_and_size(
                file_path
            )
        except Exception as e:
            logger.info(
                f"Error getting last modified time for {file_path}: {e}"
            )
            yield None, 0.0, 0.0
            continue

        # Check if the file matches any user rules
        if user_rules and not matches_rules(
            user_rules, file_path, size=file_size
        ):
            logger.debug(f"File {file_path} does not match any rules")
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
                logger.error(f"Error extracting metadata for {file_path}: {e}")
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


def matches_rules(rules: List[StoredRule], file_path: str, size: int) -> bool:
    """
    Check if the given file path matches any of the given rules.
    Only checks MIME type and path for now.
    """
    for rule in rules:
        if matches_rule(rule, file_path, size):
            return True
    return False


def matches_rule(rule: StoredRule, file_path: str, size: int) -> bool:
    """
    Check if the given file path matches the given rule.
    """
    for filter in rule.filters.positive:
        if (
            not isinstance(filter, PathFilter)
            and not isinstance(filter, NotInPathFilter)
            and not isinstance(filter, MimeFilter)
            and not (
                isinstance(filter, MinMaxFilter)
                and filter.column_name == "size"
            )
        ):
            logger.debug(
                f"Unsupported filter type: {type(filter)} for file {file_path}"
            )
            continue
        if not matches_filter(filter, file_path, size):
            return False
    for filter in rule.filters.negative:
        # For negative filters,
        # we only support MimeFilter and size because
        # PathFilter and NotInPathFilter would
        # have inconsistent behavior with the SQL equivalent
        # which uses EXCEPT on the positive query.
        if not isinstance(filter, MimeFilter) and not (
            isinstance(filter, MinMaxFilter) and filter.column_name == "size"
        ):
            logger.debug(
                f"Unsupported filter type: {type(filter)} for file {file_path}"
            )
            continue
        if matches_filter(filter, file_path, size):
            return False
    return True


def matches_filter(
    filter: PathFilter | NotInPathFilter | MimeFilter | MinMaxFilter,
    file_path: str,
    size: int,
) -> bool:
    """
    Check if the given file path matches the given filter.
    """
    if isinstance(filter, PathFilter):
        for path in filter.path_prefixes:
            if file_path.startswith(path):
                return True
    elif isinstance(filter, NotInPathFilter):
        for path in filter.path_prefixes:
            if file_path.startswith(path):
                return False
        return True
    elif isinstance(filter, MimeFilter):
        file_mime_type = get_mime_type(file_path)
        for mime_type in filter.mime_type_prefixes:
            if file_mime_type.startswith(mime_type):
                return True
    elif isinstance(filter, MinMaxFilter):
        assert filter.column_name == "size", "Unsupported column name"
        if filter.min_value == filter.max_value:
            return size == filter.min_value
        if filter.min_value > 0 and filter.max_value == 0:
            # Only min value is set
            return size >= filter.min_value
        return filter.min_value <= size <= filter.max_value
    return False


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
        logger.warning(
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
        logger.info(f"Item already exists: {file_path}")
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
    logger.info(f"Extracting metadata for {file_path}")
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
        logger.error(f"Error: The file '{file_path}' does not exist.")
    except PermissionError:
        logger.error(
            f"Error: You do not have permission to access the file '{file_path}'."
        )
    except IsADirectoryError:
        logger.error(
            f"Error: The path '{file_path}' is a directory, not a file."
        )
    except NotADirectoryError:
        logger.error(
            f"Error: A component of the path '{file_path}' is not a directory."
        )
    except OSError as e:
        logger.error(
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
        logger.error(f"Error: The path '{path}' does not exist.")
    except PermissionError:
        logger.error(
            f"Error: You do not have permission to access the path '{path}'."
        )
    except NotADirectoryError:
        logger.error(
            f"Error: A component of the path '{path}' is not a directory."
        )
    except OSError as e:
        logger.error(
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


def ensure_thumbnail_exists(
    conn: sqlite3.Connection, sha256: str, file_path: str
):
    """
    Ensure that a thumbnail exists for the given item.
    """
    start_time = time.time()
    thumbnail_process_version = 1
    frame_version = 1
    if has_thumbnail(conn, sha256, thumbnail_process_version):
        return
    mime_type = get_mime_type(file_path)

    if mime_type.startswith("video"):
        if frames := get_frames(conn, sha256):
            logger.debug(f"Found video frames for {file_path}")
        else:
            logger.debug(f"Extracting video frames for {file_path}")
            frames = video_to_frames(file_path, num_frames=4)
            store_frames(
                conn,
                sha256=sha256,
                file_mime_type=mime_type,
                process_version=frame_version,
                frames=frames,
            )
        assert len(frames) > 0, "No frames found"
        logger.debug(f"Generating video thumbnails for {file_path}")
        thumbs = make_video_thumbnails(frames, sha256, mime_type)
    elif mime_type.startswith("audio"):
        thumbs = [get_audio_thumbnail(mime_type, file_path)]
    elif mime_type.startswith("image"):
        thumb = generate_thumbnail(file_path)
        thumbs = [thumb] if thumb else []
    elif mime_type.startswith("application/pdf"):
        thumbs = [get_pdf_image(file_path)]
    elif mime_type.startswith("text/html"):
        thumbs = [get_html_image(file_path)]
    else:
        logger.debug(
            f"No thumbnail generation for type {mime_type}: {file_path}"
        )
        return
    generation_time = round(time.time() - start_time, 2)
    if thumbs:
        store_start = time.time()
        store_thumbnails(
            conn,
            sha256,
            mime_type,
            thumbnail_process_version,
            thumbs,
        )
        store_time = round(time.time() - store_start, 2)
        logger.debug(
            f"Generated image thumbnail (Gen: {generation_time} DB: {store_time}) for {file_path}"
        )
