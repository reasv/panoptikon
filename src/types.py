from dataclasses import dataclass
from typing import Literal


@dataclass
class FileScanData:
    sha256: str
    md5: str
    mime_type: str
    last_modified: str
    size: int
    path: str
    path_in_db: bool
    modified: bool


@dataclass
class ItemWithPath:
    sha256: str
    md5: str
    type: str
    size: int
    time_added: str
    path: str


@dataclass
class ExtractedText:
    item_sha256: str
    model_type: str
    setter: str
    language: str
    text: str
    confidence: float | None
    score: float


OrderByType = (
    Literal["last_modified", "path", "rank_fts", "rank_path_fts"] | None
)

OrderType = Literal["asc", "desc"] | None


@dataclass
class FileSearchResult:
    path: str
    sha256: str
    last_modified: str
    type: str


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


@dataclass
class FileRecord:
    sha256: str
    path: str
    last_modified: str


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str
    type: str
    setter: str
    threshold: float | None
    batch_size: int
    image_files: int
    video_files: int
    other_files: int
    total_segments: int
    errors: int
    total_remaining: int
