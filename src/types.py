from dataclasses import dataclass, field
from typing import List, Tuple, Union


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
    width: int | None = None
    height: int | None = None
    duration: float | None = None
    audio_tracks: int | None = None
    video_tracks: int | None = None
    subtitle_tracks: int | None = None


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
    setter_name: str
    language: str
    language_confidence: float | None
    text: str
    confidence: float | None


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
    setter_id: int | None
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


@dataclass
class ExtractedTextStats:
    lowest_confidence: Union[float, None] = None
    lowest_language_confidence: Union[float, None] = None
    languages: List[str] = field(default_factory=list)


@dataclass
class SearchStats:
    all_setters: List[Tuple[str, str]] = field(default_factory=list)
    et_setters: List[Tuple[str, str]] = field(default_factory=list)
    et_stats: ExtractedTextStats = field(default_factory=ExtractedTextStats)
    clip_setters: List[str] = field(default_factory=list)
    te_setters: List[str] = field(default_factory=list)
    tag_setters: List[str] = field(default_factory=list)
    tag_namespaces: List[str] = field(default_factory=list)
    bookmark_namespaces: List[str] = field(default_factory=list)
    file_types: List[str] = field(default_factory=list)
    folders: List[str] = field(default_factory=list)


@dataclass
class RuleStats:
    folders: List[str] = field(default_factory=list)
    file_types: List[str] = field(default_factory=list)
