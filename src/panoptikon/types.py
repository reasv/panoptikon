from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Literal, Optional, Tuple, Type, Union

from pydantic import BaseModel, Field

# from panoptikon.db.pql.pql_model import QueryElement

if TYPE_CHECKING:
    from panoptikon.data_extractors.models import ModelOpts


@dataclass
class ItemScanMeta:
    md5: str
    mime_type: str
    width: int | None = None
    height: int | None = None
    duration: float | None = None
    audio_tracks: int | None = None
    video_tracks: int | None = None
    subtitle_tracks: int | None = None


@dataclass
class ItemRecord:
    id: int
    sha256: str
    md5: str
    type: str
    size: Optional[int]
    width: Optional[int]
    height: Optional[int]
    duration: Optional[float]
    audio_tracks: Optional[int]
    video_tracks: Optional[int]
    subtitle_tracks: Optional[int]
    blurhash: Optional[str]
    time_added: str


@dataclass
class FileScanData:
    sha256: str
    last_modified: str
    path: str
    new_file_timestamp: bool
    new_file_hash: bool
    file_size: int | None = None
    item_metadata: ItemScanMeta | None = None
    blurhash: str | None = None


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
    id: int
    item_sha256: str
    setter_name: str
    language: str
    language_confidence: float | None
    text: str
    confidence: float | None
    length: int


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
    false_changes: int
    metadata_time: float
    hashing_time: float
    thumbgen_time: float
    blurhash_time: float


@dataclass
class FileRecord:
    id: int
    sha256: str
    path: str
    last_modified: str
    filename: str = ""


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str
    items_in_db: int
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
    data_load_time: float
    inference_time: float
    failed: int
    completed: int
    status: int


@dataclass
class ExtractedTextStats:
    lowest_confidence: Union[float, None] = None
    lowest_language_confidence: Union[float, None] = None
    languages: List[str] = field(default_factory=list)


@dataclass
class SearchStats:
    all_setters: List[Tuple[str, str]] = field(default_factory=list)
    et_setters: List[str] = field(default_factory=list)
    et_stats: ExtractedTextStats = field(default_factory=ExtractedTextStats)
    clip_setters: List[str] = field(default_factory=list)
    te_setters: List[str] = field(default_factory=list)
    tag_setters: List[str] = field(default_factory=list)
    tag_namespaces: List[str] = field(default_factory=list)
    bookmark_namespaces: List[str] = field(default_factory=list)
    file_types: List[str] = field(default_factory=list)
    folders: List[str] = field(default_factory=list)
    loaded: bool = False


@dataclass
class RuleStats:
    folders: List[str] = field(default_factory=list)
    file_types: List[str] = field(default_factory=list)
    model_types: List[Type["ModelOpts"]] = field(default_factory=list)


class JobSettings(BaseModel):
    group_name: str
    inference_id: Optional[str] = None
    default_batch_size: Optional[int] = None
    default_threshold: Optional[float] = None


class CronJob(BaseModel):
    inference_id: str
    batch_size: Optional[int] = None
    threshold: Optional[float] = None


OutputDataType = Literal["tags", "text", "clip", "text-embedding"]
TargetEntityType = Literal["items", "text", "tags"]
