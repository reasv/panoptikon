from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Literal, Tuple, Type, Union

if TYPE_CHECKING:
    from src.data_extractors.models import ModelOpts


@dataclass
class ItemScanMeta:
    md5: str
    mime_type: str
    size: int
    width: int | None = None
    height: int | None = None
    duration: float | None = None
    audio_tracks: int | None = None
    video_tracks: int | None = None
    subtitle_tracks: int | None = None


@dataclass
class FileScanData:
    sha256: str
    last_modified: str
    path: str
    new_file_timestamp: bool
    new_file_hash: bool
    item_metadata: ItemScanMeta | None = None


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
    false_changes: int
    metadata_time: float
    hashing_time: float
    thumbgen_time: float


@dataclass
class FileRecord:
    sha256: str
    path: str
    last_modified: str


@dataclass
class LogRecord:
    id: int
    start_time: str
    end_time: str | None
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
    data_load_time: float
    inference_time: float


@dataclass
class ExtractedTextStats:
    lowest_confidence: Union[float, None] = None
    lowest_language_confidence: Union[float, None] = None
    languages: List[str] = field(default_factory=list)


@dataclass
class SearchStats:
    all_setters: List[Tuple[str, str]] = field(default_factory=list)
    et_setters: List[Tuple[str, Tuple[str, str]]] = field(default_factory=list)
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


@dataclass
class SystemConfig:
    remove_unavailable_files: bool = True
    scan_images: bool = True
    scan_video: bool = True
    scan_audio: bool = False
    scan_html: bool = False
    scan_pdf: bool = False
    transaction_per_item: bool = True
    enable_cron_job: bool = False
    cron_schedule: str = "0 3 * * *"


OutputDataType = Literal["tags", "text", "clip", "text-embedding"]
TargetEntityType = Literal["items", "text", "tags"]
