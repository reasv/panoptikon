from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel


@dataclass
class ExtractionJobStart:
    start_time: datetime
    total_items: int
    job_id: int


class JobInputData(BaseModel):
    file_id: int
    item_id: int
    path: str
    sha256: str
    md5: str
    last_modified: str
    type: str
    # Video/audio columns (only present for file-* queries)
    duration: Optional[float] = None
    audio_tracks: Optional[int] = None
    video_tracks: Optional[int] = None
    subtitle_tracks: Optional[int] = None
    # Text columns (only present for text-* queries)
    data_id: Optional[int] = None  # Always present for text-* queries
    text: Optional[str] = None


@dataclass
class ExtractionJobProgress:
    start_time: datetime
    processed_items: int
    total_items: int
    eta_string: str
    item: JobInputData
    job_id: int


@dataclass
class ExtractionJobReport:
    start_time: datetime
    end_time: datetime
    images: int
    videos: int
    other: int
    total: int
    units: int
    failed_paths: List[str]


@dataclass
class TagResult:
    namespace: str
    tags: List[Tuple[str, dict[str, float]]]
    mcut: float
    rating_severity: List[str]
    metadata: dict[str, str]
