from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from panoptikon.types import ItemData


@dataclass
class ExtractionJobStart:
    start_time: datetime
    total_items: int
    job_id: int


@dataclass
class ExtractionJobProgress:
    start_time: datetime
    processed_items: int
    total_items: int
    eta_string: str
    item: ItemData
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
