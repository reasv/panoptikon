from dataclasses import dataclass
from datetime import datetime
from typing import List

from src.types import ItemWithPath


@dataclass
class ExtractorJobProgress:
    start_time: datetime
    processed_items: int
    total_items: int
    eta_string: str
    item: ItemWithPath


@dataclass
class ExtractorJobReport:
    start_time: datetime
    end_time: datetime
    images: int
    videos: int
    other: int
    total: int
    units: int
    failed_paths: List[str]
