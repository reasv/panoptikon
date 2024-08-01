from dataclasses import dataclass
from typing import List, Literal, Union


@dataclass
class PathFilter:
    path_prefixes: List[str]


@dataclass
class MimeFilter:
    mime_type_prefixes: List[str]


@dataclass
class MinMaxFilter:
    min_value: float
    max_value: float
    column_name: Literal[
        "width",
        "height",
        "duration",
        "size",
        "video_tracks",
        "audio_tracks",
        "subtitle_tracks",
        "largest_dimension",
        "smallest_dimension",
    ]


@dataclass
class ProcessedItemsFilter:
    setter_id: int


FilterType = Union[PathFilter, MimeFilter, MinMaxFilter, ProcessedItemsFilter]


@dataclass
class FilterSet:
    processed_items: ProcessedItemsFilter
    mime_types: MimeFilter
    paths: PathFilter
    min_max: List[MinMaxFilter]


@dataclass
class RuleItemFilters:
    positive: List[FilterType]
    negative: List[FilterType]
