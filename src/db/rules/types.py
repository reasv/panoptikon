from dataclasses import dataclass
from typing import List, Literal, Union

from sqlalchemy import column
from zipp import Path


class PathFilter:
    path_prefixes: List[str]


class MimeFilter:
    mime_type_prefixes: List[str]


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
    ]


FilterType = Union[PathFilter, MimeFilter, MinMaxFilter]


@dataclass
class FileFilters:
    positive: List[FilterType]
    negative: List[FilterType]
