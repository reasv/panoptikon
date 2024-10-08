from dataclasses import dataclass
from typing import List, Literal, Sequence, Union

from panoptikon.types import OutputDataType


@dataclass
class PathFilter:
    path_prefixes: List[str]


@dataclass
class NotInPathFilter:
    path_prefixes: List[str]


@dataclass
class MimeFilter:
    mime_type_prefixes: List[str]


min_max_columns = [
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
MinMaxColumnType = Literal[
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
class MinMaxFilter:
    min_value: float
    max_value: float
    column_name: MinMaxColumnType


@dataclass
class ProcessedItemsFilter:
    setter_name: str


@dataclass
class ProcessedExtractedDataFilter:
    setter_name: str
    data_types: Sequence[OutputDataType]


FilterType = Union[
    PathFilter,
    NotInPathFilter,
    MimeFilter,
    MinMaxFilter,
    ProcessedItemsFilter,
    ProcessedExtractedDataFilter,
]


@dataclass
class RuleItemFilters:
    positive: List[FilterType]
    negative: List[FilterType]


@dataclass
class StoredRule:
    id: int
    enabled: bool
    setters: List[str]
    filters: RuleItemFilters


def combine_rule_item_filters(
    first: RuleItemFilters, second: RuleItemFilters
) -> RuleItemFilters:
    """
    Combines two RuleItemFilters objects by chaining their positive and negative filters.

    Args:
    first (RuleItemFilters): The first RuleItemFilters object.
    second (RuleItemFilters): The second RuleItemFilters object.

    Returns:
    RuleItemFilters: A new RuleItemFilters object with combined filters.
    """
    return RuleItemFilters(
        positive=first.positive + second.positive,
        negative=first.negative + second.negative,
    )
