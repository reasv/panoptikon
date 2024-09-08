from typing import Literal, Union

OrderByType = Literal[
    "last_modified",
    "path",
    "type",
    "size",
    "filename",
    "width",
    "height",
    "duration",
    "time_added",
]


OrderType = Union[Literal["asc", "desc"], None]
OrderTypeNN = Literal["asc", "desc"]
