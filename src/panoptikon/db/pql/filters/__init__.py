from typing import Union

from sqlalchemy import CTE, literal_column

from panoptikon.db.pql.filters.path_in import InPaths
from panoptikon.db.pql.filters.sortable.bookmarks import (
    InBookmarks,
    InBookmarksArgs,
)
from panoptikon.db.pql.filters.sortable.extracted_text import (
    MatchText,
    MatchTextArgs,
)
from panoptikon.db.pql.filters.sortable.path_text import (
    MatchPath,
    MatchPathArgs,
)
from panoptikon.db.pql.filters.type_in import TypeIn
from panoptikon.db.pql.types import Filter, SortableFilter
from panoptikon.db.pql.utils import ExtraColumn, QueryState

Filters = Union[InPaths, InBookmarks, TypeIn, MatchPath, MatchText]
