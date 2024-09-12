from typing import Union

from panoptikon.db.pql.filters.path_in import InPaths
from panoptikon.db.pql.filters.sortable.bookmarks import (
    InBookmarks,
    InBookmarksArgs,
)
from panoptikon.db.pql.filters.sortable.extracted_text import (
    MatchText,
    MatchTextArgs,
)
from panoptikon.db.pql.filters.sortable.image_embeddings import (
    SemanticImageArgs,
    SemanticImageSearch,
)
from panoptikon.db.pql.filters.sortable.path_text import (
    MatchPath,
    MatchPathArgs,
)
from panoptikon.db.pql.filters.sortable.text_embeddings import (
    EmbedArgs,
    SemanticTextArgs,
    SemanticTextSearch,
)
from panoptikon.db.pql.filters.type_in import TypeIn

Filters = Union[
    InPaths,
    InBookmarks,
    TypeIn,
    MatchPath,
    MatchText,
    SemanticTextSearch,
    SemanticImageSearch,
]
