from typing import Union

from panoptikon.db.pql.filters.kvfilters import (
    Match,
    MatchAnd,
    MatchNot,
    MatchOps,
    MatchOr,
    MatchValue,
    MatchValues,
)
from panoptikon.db.pql.filters.processed_by import ProcessedBy
from panoptikon.db.pql.filters.processed_extracted_data import (
    DerivedDataArgs,
    HasUnprocessedData,
)
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
from panoptikon.db.pql.filters.sortable.item_similarity import (
    SimilarityArgs,
    SimilarTo,
    SourceArgs,
)
from panoptikon.db.pql.filters.sortable.path_text import (
    MatchPath,
    MatchPathArgs,
)
from panoptikon.db.pql.filters.sortable.tags import MatchTags, TagsArgs
from panoptikon.db.pql.filters.sortable.text_embeddings import (
    EmbedArgs,
    SemanticTextArgs,
    SemanticTextSearch,
)

Filters = Union[
    SimilarTo,
    InBookmarks,
    MatchPath,
    MatchText,
    SemanticTextSearch,
    SemanticImageSearch,
    MatchTags,
    HasUnprocessedData,
    ProcessedBy,
    Match,
]
