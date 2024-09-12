from typing import List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import and_, asc, func, literal, literal_column
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.filters.sortable.sortable_filter import SortableFilter
from panoptikon.db.pql.types import (
    ExtraColumn,
    OrderTypeNN,
    QueryState,
    get_order_by_field,
    get_order_direction_field,
    get_std_cols,
)


class SemanticTextArgs(BaseModel):
    query: str = Field(
        ...,
        title="Query",
        description="Semantic query to match against the text",
    )

    model: str = Field(
        title="The text embedding model to use",
        description="""
The text embedding model to use for the semantic search.
Will search embeddings produced by this model.
""",
    )

    setters: List[str] = Field(
        default_factory=list,
        title="Include text from these setters",
        description="""
Filter out text that is was not set by these setters.
The setters are usually the names of the models that extracted or generated the text.
For example, the OCR model, the Whisper STT model, the captioning model or the tagger model.
""",
    )
    languages: List[str] = Field(
        default_factory=list,
        title="Included languages",
        description="Filter out text that is not in these languages",
    )
    language_min_confidence: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
        title="Minimum Confidence for Language Detection",
        description="""
Filter out text that has a language confidence score below this threshold.
Must be a value between 0 and 1.
Language confidence scores are usually set by the model that extracted the text.
For tagging models, it's always 1.
""",
    )
    min_confidence: Optional[float] = Field(
        default=None,
        ge=0,
        le=1,
        title="Minimum Confidence for the text",
        description="""
Filter out text that has a confidence score below this threshold.
Must be a value between 0 and 1.
Confidence scores are usually set by the model that extracted the text.
""",
    )
    min_length: Optional[int] = Field(
        default=None,
        ge=0,
        title="Minimum Length",
        description="Filter out text that is shorter than this. Inclusive.",
    )
    max_length: Optional[int] = Field(
        default=None,
        ge=0,
        title="Maximum Length",
        description="Filter out text that is longer than this. Inclusive.",
    )


class SemanticTextSearch(SortableFilter):
    order_by: bool = get_order_by_field(True)
    direction: OrderTypeNN = get_order_direction_field("asc")
    text_embeddings: SemanticTextArgs = Field(
        ...,
        title="Search Text Embeddings",
        description="""
Search for text using semantic search on text embeddings.
""",
    )

    def validate(self):
        if len(self.text_embeddings.query.strip()) == 0:
            return self.set_validated(False)

        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        raise NotImplementedError("build_query not implemented")
