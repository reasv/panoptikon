from typing import List, Optional

from click import group
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
    get_std_group_by,
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
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            embeddings,
            extracted_text,
            item_data,
            setters,
        )

        args = self.text_embeddings
        criteria = []
        text_data = item_data.alias("text_data")
        text_setters = setters.alias("text_setters")
        vec_data = item_data.alias("vec_data")
        vec_setters = setters.alias("vec_setters")
        if args.min_length:
            criteria.append(extracted_text.c.text_text >= args.min_length)
        if args.max_length:
            criteria.append(extracted_text.c.text_text <= args.max_length)
        if args.setters:
            criteria.append(text_setters.c.name.in_(args.setters))
        if args.languages:
            criteria.append(extracted_text.c.language.in_(args.languages))
        if args.language_min_confidence:
            criteria.append(
                extracted_text.c.language_confidence
                >= args.language_min_confidence
            )
        if args.min_confidence:
            criteria.append(extracted_text.c.confidence >= args.min_confidence)
        if args.model:
            criteria.append(vec_setters.c.name == args.model)

        vec_distance = func.vec_distance_L2(
            embeddings.c.embedding, literal(args.query)
        )

        rank_column = func.min(vec_distance)

        return self.wrap_query(
            select(
                *get_std_cols(context, state),
                self.derive_rank_column(rank_column)
            )
            .join(
                text_data,
                (text_data.c.item_id == context.c.item_id)
                & (text_data.c.data_type == "text"),
            )
            .join(text_setters, text_setters.c.id == text_data.c.setter_id)
            .join(extracted_text, text_data.c.id == extracted_text.c.id)
            .join(
                vec_data,
                (vec_data.c.source_id == extracted_text.c.id)
                & (vec_data.c.data_type == "text-embedding"),
            )
            .join(vec_setters, vec_setters.c.id == vec_data.c.setter_id)
            .join(embeddings, embeddings.c.id == vec_data.c.id)
            .where(and_(*criteria))
            .group_by(*get_std_group_by(context, state)),
            context,
            state,
        )
