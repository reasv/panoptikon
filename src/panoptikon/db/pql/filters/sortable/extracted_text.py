from typing import List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import Select, and_, asc, desc, func, literal_column, or_, text
from sqlalchemy.sql.expression import CTE, select

from panoptikon.db.pql.tables import (
    extracted_text,
    extracted_text_fts,
    item_data,
    setters,
)
from panoptikon.db.pql.types import (
    OrderTypeNN,
    SortableFilter,
    get_order_by_field,
    get_order_direction_field,
)


# # Filter arguments
class MatchTextArgs(BaseModel):
    match: str = Field(
        ...,
        title="Match",
        description="The query to match against text",
    )
    targets: List[str] = Field(
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
    raw_fts5_match: bool = Field(
        default=True,
        title="Allow raw FTS5 MATCH Syntax",
        description="If set to False, the query will be escaped before being passed to the FTS5 MATCH function",
    )


class MatchText(SortableFilter):
    order_by: bool = get_order_by_field(False)
    order_direction: OrderTypeNN = get_order_direction_field("asc")
    match_text: MatchTextArgs = Field(
        ...,
        title="Match Extracted Text",
        description="""
Match a query against text extracted from files or associated with them,
including tags and OCR text
""",
    )

    def build_query(self, context: CTE) -> Select:
        args = self.match_text
        criteria = [extracted_text_fts.c.text.match(args.match)]
        if args.targets:
            criteria.append(setters.c.name.in_(args.targets))
        if args.languages:
            criteria.append(extracted_text.c.language.in_(args.languages))
        if args.language_min_confidence:
            criteria.append(
                extracted_text.c.language_confidence
                >= args.language_min_confidence
            )
        if args.min_confidence:
            criteria.append(extracted_text.c.confidence >= args.min_confidence)

        return (
            select(
                context.c.file_id,
                context.c.item_id,
                self.get_rank_column(literal_column("rank")),
            )
            .join(item_data, item_data.c.item_id == context.c.item_id)
            .join(setters, setters.c.id == item_data.c.setter_id)
            .join(extracted_text, item_data.c.id == extracted_text.c.id)
            .join(
                extracted_text_fts,
                text("extracted_text_fts.rowid") == extracted_text.c.id,
            )
            .where(and_(*criteria))
            .group_by(context.c.file_id)
        )
