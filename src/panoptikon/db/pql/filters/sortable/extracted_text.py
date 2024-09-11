from typing import List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import (
    Select,
    and_,
    asc,
    desc,
    func,
    literal,
    literal_column,
    or_,
    text,
)
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
from panoptikon.db.search.utils import parse_and_escape_query


class MatchTextArgs(BaseModel):
    match: str = Field(
        ...,
        title="Match",
        description="The query to match against text",
    )
    filter_only: bool = Field(
        default=False,
        title="Filter Only",
        description="""
Only filter out text based on the other criteria,
without actually matching the query.

If set to True, the match field will be ignored.
Order by, select_as, and row_n will also be ignored.

If set to False (default), and the match field is empty,
this filter will be skipped entirely.
""",
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
    select_snippet_as: Optional[str] = Field(
        default=None,
        title="Return matching text snippet",
        description="""
If set, the best matching text *snippet* will be included in the `extra` dict of each result under this key
""",
    )
    s_max_len: int = Field(
        default=20,
        ge=0,
        title="Maximum Snippet Length",
        description="The maximum length (in tokens) of the snippet returned by select_snippet_as",
    )
    s_ellipsis: str = Field(
        default="...",
        title="Snippet Ellipsis",
        description="The ellipsis to use when truncating the snippet",
    )
    s_start_tag: str = Field(
        default="<b>",
        title="Snippet Start Tag",
        description="The tag to use at the beginning of the snippet",
    )
    s_end_tag: str = Field(
        default="</b>",
        title="Snippet End Tag",
        description="The tag to use at the end of the snippet",
    )


class MatchText(SortableFilter):
    order_by: bool = get_order_by_field(False)
    direction: OrderTypeNN = get_order_direction_field("asc")
    match_text: MatchTextArgs = Field(
        ...,
        title="Match Extracted Text",
        description="""
Match a query against text extracted from files or associated with them,
including tags and OCR text
""",
    )

    def validate(self) -> bool:
        if (
            not self.match_text.filter_only
            and len(self.match_text.match.strip()) == 0
        ):
            return self.set_validated(False)
        if self.match_text.filter_only:
            self.match_text.select_snippet_as = None
            self.order_by = False
            self.select_as = None
            self.row_n = False
            self.match_text.match = ""

        if not self.match_text.raw_fts5_match:
            self.match_text.match = parse_and_escape_query(
                self.match_text.match
            )
        return self.set_validated(True)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            extracted_text,
            extracted_text_fts,
            item_data,
            setters,
        )

        args = self.match_text
        criteria = [extracted_text_fts.c.text.match(args.match)]
        if args.filter_only:
            criteria = []
        if args.min_length:
            criteria.append(extracted_text.c.text_text >= args.min_length)
        if args.max_length:
            criteria.append(extracted_text.c.text_text <= args.max_length)
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

        snippet_col = func.snippet(
            literal_column("extracted_text_fts"),
            -1,
            args.s_start_tag,
            args.s_end_tag,
            args.s_ellipsis,
            args.s_max_len,
        ).label("snip")

        if not state.is_text_query:
            select_query = (
                select(*get_std_cols(context, state))
                .join(item_data, item_data.c.item_id == context.c.item_id)
                .join(setters, setters.c.id == item_data.c.setter_id)
                .join(extracted_text, item_data.c.id == extracted_text.c.id)
                .join(
                    extracted_text_fts,
                    literal_column("extracted_text_fts.rowid")
                    == extracted_text.c.id,
                )
                .where(and_(*criteria))
            )
            if args.select_snippet_as:
                rank_column = literal_column("rank")
                # Need to partition by file_id to get the best text result per file
                row_number_col = (
                    func.rank()
                    .over(
                        partition_by=context.c.file_id,
                        order_by=asc(rank_column),
                    )
                    .label("rn")
                )
                select_query = select_query.add_columns(
                    snippet_col, rank_column, row_number_col
                )
                # Wrap as subquery to filter out only the best text result per file
                select_query = select(
                    select_query.cte(
                        f"rownum_{self.get_cte_name(state.cte_counter)}"
                    )
                ).where(literal_column("rn") == 1)

            else:
                # Normal GROUP BY query
                select_query = select_query.group_by(context.c.file_id)
                rank_column = func.min(literal_column("rank"))
                if args.filter_only:
                    rank_column = literal(1)

            select_query = select_query.add_columns(
                self.derive_rank_column(rank_column)
            )

            cte = self.wrap_query(
                select_query,
                context,
                state,
            )
        else:
            # We are in a text-query and have a text_id
            rank_column = literal_column("rank")
            if args.filter_only:
                rank_column = literal(1)

            columns = [
                *get_std_cols(context, state),
                self.derive_rank_column(rank_column),
            ]
            if args.select_snippet_as:
                columns.append(snippet_col)
            cte = self.wrap_query(
                (
                    select(*columns)
                    .join(item_data, item_data.c.id == context.c.text_id)
                    .join(setters, setters.c.id == item_data.c.setter_id)
                    .join(
                        extracted_text, context.c.text_id == extracted_text.c.id
                    )
                    .join(
                        extracted_text_fts,
                        literal_column("extracted_text_fts.rowid")
                        == context.c.text_id,
                    )
                    .where(and_(*criteria))
                ),
                context,
                state,
            )
        if args.select_snippet_as and not state.is_count_query:
            state.extra_columns.append(
                ExtraColumn(
                    column=cte.c.snip,
                    cte=cte,
                    alias=args.select_snippet_as,
                    need_join=not self.order_by,
                )
            )
        return cte
