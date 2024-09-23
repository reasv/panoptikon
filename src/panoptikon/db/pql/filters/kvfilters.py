from typing import List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel
from sqlalchemy import CTE, ClauseElement, and_, not_, or_, select
from sqlalchemy.sql._typing import _ColumnExpressionArgument

from panoptikon.db.pql.filters.filter import Filter
from panoptikon.db.pql.types import (
    FileColumns,
    ItemColumns,
    Operator,
    QueryState,
    SearchResult,
    TextColumns,
    contains_text_columns,
    get_column,
    get_std_cols,
)

FieldValueType = Union[str, int, float, bool, None]


class ArgValuesBase(BaseModel):

    def get_set_values(
        self,
    ) -> Sequence[
        Tuple[
            Union[FileColumns, ItemColumns, TextColumns],
            FieldValueType | List[FieldValueType],
        ]
    ]:
        mdict = self.model_dump(exclude_unset=True)
        return [(k, v) for k, v in mdict.items()]  # type: ignore


class ArgValues(ArgValuesBase):
    file_id: Optional[Union[int, List[int]]] = None
    item_id: Optional[Union[int, List[int]]] = None
    path: Optional[Union[str, List[str]]] = None
    filename: Optional[Union[str, List[str]]] = None
    sha256: Optional[Union[str, List[str]]] = None
    last_modified: Optional[Union[str, List[str]]] = None
    type: Optional[Union[str, List[str]]] = None
    size: Optional[Union[int, List[int]]] = None
    width: Optional[Union[int, List[int]]] = None
    height: Optional[Union[int, List[int]]] = None
    duration: Optional[Union[float, List[float]]] = None
    time_added: Optional[Union[str, List[str]]] = None
    md5: Optional[Union[str, List[str]]] = None
    audio_tracks: Optional[Union[int, List[int]]] = None
    video_tracks: Optional[Union[int, List[int]]] = None
    subtitle_tracks: Optional[Union[int, List[int]]] = None
    data_id: Optional[Union[int, List[int]]] = None
    language: Optional[Union[str, List[str]]] = None
    language_confidence: Optional[Union[float, List[float]]] = None
    text: Optional[Union[str, List[str]]] = None
    confidence: Optional[Union[float, List[float]]] = None
    text_length: Optional[Union[int, List[int]]] = None
    job_id: Optional[Union[int, List[int]]] = None
    setter_id: Optional[Union[int, List[int]]] = None
    setter_name: Optional[Union[str, List[str]]] = None
    data_index: Optional[Union[int, List[int]]] = None
    source_id: Optional[Union[int, List[int]]] = None


class ArgValuesScalar(ArgValuesBase):
    file_id: Optional[int] = None
    item_id: Optional[int] = None
    path: Optional[str] = None
    filename: Optional[str] = None
    sha256: Optional[str] = None
    last_modified: Optional[str] = None
    type: Optional[str] = None
    size: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[float] = None
    time_added: Optional[str] = None
    md5: Optional[str] = None
    audio_tracks: Optional[int] = None
    video_tracks: Optional[int] = None
    subtitle_tracks: Optional[int] = None
    data_id: Optional[int] = None
    language: Optional[str] = None
    language_confidence: Optional[float] = None
    text: Optional[str] = None
    confidence: Optional[float] = None
    text_length: Optional[int] = None
    job_id: Optional[int] = None
    setter_id: Optional[int] = None
    setter_name: Optional[str] = None
    data_index: Optional[int] = None
    source_id: Optional[int] = None


operatorType = Literal[
    "eq",
    "neq",
    "startswith",
    "not_startswith",
    "gt",
    "gte",
    "lt",
    "lte",
    "endswith",
    "not_endswith",
    "contains",
    "not_contains",
]


class KVFilter(Filter):
    def build_multi_kv_query(
        self,
        criteria: List[_ColumnExpressionArgument],
        context: CTE,
        state: QueryState,
    ) -> CTE:
        self.raise_if_not_validated()
        from panoptikon.db.pql.tables import (
            extracted_text,
            files,
            item_data,
            items,
            setters,
        )

        if not state.item_data_query:
            return self.wrap_query(
                select(*get_std_cols(context, state))
                .join(
                    items,
                    items.c.id == context.c.item_id,
                )
                .join(
                    files,
                    files.c.id == context.c.file_id,
                )
                .where(*criteria),
                context,
                state,
            )
        return self.wrap_query(
            select(*get_std_cols(context, state))
            .join(
                items,
                items.c.id == context.c.item_id,
            )
            .join(
                files,
                files.c.id == context.c.file_id,
            )
            .join(
                extracted_text,
                extracted_text.c.id == context.c.data_id,
            )
            .join(
                item_data,
                item_data.c.id == context.c.data_id,
            )
            .join(
                setters,
                setters.c.id == item_data.c.setter_id,
            )
            .where(*criteria),
            context,
            state,
        )


class MatchOps(BaseModel):
    eq: Optional[ArgValuesScalar] = None
    neq: Optional[ArgValuesScalar] = None
    in_: Optional[ArgValues] = None
    nin: Optional[ArgValues] = None
    gt: Optional[ArgValuesScalar] = None
    gte: Optional[ArgValuesScalar] = None
    lt: Optional[ArgValuesScalar] = None
    lte: Optional[ArgValuesScalar] = None
    startswith: Optional[ArgValues] = None
    not_startswith: Optional[ArgValues] = None
    endswith: Optional[ArgValues] = None
    not_endswith: Optional[ArgValues] = None
    contains: Optional[ArgValues] = None
    not_contains: Optional[ArgValues] = None
    and_: Optional[List["MatchOps"]] = None
    or_: Optional[List["MatchOps"]] = None
    not_: Optional["MatchOps"] = None


class MatchValues(KVFilter):
    match: MatchOps

    def _validate(self):
        for operator, _ in self.match.model_dump().items():
            args = getattr(self.match, operator, None)
            if args is not None:
                assert isinstance(args, ArgValuesBase), f"Invalid args: {args}"
                if len(args.get_set_values()) == 0:
                    setattr(self.match, operator, None)
                    continue
                # Find at least one valid operator
                return self.set_validated(True)

    def _build_expression(
        self, match_ops: MatchOps, text_columns: bool
    ) -> _ColumnExpressionArgument:
        expressions = []

        # Handle basic operators
        basic_operators = [
            "eq",
            "neq",
            "in_",
            "nin",
            "gt",
            "gte",
            "lt",
            "lte",
            "startswith",
            "not_startswith",
            "endswith",
            "not_endswith",
            "contains",
            "not_contains",
        ]

        for operator in basic_operators:
            args = getattr(match_ops, operator, None)
            if args is not None:
                for key, value in args.get_set_values():
                    if not text_columns and contains_text_columns([key]):
                        raise ValueError(
                            "Text columns are not allowed in this context"
                        )
                    column = get_column(key)
                    if not isinstance(value, list):
                        if operator == "eq":
                            expressions.append(column == value)
                        elif operator == "neq":
                            expressions.append(column != value)
                        elif operator == "startswith":
                            expressions.append(column.startswith(value))
                        elif operator == "not_startswith":
                            expressions.append(not_(column.startswith(value)))
                        elif operator == "endswith":
                            expressions.append(column.endswith(value))
                        elif operator == "not_endswith":
                            expressions.append(not_(column.endswith(value)))
                        elif operator == "contains":
                            expressions.append(column.contains(value))
                        elif operator == "not_contains":
                            expressions.append(not_(column.contains(value)))
                        elif operator == "gt":
                            expressions.append(column > value)
                        elif operator == "gte":
                            expressions.append(column >= value)
                        elif operator == "lt":
                            expressions.append(column < value)
                        elif operator == "lte":
                            expressions.append(column <= value)
                    else:
                        if operator == "eq":
                            expressions.append(column.in_(value))
                        elif operator == "neq":
                            expressions.append(column.notin_(value))
                        elif operator == "startswith":
                            expressions.append(
                                or_(*[column.startswith(v) for v in value])
                            )
                        elif operator == "not_startswith":
                            expressions.append(
                                and_(
                                    *[not_(column.startswith(v)) for v in value]
                                )
                            )
                        elif operator == "endswith":
                            expressions.append(
                                or_(*[column.endswith(v) for v in value])
                            )
                        elif operator == "not_endswith":
                            expressions.append(
                                and_(*[not_(column.endswith(v)) for v in value])
                            )
                        elif operator == "contains":
                            expressions.append(
                                or_(*[column.contains(v) for v in value])
                            )
                        elif operator == "not_contains":
                            expressions.append(
                                and_(*[not_(column.contains(v)) for v in value])
                            )
                        else:
                            raise ValueError("Invalid operator for list values")

        # Handle logical operators
        if match_ops.and_:
            and_expressions = [
                self._build_expression(sub_op, text_columns)
                for sub_op in match_ops.and_
            ]
            expressions.append(and_(*and_expressions))
        if match_ops.or_:
            or_expressions = [
                self._build_expression(sub_op, text_columns)
                for sub_op in match_ops.or_
            ]
            expressions.append(or_(*or_expressions))
        if match_ops.not_:
            not_expression = self._build_expression(
                match_ops.not_, text_columns
            )
            expressions.append(not_(not_expression))

        if not expressions:
            raise ValueError("No valid expressions found in MatchOps")

        # Combine all expressions with AND
        return and_(*expressions)

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        # Start building the expression from the root MatchOps
        expression = self._build_expression(self.match, state.item_data_query)
        return self.build_multi_kv_query([expression], context, state)
