from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from pydantic import BaseModel, Field
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


class MatchValuesBase(BaseModel):

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


class MatchValues(MatchValuesBase):
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


class MatchValue(MatchValuesBase):
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
        criteria: _ColumnExpressionArgument,
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
                .where(criteria),
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
            .where(criteria),
            context,
            state,
        )


class MatchOps(BaseModel):
    eq: Optional[MatchValue] = None
    neq: Optional[MatchValue] = None
    in_: Optional[MatchValues] = None
    nin: Optional[MatchValues] = None
    gt: Optional[MatchValue] = None
    gte: Optional[MatchValue] = None
    lt: Optional[MatchValue] = None
    lte: Optional[MatchValue] = None
    startswith: Optional[MatchValues] = None
    not_startswith: Optional[MatchValues] = None
    endswith: Optional[MatchValues] = None
    not_endswith: Optional[MatchValues] = None
    contains: Optional[MatchValues] = None
    not_contains: Optional[MatchValues] = None


class MatchAnd(BaseModel):
    and_: List[MatchOps]


class MatchOr(BaseModel):
    or_: List[MatchOps]


class MatchNot(BaseModel):
    not_: MatchOps


Matches = Union[MatchOps, MatchAnd, MatchOr, MatchNot]


class Match(KVFilter):
    match: Matches = Field(
        ...,
        description="""
The match operations to apply. Match filters operate on key-value pairs representing
the primitive attributes of items, files, and extracted data.
For example, a match filter can be used to filter items 
based on their type, size, or the path of the file they are associated with.
""",
    )

    def _validate(self):
        """
        Recursively validate and clean the MatchOps structure.
        Remove any empty or invalid operators.
        Set the filter as validated only if at least one valid condition exists.
        """

        def clean_match_ops(match_ops: Matches) -> bool:
            """
            Recursively clean the MatchOps instance.
            Returns True if at least one valid condition exists, else False.
            """
            has_valid_condition = False

            # Define all basic operators
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

            # Clean basic operators
            if isinstance(match_ops, MatchOps):
                for operator in basic_operators:
                    args = getattr(match_ops, operator, None)
                    if args is not None:
                        if isinstance(args, MatchValuesBase):
                            if len(args.get_set_values()) == 0:
                                setattr(match_ops, operator, None)
                            else:
                                has_valid_condition = True
            if isinstance(match_ops, MatchAnd):
                # Clean and_ operator
                new_and = []
                for sub_op in match_ops.and_:
                    if clean_match_ops(sub_op):
                        new_and.append(sub_op)
                if new_and:
                    match_ops.and_ = new_and
                    has_valid_condition = True

            # Clean or_ operator
            if isinstance(match_ops, MatchOr):
                new_or = []
                for sub_op in match_ops.or_:
                    if clean_match_ops(sub_op):
                        new_or.append(sub_op)
                if new_or:
                    match_ops.or_ = new_or
                    has_valid_condition = True

            # Clean not_ operator
            if isinstance(match_ops, MatchNot):
                if clean_match_ops(match_ops.not_):
                    has_valid_condition = True

            return has_valid_condition

        # Start cleaning from the root MatchOps
        is_valid = clean_match_ops(self.match)

        if is_valid:
            return self.set_validated(True)
        else:
            return self.set_validated(False)

    def _build_expression(
        self, match_ops: Matches, text_columns: bool
    ) -> _ColumnExpressionArgument:
        # Handle logical operators first
        if isinstance(match_ops, MatchOr):
            or_expressions = [
                self._build_expression(sub_op, text_columns)
                for sub_op in match_ops.or_
            ]
            return or_(*or_expressions)
        if isinstance(match_ops, MatchAnd):
            and_expressions = [
                self._build_expression(sub_op, text_columns)
                for sub_op in match_ops.and_
            ]
            return and_(*and_expressions)
        if isinstance(match_ops, MatchNot):
            not_expression = not_(
                self._build_expression(match_ops.not_, text_columns)
            )
            return not_expression

        assert isinstance(match_ops, MatchOps), "Invalid Matches type"
        # Handle basic operators
        basic_expressions = []
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
            if args:
                assert isinstance(args, MatchValuesBase), "Invalid Args type"
                for key, value in args.get_set_values():
                    if not text_columns and contains_text_columns([key]):
                        raise ValueError(
                            "Text columns are not allowed in this context"
                        )
                    column = get_column(key)
                    if not isinstance(value, list):
                        if operator == "eq":
                            expr = column == value
                        elif operator == "neq":
                            expr = column != value
                        elif operator == "startswith":
                            expr = column.startswith(value)
                        elif operator == "not_startswith":
                            expr = not_(column.startswith(value))
                        elif operator == "endswith":
                            expr = column.endswith(value)
                        elif operator == "not_endswith":
                            expr = not_(column.endswith(value))
                        elif operator == "contains":
                            expr = column.contains(value)
                        elif operator == "not_contains":
                            expr = not_(column.contains(value))
                        elif operator == "gt":
                            expr = column > value
                        elif operator == "gte":
                            expr = column >= value
                        elif operator == "lt":
                            expr = column < value
                        elif operator == "lte":
                            expr = column <= value
                        else:
                            raise ValueError("Invalid operator")
                        basic_expressions.append(expr)
                    else:
                        # List values
                        if operator == "eq":
                            expr = column.in_(value)
                        elif operator == "neq":
                            expr = column.notin_(value)
                        elif operator == "in_":
                            expr = column.in_(value)
                        elif operator == "nin":
                            expr = column.notin_(value)
                        elif operator == "startswith":
                            expr = or_(*[column.startswith(v) for v in value])
                        elif operator == "not_startswith":
                            expr = and_(
                                *[not_(column.startswith(v)) for v in value]
                            )
                        elif operator == "endswith":
                            expr = or_(*[column.endswith(v) for v in value])
                        elif operator == "not_endswith":
                            expr = and_(
                                *[not_(column.endswith(v)) for v in value]
                            )
                        elif operator == "contains":
                            expr = or_(*[column.contains(v) for v in value])
                        elif operator == "not_contains":
                            expr = and_(
                                *[not_(column.contains(v)) for v in value]
                            )
                        else:
                            raise ValueError("Invalid operator for list values")
                        basic_expressions.append(expr)

        if basic_expressions:
            return and_(*basic_expressions)

        raise ValueError("No valid expressions found in MatchOps")

    def build_query(self, context: CTE, state: QueryState) -> CTE:
        # Start building the expression from the root MatchOps
        expression = self._build_expression(self.match, state.item_data_query)
        return self.build_multi_kv_query(expression, context, state)


# Evaluate Match object directly against a MatchValue object
def evaluate_match(match: Match, obj: MatchValue) -> bool:
    """
    Evaluates whether the given MatchValue object satisfies the Match rules.

    Args:
        match (Match): The Match object containing the matching rules.
        obj (MatchValue): The MatchValue object to evaluate against the rules.

    Returns:
        bool: True if the object satisfies the match conditions, False otherwise.
    """
    # Extract all set fields from the object, including those set to None
    obj_fields: Dict[str, Any] = dict(obj.get_set_values())

    def evaluate(matches: Matches, obj_fields: Dict[str, Any]) -> bool:
        if isinstance(matches, MatchOps):
            return evaluate_match_ops(matches, obj_fields)
        elif isinstance(matches, MatchAnd):
            return all(evaluate(sub_op, obj_fields) for sub_op in matches.and_)
        elif isinstance(matches, MatchOr):
            return any(evaluate(sub_op, obj_fields) for sub_op in matches.or_)
        elif isinstance(matches, MatchNot):
            return not evaluate(matches.not_, obj_fields)
        else:
            raise ValueError("Unsupported Matches type")

    def evaluate_match_ops(
        match_ops: MatchOps, obj_fields: Dict[str, Any]
    ) -> bool:
        # Collect all individual operator results
        results = []

        # Define operator functions
        operator_functions: Dict[str, Callable[[Any, Any], bool]] = {
            "eq": op_eq,
            "neq": op_neq,
            "in_": op_in,
            "nin": op_nin,
            "gt": op_gt,
            "gte": op_gte,
            "lt": op_lt,
            "lte": op_lte,
            "startswith": op_startswith,
            "not_startswith": op_not_startswith,
            "endswith": op_endswith,
            "not_endswith": op_not_endswith,
            "contains": op_contains,
            "not_contains": op_not_contains,
        }

        for op_name, op_func in operator_functions.items():
            op_value = getattr(match_ops, op_name, None)
            if op_value is not None:
                # op_value is either MatchValue or MatchValues
                if isinstance(op_value, MatchValue):
                    # Single value operations
                    for field, value in op_value.get_set_values():
                        if field in obj_fields:
                            field_value = obj_fields[field]
                            results.append(op_func(field_value, value))
                        # If the field is not set in obj, ignore this condition
                elif isinstance(op_value, MatchValues):
                    # List-based operations
                    for field, value in op_value.get_set_values():
                        if field in obj_fields:
                            field_value = obj_fields[field]
                            results.append(op_func(field_value, value))
                        # If the field is not set in obj, ignore this condition

        if not results:
            # No applicable conditions; default to True
            return True
        # All operators in MatchOps are ANDed together
        return all(results)

    # Define operator implementations
    def op_eq(field_value: Any, value: Any) -> bool:
        if isinstance(value, list):
            return field_value in value
        return field_value == value

    def op_neq(field_value: Any, value: Any) -> bool:
        if isinstance(value, list):
            return field_value not in value
        return field_value != value

    def op_in(field_value: Any, values: List[Any]) -> bool:
        return field_value in values

    def op_nin(field_value: Any, values: List[Any]) -> bool:
        return field_value not in values

    def op_gt(field_value: Any, value: Any) -> bool:
        if field_value is None:
            return False
        return field_value > value

    def op_gte(field_value: Any, value: Any) -> bool:
        if field_value is None:
            return False
        return field_value >= value

    def op_lt(field_value: Any, value: Any) -> bool:
        if field_value is None:
            return False
        return field_value < value

    def op_lte(field_value: Any, value: Any) -> bool:
        if field_value is None:
            return False
        return field_value <= value

    def op_startswith(field_value: str, value: Union[str, List[str]]) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return any(field_value.startswith(v) for v in value)
        return field_value.startswith(value)

    def op_not_startswith(
        field_value: str, value: Union[str, List[str]]
    ) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return all(not field_value.startswith(v) for v in value)
        return not field_value.startswith(value)

    def op_endswith(field_value: str, value: Union[str, List[str]]) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return any(field_value.endswith(v) for v in value)
        return field_value.endswith(value)

    def op_not_endswith(field_value: str, value: Union[str, List[str]]) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return all(not field_value.endswith(v) for v in value)
        return not field_value.endswith(value)

    def op_contains(field_value: Any, value: Union[Any, List[Any]]) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return any(v in field_value for v in value)
        return value in field_value

    def op_not_contains(field_value: Any, value: Union[Any, List[Any]]) -> bool:
        if field_value is None:
            return False
        if isinstance(value, list):
            return all(v not in field_value for v in value)
        return value not in field_value

    # Start evaluation from the root of the Match object
    return evaluate(match.match, obj_fields)
