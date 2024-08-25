import logging
import re
import shlex
from dataclasses import asdict, fields, is_dataclass
from pprint import pformat
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

import numpy as np

from panoptikon.db.search.types import (
    QueryFilters,
    QueryTagFilters,
    SearchQuery,
)

logger = logging.getLogger(__name__)


def clean_input(args: SearchQuery) -> SearchQuery:
    args.query.tags = clean_tag_params(args.query.tags)
    args.query.filters = clean_filter_params(args.query.filters)
    args.query.filters = parse_fts_queries(args.query.filters)
    return args


def parse_fts_queries(args: QueryFilters):
    """
    Parse and escape the query strings in the filters if they are not raw FTS5 queries.
    """
    if args.extracted_text and not args.extracted_text.raw_fts5_match:
        args.extracted_text.query = parse_and_escape_query(
            args.extracted_text.query
        )
    any_text = args.any_text
    if any_text is not None:
        if (
            any_text.extracted_text
            and not any_text.extracted_text.raw_fts5_match
        ):
            any_text.extracted_text.query = parse_and_escape_query(
                any_text.extracted_text.query
            )

        if any_text.path and not any_text.path.raw_fts5_match:
            any_text.path.query = parse_and_escape_query(any_text.path.query)

    return args


def clean_filter_params(args: QueryFilters) -> QueryFilters:
    # Remove filters with emtpy query values
    if args.extracted_text:
        if not args.extracted_text.query:
            args.extracted_text = None

    if args.extracted_text_embeddings:
        if (
            not args.extracted_text_embeddings.query
            or len(args.extracted_text_embeddings.query) == 0
            or not args.extracted_text_embeddings.model
            or len(args.extracted_text_embeddings.model) == 0
        ):
            args.extracted_text_embeddings = None

    if args.image_embeddings:
        if (
            not args.image_embeddings.query
            or len(args.image_embeddings.query) == 0
            or not args.image_embeddings.model
            or len(args.image_embeddings.model) == 0
        ):
            args.image_embeddings = None

    any_text = args.any_text
    if any_text is not None:
        if not any_text.extracted_text and not any_text.path:
            args.any_text = None

        if any_text.extracted_text and not any_text.extracted_text.query:
            any_text.extracted_text = None

        if any_text.path and not any_text.path.query:
            any_text.path = None

    args.any_text = any_text

    return args


def parse_and_escape_query(user_input: str) -> str:
    """
    Parse and escape a query string for use in FTS5 MATCH statements.
    """
    original_str = user_input
    # Replace escaped double quotes with a double double quote
    user_input = re.sub(r'\\"', r'""', user_input)
    # Step 1: Count the number of double quotes
    double_quote_count = user_input.count('"')

    # Step 2: If the number of double quotes is odd, add a closing quote
    if double_quote_count % 2 != 0:
        user_input += '"'
    # Escape for shlex
    user_input = re.sub(r"'", r"\'", user_input)
    # Convert SQL escape sequence to shlex accepted escape sequence
    user_input = re.sub(r'""', r"\"", user_input)
    # Step 3: Split the string into tokens
    try:
        tokens = shlex.split(user_input)
    except ValueError:
        logger.error(
            f"Shlex failed to parse query: '{user_input}' (from '{original_str}')"
        )
        tokens = user_input.split()
    # # Step 4: Escape double quotes in each token using the SQL escape sequence
    escaped_tokens = [re.sub(r'"', r'""', token) for token in tokens]

    # Step 5: Join the escaped tokens back into a single string
    return " ".join([f'"{token}"' for token in escaped_tokens])


def clean_tag_params(args: QueryTagFilters):
    # Normalize/clean/deduplicate the inputs
    def clean_tag_list(tag_list: List[str] | None) -> List[str]:
        if not tag_list:
            return []
        cleaned_tags = [
            tag.lower().strip() for tag in tag_list if tag.strip() != ""
        ]
        return list(set(cleaned_tags))

    tag_args = QueryTagFilters(
        pos_match_all=clean_tag_list(args.pos_match_all),
        pos_match_any=clean_tag_list(args.pos_match_any),
        neg_match_any=clean_tag_list(args.neg_match_any),
        neg_match_all=clean_tag_list(args.neg_match_all),
        all_setters_required=args.all_setters_required,
        setters=args.setters,
        namespaces=args.namespaces,
        min_confidence=args.min_confidence,
    )
    if len(tag_args.pos_match_any) == 1:
        # If only one tag is provided for "match any",
        # we can just set it as a regular "match all" tag
        tag_args.pos_match_all.append(tag_args.pos_match_any[0])
        tag_args.pos_match_any = []
    if len(tag_args.neg_match_all) == 1:
        # If only one tag is provided for negative "match all",
        # we can just set it as a regular "match any" negative tag
        tag_args.neg_match_any.append(tag_args.neg_match_all[0])
        tag_args.neg_match_all = []

    return tag_args


def dataclass_to_dict(obj):
    if is_dataclass(obj):
        return {k: dataclass_to_dict(v) for k, v in asdict(obj).items()}  # type: ignore
    elif isinstance(obj, list):
        return [dataclass_to_dict(i) for i in obj]
    else:
        return obj


def replace_bytes_with_length(d):
    """
    Recursively traverses a dictionary and replaces any value that is bytes
    with a string that says "[x] bytes" where x is the length.

    :param d: Dictionary to traverse
    :return: Modified dictionary with bytes replaced by their length descriptions
    """
    if isinstance(d, dict):
        return {k: replace_bytes_with_length(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [replace_bytes_with_length(v) for v in d]
    elif isinstance(d, bytes):
        return f"[{len(d)} bytes]"
    elif isinstance(d, np.ndarray):
        return f"[{d.size} bytes]"
    else:
        return d


def remove_empty_and_none(data):
    if isinstance(data, dict):
        return {
            k: remove_empty_and_none(v)
            for k, v in data.items()
            if v is not None
            and not (isinstance(v, (list, np.ndarray)) and len(v) == 0)
        }
    elif isinstance(data, list):
        return [
            remove_empty_and_none(item)
            for item in data
            if item is not None
            and not (isinstance(item, (list, np.ndarray)) and len(item) == 0)
        ]
    elif isinstance(data, np.ndarray):
        if data.size == 0:
            return None
        return data
    else:
        return data


def pprint_dataclass(obj):
    dictclass = dataclass_to_dict(obj)
    dictclass = remove_empty_and_none(dictclass)
    pstring = pformat(replace_bytes_with_length(dictclass))
    logger.debug(pstring)


from dataclasses import fields, is_dataclass
from typing import Any, Optional, Type, get_args, get_origin

T = TypeVar("T")


def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
    # if not is_dataclass(cls):
    #     raise TypeError(f"{cls} is not a dataclass.")

    field_types = {f.name: f.type for f in fields(cls)}  # type: ignore
    init_kwargs = {}
    for field_name, field_type in field_types.items():

        field_value = data.get(field_name, None)

        # Check if the field type is Optional
        origin_type = get_origin(field_type)
        if origin_type is Union:
            # Extract types in the Union
            union_args = get_args(field_type)
            # Handle the case where the Union includes None
            non_none_types = [
                arg for arg in union_args if arg is not type(None)
            ]
            # If there's only one non-None type, use it
            if len(non_none_types) == 1:
                field_type = non_none_types[0]
            else:
                # If multiple non-None types exist, decide how to handle
                # For simplicity, we use the first one here, but more complex logic may be required
                field_type = non_none_types[0]

        if field_value is not None:
            if is_dataclass(field_type):
                # Recursively convert for nested dataclasses
                init_kwargs[field_name] = from_dict(field_type, field_value)
            else:
                # Directly assign the value
                init_kwargs[field_name] = field_value
        # Else: field_value is None, leave out if not needed or keep as None

    return cls(**init_kwargs)
