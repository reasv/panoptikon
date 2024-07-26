from dataclasses import asdict, is_dataclass
from pprint import pprint
from typing import List

from typeguard import typechecked

from src.db.search.types import QueryTagFilters, SearchQuery


@typechecked
def clean_input(args: SearchQuery) -> SearchQuery:
    args.query.tags = clean_tag_params(args.query.tags)
    return args


@typechecked
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
    else:
        return d


def pprint_dataclass(obj):
    dictclass = dataclass_to_dict(obj)
    pprint(replace_bytes_with_length(dictclass))
