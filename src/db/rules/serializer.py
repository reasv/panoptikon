import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import (
    Any,
    Callable,
    List,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from src.db.rules.types import FilterType, RuleItemFilters

T = TypeVar("T")


def create_serialization_methods(type_def: Any):
    if get_origin(type_def) is Union:
        filter_types = get_args(type_def)
    elif isinstance(type_def, type):
        filter_types = (type_def,)
    else:
        raise ValueError("Input must be a Union type or a single type")

    class DynamicEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, filter_types):
                return {"__filter_type__": o.__class__.__name__, **asdict(o)}
            return super().default(o)

    def serialize(obj: Any) -> str:
        return json.dumps(obj, cls=DynamicEncoder)

    def decode_filter(dct):
        if "__filter_type__" in dct:
            filter_type = dct.pop("__filter_type__")
            for filter_class in filter_types:
                if filter_class.__name__ == filter_type:
                    return filter_class(**dct)
        return dct

    def deserialize(json_str: str) -> Any:
        return json.loads(json_str, object_hook=decode_filter)

    return serialize, deserialize


# Create serialization methods for FilterType
serialize_filter, deserialize_filter = create_serialization_methods(FilterType)


# Create serialization methods for RuleItemFilters
class RuleItemFiltersEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, RuleItemFilters):
            return {
                "__dataclass__": "RuleItemFilters",
                "positive": o.positive,
                "negative": o.negative,
            }
        return super().default(o)


def serialize_rule_item_filters(rule_filters: RuleItemFilters) -> str:
    return json.dumps(rule_filters, cls=RuleItemFiltersEncoder)


def deserialize_rule_item_filters(json_str: str) -> RuleItemFilters:
    def decode_rule_item_filters(dct):
        if "__dataclass__" in dct and dct["__dataclass__"] == "RuleItemFilters":
            return RuleItemFilters(
                positive=[
                    deserialize_filter(json.dumps(item))
                    for item in dct["positive"]
                ],
                negative=[
                    deserialize_filter(json.dumps(item))
                    for item in dct["negative"]
                ],
            )
        return dct

    return json.loads(json_str, object_hook=decode_rule_item_filters)
