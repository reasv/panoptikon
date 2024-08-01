from typing import List

from src.db.rules.mime_type_filter import build_mime_type_filter_cte
from src.db.rules.min_max_filter import build_min_max_filter_cte
from src.db.rules.path_filter import build_path_filter_cte
from src.db.rules.processed_items_filter import build_processed_items_filter_cte
from src.db.rules.types import (
    FilterType,
    MimeFilter,
    MinMaxFilter,
    PathFilter,
    ProcessedItemsFilter,
    RuleItemFilters,
)


def get_filter_builder(filter: FilterType):
    if isinstance(filter, MinMaxFilter):

        return build_min_max_filter_cte
    elif isinstance(filter, PathFilter):

        return build_path_filter_cte
    elif isinstance(filter, ProcessedItemsFilter):

        return build_processed_items_filter_cte
    elif isinstance(filter, MimeFilter):

        return build_mime_type_filter_cte
    else:
        raise NotImplementedError(
            f"Filter type {type(filter)} is not implemented"
        )


def build_chained_filters_cte(name_prefix: str, filters: List[FilterType]):
    ctes = []
    params = []
    last_name = None
    for i, filter in enumerate(filters):
        filter_builder = get_filter_builder(filter)
        cte_name = f"{name_prefix}_filter_{i}"
        cte, args = filter_builder(filter, last_name, cte_name)  # type: ignore
        ctes.append(cte)
        params.extend(args)
        last_name = cte_name
    return ctes, params, last_name


def build_independent_filters_cte(name_prefix: str, filters: List[FilterType]):
    ctes = []
    params = []
    cte_names = []
    for i, filter in enumerate(filters):
        filter_builder = get_filter_builder(filter)
        cte_name = f"{name_prefix}_filter_{i}"
        cte, args = filter_builder(filter, None, cte_name)  # type: ignore
        ctes.append(cte)
        params.extend(args)
        cte_names.append(cte_name)
    return ctes, params, cte_names


def build_query(positive: List[FilterType], negative: List[FilterType]):
    positive_ctes, positive_params, positive_last = build_chained_filters_cte(
        "positive",
        positive,
    )
    negative_ctes, negative_params, negative_cte_names = (
        build_independent_filters_cte(
            "negative",
            negative,
        )
    )
    negative_union = ""
    for i, cte_name in enumerate(negative_cte_names):
        if i == 0:
            negative_union = f"SELECT id FROM {cte_name}"
        else:
            negative_union = f"{negative_union} UNION SELECT id FROM {cte_name}"
    negative_union_clause = (
        f"""
        EXCEPT
        SELECT id
        FROM ({negative_union})
        """
        if negative_union
        else ""
    )
    query = f"""
        {', '.join(positive_ctes + negative_ctes)}
        WITH final_results AS (
            SELECT items.id
            FROM {positive_last}
            {negative_union_clause}
        )
        SELECT
            items.sha256,
            items.md5,
            items.type,
            items.size,
            items.time_added
        FROM items JOIN final_results
        ON items.id = final_results.id
    """
    return query, positive_params + negative_params
