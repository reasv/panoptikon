from src.db.search.inner_query import build_inner_query
from src.db.search.types import (
    InnerQueryParams,
    InnerQueryTagFilters,
    QueryParams,
)


def build_search_query(
    args: QueryParams,
):
    tags = args.tags
    filters = args.filters
    if tags.pos_match_any and not tags.pos_match_all:
        # If "match any" tags are provided,
        # but no positive match all tags are provided
        # We need to build a query to match on *any* of them being present
        main_query, params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagFilters(
                    positive=tags.pos_match_any,
                    negative=tags.neg_match_any,
                    all_setters_required=False,
                    any_positive_tags_match=True,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )
    else:
        # Basic case where we need to match all positive tags and none of the negative tags
        # There might even be no tags at all in this case
        main_query, params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagFilters(
                    positive=tags.pos_match_all,
                    negative=tags.neg_match_any,
                    any_positive_tags_match=False,
                    all_setters_required=tags.all_setters_required,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )

    if tags.pos_match_any and tags.pos_match_all:
        # If tags "match any" are provided along with match all regular positive tags
        # We need to build a separate query to match on *any* of them being present
        # And then intersect the results with the main query
        any_tags_query, any_tags_params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagFilters(
                    positive=tags.pos_match_any,
                    negative=[],
                    any_positive_tags_match=True,
                    all_setters_required=False,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                ),
                filters=filters,
            )
        )

        # Append the tags query to the main query
        main_query = f"""
        {main_query}
        INTERSECT
        {any_tags_query}
        """
        params += any_tags_params

    if tags.neg_match_all:
        # If negative tags "match all" are provided
        # We need to build a separate query to match on *all* of them being present
        # And then exclude the results from the main query

        negative_tags_query, negative_tags_params = build_inner_query(
            InnerQueryParams(
                tags=InnerQueryTagFilters(
                    positive=tags.neg_match_all,
                    negative=[],
                    any_positive_tags_match=False,
                    namespaces=tags.namespaces,
                    min_confidence=tags.min_confidence,
                    setters=tags.setters,
                    all_setters_required=tags.all_setters_required,
                ),
                filters=filters,
            )
        )

        # Append the negative tags query to the main query
        if tags.pos_match_any and tags.pos_match_all:
            # If we already have an INTERSECT query, we need to use it as a subquery
            main_query = f"""
            SELECT *
            FROM (
                {main_query}
            )
            EXCEPT
            {negative_tags_query}
            """
        else:
            main_query = f"""
            {main_query}
            EXCEPT
            {negative_tags_query}
            """
        params += negative_tags_params

    return main_query, params
