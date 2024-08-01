from src.db.rules.types import MimeFilter


def build_mime_type_filter_cte(
    filter: MimeFilter, filter_on: str | None, name: str
):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN
            {filter_on} ON items.id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""
    assert (
        filter.mime_type_prefixes
    ), "MimeFilter must have at least one mime type prefix"

    or_conditions = " OR ".join(
        ["items.type LIKE ? || '%'" for _ in filter.mime_type_prefixes]
    )
    cte = f"""
    WITH {name} AS (
        SELECT items.id
        FROM items
        {prev_cte_join_clause}
        WHERE ( {or_conditions} )
    )
    """
    return cte, filter.mime_type_prefixes
