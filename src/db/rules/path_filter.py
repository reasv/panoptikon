from src.db.rules.types import PathFilter


def build_path_filter_cte(filter: PathFilter, filter_on: str | None, name: str):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN
            {filter_on} ON items.id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""
    assert filter.path_prefixes, "PathFilter must have at least one path prefix"

    or_conditions = " OR ".join(
        ['files.path LIKE ? || "%"' for _ in filter.path_prefixes]
    )
    cte = f"""
    {name} AS (
        SELECT items.id
        FROM items
        {prev_cte_join_clause}
        JOIN files ON items.id = files.id
        WHERE {or_conditions}
        GROUP BY items.id
    )
    """
    return cte, filter.path_prefixes
