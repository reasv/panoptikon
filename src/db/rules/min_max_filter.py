from src.db.rules.types import MinMaxFilter


def build_min_max_filter_cte(
    filter: MinMaxFilter, filter_on: str | None, name: str
):
    if filter.max_value:
        less_than_clause = f"AND items_data.{filter.column_name} <= ?"
    else:
        less_than_clause = ""

    if filter_on:
        prev_cte_join_clause = f"""
        JOIN
            {filter_on} ON items.id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""

    cte = f"""
    {name} AS (
        SELECT items.id
        FROM (
            SELECT 
                CASE 
                    WHEN height > width THEN height 
                    ELSE width 
                END AS largest_dimension,
                CASE 
                    WHEN height < width THEN height 
                    ELSE width 
                END AS smallest_dimension,
                items.*
            FROM 
                items
            {prev_cte_join_clause}
        ) AS items_data
        WHERE items_data.{filter.column_name} >= ?
        {less_than_clause}
    )
    """
    return cte, (
        [filter.min_value, filter.max_value]
        if filter.max_value
        else [filter.min_value]
    )
