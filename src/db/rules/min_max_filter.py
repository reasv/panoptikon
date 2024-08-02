from src.db.rules.types import MinMaxFilter


def build_min_max_filter_cte(
    filter: MinMaxFilter, filter_on: str | None, name: str
):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN
            {filter_on} ON items.id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""

    if filter.max_value == filter.min_value:
        where_conditions = [f"items_data.{filter.column_name} = ?"]
        params = [filter.min_value]
    else:
        where_conditions = [f"items_data.{filter.column_name} >= ?"]
        params = [filter.min_value]
        if filter.max_value:
            where_conditions.append(f"items_data.{filter.column_name} <= ?")
            params.append(filter.max_value)

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
        WHERE {' AND '.join(where_conditions)}
    )
    """
    return cte, params
