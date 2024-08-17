from src.db.rules.types import ProcessedExtractedDataFilter


def build_processed_extracted_data_filter_cte(
    filter: ProcessedExtractedDataFilter, filter_on: str | None, name: str
):
    if filter_on:
        prev_cte_join_clause = f"""
        JOIN {filter_on} ON ie.item_id = {filter_on}.id
        """
    else:
        prev_cte_join_clause = ""

    target_type_conditions = ", ".join(["?" for _ in filter.data_types])
    cte = f"""
    {name} AS (
        SELECT DISTINCT ie.item_id AS id
        FROM items_extractions AS ie
        {prev_cte_join_clause}
        WHERE ie.data_type IN ({target_type_conditions})
        AND NOT EXISTS (
            SELECT 1
            FROM items_extractions AS ie2
            JOIN setters AS s2 ON ie2.setter_id = s2.id
            WHERE ie2.source_extraction_id = ie.id
            AND s2.name = ?
        )
    )
    """
    return cte, [*filter.data_types, filter.setter_name]
