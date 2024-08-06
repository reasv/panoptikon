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

    target_type_conditions = " OR ".join(
        [f"s.type = ?" for _ in range(len(filter.data_types))]
    )
    cte = f"""
    {name} AS (
        SELECT DISTINCT ie.item_id AS id
        FROM items_extractions AS ie
        {prev_cte_join_clause}
        JOIN setters AS s ON ie.setter_id = s.id
        WHERE ({target_type_conditions})
        AND NOT EXISTS (
            SELECT 1
            FROM items_extractions AS ie2
            JOIN setters AS s2 ON ie2.setter_id = s2.id
            WHERE ie2.source_extraction_id = ie.id
            AND s2.type = ?
            AND s2.name = ?
        )
    )
    """
    return cte, [*filter.data_types, filter.setter_type, filter.setter_name]
