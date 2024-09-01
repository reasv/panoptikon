import logging
import os
import sqlite3
import time
from typing import List, Literal, Optional, Tuple

from panoptikon.db.files import get_existing_file_for_sha256
from panoptikon.db.utils import serialize_f32
from panoptikon.types import FileSearchResult, OutputDataType

logger = logging.getLogger(__name__)

def add_embedding(
    conn: sqlite3.Connection,
    data_id: int,
    data_type: OutputDataType,
    embedding: List[float],
) -> int:
    """
    Insert image embedding into the database
    """
    embedding_bytes = serialize_f32(embedding)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO embeddings
            (id, embedding)
        SELECT item_data.id, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = ?
    """,
        (embedding_bytes, data_id, data_type),
    )

    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def find_similar_items(
    conn: sqlite3.Connection,
    sha256: str,
    setter_name: str,
    src_setter_names: Optional[List[str]] = None,
    src_languages: Optional[List[str]] = None,
    src_min_text_length: int = 0,
    src_min_confidence: float = 0.0,
    src_min_language_confidence: float = 0.0,
    clip_xmodal: bool = False,
    xmodal_t2t: bool = True,
    xmodal_i2i: bool = True,
    distance_aggregation_func: Literal["MIN", "MAX", "AVG"] = "MIN",
    confidence_weight: float = 0,
    language_confidence_weight: float = 0,
    page_size: int = 10,
    page_number: int = 1,
    full_count: bool = False,
) -> Tuple[List[FileSearchResult], Optional[int]]:
    if page_number < 1:
        page_number = 1
    offset = (page_number - 1) * page_size

    # Step 1: Retrieve item_id, setter_id, and data_type from the provided sha256 and setter_name
    query = """
    SELECT 
        items.id AS item_id,
        setters.id AS setter_id,
        item_data.data_type AS data_type
    FROM items
    JOIN item_data ON items.id = item_data.item_id
    JOIN setters ON item_data.setter_id = setters.id
    WHERE items.sha256 = ? AND setters.name = ?
    LIMIT 1;
    """
    cursor = conn.execute(query, (sha256, setter_name))
    result = cursor.fetchone()
    
    if not result:
        return [], None  # No item or setter found, return empty list and None for the count

    item_id, main_setter_id, data_type = result

    # Check if cross-comparison is enabled and retrieve the text embedding setter_id
    text_setter_id = None
    if clip_xmodal:
        tclip_setter_name = f"t{setter_name}"
        cursor = conn.execute(
            "SELECT id FROM setters WHERE name = ? LIMIT 1;",
            (tclip_setter_name,),
        )
        text_setter_id_result = cursor.fetchone()
        if text_setter_id_result:
            text_setter_id = text_setter_id_result[0]

    # Step 2: Prepare the filtering clauses
    main_setter_ids_clause = ""
    other_setter_ids_clause = ""
    parameters = [item_id, main_setter_id]  # Base parameters for the query

    if src_setter_names:
        placeholder = ",".join("?" for _ in src_setter_names)
        query = f"SELECT id FROM setters WHERE name IN ({placeholder})"
        cursor = conn.execute(query, tuple(src_setter_names))
        setter_ids = [row[0] for row in cursor.fetchall()]

        if setter_ids:
            setter_ids_placeholder = ",".join("?" for _ in setter_ids)
            main_setter_ids_clause = f"""
            JOIN item_data AS derived_main_item_data
                ON main_item_data.source_id = derived_main_item_data.id
                AND derived_main_item_data.setter_id IN ({setter_ids_placeholder})
            """
            parameters.extend(setter_ids)

            other_setter_ids_clause = f"""
            JOIN item_data AS derived_other_item_data
                ON other_item_data.source_id = derived_other_item_data.id
                AND derived_other_item_data.setter_id IN ({setter_ids_placeholder})
            """
            parameters.extend(setter_ids)
    else:
        setter_ids = []

    extracted_text_clause = ""
    if src_languages or src_min_text_length > 0:
        extracted_text_clause = f"""
            JOIN extracted_text AS main_source_text
                ON main_item_data.source_id = main_source_text.id
            JOIN extracted_text AS other_source_text
                ON other_item_data.source_id = other_source_text.id
        """
        if src_languages:
            lang_placeholder = ",".join("?" for _ in src_languages)
            extracted_text_clause += (
                f" AND main_source_text.language IN ({lang_placeholder})"
            )
            parameters.extend(src_languages)
            extracted_text_clause += (
                f" AND other_source_text.language IN ({lang_placeholder})"
            )
            parameters.extend(src_languages)
        if src_min_text_length > 0:
            extracted_text_clause += " AND main_source_text.text_length >= ?"
            parameters.append(src_min_text_length)
            extracted_text_clause += " AND other_source_text.text_length >= ?"
            parameters.append(src_min_text_length)
        if src_min_confidence > 0:
            extracted_text_clause += " AND main_source_text.confidence >= ?"
            parameters.append(src_min_confidence)
            extracted_text_clause += " AND other_source_text.confidence >= ?"
            parameters.append(src_min_confidence)
        if src_min_language_confidence > 0:
            extracted_text_clause += (
                " AND main_source_text.language_confidence >= ?"
            )
            parameters.append(src_min_language_confidence)
            extracted_text_clause += (
                " AND other_source_text.language_confidence >= ?"
            )
            parameters.append(src_min_language_confidence)

    # Step 3: Choose the appropriate distance function based on the data_type
    if data_type == "clip":
        distance_function = "vec_distance_cosine(vec_normalize(other_embeddings.embedding), vec_normalize(main_embeddings.embedding))"
    else:
        distance_function = "vec_distance_L2(other_embeddings.embedding, main_embeddings.embedding)"

    order_by_clause = f"{distance_aggregation_func}({distance_function})"

    confidence_weight_clause = f"POW((COALESCE(main_source_text.confidence, 1) * COALESCE(other_source_text.confidence, 1)), {confidence_weight})"
    language_confidence_weight_clause = f"POW((COALESCE(main_source_text.language_confidence, 1) * COALESCE(other_source_text.language_confidence, 1)), {language_confidence_weight})"

    if confidence_weight > 0 and language_confidence_weight > 0:
        weights = f"({confidence_weight_clause} * {language_confidence_weight_clause})"
        # Normalize the distance function by the sum of the weights
        order_by_clause = f"(SUM({distance_function} * {weights}) / SUM ({weights}))"
    elif confidence_weight > 0:
        order_by_clause = f"(SUM({distance_function} * {confidence_weight_clause}) / SUM ({confidence_weight_clause}))"
    elif language_confidence_weight > 0:
        order_by_clause = f"(SUM({distance_function} * {language_confidence_weight_clause}) / SUM ({language_confidence_weight_clause}))"

    if clip_xmodal and text_setter_id:
        if not xmodal_t2t:
            remove_text_to_text_condition = "AND (main_item_data.data_type != 'text-embedding' OR other_item_data.data_type != 'text-embedding')"
        else:
            remove_text_to_text_condition = ""
        if not xmodal_i2i:
            remove_image_to_image_condition = "AND (main_item_data.data_type != 'clip' OR other_item_data.data_type != 'clip')"
        else:
            remove_image_to_image_condition = ""

        base_query = f"""
        FROM embeddings AS main_embeddings
        JOIN item_data AS main_item_data
            ON main_embeddings.id = main_item_data.id
            AND main_item_data.item_id = ?
            AND (main_item_data.setter_id = ? OR main_item_data.setter_id = ?)
        LEFT JOIN item_data AS derived_main_item_data
            ON main_item_data.source_id = derived_main_item_data.id
            {"AND derived_main_item_data.setter_id IN (" + ",".join("?" for _ in src_setter_names) + ")" if src_setter_names else ""}
        LEFT JOIN extracted_text AS main_source_text
            ON main_item_data.source_id = main_source_text.id
            {"AND main_source_text.language IN (" + ",".join("?" for _ in src_languages) + ")" if src_languages else ""}
            {"AND main_source_text.text_length >= ?" if src_min_text_length > 0 else ""}
            {"AND main_source_text.confidence >= ?" if src_min_confidence > 0 else ""}
            {"AND main_source_text.language_confidence >= ?" if src_min_language_confidence > 0 else ""}
        JOIN item_data AS other_item_data
            ON other_item_data.item_id != main_item_data.item_id
            AND (other_item_data.setter_id = ? OR other_item_data.setter_id = ?)
        LEFT JOIN item_data AS derived_other_item_data
            ON other_item_data.source_id = derived_other_item_data.id
            {"AND derived_other_item_data.setter_id IN (" + ",".join("?" for _ in src_setter_names) + ")" if src_setter_names else ""}
        LEFT JOIN extracted_text AS other_source_text
            ON other_item_data.source_id = other_source_text.id
            {"AND other_source_text.language IN (" + ",".join("?" for _ in src_languages) + ")" if src_languages else ""}
            {"AND other_source_text.text_length >= ?" if src_min_text_length > 0 else ""}
            {"AND other_source_text.confidence >= ?" if src_min_confidence > 0 else ""}
            {"AND other_source_text.language_confidence >= ?" if src_min_language_confidence > 0 else ""}
        JOIN embeddings AS other_embeddings
            ON other_item_data.id = other_embeddings.id
            AND other_embeddings.id != main_embeddings.id
        JOIN items ON other_item_data.item_id = items.id
        JOIN files ON files.item_id = items.id
        WHERE (
                (
                    (derived_main_item_data.id IS NOT NULL AND main_source_text.id IS NOT NULL)
                    OR
                    (main_item_data.data_type = 'clip')
                )
                AND
                (
                    (derived_other_item_data.id IS NOT NULL AND other_source_text.id IS NOT NULL)
                    OR
                    (other_item_data.data_type = 'clip')
                )
                {remove_text_to_text_condition}
                {remove_image_to_image_condition}
            )
        """

        query = f"""
        SELECT 
            files.path AS path,
            items.sha256 AS sha256,
            files.last_modified,
            items.type AS type
        {base_query}
        GROUP BY other_item_data.item_id
        ORDER BY {order_by_clause} ASC
        LIMIT ? OFFSET ?;
        """

        count_query = f"SELECT COUNT(DISTINCT other_item_data.item_id) {base_query}"

        parameters = [item_id, main_setter_id, text_setter_id]
        src_parameters = []
        if src_setter_names:
            src_parameters = src_parameters + setter_ids
        if src_languages:
            src_parameters = src_parameters + src_languages
        if src_min_text_length > 0:
            src_parameters = src_parameters + [src_min_text_length]
        if src_min_confidence > 0:
            src_parameters = src_parameters + [src_min_confidence]
        if src_min_language_confidence > 0:
            src_parameters = src_parameters + [src_min_language_confidence]
        parameters = parameters + src_parameters
        parameters = parameters + [main_setter_id, text_setter_id]
        parameters = parameters + src_parameters
        parameters = parameters + [page_size, offset]

    else:
        require_text_left_join = False  # Flag to determine if we need to left join the extracted_text table
        if (confidence_weight > 0 or language_confidence_weight > 0) and not extracted_text_clause:
            # If confidence_weight or language_confidence_weight is set, we need to left join the extracted_text table
            # If the extracted_text_clause is set, then we have already joined the extracted_text table
            require_text_left_join = True

        base_query = f"""
        FROM embeddings AS main_embeddings
        JOIN item_data AS main_item_data
            ON main_embeddings.id = main_item_data.id
            AND main_item_data.item_id = ?
            AND main_item_data.setter_id = ?
        {main_setter_ids_clause}
        JOIN item_data AS other_item_data
            ON other_item_data.item_id != main_item_data.item_id
            AND other_item_data.setter_id = main_item_data.setter_id
        {other_setter_ids_clause}
        {extracted_text_clause}
        JOIN embeddings AS other_embeddings
            ON other_item_data.id = other_embeddings.id
            AND other_embeddings.id != main_embeddings.id
        {"""
        LEFT JOIN extracted_text AS main_source_text
            ON main_item_data.source_id = main_source_text.id
        LEFT JOIN extracted_text AS other_source_text
            ON other_item_data.source_id = other_source_text.id
        """
         if require_text_left_join
         else ""}
        JOIN items ON other_item_data.item_id = items.id
        JOIN files ON files.item_id = items.id
        """

        query = f"""
        SELECT 
            files.path AS path,
            items.sha256 AS sha256,
            files.last_modified,
            items.type AS type
        {base_query}
        GROUP BY other_item_data.item_id
        ORDER BY {order_by_clause} ASC
        LIMIT ? OFFSET ?;
        """
        count_query = f"SELECT COUNT(DISTINCT other_item_data.item_id) {base_query}"

        parameters.extend([page_size, offset])

    # Step 5: Execute the count query if full_count is True
    total_count = None
    if full_count:
        start_time = time.time()
        cursor = conn.execute(count_query, tuple(parameters[:-2]))  # Exclude LIMIT and OFFSET parameters for the count query
        total_count = cursor.fetchone()[0]
        logger.debug(f"Count query took {time.time() - start_time:.2f} seconds")

    # Step 6: Execute the main query
    cursor = conn.execute(query, tuple(parameters))

    # Step 7: Fetch results and create a list of FileSearchResult objects
    results = cursor.fetchall()
    file_search_results = [
        FileSearchResult(
            path=row[0], sha256=row[1], last_modified=row[2], type=row[3]
        )
        for row in results
    ]
    existing_results = []
    for result in file_search_results:
        if not os.path.exists(result.path):
            file_record = get_existing_file_for_sha256(conn, result.sha256)
            if file_record:
                result.path = file_record.path
                result.last_modified = file_record.last_modified
                existing_results.append(result)
        else:
            existing_results.append(result)

    return existing_results, total_count
