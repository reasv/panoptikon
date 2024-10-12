import sqlite3
from typing import List, Optional, Tuple

from panoptikon.types import ExtractedText, ExtractedTextStats


def add_extracted_text(
    conn: sqlite3.Connection,
    data_id: int,
    text: str,
    language: str | None,
    language_confidence: float | None,
    confidence: float | None,
) -> int:
    """
    Insert extracted text into the database
    """

    confidence = round(float(confidence), 4) if confidence is not None else None
    language_confidence = (
        round(float(language_confidence), 4)
        if language_confidence is not None
        else None
    )
    text_length = len(text)  # Calculate the length of the text

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO extracted_text
            (id, language, language_confidence, confidence, text, text_length)
        SELECT item_data.id, ?, ?, ?, ?, ?
        FROM item_data
        WHERE item_data.id = ?
        AND item_data.data_type = 'text'
        """,
        (
            language,
            language_confidence,
            confidence,
            text,
            text_length,  # Include the text length in the insert
            data_id,
        ),
    )
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def get_extracted_text_for_item(
    conn: sqlite3.Connection,
    item_id: int,
    max_length: Optional[int] = None,
) -> List[ExtractedText]:
    """
    Get all extracted text for an item, with an optional maximum text length.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            items.sha256,
            setters.name,
            language,
            text,
            confidence,
            language_confidence,
            text_length,
            extracted_text.id
        FROM extracted_text
        JOIN item_data
        ON extracted_text.id = item_data.id
        JOIN setters AS setters
        ON item_data.setter_id = setters.id
        JOIN items ON item_data.item_id = items.id
        WHERE item_data.item_id = ?
        ORDER BY setters.name, item_data.source_id, item_data.idx
    """,
        (item_id,),
    )
    rows = cursor.fetchall()

    # Map each row to an ExtractedText dataclass instance
    extracted_texts = []
    for row in rows:
        original_text = row[3]
        original_length = len(original_text)
        if max_length is not None and original_length > max_length:
            text = original_text[:max_length]
        else:
            text = original_text

        extracted_text = ExtractedText(
            item_sha256=row[0],
            setter_name=row[1],
            language=row[2],
            text=text,
            confidence=row[4],
            language_confidence=row[5],
            length=row[6],
            id=row[7],
        )
        extracted_texts.append(extracted_text)

    return extracted_texts


def get_text_by_ids(
    conn: sqlite3.Connection, text_ids: List[int]
) -> List[Tuple[int, ExtractedText]]:
    """
    Get extracted text by their IDs
    """
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            items.sha256,
            setters.name,
            language,
            text,
            confidence,
            language_confidence,
            extracted_text.id
        FROM extracted_text
        JOIN item_data
            ON extracted_text.id = item_data.id
        JOIN setters AS setters
            ON item_data.setter_id = setters.id
        JOIN items
            ON item_data.item_id = items.id
        WHERE extracted_text.id IN ({', '.join('?' * len(text_ids))})
    """,
        text_ids,
    )
    rows = cursor.fetchall()

    # Map each row to an ExtractedText dataclass instance
    extracted_texts = [
        (
            row[6],
            ExtractedText(
                id=row[6],
                item_sha256=row[0],
                setter_name=row[1],
                language=row[2],
                text=row[3],
                confidence=row[4],
                language_confidence=row[5],
                length=len(row[3]),
            ),
        )
        for row in rows
    ]
    return extracted_texts


def get_text_stats(conn: sqlite3.Connection) -> ExtractedTextStats:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT language
        FROM extracted_text
        WHERE language IS NOT NULL
        """
    )
    rows = cursor.fetchall()
    languages = [row[0] for row in rows]
    # Get the minimum overall language confidence and the minimum confidence

    cursor.execute(
        """
        SELECT MIN(language_confidence), MIN(confidence)
        FROM extracted_text
        """
    )
    row = cursor.fetchone()
    language_confidence = row[0]
    confidence = row[1]
    return ExtractedTextStats(
        languages=languages,
        lowest_language_confidence=language_confidence,
        lowest_confidence=confidence,
    )
