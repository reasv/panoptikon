import sqlite3
from typing import List

from src.db import get_item_id
from src.types import ExtractedText


def insert_extracted_text(
    conn: sqlite3.Connection,
    item_sha256: str,
    log_id: int,
    text: str,
    language: str | None,
    language_confidence: float | None,
    confidence: float | None,
) -> int:
    """
    Insert extracted text into the database
    """
    text = text.strip()
    if len(text) < 3:
        return -1

    item_id = get_item_id(conn, item_sha256)
    assert item_id is not None, f"Item with SHA256 {item_sha256} not found"

    cursor = conn.cursor()

    sql = """
    INSERT INTO extracted_text (item_id, log_id, language, language_confidence, confidence, text)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cursor.execute(
        sql, (item_id, log_id, language, language_confidence, confidence, text)
    )
    assert cursor.lastrowid is not None, "Last row ID is None"
    return cursor.lastrowid


def get_extracted_text_for_item(
    conn: sqlite3.Connection, item_sha256: str
) -> List[ExtractedText]:
    """
    Get all extracted text for an item
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            items.sha256 as item_sha256,
            log.type as model_type,
            log.setter,
            language,
            text,
            confidence
        FROM extracted_text
        JOIN data_extraction_log AS log
        ON extracted_text.log_id = log.id
        JOIN items ON extracted_text.item_id = items.id
        WHERE items.sha256 = ?
    """,
        (item_sha256,),
    )
    rows = cursor.fetchall()

    # Map each row to an ExtractedText dataclass instance
    extracted_texts = [
        ExtractedText(
            item_sha256=row[0],
            model_type=row[1],
            setter=row[2],
            language=row[3],
            text=row[4],
            confidence=row[5],
            score=0,
        )
        for row in rows
    ]
    return extracted_texts


def delete_text_extracted_by_setter(
    conn: sqlite3.Connection, model_type: str, setter: str
):
    cursor = conn.cursor()
    cursor.execute(
        """
    DELETE FROM extracted_text
    WHERE log_id IN (
        SELECT data_extraction_log.id
        FROM data_extraction_log
        WHERE setter = ?
        AND type = ?
    )
    """,
        (model_type, setter),
    )
