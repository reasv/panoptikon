import sqlite3
from typing import Any, Dict, Sequence

from src.db.extracted_text import insert_extracted_text
from src.types import ItemWithPath


def handle_text(
    conn: sqlite3.Connection,
    log_id: int,
    item: ItemWithPath,
    text_results: Sequence[Dict[str, Any]],
):
    """
    Handle the extracted text from a text extraction model.

    Args:
        text_results: The extracted text results from the model.
    """
    string_set = set()
    for idx, text_result in enumerate(text_results):
        transcription: str | None = text_result.get("transcription", None)
        assert (
            transcription is not None
        ), "Text transcription should not be None"
        cleaned_string = transcription.strip()
        if len(cleaned_string) < 3:
            continue
        if cleaned_string.lower() in string_set:
            continue
        string_set.add(cleaned_string.lower())
        confidence = text_result.get("confidence")
        language = text_result.get("language")
        language_confidence = text_result.get("language_confidence")
        insert_extracted_text(
            conn,
            item.sha256,
            index=idx,
            log_id=log_id,
            text=cleaned_string,
            language=language,
            language_confidence=language_confidence,
            confidence=confidence,
        )
