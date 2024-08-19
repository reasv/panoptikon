import sqlite3
from typing import Any, Dict, Sequence

from panoptikon.db.extracted_text import add_extracted_text
from panoptikon.db.extraction_log import add_item_data
from panoptikon.types import ItemData


def handle_text(
    conn: sqlite3.Connection,
    job_id: int,
    setter_name: str,
    item: ItemData,
    text_results: Sequence[Dict[str, Any]],
):
    """
    Handle the extracted text from a text extraction model.

    Args:
        text_results: The extracted text results from the model.
    """
    string_set = set()
    data_ids = []
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

        data_id = add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text",
            index=idx,
        )
        data_ids.append(data_id)
        add_extracted_text(
            conn,
            data_id=data_id,
            text=cleaned_string,
            language=language,
            language_confidence=language_confidence,
            confidence=confidence,
        )
    if len(data_ids) == 0:
        # Add a dummy item_data entry to indicate that the item was processed
        # but no text was extracted
        add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text",
            index=0,
            is_placeholder=True,
        )
    return data_ids
