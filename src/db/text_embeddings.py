import sqlite3
from typing import List

from src.db.utils import serialize_f32, trigger_exists


def add_text_embedding(
    conn: sqlite3.Connection, text_id: int, embedding: List[float]
):
    cursor = conn.cursor()
    embedding_bytes = serialize_f32(embedding)
    cursor.execute(
        "INSERT INTO extracted_text_embed (id, sentence_embedding) VALUES (?, ?)",
        (text_id, embedding_bytes),
    )
    assert cursor.lastrowid is not None, "Last row ID is None"


def create_text_embeddings_table(
    conn: sqlite3.Connection, embedding_size: int = 768
):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS extracted_text_embed USING vec0(
            id INTEGER PRIMARY KEY,
            sentence_embedding FLOAT[{embedding_size}]
        );
        """
    )
    if trigger_exists(conn, "extracted_text_ad_embed"):
        cursor.execute("DROP TRIGGER extracted_text_ad_embed")
    # Create triggers to keep the extracted_text_embed table up to date
    cursor.execute(
        """
        CREATE TRIGGER extracted_text_ad_embed AFTER DELETE ON extracted_text BEGIN
            DELETE FROM extracted_text_embed WHERE id = old.id;
        END;
        """
    )
