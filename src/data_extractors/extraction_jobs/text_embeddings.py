import sqlite3
from typing import List, Sequence, Tuple

from src.data_extractors.extraction_jobs import run_extraction_job
from src.data_extractors.models import TextEmbeddingModel
from src.db.text_embeddings import (
    add_text_embedding,
    get_text_missing_embeddings,
)
from src.types import ItemWithPath


def run_text_embedding_extractor_job(
    conn: sqlite3.Connection, model_opt: TextEmbeddingModel
):
    model_opt.load_model()

    def get_item_text(item: ItemWithPath) -> List[Tuple[int, str]]:
        return get_text_missing_embeddings(
            conn, item.sha256, model_opt.data_type(), model_opt.setter_name()
        )

    def process_batch(
        batch: Sequence[Tuple[int, str]]
    ) -> List[Tuple[int, List[float]]]:
        embeddings = model_opt.run_batch_inference([text for _, text in batch])
        return [
            (text_id, embedding)
            for (text_id, _), embedding in zip(batch, embeddings)
        ]

    def handle_item_result(
        log_id: int,
        __: ItemWithPath,
        _: Sequence[Tuple[int, str]],
        embeddings: Sequence[Tuple[int, List[float]]],
    ):
        for text_id, embedding in embeddings:
            add_text_embedding(conn, text_id, log_id, embedding)

    def cleanup():
        model_opt.unload_model()

    return run_extraction_job(
        conn,
        model_opt,
        get_item_text,
        process_batch,
        handle_item_result,
        cleanup,
    )
