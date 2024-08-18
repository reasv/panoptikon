import sqlite3
from typing import Sequence

from src.data_extractors.data_handlers.utils import deserialize_array
from src.db.embeddings import add_embedding
from src.db.extraction_log import add_item_data
from src.types import ItemData


def handle_clip(
    conn: sqlite3.Connection,
    job_id: int,
    setter_name: str,
    item: ItemData,
    embeddings: Sequence[bytes],
):
    data_ids = []
    for idx, embedding_buf in enumerate(embeddings):
        embedding = deserialize_array(embedding_buf).tolist()
        data_id = add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="clip",
            index=idx,
        )
        add_embedding(conn, data_id, "clip", embedding)
        data_ids.append(data_id)
    return data_ids
