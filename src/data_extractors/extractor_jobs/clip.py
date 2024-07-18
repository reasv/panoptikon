import sqlite3
from typing import Sequence

import numpy as np
from chromadb.api import ClientAPI

from src.data_extractors.ai.clip import CLIPEmbedder
from src.data_extractors.data_loaders.images import item_image_loader_numpy
from src.data_extractors.extractor_jobs import run_extractor_job
from src.data_extractors.image_embeddings import add_item_image_embeddings
from src.data_extractors.models import ImageEmbeddingModel
from src.types import ItemWithPath


def run_image_embedding_extractor_job(
    conn: sqlite3.Connection, cdb: ClientAPI, model_opt: ImageEmbeddingModel
):
    embedder = CLIPEmbedder(
        model_name=model_opt.clip_model_name(),
        pretrained=model_opt.clip_model_checkpoint(),
        batch_size=model_opt.batch_size(),
    )
    embedder.load_model()

    def process_batch(batch: Sequence[np.ndarray]):
        return embedder.get_image_embeddings(batch)

    def handle_item_result(
        item: ItemWithPath,
        inputs: Sequence[np.ndarray],
        embeddings: Sequence[np.ndarray],
    ):
        embeddings_list = [embedding.tolist() for embedding in embeddings]
        add_item_image_embeddings(cdb, model_opt, item, inputs, embeddings_list)

    return run_extractor_job(
        conn,
        model_opt.setter_id(),
        model_opt.batch_size(),
        item_image_loader_numpy,
        process_batch,
        handle_item_result,
    )
