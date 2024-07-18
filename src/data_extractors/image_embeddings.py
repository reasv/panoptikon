import sqlite3
from typing import List, Sequence

import chromadb
import chromadb.api
import numpy as np
from chromadb.api import ClientAPI

from src.data_extractors.clip import CLIPEmbedder
from src.data_extractors.extractor_job import run_extractor_job
from src.data_extractors.images import item_image_extractor_np
from src.data_extractors.models import ImageEmbeddingModel
from src.data_extractors.utils import query_result_to_file_search_result
from src.types import ItemWithPath


def search_item_image_embeddings(
    conn: sqlite3.Connection,
    cdb: chromadb.api.ClientAPI,
    embedder: CLIPEmbedder,
    image_query: np.ndarray | None = None,
    text_query: str | None = None,
    allowed_types: List[str] | None = None,
    allowed_general_types: List[str] | None = None,
    limit: int = 10,
):
    model_opt = ImageEmbeddingModel(
        model_name=embedder.model_name,
        pretrained=embedder.pretrained,
    )
    collection = get_image_embeddings_collection(cdb, embedder)
    where_query = []
    if allowed_types:
        where_query.append({"type": {"$in": allowed_types}})
    if allowed_general_types:
        where_query.append({"general_type": {"$in": allowed_general_types}})

    results = collection.query(
        query_texts=text_query,
        query_images=image_query,
        n_results=limit,
        where={
            "$and": (
                [
                    {"setter": model_opt.setter_id()},
                ]
                + [{"$or": where_query}]
                if where_query
                else []
            )
        },  # type: ignore
    )

    return query_result_to_file_search_result(conn, results)


def get_image_embeddings_collection(
    cdb: ClientAPI, embedder: CLIPEmbedder | None = None
):
    collection_name = f"image_embeddings"
    try:
        collection = cdb.get_collection(
            name=collection_name, embedding_function=embedder
        )
    except ValueError:
        collection = cdb.create_collection(
            name=collection_name, embedding_function=embedder
        )

    return collection


def run_image_embedding_extractor_job(
    conn: sqlite3.Connection, cdb: ClientAPI, model_opt: ImageEmbeddingModel
):
    embedder = CLIPEmbedder(
        model_name=model_opt.model_name(),
        pretrained=model_opt.model_checkpoint(),
        batch_size=64,
    )
    embedder.load_model()
    collection = get_image_embeddings_collection(cdb)

    def process_batch(batch: Sequence[np.ndarray]):
        return embedder.get_image_embeddings(batch)

    def handle_item_result(
        item: ItemWithPath,
        inputs: Sequence[np.ndarray],
        embeddings: Sequence[np.ndarray],
    ):
        embeddings_list = [embedding.tolist() for embedding in embeddings]
        collection.add(
            ids=[
                f"{item.sha256}-{i}-{model_opt.setter_id()}"
                for i, _ in enumerate(embeddings)
            ],
            embeddings=embeddings_list,
            images=list(inputs),
            metadatas=(
                [
                    {
                        "item": item.sha256,
                        "setter": model_opt.setter_id(),
                        "model": model_opt.model_name(),
                        "type": item.type,
                        "general_type": item.type.split("/")[0],
                    }
                    for _ in embeddings
                ]
            ),
        )

    return run_extractor_job(
        conn,
        model_opt.setter_id(),
        model_opt.batch_size(),
        item_image_extractor_np,
        process_batch,
        handle_item_result,
    )
