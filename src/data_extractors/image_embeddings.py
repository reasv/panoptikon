import sqlite3
from typing import List, Sequence

import chromadb
import chromadb.api
import numpy as np
from chromadb.api import ClientAPI

from src.data_extractors.ai.clip import CLIPEmbedder
from src.data_extractors.models import ImageEmbeddingModel, ModelOpts
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
    setter_id = ImageEmbeddingModel._model_to_setter_id(
        embedder.model_name, embedder.pretrained
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
                    {"setter": setter_id},
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


def add_item_image_embeddings(
    cdb: ClientAPI,
    model_opt: ImageEmbeddingModel,
    item: ItemWithPath,
    inputs: Sequence[np.ndarray],
    embeddings: List[np.ndarray],
):
    collection = get_image_embeddings_collection(cdb)
    collection.upsert(
        ids=[
            f"{item.sha256}-{i}-{model_opt.setter_id()}"
            for i, _ in enumerate(embeddings)
        ],
        embeddings=embeddings,
        images=list(inputs),
        metadatas=(
            [
                {
                    "item": item.sha256,
                    "source": model_opt.model_type(),
                    "setter": model_opt.setter_id(),
                    "language": "None",
                    "type": item.type,
                    "general_type": item.type.split("/")[0],
                }
                for _ in embeddings
            ]
        ),
    )


def delete_all_embeddings_from_model(cdb: ClientAPI, model_opt: ModelOpts):
    collection = get_image_embeddings_collection(cdb)

    collection.delete(
        where={
            "$and": [
                {"setter": model_opt.setter_id()},
                {"source": model_opt.model_type()},
            ]
        }
    )
