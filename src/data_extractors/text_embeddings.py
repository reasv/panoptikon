import sqlite3
from typing import List

from chromadb.api import ClientAPI

from src.data_extractors.models import ModelOpts
from src.data_extractors.utils import (
    ExtractedText,
    query_result_to_file_search_result,
)
from src.types import ItemWithPath


def get_text_collection(cdb: ClientAPI):
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(name=collection_name)
    except ValueError:
        collection = cdb.create_collection(name=collection_name)
    return collection


def add_item_text(
    cdb: ClientAPI,
    item: ItemWithPath,
    model: ModelOpts,
    language: str,
    text: str,
):
    text = text.strip()
    if len(text) < 3:
        return
    collection = get_text_collection(cdb)
    collection.upsert(
        ids=[f"{item.sha256}-{model.model_type()}-{model.setter_id()}"],
        documents=[text],
        metadatas=[
            {
                "item": item.sha256,
                "source": model.model_type(),
                "setter": model.setter_id(),
                "language": language,
                "type": item.type,
                "general_type": item.type.split("/")[0],
            }
        ],
    )


def search_item_text(
    conn: sqlite3.Connection,
    cdb: ClientAPI,
    text_query: str,
    semantic_search: bool = False,
    full_text_search: bool = False,
    allowed_sources: List[str] | None = None,
    allowed_setters: List[str] | None = None,
    allowed_types: List[str] | None = None,
    allowed_general_types: List[str] | None = None,
    allowed_languages: List[str] | None = None,
    limit: int = 10,
):
    collection = get_text_collection(cdb)

    where_query = []
    if allowed_sources:
        where_query.append({"source": {"$in": allowed_sources}})
    if allowed_setters:
        where_query.append({"setter": {"$in": allowed_setters}})
    if allowed_types or allowed_general_types:
        types = []
        if allowed_types:
            types.append({"type": {"$in": allowed_types}})
        if allowed_general_types:
            types.append({"general_type": {"$in": allowed_general_types}})
        where_query.append({"$or": types})

    if allowed_languages:
        where_query.append({"language": {"$in": allowed_languages}})

    results = collection.query(
        query_texts=text_query if semantic_search else None,
        where_document={"$contains": text_query} if full_text_search else None,
        where={"$and": where_query} if where_query else None,
        n_results=limit,
    )

    return query_result_to_file_search_result(conn, results)


def delete_all_text_from_model(cdb: ClientAPI, model_opt: ModelOpts):
    collection = get_text_collection(cdb)

    collection.delete(
        where={
            "$and": [
                {"setter": model_opt.setter_id()},
                {"source": model_opt.model_type()},
            ]
        }
    )
