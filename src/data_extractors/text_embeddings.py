import os
import sqlite3
from cgitb import text
from dataclasses import dataclass
from typing import Dict, List, Tuple

from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import QueryResult
from chromadb.types import Metadata
from numpy import where

from src.data_extractors.models import ModelOption
from src.db import FileSearchResult, get_existing_file_for_sha256
from src.types import ItemWithPath


def get_chromadb_client() -> ClientAPI:
    sqlite_db_file = os.getenv("DB_FILE", "./db/sqlite.db")
    cdb_file = f"{sqlite_db_file}.chromadb"
    return PersistentClient(path=cdb_file)


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
    model: ModelOption,
    language: str,
    text: str,
):
    if len(text) == 0:
        return
    collection = get_text_collection(cdb)
    collection.upsert(
        ids=[f"{item.sha256}-{model.setter_id()}"],
        documents=[text],
        metadatas=[
            {
                "item": item.sha256,
                "source": model.model_type(),
                "model": model.model_name(),
                "setter": model.setter_id(),
                "language": language,
                "type": item.type,
                "general_type": item.type.split("/")[0],
            }
        ],
    )


@dataclass
class ExtractedText:
    item: str
    source: str
    model: str
    setter: str
    language: str
    type: str
    general_type: str
    text: str
    score: float


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
    if allowed_types:
        where_query.append({"type": {"$in": allowed_types}})
    if allowed_general_types:
        where_query.append({"general_type": {"$in": allowed_general_types}})
    if allowed_languages:
        where_query.append({"language": {"$in": allowed_languages}})

    results = collection.query(
        query_texts=text_query if semantic_search else None,
        where_document={"$contains": text_query} if full_text_search else None,
        where={"$and": where_query} if where_query else None,
        n_results=limit,
    )

    items: Dict[str, List[ExtractedText]] = {}
    for extracted_text in process_result_single_query(results):
        if extracted_text.item not in items:
            items[extracted_text.item] = []
        items[extracted_text.item].append(extracted_text)

    results_scores: List[Tuple[FileSearchResult, float]] = []
    for sha256, text_list in items.items():
        scores = [text.score for text in text_list]
        highest_score = max(scores)
        file = get_existing_file_for_sha256(conn, sha256=sha256)
        if file is None:
            continue
        results_scores.append(
            (
                FileSearchResult(
                    path=file.path,
                    sha256=file.sha256,
                    last_modified=file.last_modified,
                    type=text_list[0].type,
                ),
                highest_score,
            )
        )

    results_scores.sort(key=lambda x: x[1], reverse=True)
    return results_scores


def retrieve_item_text(
    cdb: ClientAPI,
    item_sha256: str,
) -> List[ExtractedText]:
    collection = get_text_collection(cdb)

    results = collection.query(
        query_texts="",
        where={"item": item_sha256},
    )

    return process_result_single_query(results)


def process_result_single_query(result: QueryResult) -> List[ExtractedText]:
    metadatas = result["metadatas"]
    if not metadatas or len(metadatas[0]) == 0:
        return []

    metadatas = metadatas[0]  # Only one query
    scores = result["distances"]
    if not scores:
        return []
    scores = scores[0]

    documents = result["documents"]
    if not documents:
        return []
    documents = documents[0]

    return [
        process_single_result(metadata, document, score)
        for metadata, document, score in zip(metadatas, documents, scores)
    ]


def process_single_result(metadata: Metadata, document: str, score: float):
    assert isinstance(metadata["item"], str)
    assert isinstance(metadata["source"], str)
    assert isinstance(metadata["model"], str)
    assert isinstance(metadata["setter"], str)
    assert isinstance(metadata["language"], str)
    assert isinstance(metadata["type"], str)
    assert isinstance(metadata["general_type"], str)
    return ExtractedText(
        item=metadata["item"],
        source=metadata["source"],
        model=metadata["model"],
        setter=metadata["setter"],
        language=metadata["language"],
        type=metadata["type"],
        general_type=metadata["general_type"],
        text=document,
        score=score,
    )
