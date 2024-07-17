import sqlite3
from dataclasses import dataclass
from typing import List

from chromadb.api import ClientAPI
from chromadb.api.types import QueryResult
from chromadb.types import Metadata


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


def retrieve_item_text(
    cdb: ClientAPI,
    item_sha256: str,
) -> List[ExtractedText]:
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(name=collection_name)
    except ValueError:
        return []

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
