import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Tuple

from chromadb import PersistentClient
from chromadb.api import ClientAPI
from chromadb.api.types import QueryResult
from chromadb.types import Metadata

from src.db import FileSearchResult, get_existing_file_for_sha256


@dataclass
class ExtractedText:
    item: str
    source: str
    setter: str
    language: str
    type: str
    general_type: str
    text: str
    score: float


def query_result_to_file_search_result(
    conn: sqlite3.Connection, results: QueryResult
):
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
    assert isinstance(metadata["setter"], str)
    assert isinstance(metadata["language"], str)
    assert isinstance(metadata["type"], str)
    assert isinstance(metadata["general_type"], str)
    return ExtractedText(
        item=metadata["item"],
        source=metadata["source"],
        setter=metadata["setter"],
        language=metadata["language"],
        type=metadata["type"],
        general_type=metadata["general_type"],
        text=document,
        score=score,
    )


def get_chromadb_client() -> ClientAPI:
    sqlite_db_file = os.getenv("DB_FILE", "./db/sqlite.db")
    cdb_file = f"{sqlite_db_file}.chromadb"
    return PersistentClient(path=cdb_file)


def get_threshold_from_env() -> float:
    threshold = os.getenv("SCORE_THRESHOLD")
    if threshold is None:
        return 0.1
    return float(threshold)


def get_timeout_from_env() -> int:
    timeout = os.getenv("TAGSCAN_TIMEOUT")
    if timeout is None:
        return 40
    return int(timeout)
