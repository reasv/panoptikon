import os
import sqlite3
from typing import List

import PIL.Image
import numpy as np
import PIL

import chromadb
import chromadb.api

from src.db import get_existing_file_for_sha256, FileSearchResult
from src.files import get_mime_type
from src.clip import CLIPEmbedder

def get_chromadb_client() -> chromadb.api.BaseAPI:
    sqlite_db_file = os.getenv('DB_FILE', './db/sqlite.db')
    cdb_file = f"{sqlite_db_file}.chromadb"
    return chromadb.PersistentClient(path=cdb_file)

def search_item_image_embeddings(
        conn: sqlite3.Connection,
        cdb: chromadb.api.BaseAPI,
        embedder: CLIPEmbedder,
        image_query: np.ndarray | None = None,
        text_query: str | None = None,
        limit: int = 10,
        model = "ViT-H-14-378-quickgelu",
        checkpoint = "dfn5b",
    ):
    setter = f"{model}_ckpt_{checkpoint}"
    collection_name = f"image_embeddings.{setter}"

    try:
        collection = cdb.get_collection(
            name=collection_name,
            embedding_function=embedder,
        )
    except ValueError:
        return [], []
    
    results = collection.query(
        query_texts=text_query,
        query_images=image_query,
        n_results=limit
    )
    metadatas = results['metadatas']
    if not metadatas:
        return [], []
    metadatas = metadatas[0] # Only one query

    scores = results['distances']
    if not scores:
        return [], []
    scores = scores[0] # Only one query
    
    searchResults = []
    resultScores = []
    for metadata, distance in zip(metadatas, scores):
        sha256 = metadata['item']
        if not isinstance(sha256, str):
            continue
        file = get_existing_file_for_sha256(conn, sha256=sha256)
        if file is None:
            continue
        searchResults.append(FileSearchResult(
            path=file.path,
            sha256=file.sha256,
            last_modified=file.last_modified,
            type=get_mime_type(file.path) or "unknown",
        ))
        resultScores.append(distance)

    return searchResults, resultScores