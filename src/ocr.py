from typing import List, Sequence
import sqlite3

import torch
import numpy as np
from chromadb.api import ClientAPI
from doctr.models import ocr_predictor

from src.db import get_existing_file_for_sha256, FileSearchResult
from src.files import get_mime_type
from src.types import ItemWithPath
from src.utils import item_image_extractor_np
from src.extractor_job import run_extractor_job

def run_ocr_extractor_job(
        conn: sqlite3.Connection,
        cdb: ClientAPI,
        language="en",
        detection_model='db_resnet50',
        recognition_model='crnn_mobilenet_v3_small'
    ):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """
    setter_name = f"{detection_model}-{recognition_model}"
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(name=collection_name)
    except ValueError:
        collection = cdb.create_collection(name=collection_name)
    
    doctr_model = ocr_predictor(
        det_arch=detection_model,
        reco_arch=recognition_model,
        pretrained=True
    )
    if torch.cuda.is_available():
        doctr_model = doctr_model.cuda().half()

    def process_batch(batch: Sequence[np.ndarray]) -> List[str]:
        result = doctr_model(batch)
        files_texts: List[str] = []
        for page in result.pages:
            file_text = ""
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        file_text += word.value + " "
                    file_text += "\n"
                file_text += "\n"
            files_texts.append(file_text)
        return files_texts
    
    def handle_item_result(item: ItemWithPath, inputs: Sequence[np.ndarray], outputs: Sequence[str]):
        merged_text = "\n".join(list(set(outputs)))
        collection.add(
            ids=[f"{item.sha256}-{setter_name}"],
            documents=[merged_text],
            metadatas=[{
                "item": item.sha256,
                "source": "ocr",
                "model": setter_name,
                "setter": setter_name,
                "language": language,
                "type": item.type,
                "general_type": item.type.split("/")[0],
            }]
        )
    
    return run_extractor_job(
        conn,
        setter_name,
        64,
        item_image_extractor_np,
        process_batch,
        handle_item_result
    )

def search_item_text(
        conn: sqlite3.Connection,
        cdb: ClientAPI,
        text_query: str,
        semantic_search: bool = False,
        full_text_search: bool = False,
        limit: int = 10,
    ):
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(
            name=collection_name,
        )
    except ValueError:
        return [], []
    
    results = collection.query(
        query_texts=text_query if semantic_search else None,
        where_document={"$contains": text_query} if full_text_search else None,
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