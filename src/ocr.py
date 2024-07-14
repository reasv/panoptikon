import sqlite3
from datetime import datetime
from typing import Any, Generator, List, Sequence

from PIL import Image as PILImage
import numpy as np
from chromadb.api import ClientAPI

from doctr.models import ocr_predictor
from doctr.io.html import read_html
from doctr.io.pdf import read_pdf

from src.db import ItemWithPath, get_existing_file_for_sha256, FileSearchResult
from src.db import get_items_missing_tag_scan, add_item_tag_scan, add_tag_scan
from src.files import get_mime_type
from src.utils import estimate_eta, make_video_thumbnails, pil_ensure_rgb
from src.utils import batch_items_generator, batch_items_consumer, create_item_image_extractor
from src.video import video_to_frames

def scan_extract_text(
        conn: sqlite3.Connection,
        cdb: ClientAPI,
        language="en",
        detection_model='db_resnet50',
        recognition_model='crnn_mobilenet_v3_small',
    ):
    """
    Scan all items in the database that have not had text extracted yet.
    """
    scan_time = datetime.now().isoformat()
    setter = f"{detection_model}-{recognition_model}"
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(
            name=collection_name,
        )
    except ValueError:
        collection = cdb.create_collection(
            name=collection_name,
        )

    doctr_model = ocr_predictor(det_arch=detection_model, reco_arch=recognition_model, pretrained=True).cuda().half()
    videos, images, total_video_frames, total_processed_frames, items = 0, 0, 0, 0, 0
    def process_batch(batch: Sequence[np.ndarray]) -> List[str]:
        nonlocal total_processed_frames
        total_processed_frames += len(batch)
        result = doctr_model([image for image in batch])
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

    failed_paths: List[str] = []

    item_extractor = create_item_image_extractor(error_callback=lambda x: failed_paths.append(x.path))

    for batch, remaining, total_items in batch_items_generator(get_items_missing_tag_scan(conn, setter=setter), batch_size=64):
        for item, ocr_results in batch_items_consumer(batch, process_batch, item_extractor):
            items += 1
            merged_text = "\n".join(list(set(ocr_results)))
            collection.add(
                    ids=[f"{item.sha256}-{setter}"],
                    documents=[merged_text],
                    metadatas=[{
                        "item": item.sha256,
                        "source": "ocr",
                        "model": setter,
                        "setter": setter,
                        "language": language,
                        "type": item.type,
                        "general_type": item.type.split("/")[0],
                    }]
                )
            add_item_tag_scan(conn, item.sha256, setter, scan_time)
            if item.type.startswith("image"):
                images += 1
            if item.type.startswith("video"):
                videos += 1
                total_video_frames += len(ocr_results)

        print(f"{setter}: ({items}/{total_items}) (ETA: {estimate_eta(scan_time, items, remaining)}) Last item ({item.type}) {item.path}")

    # Record the scan in the database log
    scan_end_time = datetime.now().isoformat()
    # Get first item from get_items_missing_tag_scan(conn, setter) to get the total number of items remaining
    remaining_paths = next(get_items_missing_tag_scan(conn, setter), [0, 0, 0])[2]
    add_tag_scan(
        conn,
        scan_time,
        scan_end_time,
        setter=setter,
        threshold=0,
        image_files=images,
        video_files=videos,
        other_files=0,
        video_frames=total_video_frames,
        total_frames=total_processed_frames,
        errors=len(failed_paths),
        timeouts=0,
        total_remaining=remaining_paths
    )

    return images, videos, failed_paths, []

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