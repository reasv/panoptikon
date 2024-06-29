import sqlite3
from datetime import datetime

from PIL import Image as PILImage
import numpy as np
from chromadb.api import BaseAPI
from paddleocr import PaddleOCR

from src.db import get_existing_file_for_sha256, FileSearchResult
from src.db import get_items_missing_tag_scan, add_item_tag_scan, add_tag_scan
from src.files import get_mime_type
from src.utils import estimate_eta, make_video_thumbnails
from src.video import video_to_frames

def scan_extract_text(
        conn: sqlite3.Connection,
        cdb: BaseAPI,
        language="en",
        model="paddleocr",
    ):
    """
    Scan all items in the database that have not had text extracted yet.
    """
    scan_time = datetime.now().isoformat()
    setter = f"{model}-{language}"
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(
            name=collection_name,
        )
    except ValueError:
        collection = cdb.create_collection(
            name=collection_name,
        )
    ocr = PaddleOCR(use_angle_cls=True, lang=language, use_gpu=False)
    failed_paths = []
    videos, images, total_video_frames, total_processed_frames, items = 0, 0, 0, 0, 0
    for item, remaining, total_items in get_items_missing_tag_scan(conn, setter=setter):
        items += 1
        print(f"{setter}: ({items}/{total_items}) (ETA: {estimate_eta(scan_time, items, remaining)}) Processing ({item.type}) {item.path}")
        try:
            if item.type.startswith("image"):
                image_array = np.array(PILImage.open(item.path))
                ocr_result = ocr.ocr(image_array)[0]
                text = "\n".join([line[1][0] for line in ocr_result])
                collection.add(
                    ids=[f"{item.sha256}-{setter}"],
                    documents=[text],
                    metadatas=[{
                        "item": item.sha256,
                        "source": "ocr",
                        "model": model,
                        "setter": setter,
                        "language": language,
                        "type": item.type,
                        "general_type": item.type.split("/")[0],
                    }]
                )
                images += 1
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                ocr_results = [ocr.ocr(np.array(frame))[0] for frame in frames]
                texts = ["\n".join([line[1][0] for line in result]) for result in ocr_results]
                # Deduplicate text
                texts = list(set(texts))
                # Get a single text for the video
                full_text = "\n".join(texts)
                collection.add(
                        ids=[f"{item.sha256}-{setter}"],
                        documents=[full_text],
                        metadatas=([
                            {
                                "item": item.sha256,
                                "source": "ocr",
                                "model": model,
                                "setter": setter,
                                "language": language,
                                "type": item.type,
                                "general_type": item.type.split("/")[0],
                            }]
                        )
                )
                make_video_thumbnails(frames, item.sha256, item.type)
                videos += 1
                total_video_frames += len(frames)
            add_item_tag_scan(conn, item.sha256, setter, scan_time)
        except Exception as e:
            print(f"Failed to embed {item.path}: {e}")
            failed_paths.append(item.path)

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
        cdb: BaseAPI,
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