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
from src.video import video_to_frames

def batch_items_generator(items_generator: Generator[tuple[ItemWithPath, int, Any], Any, None], batch_size: int):
    batch: List[ItemWithPath] = []
    last_remaining = 0
    total_items = 0
    for item, remaining, total_items in items_generator:
        last_remaining = remaining
        total_items = total_items
        batch.append(item)
        if len(batch) == batch_size:
            yield batch, last_remaining, total_items
            batch = []
    if batch:
        yield batch, last_remaining, total_items

def batch_items_consumer(batch: List[ItemWithPath], process_batch_func, items_to_batch_items_func):
    work_units = []
    batch_index_to_work_units: dict[int, List[int]] = {}
    for batch_index, item in enumerate(batch):
        batch_index_to_work_units[batch_index] = []
        item_wus = items_to_batch_items_func(item)
        for wu in item_wus:
            # The index of the work unit we are adding
            wu_index = len(work_units)
            work_units.append(wu)
            batch_index_to_work_units[batch_index].append(wu_index)
    processed_batch_items = process_batch_func(work_units)
    # Yield the batch and the processed items matching the work units to the batch item
    for batch_index, wu_indices in batch_index_to_work_units.items():
        yield batch[batch_index], [processed_batch_items[i] for i in wu_indices]

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
    def item_to_batch_items(item: ItemWithPath) -> List[np.ndarray]:
        nonlocal failed_paths
        try:
            if item.type.startswith("image"):
                return [np.array(pil_ensure_rgb(PILImage.open(item.path)))]
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                make_video_thumbnails(frames, item.sha256, item.type)
                return [np.array(pil_ensure_rgb(frame)) for frame in frames]
            if item.type.startswith("application/pdf"):
                return read_pdf(item.path)
            if item.type.startswith("text/html"):
                return read_pdf(read_html(item.path))
        except Exception as e:
            print(f"Failed to read {item.path}: {e}")
            failed_paths.append(item.path)
        return []

    for batch, remaining, total_items in batch_items_generator(get_items_missing_tag_scan(conn, setter=setter), batch_size=64):
        for item, ocr_results in batch_items_consumer(batch, process_batch, item_to_batch_items):
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