
from __future__ import annotations
from typing import Callable, Dict, Sequence, TypeVar, List, Tuple, Generator, Any
from datetime import datetime
import sqlite3

from src.types import ItemWithPath
from src.utils import estimate_eta
from src.db import add_tag_scan, add_item_tag_scan, get_items_missing_tag_scan

R = TypeVar('R')
I = TypeVar('I')
def run_extractor_job(
        conn: sqlite3.Connection,
        setter_name: str,
        item_extractor: Callable[[ItemWithPath], Sequence[I]],
        run_batch_inference: Callable[[Sequence[I]], Sequence[R]],
        handle_item_result: Callable[[sqlite3.Connection, str, ItemWithPath, Sequence[R]], None],
    ):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """
    scan_time = datetime.now().isoformat()
    failed_items: Dict[str, ItemWithPath] = {}
    processed_items, videos, images, other, total_processed_units = 0, 0, 0, 0, 0

    def run_batch_inference_counter(work_units: Sequence):
        nonlocal total_processed_units
        total_processed_units += len(work_units)
        return run_batch_inference(work_units)
    
    def item_extractor_error_handled(item: ItemWithPath):
        try:
            return item_extractor(item)
        except Exception as e:
            print(f"Error processing item {item.path}: {e}")
            failed_items[item.sha256] = item
            return []
    
    for item, remaining, results in batch_items(
            get_items_missing_tag_scan(conn, setter_name),
            64,
            item_extractor_error_handled,
            run_batch_inference_counter
        ):
        processed_items += 1
        if failed_items.get(item.sha256) is not None:
            continue

        if item.type.startswith("video"):
            videos += 1
        elif item.type.startswith("image"):
            images += 1
        else:
            other += 1

        handle_item_result(conn, setter_name, item, results)
        add_item_tag_scan(conn, item=item.sha256, setter=setter_name, last_scan=scan_time, tags_set=0, tags_removed=0)
        print(f"{setter_name}: ({processed_items}/{remaining+processed_items}) (ETA: {estimate_eta(scan_time, processed_items, remaining)}) Processed ({item.type}) {item.path}")    
    
    print(f"Processed {images} images and {videos} videos totalling {total_processed_units} frames")
    
    # Record the scan in the database log
    scan_end_time = datetime.now().isoformat()
    # Get first item from get_items_missing_tag_scan(conn, setter) to get the total number of items remaining
    remaining_paths = next(get_items_missing_tag_scan(conn, setter_name), [0, 0, 0])[2]
    add_tag_scan(
        conn,
        scan_time,
        scan_end_time,
        setter=setter_name,
        threshold=0,
        image_files=images,
        video_files=videos,
        other_files=other,
        video_frames=0,
        total_frames=total_processed_units,
        errors=len(failed_items.keys()),
        timeouts=0,
        total_remaining=remaining_paths
    )
    print("Added scan to database")

    failed_paths = [item.path for item in failed_items.values()]
    return images, videos, failed_paths

def batch_items(
        items_generator: Generator[Tuple[ItemWithPath, int, int], Any, None],
        batch_size: int,
        item_extractor_func: Callable[[ItemWithPath], Sequence[I]],
        process_batch_func: Callable[[Sequence[I]], Sequence[R]]
    ):
    """
    Process items in batches using the given item extractor and batch processing functions.
    """
    while True:
        batch: List[Tuple[ItemWithPath, int]] = []
        work_units: List[I] = []
        batch_index_to_work_units: dict[int, List[int]] = {}
        for item, remaining, _ in items_generator:
            batch_index = len(batch)
            batch.append((item, remaining))
            batch_index_to_work_units[batch_index] = []
            item_wus = item_extractor_func(item)
            for wu in item_wus:
                # The index of the work unit we are adding
                wu_index = len(work_units)
                work_units.append(wu)
                batch_index_to_work_units[batch_index].append(wu_index)
            if len(work_units) >= batch_size:
                # Stop adding items to the batch, and process
                break
        if len(work_units) == 0:
            # No more work to do
            break
        processed_batch_items = process_batch_func(work_units)
        # Yield the batch and the processed items matching the work units to the batch item
        for batch_index, wu_indices in batch_index_to_work_units.items():
            item, remaining = batch[batch_index]
            yield item, remaining, [processed_batch_items[i] for i in wu_indices]

def minibatcher(
        input_list: Sequence[I],
        run_minibatch: Callable[[Sequence[I]], Sequence[R]],
        batch_size: int
    )-> List[R]:
    """
    Process a list of items in batches using the given batch processing function.
    """
    result: List[None | R] = [None] * len(input_list)  # Initialize a result list with None values
    start = 0  # Starting index for each batch
    while start < len(input_list):
        end = min(start + batch_size, len(input_list))  # Calculate end index for the current batch
        batch = input_list[start:end]  # Extract the current batch
        batch_result = run_minibatch(batch)  # Process the batch
        result[start:end] = batch_result  # Insert the batch result into the result list
        start = end  # Move to the next batch
    filtered_result = [r for r in result if r is not None]  # Filter out the None values
    assert len(filtered_result) == len(input_list), "Result length does not match input length"
    return filtered_result