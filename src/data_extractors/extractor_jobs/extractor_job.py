from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from tracemalloc import start
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Sequence,
    Tuple,
    TypeVar,
)

from matplotlib import units

from src.db import (
    add_item_to_log,
    add_tag_scan,
    get_items_missing_data_extraction,
)
from src.types import ItemWithPath
from src.utils import estimate_eta


@dataclass
class ExtractorJobProgress:
    start_time: datetime
    processed_items: int
    total_items: int
    eta_string: str
    item: ItemWithPath


@dataclass
class ExtractorJobReport:
    start_time: datetime
    end_time: datetime
    images: int
    videos: int
    other: int
    total: int
    units: int
    failed_paths: List[str]


R = TypeVar("R")
I = TypeVar("I")


def run_extractor_job(
    conn: sqlite3.Connection,
    setter_name: str,
    batch_size: int,
    input_transform: Callable[[ItemWithPath], Sequence[I]],
    run_batch_inference: Callable[[Sequence[I]], Sequence[R]],
    output_handler: Callable[[ItemWithPath, Sequence[I], Sequence[R]], None],
):
    """
    Run a job that processes items in the database
    using the given batch inference function and item extractor.
    """
    scan_time = datetime.now().isoformat()
    start_time = datetime.now()
    failed_items: Dict[str, ItemWithPath] = {}
    processed_items, videos, images, other, total_processed_units = (
        0,
        0,
        0,
        0,
        0,
    )

    def run_batch_inference_with_counter(work_units: Sequence):
        nonlocal total_processed_units
        total_processed_units += len(work_units)
        return run_batch_inference(work_units)

    def transform_input_handle_error(item: ItemWithPath):
        try:
            return input_transform(item)
        except Exception as e:
            print(f"Error processing item {item.path}: {e}")
            failed_items[item.sha256] = item
            return []

    for item, remaining, inputs, outputs in batch_items(
        get_items_missing_data_extraction(conn, setter_name),
        batch_size,
        transform_input_handle_error,
        run_batch_inference_with_counter,
    ):
        processed_items += 1
        if failed_items.get(item.sha256) is not None:
            continue

        try:
            if len(inputs) > 0:
                output_handler(item, inputs, outputs)
            add_item_to_log(
                conn,
                item=item.sha256,
                setter=setter_name,
                last_scan=scan_time,
                tags_set=0,
                tags_removed=0,
            )
            if len(inputs) == 0:
                continue
        except Exception as e:
            print(f"Error handling item {item.path}: {e}")
            failed_items[item.sha256] = item
            continue
        if item.type.startswith("video"):
            videos += 1
        elif item.type.startswith("image"):
            images += 1
        else:
            other += 1
        total_items = remaining + processed_items
        eta_str = estimate_eta(scan_time, processed_items, remaining)
        print(
            f"{setter_name}: ({processed_items}/{total_items}) "
            + f"(ETA: {eta_str}) "
            + f"Processed ({item.type}) {item.path}"
        )
        yield ExtractorJobProgress(
            start_time, processed_items, total_items, eta_str, item
        )

    print(
        f"Processed {processed_items} items:"
        + f" {images} images and {videos} videos "
        + f"totalling {total_processed_units} frames"
    )

    # Record the scan in the database log
    scan_end_time = datetime.now().isoformat()
    # Get first item from get_items_missing_tag_scan(conn, setter) to get the total number of items remaining
    remaining_paths = (
        next(get_items_missing_data_extraction(conn, setter_name), [None, -1])[
            1
        ]
        + 1
    )
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
        total_remaining=remaining_paths,
    )
    print("Added scan to database")

    failed_paths = [item.path for item in failed_items.values()]
    yield ExtractorJobReport(
        start_time=start_time,
        end_time=datetime.now(),
        images=images,
        videos=videos,
        other=other,
        total=processed_items,
        units=total_processed_units,
        failed_paths=failed_paths,
    )


def batch_items(
    items_generator: Generator[Tuple[ItemWithPath, int], Any, None],
    batch_size: int,
    input_transform_func: Callable[[ItemWithPath], Sequence[I]],
    process_batch_func: Callable[[Sequence[I]], Sequence[R]],
):
    """
    Process items in batches using the given
    item extractor and batch processing functions.
    """
    while True:
        batch: List[Tuple[ItemWithPath, int]] = []
        work_units: List[I] = []
        batch_index_to_work_units: dict[int, List[int]] = {}
        for item, remaining in items_generator:
            batch_index = len(batch)
            batch.append((item, remaining))
            batch_index_to_work_units[batch_index] = []
            item_wus = input_transform_func(item)
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
        processed_batch_items = minibatcher(
            work_units, process_batch_func, batch_size
        )
        # Yield the batch and the processed items matching the work units to the batch item
        for batch_index, wu_indices in batch_index_to_work_units.items():
            item, remaining = batch[batch_index]
            yield item, remaining, [work_units[i] for i in wu_indices], [
                processed_batch_items[i] for i in wu_indices
            ]


def minibatcher(
    input_list: Sequence[I],
    run_minibatch: Callable[[Sequence[I]], Sequence[R]],
    batch_size: int,
) -> List[R]:
    """
    Process a list of items in batches using the given batch processing function.
    """
    result: List[None | R] = [None] * len(
        input_list
    )  # Initialize a result list with None values
    start = 0  # Starting index for each batch
    while start < len(input_list):
        end = min(
            start + batch_size, len(input_list)
        )  # Calculate end index for the current batch
        batch = input_list[start:end]  # Extract the current batch
        batch_result = run_minibatch(batch)  # Process the batch
        result[start:end] = (
            batch_result  # Insert the batch result into the result list
        )
        start = end  # Move to the next batch
    filtered_result = [
        r for r in result if r is not None
    ]  # Filter out the None values
    assert len(filtered_result) == len(
        input_list
    ), "Result length does not match input length"
    return filtered_result
