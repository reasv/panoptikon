from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
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

import panoptikon.data_extractors.models as models
from panoptikon.data_extractors.extraction_jobs.types import (
    ExtractionJobProgress,
    ExtractionJobReport,
    ExtractionJobStart,
)
from panoptikon.db.extraction_log import (
    add_data_log,
    get_items_missing_data_extraction,
    remove_incomplete_jobs,
    update_log,
)
from panoptikon.db.setters import upsert_setter
from panoptikon.types import ItemData
from panoptikon.utils import estimate_eta

logger = logging.getLogger(__name__)

R = TypeVar("R")
I = TypeVar("I")


def run_extraction_job(
    conn: sqlite3.Connection,
    model_opts: models.ModelOpts,
    batch_size: int,
    threshold: float | None,
    input_transform: Callable[[ItemData], Sequence[I]],
    run_batch_inference: Callable[[Sequence[I]], Sequence[R]],
    output_handler: Callable[[int, ItemData, Sequence[I], Sequence[R]], None],
    final_callback: Callable[[], None] = lambda: None,
    load_callback: Callable[[], None] = lambda: None,
):
    """
    Run a job that processes items in the database
    using the given batch inference function and item extractor.
    """
    remove_incomplete_jobs(conn)

    def get_remaining():
        # Get first item to obtain the total number of items remaining
        return (
            next(
                get_items_missing_data_extraction(
                    conn,
                    model_opts=model_opts,
                ),
                [None, -1],
            )[1]
            + 1
        )

    initial_remaining = get_remaining()
    if initial_remaining < 1:
        logger.info(f"No items to process, aborting {model_opts.setter_name()}")
        return

    load_callback()

    start_time = datetime.now()
    scan_time = start_time.isoformat()

    failed_items: Dict[str, ItemData] = {}
    processed_items, videos, images, other, total_processed_units = (
        0,
        0,
        0,
        0,
        0,
    )
    data_load_time, inference_time = 0.0, 0.0
    job_id = add_data_log(
        conn,
        scan_time,
        threshold,
        [model_opts.data_type()],
        model_opts.setter_name(),
        batch_size,
    )
    upsert_setter(
        conn,
        model_opts.setter_name(),
    )
    transaction_per_item = True  # Now hardcoded to True
    if transaction_per_item:
        # Commit the current transaction after adding the log
        conn.commit()

    yield ExtractionJobStart(
        start_time,
        initial_remaining,
        job_id,
    )

    def run_batch_inference_with_counter(work_units: Sequence):
        nonlocal total_processed_units, inference_time
        total_processed_units += len(work_units)
        inf_start = datetime.now()
        o = run_batch_inference(work_units)
        inference_time += (datetime.now() - inf_start).total_seconds()
        return o

    def transform_input_handle_error(item: ItemData):
        try:
            nonlocal data_load_time
            load_start = datetime.now()
            o = input_transform(item)
            data_load_time += (datetime.now() - load_start).total_seconds()
            return o
        except Exception as e:
            logger.error(
                f"Error processing item {item.path}: {e}", exc_info=True
            )
            failed_items[item.sha256] = item
            return []

    for item, remaining, inputs, outputs in batch_items(
        get_items_missing_data_extraction(
            conn,
            model_opts=model_opts,
        ),
        batch_size,
        transform_input_handle_error,
        run_batch_inference_with_counter,
    ):
        processed_items += 1
        if failed_items.get(item.sha256) is not None:
            # Skip items that have already failed
            continue
        if transaction_per_item:
            # Start a new transaction for each item
            conn.execute("BEGIN TRANSACTION")
        try:
            output_handler(job_id, item, inputs, outputs)
        except Exception as e:
            logger.error(f"Error handling item {item.path}: {e}")
            failed_items[item.sha256] = item
            if transaction_per_item:
                conn.rollback()
            continue
        if item.type.startswith("video"):
            videos += 1
        elif item.type.startswith("image"):
            images += 1
        else:
            other += 1
        total_items = remaining + processed_items
        eta_str = estimate_eta(scan_time, processed_items, remaining)
        logger.info(
            f"{model_opts.setter_name()}: ({processed_items}/{total_items}) "
            + f"(ETA: {eta_str}) "
            + f"Processed ({item.type}) {item.path}"
        )
        update_log(
            conn,
            job_id,
            image_files=images,
            video_files=videos,
            other_files=other,
            total_segments=total_processed_units,
            errors=len(failed_items.keys()),
            total_remaining=remaining,
            data_load_time=data_load_time,
            inference_time=inference_time,
            finished=False,
        )
        if transaction_per_item:
            # Commit the transaction after updating the log
            conn.commit()
        yield ExtractionJobProgress(
            start_time, processed_items, total_items, eta_str, item, job_id
        )

    logger.info(
        f"Processed {processed_items} items:"
        + f" {images} images and {videos} videos "
        + f"totalling {total_processed_units} frames"
    )
    remaining_paths = get_remaining()
    if transaction_per_item:
        # Start a new transaction to update the log with the final results
        # The transaction will be committed by the caller
        conn.execute("BEGIN TRANSACTION")
    update_log(
        conn,
        job_id,
        image_files=images,
        video_files=videos,
        other_files=other,
        total_segments=total_processed_units,
        errors=len(failed_items.keys()),
        total_remaining=remaining_paths,
        data_load_time=data_load_time,
        inference_time=inference_time,
        finished=True,
    )
    logger.info("Updated log with scan results")

    failed_paths = [item.path for item in failed_items.values()]
    final_callback()
    yield ExtractionJobReport(
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
    items_generator: Generator[Tuple[ItemData, int], Any, None],
    batch_size: int,
    input_transform_func: Callable[[ItemData], Sequence[I]],
    process_batch_func: Callable[[Sequence[I]], Sequence[R]],
):
    """
    Process items in batches using the given
    item extractor and batch processing functions.
    """
    while True:
        batch: List[Tuple[ItemData, int]] = []
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
        logger.debug(f"Processing minibatch of size {len(batch)}")
        batch_result = run_minibatch(batch)  # Process the batch
        assert len(batch_result) == len(
            batch
        ), f"Minibatch result length {len(batch_result)} does not match minibatch length {len(batch)}"
        result[start:end] = (
            batch_result  # Insert the batch result into the result list
        )
        start = end  # Move to the next batch
    filtered_result = [
        r for r in result if r is not None
    ]  # Filter out the None values
    assert len(filtered_result) == len(
        input_list
    ), f"Result length {len(filtered_result)} does not match input length {len(input_list)}"
    return filtered_result
