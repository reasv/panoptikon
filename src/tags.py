from __future__ import annotations
from dataclasses import dataclass
from typing import List
import os
from datetime import datetime
import sqlite3

from src.utils import estimate_eta

import PIL.IcnsImagePlugin
import PIL.Image

from src.db import add_tag_scan, add_item_tag_scan, get_items_missing_tag_scan, create_tag_setter, insert_tag_item, get_item_rowid
from src.wd_tagger import Predictor, V3_MODELS
from src.video import video_to_frames
from src.utils import create_image_grid, write_text_on_image

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

@dataclass
class TaggingResult:
    sha256: str
    path: str
    mime_type: str
    frames: int
    character_tags: dict[str, float]
    general_tags: dict[str, float]

def combine_results(results: List[dict[str, float]]) -> dict[str, float]:
    """
    Combine multiple results into a single result by picking the highest confidence score for each tag.
    :param results: List of results to combine
    :return: Combined result as a dictionary of tags and scores
    """
    combined_result = dict()
    for result in results:
        for tag, score in result.items():
            if tag not in combined_result or score > combined_result[tag]:
                combined_result[tag] = score
    return combined_result

def aggregate_results(results: List[tuple[dict[str, float], dict[str, float], dict[str, float]]]):
    # Combine all results into a single result for each category
    rating_res, character_res, general_res = zip(*results)
    return translate_tags_result(combine_results(list(rating_res)), combine_results(list(character_res)), combine_results(list(general_res)))

def translate_tags_result(rating_res: dict[str, float], character_res: dict[str, float], general_res: dict[str, float]):
    # Pick the highest rated tag
    rating, rating_confidence = max(rating_res.items(), key=lambda x: x[1])
    rating_tag = "rating:" + rating
    general_res[rating_tag] = rating_confidence
    return character_res, general_res

def process_single_file(sha256: str, mime_type: str, path: str, tag_predictor: Predictor, tag_threshold=0.25):
    """
    Process a single file and predict tags for it. Returns a TaggingResult object, or None if an error occurred.
    """
    try:
        if mime_type.startswith("video"):
            frames = video_to_frames(path, num_frames=4)
            if not frames:
                raise Exception("No frames found")
            os.makedirs("./thumbs", exist_ok=True)
            grid = create_image_grid(frames)
            write_text_on_image(grid, mime_type)
            grid.save(f"./thumbs/{sha256}-grid.jpg")
            write_text_on_image(frames[0], mime_type)
            frames[0].save(f"./thumbs/{sha256}-0.jpg")
            character_res, general_res = aggregate_results([tag_predictor.predict(frame, general_thresh=tag_threshold, character_thresh=None) for frame in frames])
            n_frames = len(frames)
        else:
            image = PIL.Image.open(path)
            character_res, general_res = translate_tags_result(*tag_predictor.predict(image, general_thresh=tag_threshold, character_thresh=None))
            n_frames = 1
        return TaggingResult(sha256, path, mime_type, n_frames, character_res, general_res)
    except Exception as e:
        print(f"Error processing {path} with error {e}")
        return None

def scan_and_predict_tags(conn: sqlite3.Connection, setter=V3_MODELS[0]):
    """
    Scan and predict tags for all items in the database that are missing tags from the given tagging ML model.
    """
    scan_time = datetime.now().isoformat()
    tag_predictor = Predictor(model_repo=setter)
    score_threshold = get_threshold_from_env()
    print(f"Using score threshold {score_threshold}")
    failed_paths = []
    videos, images, total_video_frames, total_processed_frames = 0, 0, 0, 0
    counter = 0
    for item, remaining, total_items in get_items_missing_tag_scan(conn, setter):
        counter += 1
        print(f"{setter}: ({counter}/{total_items}) (ETA: {estimate_eta(scan_time, counter, remaining)}) Processing ({item.type}) {item.path}")
        tag_result = process_single_file(
                item.sha256,
                item.type,
                item.path,
                tag_predictor=tag_predictor,
                tag_threshold=score_threshold
            )
        if tag_result is None:
            failed_paths.append(item.path)
            continue

        total_processed_frames += tag_result.frames

        if item.type.startswith("video"):
            videos += 1
            total_video_frames += tag_result.frames
        else:
            images += 1
        tags = [
            ("danbooru:character", tag, confidence) for tag, confidence in tag_result.character_tags.items()
            ] + [
            ("danbooru:general", tag, confidence) for tag, confidence in tag_result.general_tags.items() if not tag.startswith("rating:")
            ] + [
            ("danbooru:rating", tag, confidence) for tag, confidence in tag_result.general_tags.items() if tag.startswith("rating:")
            ]
        for namespace, tag, confidence in tags:
            tag_rowid = create_tag_setter(conn, namespace=namespace, name=tag, setter=setter)
            item_rowid = get_item_rowid(conn, item.sha256)
            assert item_rowid is not None
            insert_tag_item(
                conn,
                item_rowid=item_rowid,
                tag_rowid=tag_rowid,
                confidence=confidence,
            )
        add_item_tag_scan(conn, item=item.sha256, setter=setter, last_scan=scan_time, tags_set=len(tags), tags_removed=0)

    print(f"Processed {images} images and {videos} videos totalling {total_processed_frames} frames ({total_video_frames} video frames)")
    
    # Record the scan in the database log
    scan_end_time = datetime.now().isoformat()
    # Get first item from get_items_missing_tag_scan(conn, setter) to get the total number of items remaining
    remaining_paths = next(get_items_missing_tag_scan(conn, setter), [0, 0, 0])[2]
    add_tag_scan(
        conn,
        scan_time,
        scan_end_time,
        setter=setter,
        threshold=score_threshold,
        image_files=images,
        video_files=videos,
        other_files=0,
        video_frames=total_video_frames,
        total_frames=total_processed_frames,
        errors=len(failed_paths),
        timeouts=0,
        total_remaining=remaining_paths
    )
    print("Added scan to database")

    return images, videos, failed_paths, []