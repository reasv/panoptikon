from __future__ import annotations
from typing import List
import os
from datetime import datetime
import concurrent.futures
import time
import sqlite3

import PIL.IcnsImagePlugin
import PIL.Image

from src.db import insert_tag, get_items_missing_tags, add_tag_scan
from src.deepdanbooru import load_model, load_labels, predict, predict_batch
from src.video import video_to_frames, combine_results
from src.utils import create_image_grid

def get_threshold_from_env() -> float:
    threshold = os.getenv("SCORE_THRESHOLD")
    if threshold is None:
        return 0.25
    return float(threshold)

def get_timeout_from_env() -> int:
    timeout = os.getenv("TAGSCAN_TIMEOUT")
    if timeout is None:
        return 40
    return int(timeout)

def process_video_dd(sha256: str, video_path: str, model, labels, keyframe_threshold=0.8, num_frames=None, tag_threshold=0.25):
    try:
        frames = video_to_frames(video_path, keyframe_threshold, num_frames, thumbnail_save_path=f"./thumbs/{sha256}")
        create_image_grid(frames).save(f"./thumbs/{sha256}.jpg")
    except Exception as e:
        print(f"Error processing video {video_path}: {e}")
        return None, None
    if not frames:
        print(f"No frames found for {video_path}")
        return None, None
    results = []
    for result_threshold, _result_all, _result_text in predict_batch(frames, model, labels, score_threshold=tag_threshold):
        results.append((result_threshold))

    combined_result = combine_results(results)
    return combined_result, frames

def process_single_file(sha256: str, mime_type: str, path: str, model, labels, tag_threshold=0.25):
    try:
        if mime_type.startswith("video"):
            result_threshold, video_frames = process_video_dd(sha256, path, model=model, labels=labels, keyframe_threshold=None, num_frames=4, tag_threshold=tag_threshold)
            if result_threshold is None:
                return None, 0
            return result_threshold, len(video_frames)
        else:
            image = PIL.Image.open(path)
            result_threshold, _result_all, _result_text = predict(image, model, labels, score_threshold=tag_threshold)
            return result_threshold, 1
    except Exception as e:
        print(f"Error processing {path} with error {e}")
        return None, 0

def scan_and_predict_tags(conn: sqlite3.Connection, setter="deepdanbooru"):
    """
    Scan and predict tags for all items in the database that are missing tags from the given tagging ML model.
    """
    scan_time = datetime.now().isoformat()
    model = load_model()
    labels = load_labels()
    score_threshold = get_threshold_from_env()
    print(f"Using score threshold {score_threshold}")
    timeout = get_timeout_from_env()
    failed_paths = []
    videos, images, total_video_frames, total_processed_frames = 0, 0, 0, 0
    for item in get_items_missing_tags(conn, setter):
        print(f"Processing {item.path} ({item.type})")
        result_threshold, frames = process_single_file(
                item.sha256,
                item.type,
                item.path,
                model,
                labels,
                tag_threshold=score_threshold
            )
        if result_threshold is None:
            failed_paths.append(item.path)
            continue

        total_processed_frames += frames

        if item.type.startswith("video"):
            videos += 1
            total_video_frames += frames
        else:
            images += 1

        for tag, confidence in result_threshold.items():
            insert_tag(
                conn,
                scan_time=scan_time,
                namespace="danbooru",
                name=tag,
                item=item.sha256,
                confidence=confidence,
                setter=setter,
                value=None
            )
    print(f"Processed {images} images and {videos} videos totalling {total_processed_frames} frames ({total_video_frames} video frames)")
    
    # Record the scan in the database log
    scan_end_time = datetime.now().isoformat()
    remaining_paths = len(list(get_items_missing_tags(conn, setter)))
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