from __future__ import annotations
from typing import List

from datetime import datetime
import sqlite3

import PIL.IcnsImagePlugin
import PIL.Image

from src.db import insert_tag, find_working_paths_without_tags
from src.files import get_mime_type
from src.deepdanbooru import load_model, load_labels, predict
from src.video import video_to_frames, combine_results

# def get_threshold_from_env() -> float:
#     threshold = os.getenv("SCORE_THRESHOLD")
#     if threshold is None:
#         return 0.25
#     return float(threshold)



def process_video_dd(sha256: str, video_path: str, model, labels, keyframe_threshold=0.8, num_frames=None, tag_threshold=0.25):
    try:
        frames = video_to_frames(video_path, keyframe_threshold, num_frames, thumbnail_save_path=f"./thumbs/{sha256}")
    except Exception as e:
        print(f"Error processing video {video_path}: {e}")
        return None, None
    results = []
    for frame in frames:
        result_threshold, _result_all, _result_text = predict(frame, model, labels, score_threshold=tag_threshold)
        results.append((result_threshold))
    combined_result = combine_results(results)
    return combined_result, frames

def scan_and_predict_tags(conn: sqlite3.Connection, setter="deepdanbooru"):
    scan_time = datetime.now().isoformat()
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO tag_scans (start_time, setter)
    VALUES (?, ?)
    ''', (scan_time, setter))

    model = load_model()
    labels = load_labels()
    score_threshold = 0.25

    for sha256, path in find_working_paths_without_tags(conn, setter).items():
        try:
            mime_type = get_mime_type(path)
            if mime_type.startswith("video"):
                result_threshold, video_frames = process_video_dd(sha256, path, model=model, labels=labels, keyframe_threshold=0.8, num_frames=5, tag_threshold=score_threshold)
                if result_threshold is None:
                    continue
            else:
                image = PIL.Image.open(path)
                result_threshold, _result_all, _result_text = predict(image, model, labels, score_threshold=score_threshold)
        except Exception as e:
            print(f"Error processing {path}")
            continue
        for tag, confidence in result_threshold.items():
            insert_tag(
                conn,
                scan_time=scan_time,
                namespace="danbooru",
                name=tag,
                item=sha256,
                confidence=confidence,
                setter=setter,
                value=None
            )
    
    scan_end_time = datetime.now().isoformat()

    cursor = conn.cursor()
    cursor.execute('''
        UPDATE tag_scans
        SET end_time = ?
        WHERE start_time = ? AND setter = ?
    ''', (scan_end_time, scan_time, setter))