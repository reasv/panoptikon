import sqlite3
from datetime import datetime

from PIL import Image
import numpy as np
from chromadb.api import BaseAPI

from src.db import get_items_missing_tag_scan, add_item_tag_scan, add_tag_scan
from src.utils import estimate_eta, make_video_thumbnails
from src.video import video_to_frames
from src.image_embeddings import CLIPEmbedder

def scan_and_embed(
        conn: sqlite3.Connection,
        cdb: BaseAPI,
        model="ViT-H-14-378-quickgelu",
        checkpoint="dfn5b",
    ):
    """
    Scan and embed all items in the database that are missing embeddings from the given embedding model.
    """
    scan_time = datetime.now().isoformat()
    embedder = CLIPEmbedder(
        model_name=model,
        pretrained=checkpoint,
    )
    embedder.load_model()
    setter = f"{model}_ckpt_{checkpoint}"
    collection_name = f"image_embeddings.{setter}"
    try:
        collection = cdb.get_collection(
            name=collection_name,
            embedding_function=embedder,
        )
    except ValueError:
        collection = cdb.create_collection(
            name=collection_name,
            embedding_function=embedder,
        )
    failed_paths = []
    videos, images, total_video_frames, total_processed_frames, items = 0, 0, 0, 0, 0
    for item, remaining, total_items in get_items_missing_tag_scan(conn, setter=setter):
        items += 1
        print(f"{setter}: ({items}/{total_items}) (ETA: {estimate_eta(scan_time, items, remaining)}) Processing ({item.type}) {item.path}")
        try:
            if item.type.startswith("image"):
                image_array = np.array(Image.open(item.path))
                collection.add(
                    ids=[item.sha256],
                    images=[image_array],
                    metadatas=[{"item": item.sha256}]
                )
                images += 1
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                collection.add(
                        ids=[f"{item.sha256}-{i}" for i, f in enumerate(frames)],
                        images=[np.array(frame) for frame in frames],
                        metadatas=([{"item": item.sha256} for _ in frames])
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