import os
import sqlite3
from datetime import datetime
from typing import List, Sequence, Union, cast

from PIL import Image as PILImage
from httpx import Client
import torch
import open_clip
import numpy as np
import chromadb
import chromadb.api
from chromadb.api.types import is_image, is_document, EmbeddingFunction, Image, Images, Document, Documents, Embeddings, Embedding
from chromadb.api import ClientAPI

from src.db import get_existing_file_for_sha256, FileSearchResult
from src.db import get_items_missing_tag_scan, add_item_tag_scan, add_tag_scan
from src.files import get_mime_type
from src.utils import estimate_eta, make_video_thumbnails
from src.video import video_to_frames

class CLIPEmbedder(EmbeddingFunction[Union[Documents, Images]]):
    def __init__(
            self,
            model_name='ViT-H-14-378-quickgelu',
            pretrained='dfn5b',
            batch_size=8
        ):
        self.model_name = model_name
        self.pretrained = pretrained
        self.batch_size = batch_size
        self.model = None
        self.tokenizer = None
        self.preprocess = None
        self.device = torch.device(
            "cuda" if torch.cuda.is_available() 
            else "cpu"
        )

    def _load_model(self):
        if self.model is None:
            self.model, _, self.preprocess = open_clip.create_model_and_transforms(
                self.model_name, pretrained=self.pretrained
            )
            self.model.eval().to(self.device)
            self.tokenizer = open_clip.get_tokenizer(self.model_name)
    
    def load_model(self):
        self._load_model()

    def get_image_embeddings(
            self,
            images: Sequence[str | PILImage.Image | np.ndarray]
        ):
        self._load_model()
        embeddings = []

        for i in range(0, len(images), self.batch_size):
            image_batch = images[i:i + self.batch_size]
            # Check if they're all PIL images rather than paths
            if all(isinstance(image, PILImage.Image) for image in image_batch):
                batch_images = [self.preprocess(image).unsqueeze(0) for image in image_batch] # type: ignore
            # Check if they're ndarrays
            elif all(isinstance(image, np.ndarray) for image in image_batch):
                batch_images = [self.preprocess(PILImage.fromarray(image_array)).unsqueeze(0) for image_array in image_batch] # type: ignore
            # Otherwise, assume they're all paths
            else:
                batch_images = [self.preprocess(PILImage.open(image)).unsqueeze(0) for image in image_batch] # type: ignore
            batch_images = torch.cat(batch_images).to(self.device)

            with torch.no_grad(), torch.cuda.amp.autocast():
                image_features = self.model.encode_image(batch_images) # type: ignore
                image_features /= image_features.norm(dim=-1, keepdim=True)
            
            embeddings.extend(image_features.cpu().numpy())

        return embeddings

    def get_text_embeddings(self, texts: List[str]):
        self._load_model()
        embeddings = []

        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i:i + self.batch_size]
            batch_tokens = self.tokenizer(batch_texts).to(self.device) # type: ignore

            with torch.no_grad(), torch.cuda.amp.autocast():
                text_features = self.model.encode_text(batch_tokens) # type: ignore
                text_features /= text_features.norm(dim=-1, keepdim=True)

            embeddings.extend(text_features.cpu().numpy())

        return embeddings

    def unload_model(self):
        if self.model:
            del self.model
            self.model = None
            torch.cuda.empty_cache()

    def rank_images_by_similarity(self, image_embeddings_dict, text_embedding):
        image_hashes = list(image_embeddings_dict.keys())
        image_embeddings = torch.tensor(list(image_embeddings_dict.values()))

        # Normalize text embedding
        text_embedding = torch.tensor(text_embedding).unsqueeze(0)
        text_embedding /= text_embedding.norm(dim=-1, keepdim=True)

        # Compute similarities
        similarities = torch.matmul(image_embeddings, text_embedding.T).squeeze()

        # Sort image hashes by similarity
        sorted_indices = torch.argsort(similarities, descending=True)
        sorted_hashes = [image_hashes[i] for i in sorted_indices]

        return sorted_hashes
    
    def __call__(self, input: Union[Documents, Images]) -> Embeddings:
        embeddings: Embeddings = []
        for item in input:
            if is_image(item):
                embeddings.append(self.get_image_embeddings([cast(Image, item)])[0].squeeze().tolist())
            elif is_document(item):
                embeddings.append(self.get_text_embeddings([cast(Document, item)])[0].squeeze().tolist())
        return embeddings

def get_chromadb_client() -> chromadb.api.BaseAPI:
    sqlite_db_file = os.getenv('DB_FILE', './db/sqlite.db')
    cdb_file = f"{sqlite_db_file}.chromadb"
    return chromadb.PersistentClient(path=cdb_file)

def search_item_image_embeddings(
        conn: sqlite3.Connection,
        cdb: chromadb.api.ClientAPI,
        embedder: CLIPEmbedder,
        image_query: np.ndarray | None = None,
        text_query: str | None = None,
        limit: int = 10,
        model = "ViT-H-14-378-quickgelu",
        checkpoint = "dfn5b",
    ):
    setter = f"{model}_ckpt_{checkpoint}"
    collection_name = f"image_embeddings.{setter}"

    try:
        collection = cdb.get_collection(
            name=collection_name,
            embedding_function=embedder,
        )
    except ValueError:
        return [], []
    
    results = collection.query(
        query_texts=text_query,
        query_images=image_query,
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

def scan_and_embed(
        conn: sqlite3.Connection,
        cdb: ClientAPI,
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
                image_array = np.array(PILImage.open(item.path))
                collection.add(
                    ids=[item.sha256],
                    images=[image_array],
                    metadatas=[{
                        "item": item.sha256,
                        "type": item.type,
                        "general_type": item.type.split("/")[0],   
                    }]
                )
                images += 1
            if item.type.startswith("video"):
                frames = video_to_frames(item.path, num_frames=4)
                collection.add(
                        ids=[f"{item.sha256}-{i}" for i, f in enumerate(frames)],
                        images=[np.array(frame) for frame in frames],
                        metadatas=([{
                            "item": item.sha256,
                            "type": item.type,
                            "general_type": item.type.split("/")[0],
                        } for _ in frames])
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