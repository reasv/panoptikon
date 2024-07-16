from typing import List, Sequence
import sqlite3

import torch
import whisperx
from whisperx.types import TranscriptionResult
from chromadb.api import ClientAPI

from src.types import ItemWithPath
from src.data_extractors.extractor_job import run_extractor_job

def run_whisper_extractor_job(
        conn: sqlite3.Connection,
        cdb: ClientAPI,
        batch_size=8,
        whisper_model="base",
    ):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """
    setter_name = f"{whisper_model}"
    collection_name = f"text_embeddings"
    try:
        collection = cdb.get_collection(name=collection_name)
    except ValueError:
        collection = cdb.create_collection(name=collection_name)

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

    whisper_model = whisperx.load_model(whisper_model, device=device)

    def get_media_paths(item: ItemWithPath) -> Sequence[str]:
        if item.type.startswith("video"):
            return [item.path]
        elif item.type.startswith("audio"):
            return [item.path]
        return []

    def process_batch(batch: Sequence[str]) -> List[TranscriptionResult]:
        outputs: List[TranscriptionResult] = []
        for path in batch:
            outputs.append(whisper_model.transcribe(
                audio=path,
                batch_size=batch_size
            ))
        return outputs
    
    def handle_item_result(item: ItemWithPath, _: Sequence[str], outputs: Sequence[TranscriptionResult]):
        transcriptionResult = outputs[0] # Only one output per item
        merged_text = "\n".join([segment["text"] for segment in transcriptionResult["segments"]])
        collection.add(
            ids=[f"{item.sha256}-{setter_name}"],
            documents=[merged_text],
            metadatas=[{
                "item": item.sha256,
                "source": "stt",
                "model": setter_name,
                "setter": setter_name,
                "type": item.type,
                "language": transcriptionResult["language"],
                "general_type": item.type.split("/")[0],
            }]
        )
    
    return run_extractor_job(
        conn,
        setter_name,
        1,
        get_media_paths,
        process_batch,
        handle_item_result
    )
