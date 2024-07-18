import sqlite3
from typing import List, Sequence

import numpy as np
import torch
import whisperx
from chromadb.api import ClientAPI
from whisperx.types import TranscriptionResult

from src.data_extractors.data_loaders.audio import load_audio
from src.data_extractors.extractor_jobs import run_extractor_job
from src.data_extractors.models import WhisperSTTModel
from src.data_extractors.text_embeddings import add_item_text
from src.types import ItemWithPath


def run_whisper_extractor_job(
    conn: sqlite3.Connection, cdb: ClientAPI, model_opts: WhisperSTTModel
):
    """
    Run a job that processes items in the database using the given batch inference function and item extractor.
    """

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

    whisper_model = whisperx.load_model(model_opts.model_name(), device=device)

    def get_media_paths(item: ItemWithPath) -> Sequence[np.ndarray]:
        if item.type.startswith("video"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        elif item.type.startswith("audio"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        return []

    def process_batch(batch: Sequence[np.ndarray]) -> List[TranscriptionResult]:
        outputs: List[TranscriptionResult] = []
        for audio in batch:
            outputs.append(
                whisper_model.transcribe(
                    audio=audio, batch_size=model_opts.batch_size()
                )
            )
        return outputs

    def handle_item_result(
        item: ItemWithPath,
        _: Sequence[np.ndarray],
        outputs: Sequence[TranscriptionResult],
    ):
        if len(outputs) == 0:
            return
        transcriptionResult = outputs[0]  # Only one output per item
        merged_text = "\n".join(
            [segment["text"] for segment in transcriptionResult["segments"]]
        )

        merged_text = merged_text.strip()

        add_item_text(
            cdb,
            item,
            model_opts,
            transcriptionResult["language"],
            merged_text,
        )

    return run_extractor_job(
        conn,
        model_opts.setter_id(),
        1,
        get_media_paths,
        process_batch,
        handle_item_result,
    )
