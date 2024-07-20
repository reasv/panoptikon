import sqlite3
from ast import mod
from typing import Iterable, List, Sequence, Tuple

import faster_whisper
import numpy as np
import torch
from chromadb.api import ClientAPI
from faster_whisper.transcribe import Segment, TranscriptionInfo

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

    whisper_model = faster_whisper.WhisperModel(
        model_opts.model_repo(), device=device, compute_type="float16"
    )
    if model_opts.batch_size() > 1:
        whisper_model = faster_whisper.BatchedInferencePipeline(
            model=whisper_model, batch_size=model_opts.batch_size()
        )

    def get_media_paths(item: ItemWithPath) -> Sequence[np.ndarray]:
        if item.type.startswith("video"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        elif item.type.startswith("audio"):
            audio = load_audio(item.path)
            return [audio] if audio is not None else []
        return []

    def process_batch(batch: Sequence[np.ndarray]) -> List[
        Tuple[
            Iterable[Segment],
            TranscriptionInfo,
        ]
    ]:
        outputs: List[
            Tuple[
                Iterable[Segment],
                TranscriptionInfo,
            ]
        ] = []
        for audio in batch:
            outputs.append(whisper_model.transcribe(audio=audio))
        return outputs

    def handle_item_result(
        item: ItemWithPath,
        _: Sequence[np.ndarray],
        outputs: Sequence[
            Tuple[
                Iterable[Segment],
                TranscriptionInfo,
            ]
        ],
    ):
        if len(outputs) == 0:
            return
        segments, info = outputs[0]  # Only one output per item
        merged_text = "\n".join([segment.text for segment in segments])

        merged_text = merged_text.strip()

        add_item_text(
            cdb,
            item,
            model_opts,
            info.language,
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
