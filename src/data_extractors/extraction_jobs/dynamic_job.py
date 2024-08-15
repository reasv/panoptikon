import logging
import sqlite3
from typing import Any, Dict, List, Sequence, Tuple

from src.data_extractors.data_handlers.clip import handle_clip
from src.data_extractors.data_handlers.tags import handle_tag_result
from src.data_extractors.data_handlers.text import handle_text
from src.data_extractors.data_handlers.text_embeddings import (
    handle_text_embeddings,
)
from src.data_extractors.data_loaders.audio import load_audio
from src.data_extractors.data_loaders.images import image_loader
from src.data_extractors.extraction_jobs.extraction_job import (
    run_extraction_job,
)
from src.data_extractors.models import ModelGroup
from src.db.text_embeddings import get_text_missing_embeddings
from src.types import ItemWithPath

logger = logging.getLogger(__name__)


def run_dynamic_extraction_job(conn: sqlite3.Connection, model: ModelGroup):
    """
    Run a job that processes items in the database using the given model.
    """
    score_threshold = model.get_group_threshold(conn)
    if score_threshold:
        logger.info(f"Using score threshold {score_threshold}")
    else:
        logger.info("No score threshold set")

    cache_args = "batch", 1, 60

    def load_model():
        model.load_model(*cache_args)

    handler_name, handler_opts = model.input_spec()

    if handler_name == "image_frames":

        def frame_loader(item: ItemWithPath):
            max_frames = handler_opts.get("max_frames", 4)
            frames = image_loader(conn, item)
            return frames[:max_frames]

        data_loader = frame_loader

    elif handler_name == "audio_tracks":
        sample_rate: int = handler_opts.get("sample_rate", 16000)
        max_tracks: int = handler_opts.get("max_tracks", -1)

        def audio_loader(item: ItemWithPath) -> Sequence[bytes]:
            if item.type.startswith("video") or item.type.startswith("audio"):
                audio = load_audio(item.path, sr=sample_rate)
                return [track.tobytes() for track in audio[:max_tracks]]
            return []

        data_loader = audio_loader

    elif handler_name == "extracted_text":

        def get_item_text(item: ItemWithPath) -> List[Tuple[int, str]]:
            return get_text_missing_embeddings(
                conn, item.sha256, model.data_type(), model.setter_name()
            )

        data_loader = get_item_text
    else:
        raise ValueError(f"Data loader {handler_name} not found")

    if (
        handler_name == "extracted_text"
        and model.data_type() == "text_embedding"
    ):

        def run_batch_emb(
            batch: Sequence[Tuple[int, str]],
        ) -> List[bytes]:
            return model.run_batch_inference(
                *cache_args, [(text, None) for _, text in batch]
            )  # type: ignore

        batch_inference_func = run_batch_emb

    if handler_name == "image_frames" and model.data_type() == "clip":

        def run_batch_clip(
            batch: Sequence[bytes],
        ) -> List[bytes]:
            return model.run_batch_inference(
                *cache_args, [(None, frame) for frame in batch]
            )  # type: ignore

        batch_inference_func = run_batch_clip
    else:

        def run_batch(
            batch: Sequence[bytes | str],
        ) -> List[Dict[str, Any]]:
            opts = {"threshold": score_threshold} if score_threshold else None
            return model.run_batch_inference(
                *cache_args,
                [(opts, frame) for frame in batch],
            )  # type: ignore

        batch_inference_func = run_batch

    if model.data_type() == "tags":

        def tag_handler(
            log_id: int,
            item: ItemWithPath,
            _: Sequence[bytes | str],
            outputs: Sequence[Dict[str, Any]],
        ):
            handle_tag_result(conn, log_id, model.setter_name(), item, outputs)

        result_handler = tag_handler
    elif model.data_type() == "text":

        def text_handler(
            log_id: int,
            item: ItemWithPath,
            _: Sequence[bytes | str],
            outputs: Sequence[Dict[str, Any]],
        ):
            handle_text(conn, log_id, item, outputs)

        result_handler = text_handler

    elif model.data_type() == "clip":

        def clip_handler(
            log_id: int,
            item: ItemWithPath,
            _: Sequence[bytes],
            embeddings: Sequence[bytes],  # bytes
        ):
            handle_clip(conn, log_id, item, embeddings)

        result_handler = clip_handler

    elif model.data_type() == "text_embedding":

        def text_emb_handler(
            log_id: int,
            _: ItemWithPath,
            inputs: Sequence[Tuple[int, str]],
            embeddings: Sequence[bytes],
        ):
            handle_text_embeddings(conn, log_id, inputs, embeddings)

        result_handler = text_emb_handler
    else:
        raise ValueError(f"Data handler not found for {model.data_type()}")

    def cleanup():
        model.unload_model("batch")

    return run_extraction_job(
        conn,
        model,
        data_loader,
        batch_inference_func,  # type: ignore
        result_handler,  # type: ignore
        cleanup,
        load_callback=load_model,
    )
