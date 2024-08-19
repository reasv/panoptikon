import logging
import sqlite3
from typing import Any, Dict, List, Sequence, Tuple

from panoptikon.data_extractors.data_handlers.clip import handle_clip
from panoptikon.data_extractors.data_handlers.tags import handle_tag_result
from panoptikon.data_extractors.data_handlers.text import handle_text
from panoptikon.data_extractors.data_handlers.text_embeddings import (
    handle_text_embeddings,
)
from panoptikon.data_extractors.data_loaders.audio import (
    load_audio,
    load_audio_single,
)
from panoptikon.data_extractors.data_loaders.images import image_loader
from panoptikon.data_extractors.extraction_jobs.extraction_job import (
    run_extraction_job,
)
from panoptikon.data_extractors.models import ModelGroup
from panoptikon.db.extracted_text import get_text_by_ids
from panoptikon.inferio.impl.utils import serialize_array
from panoptikon.types import ItemData

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
    inference_opts = {"threshold": score_threshold} if score_threshold else {}

    cache_args = "batch", 1, 60

    def load_model():
        model.load_model(*cache_args)

    def cleanup():
        model.unload_model("batch")

    handler_name, handler_opts = model.input_spec()

    if handler_name == "image_frames":

        def frame_loader(
            item: ItemData,
        ) -> Sequence[Tuple[Dict[str, Any], bytes]]:
            max_frames = handler_opts.get("max_frames", 4)
            frames = image_loader(conn, item)
            return [({}, frame) for frame in frames[:max_frames]]

        data_loader = frame_loader

    elif handler_name == "audio_tracks":
        sample_rate: int = handler_opts.get("sample_rate", 16000)
        max_tracks: int = handler_opts.get("max_tracks", 4)

        def audio_loader(
            item: ItemData,
        ) -> Sequence[Tuple[Dict[str, Any], bytes]]:
            if item.type.startswith("video") or item.type.startswith("audio"):
                audio = load_audio_single(item.path, sr=sample_rate)

                return [
                    ({}, serialize_array(track)) for track in audio[:max_tracks]
                ]
            return []

        data_loader = audio_loader

    elif handler_name == "extracted_text":

        def get_item_text(
            item: ItemData,
        ) -> Sequence[Tuple[Dict[str, Any], None]]:
            ids_to_text = {
                text_id: et.text
                for text_id, et in get_text_by_ids(conn, item.item_data_ids)
            }
            assert len(ids_to_text) == len(item.item_data_ids), (
                f"Text ids {item.item_data_ids} not found in "
                f"extracted text {ids_to_text.keys()}"
            )
            # Ensure that the text is in the same order as the item_data_ids
            return [
                ({"text": ids_to_text[text_id]}, None)
                for text_id in item.item_data_ids
            ]

        data_loader = get_item_text
    else:
        raise ValueError(f"Data loader {handler_name} not found")

    def batch_inference_func(
        batch: Sequence[Tuple[Dict[str, Any], bytes | None]],
    ) -> List[Dict[str, Any]] | List[bytes]:
        return model.run_batch_inference(
            *cache_args,
            [({**data, **inference_opts}, file) for data, file in batch],
        )

    if model.data_type() == "tags":

        def tag_handler(
            job_id: int,
            item: ItemData,
            _: Sequence[Any],
            outputs: Sequence[Dict[str, Any]],
        ):
            handle_tag_result(conn, job_id, model.setter_name(), item, outputs)

        result_handler = tag_handler

    elif model.data_type() == "text":

        def text_handler(
            job_id: int,
            item: ItemData,
            _: Sequence[Any],
            outputs: Sequence[Dict[str, Any]],
        ):
            handle_text(conn, job_id, model.setter_name(), item, outputs)

        result_handler = text_handler

    elif model.data_type() == "clip":

        def clip_handler(
            job_id: int,
            item: ItemData,
            _: Sequence[Any],
            embeddings: Sequence[bytes],
        ):
            handle_clip(conn, job_id, model.setter_name(), item, embeddings)

        result_handler = clip_handler

    elif model.data_type() == "text-embedding":

        def text_emb_handler(
            job_id: int,
            item: ItemData,
            _: Sequence[Any],
            embeddings: Sequence[bytes],
        ):
            handle_text_embeddings(
                conn, job_id, model.setter_name(), item, embeddings
            )

        result_handler = text_emb_handler
    else:
        raise ValueError(f"Data handler not found for {model.data_type()}")

    return run_extraction_job(
        conn,
        model,
        data_loader,
        batch_inference_func,
        result_handler,  # type: ignore
        cleanup,
        load_callback=load_model,
    )
