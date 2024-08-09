from __future__ import annotations

import logging
import sqlite3
from typing import Dict, List, Sequence, Tuple

import numpy as np
import PIL.Image

from src.data_extractors.ai.wd_tagger import Predictor, mcut_threshold
from src.data_extractors.data_loaders.images import item_image_loader_pillow
from src.data_extractors.extraction_jobs import run_extraction_job
from src.data_extractors.models import TagsModel
from src.db.extracted_text import insert_extracted_text
from src.db.tags import add_tag_to_item
from src.types import ItemWithPath

logger = logging.getLogger(__name__)


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


def aggregate_results(
    results: List[tuple[dict[str, float], dict[str, float], dict[str, float]]]
):
    if len(results) == 1:
        return translate_tags_result(*results[0])
    # Combine all results into a single result for each category
    rating_res, character_res, general_res = zip(*results)
    return translate_tags_result(
        combine_results(list(rating_res)),
        combine_results(list(character_res)),
        combine_results(list(general_res)),
    )


def translate_tags_result(
    rating_res: dict[str, float],
    character_res: dict[str, float],
    general_res: dict[str, float],
):
    # Pick the highest rated tag
    rating, rating_confidence = max(rating_res.items(), key=lambda x: x[1])
    rating_tag = "rating:" + rating
    general_res[rating_tag] = rating_confidence
    return character_res, general_res


def handle_individual_result(
    conn: sqlite3.Connection,
    log_id: int,
    setter: str,
    item: ItemWithPath,
    results: Sequence[
        Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]
    ],
):
    character_res, general_rating_res = aggregate_results(list(results))

    chars = [(tag, confidence) for tag, confidence in character_res.items()]
    chars.sort(key=lambda x: x[1], reverse=True)
    general = [
        (tag, confidence)
        for tag, confidence in general_rating_res.items()
        if not tag.startswith("rating:")
    ]
    general.sort(key=lambda x: x[1], reverse=True)
    rating = [
        (tag, confidence)
        for tag, confidence in general_rating_res.items()
        if tag.startswith("rating:")
    ]
    tags = (
        [("danbooru:character", tag, confidence) for tag, confidence in chars]
        + [("danbooru:general", tag, confidence) for tag, confidence in general]
        + [("danbooru:rating", tag, confidence) for tag, confidence in rating]
    )
    for namespace, tag, confidence in tags:
        add_tag_to_item(
            conn,
            namespace=namespace,
            name=tag,
            sha256=item.sha256,
            setter=setter,
            confidence=confidence,
            log_id=log_id,
        )

    all_tags_string = ", ".join([tag for tag, _ in rating + chars + general])
    min_confidence = min([confidence for _, confidence in general])

    insert_extracted_text(
        conn,
        item.sha256,
        0,
        log_id=log_id,
        text=all_tags_string,
        language="danbooru",
        language_confidence=1.0,
        confidence=min_confidence,
    )

    # Save another tag set as text using mcut threshold
    m_thresh = mcut_threshold(
        np.array([confidence for _, confidence in general])
    )
    new_general = [
        (tag, confidence)
        for tag, confidence in general
        if confidence >= m_thresh
    ]
    mcut_tags_string = ", ".join(
        [tag for tag, _ in rating + chars + new_general]
    )
    # During search, we can filter by this confidence value
    insert_extracted_text(
        conn,
        item.sha256,
        1,
        log_id=log_id,
        text=mcut_tags_string,
        language="danbooru-mcut",
        language_confidence=1.0,
        confidence=m_thresh,
    )


def run_tag_extractor_job(conn: sqlite3.Connection, model: TagsModel):
    """
    Run a job that processes items in the database using the given tagging model.
    """
    score_threshold = model.get_group_threshold(conn)
    logger.info(f"Using score threshold {score_threshold}")
    tag_predictor = Predictor(model_repo=model.model_repo())
    tag_predictor.load_model()

    def load_images(item: ItemWithPath):
        return item_image_loader_pillow(conn, item)

    def batch_inference_func(batch_images: Sequence[PIL.Image.Image]):
        return tag_predictor.predict(
            batch_images, general_thresh=score_threshold, character_thresh=None
        )

    def handle_result(
        log_id: int,
        item: ItemWithPath,
        _: Sequence[PIL.Image.Image],
        outputs: Sequence[
            Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]
        ],
    ):
        handle_individual_result(
            conn, log_id, model.setter_name(), item, outputs
        )

    return run_extraction_job(
        conn,
        model,
        load_images,
        batch_inference_func,
        handle_result,
    )
