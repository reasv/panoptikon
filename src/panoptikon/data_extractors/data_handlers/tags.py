import json
import logging
import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np

from panoptikon.data_extractors.data_handlers.utils import from_dict
from panoptikon.data_extractors.types import JobInputData, TagResult
from panoptikon.db.extracted_text import add_extracted_text
from panoptikon.db.extraction_log import add_item_data
from panoptikon.db.tags import add_tag_to_item

logger = logging.getLogger(__name__)


def mcut_threshold(probs: np.ndarray) -> float:
    """
    Maximum Cut Thresholding (MCut)
    Largeron, C., Moulin, C., & Gery, M. (2012). MCut: A Thresholding Strategy
     for Multi-label Classification. In 11th International Symposium, IDA 2012
     (pp. 172-183).
    """
    sorted_probs = probs[probs.argsort()[::-1]]
    difs = sorted_probs[:-1] - sorted_probs[1:]
    t = difs.argmax()
    thresh = (sorted_probs[t] + sorted_probs[t + 1]) / 2
    return thresh


def combine_ns(tags: Sequence[dict[str, float]]) -> List[Tuple[str, float]]:
    combined_result = dict()
    for result in tags:
        for tag, score in result.items():
            if tag not in combined_result or score > combined_result[tag]:
                combined_result[tag] = score

    result_list = list(combined_result.items())
    result_list.sort(key=lambda x: x[1], reverse=True)
    return result_list


def get_rating(tags: Sequence[dict[str, float]], severity_order: list[str]):
    final_rating, final_score = None, 0

    # Create a dictionary to map labels to their severity
    severity_map = {label: index for index, label in enumerate(severity_order)}

    for result in tags:
        # get the highest rating in result
        rating, score = max(result.items(), key=lambda x: x[1])

        # Compare both the confidence and the severity order
        if final_rating is None or (
            severity_map.get(rating, 0) > severity_map.get(final_rating, 0)
            or (
                severity_map.get(rating, 0) == severity_map.get(final_rating, 0)
                and score > final_score
            )
        ):
            final_rating = rating
            final_score = score

    assert final_rating is not None, "No rating found"
    return final_rating, final_score


def aggregate_tags(
    namespaces_tags: Sequence[List[Tuple[str, dict[str, float]]]],
    severity_order: list[str],
) -> List[Tuple[str, str, float]]:
    combined_ns: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for namespaces_list in namespaces_tags:
        for namespace, tags in namespaces_list:
            combined_ns[namespace].append(tags)

    all_tags: List[Tuple[str, str, float]] = []
    for namespace, tags in combined_ns.items():
        if namespace == "rating":
            rating, score = get_rating(tags, severity_order)
            all_tags.append((namespace, f"rating:{rating}", score))
        else:
            all_tags.extend(
                [(namespace, tag, score) for tag, score in combine_ns(tags)]
            )

    return all_tags


def handle_tag_result(
    conn: sqlite3.Connection,
    job_id: int,
    setter_name: str,
    item: JobInputData,
    results: Sequence[Dict[str, Any]],
):
    if len(results) == 0:
        add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="tags",
            index=0,
            is_placeholder=True,
        )
        return []
    if results[0].get("skip", False):
        # This item is to be skipped, without adding placeholder data
        # This way, it will be processed again next time
        logger.info(f"Skipping item {item.sha256}")
        return []
    # Check if the results are empty or contain no tags
    total_tag_groups = sum([len(tag_result["tags"]) for tag_result in results])
    if total_tag_groups == 0:
        add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="tags",
            index=0,
            is_placeholder=True,
        )
        return []

    tag_results = [from_dict(TagResult, tag_result) for tag_result in results]
    main_namespace = tag_results[0].namespace
    rating_severity = tag_results[0].rating_severity
    tags = [
        (namespace, tag, confidence)
        for namespace, tag, confidence in aggregate_tags(
            [tag_results.tags for tag_results in tag_results],
            rating_severity,
        )
    ]
    tags_data_id = add_item_data(
        conn,
        item=item.sha256,
        setter_name=setter_name,
        job_id=job_id,
        data_type="tags",
        index=0,
        is_placeholder=len(tags) == 0,
    )
    for namespace, tag, confidence in tags:
        add_tag_to_item(
            conn,
            data_id=tags_data_id,
            namespace=f"{main_namespace}:{namespace}",
            name=tag,
            confidence=confidence,
        )

    if len(tags) == 0:
        return []
    all_tags_string = ", ".join([tag for __, tag, _ in tags])
    min_confidence = min([confidence for __, _, confidence in tags])

    text_data_id = add_item_data(
        conn,
        item=item.sha256,
        setter_name=setter_name,
        job_id=job_id,
        data_type="text",
        src_data_id=tags_data_id,
        index=0,
    )
    add_extracted_text(
        conn,
        data_id=text_data_id,
        text=all_tags_string,
        language=main_namespace,
        language_confidence=1.0,
        confidence=min_confidence,
    )
    mcut_text_data_id = None
    if tag_results[0].mcut > 0:
        # Save another tag set as text using mcut threshold on general tags
        general = [confidence for ns, _, confidence in tags if ns == "general"]
        if not general:
            return
        m_thresh = mcut_threshold(np.array(general))
        mcut_tags_string = ", ".join(
            [
                tag
                for ns, tag, confidence in tags
                if confidence >= m_thresh or ns != "general"
            ]
        )
        # During search, we can filter by this confidence value
        mcut_text_data_id = add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text",
            src_data_id=tags_data_id,
            index=1,
        )
        add_extracted_text(
            conn,
            data_id=mcut_text_data_id,
            text=mcut_tags_string,
            language=f"{main_namespace}-mcut",
            language_confidence=1.0,
            confidence=m_thresh,
        )
    # Add metadata text
    metadata_text_data_id = None
    if tag_results[0].metadata:
        metadata_text_data_id = add_item_data(
            conn,
            item=item.sha256,
            setter_name=setter_name,
            job_id=job_id,
            data_type="text",
            src_data_id=tags_data_id,
            index=2,
        )
        add_extracted_text(
            conn,
            data_id=metadata_text_data_id,
            text=json.dumps(tag_results[0].metadata),
            language=f"metadata",
            language_confidence=1.0,
            confidence=tag_results[0].metadata_score,
        )
    return [
        tags_data_id,
        text_data_id,
        *([mcut_text_data_id] if mcut_text_data_id else []),
        *([metadata_text_data_id] if tag_results[0].metadata else []),
    ]
