import logging
import os
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from inferio.impl.saucenao import SauceNao
from inferio.impl.saucenao.errors import (
    LongLimitReachedError,
    ShortLimitReachedError,
)
from inferio.model import InferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)


@dataclass
class DanbooruPost:
    id: int
    source: str | None
    danbooru_url: str
    tags: Dict[str, List[str]]


class DanbooruFetchError(Exception):
    pass


def get_danbooru_post(id_or_hash: str | int) -> Optional[DanbooruPost]:
    """
    Retrieves post information from Danbooru using MD5 hash.

    Args:
        md5_hash (str): MD5 hash of the image

    Returns:
        Optional[DanbooruPost]: Structured post data or None if not found
    """

    api_url = f"https://danbooru.donmai.us/posts.json"
    if isinstance(id_or_hash, int):
        params = {"tags": f"id:{id_or_hash}"}
    else:
        params = {"tags": f"md5:{id_or_hash}"}

    attempts = 0
    response = None
    while attempts <= 4:
        attempts += 1
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            break
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.info("Retrying...")
            time.sleep(1)

    if response is None:
        raise DanbooruFetchError("Failed to fetch data from Danbooru")
    try:

        posts = response.json()
        if not posts:
            return None

        post = posts[0]

        # Extract all tag categories
        tags = {
            "rating": [translate_rating(post.get("rating", "unknown"))],
            "general": post.get("tag_string_general", "").split(),
            "character": post.get("tag_string_character", "").split(),
            "copyright": post.get("tag_string_copyright", "").split(),
            "artist": post.get("tag_string_artist", "").split(),
            "meta": post.get("tag_string_meta", "").split(),
        }

        # Construct danbooru URL
        danbooru_url = f"https://danbooru.donmai.us/posts/{post['id']}"

        return DanbooruPost(
            id=post["id"],
            source=post.get("source"),
            danbooru_url=danbooru_url,
            tags=tags,
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error processing data: {e}")
        return None


def translate_rating(rating_letter: str) -> str:
    """
    Translates Danbooru's single-letter rating to full rating name.

    Args:
        rating_letter (str): Single letter rating from Danbooru

    Returns:
        str: Full rating name
    """
    ratings = {
        "g": "general",
        "s": "safe",
        "n": "sensitive",
        "q": "questionable",
        "e": "explicit",
    }
    return ratings.get(rating_letter.lower(), "unknown")


def add_confidence_level(
    tags: List[str], confidence: float
) -> Dict[str, float]:
    """
    Adds confidence level to each tag in the list.

    Args:
        tags (List[str]): List of tags
        confidence (float): Confidence level

    Returns:
        List[str]: List of tags with confidence level
    """
    return {tag: confidence for tag in tags}


class SauceNaoError(Exception):
    pass


def find_on_sauce_nao(
    image: BytesIO, threshold: float
) -> Tuple[int | None, float]:
    """
    Finds the best match for the image on SauceNAO.

    Args:
        image (BytesIO): Image to search

    Returns:
        Tuple[str, float]: Best match URL and similarity score
    """

    sauce = SauceNao(os.getenv("SAUCENAO_API_KEY"))
    attempts = 0
    results = None
    while attempts <= 4:
        attempts += 1
        try:
            results = sauce.from_file(image)
            break
        except ShortLimitReachedError as e:
            logger.error(
                "30 Seconds limit reached on SauceNAO. Waiting for 10 seconds..."
            )
            time.sleep(10)
        except LongLimitReachedError as e:
            logger.error(
                "24 hour limit reached on SauceNAO. Skipping this image this time, run the job again tomorrow..."
            )
            raise SauceNaoError("24 hour limit reached on SauceNAO")
        except Exception as e:
            logger.error(f"Error searching on SauceNAO: {e}")
            logger.info("Retrying...")
            time.sleep(1)

    if results is None:
        logger.error("Failed to search on SauceNAO")
        raise SauceNaoError("Failed to search on SauceNAO")

    best_id: int | None = None
    best_similarity = 0
    for result in results:
        similarity = float(result.raw.get("header", {}).get("similarity", "0"))
        if similarity >= threshold and similarity > best_similarity:
            if danbooru_id := result.raw.get("data", {}).get("danbooru_id"):
                best_id = int(danbooru_id)
                best_similarity = similarity
    return best_id, best_similarity


class DanbooruTagger(InferenceModel):
    def __init__(self, sauce_nao_enabled: bool = False):
        self.sauce_nao_enabled: bool = sauce_nao_enabled
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "danbooru_tagger"

    def load(self):
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        md5_inputs: List[str] = []
        images: Dict[str, BytesIO] = {}
        thresholds: Dict[str, float] = {}
        for input_item in inputs:
            if input_item.data:
                threshold = 0.5
                if isinstance(input_item.data, dict):
                    md5 = input_item.data.get("md5", None)
                    threshold = input_item.data.get("threshold", 0.5)
                else:
                    md5 = input_item.data
                md5_inputs.append(md5)
                thresholds[md5] = threshold
                if input_item.file:
                    images[md5] = BytesIO(input_item.file)
            else:
                raise ValueError("Danbooru requires md5 hashes")

        logger.debug(
            f"Running danbooru tag matching on {len(md5_inputs)} images"
        )
        outputs: List[dict] = []
        for md5 in md5_inputs:
            item_confidence = 1
            try:
                post = get_danbooru_post(md5)
            except DanbooruFetchError:
                logger.warning(f"Skipping {md5} after Danbooru fetch failed.")
                # Ensures Panoptikon will try this image again next time
                outputs.append({"skip": True})
                continue
            if not post:
                logger.debug(f"Post not found for md5: {md5}")
                if self.sauce_nao_enabled and md5 in images:
                    if not os.getenv("SAUCENAO_API_KEY"):
                        raise ValueError(
                            "SAUCENAO_API_KEY environment variable must be set for SauceNAO search"
                        )
                    logger.debug(f"Searching on SauceNAO for md5: {md5}")
                    try:
                        danbooru_id, confidence = find_on_sauce_nao(
                            images[md5], thresholds[md5]
                        )
                    except SauceNaoError:
                        logger.warning(
                            f"Skipping {md5} after SauceNAO search failed."
                        )
                        # Ensures Panoptikon will try this image again next time
                        outputs.append({"skip": True})
                        continue

                    if danbooru_id:
                        logger.info(
                            f"Found Danbooru ID for md5 {md5}: https://danbooru.donmai.us/posts/{danbooru_id}"
                        )
                        try:
                            post = get_danbooru_post(danbooru_id)
                        except DanbooruFetchError:
                            logger.warning(
                                f"Skipping {md5} after Danbooru fetch failed."
                            )
                            # Ensures Panoptikon will try this image again next time
                            outputs.append({"skip": True})
                            continue
                        item_confidence = confidence
                if not post:
                    logger.warning(f"Failed to find {md5} through SauceNAO")
                    # Not found. Will not be retried next time
                    outputs.append(
                        {
                            "namespace": "danbooru",
                            "tags": [],
                        }
                    )
                    continue

            logger.debug(f"Post: {post.danbooru_url}")
            outputs.append(
                {
                    "namespace": "danbooru",
                    "tags": [
                        (
                            "rating",
                            add_confidence_level(
                                post.tags["rating"], item_confidence
                            ),
                        ),
                        (
                            "character",
                            add_confidence_level(
                                post.tags["character"], item_confidence
                            ),
                        ),
                        (
                            "general",
                            add_confidence_level(
                                post.tags["general"], item_confidence
                            ),
                        ),
                        (
                            "artist",
                            add_confidence_level(
                                post.tags["artist"], item_confidence
                            ),
                        ),
                        (
                            "meta",
                            add_confidence_level(
                                post.tags["meta"], item_confidence
                            ),
                        ),
                    ],
                    "mcut": 0.0,
                    "rating_severity": [
                        "general",
                        "safe",
                        "sensitive",
                        "questionable",
                        "explicit",
                    ],
                    "metadata_score": item_confidence,
                    "metadata": {
                        "source_url": post.source,
                        "danbooru_url": post.danbooru_url,
                    },
                }
            )

        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            self._model_loaded = False

    def __del__(self):
        if self._model_loaded:
            logger.debug(f"Model danbooru deleted")
        self.unload()


@dataclass
class TagResult:
    rating: Dict[str, float]
    character: Dict[str, float]
    general: Dict[str, float]
    character_mcut: float
    general_mcut: float
