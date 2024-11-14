import asyncio
import logging
import os
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Optional, Sequence, Tuple

import aiohttp

from inferio.impl.saucenao.errors import (
    LongLimitReachedError,
    ShortLimitReachedError,
)
from inferio.impl.saucenao.saucenao_api import AIOSauceNao
from inferio.model import InferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)


@dataclass
class DanbooruPost:
    id: int
    source: str | None
    pixiv_url: str | None
    danbooru_url: str
    tags: Dict[str, List[str]]


class DanbooruFetchError(Exception):
    pass


async def get_danbooru_post_async(
    id_or_hash: str | int, session: aiohttp.ClientSession
) -> Optional[DanbooruPost]:
    """
    Retrieves post information from Danbooru using MD5 hash.

    Args:
        md5_hash (str): MD5 hash of the image
        session (aiohttp.ClientSession): Aiohttp session

    Returns:
        Optional[DanbooruPost]: Structured post data or None if not found
    """
    api_url = f"https://danbooru.donmai.us/posts.json"
    if isinstance(id_or_hash, int):
        params = {"tags": f"id:{id_or_hash}"}
    else:
        params = {"tags": f"md5:{id_or_hash}"}

    attempts = 0
    posts = None
    while attempts <= 4:
        attempts += 1
        try:
            async with session.get(api_url, params=params) as response:
                response.raise_for_status()
                posts = await response.json()
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            if attempts <= 4:
                logger.info("Retrying...")
                await asyncio.sleep(2 * attempts)
            else:
                raise DanbooruFetchError("Failed to fetch data from Danbooru")
    try:
        if not posts:
            return None

        post = posts[0]

        tags = {
            "rating": [translate_rating(post.get("rating", "unknown"))],
            "general": post.get("tag_string_general", "").split(),
            "character": post.get("tag_string_character", "").split(),
            "copyright": post.get("tag_string_copyright", "").split(),
            "artist": post.get("tag_string_artist", "").split(),
            "meta": post.get("tag_string_meta", "").split(),
        }

        pixiv_id = post.get("pixiv_id")
        pixiv_url = (
            f"https://www.pixiv.net/en/artworks/{pixiv_id}"
            if pixiv_id
            else None
        )
        danbooru_url = f"https://danbooru.donmai.us/posts/{post['id']}"

        return DanbooruPost(
            id=post["id"],
            source=post.get("source"),
            danbooru_url=danbooru_url,
            tags=tags,
            pixiv_url=pixiv_url,
        )
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


async def find_on_sauce_nao_async(
    image: bytes, threshold: float, saucenao_api_key: str
) -> Tuple[int | None, float]:
    """
    Finds the best match for the image on SauceNAO.

    Args:
        image (BytesIO): Image to search
        session (aiohttp.ClientSession): Aiohttp session

    Returns:
        Tuple[str, float]: Best match URL and similarity score
    """
    async with AIOSauceNao(api_key=saucenao_api_key) as sauce:
        attempts = 0
        results = None
        while attempts <= 4:
            attempts += 1
            try:
                results = await sauce.from_file(BytesIO(image))
                break
            except ShortLimitReachedError:
                logger.error(
                    "30 Seconds limit reached on SauceNAO. Waiting for 31 seconds..."
                )
                await asyncio.sleep(31)
            except LongLimitReachedError:
                logger.error("24 hour limit reached on SauceNAO...")
                raise SauceNaoError("24 hour limit reached on SauceNAO")
            except Exception as e:
                logger.error(f"Error searching on SauceNAO: {e}")
                if attempts <= 4:
                    logger.info("Retrying...")
                    await asyncio.sleep(1)

    if results is None:
        raise SauceNaoError("Failed to search on SauceNAO")

    best_id: int | None = None
    best_similarity = 0
    for result in results:
        similarity = (
            float(result.raw.get("header", {}).get("similarity", "0")) / 100
        )
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

        async def process_all():
            sauce = None
            if self.sauce_nao_enabled:
                if not os.getenv("SAUCENAO_API_KEY"):
                    raise ValueError(
                        "SAUCENAO_API_KEY environment variable must be set for SauceNAO search"
                    )
                sauce = os.getenv("SAUCENAO_API_KEY")

            async with aiohttp.ClientSession() as session:
                md5_inputs: List[str] = []
                images: Dict[str, bytes] = {}
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
                            images[md5] = input_item.file
                    else:
                        raise ValueError("Danbooru requires md5 hashes")

                logger.debug(
                    f"Running danbooru tag matching on {len(md5_inputs)} images"
                )

                tasks = []
                for md5 in md5_inputs:
                    task = self.process_item_async(
                        md5,
                        thresholds[md5],
                        images.get(md5),
                        session,
                        sauce,
                    )
                    tasks.append(task)
                if self.sauce_nao_enabled:
                    results = []
                    for task in tasks:
                        results.append(await task)
                else:
                    # Wait for all tasks to complete while preserving order
                    results = await asyncio.gather(*tasks)
                return results

        return asyncio.run(process_all())

    async def process_item_async(
        self,
        md5: str,
        threshold: float,
        image: bytes | None,
        session: aiohttp.ClientSession,
        sauce: str | None = None,
    ) -> dict:
        item_confidence = 1
        try:
            post = await get_danbooru_post_async(md5, session)
        except DanbooruFetchError:
            logger.warning(f"Skipping {md5} after Danbooru fetch failed.")
            return {"skip": True}

        if not post:
            logger.debug(f"Post not found for md5: {md5}")
            if self.sauce_nao_enabled and image is not None:
                assert sauce is not None, "SauceNAO instance must be provided"
                logger.debug(f"Searching on SauceNAO for md5: {md5}")
                try:
                    danbooru_id, confidence = await find_on_sauce_nao_async(
                        image, threshold, sauce
                    )
                except SauceNaoError:
                    logger.warning(
                        f"Skipping {md5} after SauceNAO search failed."
                    )
                    return {"skip": True}

                if danbooru_id:
                    logger.info(
                        f"Found Danbooru ID for md5 {md5}: https://danbooru.donmai.us/posts/{danbooru_id}"
                    )
                    try:
                        post = await get_danbooru_post_async(
                            danbooru_id, session
                        )
                    except DanbooruFetchError:
                        logger.warning(
                            f"Skipping {md5} after Danbooru fetch failed."
                        )
                        return {"skip": True}
                    item_confidence = confidence

            if not post:
                if self.sauce_nao_enabled:
                    logger.warning(f"Failed to find {md5} through SauceNAO")
                return {"namespace": "danbooru", "tags": []}

        logger.debug(f"Post: {post.danbooru_url} (md5: {md5})")
        metadata = {
            "source_url": post.source,
            "danbooru_url": post.danbooru_url,
        }
        if post.pixiv_url:
            metadata["pixiv_url"] = post.pixiv_url

        return {
            "namespace": "danbooru",
            "tags": [
                (
                    "rating",
                    add_confidence_level(post.tags["rating"], item_confidence),
                ),
                (
                    "character",
                    add_confidence_level(
                        post.tags["character"], item_confidence
                    ),
                ),
                (
                    "general",
                    add_confidence_level(post.tags["general"], item_confidence),
                ),
                (
                    "artist",
                    add_confidence_level(post.tags["artist"], item_confidence),
                ),
                (
                    "meta",
                    add_confidence_level(post.tags["meta"], item_confidence),
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
            "metadata": metadata,
        }

    def unload(self) -> None:
        if self._model_loaded:
            self._model_loaded = False

    def __del__(self):
        if self._model_loaded:
            logger.debug(f"Model danbooru deleted")
        self.unload()
