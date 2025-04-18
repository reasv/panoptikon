import asyncio
import logging
import os
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Optional, Sequence, Tuple, Type

import aiohttp

from inferio.impl.saucenao.containers import SauceResponse
from inferio.impl.saucenao.errors import (
    LongLimitReachedError,
    ShortLimitReachedError,
)
from inferio.impl.saucenao.saucenao_api import AIOSauceNao
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT = 15
TOTAL_TIMEOUT = 75

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

    attempt = 0
    posts = None
    while attempt <= 4:
        try:
            async with session.get(api_url, params=params, timeout=aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=TOTAL_TIMEOUT)) as response:
                response.raise_for_status()
                posts = await response.json()
                break
        except Exception as e:
            logger.error(f"Error fetching data from danbooru: {e}")

        wait_time = 2 ** attempt
        logger.info(f"Retrying in {wait_time}s...")
        await asyncio.sleep(wait_time)
        attempt += 1
        if attempt > 4:
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

_RATINGS = {
        "g": "general",
        "s": "safe",
        "n": "sensitive",
        "q": "questionable",
        "e": "explicit",
}

def translate_rating(rating_letter: str) -> str:
    """
    Translates Danbooru's single-letter rating to full rating name.

    Args:
        rating_letter (str): Single letter rating from Danbooru

    Returns:
        str: Full rating name
    """

    return _RATINGS.get(rating_letter.lower(), "unknown")


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
        attempt = 0
        results: SauceResponse | None = None
        while attempt <= 4:
            try:
                results = await sauce.from_file(BytesIO(image))
                break
            except ShortLimitReachedError:
                logger.error(
                    "30 Seconds limit reached on SauceNAO. Waiting for 31 seconds..."
                )
                await asyncio.sleep(31)
            except LongLimitReachedError:
                # Propagate the error to stop trying SauceNAO for other images
                raise LongLimitReachedError
            except Exception as e:
                logger.error(f"Error searching on SauceNAO: {e}")
            
            if attempt > 4:
                raise SauceNaoError("Failed to search on SauceNAO")
            logger.info("Retrying SauceNAO search...")
            await asyncio.sleep(1)
            attempt += 1

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
        self.saucenao_daily_limit_reached: bool = False
        self.limit_reached_time: Optional[float] = None
        self.last_saucenao_request_time: Optional[float] = None

    @classmethod
    def name(cls) -> str:
        return "danbooru_tagger"

    def load(self):
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        
        if self.sauce_nao_enabled and not os.getenv("SAUCENAO_API_KEY"):
            raise ValueError(
                "SAUCENAO_API_KEY environment variable must be set for SauceNAO search"
            )
        
        self.reset_limit_reached()

        return asyncio.run(self.predict_async(inputs))
    
    async def predict_async(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        async with aiohttp.ClientSession() as session:
            md5_inputs: List[str] = []
            images: Dict[str, bytes] = {}
            thresholds: Dict[str, float] = {}

            for input_item in inputs:
                if input_item.data:
                    threshold = 0.5
                    if isinstance(input_item.data, dict):
                        md5 = input_item.data.get("md5", None)
                        assert md5, "Danbooru requires md5 hashes"
                        threshold = input_item.data.get("threshold", 0.5)
                    else:
                        md5 = input_item.data
                    md5_inputs.append(md5)
                    thresholds[md5] = threshold
                    if input_item.file:
                        images[md5] = input_item.file
                    elif self.sauce_nao_enabled:
                        raise ValueError(
                            f"SauceNAO requires image data to be provided (md5: {md5})"
                        )
                else:
                    raise ValueError("Danbooru requires md5 hashes")

            logger.debug(
                f"Running danbooru tag matching on {len(md5_inputs)} images"
            )
            # First attempt to find the image on Danbooru
            tasks = []
            for md5 in md5_inputs:
                task = self.try_danbooru_async(md5, session)
                tasks.append(task)
            # Wait for all tasks to complete while preserving order
            results: List[dict | None] = await asyncio.gather(*tasks)

            if not self.sauce_nao_enabled:
                # If SauceNAO is not enabled, return results directly
                def process_none(result: dict | None) -> dict:
                    if result is None:
                        return {"namespace": "danbooru", "tags": []}
                    return result
                return [process_none(result) for result in results]

            # If SauceNAO is enabled, try to find the image on SauceNAO for any None results
            # Due to SauceNAO's low rate limit, we do not use it concurrently
            final_results: List[dict] = []
            sauce_nao_key = os.getenv("SAUCENAO_API_KEY")
            assert sauce_nao_key, "SAUCENAO_API_KEY environment variable must be set for SauceNAO search"
            for i, result in enumerate(results):
                if result is not None:
                    # Already found on Danbooru, add to final results
                    final_results.append(result)
                    continue
                
                if self.saucenao_daily_limit_reached:
                    # Stop trying SauceNAO for other images
                    final_results.append({"skip": True})
                    continue
                # If the result is None, try SauceNAO
                md5 = md5_inputs[i]
                try:
                    sn_result = await self.try_sauce_nao_async(
                        md5_inputs[i],
                        images[md5],
                        thresholds[md5],
                        sauce_nao_key,
                        session,
                    )
                except LongLimitReachedError:
                    # Stop trying SauceNAO for other images
                    logger.error(
                        "24 hour limit reached on SauceNAO. Stopping further searches."
                    )
                    self.register_limit_reached()
                    final_results.append({"skip": True})
                    continue
                final_results.append(sn_result)

            return final_results

    async def try_sauce_nao_async(
        self,
        md5: str,
        image: bytes,
        threshold: float,
        saucenao_api_key: str,
        session: aiohttp.ClientSession,) -> dict:
        """
        Finds the image hash on SauceNAO.
        """
        logger.debug(f"Searching on SauceNAO for md5: {md5}")
        if self.last_saucenao_request_time is not None:
            elapsed_time = time.time() - self.last_saucenao_request_time
            time_between_requests = 1.77 # SauceNAO's rate limit is 1 request every ~1.76 seconds (17 in 30 seconds)
            if elapsed_time < time_between_requests:
                wait_time = time_between_requests - elapsed_time
                logger.debug(
                    f"Waiting for {wait_time:.2f} seconds before next SauceNAO request."
                )
                await asyncio.sleep(wait_time)
        try:
            danbooru_id, confidence = await find_on_sauce_nao_async(
                image, threshold, saucenao_api_key=saucenao_api_key
            )
        except LongLimitReachedError:
            # Propagate the error to stop trying SauceNAO for other images
            raise LongLimitReachedError
        except SauceNaoError:
            logger.warning(
                f"Skipping {md5} after SauceNAO search failed."
            )
            return {"skip": True}
        finally:
            self.last_saucenao_request_time = time.time()

        if not danbooru_id:
            logger.info(f"Failed to find {md5} through SauceNAO")
            return {"namespace": "danbooru", "tags": []}

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
        if not post:
            logger.warning(f"Post found on SauceNAO but not on Danbooru for md5: {md5} (Danbooru Link: https://danbooru.donmai.us/posts/{danbooru_id})")
            return {"namespace": "danbooru", "tags": []}

        logger.debug(f"Post: {post.danbooru_url} (md5: {md5})")
        return self.process_item_result(md5, post, confidence)
    
    async def try_danbooru_async(self,
        md5: str,
        session: aiohttp.ClientSession
    ) -> dict | None:
        """
        Finds the image hash on Danbooru.
        """
        try:
            post = await get_danbooru_post_async(md5, session)
        except DanbooruFetchError:
            logger.warning(f"Skipping {md5} after Danbooru fetch failed.")
            return {"skip": True}
        
        if not post:
            logger.debug(f"Post not found on danbooru for md5: {md5}")
            return None
        
        logger.debug(f"Post: {post.danbooru_url} (md5: {md5})")
    
        return self.process_item_result(md5, post, 1.0)

    def process_item_result(self, md5: str, post: DanbooruPost, confidence: float) -> dict:
        """
        Processes the result from Danbooru and returns a structured response.
        """
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
                    add_confidence_level(post.tags["rating"], confidence),
                ),
                (
                    "character",
                    add_confidence_level(
                        post.tags["character"], confidence
                    ),
                ),
                (
                    "general",
                    add_confidence_level(post.tags["general"], confidence),
                ),
                (
                    "artist",
                    add_confidence_level(post.tags["artist"], confidence),
                ),
                (
                    "meta",
                    add_confidence_level(post.tags["meta"], confidence),
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
            "metadata_score": confidence,
            "metadata": metadata,
        }
    
    def register_limit_reached(self) -> None:
        """
        Register that the SauceNAO daily limit has been reached.
        """
        self.saucenao_daily_limit_reached = True
        self.limit_reached_time = time.time()

    def reset_limit_reached(self) -> None:
        """
        Reset the SauceNAO daily limit reached status if the time since it was reached is greater than 3 hours.
        """
        if self.limit_reached_time:
            elapsed_time = time.time() - self.limit_reached_time
            if elapsed_time > 3 * 60 * 60:
                logger.debug(
                    "Resetting SauceNAO daily limit reached status after 3 hours."
                )
                self.saucenao_daily_limit_reached = False
                self.limit_reached_time = None
            else:
                logger.debug(
                    f"Still within 3 hours of SauceNAO daily limit reached. Elapsed time: {elapsed_time} seconds."
                )

    def unload(self) -> None:
        if self._model_loaded:
            self._model_loaded = False

class DanbooruIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[DanbooruTagger]:  # type: ignore
        return DanbooruTagger
