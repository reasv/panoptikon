from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from inferio.model import InferenceModel
from inferio.types import PredictionInput


@dataclass
class DanbooruPost:
    id: int
    md5: str
    rating: str
    source: str
    danbooru_url: str
    tags: Dict[str, List[str]]


def get_danbooru_post_by_md5(md5_hash: str) -> Optional[DanbooruPost]:
    """
    Retrieves post information from Danbooru using MD5 hash.

    Args:
        md5_hash (str): MD5 hash of the image

    Returns:
        Optional[DanbooruPost]: Structured post data or None if not found
    """

    api_url = f"https://danbooru.donmai.us/posts.json"
    params = {"tags": f"md5:{md5_hash}"}

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        posts = response.json()
        if not posts:
            return None

        post = posts[0]

        # Extract all tag categories
        tags = {
            "rating": [translate_rating(post.get("rating", ""))],
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
            md5=post["md5"],
            rating=post["rating"],
            source=post["source"],
            danbooru_url=danbooru_url,
            tags=tags,
        )

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except (KeyError, IndexError, ValueError) as e:
        print(f"Error processing data: {e}")
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


import logging

logger = logging.getLogger(__name__)


class DanbooruTagger(InferenceModel):
    def __init__(self):
        self._model_loaded = False

    @classmethod
    def name(cls) -> str:
        return "danbooru_tagger"

    def load(self):
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        image_inputs: List[str] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for input_item in inputs:
            if input_item.data:
                if isinstance(input_item.data, dict):
                    md5 = input_item.data.get("md5", None)
                else:
                    md5 = input_item.data
                image_inputs.append(md5)
            else:
                raise ValueError("Danbooru requires md5 hashes")

        logger.debug(
            f"Running danbooru tag matching on {len(image_inputs)} images"
        )
        outputs: List[dict] = []
        for md5 in image_inputs:
            post = get_danbooru_post_by_md5(md5)
            if not post:
                logger.debug(f"Post not found for md5: {md5}")
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
                            add_confidence_level(post.tags["rating"], 1),
                        ),
                        (
                            "character",
                            add_confidence_level(post.tags["character"], 1),
                        ),
                        (
                            "general",
                            add_confidence_level(post.tags["general"], 1),
                        ),
                        (
                            "artist",
                            add_confidence_level(post.tags["artist"], 1),
                        ),
                        (
                            "meta",
                            add_confidence_level(post.tags["meta"], 1),
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
