import os
import base64
import json
import numpy as np
from io import BytesIO
from typing import List, Sequence, Type, Union

from PIL import Image as PILImage
from PIL import ImageFile

from inferio.impl.utils import serialize_array
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput
import logging

logger = logging.getLogger(__name__)
import requests

ImageFile.LOAD_TRUNCATED_IMAGES = True


class JinaClipModel(InferenceModel):
    """
    A drop-in replacement for CLIP that uses Jina's embeddings API.
    """

    def __init__(
        self,
        model_name: str = "jina-clip-v2",
        **kwargs: Union[int, str, bool],
    ):
        """
        :param model_name: Name of the Jina model to use, e.g. "jina-clip-v2".
        :param dimensions: The number of dimensions in the returned embeddings.
        :param normalized: Whether or not the embeddings should be normalized.
        :param embedding_type: The numeric type of the embeddings: "float" or "int".
        """
        self.model_name = model_name
        self.model_config = kwargs

        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "jina-clip-api"

    def load(self) -> None:
        """
        Jina doesn't require a local model load. Instead, we only check if
        the API key environment variable is available.
        """
        if self._model_loaded:
            return

        # Ensure the JINA_API_KEY environment variable is set
        api_key = os.environ.get("JINA_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "JINA_API_KEY environment variable not found. "
                "Please set it before using JinaClipModel."
            )

        self._model_loaded = True

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        """
        Sends inputs to Jina's embeddings API and returns serialized embeddings
        as bytes, analogous to the CLIP local inference version.
        """
        # Ensure model (API key presence) is 'loaded'
        self.load()

        results: List[None | bytes] = [None] * len(inputs)

        text_inputs = []
        image_inputs = []

        # Separate text and image inputs, storing their original indices
        for idx, input_item in enumerate(inputs):
            if input_item.file:
                # Convert raw bytes to base64 data URL
                image_bytes = input_item.file
                image = PILImage.open(BytesIO(image_bytes)).convert("RGB")

                # If the format is unknown, fallback to 'png'
                mime = image.format.lower() if image.format else "png"
                if mime == "jpeg":
                    mime = "jpg"

                # Create data URL
                image_base64 = base64.b64encode(image_bytes).decode("utf-8")
                image_inputs.append((idx, image_base64))
            else:
                # Expect a dict with key 'text'
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        # Prepare the payload for Jina's embeddings API
        # Keep track of text vs. image ordering
        jina_input = []
        text_map = []
        image_map = []
        

        for idx, txt in text_inputs:
            text_map.append(idx)
            jina_input.append({"text": txt})

        for idx, img_url in image_inputs:
            image_map.append(idx)
            jina_input.append({"image": img_url})

        # If nothing to send, return empty
        if not jina_input:
            return []

        # Make the request
        api_key = os.environ["JINA_API_KEY"]  # already checked in load()
        url = "https://api.jina.ai/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "Panoptikon/0.1.0 (https://github.com/reasv/panoptikon)",
        }
        data = {
            "model": self.model_name,
            **self.model_config,
            "input": jina_input,
        }

        # Attempt the request up to 3 times if we detect
        # network failures or 5xx server errors
        max_retries = int(os.environ.get("JINA_MAX_RETRIES", 3))
        timeout = int(os.environ.get("JINA_TIMEOUT", 0)) or None
        response = None
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, data=json.dumps(data), timeout=timeout)
                # If successful, break
                if response.status_code == 200:
                    break
                # If 5xx, try again (unless last attempt)
                if 500 <= response.status_code < 600:
                    if attempt < max_retries - 1:
                        continue
                    else:
                        # Out of attempts
                        response.raise_for_status()
                else:
                    logger.error(f"Error response returned by Jina API: {response.json()}")
                    # It's not 5xx, so let's raise
                    response.raise_for_status()
            except (requests.ConnectionError, requests.Timeout) as e:
                # Connection-related errors -> retry if not exhausted
                if attempt < max_retries - 1:
                    continue
                else:
                    raise RuntimeError(f"Request failed after {max_retries} attempts: {str(e)}")
            except requests.RequestException as e:
                # Some other request error, do not retry
                raise RuntimeError(
                    f"Request to Jina embeddings API failed: {str(e)}"
                ) from e

        if response is None:
            raise RuntimeError("Request could not be completed and did not raise an exception.")

        resp_json = response.json()
        # Each item in resp_json["data"] should have { "index": i, "embedding": [...] }
        embeddings = resp_json.get("data", [])

        if len(embeddings) != len(jina_input):
            raise ValueError(
                "Mismatch between returned embeddings and input length. "
                f"Got {len(embeddings)} embeddings vs {len(jina_input)} inputs."
            )

        # Map the embeddings back to the original indices
        for i, emb_data in enumerate(embeddings):
            emb_list = emb_data["embedding"]
            # Convert from list -> np.array
            emb_array = np.array(emb_list, dtype=np.float32)

            # text_map and image_map combine in order:
            # first part is text indices, second part is image indices
            if i < len(text_map):
                idx = text_map[i]
            else:
                idx = image_map[i - len(text_map)]

            # Now store the serialized array
            results[idx] = serialize_array(emb_array)

        # Filter out None from the results if any
        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        """
        Jina is remote, so there's no local model to unload.
        """
        if self._model_loaded:
            self._model_loaded = False


class JinaCLIPIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[JinaClipModel]:  # type: ignore
        return JinaClipModel
