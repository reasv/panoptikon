import os
import base64
import numpy as np
from io import BytesIO
from typing import List, Sequence, Type, Union

from PIL import Image as PILImage
from PIL import ImageFile

from inferio.impl.utils import serialize_array
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

import requests

ImageFile.LOAD_TRUNCATED_IMAGES = True


class JinaClipModel(InferenceModel):
    """
    A drop-in replacement for CLIP that uses Jina's embeddings API.
    """

    def __init__(
        self,
        model_name: str = "jina-clip-v2",
        dimensions: int = 1024,
        normalized: bool = True,
        embedding_type: str = "float",
    ):
        """
        :param model_name: Name of the Jina model to use, e.g. "jina-clip-v2".
        :param dimensions: The number of dimensions in the returned embeddings.
        :param normalized: Whether or not the embeddings should be normalized.
        :param embedding_type: The numeric type of the embeddings: "float" or "int".
        """
        self.model_name = model_name
        self.dimensions = dimensions
        self.normalized = normalized
        self.embedding_type = embedding_type

        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "jina-clip"

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
                data_url = f"data:image/{mime};base64,{image_base64}"

                image_inputs.append((idx, data_url))
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
        }
        data = {
            "model": self.model_name,
            "dimensions": self.dimensions,
            "normalized": self.normalized,
            "embedding_type": self.embedding_type,
            "input": jina_input,
        }

        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            raise RuntimeError(
                f"Jina API request failed with status {response.status_code}: "
                f"{response.text}"
            )

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
