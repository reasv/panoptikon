import os
import base64
from io import BytesIO
from typing import List, Sequence, Type, Union

from PIL import Image as PILImage
from PIL import ImageFile

from inferio.impl.utils import clear_cache, get_device, serialize_array
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

import requests

ImageFile.LOAD_TRUNCATED_IMAGES = True


class JinaClipModel(InferenceModel):
    """
    A drop-in replacement for ClipModel that uses Jina's embeddings API.
    """

    def __init__(
        self,
        model_name: str,
        pretrained: str | None = None,
        context_length: int | None = None,
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.pretrained: str | None = pretrained
        self.context_length: int | None = context_length
        self.init_args = init_args
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

        # For example, the environment variable could be called JINA_API_KEY
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

                # We can guess mime type from the format or default to image/png
                # If the format is not recognized, fallback to "png"
                mime = image.format.lower() if image.format else "png"
                if mime == "jpeg":
                    mime = "jpg"
                image_base64 = base64.b64encode(image_bytes).decode("utf-8")
                data_url = f"data:image/{mime};base64,{image_base64}"

                image_inputs.append((idx, data_url))
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        # Prepare the payload for Jina's embeddings API
        # We keep the order for text first, then images (this is not mandatory
        # but it helps us map the indices back correctly).
        jina_input = []
        text_map = []
        image_map = []

        for idx, txt in text_inputs:
            text_map.append(idx)
            jina_input.append({"text": txt})

        for idx, img_url in image_inputs:
            image_map.append(idx)
            jina_input.append({"image": img_url})

        # If we have nothing to send, return empty right away
        if not jina_input:
            return []

        # Make the request
        api_key = os.environ["JINA_API_KEY"]  # Already checked in load()
        url = "https://api.jina.ai/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        data = {
            "model": "jina-clip-v2",
            "dimensions": 1024,
            "normalized": True,
            "embedding_type": "float",
            "input": jina_input,
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code != 200:
            raise RuntimeError(
                f"Jina API request failed with status {response.status_code}: "
                f"{response.text}"
            )

        resp_json = response.json()
        embeddings = resp_json.get("data", [])
        # embeddings is a list of dicts, each with `index` and `embedding`

        if len(embeddings) != len(jina_input):
            raise ValueError(
                "Mismatch between returned embeddings and input length."
            )

        # Map the embeddings back to the original indices
        for i, emb_data in enumerate(embeddings):
            emb_list = emb_data["embedding"]
            idx = text_map[i] if i < len(text_map) else image_map[i - len(text_map)]
            results[idx] = serialize_array(emb_list)

        # Filter out None from the results if any
        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        """
        Unload or clear any cached data. Here, we just clear local caches
        or placeholders. Jina is remote, so there's no local model to unload.
        """
        if self._model_loaded:
            clear_cache()
            self._model_loaded = False


class JinaCLIPIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[JinaClipModel]:  # type: ignore
        return JinaClipModel
