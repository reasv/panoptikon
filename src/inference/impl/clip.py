from io import BytesIO
from typing import List, Sequence, Union

from PIL import Image as PILImage

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput


class ClipModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        pretrained: str | None = None,
        context_length: int | None = None,
        **kwargs,
    ):
        self.model_name: str = model_name
        self.pretrained: str | None = pretrained
        self.context_length: int | None = context_length
        self.init_args = kwargs
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "openclip"

    def load(self) -> None:
        if self._model_loaded:
            return
        import open_clip

        self.model, _, preprocess = open_clip.create_model_and_transforms(
            model_name=self.model_name,
            pretrained=self.pretrained,
            **self.init_args,
        )
        assert not isinstance(
            preprocess, tuple
        ), "Expected single preprocess function"
        self.preprocess = preprocess

        self.devices = get_device()
        self.device = (
            self.devices[0] if isinstance(self.devices, list) else self.devices
        )
        self.model.eval().to(self.device)
        self.tokenizer = open_clip.get_tokenizer(
            model_name=self.model_name, context_length=self.context_length
        )
        self._model_loaded = True

    def __del__(self):
        self.unload()

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        import torch

        # Ensure the model is loaded
        self.load()

        text_inputs = []
        image_inputs = []
        results: List[None | bytes] = [None] * len(inputs)

        # Separate text and image inputs, storing their original indices
        for idx, input_item in enumerate(inputs):
            if isinstance(input_item.data, str):
                text_inputs.append((idx, input_item.data))
            elif input_item.file:
                image = PILImage.open(BytesIO(input_item.file)).convert("RGB")
                image_inputs.append((idx, image))

        # Use inference_mode for optimized inference
        with torch.inference_mode():
            # Process text inputs if any
            if text_inputs:
                indices, texts = zip(*text_inputs)
                tokens = self.tokenizer(list(texts))
                tokens = torch.tensor(tokens).to(self.device)

                text_features = self.model.encode_text(tokens)
                text_features /= text_features.norm(
                    dim=-1, keepdim=True
                )  # Normalize the text features

                # Convert text features to list and store them in the results list
                for i, idx in enumerate(indices):
                    results[idx] = text_features[i].cpu().numpy().tobytes()

            # Process image inputs if any
            if image_inputs:
                indices, images = zip(*image_inputs)
                processed_images = torch.stack(
                    [
                        self.preprocess(img).to(self.device)  # type: ignore
                        for img in images
                    ]
                )

                image_features = self.model.encode_image(processed_images)
                image_features /= image_features.norm(
                    dim=-1, keepdim=True
                )  # Normalize the image features

                # Convert image features to list and store them in the results list
                for i, idx in enumerate(indices):
                    results[idx] = image_features[i].cpu().numpy().tobytes()

        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.tokenizer
            del self.preprocess
            clear_cache()
            self._model_loaded = False
