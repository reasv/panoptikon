from io import BytesIO
from typing import List, Sequence, Union

from PIL import Image as PILImage

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.registry import ModelRegistry
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

    def load(self) -> None:
        if self._model_loaded:
            return
        import open_clip
        import torch

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

        if isinstance(self.devices, list):
            # If multiple devices are available, use DataParallel to handle distribution
            self.model = torch.nn.DataParallel(
                self.model, device_ids=self.devices
            )
            self.device = self.devices[0]  # Primary device
        else:
            self.device = self.devices

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

                if isinstance(self.devices, list):
                    # Split the tokens across devices if multiple GPUs are available
                    token_batches = torch.split(
                        tokens, max(1, len(tokens) // len(self.devices))
                    )
                    text_features = []

                    for i, token_batch in enumerate(token_batches):
                        token_batch = token_batch.to(
                            self.devices[i % len(self.devices)]
                        )
                        text_features_batch = self.model.module.encode_text(
                            token_batch
                        )
                        text_features_batch /= text_features_batch.norm(
                            dim=-1, keepdim=True
                        )
                        text_features.append(text_features_batch.cpu())

                    text_features = torch.cat(text_features)
                else:
                    text_features = self.model.encode_text(tokens)
                    text_features /= text_features.norm(dim=-1, keepdim=True)

                for i, idx in enumerate(indices):
                    results[idx] = text_features[i].numpy().tobytes()

            # Process image inputs if any
            if image_inputs:
                indices, images = zip(*image_inputs)
                processed_images = torch.stack(
                    [self.preprocess(img) for img in images]  # type: ignore
                )

                if isinstance(self.devices, list):
                    # Split the images across devices if multiple GPUs are available
                    image_batches = torch.split(
                        processed_images,
                        max(1, len(processed_images) // len(self.devices)),
                    )
                    image_features = []

                    for i, image_batch in enumerate(image_batches):
                        image_batch = image_batch.to(
                            self.devices[i % len(self.devices)]
                        )
                        image_features_batch = self.model.module.encode_image(
                            image_batch
                        )
                        image_features_batch /= image_features_batch.norm(
                            dim=-1, keepdim=True
                        )
                        image_features.append(image_features_batch.cpu())

                    image_features = torch.cat(image_features)
                else:
                    processed_images = processed_images.to(self.device)
                    image_features = self.model.encode_image(processed_images)
                    image_features /= image_features.norm(dim=-1, keepdim=True)

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


ModelRegistry.register_model(ClipModel, "clip")
