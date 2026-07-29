import logging
from io import BytesIO
from typing import List, Sequence, Type, Union

from PIL import Image as PILImage
from PIL import ImageFile

from inferio.impl.utils import (
    clear_cache,
    get_device,
    load_image_from_buffer,
    run_with_oom_retry,
    serialize_array,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

ImageFile.LOAD_TRUNCATED_IMAGES = True

logger = logging.getLogger(__name__)


class ClipModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        pretrained: str | None = None,
        context_length: int | None = None,
        precision: str = "fp16",
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.pretrained: str | None = pretrained
        self.context_length: int | None = context_length
        # fp16 halves resident VRAM (3829 -> 1982 MiB for ViT-H-14-378) and is
        # substantially faster, since fp32 matmuls do not use tensor cores.
        # Retrieval impact was measured as negligible; see
        # docs/clip-fp16-precision-evaluation.md. Override per inference id
        # with `config.precision` if a model or GPU needs fp32.
        self.precision: str = precision
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "openclip"

    def load(self) -> None:
        if self._model_loaded:
            return
        import open_clip

        self.devices = get_device()
        self.device = (
            self.devices[0] if isinstance(self.devices, list) else self.devices
        )
        precision = self._effective_precision()

        self.model, _, preprocess = open_clip.create_model_and_transforms(
            model_name=self.model_name,
            pretrained=self.pretrained,
            precision=precision,
            **self.init_args,
        )
        assert not isinstance(
            preprocess, tuple
        ), "Expected single preprocess function"
        self.preprocess = preprocess

        # open_clip builds and converts on CPU; moving afterwards transfers
        # half the bytes, so a low-precision load is faster, not slower.
        self.model.eval().to(self.device)
        self.input_dtype = self._input_dtype()
        self.tokenizer = open_clip.get_tokenizer(
            model_name=self.model_name, context_length=self.context_length
        )
        self._model_loaded = True

    def _effective_precision(self) -> str:
        """Low precision only on CUDA/ROCm; fp16 on CPU is slow and patchily
        supported, and MPS is unvalidated for this path."""
        if self.precision == "fp32" or self.device.type == "cuda":
            return self.precision
        logger.warning(
            "Precision %r requested for %s but device is %s; using fp32.",
            self.precision,
            self.model_name,
            self.device.type,
        )
        return "fp32"

    def _input_dtype(self):
        """Dtype the image tower expects its input in.

        open_clip's fp16/bf16 modes cast weights only and leave input casting
        to the caller (the same contract as OpenAI's original reference
        implementation, `model.encode_image(image.half())`). Feeding fp32
        pixels to converted weights raises at the patch-embed conv. Taking the
        first conv/linear in the visual tower covers both the native and the
        timm-backed branches of `_set_model_device_and_precision`.
        """
        import torch

        for module in self.model.visual.modules():
            if isinstance(
                module, (torch.nn.Conv1d, torch.nn.Conv2d, torch.nn.Conv3d, torch.nn.Linear)
            ):
                return module.weight.dtype
        return torch.float32

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
            if input_item.file:
                image = load_image_from_buffer(input_item.file)
                image_inputs.append((idx, image))
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        def encode_text_chunk(chunk):
            tokens = torch.tensor(self.tokenizer(list(chunk))).to(self.device)
            features = self.model.encode_text(tokens, normalize=True)
            return [
                serialize_array(features[i].cpu().numpy())
                for i in range(features.size(0))
            ]

        def encode_image_chunk(chunk):
            processed = torch.stack(
                [self.preprocess(img) for img in chunk]  # type: ignore
            ).to(self.device, dtype=self.input_dtype)
            features = self.model.encode_image(processed, normalize=True)
            return [
                serialize_array(features[i].cpu().numpy())
                for i in range(features.size(0))
            ]

        # Use inference_mode for optimized inference
        with torch.inference_mode():
            # Process text inputs if any
            if text_inputs:
                indices, texts = zip(*text_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_text_chunk, list(texts), logger=logger
                    ),
                ):
                    results[idx] = res

            # Process image inputs if any
            if image_inputs:
                indices, images = zip(*image_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_image_chunk, list(images), logger=logger
                    ),
                ):
                    results[idx] = res

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

IMPL_CLASS = ClipModel