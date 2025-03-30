import logging
import re
from io import BytesIO
from typing import List, Sequence, Type

from PIL import Image as PILImage

from inferio.impl.utils import clear_cache, get_device
from inferio.model import InferenceModel
from inferio.process_model import ProcessIsolatedInferenceModel
from inferio.types import PredictionInput

logger = logging.getLogger(__name__)

class MoondreamCaptioner(InferenceModel):
    def __init__(
        self,
        model_name: str = "vikhyatk/moondream2",
        model_revision: str = "2025-03-27",
        task: str = "caption",
        caption_length: str = "normal",
        prompt: str = "",
        confidence: float = 1.0,
        language_confidence: float = 1.0,
        language: str | None = None,
        max_output: int = 1024,
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.model_revision: str = model_revision
        self.task: str = task
        self.prompt: str = prompt
        self.confidence: float = confidence
        if self.confidence < 0.0 or self.confidence > 1.0:
            logger.error(
                f"Confidence value {self.confidence} is out of range. Setting to 1.0.")
            self.confidence = 1.0
        self.language_confidence: float = language_confidence
        if self.language_confidence < 0.0 or self.language_confidence > 1.0:
            logger.error(
                f"Language confidence value {self.language_confidence} is out of range. Setting to 1.0.")
            self.language_confidence = 1.0
        if self.task not in ["query", "caption"]:
            logger.error(
                f"Task {self.task} is not supported. Defaulting to caption.")
            self.task = "caption"
        self.caption_length: str = caption_length
        if language is not None and len(language) > 0:
            self.language: str = language
        else:
            self.language: str = f"moondream-{self.task}-{self.caption_length}"
        self.max_output: int = max_output
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "moondream_captioner"

    def load(self) -> None:
        if self._model_loaded:
            return
        
        from transformers import AutoModelForCausalLM
        
        # Check if accelerate is installed
        try:
            import accelerate
            ACCELERATE_AVAILABLE = True
        except ImportError:
            ACCELERATE_AVAILABLE = False

        self.devices = get_device()
        device = self.devices[0]

        if ACCELERATE_AVAILABLE and device == "cuda":
            # Optimized loading for GPU using accelerate
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                revision=self.model_revision,
                trust_remote_code=True,
                device_map={"": "cuda"},
                **self.init_args,
            ).eval()
        else:
            # Fallback loading (standard PyTorch)
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                revision=self.model_revision,
                trust_remote_code=True,
                **self.init_args,
            ).to(device).eval()
        logger.debug(f"Model {self.model_name} loaded.")
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        image_inputs: List[PILImage.Image] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for input_item in inputs:
            if input_item.file:
                image: PILImage.Image = PILImage.open(
                    BytesIO(input_item.file)
                ).convert("RGB")
                image_inputs.append(image)
            else:
                raise ValueError("Moondream requires image inputs.")

        results: List[str] = []
        for image in image_inputs:
            # Process the inputs and ensure they are in the correct dtype and device
            encoded_image = self.model.encode_image(image)
            if self.task == "query":
                answer: str = self.model.query(encoded_image, self.prompt)["answer"]
            elif self.task == "caption":
                answer: str = self.model.caption(
                    encoded_image,
                    length=self.caption_length,
                )["caption"]
            else:
                raise ValueError(f"Unsupported task: {self.task}")
            assert (
                answer is not None and answer != ""
            ), f"No output found. (Result: {answer})"
            logger.debug(f"Output: {answer}")
            results.append(answer)

        assert len(results) == len(
            image_inputs
        ), "Mismatch in input and output."

        outputs: List[dict] = []
        for file_text, config in zip(results, configs):
            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            outputs.append(
                {
                    "transcription": file_text,
                    "confidence": self.confidence,
                    "language": self.language,
                    "language_confidence": self.language_confidence,
                }
            )

        assert len(outputs) == len(
            inputs
        ), f"Expected {len(inputs)} outputs but got {len(outputs)}"

        return outputs

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False


def clean_whitespace(input_string: str) -> str:
    # Replace three or more consecutive whitespaces with just two
    cleaned_string = re.sub(r"(\s)\1{2,}", r"\1\1", input_string)

    return cleaned_string


class MoondreamCaptionerIsolated(ProcessIsolatedInferenceModel):
    @classmethod
    def concrete_class(cls) -> Type[MoondreamCaptioner]:  # type: ignore
        return MoondreamCaptioner
