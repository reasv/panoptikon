import re
from io import BytesIO
from typing import List, Sequence, Type

import numpy as np
from PIL import Image as PILImage

from inferio.impl.utils import clean_whitespace, clear_cache, get_device, load_image_from_buffer
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput


class DoctrModel(InferenceModel):
    def __init__(
        self,
        detection_model: str,
        recognition_model: str,
        detect_language: bool = True,
        pretrained: bool = True,
        init_args: dict = {},
    ):
        self.detection_model: str = detection_model
        self.recognition_model: str = recognition_model
        self.detect_language: bool = detect_language
        self.pretrained: bool = pretrained
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "doctr"

    def load(self) -> None:
        import torch
        from doctr.models import ocr_predictor

        if self._model_loaded:
            return

        self.devices = get_device()
        self.model = ocr_predictor(
            det_arch=self.detection_model,
            reco_arch=self.recognition_model,
            detect_language=self.detect_language,
            pretrained=self.pretrained,
            **self.init_args,
        )
        if torch.cuda.is_available():
            self.model = self.model.cuda().half()
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        image_inputs: List[np.ndarray] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for input_item in inputs:
            if input_item.file:
                image: PILImage.Image = load_image_from_buffer(input_item.file)
                image_inputs.append((np.array(image)))
            else:
                raise ValueError("OCR requires image inputs.")

        result = self.model(image_inputs)

        assert len(result.pages) == len(
            image_inputs
        ), "Mismatch in input and output."

        outputs: List[dict] = []
        for page, config in zip(result.pages, configs):
            threshold = config.get("threshold", None)
            assert (
                isinstance(threshold, float) or threshold is None
            ), "Threshold must be a float."

            file_text = ""
            language = page.language
            page_word_confidences = []
            for block in page.blocks:
                for line in block.lines:
                    for word in line.words:
                        if threshold and word.confidence < threshold:
                            continue
                        file_text += word.value + " "
                        page_word_confidences.append(word.confidence)
                    file_text += "\n"
                file_text += "\n"

            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            avg_confidence = sum(page_word_confidences) / max(
                len(page_word_confidences), 1
            )
            assert (
                isinstance(language["confidence"], float)
                or language["confidence"] is None
            ), "Language confidence should be a float or None"
            assert (
                isinstance(language["value"], str) or language["value"] is None
            ), "Language value should be a string or None"
            outputs.append(
                {
                    "transcription": file_text,
                    "confidence": avg_confidence,
                    "language": language["value"],
                    "language_confidence": language["confidence"],
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
IMPL_CLASS = DoctrModel