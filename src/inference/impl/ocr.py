import multiprocessing as mp
import re
from io import BytesIO
from multiprocessing.queues import Queue
from typing import List, Optional, Sequence, Tuple, Union

import numpy as np
import open_clip
import torch
from PIL import Image as PILImage

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput


class DoctrModel(InferenceModel):
    def __init__(
        self,
        detection_model: str,
        recognition_model: str,
        detect_language: bool = True,
        pretrained: bool = True,
        **kwargs,
    ):
        self.detection_model: str = detection_model
        self.recognition_model: str = recognition_model
        self.detect_language: bool = detect_language
        self.pretrained: bool = pretrained
        self.init_args = kwargs
        self._model_loaded: bool = False

    def load(self) -> None:
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

    def __del__(self):
        self.unload()

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        image_inputs: List[np.ndarray] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for input_item in inputs:
            if input_item.file:
                image: PILImage.Image = PILImage.open(
                    BytesIO(input_item.file)
                ).convert("RGB")
                image_inputs.append((np.array(image)))
            else:
                raise ValueError("OCR requires image inputs.")

        result = self.model(image_inputs)

        files_texts: List[str] = []
        languages: List[dict[str, str | float | None]] = []
        word_confidences: List[Sequence[float]] = []
        for page, config in zip(result.pages, configs):
            threshold = config.get("threshold", None)
            assert (
                isinstance(threshold, float) or threshold is None
            ), "Threshold must be a float."

            file_text = ""
            languages.append(page.language)
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
            files_texts.append(file_text)
            word_confidences.append(page_word_confidences)

        outputs: List[dict] = []

        for file_text, language, word_confidence in zip(
            files_texts, languages, word_confidences
        ):
            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            if not file_text:
                continue
            avg_confidence = sum(word_confidence) / len(word_confidences)
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
