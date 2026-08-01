import re
from io import BytesIO
from typing import List, Sequence, Type

import numpy as np
from PIL import Image as PILImage

from inferio.impl.utils import (
    assemble_slots,
    clean_whitespace,
    clear_cache,
    decode_image_inputs,
    get_device,
    run_with_oom_retry,
    select_dtype,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput


class DoctrModel(InferenceModel):
    def __init__(
        self,
        detection_model: str,
        recognition_model: str,
        detect_language: bool = True,
        pretrained: bool = True,
        precision: str | None = None,
        init_args: dict = {},
    ):
        self.detection_model: str = detection_model
        self.recognition_model: str = recognition_model
        self.detect_language: bool = detect_language
        self.pretrained: bool = pretrained
        self.precision: str | None = precision
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
        dev = self.devices[0]
        if dev.type == "cuda":
            dtype = select_dtype(dev, "fp16", explicit=self.precision)
            self.model = self.model.to(dev)
            if dtype == torch.float16:
                self.model = self.model.half()
        self._model_loaded = True

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        # Undecodable payloads are excluded before the batch is assembled and
        # come back as error slots (docs/inferio-worker-protocol.md).
        images, kept, slots = decode_image_inputs(inputs, what="OCR")
        image_inputs: List[np.ndarray] = [np.array(image) for image in images]

        pages = run_with_oom_retry(
            lambda chunk: list(self.model(list(chunk)).pages), image_inputs
        )

        assert len(pages) == len(
            image_inputs
        ), "Mismatch in input and output."

        outputs: List[dict] = []
        for page, index in zip(pages, kept):
            config = configs[index]
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

        return assemble_slots(len(inputs), kept, outputs, slots)

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False
IMPL_CLASS = DoctrModel