from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Sequence, Type

import numpy as np
import pandas as pd
from PIL import Image
from PIL import Image as PILImage

from inferio.impl.utils import (
    clear_cache,
    get_device,
    mcut_threshold,
    pil_ensure_rgb,
    pil_pad_square,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

LABEL_FILENAME = "selected_tags.csv"

@dataclass
class LabelData:
    names: list[str]
    rating: list[np.int64]
    general: list[np.int64]
    character: list[np.int64]


def load_labels(model_repo: str):
    import huggingface_hub

    csv_path = huggingface_hub.hf_hub_download(
        model_repo,
        LABEL_FILENAME,
    )
    dataframe = pd.read_csv(csv_path)
    name_series = dataframe["name"]
    # name_series = name_series.map(
    #     lambda x: x.replace("_", " ") if x not in kaomojis else x
    # )
    tag_names = name_series.tolist()
    rating_indexes = list(np.where(dataframe["category"] == 9)[0])
    general_indexes = list(np.where(dataframe["category"] == 0)[0])
    character_indexes = list(np.where(dataframe["category"] == 4)[0])
    tag_data = LabelData(
        names=tag_names,
        rating=rating_indexes,
        general=general_indexes,
        character=character_indexes,
    )
    return tag_data


import logging

logger = logging.getLogger(__name__)


class WDTagger(InferenceModel):
    def __init__(self, model_repo: str, init_args: dict = {}):
        self.model_repo = model_repo
        self.init_args = init_args
        self._model_loaded = False

    @classmethod
    def name(cls) -> str:
        return "wd_tagger"

    def load(self):
        import timm
        from timm.data import create_transform, resolve_data_config
        from torch import nn

        if self._model_loaded:
            return
        self.labels = load_labels(self.model_repo)

        model: nn.Module = timm.create_model(
            "hf-hub:" + self.model_repo, **self.init_args
        ).eval()
        state_dict = timm.models.load_state_dict_from_hf(self.model_repo)
        model.load_state_dict(state_dict)
        transform = create_transform(
            **resolve_data_config(model.pretrained_cfg, model=model)
        )
        assert not isinstance(transform, tuple), "Multiple preprocess functions"
        self.transform = transform

        self.model = model
        self.devices = get_device()
        self.model.to(self.devices[0])
        self._model_loaded = True
        logger.debug(f"Model {self.model_repo} loaded")

    def prepare_image(self, image: Image.Image):
        # ensure image is RGB
        image = pil_ensure_rgb(image)
        # pad to square with white background
        image = pil_pad_square(image)
        # run the model's input transform to convert to tensor and rescale
        if self.transform is None:
            raise ValueError("Model not loaded")
        inputs: Tensor = self.transform(image).unsqueeze(0)  # type: ignore
        # NCHW image RGB to BGR
        inputs = inputs[:, [2, 1, 0]]
        return inputs

    def prepare_images(self, images: Sequence[Image.Image]):
        import torch

        batch = [self.prepare_image(image) for image in images]
        return torch.cat(batch, dim=0)

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
                raise ValueError("Tagger requires image inputs.")

        logger.debug(f"Running inference on {len(image_inputs)} images")

        prob_list = self.run_batch(image_inputs, 0)
        outputs: List[dict] = []
        for probs, config in zip(prob_list, configs):
            general_thresh = config.get("threshold", None)
            if general_thresh == 0:
                general_thresh = None  # Use mcut thresholding
            character_thresh = config.get("character_threshold", None)
            tags = self.get_tags(probs, general_thresh, character_thresh)
            outputs.append(
                {
                    "namespace": "danbooru",
                    "tags": [
                        ("rating", tags.rating),
                        ("character", tags.character),
                        ("general", tags.general),
                    ],
                    "mcut": tags.general_mcut,
                    "rating_severity": [
                        "general",
                        "safe",
                        "sensitive",
                        "questionable",
                        "explicit",
                    ],
                    "metadata": {},
                    "metadata_score": 0.0,
                }
            )

        return outputs

    def run_batch(
        self,
        images: Sequence[Image.Image],
        dev_idx: int,
    ):
        import torch
        from torch.nn import functional as F

        self.load()

        image_inputs = self.prepare_images(images)

        with torch.inference_mode():
            # move model to GPU, if available
            if self.devices[dev_idx].type != "cpu":
                image_inputs = image_inputs.to(self.devices[dev_idx])
            # run the model
            outputs = self.model.forward(image_inputs)
            # apply the final activation function
            # (timm doesn't support doing this internally)
            outputs = F.sigmoid(outputs)
            # move inputs, outputs, and model back to to cpu if we were on GPU
            if self.devices[dev_idx].type != "cpu":
                image_inputs = image_inputs.cpu()
                outputs = outputs.cpu()

        return [outputs[i] for i in range(outputs.size(0))]

    def get_tags(
        self,
        probs: Any,
        general_thresh: float | None,
        character_thresh: float | None,
    ):
        if self.labels is None:
            raise ValueError("Labels not loaded")

        # Convert indices+probs to labels
        labels = list(zip(self.labels.names, probs.numpy()))

        # First 4 labels_data are actually ratings
        rating_labels_all = [labels[i] for i in self.labels.rating]
        rating_labels = dict(
            [(label, float(probs)) for label, probs in rating_labels_all]
        )
        # General labels, pick any where prediction confidence > threshold
        general_labels_all = [labels[i] for i in self.labels.general]

        general_probs = np.array([x[1] for x in general_labels_all])
        general_mcut = mcut_threshold(general_probs)

        if not general_thresh:
            # Use MCut thresholding
            general_thresh = general_mcut

        general_labels = dict(
            [
                (label, float(probs))
                for label, probs in general_labels_all
                if probs > general_thresh
            ]
        )

        character_labels_all = [labels[i] for i in self.labels.character]

        character_probs = np.array([x[1] for x in character_labels_all])
        character_mcut = mcut_threshold(character_probs)

        if not character_thresh:
            # Use MCut thresholding
            character_thresh = max(0.05, character_mcut)

        # Character labels, pick any where prediction confidence > threshold
        character_labels = dict(
            [
                (label, float(probs))
                for label, probs in character_labels_all
                if probs > character_thresh
            ]
        )

        return TagResult(
            rating=rating_labels,
            character=character_labels,
            general=general_labels,
            character_mcut=float(character_mcut),  # Ensure Python float
            general_mcut=float(general_mcut),      # Ensure Python float
        )


    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.transform
            del self.labels
            clear_cache()
            logger.debug(f"Model {self.model_repo} unloaded")
            self._model_loaded = False

IMPL_CLASS = WDTagger
@dataclass
class TagResult:
    rating: Dict[str, float]
    character: Dict[str, float]
    general: Dict[str, float]
    character_mcut: float
    general_mcut: float