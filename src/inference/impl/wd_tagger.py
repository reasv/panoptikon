from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Sequence

import huggingface_hub
import numpy as np
import pandas as pd
import timm
import torch
from PIL import Image
from PIL import Image as PILImage
from timm.data import create_transform, resolve_data_config
from torch import Tensor, nn
from torch.nn import functional as F

from src.inference.impl.utils import clear_cache, get_device
from src.inference.model import InferenceModel
from src.inference.types import PredictionInput

LABEL_FILENAME = "selected_tags.csv"


@dataclass
class LabelData:
    names: list[str]
    rating: list[np.int64]
    general: list[np.int64]
    character: list[np.int64]


def load_labels(model_repo: str):
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


def mcut_threshold(probs: np.ndarray) -> float:
    """
    Maximum Cut Thresholding (MCut)
    Largeron, C., Moulin, C., & Gery, M. (2012). MCut: A Thresholding Strategy
     for Multi-label Classification. In 11th International Symposium, IDA 2012
     (pp. 172-183).
    """
    sorted_probs = probs[probs.argsort()[::-1]]
    difs = sorted_probs[:-1] - sorted_probs[1:]
    t = difs.argmax()
    thresh = (sorted_probs[t] + sorted_probs[t + 1]) / 2
    return thresh


def pil_pad_square(image: Image.Image) -> Image.Image:
    w, h = image.size
    # get the largest dimension so we can pad to a square
    px = max(image.size)
    # pad to square with white background
    canvas = Image.new("RGB", (px, px), (255, 255, 255))
    canvas.paste(image, ((px - w) // 2, (px - h) // 2))
    return canvas


def pil_ensure_rgb(image: Image.Image) -> Image.Image:
    # convert to RGB/RGBA if not already (deals with palette images etc.)
    if image.mode not in ["RGB", "RGBA"]:
        image = (
            image.convert("RGBA")
            if "transparency" in image.info
            else image.convert("RGB")
        )
    # convert RGBA to RGB with white background
    if image.mode == "RGBA":
        canvas = Image.new("RGBA", image.size, (255, 255, 255))
        canvas.alpha_composite(image)
        image = canvas.convert("RGB")
    return image


class WDTagger(InferenceModel):
    def __init__(self, model_repo: str):
        self.model_repo = model_repo
        self._model_loaded = False

    def load(self):
        if self._model_loaded:
            return
        self.labels = load_labels(self.model_repo)

        model: nn.Module = timm.create_model("hf-hub:" + self.model_repo).eval()
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

    def prepare_images(self, images: Sequence[Image.Image]) -> Tensor:
        batch = [self.prepare_image(image) for image in images]
        return torch.cat(batch, dim=0)

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        image_inputs: List[PILImage.Image] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        for idx, input_item in enumerate(inputs):
            if input_item.file:
                image: PILImage.Image = PILImage.open(
                    BytesIO(input_item.file)
                ).convert("RGB")
                image_inputs.append(image)
            else:
                raise ValueError("Tagger requires image inputs.")

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
                    "tags": {
                        "rating": tags.rating,
                        "character": tags.character,
                        "general": tags.general,
                    },
                    "mcut": tags.general_mcut,
                    "character_mcut": tags.general_mcut,
                }
            )

        outputs: List[dict] = []

        return outputs

    def run_batch(
        self,
        images: Sequence[Image.Image],
        dev_idx: int,
    ):
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
        probs: Tensor,
        general_thresh: float | None,
        character_thresh: float | None,
    ):
        if self.labels is None:
            raise ValueError("Labels not loaded")

        # Convert indices+probs to labels
        labels = list(zip(self.labels.names, probs.numpy()))

        # First 4 labels_data are actually ratings
        rating_labels = dict([labels[i] for i in self.labels.rating])

        # General labels, pick any where prediction confidence > threshold
        general_labels_all = [labels[i] for i in self.labels.general]

        general_probs = np.array([x[1] for x in general_labels_all])
        general_mcut = mcut_threshold(general_probs)

        if not general_thresh:
            # Use MCut thresholding
            general_thresh = general_mcut

        general_labels = dict(
            [x for x in general_labels_all if x[1] > general_thresh]
        )

        character_labels_all = [labels[i] for i in self.labels.character]

        character_probs = np.array([x[1] for x in character_labels_all])
        character_mcut = mcut_threshold(character_probs)

        if not character_thresh:
            # Use MCut thresholding
            character_thresh = max(0.05, character_mcut)

        # Character labels, pick any where prediction confidence > threshold
        character_labels = dict(
            [x for x in character_labels_all if x[1] > character_thresh]
        )

        return TagResult(
            rating=rating_labels,
            character=character_labels,
            general=general_labels,
            character_mcut=character_mcut,
            general_mcut=general_mcut,
        )

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.transform
            del self.labels
            clear_cache()
            self._model_loaded = False

    def __del__(self):
        self.unload()


@dataclass
class TagResult:
    rating: Dict[str, float]
    character: Dict[str, float]
    general: Dict[str, float]
    character_mcut: float
    general_mcut: float
