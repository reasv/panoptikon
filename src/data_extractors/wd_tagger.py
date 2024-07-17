from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

import huggingface_hub
import numpy as np
import pandas as pd
import timm
import torch
from PIL import Image
from timm.data import create_transform, resolve_data_config
from torch import Tensor, nn
from torch.nn import functional as F

from src.utils import pil_ensure_rgb, pil_pad_square

# Dataset v3 series of models:
SWINV2_MODEL_DSV3_REPO = "SmilingWolf/wd-swinv2-tagger-v3"
CONV_MODEL_DSV3_REPO = "SmilingWolf/wd-convnext-tagger-v3"
VIT_MODEL_DSV3_REPO = "SmilingWolf/wd-vit-tagger-v3"

V3_MODELS = [
    SWINV2_MODEL_DSV3_REPO,
    CONV_MODEL_DSV3_REPO,
    VIT_MODEL_DSV3_REPO,
]

# Dataset v2 series of models:
MOAT_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-moat-tagger-v2"
SWIN_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-swinv2-tagger-v2"
CONV_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-convnext-tagger-v2"
CONV2_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-convnextv2-tagger-v2"
VIT_MODEL_DSV2_REPO = "SmilingWolf/wd-v1-4-vit-tagger-v2"

# Files to download from the repos
LABEL_FILENAME = "selected_tags.csv"

# https://github.com/toriato/stable-diffusion-webui-wd14-tagger/blob/a9eacb1eff904552d3012babfa28b57e1d3e295c/tagger/ui.py#L368
kaomojis = [
    "0_0",
    "(o)_(o)",
    "+_+",
    "+_-",
    "._.",
    "<o>_<o>",
    "<|>_<|>",
    "=_=",
    ">_<",
    "3_3",
    "6_9",
    ">_o",
    "@_@",
    "^_^",
    "o_o",
    "u_u",
    "x_x",
    "|_|",
    "||_||",
]


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


class Predictor:
    labels: LabelData | None = None
    transform: Any | None = None
    default_model_repo: str | None = None
    last_loaded_repo: str | None = None
    torch_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, model_repo: str | None = None):
        self.default_model_repo = model_repo
        self.last_loaded_repo = None
        self.torch_device = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu"
        )

    def load_model(self, model_repo: str | None = None):
        if model_repo is None:
            model_repo = self.default_model_repo

        if model_repo is None:
            raise ValueError("No model repo provided")

        if model_repo == self.last_loaded_repo:
            return
        self.labels = load_labels(model_repo)

        model: nn.Module = timm.create_model("hf-hub:" + model_repo).eval()
        state_dict = timm.models.load_state_dict_from_hf(model_repo)
        model.load_state_dict(state_dict)
        self.transform = create_transform(
            **resolve_data_config(model.pretrained_cfg, model=model)
        )

        self.last_loaded_repo = model_repo
        self.model = model
        if self.torch_device.type != "cpu":
            self.model = self.model.to(self.torch_device)

    def prepare_image(self, image: Image.Image):
        # ensure image is RGB
        image = pil_ensure_rgb(image)
        # pad to square with white background
        image = pil_pad_square(image)
        # run the model's input transform to convert to tensor and rescale
        if self.transform is None:
            raise ValueError("Model not loaded")
        inputs: Tensor = self.transform(image).unsqueeze(0)
        # NCHW image RGB to BGR
        inputs = inputs[:, [2, 1, 0]]
        return inputs

    def prepare_images(self, images: Sequence[Image.Image]) -> Tensor:
        batch = [self.prepare_image(image) for image in images]
        return torch.cat(batch, dim=0)

    def predict(
        self,
        images: Sequence[Image.Image],
        model_repo: str | None = None,
        general_thresh: float | None = None,
        character_thresh: float | None = None,
    ):
        if model_repo is None:
            model_repo = self.default_model_repo
        self.load_model(model_repo)

        image_inputs = self.prepare_images(images)

        with torch.inference_mode():
            # move model to GPU, if available
            if self.torch_device.type != "cpu":
                image_inputs = image_inputs.to(self.torch_device)
            # run the model
            outputs = self.model.forward(image_inputs)
            # apply the final activation function
            # (timm doesn't support doing this internally)
            outputs = F.sigmoid(outputs)
            # move inputs, outputs, and model back to to cpu if we were on GPU
            if self.torch_device.type != "cpu":
                image_inputs = image_inputs.to("cpu")
                outputs = outputs.to("cpu")

        # Process each image's output individually
        results: List[
            Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]
        ] = []
        for i in range(outputs.size(0)):
            probs = outputs[i]
            tags = self.get_tags(probs, general_thresh, character_thresh)
            results.append(tags)
        return results

    def get_tags(
        self,
        probs: Tensor,
        general_thresh: float | None,
        character_thresh: float | None,
    ) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        if self.labels is None:
            raise ValueError("Labels not loaded")

        # Convert indices+probs to labels
        labels = list(zip(self.labels.names, probs.numpy()))

        # First 4 labels_data are actually ratings
        rating_labels = dict([labels[i] for i in self.labels.rating])

        # General labels, pick any where prediction confidence > threshold
        general_labels_all = [labels[i] for i in self.labels.general]

        if not general_thresh:
            # Use MCut thresholding
            general_probs = np.array([x[1] for x in general_labels_all])
            general_thresh = mcut_threshold(general_probs)

        general_labels = dict(
            [x for x in general_labels_all if x[1] > general_thresh]
        )

        character_labels_all = [labels[i] for i in self.labels.character]

        if not character_thresh:
            # Use MCut thresholding
            character_probs = np.array([x[1] for x in character_labels_all])
            character_thresh = mcut_threshold(character_probs)
            character_thresh = max(0.05, character_thresh)

        # Character labels, pick any where prediction confidence > threshold
        character_labels = dict(
            [x for x in character_labels_all if x[1] > character_thresh]
        )

        return rating_labels, character_labels, general_labels
