from typing import List

import numpy as np
from doctr.io.html import read_html
from doctr.io.pdf import read_pdf
from PIL import Image as PILImage

from src.data_extractors.video import video_to_frames
from src.types import ItemWithPath
from src.utils import make_video_thumbnails, pil_ensure_rgb


def item_image_extractor_np(item: ItemWithPath) -> List[np.ndarray]:
    if item.type.startswith("image"):
        return [np.array(pil_ensure_rgb(PILImage.open(item.path)))]
    if item.type.startswith("video"):
        frames = video_to_frames(item.path, num_frames=4)
        make_video_thumbnails(frames, item.sha256, item.type)
        return [np.array(pil_ensure_rgb(frame)) for frame in frames]
    if item.type.startswith("application/pdf"):
        return read_pdf(item.path)
    if item.type.startswith("text/html"):
        return read_pdf(read_html(item.path))
    return []


def item_image_extractor_pil(item: ItemWithPath) -> List[PILImage.Image]:
    if item.type.startswith("image"):
        return [PILImage.open(item.path)]
    if item.type.startswith("video"):
        frames = video_to_frames(item.path, num_frames=4)
        make_video_thumbnails(frames, item.sha256, item.type)
        return frames
    if item.type.startswith("application/pdf"):
        return [PILImage.fromarray(page) for page in read_pdf(item.path)]
    if item.type.startswith("text/html"):
        return [
            PILImage.fromarray(page) for page in read_pdf(read_html(item.path))
        ]
    return []
