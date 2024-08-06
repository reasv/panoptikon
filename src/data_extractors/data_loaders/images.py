from typing import List

import numpy as np
from doctr.io.html import read_html
from doctr.io.pdf import read_pdf
from PIL import Image as PILImage
from PIL import ImageSequence

from src.data_extractors.data_loaders.video import video_to_frames
from src.types import ItemWithPath
from src.utils import pil_ensure_rgb


def gif_to_frames(path: str) -> List[PILImage.Image]:
    gif = PILImage.open(path)
    frames = []

    # Count the total number of frames
    total_frames = 0
    for _ in ImageSequence.Iterator(gif):
        total_frames += 1

    # Calculate the step to get 4 evenly spaced frames
    step = max(total_frames // 4, 1)

    # Extract 4 evenly spaced frames
    for i, frame in enumerate(ImageSequence.Iterator(gif)):
        if i % step == 0:
            frames.append(frame.copy())
        if len(frames) == 4:  # Stop after extracting 4 frames
            break

    return frames


def item_image_loader_numpy(item: ItemWithPath) -> List[np.ndarray]:
    if item.type.startswith("image/gif"):
        return [
            np.array(pil_ensure_rgb(frame))
            for frame in gif_to_frames(item.path)
        ]
    if item.type.startswith("image"):
        return [np.array(pil_ensure_rgb(PILImage.open(item.path)))]
    if item.type.startswith("video"):
        frames = video_to_frames(item.path, num_frames=4)
        return [np.array(pil_ensure_rgb(frame)) for frame in frames]
    if item.type.startswith("application/pdf"):
        return read_pdf(item.path)
    if item.type.startswith("text/html"):
        return read_pdf(read_html(item.path))
    return []


def item_image_loader_pillow(item: ItemWithPath) -> List[PILImage.Image]:
    if item.type.startswith("image/gif"):
        return [frame for frame in gif_to_frames(item.path)]
    if item.type.startswith("image"):
        return [PILImage.open(item.path)]
    if item.type.startswith("video"):
        frames = video_to_frames(item.path, num_frames=4)
        return frames
    if item.type.startswith("application/pdf"):
        return [PILImage.fromarray(page) for page in read_pdf(item.path)]
    if item.type.startswith("text/html"):
        return [
            PILImage.fromarray(page) for page in read_pdf(read_html(item.path))
        ]
    return []
