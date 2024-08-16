import io

import numpy as np
from PIL import Image


def get_device():
    import torch

    """
    Returns the appropriate torch device based on the available hardware.
    Supports CUDA, ROCm, MPS (Apple Silicon), and CPU.
    """
    if torch.cuda.is_available():  # This covers both CUDA and ROCm
        num_gpus = torch.cuda.device_count()
        if num_gpus > 1:
            return [torch.device(f"cuda:{i}") for i in range(num_gpus)]
        return [torch.device("cuda")]
    elif torch.backends.mps.is_available():  # Apple Silicon (M1/M2)
        return [torch.device("mps")]
    else:
        return [torch.device("cpu")]


def clear_cache() -> None:
    """
    Clears the GPU cache if applicable. Supports CUDA and ROCm.
    For MPS (Apple Silicon) and CPU, no operation is needed.
    """
    import torch

    if torch.cuda.is_available():  # This covers both CUDA and ROCm
        return torch.cuda.empty_cache()
    # No need to clear cache for MPS or CPU as they handle memory differently


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


def serialize_array(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array)
    buffer.seek(0)
    return buffer.read()


def deserialize_array(buffer: bytes) -> np.ndarray:
    bio = io.BytesIO(buffer)
    bio.seek(0)
    return np.load(bio, allow_pickle=False)
