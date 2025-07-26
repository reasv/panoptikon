import io
import sys
import os
import logging
import json
import re
from typing import List, Optional
import numpy as np
from PIL import Image
import PIL.Image
from io import BytesIO
from typing import Optional

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
    Clears the torch memory cache if applicable:
    - CUDA (NVIDIA and ROCm): uses torch.cuda.empty_cache()
    - MPS (Apple Silicon): uses torch.mps.empty_cache()
    """
    import torch

    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    elif hasattr(torch, "mps") and torch.backends.mps.is_available():
        torch.mps.empty_cache()

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



def extract_partial_json_array(json_str: str) -> Optional[List[str]]:
    """
    Attempts to extract a partial JSON array from a potentially truncated string.
    Returns the longest valid prefix of the array that can be parsed.
    """
    # Find the first opening bracket to start parsing
    start_idx = json_str.find('[')
    if start_idx == -1:
        return None  # No array found
    
    # Extract from first [ to end
    partial_str = json_str[start_idx:]
    
    # First try parsing the complete JSON
    try:
        return json.loads(partial_str)
    except json.JSONDecodeError:
        pass  # We'll handle this below
    
    # If we're here, the JSON is incomplete. We'll try to find the longest valid prefix.
    # We'll work backwards from the end, removing characters until we get valid JSON
    for end_idx in range(len(partial_str), start_idx + 1, -1):
        test_str = partial_str[:end_idx] + ']'  # Close the array
        try:
            result = json.loads(test_str)
            if isinstance(result, list):
                # Verify all elements are strings (as per your requirement)
                if all(isinstance(item, str) for item in result):
                    return result
        except (json.JSONDecodeError, TypeError):
            continue
    
    # Try one more approach - extract individual elements
    # This handles cases where the array is cut off in the middle of an element
    elements = []
    current_pos = start_idx + 1  # position after '['
    while current_pos < len(partial_str):
        # Try to parse from current position to end
        try:
            # Attempt to parse a complete JSON string
            end_of_str = current_pos
            while True:
                next_quote = partial_str.find('"', end_of_str)
                if next_quote == -1:
                    break  # No closing quote found
                
                # Check if this is an unescaped quote
                if partial_str[next_quote-1] != '\\':
                    # Try to parse from current_pos to next_quote+1
                    test_str = '[' + partial_str[current_pos:next_quote+1] + ']'
                    try:
                        element = json.loads(test_str)[0]
                        elements.append(element)
                        current_pos = next_quote + 2  # move past quote and comma/whitespace
                        break
                    except json.JSONDecodeError:
                        pass
                end_of_str = next_quote + 1
            else:
                break
        except (IndexError, json.JSONDecodeError):
            break
    
    return elements if elements else None

def clean_whitespace(input_string: str) -> str:
    # Replace three or more consecutive whitespaces with just two
    cleaned_string = re.sub(r"(\s)\1{2,}", r"\1\1", input_string)

    return cleaned_string


def print_resource_usage(logger: logging.Logger | None = None):
    """
    Logs process resource usage.
    - On Unix (Linux/macOS): tries to use `resource` for max RSS.
    - On all platforms: uses `psutil` (if available) for RSS, VMS, threads, and CPU.
    - Falls back to `print()` if no logger is given.
    """
    def log(msg):
        if logger is not None:
            logger.info(msg)
        else:
            print(msg)

    log(f"Resource usage for PID {os.getpid()}:")

    # Try using resource for max RSS on Unix platforms
    if sys.platform != 'win32':
        try:
            import resource
            maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # On Mac, this is bytes; on Linux, it's kilobytes
            if sys.platform == "darwin":
                maxrss_mb = maxrss / (1024*1024)
                log(f"  [resource] Max Resident Set Size (Mac): {maxrss_mb:.2f} MB")
            else:
                maxrss_mb = maxrss / 1024
                log(f"  [resource] Max Resident Set Size (Linux): {maxrss_mb:.2f} MB")
        except Exception as e:
            log(f"  [resource] Unable to get max RSS via resource module: {e}")

    # Universal: try psutil for more detail
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        rss = proc.memory_info().rss / (1024 ** 2)  # MB
        vms = proc.memory_info().vms / (1024 ** 2)  # MB
        threads = proc.num_threads()
        cpu = proc.cpu_percent(interval=0.1)
        log(f"  [psutil] Resident RAM (RSS):  {rss:.2f} MB")
        log(f"  [psutil] Virtual Memory (VMS): {vms:.2f} MB")
        log(f"  [psutil] Num Threads:          {threads}")
        log(f"  [psutil] CPU usage:            {cpu:.1f}%")
    except ImportError:
        log("  [psutil] psutil not installed. Install with `pip install psutil` for more details.")

def load_image_from_buffer(
    buf: bytes,
    *,
    accept_truncated: bool = True,
    try_fix_jpeg: bool = True,
    fallback_opencv: bool = True,
) -> "PIL.Image.Image":
    """
    Load an image from a raw byte buffer and return a Pillow Image in RGB mode.

    Parameters
    ----------
    buf : bytes
        Raw image data.
    accept_truncated : bool, default True
        If True, sets PIL.ImageFile.LOAD_TRUNCATED_IMAGES so Pillow will
        return partially‑decoded images instead of raising OSError.
    try_fix_jpeg : bool, default True
        If True, appends the JPEG end‑of‑image marker 0xFF 0xD9 if it is missing.
    fallback_opencv : bool, default True
        If Pillow still cannot decode, fall back to OpenCV and convert the
        result back to Pillow.

    Raises
    ------
    ValueError
        If the image is unreadable by all enabled back‑ends.
    """
    # ––––– 1.  Pillow first –––––
    try:
        from PIL import Image as PILImage
        from PIL import ImageFile

        if accept_truncated:
            ImageFile.LOAD_TRUNCATED_IMAGES = True

        raw = buf
        if try_fix_jpeg and not raw.endswith(b"\xFF\xD9"):  # add missing EOI
            raw += b"\xFF\xD9"

        with PILImage.open(BytesIO(raw)) as im:
            im.load()                   # force decoding now
            return im.convert("RGB")

    except (ModuleNotFoundError, ImportError):
        raise ValueError("Pillow is not installed") from None
    except Exception as err:
        # Any other OSError & friends fall through to optional fallback
        last_err: Optional[Exception] = err

    # ––––– 2.  OpenCV fallback –––––
    if fallback_opencv:
        try:
            import cv2
            import numpy as np

            arr = np.frombuffer(buf, dtype=np.uint8)
            img_cv = cv2.imdecode(arr, cv2.IMREAD_UNCHANGED)
            if img_cv is None:
                raise ValueError("OpenCV could not decode image")
            # BGR ➜ RGB and back to Pillow
            img_cv = cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB)
            from PIL import Image as PILImage
            return PILImage.fromarray(img_cv)
        except Exception as err:
            last_err = err

    # ––––– 3.  Give up –––––
    raise ValueError("Unreadable image") from last_err
