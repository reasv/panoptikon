import re
import logging
from io import BytesIO
from typing import List, Sequence
import numpy as np
from PIL import Image as PILImage
from inferio.impl.utils import (
    InferenceOOMError,
    assemble_slots,
    clean_whitespace,
    clear_cache,
    decode_image_inputs,
    get_device,
    looks_like_index_limit,
    note_index_limit_event,
    run_with_oom_retry,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

logger = logging.getLogger(__name__)

# EasyOCR's default `canvas_size`: the CRAFT detector bounds every input's
# longer side at it.
DETECTOR_CANVAS_SIZE = 2560

# EasyOCR's `min_size` default, in pixels of the submitted image.
DEFAULT_MIN_SIZE = 20

# easyOCR's `mag_ratio` default (it also scales the detector's tensor).
DEFAULT_MAG_RATIO = 1.0

# `easyocr.imgproc.resize_aspect_ratio` pads each side of the detector's input
# up to the next multiple of this.
DETECTOR_SIZE_MULTIPLE = 32

# The detector's batch ceiling on CUDA: `max_pool2d_with_indices` indexes its
# output (`B x 64 x H//2 x W//2`) with a signed 32-bit int. CPU has no such
# limit. See docs/inferio-worker-protocol.md, "The easyOCR ceiling in full".
KERNEL_INDEX_ELEMENT_LIMIT = 2**31 - 1
DETECTOR_POOL_CHANNELS = 64

# The per-request parameters forwarded, split by the easyOCR call each belongs
# to (`threshold` is easyOCR's box threshold, not this impl's confidence floor).
DETECT_PARAMS = frozenset({
    "min_size", "text_threshold", "low_text", "link_threshold", "canvas_size",
    "mag_ratio", "slope_ths", "ycenter_ths", "height_ths", "width_ths",
    "add_margin", "threshold", "bbox_min_score", "bbox_min_size",
    "max_candidates",
})
RECOGNIZE_PARAMS = frozenset({
    "decoder", "beamWidth", "batch_size", "workers", "allowlist", "blocklist",
    "detail", "rotation_info", "paragraph", "contrast_ths", "adjust_contrast",
    "filter_ths", "y_ths", "x_ths", "output_format",
})
BATCH_PARAMS = frozenset(DETECT_PARAMS | RECOGNIZE_PARAMS)


def _positive_int(value) -> int | None:
    """`value` as a positive int, or None; bools are refused."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = int(value)
    except Exception:
        return None
    return number if number > 0 else None


def _dims_label(dims: tuple[int, int] | None) -> str:
    """`(height, width)` as `"WxH"` for a log line."""
    return "unknown" if dims is None else f"{dims[1]}x{dims[0]}"


def _shape_as_height_width(shape) -> tuple[int, int] | None:
    """A harness `(width, height)` pair as `(height, width)`, or None unless
    both are positive integers."""
    if shape is None:
        return None
    try:
        width, height = int(shape[0]), int(shape[1])
    except Exception:
        return None
    if width <= 0 or height <= 0:
        return None
    return height, width


def ceil_to_multiple(value: int, multiple: int = DETECTOR_SIZE_MULTIPLE) -> int:
    """`value` rounded up to a multiple of `multiple`, as easyOCR pads."""
    remainder = value % multiple
    return value if remainder == 0 else value + (multiple - remainder)


def bounded_dims(
    shape: tuple[int, int], canvas_size: int = DETECTOR_CANVAS_SIZE
) -> tuple[int, int]:
    """`fit_to_canvas`'s output dimensions for `(height, width)`."""
    height, width = int(shape[0]), int(shape[1])
    longest = max(height, width)
    if longest <= 0 or longest <= canvas_size:
        return max(1, height), max(1, width)
    ratio = canvas_size / longest
    return max(1, int(height * ratio)), max(1, int(width * ratio))


def detector_tensor_dims(
    shapes: Sequence[tuple[int, int] | None],
    canvas_size: int = DETECTOR_CANVAS_SIZE,
    mag_ratio: float = DEFAULT_MAG_RATIO,
) -> tuple[int, int] | None:
    """`(height, width)` of the CRAFT input tensor a batch of these builds (each
    bounded by the canvas, element-wise maximum, rescaled by `mag_ratio` and
    padded to a multiple of 32), or None when no shape is known.
    """
    dims = [bounded_dims(shape, canvas_size) for shape in shapes if shape]
    if not dims:
        return None
    height = max(dim[0] for dim in dims)
    width = max(dim[1] for dim in dims)
    longest = max(height, width)
    if longest <= 0:
        return None
    target = min((mag_ratio or DEFAULT_MAG_RATIO) * longest, canvas_size)
    ratio = target / longest
    return (
        ceil_to_multiple(max(1, int(height * ratio))),
        ceil_to_multiple(max(1, int(width * ratio))),
    )


def detector_pool_elements(height: int, width: int) -> int:
    """Pooling-output elements per item of an `H x W` batch."""
    return DETECTOR_POOL_CHANNELS * (height // 2) * (width // 2)


def max_detector_batch(
    shapes: Sequence[tuple[int, int] | None],
    canvas_size: int = DETECTOR_CANVAS_SIZE,
    mag_ratio: float = DEFAULT_MAG_RATIO,
) -> int | None:
    """Largest batch of these shapes CRAFT's pooling kernel can index (at least
    1), or None when no shape is known.
    """
    dims = detector_tensor_dims(shapes, canvas_size, mag_ratio)
    return max_batch_for_dims(dims)


def max_batch_for_dims(dims: tuple[int, int] | None) -> int | None:
    """`max_detector_batch` for already-computed padded dims."""
    if dims is None:
        return None
    per_item = detector_pool_elements(*dims)
    if per_item <= 0:  # pragma: no cover - defensive
        return None
    return max(1, KERNEL_INDEX_ELEMENT_LIMIT // per_item)


class EasyOCRModel(InferenceModel):
    def __init__(
        self,
        languages: List[str] = ["en"],
        gpu: bool = True,
        enable_batching: bool = True,
        model_storage_directory: str | None = None,
        download_enabled: bool = True,
        recog_network: str = 'standard',
        detector: bool = True,
        recognizer: bool = True,
        verbose: bool = True,
        quantize: bool = True,
        cudnn_benchmark: bool = False,
        canvas_size: int = DETECTOR_CANVAS_SIZE,
    ):
        self.canvas_size = _positive_int(canvas_size) or DETECTOR_CANVAS_SIZE
        # Read by the packing harness: the batch tensor's per-item area never
        # exceeds `canvas_pixels`, and it is padded to its largest member.
        self.canvas_pixels = self.canvas_size * self.canvas_size
        self.pads_to_common_size = True
        self.languages = languages
        self.gpu = gpu
        self.model_storage_directory = model_storage_directory
        self.download_enabled = download_enabled
        self.recog_network = recog_network
        self.detector = detector
        self.recognizer = recognizer
        self.verbose = verbose
        self.quantize = quantize
        self.enable_batching = enable_batching
        self.cudnn_benchmark = cudnn_benchmark
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "easyocr"

    def load(self) -> None:
        import torch
        import easyocr
        
        if self._model_loaded:
            return

        self.devices = get_device()
        # The resolved device, so the model runs where it is budgeted.
        use_gpu = self.gpu and self.devices[0].type == "cuda"
        # ROCm/HIP: EasyOCR's CRAFT detector hits MIOpen GEMM paths that warn
        # IsEnoughWorkspace (ptr=0) and can stall for tens of seconds per unique
        # shape under default HYBRID find. Prefer a single device string over
        # bool True so DataParallel still gets one GPU; quantize only applies
        # on CPU in EasyOCR so leave it as configured.
        hip = bool(getattr(torch.version, "hip", None))
        if use_gpu and hip:
            # Single-device string avoids multi-GPU DataParallel fan-out.
            gpu_arg: bool | str = "cuda:0"
            if self.verbose:
                logger.info(
                    "EasyOCR on ROCm/HIP (device=%s); MIOpen find-mode should "
                    "be FAST via accelerator_env to avoid workspace stalls",
                    gpu_arg,
                )
        else:
            gpu_arg = use_gpu

        self.model = easyocr.Reader(
            lang_list=self.languages,
            gpu=gpu_arg,
            model_storage_directory=self.model_storage_directory,
            download_enabled=self.download_enabled,
            recog_network=self.recog_network,
            detector=self.detector,
            recognizer=self.recognizer,
            verbose=self.verbose,
            quantize=self.quantize,
            cudnn_benchmark=self.cudnn_benchmark
        )
        
        self._model_loaded = True

    def _index_ceiling_applies(self) -> bool:
        """Whether CRAFT runs on the CUDA (or HIP) pooling kernel that has the
        index ceiling. True unless known otherwise: a missing cap costs a failed
        batch, a needless one only a smaller batch.
        """
        if not self.gpu:
            return False
        devices = getattr(self, "devices", None)
        if not devices:
            return True
        return getattr(devices[0], "type", "cuda") == "cuda"

    def max_batch_for(
        self, shapes: Sequence[tuple[int, int] | None]
    ) -> int | None:
        """Largest batch of these inputs one `predict` call can execute (the
        packing harness's shape-ceiling hook), or None for no ceiling.

        `shapes` are `(width, height)` pairs, None where unreadable (charged the
        square canvas). Uses the configured canvas; the batched path enforces
        the exact cap for per-request parameters.
        """
        if not self.enable_batching or not self._index_ceiling_applies():
            return None
        sizes: List[tuple[int, int] | None] = []
        for shape in shapes:
            size = _shape_as_height_width(shape)
            sizes.append(size or (self.canvas_size, self.canvas_size))
        if not sizes:
            return None
        return max_detector_batch(sizes, self.canvas_size)

    def predict(self, inputs: Sequence[PredictionInput]) -> List[dict]:
        self.load()
        
        outputs: List[dict] = []
        configs: List[dict] = [inp.data for inp in inputs]  # type: ignore
        
        # Collect all images. Undecodable payloads are excluded here, before
        # the batch is assembled, and come back as error slots
        # (docs/inferio-worker-protocol.md).
        images, kept, slots = decode_image_inputs(
            inputs, what="OCR", logger=logger
        )

        # Extract batch parameters from configs. The batch is the *kept*
        # inputs, so its first config is `configs[kept[0]]` — reading
        # `configs[0]` would apply a rejected input's settings to the batch
        # that never contained it.
        batch_params = {}
        if kept:
            first_config = configs[kept[0]]
            for param in sorted(BATCH_PARAMS):
                if param in first_config:
                    batch_params[param] = first_config[param]

        raw_images: List[np.ndarray] = [np.array(image) for image in images]

        use_batched = self.enable_batching and len(raw_images) > 1

        batch_results: List = []
        if use_batched:
            try:
                batch_results = self._detect_bounded_recognize_raw(
                    raw_images, batch_params
                )
            except InferenceOOMError:
                # A single input still OOMs after halving; individual
                # processing would just OOM again unclassified.
                raise
            except Exception as error:
                # Logged with the tensor dimensions, never silent.
                dims = detector_tensor_dims(
                    [
                        (int(image.shape[0]), int(image.shape[1]))
                        for image in raw_images
                    ],
                    self._batch_canvas_size(batch_params),
                    self._batch_mag_ratio(batch_params),
                )
                logger.warning(
                    "easyOCR's batched path failed on %d inputs at a padded "
                    "detector tensor of %s (%s); falling back to per-image "
                    "processing",
                    len(raw_images),
                    _dims_label(dims),
                    "a kernel index ceiling"
                    if looks_like_index_limit(error)
                    else type(error).__name__,
                    exc_info=True,
                )
                use_batched = False

        if not use_batched:
            # Per image at submitted resolution; easyOCR bounds the detector.
            batch_results = []
            for img in raw_images:
                result = self.model.readtext(img, **batch_params)
                batch_results.append(result)

        # Process results for each image
        for result, index in zip(batch_results, kept):
            config = configs[index]
            threshold = config.get("threshold", None)
            assert (
                isinstance(threshold, float) or threshold is None
            ), "Threshold must be a float."
            
            if not result:
                outputs.append({
                    "transcription": "",
                    "confidence": 0.0,
                    "language": self.languages[0] if self.languages else None,
                    "language_confidence": None,
                })
                continue
            
            # Group text into lines based on vertical position
            line_height_median = np.median([bbox[2][1] - bbox[0][1] for bbox, _, _ in result])
            line_gap = line_height_median * 0.5  # Use half the median line height as line gap threshold
            
            # Sort by top coordinate
            result.sort(key=lambda x: x[0][0][1])
            
            lines = []
            current_line = []
            last_bottom = None
            
            for detection in result:
                bbox, text, confidence = detection
                
                if threshold and confidence < threshold:
                    continue
                
                top = bbox[0][1]
                bottom = bbox[2][1]
                
                if last_bottom is not None and top > last_bottom + line_gap:
                    # This text is significantly below the previous line
                    if current_line:
                        lines.append(current_line)
                        current_line = []
                
                current_line.append((bbox, text, confidence))
                last_bottom = max(bottom, last_bottom) if last_bottom is not None else bottom
            
            if current_line:
                lines.append(current_line)
            
            # Sort each line by x-coordinate
            for i in range(len(lines)):
                lines[i].sort(key=lambda x: x[0][0][0])  # Sort by left x-coordinate
            
            # Construct the text
            file_text = ""
            confidences = []
            
            for line in lines:
                line_text = ""
                for _, text, confidence in line:
                    line_text += text + " "
                    confidences.append(confidence)
                file_text += line_text.strip() + "\n"
            
            file_text = file_text.strip()
            file_text = clean_whitespace(file_text)
            
            avg_confidence = sum(confidences) / max(len(confidences), 1)
            
            outputs.append({
                "transcription": file_text,
                "confidence": avg_confidence,
                "language": self.languages[0] if self.languages else None,
                "language_confidence": 1,  # EasyOCR doesn't provide language confidence
            })
        
        return assemble_slots(len(inputs), kept, outputs, slots)

    def _detect_bounded_recognize_raw(
        self, raw_images: List[np.ndarray], batch_params: dict
    ) -> List:
        """Batch the detector under the canvas; recognise from the raw image.

        `Reader.readtext_batched` split into `detect` and `recognize`: detection
        runs on canvas-bounded, padded arrays; boxes are mapped back to raw
        coordinates and crops taken from the raw image. Chunked at
        `max_detector_batch`.
        """
        canvas_size = self._batch_canvas_size(batch_params)
        detect_params = {
            key: value
            for key, value in batch_params.items()
            if key in DETECT_PARAMS
        }
        recognize_params = {
            key: value
            for key, value in batch_params.items()
            if key in RECOGNIZE_PARAMS
        }
        # `min_size` is applied in raw pixels after detection; 0 disables it.
        min_size = detect_params.get("min_size", DEFAULT_MIN_SIZE)
        detect_params["min_size"] = 0

        bounded: List[np.ndarray] = []
        scales: List[float] = []
        for raw in raw_images:
            array, scale = fit_to_canvas(raw, canvas_size)
            bounded.append(array)
            scales.append(scale)
        if len({array.shape for array in bounded}) > 1:
            bounded = pad_images_to_same_size(bounded)

        # The exact index ceiling, from the decoded arrays.
        tensor_dims = detector_tensor_dims(
            [(int(array.shape[0]), int(array.shape[1])) for array in bounded],
            canvas_size,
            self._batch_mag_ratio(batch_params),
        )
        chunk_cap = (
            max_batch_for_dims(tensor_dims)
            if self._index_ceiling_applies()
            else None
        )
        if chunk_cap is not None and chunk_cap < len(bounded):
            # Reported as `clamped.reason = "index_limit"`, not a memory event.
            note_index_limit_event()
            logger.warning(
                "capping easyOCR's detector batch at %d of %d inputs: a "
                "%s tensor costs %d pooling-output elements per item and "
                "the kernel indexes at most %d",
                chunk_cap,
                len(bounded),
                _dims_label(tensor_dims),
                detector_pool_elements(*tensor_dims) if tensor_dims else 0,
                KERNEL_INDEX_ELEMENT_LIMIT,
            )

        def process_chunk(chunk):
            # `reformat=False`: `reformat_input` cannot read a 4-D stack.
            horizontal_agg, free_agg = self.model.detect(
                np.stack([item[0] for item in chunk]),
                reformat=False,
                **detect_params,
            )
            results = []
            for (_, raw, scale), horizontal, free in zip(
                chunk, horizontal_agg, free_agg
            ):
                horizontal, free = scale_detections_to_original(
                    horizontal, free, scale
                )
                horizontal, free = filter_small_detections(
                    horizontal, free, min_size, raw.shape
                )
                # Default `reformat=True`, as `readtext_batched` does.
                results.append(
                    self.model.recognize(
                        raw, horizontal, free, **recognize_params
                    )
                )
            return results

        return run_with_oom_retry(
            process_chunk,
            list(zip(bounded, raw_images, scales)),
            initial_chunk_size=chunk_cap,
            logger=logger,
        )

    def _batch_canvas_size(self, batch_params: dict) -> int:
        """The canvas this batch is bounded by: the caller's, else ours."""
        return (
            _positive_int(batch_params.get("canvas_size")) or self.canvas_size
        )

    def _batch_mag_ratio(self, batch_params: dict) -> float:
        """The caller's `mag_ratio`, at least the default, so the ceiling never
        under-states the tensor."""
        value = batch_params.get("mag_ratio")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return DEFAULT_MAG_RATIO
        return float(value) if value > DEFAULT_MAG_RATIO else DEFAULT_MAG_RATIO

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

def fit_to_canvas(
    image: np.ndarray, canvas_size: int = DETECTOR_CANVAS_SIZE
) -> tuple[np.ndarray, float]:
    """Downscale `image` so its longer side is at most `canvas_size`, as
    easyOCR's `resize_aspect_ratio` does (so the detector's own resize is the
    identity). Never upscales. Returns `(array, scale)`.
    """
    height, width = int(image.shape[0]), int(image.shape[1])
    longest = max(height, width)
    if longest <= 0 or longest <= canvas_size:
        return image, 1.0
    ratio = canvas_size / longest
    target_h = max(1, int(height * ratio))
    target_w = max(1, int(width * ratio))
    try:
        import cv2

        resized = cv2.resize(
            image, (target_w, target_h), interpolation=cv2.INTER_LINEAR
        )
    except ImportError:  # pragma: no cover - easyocr depends on opencv
        resized = np.array(
            PILImage.fromarray(image).resize(
                (target_w, target_h), PILImage.BILINEAR
            )
        )
    return resized, ratio


def scale_detections_to_original(horizontal_list, free_list, scale: float):
    """Undo `fit_to_canvas`'s scale on one image's detections: horizontal boxes
    `[x_min, x_max, y_min, y_max]`, free boxes four `[x, y]` points.
    """
    if scale >= 1.0 or scale <= 0:
        return horizontal_list, free_list
    inverse = 1.0 / scale

    def horizontal(box):
        try:
            return [value * inverse for value in box]
        except Exception:  # pragma: no cover - defensive
            return box

    def free(box):
        try:
            return [[point[0] * inverse, point[1] * inverse] for point in box]
        except Exception:  # pragma: no cover - defensive
            return box

    return (
        [horizontal(box) for box in horizontal_list or []],
        [free(box) for box in free_list or []],
    )


def filter_small_detections(horizontal_list, free_list, min_size, shape):
    """easyOCR's `min_size` filter in the submitted image's pixels; also drops
    boxes wholly in the padding outside the raw image.
    """
    height, width = int(shape[0]), int(shape[1])

    def inside(x_min, x_max, y_min, y_max) -> bool:
        return (
            min(x_max, width) > max(x_min, 0)
            and min(y_max, height) > max(y_min, 0)
        )

    kept_horizontal = []
    for box in horizontal_list or []:
        try:
            x_min, x_max, y_min, y_max = box[0], box[1], box[2], box[3]
            if min_size and max(x_max - x_min, y_max - y_min) <= min_size:
                continue
            if not inside(x_min, x_max, y_min, y_max):
                continue
        except Exception:  # pragma: no cover - defensive
            pass
        kept_horizontal.append(box)

    kept_free = []
    for box in free_list or []:
        try:
            xs = [point[0] for point in box]
            ys = [point[1] for point in box]
            if min_size and max(
                max(xs) - min(xs), max(ys) - min(ys)
            ) <= min_size:
                continue
            if not inside(min(xs), max(xs), min(ys), max(ys)):
                continue
        except Exception:  # pragma: no cover - defensive
            pass
        kept_free.append(box)

    return kept_horizontal, kept_free


def pad_images_to_same_size(images: List[np.ndarray]) -> List[np.ndarray]:
        """
        Pad all images to the size of the largest image in the batch.

        Precondition: every member is already bounded by the canvas.

        Args:
            images: List of numpy arrays representing images

        Returns:
            List of padded images all with the same dimensions
        """
        if not images:
            return []
            
        # Find max height and width
        max_height = max(img.shape[0] for img in images)
        max_width = max(img.shape[1] for img in images)
        
        # Pad images to max dimensions
        padded_images = []
        for img in images:
            h, w = img.shape[:2]
            # Create a black canvas of the max size
            padded_img = np.zeros((max_height, max_width, 3), dtype=np.uint8)
            # Place the original image in the top-left corner
            padded_img[:h, :w] = img
            padded_images.append(padded_img)
            
        return padded_images

IMPL_CLASS = EasyOCRModel