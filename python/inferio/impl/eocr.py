import re
import logging
from io import BytesIO
from typing import List, Sequence, Type
import numpy as np
from PIL import Image as PILImage
from inferio.impl.utils import (
    InferenceOOMError,
    assemble_slots,
    clean_whitespace,
    clear_cache,
    decode_image_inputs,
    get_device,
    run_with_oom_retry,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

logger = logging.getLogger(__name__)

# EasyOCR's own default `canvas_size`, and therefore this model's canvas: the
# CRAFT detector resizes every input so its **longer side** is at most this
# many pixels (`easyocr.imgproc.resize_aspect_ratio`, never upscaling at
# `mag_ratio = 1`), then pads each side up to the next multiple of 32 — which
# 2560 already is. `2560**2 = 6 553 600` is thus the supremum of the area one
# input can cost the detector, and it is the figure the registry declares as
# `metadata.cost.canvas_pixels` for the three `doctr/easyocr_*` ids.
DETECTOR_CANVAS_SIZE = 2560

# EasyOCR's own `min_size` default (`easyocr.Reader.readtext`): boxes whose
# longer side is at or below this many pixels **of the submitted image** are
# dropped. The batched path applies it here rather than inside `detect`, so
# that it keeps meaning raw pixels once detection has moved onto the canvas.
DEFAULT_MIN_SIZE = 20

# The per-request parameters this impl forwards, split by the easyOCR call
# each one belongs to. `readtext`/`readtext_batched` take the union and route
# them internally; the batched path here calls `Reader.detect` and
# `Reader.recognize` itself (see `predict`), so it has to do that routing.
#
# `threshold` is easyOCR's DBNet box threshold and goes to the detector; this
# impl *also* reads a `threshold` off the same config as its own confidence
# floor, which is pre-existing and left alone.
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
# The allow-list is the union by construction, so a parameter can never be
# accepted from a caller and then silently dropped on the batched path.
BATCH_PARAMS = frozenset(DETECT_PARAMS | RECOGNIZE_PARAMS)


def _positive_int(value) -> int | None:
    """`value` as a positive int, or None. Refuses bools: a per-request
    `canvas_size = True` must not become a 1-pixel canvas."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = int(value)
    except Exception:
        return None
    return number if number > 0 else None


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
        # The two attributes the worker's packing harness reads off a loaded
        # impl (docs/inferio-worker-protocol.md, "Memory grants"):
        #
        # `canvas_pixels` is tier 2 of the canvas resolution order — what this
        # model knows about itself when the registry declares nothing — and,
        # since run2's D1-b fix, it is also this impl's *promise* that the
        # batch tensor it builds never exceeds that area per item
        # (`fit_to_canvas`, in `_detect_bounded_recognize_raw`). Declaring it
        # and not enforcing it is what under-prices a batch. The promise is
        # about the tensor, not about every array: the recogniser's crops come
        # from the raw image, and cost the same either way because each crop
        # is resized to a fixed `imgH x imgW` before it becomes a tensor.
        #
        # `pads_to_common_size` says this impl builds one batch tensor at the
        # dimensions of its largest member (`pad_images_to_same_size`), so its
        # cost is a function of that member and not of the batch's mean. The
        # harness pairs the two: an impl that pads *and* states no canvas of
        # its own gets a one-shot warning when a priced-flat batch mixes raw
        # sizes, because that is the shape D1-b measured.
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
        # Derived from the device we resolved, not from a second probe of the
        # hardware: `torch.cuda.is_available()` answers about the *machine*,
        # and on a host the orchestrator priced against system RAM
        # (`INFERIO_DEVICE=cpu`) that is not the question — the model must run
        # where it is budgeted (docs/unified-memory-admission.md, backend C).
        # EasyOCR's `gpu` argument only ever means CUDA/HIP, so any other
        # device kind (Metal, CPU) is CPU here.
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
        #
        # Read before anything is batched, because two of these parameters
        # decide how the batched path is built: `canvas_size` bounds the
        # detector's tensor, and `min_size` is applied by this impl rather
        # than by `Reader.detect` (see `_detect_bounded_recognize_raw`).
        batch_params = {}
        if kept:
            first_config = configs[kept[0]]
            for param in sorted(BATCH_PARAMS):
                if param in first_config:
                    batch_params[param] = first_config[param]

        # Every image at the resolution it was submitted at. These arrays
        # exist either way: `decode_image_inputs` above has already decoded
        # each payload at full size, so nothing below *adds* a raw-sized
        # allocation — the canvas is a statement about the tensors this impl
        # builds, not about the decode buffer every impl holds.
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
            except Exception as e:
                # Fall back to individual processing if batched processing fails
                logger.error(f"Batch processing failed with error: {e}. Falling back to individual processing.")
                use_batched = False

        if not use_batched:
            # Process images individually, at the resolution the caller
            # submitted. There is no batch tensor on this path — easyOCR's own
            # `resize_aspect_ratio` bounds the detector for us
            # (`easyocr/detection.py:33`) and the recogniser's tensor is a
            # fixed `imgH x imgW` per crop regardless
            # (`easyocr/recognition.py:42-45`) — so bounding here would cost
            # transcription quality and save nothing.
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

        This is `easyocr.Reader.readtext_batched` (`easyocr/easyocr.py:538-579`)
        taken apart into the two public calls it is made of, because the two
        halves want different arrays:

        * **Detection** is the half whose tensor scales with the input's area
          (`detection.py:24-46`: every member of the batch is resized onto
          `canvas_size` and stacked into one CRAFT input), and it is the half
          the batch tensor exists for. `pad_images_to_same_size` builds that
          batch at its largest member's dimensions, so each input is bounded
          by the canvas *first* (`fit_to_canvas`). Two things follow: the
          padded array is inside the area the window was priced for
          (`min(raw, canvas_pixels)` — run2 R7), and a small image is no
          longer shrunk into the corner of a huge frame and then downscaled
          again by the detector, which is what a mixed batch used to do to it.
        * **Recognition** is not that half. Every crop is resized to the
          recogniser's fixed `imgH x imgW` before it becomes a tensor
          (`utils.py:566-577` then `recognition.py:70-97`, `NormalizePAD` at
          `:42-45`), so its device memory is `batch_size x 1 x imgH x imgW` —
          independent of the page's resolution. Cropping from the
          canvas-bounded array would therefore hand the recogniser a ~0.32x
          resolution crop on an 8000px sheet and buy exactly nothing, so the
          crops come from the **raw** array, as they did before run2 D1-b.

        The boxes bridge the two: they come back in the bounded array's space
        and are mapped to raw coordinates before recognition, which is also
        what makes `min_size` keep meaning raw pixels (it is applied here
        rather than inside `detect`, see [`filter_small_detections`]).
        """
        canvas_size = (
            _positive_int(batch_params.get("canvas_size")) or self.canvas_size
        )
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
        # Detection must not filter: its boxes are in canvas space, where a
        # `min_size` in raw pixels means something else. Zero disables the
        # filter inside easyOCR (`easyocr.py:343`, a plain truthiness test).
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

        def process_chunk(chunk):
            # One stacked 4-D array is what `test_net` batches
            # (`detection.py:25`); `reformat=False` because the members are
            # already decoded arrays, and `reformat_input` cannot read a 4-D
            # one at all.
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
                # `reformat=True` (the default) is deliberate: it runs the raw
                # array through easyOCR's own `reformat_input`, so the grey
                # image the crops come from is byte-for-byte the one
                # `readtext_batched` would have produced.
                results.append(
                    self.model.recognize(
                        raw, horizontal, free, **recognize_params
                    )
                )
            return results

        return run_with_oom_retry(
            process_chunk,
            list(zip(bounded, raw_images, scales)),
            logger=logger,
        )

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            clear_cache()
            self._model_loaded = False

def fit_to_canvas(
    image: np.ndarray, canvas_size: int = DETECTOR_CANVAS_SIZE
) -> tuple[np.ndarray, float]:
    """Downscale `image` so its longer side is at most `canvas_size`.

    The downscale half of easyOCR's own `imgproc.resize_aspect_ratio` at
    `mag_ratio = 1`: the ratio is `canvas_size / max(height, width)`, the
    target dimensions are that ratio times each side truncated to an int, and
    the interpolation is `cv2.INTER_LINEAR` — the same call the CRAFT detector
    would make on this array a moment later. Because the array handed to the
    detector is then already at (or below) the canvas, its own resize becomes
    the identity (`target_size = min(mag_ratio * max(h, w), canvas_size)`,
    hence `ratio = 1`), so nothing is interpolated twice.

    Never upscales: a small image keeps every pixel it was submitted with, and
    the detector may still magnify it if the caller asked for `mag_ratio > 1`
    — which cannot breach the canvas, since `resize_aspect_ratio` caps its own
    target at `canvas_size` regardless.

    Returns `(array, scale)`, `scale` being what the original was multiplied
    by (1.0 when nothing was resized), which is what
    [`scale_boxes_to_original`] needs to undo it.

    Falls back to Pillow's bilinear resize if OpenCV is not importable —
    easyOCR depends on it, so that path is unreachable wherever this model can
    actually run, and the *bound* (the target dimensions) is identical either
    way; only the interpolation kernel differs.
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
    """Undo [`fit_to_canvas`]'s ratio on one image's detections.

    `Reader.detect` returns boxes in the space of the array it was handed —
    it has already undone the *detector's* own internal ratio
    (`craft_utils.adjustResultCoordinates`, `detection.py:59-60`), but not
    ours. Undoing ours is what lets the crops be taken from the raw image,
    which is the whole point of the split in
    [`EasyOCRModel._detect_bounded_recognize_raw`].

    Two shapes, both from `utils.group_text_box`: a horizontal box is
    `[x_min, x_max, y_min, y_max]`, a free box is four `[x, y]` points. A
    no-op at `scale == 1`, which is every image already inside the canvas.
    Anything unreadable is passed through untouched rather than guessed at.
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
    """easyOCR's own `min_size` filter, in the submitted image's pixels.

    A verbatim restatement of `easyocr.py:343-347` — drop a box whose longer
    side is not greater than `min_size` — applied here because the detector
    ran on the canvas-bounded array, where "20 pixels" would silently mean 62
    raw pixels on an 8000px sheet. Running it after
    [`scale_detections_to_original`] keeps the threshold meaning what the
    caller (and the unbatched path, and every pre-run2 release) means by it.

    Also drops a box that does not intersect the raw image at all: detection
    ran on a padded frame that is larger than this image, so a box found in
    the padding has nowhere to be cropped from. `utils.get_image_list` clamps
    a *partly* outside box itself (`:601-604`), so only the empty case needs
    handling here.
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

        **Precondition**: every member is already bounded by the model's
        canvas ([`fit_to_canvas`]). This function's cost is set by the batch's
        *largest* member, so a batch mixing raw sizes pays the largest one's
        area for every item — and under the run2 R7 price cap those items can
        price identically, which is what made run2 D1-b an under-priced batch
        rather than merely a wasteful one. Padding after the bound keeps the
        tensor inside what the batch was charged for.

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