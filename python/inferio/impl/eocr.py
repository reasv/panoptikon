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
        # since run2's D1-b fix, it is also this impl's *promise* that no
        # input reaches the detector or the recogniser above that area
        # (`fit_to_canvas` in `predict`). Declaring it and not enforcing it is
        # what under-prices a batch.
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
        # Read before the images are turned into arrays, because one of these
        # parameters — `canvas_size` — decides how large those arrays may be.
        batch_params = {}
        if kept:
            first_config = configs[kept[0]]
            for param in ['decoder', 'beamWidth', 'batch_size', 'workers', 'allowlist',
                          'blocklist', 'detail', 'rotation_info', 'paragraph', 'min_size',
                          'contrast_ths', 'adjust_contrast', 'filter_ths', 'text_threshold',
                          'low_text', 'link_threshold', 'canvas_size', 'mag_ratio',
                          'slope_ths', 'ycenter_ths', 'height_ths', 'width_ths', 'y_ths',
                          'x_ths', 'add_margin', 'threshold', 'bbox_min_score',
                          'bbox_min_size', 'max_candidates', 'output_format']:
                if param in first_config:
                    batch_params[param] = first_config[param]

        # **Bound every input by the canvas before the batch tensor exists.**
        #
        # The detector would resize each image onto `canvas_size` itself — but
        # only *after* `pad_images_to_same_size` has already materialised every
        # member of the batch at the largest member's raw dimensions, and only
        # for the detector: the recogniser crops from the same raw-sized array.
        # Under the run2 R7 price cap that is a mispricing, not just waste:
        # every input at or above the canvas prices identically
        # (`min(raw, 6 553 600)`), so a 2480x3508 scan and an 8000x6000 sheet
        # are indistinguishable to the harness's bucketing and can share a
        # batch whose tensor is then 5.5x the area the batch was charged for
        # (run2 D1-b; six such batches measured at 2 209-4 538 ms against
        # 1 146-1 236 ms for size-homogeneous ones).
        #
        # Declaring a canvas is a statement that the model never processes more
        # than that area per item, so this impl makes the statement true here,
        # before anything is padded or batched. `fit_to_canvas` is the
        # detector's own resize (`easyocr.imgproc.resize_aspect_ratio`,
        # downscale half), so for the detector this is a no-op moved earlier;
        # what changes is that the recogniser now crops from the same
        # canvas-bounded array, which is the half that was never bounded.
        canvas_size = (
            _positive_int(batch_params.get("canvas_size")) or self.canvas_size
        )
        image_inputs: List[np.ndarray] = []
        scales: List[float] = []
        for image in images:
            array, scale = fit_to_canvas(np.array(image), canvas_size)
            image_inputs.append(array)
            scales.append(scale)

        # Check if we need to pad images
        heights = [img.shape[0] for img in image_inputs]
        widths = [img.shape[1] for img in image_inputs]

        use_batched = self.enable_batching and len(image_inputs) > 1

        # If images have different sizes, pad them. Every member is already
        # bounded by the canvas, so the padded tensor is too.
        if (len(set(heights)) > 1 or len(set(widths)) > 1) and use_batched:
            image_inputs = pad_images_to_same_size(image_inputs)

        batch_results = []
        # Process with batched method
        if use_batched:
            try:
                batch_results = run_with_oom_retry(
                    lambda chunk: self.model.readtext_batched(
                        list(chunk), **batch_params
                    ),
                    image_inputs,
                    logger=logger,
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
            # Process images individually
            batch_results = []
            for img in image_inputs:
                result = self.model.readtext(img, **batch_params)
                batch_results.append(result)

        # Detection ran on the canvas-bounded array, so every box comes back in
        # *that* space. Put them back in the coordinates of the image that was
        # submitted, which is the space easyOCR's own boxes are in when it does
        # the resize internally (`craft_utils.adjustResultCoordinates` undoes
        # the detector's ratio the same way). The line grouping below is
        # scale-invariant, but the boxes are the only geometry this impl ever
        # sees and they must mean what they say.
        batch_results = [
            scale_boxes_to_original(result, scale)
            for result, scale in zip(batch_results, scales)
        ]

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


def scale_boxes_to_original(results, scale: float):
    """Undo [`fit_to_canvas`]'s ratio on every detected box.

    Defensive by construction: easyOCR's return shape depends on `detail`,
    `paragraph` and `output_format`, all of which a caller may set per
    request, so anything that does not look like `(4-point box, ...)` is
    passed through untouched rather than guessed at. A no-op at `scale == 1`,
    which is every image that was already inside the canvas.
    """
    if not results or scale >= 1.0 or scale <= 0:
        return results
    inverse = 1.0 / scale
    scaled = []
    for entry in results:
        try:
            box = entry[0]
            moved = [[point[0] * inverse, point[1] * inverse] for point in box]
            scaled.append([moved, *list(entry[1:])])
        except Exception:
            scaled.append(entry)
    return scaled


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