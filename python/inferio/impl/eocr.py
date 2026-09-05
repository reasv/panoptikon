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

# EasyOCR's own default `canvas_size`, and therefore this model's canvas: the
# CRAFT detector bounds every input's longer side at it. Its square is the
# registry's `metadata.cost.canvas_pixels` for the `doctr/easyocr_*` ids.
DETECTOR_CANVAS_SIZE = 2560

# EasyOCR's own `min_size` default: boxes whose longer side is at or below
# this many pixels **of the submitted image** are dropped. The batched path
# applies it here, not inside `detect`, so it keeps meaning raw pixels.
DEFAULT_MIN_SIZE = 20

# easyOCR's own `mag_ratio` default: the one detect parameter besides
# `canvas_size` that moves the detector's tensor dimensions.
DEFAULT_MAG_RATIO = 1.0

# `easyocr.imgproc.resize_aspect_ratio` pads each side of the detector's input
# up to the next multiple of this.
DETECTOR_SIZE_MULTIPLE = 32

# The detector's hard batch ceiling: CUDA's `max_pool2d_with_indices` downcasts
# its output element count to a signed 32-bit int, so CRAFT's first pool
# (`vgg16_bn.features[6]`, `B x 64 x H//2 x W//2`) refuses a batch whatever the
# GPU has free; CPU torch's pooling kernel indexes in 64 bits and has no such
# limit, hence [`EasyOCRModel._index_ceiling_applies`]. Derivation and worked
# figures: docs/inferio-worker-protocol.md, "The easyOCR ceiling in full".
KERNEL_INDEX_ELEMENT_LIMIT = 2**31 - 1
DETECTOR_POOL_CHANNELS = 64

# The per-request parameters this impl forwards, split by the easyOCR call
# each belongs to: the batched path calls `Reader.detect` and
# `Reader.recognize` itself, so it has to route them. `threshold` here is
# easyOCR's DBNet box threshold, not this impl's own confidence floor.
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
# The union by construction, so a parameter can never be accepted from a
# caller and then silently dropped on the batched path.
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


def _dims_label(dims: tuple[int, int] | None) -> str:
    """A padded tensor's dimensions for a log line, as `"width x height"` —
    the transpose of the `(height, width)` the arithmetic works in, done in
    exactly one place."""
    return "unknown" if dims is None else f"{dims[1]}x{dims[0]}"


def _shape_as_height_width(shape) -> tuple[int, int] | None:
    """A harness `(width, height)` pair as `(height, width)`, or None. The
    value crosses a process boundary as a header reading, so anything that is
    not a pair of positive integers is "unknown", never a guess."""
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
    """[`fit_to_canvas`]'s output dimensions for `(height, width)`: the
    arithmetic only, so the harness can ask what a batch *would* build before
    anything is decoded."""
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
    """`(height, width)` of the CRAFT input tensor a batch of these builds:
    each shape bounded by the canvas, then the element-wise maximum, then
    `resize_aspect_ratio`'s own rescale and pad to a multiple of 32. That last
    step is the identity at the shipped `mag_ratio = 1`. None when no shape is
    known. See docs/inferio-worker-protocol.md, "The easyOCR ceiling in full".
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
    """Elements of the binding pooling output for one item of a `H x W` batch:
    `64 x H//2 x W//2`, the output of `vgg16_bn.features[6]`."""
    return DETECTOR_POOL_CHANNELS * (height // 2) * (width // 2)


def max_detector_batch(
    shapes: Sequence[tuple[int, int] | None],
    canvas_size: int = DETECTOR_CANVAS_SIZE,
    mag_ratio: float = DEFAULT_MAG_RATIO,
) -> int | None:
    """Largest batch of these shapes CRAFT's pooling kernel can index,
    `(2**31 - 1) // per_item_elements`. Never below 1: a single item over the
    ceiling is the caller's per-image fallback's problem. None when no shape
    is known.
    """
    dims = detector_tensor_dims(shapes, canvas_size, mag_ratio)
    return max_batch_for_dims(dims)


def max_batch_for_dims(dims: tuple[int, int] | None) -> int | None:
    """[`max_detector_batch`] for a tensor whose padded dims are already
    known, so a caller holding them does not price them twice."""
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
        # Two of the three things the packing harness reads off a loaded impl
        # (docs/inferio-worker-protocol.md, "Memory grants"); the third,
        # [`max_batch_for`], is a question about a specific batch.
        # `canvas_pixels` is tier 2 of the canvas resolution order *and* this
        # impl's promise that the batch tensor never exceeds that area per
        # item — about the tensor, not about every array.
        # `pads_to_common_size` says the tensor is built at its largest
        # member's dimensions (`pad_images_to_same_size`).
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
        # From the device we resolved, not a second probe of the hardware:
        # the model must run where it is budgeted, not where the machine
        # happens to have CUDA (docs/unified-memory-admission.md, backend C).
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
        """Whether CRAFT will run where the pooling kernel has the ceiling.

        Only the CUDA kernel has it, and `clamped.reason = "index_limit"` is
        the signal the ledger treats as permanent, so it must not be asserted
        on a host without the limit. Answers **True unless it can positively
        establish otherwise**: a missing cap costs a failed batch, a needless
        one at most a smaller batch. `gpu = False` is the operator saying CPU;
        a loaded model uses the device `load` resolved (HIP compiles the same
        kernel and torch spells it `cuda`); an unloaded one is charged it.
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
        """Largest batch of these inputs one `predict` call can execute.

        The packing harness's shape-ceiling hook (protocol doc, "Memory
        grants"): the batch size above which a kernel's 32-bit element index
        overflows on the tensor this impl builds ([`max_detector_batch`]).
        Never a memory opinion.

        `shapes` are `(width, height)` pairs in PIL's order, None where a
        header could not be read; an unreadable member is charged the square
        canvas, this impl's worst case. None means "no ceiling from me":
        batching is off, or [`_index_ceiling_applies`] is false. It answers
        for the *configured* canvas at `mag_ratio = 1`, so a per-request one —
        which the harness never sees — makes it optimistic, and the exact cap
        in the batched path binds then.
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
        # Read before anything is batched: `canvas_size` bounds the
        # detector's tensor and `min_size` is applied by this impl.
        batch_params = {}
        if kept:
            first_config = configs[kept[0]]
            for param in sorted(BATCH_PARAMS):
                if param in first_config:
                    batch_params[param] = first_config[param]

        # Every image at the resolution it was submitted at; these arrays
        # exist either way, `decode_image_inputs` having decoded them.
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
                # Never a silent fallback: the traceback and the padded
                # tensor's dimensions are what say whether this was the index
                # ceiling, which only reaches here at a single input.
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
            # Individually, at the resolution the caller submitted: there is
            # no batch tensor here and easyOCR bounds the detector itself, so
            # bounding here would only cost transcription quality.
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

        `easyocr.Reader.readtext_batched` taken apart into the two public
        calls it is made of, because the halves want different arrays. Only
        detection's tensor scales with the input's area, so each input is
        bounded by the canvas before `pad_images_to_same_size` builds the
        batch. Recognition resizes every crop to a fixed `imgH x imgW`
        regardless, so its crops come from the **raw** array; the boxes are
        mapped back to raw coordinates first, which also keeps `min_size`
        meaning raw pixels ([`filter_small_detections`]). The batch is
        additionally chunked at [`max_detector_batch`], a shape ceiling and
        not a memory one (docs/inferio-worker-protocol.md, "Memory grants").
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
        # Detection must not filter: its boxes are in canvas space, where a
        # `min_size` in raw pixels means something else. Zero disables it.
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

        # The index ceiling from the arrays that exist rather than from
        # headers: the authoritative one, the harness's `max_batch_for`
        # pre-cap being the same arithmetic run early enough to price it.
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
            # Reported, not swallowed: this is what puts
            # `clamped.reason = "index_limit"` on the measurement, so the
            # ledger sees a short batch whose reason is not memory.
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
            # One stacked 4-D array is what `test_net` batches;
            # `reformat=False` because `reformat_input` cannot read one.
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
                # `reformat=True` (the default) is deliberate: the grey image
                # the crops come from is then byte-for-byte the one
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
            initial_chunk_size=chunk_cap,
            logger=logger,
        )

    def _batch_canvas_size(self, batch_params: dict) -> int:
        """The canvas this batch is bounded by: the caller's, else ours."""
        return (
            _positive_int(batch_params.get("canvas_size")) or self.canvas_size
        )

    def _batch_mag_ratio(self, batch_params: dict) -> float:
        """The caller's `mag_ratio`, else easyOCR's default. It only ever
        magnifies, so a value below 1 is read as the default rather than
        allowed to shrink the estimate: the ceiling arithmetic must never
        under-state the tensor."""
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
    """Downscale `image` so its longer side is at most `canvas_size`.

    The downscale half of easyOCR's own `imgproc.resize_aspect_ratio` at
    `mag_ratio = 1`, down to the `cv2.INTER_LINEAR` call, so the detector's
    own resize becomes the identity and nothing is interpolated twice. Never
    upscales. Returns `(array, scale)`, what the original was multiplied by,
    which [`scale_detections_to_original`] undoes. The Pillow fallback bounds
    the array identically; only the interpolation kernel differs.
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
    """Undo [`fit_to_canvas`]'s ratio on one image's detections, which is what
    lets the crops be taken from the raw image (`Reader.detect` has undone the
    detector's own internal ratio, but not ours). Two box shapes from
    `utils.group_text_box`: horizontal is `[x_min, x_max, y_min, y_max]`, free
    is four `[x, y]` points. A no-op at `scale == 1`; anything unreadable is
    passed through untouched.
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

    Drop a box whose longer side is not greater than `min_size`, applied after
    [`scale_detections_to_original`] because the detector ran on the bounded
    array, where "20 pixels" would mean 62 raw pixels on an 8000px sheet.
    Also drops a box that does not intersect the raw image at all: detection
    ran on a padded frame larger than it, so a box in the padding has nowhere
    to be cropped from (`utils.get_image_list` clamps a *partly* outside one).
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
        canvas ([`fit_to_canvas`]), so the tensor stays inside what the batch
        was charged for — the cost is set by the largest member, and a canvas
        price cap can price those members identically.

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