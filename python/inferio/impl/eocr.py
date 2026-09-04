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
    looks_like_index_limit,
    note_index_limit_event,
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

# easyOCR's own `mag_ratio` default (`easyocr.Reader.readtext`). It is the
# one detect parameter besides `canvas_size` that moves the detector's tensor
# dimensions, so the ceiling arithmetic below has to read it.
DEFAULT_MAG_RATIO = 1.0

# `easyocr.imgproc.resize_aspect_ratio` pads each side of the detector's
# input up to the next multiple of this (`imgproc.py:54-61`).
DETECTOR_SIZE_MULTIPLE = 32

# ---------------------------------------------------------------------------
# The detector's hard batch ceiling
# ---------------------------------------------------------------------------
#
# CRAFT's batch dies of an **index**, not of memory, well before the board
# fills: at batch 29 of 2560-bounded A4 pages `torch.max_pool2d` inside the
# VGG backbone raises `RuntimeError: integer out of range`, with 3 GiB of a
# 96 GiB board still free (run2 `run2-probes-report.md`, S1).
#
# The cause, and therefore the formula:
#
# * `at::native::safe_downcast<int32_t, int64_t>` (`ATen/native/Pool.h:49-57`,
#   whose `TORCH_CHECK` message *is* "integer out of range") is applied by
#   CUDA's `max_pool2d_with_indices` forward to its **output element count**,
#   because the kernel is launched over that count as a signed 32-bit int.
#   The ceiling is therefore `2**31 - 1` **elements of one pooling output**,
#   not bytes, and no amount of free memory moves it.
# * The binding pool is the **first** one in `vgg16_bn`
#   (`torchvision.models.vgg16_bn().features[6]`, inside easyOCR's
#   `vgg16_bn.slice1 = features[0:12]`, `easyocr/model/modules.py:38-39`,
#   which is where run2's traceback lands). It is `MaxPool2d(2, 2)` over the
#   64-channel block, so its output is `B x 64 x H//2 x W//2` for a detector
#   input of `B x 3 x H x W`. Every later pool halves the resolution again
#   while only doubling the channels, so each one is at most half the size of
#   this one: 64·(H/2)·(W/2) > 128·(H/4)·(W/4) > 256·(H/8)·(W/8) > …. The
#   3-channel input tensor itself is 21x smaller again and never binds.
# * Convolutions do not share the ceiling — cuDNN indexes them in 64 bits or
#   splits the launch — which is why the measured boundary is the pool's and
#   not the 64-channel conv output's one item earlier.
#
# So, with `H`, `W` the padded (multiple-of-32) dimensions of the detector's
# input tensor:
#
#     per_item_elements = 64 * (H // 2) * (W // 2)     ( = 16*H*W, H,W even)
#     max_batch         = (2**31 - 1) // per_item_elements
#
# At run2's measured shape — `scan-2480x3508` fitted to the 2560 canvas is
# 1809x2560, padded to 1824x2560 — that is 64·912·1280 = 74 711 040 elements
# per item and `2147483647 // 74711040 = 28`: exactly the 28-ok/29-fail
# boundary the probes found. A square 2560x2560 page gives 20, and a
# 1240x1754 page (below the canvas, padded to 1248x1760) gives 61 — smaller
# pages really do allow more, which is why the cap is computed per batch from
# that batch's own padded dimensions and never fixed at a constant.
KERNEL_INDEX_ELEMENT_LIMIT = 2**31 - 1
DETECTOR_POOL_CHANNELS = 64

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


def _dims_label(dims: tuple[int, int] | None) -> str:
    """A padded tensor's dimensions for a log line, as `"width x height"`.

    The order every run2 report states them in (`1824x2560` for a 300 dpi A4
    scan under the 2560 canvas), which is the transpose of the `(height,
    width)` the arithmetic works in — so it is done in exactly one place.
    """
    return "unknown" if dims is None else f"{dims[1]}x{dims[0]}"


def _shape_as_height_width(shape) -> tuple[int, int] | None:
    """A harness `(width, height)` pair as `(height, width)`, or None.

    Defensive by design: the value crosses a process boundary as an image
    header reading, so anything that is not a usable pair of positive
    integers is "unknown", never a guess.
    """
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
    """[`fit_to_canvas`]'s output dimensions for `(height, width)`.

    The arithmetic only — no pixels are touched — so the packing harness can
    ask what a batch *would* build before anything is decoded.
    """
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
    """`(height, width)` of the CRAFT input tensor a batch of these builds.

    Three steps, in the order this impl and easyOCR perform them:

    1. [`fit_to_canvas`] bounds each raw shape ([`bounded_dims`]);
    2. [`pad_images_to_same_size`] takes the element-wise maximum, so the
       batch is one array at the largest bounded height by the largest
       bounded width — which can come from two different members;
    3. `easyocr.imgproc.resize_aspect_ratio` (`imgproc.py:37-61`) rescales
       that array so its longer side is `min(mag_ratio * longest,
       canvas_size)` and pads each side up to the next multiple of 32.

    Step 3 is the identity at the shipped `mag_ratio = 1` (step 1 already put
    the array at or under the canvas), but a caller *may* pass a larger one,
    which magnifies up to — never past — the canvas. Returns None when no
    shape is known.
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
    """Elements of the binding pooling output for one item of a `H x W` batch.

    `64 x H//2 x W//2` — the output of `vgg16_bn.features[6]`, the first and
    largest of CRAFT's pools. See the module header for why this one binds.
    """
    return DETECTOR_POOL_CHANNELS * (height // 2) * (width // 2)


def max_detector_batch(
    shapes: Sequence[tuple[int, int] | None],
    canvas_size: int = DETECTOR_CANVAS_SIZE,
    mag_ratio: float = DEFAULT_MAG_RATIO,
) -> int | None:
    """Largest batch of these shapes CRAFT's pooling kernel can index.

    `(2**31 - 1) // per_item_elements`, from the module header's derivation.
    Never below 1: a single item over the ceiling has no smaller batch to
    fall back to, and saying so is the caller's per-image fallback's job, not
    this function's. None when no shape is known.
    """
    dims = detector_tensor_dims(shapes, canvas_size, mag_ratio)
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
        # Two of the three things the worker's packing harness reads off a
        # loaded impl (docs/inferio-worker-protocol.md, "Memory grants"); the
        # third is [`max_batch_for`], the index ceiling, which is a question
        # about a specific batch rather than a constant:
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

    def max_batch_for(
        self, shapes: Sequence[tuple[int, int] | None]
    ) -> int | None:
        """Largest batch of these inputs one `predict` call can execute.

        The third attribute the worker's packing harness reads off a loaded
        impl (docs/inferio-worker-protocol.md, "Memory grants"), and the only
        one that is a *question* rather than a statement. It answers exactly
        one thing: the batch size above which a kernel's 32-bit element index
        overflows on the tensor this impl builds — see the module header's
        derivation and [`max_detector_batch`]. It is not a memory opinion;
        memory is the grant's business, and the two ceilings are unrelated
        (run2 measured this one firing with 3 GiB of 96 still free).

        `shapes` are `(width, height)` pairs, in PIL's order, because the
        harness reads them from an image header (`Image.size`); a member is
        None where the header could not be read. An unreadable member is
        charged the **square canvas**, this impl's worst case, on the same
        principle as the harness's own unreadable-input pricing: a shape we
        cannot see must not be assumed small.

        None means "no ceiling from me" — which is the honest answer when
        batching is disabled, since the unbatched path builds no batch tensor
        at all and easyOCR bounds each `readtext` call by itself.
        """
        if not self.enable_batching:
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
            except Exception as error:
                # Never a silent fallback. Before run2 this logged one line
                # carrying only `str(error)` and discarded the traceback, so
                # the one failure this path actually meets — CRAFT's pooling
                # kernel overflowing its 32-bit element index at batch 29 of
                # 2560-bounded pages — named neither the operator nor the
                # batch size, and the ledger, seeing a slower success rather
                # than a failure, kept widening `unit_budget` past a batch
                # this impl cannot execute (run2 probes report, S1).
                #
                # An index-limit failure only reaches here at a *single*
                # input: `run_with_oom_retry` halves on it above, and this
                # impl caps the batch before the kernel ever sees it. So this
                # is the honest last resort it was always meant to be, and it
                # says so with a traceback.
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

        The detector batch additionally carries a **hard ceiling that is not
        about memory**: CRAFT's first pooling kernel indexes its output in a
        signed 32-bit int, so the batch is chunked at
        [`max_detector_batch`] of the padded dimensions this batch actually
        builds — 28 items for canvas-bounded A4 pages, 20 for square ones.
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

        # The index ceiling, computed from the arrays that exist rather than
        # from headers: this is the authoritative one, and the harness's
        # `max_batch_for` pre-cap is the same arithmetic run early enough to
        # keep the batch priceable. Both are needed — an older orchestrator,
        # an unreadable header or a `mag_ratio` the harness cannot see all
        # leave the pre-cap absent or optimistic, and none of them may be
        # allowed to hand the kernel a batch it cannot index.
        tensor_dims = detector_tensor_dims(
            [
                (int(array.shape[0]), int(array.shape[1]))
                for array in bounded
            ],
            canvas_size,
            self._batch_mag_ratio(batch_params),
        )
        chunk_cap = max_detector_batch(
            [
                (int(array.shape[0]), int(array.shape[1]))
                for array in bounded
            ],
            canvas_size,
            self._batch_mag_ratio(batch_params),
        )
        if chunk_cap is not None and chunk_cap < len(bounded):
            # Reported, not swallowed: `note_index_limit_event` is what lets
            # the worker put `clamped.reason = "index_limit"` on this batch's
            # measurement, so the ledger sees a batch that ran short of its
            # budget for a reason that is not memory — instead of a silently
            # slower window (run2 probes report, S1).
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
            initial_chunk_size=chunk_cap,
            logger=logger,
        )

    def _batch_canvas_size(self, batch_params: dict) -> int:
        """The canvas this batch is bounded by: the caller's, else ours."""
        return (
            _positive_int(batch_params.get("canvas_size")) or self.canvas_size
        )

    def _batch_mag_ratio(self, batch_params: dict) -> float:
        """The caller's `mag_ratio`, else easyOCR's default.

        It only ever *magnifies*, and `resize_aspect_ratio` clamps its own
        target at the canvas, so a value below 1 (or a nonsensical one) is
        read as the default rather than allowed to shrink the estimate — the
        ceiling arithmetic must never under-state the tensor.
        """
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
    [`scale_detections_to_original`] needs to undo it.

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