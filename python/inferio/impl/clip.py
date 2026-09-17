import logging
from io import BytesIO
from typing import List, Sequence, Type, Union

from PIL import Image as PILImage
from PIL import ImageFile

from inferio.impl.utils import (
    clear_cache,
    get_device,
    load_image_or_slot,
    run_with_oom_retry,
    serialize_array,
)
from inferio.model import InferenceModel
from inferio.inferio_types import PredictionInput

ImageFile.LOAD_TRUNCATED_IMAGES = True

logger = logging.getLogger(__name__)


def _norm_module_types() -> tuple:
    """Modules whose parameters must stay fp32 in a low-precision model."""
    import torch

    types = [
        torch.nn.LayerNorm,
        torch.nn.GroupNorm,
        torch.nn.modules.batchnorm._BatchNorm,
    ]
    rms_norm = getattr(torch.nn, "RMSNorm", None)
    if rms_norm is not None:
        types.append(rms_norm)
    return tuple(types)


def finish_lp_conversion(model, precision: str, logger=logger) -> List[str]:
    """Cast the fp32 parameters open_clip's fp16/bf16 conversion leaves behind.

    `convert_weights_to_lp` casts module *weights* — Conv1d/2d, Linear,
    MultiheadAttention projections — plus exactly two named Parameters
    (`text_projection`, visual `proj`). Every other bare `nn.Parameter` stays
    fp32. The plain towers survive that because they cast their own leftovers
    at the use site (`self.positional_embedding.to(cast_dtype)`), but a model
    that feeds such a parameter straight into a converted module does not:
    CoCa's `visual.attn_pool.query` and `text.cls_emb` reach a half-precision
    MultiheadAttention as fp32 and raise `expected scalar type Half but found
    Float` (CUDA) / `mat1 and mat2 must have the same dtype` (CPU), on both
    the image and the text path.

    So finish the job, keeping normalization parameters in fp32 — the same
    policy open_clip's own timm branch of `_set_model_device_and_precision`
    applies (cast the whole model, then put the norms back). For a model that
    already works this is a no-op in value as well as in dtype: the leftovers
    are the ones the towers were casting to this dtype at every forward
    anyway.

    Returns the qualified names of the parameters it cast.
    """
    import torch

    dtypes = {"fp16": torch.float16, "bf16": torch.bfloat16}
    dtype = dtypes.get(precision)
    if dtype is None:  # fp32, or a pure_* mode that cast everything already
        return []

    norms = _norm_module_types()
    converted: List[str] = []
    for module_name, module in model.named_modules():
        if isinstance(module, norms):
            continue
        for name, param in module.named_parameters(recurse=False):
            if param.dtype is not torch.float32:
                continue
            param.data = param.data.to(dtype)
            converted.append(f"{module_name}.{name}" if module_name else name)

    if converted:
        logger.debug(
            "Cast %d parameter(s) open_clip left in fp32 to %s: %s",
            len(converted),
            precision,
            ", ".join(converted),
        )
    return converted


def promote_plain_layernorms(model, precision: str, logger=logger) -> List[str]:
    """Give every remaining plain LayerNorm the fp32-upcasting forward.

    open_clip builds a low-precision native tower out of `LayerNormFp32`,
    which computes in fp32 and casts back, precisely because the converter
    leaves norm weights in fp32 while the activations flowing through them are
    half. A plain `nn.LayerNorm` (and open_clip's own `LayerNorm`, which only
    casts the *output* back) does not: `F.layer_norm` with a half input and
    fp32 weights raises `expected scalar type Half but found Float` on CUDA.
    CPU's kernel tolerates the mismatch, which is why this only shows up on a
    GPU.

    `VisionTransformer.__init__` builds its `AttentionalPooler` without
    forwarding `norm_layer`, so `attn_pool.ln_q`/`ln_k` keep the default plain
    `LayerNorm` no matter what precision the tower was built for — CoCa's
    image path dies there before it reaches anything
    `finish_lp_conversion` fixed. Applying open_clip's own rule to the norms
    it missed is a structural fix, not a per-model one.

    Only norms whose affine parameters are still fp32 are promoted: a
    timm-backed tower casts its norms to the low precision wholesale (its
    branch of `_set_model_device_and_precision` restores only `LayerNormFp32`
    instances), so nothing there mismatches and nothing there is touched. The
    weight and bias tensors are moved over as-is — same objects, same eps,
    same shape — so the promotion cannot change a value.

    Returns the qualified names of the modules it promoted.
    """
    import torch

    if precision not in ("fp16", "bf16"):
        return []

    try:
        from open_clip.transformer import LayerNormFp32
    except ImportError:  # pragma: no cover - open_clip is a hard dep of load()
        return []

    promoted: List[str] = []
    for parent_name, parent in list(model.named_modules()):
        for name, child in list(parent.named_children()):
            if not isinstance(child, torch.nn.LayerNorm):
                continue
            if isinstance(child, LayerNormFp32):
                continue
            if child.weight is None or child.weight.dtype is not torch.float32:
                continue
            replacement = LayerNormFp32(
                child.normalized_shape,
                eps=child.eps,
                elementwise_affine=child.elementwise_affine,
            )
            replacement.weight = child.weight
            replacement.bias = child.bias
            replacement.training = child.training
            setattr(parent, name, replacement)
            promoted.append(f"{parent_name}.{name}" if parent_name else name)

    if promoted:
        logger.debug(
            "Promoted %d plain LayerNorm(s) to LayerNormFp32: %s",
            len(promoted),
            ", ".join(promoted),
        )
    return promoted


class ClipModel(InferenceModel):
    def __init__(
        self,
        model_name: str,
        pretrained: str | None = None,
        context_length: int | None = None,
        precision: str = "fp16",
        init_args: dict = {},
    ):
        self.model_name: str = model_name
        self.pretrained: str | None = pretrained
        self.context_length: int | None = context_length
        # fp16 halves resident VRAM (3829 -> 1982 MiB for ViT-H-14-378) and is
        # substantially faster, since fp32 matmuls do not use tensor cores.
        # Retrieval impact was measured as negligible; see
        # docs/clip-fp16-precision-evaluation.md. Override per inference id
        # with `config.precision` if a model or GPU needs fp32.
        self.precision: str = precision
        self.init_args = init_args
        self._model_loaded: bool = False

    @classmethod
    def name(cls) -> str:
        return "openclip"

    def load(self) -> None:
        if self._model_loaded:
            return
        import open_clip

        self.devices = get_device()
        self.device = (
            self.devices[0] if isinstance(self.devices, list) else self.devices
        )
        precision = self._effective_precision()

        self.model, _, preprocess = open_clip.create_model_and_transforms(
            model_name=self.model_name,
            pretrained=self.pretrained,
            precision=precision,
            **self.init_args,
        )
        assert not isinstance(
            preprocess, tuple
        ), "Expected single preprocess function"
        self.preprocess = preprocess

        # open_clip builds and converts on CPU; moving afterwards transfers
        # half the bytes, so a low-precision load is faster, not slower.
        # Finish its conversion there too, for the same reason: the leftover
        # fp32 parameters, then the norms it built plain.
        finish_lp_conversion(self.model, precision, logger=logger)
        promote_plain_layernorms(self.model, precision, logger=logger)
        self.model.eval().to(self.device)
        self.input_dtype = self._input_dtype()
        self.tokenizer = open_clip.get_tokenizer(
            model_name=self.model_name, context_length=self.context_length
        )
        self._model_loaded = True

    def _effective_precision(self) -> str:
        """Low precision only on CUDA/ROCm; fp16 on CPU is slow and patchily
        supported, and MPS is unvalidated for this path."""
        if self.precision == "fp32" or self.device.type == "cuda":
            return self.precision
        logger.warning(
            "Precision %r requested for %s but device is %s; using fp32.",
            self.precision,
            self.model_name,
            self.device.type,
        )
        return "fp32"

    def _input_dtype(self):
        """Dtype the image tower expects its input in.

        open_clip's fp16/bf16 modes cast weights only and leave input casting
        to the caller (the same contract as OpenAI's original reference
        implementation, `model.encode_image(image.half())`). Feeding fp32
        pixels to converted weights raises at the patch-embed conv. Taking the
        first conv/linear in the visual tower covers both the native and the
        timm-backed branches of `_set_model_device_and_precision`.
        """
        import torch

        for module in self.model.visual.modules():
            if isinstance(
                module, (torch.nn.Conv1d, torch.nn.Conv2d, torch.nn.Conv3d, torch.nn.Linear)
            ):
                return module.weight.dtype
        return torch.float32

    def predict(
        self, inputs: Sequence[PredictionInput]
    ) -> Sequence[Union[bytes, dict, list, str]]:
        import torch

        # Ensure the model is loaded
        self.load()

        text_inputs = []
        image_inputs = []
        results: List[None | bytes | dict] = [None] * len(inputs)

        # Separate text and image inputs, storing their original indices
        for idx, input_item in enumerate(inputs):
            if input_item.file:
                # An undecodable payload takes its own slot instead of the
                # whole batch (docs/inferio-worker-protocol.md).
                image, slot = load_image_or_slot(input_item.file, logger=logger)
                if slot is not None:
                    results[idx] = slot
                    continue
                image_inputs.append((idx, image))
            else:
                assert isinstance(
                    input_item.data, dict
                ), "Input must be a dictionary"
                assert "text" in input_item.data, "Input must have 'text' key"
                text_inputs.append((idx, input_item.data["text"]))

        def encode_text_chunk(chunk):
            tokens = torch.tensor(self.tokenizer(list(chunk))).to(self.device)
            features = self.model.encode_text(tokens, normalize=True)
            return [
                serialize_array(features[i].cpu().numpy())
                for i in range(features.size(0))
            ]

        def encode_image_chunk(chunk):
            processed = torch.stack(
                [self.preprocess(img) for img in chunk]  # type: ignore
            ).to(self.device, dtype=self.input_dtype)
            features = self.model.encode_image(processed, normalize=True)
            return [
                serialize_array(features[i].cpu().numpy())
                for i in range(features.size(0))
            ]

        # Use inference_mode for optimized inference
        with torch.inference_mode():
            # Process text inputs if any
            if text_inputs:
                indices, texts = zip(*text_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_text_chunk, list(texts), logger=logger
                    ),
                ):
                    results[idx] = res

            # Process image inputs if any
            if image_inputs:
                indices, images = zip(*image_inputs)
                for idx, res in zip(
                    indices,
                    run_with_oom_retry(
                        encode_image_chunk, list(images), logger=logger
                    ),
                ):
                    results[idx] = res

        output = [res for res in results if res is not None]
        assert len(output) == len(
            inputs
        ), "Mismatched output length and input length"
        return output

    def unload(self) -> None:
        if self._model_loaded:
            del self.model
            del self.tokenizer
            del self.preprocess
            clear_cache()
            self._model_loaded = False

IMPL_CLASS = ClipModel