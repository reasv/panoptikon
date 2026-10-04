"""Keep grouped-query attention (GQA) off torch's math SDPA kernel.

transformers calls SDPA with `enable_gqa=True` (fewer KV heads than query
heads) whenever there is no attention mask. When no fused kernel of this torch
build accepts a GQA call in the key's dtype on the device, SDPA falls back to
the math kernel, which holds the full score matrix of every head. For those
dtypes transformers is made to expand the KV heads (`repeat_kv`) before SDPA,
so a fused kernel without GQA support (memory-efficient attention, which also
takes fp32) takes the call instead.
"""

from __future__ import annotations

import logging
import sys
import warnings
from typing import Any

logger = logging.getLogger(__name__)

# The transformers module whose `use_gqa_in_sdpa` decides between passing
# `enable_gqa=True` and expanding the KV heads; `sdpa_attention_forward` looks
# the name up in this module's globals on every call.
SDPA_MODULE = "transformers.integrations.sdpa_attention"

# The dtypes tested after a load: those the models run attention in.
TEST_DTYPES = ("float16", "bfloat16", "float32")

# What torch raises when no enabled SDPA kernel takes a call.
NO_KERNEL = "No available kernel"

# Key dtype -> whether a fused kernel took the GQA test call on the model's
# device. A dtype is absent until a test call answers either way.
_fused_gqa: dict[Any, bool] = {}
# transformers' own `use_gqa_in_sdpa`, once the replacement is installed.
_transformers_use_gqa: Any = None


def _use_gqa_in_sdpa(attention_mask: Any, key: Any) -> bool:
    """Replacement for `use_gqa_in_sdpa`: transformers' answer, except False
    (expand the KV heads) for a key dtype no fused kernel takes GQA in."""
    return _transformers_use_gqa(attention_mask, key) and _fused_gqa.get(
        key.dtype, True
    )


def fused_kernel_accepts_gqa(torch: Any, device: Any, dtype: Any) -> bool | None:
    """Whether flash or memory-efficient attention takes a GQA call in `dtype`
    on `device`, found by making one: head_dim 128, causal, no mask, as the
    language models call it. None when the call fails for any other reason
    than no kernel taking it (out of memory, for one): undecided.
    """
    try:
        from torch.nn.attention import SDPBackend, sdpa_kernel

        query = torch.zeros(1, 2, 16, 128, dtype=dtype, device=device)
        key = torch.zeros(1, 1, 16, 128, dtype=dtype, device=device)
        with warnings.catch_warnings():
            # A rejected kernel is reported through warnings before the raise.
            warnings.simplefilter("ignore")
            with sdpa_kernel(
                [SDPBackend.FLASH_ATTENTION, SDPBackend.EFFICIENT_ATTENTION]
            ):
                torch.nn.functional.scaled_dot_product_attention(
                    query, key, key, is_causal=True, enable_gqa=True
                )
        return True
    except Exception as e:
        if NO_KERNEL in str(e):
            logger.debug(
                "GQA attention test call in %s on %s raised: %s", dtype, device, e
            )
            return False
        logger.info(
            "GQA attention test call in %s on %s failed: %s; testing again at "
            "the next load",
            dtype,
            device,
            e,
        )
        return None


def _model_device_index(torch: Any) -> int | None:
    """The lowest CUDA device on which this process's allocator holds memory,
    or None. Reads allocator statistics only, so it creates no context.
    """
    for index in range(torch.cuda.device_count()):
        if torch.cuda.memory_reserved(index) > 0:
            return index
    return None


def expand_kv_heads_without_fused_gqa() -> None:
    """After a model load: if transformers is imported and the load put memory
    on a CUDA/HIP device, make the GQA test call in each dtype not decided yet,
    and install `_use_gqa_in_sdpa` once some dtype has no fused kernel. Never
    creates a context; never raises.
    """
    global _transformers_use_gqa
    try:
        torch = sys.modules.get("torch")
        sdpa = sys.modules.get(SDPA_MODULE)
        if torch is None or sdpa is None:
            return
        if not torch.cuda.is_initialized():
            return
        # Initialised alone does not mean the model is on a GPU: loading a
        # model onto the CPU may still have queried the devices.
        index = _model_device_index(torch)
        if index is None:
            return
        device = torch.device("cuda", index)
        expanded = []
        # Kernel selection reads the current device's properties.
        with torch.cuda.device(index):
            for name in TEST_DTYPES:
                dtype = getattr(torch, name)
                if dtype in _fused_gqa:
                    continue
                accepts = fused_kernel_accepts_gqa(torch, device, dtype)
                if accepts is not None:
                    _fused_gqa[dtype] = accepts
                if accepts is False:
                    expanded.append(name)
        if not expanded:
            return
        if sdpa.use_gqa_in_sdpa is not _use_gqa_in_sdpa:
            _transformers_use_gqa = sdpa.use_gqa_in_sdpa
            sdpa.use_gqa_in_sdpa = _use_gqa_in_sdpa
        # fp32 alone is the common case: flash attention takes no fp32.
        level = logging.DEBUG if expanded == ["float32"] else logging.INFO
        logger.log(
            level,
            "PyTorch %s has no fused grouped-query attention kernel in %s on "
            "%s; such calls expand their key/value heads first (uses less GPU "
            "memory)",
            torch.__version__,
            ", ".join(expanded),
            device,
        )
    except Exception as e:
        logger.warning("GQA attention check failed: %s", e, exc_info=True)
