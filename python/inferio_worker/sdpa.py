"""Keep grouped-query attention (GQA) off torch's math SDPA kernel.

transformers calls SDPA with `enable_gqa=True` (fewer KV heads than query
heads) whenever there is no attention mask. When no fused kernel of this torch
build accepts GQA on the device, SDPA falls back to the math kernel, which
computes in fp32 and holds the full score matrix of every head. In that case
transformers is made to expand the KV heads (`repeat_kv`) before SDPA, so the
memory-efficient kernel takes the call instead.
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

_checked = False


def _never_gqa(attention_mask: Any, key: Any) -> bool:
    """Replacement for `use_gqa_in_sdpa`: always expand the KV heads."""
    return False


def fused_kernel_accepts_gqa(torch: Any, device: Any) -> bool:
    """Whether flash or memory-efficient attention takes a GQA call on
    `device`, found by making one: fp16, head_dim 128, causal, no mask, as the
    language models call it. A call that raises (no kernel, or anything else)
    answers False.
    """
    try:
        from torch.nn.attention import SDPBackend, sdpa_kernel

        query = torch.zeros(1, 2, 16, 128, dtype=torch.float16, device=device)
        key = torch.zeros(1, 1, 16, 128, dtype=torch.float16, device=device)
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
        logger.debug("GQA attention test call on %s raised: %s", device, e)
        return False


def _model_device_index(torch: Any) -> int | None:
    """The lowest CUDA device on which this process's allocator holds memory,
    or None. Reads allocator statistics only, so it creates no context.
    """
    for index in range(torch.cuda.device_count()):
        if torch.cuda.memory_reserved(index) > 0:
            return index
    return None


def expand_kv_heads_without_fused_gqa() -> None:
    """Once per process, after a model load: if transformers is imported, the
    load put memory on a CUDA/HIP device, and no fused kernel accepts GQA on
    that device, patch `use_gqa_in_sdpa` to return False. Never creates a
    context; never raises.
    """
    global _checked
    if _checked:
        return
    _checked = True
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
        # Kernel selection reads the current device's properties.
        with torch.cuda.device(index):
            if fused_kernel_accepts_gqa(torch, device):
                return
        sdpa.use_gqa_in_sdpa = _never_gqa
        logger.info(
            "PyTorch %s has no fused attention kernel for grouped-query "
            "attention on %s; expanding key/value heads before attention "
            "instead (uses less GPU memory)",
            torch.__version__,
            device,
        )
    except Exception as e:
        logger.warning("GQA attention check failed: %s", e, exc_info=True)
