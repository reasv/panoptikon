"""Unit tests for `inferio.impl.clip.finish_lp_conversion`.

No GPU and no model download: the defect is a dtype bookkeeping bug, so a
two-parameter module on the CPU reproduces it exactly. One test additionally
builds open_clip's real `AttentionalPooler` — the module CoCa fails in — and
converts it with open_clip's real `convert_weights_to_lp`, so the claim
"open_clip leaves that parameter in fp32" is checked against open_clip rather
than against a hand-made imitation. It skips if open_clip is not installed
(it is in the `inference` dependency group, not `test`).
"""

import pytest

import torch
from torch import nn

from inferio.impl.clip import finish_lp_conversion


def _convert_like_open_clip(model: nn.Module, dtype=torch.float16) -> None:
    """What `open_clip.model.convert_weights_to_lp` does to these fixtures:
    module weights get cast, bare `nn.Parameter` attributes do not."""
    for module in model.modules():
        if isinstance(module, (nn.Conv1d, nn.Conv2d, nn.Linear)):
            module.weight.data = module.weight.data.to(dtype)
            if module.bias is not None:
                module.bias.data = module.bias.data.to(dtype)


class _PooledHead(nn.Module):
    """CoCa's shape in miniature: a learned query parameter that is fed
    straight into a converted projection (`AttentionalPooler.query` ->
    `AttentionalPooler.attn`), with a norm in between."""

    def __init__(self, width: int = 8):
        super().__init__()
        self.query = nn.Parameter(torch.randn(2, width))
        self.ln_q = nn.LayerNorm(width)
        self.proj = nn.Linear(width, width)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        q = self.ln_q(self.query)
        return self.proj(q) + x.mean()


class TestFinishLpConversion:
    def test_repairs_a_head_open_clip_left_in_fp32(self):
        torch.manual_seed(0)
        model = _PooledHead().eval()
        _convert_like_open_clip(model)
        x = torch.zeros(1, 8, dtype=torch.float16)

        with pytest.raises(RuntimeError, match="dtype|Half"):
            model(x)

        cast = finish_lp_conversion(model, "fp16")

        assert "query" in cast
        out = model(x)
        assert out.dtype is torch.float16
        assert torch.isfinite(out).all()

    def test_normalization_parameters_stay_fp32(self):
        model = _PooledHead()
        _convert_like_open_clip(model)

        cast = finish_lp_conversion(model, "fp16")

        assert "ln_q.weight" not in cast and "ln_q.bias" not in cast
        assert model.ln_q.weight.dtype is torch.float32
        assert model.ln_q.bias.dtype is torch.float32
        assert model.query.dtype is torch.float16

    def test_matches_the_requested_low_precision(self):
        model = _PooledHead()
        finish_lp_conversion(model, "bf16")
        assert model.query.dtype is torch.bfloat16

    @pytest.mark.parametrize("precision", ["fp32", "pure_fp16"])
    def test_no_op_unless_open_clip_did_a_partial_conversion(self, precision):
        # fp32 has nothing to finish, and the `pure_*` modes already cast
        # every parameter including the norms — reverting those would be a
        # regression, not a fix.
        model = _PooledHead()
        assert finish_lp_conversion(model, precision) == []
        assert model.query.dtype is torch.float32

    def test_is_idempotent(self):
        model = _PooledHead()
        _convert_like_open_clip(model)
        assert finish_lp_conversion(model, "fp16")
        assert finish_lp_conversion(model, "fp16") == []


def test_open_clip_leaves_the_attentional_pooler_query_in_fp32():
    """The upstream half of the root cause, checked against upstream."""
    open_clip = pytest.importorskip("open_clip")
    from open_clip.model import convert_weights_to_lp
    from open_clip.transformer import AttentionalPooler

    torch.manual_seed(0)
    pooler = AttentionalPooler(
        d_model=8, context_dim=8, n_head=2, n_queries=2
    ).eval()
    convert_weights_to_lp(pooler, dtype=torch.float16)

    assert pooler.attn.in_proj_weight.dtype is torch.float16
    assert pooler.query.dtype is torch.float32  # the defect

    tokens = torch.zeros(1, 4, 8, dtype=torch.float16)
    with pytest.raises(RuntimeError):
        pooler(tokens)

    assert "query" in finish_lp_conversion(pooler, "fp16")
    assert pooler(tokens).dtype is torch.float16
