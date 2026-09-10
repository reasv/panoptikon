"""Unit tests for the two halves of `inferio.impl.clip`'s fp16 completion:
`finish_lp_conversion` (leftover fp32 parameters) and
`promote_plain_layernorms` (norms open_clip built without the fp32 upcast).

No GPU and no model download: the parameter half is a dtype bookkeeping bug,
so a two-parameter module on the CPU reproduces it exactly, and some tests
build open_clip's real modules (random weights) so the upstream claims are
checked against open_clip rather than against a hand-made imitation. They skip
if open_clip is not installed (it is in the `inference` dependency group, not
`test`).

**The norm half cannot be reproduced on the CPU at all.** `F.layer_norm` with
a half input and fp32 weights raises on CUDA and silently succeeds on the CPU,
which is exactly why the first round of these tests passed while the GPU still
failed. So the tests below pin the *structure* the fix establishes — no plain
fp32-weighted `LayerNorm` survives a converted CoCa, `attn_pool.ln_q`/`ln_k`
are `LayerNormFp32`, the weights are the same tensors — and the runtime claim
("the image path runs at batch 1 on CUDA") is a GPU check, not a CPU one.
"""

import pytest

import torch
from torch import nn

from inferio.impl.clip import (
    finish_lp_conversion,
    promote_plain_layernorms,
)


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


class _PlainNormHead(nn.Module):
    """What `AttentionalPooler` is, structurally: a norm the tower's
    `norm_layer` never reached, in front of a converted projection."""

    def __init__(self, width: int = 8):
        super().__init__()
        self.ln_k = nn.LayerNorm(width, eps=1e-3)
        self.proj = nn.Linear(width, width)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.proj(self.ln_k(x))


class TestPromotePlainLayerNorms:
    def test_promotes_a_norm_whose_weights_open_clip_left_in_fp32(self):
        LayerNormFp32 = pytest.importorskip(
            "open_clip.transformer"
        ).LayerNormFp32
        model = _PlainNormHead().eval()
        _convert_like_open_clip(model)
        weight, bias = model.ln_k.weight, model.ln_k.bias

        promoted = promote_plain_layernorms(model, "fp16")

        assert promoted == ["ln_k"]
        assert isinstance(model.ln_k, LayerNormFp32)
        # Same tensors, not copies: the promotion cannot change a value.
        assert model.ln_k.weight is weight and model.ln_k.bias is bias
        assert model.ln_k.eps == 1e-3
        assert model.ln_k.normalized_shape == (8,)
        assert model.ln_k.weight.dtype is torch.float32
        out = model(torch.zeros(1, 8, dtype=torch.float16))
        assert out.dtype is torch.float16 and torch.isfinite(out).all()

    def test_leaves_a_norm_the_conversion_already_matched(self):
        # A timm-backed tower casts its norms to the low precision wholesale
        # (only `LayerNormFp32` instances are restored to fp32), so nothing
        # there mismatches and nothing there may be touched.
        model = _PlainNormHead()
        _convert_like_open_clip(model)
        model.ln_k.weight.data = model.ln_k.weight.data.half()
        model.ln_k.bias.data = model.ln_k.bias.data.half()

        assert promote_plain_layernorms(model, "fp16") == []
        assert type(model.ln_k) is nn.LayerNorm

    @pytest.mark.parametrize("precision", ["fp32", "pure_fp16", "pure_bf16"])
    def test_no_op_unless_open_clip_did_a_partial_conversion(self, precision):
        model = _PlainNormHead()
        assert promote_plain_layernorms(model, precision) == []
        assert type(model.ln_k) is nn.LayerNorm

    def test_is_idempotent(self):
        pytest.importorskip("open_clip")
        model = _PlainNormHead()
        _convert_like_open_clip(model)
        assert promote_plain_layernorms(model, "fp16") == ["ln_k"]
        assert promote_plain_layernorms(model, "fp16") == []


def _build(name: str, precision: str = "fp16"):
    """A real open_clip model with random weights — no download."""
    open_clip = pytest.importorskip("open_clip")
    torch.manual_seed(0)
    return open_clip.create_model(
        name, pretrained=None, precision=precision, device="cpu"
    ).eval()


def _plain_fp32_norms(model) -> list:
    from open_clip.transformer import LayerNormFp32

    return [
        name
        for name, module in model.named_modules()
        if isinstance(module, nn.LayerNorm)
        and not isinstance(module, LayerNormFp32)
        and module.weight is not None
        and module.weight.dtype is torch.float32
    ]


def test_open_clip_builds_cocas_attention_pooler_without_the_fp32_norm():
    """The upstream gap, checked against upstream.

    `VisionTransformer.__init__` constructs its `AttentionalPooler` without
    forwarding `norm_layer`, so `ln_q`/`ln_k` are plain `LayerNorm` even
    though every other norm in the fp16 tower is `LayerNormFp32`. On CUDA that
    plain norm raises `expected scalar type Half but found Float` on the half
    activations flowing into it; on the CPU it silently succeeds, so only the
    structure is assertable here.
    """
    from open_clip.transformer import LayerNormFp32

    model = _build("coca_ViT-B-32")

    assert isinstance(model.visual.ln_pre, LayerNormFp32)  # the tower is fine
    assert _plain_fp32_norms(model) == [
        "visual.attn_pool.ln_q",
        "visual.attn_pool.ln_k",
    ]


def test_a_converted_coca_keeps_no_plain_norm_and_no_fp32_leftover():
    """The structural property the GPU failure reduces to."""
    from open_clip.transformer import LayerNormFp32

    model = _build("coca_ViT-B-32")
    pooler = model.visual.attn_pool
    before = {
        name: param.detach().clone()
        for name, param in pooler.named_parameters()
        if name.startswith(("ln_q.", "ln_k."))
    }
    eps = (pooler.ln_q.eps, pooler.ln_k.eps)

    finish_lp_conversion(model, "fp16")
    promoted = promote_plain_layernorms(model, "fp16")

    assert promoted == ["visual.attn_pool.ln_q", "visual.attn_pool.ln_k"]
    pooler = model.visual.attn_pool
    assert isinstance(pooler.ln_q, LayerNormFp32)
    assert isinstance(pooler.ln_k, LayerNormFp32)
    assert _plain_fp32_norms(model) == []
    # Promotion moves the tensors over untouched: same values, same eps.
    after = dict(pooler.named_parameters())
    assert set(after) >= set(before)
    for name, param in before.items():
        assert torch.equal(after[name], param)
        assert after[name].dtype is torch.float32
    assert (pooler.ln_q.eps, pooler.ln_k.eps) == eps

    image = torch.zeros(1, 3, 224, 224, dtype=torch.float16)
    with torch.no_grad():
        out = model.encode_image(image)
    assert out.dtype is torch.float16 and torch.isfinite(out).all()


def test_a_working_model_is_left_bit_for_bit_identical():
    """Blast radius. The one property a future edit to either rule could
    break silently: the eight ids that already run must not move at all."""
    model = _build("ViT-B-32")
    image = torch.zeros(1, 3, 224, 224, dtype=torch.float16)
    text = torch.zeros(1, 77, dtype=torch.long)
    with torch.no_grad():
        image_before = model.encode_image(image)
        text_before = model.encode_text(text)

    finish_lp_conversion(model, "fp16")
    promoted = promote_plain_layernorms(model, "fp16")

    # A model open_clip converted completely has nothing left to promote.
    assert promoted == []
    with torch.no_grad():
        assert torch.equal(model.encode_image(image), image_before)
        assert torch.equal(model.encode_text(text), text_before)
