"""Unit tests for `inferio_worker.sdpa`. No GPU needed: the device state is
faked, and the GQA test call itself runs on the CPU, where no fused kernel
accepts GQA.
"""

from __future__ import annotations

import contextlib
import inspect
import sys
import types

import pytest

from inferio_worker import sdpa

torch = pytest.importorskip("torch")
transformers_sdpa = pytest.importorskip(sdpa.SDPA_MODULE)


def _fake_torch(initialized: bool = True, reserved: tuple = (0, 4096)):
    """A torch whose CUDA side is initialised or not and whose allocator holds
    `reserved[i]` bytes on device i; `cuda.current` tracks `cuda.device`.
    Its dtypes are their names."""
    cuda = types.SimpleNamespace(current=0)

    @contextlib.contextmanager
    def device(index):
        previous, cuda.current = cuda.current, index
        try:
            yield
        finally:
            cuda.current = previous

    cuda.is_initialized = lambda: initialized
    cuda.device_count = lambda: len(reserved)
    cuda.memory_reserved = lambda index: reserved[index]
    cuda.device = device
    return types.SimpleNamespace(
        __version__="2.7.1",
        device=lambda kind, index: f"{kind}:{index}",
        cuda=cuda,
        **{name: name for name in sdpa.TEST_DTYPES},
    )


def _key(dtype: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(dtype=dtype)


@pytest.fixture
def fake_worker(monkeypatch: pytest.MonkeyPatch):
    """Fresh check state, a stand-in transformers module whose own answer is
    "GQA when unmasked", a fake torch with the model's memory on cuda:1, and a
    GQA test call that records the device it was given, the current device and
    the dtype, and answers `answers[dtype]` (True when absent)."""

    def original(attention_mask, key):
        return attention_mask is None

    module = types.SimpleNamespace(use_gqa_in_sdpa=original)
    fake = _fake_torch()
    state = types.SimpleNamespace(
        module=module, original=original, calls=[], answers={}, torch=fake
    )

    def test_call(torch_module, device, dtype):
        state.calls.append((device, torch_module.cuda.current, dtype))
        return state.answers.get(dtype, True)

    monkeypatch.setattr(sdpa, "_fused_gqa", {})
    monkeypatch.setattr(sdpa, "_transformers_use_gqa", None)
    monkeypatch.setattr(sdpa, "fused_kernel_accepts_gqa", test_call)
    monkeypatch.setitem(sys.modules, sdpa.SDPA_MODULE, module)
    monkeypatch.setitem(sys.modules, "torch", fake)
    return state


def test_only_a_dtype_without_a_fused_gqa_kernel_expands_the_kv_heads(
    fake_worker,
) -> None:
    """fp32 has no fused GQA kernel: its keys are expanded; fp16 and bf16 keep
    transformers' own answer, masked calls included."""
    fake_worker.answers = {"float32": False}
    sdpa.expand_kv_heads_without_fused_gqa()
    use_gqa = fake_worker.module.use_gqa_in_sdpa
    assert use_gqa is sdpa._use_gqa_in_sdpa
    assert use_gqa(None, _key("float32")) is False
    assert use_gqa(None, _key("float16")) is True
    assert use_gqa(None, _key("bfloat16")) is True
    assert use_gqa("mask", _key("float16")) is False


def test_a_fused_gqa_kernel_in_every_dtype_leaves_transformers_untouched(
    fake_worker,
) -> None:
    sdpa.expand_kv_heads_without_fused_gqa()
    assert [dtype for _, _, dtype in fake_worker.calls] == list(sdpa.TEST_DTYPES)
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_the_call_runs_on_the_device_holding_the_model(fake_worker) -> None:
    """Memory only on cuda:1: the calls get cuda:1 and run with it current."""
    sdpa.expand_kv_heads_without_fused_gqa()
    assert {call[:2] for call in fake_worker.calls} == {("cuda:1", 1)}
    assert fake_worker.torch.cuda.current == 0


def test_no_cuda_memory_means_no_call(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CUDA initialised by a device query, model on the CPU: no call, which
    would create a context."""
    monkeypatch.setitem(sys.modules, "torch", _fake_torch(reserved=(0, 0)))
    fake_worker.answers = {"float16": False}
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_uninitialised_cuda_means_no_call(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CPU and MPS workers, and a load that never touched CUDA."""
    monkeypatch.setitem(sys.modules, "torch", _fake_torch(initialized=False))
    fake_worker.answers = {"float16": False}
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_without_transformers_there_is_nothing_to_check(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delitem(sys.modules, sdpa.SDPA_MODULE)
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []


def test_only_an_undecided_dtype_is_tested_again_at_the_next_load(
    fake_worker,
) -> None:
    """A test call that failed otherwise than for want of a kernel (out of
    memory, say) decides and patches nothing; the next load tests that dtype
    again, and only it."""
    fake_worker.answers = {"bfloat16": None}
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original

    fake_worker.calls.clear()
    fake_worker.answers = {"bfloat16": False}
    sdpa.expand_kv_heads_without_fused_gqa()
    assert [dtype for _, _, dtype in fake_worker.calls] == ["bfloat16"]
    use_gqa = fake_worker.module.use_gqa_in_sdpa
    assert use_gqa(None, _key("bfloat16")) is False
    assert use_gqa(None, _key("float16")) is True

    fake_worker.calls.clear()
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []
    assert fake_worker.module.use_gqa_in_sdpa is use_gqa


def test_the_check_never_raises(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexpected error, from the test call or from a torch without the
    expected attributes, leaves transformers untouched."""

    def broken(torch_module, device, dtype):
        raise KeyError("unexpected")

    monkeypatch.setattr(sdpa, "fused_kernel_accepts_gqa", broken)
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original

    monkeypatch.setitem(sys.modules, "torch", types.SimpleNamespace())
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_the_test_call_answers_by_its_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CPU SDPA has no fused kernel that accepts GQA: False. A call that runs:
    True, having been made in the dtype asked for, with GQA and causal. Out of
    memory: None (undecided)."""
    cpu = torch.device("cpu")
    assert sdpa.fused_kernel_accepts_gqa(torch, cpu, torch.float16) is False

    seen: list = []

    def runs(query, key, value, **kwargs):
        seen.append((query.dtype, key.dtype, kwargs))

    monkeypatch.setattr(torch.nn.functional, "scaled_dot_product_attention", runs)
    assert sdpa.fused_kernel_accepts_gqa(torch, cpu, torch.float32) is True
    assert seen == [
        (torch.float32, torch.float32, {"is_causal": True, "enable_gqa": True})
    ]

    def out_of_memory(*args, **kwargs):
        raise torch.OutOfMemoryError("CUDA out of memory")

    monkeypatch.setattr(
        torch.nn.functional, "scaled_dot_product_attention", out_of_memory
    )
    assert sdpa.fused_kernel_accepts_gqa(torch, cpu, torch.float16) is None


def test_transformers_still_decides_gqa_through_the_patched_name() -> None:
    """transformers is pinned exactly; this fails when the function the patch
    replaces is renamed, changes signature, or stops being looked up as a
    module global by `sdpa_attention_forward`."""
    params = list(inspect.signature(transformers_sdpa.use_gqa_in_sdpa).parameters)
    assert params == ["attention_mask", "key"]
    code = transformers_sdpa.sdpa_attention_forward.__code__
    assert "use_gqa_in_sdpa" in code.co_names


def test_the_patch_expands_the_kv_heads_of_a_dtype_without_a_kernel_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the patch and fp32 without a fused GQA kernel, an unmasked fp32
    GQA call reaches SDPA with the KV heads repeated to the query head count
    and without `enable_gqa`; an fp16 call keeps `enable_gqa`."""
    seen: list = []

    def capture(query, key, value, **kwargs):
        seen.append((key.shape[1], kwargs.get("enable_gqa")))
        return torch.zeros_like(query)

    monkeypatch.setattr(torch.nn.functional, "scaled_dot_product_attention", capture)
    monkeypatch.setattr(
        sdpa, "_transformers_use_gqa", transformers_sdpa.use_gqa_in_sdpa
    )
    monkeypatch.setattr(sdpa, "_fused_gqa", {torch.float32: False})
    monkeypatch.setattr(transformers_sdpa, "use_gqa_in_sdpa", sdpa._use_gqa_in_sdpa)
    module = torch.nn.Module()
    module.num_key_value_groups = 4
    for dtype in (torch.float32, torch.float16):
        query = torch.zeros(1, 8, 16, 128, dtype=dtype)
        key = torch.zeros(1, 2, 16, 128, dtype=dtype)
        transformers_sdpa.sdpa_attention_forward(module, query, key, key, None)
    assert seen == [(8, None), (2, True)]
