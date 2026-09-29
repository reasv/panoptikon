"""Unit tests for `inferio_worker.sdpa`. No GPU needed: the device state is
faked, and the GQA test call itself runs on the CPU, where no fused kernel
accepts GQA.
"""

from __future__ import annotations

import contextlib
import inspect
import logging
import sys
import types

import pytest

from inferio_worker import sdpa

torch = pytest.importorskip("torch")
transformers_sdpa = pytest.importorskip(sdpa.SDPA_MODULE)


def _fake_torch(initialized: bool = True, reserved: tuple = (0, 4096)):
    """A torch whose CUDA side is initialised or not and whose allocator holds
    `reserved[i]` bytes on device i; `cuda.current` tracks `cuda.device`."""
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
    )


@pytest.fixture
def fake_worker(monkeypatch: pytest.MonkeyPatch):
    """Fresh check state, a stand-in transformers module, a fake torch with
    the model's memory on cuda:1, and a GQA test call that records the device
    it was given and the current device, and answers `accepts`."""
    original = object()
    module = types.SimpleNamespace(use_gqa_in_sdpa=original)
    fake = _fake_torch()
    state = types.SimpleNamespace(
        module=module, original=original, calls=[], accepts=True, torch=fake
    )

    def test_call(torch_module, device):
        state.calls.append((device, torch_module.cuda.current))
        return state.accepts

    monkeypatch.setattr(sdpa, "_checked", False)
    monkeypatch.setattr(sdpa, "fused_kernel_accepts_gqa", test_call)
    monkeypatch.setitem(sys.modules, sdpa.SDPA_MODULE, module)
    monkeypatch.setitem(sys.modules, "torch", fake)
    return state


def test_no_fused_gqa_kernel_patches_transformers(
    fake_worker, caplog: pytest.LogCaptureFixture
) -> None:
    fake_worker.accepts = False
    with caplog.at_level(logging.INFO, logger=sdpa.logger.name):
        sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is sdpa._never_gqa
    assert "expanding key/value heads" in caplog.text


def test_a_fused_gqa_kernel_leaves_transformers_untouched(fake_worker) -> None:
    fake_worker.accepts = True
    sdpa.expand_kv_heads_without_fused_gqa()
    assert len(fake_worker.calls) == 1
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_the_call_runs_on_the_device_holding_the_model(fake_worker) -> None:
    """Memory only on cuda:1: the call gets cuda:1 and runs with it current."""
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == [("cuda:1", 1)]
    assert fake_worker.torch.cuda.current == 0


def test_no_cuda_memory_means_no_call(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CUDA initialised by a device query, model on the CPU: no call, which
    would create a context."""
    monkeypatch.setitem(sys.modules, "torch", _fake_torch(reserved=(0, 0)))
    fake_worker.accepts = False
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_uninitialised_cuda_means_no_call(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CPU and MPS workers, and a load that never touched CUDA."""
    monkeypatch.setitem(sys.modules, "torch", _fake_torch(initialized=False))
    fake_worker.accepts = False
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_without_transformers_there_is_nothing_to_check(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delitem(sys.modules, sdpa.SDPA_MODULE)
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.calls == []


def test_the_check_runs_once_per_process(fake_worker) -> None:
    sdpa.expand_kv_heads_without_fused_gqa()
    sdpa.expand_kv_heads_without_fused_gqa()
    assert len(fake_worker.calls) == 1


def test_the_check_never_raises(
    fake_worker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexpected error, from the test call or from a torch without the
    expected attributes, leaves transformers untouched."""

    def broken(torch_module, device):
        raise KeyError("unexpected")

    monkeypatch.setattr(sdpa, "fused_kernel_accepts_gqa", broken)
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original

    monkeypatch.setattr(sdpa, "_checked", False)
    monkeypatch.setitem(sys.modules, "torch", types.SimpleNamespace())
    sdpa.expand_kv_heads_without_fused_gqa()
    assert fake_worker.module.use_gqa_in_sdpa is fake_worker.original


def test_a_test_call_that_raises_answers_false() -> None:
    """CPU SDPA has no fused kernel that accepts GQA, so the real call raises
    inside and answers False."""
    assert sdpa.fused_kernel_accepts_gqa(torch, torch.device("cpu")) is False


def test_a_test_call_that_runs_answers_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        torch.nn.functional,
        "scaled_dot_product_attention",
        lambda *args, **kwargs: None,
    )
    assert sdpa.fused_kernel_accepts_gqa(torch, torch.device("cpu")) is True


def test_transformers_still_decides_gqa_through_the_patched_name() -> None:
    """transformers is pinned exactly; this fails when the function the patch
    replaces is renamed, changes signature, or stops being looked up as a
    module global by `sdpa_attention_forward`."""
    params = list(inspect.signature(transformers_sdpa.use_gqa_in_sdpa).parameters)
    assert params == ["attention_mask", "key"]
    code = transformers_sdpa.sdpa_attention_forward.__code__
    assert "use_gqa_in_sdpa" in code.co_names


def test_the_patch_expands_the_kv_heads(monkeypatch: pytest.MonkeyPatch) -> None:
    """With the patch, an unmasked GQA call reaches SDPA with the KV heads
    repeated to the query head count and without `enable_gqa`."""
    seen: dict = {}

    def capture(query, key, value, **kwargs):
        seen.update(key_heads=key.shape[1], kwargs=kwargs)
        return torch.zeros_like(query)

    monkeypatch.setattr(torch.nn.functional, "scaled_dot_product_attention", capture)
    monkeypatch.setattr(transformers_sdpa, "use_gqa_in_sdpa", sdpa._never_gqa)
    module = torch.nn.Module()
    module.num_key_value_groups = 4
    query = torch.zeros(1, 8, 16, 128)
    key = torch.zeros(1, 2, 16, 128)
    transformers_sdpa.sdpa_attention_forward(module, query, key, key, None)
    assert seen["key_heads"] == 8
    assert "enable_gqa" not in seen["kwargs"]
