"""Unit tests for the worker's memory sensing (`inferio_worker.memory`).

These run everywhere: the module only ever uses torch if it is *already* in
`sys.modules`, so a fake torch injected there drives every code path
(including the tiers that cannot be reached on this machine's real
hardware). NVML stays unavailable throughout unless a test injects it, which
is the tier-2/3 world.
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

import pytest

from inferio_worker import memory

MIB = 1024 * 1024


class FakeDtype:
    """Stand-in for the `torch.dtype` type object and its instances."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return self.name


class FakeCuda:
    """Just enough of `torch.cuda` for the memory helpers."""

    def __init__(self, free_mb=8000, total_mb=8192, initialized=True):
        self.free = free_mb * MIB
        self.total = total_mb * MIB
        self.reserved = 0
        self.allocated = 0
        self.peak_reserved = 0
        self.peak_allocated = 0
        self.reset_calls = 0
        self.initialized = initialized
        self.uuid = "1a2b3c4d-0000-0000-0000-000000000000"
        self.name = "Fake GPU 5090"

    def is_available(self):
        return True

    def is_initialized(self):
        return self.initialized

    def mem_get_info(self):
        return (self.free, self.total)

    def memory_reserved(self):
        return self.reserved

    def memory_allocated(self):
        return self.allocated

    def max_memory_reserved(self):
        return self.peak_reserved

    def max_memory_allocated(self):
        return self.peak_allocated

    def get_device_properties(self, index):
        assert index == 0, "a pinned worker only ever has device 0"
        return SimpleNamespace(uuid=self.uuid, name=self.name)

    def reset_peak_memory_stats(self):
        self.reset_calls += 1
        self.peak_reserved = self.reserved
        self.peak_allocated = self.allocated

    # Test helper: pretend a load or a batch allocated `mb`.
    def allocate(self, mb, reserved_mb=None):
        self.allocated += mb * MIB
        self.reserved += (reserved_mb if reserved_mb is not None else mb) * MIB
        self.free -= (reserved_mb if reserved_mb is not None else mb) * MIB
        self.peak_allocated = max(self.peak_allocated, self.allocated)
        self.peak_reserved = max(self.peak_reserved, self.reserved)


def fake_torch_module(cuda: object) -> SimpleNamespace:
    """A torch stand-in carrying the attributes the module reads."""
    return SimpleNamespace(cuda=cuda, dtype=FakeDtype, __version__="2.7.1+cu128")


@contextmanager
def isolated(torch_module=None):
    """Control every input the module reads from the process.

    `sys.modules` is process-global: other tests in the same session import
    real torch and real `inferio.impl.utils` (whose `select_dtype` records
    its last decision), and the module deliberately observes both. Dropping
    them keeps each case hermetic. NVML is forced into its "import already
    tried and unusable" state so no real driver call happens, and the
    one-shot log flags are reset so a test can assert on them.
    """
    with mock.patch.dict(sys.modules, {}, clear=False):
        sys.modules.pop("inferio.impl.utils", None)
        if torch_module is None:
            sys.modules.pop("torch", None)
        else:
            sys.modules["torch"] = torch_module
        with (
            mock.patch.dict(
                memory._nvml_state,
                {"module_tried": True, "module": None, "handle": None},
                clear=False,
            ),
            mock.patch.dict(memory._logged, {}, clear=False),
        ):
            for key in memory._logged:
                memory._logged[key] = False
            yield


@pytest.fixture()
def fake_torch():
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        yield cuda


@pytest.fixture()
def no_torch():
    with isolated():
        yield


def fake_pynvml_for(fake_torch, procs) -> SimpleNamespace:
    """NVML stand-in whose memory info tracks the fake torch device."""
    return SimpleNamespace(
        nvmlDeviceGetComputeRunningProcesses=lambda handle: list(procs),
        nvmlDeviceGetMemoryInfo=lambda handle: SimpleNamespace(
            free=fake_torch.free, total=fake_torch.total
        ),
    )


@contextmanager
def with_nvml(fake_pynvml, handle: object = None):
    """Present NVML as importable, initialized and already resolved."""
    with mock.patch.dict(
        memory._nvml_state,
        {"module_tried": True, "module": fake_pynvml, "handle": handle or object()},
        clear=False,
    ):
        yield


def test_sample_is_none_without_torch_or_nvml(no_torch) -> None:
    assert memory.device_memory_sample() is None


def test_sample_reports_allocator_and_driver_state(fake_torch) -> None:
    fake_torch.allocate(100)
    sample = memory.device_memory_sample()
    assert sample == {
        "free_mb": 7900,
        "total_mb": 8192,
        "free_source": "torch",
        "reserved_mb": 100,
        "allocated_mb": 100,
    }


def test_base_is_unreported_without_torch(no_torch) -> None:
    # A CPU or remote-API worker has no measurable device footprint: the
    # load response must stay a plain ok rather than attributing another
    # process's memory to this model.
    before = memory.begin_load()
    assert memory.finish_load(before, object()) == {}


def test_sensing_never_initializes_cuda() -> None:
    # Expected behavior: torch's reset_peak_memory_stats, mem_get_info and
    # get_device_properties all CREATE a CUDA context when none exists, so a
    # harness that called them would allocate the 300-600 MB context it is
    # supposed to be measuring — in a process that may never touch the GPU
    # at all. Every torch path therefore requires is_initialized() first.
    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        def mem_get_info(self):
            raise AssertionError("mem_get_info would initialize CUDA")

        def reset_peak_memory_stats(self):
            raise AssertionError("reset_peak_memory_stats would initialize CUDA")

        def memory_reserved(self):
            raise AssertionError("allocator stats would initialize CUDA")

        def memory_allocated(self):
            raise AssertionError("allocator stats would initialize CUDA")

        def get_device_properties(self, index):
            raise AssertionError("get_device_properties would initialize CUDA")

    with isolated(fake_torch_module(Tripwire())):
        assert memory.device_memory_sample() is None
        assert memory.device_identity() == (None, None)
        before = memory.begin_load()
        report = memory.finish_load(before, object())
        # The version needs no device and is still reported; nothing else is.
        assert report == {"torch_version": "2.7.1+cu128"}
        payload = memory.finish_batch(memory.begin_batch(), items=2)
        assert payload["measurements"][0]["items"] == 2
        assert payload["measurements"][0]["peak_reserved_mb"] is None


def test_load_that_initializes_cuda_is_still_measured() -> None:
    # Expected behavior: the common case is that `load()` itself creates the
    # CUDA context, so before-values do not exist. Skipping the peak reset
    # then must not skip the measurement: the allocator is re-read after the
    # load, and a missing "before" is a baseline of 0 — exactly right for a
    # context that did not exist yet. This is also why the free reading is
    # taken before any torch call: the new context is inside the window.
    cuda = FakeCuda(initialized=False)
    with isolated(fake_torch_module(cuda)):
        before = memory.begin_load()
        assert before["free_mb"] is None, "no NVML and no context: nothing to read"
        cuda.initialized = True
        cuda.allocate(1024, reserved_mb=1200)
        report = memory.finish_load(before, object())
    assert report["base_mb"] == 1024 + memory.CONTEXT_ESTIMATE_MB
    assert report["base_method"] == "alloc_delta"
    assert report["reserved_at_load_mb"] == 1200
    assert report["gpu_uuid"] == f"GPU-{cuda.uuid}"


def test_no_base_when_the_process_never_allocated(fake_torch) -> None:
    # Expected behavior: torch present and CUDA live, but this load put
    # nothing in the allocator (a CTranslate2/faster-whisper engine, a
    # CPU-fallback impl, a remote API). Its VRAM, if any, belongs in the
    # ledger's external-usage term, so base is ABSENT — never 0, which would
    # read as "measured, and it is free".
    before = memory.begin_load()
    # Another process took 2 GB during our load window; nothing of ours moved.
    fake_torch.free -= 2048 * MIB
    report = memory.finish_load(before, object())
    assert "base_mb" not in report, report
    assert "base_method" not in report, report
    # The rest of the load report is still filled in.
    assert report["reserved_at_load_mb"] == 0
    assert report["torch_version"] == "2.7.1+cu128"


def test_base_uses_the_free_delta_and_records_the_pool(fake_torch) -> None:
    before = memory.begin_load()
    # 1 GB of weights, but the driver lost 1.5 GB — the extra is our CUDA
    # context and workspaces, which is precisely why base is measured in
    # driver currency rather than allocator currency.
    fake_torch.allocate(1024, reserved_mb=1536)
    report = memory.finish_load(before, object())
    assert report["base_mb"] == 1536
    assert report["base_method"] == "free_delta"
    assert report["reserved_at_load_mb"] == 1536
    assert report["memory"]["reserved_mb"] == 1536


def test_load_reports_the_board_identity_and_torch_version(fake_torch) -> None:
    # The ledger keys on the board the worker ACTUALLY got (and on the torch
    # build), neither of which the orchestrator can see: the spawn pin can be
    # an index, or absent, or a UUID for a board CUDA reordered.
    before = memory.begin_load()
    fake_torch.allocate(512)
    report = memory.finish_load(before, object())
    assert report["gpu_uuid"] == f"GPU-{fake_torch.uuid}"
    assert report["gpu_name"] == "Fake GPU 5090"
    assert report["torch_version"] == "2.7.1+cu128"


def test_implausible_free_delta_falls_back_to_allocated_plus_context(
    fake_torch,
) -> None:
    before = memory.begin_load()
    fake_torch.allocate(1024, reserved_mb=1024)
    # Another process grabbed 6 GB during our load window.
    fake_torch.free -= 6144 * MIB
    report = memory.finish_load(before, object())
    assert report["base_method"] == "alloc_delta"
    assert report["base_mb"] == 1024 + memory.CONTEXT_ESTIMATE_MB


def test_pool_overshoot_is_not_treated_as_implausible(fake_torch) -> None:
    # Expected behavior: plausibility is judged against the RESERVED delta,
    # not the allocated one. `from_pretrained` legitimately leaves the
    # caching allocator holding far more than the live weights (transient
    # copies, fragmentation), and the driver's free delta tracks reserved.
    # Judging against allocated would reject this perfectly good reading.
    before = memory.begin_load()
    fake_torch.allocate(1024, reserved_mb=4096)
    report = memory.finish_load(before, object())
    assert report["base_method"] == "free_delta"
    assert report["base_mb"] == 4096


def test_unusable_free_delta_still_charges_the_context(fake_torch) -> None:
    # Expected behavior: the free delta is unusable (the driver reports MORE
    # free memory than before — another process released during our window),
    # so the allocator peak delta is the floor. The CUDA context is real even
    # when the driver reading cannot show it, so the fallback adds the fixed
    # context allowance instead of pretending base equals the weights.
    before = memory.begin_load()
    fake_torch.allocated += 800 * MIB
    fake_torch.reserved += 800 * MIB
    fake_torch.peak_allocated = fake_torch.allocated
    fake_torch.free += 512 * MIB
    report = memory.finish_load(before, object())
    assert report["base_method"] == "alloc_delta"
    assert report["base_mb"] == 800 + memory.CONTEXT_ESTIMATE_MB


def test_base_method_matches_the_reported_value(fake_torch) -> None:
    # Expected behavior: the driver's free delta is used when it is at least
    # the allocator floor; below it, the allocator floor wins and the
    # provenance says so. Claiming "free_delta" for an allocator-derived
    # number would make the stored profile lie about how it was measured (and
    # about which platform tier produced it). Both routes to "alloc_delta"
    # report the SAME formula — floor + context allowance — because one
    # base_method value cannot name two different quantities.
    before = memory.begin_load()
    # 2 GB of weights, but another process released 1 GB inside our window,
    # so the driver's free delta (1 GB) understates what we took. Usable
    # (positive, plausible) yet below the allocator floor.
    fake_torch.allocate(2048, reserved_mb=2048)
    fake_torch.free += 1024 * MIB
    report = memory.finish_load(before, object())
    assert report["base_mb"] == 2048 + memory.CONTEXT_ESTIMATE_MB, report
    assert report["base_method"] == "alloc_delta", report


def test_free_source_is_consistent_across_the_load_window(fake_torch) -> None:
    # Expected behavior: NVML free and torch's mem_get_info disagree by GBs
    # on the same board (measured 3.4 GB apart on the dev box), so a delta
    # between one of each is meaningless. The source of the "before" reading
    # is recorded and required for the "after" one; when it cannot be
    # matched, tier 2 is skipped rather than mixed.
    nvml_answers = [(20000, 24576), (None, None)]
    with mock.patch.object(
        memory,
        "_nvml_memory",
        side_effect=lambda: nvml_answers.pop(0) if nvml_answers else (None, None),
    ):
        before = memory.begin_load()
        assert before["free_source"] == "nvml", "NVML is preferred when present"
        fake_torch.allocate(1024)
        report = memory.finish_load(before, object())
    # torch's own free reading (8000 -> 6976 MiB) is NOT differenced against
    # the NVML one, which would have "measured" a 13 GB base.
    assert report["base_method"] == "alloc_delta", report
    assert report["base_mb"] == 1024 + memory.CONTEXT_ESTIMATE_MB, report


def test_nvml_per_process_wins_and_missing_pid_is_logged(fake_torch, caplog) -> None:
    # Tier 1: NVML's own-PID figure is absolute and pollution-free.
    proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=3000 * MIB)
    fake_pynvml = SimpleNamespace(
        nvmlDeviceGetComputeRunningProcesses=lambda handle: [proc],
        nvmlDeviceGetMemoryInfo=lambda handle: SimpleNamespace(
            free=4000 * MIB, total=8192 * MIB
        ),
    )
    with mock.patch.dict(
        memory._nvml_state,
        {"module_tried": True, "module": fake_pynvml, "handle": object()},
        clear=False,
    ):
        before = memory.begin_load()
        fake_torch.allocate(1024)
        report = memory.finish_load(before, object())
        assert report["base_mb"] == 3000
        assert report["base_method"] == "nvml"

        # Same NVML, but our PID is not in the list (host PIDs seen from
        # inside a PID namespace). The tier silently degrades, so it is
        # logged once.
        proc.pid = proc.pid + 1
        memory._logged["nvml_pid_missing"] = False
        with caplog.at_level("INFO", logger="inferio_worker.memory"):
            before = memory.begin_load()
            fake_torch.allocate(512)
            report = memory.finish_load(before, object())
            # A second degraded load must not repeat the line.
            second = memory.begin_load()
            fake_torch.allocate(256)
            memory.finish_load(second, object())
        assert report["base_method"] != "nvml", report
        messages = [
            record.message
            for record in caplog.records
            if "NVML lists no process" in record.message
        ]
        assert len(messages) == 1, caplog.records


def test_wddm_declines_the_per_process_figure_without_logging(
    fake_torch, caplog
) -> None:
    # Expected behavior: under Windows' WDDM driver model NVML lists our
    # process but reports usedGpuMemory as N/A. That is the ordinary Windows
    # path, NOT the container/PID-namespace degradation, so tier 1 drops to
    # the free-memory delta and the "NVML lists no process" line must stay
    # silent — it would fire on every Windows load and tell operators to go
    # look for a PID-namespace problem they do not have.
    proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=None)
    with with_nvml(fake_pynvml_for(fake_torch, [proc])):
        with caplog.at_level("INFO", logger="inferio_worker.memory"):
            before = memory.begin_load()
            assert before["free_source"] == "nvml"
            fake_torch.allocate(1024, reserved_mb=1536)
            report = memory.finish_load(before, object())
    assert report["base_method"] == "free_delta", report
    assert report["base_mb"] == 1536, report
    assert not [
        record
        for record in caplog.records
        if "NVML lists no process" in record.message
    ], caplog.records


def test_nvml_reading_of_the_whole_board_is_rejected(fake_torch) -> None:
    # Expected behavior: some driver/NVML combinations answer usedGpuMemory
    # with a filled-in sentinel rather than None. Tier 1 is the most
    # authoritative provenance we have, so a figure at least as large as the
    # entire board is garbage by construction and must not be believed.
    proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=fake_torch.total)
    with with_nvml(fake_pynvml_for(fake_torch, [proc])):
        before = memory.begin_load()
        fake_torch.allocate(1024, reserved_mb=1536)
        report = memory.finish_load(before, object())
    assert report["base_method"] == "free_delta", report
    assert report["base_mb"] == 1536, report


def test_nvml_handle_resolution_is_retried_after_cuda_comes_up(fake_torch) -> None:
    # Expected behavior: the FIRST NVML call of a worker's life happens in
    # begin_load, before the impl has initialized CUDA — so on a host whose
    # pin is not in UUID form there is no device identity to resolve the
    # board with yet, and (with more than one board) no unambiguous fallback.
    # Caching that failure would disable NVML for the process forever; the
    # lookup is retried instead and succeeds once load() brings CUDA up.
    boards = {
        "GPU-1a2b3c4d-0000-0000-0000-000000000000": "handle-a",
        "GPU-other": "handle-b",
    }
    lookups: list[str] = []

    def by_uuid(raw: bytes):
        uuid = raw.decode()
        lookups.append(uuid)
        if uuid not in boards:
            raise RuntimeError("Not Found")
        return boards[uuid]

    fake_pynvml = SimpleNamespace(
        nvmlDeviceGetHandleByUUID=by_uuid,
        nvmlDeviceGetCount=lambda: len(boards),
        nvmlDeviceGetHandleByIndex=lambda index: list(boards.values())[index],
        nvmlDeviceGetUUID=lambda handle: b"unrelated",
        nvmlDeviceGetComputeRunningProcesses=lambda handle: [],
        nvmlDeviceGetMemoryInfo=lambda handle: SimpleNamespace(
            free=fake_torch.free, total=fake_torch.total
        ),
    )
    fake_torch.initialized = False
    with mock.patch.dict(
        memory._nvml_state,
        {"module_tried": True, "module": fake_pynvml, "handle": None},
        clear=False,
    ):
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "1"}, clear=False):
            assert memory._nvml() is None, "no CUDA context yet: board unknown"
            assert lookups == [], "nothing to look up before torch has a device"
            fake_torch.initialized = True
            nvml = memory._nvml()
        assert nvml is not None and nvml[1] == "handle-a"
        assert lookups == [f"GPU-{fake_torch.uuid}"]


def test_abbreviated_uuid_pins_are_resolved_by_prefix(fake_torch) -> None:
    # Expected behavior: resolve_pin passes an operator's abbreviated
    # `GPU-1a2b` through verbatim because CUDA accepts prefixes, but
    # nvmlDeviceGetHandleByUUID needs the full string. The prefix scan is the
    # fallback; an ambiguous prefix resolves to nothing, because a reading
    # from the wrong board is worse than no reading.
    handles = {"h0": "GPU-1a2b0000-0000-0000-0000-000000000000", "h1": "GPU-9999"}
    order = list(handles)

    def unknown_uuid(raw: bytes):
        raise RuntimeError("Not Found")

    fake_pynvml = SimpleNamespace(
        nvmlDeviceGetHandleByUUID=unknown_uuid,
        nvmlDeviceGetCount=lambda: len(order),
        nvmlDeviceGetHandleByIndex=lambda index: order[index],
        nvmlDeviceGetUUID=lambda handle: handles[handle].encode(),
    )
    with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "GPU-1a2b"}, clear=False):
        assert memory._nvml_handle(fake_pynvml) == "h0"
    # Case-insensitive, as CUDA is.
    with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "gpu-1A2B"}, clear=False):
        assert memory._nvml_handle(fake_pynvml) == "h0"
    # `GPU-` alone matches both boards: refuse rather than guess. (The torch
    # fallback cannot rescue it either — its UUID is not one of these.)
    with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "GPU-"}, clear=False):
        assert memory._nvml_handle(fake_pynvml) is None


def test_dtype_prefers_the_negotiated_value_over_config_strings(fake_torch) -> None:
    assert memory.resolved_dtype_name(object()) is None
    # 1. The forward-looking convention: an impl stating its outcome.
    assert (
        memory.resolved_dtype_name(SimpleNamespace(resolved_dtype="torch.bfloat16"))
        == "bf16"
    )
    # 3. `dtype`/`_dtype` count only when they hold a real torch.dtype:
    # dots_ocr stores the *requested* precision string there, which
    # select_dtype may have downgraded.
    assert memory.resolved_dtype_name(SimpleNamespace(dtype="torch.float16")) is None
    assert (
        memory.resolved_dtype_name(SimpleNamespace(dtype=FakeDtype("torch.float16")))
        == "fp16"
    )
    assert (
        memory.resolved_dtype_name(SimpleNamespace(_dtype=FakeDtype("torch.float32")))
        == "fp32"
    )
    # An unrecognised value is "unknown", never a guess.
    assert memory.resolved_dtype_name(SimpleNamespace(dtype=FakeDtype("int8"))) is None

    # 2. select_dtype's recorded decision outranks a config string, and is
    # read without importing the inferio package.
    fake_utils = SimpleNamespace(last_selected_dtype=lambda: "torch.bfloat16")
    with mock.patch.dict(
        sys.modules, {"inferio.impl.utils": fake_utils}, clear=False
    ):
        assert memory.resolved_dtype_name(object()) == "bf16"
        assert (
            memory.resolved_dtype_name(SimpleNamespace(dtype="fp32")) == "bf16"
        ), "a requested-precision string must not beat the negotiated dtype"
        assert (
            memory.resolved_dtype_name(SimpleNamespace(resolved_dtype="fp32")) == "fp32"
        ), "an explicit resolved_dtype is still the most authoritative"


def test_batch_measurement_is_per_call(fake_torch) -> None:
    fake_torch.allocate(500)  # weights, before any batch
    state = memory.begin_batch()
    assert fake_torch.reset_calls >= 1, "peaks are reset before the batch"
    fake_torch.allocate(200, reserved_mb=300)
    payload = memory.finish_batch(state, items=8)
    measurement = payload["measurements"][0]
    assert measurement["items"] == 8
    assert measurement["reserved_before_mb"] == 500
    assert measurement["allocated_before_mb"] == 500
    assert measurement["peak_reserved_mb"] == 800
    assert measurement["peak_allocated_mb"] == 700
    assert measurement["duration_ms"] >= 0.0
    assert payload["memory"]["reserved_mb"] == 800

    # A second batch that stays inside the existing pool measures its own
    # transient, not the accumulated peak from the first one.
    fake_torch.allocated -= 200 * MIB
    state = memory.begin_batch()
    fake_torch.allocated += 50 * MIB
    fake_torch.peak_allocated = fake_torch.allocated
    measurement = memory.finish_batch(state, items=1)["measurements"][0]
    assert measurement["allocated_before_mb"] == 500
    assert measurement["peak_allocated_mb"] == 550


def test_helpers_never_raise_on_hostile_torch() -> None:
    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    with isolated(SimpleNamespace(cuda=Exploding())):
        assert memory.device_memory_sample() is None
        assert memory.device_identity() == (None, None)
        before = memory.begin_load()
        assert memory.finish_load(before, object()) == {}
        payload = memory.finish_batch(memory.begin_batch(), items=3)
        assert payload["measurements"][0]["items"] == 3
