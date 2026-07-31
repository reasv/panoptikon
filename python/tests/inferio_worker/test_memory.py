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
from pathlib import Path
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
        self.empty_cache_calls = 0
        self.initialized = initialized
        self.uuid = "1a2b3c4d-0000-0000-0000-000000000000"
        self.name = "Fake GPU 5090"
        # torch exposes the device's PCI fields from 2.8 (hipDeviceProp_t on
        # ROCm, _CudaDeviceProperties on CUDA). `None` here stands for the
        # older builds that do not — which includes the 2.7.1 the cpu/cu128
        # extras currently pin, so on the shipped CUDA build that is the
        # live case and no `gpu_bdf` is emitted at all.
        self.pci = (0, 0x03, 0x00)

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
        props = SimpleNamespace(
            uuid=self.uuid, name=self.name, total_memory=self.total
        )
        if self.pci is not None:
            domain, bus, device = self.pci
            props.pci_domain_id = domain
            props.pci_bus_id = bus
            props.pci_device_id = device
        return props

    def reset_peak_memory_stats(self):
        self.reset_calls += 1
        self.peak_reserved = self.reserved
        self.peak_allocated = self.allocated

    def empty_cache(self):
        """Return the pool blocks no live tensor is using, as torch does."""
        self.empty_cache_calls += 1
        self.free += self.reserved - self.allocated
        self.reserved = self.allocated
        self.peak_reserved = max(self.peak_reserved, self.reserved)

    # Test helper: pretend a load or a batch allocated `mb`.
    def allocate(self, mb, reserved_mb=None):
        self.allocated += mb * MIB
        self.reserved += (reserved_mb if reserved_mb is not None else mb) * MIB
        self.free -= (reserved_mb if reserved_mb is not None else mb) * MIB
        self.peak_allocated = max(self.peak_allocated, self.allocated)
        self.peak_reserved = max(self.peak_reserved, self.reserved)


def fake_torch_module(cuda: object, hip: str | None = None) -> SimpleNamespace:
    """A torch stand-in carrying the attributes the module reads.

    `hip` makes it a ROCm build: `torch.version.hip` is the one positive
    signal for that, and the hipified `torch.cuda.*` namespace is otherwise
    indistinguishable from the real thing (which is the point of hipifying).
    """
    version = SimpleNamespace(hip=hip, cuda=None if hip else "12.8")
    return SimpleNamespace(
        cuda=cuda,
        dtype=FakeDtype,
        version=version,
        __version__="2.11.0+rocm7.2" if hip else "2.7.1+cu128",
    )


@contextmanager
def isolated(torch_module=None):
    """Control every input the module reads from the process.

    `sys.modules` is process-global: other tests in the same session import
    real torch and real `inferio.impl.utils` (whose `select_dtype` records
    its last decision), and the module deliberately observes both. Dropping
    them keeps each case hermetic. NVML is forced into its "import already
    tried and unusable" state so no real driver call happens, the memoized
    board address is cleared (it is resolved once per *process*, so leaking it
    would point one test's amdgpu tiers at another's board), and the one-shot
    log flags are reset so a test can assert on them. `HIP_VISIBLE_DEVICES`
    is dropped for the same reason: it is the module's pre-torch-import
    signal that this worker is on a HIP device (`_hip_pinned`), so a value
    inherited from the developer's shell would silently switch NVML off in
    every CUDA-path test.
    """
    with (
        mock.patch.dict(sys.modules, {}, clear=False),
        mock.patch.dict(os.environ, {}, clear=False),
    ):
        sys.modules.pop("inferio.impl.utils", None)
        os.environ.pop("HIP_VISIBLE_DEVICES", None)
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
            mock.patch.dict(memory._bdf_state, {"bdf": None}, clear=False),
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


def test_the_free_source_pin_holds_even_when_the_mix_looks_plausible(
    fake_torch, monkeypatch, tmp_path
) -> None:
    # The discriminating case for the pin. The test above catches a mixed
    # pair because the cross-source skew there is enormous (a 13 GB "base"),
    # which the implausibility ceiling would have rejected anyway — so it
    # cannot tell the pin apart from the ceiling. Here the mix lands *below*
    # the ceiling and would be accepted as a perfectly ordinary free delta:
    # NVML says 9000 MiB free before, NVML then goes away, and torch says
    # 6976 MiB after. Against a 100 MiB reserved delta the ceiling is
    # 100 + 500 + 2048 = 2648 MiB and the mixed delta is 2024 — plausible,
    # wrong, and entirely a measurement of the two sources' disagreement.
    #
    # Pinned, the "after" reading simply does not exist and the allocator
    # delta answers. Unpinned, the mixed reading wins.
    monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "no-pci"))
    nvml_answers = [(9000, 24_576), (None, None)]
    with mock.patch.object(
        memory,
        "_nvml_memory",
        side_effect=lambda: nvml_answers.pop(0) if nvml_answers else (None, None),
    ):
        before = memory.begin_load()
        assert before["free_source"] == "nvml"
        fake_torch.allocate(100, reserved_mb=100)
        fake_torch.free = 6976 * MIB
        report = memory.finish_load(before, object())
        # The reading the unpinned re-read would have found: live, readable,
        # and 2024 MiB away from the "before" one.
        assert memory._free_total_mb() == (6976, 8192, "torch")
    assert report["base_method"] == "alloc_delta", report
    assert report["base_mb"] == 100 + memory.CONTEXT_ESTIMATE_MB, report
    assert report["base_mb"] != 9000 - 6976, "that is the cross-source skew"


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


def test_empty_cache_releases_the_pool_only_when_cuda_is_live(fake_torch) -> None:
    """The only way our process gives VRAM back to the board short of exiting:
    freeing tensors leaves the caching allocator holding the blocks."""
    fake_torch.allocate(400, reserved_mb=1000)
    fake_torch.allocated = 0  # the batch's tensors are gone; the pool is not
    assert memory.empty_cache() is True
    assert fake_torch.empty_cache_calls == 1
    assert fake_torch.reserved == 0, "the pool went back to the driver"

    # An uninitialized CUDA device is the case this gate exists for: calling
    # `empty_cache` there would CREATE the 300-600 MB context this module
    # exists to avoid creating, on a host that was never going to use the GPU.
    fake_torch.initialized = False
    assert memory.empty_cache() is False
    assert fake_torch.empty_cache_calls == 1, "not even attempted"


def test_empty_cache_is_false_without_torch(no_torch) -> None:
    assert memory.empty_cache() is False


def test_empty_cache_never_raises() -> None:
    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    with isolated(SimpleNamespace(cuda=Exploding())):
        assert memory.empty_cache() is False


# ---------------------------------------------------------------------------
# Board identity: PCI address, total memory, HIP UUID suppression
# (docs/rocm-batch-calibration-parity.md, D3)
# ---------------------------------------------------------------------------


def test_bdf_is_formatted_from_torchs_pci_fields(fake_torch) -> None:
    # Expected behavior: the PCI address is the one identity vocabulary the
    # kernel, the amdgpu driver and the HIP runtime all speak, so it is the
    # ROCm ledger join. Lower-case hex, zero-padded, and the function digit
    # forced to .0 — the amdgpu GPU function is always 0 (the HDMI/DP audio
    # controller is .1 of the same *device*), which is also how the
    # orchestrator's KFD probe renders it, so the two sides stay joinable.
    assert memory.device_bdf() == "0000:03:00.0"
    fake_torch.pci = (1, 0xC1, 0x1F)
    assert memory.device_bdf() == "0001:c1:1f.0"
    # Out-of-range values are not an address: a changed encoding must read as
    # unknown rather than as a fabricated one.
    fake_torch.pci = (0, 0x100, 0)
    assert memory.device_bdf() is None
    # An older torch simply does not carry the fields. On CUDA that is the
    # end of it — the UUID is the identity there anyway.
    fake_torch.pci = None
    assert memory.device_bdf() is None


def test_total_memory_is_reported_in_mib(fake_torch) -> None:
    # The independent half of the registration cross-check: this comes from
    # torch/HIP, never from the sysfs file the orchestrator's inventory total
    # was read from, so agreement between them is evidence.
    assert memory.gpu_total_mb() == 8192
    fake_torch.total = 24_560 * MIB
    assert memory.gpu_total_mb() == 24_560


def test_hip_suppresses_the_uuid_but_keeps_the_address() -> None:
    # Expected behavior: torch >= 2.5 renders a UUID on ROCm too, but it is a
    # THIRD vocabulary — derived from the ASIC serial, matching neither KFD's
    # `GPU-<16hex>` nor amd-smi's 8-4-4-4-12 form — and on consumer boards
    # without a fused serial it is identical for every card of a model. A
    # value that can neither match nor be trusted to differ is worse than
    # none, so the worker reports no `gpu_uuid` at all on HIP and the ledger
    # keys the replica by its PCI address instead.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda, hip="7.2.0")):
        uuid, name = memory.device_identity()
        assert uuid is None
        assert name == "Fake GPU 5090", "the name is informational and kept"
        before = memory.begin_load()
        cuda.allocate(1024)
        report = memory.finish_load(before, object())
    assert "gpu_uuid" not in report, report
    assert report["gpu_bdf"] == "0000:03:00.0"
    assert report["gpu_total_mb"] == 8192
    assert report["torch_version"] == "2.11.0+rocm7.2"

    # The same board on a CUDA build reports the UUID, and the address rides
    # along additively (registration keys on the UUID first there).
    with isolated(fake_torch_module(FakeCuda())):
        uuid, _ = memory.device_identity()
        assert uuid == "GPU-1a2b3c4d-0000-0000-0000-000000000000"


def test_load_report_omits_the_identity_fields_it_cannot_measure(
    fake_torch,
) -> None:
    # Every field here is additive and "absent means unknown": a worker with
    # no PCI fields and no total must reply exactly as it did before D3.
    fake_torch.pci = None
    fake_torch.total = None
    before = memory.begin_load()
    fake_torch.allocate(512)
    report = memory.finish_load(before, object())
    assert "gpu_bdf" not in report, report
    assert "gpu_total_mb" not in report, report
    assert report["gpu_uuid"] == f"GPU-{fake_torch.uuid}"


def test_a_raising_props_getter_degrades_one_field_not_the_whole_report() -> None:
    # Expected behavior: the fields of `get_device_properties` are pybind
    # getters, not plain attributes — arbitrary C++ that can raise anything,
    # not only the AttributeError an older build produces. One unreadable
    # field must read as unknown; letting the exception out would take down
    # finish_load and lose the measured base and the negotiated dtype with
    # it, over an identity field nothing needed.
    class Hostile:
        name = "Fake GPU 5090"
        total_memory = 8192 * MIB
        pci_domain_id = 0
        pci_bus_id = 0x03
        pci_device_id = 0x00

        @property
        def uuid(self):
            raise RuntimeError("the pybind getter blew up")

    class HostileProps(FakeCuda):
        def get_device_properties(self, index):
            assert index == 0
            return Hostile()

    cuda = HostileProps()
    with isolated(fake_torch_module(cuda)):
        assert memory.device_identity() == (None, "Fake GPU 5090")
        assert memory.device_bdf() == "0000:03:00.0", "the other fields still read"
        assert memory.gpu_total_mb() == 8192
        before = memory.begin_load()
        cuda.allocate(1024)
        report = memory.finish_load(before, object())
    assert "gpu_uuid" not in report, report
    assert report["gpu_name"] == "Fake GPU 5090"
    assert report["base_mb"] is not None, "the measurement survived intact"


# ---------------------------------------------------------------------------
# DRM fdinfo parsing (the older-ROCm-torch identity fallback, and the parser
# D4's per-process memory tier reuses)
# ---------------------------------------------------------------------------


def fdinfo(pdev: str, client: int, vram: str | None, key: str = "drm-resident-vram") -> str:
    lines = [
        "pos:\t0",
        "flags:\t02100002",
        "drm-driver:\tamdgpu",
        f"drm-pdev:\t{pdev}",
        f"drm-client-id:\t{client}",
    ]
    if vram is not None:
        lines.append(f"{key}:\t{vram}")
    return "\n".join(lines) + "\n"


def test_fdinfo_parses_both_memory_spellings_and_the_documented_units() -> None:
    # `drm-memory-<region>` is the kernel docs' deprecated alias for
    # `drm-resident-<region>` and is "only printed by amdgpu" — exactly the
    # driver this exists for — so a parser that knew only the modern
    # spelling would read every AMD client as zero.
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, "1024 KiB")) == (
        "0000:03:00.0",
        7,
        1024 * 1024,
    )
    assert memory.parse_drm_fdinfo(
        fdinfo("0000:03:00.0", 7, "2 MiB", key="drm-memory-vram")
    ) == ("0000:03:00.0", 7, 2 * 1024 * 1024)
    # The unit suffix is optional; bare means bytes.
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, "4096"))[2] == 4096
    # And the grammar is `<uint> [KiB|MiB]` and nothing else: a spelling the
    # format does not define is a line we do not understand, not a number.
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, "8 GiB")) is None
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, "4096 B")) is None
    # Both spellings present: the modern one wins (they are aliases).
    both = fdinfo("0000:03:00.0", 7, "8 MiB") + "drm-memory-vram:\t1 KiB\n"
    assert memory.parse_drm_fdinfo(both)[2] == 8 * 1024 * 1024
    # Upper-case addresses compare against ours, which are lower-case.
    assert memory.parse_drm_fdinfo(fdinfo("0000:0C:00.0", 1, "1 KiB"))[0] == (
        "0000:0c:00.0"
    )


def test_fdinfo_records_that_are_not_readings() -> None:
    # A non-DRM fd (a socket, a file) carries none of the keys.
    assert memory.parse_drm_fdinfo("pos:\t0\nflags:\t02\nmnt_id:\t24\n") is None
    # The address and the client id are both required: the address is what
    # the reading is about, the client id is what makes duplicated fds
    # countable once.
    assert memory.parse_drm_fdinfo("drm-pdev:\t0000:03:00.0\n") is None
    assert memory.parse_drm_fdinfo("drm-client-id:\t7\n") is None
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, None)) == (
        "0000:03:00.0",
        7,
        0,
    ), "a client with no VRAM line holds no VRAM — a record, not a failure"
    # Garbage never raises and never becomes a number.
    for junk in ("", "not a fdinfo at all", "drm-pdev\t0000:03:00.0\n"):
        assert memory.parse_drm_fdinfo(junk) is None
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", "seven", "1 KiB")) is None
    # Expected behavior: absent and UNREADABLE are different answers. A
    # memory line that is present but does not parse invalidates the whole
    # record — reading it as 0 would invent an observation, and the
    # observation it invents is the one that hands dominance (and with it
    # this worker's board identity) to a different card.
    for unreadable in ("lots", "-4 KiB", "4 furlongs", "1 2 KiB", "KiB"):
        assert (
            memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, unreadable)) is None
        ), unreadable
    # And an invalidated record contributes nothing to the map, not a zero.
    assert memory.fdinfo_vram_by_pdev(
        [fdinfo("0000:03:00.0", 7, "lots"), fdinfo("0000:0c:00.0", 8, "1 KiB")]
    ) == {"0000:0c:00.0": 1024}


def test_fdinfo_sums_per_board_and_dedupes_by_client() -> None:
    # Expected behavior: several fds of one DRM client (dup(), fork) are ONE
    # client, and summing them would double the process's VRAM. Different
    # boards accumulate separately — the map is what D4's per-process tier
    # filters by the identity address.
    texts = [
        fdinfo("0000:03:00.0", 1, "1024 KiB"),
        fdinfo("0000:03:00.0", 1, "1024 KiB"),  # the same client, dup()ed
        fdinfo("0000:03:00.0", 2, "512 KiB"),
        fdinfo("0000:0c:00.0", 3, "8 MiB", key="drm-memory-vram"),
        "not a drm fd at all\n",
    ]
    assert memory.fdinfo_vram_by_pdev(texts) == {
        "0000:03:00.0": (1024 + 512) * 1024,
        "0000:0c:00.0": 8 * 1024 * 1024,
    }
    assert memory.fdinfo_vram_by_pdev([]) == {}


def test_dominant_vram_pdev_needs_a_strict_winner(tmp_path) -> None:
    # The identity fallback for an older ROCm torch with no PCI fields. HIP
    # filters ABOVE ROCr, so a pinned worker still holds render nodes for
    # every ROCR-visible board: the board it is actually *using* is the one
    # its VRAM is on. A tie identifies nothing, and guessing here does not
    # degrade a reading — it prices one model's memory against another
    # board's ledger.
    def write(entries):
        root = tmp_path / str(len(list(tmp_path.iterdir())))
        root.mkdir()
        for index, text in enumerate(entries):
            (root / str(index)).write_text(text, encoding="utf-8")
        return str(root)

    winner = write(
        [
            fdinfo("0000:03:00.0", 1, "4 KiB"),
            fdinfo("0000:0c:00.0", 2, "8192 MiB"),
        ]
    )
    assert memory.dominant_vram_pdev(winner) == "0000:0c:00.0"

    tied = write([fdinfo("0000:03:00.0", 1, "8 MiB"), fdinfo("0000:0c:00.0", 2, "8 MiB")])
    assert memory.dominant_vram_pdev(tied) is None

    idle = write([fdinfo("0000:03:00.0", 1, None), fdinfo("0000:0c:00.0", 2, "0")])
    assert memory.dominant_vram_pdev(idle) is None, "nothing allocated yet"

    # The same emptiness with only ONE board open, which the tie rule cannot
    # see: a lone record is trivially the maximum. Holding nothing is not
    # evidence of which board this worker is using — a process that has
    # opened a render node and not allocated on it is exactly the pre-load
    # state — so the strict-positive guard is what answers here.
    lone_idle = write([fdinfo("0000:03:00.0", 1, "0")])
    assert memory.dominant_vram_pdev(lone_idle) is None, "open, but holding nothing"
    lone_keyless = write([fdinfo("0000:03:00.0", 1, None)])
    assert memory.dominant_vram_pdev(lone_keyless) is None

    # A single client that has allocated is unambiguous.
    alone = write([fdinfo("0000:03:00.0", 1, "512 MiB")])
    assert memory.dominant_vram_pdev(alone) == "0000:03:00.0"

    # No /proc at all (every platform but Linux) is simply unknown.
    assert memory.dominant_vram_pdev(str(tmp_path / "missing")) is None


def test_the_fdinfo_fallback_is_hip_only(fake_torch, monkeypatch) -> None:
    # Expected behavior: the fdinfo scan exists for ROCm torch too old to
    # expose the PCI fields. On a CUDA host those fds are nvidia character
    # devices, not DRM clients, and the identity is the UUID anyway — so the
    # scan must not even run.
    scans: list[str] = []

    def scan(root=memory.FDINFO_ROOT):
        scans.append(root)
        return "0000:0c:00.0"

    monkeypatch.setattr(memory, "dominant_vram_pdev", scan)
    fake_torch.pci = None
    assert memory.device_bdf() is None
    assert scans == [], "no fdinfo scan on a CUDA build"

    cuda = FakeCuda()
    cuda.pci = None
    with isolated(fake_torch_module(cuda, hip="7.2.0")):
        assert memory.device_bdf() == "0000:0c:00.0"
    assert len(scans) == 1

    # And when torch DOES carry the fields they win: they are device-0
    # scoped, i.e. exactly the board the pin selected, which no scan of this
    # process's open files could establish.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda, hip="7.2.0")):
        assert memory.device_bdf() == "0000:03:00.0"
    assert len(scans) == 1, "the fallback was not consulted"


def test_an_fdinfo_derived_address_must_look_like_a_pci_address(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: unlike the torch-derived BDF, which this module
    # formats itself out of three integers, the fdinfo one is a string lifted
    # verbatim from a `drm-pdev` line — the parser only requires the key to be
    # present and non-empty. Anything else there would become this worker's
    # identity, go out on the wire as `gpu_bdf` to be joined against the
    # orchestrator's inventory, and be pasted into a
    # `/sys/bus/pci/devices/<bdf>` path. It has to look like an address first.
    cuda = FakeCuda()
    cuda.pci = None  # the older-ROCm-torch chain, i.e. the fdinfo fallback
    hostile = [
        "drm-pdev:\t../../../etc\ndrm-client-id:\t1\ndrm-resident-vram:\t512 MiB\n",
        "drm-pdev:\t0000:03:00\ndrm-client-id:\t2\ndrm-resident-vram:\t8 MiB\n",
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=hostile, cuda=cuda):
        assert memory.dominant_vram_pdev() == "../../../etc", "the parse is neutral"
        assert memory.device_bdf() is None, "the identity is not"
        assert memory._identity_bdf() is None
        assert memory.fdinfo_own_vram_mb() is None
        assert memory.amdgpu_free_total_mb() == (None, None)

    # The well-formed spelling still resolves, so this is a shape check and
    # not an accidental ban on the fallback.
    good = [fdinfo("0000:0c:00.0", 1, "512 MiB")]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=good, cuda=cuda):
        assert memory.device_bdf() == "0000:0c:00.0"


def test_identity_helpers_never_raise_and_never_initialize_cuda() -> None:
    # The forbidden calls are *recorded* as well as raised: every torch call
    # in this module sits inside a `try/except` (its never-raise rule), so an
    # AssertionError alone would be swallowed by the code under test and the
    # tripwire would report success. The list is what actually fails the test.
    calls: list[str] = []

    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        def get_device_properties(self, index):
            calls.append("get_device_properties")
            raise AssertionError("get_device_properties would initialize CUDA")

    with isolated(fake_torch_module(Tripwire(), hip="7.2.0")):
        assert memory.device_bdf() is None
        assert memory.gpu_total_mb() is None
        assert memory.device_identity() == (None, None)
    assert calls == [], calls

    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    with isolated(SimpleNamespace(cuda=Exploding())):
        assert memory.device_bdf() is None
        assert memory.gpu_total_mb() is None


# ---------------------------------------------------------------------------
# amdgpu memory tiers: device-wide free/total from sysfs and this process's own
# footprint from DRM fdinfo (docs/rocm-batch-calibration-parity.md, D4)
# ---------------------------------------------------------------------------


def write_board(root: str, bdf: str, total=None, used=None) -> str:
    """One board's amdgpu VRAM counters under a fake `/sys/bus/pci/devices`.

    The directory name goes through the module's own `_pci_device_dir`, which
    swaps the BDF's colons for dashes on Windows — a colon cannot appear in a
    Windows path component, so a fixture for `0000:03:00.0` is otherwise
    unwritable on the dev box (the orchestrator's `rocm.rs` fixtures do the
    same thing for the same reason).
    """
    device = Path(memory._pci_device_dir(root, bdf))
    device.mkdir(parents=True, exist_ok=True)
    if total is not None:
        (device / "mem_info_vram_total").write_text(f"{total}\n", encoding="utf-8")
    if used is not None:
        (device / "mem_info_vram_used").write_text(f"{used}\n", encoding="utf-8")
    return root


def _fresh(tmp_path, prefix: str) -> Path:
    """A directory no earlier call in this test has written to.

    Reusing one would leak the previous fixture's files into the next case —
    both trees are read by *listing* them, so a stale fd file or a stale board
    directory is indistinguishable from a real one.
    """
    root = tmp_path / f"{prefix}-{len(list(tmp_path.iterdir()))}"
    root.mkdir()
    return root


def pci_root(tmp_path, boards: dict) -> str:
    """A fake PCI device tree: `{bdf: (total_bytes, used_bytes)}`."""
    root = _fresh(tmp_path, "pci")
    for bdf, (total, used) in boards.items():
        write_board(str(root), bdf, total, used)
    return str(root)


def fdinfo_root(tmp_path, texts) -> str:
    """A fake `/proc/self/fdinfo` holding one file per open fd."""
    root = _fresh(tmp_path, "fdinfo")
    for index, text in enumerate(texts):
        (root / str(index)).write_text(text, encoding="utf-8")
    return str(root)


def empty_dir(tmp_path, name: str) -> str:
    """A root that exists and answers nothing — how a test switches a tier off
    without depending on the machine it runs on."""
    root = tmp_path / name
    root.mkdir(exist_ok=True)
    return str(root)


@contextmanager
def rocm_host(tmp_path, monkeypatch, pci=None, fdinfo_texts=None, cuda=None):
    """A ROCm worker whose two sysfs roots point at fixture trees.

    Both roots are always redirected, never left at their defaults: the tiers
    read `/sys` and `/proc`, and what this machine has there is not this
    suite's business.
    """
    cuda = cuda if cuda is not None else FakeCuda()
    with isolated(fake_torch_module(cuda, hip="7.2.0")):
        monkeypatch.setattr(
            memory,
            "PCI_DEVICES_ROOT",
            pci if pci is not None else empty_dir(tmp_path, "no-pci"),
        )
        monkeypatch.setattr(
            memory,
            "FDINFO_ROOT",
            fdinfo_root(tmp_path, fdinfo_texts)
            if fdinfo_texts is not None
            else empty_dir(tmp_path, "no-fdinfo"),
        )
        yield cuda


def test_amdgpu_sysfs_free_is_total_minus_used(tmp_path) -> None:
    # Expected behavior: the driver publishes a total and a used figure, not a
    # free one, and this is the SAME pair of files the orchestrator's refresh
    # reads — which is what makes the free-source consistency rule hold on
    # ROCm by construction rather than by two drivers agreeing.
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        root = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
        assert memory.amdgpu_free_total_mb(root) == (23_552, 24_576)

        # Bytes, floored to whole MiB like every other reading here: the odd
        # 700 KB of a partial MiB is not memory anything can be granted out of.
        write_board(root, "0000:03:00.0", total=8 * MIB + 700_000, used=0)
        assert memory.amdgpu_free_total_mb(root) == (8, 8)

        # The two counters are read a moment apart and the driver updates them
        # independently, so `used > total` is possible; a negative free
        # reading is not. It saturates at 0 — a full board, which is true.
        write_board(root, "0000:03:00.0", total=8 * MIB, used=9 * MIB)
        assert memory.amdgpu_free_total_mb(root) == (0, 8)

        # Both files are required: a total without a used figure is not a free
        # reading, and half of one must never be reported as the whole.
        partial = tmp_path / "partial"
        partial.mkdir()
        write_board(str(partial), "0000:03:00.0", total=8 * MIB)
        assert memory.amdgpu_free_total_mb(str(partial)) == (None, None)

        # A driver whose format changed reads as unknown, never as a guess.
        write_board(root, "0000:03:00.0", total=8 * MIB, used="lots")
        assert memory.amdgpu_free_total_mb(root) == (None, None)

        # This worker's board is not in the tree at all (a container with a
        # subset of `/sys`, a fabricated SR-IOV address).
        assert memory.amdgpu_free_total_mb(str(tmp_path / "missing")) == (None, None)

    # And with no identity there is nothing to read *about*: the tier is about
    # one board, so an unidentified worker gets no reading rather than the
    # first board it can find.
    with isolated():
        assert memory.amdgpu_free_total_mb(
            pci_root(tmp_path, {"0000:03:00.0": (8 * MIB, 0)})
        ) == (None, None)


def test_the_sysfs_tier_outranks_torch_on_a_rocm_host(tmp_path, monkeypatch) -> None:
    # Expected behavior: `mem_get_info` on HIP was historically process-local
    # (ROCm/hip#348) and can raise outright in containers, so amdgpu's
    # whole-board counters are preferred and torch is the last resort. The
    # label is `"amdgpu-sysfs"`, byte-identical to the Rust MemoryQuery's, and
    # the ledger treats exactly that string as authoritative.
    board = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    with rocm_host(tmp_path, monkeypatch, pci=pci_root(tmp_path, board)):
        assert memory.free_total_mb() == (23_552, 24_576, "amdgpu-sysfs")
        sample = memory.device_memory_sample()
    assert sample["free_source"] == "amdgpu-sysfs"
    assert (sample["free_mb"], sample["total_mb"]) == (23_552, 24_576)


def test_the_tier_chain_falls_through_by_availability_not_by_platform(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: one chain on every host — NVML, then amdgpu sysfs,
    # then torch — because each tier's own availability is already the
    # platform test. `nvmlInit` fails once and permanently on a ROCm host, and
    # an NVIDIA board's PCI directory carries no `mem_info_vram_*` files.
    board = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", pci_root(tmp_path, board))
        with with_nvml(fake_pynvml_for(cuda, [])):
            assert memory.free_total_mb() == (
                8000,
                8192,
                "nvml",
            ), "NVML answers first and the sysfs files are never consulted"

    # No NVML, and this board has no amdgpu counters (the CUDA case, and also
    # a ROCm host whose `/sys` is not visible): torch is what is left.
    with rocm_host(tmp_path, monkeypatch):
        assert memory.free_total_mb() == (8000, 8192, "torch")


def test_a_resolvable_board_is_not_an_amdgpu_board(tmp_path, monkeypatch) -> None:
    # Expected behavior: torch >= 2.8 carries the PCI fields on CUDA too, so
    # the board address resolves on an NVIDIA host and the tier is *reached* —
    # and it is still dead, because the PCI directory that exists there (every
    # PCI device has one) carries no `mem_info_vram_*`. That absence is the
    # whole platform test: the tier needs no `torch.version.hip` branch, which
    # is the second thing that would have to be kept true.
    cuda = FakeCuda()
    root = _fresh(tmp_path, "cuda-pci")
    write_board(str(root), "0000:03:00.0")  # the directory, none of the files
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", str(root))
        monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-fdinfo"))
        assert memory._identity_bdf() == "0000:03:00.0", "the address resolves"
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory.free_total_mb() == (8000, 8192, "torch")


def test_the_free_source_pins_the_load_window_on_rocm(tmp_path, monkeypatch) -> None:
    # Expected behavior: a base measured as a free-memory delta is only
    # meaningful between two readings of the SAME source (the sources disagree
    # by gigabytes). The "before" reading records `amdgpu-sysfs` and the
    # "after" one is required to come from there too.
    board = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    root = pci_root(tmp_path, board)
    with rocm_host(tmp_path, monkeypatch, pci=root) as cuda:
        before = memory.begin_load()
        assert before["free_source"] == "amdgpu-sysfs"
        # 1.5 GB left the board device-wide while our allocator grew by 1 GB:
        # the extra is the HIP context, which is exactly why base is measured
        # in driver currency.
        write_board(root, "0000:03:00.0", total=24_576 * MIB, used=(1024 + 1536) * MIB)
        cuda.allocate(1024, reserved_mb=1024)
        report = memory.finish_load(before, object())
    assert report["base_method"] == "free_delta", report
    assert report["base_mb"] == 1536, report

    # And when the pinned source cannot answer the second time (a driver
    # reload took the directory away, a container remounted `/sys`), the tier
    # is skipped rather than differenced against torch's own reading — which
    # would have "measured" a 15 GB base here.
    root = pci_root(tmp_path, board)
    with rocm_host(tmp_path, monkeypatch, pci=root) as cuda:
        before = memory.begin_load()
        assert before["free_source"] == "amdgpu-sysfs"
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "gone"))
        cuda.allocate(1024, reserved_mb=1024)
        report = memory.finish_load(before, object())
    assert report["base_method"] == "alloc_delta", report
    assert report["base_mb"] == 1024 + memory.CONTEXT_ESTIMATE_MB, report


def test_a_pinned_free_source_never_slides_to_another_tier(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: the pin exists because the sources disagree by
    # gigabytes, so an unhonourable pin must yield nothing rather than the
    # next tier's answer — in EITHER direction. On the fixture below the two
    # whole-board tiers and torch are ~15 GB apart, which is exactly the base
    # a mixed pair of readings would "measure".
    board = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    with rocm_host(tmp_path, monkeypatch, pci=pci_root(tmp_path, board)):
        # Downwards: the amdgpu tier is readable and outranks torch, but a
        # window that began on torch ends on torch.
        assert memory._free_total_mb("torch") == (8000, 8192, "torch")
        assert memory._free_total_mb("amdgpu-sysfs") == (
            23_552,
            24_576,
            "amdgpu-sysfs",
        )
        # A label no tier answers to is not a fallback instruction. This is
        # the orchestrator's `"nvidia-smi"` source, which only ever labels the
        # orchestrator's own refresh and can never be a worker's before-reading
        # — but if one ever arrived, no tier here may claim it.
        assert memory._free_total_mb("nvidia-smi") == (None, None, None)

    # Upwards, on a CUDA build — the only place NVML can answer at all now
    # that `_nvml` refuses a HIP worker outright (a hybrid AMD+NVIDIA host
    # would otherwise initialize NVML happily and hand back the wrong board).
    # The amdgpu files are readable here too, which is what makes the pin
    # meaningful rather than the only tier that could have answered.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", pci_root(tmp_path, board))
        monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-up"))
        with with_nvml(fake_pynvml_for(cuda, [])):
            # NVML answers and is the unpinned preference...
            assert memory._free_total_mb() == (8000, 8192, "nvml")
            # ...and a pin to the tier below it is still honoured there.
            assert memory._free_total_mb("amdgpu-sysfs") == (
                23_552,
                24_576,
                "amdgpu-sysfs",
            )

    # And when the pinned tier goes away, the tier sitting ready above it is
    # precisely the reading that must not be substituted for it.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "gone-up"))
        monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-up2"))
        with with_nvml(fake_pynvml_for(cuda, [])):
            assert memory._free_total_mb("amdgpu-sysfs") == (None, None, None)


def test_fdinfo_is_the_rocm_per_process_base_tier(tmp_path, monkeypatch) -> None:
    # Tier 1's ROCm twin: an absolute whole-process footprint, which is what
    # `base_mb` is defined as, read from the kernel about OUR process — no
    # root, no amdsmi, and no PID-namespace caveat (NVML's tier 1 has all
    # three). Only clients on the board this worker was pinned to count: HIP
    # filters above ROCr, so the process holds render nodes for boards it is
    # not using.
    texts = [
        fdinfo("0000:03:00.0", 1, "1024 MiB"),
        fdinfo("0000:03:00.0", 1, "1024 MiB"),  # the same client, dup()ed
        fdinfo("0000:03:00.0", 2, "512 MiB", key="drm-memory-vram"),
        fdinfo("0000:0c:00.0", 3, "8192 MiB"),  # a board we merely have open
        "not a drm fd at all\n",
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
        assert memory.fdinfo_own_vram_mb() == 1536
        before = memory.begin_load()
        cuda.allocate(1024, reserved_mb=1200)
        report = memory.finish_load(before, object())
    assert report["base_method"] == "fdinfo", report
    assert report["base_mb"] == 1536, report

    # A board with no clients of ours, and a board holding nothing, are both
    # "no reading" rather than a zero footprint — the never-invent-a-footprint
    # rule, and the reason such a worker falls to the coarser tiers.
    with rocm_host(
        tmp_path, monkeypatch, fdinfo_texts=[fdinfo("0000:0c:00.0", 3, "8192 MiB")]
    ):
        assert memory.fdinfo_own_vram_mb() is None
    with rocm_host(
        tmp_path, monkeypatch, fdinfo_texts=[fdinfo("0000:03:00.0", 1, None)]
    ):
        assert memory.fdinfo_own_vram_mb() is None


def test_the_fdinfo_tier_works_off_the_dominant_client_identity(
    tmp_path, monkeypatch
) -> None:
    # The older-ROCm-torch chain end to end: `get_device_properties` carries
    # no PCI fields, so the identity is the dominant DRM client — and the
    # per-process tier then filters the very same tree by it. Two things this
    # is the regression test for. The identity scan's root is resolved per
    # call, not bound at import, or it would read the real `/proc/self/fdinfo`
    # while the tier read the fixture. And the address on the wire is the
    # *memoized* one, so the ledger joins on the board the tier measured:
    # dominance moves as a process allocates, and re-resolving at emission
    # time would attribute a load to one board while pricing it on another.
    cuda = FakeCuda()
    cuda.pci = None
    texts = [
        fdinfo("0000:0c:00.0", 1, "1536 MiB"),
        fdinfo("0000:03:00.0", 2, "4 MiB"),
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts, cuda=cuda):
        assert memory._identity_bdf() == "0000:0c:00.0"
        before = memory.begin_load()
        cuda.allocate(1024, reserved_mb=1200)
        report = memory.finish_load(before, object())
        assert (report["base_mb"], report["base_method"]) == (1536, "fdinfo"), report
        assert report["gpu_bdf"] == "0000:0c:00.0"

        # Another process's board becomes the one we hold the most on. The
        # scan does follow it; the identity does not.
        Path(memory.FDINFO_ROOT, "dominance-moved").write_text(
            fdinfo("0000:03:00.0", 3, "8000 MiB"), encoding="utf-8"
        )
        assert memory.dominant_vram_pdev() == "0000:03:00.0", "the scan moves"
        second = memory.begin_load()
        cuda.allocate(64, reserved_mb=64)
        again = memory.finish_load(second, object())
    assert again["gpu_bdf"] == "0000:0c:00.0", "the wire field is the memoized identity"


def test_the_fdinfo_base_tier_is_hip_only(tmp_path, monkeypatch) -> None:
    # Expected behavior: unlike the sysfs free/total tier — whose files exist
    # under no other driver's PCI directory, so absence gates it — recent
    # nvidia-drm publishes DRM fdinfo memory stats too, and they are a
    # DIFFERENT quantity under the same key: GEM/DRM allocations, not the CUDA
    # context and caching allocator a base must account for. The plausibility
    # floor cannot catch that (a small model's reserved delta sits below the
    # tolerance, so any reading passes), and the result would be a base of a
    # few MiB for a process holding a 600 MB context.
    texts = [fdinfo("0000:03:00.0", 1, "8 MiB")]
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "FDINFO_ROOT", fdinfo_root(tmp_path, texts))
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "no-pci"))
        assert memory.fdinfo_own_vram_mb() == 8, "the reader itself is neutral"
        before = memory.begin_load()
        cuda.allocate(200, reserved_mb=200)
        report = memory.finish_load(before, object())
    assert report["base_method"] == "free_delta", report
    assert report["base_mb"] == 200, report


def test_nvml_is_refused_outright_on_a_rocm_worker(tmp_path, monkeypatch) -> None:
    # Expected behavior: NVML is not merely *unavailable* on a ROCm worker,
    # it is refused. `pynvml` is an unconditional base dependency and
    # `nvmlInit` succeeds on any host with an NVIDIA driver loaded — which a
    # hybrid AMD+NVIDIA box has. There, the D3 UUID suppression removes the
    # one thing that would have disambiguated the handle lookup, so the
    # single-GPU last-resort arm could return the NVIDIA board's handle and
    # a single load report would describe two pieces of silicon: identity and
    # base from the AMD board, free/total from the NVIDIA one. Nothing
    # downstream can detect that, so the gate is explicit.
    board = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    texts = [fdinfo("0000:03:00.0", 1, "1536 MiB")]
    with rocm_host(
        tmp_path, monkeypatch, pci=pci_root(tmp_path, board), fdinfo_texts=texts
    ) as cuda:
        proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=3000 * MIB)
        # A *working* NVML, presented exactly as a CUDA host's would be.
        with with_nvml(fake_pynvml_for(cuda, [proc])):
            assert memory._nvml() is None, "torch.version.hip is set"
            assert memory._nvml_memory() == (None, None)
            assert memory._nvml_own_process_mb() is None
            # ...so the free/total chain lands on amdgpu sysfs, not on the
            # NVIDIA board's 8192 MiB that this NVML would have reported.
            assert memory.free_total_mb() == (23_552, 24_576, "amdgpu-sysfs")
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1200)
            report = memory.finish_load(before, object())
    assert report["base_method"] == "fdinfo", report
    assert report["base_mb"] == 1536, report

    # The other half of the gate, and the one that matters for the *first*
    # reading of a worker's life: `begin_load` runs before any impl has
    # imported torch, so `torch.version.hip` cannot answer yet. Our own
    # spawner writes `HIP_VISIBLE_DEVICES` on every pinned ROCm worker and on
    # no other kind, so a non-empty value is proof of the backend with
    # nothing imported.
    with isolated():
        with with_nvml(fake_pynvml_for(FakeCuda(), [])):
            assert memory._nvml() is not None, "no signal either way yet"
            with mock.patch.dict(
                os.environ, {"HIP_VISIBLE_DEVICES": "1"}, clear=False
            ):
                assert memory._nvml() is None, "pinned to a HIP device"
            # Whitespace/comma-only is "not configured", as everywhere else.
            with mock.patch.dict(
                os.environ, {"HIP_VISIBLE_DEVICES": " , "}, clear=False
            ):
                assert memory._nvml() is not None


def test_an_under_reporting_fdinfo_loses_to_the_deltas(
    tmp_path, monkeypatch, caplog
) -> None:
    # Expected behavior: fdinfo's KFD/compute figures are VM-walk-based and
    # comparatively recent, and an older kernel can report a fraction of what
    # we hold. An under-measured base is phantom headroom — the ledger hands
    # out memory that is already spent — so a reading materially below our own
    # allocator's growth is rejected and the coarser tiers take over. The
    # mirror of the free-delta implausibility guard, pointed the other way.
    texts = [fdinfo("0000:03:00.0", 1, "1000 MiB")]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
        with caplog.at_level("DEBUG", logger="inferio_worker.memory"):
            before = memory.begin_load()
            cuda.allocate(4096, reserved_mb=4096)
            report = memory.finish_load(before, object())
            # A second load that lands in the SAME branch must not repeat the
            # line: on such a kernel the tier degrades on every load of the
            # worker's life, and one message is the whole point of the flag.
            # (A second load that *passed* the floor would prove nothing —
            # it would take no branch that could log.)
            second = memory.begin_load()
            cuda.allocate(4096, reserved_mb=4096)
            second_report = memory.finish_load(second, object())
        assert memory._logged["fdinfo_under_reported"] is True
    assert report["base_method"] == "free_delta", report
    assert report["base_mb"] == 4096, report
    assert second_report["base_method"] != "fdinfo", second_report
    assert len(
        [record for record in caplog.records if "under-report" in record.message]
    ) == 1, caplog.records


def test_the_fdinfo_floor_allows_the_innocent_shortfalls(tmp_path, monkeypatch) -> None:
    # Expected behavior: fdinfo ABOVE our allocator delta is the normal case
    # (the HIP context and every non-torch allocation ride on top of it), so
    # only a shortfall is suspicious — and only by more than the tolerance,
    # which exists for MiB truncation on both sides and for pages the driver
    # evicted since we committed them (`drm-resident-vram` counts resident
    # pages). The tolerance is well under the context estimate, so a reading
    # that missed a whole HIP context can never pass as jitter.
    delta, slack = 4096, memory.FDINFO_UNDERREPORT_SLACK_MB

    def base_with(vram_mb: int) -> tuple:
        texts = [fdinfo("0000:03:00.0", 1, f"{vram_mb} MiB")]
        with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
            before = memory.begin_load()
            cuda.allocate(delta, reserved_mb=delta)
            report = memory.finish_load(before, object())
        return (report["base_mb"], report["base_method"])

    assert base_with(delta - slack) == (delta - slack, "fdinfo"), "exactly at the floor"
    assert base_with(delta - slack - 1)[1] == "free_delta", "one MiB below it"
    assert base_with(delta + 500) == (delta + 500, "fdinfo"), "the ordinary case"
    assert slack < memory.CONTEXT_ESTIMATE_MB, "a missed context is never jitter"


def test_the_fdinfo_floor_compares_against_the_absolute_pool(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: fdinfo reports ABSOLUTE whole-process VRAM, so the
    # comparand is the absolute allocator pool, not the load window's delta.
    # The two coincide only on a process's FIRST load — and the ledger
    # explicitly anticipates repeat loads into one worker (a model reloaded
    # after a trim, a replica taking a second model), where a windowed
    # comparand would wave an under-report straight through for no better
    # reason than that the second load happened to be small.
    def two_loads(vram_mb: int) -> tuple:
        texts = [fdinfo("0000:03:00.0", 1, f"{vram_mb} MiB")]
        with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
            first = memory.begin_load()
            cuda.allocate(3000, reserved_mb=3000)
            memory.finish_load(first, object())
            second = memory.begin_load()
            cuda.allocate(100, reserved_mb=100)
            report = memory.finish_load(second, object())
        return (report["base_mb"], report["base_method"])

    # 900 MiB is an under-report against the 3100 MiB pool the process is
    # holding by then, however small the second load was. Against that load's
    # own 100 MiB delta it would have passed as a plausible whole-process
    # footprint — and the ledger would have been handed 2.2 GB of phantom
    # headroom on a worker that had already spent it.
    assert two_loads(900)[1] != "fdinfo"
    # And a reading that does account for the whole process still wins the
    # tier, on the second load exactly as on the first.
    assert two_loads(3500) == (3500, "fdinfo")


def test_an_fdinfo_reading_at_the_board_capacity_is_rejected(
    tmp_path, monkeypatch
) -> None:
    # The mirror of NVML's sentinel guard (`_nvml_own_process_mb` rejects a
    # filled-in `-1`): a PER-PROCESS figure at or above the whole DEVICE is
    # not a footprint, it is a parse or a kernel accounting artefact. An
    # absolute tier that accepted it would charge the ledger the entire board
    # under the most authoritative provenance ROCm has — and the floor above
    # cannot catch it, since an over-report is the direction the floor treats
    # as normal.
    def base_with(vram_mb: int) -> str:
        texts = [fdinfo("0000:03:00.0", 1, f"{vram_mb} MiB")]
        with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1024)
            report = memory.finish_load(before, object())
        return report["base_method"]

    total = 8192  # `FakeCuda`'s board, i.e. what `gpu_total_mb` reports
    assert base_with(total) != "fdinfo", "exactly the capacity"
    assert base_with(total + 4096) != "fdinfo", "and beyond it"
    assert base_with(total - 1) == "fdinfo", "one MiB under it is a real reading"


def test_the_amdgpu_tiers_never_initialize_cuda(tmp_path, monkeypatch) -> None:
    # Expected behavior: both tiers need this worker's board address, which
    # comes from `get_device_properties` — the call that CREATES the context
    # this module exists to avoid creating. No context yet means no identity,
    # which means no reading, however complete the fixtures are.
    #
    # Each forbidden call is recorded as well as raised, and the recording is
    # what the assertion reads: the module wraps every torch call in a
    # `try/except` by rule, so a raise alone is swallowed and a tripwire that
    # only raised would pass while the context was being created.
    calls: list[str] = []

    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        def get_device_properties(self, index):
            calls.append("get_device_properties")
            raise AssertionError("get_device_properties would initialize CUDA")

        def mem_get_info(self):
            calls.append("mem_get_info")
            raise AssertionError("mem_get_info would initialize CUDA")

    pci = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
    texts = [fdinfo("0000:03:00.0", 1, "1536 MiB")]
    with rocm_host(tmp_path, monkeypatch, pci=pci, fdinfo_texts=texts, cuda=Tripwire()):
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory.fdinfo_own_vram_mb() is None
        assert memory.free_total_mb() == (None, None, None)
        assert memory.device_memory_sample() is None
    assert calls == [], calls


def test_the_board_address_is_re_resolved_until_it_is_known(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: the FIRST reading of a worker's life is taken in
    # `begin_load`, before the impl has touched torch, so the board is not
    # identifiable yet — exactly the NVML handle's situation. Caching that
    # `None` would silence both amdgpu tiers for the process's whole life over
    # a question that answers itself moments later.
    pci = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
    cuda = FakeCuda(initialized=False)
    with rocm_host(tmp_path, monkeypatch, pci=pci, cuda=cuda):
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory._bdf_state["bdf"] is None, "a failure is not remembered"
        cuda.initialized = True
        assert memory.amdgpu_free_total_mb() == (23_552, 24_576)
        assert memory._bdf_state["bdf"] == "0000:03:00.0", "a success is"


def test_the_amdgpu_tiers_never_raise(tmp_path, monkeypatch) -> None:
    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    with isolated(SimpleNamespace(cuda=Exploding())):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", str(tmp_path / "nowhere"))
        monkeypatch.setattr(memory, "FDINFO_ROOT", str(tmp_path / "nowhere"))
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory.fdinfo_own_vram_mb() is None
        assert memory.free_total_mb() == (None, None, None)

    # A board "directory" that is really a file: the module reports unknown
    # rather than letting an OSError out of `finish_load`, which would lose the
    # whole load report over a memory reading nothing depended on.
    hostile = tmp_path / "hostile"
    hostile.mkdir()
    Path(memory._pci_device_dir(str(hostile), "0000:03:00.0")).write_text(
        "not a directory", encoding="utf-8"
    )
    with rocm_host(tmp_path, monkeypatch, pci=str(hostile)):
        assert memory.amdgpu_free_total_mb() == (None, None)


def test_the_bdf_reaches_sysfs_verbatim_off_windows(monkeypatch) -> None:
    # Expected behavior: the colon→dash swap is a Windows FIXTURE affordance
    # only (a colon in a path component opens an NTFS alternate data stream,
    # so a tree named `0000:03:00.0` is unwritable on the dev box). The
    # production path is `/sys/bus/pci/devices/<bdf>` and the address must
    # reach it byte for byte — the dashed spelling names a directory the
    # amdgpu driver never creates, so a swap that leaked to Linux would take
    # the free/total tier off the whole platform it exists for. Every test in
    # this file runs on the dev box's `os.name == "nt"`, which is precisely
    # why the branch that ships needs asserting explicitly.
    monkeypatch.setattr(os, "name", "posix")
    assert memory._pci_device_dir("/sys/bus/pci/devices", "0000:03:00.0").endswith(
        "0000:03:00.0"
    )
    monkeypatch.setattr(os, "name", "nt")
    assert memory._pci_device_dir("C:/fixtures", "0000:03:00.0").endswith(
        "0000-03-00.0"
    )
