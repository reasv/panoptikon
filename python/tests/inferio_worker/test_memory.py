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


class FakeModule:
    """Stand-in for a `torch.nn.Module` holding weights of a given dtype.

    Only what the parameter walk touches: `parameters()` and `buffers()`
    yielding objects with a `.dtype`. Real modules recurse into their
    children through `parameters()`, which is exactly why the walk does not
    have to.
    """

    def __init__(self, params: tuple = (), buffers: tuple = ()) -> None:
        self._params = [SimpleNamespace(dtype=FakeDtype(n)) for n in params]
        self._buffers = [SimpleNamespace(dtype=FakeDtype(n)) for n in buffers]

    def parameters(self):
        return iter(self._params)

    def buffers(self):
        return iter(self._buffers)


def with_fake_nn() -> None:
    """Give the injected fake torch an `nn.Module` type to match against."""
    sys.modules["torch"].nn = SimpleNamespace(Module=FakeModule)


class FakeCuda:
    """Just enough of `torch.cuda` for the memory helpers."""

    def __init__(self, free_mb=8000, total_mb=8192, initialized=True, device_count=1):
        # How many devices the runtime enumerates. Zero is the shape a pin
        # naming a board ROCm does not enumerate produces, and the only thing
        # standing between that and a silent CPU fallback.
        self.devices = device_count
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

    def device_count(self):
        return self.devices

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
    every CUDA-path test. `INFERIO_DEVICE` is dropped with it, and is the
    stronger hazard of the two: it does not merely disable a tier, it
    re-denominates *every* reading this module produces (`_ram_currency`), so
    an inherited value would turn each of these cases into a CPU-priced host
    without changing a line of the test. The measured accelerator context is
    reset for the same reason the board address is: it is measured once per
    *process* and everything downstream of `base_method` reads it, so one
    test's measurement would silently re-price the next test's base.
    """
    with (
        mock.patch.dict(sys.modules, {}, clear=False),
        mock.patch.dict(os.environ, {}, clear=False),
    ):
        sys.modules.pop("inferio.impl.utils", None)
        os.environ.pop("HIP_VISIBLE_DEVICES", None)
        os.environ.pop("INFERIO_DEVICE", None)
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
            mock.patch.dict(
                memory._context_state,
                {"measured_mb": None, "logged": False},
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


# ---------------------------------------------------------------------------
# Measured accelerator context (run2 R8)
# ---------------------------------------------------------------------------


class ProbeWorld:
    """A fake memory query for the context probe: a torch whose CUDA comes up
    when the test says so, and a driver free reading the test controls."""

    def __init__(self, free_at_init=8000, reserved_at_init=0):
        self.initialized = False
        self.free_at_init = free_at_init
        self.reserved_at_init = reserved_at_init
        self.free_reads = 0
        self.torch = SimpleNamespace(
            cuda=SimpleNamespace(is_initialized=lambda: self.initialized)
        )

    def probe(self, free_before=8700):
        return memory._ContextProbe(
            free_before,
            "nvml",
            torch_reader=lambda: self.torch,
            free_reader=self._read_free,
            reserved_reader=lambda: self.reserved_at_init,
        )

    def _read_free(self):
        self.free_reads += 1
        return self.free_at_init


def test_the_context_probe_measures_across_the_first_cuda_init() -> None:
    # Expected behavior: the probe watches for the moment CUDA becomes live
    # and differences the driver's free memory across it. 8700 -> 8000 is a
    # 700 MiB context, which is the order run1 actually measured (report §4,
    # A3: 666-678 MiB) against the 500 MiB constant.
    world = ProbeWorld(free_at_init=8000)
    probe = world.probe(free_before=8700)
    assert probe.poll() is False, "CUDA is not up yet, so there is nothing to read"
    assert world.free_reads == 0, "and nothing is read"
    world.initialized = True
    assert probe.poll() is True
    assert probe.poll() is True, "idempotent once it has its answer"
    assert world.free_reads == 1, "exactly one reading, ever"
    assert probe.result() == 700


def test_the_probe_subtracts_whatever_was_already_allocated() -> None:
    # The flag flips before the weights are copied, but a few milliseconds is
    # enough for a small allocation to land. Whatever landed is in the
    # allocator pool at the same instant, so subtracting it makes the figure
    # the context alone rather than the context plus a race.
    world = ProbeWorld(free_at_init=7800, reserved_at_init=100)
    probe = world.probe(free_before=8700)
    world.initialized = True
    probe.poll()
    assert probe.result() == 800, "900 MiB of driver memory, 100 of it allocated"


def test_an_implausible_context_measurement_is_discarded() -> None:
    # A window a few milliseconds wide can still catch another process
    # starting or stopping. Outside the band it is not a context.
    for free_before, expected in (
        (8000 + memory.CONTEXT_MIN_MB - 1, None),
        (8000 + memory.CONTEXT_MAX_MB + 1, None),
        (8000 + memory.CONTEXT_MIN_MB, memory.CONTEXT_MIN_MB),
        (8000 + memory.CONTEXT_MAX_MB, memory.CONTEXT_MAX_MB),
        (7000, None),  # the driver reported *more* free memory afterwards
    ):
        world = ProbeWorld(free_at_init=8000)
        probe = world.probe(free_before=free_before)
        world.initialized = True
        probe.poll()
        assert probe.result() == expected, free_before


def test_a_process_that_never_initialises_cuda_measures_nothing() -> None:
    # The CPU-fallback impl, the remote API, the CTranslate2 engine: the probe
    # reads nothing at all and the fixed estimate stands.
    world = ProbeWorld()
    probe = world.probe()
    probe.start()
    assert probe.result() is None
    assert world.free_reads == 0


def test_the_probe_is_only_started_when_a_measurement_is_possible(
    fake_torch, monkeypatch
) -> None:
    # Each gate states an impossibility, not a preference.
    with isolated():
        assert memory._start_context_probe(None, "nvml") is None, "no baseline"
        assert memory._start_context_probe(8000, "torch") is None, "not a driver"
        assert memory._start_context_probe(8000, None) is None
        monkeypatch.setenv("INFERIO_DEVICE", "cpu")
        assert memory._start_context_probe(8000, "nvml") is None, "RAM-priced"
        monkeypatch.delenv("INFERIO_DEVICE")
        memory._context_state["measured_mb"] = 700
        assert memory._start_context_probe(8000, "nvml") is None, "already measured"
        memory._context_state["measured_mb"] = None
        started = memory._start_context_probe(8000, "nvml")
        assert started is not None
        assert started.result() is None, "and it stops cleanly"

    with isolated(fake_torch_module(FakeCuda(initialized=True))):
        assert memory._start_context_probe(8000, "nvml") is None, (
            "a context that predates this window cannot be measured"
        )


def test_the_measured_context_replaces_the_estimate_in_the_base() -> None:
    # End to end: a degraded load (no NVML own-PID) whose free delta is
    # unusable falls to the allocator tier, and that tier now charges the
    # context this process measured rather than the constant — and says so in
    # `base_method`, because the two are different formulas.
    cuda = FakeCuda(initialized=False)
    with isolated(fake_torch_module(cuda)):
        with mock.patch.object(memory, "_nvml_memory", return_value=(8700, 24_576)):
            before = memory.begin_load()
            assert before["free_source"] == "nvml"
            world = ProbeWorld(free_at_init=8000)
            world.initialized = True
            probe = world.probe(free_before=8700)
            probe.poll()
            before["context_probe"] = probe

            cuda.initialized = True
            cuda.allocate(1024, reserved_mb=1024)
            # The driver reports MORE free memory than before, so the free
            # delta is unusable and the allocator floor answers.
            report = memory.finish_load(before, object())
            assert memory.context_allowance_mb() == (700, "measured")
    assert report["base_method"] == "alloc_delta_measured"
    assert report["base_mb"] == 1024 + 700
    assert report["base_mb"] != 1024 + memory.CONTEXT_ESTIMATE_MB


def test_the_fixed_estimate_is_the_last_resort_and_names_itself(fake_torch) -> None:
    # No driver reading to measure against: the constant stands, and
    # `base_method` keeps its original spelling so a profile written under it
    # is not silently reinterpreted.
    assert memory.context_allowance_mb() == (memory.CONTEXT_ESTIMATE_MB, "estimate")
    before = memory.begin_load()
    assert before["context_probe"] is None
    fake_torch.allocate(800, reserved_mb=800)
    fake_torch.free += 512 * MIB
    report = memory.finish_load(before, object())
    assert report["base_method"] == "alloc_delta"
    assert report["base_mb"] == 800 + memory.CONTEXT_ESTIMATE_MB


def test_a_measured_context_sharpens_the_plausibility_ceiling() -> None:
    # The ceiling is `reserved_delta + context + slack`, and it is not
    # circular: the context was measured over the initialisation window, not
    # over this whole-load delta. With a 700 MiB context the ceiling is
    # 100 + 700 + 2048 = 2848, so a 2800 MiB delta is now plausible where the
    # 500 MiB constant (ceiling 2648) would have rejected it.
    for measured, method in ((700, "free_delta"), (None, "alloc_delta")):
        cuda = FakeCuda(initialized=True)
        with isolated(fake_torch_module(cuda)):
            memory._context_state["measured_mb"] = measured
            answers = [(8700, 24_576), (5900, 24_576)]
            with mock.patch.object(
                memory,
                "_nvml_memory",
                side_effect=lambda: answers.pop(0) if answers else (5900, 24_576),
            ):
                before = memory.begin_load()
                cuda.allocate(100, reserved_mb=100)
                report = memory.finish_load(before, object())
        assert report["base_method"] == method, measured


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
    # An unrecognised value reads as no answer at all, never as a guess.
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


def test_dtype_is_inferred_from_the_loaded_weights(fake_torch) -> None:
    # Expected behavior: an impl that states nothing (which is all of them
    # but four) is not unkeyable — the weights it just loaded say what
    # precision it is running in, and that is what the profile is keyed on.
    with_fake_nn()

    # The common shape: `self.model` is the module (wd tagger, CLIP, CLAP,
    # sentence-transformers).
    direct = SimpleNamespace(model=FakeModule(params=("torch.float16",)))
    assert memory.resolved_dtype(direct) == ("fp16", "inferred")

    # One level further in, for the wrappers that are not modules
    # themselves: easyocr's `Reader` and its detector, a HF pipeline and its
    # model.
    reader = SimpleNamespace(
        detector=None, recognizer=FakeModule(params=("torch.float32",))
    )
    assert memory.resolved_dtype(SimpleNamespace(model=reader)) == (
        "fp32",
        "inferred",
    )

    # Containers count as a level: an impl holding two towers in a list is
    # not a different case from one holding them in two attributes.
    towers = SimpleNamespace(parts=[FakeModule(params=("torch.bfloat16",))])
    assert memory.resolved_dtype(towers) == ("bf16", "inferred")

    # The model wins over another module that happens to be in there — a
    # projection head, a preprocessor — however the attributes are ordered.
    two = SimpleNamespace(
        head=FakeModule(params=("torch.float32",)),
        model=FakeModule(params=("torch.float16",)),
    )
    assert memory.resolved_dtype(two) == ("fp16", "inferred")

    # Non-float tensors are skipped rather than reported: an int8 weight is
    # not the compute precision, and a buffer answers when no parameter
    # does.
    quantized = FakeModule(
        params=("torch.int8", "torch.uint8"), buffers=("torch.float16",)
    )
    assert memory.resolved_dtype(SimpleNamespace(model=quantized)) == (
        "fp16",
        "inferred",
    )

    # And the weights never outrank a stated dtype: `select_dtype` knows what
    # was negotiated, the walk only knows what the first tensor happens to
    # hold.
    weights = FakeModule(params=("torch.float16",))
    stated = SimpleNamespace(resolved_dtype="torch.bfloat16", model=weights)
    assert memory.resolved_dtype(stated) == ("bf16", "selected")
    attribute = SimpleNamespace(_dtype=FakeDtype("torch.float32"), model=weights)
    assert memory.resolved_dtype(attribute) == ("fp32", "attribute")


def test_a_non_torch_model_reports_the_unstated_sentinel(fake_torch) -> None:
    # CTranslate2/faster-whisper, ONNX Runtime, a remote API: nothing in the
    # instance is a module, so there is nothing to read a precision off.
    # "unstated" is a value, not an omission — a key component that is absent
    # makes the whole profile unkeyable, which is the bug this exists for. It
    # says the impl stated no precision, which is a fact about the impl, not
    # about the worker's ability to look (run2 R11; it was spelled "unknown"
    # when the sentinel was introduced during run1).
    with_fake_nn()
    engine = SimpleNamespace(model=SimpleNamespace(compute_type="float16"))
    assert memory.resolved_dtype(engine) == ("unstated", "unstated")
    assert memory.resolved_dtype_name(engine) is None, (
        "the stated-precision helper still answers None; only the reported "
        "value falls back"
    )
    # A torch build with no `nn` (and a worker with no torch at all) is the
    # same answer by a different route.
    del sys.modules["torch"].nn
    assert memory.resolved_dtype(SimpleNamespace(model=object())) == (
        "unstated",
        "unstated",
    )


def test_the_dtype_walk_never_touches_a_property(fake_torch) -> None:
    # Expected behavior: an impl's properties can load, download or move a
    # model. A measurement harness must not trigger any of that, so the walk
    # reads `__dict__` and never `getattr` on the class's descriptors.
    with_fake_nn()
    touched: list[str] = []

    class Impl:
        def __init__(self) -> None:
            self.model = FakeModule(params=("torch.float16",))

        @property
        def expensive(self):  # pragma: no cover - must never run
            touched.append("expensive")
            raise AssertionError("the walk read a property")

    assert memory.resolved_dtype(Impl()) == ("fp16", "inferred")
    assert touched == []


def test_the_dtype_walk_survives_a_hostile_object_graph(fake_torch) -> None:
    # Expected behavior: the walk runs on the load path of every model, over
    # an object graph this module does not own. A module that refuses to
    # enumerate its weights, a self-referencing attribute, and a container of
    # a thousand things are all shapes a real impl can present, and none of
    # them may hang, recurse or raise — the walk answers if it can and
    # reports the sentinel if it cannot.
    with_fake_nn()

    class Angry(FakeModule):
        """`parameters()` raises — a meta-device or offloaded module."""

        def parameters(self):
            raise RuntimeError("weights live on another device")

    # The buffers still answer.
    offloaded = SimpleNamespace(model=Angry(buffers=("torch.bfloat16",)))
    assert memory.resolved_dtype(offloaded) == ("bf16", "inferred")

    class Mute(Angry):
        """Neither accessor answers."""

        def buffers(self):
            raise RuntimeError("nor here")

    # And a module that answers nothing does not end the search: the next
    # object in the queue is still reached.
    both = SimpleNamespace(
        model=Mute(), spare=FakeModule(params=("torch.float16",))
    )
    assert memory.resolved_dtype(both) == ("fp16", "inferred")

    # A cycle is visited once. `weights` is last in `__dict__` order, so the
    # walk goes through the loop to reach it.
    loop = SimpleNamespace()
    loop.me = loop
    loop.peer = SimpleNamespace(back=loop)
    loop.weights = FakeModule(params=("torch.float32",))
    assert memory.resolved_dtype(loop) == ("fp32", "inferred")

    # A cycle with no module in it terminates rather than spinning.
    left = SimpleNamespace()
    right = SimpleNamespace(other=left)
    left.other = right
    assert memory.resolved_dtype(left) == ("unstated", "unstated")

    # A thousand modules in one dict: the container cap means only the first
    # few are ever unwrapped, and one of them answers.
    horde = SimpleNamespace(
        bag={
            f"m{i}": FakeModule(params=("torch.float16",)) for i in range(1000)
        }
    )
    assert memory.resolved_dtype(horde) == ("fp16", "inferred")

    # A thousand *attributes* are not capped, but the visit budget is: a
    # module sitting past it is not found, and the sentinel — not a hang — is
    # the answer. This asserts the bound, not a wish.
    crowd = SimpleNamespace(**{f"a{i:04d}": object() for i in range(1000)})
    crowd.zz_weights = FakeModule(params=("torch.float16",))
    assert memory.resolved_dtype(crowd) == ("unstated", "unstated")
    # The same crowd answers the moment the module is under a name the walk
    # looks at first, which is what `_MODEL_ATTRS` is for.
    crowd.model = FakeModule(params=("torch.float16",))
    assert memory.resolved_dtype(crowd) == ("fp16", "inferred")


def test_the_load_report_carries_the_dtype_and_how_it_was_obtained(
    fake_torch,
) -> None:
    with_fake_nn()
    impl = SimpleNamespace(model=FakeModule(params=("torch.float16",)))
    before = memory.begin_load()
    fake_torch.allocate(1024, reserved_mb=1536)
    report = memory.finish_load(before, impl)
    assert report["base_mb"] == 1536
    assert report["dtype"] == "fp16"
    assert report["dtype_method"] == "inferred"

    # A process with no footprint to key reports neither: there is nothing
    # for the orchestrator to persist without a base, and a worker that
    # measured nothing must answer exactly as it did before this existed.
    unmeasured = memory.finish_load(memory.begin_load(), object())
    assert "base_mb" not in unmeasured, unmeasured
    assert "dtype" not in unmeasured, unmeasured
    assert "dtype_method" not in unmeasured, unmeasured


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


def write_gtt(root: str, bdf: str, total=None, used=None) -> str:
    """The GTT counters beside them, which a unified board is also budgeted
    against (docs/unified-memory-admission.md, backend B). amdgpu publishes
    these for discrete boards too — they are read only under the DP-5 flag,
    which is what keeps a dGPU worker's numbers where they were."""
    device = Path(memory._pci_device_dir(root, bdf))
    device.mkdir(parents=True, exist_ok=True)
    if total is not None:
        (device / "mem_info_gtt_total").write_text(f"{total}\n", encoding="utf-8")
    if used is not None:
        (device / "mem_info_gtt_used").write_text(f"{used}\n", encoding="utf-8")
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


# ---------------------------------------------------------------------------
# Unified boards: AMD APUs (docs/unified-memory-admission.md, backend B).
# `PANOPTIKON_UNIFIED_GPU=<pci address>` is the spawner's statement about
# which board this worker's replica was pinned to (DP-5) — acted on only when
# the worker resolves that same address for itself. Absent, or naming another
# board, every reading below is byte-identical to what a discrete board
# reported before backend B existed.
# ---------------------------------------------------------------------------

# A BC-250/Strix-Halo-shaped board: a 512 MiB BIOS carve-out with a 64 GiB GTT
# window, 4 GiB of GTT already taken, and 8 GiB of RAM the OS says it could
# actually deliver.
APU_CARVEOUT_MIB = 512
APU_GTT_MIB = 64 * 1024


@contextmanager
def unified(ram_available_mb: int | None = 8 * 1024, bdf: str = "0000:03:00.0"):
    """The DP-5 signal set to a board address, with the RAM reading stubbed —
    for the duration of the block and not a line longer, because several
    cases below assert its *absence* after asserting its presence.

    The value is the board's PCI address, not a flag: the worker only counts
    GTT when the address matches the board it independently resolved, so that
    a mis-enumerated pin cannot make it price one board's memory as another's
    (`gpu.rs::UNIFIED_GPU_ENV_VAR`). `0000:03:00.0` is what `FakeCuda`'s PCI
    fields render to, i.e. the board the fixtures' worker is on.

    psutil is stubbed rather than read: the clamp is the whole point of the
    formula, and a test whose expected numbers came from the machine it runs
    on would assert nothing.
    """
    real = memory._ram_available_bytes
    memory._ram_available_bytes = (
        lambda: None if ram_available_mb is None else ram_available_mb * MIB
    )
    os.environ["PANOPTIKON_UNIFIED_GPU"] = bdf
    try:
        yield
    finally:
        del os.environ["PANOPTIKON_UNIFIED_GPU"]
        memory._ram_available_bytes = real


def test_the_amdgpu_tier_is_gtt_inclusive_on_a_unified_board(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: an APU is budgeted against carve-out + GTT, because
    # that is where its allocations land once the carve-out fills, and its
    # free reading clamps unclaimed GTT to the RAM that exists right now —
    # the pages behind that address space come out of the same memory every
    # other process is using. The orchestrator's refresh computes the
    # identical formula from the identical files, so the two sides still
    # speak one vocabulary under the one label.
    root = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(root, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        # Flag absent: exactly today's VRAM-only arithmetic, GTT files or no.
        assert memory.amdgpu_free_total_mb(root) == (256, 512)
        with unified():
            assert memory.amdgpu_free_total_mb(root) == (
                256 + 8 * 1024,
                APU_CARVEOUT_MIB + APU_GTT_MIB,
            )
        # Plenty of RAM: the GTT term is the driver's own free figure again.
        with unified(ram_available_mb=100 * 1024):
            assert memory.amdgpu_free_total_mb(root) == (
                256 + 60 * 1024,
                APU_CARVEOUT_MIB + APU_GTT_MIB,
            )
        # Every term is required. A board whose GTT counters or whose RAM
        # figure cannot be read is *no* reading — reporting the carve-out
        # alone under a label that now means carve+GTT would hand the ledger
        # two incompatible numbers in one field.
        with unified(ram_available_mb=None):
            assert memory.amdgpu_free_total_mb(root) == (None, None)
        no_gtt = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 0)})
        with unified():
            assert memory.amdgpu_free_total_mb(no_gtt) == (None, None)
        # …and a discrete worker never acquires that dependency.
        assert memory.amdgpu_free_total_mb(no_gtt) == (512, 512)


def test_the_unified_sample_keeps_the_amdgpu_sysfs_label(tmp_path, monkeypatch) -> None:
    # Expected behavior: the label names the driver, not the arithmetic. Both
    # sides of the ledger read the same files through the same flag, so the
    # free-source consistency rule holds without a second label to keep in
    # sync (and a new one would silently lose `amdgpu-sysfs`'s authority).
    root = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(root, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    with rocm_host(tmp_path, monkeypatch, pci=root):
        with unified():
            sample = memory.device_memory_sample()
    assert sample["free_source"] == "amdgpu-sysfs"
    assert (sample["free_mb"], sample["total_mb"]) == (
        256 + 8 * 1024,
        APU_CARVEOUT_MIB + APU_GTT_MIB,
    )


def test_the_fdinfo_tier_counts_gtt_on_a_unified_board(tmp_path, monkeypatch) -> None:
    # Expected behavior: on an APU our own allocations are VRAM + GTT, and a
    # VRAM-only figure would report a multi-gigabyte model as holding a few
    # hundred MB — an under-measured base, which is headroom the ledger hands
    # out twice. Both kernel spellings apply to the GTT keys as they do to
    # the VRAM ones, and the deduplication by client id is unchanged.
    texts = [
        fdinfo("0000:03:00.0", 1, "256 MiB") + "drm-resident-gtt:\t2048 MiB\n",
        fdinfo("0000:03:00.0", 1, "256 MiB") + "drm-resident-gtt:\t2048 MiB\n",
        fdinfo("0000:03:00.0", 2, "128 MiB", key="drm-memory-vram")
        + "drm-memory-gtt:\t512 MiB\n",
        # A board we merely hold open: not ours to charge, either region.
        fdinfo("0000:0c:00.0", 3, "8192 MiB") + "drm-resident-gtt:\t8192 MiB\n",
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts):
        assert memory.fdinfo_own_vram_mb() == 384, "VRAM alone without the flag"
        with unified():
            assert memory.fdinfo_own_vram_mb() == 384 + 2560

    # End to end, on the board shape that motivates it: HIP reports the
    # 512 MiB carve-out as `total_memory`, and the tier's upper sanity bound
    # is measured against **carve-out + GTT** instead — a footprint that
    # includes GTT is legitimately larger than the carve-out, so bounding it
    # by HIP's figure would lose the best tier this backend has on every
    # model worth measuring.
    board = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(board, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    carveout = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(
        tmp_path, monkeypatch, pci=board, fdinfo_texts=texts, cuda=carveout
    ):
        with unified():
            before = memory.begin_load()
            carveout.allocate(2048, reserved_mb=2400)
            report = memory.finish_load(before, object())
    assert report["base_method"] == "fdinfo", report
    assert report["base_mb"] == 384 + 2560, report


def test_the_fdinfo_upper_bound_follows_the_unified_total(tmp_path, monkeypatch) -> None:
    # Expected behavior: the bound is kept on a unified board, with the right
    # comparand. A per-process figure at or above the *board's* whole capacity
    # is a parse or kernel-accounting artefact, not a footprint — and the
    # under-report floor cannot catch it, because over-reporting is the
    # direction that floor treats as normal. On an APU the capacity is
    # carve-out + GTT, which is what the sysfs tier already reads.
    small_gtt_mib = 2048
    board = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 0)})
    write_gtt(board, "0000:03:00.0", small_gtt_mib * MIB, 0)
    # 4 GiB claimed on a board whose whole capacity is 512 MiB + 2 GiB.
    texts = [fdinfo("0000:03:00.0", 1, "1024 MiB") + "drm-resident-gtt:\t3072 MiB\n"]
    cuda = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(tmp_path, monkeypatch, pci=board, fdinfo_texts=texts, cuda=cuda):
        with unified():
            assert memory.fdinfo_own_vram_mb() == 4096, "the reader is neutral"
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1200)
            report = memory.finish_load(before, object())
    assert report["base_method"] != "fdinfo", report

    # And the bound does not depend on psutil. The unified *free* formula
    # needs a RAM figure; the capacity does not, and deriving one from the
    # other would have made this guard vanish on a machine without psutil —
    # the one way a missing dependency could produce an over-reported
    # footprint instead of a missing one.
    cuda = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(tmp_path, monkeypatch, pci=board, fdinfo_texts=texts, cuda=cuda):
        with unified(ram_available_mb=None):
            assert memory.amdgpu_free_total_mb() == (None, None), "no RAM figure"
            assert memory.amdgpu_board_total_mb() == APU_CARVEOUT_MIB + small_gtt_mib
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1200)
            report = memory.finish_load(before, object())
    assert report["base_method"] != "fdinfo", report

    # Just under the capacity, the same reading is a footprint again.
    texts = [fdinfo("0000:03:00.0", 1, "1024 MiB") + "drm-resident-gtt:\t1024 MiB\n"]
    cuda = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(tmp_path, monkeypatch, pci=board, fdinfo_texts=texts, cuda=cuda):
        with unified():
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1200)
            report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("fdinfo", 2048), report

    # Without the flag the same worker's reading is below its own allocator
    # pool *and* at the board's capacity, so it loses the tier — which is the
    # pre-existing guard doing its job, and the symptom a missing flag would
    # produce rather than a silently wrong number.
    carveout = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts, cuda=carveout):
        before = memory.begin_load()
        carveout.allocate(2048, reserved_mb=2400)
        report = memory.finish_load(before, object())
    assert report["base_method"] != "fdinfo", report


def test_the_fdinfo_parser_sums_only_the_regions_it_is_asked_for() -> None:
    # Expected behavior: the region set is a parameter, not a mode. The
    # identity fallback's dominance rule keeps asking about VRAM alone (it
    # ranks boards, and GTT is not a property of the board), while the
    # per-process tier asks for both on a unified host. Absent is still 0 and
    # unreadable is still None, per region.
    text = fdinfo("0000:03:00.0", 7, "256 MiB") + "drm-resident-gtt:\t2048 MiB\n"
    assert memory.parse_drm_fdinfo(text) == ("0000:03:00.0", 7, 256 * MIB)
    assert memory.parse_drm_fdinfo(text, ("vram", "gtt")) == (
        "0000:03:00.0",
        7,
        2304 * MIB,
    )
    # A record with no GTT line at all is a real record holding no GTT.
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, "8 MiB"), ("vram", "gtt")) == (
        "0000:03:00.0",
        7,
        8 * MIB,
    )
    # A GTT line in a unit the documented grammar does not define makes the
    # whole record unreadable, exactly as the VRAM one does: reading it as 0
    # would be inventing an observation.
    broken = fdinfo("0000:03:00.0", 7, "256 MiB") + "drm-resident-gtt:\t2 GiB\n"
    assert memory.parse_drm_fdinfo(broken, ("vram", "gtt")) is None
    assert memory.parse_drm_fdinfo(broken) == ("0000:03:00.0", 7, 256 * MIB)


def test_the_fdinfo_parser_never_mixes_the_two_key_vintages() -> None:
    # Expected behavior: `drm-memory-*` is the deprecated alias for
    # `drm-resident-*`, and the two are different vintages of the same
    # accounting. A kernel printing resident VRAM but only legacy GTT (or the
    # reverse) must not have the two added together — that sum is not a
    # reading of anything. Resident wins for the WHOLE record when it appears
    # at all, so the fallback is per-record, never per-region.
    mixed = (
        fdinfo("0000:03:00.0", 7, "256 MiB")  # drm-resident-vram
        + "drm-memory-gtt:\t2048 MiB\n"
    )
    assert memory.parse_drm_fdinfo(mixed, ("vram", "gtt")) == (
        "0000:03:00.0",
        7,
        256 * MIB,
    ), "the legacy GTT line is ignored because a resident line exists"
    # The all-legacy record is read in full, which is the case the fallback
    # exists for (amdgpu is the only driver that prints the old spelling).
    legacy = (
        fdinfo("0000:03:00.0", 7, "256 MiB", key="drm-memory-vram")
        + "drm-memory-gtt:\t2048 MiB\n"
    )
    assert memory.parse_drm_fdinfo(legacy, ("vram", "gtt")) == (
        "0000:03:00.0",
        7,
        2304 * MIB,
    )
    # And a modern record that simply holds no GTT is still a full reading.
    assert memory.parse_drm_fdinfo(
        fdinfo("0000:03:00.0", 7, "256 MiB"), ("vram", "gtt")
    ) == ("0000:03:00.0", 7, 256 * MIB)


# ---------------------------------------------------------------------------
# The pinned-but-invisible tripwire (docs/rocm-batch-calibration-parity.md):
# backend B moved dGPU+iGPU desktops from "unpinned" to row-index pins, and a
# pin naming a board HIP does not enumerate is a silent CPU fallback.
# ---------------------------------------------------------------------------


def test_a_pin_that_names_no_device_fails_the_load(monkeypatch) -> None:
    # Expected behavior: pinned + zero enumerated devices = a load failure
    # with an actionable message, not a model running twenty times slower on
    # the CPU while the ledger prices it against a board.
    with isolated(fake_torch_module(FakeCuda(device_count=0), hip="7.2.0")):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "1")
        problem = memory.pinned_device_missing()
    assert problem is not None
    assert "'1'" in problem, "names the pin the orchestrator wrote"
    assert "HSA_OVERRIDE_GFX_VERSION" in problem
    assert "CPU" in problem


def test_the_pin_tripwire_stays_quiet_when_it_cannot_be_sure(monkeypatch) -> None:
    # Expected behavior: it reports only what it can positively call wrong.
    # A device the runtime does enumerate, no pin at all, no torch, and the
    # documented hide-everything idioms are all silence.
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "0")
        assert memory.pinned_device_missing() is None, "the device is there"
    with isolated(fake_torch_module(FakeCuda(device_count=0), hip="7.2.0")):
        monkeypatch.delenv("PANOPTIKON_DEVICE_PIN", raising=False)
        assert memory.pinned_device_missing() is None, "nothing was pinned"
        for blank in ("", " "):
            monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", blank)
            assert memory.pinned_device_missing() is None, repr(blank)
        # An operator hiding every device is NOT our placement, and the
        # visibility variables alone cannot tell the two apart — which is why
        # the marker exists. `CUDA_VISIBLE_DEVICES=-1` is the documented
        # hide-everything idiom and must keep working.
        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "-1")
        monkeypatch.setenv("HIP_VISIBLE_DEVICES", "2")
        assert memory.pinned_device_missing() is None, "ambient, not ours"
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "2")
        assert memory.pinned_device_missing() is not None, "ours"
    # No torch in the process at all: a CPU impl on a pinned host reports
    # nothing rather than a fault it has no evidence for.
    with isolated():
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "1")
        assert memory.pinned_device_missing() is None


def test_the_pin_tripwire_ignores_a_cpu_only_torch_build(monkeypatch) -> None:
    # Expected behavior: a CPU-only wheel never enumerated a device to lose,
    # so its empty device list is not a fault. This is the common shape, not a
    # corner: pinning is universal, so every replica on a priced host carries
    # the marker, and the probe prices `accelerator = "cpu"` hosts through
    # nvidia-smi by design — a box with an NVIDIA card, the CPU wheels and
    # `accelerator = "cpu"` is pinned, sees no devices, and is working exactly
    # as configured. Without this it would fail every torch model's load.
    cpu_build = fake_torch_module(FakeCuda(device_count=0))
    cpu_build.version = SimpleNamespace(cuda=None, hip=None)
    with isolated(cpu_build):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "GPU-1a2b")
        assert memory.pinned_device_missing() is None
    # The same build reporting a CUDA version is an accelerated one that lost
    # its device, which is the fault this exists for.
    with isolated(fake_torch_module(FakeCuda(device_count=0))):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "GPU-1a2b")
        assert memory.pinned_device_missing() is not None


def test_the_unified_signal_is_an_address_the_worker_verifies(
    tmp_path, monkeypatch
) -> None:
    # Expected behavior: the orchestrator names the *board* it believes this
    # replica is pinned to, and the worker only counts GTT when that is the
    # board it independently resolved. The pin is a belief — KFD row order
    # being HIP device order is the ROCm design's one load-bearing
    # unverifiable — and a wrong belief is expensive in both directions: a
    # worker that landed on a dGPU would report GTT-inflated free memory
    # under the authoritative `amdgpu-sysfs` label (phantom headroom), and
    # one that landed on the APU without the signal prices a 64 GB board at
    # its 512 MB carve-out. So a bare flag is deliberately NOT accepted.
    with rocm_host(tmp_path, monkeypatch):  # FakeCuda sits at 0000:03:00.0
        assert memory._identity_bdf() == "0000:03:00.0"
        cases = [
            ("0000:03:00.0", True),
            ("0000:03:00.0 ", True),
            ("0000:03:00.0".upper(), True),  # rendered case must not matter
            ("0000:0c:00.0", False),  # the replica landed elsewhere
            ("1", False),  # a bare flag is not an address
            ("", False),
            ("yes", False),
            ("0000:03:00", False),  # not a whole address
        ]
        for value, expected in cases:
            os.environ["PANOPTIKON_UNIFIED_GPU"] = value
            try:
                assert memory._unified_gpu() is expected, value
                assert memory._memory_regions() == (
                    ("vram", "gtt") if expected else ("vram",)
                )
            finally:
                del os.environ["PANOPTIKON_UNIFIED_GPU"]
        assert memory._unified_gpu() is False, "absent is the default everywhere"

    # And with no identity yet — the pre-load reading, before any impl has
    # touched torch — a perfectly correct address still answers false: there
    # is nothing to check it against, and the discrete arithmetic is the
    # conservative reading in both directions.
    with isolated():
        os.environ["PANOPTIKON_UNIFIED_GPU"] = "0000:03:00.0"
        try:
            assert memory._unified_gpu() is False
        finally:
            del os.environ["PANOPTIKON_UNIFIED_GPU"]


def test_a_mislanded_worker_reads_its_board_discretely(tmp_path, monkeypatch) -> None:
    # Expected behavior: the whole point of the address. This worker is on
    # 0000:03:00.0 and the orchestrator believed it was on the APU at
    # 0000:0c:00.0 (a mis-ordered HIP enumeration). It must not add that
    # board's GTT to its own free reading — the ledger treats `amdgpu-sysfs`
    # as authoritative, so the inflated figure would become headroom nothing
    # else could contradict.
    root = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(root, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        with unified(bdf="0000:0c:00.0"):
            assert memory.amdgpu_free_total_mb(root) == (256, 512), (
                "the sample stays in the discrete currency, which is the one "
                "the orchestrator prices a dGPU row in"
            )
        with unified(bdf="0000:03:00.0"):
            assert memory.amdgpu_free_total_mb(root) == (
                256 + 8 * 1024,
                APU_CARVEOUT_MIB + APU_GTT_MIB,
            )


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


# ---------------------------------------------------------------------------
# MPS (Apple Silicon): a unified-memory board
# ---------------------------------------------------------------------------


class FakeMpsAllocator:
    """Just enough of `torch.mps` for the memory helpers.

    Deliberately *without* peak/reset APIs, because torch.mps has none: the
    reported peaks are the post-batch live figures, which is exactly the
    approximation the protocol doc records.
    """

    def __init__(self, recommended_mb: int = 96 * 1024) -> None:
        self.recommended = recommended_mb * MIB
        self.driver = 0
        self.allocated = 0
        self.empty_cache_calls = 0

    def recommended_max_memory(self) -> int:
        return self.recommended

    def driver_allocated_memory(self) -> int:
        return self.driver

    def current_allocated_memory(self) -> int:
        return self.allocated

    def empty_cache(self) -> None:
        self.empty_cache_calls += 1
        self.driver = self.allocated

    # Test helper: pretend a load or a batch allocated `mb`.
    def allocate(self, mb: int, driver_mb: int | None = None) -> None:
        self.allocated += mb * MIB
        self.driver += (driver_mb if driver_mb is not None else mb) * MIB


def fake_mps_torch_module(mps: object | None, available: bool = True) -> SimpleNamespace:
    """A torch stand-in for an Apple Silicon host: a Metal backend, and no
    `torch.cuda` at all (reading it raises `AttributeError`, which every
    guarded reader here has to survive)."""
    module = SimpleNamespace(
        backends=SimpleNamespace(mps=SimpleNamespace(is_available=lambda: available)),
        dtype=FakeDtype,
        version=SimpleNamespace(hip=None, cuda=None),
        __version__="2.7.1",
    )
    if mps is not None:
        module.mps = mps
    return module


@contextmanager
def mps_host(available_mb: int, mps: FakeMpsAllocator | None = None):
    """An MPS worker with `available_mb` of RAM the OS says it could deliver."""
    mps = mps if mps is not None else FakeMpsAllocator()
    memory_info = SimpleNamespace(available=available_mb * MIB)
    with isolated(fake_mps_torch_module(mps)):
        with mock.patch("psutil.virtual_memory", return_value=memory_info):
            yield mps


def test_the_mps_sample_reports_the_pool_and_ram_clamped_free() -> None:
    # The unified reading: `total` is Metal's recommended-max (what our
    # allocations are judged against), `free` is what the OS says it could
    # actually deliver — which is the only way external pressure on a shared
    # pool is visible at all. The pool figures are the driver's allocation
    # (the reserved analogue) and the live tensors.
    with mps_host(available_mb=40 * 1024) as mps:
        mps.allocate(1024, driver_mb=1200)
        assert memory.device_memory_sample() == {
            "free_mb": 40 * 1024,
            "total_mb": 96 * 1024,
            "free_source": "mps",
            "reserved_mb": 1200,
            "allocated_mb": 1024,
        }


def test_mps_free_is_clamped_by_both_terms() -> None:
    # An idle 128 GB Mac can report more available RAM than the accelerator is
    # allowed to use; the budget is the recommended-max either way.
    with mps_host(available_mb=120 * 1024):
        assert memory.free_total_mb() == (96 * 1024, 96 * 1024, "mps")
    # Under real pressure the RAM term is what binds, and that is the whole
    # point of it.
    with mps_host(available_mb=3 * 1024):
        assert memory.free_total_mb() == (3 * 1024, 96 * 1024, "mps")


def test_mps_base_is_the_driver_allocation_at_load_end() -> None:
    # `driver_allocated_memory()` is per-process by construction (each process
    # owns its Metal heap), so it is a tier-1 reading like NVML's own-PID
    # figure — no delta, no context estimate, no plausibility floor.
    with mps_host(available_mb=40 * 1024) as mps:
        before = memory.begin_load()
        mps.allocate(2048, driver_mb=2560)
        report = memory.finish_load(before, object())
    assert report["base_mb"] == 2560
    assert report["base_method"] == "mps"
    assert report["reserved_at_load_mb"] == 2560
    assert report["gpu_total_mb"] == 96 * 1024, "the authoritative total (DP-4)"
    assert report["memory"]["free_source"] == "mps"
    assert "gpu_uuid" not in report, "Apple Silicon has one device and no UUID"
    assert "gpu_bdf" not in report, "and no PCI address"


def test_an_mps_batch_measurement_reports_the_pool_as_its_peak() -> None:
    # torch.mps has no peak counters. The pool is monotone absent an
    # `empty_cache()`, so the post-batch driver allocation is the batch's
    # high-water reserved size — the accepted approximation.
    with mps_host(available_mb=40 * 1024) as mps:
        mps.allocate(1000, driver_mb=1000)
        state = memory.begin_batch()
        mps.allocate(500, driver_mb=800)
        payload = memory.finish_batch(state, items=4)
    batch = payload["measurements"][0]
    assert batch["reserved_before_mb"] == 1000
    assert batch["peak_reserved_mb"] == 1800
    assert batch["allocated_before_mb"] == 1000
    assert batch["peak_allocated_mb"] == 1500


def test_trim_releases_the_mps_pool() -> None:
    # The worker's trim path was CUDA-only; on a unified board the retained
    # pool squeezes the whole machine rather than one card.
    with mps_host(available_mb=40 * 1024) as mps:
        mps.allocate(1024, driver_mb=4096)
        assert memory.empty_cache() is True
        assert mps.empty_cache_calls == 1
        assert memory.pool_stats_mb() == (1024, 1024), "only the slack went back"


def test_the_mps_tier_survives_a_torch_without_it() -> None:
    # Every reader is getattr-guarded: `torch.mps` and `torch.backends.mps`
    # are not on every build, and an AttributeError here would take down the
    # whole load report over a memory reading nothing depended on.
    for module in (
        fake_mps_torch_module(None),  # backends.mps says yes, torch.mps absent
        fake_mps_torch_module(FakeMpsAllocator(), available=False),
        SimpleNamespace(__version__="2.7.1"),  # no backends at all
    ):
        with isolated(module):
            assert memory.device_memory_sample() is None
            assert memory.free_total_mb() == (None, None, None)
            assert memory.pool_stats_mb() == (None, None)
            assert memory.empty_cache() is False
            assert memory.device_identity() == (None, None)
            assert memory.gpu_total_mb() is None
            assert memory.finish_load(memory.begin_load(), object()) == {
                "torch_version": "2.7.1"
            }


def test_the_mps_board_name_is_derived_from_the_same_facts_as_the_probe() -> None:
    # The name is `machdep.cpu.brand_string` plus `hw.memsize` rounded to the
    # nearest GiB — the same two sysctls and the same rounding the
    # orchestrator's `mps.rs::board_name` uses, so the informational
    # `gpu_name` on the wire cannot drift from the board name profiles are
    # keyed by. Off macOS the sysctls answer nothing and the field is simply
    # absent, which is what this box asserts.
    with mock.patch.object(memory, "_sysctl_string", return_value="Apple M3 Max"):
        with mock.patch.object(memory, "_sysctl_u64", return_value=128 * 1024 * MIB):
            assert memory.mps_gpu_name() == "Apple M3 Max (128 GB)"
        with mock.patch.object(memory, "_sysctl_u64", return_value=0):
            assert memory.mps_gpu_name() is None
    if sys.platform != "darwin":
        assert memory._sysctl_string("machdep.cpu.brand_string") is None
        assert memory.mps_gpu_name() is None
        with mps_host(available_mb=40 * 1024):
            assert memory.device_identity() == (None, None)


# ---------------------------------------------------------------------------
# CPU-only hosts: host RAM as the memory currency
# ---------------------------------------------------------------------------


class FakeRam:
    """The machine's RAM and this process's share of it.

    `peak` is deliberately monotone and has no reset: that is what the OS
    high-water mark actually is on every platform, and the whole reason this
    backend maps it onto the *pool* rather than onto a per-batch peak.
    """

    def __init__(
        self,
        total_mb: int = 64 * 1024,
        available_mb: int = 40 * 1024,
        rss_mb: int = 200,
    ) -> None:
        self.total_mb = total_mb
        self.available_mb = available_mb
        self.rss_mb = rss_mb
        self.peak_mb = rss_mb

    def grow(self, mb: int) -> None:
        self.rss_mb += mb
        self.peak_mb = max(self.peak_mb, self.rss_mb)
        self.available_mb -= mb

    def release(self, mb: int) -> None:
        """Free tensors: the resident set drops, the high-water does not."""
        self.rss_mb -= mb
        self.available_mb += mb


@contextmanager
def cpu_host(ram: FakeRam | None = None, torch_module=None, pinned: bool = True):
    """A worker on a host priced against system RAM.

    `pinned` writes the spawner's `INFERIO_DEVICE=cpu`, which is the whole of
    the signal. With it off nothing has been claimed at all — the negative
    half of `_ram_currency`, where a worker with no accelerator facts stays
    silent rather than guessing that RAM is its currency.
    """
    ram = ram if ram is not None else FakeRam()
    with isolated(torch_module):
        os.environ.pop("PANOPTIKON_DEVICE_PIN", None)
        os.environ.pop("INFERIO_DEVICE", None)
        if pinned:
            os.environ["INFERIO_DEVICE"] = "cpu"
        with (
            mock.patch(
                "psutil.virtual_memory",
                side_effect=lambda: SimpleNamespace(
                    total=ram.total_mb * MIB, available=ram.available_mb * MIB
                ),
            ),
            mock.patch.object(memory, "_rss_bytes", lambda: ram.rss_mb * MIB),
            mock.patch.object(memory, "_peak_rss_bytes", lambda: ram.peak_mb * MIB),
        ):
            yield ram


def test_the_cpu_sample_reports_ram_and_the_process_high_water() -> None:
    # The degenerate unified reading: there is no accelerator pool to
    # intersect with, so `free` is what the OS says it could deliver and
    # `total` is the RAM the machine has. The pool figures are this process's
    # own residency — the high-water as the pool, the live RSS as allocated.
    with cpu_host() as ram:
        ram.grow(1300)
        ram.release(300)
        assert memory.device_memory_sample() == {
            "free_mb": 40 * 1024 - 1000,
            "total_mb": 64 * 1024,
            "free_source": "ram",
            "reserved_mb": 1500,
            "allocated_mb": 1200,
        }


def test_the_cpu_tier_is_the_orchestrators_statement_not_a_guess() -> None:
    # A worker with no accelerator facts is *not* enough. That shape also
    # covers a remote-API impl and a `none`-class model on a CUDA host, which
    # would then report host RAM under a label the ledger treats as
    # authoritative against a board whose total is a card's VRAM. How the host
    # was priced is a fact only the orchestrator has, so it says it.
    with cpu_host(pinned=False):
        assert memory._ram_currency() is False
        assert memory.free_total_mb() == (None, None, None)
        assert memory.device_memory_sample() is None
        assert memory.finish_load(memory.begin_load(), object()) == {}


def test_the_cpu_tier_is_gated_off_on_every_accelerator_host(fake_torch) -> None:
    # A CUDA worker's readings must be byte-identical to what they were
    # before this tier existed.
    assert memory._ram_currency() is False
    assert memory.free_total_mb() == (8000, 8192, "torch")
    assert memory.device_memory_sample()["free_source"] == "torch"

    # An MPS worker is on Metal, not on RAM, even though its free reading is
    # also derived from RAM statistics — the two are different currencies
    # (`recommended_max_memory` against physical RAM).
    with mps_host(available_mb=40 * 1024):
        assert memory._ram_currency() is False
        assert memory.free_total_mb()[2] == "mps"


def test_the_cpu_base_is_the_load_windows_rss_growth() -> None:
    # `base_method: "rss"`. A **window** delta, not growth since spawn: a
    # worker that loads a second model must not be charged the first model's
    # residency (the absolute-against-windowed confusion `_fdinfo_base_mb`
    # documents).
    with cpu_host() as ram:
        before = memory.begin_load()
        ram.grow(2048)
        report = memory.finish_load(before, object())
    assert report["base_mb"] == 2048
    assert report["base_method"] == "rss"
    assert report["reserved_at_load_mb"] == 2248, "the high-water at load end"
    assert report["gpu_name"] == "CPU (64 GB)"
    assert report["gpu_total_mb"] == 64 * 1024, "physical RAM, the cross-check"
    assert report["memory"]["free_source"] == "ram"
    assert "gpu_uuid" not in report, "a CPU host has no board to identify"
    assert "gpu_bdf" not in report


def test_a_second_cpu_load_is_charged_only_its_own_window() -> None:
    with cpu_host() as ram:
        first = memory.begin_load()
        ram.grow(2048)
        assert memory.finish_load(first, object())["base_mb"] == 2048
        second = memory.begin_load()
        ram.grow(512)
        assert memory.finish_load(second, object())["base_mb"] == 512


def test_pages_returned_between_loads_do_not_reach_the_next_base() -> None:
    # Allocators normally keep their arenas, but if an unload really does hand
    # pages back, the base must not inherit the drop. It cannot: both ends of
    # the window delta are the *live* resident set (the worker's "peak
    # allocated" slot is the live figure on this backend), so a fall lowers
    # both equally. The stale quantity is the *pool* baseline
    # `reserved_at_load_mb`, which is the lifetime high-water — asserted here
    # too, because that is where the effect actually lands.
    with cpu_host() as ram:
        first = memory.begin_load()
        ram.grow(2048)
        first_report = memory.finish_load(first, object())
        assert first_report["base_mb"] == 2048
        assert first_report["reserved_at_load_mb"] == 2248

        ram.release(1024)  # an unload that genuinely gave pages back
        second = memory.begin_load()
        ram.grow(512)
        second_report = memory.finish_load(second, object())
    assert second_report["base_mb"] == 512, "this load's own growth, and only it"
    assert second_report["reserved_at_load_mb"] == 2248, (
        "the high-water is monotone and has no reset, so the pool baseline "
        "still carries the freed pages — the fit prices batches against it "
        "until one exceeds it again"
    )


def test_a_cpu_load_that_allocates_nothing_reports_no_base() -> None:
    # The never-invent-a-footprint rule, unchanged on this backend: a remote
    # API wrapper that holds nothing reports nothing rather than 0.
    with cpu_host():
        report = memory.finish_load(memory.begin_load(), object())
    assert "base_mb" not in report
    assert "base_method" not in report


def test_a_cpu_batch_measurement_reports_the_high_water_as_its_peak() -> None:
    # The high-water is a *real* peak (the kernel records it as it happens),
    # unlike the MPS approximation — but it is never resettable, so it is
    # reported as the pool. A batch that sets a new high-water reads as
    # pool-growing, which is what the cost fit regresses on...
    with cpu_host() as ram:
        ram.grow(1000)
        state = memory.begin_batch()
        ram.grow(500)
        ram.release(300)
        growing = memory.finish_batch(state, items=4)["measurements"][0]
    assert growing["reserved_before_mb"] == 1200
    assert growing["peak_reserved_mb"] == 1700
    assert growing["allocated_before_mb"] == 1200
    assert growing["peak_allocated_mb"] == 1400

    # ...and a smaller repeat does not, which is what fills the throughput
    # knee's warm-pool ring. Without the high-water-as-pool mapping every
    # batch would look pool-growing and the knee would never get a sample.
    with cpu_host() as ram:
        ram.grow(1000)  # a big batch already reached 1200 MiB…
        ram.release(500)
        state = memory.begin_batch()
        ram.grow(200)  # …so this smaller one sets no new high-water
        ram.release(200)
        warm = memory.finish_batch(state, items=4)["measurements"][0]
    assert warm["peak_reserved_mb"] == warm["reserved_before_mb"] == 1200
    assert warm["allocated_before_mb"] == 700, "the live residency did move"


def test_the_cpu_trim_is_a_no_op() -> None:
    # Decided, not missing (docs/unified-memory-admission.md, "Trim"): there
    # is no allocator pool to release, and the arenas Python frees into do
    # not go back to the OS.
    with cpu_host() as ram:
        ram.grow(1024)
        assert memory.empty_cache() is False
        assert memory.pool_stats_mb() == (1224, 1224)


def test_the_cpu_board_name_is_derived_from_the_same_fact_as_the_probe() -> None:
    # Physical RAM rounded *up* to a 4 GiB grid — the same rule and the same
    # source `cpu.rs::board_name` uses, so the informational `gpu_name` on the
    # wire cannot drift from the board name profiles are keyed by. The grid is
    # what stops a kernel update (which moves what the OS can count) from
    # renaming the machine and orphaning its profiles.
    for total_mb, expected in (
        (64 * 1024 - 700, "CPU (64 GB)"),
        (64 * 1024, "CPU (64 GB)"),
        (16 * 1024 - 400, "CPU (16 GB)"),
        (65 * 1024, "CPU (68 GB)"),
        (1, "CPU (4 GB)"),
    ):
        with cpu_host(FakeRam(total_mb=total_mb, available_mb=1)):
            assert memory.ram_gpu_name() == expected
    with isolated():
        with mock.patch.object(memory, "_virtual_memory", return_value=None):
            assert memory.ram_gpu_name() is None
            assert memory.ram_free_total_mb() == (None, None)


def test_the_process_high_water_is_read_in_the_right_unit() -> None:
    # Two readers, two units, and getting either wrong is a factor of 1024.
    assert memory.parse_vm_high_water("VmRSS:\t 100 kB\nVmHWM:\t   2048 kB\n") == (
        2048 * 1024
    )
    assert memory.parse_vm_high_water("VmHWM:\t 2048 MB\n") is None, (
        "an undocumented unit is not a reading"
    )
    assert memory.parse_vm_high_water("VmRSS:\t 100 kB\n") is None

    # `resource` does not exist on Windows, so the module is injected rather
    # than patched — which is also the only way this reader is reachable from
    # a Windows test run at all.
    with fake_resource(ru_maxrss=4096):
        with mock.patch.object(sys, "platform", "darwin"):
            assert memory._rusage_peak_bytes() == 4096, "macOS reports bytes"
        with mock.patch.object(sys, "platform", "linux"):
            assert memory._rusage_peak_bytes() == 4096 * 1024, "elsewhere, KiB"


@contextmanager
def fake_resource(ru_maxrss: int | None):
    """A stand-in `resource` module; `None` makes `getrusage` fail."""

    def getrusage(_who):
        if ru_maxrss is None:
            raise OSError("no rusage here")
        return SimpleNamespace(ru_maxrss=ru_maxrss)

    module = SimpleNamespace(RUSAGE_SELF=0, getrusage=getrusage)
    with mock.patch.dict(sys.modules, {"resource": module}, clear=False):
        yield


def test_the_high_water_is_never_below_the_live_residency() -> None:
    # The two come from different interfaces; a "peak" under the live reading
    # would be a reading of nothing.
    with isolated():
        with (
            mock.patch.object(memory, "_rss_bytes", lambda: 900 * MIB),
            mock.patch.object(sys, "platform", "sunos5"),
        ):
            with fake_resource(ru_maxrss=None):
                assert memory._peak_rss_bytes() == 900 * MIB, "no peak reader"
            with fake_resource(ru_maxrss=100 * 1024):
                assert memory._peak_rss_bytes() == 900 * MIB, "a peak under it"
            with fake_resource(ru_maxrss=2000 * 1024):
                assert memory._peak_rss_bytes() == 2000 * MIB


def test_a_cpu_priced_host_reports_one_currency_even_with_a_live_gpu() -> None:
    # The currency-salad guard. `INFERIO_DEVICE=cpu` is what `get_device()`
    # honours, but an impl that ignores it — or a library that initializes a
    # context on import — can still leave this process holding a live CUDA
    # device. Every reading has to come from the *one* statement about how
    # this host was priced, or the report names a card while its free, total
    # and base figures are all the machine's RAM, and the ledger has no way
    # to tell.
    cuda = FakeCuda()
    cuda.reserved = 4096 * MIB
    cuda.allocated = 3000 * MIB
    with cpu_host(torch_module=fake_torch_module(cuda)) as ram:
        assert memory._ram_currency() is True
        before = memory.begin_load()
        ram.grow(2048)
        report = memory.finish_load(before, object())

    assert report["base_method"] == "rss"
    assert report["base_mb"] == 2048
    assert report["gpu_total_mb"] == 64 * 1024, "RAM, not the card's VRAM"
    assert report["gpu_name"] == "CPU (64 GB)"
    assert "gpu_uuid" not in report, "no board identity on a RAM-priced report"
    assert "gpu_bdf" not in report
    assert report["memory"] == {
        "free_mb": 40 * 1024 - 2048,
        "total_mb": 64 * 1024,
        "free_source": "ram",
        "reserved_mb": 2248,
        "allocated_mb": 2248,
    }, "no allocator statistics from a device this host is not priced against"
    # And the trim path releases nothing rather than shrinking a pool the
    # ledger measures in RSS and would never see move.
    with cpu_host(torch_module=fake_torch_module(cuda)):
        assert memory.empty_cache() is False
    assert cuda.empty_cache_calls == 0


def test_a_cpu_priced_mac_reports_ram_and_not_metal() -> None:
    # DP-3's one unaccelerated path: an `accelerator = "cpu"` Mac has a
    # perfectly available Metal backend *and* is priced as a CPU board, so
    # every reading has to be the RAM one — including the informational name,
    # which would otherwise put the chip on the wire for a board keyed `CPU`.
    ram = FakeRam(total_mb=128 * 1024, available_mb=90 * 1024)
    with cpu_host(ram, torch_module=fake_mps_torch_module(FakeMpsAllocator())):
        assert memory._ram_currency() is True
        assert memory.free_total_mb() == (90 * 1024, 128 * 1024, "ram")
        assert memory.device_identity() == (None, "CPU (128 GB)")
        assert memory.gpu_total_mb() == 128 * 1024, "RAM, not recommended-max"
        ram.grow(1500)
        assert memory.pool_stats_mb() == (1700, 1700)
