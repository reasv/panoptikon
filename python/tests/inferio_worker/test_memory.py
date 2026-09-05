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
        # `device_count=0` is the shape a pin naming a GPU ROCm does not
        # enumerate produces — the silent CPU fallback the tripwire catches.
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
        # torch exposes the PCI fields from 2.8; `None` stands for the older
        # builds that do not, which includes the 2.7.1 the CUDA extras pin.
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
        props = SimpleNamespace(uuid=self.uuid, name=self.name, total_memory=self.total)
        if self.pci is not None:
            keys = ("pci_domain_id", "pci_bus_id", "pci_device_id")
            props.__dict__.update(dict(zip(keys, self.pci)))
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
    """Control every input the module reads from the process: `sys.modules`,
    the NVML state, the memoized GPU address, the measured context, the
    one-shot log flags and the two environment variables that would otherwise
    switch a tier off or re-denominate every reading. Each is resolved once per
    *process*, so leaving one would leak between cases."""
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
                {"measured_mb": None, "logged": False, "probe": None},
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


def test_sample_reports_allocator_and_driver_state(fake_torch) -> None:
    fake_torch.allocate(100)
    assert memory.device_memory_sample() == {
        "free_mb": 7900,
        "total_mb": 8192,
        "free_source": "torch",
        "reserved_mb": 100,
        "allocated_mb": 100,
    }


def test_a_worker_without_torch_measures_and_reports_nothing(no_torch) -> None:
    # A CPU or remote-API worker has no measurable device footprint.
    assert memory.device_memory_sample() is None
    assert memory.finish_load(memory.begin_load(), object()) == {}


def test_sensing_never_initializes_cuda() -> None:
    # reset_peak_memory_stats, mem_get_info and get_device_properties all
    # CREATE a CUDA context when none exists.
    def boom(self, *args):
        raise AssertionError("this call would initialize CUDA")

    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        mem_get_info = reset_peak_memory_stats = boom
        memory_reserved = memory_allocated = get_device_properties = boom

    with isolated(fake_torch_module(Tripwire())):
        assert memory.device_memory_sample() is None
        assert memory.device_identity() == (None, None)
        # The version needs no device and is still reported; nothing else is.
        report = memory.finish_load(memory.begin_load(), object())
        assert report == {"torch_version": "2.7.1+cu128"}
        batch = memory.finish_batch(memory.begin_batch(), items=2)["measurements"][0]
        assert (batch["items"], batch["peak_reserved_mb"]) == (2, None)


def test_load_that_initializes_cuda_is_still_measured() -> None:
    # The common case is that `load()` itself creates the CUDA context, so
    # before-values do not exist and a missing "before" is a baseline of 0.
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
    # Torch present and CUDA live, but this load put nothing in the allocator
    # (a CTranslate2 engine, a CPU-fallback impl, a remote API).
    before = memory.begin_load()
    fake_torch.free -= 2048 * MIB  # another process, not us
    report = memory.finish_load(before, object())
    assert "base_mb" not in report and "base_method" not in report, report
    assert report["reserved_at_load_mb"] == 0
    assert report["torch_version"] == "2.7.1+cu128"


def test_the_base_tier_and_the_provenance_it_reports() -> None:
    # One decision, not a ladder.
    cases = (
        ("the driver's own delta", 1024, 1536, 0, "free_delta", 1536),
        ("a pool overshoot is not implausible", 1024, 4096, 0, "free_delta", 4096),
        ("implausible: a neighbour took 6 GB", 1024, 1024, -6144, "alloc_delta", None),
        ("unusable: a neighbour released 1 GB", 800, 800, +1024, "alloc_delta", None),
        ("below the allocator floor", 2048, 2048, +1024, "alloc_delta", None),
    )
    for label, allocated, reserved, adjust, method, base in cases:
        cuda = FakeCuda()
        with isolated(fake_torch_module(cuda)):
            before = memory.begin_load()
            cuda.allocate(allocated, reserved_mb=reserved)
            cuda.free += adjust * MIB
            report = memory.finish_load(before, object())
        expected = base if base is not None else allocated + memory.CONTEXT_ESTIMATE_MB
        assert (report["base_method"], report["base_mb"]) == (method, expected), label
        assert report["reserved_at_load_mb"] == reserved, label
        assert report["memory"]["reserved_mb"] == reserved, label


# --- Measured accelerator context (run2 R8) ---


class ProbeWorld:
    """A torch whose CUDA comes up when the test says so, over a driver free
    reading the test controls."""

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
    # The probe watches for the moment CUDA becomes live and differences the
    # driver's free memory across it.
    for free_at_init, reserved, expected in ((8000, 0, 700), (7800, 100, 800)):
        world = ProbeWorld(free_at_init=free_at_init, reserved_at_init=reserved)
        probe = world.probe(free_before=8700)
        assert probe.poll() is False, "CUDA is not up yet, nothing to read"
        assert world.free_reads == 0, "and nothing is read"
        world.initialized = True
        assert probe.poll() is True
        assert probe.poll() is True, "idempotent once it has its answer"
        assert world.free_reads == 1, "exactly one reading, ever"
        assert probe.result() == expected, (free_at_init, reserved)


def _waiting_probe(free_before, readings, live):
    """A probe over a clock-free world: `readings` are handed out in order and
    `live` says whether CUDA has come up."""
    return memory._ContextProbe(
        free_before,
        "nvml",
        torch_reader=lambda: SimpleNamespace(
            cuda=SimpleNamespace(is_initialized=lambda: live["on"])
        ),
        free_reader=lambda: readings.pop(0),
        reserved_reader=lambda: 0,
    )


def test_the_flip_reads_the_pool_before_the_free_memory() -> None:
    # Order, not just subtraction: an allocation that lands *between* the two
    # reads is missing from the pool figure, which over-states the context.
    order: list[str] = []
    probe = _waiting_probe(8700, [], {"on": True})
    probe._read_free = lambda: (order.append("free"), 8000)[1]
    probe._read_reserved = lambda: (order.append("reserved"), 0)[1]
    probe.poll()
    assert order == ["reserved", "free"]
    assert probe.result() == 700


def test_the_baseline_is_refreshed_while_the_probe_waits() -> None:
    # `begin_load`'s reading can be minutes old by the time an impl initialises
    # CUDA, and a neighbour releasing memory there would under-state the base.
    live = {"on": False}
    probe = _waiting_probe(9500, [9000, 8700, 8000], live)
    for _ in range(20):
        assert probe.poll() is False
    assert probe._free_before == 9500, "no refresh inside one interval"
    for expected in (9000, 8700):
        probe._baseline_at -= memory._CONTEXT_BASELINE_SECONDS
        assert probe.poll() is False
        assert probe._free_before == expected
    live["on"] = True
    assert probe.poll() is True
    assert probe.result() == 700, "measured against the freshest baseline, not 9500"


def test_a_baseline_read_that_races_the_initialisation_is_discarded() -> None:
    # A reading taken while CUDA was coming up already contains the context:
    # using it as the baseline would measure a context of nothing.
    live = {"on": False}
    readings = [8000, 8000]
    probe = _waiting_probe(8700, readings, live)

    def racing_read():
        live["on"] = True
        return readings.pop(0)

    probe._read_free = racing_read
    probe._baseline_at -= memory._CONTEXT_BASELINE_SECONDS
    assert probe.poll() is False
    assert probe._free_before == 8700, "the racing reading is not a baseline"
    assert probe.poll() is True
    assert probe.result() == 700


def test_an_implausible_context_measurement_is_discarded() -> None:
    # A window a few milliseconds wide can still catch another process starting
    # or stopping; outside the band it is not a context.
    for free_before, expected in (
        (8000 + memory.CONTEXT_MIN_MB - 1, None),
        (8000 + memory.CONTEXT_MAX_MB + 1, None),
        (8000 + memory.CONTEXT_MAX_MB, memory.CONTEXT_MAX_MB),
        (7000, None),  # the driver reported *more* free memory afterwards
    ):
        world = ProbeWorld(free_at_init=8000)
        probe = world.probe(free_before=free_before)
        world.initialized = True
        probe.poll()
        assert probe.result() == expected, free_before


def test_a_process_that_never_initialises_cuda_measures_nothing() -> None:
    # The CPU-fallback impl, the remote API, the CTranslate2 engine.
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
        monkeypatch.setenv("INFERIO_DEVICE", "cpu")
        assert memory._start_context_probe(8000, "nvml") is None, "RAM-priced"
        monkeypatch.delenv("INFERIO_DEVICE")
        memory._context_state["measured_mb"] = 700
        assert memory._start_context_probe(8000, "nvml") is None, "already measured"
        memory._context_state["measured_mb"] = None
        # A probe left over from an earlier load is collected first, so two
        # loads in one process cannot leave two watchers polling.
        stale = ProbeWorld().probe()
        stale.start()
        memory._context_state["probe"] = stale
        started = memory._start_context_probe(8000, "nvml")
        assert stale._stop.is_set(), "the previous watcher was stopped"
        assert started is not None and started is not stale
        assert memory._context_state["probe"] is started

    with isolated(fake_torch_module(FakeCuda(initialized=True))):
        assert memory._start_context_probe(8000, "nvml") is None, (
            "a context that predates this window cannot be measured"
        )


def test_a_failed_load_collects_its_probe_and_keeps_what_it_measured() -> None:
    # `finish_load` is never reached when `instance.load()` raises, so a
    # retried load would otherwise accumulate one watcher per attempt.
    with isolated():
        probe = ProbeWorld().probe()
        probe.start()
        memory._context_state["probe"] = probe
        memory.abort_load({"context_probe": probe})
        assert probe._stop.is_set(), "the watcher was told to stop"
        assert memory._context_state["probe"] is None
        assert memory._context_state["logged"] is False, (
            "a failed load must not burn the one-shot line: the next load in "
            "this process may still measure a context"
        )
        # `begin_load` returns {} when it could measure nothing at all, and the
        # cleanup path must report the load's own error, never one of its own.
        memory.abort_load({})
        memory.abort_load(None)  # type: ignore[arg-type]
    with isolated():
        world = ProbeWorld(free_at_init=8000)
        measured = world.probe(free_before=8700)
        world.initialized = True
        measured.poll()
        memory.abort_load({"context_probe": measured})
        assert memory.context_allowance_mb() == (700, "measured")


def test_the_measured_context_replaces_the_estimate_in_the_base() -> None:
    # End to end: a degraded load whose free delta is unusable falls to the
    # allocator tier, which charges the measured context and says so.
    cuda = FakeCuda(initialized=False)
    with isolated(fake_torch_module(cuda)):
        with mock.patch.object(memory, "_nvml_memory", return_value=(8700, 24_576)):
            before = memory.begin_load()
            assert before["free_source"] == "nvml"
            world = ProbeWorld(free_at_init=8000)
            world.initialized = True
            before["context_probe"] = world.probe(free_before=8700)
            before["context_probe"].poll()
            cuda.initialized = True
            cuda.allocate(1024, reserved_mb=1024)
            report = memory.finish_load(before, object())
            assert memory.context_allowance_mb() == (700, "measured")
    assert report["base_method"] == "alloc_delta_measured"
    assert report["base_mb"] == 1024 + 700


def test_the_fixed_estimate_is_the_last_resort_and_names_itself(fake_torch) -> None:
    # No driver reading to measure against: the constant stands, and
    # `base_method` keeps its own spelling.
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
    # circular.
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


def test_the_load_window_pins_its_free_source(
    fake_torch, tmp_path, monkeypatch
) -> None:
    # NVML free and torch's `mem_get_info` disagree by GBs on the same GPU (3.4
    # GB apart on the dev box).
    monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "no-pci"))
    for nvml_free, pool, after_mb in ((20000, 1024, None), (9000, 100, 6976)):
        cuda = FakeCuda()
        with isolated(fake_torch_module(cuda)):
            answers = [(nvml_free, 24_576), (None, None)]
            with mock.patch.object(
                memory,
                "_nvml_memory",
                side_effect=lambda: answers.pop(0) if answers else (None, None),
            ):
                before = memory.begin_load()
                assert before["free_source"] == "nvml", "NVML is preferred"
                cuda.allocate(pool, reserved_mb=pool)
                if after_mb is not None:
                    # An unpinned re-read would have found a live, readable
                    # reading 2024 MiB from the "before" one — below the
                    # ceiling, and entirely the two sources' disagreement.
                    cuda.free = after_mb * MIB
                report = memory.finish_load(before, object())
        assert report["base_method"] == "alloc_delta", report
        assert report["base_mb"] == pool + memory.CONTEXT_ESTIMATE_MB, report


def test_nvml_per_process_wins_and_missing_pid_is_logged(fake_torch, caplog) -> None:
    # Tier 1: NVML's own-pid figure is absolute and pollution-free.
    proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=3000 * MIB)
    with with_nvml(fake_pynvml_for(fake_torch, [proc])):
        before = memory.begin_load()
        fake_torch.allocate(1024)
        report = memory.finish_load(before, object())
        assert (report["base_mb"], report["base_method"]) == (3000, "nvml")

        proc.pid = proc.pid + 1
        memory._logged["nvml_pid_missing"] = False
        with caplog.at_level("INFO", logger="inferio_worker.memory"):
            before = memory.begin_load()
            fake_torch.allocate(512)
            report = memory.finish_load(before, object())
            memory.finish_load(memory.begin_load(), object())  # no repeat line
        assert report["base_method"] != "nvml", report
        assert len(_pid_lines(caplog)) == 1, caplog.records


def _pid_lines(caplog) -> list[str]:
    return [r.message for r in caplog.records if "NVML lists no process" in r.message]


def test_the_per_process_figure_is_declined_without_the_namespace_line(
    fake_torch, caplog
) -> None:
    # Two ordinary declines that are not the container degradation, so the
    # "NVML lists no process" line must stay silent.
    for used, label in ((None, "WDDM"), (fake_torch.total, "a whole-GPU sentinel")):
        proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=used)
        cuda = FakeCuda()
        with isolated(fake_torch_module(cuda)):
            with with_nvml(fake_pynvml_for(cuda, [proc])):
                with caplog.at_level("INFO", logger="inferio_worker.memory"):
                    before = memory.begin_load()
                    assert before["free_source"] == "nvml", label
                    cuda.allocate(1024, reserved_mb=1536)
                    report = memory.finish_load(before, object())
        assert (report["base_method"], report["base_mb"]) == ("free_delta", 1536), label
        assert _pid_lines(caplog) == [], label


def test_nvml_handle_resolution_is_retried_after_cuda_comes_up(fake_torch) -> None:
    # The FIRST NVML call of a worker's life happens in `begin_load`, before
    # the impl initialized CUDA, so on a host whose pin is not a UUID there is
    # no identity to resolve the GPU with yet; caching that failure would
    # disable NVML for the process forever.
    gpus = {f"GPU-{fake_torch.uuid}": "handle-a", "GPU-other": "handle-b"}
    lookups: list[str] = []

    def by_uuid(raw: bytes):
        lookups.append(raw.decode())
        if lookups[-1] not in gpus:
            raise RuntimeError("Not Found")
        return gpus[lookups[-1]]

    fake_pynvml = SimpleNamespace(
        nvmlDeviceGetHandleByUUID=by_uuid,
        nvmlDeviceGetCount=lambda: len(gpus),
        nvmlDeviceGetHandleByIndex=lambda index: list(gpus.values())[index],
        nvmlDeviceGetUUID=lambda handle: b"unrelated",
    )
    fake_torch.initialized = False
    with mock.patch.dict(
        memory._nvml_state,
        {"module_tried": True, "module": fake_pynvml, "handle": None},
        clear=False,
    ):
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "1"}, clear=False):
            assert memory._nvml() is None, "no CUDA context yet: GPU unknown"
            assert lookups == [], "nothing to look up before torch has a device"
            fake_torch.initialized = True
            nvml = memory._nvml()
        assert nvml is not None and nvml[1] == "handle-a"
        assert lookups == [f"GPU-{fake_torch.uuid}"]


def test_abbreviated_uuid_pins_are_resolved_by_prefix(fake_torch) -> None:
    # `resolve_pin` passes an operator's abbreviated `GPU-1a2b` through
    # verbatim because CUDA accepts prefixes, but NVML wants the full string.
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
    for pin, expected in (("gpu-1A2B", "h0"), ("GPU-", None)):  # CUDA folds case
        with mock.patch.dict(
            os.environ, {"CUDA_VISIBLE_DEVICES": pin}, clear=False
        ):
            assert memory._nvml_handle(fake_pynvml) == expected, pin


def test_dtype_prefers_the_negotiated_value_over_config_strings(fake_torch) -> None:
    # Three stated sources in order of authority, and `dtype`/`_dtype` count
    # only when they hold a real torch.dtype.
    for instance, expected, label in (
        (object(), None, "nothing stated"),
        (SimpleNamespace(resolved_dtype="torch.bfloat16"), "bf16", "the convention"),
        (SimpleNamespace(dtype="torch.float16"), None, "a config string"),
        (SimpleNamespace(dtype=FakeDtype("torch.float16")), "fp16", "a real dtype"),
        (SimpleNamespace(_dtype=FakeDtype("int8")), None, "an unmapped dtype"),
    ):
        assert memory.resolved_dtype_name(instance) == expected, label

    # select_dtype's recorded decision outranks a config string, and is read
    # without importing the inferio package.
    fake_utils = SimpleNamespace(last_selected_dtype=lambda: "torch.bfloat16")
    with mock.patch.dict(sys.modules, {"inferio.impl.utils": fake_utils}, clear=False):
        assert memory.resolved_dtype_name(object()) == "bf16"
        assert (
            memory.resolved_dtype_name(SimpleNamespace(dtype="fp32")) == "bf16"
        ), "a requested-precision string must not beat the negotiated dtype"
        assert (
            memory.resolved_dtype_name(SimpleNamespace(resolved_dtype="fp32")) == "fp32"
        ), "an explicit resolved_dtype is still the most authoritative"


def test_dtype_is_inferred_from_the_loaded_weights(fake_torch) -> None:
    # An impl that states nothing (which is all of them but four) is not
    # unkeyable.
    with_fake_nn()
    weights = FakeModule(params=("torch.float16",))
    inferred = "inferred"
    for instance, expected, label in (
        (SimpleNamespace(model=FakeModule(params=("torch.float16",))),
         ("fp16", inferred), "`self.model` is the module (wd tagger, CLIP)"),
        (SimpleNamespace(model=SimpleNamespace(
            detector=None, recognizer=FakeModule(params=("torch.float32",)))),
         ("fp32", inferred), "one level in, for wrappers (easyocr, HF)"),
        (SimpleNamespace(head=FakeModule(params=("torch.float32",)),
                         parts=[FakeModule(params=("torch.bfloat16",))],
                         model=FakeModule(params=("torch.float16",))),
         ("fp16", inferred), "a container is a level, and the model wins"),
        (SimpleNamespace(model=FakeModule(params=("torch.int8", "torch.uint8"),
                                          buffers=("torch.float16",))),
         ("fp16", inferred), "non-float tensors are skipped, buffers answer"),
        (SimpleNamespace(resolved_dtype="torch.bfloat16", model=weights),
         ("bf16", "selected"), "the weights never outrank a stated dtype"),
        (SimpleNamespace(_dtype=FakeDtype("torch.float32"), model=weights),
         ("fp32", "attribute"), "nor a real torch.dtype attribute"),
        (SimpleNamespace(model=SimpleNamespace(compute_type="float16")),
         ("unstated", "unstated"), "nothing in the instance is a module"),
    ):
        assert memory.resolved_dtype(instance) == expected, label
    engine = SimpleNamespace(model=SimpleNamespace(compute_type="float16"))
    assert memory.resolved_dtype_name(engine) is None, (
        "the stated-precision helper still answers None; only the reported "
        "value falls back"
    )
    # A torch build with no `nn` is the same answer by a different route.
    del sys.modules["torch"].nn
    assert memory.resolved_dtype(SimpleNamespace(model=object())) == (
        "unstated",
        "unstated",
    )


def test_the_dtype_walk_survives_a_hostile_object_graph(fake_torch) -> None:
    # The walk runs on the load path of every model, over an object graph this
    # module does not own: a module that refuses to enumerate its weights, a
    # cycle and a container of a thousand things must not hang, recurse or
    # raise, and a property is never read (an impl's can load or move a model).
    with_fake_nn()
    touched: list[str] = []

    class Impl:
        def __init__(self) -> None:
            self.model = FakeModule(params=("torch.float16",))

        @property
        def expensive(self):  # pragma: no cover - must never run
            touched.append("expensive")
            raise AssertionError("the walk read a property")

    class Angry(FakeModule):
        # `parameters()` raises: a meta-device or offloaded module.
        def parameters(self):
            raise RuntimeError("weights live on another device")

    class Mute(Angry):
        # Neither accessor answers.
        def buffers(self):
            raise RuntimeError("nor here")

    loop = SimpleNamespace()
    loop.me = loop
    loop.peer = SimpleNamespace(back=loop)
    loop.weights = FakeModule(params=("torch.float32",))
    crowd = SimpleNamespace(**{f"a{i:04d}": object() for i in range(1000)})
    crowd.zz_weights = FakeModule(params=("torch.float16",))
    fp16, unstated = ("fp16", "inferred"), ("unstated", "unstated")
    for instance, expected, label in (
        (Impl(), fp16, "a property is never read"),
        (SimpleNamespace(model=Angry(buffers=("torch.bfloat16",))),
         ("bf16", "inferred"), "the buffers still answer"),
        (SimpleNamespace(model=Mute(), spare=FakeModule(params=("torch.float16",))),
         fp16, "a module that answers nothing does not end the search"),
        (loop, ("fp32", "inferred"), "a cycle is visited once"),
        (SimpleNamespace(bag={f"m{i}": FakeModule(params=("torch.float16",))
                              for i in range(1000)}),
         fp16, "only the first few of a container are unwrapped"),
        (crowd, unstated, "a module past the visit budget is not found"),
    ):
        assert memory.resolved_dtype(instance) == expected, label
    assert touched == []
    crowd.model = FakeModule(params=("torch.float16",))
    assert memory.resolved_dtype(crowd) == fp16, "unless it is under a walked name"


def test_the_load_report_carries_the_dtype_and_how_it_was_obtained(
    fake_torch,
) -> None:
    with_fake_nn()
    impl = SimpleNamespace(model=FakeModule(params=("torch.float16",)))
    before = memory.begin_load()
    fake_torch.allocate(1024, reserved_mb=1536)
    report = memory.finish_load(before, impl)
    assert (report["dtype"], report["dtype_method"]) == ("fp16", "inferred")

    # A process with no footprint to key reports neither.
    unmeasured = memory.finish_load(memory.begin_load(), object())
    assert not {"base_mb", "dtype", "dtype_method"} & set(unmeasured), unmeasured


def test_batch_measurement_is_per_call(fake_torch) -> None:
    fake_torch.allocate(500)  # weights, before any batch
    state = memory.begin_batch()
    assert fake_torch.reset_calls >= 1, "peaks are reset before the batch"
    fake_torch.allocate(200, reserved_mb=300)
    payload = memory.finish_batch(state, items=8)
    m = payload["measurements"][0]
    assert m["items"] == 8
    assert (m["reserved_before_mb"], m["peak_reserved_mb"]) == (500, 800)
    assert (m["allocated_before_mb"], m["peak_allocated_mb"]) == (500, 700)
    assert m["duration_ms"] >= 0.0
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


def test_an_unreadable_allocator_keeps_what_the_caller_already_knew(
    fake_torch, monkeypatch
) -> None:
    # Only the *peaks* come from the allocator.
    def exploding():
        raise RuntimeError("the allocator query failed")

    monkeypatch.setattr(memory, "_allocator_stats", exploding)
    measurement = memory.measure_batch(
        memory.begin_batch(),
        items=4,
        oom=True,
        oom_class={"source": "typed_exception", "exception": "OutOfMemoryError"},
        free_mb=1234,
        free_source="nvml",
        clamped={"from_units": 8, "to_units": 3, "free_mb": 1234},
    )
    assert isinstance(measurement.pop("duration_ms"), float)
    assert measurement == {
        "items": 4,
        "reserved_before_mb": None,
        "peak_reserved_mb": None,
        "allocated_before_mb": None,
        "peak_allocated_mb": None,
        "oom": True,
        "oom_class": {"source": "typed_exception", "exception": "OutOfMemoryError"},
        "free_mb": 1234,
        "free_source": "nvml",
        "clamped": {"from_units": 8, "to_units": 3, "free_mb": 1234},
    }, "the peaks are what failed; nothing the caller knew is dropped"


def test_empty_cache_releases_the_pool_only_when_cuda_is_live(fake_torch) -> None:
    """The only way our process gives VRAM back to the GPU short of exiting:
    freeing tensors leaves the caching allocator holding the blocks."""
    fake_torch.allocate(400, reserved_mb=1000)
    fake_torch.allocated = 0  # the batch's tensors are gone; the pool is not
    assert memory.empty_cache() is True
    assert fake_torch.empty_cache_calls == 1
    assert fake_torch.reserved == 0, "the pool went back to the driver"

    # An uninitialized CUDA device is the case this gate exists for.
    fake_torch.initialized = False
    assert memory.empty_cache() is False
    assert fake_torch.empty_cache_calls == 1, "not even attempted"
    with isolated():
        assert memory.empty_cache() is False, "and a worker with no torch at all"

# ---------------------------------------------------------------------------
# GPU identity: PCI address, total memory, HIP UUID suppression (docs/rocm-
# batch-calibration-parity.md, D3)
# ---------------------------------------------------------------------------


def test_the_identity_fields_the_load_report_carries(fake_torch) -> None:
    # The PCI address is the one identity vocabulary the kernel, amdgpu and HIP
    # all speak, so it is the ROCm ledger join.
    assert memory.device_bdf() == "0000:03:00.0"
    assert memory.gpu_total_mb() == 8192
    fake_torch.pci = (1, 0xC1, 0x1F)
    fake_torch.total = 24_560 * MIB
    assert memory.device_bdf() == "0001:c1:1f.0"
    assert memory.gpu_total_mb() == 24_560
    fake_torch.pci = (0, 0x100, 0)  # out of range is not an address
    assert memory.device_bdf() is None

    # An older torch carries no PCI fields and no total at all.
    fake_torch.pci = None
    fake_torch.total = None
    before = memory.begin_load()
    fake_torch.allocate(512)
    report = memory.finish_load(before, object())
    assert "gpu_bdf" not in report and "gpu_total_mb" not in report, report
    assert report["gpu_uuid"] == f"GPU-{fake_torch.uuid}"


def test_hip_suppresses_the_uuid_but_keeps_the_address() -> None:
    # Torch >= 2.5 renders a UUID on ROCm too, but it is a THIRD vocabulary
    # that repeats across cards of a model, so those replicas key on the BDF.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda, hip="7.2.0")):
        assert memory.device_identity() == (None, "Fake GPU 5090"), "name kept"
        before = memory.begin_load()
        cuda.allocate(1024)
        report = memory.finish_load(before, object())
    assert "gpu_uuid" not in report, report
    assert (report["gpu_bdf"], report["gpu_total_mb"]) == ("0000:03:00.0", 8192)
    assert report["torch_version"] == "2.11.0+rocm7.2"
    # The same GPU on a CUDA build reports the UUID, and the address rides
    # along additively (registration keys on the UUID first there).
    with isolated(fake_torch_module(FakeCuda())):
        assert memory.device_identity()[0] == "GPU-1a2b3c4d-0000-0000-0000-000000000000"


def test_a_raising_props_getter_degrades_one_field_not_the_whole_report() -> None:
    # The fields of `get_device_properties` are pybind getters, not plain
    # attributes, and one that raises must not take down the load report.
    class Hostile:
        name = "Fake GPU 5090"
        total_memory = 8192 * MIB
        pci_domain_id, pci_bus_id, pci_device_id = 0, 0x03, 0x00

        @property
        def uuid(self):
            raise RuntimeError("the pybind getter blew up")

    class HostileProps(FakeCuda):
        def get_device_properties(self, index):
            return Hostile()

    cuda = HostileProps()
    with isolated(fake_torch_module(cuda)):
        assert memory.device_bdf() == "0000:03:00.0", "the other fields still read"
        before = memory.begin_load()
        cuda.allocate(1024)
        report = memory.finish_load(before, object())
    assert "gpu_uuid" not in report, report
    assert report["gpu_name"] == "Fake GPU 5090"
    assert report["base_mb"] is not None, "the measurement survived intact"


def fdinfo(pdev: str, client: int, vram: str | None, key: str = "drm-resident-vram") -> str:
    lines = ["pos:\t0", "drm-driver:\tamdgpu",
             f"drm-pdev:\t{pdev}", f"drm-client-id:\t{client}"]
    if vram is not None:
        lines.append(f"{key}:\t{vram}")
    return "\n".join(lines) + "\n"


def test_the_fdinfo_parser_reads_only_what_the_format_defines() -> None:
    # The documented grammar is `<uint> [KiB|MiB]`, and `drm-memory-<region>`
    # is the kernel docs' deprecated alias for `drm-resident-<region>`.
    gtt = "drm-resident-gtt:\t2048 MiB\n"
    legacy_gtt = "drm-memory-gtt:\t2048 MiB\n"
    for text, regions, expected, label in (
        (fdinfo("0000:03:00.0", 7, "1024 KiB"), None, 1024 * 1024, "KiB"),
        (fdinfo("0000:03:00.0", 7, "2 MiB", key="drm-memory-vram"), None,
         2 * 1024 * 1024, "the deprecated alias"),
        (fdinfo("0000:03:00.0", 7, "4096"), None, 4096, "bare means bytes"),
        (fdinfo("0000:03:00.0", 7, "8 GiB"), None, None, "GiB is not in the grammar"),
        (fdinfo("0000:03:00.0", 7, "8 MiB") + "drm-memory-vram:\t1 KiB\n", None,
         8 * 1024 * 1024, "both spellings present: the modern one wins"),
        (fdinfo("0000:03:00.0", 7, "256 MiB") + gtt, None, 256 * MIB,
         "GTT is not summed unless it is asked for"),
        (fdinfo("0000:03:00.0", 7, "256 MiB") + gtt, ("vram", "gtt"), 2304 * MIB,
         "and is when it is"),
        (fdinfo("0000:03:00.0", 7, "256 MiB") + "drm-resident-gtt:\t2 GiB\n",
         ("vram", "gtt"), None, "an unreadable GTT line invalidates the record"),
        (fdinfo("0000:03:00.0", 7, "256 MiB") + legacy_gtt, ("vram", "gtt"), 256 * MIB,
         "a legacy line is ignored where a resident line exists"),
        (fdinfo("0000:03:00.0", 7, "256 MiB", key="drm-memory-vram") + legacy_gtt,
         ("vram", "gtt"), 2304 * MIB, "the all-legacy record is read in full"),
    ):
        record = memory.parse_drm_fdinfo(*(text, regions) if regions else (text,))
        assert (record[2] if record else None) == expected, label
    # Upper-case addresses compare against ours, which are lower-case.
    assert memory.parse_drm_fdinfo(fdinfo("0000:0C:00.0", 1, "1 KiB"))[0] == (
        "0000:0c:00.0"
    )


def test_fdinfo_records_that_are_not_readings() -> None:
    # Absent and UNREADABLE are different answers: a client with no memory line
    # holds no VRAM — a record the dominance rule needs to see — while a line
    # that does not parse invalidates the whole record, since reading it as 0
    # would hand dominance to a different GPU.
    assert memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, None)) == (
        "0000:03:00.0",
        7,
        0,
    )
    for text, label in (
        ("pos:\t0\nflags:\t02\nmnt_id:\t24\n", "a non-DRM fd"),
        ("drm-pdev:\t0000:03:00.0\n", "no client id"),
        ("drm-client-id:\t7\n", "no address"),
        ("not a fdinfo at all", "junk"),
        (fdinfo("0000:03:00.0", "seven", "1 KiB"), "a client id that is not one"),
    ):
        assert memory.parse_drm_fdinfo(text) is None, label
    for unreadable in ("lots", "-4 KiB", "1 2 KiB", "KiB"):
        assert (
            memory.parse_drm_fdinfo(fdinfo("0000:03:00.0", 7, unreadable)) is None
        ), unreadable
    # Several fds of one DRM client (dup(), fork) are ONE client, summing them
    # would double the process's VRAM, and an invalidated record contributes
    # nothing at all rather than a zero.
    assert memory.fdinfo_vram_by_pdev(
        [
            fdinfo("0000:03:00.0", 1, "1024 KiB"),
            fdinfo("0000:03:00.0", 1, "1024 KiB"),  # the same client, dup()ed
            fdinfo("0000:03:00.0", 2, "512 KiB"),
            fdinfo("0000:0c:00.0", 3, "8 MiB", key="drm-memory-vram"),
            fdinfo("0000:0c:00.0", 4, "lots"),
            "not a drm fd at all\n",
        ]
    ) == {"0000:03:00.0": (1024 + 512) * 1024, "0000:0c:00.0": 8 * 1024 * 1024}
    assert memory.fdinfo_vram_by_pdev([]) == {}


def test_dominant_vram_pdev_needs_a_strict_winner(tmp_path) -> None:
    # The identity fallback for an older ROCm torch with no PCI fields.
    a, b = "0000:03:00.0", "0000:0c:00.0"
    for entries, expected, label in (
        ([fdinfo(a, 1, "4 KiB"), fdinfo(b, 2, "8192 MiB")], b, "a strict maximum"),
        ([fdinfo(a, 1, "8 MiB"), fdinfo(b, 2, "8 MiB")], None, "a tie"),
        ([fdinfo(a, 1, None), fdinfo(b, 2, "0")], None, "nothing allocated yet"),
        ([fdinfo(a, 1, "0")], None, "one GPU open, holding nothing"),
        ([fdinfo(a, 1, "512 MiB")], a, "a lone allocator"),
    ):
        root = fdinfo_root(tmp_path, entries)
        assert memory.dominant_vram_pdev(root) == expected, label
    # No /proc at all (every platform but Linux) is simply unknown.
    assert memory.dominant_vram_pdev(str(tmp_path / "missing")) is None


def test_the_fdinfo_fallback_is_hip_only(fake_torch, monkeypatch) -> None:
    # The fdinfo scan exists for ROCm torch too old to expose the PCI fields.
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

    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        assert memory.device_bdf() == "0000:03:00.0", "the PCI fields win"
    assert len(scans) == 1, "the fallback was not consulted"


def test_an_fdinfo_derived_address_must_look_like_a_pci_address(
    tmp_path, monkeypatch
) -> None:
    # Unlike the torch-derived BDF, which this module formats itself out of
    # three integers, this one is a string lifted out of a `drm-pdev` line.
    cuda = FakeCuda()
    cuda.pci = None  # the older-ROCm-torch chain, i.e. the fdinfo fallback
    hostile = [
        "drm-pdev:\t../../../etc\ndrm-client-id:\t1\ndrm-resident-vram:\t512 MiB\n",
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=hostile, cuda=cuda):
        assert memory.dominant_vram_pdev() == "../../../etc", "the parse is neutral"
        assert memory.device_bdf() is None, "the identity is not"
        assert memory._identity_bdf() is None
        assert memory.fdinfo_own_vram_mb() is None
        assert memory.amdgpu_free_total_mb() == (None, None)
    # The well-formed spelling still resolves, so this is a shape check and not
    # an accidental ban on the fallback.
    good = [fdinfo("0000:0c:00.0", 1, "512 MiB")]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=good, cuda=cuda):
        assert memory.device_bdf() == "0000:0c:00.0"


def test_identity_helpers_never_raise_and_never_initialize_cuda() -> None:
    # The forbidden call is *recorded* as well as raised, so a helper that
    # reached it would fail here rather than silently create a context; and a
    # torch that raises on every attribute must degrade, not propagate.
    calls: list[str] = []

    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        def get_device_properties(self, index):
            calls.append("get_device_properties")
            raise AssertionError("this would initialize CUDA")

    class Exploding:
        def __getattr__(self, name):
            raise RuntimeError("boom")

    with isolated(fake_torch_module(Tripwire(), hip="7.2.0")):
        assert memory.device_bdf() is None
        assert memory.gpu_total_mb() is None
        assert memory.device_identity() == (None, None)
    assert calls == [], calls
    with isolated(SimpleNamespace(cuda=Exploding())):
        assert memory.device_bdf() is None
        assert memory.gpu_total_mb() is None
        assert memory.device_memory_sample() is None
        assert memory.empty_cache() is False
        assert memory.finish_load(memory.begin_load(), object()) == {}
        assert memory.finish_batch(memory.begin_batch(), items=3)["measurements"]


def write_gpu(root: str, bdf: str, total=None, used=None) -> str:
    """One GPU's amdgpu VRAM counters under a fake `/sys/bus/pci/devices`. The
    directory name goes through `_pci_device_dir`, which swaps the BDF's colons
    for dashes on Windows, where a colon cannot appear in a path component."""
    device = Path(memory._pci_device_dir(root, bdf))
    device.mkdir(parents=True, exist_ok=True)
    if total is not None:
        (device / "mem_info_vram_total").write_text(f"{total}\n", encoding="utf-8")
    if used is not None:
        (device / "mem_info_vram_used").write_text(f"{used}\n", encoding="utf-8")
    return root


def write_gtt(root: str, bdf: str, total=None, used=None) -> str:
    """The GTT counters beside them, which a unified-memory device is also
    budgeted against. amdgpu publishes these for discrete GPUs too; they are
    read only under the DP-5 flag."""
    device = Path(memory._pci_device_dir(root, bdf))
    device.mkdir(parents=True, exist_ok=True)
    if total is not None:
        (device / "mem_info_gtt_total").write_text(f"{total}\n", encoding="utf-8")
    if used is not None:
        (device / "mem_info_gtt_used").write_text(f"{used}\n", encoding="utf-8")
    return root


def _fresh(tmp_path, prefix: str) -> Path:
    """A directory no earlier call in this test has written to: both trees are
    read by *listing* them, so a stale file is indistinguishable from a real
    one."""
    root = tmp_path / f"{prefix}-{len(list(tmp_path.iterdir()))}"
    root.mkdir()
    return root


def pci_root(tmp_path, gpus: dict) -> str:
    """A fake PCI device tree: `{bdf: (total_bytes, used_bytes)}`."""
    root = _fresh(tmp_path, "pci")
    for bdf, (total, used) in gpus.items():
        write_gpu(str(root), bdf, total, used)
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
    """A ROCm worker whose two sysfs roots point at fixture trees. Both roots
    are always redirected: the tiers read `/sys` and `/proc`, and what this
    machine has there is not this suite's business."""
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
    # The driver publishes a total and a used figure, not a free one, and this
    # is the SAME pair of files the orchestrator's refresh reads.
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        root = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
        assert memory.amdgpu_free_total_mb(root) == (23_552, 24_576)
        for total, used, expected, label in (
            (8 * MIB + 700_000, 0, (8, 8), "floored to whole MiB"),
            (8 * MIB, 9 * MIB, (0, 8), "the counters update independently"),
            (8 * MIB, None, (None, None), "both files are required"),
        ):
            fresh = str(_fresh(tmp_path, "sysfs"))
            write_gpu(fresh, "0000:03:00.0", total=total, used=used)
            assert memory.amdgpu_free_total_mb(fresh) == expected, label
        # This worker's GPU is not in the tree at all (a container with a
        # subset of `/sys`, a fabricated SR-IOV address).
        assert memory.amdgpu_free_total_mb(str(tmp_path / "missing")) == (None, None)
    # And with no identity there is nothing to read *about*.
    with isolated():
        assert memory.amdgpu_free_total_mb(
            pci_root(tmp_path, {"0000:03:00.0": (8 * MIB, 0)})
        ) == (None, None)


def test_the_tier_chain_falls_through_by_availability_not_by_platform(
    tmp_path, monkeypatch
) -> None:
    # One chain on every host — NVML, then amdgpu sysfs, then torch — because
    # each tier's own availability is already the platform test. `mem_get_info`
    # on HIP was historically process-local (ROCm/hip#348), so the sysfs tier
    # outranks it there; on CUDA the address may resolve (torch >= 2.8) and the
    # tier is *reached*, answering nothing by absence of the files.
    gpu = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    with rocm_host(tmp_path, monkeypatch, pci=pci_root(tmp_path, gpu)):
        assert memory.free_total_mb() == (23_552, 24_576, "amdgpu-sysfs")
        sample = memory.device_memory_sample()
        assert sample["free_source"] == "amdgpu-sysfs"
        assert (sample["free_mb"], sample["total_mb"]) == (23_552, 24_576)
    with rocm_host(tmp_path, monkeypatch):
        assert memory.free_total_mb() == (8000, 8192, "torch"), "no sysfs files"
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", pci_root(tmp_path, gpu))
        monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-fdinfo"))
        with with_nvml(fake_pynvml_for(cuda, [])):
            assert memory.free_total_mb() == (
                8000,
                8192,
                "nvml",
            ), "NVML answers first and the sysfs files are never consulted"
    root = _fresh(tmp_path, "cuda-pci")
    write_gpu(str(root), "0000:03:00.0")  # the directory, none of the files
    with isolated(fake_torch_module(FakeCuda())):
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", str(root))
        monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-fd2"))
        assert memory._identity_bdf() == "0000:03:00.0", "the address resolves"
        assert memory.amdgpu_free_total_mb() == (None, None), "and answers nothing"


def test_the_free_source_is_pinned_across_a_rocm_load_window(
    tmp_path, monkeypatch
) -> None:
    # A base measured as a free-memory delta is only meaningful between two
    # readings of the SAME source, and an unhonourable pin must yield nothing
    # rather than the next tier's answer — in either direction.
    gpu = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    root = pci_root(tmp_path, gpu)
    with rocm_host(tmp_path, monkeypatch, pci=root) as cuda:
        before = memory.begin_load()
        assert before["free_source"] == "amdgpu-sysfs"
        # 1.5 GB left the GPU device-wide while our allocator grew by 1 GB.
        write_gpu(root, "0000:03:00.0", total=24_576 * MIB, used=(1024 + 1536) * MIB)
        cuda.allocate(1024, reserved_mb=1024)
        report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("free_delta", 1536), report
    with rocm_host(tmp_path, monkeypatch, pci=pci_root(tmp_path, gpu)):
        # A window that began on torch ends on torch, and a label no tier
        # answers to is not a fallback instruction.
        assert memory._free_total_mb("torch") == (8000, 8192, "torch")
        assert memory._free_total_mb("nvidia-smi") == (None, None, None)
    # Upwards on a CUDA build: NVML is the unpinned preference, a pin to the
    # tier below it is honoured, and when that tier goes away the tier sitting
    # ready above it is precisely what must not be substituted.
    for pci, pinned in (
        (pci_root(tmp_path, gpu), (23_552, 24_576, "amdgpu-sysfs")),
        (empty_dir(tmp_path, "gone-up"), (None, None, None)),
    ):
        cuda = FakeCuda()
        with isolated(fake_torch_module(cuda)):
            monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", pci)
            monkeypatch.setattr(memory, "FDINFO_ROOT", empty_dir(tmp_path, "cuda-up"))
            with with_nvml(fake_pynvml_for(cuda, [])):
                assert memory._free_total_mb() == (8000, 8192, "nvml")
                assert memory._free_total_mb("amdgpu-sysfs") == pinned


def test_fdinfo_is_the_rocm_per_process_base_tier(tmp_path, monkeypatch) -> None:
    # Tier 1's ROCm twin: an absolute whole-process footprint, read about OUR
    # process, deduplicated by client id and filtered to this worker's own GPU.
    texts = [
        fdinfo("0000:03:00.0", 1, "1024 MiB"),
        fdinfo("0000:03:00.0", 1, "1024 MiB"),  # the same client, dup()ed
        fdinfo("0000:03:00.0", 2, "512 MiB", key="drm-memory-vram"),
        fdinfo("0000:0c:00.0", 3, "8192 MiB"),  # a GPU we merely have open
        "not a drm fd at all\n",
    ]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
        assert memory.fdinfo_own_vram_mb() == 1536
        before = memory.begin_load()
        cuda.allocate(1024, reserved_mb=1200)
        report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("fdinfo", 1536), report
    # A GPU with no clients of ours, and a GPU holding nothing, are both "no
    # reading" rather than a zero footprint.
    other, idle = fdinfo("0000:0c:00.0", 3, "8 MiB"), fdinfo("0000:03:00.0", 1, None)
    for absent in ([other], [idle]):
        with rocm_host(tmp_path, monkeypatch, fdinfo_texts=absent):
            assert memory.fdinfo_own_vram_mb() is None
    # HIP-gated, unlike the sysfs tier: nvidia-drm publishes the same keys for
    # a different quantity, so the reader stays neutral but the tier does not
    # answer on a CUDA build.
    cuda = FakeCuda()
    with isolated(fake_torch_module(cuda)):
        one = fdinfo_root(tmp_path, [fdinfo("0000:03:00.0", 1, "8 MiB")])
        monkeypatch.setattr(memory, "FDINFO_ROOT", one)
        monkeypatch.setattr(memory, "PCI_DEVICES_ROOT", empty_dir(tmp_path, "no-pci"))
        assert memory.fdinfo_own_vram_mb() == 8, "the reader itself is neutral"
        before = memory.begin_load()
        cuda.allocate(200, reserved_mb=200)
        report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("free_delta", 200), report


def test_the_fdinfo_tier_works_off_the_dominant_client_identity(
    tmp_path, monkeypatch
) -> None:
    # The older-ROCm-torch chain end to end: `get_device_properties` carries no
    # PCI fields, so the identity is the dominant DRM client.
    cuda = FakeCuda()
    cuda.pci = None
    texts = [fdinfo("0000:0c:00.0", 1, "1536 MiB"), fdinfo("0000:03:00.0", 2, "4 MiB")]
    with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts, cuda=cuda):
        assert memory._identity_bdf() == "0000:0c:00.0"
        before = memory.begin_load()
        cuda.allocate(1024, reserved_mb=1200)
        report = memory.finish_load(before, object())
        assert (report["base_mb"], report["base_method"]) == (1536, "fdinfo"), report
        assert report["gpu_bdf"] == "0000:0c:00.0"

        Path(memory.FDINFO_ROOT, "dominance-moved").write_text(
            fdinfo("0000:03:00.0", 3, "8000 MiB"), encoding="utf-8"
        )
        assert memory.dominant_vram_pdev() == "0000:03:00.0", "the scan moves"
        second = memory.begin_load()
        cuda.allocate(64, reserved_mb=64)
        again = memory.finish_load(second, object())
    assert again["gpu_bdf"] == "0000:0c:00.0", "the wire field is the memoized identity"


# --- Unified GPUs: AMD APUs (docs/unified-memory-admission.md, backend B). ---

# A BC-250/Strix-Halo-shaped GPU.
APU_CARVEOUT_MIB = 512
APU_GTT_MIB = 64 * 1024


@contextmanager
def unified(ram_available_mb: int | None = 8 * 1024, bdf: str = "0000:03:00.0"):
    """The DP-5 signal set to a GPU address, with the RAM reading stubbed, for
    the duration of the block and not a line longer (several cases assert its
    absence right after asserting its presence). `0000:03:00.0` is what
    `FakeCuda`'s PCI fields render to. psutil is stubbed rather than read: a
    test whose expected numbers came from the machine it runs on asserts
    nothing."""
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


def test_the_amdgpu_tier_is_gtt_inclusive_on_a_unified_device(
    tmp_path, monkeypatch
) -> None:
    # An APU is budgeted against carve-out + GTT, clamped by the RAM the OS
    # says it could deliver, because that is where its allocations land once
    # the carve-out fills. The label still names the driver, not the formula.
    root = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(root, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    total = APU_CARVEOUT_MIB + APU_GTT_MIB
    no_gtt = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 0)})
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        for pci, ram_mb, expected, label in (
            (root, None, (256, 512), "no flag: today's VRAM-only arithmetic"),
            (root, 8 * 1024, (256 + 8 * 1024, total), "the RAM term binds"),
            (root, 100 * 1024, (256 + 60 * 1024, total), "the driver's GTT free"),
            (root, "none", (None, None), "every term is required"),
            (no_gtt, 8 * 1024, (None, None), "including the GTT counters"),
            (no_gtt, None, (512, 512), "and a dGPU acquires no such dependency"),
        ):
            if ram_mb is None:
                assert memory.amdgpu_free_total_mb(pci) == expected, label
                continue
            with unified(ram_available_mb=None if ram_mb == "none" else ram_mb):
                assert memory.amdgpu_free_total_mb(pci) == expected, label
    with rocm_host(tmp_path, monkeypatch, pci=root):
        with unified():
            sample = memory.device_memory_sample()
    assert sample["free_source"] == "amdgpu-sysfs", "the driver, not the formula"
    assert (sample["free_mb"], sample["total_mb"]) == (256 + 8 * 1024, total)


def test_the_fdinfo_tier_counts_gtt_on_a_unified_device(tmp_path, monkeypatch) -> None:
    # On an APU our own allocations are VRAM + GTT, and a VRAM-only figure
    # would report a multi-gigabyte model as holding a few hundred MB.
    texts = [
        fdinfo("0000:03:00.0", 1, "256 MiB") + "drm-resident-gtt:\t2048 MiB\n",
        fdinfo("0000:03:00.0", 1, "256 MiB") + "drm-resident-gtt:\t2048 MiB\n",
        fdinfo("0000:03:00.0", 2, "128 MiB", key="drm-memory-vram")
        + "drm-memory-gtt:\t512 MiB\n",
        # A GPU we merely hold open: not ours to charge, either region.
        fdinfo("0000:0c:00.0", 3, "8192 MiB") + "drm-resident-gtt:\t8192 MiB\n",
    ]
    gpu = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(gpu, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    carveout = FakeCuda(total_mb=APU_CARVEOUT_MIB)
    with rocm_host(tmp_path, monkeypatch, pci=gpu, fdinfo_texts=texts, cuda=carveout):
        assert memory.fdinfo_own_vram_mb() == 384, "VRAM alone without the flag"
        with unified():
            assert memory.fdinfo_own_vram_mb() == 384 + 2560
            before = memory.begin_load()
            carveout.allocate(2048, reserved_mb=2400)
            report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("fdinfo", 384 + 2560), report


def test_the_fdinfo_upper_bound_follows_the_unified_total(tmp_path, monkeypatch) -> None:
    # The bound is kept on a unified-memory device with the right comparand:
    # HIP may report an APU's `total_memory` as the carve-out alone, which any
    # GTT-inclusive footprint worth measuring exceeds. It must not depend on
    # psutil either, or a missing dependency would over-report a footprint.
    small_gtt_mib = 2048
    gpu = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 0)})
    write_gtt(gpu, "0000:03:00.0", small_gtt_mib * MIB, 0)
    over = [fdinfo("0000:03:00.0", 1, "1024 MiB") + "drm-resident-gtt:\t3072 MiB\n"]
    under = [fdinfo("0000:03:00.0", 1, "1024 MiB") + "drm-resident-gtt:\t1024 MiB\n"]

    def base(texts, ram_available_mb=8 * 1024):
        cuda = FakeCuda(total_mb=APU_CARVEOUT_MIB)
        with rocm_host(tmp_path, monkeypatch, pci=gpu, fdinfo_texts=texts, cuda=cuda):
            with unified(ram_available_mb=ram_available_mb):
                before = memory.begin_load()
                cuda.allocate(1024, reserved_mb=1200)
                report = memory.finish_load(before, object())
                total = memory.amdgpu_device_total_mb()
        return (report["base_method"], report.get("base_mb"), total)

    assert base(over)[0] != "fdinfo", "4 GiB on a 512 MiB + 2 GiB GPU"
    assert base(over, ram_available_mb=None)[0] != "fdinfo", "and without psutil"
    assert base(under) == ("fdinfo", 2048, APU_CARVEOUT_MIB + small_gtt_mib)


# --- The pinned-but-invisible tripwire (docs/rocm-batch-calibration-parity.md). ---


def test_the_pin_tripwire_fires_only_on_our_own_placement(monkeypatch) -> None:
    # Pinned + zero enumerated devices = a load failure with an actionable
    # message, because the alternative is a silent CPU fallback priced against
    # a GPU. Everything it cannot positively call wrong answers None.
    with isolated(fake_torch_module(FakeCuda(device_count=0), hip="7.2.0")):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "1")
        problem = memory.pinned_device_missing()
    assert problem is not None
    assert "'1'" in problem, "it names the pin the orchestrator wrote"
    assert "HSA_OVERRIDE_GFX_VERSION" in problem and "CPU" in problem
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "0")
        assert memory.pinned_device_missing() is None, "the device is there"
    with isolated(fake_torch_module(FakeCuda(device_count=0), hip="7.2.0")):
        monkeypatch.delenv("PANOPTIKON_DEVICE_PIN", raising=False)
        assert memory.pinned_device_missing() is None, "nothing was pinned"
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", " ")
        assert memory.pinned_device_missing() is None, "blank is not a pin"
        # An operator hiding every device is NOT our placement, and the
        # visibility variables alone cannot tell the two apart.
        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "-1")
        monkeypatch.setenv("HIP_VISIBLE_DEVICES", "2")
        assert memory.pinned_device_missing() is None, "ambient, not ours"
        monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "2")
        assert memory.pinned_device_missing() is not None, "ours"
    # No torch at all, and a CPU-only wheel that never enumerated a device to
    # lose, are silent; the same build reporting a CUDA version is the fault.
    cpu_build = fake_torch_module(FakeCuda(device_count=0))
    cpu_build.version = SimpleNamespace(cuda=None, hip=None)
    for module, fires in (
        (None, False),
        (cpu_build, False),
        (fake_torch_module(FakeCuda(device_count=0)), True),
    ):
        with isolated(module):
            monkeypatch.setenv("PANOPTIKON_DEVICE_PIN", "GPU-1a2b")
            assert (memory.pinned_device_missing() is not None) is fires


def test_the_unified_signal_is_an_address_the_worker_verifies(
    tmp_path, monkeypatch
) -> None:
    # The orchestrator names the *GPU* it believes this replica is pinned to,
    # and the worker counts GTT only when that address is the one it resolved
    # for itself: a wrong belief prices one GPU's memory as another's.
    with rocm_host(tmp_path, monkeypatch):  # FakeCuda sits at 0000:03:00.0
        assert memory._identity_bdf() == "0000:03:00.0"
        for value, expected in (
            ("0000:03:00.0", True),
            ("0000:03:00.0 ".upper(), True),  # case and spacing must not matter
            ("0000:0c:00.0", False),  # the replica landed elsewhere
            ("1", False),  # a bare flag is not an address
            ("", False),
            ("0000:03:00", False),  # not a whole address
        ):
            with mock.patch.dict(
                os.environ, {"PANOPTIKON_UNIFIED_GPU": value}, clear=False
            ):
                assert memory._unified_gpu() is expected, value
                regions = ("vram", "gtt") if expected else ("vram",)
                assert memory._memory_regions() == regions, value
        assert memory._unified_gpu() is False, "absent is the default everywhere"
    # With no identity yet — the pre-load reading — the answer is discrete.
    with isolated():
        with mock.patch.dict(
            os.environ, {"PANOPTIKON_UNIFIED_GPU": "0000:03:00.0"}, clear=False
        ):
            assert memory._unified_gpu() is False
    # And a worker that landed on another GPU keeps the discrete currency.
    root = pci_root(tmp_path, {"0000:03:00.0": (APU_CARVEOUT_MIB * MIB, 256 * MIB)})
    write_gtt(root, "0000:03:00.0", APU_GTT_MIB * MIB, 4096 * MIB)
    with isolated(fake_torch_module(FakeCuda(), hip="7.2.0")):
        with unified(bdf="0000:0c:00.0"):
            assert memory.amdgpu_free_total_mb(root) == (256, 512)


def test_nvml_is_refused_outright_on_a_rocm_worker(tmp_path, monkeypatch) -> None:
    # NVML is not merely *unavailable* on a ROCm worker, it is refused.
    gpu = {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)}
    texts = [fdinfo("0000:03:00.0", 1, "1536 MiB")]
    with rocm_host(
        tmp_path, monkeypatch, pci=pci_root(tmp_path, gpu), fdinfo_texts=texts
    ) as cuda:
        proc = SimpleNamespace(pid=os.getpid(), usedGpuMemory=3000 * MIB)
        with with_nvml(fake_pynvml_for(cuda, [proc])):
            assert memory._nvml() is None, "torch.version.hip is set"
            assert memory._nvml_own_process_mb() is None
            assert memory.free_total_mb() == (23_552, 24_576, "amdgpu-sysfs")
            before = memory.begin_load()
            cuda.allocate(1024, reserved_mb=1200)
            report = memory.finish_load(before, object())
    assert (report["base_method"], report["base_mb"]) == ("fdinfo", 1536), report

    # The pre-torch-import half of the gate, which is what answers for the
    # FIRST reading of a worker's life.
    with isolated():
        with with_nvml(fake_pynvml_for(FakeCuda(), [])):
            assert memory._nvml() is not None, "no signal either way yet"
            for value, refused in (("1", True), (" , ", False)):
                with mock.patch.dict(
                    os.environ, {"HIP_VISIBLE_DEVICES": value}, clear=False
                ):
                    assert (memory._nvml() is None) is refused, value


def test_the_fdinfo_reading_is_bounded_below_and_above(
    tmp_path, monkeypatch
) -> None:
    # fdinfo's KFD/compute figures are VM-walk-based and need a recent kernel,
    # so a reading materially below our own allocator pool is an under-report,
    # not a measurement — phantom headroom is the error the ledger cannot
    # absorb. A reading at or above the GPU's capacity is the mirror case.
    slack = memory.FDINFO_UNDERREPORT_SLACK_MB
    total = 8192  # `FakeCuda`'s GPU, i.e. what `gpu_total_mb` reports

    def base_method(vram_mb: int, pool: int = 4096, loads: int = 1) -> str:
        texts = [fdinfo("0000:03:00.0", 1, f"{vram_mb} MiB")]
        with rocm_host(tmp_path, monkeypatch, fdinfo_texts=texts) as cuda:
            for _ in range(loads):
                before = memory.begin_load()
                cuda.allocate(pool, reserved_mb=pool)
                report = memory.finish_load(before, object())
        return report["base_method"]

    for vram, pool, loads, expected, label in (
        (4096 - slack, 4096, 1, "fdinfo", "exactly at the floor"),
        (4096 - slack - 1, 4096, 1, "free_delta", "one MiB below it"),
        (4096 + 500, 4096, 1, "fdinfo", "above the pool is the ordinary case"),
        (total, 1024, 1, "free_delta", "exactly the GPU's capacity"),
        (total - 1, 1024, 1, "fdinfo", "one MiB under it is a real reading"),
        # The comparand is the ABSOLUTE post-load pool, not the window delta:
        # a windowed one would pass an under-report on every reload.
        (900, 3000, 2, "free_delta", "an under-report against the pool by then"),
    ):
        assert base_method(vram, pool, loads) == expected, label
    assert slack < memory.CONTEXT_ESTIMATE_MB, "a missed context is never jitter"


def test_the_amdgpu_tiers_never_initialize_cuda_and_never_raise(
    tmp_path, monkeypatch
) -> None:
    # Both tiers need this worker's GPU address, which comes from
    # `get_device_properties` — a call that would create the context.
    calls: list[str] = []

    class Tripwire(FakeCuda):
        def __init__(self):
            super().__init__(initialized=False)

        def get_device_properties(self, index):
            calls.append("get_device_properties")
            raise AssertionError("this would initialize CUDA")

        mem_get_info = get_device_properties

    pci = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
    texts = [fdinfo("0000:03:00.0", 1, "1536 MiB")]
    hostile = tmp_path / "hostile"  # a GPU "directory" that is really a file
    hostile.mkdir()
    Path(memory._pci_device_dir(str(hostile), "0000:03:00.0")).write_text("x")
    with rocm_host(tmp_path, monkeypatch, pci=pci, fdinfo_texts=texts, cuda=Tripwire()):
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory.fdinfo_own_vram_mb() is None
        assert memory.free_total_mb() == (None, None, None)
        assert memory.device_memory_sample() is None
    assert calls == [], calls
    with rocm_host(tmp_path, monkeypatch, pci=str(hostile)):
        assert memory.amdgpu_free_total_mb() == (None, None)


def test_the_gpu_address_is_re_resolved_until_it_is_known(
    tmp_path, monkeypatch
) -> None:
    # The FIRST reading of a worker's life is taken in `begin_load`, before the
    # impl has touched torch.
    pci = pci_root(tmp_path, {"0000:03:00.0": (24_576 * MIB, 1024 * MIB)})
    cuda = FakeCuda(initialized=False)
    with rocm_host(tmp_path, monkeypatch, pci=pci, cuda=cuda):
        assert memory.amdgpu_free_total_mb() == (None, None)
        assert memory._bdf_state["bdf"] is None, "a failure is not remembered"
        cuda.initialized = True
        assert memory.amdgpu_free_total_mb() == (23_552, 24_576)
        assert memory._bdf_state["bdf"] == "0000:03:00.0", "a success is"
    # The colon-to-dash swap is a Windows FIXTURE affordance only (a colon in a
    # path component opens an NTFS alternate data stream).
    monkeypatch.setattr(os, "name", "posix")
    assert memory._pci_device_dir("/sys", "0000:03:00.0").endswith("0000:03:00.0")
    monkeypatch.setattr(os, "name", "nt")
    assert memory._pci_device_dir("C:/f", "0000:03:00.0").endswith("0000-03-00.0")


# --- MPS (Apple Silicon): a unified-memory device ---


class FakeMpsAllocator:
    """Just enough of `torch.mps`, deliberately *without* peak/reset APIs
    because torch.mps has none."""

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
    # The unified reading: the pool from Metal, the free figure from the OS's
    # RAM statistics clamped by the recommended-max.
    with mps_host(available_mb=40 * 1024) as mps:
        mps.allocate(1024, driver_mb=1200)
        assert memory.device_memory_sample() == {
            "free_mb": 40 * 1024,
            "total_mb": 96 * 1024,
            "free_source": "mps",
            "reserved_mb": 1200,
            "allocated_mb": 1024,
        }
    for available, free in ((120 * 1024, 96 * 1024), (3 * 1024, 3 * 1024)):
        with mps_host(available_mb=available):
            assert memory.free_total_mb() == (free, 96 * 1024, "mps")


def test_mps_base_is_the_driver_allocation_at_load_end() -> None:
    # `driver_allocated_memory()` is per-process by construction (each process
    # owns its Metal heap), and it is also the only pool a trim can release.
    with mps_host(available_mb=40 * 1024) as mps:
        before = memory.begin_load()
        mps.allocate(2048, driver_mb=2560)
        report = memory.finish_load(before, object())
        assert memory.empty_cache() is True
        assert mps.empty_cache_calls == 1
        assert memory.pool_stats_mb() == (2048, 2048), "only the slack went back"
    assert (report["base_mb"], report["base_method"]) == (2560, "mps")
    assert report["reserved_at_load_mb"] == 2560
    assert report["gpu_total_mb"] == 96 * 1024, "the authoritative total (DP-4)"
    assert report["memory"]["free_source"] == "mps"
    assert "gpu_uuid" not in report, "Apple Silicon has one device and no UUID"
    assert "gpu_bdf" not in report, "and no PCI address"


def test_an_mps_batch_measurement_reports_the_pool_as_its_peak() -> None:
    # Torch.mps has no peak counters.
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


def test_the_mps_tier_survives_a_torch_without_it() -> None:
    # Every reader is getattr-guarded.
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
            assert memory.gpu_total_mb() is None
            assert memory.finish_load(memory.begin_load(), object()) == {
                "torch_version": "2.7.1"
            }


# --- CPU-only hosts: host RAM as the memory currency ---


class FakeRam:
    """The machine's RAM and this process's share of it. `peak` is monotone
    and has no reset, which is what the OS high-water mark is."""

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
    """A worker priced against system RAM. `pinned` writes the spawner's
    `INFERIO_DEVICE=cpu`, which is the whole of the signal."""
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
    # The degenerate unified reading.
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
    # A worker with no accelerator facts is *not* enough.
    with cpu_host(pinned=False):
        assert memory._ram_currency() is False
        assert memory.free_total_mb() == (None, None, None)
        assert memory.device_memory_sample() is None
        assert memory.finish_load(memory.begin_load(), object()) == {}


def test_the_cpu_tier_is_gated_off_on_every_accelerator_host(fake_torch) -> None:
    # A CUDA worker's readings must be byte-identical to what they were before
    # this tier existed.
    assert memory._ram_currency() is False
    assert memory.free_total_mb() == (8000, 8192, "torch")

    # An MPS worker is on Metal, not on RAM, even though its free reading is
    # also derived from RAM statistics.
    with mps_host(available_mb=40 * 1024):
        assert memory._ram_currency() is False
        assert memory.free_total_mb()[2] == "mps"


def test_the_cpu_base_is_the_load_windows_rss_growth() -> None:
    # `base_method: "rss"`, a window delta and not growth since process start,
    # so a second load is charged only its own window even when an unload
    # really did hand pages back — while the pool baseline, the monotone
    # high-water, still carries them.
    with cpu_host() as ram:
        first = memory.begin_load()
        ram.grow(2048)
        report = memory.finish_load(first, object())
        assert (report["base_mb"], report["base_method"]) == (2048, "rss")
        assert report["reserved_at_load_mb"] == 2248, "the high-water at load end"
        assert report["gpu_total_mb"] == 64 * 1024, "physical RAM, the cross-check"
        assert report["gpu_name"] == "CPU (64 GB)"
        assert report["memory"]["free_source"] == "ram"
        assert "gpu_uuid" not in report and "gpu_bdf" not in report, report
        ram.release(1024)  # an unload that genuinely gave pages back
        second = memory.begin_load()
        ram.grow(512)
        report = memory.finish_load(second, object())
        assert report["base_mb"] == 512, "this load's own growth, and only it"
        assert report["reserved_at_load_mb"] == 2248, "the high-water has no reset"
        # Never invent a footprint: a wrapper holding nothing reports nothing.
        idle = memory.finish_load(memory.begin_load(), object())
        assert "base_mb" not in idle and "base_method" not in idle, idle


def test_a_cpu_batch_measurement_reports_the_high_water_as_its_peak() -> None:
    # The high-water is a *real* peak (the kernel records it as it happens),
    # and a smaller repeat sets no new one.
    with cpu_host() as ram:
        ram.grow(1000)
        state = memory.begin_batch()
        ram.grow(500)
        ram.release(300)
        g = memory.finish_batch(state, items=4)["measurements"][0]
        assert (g["reserved_before_mb"], g["peak_reserved_mb"]) == (1200, 1700)
        assert (g["allocated_before_mb"], g["peak_allocated_mb"]) == (1200, 1400)
        assert memory.empty_cache() is False, "no allocator pool to hand back"
        assert memory.pool_stats_mb() == (1700, 1400)
    with cpu_host() as ram:
        ram.grow(1000)  # a big batch already reached 1200 MiB…
        ram.release(500)
        state = memory.begin_batch()
        ram.grow(200)  # …so this smaller one sets no new high-water
        ram.release(200)
        warm = memory.finish_batch(state, items=4)["measurements"][0]
    assert warm["peak_reserved_mb"] == warm["reserved_before_mb"] == 1200
    assert warm["allocated_before_mb"] == 700, "the live residency did move"


def test_the_diagnostic_gpu_names_match_the_orchestrator_probes() -> None:
    # Both are byte-identical to the probe's own name for the same host: RAM
    # rounded *up* to a 4 GiB grid, and the Mac's chip plus `hw.memsize`.
    for total_mb, expected in (
        (64 * 1024 - 700, "CPU (64 GB)"),
        (65 * 1024, "CPU (68 GB)"),
        (1, "CPU (4 GB)"),
    ):
        with cpu_host(FakeRam(total_mb=total_mb, available_mb=1)):
            assert memory.ram_gpu_name() == expected
    with isolated():
        with mock.patch.object(memory, "_virtual_memory", return_value=None):
            assert memory.ram_gpu_name() is None
            assert memory.ram_free_total_mb() == (None, None)
    with mock.patch.object(memory, "_sysctl_string", return_value="Apple M3 Max"):
        with mock.patch.object(memory, "_sysctl_u64", return_value=128 * 1024 * MIB):
            assert memory.mps_gpu_name() == "Apple M3 Max (128 GB)"
        with mock.patch.object(memory, "_sysctl_u64", return_value=0):
            assert memory.mps_gpu_name() is None
    if sys.platform != "darwin":
        assert memory.mps_gpu_name() is None, "the sysctls answer nowhere else"


def test_the_process_high_water_is_read_in_the_right_unit() -> None:
    # Two readers, two units, and getting either wrong is a factor of 1024.
    assert memory.parse_vm_high_water("VmRSS:\t 100 kB\nVmHWM:\t   2048 kB\n") == (
        2048 * 1024
    )
    assert memory.parse_vm_high_water("VmHWM:\t 2048 MB\n") is None, (
        "an undocumented unit is not a reading"
    )

    # `resource` does not exist on Windows, so the module is injected rather
    # than patched.
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
    # The currency-salad guard: a process holding a live CUDA context must not
    # put allocator statistics or a GPU identity on a RAM-priced report.
    cuda = FakeCuda()
    cuda.reserved, cuda.allocated = 4096 * MIB, 3000 * MIB
    with cpu_host(torch_module=fake_torch_module(cuda)) as ram:
        assert memory._ram_currency() is True
        before = memory.begin_load()
        ram.grow(2048)
        report = memory.finish_load(before, object())
        assert memory.empty_cache() is False
    assert (report["base_method"], report["base_mb"]) == ("rss", 2048)
    assert report["gpu_total_mb"] == 64 * 1024, "RAM, not the card's VRAM"
    assert report["gpu_name"] == "CPU (64 GB)"
    assert "gpu_uuid" not in report and "gpu_bdf" not in report, report
    assert report["memory"] == {
        "free_mb": 40 * 1024 - 2048,
        "total_mb": 64 * 1024,
        "free_source": "ram",
        "reserved_mb": 2248,
        "allocated_mb": 2248,
    }, "no allocator statistics from a device this host is not priced against"
    assert cuda.empty_cache_calls == 0


def test_a_cpu_priced_mac_reports_ram_and_not_metal() -> None:
    # DP-3's one unaccelerated path.
    ram = FakeRam(total_mb=128 * 1024, available_mb=90 * 1024)
    with cpu_host(ram, torch_module=fake_mps_torch_module(FakeMpsAllocator())):
        assert memory._ram_currency() is True
        assert memory.free_total_mb() == (90 * 1024, 128 * 1024, "ram")
        assert memory.device_identity() == (None, "CPU (128 GB)")
        assert memory.gpu_total_mb() == 128 * 1024, "RAM, not recommended-max"
        ram.grow(1500)
        assert memory.pool_stats_mb() == (1700, 1700)
