"""End-to-end tests for the `inferio_worker` harness (protocol v2).

Each test spawns `python -m inferio_worker` as a real subprocess and speaks the
framed-msgpack protocol from docs/inferio-worker-protocol.md over its
stdin/stdout, as the orchestrator does. The state machine under test: handshake
(identity only) -> optional prewarm -> configure (instantiates) -> load ->
predict, with unload valid in every state.

The subprocess resolves the package via PYTHONPATH=python, since the root
conftest only patches sys.path for the test process.
"""

from __future__ import annotations

import io
import os
import struct
import subprocess
import sys
import threading
from pathlib import Path

import msgpack
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "python"
FIXTURE_DIR = Path(__file__).resolve().parent / "fixture_impls"

READ_TIMEOUT = 60.0


class WorkerProcess:
    """Drives one worker subprocess over the framed protocol."""

    def __init__(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC_DIR) + os.pathsep + env.get(
            "PYTHONPATH", ""
        )
        # Deterministic startup: no CUDA path probing, which would import
        # torch if the venv has it.
        env["NO_CUDNN"] = "true"
        env["INFERIO_WORKER"] = "1"
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "inferio_worker"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=str(REPO_ROOT),
        )
        self._stderr_lines: list[str] = []
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, daemon=True
        )
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            self._stderr_lines.append(
                line.decode("utf-8", errors="replace").rstrip()
            )

    @property
    def stderr_text(self) -> str:
        return "\n".join(self._stderr_lines)

    def send(self, message: dict) -> None:
        payload = msgpack.packb(message, use_bin_type=True)
        assert self.proc.stdin is not None
        self.proc.stdin.write(struct.pack("<I", len(payload)) + payload)
        self.proc.stdin.flush()

    def recv(self, timeout: float = READ_TIMEOUT) -> dict:
        """Read one response frame, failing (not hanging) on a dead worker."""
        result: list[dict] = []
        error: list[BaseException] = []

        def _read() -> None:
            try:
                header = self._read_exact(4)
                (length,) = struct.unpack("<I", header)
                payload = self._read_exact(length)
                result.append(msgpack.unpackb(payload, raw=False))
            except BaseException as e:  # surfaced in the main thread
                error.append(e)

        t = threading.Thread(target=_read, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            self.kill()
            pytest.fail(
                f"Timed out waiting for a frame. Worker stderr:\n"
                f"{self.stderr_text}"
            )
        if error:
            pytest.fail(
                f"Failed to read a frame: {error[0]!r}. Worker stderr:\n"
                f"{self.stderr_text}"
            )
        return result[0]

    def _read_exact(self, size: int) -> bytes:
        assert self.proc.stdout is not None
        buf = bytearray()
        while len(buf) < size:
            chunk = self.proc.stdout.read(size - len(buf))
            if not chunk:
                raise EOFError(
                    f"worker stdout closed after {len(buf)}/{size} bytes"
                )
            buf += chunk
        return bytes(buf)

    def wait(self, timeout: float = READ_TIMEOUT) -> int:
        code = self.proc.wait(timeout=timeout)
        self._stderr_thread.join(timeout=10)
        return code

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def __enter__(self) -> "WorkerProcess":
        return self

    def __exit__(self, *exc) -> None:
        self.kill()


@pytest.fixture()
def worker():
    w = WorkerProcess()
    try:
        yield w
    finally:
        w.kill()


def handshake_msg(
    req_id: int = 1,
    impl_class: str = "echo_test",
    protocol_version: int = 2,
) -> dict:
    # v2: identity only; inference_id and config move to `configure`.
    return {
        "type": "handshake",
        "id": req_id,
        "protocol_version": protocol_version,
        "impl_class": impl_class,
        "impl_dirs": [str(FIXTURE_DIR)],
    }


def configure_msg(
    req_id: int,
    config: dict | None = None,
    inference_id: str = "test/worker",
) -> dict:
    return {
        "type": "configure",
        "id": req_id,
        "inference_id": inference_id,
        "config": config or {},
    }


def bring_up(
    worker: WorkerProcess, impl_class: str = "echo_test", load: bool = True
) -> None:
    """handshake -> configure (-> load), asserting each replies ok. Request
    ids 1-3 are used, so a caller continues from 4."""
    worker.send(handshake_msg(req_id=1, impl_class=impl_class))
    assert worker.recv()["type"] == "ok"
    worker.send(configure_msg(req_id=2))
    assert worker.recv()["type"] == "ok"
    if load:
        worker.send({"type": "load", "id": 3})
        assert worker.recv()["type"] == "ok"


def test_full_lifecycle_happy_path(worker: WorkerProcess) -> None:
    """handshake (identity only) -> configure (instantiates) -> load ->
    predict, one output per input in order with bytes staying msgpack bin,
    -> unload and exit 0."""
    worker.send(handshake_msg(req_id=1))
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 1
    assert resp["protocol_version"] == 2

    worker.send(configure_msg(req_id=2))
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 2

    worker.send({"type": "load", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 3

    inputs = [
        {"data": {"text": "hello"}, "file": None},
        {"data": None, "file": b"\x00\x01\xfe\xff"},
    ]
    worker.send({"type": "predict", "id": 4, "inputs": inputs})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 4
    outputs = resp["outputs"]
    assert len(outputs) == 2
    assert outputs[0] == {"echo": {"text": "hello"}}, "a JSON-like map"
    assert isinstance(outputs[1], bytes), "bytes stay msgpack bin, not str"
    assert outputs[1] == b"echo:\x00\x01\xfe\xff"

    # A repeated load is a no-op: the impl's own load() guard makes it one.
    worker.send({"type": "load", "id": 5})
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "unload", "id": 6})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 6
    assert worker.wait() == 0


def test_a_failed_handshake_is_fatal() -> None:
    """The one error the worker does not survive: an error frame with a
    traceback, then a non-zero exit. Both an unknown `impl_class` and a
    version mismatch take it."""
    for label, message, expect in (
        ("unknown impl", handshake_msg(req_id=1, impl_class="does_not_exist"),
         "does_not_exist"),
        ("v1 handshake", handshake_msg(req_id=1, protocol_version=1), "version"),
    ):
        with WorkerProcess() as worker:
            worker.send(message)
            resp = worker.recv()
            assert resp["type"] == "error", label
            assert resp["id"] == 1, label
            assert expect in resp["message"], label
            assert isinstance(resp["traceback"], str), label
            assert worker.wait() != 0, label


def test_out_of_order_requests_are_errors_and_the_worker_survives(
    worker: WorkerProcess,
) -> None:
    """The state machine's per-request errors: predict or load before
    configure, predict before load, prewarm after configure, and a second
    configure. Each replies error and leaves the worker serviceable."""
    predict = {"type": "predict", "id": 2, "inputs": [{"data": "x", "file": None}]}
    for label, request, expect in (
        ("predict before configure", predict, "configure"),
        ("load before configure", {"type": "load", "id": 2}, "configure"),
    ):
        with WorkerProcess() as fresh:
            fresh.send(handshake_msg(req_id=1))
            assert fresh.recv()["type"] == "ok", label
            fresh.send(request)
            resp = fresh.recv()
            assert resp["type"] == "error", label
            assert resp["id"] == 2, label
            assert expect in resp["message"], label
            fresh.send(configure_msg(req_id=3))
            assert fresh.recv()["type"] == "ok", f"{label}: still serviceable"

    bring_up(worker, load=False)
    for request, expect in (
        ({"type": "predict", "id": 3, "inputs": predict["inputs"]}, "load"),
        ({"type": "prewarm", "id": 3}, "configure"),
    ):
        worker.send(request)
        resp = worker.recv()
        assert resp["type"] == "error", request["type"]
        assert resp["id"] == 3
        assert expect in resp["message"], request["type"]

    # Configure is allowed exactly once; the first instance stays intact.
    worker.send(configure_msg(req_id=4, inference_id="test/other"))
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 4
    assert "configure" in resp["message"] or "configured" in resp["message"]

    worker.send({"type": "load", "id": 5})
    assert worker.recv()["type"] == "ok"
    worker.send(
        {"type": "predict", "id": 6, "inputs": [{"data": 1, "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"echo": 1}]


def test_prewarm_runs_prepare_classmethod(worker: WorkerProcess) -> None:
    """prewarm on an impl WITH a prepare() classmethod calls it exactly once
    across repeats; the stderr marker proves it ran. An impl without one — the
    echo fixture — takes the same path as a plain no-op."""
    with WorkerProcess() as plain:
        plain.send(handshake_msg(req_id=1))
        assert plain.recv()["type"] == "ok"
        for req_id in (2, 3):
            plain.send({"type": "prewarm", "id": req_id})
            assert plain.recv()["type"] == "ok", req_id

    worker.send(handshake_msg(req_id=1, impl_class="prepare_test"))
    assert worker.recv()["type"] == "ok"
    # Idempotent: the second prewarm replies ok without re-running prepare.
    for req_id in (2, 3):
        worker.send({"type": "prewarm", "id": req_id})
        resp = worker.recv()
        assert resp["type"] == "ok"
        assert resp["id"] == req_id

    worker.send(configure_msg(req_id=4))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 5})
    assert worker.recv()["type"] == "ok"

    worker.send(
        {"type": "predict", "id": 6, "inputs": [{"data": 1, "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"prepared": True}]

    worker.send({"type": "unload", "id": 7})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
    assert (
        worker.stderr_text.count("prepare_test-prepare-marker") == 1
    ), worker.stderr_text

    # And without a prewarm, prepare() never runs: load must not depend on it.
    with WorkerProcess() as fresh:
        bring_up(fresh, impl_class="prepare_test")
        fresh.send(
            {"type": "predict", "id": 4, "inputs": [{"data": 1, "file": None}]}
        )
        resp = fresh.recv()
        assert resp["outputs"] == [{"prepared": False}]
        assert "prepare_test-prepare-marker" not in fresh.stderr_text


def test_prewarm_failure_is_per_request_and_nonfatal(
    worker: WorkerProcess,
) -> None:
    """A raising prepare() is a per-request error with a traceback; the
    worker stays fully usable, load simply pays the imports."""
    worker.send(handshake_msg(req_id=1, impl_class="prepare_fail_test"))
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "prewarm", "id": 2})
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 2
    assert "prepare exploded" in resp["message"]
    assert "RuntimeError" in resp["traceback"]

    worker.send(configure_msg(req_id=3))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 4})
    assert worker.recv()["type"] == "ok"
    worker.send(
        {"type": "predict", "id": 5, "inputs": [{"data": 1, "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"ok": True}]


def test_unload_while_parked_exits_cleanly(worker: WorkerProcess) -> None:
    """unload is valid in every state: a parked prewarmed worker with no
    instance replies ok and exits 0. An unknown request type in the same state
    is a per-request error and the worker keeps serving."""
    worker.send(handshake_msg(req_id=1, impl_class="prepare_test"))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "prewarm", "id": 2})
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "frobnicate", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 3
    assert "unsupported" in resp["message"]

    worker.send({"type": "unload", "id": 4})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 4
    assert worker.wait() == 0


def test_stdout_hygiene_survives_prints(worker: WorkerProcess) -> None:
    """An impl that print()s cannot corrupt the stream: fd 1 is redirected to
    stderr and sys.stdout rebound before impl code ever runs."""
    bring_up(worker, impl_class="printing_test")

    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": [{"data": 1, "file": None}, {"data": 2, "file": None}],
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"printed": True}, {"printed": True}]

    worker.send({"type": "unload", "id": 5})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
    # All three print() outputs went to stderr, not lost and not on stdout.
    assert "garbage on load stdout" in worker.stderr_text
    assert "garbage on predict stdout" in worker.stderr_text
    assert "garbage on unload stdout" in worker.stderr_text


def test_oom_error_frame_preserves_prefix(worker: WorkerProcess) -> None:
    """The batch-1 OOM error surfaces as an error frame whose message starts
    with the literal `OOM_BATCH1_PREFIX`, which is what the orchestrator's
    classification relies on."""
    bring_up(worker, impl_class="oom_test")

    worker.send(
        {"type": "predict", "id": 4, "inputs": [{"data": {}, "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "error"
    assert resp["id"] == 4
    assert resp["message"].startswith("INFERENCE_OOM_BATCH_SIZE_1:")

    worker.send({"type": "unload", "id": 5})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


def test_an_internally_subbatching_impl_reports_no_units(
    worker: WorkerProcess,
) -> None:
    """The fixture uses the real `run_with_oom_retry` at chunk size 1, so the
    GPU batch the allocator saw is one item however many the harness packed.
    The harness sees that through the helper's record and omits `units`: an
    unpriceable batch must never reach the cost fit (protocol doc, "Memory
    sensing")."""
    bring_up(worker, impl_class="subbatching_test")

    inputs = [{"data": index, "file": None} for index in range(4)]
    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": inputs,
            "grant": {
                "unit_budget": 4, "mb": 1024, "unit": "item",
                "aggregation": "count", "user_cap_items": None,
            },
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok", resp
    assert resp["outputs"] == [{"chunk": 1}] * 4, "one item at a time, in order"
    measurements = resp["measurements"]
    assert len(measurements) == 1, "the harness packed one batch of 4"
    assert measurements[0]["items"] == 4
    assert "units" not in measurements[0], measurements[0]
    assert measurements[0].get("oom") is None, "no halvings happened"

    worker.send({"type": "unload", "id": 5})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


def test_an_error_frame_carries_the_measurements_of_a_failed_window(
    worker: WorkerProcess,
) -> None:
    """Two batches: the first runs and is measured, the second raises a
    driver-shaped OOM. The window fails as a whole, but the error frame still
    carries both measurements, the failing one flagged `oom` and unpriced, and
    the message carries INFERENCE_OOM_WINDOW (protocol doc, "Memory sensing on
    `error` frames")."""
    bring_up(worker, impl_class="oom_second_batch_test")

    inputs = [{"data": index, "file": None} for index in range(4)]
    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": inputs,
            "grant": {
                "unit_budget": 2, "mb": 1024, "unit": "item",
                "aggregation": "count", "user_cap_items": None,
            },
        }
    )
    resp = worker.recv()
    assert resp["type"] == "error", resp
    assert resp["id"] == 4
    assert resp["message"].startswith("INFERENCE_OOM_WINDOW:"), resp["message"]
    measurements = resp["measurements"]
    assert len(measurements) == 2, measurements
    assert [m["items"] for m in measurements] == [2, 2]
    assert measurements[0].get("oom") is None, "the first batch ran fine"
    assert measurements[1]["oom"] is True, "the second is the negative sample"
    assert "units" not in measurements[1], "a failed batch is never priced"

    # The worker survives: the request failed, not the process.
    worker.send({"type": "ping", "id": 5})
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "unload", "id": 6})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


def test_an_impl_with_batching_off_ignores_the_grant(
    worker: WorkerProcess,
) -> None:
    """A falsy `enable_batching` means the impl decides its own batch shape,
    so the worker ignores the grant and takes the compatibility path: the whole
    window in one call, one measurement, and no `units` (protocol doc, "Memory
    grants")."""
    bring_up(worker, impl_class="nobatching_test")

    inputs = [{"data": index, "file": None} for index in range(4)]
    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": inputs,
            "grant": {
                "unit_budget": 1, "mb": 1024, "unit": "item",
                "aggregation": "count", "user_cap_items": None,
            },
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok", resp
    assert resp["outputs"] == [{"batch": 4}] * 4, (
        "the grant's unit budget of 1 was ignored: one predict call, whole window"
    )
    measurements = resp["measurements"]
    assert len(measurements) == 1, measurements
    assert measurements[0]["items"] == 4
    assert "units" not in measurements[0], (
        "the grantless path prices nothing"
    )

    worker.send({"type": "unload", "id": 5})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


MEMORY_SAMPLE_KEYS = (
    "free_mb", "total_mb", "free_source", "reserved_mb", "allocated_mb",
)


def test_load_memory_fields_are_optional(worker: WorkerProcess) -> None:
    """The memory-sensing fields on the load response are optional: the echo
    fixture never imports torch, so the reply is a plain ok and a consumer sees
    "unknown" rather than a wrong number — GPU identity and torch version
    included. Whatever is reported has the declared type."""
    bring_up(worker, load=False)

    worker.send({"type": "load", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 3
    for field in ("base_mb", "base_method", "reserved_at_load_mb", "dtype",
                  "gpu_uuid", "gpu_name", "torch_version"):
        assert resp.get(field) is None, (field, resp)
    sample = resp.get("memory")
    if sample is not None:  # only on a host with NVML available
        assert set(sample) == set(MEMORY_SAMPLE_KEYS)


@pytest.mark.parametrize(
    "tier,expected",
    [("one", 1_843_200), ("two", 11_289_600), ("none", None), ("floored", None)],
)
def test_load_reports_the_resolved_pixel_canvas(
    worker: WorkerProcess, tier: str, expected: int | None
) -> None:
    """The `load` ok response carries `canvas_pixels`, the per-item canvas
    this worker read off the loaded impl — the orchestrator's only way to learn
    one that lives in a downloaded processor config. Absent, never zero and
    never a guess, when nothing could be read or the reading fell below the
    floor."""
    worker.send(handshake_msg(req_id=1, impl_class="canvas_test"))
    assert worker.recv()["type"] == "ok"
    worker.send(configure_msg(req_id=2, config={"canvas_tier": tier}))
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "load", "id": 3})
    resp = worker.recv()
    assert resp["type"] == "ok", resp
    assert resp.get("canvas_pixels") == expected, resp

    worker.send({"type": "unload", "id": 4})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


def test_predict_reports_one_measurement_per_call(worker: WorkerProcess) -> None:
    """`measurements` is always reported — the input count and wall time need
    no torch — one entry per GPU batch, counting `items` and not `units`. The
    memory columns are None on a torch-less worker, which is the degradation
    the wire contract promises."""
    bring_up(worker)
    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": [{"data": 1, "file": None}, {"data": 2, "file": None}],
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"echo": 1}, {"echo": 2}]

    measurements = resp["measurements"]
    assert len(measurements) == 1, measurements
    measurement = measurements[0]
    assert measurement["items"] == 2
    assert isinstance(measurement["duration_ms"], float)
    assert measurement["duration_ms"] >= 0.0
    for key in ("reserved_before_mb", "peak_reserved_mb",
                "allocated_before_mb", "peak_allocated_mb"):
        assert measurement[key] is None, measurement

    # A second predict re-measures from scratch rather than accumulating.
    worker.send(
        {"type": "predict", "id": 5, "inputs": [{"data": 3, "file": None}]}
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["measurements"][0]["items"] == 1


def test_broken_module_does_not_prevent_discovery(
    worker: WorkerProcess,
) -> None:
    """broken_impl.py raises at import time and sorts first, so discovery hits
    it, warns and skips it; echo_test is still found. The immediate unload also
    covers "unload right after handshake" -> ok + exit 0."""
    worker.send(handshake_msg(req_id=1, impl_class="echo_test"))
    resp = worker.recv()
    assert resp["type"] == "ok"

    worker.send({"type": "unload", "id": 2})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
    assert "broken_impl" in worker.stderr_text


def test_trim_is_ok_in_every_state_and_leaves_the_worker_serving(
    worker: WorkerProcess,
) -> None:
    """`trim` is valid in every state and never an error path: a parked worker
    and a loaded torch-less one both have no pool to release, and "nothing to
    free" is a successful trim, which is what lets the orchestrator send one
    without knowing which residents are trimmable (protocol doc, "Reactive
    shrink and trim")."""
    worker.send(handshake_msg(req_id=1))
    assert worker.recv()["type"] == "ok"

    # Before configure: valid, and does not disturb the state machine.
    worker.send({"type": "trim", "id": 2})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 2

    worker.send(configure_msg(req_id=3))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 4})
    assert worker.recv()["type"] == "ok"

    worker.send({"type": "trim", "id": 5})
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["id"] == 5
    # No torch in the fixture -> no pool, no sample. Absent, never zero: a
    # fabricated 0 would claim knowledge the worker does not have.
    assert "memory" not in resp

    # Idempotent, and the stream is still in sync afterwards.
    worker.send({"type": "trim", "id": 6})
    assert worker.recv()["type"] == "ok"
    worker.send(
        {
            "type": "predict",
            "id": 7,
            "inputs": [{"data": {"text": "after trim"}, "file": None}],
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok"
    assert resp["outputs"] == [{"echo": {"text": "after trim"}}]

    worker.send({"type": "unload", "id": 8})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0


def test_a_trim_that_released_nothing_leaves_the_shrink_state_alone() -> None:
    """`note_trimmed()` runs only when `empty_cache()` actually released.

    It invalidates the throughput comparator and the reactive-shrink
    hysteresis, neither of which is stale on a worker that released nothing —
    and discarding the comparator anyway would let a stream of trims to an
    idle-but-torchless resident keep throwing away the WDDM signal.

    Driven in-process because the assertion is about module state no frame
    reports; the ambient torch is removed for the duration, since an earlier
    test module may have left a live CUDA context in this interpreter.
    """
    from unittest import mock

    from inferio_worker import __main__ as harness
    from inferio_worker import packing

    def frames(*messages: dict) -> io.BytesIO:
        buffer = io.BytesIO()
        for message in messages:
            payload = msgpack.packb(message, use_bin_type=True)
            buffer.write(struct.pack("<I", len(payload)) + payload)
        buffer.seek(0)
        return buffer

    packing._last_growth = (4096, 123.0)
    packing._under_grant_windows = 1
    try:
        proto_in = frames(
            handshake_msg(req_id=1),
            {"type": "trim", "id": 2},
            {"type": "unload", "id": 3},
        )
        proto_out = io.BytesIO()
        with mock.patch.dict(sys.modules):
            sys.modules.pop("torch", None)
            assert harness._serve(proto_in, proto_out) == 0
        assert packing._last_growth == (4096, 123.0), (
            "a trim that freed nothing must not retire the comparator"
        )
        assert packing._under_grant_windows == 1, (
            "nor reset the hysteresis that is counting towards a real release"
        )
    finally:
        packing.note_trimmed()


def test_the_batch_memory_frames_capability_is_read_off_the_handshake() -> None:
    """`batch_memory_frames` is announced, not agreed: present-and-true means
    the orchestrator reads mid-request `memory` frames, and every other answer
    — absent (an orchestrator predating them), null, or a stray truthy value —
    means it does not and the frames are never written."""
    from inferio_worker import __main__ as worker_main
    from inferio_worker import protocol

    def negotiated(**extra) -> tuple[bool, bool]:
        request = io.BytesIO()
        protocol.write_frame(request, {**handshake_msg(req_id=1), **extra})
        request.seek(0)
        reply = io.BytesIO()
        impl_cls, wanted = worker_main._handshake(request, reply)
        return impl_cls is not None, wanted

    assert negotiated() == (True, False), "absent: the pre-frames orchestrator"
    assert negotiated(batch_memory_frames=True) == (True, True)
    for hostile in (None, False, 1, "true", {}):
        assert negotiated(batch_memory_frames=hostile) == (True, False), hostile


def test_a_worker_that_can_measure_nothing_answers_the_capability_in_silence(
    worker: WorkerProcess,
) -> None:
    """The old-worker/new-host and no-torch directions in one exchange: the
    extra handshake key is an unknown key to anything that does not want it,
    and a granted window on a host with no accelerator writes exactly one
    frame for the request — the `ok`."""
    worker.send({**handshake_msg(req_id=1), "batch_memory_frames": True})
    assert worker.recv()["type"] == "ok"
    worker.send(configure_msg(req_id=2))
    assert worker.recv()["type"] == "ok"
    worker.send({"type": "load", "id": 3})
    assert worker.recv()["type"] == "ok"

    worker.send(
        {
            "type": "predict",
            "id": 4,
            "inputs": [{"data": index, "file": None} for index in range(4)],
            "grant": {
                "unit_budget": 1, "mb": 1024, "unit": "item",
                "aggregation": "count", "user_cap_items": None,
            },
        }
    )
    resp = worker.recv()
    assert resp["type"] == "ok", resp
    assert resp["id"] == 4
    assert len(resp["measurements"]) == 4, "four batches, and still no frames"

    # The next frame on the stream is the next reply, not a straggler.
    worker.send({"type": "ping", "id": 5})
    assert worker.recv() == {"type": "ok", "id": 5}
    worker.send({"type": "unload", "id": 6})
    assert worker.recv()["type"] == "ok"
    assert worker.wait() == 0
