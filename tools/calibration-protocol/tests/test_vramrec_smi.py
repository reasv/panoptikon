"""The Windows oracle's parser: `nvidia-smi --query-compute-apps` CSV.

`vramrec.py`'s per-process figures come from NVML, which answers **N/A on
Windows' WDDM** — the display driver owns the allocations there and NVML
cannot attribute them per process. `nvidia-smi --query-compute-apps=pid,
used_memory` does answer on that platform, so it is the attribution a Windows
pass has (`docs/batch-calibration-test-protocol.md` §9, run1 report §8).

The parser is the only part of that path a Linux host can exercise, and it is
the part that can be wrong quietly: a mis-parsed unit suffix would put a
1024× error into `base_accuracy` and `footprint_agreement` on the one platform
that has no second reader to contradict it. The samples below are captured
`nvidia-smi` output, not invented shapes.

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

VRAMREC = Path(__file__).resolve().parents[1] / "vramrec.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_vramrec", VRAMREC)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vramrec = _load()

# Captured on `gcs` (driver 590.48.01, two RTX PRO 6000 Blackwell) while
# SGLang held both boards, with the exact command the platform notes name.
CAPTURED_CSV = """pid, used_gpu_memory [MiB]
80613, 92908 MiB
80614, 92908 MiB
"""

# The same query with `--format=csv,noheader,nounits`, which a caller reaching
# for machine-readable output will use instead.
CAPTURED_NOUNITS = """80613, 92908
80614, 92908
"""

# What the driver prints with nothing on the GPU.
CAPTURED_EMPTY = "pid, used_gpu_memory [MiB]\n"


def test_captured_csv_with_header_and_units():
    assert vramrec.parse_compute_apps(CAPTURED_CSV) == {80613: 92908,
                                                        80614: 92908}


def test_same_reading_without_header_or_units():
    assert (vramrec.parse_compute_apps(CAPTURED_NOUNITS)
            == vramrec.parse_compute_apps(CAPTURED_CSV))


def test_empty_and_no_processes_forms_parse_to_nothing():
    assert vramrec.parse_compute_apps(CAPTURED_EMPTY) == {}
    assert vramrec.parse_compute_apps("") == {}
    assert vramrec.parse_compute_apps("No running processes found\n") == {}


@pytest.mark.parametrize("cell", ["[N/A]", "[Not Supported]", "N/A",
                                  "[Insufficient Permissions]"])
def test_an_unanswerable_cell_is_none_and_never_zero(cell: str):
    """The same rule the NVML path follows: N/A is `null`, not 0.

    A 0 would be read as "this process holds nothing", which on a GPU it is
    demonstrably running on is a false statement, and `analyze.py` would sum
    it into `footprint_agreement` as a real reading.
    """
    parsed = vramrec.parse_compute_apps(f"pid, used_gpu_memory [MiB]\n7, {cell}\n")
    assert parsed == {7: None}


def test_the_unit_is_checked_not_stripped():
    """The parser never rescales, so a cell in any unit but MiB is refused
    outright rather than read as a number 1024x too small."""
    assert vramrec.parse_compute_apps("pid, used\n3, 512 MiB\n") == {3: 512}
    assert vramrec.parse_compute_apps("pid, used\n3, 512.0 MiB\n") == {3: 512}
    assert vramrec.parse_compute_apps("pid, used\n3, 512\n") == {3: 512}
    assert vramrec.parse_compute_apps("pid, used\n3, 2 GiB\n") == {3: None}
    # `MB` is a different unit from `MiB` by 4.9%, and a driver that ever
    # printed it would be read 4.9% low with nothing to contradict it.
    assert vramrec.parse_compute_apps("pid, used\n3, 512 MB\n") == {3: None}


def test_nvml_is_blind_recognises_the_wddm_shape():
    """Listed processes, none of them priced -- and nothing else."""
    assert vramrec.nvml_is_blind([{"pid": 1, "used_mb": None},
                                  {"pid": 2, "used_mb": None}])
    assert not vramrec.nvml_is_blind([])
    assert not vramrec.nvml_is_blind([{"pid": 1, "used_mb": None},
                                      {"pid": 2, "used_mb": 512}])
    assert not vramrec.nvml_is_blind([{"pid": 1, "used_mb": 0}])


def test_an_empty_list_earns_a_query_only_where_nvml_is_known_to_hide():
    """An idle GPU lists nothing on every platform, so the empty shape is not
    on its own a reason to pay for a subprocess four times a second."""
    assert vramrec.should_consult_smi([]) is vramrec.IS_WINDOWS
    assert vramrec.should_consult_smi([], host_proven_blind=True)
    assert vramrec.should_consult_smi([{"pid": 1, "used_mb": None}])
    assert not vramrec.should_consult_smi([{"pid": 1, "used_mb": 512}])


class _FakeSmi:
    """A `SmiOracle` stand-in that answers without running a subprocess."""

    def __init__(self, rows):
        self.rows = rows
        self.error = None
        self.calls = 0
        self.proved_nvml_blind = False

    def read(self, _uuid):
        self.calls += 1
        return dict(self.rows), 0.0


class _FakeNvml:
    def __init__(self, rows):
        self._rows = rows

    def sample(self):
        return [dict(row, _procs=list(row["_procs"])) for row in self._rows]


def _sample(nvml_rows, smi_rows, always=False):
    smi = _FakeSmi(smi_rows)
    cache = vramrec.ProcCache((), False)
    out = vramrec.build_sample(0, _FakeNvml(nvml_rows), cache, None, 0.0,
                               smi, always)
    return out["gpus"][0], smi


def _gpu(procs):
    return {"index": 0, "uuid": "GPU-abc", "name": "x", "total_mb": 100,
            "used_mb": 50, "free_mb": 50, "error": None, "_procs": procs}


def test_a_healthy_nvml_gpu_never_calls_nvidia_smi():
    row, smi = _sample([_gpu([{"pid": 9, "used_mb": 512, "type": "compute"}])],
                       {9: 999})
    assert row["oracle_source"] == "nvml"
    assert row["oracle_age_ms"] is None
    assert smi.calls == 0
    assert row["procs"][0]["used_mb"] == 512


def test_a_blind_gpu_is_priced_from_nvidia_smi_and_says_so():
    row, smi = _sample([_gpu([{"pid": 9, "used_mb": None, "type": "compute"}])],
                       {9: 4096})
    assert smi.calls == 1
    assert row["oracle_source"] == "nvidia-smi"
    assert row["oracle_age_ms"] == 0.0
    assert row["procs"][0]["used_mb"] == 4096


def test_an_empty_nvml_list_on_a_posix_host_never_calls_nvidia_smi():
    """The idle-GPU state of S2 and S3 before the model loads: NVML lists
    nothing, and on POSIX that is an idle board rather than a hidden answer."""
    if vramrec.IS_WINDOWS:  # pragma: no cover - the rule inverts there
        pytest.skip("on Windows an empty list is a hidden answer")
    row, smi = _sample([_gpu([])], {77: 2048})
    assert smi.calls == 0
    assert row["oracle_source"] == "none"
    assert row["procs"] == []


def test_a_pid_only_nvidia_smi_sees_is_added():
    """The other WDDM shape, once this host has proved NVML blind."""
    smi = _FakeSmi({77: 2048})
    smi.proved_nvml_blind = True
    cache = vramrec.ProcCache((), False)
    out = vramrec.build_sample(0, _FakeNvml([_gpu([])]), cache, None, 0.0,
                               smi, False)
    row = out["gpus"][0]
    assert row["oracle_source"] == "nvidia-smi"
    assert [(entry["pid"], entry["used_mb"]) for entry in row["procs"]] \
        == [(77, 2048)]


def test_one_blind_gpu_proves_nvml_blind_for_the_idle_ones():
    """What licenses the empty-list query: a fallback that already answered
    where NVML listed processes and priced none."""
    smi = _FakeSmi({9: 4096})
    cache = vramrec.ProcCache((), False)
    vramrec.build_sample(
        0, _FakeNvml([_gpu([{"pid": 9, "used_mb": None, "type": "compute"}])]),
        cache, None, 0.0, smi, False)
    assert smi.proved_nvml_blind


def test_a_partly_priced_gpu_names_both_instruments():
    row, _ = _sample(
        [_gpu([{"pid": 1, "used_mb": None, "type": "compute"},
               {"pid": 2, "used_mb": 700, "type": "compute"}])],
        {1: 300},
        always=True,
    )
    assert row["oracle_source"] == "nvml+nvidia-smi"
    assert {entry["pid"]: entry["used_mb"] for entry in row["procs"]} \
        == {1: 300, 2: 700}


def test_nvml_wins_a_pid_both_instruments_price():
    """The precedence the header states: the fallback fills nulls, it does not
    correct NVML, so a disagreement leaves NVML's figure standing."""
    row, _ = _sample(
        [_gpu([{"pid": 1, "used_mb": 700, "type": "compute"},
               {"pid": 2, "used_mb": None, "type": "compute"}])],
        {1: 300, 2: 512},
        always=True,
    )
    assert {entry["pid"]: entry["used_mb"] for entry in row["procs"]} \
        == {1: 700, 2: 512}
    assert row["oracle_source"] == "nvml+nvidia-smi"


def test_a_partly_priced_gpu_is_not_claimed_as_nvml():
    """`"nvml"` promises every listed process carries an NVML figure, so the
    partly-priced GPU `--smi auto` leaves alone must not read as one."""
    row, smi = _sample(
        [_gpu([{"pid": 1, "used_mb": None, "type": "compute"},
               {"pid": 2, "used_mb": 700, "type": "compute"}])],
        {1: 300},
    )
    assert smi.calls == 0
    assert row["oracle_source"] == "none"


def test_no_instrument_answers_and_the_sample_says_none():
    row, _ = _sample([_gpu([{"pid": 9, "used_mb": None, "type": "compute"}])],
                     {})
    assert row["oracle_source"] == "none"
    assert row["procs"][0]["used_mb"] is None


def test_a_wddm_null_answer_is_not_recorded_as_a_priced_gpu():
    """The measured Windows shape: the fallback answers `[N/A]` for the pid
    NVML could not price, so nothing was priced and the label must say so."""
    row, smi = _sample([_gpu([{"pid": 9, "used_mb": None, "type": "compute"}])],
                       {9: None})
    assert smi.calls == 1
    assert row["oracle_source"] == "none"
    assert row["oracle_age_ms"] == 0.0  # the query still ran, and when
    assert row["procs"][0]["used_mb"] is None
    assert not smi.proved_nvml_blind


def test_a_pid_only_nvidia_smi_sees_but_cannot_price_is_added_unpriced():
    smi = _FakeSmi({77: None})
    smi.proved_nvml_blind = True
    cache = vramrec.ProcCache((), False)
    out = vramrec.build_sample(0, _FakeNvml([_gpu([])]), cache, None, 0.0,
                               smi, False)
    row = out["gpus"][0]
    assert row["oracle_source"] == "none"
    assert [(entry["pid"], entry["used_mb"]) for entry in row["procs"]] \
        == [(77, None)]
