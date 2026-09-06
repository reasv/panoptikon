"""The macOS oracle's device total: the worker's recommended-max, not a seed.

`vramrec.py`'s `GPU-MPS` row is what `analyze.py::grant_safety` prices grants
against. On an M3 Max the ledger admits against **110 100 MiB** -- the
recommended-max the first worker report adopts (DP-4) -- while the row's old
fallback, 75 % of `hw.memsize`, answers 98 304. The 11 796 MiB gap failed the
oracle clause on seven legs of an idle machine, on grants that were nowhere
near the device (MPS pass, T2).

So the total is resolved best-first -- `/health`, then torch, then the wired
limit, then the seed -- and `gpu_total_source` says which. The `/health`
payload below is the one the M3 Max published, verbatim, including the trap it
sets: the `gpus` inventory row keeps the seed for the process's life while the
`vram` admission row carries the adopted figure (MPS pass, F6).

Run with the managed interpreter:

    python/.venv/bin/python -m pytest tools/calibration-protocol/tests -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

VRAMREC = Path(__file__).resolve().parents[1] / "vramrec.py"


def _load():
    spec = importlib.util.spec_from_file_location("_calib_vramrec_total",
                                                  VRAMREC)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


vramrec = _load()

# Captured from the M3 Max under test: results/mps/S1/health-end.json.
HEALTH = {
    "gpus": [{"index": 0, "uuid": "GPU-MPS", "name": "Apple M3 Max (128 GB)",
              "total_mb": 98304, "unified_ram_mb": 131072}],
    "vram": [{"gpu_uuid": "GPU-MPS", "gpu_name": "Apple M3 Max (128 GB)",
              "gpu_arch": "apple-m3", "total_mb": 110100,
              "external_mb": 8554, "external_known": True,
              "external_source": "mps", "limit_mb": 100690,
              "headroom_mb": 100690, "workers": []}],
}


def _oracle(*, wired_limit_mb=0, memsize_mb=131072, health_url=None,
            fetch=None, torch_mb=None, health_interval=5.0):
    """Every reading injected: no sysctl, no torch, no socket."""
    return vramrec.MpsOracle(
        memsize_mb=memsize_mb, wired_limit_mb=wired_limit_mb,
        chip="Apple M3 Max", health_url=health_url,
        health_interval=health_interval,
        fetch=(lambda _url: None) if fetch is None else fetch,
        torch_total_mb=lambda: torch_mb)


# --- which row of /health, and which key -----------------------------------


def test_the_admission_row_is_read_not_the_inventory_row():
    """The `gpus` row keeps the seed forever; reading it re-imports the bug."""
    assert vramrec.mps_total_from_health(HEALTH) == 110100


def test_a_payload_without_the_device_yields_none():
    assert vramrec.mps_total_from_health({"vram": [
        {"gpu_uuid": "GPU-0", "total_mb": 24564}]}) is None
    assert vramrec.mps_total_from_health({"vram": []}) is None
    assert vramrec.mps_total_from_health({}) is None
    assert vramrec.mps_total_from_health(None) is None


def test_a_zero_or_unparseable_total_is_not_adopted():
    """Never replace a real reading with 0: the row prices grants."""
    assert vramrec.mps_total_from_health(
        {"vram": [{"gpu_uuid": "GPU-MPS", "total_mb": 0}]}) is None
    assert vramrec.mps_total_from_health(
        {"vram": [{"gpu_uuid": "GPU-MPS", "total_mb": None}]}) is None


def test_the_url_may_be_a_base_or_the_endpoint():
    endpoint = "http://127.0.0.1:6342/api/inference/health"
    assert vramrec.health_url_for("http://127.0.0.1:6342") == endpoint
    assert vramrec.health_url_for("http://127.0.0.1:6342/") == endpoint
    assert vramrec.health_url_for(endpoint) == endpoint


def test_a_dead_gateway_is_not_an_error():
    """The recorder starts before the gateway; a refusal is the normal
    first answer and must never abort the recording."""
    assert vramrec.fetch_health("http://127.0.0.1:1/health",
                                timeout=0.25) is None


# --- the resolution order --------------------------------------------------


def test_health_wins_over_every_other_reading():
    oracle = _oracle(health_url="http://127.0.0.1:6342",
                     fetch=lambda _url: HEALTH, torch_mb=99999,
                     wired_limit_mb=120000)
    assert oracle.total_mb == 110100
    assert oracle.total_source == vramrec.MPS_TOTAL_SOURCES[0]
    # torch is never asked once /health has answered.
    assert oracle.torch_total_mb is None


def test_torch_answers_when_no_health_url_is_given():
    oracle = _oracle(torch_mb=110100)
    assert oracle.total_mb == 110100
    assert oracle.total_source == vramrec.MPS_TOTAL_SOURCES[1]


def test_the_wired_limit_is_below_torch_and_above_the_seed():
    oracle = _oracle(wired_limit_mb=120000, torch_mb=None)
    assert oracle.total_mb == 120000
    assert oracle.total_source == vramrec.MPS_TOTAL_SOURCES[2]


def test_the_seed_is_the_last_resort_and_says_so():
    oracle = _oracle()
    assert oracle.total_mb == 98304
    assert oracle.total_source == vramrec.MPS_TOTAL_SOURCES[3]
    assert "0.75" in oracle.total_source


def test_no_reading_at_all_leaves_the_total_null_never_zero():
    oracle = _oracle(memsize_mb=None)
    assert oracle.total_mb is None
    assert oracle.sample({"mem_available_mb": 40000})[0]["free_mb"] is None


# --- adoption while the recorder runs --------------------------------------


def test_the_total_is_adopted_when_the_gateway_comes_up():
    """The recorder is started first, so the first read always fails."""
    answers = [None, None, HEALTH]

    def fetch(_url):
        return answers.pop(0) if answers else HEALTH

    oracle = _oracle(health_url="http://127.0.0.1:6342", fetch=fetch,
                     health_interval=0.0)
    assert oracle.total_mb == 98304  # the seed, until the gateway answers
    oracle.sample({"mem_available_mb": 120000})
    row = oracle.sample({"mem_available_mb": 120000})[0]
    assert row["total_mb"] == 110100
    assert row["total_source"] == vramrec.MPS_TOTAL_SOURCES[0]
    assert oracle.meta[0]["total_mb"] == 110100
    # And the free reading follows the adopted total, not the seed.
    assert row["free_mb"] == 110100 and row["used_mb"] == 0


def test_dp4_adoption_moves_the_total_a_second_time():
    """The ledger publishes the seed until the first worker report."""
    seeded = {"vram": [{"gpu_uuid": "GPU-MPS", "total_mb": 98304}]}
    state = {"payload": seeded}
    oracle = _oracle(health_url="http://127.0.0.1:6342",
                     fetch=lambda _url: state["payload"], health_interval=0.0)
    assert oracle.total_mb == 98304
    state["payload"] = HEALTH
    assert oracle.sample({"mem_available_mb": 120000})[0]["total_mb"] == 110100


def test_a_gateway_restart_does_not_wipe_the_total():
    """`/health` refusing again mid-run keeps the last figure."""
    state = {"payload": HEALTH}
    oracle = _oracle(health_url="http://127.0.0.1:6342",
                     fetch=lambda _url: state["payload"], health_interval=0.0)
    state["payload"] = None
    row = oracle.sample({"mem_available_mb": 120000})[0]
    assert row["total_mb"] == 110100
    assert row["total_source"] == vramrec.MPS_TOTAL_SOURCES[0]


def test_health_is_not_polled_on_every_sample():
    calls = {"n": 0}

    def fetch(_url):
        calls["n"] += 1
        return HEALTH

    oracle = _oracle(health_url="http://127.0.0.1:6342", fetch=fetch,
                     health_interval=3600.0)
    for _ in range(10):
        oracle.sample({"mem_available_mb": 120000})
    assert calls["n"] == 1  # the one at construction


def test_no_health_url_means_no_socket_is_ever_touched():
    calls = {"n": 0}

    def fetch(_url):
        calls["n"] += 1
        return HEALTH

    oracle = _oracle(fetch=fetch, torch_mb=110100)
    for _ in range(5):
        oracle.sample({"mem_available_mb": 120000})
    assert calls["n"] == 0
    assert oracle.health_url is None
