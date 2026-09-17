#!/usr/bin/env python3
"""oracle_calibrate.py - the mandatory instrument calibration of protocol §2.

Before any scenario, the oracle must be shown to see a *known* allocation, or
nothing downstream is trustworthy and the run stops. This runs that check end
to end: it starts `vramrec.py`, then `hog.py` at each requested size in turn,
and compares what the oracle saw against what the hog says it actually held.

- **GPU**: the GPU `used` delta and the hog PID's NVML per-process delta, each
  minus the CUDA context the hog measured, must match the held amount within
  `--tolerance-mb`.
- **RAM**: the hog PID's **RSS** and the **`MemAvailable` recovery at the
  moment of release** must each match within `--ram-tolerance-mb`. The
  before-vs-during `MemAvailable` delta is *reported, never judged*: a
  multi-GB hog takes minutes to fill and everything else on the host moves
  meanwhile.

Usage
-----
    # GPU 0, at 10 GiB and 40 GiB (the Phase 2 check, once SGLang is down)
    oracle_calibrate.py --target gpu --device 0 --sizes 10240,40960

    # RAM
    oracle_calibrate.py --target ram --sizes 16384 --hold 60 --settle 20

Options are in `--help`. `--alloc-timeout S` bounds the allocation phase only:
the hold starts when the target is actually reached, so a generous budget
costs nothing on a fast host and a slow RAM hog still gets its full `--hold`.

Exit code is 1 if any size failed its tolerance, so it can gate a run.

Output (`<out>/oracle-calibration.json`)
---------------------------------------
    {"schema": "oraclecal/1", "target": "gpu"|"ram", "device", "started_at",
     "tolerance_mb", "verdict": "PASS"|"FAIL",
     "sizes": [{"requested_mb", "held_mb", "context_mb", "oom_attempts",
                "last_error", "alloc_rate_mb_per_s", "note",
                "verdict": "PASS"|"FAIL",
                "gpu_used_{before,during,delta}_mb",
                "gpu_used_payload_delta_mb",        # delta minus context_mb
                "pid_nvml_{before,during,delta}_mb",
                "pid_nvml_payload_delta_mb",
                "mem_available_{before,during,delta}_mb",
                "pid_rss_{before,during,delta}_mb",
                "mem_available_release_delta_mb",   # the RAM verdict
                "gpu_used_release_delta_mb"}]}
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent


def _held_mb(path: Path) -> float:
    """The last `held_mb` the hog wrote, or -1 if it has not written one yet."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            held = -1.0
            for line in handle:
                line = line.strip()
                if not line or '"held_mb"' not in line:
                    continue
                try:
                    held = float(json.loads(line).get("held_mb", held))
                except Exception:
                    continue
            return held
    except OSError:
        return -1.0


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    if not path.is_file():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def gpu_of(sample: Dict[str, Any], uuid: Optional[str],
             index: int) -> Optional[Dict[str, Any]]:
    for gpu in sample.get("gpus", []):
        if uuid is not None and gpu.get("uuid") == uuid:
            return gpu
        if uuid is None and gpu.get("index") == index:
            return gpu
    return None


def _release_delta(low: List[Dict[str, Any]], high: List[Dict[str, Any]],
                   pick) -> Optional[int]:  # noqa: ANN001
    """max(`high`) - min(`low`) of one metric, or None when either is empty."""
    low_values = [value for value in (pick(s) for s in low) if value is not None]
    high_values = [value for value in (pick(s) for s in high) if value is not None]
    if not low_values or not high_values:
        return None
    return max(high_values) - min(low_values)


def _rss(sample: Dict[str, Any], pid: int) -> Optional[int]:
    """RSS of one PID from `vramrec.py`'s filtered process list."""
    for proc in sample.get("procs", []):
        if proc.get("pid") == pid:
            return proc.get("rss_mb")
    return None


def pid_mb(gpu: Optional[Dict[str, Any]], pid: int) -> Optional[int]:
    if gpu is None:
        return None
    for proc in gpu.get("procs", []):
        if proc.get("pid") == pid:
            return proc.get("used_mb")
    return None


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Protocol §2 instrument calibration: can the oracle see a "
                    "known allocation?",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--target", choices=("gpu", "ram"), default="gpu")
    parser.add_argument("--device", type=int, default=0)
    parser.add_argument("--sizes", default="10240,40960",
                        help="comma-separated hold sizes in MiB")
    parser.add_argument("--hold", type=float, default=25.0)
    parser.add_argument("--settle", type=float, default=10.0)
    parser.add_argument("--tolerance-mb", type=int, default=64)
    parser.add_argument("--ram-tolerance-mb", type=int, default=512)
    parser.add_argument("--chunk-mb", type=int, default=128)
    parser.add_argument("--alloc-timeout", type=float, default=0.0,
                        help="extra seconds of hog lifetime for a slow "
                             "allocation to reach the target")
    parser.add_argument("--out", default=None)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--hog-port", type=int, default=None,
                        help="give each hog an HTTP control port, so a long "
                             "hold can be ended early with POST /stop")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    sizes = [int(value) for value in args.sizes.split(",") if value.strip()]
    out = Path(args.out or (HERE / "results" / "phase0" / f"oracle-{args.target}"))
    out.mkdir(parents=True, exist_ok=True)
    vram_path = out / "vramrec.jsonl"
    if vram_path.exists():
        vram_path.unlink()

    per_size = args.hold + (args.alloc_timeout or 0.0) + args.settle + 20.0
    total = args.settle + per_size * len(sizes) + 20.0
    recorder = subprocess.Popen(
        [args.python, str(HERE / "vramrec.py"), "--out", str(vram_path),
         "--interval", "0.25", "--duration", str(total),
         "--filter", r"hog\.py|inferio|panoptikon", "--quiet"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(args.settle)

    results: List[Dict[str, Any]] = []
    try:
        for size in sizes:
            hog_path = out / f"hog-{size}.jsonl"
            if hog_path.exists():
                hog_path.unlink()
            duration = args.hold + (args.alloc_timeout or 0.0)
            cmd = [args.python, str(HERE / "hog.py"), "--target", args.target,
                   "--chunk-mb", str(args.chunk_mb), "--tick", "0.5",
                   "--progress-every", "5", "--duration", str(duration),
                   "--out", str(hog_path), "--quiet"]
            if args.hog_port:
                # So a long hold can be cut short from outside:
                #   curl -X POST http://127.0.0.1:<port>/stop
                cmd += ["--port", str(args.hog_port)]
            if args.target == "gpu":
                cmd += ["--device", str(args.device)]
            cmd += ["hold", str(size)]
            print(f"[{size} MiB] running hog (budget {duration:.0f}s) ...",
                  file=sys.stderr)
            hog = subprocess.Popen(cmd)
            # `duration` is the hog's own safety net. The hold we want is
            # `--hold` seconds *after the target is reached*, so watch the
            # hog's state log and end it there, or a generous --alloc-timeout
            # is spent holding rather than allocating.
            reached_at: Optional[float] = None
            deadline = time.monotonic() + duration + 30.0
            while hog.poll() is None and time.monotonic() < deadline:
                time.sleep(0.5)
                if reached_at is None:
                    if _held_mb(hog_path) >= size:
                        reached_at = time.monotonic()
                elif time.monotonic() - reached_at >= args.hold:
                    hog.terminate()
                    break
            try:
                hog.wait(timeout=60)
            except subprocess.TimeoutExpired:
                hog.kill()
                hog.wait(timeout=30)
            time.sleep(args.settle)
            results.append({"requested_mb": size, "hog_path": str(hog_path)})
    finally:
        recorder.terminate()
        try:
            recorder.wait(timeout=15)
        except subprocess.TimeoutExpired:
            recorder.kill()

    samples = [row for row in read_jsonl(vram_path) if row.get("kind") == "sample"]
    header = next((row for row in read_jsonl(vram_path)
                   if row.get("kind") == "header"), {})
    uuid = None
    for gpu in header.get("gpus", []):
        if gpu.get("index") == args.device:
            uuid = gpu.get("uuid")

    overall = "PASS"
    for row in results:
        hog_rows = read_jsonl(Path(row.pop("hog_path")))
        hog_header = next((r for r in hog_rows if r.get("kind") == "header"), {})
        states = [r for r in hog_rows if r.get("kind") in ("state", "progress")]
        if not states:
            row.update({"verdict": "FAIL", "note": "hog produced no state records"})
            overall = "FAIL"
            continue
        pid = hog_header.get("pid")
        held = max(state.get("held_mb") or 0 for state in states)
        steady = [state for state in states if (state.get("held_mb") or 0) == held]
        t_start, t_end = steady[0]["t_wall"], steady[-1]["t_wall"]
        first = hog_rows[0].get("t_wall") or t_start
        final = next((r for r in hog_rows if r.get("kind") == "final"), None)
        t_release = (final or steady[-1])["t_wall"]
        before = [s for s in samples if s["t_wall"] < first - 0.5]
        during = [s for s in samples if t_start + 1.0 <= s["t_wall"] <= t_end]
        # The cleanest attribution is the *release*: the hog drops everything
        # in microseconds, so whatever the kernel hands back within a second
        # or two is the hog's. A baseline taken minutes before the hold folds
        # in every other process that grew meanwhile.
        after = [s for s in samples
                 if t_release + 0.3 <= s["t_wall"] <= t_release + args.settle]
        if not before or not during:
            row.update({"verdict": "FAIL", "held_mb": held,
                        "note": "no oracle samples bracketing the hold"})
            overall = "FAIL"
            continue
        b_gpu = gpu_of(before[-1], uuid, args.device)
        d_gpu = gpu_of(during[-1], uuid, args.device)
        b_used = b_gpu.get("used_mb") if b_gpu else None
        d_used = max((gpu_of(s, uuid, args.device) or {}).get("used_mb") or 0
                     for s in during) or None
        b_pid = pid_mb(b_gpu, pid)
        d_pid = max((pid_mb(gpu_of(s, uuid, args.device), pid) or 0)
                    for s in during) or None
        b_avail = (before[-1].get("mem") or {}).get("mem_available_mb")
        d_avail = min((s.get("mem") or {}).get("mem_available_mb") or 0
                      for s in during) or None
        # Second RAM oracle: the hog PID's own RSS, from /proc. MemAvailable
        # is a kernel estimate that moves with everything else on the host.
        b_rss = _rss(before[-1], pid)
        d_rss = max((_rss(s, pid) or 0) for s in during) or None
        alloc_seconds = max(1e-6, t_start - first)
        row.update({
            "held_mb": held,
            "context_mb": hog_header.get("context_mb"),
            "gpu_used_before_mb": b_used,
            "gpu_used_during_mb": d_used,
            "gpu_used_delta_mb": (None if b_used is None or d_used is None
                                    else d_used - b_used),
            "pid_nvml_before_mb": b_pid,
            "pid_nvml_during_mb": d_pid,
            "pid_nvml_delta_mb": (None if d_pid is None
                                  else d_pid - (b_pid or 0)),
            "mem_available_before_mb": b_avail,
            "mem_available_during_mb": d_avail,
            "mem_available_delta_mb": (None if b_avail is None or d_avail is None
                                       else d_avail - b_avail),
            "pid_rss_before_mb": b_rss,
            "pid_rss_during_mb": d_rss,
            "pid_rss_delta_mb": (None if d_rss is None else d_rss - (b_rss or 0)),
            "mem_available_release_delta_mb": _release_delta(
                during, after, lambda s: (s.get("mem") or {}).get("mem_available_mb")),
            "gpu_used_release_delta_mb": _release_delta(
                after, during,
                lambda s: (gpu_of(s, uuid, args.device) or {}).get("used_mb")),
            "oom_attempts": states[-1].get("oom", 0),
            "last_error": states[-1].get("last_error"),
            "alloc_rate_mb_per_s": round(held / alloc_seconds, 2),
        })
        # The baseline predates the hog process, so both GPU deltas also
        # contain its CUDA context (600-700 MiB on this driver). `hog.py`
        # measures it once and reports it in its header; subtracting it leaves
        # the payload, which is what must equal `held_mb`.
        context = row.get("context_mb") or 0
        row["gpu_used_payload_delta_mb"] = (
            None if row["gpu_used_delta_mb"] is None
            else row["gpu_used_delta_mb"] - context)
        row["pid_nvml_payload_delta_mb"] = (
            None if row["pid_nvml_delta_mb"] is None
            else row["pid_nvml_delta_mb"] - context)

        notes = []
        ok = True
        if held != row["requested_mb"]:
            notes.append(f"hog only reached {held} of {row['requested_mb']} MiB")
            ok = False
        if args.target == "gpu":
            for key, tolerance in (
                ("gpu_used_payload_delta_mb", args.tolerance_mb),
                ("pid_nvml_payload_delta_mb", args.tolerance_mb),
            ):
                value = row.get(key)
                if value is None:
                    notes.append(f"{key} unavailable")
                    ok = False
                elif abs(value - held) > tolerance:
                    notes.append(f"{key}={value} vs held {held} "
                                 f"(> {tolerance} MiB)")
                    ok = False
        else:
            release = row.get("mem_available_release_delta_mb")
            if release is None:
                notes.append("no post-release samples: raise --settle")
                ok = False
            elif abs(release - held) > args.ram_tolerance_mb:
                notes.append(f"MemAvailable recovered {release} MiB on release "
                             f"vs held {held} (> {args.ram_tolerance_mb} MiB)")
                ok = False
            rss = row.get("pid_rss_delta_mb")
            if rss is None:
                notes.append("the hog PID's RSS was not recorded")
            elif abs(rss - held) > args.ram_tolerance_mb:
                notes.append(f"hog RSS grew {rss} MiB vs held {held} "
                             f"(> {args.ram_tolerance_mb} MiB)")
                ok = False
            drift = row.get("mem_available_delta_mb")
            if drift is not None and abs(-drift - held) > args.ram_tolerance_mb:
                notes.append(f"(informational: MemAvailable against the "
                             f"pre-hog baseline moved {drift} MiB, i.e. "
                             f"{abs(drift) - held:+d} MiB of concurrent host "
                             f"activity during the ramp)")
        row["verdict"] = "PASS" if ok else "FAIL"
        row["note"] = "; ".join(notes) or "within tolerance"
        if not ok:
            overall = "FAIL"

    payload = {
        "schema": "oraclecal/1", "target": args.target,
        "device": args.device if args.target == "gpu" else None,
        "gpu_uuid": uuid,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tolerance_mb": args.tolerance_mb,
        "ram_tolerance_mb": args.ram_tolerance_mb,
        "sizes": results, "verdict": overall,
    }
    (out / "oracle-calibration.json").write_text(
        json.dumps(payload, indent=1) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(payload, indent=1))
    else:
        print(f"\ntarget={args.target} device={args.device} uuid={uuid}")
        print(f"{'REQ MiB':>8} {'HELD':>7} {'CTX':>5} {'GPU d':>8} "
              f"{'PID d':>7} {'REL d':>8} {'RSS d':>7} {'OOM':>4}  VERDICT  NOTE")
        for row in results:
            print(f"{row['requested_mb']:>8} {row.get('held_mb', 0):>7} "
                  f"{str(row.get('context_mb')):>5} "
                  f"{str(row.get('gpu_used_payload_delta_mb')):>8} "
                  f"{str(row.get('pid_nvml_payload_delta_mb')):>7} "
                  f"{str(row.get('mem_available_release_delta_mb') if args.target == 'ram' else row.get('gpu_used_release_delta_mb')):>8} "
                  f"{str(row.get('pid_rss_delta_mb')):>7} "
                  f"{row.get('oom_attempts', 0):>4}  "
                  f"{row.get('verdict', '?'):<7}  {row.get('note', '')}")
        print(f"\noverall: {overall}   -> {out / 'oracle-calibration.json'}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
