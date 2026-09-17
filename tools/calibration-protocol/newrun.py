#!/usr/bin/env python3
"""newrun.py - create a results directory and record the host facts.

Creates `results/<run-id>/<scenario>/` (the layout
`docs/batch-calibration-test-protocol.md` §3 fixes), drops a filled-in
`runlog.md` from the template beside this script, and writes
`<run-id>/host.json` the first time a run id is used, so every recording can
be traced back to a driver, a commit and a GPU inventory.

Usage
-----
    newrun.py --scenario S2 [--run-id 20260903-c1] [--config C1] \
        [--note "cold ramp, wd-vit, ramp corpus"] [--results DIR] [--json]

    newrun.py --run-id 20260903-c1 --host-only     # refresh host.json only
    newrun.py --latest                             # print the newest run id

It prints the absolute scenario directory on stdout:
`DIR=$(python3 newrun.py --scenario S2 --run-id "$RUN")`.

host.json
---------
    {"schema": "hostfacts/1", "run_id": str, "created_at": str,
     "hostname": str, "platform": {...}, "cpu_count": int,
     "mem_total_mb": int|null,
     "git": {"commit": str, "branch": str, "dirty": bool,
             "describe": str, "submodule_ui": str|null},
     "nvidia_smi": {"driver_version": str|null, "cuda_version": str|null,
                    "gpus": [{"index","uuid","name","total_mb","compute_cap",
                              "free_mb"}], "error": str|null},
     "python": {"executable": str, "version": str, "torch": str|null,
                "torch_cuda": str|null},
     "env": {"CUDA_VISIBLE_DEVICES": ..., ...},
     "ffmpeg": str|null}
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent
ENV_KEYS = (
    "CUDA_VISIBLE_DEVICES", "CUDA_DEVICE_ORDER", "HIP_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES", "GPU_DEVICE_ORDINAL", "PYTORCH_CUDA_ALLOC_CONF",
    "RUST_LOG", "INFERIO_WORKER_LOG_LEVEL", "PANOPTIKON_CONFIG_PATH",
    "HF_HOME", "LOGLEVEL",
)


def run(cmd: List[str], cwd: Optional[Path] = None) -> Optional[str]:
    try:
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                                timeout=20)
    except Exception:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def git_facts(repo: Path) -> Dict[str, Any]:
    return {
        "commit": run(["git", "rev-parse", "HEAD"], repo),
        "branch": run(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo),
        "describe": run(["git", "describe", "--tags", "--always", "--dirty"], repo),
        "dirty": bool(run(["git", "status", "--porcelain"], repo)),
        "submodule_ui": (run(["git", "rev-parse", "HEAD:ui"], repo)),
    }


def nvidia_facts() -> Dict[str, Any]:
    if shutil.which("nvidia-smi") is None:
        return {"driver_version": None, "cuda_version": None, "gpus": [],
                "error": "nvidia-smi not on PATH"}
    rows = run([
        "nvidia-smi",
        "--query-gpu=index,uuid,name,memory.total,memory.free,compute_cap,driver_version",
        "--format=csv,noheader,nounits",
    ])
    if rows is None:
        return {"driver_version": None, "cuda_version": None, "gpus": [],
                "error": "nvidia-smi query failed"}
    gpus = []
    driver = None
    for line in rows.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 7:
            continue
        driver = parts[6]
        gpus.append({
            "index": int(parts[0]), "uuid": parts[1], "name": parts[2],
            "total_mb": _int(parts[3]), "free_mb": _int(parts[4]),
            "compute_cap": parts[5],
        })
    banner = run(["nvidia-smi"]) or ""
    cuda_version = None
    for line in banner.splitlines():
        if "CUDA Version" in line:
            cuda_version = line.split("CUDA Version:")[-1].strip(" |")
            break
    return {"driver_version": driver, "cuda_version": cuda_version,
            "gpus": gpus, "error": None}


def _int(text: str) -> Optional[int]:
    try:
        return int(float(text))
    except ValueError:
        return None


def mem_total_mb() -> Optional[int]:
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return int(int(line.split()[1]) / 1024)
    except OSError:
        pass
    return None


def python_facts() -> Dict[str, Any]:
    torch_version = torch_cuda = None
    try:
        import torch  # noqa: PLC0415

        torch_version = torch.__version__
        torch_cuda = getattr(torch.version, "cuda", None)
    except Exception:
        pass
    return {"executable": sys.executable, "version": sys.version.split()[0],
            "torch": torch_version, "torch_cuda": torch_cuda}


def host_facts(run_id: str, repo: Path) -> Dict[str, Any]:
    return {
        "schema": "hostfacts/1",
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": {
            "system": platform.system(), "release": platform.release(),
            "machine": platform.machine(), "version": platform.version(),
        },
        "cpu_count": os.cpu_count(),
        "mem_total_mb": mem_total_mb(),
        "git": git_facts(repo),
        "nvidia_smi": nvidia_facts(),
        "python": python_facts(),
        "env": {key: os.environ.get(key) for key in ENV_KEYS},
        "ffmpeg": shutil.which("ffmpeg"),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create results/<run-id>/<scenario>/ and record host facts.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--scenario", help="scenario id, e.g. S2 or S4b")
    parser.add_argument("--run-id", default=None,
                        help="run id (default: UTC YYYYmmdd-HHMMSS)")
    parser.add_argument("--config", default=None,
                        help="configuration id from §3 (C0..C7)")
    parser.add_argument("--note", default=None, help="one-line description")
    parser.add_argument("--results", default=str(HERE / "results"),
                        help="results root")
    parser.add_argument("--repo", default=str(HERE.parents[1]))
    parser.add_argument("--host-only", action="store_true",
                        help="write host.json and exit")
    parser.add_argument("--latest", action="store_true",
                        help="print the most recent run id and exit")
    parser.add_argument("--force", action="store_true",
                        help="reuse a scenario directory that already exists")
    parser.add_argument("--json", action="store_true",
                        help="print the host facts instead of the directory")
    args = parser.parse_args(argv)

    results = Path(args.results).resolve()
    if args.latest:
        # `results/` also holds `corpus/` and `phase0/`, which are not runs.
        runs = sorted((path for path in results.glob("*")
                       if path.is_dir() and (path / "host.json").is_file()),
                      key=lambda path: path.stat().st_mtime)
        if not runs:
            print("", end="")
            return 1
        print(runs[-1].name)
        return 0

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = results / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    facts_path = run_dir / "host.json"
    if args.host_only or not facts_path.exists():
        facts = host_facts(run_id, Path(args.repo).resolve())
        facts_path.write_text(json.dumps(facts, indent=1) + "\n", encoding="utf-8")
    else:
        facts = json.loads(facts_path.read_text(encoding="utf-8"))

    if args.host_only:
        print(facts_path if not args.json else json.dumps(facts, indent=1))
        return 0
    if not args.scenario:
        parser.error("--scenario is required (or use --host-only / --latest)")

    scenario_dir = run_dir / args.scenario
    if scenario_dir.exists() and any(scenario_dir.iterdir()) and not args.force:
        raise SystemExit(
            f"newrun: {scenario_dir} already exists and is not empty (use --force)"
        )
    scenario_dir.mkdir(parents=True, exist_ok=True)

    template = HERE / "runlog.md"
    target = scenario_dir / "runlog.md"
    if template.is_file() and not target.exists():
        body = template.read_text(encoding="utf-8")
        gpus = facts.get("nvidia_smi", {}).get("gpus", [])
        body = (
            body.replace("<SCENARIO>", args.scenario)
            .replace("<RUN-ID>", run_id)
            .replace("<CONFIG>", args.config or "C1")
            .replace("<DATE>", facts.get("created_at", ""))
            .replace("<HOST>", facts.get("hostname", ""))
            .replace("<COMMIT>", str(facts.get("git", {}).get("commit")))
            .replace("<BRANCH>", str(facts.get("git", {}).get("branch")))
            .replace("<DRIVER>", str(facts.get("nvidia_smi", {}).get("driver_version")))
            .replace("<GPUS>", ", ".join(
                f"{gpu['index']}:{gpu['name']} {gpu['total_mb']} MiB" for gpu in gpus)
                or "none")
            .replace("<NOTE>", args.note or "")
        )
        target.write_text(body, encoding="utf-8")

    if args.json:
        print(json.dumps({"run_id": run_id, "scenario": args.scenario,
                          "dir": str(scenario_dir), "host": facts}, indent=1))
    else:
        print(scenario_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
