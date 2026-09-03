#!/usr/bin/env python3
"""ceiling_probe.py - ground-truth base / slope / OOM boundary, outside the ledger.

Loads a shipped `inferio` impl the same way the worker does -- same registry
entry, same impl class, same device pin -- but with no orchestrator, no packer
and no grant, then measures what a batch of N units actually costs
(`docs/batch-calibration-test-protocol.md` §2). Its `base` and
`slope_mb_per_unit` are the numbers the ledger's fit should converge to, and
its OOM boundary is the line the ledger's grants must stay under.

Usage
-----
    # resolve everything and print the plan, touching no GPU
    ceiling_probe.py --model tags/wd-vit-tagger-v3 \
        --corpus results/corpus/ramp/manifest.json --dry-run

    # the real probe on board 0, batches 1,2,4,...,64
    ceiling_probe.py --model tags/wd-vit-tagger-v3 \
        --corpus results/corpus/ramp/manifest.json \
        --device 0 --max-batch 64 --repeats 2 \
        --out results/<run>/<scenario>/probe-wd-vit.json

    # with hog.py holding `leave-free 12288` on the same board, find the
    # largest batch that still runs at 12 GiB free
    ceiling_probe.py --model tags/wd-vit-tagger-v3 --corpus ... \
        --device 0 --bisect-oom --bisect-max 4096

Key options
-----------
    --model ID          inference_id, e.g. `tags/wd-vit-tagger-v3`  (required)
    --corpus PATH       corpus.py manifest.json (or its directory)
    --group / --kind    restrict which corpus items are used
    --device N          NVML board index; translated to
                        `CUDA_VISIBLE_DEVICES=GPU-<uuid>` exactly as the
                        orchestrator pins a worker (`gpu.rs: resolve_pin`)
    --batches 1,2,4,8   explicit batch sizes (default: powers of two up to
                        --max-batch)
    --max-batch N       largest power-of-two batch to measure     (default 64)
    --repeats N         measurements per batch size               (default 1)
    --warmup N          untimed batches of size 1 before measuring (default 1)
    --bisect-oom        binary-search the largest batch that does not OOM
    --bisect-max N      upper bound for the search               (default 1024)
    --bisect-start N    first size the doubling phase probes; skips the cheap
                        sizes when the boundary is known to be far above them
    --bisect-budget S   stop refining after S seconds of bisect probes and
                        report the bracket reached (0 = no limit)
    --repo PATH         repo root (default: two levels above this file)
    --impl-dir PATH     extra impl dir (repeatable)
    --registry PATH     extra registry TOML (repeatable)
    --out PATH          JSON result file (default: stdout)
    --dry-run           resolve and plan only

Measurement
-----------
Per batch: `torch.cuda.reset_peak_memory_stats()`, then `instance.predict(...)`,
then `max_memory_reserved` / `max_memory_allocated` / `memory_reserved` and the
NVML per-process figure for this PID. `delta_mb = peak_reserved_mb -
reserved_at_load_mb` is exactly the ledger's `FitSample.delta_mb`
(`ledger.rs: ingest_locked`), and the slope is fitted with the same Theil-Sen
estimator (`ledger.rs: robust_fit`: median pairwise slope, median intercept,
median absolute residual, >= 3 samples, slope > 0), so the two numbers are
directly comparable.

Units are priced with the worker's own `packing.price_inputs` /
`packing.batch_units`, so `units` means the same thing on both sides.

Output schema (JSON)
--------------------
    {"schema": "ceiling_probe/1", "model": str, "impl_class": str,
     "config": {...}, "cost": {"unit": str, "aggregation": str,
                               "seed_units": int|null, "epoch": int|null},
     "device": {"index": int, "uuid": str, "name": str, "total_mb": int,
                "cuda_visible_devices": str},
     "torch": str, "dtype": str|null, "python": str,
     "load": {"seconds": float, "base_nvml_mb": int|null,
              "base_free_delta_mb": int|null, "reserved_at_load_mb": int,
              "allocated_at_load_mb": int, "free_before_mb": int,
              "free_after_mb": int},
     "batches": [{"batch": int, "repeat": int, "units": int,
                  "items": int, "ok": bool, "oom": bool,
                  "absorbed_halvings": int, "duration_ms": float,
                  "peak_reserved_mb": int, "peak_allocated_mb": int,
                  "reserved_before_mb": int, "reserved_after_mb": int,
                  "nvml_own_mb": int|null, "board_free_mb": int|null,
                  "delta_mb": int, "error": str|null}],
     "fit": {"slope_mb_per_unit": float, "intercept_mb": float,
             "residual_mb": float, "samples": int} | null,
     "bisect": {"free_mb_at_start": int|null,
                "reserved_at_bisect_start_mb": int|null,
                "largest_ok_units": int|null,
                "largest_ok_items": int|null, "first_oom_items": int|null,
                "low_items": int, "high_items": int, "stopped_early": bool,
                "trace": [{"items": int, "ok": bool, "units": int,
                           "oom": bool, "absorbed_halvings": int,
                           "error": str|null}]} | null}

`free_mb_at_start` is the board's free memory when the search begins, which is
*after* the `--batches` sweep: the caching allocator is still holding what that
sweep reserved, so the memory a bisect probe can actually use is
`free_mb_at_start + reserved_at_bisect_start_mb`. Compare a boundary against
that sum, not against `free_mb_at_start` alone.

Caveats
-------
* Impls with their own `run_with_oom_retry` (wd taggers, openclip) absorb OOMs
  by halving internally, so a "successful" batch can still have hit one; the
  probe reads `inferio.impl.utils.total_oom_halvings()` across every call and
  reports `absorbed_halvings`, and the bisect treats a batch with absorbed
  halvings as an OOM.
* `clap` and `sentence_transformers` have no impl-side retry: an OOM there is a
  raised exception.
* Whisper (`faster_whisper`) uses CTranslate2, not the torch allocator: its
  reserved/allocated figures stay near zero and only the NVML own-PID figure
  moves. That is a property of the model, not a probe failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MIB = 1024 * 1024


# --------------------------------------------------------------------------
# Registry resolution (no torch, no gateway)
# --------------------------------------------------------------------------


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_registries(paths: List[Path]) -> Dict[str, Any]:
    import tomllib

    merged: Dict[str, Any] = {"group": {}}
    for path in paths:
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            document = tomllib.load(handle)
        merged = _deep_merge(merged, document)
    return merged


def registry_files(repo: Path, extra: List[str]) -> List[Path]:
    files = sorted((repo / "python" / "inferio" / "config").glob("*.toml"))
    files += sorted((repo / "config" / "inference").glob("*.toml"))
    files += [Path(path) for path in extra]
    return files


def resolve_model(registry: Dict[str, Any], inference_id: str) -> Dict[str, Any]:
    """Group + inference-id merge, mirroring the Rust registry loader."""
    group_name, _, model_name = inference_id.partition("/")
    if not model_name:
        raise SystemExit(f"ceiling_probe: {inference_id!r} is not <group>/<id>")
    groups = registry.get("group") or {}
    group = groups.get(group_name)
    if group is None:
        raise SystemExit(
            f"ceiling_probe: group {group_name!r} not in the registry "
            f"(have: {', '.join(sorted(groups))})"
        )
    entries = group.get("inference_ids") or {}
    entry = entries.get(model_name)
    if entry is None:
        raise SystemExit(
            f"ceiling_probe: inference id {model_name!r} not in group "
            f"{group_name!r} (have: {', '.join(sorted(entries))})"
        )
    config = _deep_merge(group.get("config") or {}, entry.get("config") or {})
    metadata = _deep_merge(group.get("metadata") or {}, entry.get("metadata") or {})
    cost = metadata.get("cost") or {}
    impl_class = config.pop("impl_class", None)
    if not impl_class:
        raise SystemExit(f"ceiling_probe: no impl_class for {inference_id}")
    return {
        "inference_id": inference_id,
        "impl_class": impl_class,
        "config": config,
        "metadata": metadata,
        "cost": {
            "unit": cost.get("unit", "item"),
            "aggregation": cost.get("aggregation", "count"),
            "seed_units": cost.get("seed_units"),
            "epoch": cost.get("epoch"),
            "degraded": not cost,
        },
    }


# --------------------------------------------------------------------------
# NVML (board identity and own-PID usage)
# --------------------------------------------------------------------------


class Nvml:
    def __init__(self) -> None:
        self.ok = False
        self.error: Optional[str] = None
        self._pynvml = None
        try:
            import pynvml

            pynvml.nvmlInit()
            self._pynvml = pynvml
            self.ok = True
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def boards(self) -> List[Dict[str, Any]]:
        if not self.ok:
            return []
        pynvml = self._pynvml
        out = []
        for index in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            uuid = pynvml.nvmlDeviceGetUUID(handle)
            name = pynvml.nvmlDeviceGetName(handle)
            info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            out.append({
                "index": index,
                "uuid": uuid.decode() if isinstance(uuid, bytes) else str(uuid),
                "name": name.decode() if isinstance(name, bytes) else str(name),
                "total_mb": int(info.total // MIB),
                "free_mb": int(info.free // MIB),
                "used_mb": int(info.used // MIB),
            })
        return out

    def handle_for_uuid(self, uuid: str):  # noqa: ANN201
        if not self.ok:
            return None
        pynvml = self._pynvml
        for index in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(index)
            got = pynvml.nvmlDeviceGetUUID(handle)
            got = got.decode() if isinstance(got, bytes) else str(got)
            if got == uuid:
                return handle
        return None

    def free_mb(self, handle) -> Optional[int]:  # noqa: ANN001
        if not self.ok or handle is None:
            return None
        try:
            return int(self._pynvml.nvmlDeviceGetMemoryInfo(handle).free // MIB)
        except Exception:
            return None

    def own_mb(self, handle) -> Optional[int]:  # noqa: ANN001
        """NVML per-process usage for this PID: the worker's `base_method="nvml"`."""
        if not self.ok or handle is None:
            return None
        pid = os.getpid()
        pynvml = self._pynvml
        for getter in (
            "nvmlDeviceGetComputeRunningProcesses_v3",
            "nvmlDeviceGetComputeRunningProcesses_v2",
            "nvmlDeviceGetComputeRunningProcesses",
        ):
            fn = getattr(pynvml, getter, None)
            if fn is None:
                continue
            try:
                for entry in fn(handle):
                    if int(entry.pid) == pid:
                        used = getattr(entry, "usedGpuMemory", None)
                        if used is None or used >= 2**63:
                            return None
                        return int(used // MIB)
            except Exception:
                continue
            return None
        return None


# --------------------------------------------------------------------------
# Theil-Sen, matching ledger.rs robust_fit
# --------------------------------------------------------------------------


def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 0:
        return (ordered[mid - 1] + ordered[mid]) / 2.0
    return ordered[mid]


def theil_sen(samples: List[Tuple[int, int]], min_samples: int = 3) -> Optional[Dict[str, Any]]:
    """(units, delta_mb) -> the same fit `ledger.rs: robust_fit` would produce."""
    if len(samples) < min_samples:
        return None
    slopes: List[float] = []
    for index, (x0, y0) in enumerate(samples):
        for x1, y1 in samples[index + 1:]:
            dx = float(x1) - float(x0)
            if dx == 0.0:
                continue
            slopes.append((float(y1) - float(y0)) / dx)
    slope = _median(slopes)
    if slope is None or slope <= 0.0:
        return None
    intercepts = [float(y) - slope * float(x) for x, y in samples]
    intercept = _median(intercepts)
    if intercept is None:
        return None
    residuals = [abs(float(y) - (intercept + slope * float(x))) for x, y in samples]
    return {
        "slope_mb_per_unit": slope,
        "intercept_mb": intercept,
        "residual_mb": _median(residuals) or 0.0,
        "samples": len(samples),
    }


# --------------------------------------------------------------------------
# Corpus -> PredictionInput
# --------------------------------------------------------------------------


def load_items(corpus: Optional[str], group: Optional[str],
               kind: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not corpus:
        return [], None
    path = Path(corpus)
    if path.is_dir():
        path = path / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    items = manifest.get("items", [])
    if group:
        items = [item for item in items if item.get("group") == group]
    if kind:
        items = [item for item in items if item.get("kind") == kind]
    if not items:
        raise SystemExit(f"ceiling_probe: no corpus items match group={group} kind={kind}")
    return items, manifest.get("root")


def build_inputs(items: List[Dict[str, Any]], count: int, mode: str,
                 data_template: Dict[str, Any]):  # noqa: ANN201
    from inferio.inferio_types import PredictionInput

    inputs = []
    for index in range(count):
        item = items[index % len(items)]
        path = Path(item["abspath"])
        as_text = mode == "text" or (mode == "auto" and item["kind"] == "text")
        if as_text:
            payload = dict(data_template)
            payload["text"] = path.read_text(encoding="utf-8", errors="replace")
            inputs.append(PredictionInput(data=payload, file=None))
        else:
            inputs.append(PredictionInput(data=dict(data_template),
                                          file=path.read_bytes()))
    return inputs


# --------------------------------------------------------------------------
# Probe
# --------------------------------------------------------------------------


def parse_batches(text: Optional[str], max_batch: int) -> List[int]:
    if text:
        return [int(value) for value in text.replace(" ", "").split(",") if value]
    sizes: List[int] = []
    size = 1
    while size <= max_batch:
        sizes.append(size)
        size *= 2
    return sizes


def main(argv: Optional[List[str]] = None) -> int:
    here = Path(__file__).resolve()
    parser = argparse.ArgumentParser(
        description="Ground-truth base/slope/OOM boundary for one impl.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--model", required=True, help="inference_id")
    parser.add_argument("--corpus", help="corpus.py manifest.json or its directory")
    parser.add_argument("--group", help="corpus group filter")
    parser.add_argument("--kind", help="corpus kind filter")
    parser.add_argument("--mode", choices=("auto", "file", "text"), default="auto")
    parser.add_argument("--data", default="{}",
                        help="JSON merged into every input's data dict")
    parser.add_argument("--device", type=int, default=0, help="NVML board index")
    parser.add_argument("--batches", help="explicit comma-separated batch sizes")
    parser.add_argument("--max-batch", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=1,
                        help="untimed single-item batches before measuring")
    parser.add_argument("--bisect-oom", action="store_true")
    parser.add_argument("--bisect-max", type=int, default=1024)
    parser.add_argument("--bisect-start", type=int, default=1,
                        help="first batch size the doubling phase probes")
    parser.add_argument("--bisect-budget", type=float, default=0.0,
                        help="seconds of bisect probing before the refinement "
                             "stops and reports the bracket (0 = no limit)")
    parser.add_argument("--repo", default=str(here.parents[2]),
                        help="repository root")
    parser.add_argument("--impl-dir", action="append", default=[])
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--out", help="JSON output path (default: stdout)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-loaded", action="store_true",
                        help="skip unload() at the end (leaves VRAM held)")
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    registry = load_registries(registry_files(repo, args.registry))
    resolved = resolve_model(registry, args.model)
    impl_dirs = [str(repo / "python" / "inferio" / "impl"),
                 str(repo / "inferio_custom")] + list(args.impl_dir)
    items, corpus_root = load_items(args.corpus, args.group, args.kind)
    batches = parse_batches(args.batches, args.max_batch)
    data_template = json.loads(args.data)

    nvml = Nvml()
    boards = nvml.boards()
    board = next((entry for entry in boards if entry["index"] == args.device), None)

    plan = {
        "schema": "ceiling_probe/1",
        "model": args.model,
        "impl_class": resolved["impl_class"],
        "config": resolved["config"],
        "cost": resolved["cost"],
        "impl_dirs": impl_dirs,
        "registry_files": [str(path) for path in registry_files(repo, args.registry)],
        "corpus": {"path": args.corpus, "root": corpus_root, "items": len(items),
                   "group": args.group, "kind": args.kind, "mode": args.mode},
        "batches": batches,
        "repeats": args.repeats,
        "device": board,
        "boards": boards,
        "nvml_error": nvml.error,
        "python": sys.version.split()[0],
    }

    if args.dry_run:
        print(json.dumps(plan, indent=1))
        return 0

    if board is None:
        raise SystemExit(
            f"ceiling_probe: NVML has no board with index {args.device} "
            f"(nvml error: {nvml.error})"
        )
    if not items:
        raise SystemExit("ceiling_probe: --corpus is required for a real run")

    # Pin exactly as the orchestrator does, BEFORE torch is imported.
    os.environ["CUDA_VISIBLE_DEVICES"] = board["uuid"]
    os.environ.setdefault("PANOPTIKON_DEVICE_PIN", board["uuid"])
    sys.path.insert(0, str(repo / "python"))

    handle = nvml.handle_for_uuid(board["uuid"])
    from inferio_worker.discovery import find_impl_class
    from inferio_worker import packing

    import logging

    logging.basicConfig(level=os.environ.get("INFERIO_WORKER_LOG_LEVEL", "WARNING"))
    impl_cls = find_impl_class(resolved["impl_class"], impl_dirs,
                               logging.getLogger("ceiling_probe"))

    free_before = nvml.free_mb(handle)
    load_started = time.monotonic()
    instance = impl_cls(**resolved["config"])
    instance.load()
    import torch

    torch.cuda.synchronize()
    load_seconds = time.monotonic() - load_started
    free_after = nvml.free_mb(handle)
    reserved_at_load = int(torch.cuda.memory_reserved() // MIB)
    allocated_at_load = int(torch.cuda.memory_allocated() // MIB)
    base_nvml = nvml.own_mb(handle)

    try:
        from inferio.impl import utils as impl_utils
    except Exception:
        impl_utils = None

    def halvings() -> int:
        reader = getattr(impl_utils, "total_oom_halvings", None) if impl_utils else None
        try:
            return int(reader()) if reader else 0
        except Exception:
            return 0

    unit = resolved["cost"]["unit"]
    aggregation = resolved["cost"]["aggregation"]

    def price(inputs) -> int:  # noqa: ANN001
        priced = packing.price_inputs(inputs, unit)
        return packing.batch_units(range(len(inputs)), priced, aggregation)

    def run_batch(count: int, repeat: int) -> Dict[str, Any]:
        inputs = build_inputs(items, count, args.mode, data_template)
        units = price(inputs)
        torch.cuda.synchronize()
        reserved_before = int(torch.cuda.memory_reserved() // MIB)
        torch.cuda.reset_peak_memory_stats()
        before_halvings = halvings()
        started = time.monotonic()
        error: Optional[str] = None
        ok = True
        try:
            instance.predict(inputs)
            torch.cuda.synchronize()
        except Exception as exc:
            ok = False
            error = f"{type(exc).__name__}: {exc}"[:600]
        duration_ms = (time.monotonic() - started) * 1000.0
        peak_reserved = int(torch.cuda.max_memory_reserved() // MIB)
        peak_allocated = int(torch.cuda.max_memory_allocated() // MIB)
        reserved_after = int(torch.cuda.memory_reserved() // MIB)
        absorbed = max(0, halvings() - before_halvings)
        oom = (not ok and _looks_like_oom(error)) or absorbed > 0
        return {
            "batch": count,
            "repeat": repeat,
            "units": units,
            "items": count,
            "ok": ok,
            "oom": bool(oom),
            "absorbed_halvings": absorbed,
            "duration_ms": round(duration_ms, 3),
            "peak_reserved_mb": peak_reserved,
            "peak_allocated_mb": peak_allocated,
            "reserved_before_mb": reserved_before,
            "reserved_after_mb": reserved_after,
            "nvml_own_mb": nvml.own_mb(handle),
            "board_free_mb": nvml.free_mb(handle),
            "delta_mb": max(0, peak_reserved - reserved_at_load),
            "error": error,
        }

    for _ in range(max(0, args.warmup)):
        try:
            run_batch(1, -1)
        except Exception:
            break

    records: List[Dict[str, Any]] = []
    for count in batches:
        for repeat in range(args.repeats):
            record = run_batch(count, repeat)
            records.append(record)
            print(
                f"batch {count:5d} units {record['units']:9d} "
                f"peak_reserved {record['peak_reserved_mb']:6d} MiB "
                f"delta {record['delta_mb']:6d} MiB "
                f"nvml {record['nvml_own_mb']} "
                f"{record['duration_ms']:.0f} ms"
                + ("  OOM" if record["oom"] else "")
                + (f"  ERROR {record['error']}" if record["error"] else ""),
                file=sys.stderr,
            )
            if record["oom"] or not record["ok"]:
                break
        if records and (records[-1]["oom"] or not records[-1]["ok"]):
            break

    clean = [
        (record["units"], record["delta_mb"])
        for record in records
        if record["ok"] and not record["oom"] and record["delta_mb"] > 0
    ]
    fit = theil_sen(clean)

    def settle_after_failure() -> None:
        """Return the allocator to a clean state between bisect probes.

        An OOM leaves the caching allocator fragmented and holding blocks the
        next attempt cannot reuse, so without this the search would find a
        boundary that depends on the order it probed in rather than on the
        model.
        """
        try:
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
        except Exception:
            pass

    bisect: Optional[Dict[str, Any]] = None
    if args.bisect_oom:
        bisect = {"free_mb_at_start": nvml.free_mb(handle),
                  "reserved_at_bisect_start_mb": int(torch.cuda.memory_reserved() // MIB),
                  "trace": [],
                  "largest_ok_items": None, "largest_ok_units": None,
                  "first_oom_items": None, "stopped_early": False}
        bisect_started = time.monotonic()
        low, high = 1, args.bisect_max
        # Grow first: double until something fails or the ceiling is hit.
        probe = max(1, args.bisect_start)
        while probe <= args.bisect_max:
            record = run_batch(probe, -2)
            bisect["trace"].append({"items": probe, "ok": record["ok"] and not record["oom"],
                                    "units": record["units"], "oom": record["oom"],
                                    "absorbed_halvings": record["absorbed_halvings"],
                                    "error": record["error"]})
            if record["ok"] and not record["oom"]:
                low = probe
                bisect["largest_ok_items"] = probe
                bisect["largest_ok_units"] = record["units"]
                probe *= 2
            else:
                high = probe
                bisect["first_oom_items"] = probe
                settle_after_failure()
                break
        else:
            high = args.bisect_max
        while high - low > 1:
            if args.bisect_budget > 0 and (
                    time.monotonic() - bisect_started > args.bisect_budget):
                bisect["stopped_early"] = True
                break
            mid = (low + high) // 2
            record = run_batch(mid, -2)
            bisect["trace"].append({"items": mid, "ok": record["ok"] and not record["oom"],
                                    "units": record["units"], "oom": record["oom"],
                                    "absorbed_halvings": record["absorbed_halvings"],
                                    "error": record["error"]})
            if record["ok"] and not record["oom"]:
                low = mid
                bisect["largest_ok_items"] = mid
                bisect["largest_ok_units"] = record["units"]
            else:
                high = mid
                bisect["first_oom_items"] = mid
                settle_after_failure()
        bisect["low_items"] = low
        bisect["high_items"] = high

    result = {
        **plan,
        "torch": torch.__version__,
        "dtype": _resolve_dtype(instance),
        "device": {**board, "cuda_visible_devices": os.environ["CUDA_VISIBLE_DEVICES"]},
        "load": {
            "seconds": round(load_seconds, 3),
            "base_nvml_mb": base_nvml,
            "base_free_delta_mb": (
                None if free_before is None or free_after is None
                else max(0, free_before - free_after)
            ),
            "reserved_at_load_mb": reserved_at_load,
            "allocated_at_load_mb": allocated_at_load,
            "free_before_mb": free_before,
            "free_after_mb": free_after,
        },
        "batches": records,
        "fit": fit,
        "bisect": bisect,
    }

    if not args.keep_loaded:
        try:
            instance.unload()
        except Exception:
            pass
        try:
            torch.cuda.empty_cache()
        except Exception:
            pass

    text = json.dumps(result, indent=1, default=str)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"ceiling_probe: wrote {args.out}", file=sys.stderr)
    else:
        print(text)
    if fit:
        print(
            f"ceiling_probe: base(nvml)={base_nvml} MiB  "
            f"slope={fit['slope_mb_per_unit']:.6g} MiB/unit  "
            f"intercept={fit['intercept_mb']:.4g} MiB  "
            f"residual={fit['residual_mb']:.4g} MiB  n={fit['samples']}",
            file=sys.stderr,
        )
    return 0


_OOM_MARKERS = (
    "out of memory", "inference_oom", "outofmemoryerror", "memoryerror",
    "hip out of memory",
)


def _looks_like_oom(message: Optional[str]) -> bool:
    if not message:
        return False
    lowered = message.lower()
    return any(marker in lowered for marker in _OOM_MARKERS)


def _resolve_dtype(instance: Any) -> Optional[str]:
    for attribute in ("dtype", "torch_dtype", "_dtype"):
        value = getattr(instance, attribute, None)
        if value is not None:
            return str(value)
    model = getattr(instance, "model", None)
    parameters = getattr(model, "parameters", None)
    if callable(parameters):
        try:
            return str(next(parameters()).dtype)
        except Exception:
            return None
    return None


if __name__ == "__main__":
    raise SystemExit(main())
