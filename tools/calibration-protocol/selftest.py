#!/usr/bin/env python3
"""selftest.py - what this platform's memory sensing can actually answer.

The first thing a platform pass runs, before any scenario: it loads one small
shipped model in-process through the real impl loader and reports, tier by
tier, what the **worker's own** measurement code returns on this machine. No
server, no gateway, no orchestrator, no job queue - just
`inferio_worker.memory` and `inferio_worker.packing` against a live device.

Everything a scenario later measures rests on those tiers, and every one of
them can degrade silently: NVML's per-process figure is N/A under Windows'
WDDM, `amdgpu` sysfs exists only on ROCm, `torch.cuda.mem_get_info`'s "free"
was process-local on old HIP, and an engine that never allocates through
torch's allocator (CTranslate2) leaves the allocator counters at zero. A
degraded tier is not a failure - the design has fallbacks for each - but a
scenario read *without knowing which tier answered* is uninterpretable, which
is why run1 and run2 both list `base_method` as the first per-platform check
(`docs/batch-calibration-test-protocol.md` §9).

Usage
-----
    selftest.py [--model tags/wd-vit-tagger-v3] [--device 0] [--batch 8]
                [--corpus results/corpus/ramp/manifest.json] [--group G]
                [--image-px 1024] [--induce-oom] [--json out.json]
                [--repo DIR] [--impl-dir D] [--registry F] [--quiet]

Runs on Windows, macOS and Linux with the repo venv (`python/.venv`) and
needs no corpus: without `--corpus` it synthesises the batch's images with
Pillow, so the tool works on a machine that has never generated one.

What it prints
--------------
One table with these sections, then a `VERDICT:` line naming the degraded
tiers (exit 0 either way - "degraded" is a fact about the platform, not an
error):

1. `platform`  - OS, machine, python, torch, backend (`cuda`/`hip`/`mps`/
   `cpu`), driver and CUDA runtime, whether the host is CPU-priced.
2. `device`    - `gpu_name`, compute capability, uuid, total, and the pin.
3. `free`      - the resolved `free_source` and free/total from
   `memory._free_total_mb()`, then every tier of that chain probed on its own
   so the ones that answered `None` are named with the reason.
4. `base`      - `memory.begin_load()` / `finish_load()` around the real
   `impl.load()`: `base_mb`, `base_method`, `allocated_at_load_mb`,
   `reserved_at_load_mb`, and the same tier-by-tier table for the base chain
   (`nvml` / `fdinfo` / `mps` / `rss` / `free_delta` / `alloc_delta`).
5. `batch`     - one batch of `--batch` items priced by the worker's own
   `packing.price_inputs` / `batch_units`, measured with
   `memory.begin_batch()` / `measure_batch()`: `peak_allocated_mb`,
   `peak_reserved_mb`, `duration_ms`.
6. `release`   - what `memory.empty_cache()` returned and what the pool did.
7. `oom`       - only with `--induce-oom`, below.

`--json PATH` writes the same content as one document
(`{"schema": "selftest/1", ...}`) with a block per section.

`--induce-oom`
--------------
Fills the device with touched filler tensors until an allocation raises, then
prints the exception type, the first 200 characters of its message, and the
verdict of the worker's own `packing.classify_oom` - `oom_class.source`
(`typed_exception` / `marker` / `message_pattern`), `.exception`,
`.free_mb_at_failure` and `.device`. That last tier is a **closed list of
message fragments** (`packing.OOM_MESSAGE_PATTERNS`), so a platform whose
allocator words its failure differently deflates on nothing at all - run2's
§10 names ROCm and MPS as where a missing wording first bites, and this is
the one-command check for it.

**Windows/WDDM has no exception to classify.** Over-admission there is a
silent spill to system memory through the driver's sysmem fallback: the batch
succeeds and only its throughput collapses, which is what
`packing._note_throughput` / `COLLAPSE_RATIO` exist to catch. So when the
filler ladder exhausts the device *without* raising, this tool runs one more
real batch and compares its units/sec against the clean batch from section 5:
below `COLLAPSE_RATIO` the verdict is a **spill**, reported as
`oom.kind = "throughput_collapse"` with both rates, and no `oom_class`.

The GPU is never left allocated: the filler, the batch inputs and the impl
are released in a `finally`, and the last line of the run reports the device's
free memory afterwards so a caller can see it came back.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform as platform_module
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

MIB = 1024 * 1024
SCHEMA = "selftest/1"

# The batch section's default: eight items is the smallest batch that shows a
# per-item slope at all and still runs on a 4 GB card.
DEFAULT_BATCH = 8
DEFAULT_MODEL = "tags/wd-vit-tagger-v3"

# Filler ladder for `--induce-oom`. The chunk is large enough that a 100 GB
# board is filled in a few dozen allocations and small enough that the last
# successful one leaves little unusable slack.
FILLER_CHUNK_MB = 1024
# Stop the ladder once this much has been held without a failure: a device
# that has taken more than its own total is not going to raise (WDDM's sysmem
# fallback, or a unified-memory host), so the throughput check takes over.
FILLER_OVERSHOOT = 1.25

# The free-memory chain in `memory._free_total_mb`, in its own order.
FREE_TIERS = ("ram", "nvml", "amdgpu-sysfs", "mps", "torch")

# Base tiers that measure *this process* directly, as opposed to inferring it
# from a driver-level delta. A `base_method` outside this set is the degraded
# path W4 has never been exercised on (run2 report §10).
DIRECT_BASE_METHODS = ("nvml", "fdinfo", "mps", "rss")


# --- loading the sibling tools and the worker ------------------------------


def load_probe(here: Path) -> Any:
    """`ceiling_probe.py` as a module, for its registry and corpus helpers.

    Loaded by path rather than imported, because these tools are scripts and
    not a package - the same thing `python/tests/inferio_worker/
    test_ceiling_probe.py` does, and safe for the same reason: the probe's
    module level imports nothing outside the standard library.
    """
    path = here / "ceiling_probe.py"
    spec = importlib.util.spec_from_file_location("_calib_ceiling_probe", path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise SystemExit(f"selftest: cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def synth_items(count: int, pixels: int, out_dir: Path) -> List[Dict[str, Any]]:
    """`count` distinct JPEGs of `pixels`x`pixels`, written under `out_dir`.

    The stand-in for `corpus.py` on a machine that has never run it. Distinct
    content per item, because an impl that caches on a checksum would
    otherwise measure one item `count` times. Not deterministic across
    versions of Pillow and not meant to be: a self-test measures this
    machine, and a cross-machine comparison uses `--corpus`.
    """
    from PIL import Image

    out_dir.mkdir(parents=True, exist_ok=True)
    items: List[Dict[str, Any]] = []
    for index in range(count):
        path = out_dir / f"selftest-{index:03d}.jpg"
        if not path.is_file():
            image = Image.new("RGB", (pixels, pixels))
            image.putdata(
                [
                    ((x * 7 + index * 13) % 256, (y * 5 + index * 29) % 256,
                     (x + y + index) % 256)
                    for y in range(pixels)
                    for x in range(pixels)
                ]
            )
            image.save(path, format="JPEG", quality=88)
        items.append({"abspath": str(path), "kind": "image", "id": path.stem})
    return items


# --- tier probing ----------------------------------------------------------


def probe_free_tiers(memory: Any) -> List[Dict[str, Any]]:
    """Every tier of `memory._free_total_mb`, asked on its own.

    The resolved reading only names the tier that won; a platform pass needs
    the ones that lost and why, because the next tier down is the one a driver
    update or a container runtime will fall back to.
    """
    rows: List[Dict[str, Any]] = []
    for tier in FREE_TIERS:
        free_mb = total_mb = None
        reason: Optional[str] = None
        try:
            free_mb, total_mb, source = memory._free_total_mb(tier)
        except Exception as exc:  # pragma: no cover - defensive
            source = None
            reason = f"raised {type(exc).__name__}: {exc}"[:200]
        if free_mb is None and reason is None:
            reason = _free_tier_reason(memory, tier)
        rows.append({"tier": tier, "free_mb": free_mb, "total_mb": total_mb,
                     "answered": free_mb is not None, "reason": reason})
    return rows


def _free_tier_reason(memory: Any, tier: str) -> str:
    """Why one free tier returned nothing, in the tier's own terms."""
    try:
        if tier == "ram":
            if not memory._ram_currency():
                return ("not a CPU-priced host: the RAM tier is only consulted "
                        "when RAM is the currency")
            return "psutil/sysctl gave no available-memory figure"
        if tier == "nvml":
            return ("NVML unavailable" if memory._nvml() is None
                    else "NVML gave no memory info for this process's device")
        if tier == "amdgpu-sysfs":
            return ("no amdgpu sysfs (not a ROCm host, or no device resolved)"
                    if memory.device_bdf() is None
                    else "amdgpu sysfs present but mem_info_vram_* unreadable")
        if tier == "mps":
            return ("torch.backends.mps unavailable"
                    if memory._torch_mps() is None
                    else "MPS present but recommended-max/allocated read as 0")
        if tier == "torch":
            return ("no live CUDA/HIP context in this process"
                    if memory._torch_cuda() is None
                    else "torch.cuda.mem_get_info raised or returned nothing")
    except Exception as exc:  # pragma: no cover - defensive
        return f"reason probe raised {type(exc).__name__}: {exc}"[:200]
    return "no value"


def probe_base_tiers(
    memory: Any,
    before: Dict[str, Any],
    reserved_mb: Optional[int],
    reserved_delta: Optional[int],
    alloc_floor: Optional[int],
) -> List[Dict[str, Any]]:
    """Every tier of `memory._resolve_base`, asked on its own, after the load.

    Values are read *now*, a moment after `finish_load` took the authoritative
    one, so `free_delta` in particular is recomputed against a fresh free
    reading and can differ from the figure `base_mb` was built from by
    whatever the device did in between. The column that matters is which
    tiers answer at all.
    """
    rows: List[Dict[str, Any]] = []

    def row(tier: str, value: Optional[int], reason: str) -> None:
        rows.append({"tier": tier, "value_at_probe_time_mb": value,
                     "answered": value is not None and value > 0,
                     "reason": None if (value is not None and value > 0)
                     else reason})

    try:
        ram = memory._ram_currency()
    except Exception:
        ram = False
    row("rss", alloc_floor if ram else None,
        "not a CPU-priced host: the RSS tier only applies when RAM is the "
        "currency")
    try:
        row("nvml", memory._nvml_own_process_mb(holding_mb=reserved_mb),
            "NVML lists no process with this pid - expected on Windows WDDM, "
            "and in a container started without --pid=host")
    except Exception as exc:  # pragma: no cover - defensive
        row("nvml", None, f"raised {type(exc).__name__}: {exc}"[:200])
    try:
        row("fdinfo", memory._fdinfo_base_mb(reserved_mb, reserved_delta),
            "no DRM fdinfo VRAM figure for this process (not a ROCm host)")
    except Exception as exc:  # pragma: no cover - defensive
        row("fdinfo", None, f"raised {type(exc).__name__}: {exc}"[:200])
    try:
        row("mps", memory._mb(memory._mps_call("driver_allocated_memory")),
            "torch.mps.driver_allocated_memory unavailable (not an MPS host)")
    except Exception as exc:  # pragma: no cover - defensive
        row("mps", None, f"raised {type(exc).__name__}: {exc}"[:200])

    free_now, _ = memory._free_mb(before.get("free_source"))
    free_delta = memory._free_delta(before.get("free_mb"), free_now)
    context_mb, context_source = memory.context_allowance_mb()
    ceiling = (reserved_delta or 0) + context_mb + memory.IMPLAUSIBLE_SLACK_MB
    if ram:
        # `_resolve_base` returns the RSS tier outright on a CPU-priced host,
        # so the two driver-delta tiers below it are never consulted there. The
        # arithmetic still evaluates, and printing it as an answer would invite
        # a reader to compare a figure the worker would never use.
        reason = ("not reached: a CPU-priced host resolves at the RSS tier "
                  "above")
    elif free_delta is None:
        reason = "no before/after free reading from the same source"
    elif free_delta <= 0:
        reason = f"free memory did not fall across the load window ({free_delta} MiB)"
    elif free_delta > ceiling:
        reason = (f"{free_delta} MiB is implausible against the {ceiling} MiB "
                  f"ceiling ({reserved_delta or 0} reserved + {context_mb} "
                  f"{context_source} context + slack)")
    else:
        reason = ""
    rows.append({
        "tier": "free_delta", "value_at_probe_time_mb": free_delta,
        "answered": bool(reason == ""), "reason": reason or None,
    })
    rows.append({
        "tier": ("alloc_delta_measured" if context_source == "measured"
                 else "alloc_delta"),
        "value_at_probe_time_mb": (alloc_floor or 0) + context_mb,
        "answered": not ram,
        "reason": ("not reached: a CPU-priced host resolves at the RSS tier "
                   "above") if ram else None,
        "context_mb": context_mb,
        "context_source": context_source,
    })
    return rows


# --- the run ---------------------------------------------------------------


def platform_block(memory: Any) -> Dict[str, Any]:
    torch = None
    try:
        import torch as torch_module

        torch = torch_module
    except Exception:
        torch = None
    backend = "cpu"
    cuda_runtime = None
    if torch is not None:
        try:
            if getattr(torch.version, "hip", None):
                backend = "hip"
            elif torch.cuda.is_available():
                backend = "cuda"
                cuda_runtime = torch.version.cuda
            elif getattr(getattr(torch, "backends", None), "mps", None) is not None \
                    and torch.backends.mps.is_available():
                backend = "mps"
        except Exception:
            pass
    driver = None
    try:
        pair = memory._nvml()
        if pair is not None:
            pynvml = pair[0]
            driver = pynvml.nvmlSystemGetDriverVersion()
            if isinstance(driver, bytes):
                driver = driver.decode("utf-8", "replace")
    except Exception:
        driver = None
    try:
        ram_currency = bool(memory._ram_currency())
    except Exception:
        ram_currency = False
    return {
        "system": platform_module.system(),
        "release": platform_module.release(),
        "machine": platform_module.machine(),
        "python": platform_module.python_version(),
        "torch": memory.torch_version(),
        "backend": backend,
        "cuda_runtime": cuda_runtime,
        "hip_runtime": getattr(getattr(torch, "version", None), "hip", None)
        if torch is not None else None,
        "driver_version": driver,
        "ram_currency": ram_currency,
        "device_label": _safe(memory.device_label),
        "memory_regions": list(_safe(memory._memory_regions) or ()),
    }


def _safe(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception:
        return None


def device_block(memory: Any, pin: Optional[str]) -> Dict[str, Any]:
    uuid, name = (None, None)
    try:
        uuid, name = memory.device_identity()
    except Exception:
        pass
    capability = None
    try:
        props = memory._device_props()
        major = memory._prop(props, "major")
        minor = memory._prop(props, "minor")
        if major is not None and minor is not None:
            capability = f"{major}.{minor}"
        elif props is not None:
            capability = memory._prop(props, "gcnArchName")
    except Exception:
        pass
    return {
        "gpu_name": name,
        "gpu_uuid": uuid,
        "compute_capability": capability,
        "gpu_total_mb": _safe(memory.gpu_total_mb),
        "gpu_bdf": _safe(memory.device_bdf),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "pin": pin,
        "pinned_device_missing": _safe(memory.pinned_device_missing),
    }


def induce_oom(
    memory: Any,
    packing: Any,
    run_batch: Callable[[int], Dict[str, Any]],
    batch_items: int,
    clean_rate: Optional[float],
    total_mb: Optional[int],
    quiet: bool,
) -> Dict[str, Any]:
    """Push the device until it fails, and say **how** it failed.

    Two outcomes, and the platform decides which: an allocation raises (every
    discrete CUDA/HIP device, and MPS under a watermark), or nothing raises
    and the driver quietly spills to host memory (Windows WDDM). The second
    is not an error condition anywhere in the stack - it is a throughput
    collapse - so it is detected by re-running the section-5 batch and
    comparing its rate, which is exactly what `packing._note_throughput` does
    inside a real window.
    """
    result: Dict[str, Any] = {
        "requested": True,
        "kind": None,
        "filler_held_mb": 0,
        "chunks": 0,
        "exception_type": None,
        "message_head": None,
        "oom_class": None,
        "clean_units_per_s": clean_rate,
        "stressed_units_per_s": None,
        "collapse_ratio_threshold": getattr(packing, "COLLAPSE_RATIO", None),
    }
    try:
        import torch
    except Exception as exc:
        result["kind"] = "unavailable"
        result["message_head"] = f"torch not importable: {exc}"[:200]
        return result

    device = "cuda" if torch.cuda.is_available() else (
        "mps" if _safe(lambda: torch.backends.mps.is_available()) else None)
    if device is None:
        result["kind"] = "unavailable"
        result["message_head"] = "no accelerator device to exhaust"
        return result

    cap_mb = int((total_mb or 0) * FILLER_OVERSHOOT) or None
    filler: List[Any] = []
    held_mb = 0
    failure: Optional[BaseException] = None
    try:
        while True:
            if cap_mb is not None and held_mb >= cap_mb:
                break
            try:
                chunk = torch.empty(
                    FILLER_CHUNK_MB * MIB, dtype=torch.uint8, device=device
                )
                # Touched, so a driver that only reserves on first write
                # commits it here rather than at some later, unrelated batch.
                chunk.fill_(1)
                if device == "cuda":
                    torch.cuda.synchronize()
                filler.append(chunk)
                held_mb += FILLER_CHUNK_MB
                result["chunks"] += 1
            except Exception as exc:
                failure = exc
                break
        result["filler_held_mb"] = held_mb

        if failure is not None:
            result["kind"] = "exception"
            result["exception_type"] = type(failure).__name__
            result["message_head"] = str(failure)[:200]
            result["oom_class"] = packing.classify_oom(failure, 0)
            if not quiet:
                print(f"  filler failed after {held_mb} MiB", file=sys.stderr)
            return result

        # Nothing raised with the board over-subscribed: the WDDM shape. Run
        # one real batch against it and let the throughput comparator decide.
        if not quiet:
            print(f"  filler held {held_mb} MiB without an exception; "
                  f"re-running the batch to test for a spill", file=sys.stderr)
        record = run_batch(batch_items)
        result["stressed_units_per_s"] = record.get("units_per_s")
        result["stressed"] = record
        if record.get("oom_class"):
            result["kind"] = "exception"
            result["exception_type"] = record.get("exception_type")
            result["message_head"] = record.get("message_head")
            result["oom_class"] = record["oom_class"]
            return result
        ratio_floor = getattr(packing, "COLLAPSE_RATIO", 0.4)
        stressed = record.get("units_per_s")
        if (clean_rate and stressed is not None
                and stressed < ratio_floor * clean_rate):
            result["kind"] = "throughput_collapse"
        else:
            result["kind"] = "no_failure"
        return result
    finally:
        filler.clear()
        try:
            memory.empty_cache()
        except Exception:
            pass


def verdict_line(document: Dict[str, Any]) -> Tuple[str, List[str]]:
    """`VERDICT: ...` plus the degraded-tier list behind it.

    "Degraded" means a tier that a scenario's checks depend on answered from
    a weaker source than the design's first choice, so a reader of the later
    verdict table knows which numbers are inferred rather than measured.
    """
    degraded: List[str] = []
    free_source = (document.get("free") or {}).get("free_source")
    if free_source is None:
        degraded.append("free:none (no tier answered)")
    elif free_source == "torch":
        degraded.append(
            "free:torch (mem_get_info; non-authoritative, the ledger says so)")

    base = document.get("base") or {}
    method = base.get("base_method")
    if method is None:
        degraded.append("base:none (the worker reports no base at all)")
    elif method not in DIRECT_BASE_METHODS:
        degraded.append(
            f"base:{method} (inferred from a driver delta, not this process)")

    # Only meaningful where the worker's memory *is* an NVML device: on a
    # CPU-priced host there is no per-process GPU figure to miss.
    nvml_row = next((row for row in base.get("tiers", [])
                     if row["tier"] == "nvml"), None)
    on_nvml = (not (document.get("platform") or {}).get("ram_currency")
               and any(row["tier"] == "nvml" and row["answered"]
                       for row in (document.get("free") or {}).get("tiers", [])))
    if on_nvml and nvml_row is not None and not nvml_row["answered"]:
        degraded.append(
            "oracle:no NVML per-process figure (attribute with "
            "`nvidia-smi --query-compute-apps`; vramrec.py does this "
            "automatically)")

    batch = document.get("batch") or {}
    if batch.get("ok") and not batch.get("peak_allocated_mb"):
        degraded.append(
            "allocator:torch counters read 0 (the engine does not allocate "
            "through torch - CTranslate2 and friends; only NVML moves)")

    release = document.get("release") or {}
    if release.get("empty_cache") is False and free_source not in (None, "ram"):
        degraded.append(
            "release:empty_cache() returned False (nothing of ours is on the "
            "device, or the backend has no release)")

    oom = document.get("oom") or {}
    if oom.get("requested"):
        if oom.get("kind") == "throughput_collapse":
            degraded.append(
                "oom:no exception - the device spills to host memory "
                "(over-admission is a throughput collapse here, not an OOM)")
        elif oom.get("kind") == "no_failure":
            degraded.append(
                "oom:the device neither raised nor collapsed under a filler "
                "larger than its own total")
        elif oom.get("kind") == "exception" and not oom.get("oom_class"):
            degraded.append(
                "oom:the classifier did not recognise this platform's "
                "allocator failure (packing.OOM_MESSAGE_PATTERNS needs its "
                "wording)")

    if not degraded:
        return "VERDICT: no degraded tiers", degraded
    return "VERDICT: degraded - " + "; ".join(degraded), degraded


# --- printing --------------------------------------------------------------


def print_document(document: Dict[str, Any], stream: Any) -> None:
    def section(title: str) -> None:
        print(f"\n== {title} ==", file=stream)

    def kv(items: Dict[str, Any], keys: Optional[List[str]] = None) -> None:
        chosen = keys if keys is not None else list(items)
        width = max((len(key) for key in chosen), default=0)
        for key in chosen:
            print(f"  {key.ljust(width)}  {items.get(key)}", file=stream)

    section("platform")
    kv(document["platform"])
    section("device")
    kv(document["device"])

    free = document["free"]
    section("free memory")
    kv({"free_source": free["free_source"], "free_mb": free["free_mb"],
        "total_mb": free["total_mb"]})
    print("  tiers:", file=stream)
    for row in free["tiers"]:
        mark = "ok " if row["answered"] else "-- "
        value = (f"{row['free_mb']} / {row['total_mb']} MiB"
                 if row["answered"] else row["reason"])
        print(f"    {mark}{row['tier']:<14} {value}", file=stream)

    base = document["base"]
    section("base (load)")
    kv(base, ["load_seconds", "base_mb", "base_method", "allocated_at_load_mb",
              "reserved_at_load_mb", "dtype", "dtype_method", "gpu_name",
              "gpu_total_mb", "torch_version"])
    print("  tiers:", file=stream)
    for row in base["tiers"]:
        mark = "ok " if row["answered"] else "-- "
        value = (f"{row['value_at_probe_time_mb']} MiB" if row["answered"]
                 else row["reason"])
        print(f"    {mark}{row['tier']:<20} {value}", file=stream)

    batch = document["batch"]
    section(f"batch of {batch['items']}")
    kv(batch, ["items", "units", "unit", "aggregation", "canvas_pixels",
               "ok", "duration_ms", "units_per_s", "peak_allocated_mb",
               "peak_reserved_mb", "allocated_before_mb",
               "reserved_before_mb", "free_mb", "free_source",
               "absorbed_halvings", "index_limit_events", "exception_type",
               "message_head"])

    section("release")
    kv(document["release"])

    if document["oom"].get("requested"):
        section("induced failure")
        kv(document["oom"], ["kind", "chunks", "filler_held_mb",
                             "exception_type", "message_head",
                             "clean_units_per_s", "stressed_units_per_s",
                             "collapse_ratio_threshold"])
        oom_class = document["oom"].get("oom_class")
        print("  oom_class: " + (json.dumps(oom_class) if oom_class
                                 else "None (the classifier says this is not "
                                      "an out-of-memory condition)"),
              file=stream)

    print("", file=stream)
    print(document["verdict"], file=stream)


# --- main ------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Per-platform report of the worker's own memory sensing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help="inference id loaded through the real impl loader")
    parser.add_argument("--device", type=int, default=0,
                        help="NVML GPU index; ignored where NVML has no device")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH,
                        help="items in the measured batch")
    parser.add_argument("--corpus",
                        help="corpus.py manifest.json (or its directory); "
                             "without it the batch's images are synthesised")
    parser.add_argument("--group", help="corpus group filter")
    parser.add_argument("--kind", help="corpus kind filter")
    parser.add_argument("--mode", choices=("auto", "file", "text"),
                        default="auto")
    parser.add_argument("--image-px", type=int, default=1024,
                        help="side of the synthesised images")
    parser.add_argument("--scratch",
                        help="where synthesised images are written "
                             "(default: a selftest-corpus dir beside --json, "
                             "else the system temp dir)")
    parser.add_argument("--induce-oom", action="store_true",
                        help="fill the device until it fails, and classify it")
    parser.add_argument("--json", dest="json_path",
                        help="write the whole report to this path")
    parser.add_argument("--repo", default=str(here.parents[1]),
                        help="repository root")
    parser.add_argument("--impl-dir", action="append", default=[])
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--quiet", action="store_true",
                        help="no progress notes on stderr")
    args = parser.parse_args(argv)

    probe = load_probe(here)
    repo = Path(args.repo).resolve()
    registry = probe.load_registries(probe.registry_files(repo, args.registry))
    resolved = probe.resolve_model(registry, args.model)
    impl_dirs = [str(repo / "python" / "inferio" / "impl"),
                 str(repo / "inferio_custom")] + list(args.impl_dir)

    # Pin before torch is imported, exactly as the orchestrator does
    # (`gpu.rs: resolve_pin`) and as `ceiling_probe.py` does. A platform with
    # no NVML - macOS, a CPU host - simply has nothing to pin.
    pin: Optional[str] = None
    nvml = probe.Nvml()
    gpu = next((entry for entry in nvml.gpus()
                if entry["index"] == args.device), None)
    if gpu is not None:
        pin = gpu["uuid"]
        os.environ["CUDA_VISIBLE_DEVICES"] = pin
        os.environ.setdefault("PANOPTIKON_DEVICE_PIN", pin)
    sys.path.insert(0, str(repo / "python"))

    import logging

    logging.basicConfig(level=os.environ.get("INFERIO_WORKER_LOG_LEVEL",
                                             "WARNING"))
    from inferio_worker import memory, packing
    from inferio_worker.discovery import find_impl_class

    if args.corpus:
        items, _ = probe.load_items(args.corpus, args.group, args.kind)
    else:
        scratch = Path(args.scratch) if args.scratch else (
            (Path(args.json_path).resolve().parent / "selftest-corpus")
            if args.json_path
            else Path(__import__("tempfile").gettempdir()) / "selftest-corpus"
        )
        if not args.quiet:
            print(f"selftest: synthesising {args.batch} images "
                  f"({args.image_px}px) in {scratch}", file=sys.stderr)
        items = synth_items(args.batch, args.image_px, scratch)

    document: Dict[str, Any] = {
        "schema": SCHEMA,
        "argv": sys.argv,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "model": args.model,
        "impl_class": resolved["impl_class"],
        "cost": resolved["cost"],
    }

    instance = None
    try:
        document["platform"] = platform_block(memory)

        # --- base ---
        impl_cls = find_impl_class(resolved["impl_class"], impl_dirs,
                                   logging.getLogger("selftest"))
        if not args.quiet:
            print(f"selftest: loading {args.model} "
                  f"({resolved['impl_class']})", file=sys.stderr)
        before = memory.begin_load()
        load_started = time.monotonic()
        instance = impl_cls(**resolved["config"])
        instance.load()
        load_seconds = round(time.monotonic() - load_started, 3)
        reserved_now, allocated_now, _, peak_allocated_now = \
            memory._allocator_stats()
        reserved_delta = memory._delta(reserved_now, before.get("reserved_mb"))
        alloc_floor = memory._delta(peak_allocated_now,
                                    before.get("allocated_mb"))
        payload = memory.finish_load(before, instance)

        document["device"] = device_block(memory, pin)
        free_mb, total_mb, free_source = memory.free_total_mb()
        document["free"] = {
            "free_source": free_source, "free_mb": free_mb,
            "total_mb": total_mb, "tiers": probe_free_tiers(memory),
        }
        document["base"] = {
            "load_seconds": load_seconds,
            "base_mb": payload.get("base_mb"),
            "base_method": payload.get("base_method"),
            "allocated_at_load_mb": payload.get("allocated_at_load_mb"),
            "reserved_at_load_mb": payload.get("reserved_at_load_mb"),
            "dtype": payload.get("dtype"),
            "dtype_method": payload.get("dtype_method"),
            "gpu_name": payload.get("gpu_name"),
            "gpu_total_mb": payload.get("gpu_total_mb"),
            "torch_version": payload.get("torch_version"),
            "memory": payload.get("memory"),
            "tiers": probe_base_tiers(memory, before, reserved_now,
                                      reserved_delta, alloc_floor),
        }

        # --- batch ---
        price, canvas = probe.batch_pricer(packing, resolved["cost"], instance)
        try:
            from inferio.impl import utils as impl_utils
        except Exception:
            impl_utils = None

        def counter(name: str) -> int:
            reader = getattr(impl_utils, name, None) if impl_utils else None
            try:
                return int(reader()) if reader else 0
            except Exception:
                return 0

        def run_batch(count: int) -> Dict[str, Any]:
            batch_inputs = probe.build_inputs(items, count, args.mode, {})
            units = price(batch_inputs)
            free_before, free_before_source = memory._free_mb()
            halvings_before = counter("total_oom_halvings")
            index_before = counter("total_index_limit_events")
            state = memory.begin_batch()
            failure: Optional[BaseException] = None
            try:
                instance.predict(batch_inputs)
            except Exception as exc:
                failure = exc
            absorbed = max(0, counter("total_oom_halvings") - halvings_before)
            index_events = max(
                0, counter("total_index_limit_events") - index_before)
            oom_class = packing.classify_oom(failure, absorbed)
            measurement = memory.measure_batch(
                state, items=count, units=units, oom=oom_class is not None,
                oom_class=oom_class, free_mb=free_before,
                free_source=free_before_source,
            )
            duration_ms = measurement.get("duration_ms") or 0.0
            record = dict(measurement)
            record.update({
                "unit": resolved["cost"]["unit"],
                "aggregation": resolved["cost"]["aggregation"],
                "canvas_pixels": canvas,
                "ok": failure is None,
                "exception_type": type(failure).__name__ if failure else None,
                "message_head": str(failure)[:200] if failure else None,
                "oom_class": oom_class,
                "absorbed_halvings": absorbed,
                "index_limit_events": index_events,
                "units_per_s": (round(units / (duration_ms / 1000.0), 3)
                                if duration_ms > 0 else None),
            })
            return record

        if not args.quiet:
            print(f"selftest: running one batch of {args.batch}",
                  file=sys.stderr)
        document["batch"] = run_batch(args.batch)

        # --- release ---
        pool_before = memory.pool_stats_mb()
        released = memory.empty_cache()
        pool_after = memory.pool_stats_mb()
        document["release"] = {
            "empty_cache": released,
            "reserved_before_mb": pool_before[0],
            "allocated_before_mb": pool_before[1],
            "reserved_after_mb": pool_after[0],
            "allocated_after_mb": pool_after[1],
            "sample": memory.device_memory_sample(),
        }

        # --- induced failure ---
        if args.induce_oom:
            if not args.quiet:
                print("selftest: inducing a failure", file=sys.stderr)
            document["oom"] = induce_oom(
                memory, packing, run_batch, args.batch,
                document["batch"].get("units_per_s"),
                document["device"].get("gpu_total_mb") or total_mb,
                args.quiet,
            )
        else:
            document["oom"] = {"requested": False}
    finally:
        if instance is not None:
            try:
                unload = getattr(instance, "unload", None)
                if unload is not None:
                    unload()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"selftest: unload failed: {exc}", file=sys.stderr)
        del instance
        try:
            memory.empty_cache()
        except Exception:
            pass

    free_after_mb, _, free_after_source = memory.free_total_mb()
    document["released"] = {"free_mb": free_after_mb,
                            "free_source": free_after_source}
    line, degraded = verdict_line(document)
    document["degraded"] = degraded
    document["verdict"] = line

    print_document(document, sys.stdout)
    print(f"device free after teardown: {free_after_mb} MiB "
          f"({free_after_source})")
    if args.json_path:
        path = Path(args.json_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(document, indent=1, default=str),
                        encoding="utf-8")
        if not args.quiet:
            print(f"selftest: wrote {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
