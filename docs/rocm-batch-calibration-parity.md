# ROCm parity for batch calibration — gap analysis

Status: **ship blocker**. Decided 2026-07-30: the batch-calibration system
(`batch-calibration-design.md`, implemented on this branch) must reach
complete ROCm parity before release. This document enumerates every known
gap between the CUDA implementation and an equivalent ROCm one, what each
gap costs today, and what closing it needs. Nothing GPU-real is exercised
in CI (CUDA included), so "verified" below always means "verified on real
hardware by a person", per project policy.

## Behaviour on a ROCm host today (degraded, not broken)

Without an inventory, the whole admission system is inert and dispatch
takes the unpriced compatibility path:

- `gpu::probe()` shells out to nvidia-smi only → `GpuInventory` unknown →
  no ledger boards, no grants, no pinning, no calibration, no trim.
- Windows/batches are bounded by `user cap → registry default_batch_size →
  server default_max_batch` (`dispatch.rs` unpriced path) — effectively the
  pre-branch behaviour.
- Impl-side `run_with_oom_retry` halving is the recovery mechanism, and
  ROCm-on-Linux raises crisp OOM exceptions (no WDDM sysmem fallback), so
  the backstop is sound there.
- The auto/cap migration still nulls stored batch numbers: a ROCm user who
  had tuned numbers *below* the registry defaults falls back to those
  defaults until they re-enter a cap. This is the one real regression on
  unpriced hosts and a parity motivation in itself.

## Gaps

### G1 — GPU inventory probe (orchestrator)

`panoptikon/src/inferio/gpu.rs` probes
`nvidia-smi --query-gpu=index,uuid,name,memory.total,compute_cap`.
Needs an AMD twin producing the same `GpuInventory`/`HostComputeCaps`
shapes: `amdsmi` (preferred, JSON output) or `rocm-smi` fallback —
index, board UUID, marketing name, VRAM total. Compute-capability has no
ROCm equivalent; the per-row `Option<compute_cap>` already tolerates
absence (identity survives, capability floors stay unknown — see G7).
Ambient-visibility handling must read the ROCm env set
(`HIP_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES`; whether torch-ROCm also
honours `CUDA_VISIBLE_DEVICES` is **unverified**) with the same
UUID-restricts / index-blanks rules.

### G2 — Worker→GPU pinning (spawn)

`worker.rs` writes `CUDA_VISIBLE_DEVICES=GPU-<uuid>`; HIP does not accept
that form. The design already anticipates this: ROCm needs the **index
form plus a spawn-time index→UUID mapping** (`HIP_VISIBLE_DEVICES=<n>`),
with the probe's enumeration order verified stable against HIP's device
order (the CUDA `FASTEST_FIRST` trap has an unknown ROCm analogue —
verify). Must compose with the ROCm env the accelerator setup sentinel
already injects via `accelerator_env::worker_env` (HSA overrides etc.),
and with eocr's internal HIP single-device pin (`eocr.py` — under
universal pinning the worker sees one device, so the impl pin should be a
no-op; verify).

### G3 — Ledger identity from the worker

`python/inferio_worker/memory.py::device_identity` reports
`f"GPU-{torch.cuda.get_device_properties(0).uuid}"` — the authoritative
ledger key on CUDA. Whether torch-on-HIP exposes `.uuid`, and whether its
string form matches what amdsmi reports for the same board, is
**unverified**. Without a matching identity, load reports cannot land on
a ledger board even once G1 exists. Fallback design if absent: worker
reports the index + name and the orchestrator maps via the inventory.

### G4 — Memory sensing in the worker

The `torch.cuda.*` namespace (mem_get_info, memory_reserved,
max_memory_allocated, empty_cache, reset_peak_memory_stats) is exposed by
ROCm torch builds and everything is `is_initialized`-gated — expected to
work but **unverified** on HIP, including MiB semantics and the
free-source consistency rules. Base-measurement tier 1 (pynvml
per-process) is NVIDIA-only: ROCm needs an `amdsmi` per-process path
(Linux-only, container PID-namespace caveat applies identically) or it
degrades to the free-delta tier, which must then be validated for HIP
context sizes (the `CONTEXT_ESTIMATE_MB` constant is CUDA-sized).

### G5 — External-usage refresh

`gpu.rs::query_memory()` (staleness refresh: one coherent
total/free snapshot) is nvidia-smi only. Needs the amdsmi/rocm-smi
equivalent behind the same 5 s timeout and all-or-nothing parse.

### G6 — Calibration store keying

Already ROCm-ready on paper: `backend = "rocm"` is a first-class key
component, so ROCm profiles never cross-contaminate CUDA ones and shipped
ROCm baselines are expressible. Needs only real entries plus verification
that torch-ROCm version strings ("2.x.y+rocmZ") flow through the
`torch` key and the major.minor fallback sensibly.

### G7 — Capability floors and dtype negotiation (Package-1 interplay)

`min_compute_capability` gating and `select_dtype`'s
`cuda_capability()` return `None`/unknown on HIP, so floors never gate
and dtype negotiation degrades on ROCm. Not strictly part of this
branch, but "complete parity" includes deciding the ROCm analogue
(gfx-arch allowlists? feature probes?) or explicitly accepting
unknown-never-filters for AMD.

### G8 — Windows-specific machinery is N/A, verify it stays dormant

The WDDM synthetic negative sample and sysmem-fallback reasoning are
NVIDIA-Windows-only. ROCm-on-Linux should never trip the collapse
comparator spuriously; ROCm-on-Windows/WSL is out of scope (state it).

## What can be built/tested without AMD hardware

- G1/G5 parse paths against captured amdsmi/rocm-smi output fixtures
  (contributed by a volunteer or from AMD docs) — same fixture style as
  the existing nvidia-smi parse tests.
- G2 env-var plumbing unit tests (pin form selection per backend).
- G6 store keying tests (backend="rocm" round trips already covered
  generically; add a rocm-keyed case).

Everything else — G3's identity equivalence, G4's allocator semantics,
enumeration-order stability, end-to-end grants on an AMD board — needs a
real ROCm machine. No AMD hardware exists on the dev box and CI has no
GPUs at all, so parity sign-off requires a manual validation pass
(mirror of the CUDA dogfooding items in `batch-calibration-design.md`).

## Suggested order

1. G1 + G5 (one amdsmi probe module, fixtures-first) — lights up the
   ledger.
2. G3 verification on hardware → pick worker-identity strategy.
3. G2 pinning (index form + mapping) once enumeration order is verified.
4. G4 amdsmi per-process base tier; validate free-delta + context
   estimate on HIP.
5. G6 baselines + G7 decision + G8 statement; update the design doc's
   ROCm open question and the README's accelerator docs.
