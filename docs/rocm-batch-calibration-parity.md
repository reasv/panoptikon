# ROCm parity for batch calibration — gap analysis and decided design

Status: **ship blocker; design decided 2026-07-31, amended same day after
adversarial review** (gaps enumerated 2026-07-30). The batch-calibration
system (`batch-calibration-design.md`, implemented on this branch) must
reach complete ROCm parity before release. No AMD hardware exists on the
dev box and CI has no GPUs at all, so the design below is built to be
**correct without hardware verification**: every assumption that could
only be proven on a real ROCm machine is either replaced by a source that
cannot disagree with itself, or guarded by a runtime cross-check that
degrades to the unpriced dispatch path instead of mis-pricing. "Verified"
still means "verified on real hardware by a person" — but nothing below
*requires* that verification to be safe to ship.

The 2026-07-31 review amendments (F1–F8, folded in below) fixed two
blockers in the first draft: worker identity now comes from torch's PCI
fields rather than an fdinfo uniqueness rule that could never hold on
multi-GPU hosts, and the inventory's index space is computed over the
*openable* render nodes so containers with a `/dev/dri` subset pin
correctly instead of falling to CPU.

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
  unpriced hosts and a parity motivation in itself. Note the flip side:
  every "degrades to unpriced" below therefore lands on *registry
  defaults*, not on the user's old tuned numbers — degradation is safe,
  but it is not free, which is why the design works to make the priced
  path the common case rather than relying on the fallback.

## Research findings (2026-07-31, documentation/source research)

Facts established from AMD/PyTorch documentation, source code and issue
trackers. These falsify two approaches the original gap analysis suggested
and dictate the design that follows.

1. **HIP device index ≠ amd-smi/rocm-smi index.** Confirmed real, not
   hypothetical: on a 4-GPU box, `HIP_VISIBLE_DEVICES=0` drove rocm-smi's
   GPU2 (pytorch#131901). HIP enumerates in ROCr/KFD topology-node order;
   the SMI CLIs sort by PCI BDF. torch itself carries a translation shim
   between the two orderings. **Any design that correlates devices by
   ordinal across the two vocabularies is unsound.**
2. **There is no single GPU-UUID vocabulary.** amd-smi synthesizes an
   8-4-4-4-12 UUID from PCI device ID + ASIC serial; KFD/rocminfo report
   `GPU-<16 hex>` (the raw `unique_id` sysfs value); torch ≥ 2.5 renders a
   third form from the same serial. On consumer boards without a fused
   serial, `unique_id` is absent (kernel: GFX9+ only, and even then not
   universal — rocminfo prints the literal placeholder `GPU-XX`), and the
   derived forms degenerate to identical values for same-model cards.
   **String-matching identities produced by two different tools cannot be
   the ledger join, and UUIDs cannot be assumed unique or present.**
3. **Visibility env semantics** (ROCm "GPU isolation" docs + torch
   source): `ROCR_VISIBLE_DEVICES` filters at the ROCr/KFD layer (accepts
   indices and `GPU-<16hex>` UUIDs); `HIP_VISIBLE_DEVICES` and its
   CUDA-compat alias `CUDA_VISIBLE_DEVICES` filter at the HIP layer,
   indices only, indexing into the ROCR-filtered set when both are set.
   torch-ROCm honours `HIP_VISIBLE_DEVICES` first, then
   `ROCR_VISIBLE_DEVICES`, then `CUDA_VISIBLE_DEVICES` — and **torch < 2.6
   crashes at init when `ROCR_VISIBLE_DEVICES` is set** (fixed by
   pytorch#142292). There is no `CUDA_DEVICE_ORDER` analogue and no
   FASTEST_FIRST reordering on HIP. Because HIP filters *above* ROCr, a
   HIP-pinned process still initializes ROCr agents for (and holds render
   nodes of) every ROCR-visible board — per-process kernel state is
   **not** scoped to the HIP-visible device (review F1).
4. **torch 2.11+rocm7.2** (our `rocm` extra, Linux x86_64 only) has the
   full hipified `torch.cuda.*` memory API: allocator statistics are the
   same code path; `mem_get_info` maps to `hipMemGetInfo` but its "free"
   was **historically process-local** on HIP (ROCm/hip#348), can raise
   `RuntimeError` in containers/partitioned GPUs, and must not be treated
   as an authoritative device-wide reading. `torch.version.hip` is set,
   `torch.version.cuda` is None, `torch.__version__` looks like
   `"2.11.0+rocm7.2"`. `get_device_properties(0)` additionally exposes
   `pci_domain_id` / `pci_bus_id` / `pci_device_id` (from
   `hipDeviceProp_t`), which identify the board in the one vocabulary the
   kernel also speaks — the PCI BDF. OOM raises the same
   `torch.OutOfMemoryError` class with the CUDA message text hipified
   (`"HIP out of memory. …"`), so the existing exception-class-based OOM
   handling works unchanged.
5. **Linux dGPU OOM is crisp**: `hipMalloc` allocates VRAM only, never GTT
   — no WDDM-style silent spillover. (APUs/iGPUs are an exception: VRAM
   carve-out vs GTT reporting is inconsistent across kernels; see G8.)
   ROCm-on-Windows *does* have WDDM spillover, but our `rocm` extra cannot
   install on Windows (pyproject markers), so it is structurally out of
   scope.
6. **amd-smi CLI is unreliable as a probe foundation**: JSON schema
   changed shape at ROCm 6.1 and keeps drifting; error paths print human
   text and may still exit 0; the binary is not on PATH on bare installs;
   NixOS deployments package `rocm-smi` but not amd-smi. Marketing names
   are good on Instinct, pci.ids-style or generic on consumer. (After
   review F7 the design no longer consumes either CLI at all.)
7. **The kernel's sysfs interfaces are the stable substrate.** KFD
   topology (`/sys/class/kfd/kfd/topology/nodes/<n>/properties`) is the
   *same enumeration source ROCr itself reads*, and carries per-node
   `location_id`/`domain` (→ PCI BDF), `drm_render_minor` (→ the
   `/dev/dri/renderD<minor>` node ROCr must open to use the board),
   `unique_id` (when fused), and `gfx_target_version`. The amdgpu driver
   exposes per-board `mem_info_vram_total` / `mem_info_vram_used` on the
   PCI device directory, and per-process VRAM via DRM fdinfo
   (`/proc/self/fdinfo/<fd>`: `drm-pdev`, `drm-client-id`, and
   `drm-memory-vram` — which kernel docs mark as amdgpu's deprecated
   alias for `drm-resident-vram`, so both spellings must parse) —
   readable without root for our own process and immune to PID-namespace
   issues by construction. fdinfo memory stats for KFD/compute
   allocations are VM-walk-based and comparatively recent; older kernels
   may under-report them (guarded in D4).

## Design principles

- **Sysfs-first; no SMI CLI at all.** Everything admission-critical
  (enumeration order, board identity, VRAM totals, free memory,
  per-process footprint) comes from kernel sysfs/procfs interfaces that
  cannot disagree with what the HIP runtime sees, because they are the
  interfaces the runtime itself (or its driver) is built on. After review
  F7, the marketing name is not consumed either: the board's display and
  profile-key name is a deterministic string derived from sysfs facts, so
  it can never flip with the environment.
- **Runtime self-verification replaces hardware verification.** Every
  remaining cross-tool assumption (KFD node order = HIP device order, pin
  honoured, sysfs mapping) is checked at worker registration against what
  the worker actually reports through an *independent* source; a failed
  check refuses admission for that replica (unpriced path — today's
  behaviour) and logs loudly enough for a field report to diagnose. A
  wrong assumption can cost a ROCm host the ledger; it can never
  mis-price a grant.
- **One memory vocabulary per host.** The worker's free/total tier and
  the orchestrator's staleness refresh read the *same* sysfs files, so
  the "free-source consistency" rule holds on ROCm by construction
  rather than by matching two drivers' opinions.

## Decisions (mapped to the gap list)

### D1 (G1) — Inventory probe: KFD topology + amdgpu sysfs

`gpu::probe()` grows a backend parameter (the setup-sentinel accelerator is
already resolved before the probe runs — `http.rs` builds the spawn config
from it first). CUDA keeps the nvidia-smi path byte-for-byte. ROCm, on
Linux only:

1. Enumerate `/sys/class/kfd/kfd/topology/nodes/<n>/properties`, filter to
   GPU nodes (`simd_count > 0`), in ascending node order.
2. Per node read `location_id` + `domain` → the PCI BDF
   (`0000:03:00.0` form), `drm_render_minor`, `unique_id` (may be
   absent/0), and `gfx_target_version` (kept per row; see D7).
3. **Openability filter (review F2):** a GPU node whose
   `/dev/dri/renderD<minor>` does not exist or cannot be opened (a
   container granted a `/dev/dri` subset sees the *whole* host topology
   but can only use the granted nodes) is **excluded**, and the row
   indices — the HIP device indices D2 pins with — are positions within
   the *openable* subset, which reconstructs ROCr's actual enumeration.
   This is what makes containerized multi-GPU hosts first-class instead
   of pinning out of range and silently falling to CPU. If *no* GPU node
   is openable, the inventory is unknown.
4. From `/sys/bus/pci/devices/<bdf>/`: `mem_info_vram_total` → `total_mb`.
   An openable node whose BDF or VRAM total cannot be read makes the
   whole probe unknown (same all-or-nothing identity rule as the CUDA
   parser). Known cost, accepted for v1: one quirky node — e.g. an APU
   without `mem_info_vram_total` — costs a hybrid host the whole ledger;
   partial inventories are worse because row indices must cover the full
   openable set to mean anything to HIP.
5. **Board key** (the ledger/config/pin identity, `GpuInfo::uuid`):
   `GPU-<16 lower hex>` from `unique_id` when it is present, nonzero and
   unique across the host's boards — the same string ROCR accepts and
   rocminfo prints; otherwise the synthetic **`GPU-BDF-<bdf>`**, stable
   across reboots by bus location. Both satisfy the existing `GPU-`
   prefix convention. Mixed hosts key each board by the best form it
   individually supports; a *duplicate* `unique_id` pair demotes both to
   BDF form.
6. **Name** (review F7): always the deterministic
   **`AMD gfx<target> (<N> GB)`**, derived from `gfx_target_version` and
   the VRAM total rounded to GB. This is the calibration profile keyspace
   and the display name. It is per-silicon (VRAM size separates the
   gfx-sharing SKUs that matter), identical on every host with the same
   board, and — unlike an amd-smi marketing-name join — cannot appear or
   disappear with PATH, packaging, or schema drift, which would orphan
   every local profile, ratchet anchor and knee on the host. A
   marketing-name join can be added later as display-only metadata; it
   must never become the key.
7. `compute_cap` stays `None` (HIP has no analogue; the per-row Option
   already tolerates it). Default placement's capability ranking
   tie-breaks by **VRAM total descending, then lowest index** (review
   F8): on an all-`None` ROCm host this stops a first-enumerated iGPU
   from out-ranking the dGPU, and on CUDA it only reorders equal-cap
   unequal-VRAM hosts, where the bigger board is the strictly better
   default.

**Ambient visibility handling (review F3):** stricter than the CUDA rule.
If *any* of `ROCR_VISIBLE_DEVICES`, `HIP_VISIBLE_DEVICES`,
`CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL` is set non-empty on a ROCm
host, the inventory is blanked and workers inherit the ambient environment
verbatim (today's behaviour). No UUID-form carve-out: an ambient ROCR
filter *changes the HIP index space* (HIP indices count the ROCR-filtered
set in the ambient list's order), so composing our relative index pins on
top of it is exactly the ordinal-correlation mistake finding 1 forbids.
CUDA can afford the UUID carve-out because its pins are absolute UUIDs;
ROCm pins are relative indices, so "symmetric" would not be symmetric.
Cost: ambient-restricted hosts (Slurm-style schedulers set ROCR) stay
unpriced in v1 — safe, documented, and revisitable once the single-var
ROCR-only composition is worth the complexity.

### D2 (G2) — Pinning: `HIP_VISIBLE_DEVICES=<row index>`

The spawn layer writes the env var the backend dictates:
`CUDA_VISIBLE_DEVICES=<uuid>` on CUDA (unchanged);
**`HIP_VISIBLE_DEVICES=<row index>`** on ROCm, where the index is the
board's position in the openable KFD-node-order inventory (D1), which is
the HIP device order on an unrestricted host. `WorkerSpawnConfig` learns
the accelerator (or a resolved "pin env var" field) to select the form;
`resolve_pin` on ROCm resolves registry `devices` entries (board key or
index) to the row index. Pin-string handling differs from CUDA where
verbatim pass-through would change meaning (review F8): an unresolvable
**numeric** pin (an index we cannot see, a comma-separated index list)
passes through verbatim with a warning — HIP accepts indices, so the
operator's intent survives; an unresolvable **non-numeric** pin (a
`GPU-…` leftover, a template) is **not written at all**, with a warning —
HIP accepts only indices, so writing it would hide every device and send
the worker to CPU, which is strictly worse than the no-pin behaviour the
warning preserves.

Why HIP and not ROCR form: torch honours HIP first on every relevant
version, torch < 2.6 (possible in user-managed venvs) crashes outright
when ROCR is set, and AMD documents HIP-level filtering as the
application-scoped mechanism. `CUDA_VISIBLE_DEVICES` is deliberately NOT
also set on ROCm (it is a HIP alias; setting both is documented as
"unintended behaviour" territory). The accelerator sentinel's HSA/MIOpen
worker env (`accelerator_env::worker_env`) composes untouched — it never
sets visibility vars. eocr's internal `cuda:0` single-device string is a
no-op under a single visible device.

The enumeration-order assumption (openable KFD node order = HIP order) is
the one load-bearing unverifiable here, and D3's registration cross-check
is its guard: a wrong order pins a worker to board A while believing it
is board B, the worker's self-reported BDF then fails to match the pinned
row, and the replica is refused admission (unpriced) with a log line
naming both BDFs — visible, safe, diagnosable from a field report.

### D3 (G3) — Worker identity: torch PCI fields; fdinfo demoted (review F1)

`torch.cuda.get_device_properties(0).uuid` is NOT used on HIP (garbage or
duplicate on consumer boards) — in fact the worker **suppresses
`gpu_uuid` entirely when `torch.version.hip` is set** (review F5), so a
rendered third-vocabulary UUID can never collide with anything. Identity
comes from the same call's PCI fields instead: `pci_domain_id`,
`pci_bus_id`, `pci_device_id` formatted as the BDF
`<domain:04x>:<bus:02x>:<device:02x>.0` (AMD GPUs are PCI function 0; the
board's audio function is a separate device). These fields are
device-0-scoped — they describe exactly the board the pin selected —
which is what an fdinfo scan could never give: HIP filters *above* ROCr,
so a pinned worker still holds render nodes for every ROCR-visible board
and "exactly one distinct BDF in fdinfo" would be false on every
multi-GPU host. fdinfo remains the *memory* tier's mechanism (D4),
filtered by this identity BDF rather than expected to be unambiguous.
Fallback when the PCI fields are absent (older torch in a user-managed
venv): the dominant-VRAM fdinfo client (largest `drm-resident-vram` /
`drm-memory-vram`), and if that is also unavailable, no identity —
unpriced.

Wire change (additive, protocol doc updated): `LoadReport` gains
`gpu_bdf` and `gpu_total_mb` (the torch-reported
`get_device_properties(0).total_memory` in MiB). `register_worker`
keying becomes: exact `gpu_uuid` match (CUDA path, unchanged) → else —
explicitly including `gpu_uuid` *present but matching no board* (review
F5) — `gpu_bdf` match against the inventory rows' BDF. Before admitting
on a BDF match, the **plausibility cross-check** runs against an
*independent* source (review F4): the worker's `gpu_total_mb` — which
comes from torch/HIP, not from the sysfs file the inventory total was
read from, so the comparison is never a file against itself — must agree
with the inventory row's total within a tolerance (±5% or ±512 MB,
whichever is larger; allocator/driver reserves and carve-outs shave a
little). Mismatch → refuse admission, log both values and both BDFs. On
single-board hosts a worker with no BDF at all may fall back to the
single inventory row iff the total-memory check passes — the NVML
single-GPU fallback's twin.

### D4 (G4) — Worker memory sensing: sysfs/fdinfo tiers

The `torch.cuda.*` allocator statistics (`memory_reserved`,
`memory_allocated`, `max_memory_allocated`, `empty_cache`,
`reset_peak_memory_stats`) are the same hipified allocator and are used
unchanged, behind the existing `is_initialized` gates.

- **Free/total tier (ROCm)**: read `mem_info_vram_{total,used}` from
  `/sys/bus/pci/devices/<bdf>/` for the board identified per D3 (cached
  BDF, re-resolved on each call until known — mirroring the NVML handle
  retry). Reported with `free_source: "sysfs"`. This is device-wide
  (other processes included) and is the *same file* the orchestrator's
  refresh reads (D5), so the single-vocabulary rule holds exactly.
  `torch.cuda.mem_get_info` remains the last-resort tier with its
  existing `"torch"` label (wrapped in try/except — it can raise on
  HIP in containers), and the ledger continues to treat `"torch"` as
  non-authoritative.
- **Per-process tier 1 (ROCm)**: sum VRAM across the process's own DRM
  clients from `/proc/self/fdinfo` whose `drm-pdev` equals the identity
  BDF (D3), deduplicated by `drm-client-id`, accepting both
  `drm-resident-vram` and the deprecated `drm-memory-vram` spellings
  (review F6) — `base_method: "fdinfo"` (new provenance value; the
  orchestrator and calibration store treat `base_method` as an opaque
  string, so this is additive). No amdsmi Python dependency (not on
  PyPI); no root; no PID-namespace caveat, because the worker reads
  *itself*. **Plausibility floor (review F6):** fdinfo memory stats for
  KFD/compute allocations are VM-walk-based and recent (~kernel 6.x); an
  older kernel can under-report them, and an under-measured base is
  phantom headroom. So an fdinfo reading materially below the worker's
  own `reserved` delta loses to the alloc-delta + context formula — the
  mirror of the existing NVML implausibility guard, pointed the other
  way. pynvml paths die naturally on ROCm (nvmlInit fails once, logged
  once).
- `free_source_is_authoritative` (ledger) adds `"sysfs"` to
  `"nvml" | "nvidia-smi"`. `"torch"` stays non-authoritative — on HIP
  doubly so given the historical process-local `hipMemGetInfo`.
- `CONTEXT_ESTIMATE_MB = 500` stays as the HIP placeholder for the
  alloc-delta tier: no published HIP figure exists; with the fdinfo tier
  available the constant is rarely load-bearing, `IMPLAUSIBLE_SLACK_MB`
  absorbs the error band, and the value is flagged as a
  field-calibration item.

### D5 (G5) — External-usage refresh: sysfs read, no subprocess

`query_memory()` dispatches on backend: ROCm reads
`mem_info_vram_{total,used}` for every inventory board's BDF in one pass
— all-or-nothing like the nvidia-smi parser (one unreadable board makes
the whole reading unknown, so external usage is never silently priced as
zero). No subprocess, no 5 s timeout, no SMI runtime dependency — and no
Nix packaging change is required for the ledger to function. One known
semantic skew (review F8): `total − used` ignores firmware/kernel
reserved carve-outs that nvidia-smi's `memory.free` excludes, so ROCm
free readings run ~100–500 MB optimistic; the ledger's default margin
absorbs it, and the D3 tolerance already accounts for it on the total
side.

### D6 (G6) — Calibration store keying

Already ROCm-ready: `backend = "rocm"` flows from the setup sentinel into
every profile key, `"2.11.0+rocm7.2"` works through the exact-then-
`major.minor` torch fallback, and ROCm profiles can never contaminate
CUDA ones. The `gpu` key component is the inventory board *name* — on
ROCm the deterministic `AMD gfx<target> (<N> GB)` form (D1.6), identical
on every host with the same silicon, so local profiles, shipped
baselines and volunteers' contributions all key alike and the key can
never flip with the environment (review F7). Documented in the
calibration README. No shipped ROCm baselines initially (none can be
measured); add a rocm-keyed round-trip test.

### D7 (G7) — Capability floors: accepted-unknown, datum recorded

Decision: **unknown-never-filters is accepted for ROCm in v1.** The only
shipped floors are CUDA-specific (sm_80 bf16/FA2), Package 1 already made
`select_dtype` skip capability filtering under HIP, and the impls carry
load-time backstops. The inventory rows record `gfx_target_version`
(free from D1), so a future gfx-arch allowlist has its datum without
another probe; the `/metadata` capability overlay simply stays absent on
ROCm hosts.

### D8 (G8) — Windows machinery: structurally dormant, comparator stays

ROCm-on-Windows/WSL is **out of scope and unreachable through managed
setup**: the `rocm` extra carries `sys_platform == 'linux'` markers, so
no supported install has ROCm torch on Windows. The WDDM throughput
comparator is platform-neutral and already runs on Linux-CUDA hosts; it
stays active on ROCm as a generic over-admission guard (research
confirms Linux dGPU OOMs crisply, so it should never fire spuriously —
the 0.4 ratio has the same false-positive margin it has on Linux-CUDA).
APU/iGPU hosts are a documented caveat: kernel/BIOS quirks can misreport
VRAM totals; admission still functions (the numbers are merely worse),
and such hosts were never priced before this branch either.

## What ships without AMD hardware, and how it is validated

Buildable and testable now:

- D1/D5 sysfs probe against **fixture directory trees** (fake
  `nodes/<n>/properties`, fake PCI device dirs, fake `/dev/dri` render
  nodes for the openability filter) — same fixture style as the
  nvidia-smi parse tests; covers fused/absent/duplicate `unique_id`,
  missing VRAM files, APU-shaped nodes, container-subset render nodes.
- D2 pin-form selection and resolve_pin-on-ROCm unit tests, including
  the numeric-verbatim vs non-numeric-don't-set rule; ambient
  restriction blanking over the four env vars.
- D3 BDF formatting from torch PCI fields; fdinfo parser fixtures
  (both memory-key spellings, multi-client filtering by pdev, dedupe by
  client id, missing keys); registration cross-check paths (BDF match,
  total mismatch, uuid-present-but-unmatched fallthrough, single-board
  fallback) against synthetic inventories.
- D4 wire round-trips for `gpu_bdf`, `gpu_total_mb`,
  `free_source: "sysfs"`, `base_method: "fdinfo"`; ledger
  authoritative-source rule; fdinfo-below-reserved plausibility floor.
- D6 rocm-keyed store round-trip.

Needs a field pass on real hardware (none blocks shipping; every failure
mode degrades to unpriced + a diagnostic log):

- Openable-KFD-node-order = HIP-order on multi-GPU hosts (guarded by
  D3's check; the refusal log names both BDFs, so one volunteer report
  confirms or refutes it).
- fdinfo `drm-resident-vram` magnitudes vs allocator expectations on
  ROCm-relevant kernels; HIP context size for the alloc-delta constant.
- End-to-end grants/trim/knee on an AMD board (mirror of the CUDA
  dogfooding list).

## Implementation order

1. D1 + D5 (sysfs inventory + memory refresh, fixtures-first) — lights
   up ledger boards on ROCm.
2. D2 pin plumbing (backend-aware spawn env, resolve_pin, ambient rules).
3. D3 worker identity (torch PCI fields + gpu_bdf/gpu_total_mb wire
   fields) + registration cross-check (this is the safety net for 2).
4. D4 worker sysfs/fdinfo memory tiers + ledger `"sysfs"` authority +
   plausibility floor.
5. D6 test + D7/D8 statements; update `batch-calibration-design.md`'s
   ROCm open question, the protocol doc's memory-sensing section, and the
   README accelerator docs.
