# ROCm parity for batch calibration — gap analysis and decided design

Status: **ship blocker; design decided 2026-07-31** (gaps enumerated
2026-07-30). The batch-calibration system (`batch-calibration-design.md`,
implemented on this branch) must reach complete ROCm parity before release.
No AMD hardware exists on the dev box and CI has no GPUs at all, so the
design below is built to be **correct without hardware verification**: every
assumption that could only be proven on a real ROCm machine is either
replaced by a source that cannot disagree with itself, or guarded by a
runtime cross-check that degrades to the unpriced dispatch path instead of
mis-pricing. "Verified" still means "verified on real hardware by a person"
— but nothing below *requires* that verification to be safe to ship.

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
   FASTEST_FIRST reordering on HIP.
4. **torch 2.11+rocm7.2** (our `rocm` extra, Linux x86_64 only) has the
   full hipified `torch.cuda.*` memory API: allocator statistics are the
   same code path; `mem_get_info` maps to `hipMemGetInfo` but its "free"
   was **historically process-local** on HIP (ROCm/hip#348), can raise
   `RuntimeError` in containers/partitioned GPUs, and must not be treated
   as an authoritative device-wide reading. `torch.version.hip` is set,
   `torch.version.cuda` is None, `torch.__version__` looks like
   `"2.11.0+rocm7.2"`. OOM raises the same `torch.OutOfMemoryError` class
   with the CUDA message text hipified (`"HIP out of memory. …"`), so the
   existing exception-class-based OOM handling works unchanged.
5. **Linux dGPU OOM is crisp**: `hipMalloc` allocates VRAM only, never GTT
   — no WDDM-style silent spillover. (APUs/iGPUs are an exception: VRAM
   carve-out vs GTT reporting is inconsistent across kernels; see G8.)
   ROCm-on-Windows *does* have WDDM spillover, but our `rocm` extra cannot
   install on Windows (pyproject markers), so it is structurally out of
   scope.
6. **amd-smi CLI is unreliable as a probe foundation**: JSON schema
   changed shape at ROCm 6.1 (bare values → `{"value","unit"}` objects)
   and keeps drifting; error paths print human text and may still exit 0;
   the binary is not on PATH on bare installs (`/opt/rocm/bin`); NixOS
   deployments package `rocm-smi` but not amd-smi
   (`contrib/package/nix/rocm-packages.nix`). Marketing names are good on
   Instinct, pci.ids-style or generic on consumer.
7. **The kernel's sysfs interfaces are the stable substrate.** KFD
   topology (`/sys/class/kfd/kfd/topology/nodes/<n>/properties`) is the
   *same enumeration source ROCr itself reads*, and carries per-node
   `location_id`/`domain` (→ PCI BDF), `unique_id` (when fused), and
   `gfx_target_version`. The amdgpu driver exposes per-board
   `mem_info_vram_total` / `mem_info_vram_used` (and `unique_id`) on the
   PCI device directory, and per-process VRAM via DRM fdinfo
   (`/proc/self/fdinfo/<fd>`: `drm-pdev`, `drm-memory-vram`,
   `drm-client-id`) — readable without root for our own process and
   immune to PID-namespace issues by construction.

## Design principles

- **Sysfs-first, subprocess-second.** Everything admission-critical
  (enumeration order, board identity, VRAM totals, free memory,
  per-process footprint) comes from kernel sysfs/procfs interfaces that
  cannot disagree with what the HIP runtime sees, because they are the
  interfaces the runtime itself (or its driver) is built on. The SMI CLIs
  are used only for the one cosmetic fact sysfs lacks — the marketing
  name — and any failure there degrades to a deterministic fallback name,
  never to a lost board.
- **Runtime self-verification replaces hardware verification.** Every
  remaining cross-tool assumption (KFD node order = HIP device order,
  pin honoured, fdinfo attribution) is checked at worker registration
  against what the worker actually reports; a failed check refuses
  admission for that replica (unpriced path — today's behaviour) and logs
  loudly enough for a field report to diagnose. A wrong assumption can
  cost a ROCm host the ledger; it can never mis-price a grant.
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
   GPU nodes (`simd_count > 0`), **in ascending node order** — this order
   *is* the ROCr enumeration order, hence the HIP device order on an
   unrestricted host. Row index = position in this list, not the node id.
2. Per node read `location_id` + `domain` → the PCI BDF
   (`0000:03:00.0` form), `unique_id` (may be absent/0), and
   `gfx_target_version` (kept per row; see D7).
3. From `/sys/bus/pci/devices/<bdf>/`: `mem_info_vram_total` → `total_mb`.
   A node whose BDF or VRAM total cannot be read makes the whole probe
   unknown (same all-or-nothing identity rule as the CUDA parser).
4. **Board key** (the ledger/config/pin identity, `GpuInfo::uuid`):
   `GPU-<16 lower hex>` from `unique_id` when it is present, nonzero and
   unique across the host's boards — the same string ROCR accepts and
   rocminfo prints; otherwise the synthetic **`GPU-BDF-<bdf>`**, stable
   across reboots by bus location. Both satisfy the existing `GPU-`
   prefix convention. Mixed hosts (some boards fused, some not) key each
   board by the best form it individually supports; a *duplicate*
   `unique_id` pair demotes both to BDF form.
5. **Name**: `amd-smi static --json` (PATH, then `/opt/rocm/bin`, then
   `/opt/rocm-*/bin`), joined to rows by BDF; `rocm-smi
   --showproductname --json` as fallback; on any failure the
   deterministic fallback `AMD gfx<target> (<N> GB)`. Name failures never
   degrade the inventory — they only cost profile-key quality. The
   detection rule for both CLIs is "parseable JSON or bust" (exit codes
   are unreliable); numeric fields must accept bare numbers, the 6.1+
   `{"value","unit"}` shape, and `"N/A"`.
6. `compute_cap` stays `None` (HIP has no analogue; the per-row Option
   already tolerates it). Default placement's capability ranking then
   falls through to lowest-index — acceptable, and D7 records the datum a
   future gfx-aware ranking would need.

Ambient visibility handling mirrors the CUDA rules over the ROCm env set,
checked in this order: any of `ROCR_VISIBLE_DEVICES`,
`HIP_VISIBLE_DEVICES`, `CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL` set
and non-empty counts as a restriction. All-UUID-form entries (`GPU-…`,
matchable to our unique_id-keyed rows) restrict the inventory; any
index-form entry blanks it (workers then inherit the ambient env verbatim,
as today). Index mapping is *knowable* on ROCm in the single-var
`ROCR`-only case, but the layered two-var semantics make composition
subtle enough that the conservative CUDA-symmetric rule wins for v1 —
the cost is "no ledger on ambient-index-restricted hosts", which is
exactly today's behaviour.

### D2 (G2) — Pinning: `HIP_VISIBLE_DEVICES=<row index>`

The spawn layer writes the env var the backend dictates:
`CUDA_VISIBLE_DEVICES=<uuid>` on CUDA (unchanged);
**`HIP_VISIBLE_DEVICES=<row index>`** on ROCm, where the index is the
board's position in the KFD-node-order inventory (D1), which is the HIP
device order on an unrestricted host. `WorkerSpawnConfig` learns the
accelerator (or a resolved "pin env var" field) to select the form;
`resolve_pin` on ROCm resolves registry `devices` entries (board key or
index) to the row index. Unresolvable pins (a board key or index the
inventory does not list, a list, a leftover) pass through on
`HIP_VISIBLE_DEVICES` verbatim with a warning, preserving operator
intent exactly as the CUDA path does — they are never promoted to
`ROCR_VISIBLE_DEVICES`, whose UUID form would silently change meaning.

Why HIP and not ROCR form: torch honours HIP first on every relevant
version, torch < 2.6 (possible in user-managed venvs) crashes outright
when ROCR is set, and AMD documents HIP-level filtering as the
application-scoped mechanism. `CUDA_VISIBLE_DEVICES` is deliberately NOT
also set on ROCm (it is a HIP alias; setting both is documented as
"unintended behaviour" territory). The accelerator sentinel's HSA/MIOpen
worker env (`accelerator_env::worker_env`) composes untouched — it never
sets visibility vars. eocr's internal `cuda:0` single-device string is a
no-op under a single visible device.

The enumeration-order assumption (KFD node order = HIP order) is the one
load-bearing unverifiable here, and D3's registration cross-check is its
guard: a wrong order pins a worker to board A while believing it is board
B, the worker's self-reported BDF then fails to match the pinned row, and
the replica is refused admission (unpriced) with a log line naming both
BDFs — visible, safe, diagnosable from a field report.

### D3 (G3) — Worker identity: self-reported PCI BDF from DRM fdinfo

`torch.cuda.get_device_properties(0).uuid` is NOT used on HIP (garbage or
duplicate on consumer boards). Instead the worker senses which board it
is actually on from its own kernel state: scan `/proc/self/fdinfo/*` for
amdgpu DRM entries and read **`drm-pdev`** (the PCI BDF of the device the
process holds). Deduplicate by `drm-client-id`; exactly one distinct BDF
→ that is the identity; zero or several → no identity (unpriced). Runs
only when torch reports CUDA initialized (same `is_initialized` gate as
every other probe), requires no root, works in containers.

Wire change (additive, protocol doc updated): `LoadReport` gains
`gpu_bdf`. `register_worker` keying becomes: exact `gpu_uuid` match
(CUDA path, unchanged) → else `gpu_bdf` match against the inventory rows'
BDF (ROCm path). Before admitting on a BDF match, the **plausibility
cross-check** runs: the worker's torch-reported `total_mb` (already in
the memory sample) must agree with the inventory row's total within a
tolerance (±5% or ±512 MB, whichever is larger — allocator/driver
reserves shave a little). Mismatch → refuse admission, log both values.
On single-board hosts a worker with no BDF (ancient kernel without
fdinfo memory keys) may fall back to the single inventory row iff the
total-memory check passes — the NVML single-GPU fallback's twin.

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
- **Per-process tier 1 (ROCm)**: sum `drm-memory-vram` across the
  process's own distinct DRM clients from `/proc/self/fdinfo` —
  `base_method: "fdinfo"` (new provenance value; the orchestrator and
  calibration store treat `base_method` as an opaque string, so this is
  additive). No amdsmi Python dependency (it is not on PyPI); no root;
  no PID-namespace caveat, because the worker reads *itself*. pynvml
  paths die naturally on ROCm (nvmlInit fails once, logged once).
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
zero). No subprocess, no 5 s timeout, no amd-smi runtime dependency —
which also means **no Nix packaging change is required for the ledger to
function**; amd-smi remains an optional name-quality improvement.

### D6 (G6) — Calibration store keying

Already ROCm-ready: `backend = "rocm"` flows from the setup sentinel into
every profile key, `"2.11.0+rocm7.2"` works through the exact-then-
`major.minor` torch fallback, and ROCm profiles can never contaminate
CUDA ones. The `gpu` key component is the inventory board *name* (D1.5):
amd-smi market name when available, else the deterministic
`AMD gfx<target> (<N> GB)` fallback — deterministic per host either way,
and documented in the calibration README so future shipped ROCm baselines
key on the amd-smi name form. No shipped ROCm baselines initially (none
can be measured); add a rocm-keyed round-trip test.

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
  `nodes/<n>/properties`, fake PCI device dirs) — same fixture style as
  the nvidia-smi parse tests; covers fused/absent/duplicate `unique_id`,
  missing VRAM files, APU-shaped nodes.
- D1.5 name-join parsers against **captured amd-smi/rocm-smi JSON** in
  both schema generations (pre-6.1 bare values, 6.1+ `{"value","unit"}`,
  `"N/A"` variants) — samples exist in AMD's docs/blogs and are cited in
  the research notes.
- D2 pin-form selection and resolve_pin-on-ROCm unit tests; ambient
  restriction rules over the four env vars.
- D3 fdinfo parser fixtures (multi-fd dedupe by client id, multi-BDF
  ambiguity, missing keys) and the registration cross-check paths
  (BDF match, total mismatch, single-board fallback) against synthetic
  inventories.
- D4 wire round-trips for `gpu_bdf`, `free_source: "sysfs"`,
  `base_method: "fdinfo"`; ledger authoritative-source rule.
- D6 rocm-keyed store round-trip.

Needs a field pass on real hardware (none blocks shipping; every failure
mode degrades to unpriced + a diagnostic log):

- KFD-node-order = HIP-order on multi-GPU hosts (guarded by D3's check;
  the refusal log names both BDFs, so one volunteer report confirms or
  refutes it).
- fdinfo `drm-memory-vram` magnitudes vs allocator expectations; HIP
  context size for the alloc-delta constant.
- amd-smi name join on consumer boards; end-to-end grants/trim/knee on
  an AMD board (mirror of the CUDA dogfooding list).

## Implementation order

1. D1 + D5 (sysfs inventory + memory refresh, fixtures-first; amd-smi
   name join behind it) — lights up ledger boards on ROCm.
2. D2 pin plumbing (backend-aware spawn env, resolve_pin, ambient rules).
3. D3 worker fdinfo identity + `gpu_bdf` wire field + registration
   cross-check (this is the safety net for 2; land together or 3 first).
4. D4 worker sysfs/fdinfo memory tiers + ledger `"sysfs"` authority.
5. D6 test + D7/D8 statements; update `batch-calibration-design.md`'s
   ROCm open question, the protocol doc's memory-sensing section, and the
   README accelerator docs.
