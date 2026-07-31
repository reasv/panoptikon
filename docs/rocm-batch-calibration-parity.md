# ROCm parity for batch calibration — gap analysis and decided design

Status: **design decided 2026-07-31, amended same day after adversarial
review, and implemented the same day** — steps 1–5 (D1+D5, D2, D3, D4,
D6–D8) on this branch (gaps enumerated 2026-07-30). What remains is the
field pass listed at the end: no AMD hardware exists on the dev box, so
every runtime cross-check below has been exercised against fixtures and
none against a real board. The batch-calibration system
(`batch-calibration-design.md`, implemented on the same branch) was a ship
blocker until this landed. CI has no GPUs at all either, so the design
below was built to be **correct without hardware verification**: every assumption that could
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

## Behaviour on a ROCm host before this branch (degraded, not broken)

This is what the gap analysis started from, and it is still what a ROCm
host falls back to wherever the design below refuses to price something.
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
   is openable, the inventory is unknown. Accepted cost: briefly opening
   every render node at startup can resume a runtime-suspended GPU, once
   per boot.
4. From `/sys/bus/pci/devices/<bdf>/`: `mem_info_vram_total` → `total_mb`.
   The all-or-nothing rule (the CUDA parser's, for the same reason) covers
   the whole identity, not just that file. Any **one** of these on any
   **one** openable node makes the whole probe unknown:
   - the KFD node's `properties` unreadable for a reason other than
     "a device cgroup hides this board" (that one is a skip — ROCr cannot
     see it either, so excluding it *reconstructs* the runtime's set);
   - `simd_count` **absent** — an explicit 0 is a CPU node and skips, but an
     absent key means the file is not the shape we parse, and reading it as
     a CPU node would drop a board and shift every later row's index;
   - **an APU** — a node reporting both SIMDs and CPU cores, which is how
     KFD models an integrated part. Detected positively rather than left to
     the VRAM rule, because amdgpu *does* publish `mem_info_vram_total` for
     iGPUs: the BIOS UMA carve-out, commonly 512 MB. Without the explicit
     test the host would be priced against the carve-out and every grant
     would collapse to batch-1;
   - no usable `location_id`/`domain`, i.e. no derivable PCI address;
   - no readable, nonzero `mem_info_vram_total`;
   - an **undecodable `gfx_target_version`** (0, or a minor/stepping outside
     a hex digit) — the board's name is the calibration keyspace and must
     never be a placeholder;
   - a duplicate board key with no PCI address to fall back to;
   - several KFD nodes behind one PCI device (a partitioned board).

   Known cost, accepted for v1: one quirky node costs a hybrid host the
   whole ledger. Partial inventories are worse, because row indices must
   cover the full openable set to mean anything to HIP. Every one of these
   paths logs a WARN naming the node or board; the probe additionally emits
   one summary WARN for the two paths that name nothing (no KFD GPU nodes
   at all, and no openable render node), so a ROCm host is never *silently*
   unpriced.
5. **Board key** (the ledger/config/pin identity, `GpuInfo::uuid`):
   `GPU-<16 lower hex>` from `unique_id` when it is present, nonzero and
   unique across the **openable** boards (the post-filter set, i.e. the
   rows this process will actually ledger — a cgroup-hidden sibling
   that happens to share a serial is not one of them) — the same string
   ROCR accepts and rocminfo prints; otherwise the synthetic **`GPU-BDF-<bdf>`**, stable
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

Blanking the inventory withdraws the pins *we* derive; it does not decide
what happens to a pin the **operator** wrote in the registry. That depends
on which layer their restriction sits in, so the probe records the kind
alongside the blanked inventory (`rocm::ambient_hip_restriction`, carried on
`MemoryBackend::RocmSysfs`) and D2 acts on it.

### D2 (G2) — Pinning: `HIP_VISIBLE_DEVICES=<row index>`

The spawn layer writes the env var the backend dictates:
`CUDA_VISIBLE_DEVICES=<uuid>` on CUDA (unchanged);
**`HIP_VISIBLE_DEVICES=<row index>`** on ROCm, where the index is the
board's position in the openable KFD-node-order inventory (D1), which is
the HIP device order on an unrestricted host. `WorkerSpawnConfig` learns
the accelerator (or a resolved "pin env var" field) to select the form;
`resolve_pin` on ROCm resolves registry `devices` entries (board key or
index) to the row index; the board-key match is **exact** (case-insensitive),
not CUDA's abbreviated-prefix match, because CUDA's abbreviation is a
property of the string handed to the CUDA runtime while these keys never
reach HIP at all — and a prefix could name two `GPU-BDF-…` boards on one
bus. Pin-string handling differs from CUDA where
verbatim pass-through would change meaning (review F8): an unresolvable
**numeric** pin (an index we cannot see, a comma-separated index list)
passes through verbatim with a warning — HIP accepts indices, so the
operator's intent survives; an unresolvable **non-numeric** pin (a
`GPU-…` leftover, a template) is **not written at all**, with a warning —
HIP accepts only indices, so writing it would hide every device and send
the worker to CPU, which is strictly worse than the no-pin behaviour the
warning preserves.

Both numeric forms are **canonicalised** (`"00"` → `"0"`, `" 1 , 2 "` →
`"1,2"`) rather than echoed as spelled. Pins are compared as strings
elsewhere — `prewarm.rs` hands a parked worker to a replica only when the
recorded and resolved pin strings are equal — so two spellings of one device
have to converge here or pooling silently stops matching the
`default_pin()` rendering.

The variable is chosen by the **resolved accelerator, not by the inventory**,
and a ROCm host whose inventory came back unknown (ambient restriction,
probe failure, non-Linux) therefore keeps HIP's vocabulary rather than
falling back to CUDA's passthrough: the backend on that blanked inventory is
still `RocmSysfs`, so its memory refresh is still amdgpu's and its pins are
still indices. What it does with a registry pin:

- a **HIP-legal** pin (an index, or an index list) → canonicalised and
  written to `HIP_VISIBLE_DEVICES`. There is no inventory to translate a
  board key against or to range-check the index with, but HIP reads an index
  without our help.
- **anything else** → dropped with a warning, exactly as on a known ROCm
  host. The harm — every board hidden, worker silently on CPU — does not
  depend on whether we could enumerate the boards.
- **anything at all, including an index, when the ambient restriction is at
  HIP's own layer** (`HIP_VISIBLE_DEVICES`, its `CUDA_VISIBLE_DEVICES`
  alias, or `GPU_DEVICE_ORDINAL`) → dropped with a warning: the operator's
  restriction wins and there is no pinning on that host at all. Ours would
  overwrite theirs, or outrank it as the stronger alias, widening a set they
  deliberately narrowed.
- an ambient **`ROCR_VISIBLE_DEVICES` alone does not** trigger that: it
  filters below HIP, and a HIP index counts into the filtered set, so a
  numeric registry pin composes with the operator's restriction instead of
  escaping it. (The *inventory* is still blanked there — D1 is unchanged —
  so this only concerns pins the operator wrote themselves.)

Choosing by the inventory instead would send those pins to
`CUDA_VISIBLE_DEVICES`, which HIP consults only when `HIP_VISIBLE_DEVICES`
is unset — the weaker of the two aliases, on exactly the hosts that are
hardest to reason about.

Why HIP and not ROCR form: torch honours HIP first on every relevant
version, torch < 2.6 (possible in user-managed venvs) crashes outright
when ROCR is set, and AMD documents HIP-level filtering as the
application-scoped mechanism. `CUDA_VISIBLE_DEVICES` is deliberately NOT
also set on ROCm (it is a HIP alias; setting both is documented as
"unintended behaviour" territory). The accelerator sentinel's HSA/MIOpen
worker env (`accelerator_env::worker_env`) composes untouched — it never
sets visibility vars. eocr's internal `cuda:0` single-device string is a
no-op under a single visible device.

**Known gap this opened — CLOSED by D3, 2026-07-31.** The description below
is kept because it is the reasoning behind where the fix lives. What shipped:
`GpuInventory::resolve_board_key` resolves the *same* registry entry
`resolve_pin` takes into the ledger's board key, `ModelManager::spawn_model`
resolves the two as a pair, and the load reservation is keyed by the key. The
CUDA half of the miss (abbreviated-UUID and unresolvable pins) is fixed by the
same call, as predicted. Original text:

*load reservations* are keyed by the resolved pin string
(`ModelManager::spawn_model` → `VramLedger::reserve_load(…, pin, …)`, which
looks the board up in the ledger's board map). That map is keyed by board
*key*, so on ROCm — where the pin is now an index — the lookup misses and no
load reservation is ever taken; the in-flight load simply is not charged
until the worker registers. Not a regression (before D2 a ROCm host had no
pins at all, so the same code reserved nothing), and it fails in the safe
direction, but it is a real parity gap: a second model loading concurrently
can be granted a window against memory the first load is about to take.
Closing it needs a pin → board-key mapping at that call site, which belongs
with D3's identity work rather than here.

Two reviewer-confirmed reasons the deferral is the right call rather than a
shortcut. First, **CUDA already misses** at this same call site: an
abbreviated-UUID pin and an index pin we could not resolve are both written
verbatim and neither matches a ledger board key, so ROCm differs from CUDA
in *frequency*, not in kind — the fix is one mechanism for both, not a ROCm
patch. Second, **ROCm is unpriced end to end until D3**: `register_worker`
matches on a torch-rendered UUID a ROCm worker cannot produce, so no ROCm
replica is admitted to the ledger at all, and a load reservation there would
be protecting memory against admissions that cannot happen. (The `TODO(D3)`
anchor that stood at the call site is gone; the pairing is there now.)

The enumeration-order assumption (openable KFD node order = HIP order) is
the one load-bearing unverifiable here, and D3's registration cross-check is
its guard — a **warning, not a refusal** (amended after review, 2026-07-31).
Registration is order-independent and self-correcting: it resolves the board
from the address the *worker* reports, so a replica pinned to board A that
comes up on board B is still admitted, under board B, which is where its
memory physically is and therefore where it must be priced. Refusing would
leave a perfectly identifiable replica unpriced over a fault in a row order.

What the mis-order does cost is the **load reservation**: that is taken
before the worker exists, keyed by the board the pin named, so for the
duration of the load it protected the wrong card. It stays keyed that way —
there is nothing to resolve it against until the worker answers — and the
divergence is what the alarm reports. `ModelManager::spawn_model` therefore
passes the pin's board key into `register_worker` as `expected_board`, and
`ledger::BoardLog::PinDiverged` names the model, both board keys, both BDFs
and both totals when the two disagree: visible, safe, diagnosable from a
field report, and the signal that the enumeration needs fixing on real
multi-board ROCm hardware.

### D3 (G3) — Worker identity: torch PCI fields; fdinfo demoted (review F1)

`torch.cuda.get_device_properties(0).uuid` is NOT used on HIP (garbage or
duplicate on consumer boards) — in fact the worker **suppresses
`gpu_uuid` entirely when `torch.version.hip` is set** (review F5), so a
rendered third-vocabulary UUID can never collide with anything. Identity
comes from the same call's PCI fields instead: `pci_domain_id`,
`pci_bus_id`, `pci_device_id` formatted as the BDF
`<domain:04x>:<bus:02x>:<device:02x>.0` (the amdgpu GPU function is
always .0; the HDMI/DP audio controller is function .1 of the *same*
device, not the GPU's own function. An SR-IOV virtual function does sit
at a nonzero function, where forcing .0 fabricates an address whose PCI
directory does not exist — the VRAM read then fails and the probe goes
unknown, i.e. unpriced, which is the safe answer for a passthrough VF).
These fields are device-0-scoped — they describe exactly the board the pin selected —
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

**As implemented (2026-07-31).** The worker gained `device_bdf()`,
`gpu_total_mb()` and a pure fdinfo parser (`parse_drm_fdinfo`,
`fdinfo_vram_by_pdev`, `dominant_vram_pdev`) that D4 reuses for its per-process
memory tier; `device_identity()` suppresses the UUID under
`torch.version.hip`. The registration table in `VramLedger::resolve_board` is:

| worker reports | ledger does |
|---|---|
| `gpu_uuid` matching a board | admit under it, **no** memory check (CUDA, unchanged) |
| `gpu_uuid` matching nothing, or none, + `gpu_bdf` matching a board's | cross-check `gpu_total_mb` (±5% or ±512 MB); pass → admit, fail or absent → refuse + warn |
| `gpu_bdf` matching no board, on an inventory whose boards *have* addresses | refuse + warn (the enumeration-order alarm) |
| nothing matched, exactly one board, **no `gpu_uuid` at all**, and no address that could have matched | cross-check `gpu_total_mb`; pass → admit, fail or absent → refuse |
| anything else | refuse (unpriced dispatch, as before) |

Three deliberate refinements to the text above. First, the single-board
fallback also applies when the worker reported an address but **no board
carries one** (every CUDA inventory) — the address is uninformative there,
not contradictory, and the memory check still gates the admission. Second, a
reported address that contradicts an inventory that *does* carry addresses
never reaches the single-board fallback: positive evidence of the wrong board
is not the same as no evidence. Third (review F3), the same rule applied to
the UUID: the fallback requires `gpu_uuid` to be **absent**, not merely
unmatched, so a MIG instance outside the enumeration is refused again rather
than folded onto the host's only board. ROCm workers suppress the UUID
entirely, so their path is untouched.

Registration also takes the board key the replica's *pin* named
(`expected_board`), purely as the D2 mis-order diagnostic described above: it
never decides anything, and a divergence is logged, not refused. It rides on
the **refusals** too (`TotalDisagrees`, `BdfOutsideInventory`), not only on
`PinDiverged`: on a host of *unequal* boards a mis-ordered enumeration is
caught by the total cross-check, which runs before admission, so the replica
never reaches the divergence alarm and the "the pin believed board A" half of
the evidence would otherwise be missing exactly where the totals are
discriminating enough to prove it. All of the log lines are formatted and
emitted **after** the ledger lock is dropped (review F8) — `resolve_board`
returns a decision plus the line to write — because a `tracing` event under
that mutex would hold every concurrent grant request behind a log write.

**Honesty about the cross-check's reach.** On a **homogeneous** host — the
common multi-GPU shape, two or four identical cards — the total-VRAM check
has *no discriminating power at all*: every board reports the same number, so
a worker that came up on the wrong card passes it and is admitted under the
board its own PCI address names, which is correct pricing. There,
`PinDiverged` is the only mis-order signal, and it is the reason the
`expected_board` plumbing exists. The check is load-bearing in two other
places: on **heterogeneous** hosts, where it is the thing that catches a
BDF match landing on a board of the wrong size; and as the **absent-total
refusal**, which is what stops a non-UUID identification being admitted on
the enumeration assumption alone. Both are real; neither is "the mis-order
guard on a normal multi-GPU box".

**Board-key form flip (accepted v1 risk).** A board's key form is decided by
whether the kernel fills `unique_id` for it: `GPU-<16 hex>` when it does,
`GPU-BDF-0000:03:00.0` when it does not. That is a *kernel and firmware*
property, not a hardware one — amdgpu has both started and stopped
populating the field for particular ASICs across releases. So a driver update
can silently flip a board's key form, and everything keyed by it is orphaned:
the user's `[inference_local.vram.gpu."…"]` override stops applying (falling
back to the section default, which is a *wider* budget, not a narrower one),
and the ledger's remembered per-board base falls back to the calibration
store. Nothing breaks and nothing mis-prices — a new key is simply an unknown
board — but the symptom (a budget override that quietly stopped working after
a system update) points nowhere near its cause. Accepted for v1; worth a
release-note line the first time a field report shows it. The calibration
*profile* key is unaffected: it is the deterministic `AMD gfx… (N GB)` name,
which is derived from `gfx_target_version` and the VRAM total only.

The `ReplicaTelemetryHealth` view still surfaces `gpu_uuid` only; a ROCm
replica shows `null` there while being correctly admitted by address. Adding
`gpu_bdf` to `/health` is a UI/schema change and was left out of this step.

One reality note on the worker half (review F4): the `cpu`/`cu128` extras pin
torch 2.7.1, and `_CudaDeviceProperties` grew the PCI fields in 2.8, so the
shipped CUDA build emits **no `gpu_bdf` at all** today — it becomes live on
CUDA when that pin moves to >= 2.8. The `rocm` extra pins 2.11, so this
identity chain is load-bearing on ROCm alone for now, which is also where it
is the only identity available.

### D4 (G4) — Worker memory sensing: sysfs/fdinfo tiers

The `torch.cuda.*` allocator statistics (`memory_reserved`,
`memory_allocated`, `max_memory_allocated`, `empty_cache`,
`reset_peak_memory_stats`) are the same hipified allocator and are used
unchanged, behind the existing `is_initialized` gates.

- **Free/total tier (ROCm)**: read `mem_info_vram_{total,used}` from
  `/sys/bus/pci/devices/<bdf>/` for the board identified per D3 (cached
  BDF, re-resolved on each call until known — mirroring the NVML handle
  retry). Reported with `free_source: "amdgpu-sysfs"` — the label names
  the driver, not the filesystem, so a future generic sysfs-derived
  reporter cannot inherit authority by string collision. This is
  device-wide (other processes included) and is the *same file* the
  orchestrator's refresh reads (D5), so the single-vocabulary rule holds
  exactly.
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
  own allocator pool loses the tier and the chain continues below it —
  the free-memory delta first, and the alloc-delta + context formula
  behind that — the mirror of the existing NVML implausibility guard,
  pointed the other way. pynvml paths die naturally on a *pure* ROCm host
  (nvmlInit fails once, logged once) — but see the explicit gate below: on a
  hybrid host they do not.
- `free_source_is_authoritative` (ledger) adds `"amdgpu-sysfs"` to
  `"nvml" | "nvidia-smi"`. `"torch"` stays non-authoritative — on HIP
  doubly so given the historical process-local `hipMemGetInfo`.
- `CONTEXT_ESTIMATE_MB = 500` stays as the HIP placeholder for the
  alloc-delta tier: no published HIP figure exists; with the fdinfo tier
  available the constant is rarely load-bearing, `IMPLAUSIBLE_SLACK_MB`
  absorbs the error band, and the value is flagged as a
  field-calibration item.

**As implemented (2026-07-31).** `inferio_worker/memory.py` gained
`amdgpu_free_total_mb`, `fdinfo_own_vram_mb`, `_fdinfo_base_mb`,
`_identity_bdf` (the memoized board address both tiers are *about*),
`_sysfs_bytes` and `_pci_device_dir`. Final orders, as coded:

- free/total (`_free_total_mb`): **nvml → amdgpu-sysfs → torch** on every
  host, with no `torch.version.hip` branch *in the chain itself*, because
  each tier's availability is already the platform test in the direction
  that matters downwards: `mem_info_vram_*` exists under no other driver's
  PCI directory, so a CUDA host falls through the amdgpu tier by its
  absence. Effective order: `nvml → torch` on CUDA, `amdgpu-sysfs → torch`
  on ROCm. The `source=` pin works for the new label exactly as for the
  others: a "before" reading from `amdgpu-sysfs` requires the "after" one to
  come from there or the delta tier is skipped rather than mixed.
  `mem_get_info` was already inside a `try/except` (verified, unchanged).
- **NVML is refused on a HIP worker, not merely unavailable** (added in
  review). The original reasoning — "`nvmlInit` fails once and permanently
  on a ROCm host" — is true of a *pure* ROCm host and false of a hybrid
  AMD+NVIDIA one, where an NVIDIA driver is loaded and `pynvml` (an
  unconditional base dependency) initializes happily. There, D3's UUID
  suppression removes the very thing that would have disambiguated the
  handle lookup, so `_nvml_handle`'s single-GPU last-resort arm could return
  the NVIDIA board's handle: one load report would then carry identity and
  base from the AMD board and free/total from the NVIDIA one, which no
  downstream check can detect (the registration cross-check compares the
  worker's total against the inventory, and the NVIDIA total is a plausible
  number for *some* board). So `_nvml` returns `None` outright when
  `torch.version.hip` is set — or, before any impl has imported torch, when
  `HIP_VISIBLE_DEVICES` is non-empty, which our own spawner writes on every
  pinned ROCm worker and on no other kind.
- base (`_resolve_base`): **nvml → fdinfo → free_delta → alloc_delta**.
- **The free-delta rung is structurally dead on a ROCm *first* load**, which
  is worth stating because the ladder above reads as though it were live.
  `begin_load` takes its "before" reading before anything has touched torch —
  deliberately, so the HIP context the load is about to create falls inside
  the measured window — and on ROCm the free/total tier is keyed by *this
  worker's board address*, which comes from `get_device_properties(0)` and
  therefore does not exist until the impl initializes HIP. So there is no
  pre-load `amdgpu-sysfs` reading to difference against, the source pin has
  nothing to pin, and the tier is skipped. NVML is why the rung lives on
  CUDA at all: it answers pre-load without a context, from the UUID pin. The
  real ROCm first-load ladder is therefore **fdinfo → alloc_delta**, and the
  free delta only ever appears on a *second* load into a worker that already
  has a device. Consequence for the field pass: `CONTEXT_ESTIMATE_MB` is
  materially more load-bearing on ROCm than the D4 text above implies —
  whenever fdinfo is unavailable (an older kernel's VM-walk stats), every
  first load falls straight to `alloc_delta + CONTEXT_ESTIMATE_MB`, so the
  HIP context size is promoted from "flagged" to the first number to measure
  on real hardware.
- **Plausibility floor:** `FDINFO_UNDERREPORT_SLACK_MB = 256`, i.e. an
  fdinfo reading below `reserved_mb - 256 MB` is rejected (one-shot debug
  line) and the next tier answers. Rationale: the reading is *expected* above
  the pool (HIP context + non-torch allocations ride on top), so only a
  shortfall is suspicious, and the only innocent shortfalls are MiB
  truncation on both sides and pages evicted since we committed them
  (`drm-resident-vram` counts *resident* pages). 256 covers those while
  staying well under `CONTEXT_ESTIMATE_MB`, so a reading that missed a whole
  HIP context can never pass as jitter. The comparand is the **absolute**
  post-load pool, not the load window's `reserved_delta`: fdinfo reports
  absolute whole-process VRAM, the two coincide only on a process's first
  load, and the ledger explicitly anticipates repeat loads into one worker —
  where a windowed comparand would wave an under-report through for no better
  reason than that the second load was small. (`reserved_delta` stays as the
  fallback for the case where the allocator could not be read after the load
  at all.)
- **Upper sanity bound:** a reading at or above the board's own
  `total_memory` is rejected too — the twin of the NVML sentinel guard that
  rejects a filled-in `-1`. A per-process figure that equals or exceeds the
  *device* is a parse or kernel-accounting artefact, not a footprint, and the
  floor cannot catch it because over-reporting is the direction the floor
  treats as normal. Skipped when the total is unknown.

One deviation from the text above, in the safe direction: the **fdinfo base
tier is gated on `torch.version.hip`** (the free/total sysfs tier is not).
Recent nvidia-drm also publishes DRM fdinfo memory stats, and they are a
different quantity under the same key — GEM/DRM allocations, not the CUDA
context and caching allocator — which the plausibility floor cannot catch,
because a small model's pool is below the tolerance and *any* reading then
passes. Ungated, a CUDA-Linux worker whose NVML tier is
unavailable (container PID namespace) could report a few MiB of base for a
process holding a 600 MB context. On CUDA the per-process tier is NVML's, by
design.

A Windows-only affordance rode along: `_pci_device_dir` swaps the BDF's
colons for dashes on Windows, the exact twin of `rocm.rs::pci_device_dir`,
so the fixture trees are writable on the dev box. Unreachable in production
(the real path is `/sys`, and the `rocm` extra is Linux-only).

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
measured); a rocm-keyed round-trip test guards the keying instead.

**As implemented (2026-07-31).** No production code changed — the store was
already backend-parametric, which was the finding. What landed is
`a_rocm_profile_round_trips_and_never_crosses_backends` in `calibration.rs`
(local write/read of a `backend = "rocm"`, `platform = "linux"`,
`gpu = "AMD gfx1100 (24 GB)"`, `torch = "2.11.0+rocm7.2"` profile; the
`major.minor` tier answering across `+rocm` patch levels; and a baseline
identical but for `backend`, invisible from the other side in both
directions) plus the ROCm authoring section in
`python/inferio/config/calibration/README.md`.

### D7 (G7) — Capability floors: accepted-unknown, datum recorded

Decision: **unknown-never-filters is accepted for ROCm in v1.** The only
shipped floors are CUDA-specific (sm_80 bf16/FA2), Package 1 already made
`select_dtype` skip capability filtering under HIP, and the impls carry
load-time backstops. The inventory rows record `gfx_target_version`
(free from D1), so a future gfx-arch allowlist has its datum without
another probe; the `/metadata` capability overlay simply stays absent on
ROCm hosts.

**Verified in code (2026-07-31), no change needed.** `gpu.rs::probe_rocm`
constructs `HostComputeCaps::unknown()` directly on every arm — the CUDA
path's `caps_of`/`from_caps` derivation is never reached — and
`rocm.rs::identify` sets `compute_cap: None` on every row (while carrying
`gfx_target_version`), so no row could supply a capability even if it were.
`capability.rs::overlay_metadata` then filters nothing on an unknown host. The module doc there now says this is
a decision (HIP has no analogue) rather than a consequence of nvidia-smi
being absent, which is what it used to imply. The README states it as an
accepted difference, so a ROCm user does not read the missing overlay as a
bug.

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

**Verified in code (2026-07-31), no change needed.** Nothing is
Windows-gated on the ROCm path because nothing can reach it: the probe
module is Linux-only by construction (`/sys/class/kfd`, `/dev/dri`) and the
extra's markers keep ROCm torch off Windows. Both the caveat and the
out-of-scope statement are in the README's ROCm section.

**APU correction (review, 2026-07-31).** The first statement of this — "a
node with no readable nonzero `mem_info_vram_total` makes the whole probe
unknown, so the common integrated-graphics shape is unpriced" — was wrong
about the *mechanism*, and therefore about the outcome. amdgpu registers
`mem_info_vram_total` for iGPUs too: it reports the BIOS UMA carve-out (512
MB is a common default), which is a perfectly readable nonzero number. The
all-or-nothing VRAM rule therefore did **not** catch APUs; it admitted them
and priced the host against the carve-out, which collapses every grant to
batch-1 with nothing in the log to say why. APU nodes are now detected
positively — KFD models an integrated part as a single node carrying both
`simd_count > 0` and `cpu_cores_count > 0`, and that combination is the only
signal there is — and any openable one makes the whole probe unknown with a
WARN naming the node and its gfx target (D1.4). The node is not *skipped*:
HIP still enumerates it, so excluding one row would shift every later row's
device index. So the outcome the design always promised — an APU host is
**unpriced**, i.e. exactly its pre-branch behaviour — is now what actually
happens, rather than being an accident of a rule that did not apply.

## What this changed on CUDA hosts

"ROCm parity" reads as though nothing moved on CUDA. Ten things did, and an
operator debugging a CUDA host deserves the honest list rather than having to
diff it out of the branch. None is a regression; several are fixes; two are
behaviour changes worth knowing about.

1. **Default placement can move on equal-capability, unequal-VRAM hosts.**
   The capability tie-break gained a VRAM-descending rung before the
   lowest-index one (D1.7), because an all-`None`-capability ROCm host would
   otherwise let a first-enumerated iGPU out-rank the dGPU behind it. On CUDA
   it only reorders boards of *equal* compute capability and different size,
   where the bigger board is the strictly better default — but it does mean
   an unpinned model on such a host relocates to the other card and
   re-measures its base there on the next load. A placement change, not a
   key-derivation change: nothing keyed by board identity is invalidated.
2. **A new CUDA admission path, live today.** The single-board fallback
   (D3) admits a worker that reported *no* UUID and *no* matchable address on
   a host with exactly one board, gated by the total-VRAM check. On CUDA that
   is the shipped `cpu`/`cu128` torch 2.7.1 build reporting `gpu_total_mb`
   but — since `_CudaDeviceProperties` grew the PCI fields only in 2.8 — no
   `gpu_bdf`. Such a replica used to fall through to unpriced dispatch; it is
   now ledgered. The total check is what keeps that safe.
3. **Load reservations stop missing on abbreviated UUIDs.** Reservations were
   keyed by the resolved *pin* string; `resolve_board_key` now resolves the
   same registry entry into the ledger's vocabulary (D2's closed gap). The
   CUDA half of that miss — an operator who wrote `devices = ["GPU-1a2b"]`
   got no load reservation at all — is fixed by the same call.
4. **Pin canonicalisation** (added in review). A CUDA `GPU-…`/`MIG-…` pin
   that names a board this host reports now comes back in the *inventory's*
   spelling: an exact match differing only in case, and an unambiguous
   abbreviation, both resolve to the full UUID. CUDA accepts the full form
   everywhere an abbreviation was legal, so nothing narrows; what it fixes is
   that `prewarm.rs` claims a parked worker by **byte-comparing** pin strings
   while `resolve_board_key` already canonicalised, so the pool and the
   ledger disagreed about whether two replicas shared a board. An ambiguous
   abbreviation, and a UUID for a board we cannot see, still pass through
   verbatim.
5. **New wire fields on the load report**: `gpu_bdf` and `gpu_total_mb`
   (additive; the CUDA build emits neither until its torch pin reaches 2.8,
   except `gpu_total_mb`, which it emits today).
6. **Two new WARNs.** One when a worker's own `env`/`env_remove` config
   touches a GPU-visibility variable, since that config is applied *after*
   the pin and silently outranks it. One — `PinDiverged` — which is reachable
   on CUDA too: an operator-set visibility variable in a worker's env can put
   a replica on a board other than the one the pin named.
7. **A new debug line** (`NoBoard`) on every registration that resolves to no
   ledger board — the ordinary CPU/remote-API worker included. Debug level,
   so it is off by default.
8. **The amdgpu-sysfs free/total tier is *probed* on CUDA**, not gated out.
   It runs only when NVML has already failed and only after the board address
   resolved (torch >= 2.8), and it then finds no `mem_info_vram_*` under an
   NVIDIA board's PCI directory and falls through to torch. That absence is
   deliberately the platform test — a `torch.version.hip` branch there would
   be a second thing to keep true — but it does mean two `open()` calls on a
   CUDA host whose NVML is unavailable.
9. **OpenAPI description deltas** on the GPU-inventory schema (`bdf`,
   `gfx_target_version`, and reworded `index`/`uuid`/`name`/`compute_cap`
   text). Field-additive; the fixture was regenerated.
10. **The probe now dispatches on the resolved accelerator.** A host
    explicitly configured `accelerator = "rocm"` no longer runs nvidia-smi at
    all, so it loses compute-capability gating for `/metadata` even if it has
    NVIDIA hardware. That is a misconfiguration-only path — `auto`, `cuda`
    and `cpu` all keep the nvidia-smi probe byte for byte, deliberately, so
    capability filtering never depends on which wheels are installed.

## What shipped without AMD hardware, and how it was validated

All of the following is implemented and covered by tests that run on any
machine:

- D1/D5 sysfs probe against **fixture directory trees** (fake
  `nodes/<n>/properties`, fake PCI device dirs, fake `/dev/dri` render
  nodes for the openability filter) — same fixture style as the
  nvidia-smi parse tests; covers fused/absent/duplicate `unique_id`,
  missing VRAM files, APU nodes (both the carve-out shape and the two
  ordinary dGPU spellings that must *not* trip it), an absent `simd_count`,
  container-subset render nodes, undecodable gfx targets (both the
  minor and the stepping guard), numeric-not-lexicographic node ordering
  (nodes 2/9/10), the read+write openability test against a read-only render
  node, the cgroup-hidden skip end to end, and (added during implementation,
  beyond the design above) partitioned MI300-class boards that publish
  several KFD nodes per PCI device, which make the probe unknown rather than
  N-fold-over-admitting one board's memory.
- D2 pin-form selection and resolve_pin-on-ROCm unit tests, including
  the numeric-verbatim vs non-numeric-don't-set rule; ambient
  restriction blanking over the four env vars; the pin variable following
  the resolved accelerator on an unknown inventory; the ambient HIP-layer
  guard holding with *and* without a board list; and CUDA pin
  canonicalisation agreeing with `resolve_board_key` for every spelling
  that names a board.
- D3 BDF formatting from torch PCI fields; the shape check on the
  fdinfo-derived address (a `drm-pdev` line that is not an address never
  becomes an identity); fdinfo parser fixtures (both memory-key spellings,
  multi-client filtering by pdev, dedupe by client id, missing keys, and the
  lone-client-holding-nothing case the tie rule cannot see); registration
  cross-check paths (BDF match, total mismatch, uuid-present-but-unmatched
  fallthrough, single-board fallback, swapped enumeration) against synthetic
  inventories, with the address-carrying test built on *two* boards so a
  ledger that dropped the addresses could not pass on the single-board
  fallback.
- D4 wire round-trips for `gpu_bdf`, `gpu_total_mb`,
  `free_source: "amdgpu-sysfs"`, `base_method: "fdinfo"`; ledger
  authoritative-source rule; fdinfo-below-reserved plausibility floor and
  the total-memory upper bound; NVML refused outright on a HIP worker (both
  signals: `torch.version.hip`, and `HIP_VISIBLE_DEVICES` before torch is
  imported at all) with a *working* fake NVML present; and the free-source
  pin holding on a mixed pair whose skew is small enough to look plausible.
- D6 rocm-keyed store round-trip
  (`calibration.rs::a_rocm_profile_round_trips_and_never_crosses_backends`):
  local write/read, the `major.minor` torch tier over `+rocm` local
  version tags, and backend isolation in both directions.

Needs a field pass on real hardware (none blocks shipping; every failure
mode degrades to unpriced + a diagnostic log):

- Openable-KFD-node-order = HIP-order on multi-GPU hosts. Guarded by D3,
  which since the same-day amendment **warns rather than refuses**: the
  replica is admitted under the board its own PCI address resolves to, and
  `ledger::BoardLog::PinDiverged` names both board keys, both BDFs and both
  totals. One volunteer report carrying that line confirms or refutes the
  ordering assumption.
- fdinfo `drm-resident-vram` magnitudes vs allocator expectations on
  ROCm-relevant kernels; HIP context size for the alloc-delta constant.
- End-to-end grants/trim/knee on an AMD board (mirror of the CUDA
  dogfooding list).

**What a volunteer report should contain.** Six log lines matter, all at
default level. The three registration alarms carry a `model` field, so grep
for the model name to correlate them — note that `TotalDisagrees` has two
wordings depending on what the worker managed to report. The probe-level
lines have no model field: they are emitted once at startup and describe the
whole host.

- `PinDiverged` — *"this replica was pinned to one board and came up on
  another"* (WARN). The enumeration alarm. Pricing is still correct; the
  load reservation was not. Names `expected_board`/`expected_bdf` vs
  `resolved_board`/`resolved_bdf`. Note the reach: on a **homogeneous** host
  this is the *only* mis-order signal, because the total cross-check cannot
  tell identical boards apart.
- `TotalDisagrees` — *"does not agree with the board it was matched to"*,
  or *"reports no total VRAM"* (WARN). A **refused** registration: that
  model dispatches unpriced. Names both totals, the tolerance, and the board
  the pin believed (`expected_board`/`expected_bdf`) — on a host of unequal
  boards a mis-ordered enumeration surfaces *here* rather than as
  `PinDiverged`, because the cross-check runs before admission. **If it
  fires on every load**, that is not an enumeration fault: report both
  totals, because the tolerance (±5% or ±512 MB, whichever is larger) may
  simply be wrong for your board's carve-outs.
- `BdfOutsideInventory` — *"is on a PCI address no board in the GPU
  inventory has"* (WARN). Also a refusal, and the other face of an
  enumeration fault: the worker is somewhere the probe never saw. Carries
  the pin's belief too.
- The **unpriced-probe** line — *"this host is configured for ROCm but no
  GPU inventory could be built"* (WARN), carrying `reason`, `gpu_nodes` and
  `openable_nodes`. Emitted once at startup for the two failure paths that
  name nothing themselves (`no KFD GPU nodes`, `no openable render node`).
  It is the answer to "the ledger is simply not there and nothing said why".
- The per-node refusals, each naming the node or board that tripped: the
  partitioned-board warning — *"this PCI device publishes several KFD
  nodes"* — the APU line — *"reports both SIMDs and CPU cores"* — the absent
  `simd_count`, unreadable-properties, undecodable-gfx-target, missing-BDF,
  missing-VRAM-total and duplicate-`unique_id` lines (all WARN).
- The ambient-restriction line — *"an ambient GPU visibility restriction is
  set"* (INFO). The whole host is deliberately unpriced; under a ROCR-only
  restriction a registry index pin is still written and selects *within* the
  operator's filtered set.

A report that includes `GET /api/inference/health` (board keys, names,
totals, per-board budgets) alongside these lines is enough to reconstruct
what the probe saw without shell access to the machine.

Two more things a field pass should settle, neither of which any fixture can:

- **`location_id` node-id packing on partitioned boards.** The partition
  detector relies on the kernel ORing the KFD node id into `location_id`'s
  *function* bits (`kfd_topology.c`), which is what makes several nodes of
  one MI300 derive the same `…:00.0` address and trip the one-node-per-PCI
  rule. Some kernel revisions may pack the node id higher (bits 31:28
  instead). If they do, the derived addresses diverge, the dedicated
  detector never fires — and the probe still fails safe, because the
  fabricated addresses have no PCI directory and the VRAM read fails, taking
  the host unpriced by the missing-total path instead. So the failure is
  contained; what cannot be known without hardware is which of the two paths
  a real partitioned board actually takes, and the dedicated test shape may
  simply never occur in the field.
- **S6: torch < 2.6 in a user-managed venv dies at import under an
  operator-set `ROCR_VISIBLE_DEVICES`** (pytorch#142292). This is not
  something the design does — we never write that variable, precisely
  because of this — but it is a documented assumption: a Slurm-style
  scheduler sets ROCR, and a venv the user manages themselves can still hold
  a torch old enough to crash on it. Such a host cannot run workers at all,
  with or without this branch; the symptom is an import-time crash in the
  worker, not an admission problem.

## Implementation order (all five landed 2026-07-31)

1. ✅ D1 + D5 (sysfs inventory + memory refresh, fixtures-first) — lights
   up ledger boards on ROCm.
2. ✅ D2 pin plumbing (backend-aware spawn env, resolve_pin, ambient rules).
3. ✅ D3 worker identity (torch PCI fields + gpu_bdf/gpu_total_mb wire
   fields) + registration cross-check (this is the safety net for 2).
4. ✅ D4 worker sysfs/fdinfo memory tiers + ledger `"amdgpu-sysfs"`
   authority + plausibility floor.
5. ✅ D6 test + D7/D8 statements; `batch-calibration-design.md`'s ROCm
   open question now points here, the protocol doc's memory-sensing
   section was updated with each step, and the README gained an "AMD GPUs
   (ROCm)" section under VRAM budgets (parity, accepted differences,
   out-of-scope, and the field-report line list).
