# Unified-memory admission: MPS, AMD APUs, CPU

**Status: DECIDED 2026-08-01. Step 1 (backend A: MPS) implemented the same
day** — the accelerator identity and resolution, the synthetic unified board
and its refresh, the worker's MPS memory tiers, DP-2, DP-3, DP-4, the
spawner's watermark, the widened OOM classifier and the trim extension.
**Step 2 (backend B: AMD APUs) implemented 2026-08-01** — the probe's APU
decline replaced by a priced unified board, the GTT-inclusive total/free
readings on both sides, DP-5's spawner signal (an address the worker
verifies, not a flag it trusts), DP-6's name and the either-of registration
cross-check (see "As implemented" under backend B for the decisions this doc
did not pin down). Backend C (CPU) is not implemented, and
neither is the README support-matrix table under "What stays unpriced".
Everything MPS-specific is covered by fixtures only: no Apple hardware exists
on the dev box and CI has no Macs yet, so the field-pass list at the end
stands in full — and the same is true of every APU claim, for the same reason.
Extends
`docs/batch-calibration-design.md` (the ledger/grant/calibration
machinery) and `docs/rocm-batch-calibration-parity.md` (the sysfs-first
ROCm probe). Decision points are marked **DP-n** inline; the table at the
end records each decision. Two were amended in review: DP-3 (Apple
Silicon always resolves to the accelerator unless the user explicitly
configures CPU) and DP-4 (the worker-reported MPS total is authoritative
— real machines run raised GPU wired limits, so no fixed window around
the seed can be allowed to reject the true figure).

## Why this exists

Batch calibration *replaces* manual batch sizes: the stamped migration
resets stored `default_batch_size`/`batch_size` values to auto, the wizard
no longer offers the setting, and what remains of the knob is a max-cap.
That is only coherent if auto actually works on every platform we ship to.
Today three shipped platform shapes get no admission at all and fall to the
unpriced path (fixed registry-default batches, `default_max_batch` = 32 as
the last resort):

- **Apple Silicon (MPS)** — a first-class Desktop target since v0.1.5.
  The worker runs and silently reports no memory facts; profiles would key
  as backend `cpu` (`http.rs::accelerator_backend` documents the collision).
- **AMD APUs on ROCm** — declined by the probe as a v1 safety measure
  (`rocm.rs`: a KFD node with both SIMDs and CPU cores makes the whole
  probe unknown). Correct against the 512 MB-carve-out failure mode, but it
  unprices real deployment hardware: a BC-250 (gfx1013, 16 GB unified
  GDDR6) is an APU by this test, and Strix Halo machines (gfx1151, up to
  128 GB unified) are close to the ideal Panoptikon desktop. Worse, the
  backstop is mushy exactly there: when the carve-out fills, amdgpu spills
  into GTT, so over-admission often manifests as the silent-slowdown
  regime rather than a crisp `hipMalloc` failure — WDDM's failure mode, on
  Linux.
- **CPU-only hosts** — fixed GPU-curated registry defaults, and the OOM
  backstop does not fire at all: `run_with_oom_retry` catches
  `torch.cuda.OutOfMemoryError` only, so a too-big batch is a
  `MemoryError` or an OOM-killed worker, not a halved retry.

Because the migration is platform-blind, this is an active regression on
these hosts, not just missing polish: a user's tuned-down batch size gets
replaced with a registry default no calibration protects.

These three are one problem. In each case the accelerator's memory is a
**pool shared with the whole OS**: "total" is a policy number rather than a
device fact, external pressure is system-wide and observable through RAM
statistics, the hard OOM signal exists but is weaker than a dGPU's, and
over-admission tends to degrade into slowdown (compression, swap, GTT
spill) before it fails. The design below defines one "unified board" model
for the ledger and instantiates it three times. Every downstream mechanism
— grants, footprint ledger, ratchet, knee, margin widening, deflation,
calibration store — is reused unchanged; only how *total*, *free*, and
*identity* are read differs per backend.

## Non-goals

- Windows APUs / DirectML, Intel XPU, Jetson (CUDA-on-unified-memory), and
  Intel Macs (releases build macOS aarch64 only). All stay on the unpriced
  path and are named in the README support matrix.
- Partitioned MI300-class boards, ambient `ROCR_VISIBLE_DEVICES`-style
  restrictions, and Slurm-managed hosts: still unpriced by design, per the
  ROCm parity doc.
- Multi-device MPS (there is none) and mixed MPS+eGPU shapes (Apple
  Silicon has no eGPU support).
- Improving MPS *performance* (fp16 negotiation on MPS, currently forced
  fp32 by `_select_dtype` for non-CUDA devices) — worth doing, keyed
  safely by the existing `dtype` field, but a separate change.

## The unified board model

A unified board is a ledger board whose memory the OS and every other
process also uses. Three readings define it:

| Reading | Meaning | dGPU analogue |
|---|---|---|
| `total` | The policy budget for accelerator use of the pool | Board VRAM |
| `pool_free` | What the accelerator stack says is unclaimed | `nvidia-smi` free / `mem_info_vram_*` |
| `ram_available` | What the OS says could actually be delivered right now | (no analogue — VRAM is private) |

The **effective free** reading handed to the ledger is:

```
free = max(0, min(total, pool_free, ram_available))
```

`ram_available` is the load-bearing addition. The ledger already infers
external usage as `total − free − (our known footprints)` and inflates it
by `margin`; feeding it a RAM-clamped free means a browser eating 40 GB
shows up as external pressure exactly like a game eating VRAM does on a
dGPU. No ledger changes are needed for this — it is a property of the
free reading, produced where the reading is produced (worker samples and
the orchestrator's staleness refresh).

`ram_available` sources: `psutil.virtual_memory().available` worker-side
(psutil is already a base dependency); orchestrator-side
`host_statistics64` via `libc` on macOS, `sysinfo`-free reads of
`/proc/meminfo` (`MemAvailable`) on Linux, `GlobalMemoryStatusEx` via
`windows-sys` on Windows. No new crates. The two producers must sum the
**same terms** — on macOS that is free + inactive pages, which is what
psutil's `available` is; counting anything more on the orchestrator side
(purgeable, compressed) would make its refresh the looser of the two and
systematically understate external pressure, the one error direction the
ledger cannot absorb.

Everything else is inherited:

- **Budget**: `usable = total − external × (1 + margin)`, `cap_fraction`
  as the hard-fraction lever — semantics identical to the shipped design.
- **Ratchet / knee / deflation**: unchanged. The ×2 extrapolation ratchet
  and the throughput knee are what keep a 60 GB budget from ever being
  *used* by a model whose curve flattens at 2 GB.
- **Collapse detector**: already platform-neutral (`packing.py`), and it is
  load-bearing on every unified backend, because all three degrade into
  slowdown regimes (MPS compression/swap, GTT spill, CPU swap) where the
  hard error fires late or never.
- **Margins**: defaults unchanged (`margin = 0.10`, unconfirmed-bonus
  +0.15 until 5 local samples). **DP-1 (decided)**: no unified-specific
  default margin — the RAM-clamped free already prices external pressure
  in, and the WDDM/dogfood revisit clause in the base design covers
  retuning.

### Negative signals, widened once for all three

`run_with_oom_retry` currently catches `torch.cuda.OutOfMemoryError` only.
It widens to:

- `torch.cuda.OutOfMemoryError` (unchanged),
- `MemoryError` (CPU),
- any exception whose text matches the existing `_looks_like_oom`
  substrings — which already cover `"out of memory"`, and MPS raises
  `RuntimeError("MPS backend out of memory (…)")`, CPU torch raises
  `RuntimeError("… DefaultCPUAllocator: can't allocate memory …")`. The
  classifier gains the `DefaultCPUAllocator` form.

This widening also fixes the pre-existing CPU dead-worker problem in
passing, and it is deliberately conservative: a generic `RuntimeError` is
only treated as OOM when the classifier agrees.

**DP-2 (decided) — worker death as a negative sample.** A SIGKILL (macOS
jetsam, Linux OOM-killer) cannot be caught in-process. When a replica
dies while a granted window is in flight on a **unified** board, the
ledger records one synthetic negative (grant halving, never fed to the
fit) before the death-cleanup path runs, tagged distinctly in the log.
Scoped to unified boards only — on dGPUs a mid-window death has too many
non-memory causes to blame the batch size, but on a unified board a death
mid-batch is overwhelmingly the memory killer.

### Trim

- MPS: `torch.mps.empty_cache()` joins the trim path (today the worker's
  trim is CUDA-only; `inferio/impl/utils.clear_cache` already knows MPS
  but the worker path does not use it).
- CPU: no-op (glibc arenas do not return memory; `malloc_trim` is not
  worth a platform branch).
- APU: existing HIP `empty_cache` path, unchanged.

## Backend A: MPS (Apple Silicon)

### Accelerator identity and resolution

- New config value `accelerator = "mps"` (`Accelerator::Mps`). Setup on
  macOS/aarch64 resolves `auto` → `Mps` and writes **`extra=mps`** to the
  sentinel. The extra maps to the same default-PyPI wheel set `cpu` maps
  to on macOS (the source markers already route every extra there), so
  this is a label change, not a dependency change; `extra_accelerator`
  learns the new line.
- **DP-3 (decided) — Apple Silicon always resolves to the accelerator.**
  The one and only way an Apple Silicon host runs unaccelerated is an
  explicit `accelerator = "cpu"` in config. Everything else — `auto`, an
  old sentinel reading `extra=cpu` ("cpu doubles as the macOS/MPS
  selection", `setup.rs::accelerator_extra`), a missing sentinel, even an
  explicit `cuda` (which setup already coerces to the default wheels with
  a warning) — resolves to `Mps`. No forced re-setup for existing Macs.
  Explicit CPU means CPU for real, including the worker's device (see
  the device-coherence item under Backend C).
- `accelerator_backend()` gains `Mps → "mps"` — the calibration keyspace
  split the `http.rs` comment already reserves.

### Inventory (orchestrator probe)

Single synthetic board:

- **Board key**: the constant `GPU-MPS`. There is exactly one device per
  host, budgets/config overrides are per-host files, and a constant key is
  what a user can actually type into
  `[inference_local.vram.gpu."GPU-MPS"]`. No hardware UUID needed.
- **Name (calibration key)**: `Apple M3 Max (128 GB)` — chip name from
  `sysctl machdep.cpu.brand_string`, RAM from `hw.memsize` rounded to
  whole GB. Same convention as the ROCm derived names: deterministic from
  kernel facts, identical on every host with that silicon, and includes
  the capacity fact that changes admission behaviour.
- **Total**: Metal's `recommendedMaxWorkingSetSize` — the same figure
  torch exposes as `torch.mps.recommended_max_memory()`. Defaults to
  ≈75 % of RAM on Apple Silicon, **but is not a constant**: raising the
  GPU wired limit (`sysctl iogpu.wired_limit_mb`, a standard tweak on
  Macs used for local ML — the reference M3 Max runs at 90 %) moves it,
  and the moved figure is precisely the one admission must budget
  against. **DP-4 (decided, amended in review) — how the orchestrator
  reads it.** Seed `total = hw.memsize × 0.75` at probe time, then
  **adopt the exact figure from the first worker's load report**
  (`gpu_total_mb` from `recommended_max_memory`) as *authoritative*.
  The adoption is sanity-bounded only — accepted when
  `0 < reported ≤ hw.memsize` — and logged; there is **no proximity
  window around the seed**, because a raised wired limit legitimately
  puts the real figure 20 %+ away from it, and rejecting exactly the
  tuned machines would be backwards. The seed exists only so budgets are
  defined between startup and the first load. **The adoption is not
  one-shot**: the wired limit is a live sysctl, so a later report that is
  still inside the sanity bound but outside the registration cross-check's
  tolerance *re-adopts* rather than being refused — otherwise raising the
  limit under a running gateway would leave the host unpriced until a
  restart. Reports inside the tolerance change nothing (the two sources
  round the same pool differently, and a total that drifted on every load
  would re-price every outstanding grant). (The rejected alternative:
  reading Metal directly from Rust via `objc2-metal` / hand-rolled objc
  externs — a new dependency to learn a number the worker already knows,
  and the worker's torch-reported figure is the one allocations are
  actually judged against.)
- **Memory refresh** (`MemoryQuery::Mps`): `free = max(0, min(total,
  ram_available))` from `host_statistics64`. No subprocess, no Metal call.
- **Pinning**: none. One device; no visibility env var exists or is
  needed. The pin-resolution path treats an MPS inventory like the
  "no pin" default everywhere.
- **Capability view**: unknown, like ROCm — the floors are CUDA-specific.

### Worker (`inferio_worker/memory.py`)

New tier alongside the CUDA/HIP ones, gated on
`torch.backends.mps.is_available()` (and not `_torch_cuda()`):

- **Sample** (`free_source: "mps"`): `total` from
  `torch.mps.recommended_max_memory()`, `free` per the unified formula
  with `psutil` for `ram_available`.
- **Pool/allocator stats**: `driver_allocated_memory()` as the
  reserved-pool analogue, `current_allocated_memory()` as allocated.
  torch.mps has **no peak/reset APIs**; the pool is monotone absent
  `empty_cache`, so post-batch `driver_allocated` ≈ peak reserved — the
  same property the CUDA pool has. One documented exception: the MPS
  allocator garbage-collects cached buffers when an allocation crosses the
  *low* watermark, so a batch that ran close to the budget can read back
  below its true peak. The bias is toward under-stating cost exactly at
  the ceiling; the collapse detector and DP-2 carry that regime. Accepted
  approximation; noted in the protocol doc, sized on the M3 Max.
- **Base** (`base_method: "mps"`): `driver_allocated_memory()` at load
  end. Per-process *by construction* (each process owns its Metal heap),
  so this is tier-1 quality — no free-delta fallback needed on the happy
  path, though the delta tiers remain beneath it by availability, as
  always.
- **Identity**: no UUID, no BDF. The worker reports `gpu_name` (from
  `sysctl` via `platform`) and `gpu_total_mb` (recommended-max). The
  ledger join on an MPS host is **the only board**: backend `mps` +
  single-board inventory + a `gpu_total_mb` cross-check with the ROCm
  tolerance (`max(total/20, 512 MB)`). A report with no MPS facts (no
  torch, remote impl) stays unregistered, exactly as today.
- **Watermarks**: the spawner sets `PYTORCH_MPS_HIGH_WATERMARK_RATIO=1.0`
  on MPS workers so torch's hard error fires at the recommended-max
  boundary instead of whatever the build's default permits (the default
  has drifted across torch versions and can sit above 1.0, i.e. inside
  the swap regime), and pins `PYTORCH_MPS_LOW_WATERMARK_RATIO=1.0` with
  it — torch asserts `high >= low` at allocator init, so an ambient
  user-set low above 1.0 would turn the high pin into a startup failure.
  **Verify the exact env semantics on real hardware before shipping** —
  this is the MPS analogue of the ROCm doc's "first number to measure"
  items. The error is a `RuntimeError` the widened classifier already
  catches. The low watermark is also where the allocator garbage-collects
  cached buffers, which is what biases the peak approximation above
  downward near the ceiling.

## Backend B: AMD APUs (ROCm)

### Un-declining the APU

The APU decline (`rocm.rs`: `cpu_cores_count > 0` fails the whole probe)
is replaced by classification: such a node becomes a **priced board
flagged `unified`** instead of a poison pill. This also un-sinks
dGPU+APU hosts — both boards become rows, index integrity is preserved
(the reason the decline had to be all-or-nothing disappears), and the
existing VRAM tie-break decides default placement between them.

### Readings

- **Total**: `mem_info_vram_total + mem_info_gtt_total`. The GTT files
  sit in the same sysfs directory the probe already reads; GTT is where
  ROCm allocations actually land on APUs once the carve-out fills, and on
  Strix Halo-class machines it is most of the usable memory.
- **Free**: `mem_info_vram_free-equivalent (total−used) +
  min(gtt_total − gtt_used, ram_available)` — GTT pages must come from
  RAM that actually exists right now, which is the unified clamp again.
- **Refresh**: `MemoryQuery::RocmSysfs` reads the two extra files for
  boards flagged unified. dGPU rows keep today's exact behaviour.
- **Worker per-process tier**: DRM fdinfo already carries
  `drm-memory-gtt` / `drm-resident-gtt` next to the VRAM counters the
  parser handles; on a unified board the worker's own usage is
  vram + gtt. **DP-5 (decided) — how the worker knows the board is
  unified.** The worker has no inventory; the spawner (which already
  writes the pin env) sets `PANOPTIKON_UNIFIED_GPU` for replicas
  pinned to a unified board. **As implemented**, the variable carries that
  board's **PCI address** rather than a bare `1`, and the worker acts on it
  only when it matches the address it resolved for itself: the pin is a
  belief about where the replica lands, and a wrong belief here would put two
  memory currencies under one authoritative label (see "As implemented"
  below). dGPU workers keep the vram-only arithmetic
  so their numbers do not shift.
- **Worker sample tier** (`free_source: "amdgpu-sysfs"`): same file pair
  extension under the same signal.

### Identity and keying

- Board keys: unchanged machinery. APUs are PCI devices; `unique_id` when
  the kernel fills it, `GPU-BDF-…` otherwise (a BC-250 will almost
  certainly key by BDF). Registration join by BDF works as on dGPUs.
- **Name (calibration key)**: must not embed the BIOS-configurable
  carve-out. `AMD gfx1151 APU (128 GB)` — gfx target + the word `APU` +
  **physical RAM** rounded to whole GB (`MemTotal`), which is the stable
  hardware fact. **DP-6 (decided)**: RAM stays in the name — capacity
  changes admission behaviour and the ROCm/MPS names both carry it; two
  Strix Halos with 32 vs 128 GB should not share ratchet anchors.
- **Registration cross-check**: HIP's `total_memory` on an APU may report
  the carve-out, the carve+GTT sum, or something else again — unverified.
  The cross-check accepts a worker total matching **either** the
  carve-out or the unified total, within the existing tolerance.
  Verifying which one HIP actually reports is a BC-250 field-pass item;
  the either-of check ships first so the mismatch cannot refuse
  admission while we find out.

### Failure modes

- The GTT spill regime is priced now (grants budget against carve+GTT and
  external RAM pressure), and the collapse detector covers what pricing
  misses. HSA/HIP OOM strings on APUs may differ from dGPU ones — a
  field-pass item; the substring classifier is data, easy to extend.
- The `HSA_OVERRIDE_GFX_VERSION` overrides BC-250-class hardware needs
  are user-managed env and pass through the worker env untouched — they
  affect kernel selection, not the sysfs facts this design reads.

### As implemented (2026-08-01)

Things this section left open, decided during implementation and hardened in
the review that followed:

- **DP-5's signal is the board's PCI address, not a flag.** The spawner knows
  which board a pin *named*; whether the replica came up on it is exactly the
  assumption the ROCm design cannot verify (KFD row order = HIP device
  order). A bare flag would therefore be a belief, and a wrong belief is
  expensive in both directions: a flagged worker that landed on a **dGPU**
  would add GTT to its free reading and report the sum under the
  authoritative `"amdgpu-sysfs"` label — phantom headroom, the one error the
  ledger cannot absorb — while an unflagged worker on the **APU** prices a
  64 GB board at its 512 MB carve-out and collapses to batch-1. So
  `PANOPTIKON_UNIFIED_GPU=<pci address>`, and the worker counts GTT only when
  that address is the one it resolved for itself. Mismatch, unparseable, or
  not yet identified → the discrete arithmetic, which is conservative both
  ways.
- **And the ledger checks the currency independently.** A free sample whose
  *own* `total_mb` disagrees with the board it was admitted under is
  discarded (once-per-replica WARN), because `external = total − free − ours`
  would otherwise turn the difference into headroom. It is a no-op for every
  well-behaved worker — NVML's total is the board's, MPS's *is* the adopted
  total (adoption runs first, in the same registration), a verified APU
  worker's is carve+GTT — and it does not depend on the worker cooperating,
  which the env-var check does.
- **Which total the placement tie-break compares.** An APU's admission total
  (carve+GTT) is not a like-for-like quantity against a dGPU's VRAM: the GTT
  half is borrowed from the RAM the OS and every other process are using, and
  ranking by it would hand default placement to the slower board on
  essentially every dGPU+APU host, since a Strix Halo's nominal budget dwarfs
  any consumer card's VRAM. `GpuInfo::placement_total_mb` therefore ranks a
  unified board by `max(carve-out, total / 8)`: the carve-out is the memory
  the operator gave the iGPU outright, and the eighth is a deliberately
  pessimistic floor so a 128 GB machine left at the 512 MB BIOS default does
  not lose to a 2 GB display card. This is placement only: the APU is fully
  priced against carve+GTT either way, and a `devices` pin still selects it.
- **The name's RAM figure is `MemTotal` + the carve-out, rounded up to 4 GiB.**
  Firmware reserves the UMA carve-out before the kernel counts memory, so it
  is *missing* from `MemTotal` — and taking `MemTotal` raw would embed the
  BIOS setting negatively, splitting a machine's profiles the moment someone
  changed it, which is the one thing DP-6 exists to prevent. Adding it back
  cancels that knob; the 4 GiB grid cancels the *other* reservations
  (firmware/ACPI maps, `crashkernel=`, kernel text — 1.5–2 GiB in practice and
  not constant across kernels or boot parameters), which would otherwise name
  a 128 GiB machine `(126 GB)` today and `(127 GB)` after an update. Budgets
  are untouched: this is the calibration key and nothing else.
- **DP-4's total adoption is scoped out of this backend.** Adoption is for a
  board whose real total nothing but the worker can read; an APU's comes from
  amdgpu's own counters, while HIP may well report its carve-out — a figure
  inside the sanity bound that would replace a 64 GB budget with 512 MB. The
  ledger's adoption path therefore requires the single board to carry **no PCI
  address**, which is true of the MPS board and never of an APU. The either-of
  cross-check is what handles the same uncertainty here instead.
- **The either-of window is bounded at both ends.** It is the *only* place the
  unknown HIP total is tolerated — it identifies the board, it never re-prices
  it — and a figure matching neither candidate is refused exactly as on a
  dGPU. Two hardenings: a reported `0` is refused outright on every board (it
  is the shape of a driver that answered without knowing), and the tolerance
  is `max(5%, min(512 MB, figure/4))` so a 512 MB carve-out admits ±128 MB
  rather than the ±100% the flat 512 MB floor would have allowed. Nothing
  moves at dGPU scale.
- **A tripwire for the hosts this newly puts on the pinned path.** Un-declining
  the APU turns previously-unpinned dGPU+iGPU desktops into two-row hosts with
  `HIP_VISIBLE_DEVICES=<row>` pins, and if ROCm does not enumerate the iGPU
  that index names nothing and the impl silently runs on the CPU. A worker
  the orchestrator placed (`PANOPTIKON_DEVICE_PIN` — distinct from the
  visibility variable, which an operator may also have set to mean the
  opposite) that finds torch enumerating **no devices** now fails its load
  with an actionable error naming the pin, which the orchestrator surfaces as
  an ordinary model-load failure (fail-fast) rather than serving
  twenty-times-slower results.

## Backend C: CPU

**DP-7 (decided) — CPU is priced**, last in the rollout. By the time MPS
and APU land, a CPU board is almost free: it is the unified model with
`pool_free` undefined (free = `ram_available` alone) and RSS as the
footprint currency. The rejected alternative — fixed registry defaults
plus the widened OOM backstop — would leave "auto works everywhere"
carrying a footnote forever, and the footnote is the whole complaint.

- **Board**: constant key `CPU`, one per host. Name for calibration:
  `CPU (64 GB)` (physical RAM rounded; the ISA/AVX level is captured
  indirectly by `platform` + `torch` in the key already). Backend key
  stays `"cpu"`.
- **Total**: physical RAM. **DP-8 (decided) — default ceiling.** A RAM
  OOM is a process kill, not a catchable exception, so the CPU board
  ships with a default `cap_fraction = 0.75` rather than relying on
  margin alone. Overridable like any board.
- **Worker readings**: `free_source: "ram"` (`psutil.virtual_memory()`);
  base = RSS at load end minus RSS at spawn (`base_method: "rss"`); batch
  peaks from the OS high-water mark (`VmHWM` on Linux, `peak_wset` via
  psutil on Windows, `ru_maxrss` on macOS) — real peaks, unlike MPS.
  RSS is pool-like (allocators rarely return pages), which is the
  property the fit machinery expects.
- **Device coherence**: an orchestrator pricing a host as CPU must
  guarantee workers *run* on CPU. Today `utils.get_device()` probes
  cuda → mps → cpu regardless of what the orchestrator thinks (an
  `accelerator = "cpu"` Mac would run on MPS while being priced as CPU).
  The spawner sets `INFERIO_DEVICE=cpu` on workers of a CPU-priced host
  and `get_device()` honours it first. This closes the same hole for
  "CPU host that happens to have an NVIDIA card".

## Calibration keying summary

| Host | `platform` | `backend` | `gpu` (profile key) | Board key (budgets) |
|---|---|---|---|---|
| Linux/Windows + NVIDIA | os | `cuda` | nvidia-smi name | `GPU-<uuid>` |
| Linux + AMD dGPU | `linux` | `rocm` | `AMD gfx1100 (24 GB)` | `GPU-<unique_id>` / `GPU-BDF-…` |
| Linux + AMD APU | `linux` | `rocm` | `AMD gfx1151 APU (128 GB)` | `GPU-<unique_id>` / `GPU-BDF-…` |
| Apple Silicon | `macos` | `mps` *(new)* | `Apple M3 Max (128 GB)` | `GPU-MPS` *(constant)* |
| CPU-only | os | `cpu` | `CPU (64 GB)` | `CPU` *(constant)* |

No existing key changes meaning; `mps` splits out of `cpu` before any Mac
ever persisted a profile (nothing registers on MPS today, so nothing was
written — the collision the `http.rs` comment tracks never materialised).

## Wire protocol additions

Documented in `docs/inferio-worker-protocol.md` when implemented:

- `free_source`: new values `"mps"`, `"ram"`; `"amdgpu-sysfs"` unchanged
  but GTT-inclusive on a worker-verified unified board
  (`PANOPTIKON_UNIFIED_GPU` naming that board's PCI address).
- `base_method`: new values `"mps"`, `"rss"`.
- `gpu_total_mb`: on MPS = recommended-max; on APUs = whatever HIP
  reports (cross-checked either-of, see above).
- No new message types, no schema break: every field is additive and
  optional, per the protocol's existing rule.

## What stays unpriced (and is now said out loud)

Ambient visibility restrictions, partitioned MI300s, unknown inventories,
remote-API impls, `none`-class models, Intel Macs, Jetson, DirectML.
The README gains a short support-matrix table — *calibrated* /
*backstopped only* / *out of scope* — so the coherence claim is explicit
and its boundary is a documented decision instead of an emergent one.

## Testing and validation

- **Unit/fixture** (runs everywhere, no hardware): a fake `torch.mps`
  namespace drives the MPS tiers exactly as the fake `torch.cuda` does
  today; APU fixtures extend the existing sysfs trees with
  `mem_info_gtt_*` files and fdinfo GTT lines; RAM readings are
  monkeypatched. Rust: probe fixtures for the APU row (flagged, not
  declined), dGPU+APU index integrity, MPS refresh clamps, total
  adoption (seed → authoritative report, sanity bounds, a report above
  `hw.memsize` kept out), CPU board construction.
- **CI**: prerequisite is the 3-OS test workflow (separate task, already
  agreed). Once it exists, the macOS arm64 runners have working MPS —
  add one *real-silicon* smoke: install torch, allocate, verify the
  worker reports `free_source: "mps"` with sane magnitudes and that
  `empty_cache` moves `driver_allocated`. This is coverage ROCm never
  had; use it.
- **Field passes** (the two machines that motivated this doc):
  - *M3 Max 128 GB*: watermark env semantics and default value on the
    shipped torch — **both** ratios, since the spawner now pins the low
    one alongside the high one (torch asserts `high >= low` at allocator
    init, so an ambient user-set low above 1.0 would hard-fail every
    worker; confirm the assertion's form and that 1.0/1.0 is accepted);
    how much the near-ceiling **GC bias** costs — the MPS allocator
    releases cached buffers when an allocation crosses the low watermark,
    so `driver_allocated_memory()` read after a batch that ran close to
    the budget can sit below the true peak, which is the one place the
    monotone-pool approximation understates cost (measure a deliberate
    near-budget batch against a small one); recommended-max adoption on a
    machine with a raised wired limit (this one runs ≈90 %, so the seed is
    ~15 points low and the adoption path is exercised for real), plus
    *re-adoption*: raise `iogpu.wired_limit_mb` with the gateway running
    and confirm the next load re-adopts instead of being refused. Note what
    the window between the two looks like, and that it is accepted: a
    wired-limit change re-prices the board at the next model **(re)load**,
    and until one happens the running replicas' samples now disagree with the
    board's total and are dropped by the currency check, so the board prices
    off the orchestrator's own refresh (which reads RAM statistics and is
    unaffected) plus a one-off WARN per replica. Safe — the stale total is
    the *lower* one after a raise — and self-correcting at the next load;
    end-to-end
    grants/ratchet/knee on CLIP + mpnet; jetsam behaviour
    under deliberate over-budget (does DP-2's death-negative fire);
    compression-regime collapse detection (over-allocate, watch for the
    flag).
  - *BC-250*: KFD topology shape (`unique_id` present? `cpu_cores_count`
    value?); what HIP reports as `total_memory`; GTT spill behaviour
    (crisp error vs slowdown, OOM string forms); fdinfo GTT counter
    names on its kernel; end-to-end grants with the unified total. Plus
    one the naming rule turns up: **a board whose memory the driver carves
    out rather than the BIOS** (the BC-250's 16 GB of GDDR6 is the GPU's
    own, not system RAM taken from the OS) does not fit the
    `MemTotal + carve-out = physical RAM` identity — the sum would count
    that memory twice, overstating the name's capacity, and it can move
    with the driver's memory mode. The name is deterministic per machine
    either way, so nothing mis-prices; what a field pass has to say is
    whether such a board names itself something an operator would find
    absurd, in which case the capacity term needs a per-shape rule rather
    than one arithmetic.
- **Honest limits** (unverifiable before hardware): MPS peak
  approximation quality; watermark default drift across torch versions;
  HIP totals on APUs; whether `ram_available` under memory pressure on
  macOS (compressor inflates "available") is optimistic — if it is, the
  margin lever and the collapse detector are the containment, same as
  the ROCm free-reading optimism already accepted in D5.

## Rollout

1. ✅ **MPS** — smallest surface (one synthetic board, no pinning, no
   multi-device), best verification story (CI silicon + the M3 Max), and
   the largest unpriced user base. Ships with the OOM-classifier
   widening (which incidentally hardens CPU) and the trim extension.
2. ✅ **APU** — extends existing ROCm machinery (probe row flag, two extra
   sysfs files, fdinfo GTT, spawner flag); validation on the BC-250 is still
   outstanding, and every claim about what HIP reports there remains a
   fixture-level claim.
3. **CPU** — the degenerate instantiation, plus `INFERIO_DEVICE`
   coherence; by now it is mostly configuration of existing parts.

Each step is independently shippable; no config or DB migration is
needed at any step (new backends only add keys and enum variants; absent
config keys track serde defaults per the config-authoring rules).

## Decisions

All decided 2026-08-01. DP-3 and DP-4 differ from the draft's
recommendation (amended in review).

| DP | Question | Decision |
|---|---|---|
| 1 | Unified-specific default margin? | No — shipped defaults + RAM-clamped free |
| 2 | Worker death mid-window = negative sample? | Yes, unified boards only |
| 3 | When does Apple Silicon run on CPU? | **Only** on explicit `accelerator = "cpu"`; everything else (auto, `extra=cpu` sentinel, missing sentinel, explicit `cuda`) resolves to MPS |
| 4 | Orchestrator source for MPS total | Seed `hw.memsize × 0.75`; worker's `recommended_max_memory` figure is **authoritative** on first report, sanity-bounded by `(0, hw.memsize]` only — no proximity window (raised wired limits are legitimate and common). A later sane figure outside the cross-check tolerance re-adopts rather than refusing the replica |
| 5 | Worker's unified-board signal | Spawner env `PANOPTIKON_UNIFIED_GPU`; **as implemented** it carries the board's PCI address and the worker acts on it only when that is the board it resolved for itself |
| 6 | RAM capacity in APU calibration name | Yes — `AMD gfx1151 APU (128 GB)` |
| 7 | Price CPU at all | Yes, last |
| 8 | CPU default ceiling | `cap_fraction = 0.75` on the CPU board |
