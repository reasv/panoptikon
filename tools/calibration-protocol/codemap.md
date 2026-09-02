# Batch calibration: code map for the test protocol

Reference material gathered on 2026-09-02 by reading the PR branch
(`claude/batch-calibration-coverage-db9ab9`). Line numbers are from that
day and will drift; grep for the quoted message text or function name
when they do. Three parts: the Rust host, the Python worker, and
build/deploy/API. The plan that uses this is
`docs/batch-calibration-test-protocol.md`.

---

## Part 1: Rust host (`panoptikon/src/inferio/`)

### 1.1 Backend resolution and inventory probe

- Backend (`http.rs:146-151`): user-managed interpreter → config; else
  the venv's setup sentinel `extra=` line (`setup.rs:581-620`); else
  `resolve_accelerator` / `decide_accelerator` (`setup.rs:537-563,
  669-699`): macOS → mps/cpu; `nvidia-smi` on PATH, System32, or
  `/proc/driver/nvidia` → cuda; `/opt/rocm` or `rocm-smi` → rocm; else
  cpu. Filesystem checks only. The one value drives `worker_env`, the pin
  env var, `gpu::probe`, and the calibration `backend` key
  (`accelerator_backend`, `http.rs:271-278`; `Auto → "cpu"`, a
  mis-keying risk on the validation-failure path).
- Probe, **once at startup, nvidia-smi subprocess, no NVML host-side**
  (`gpu.rs:342-365`, `query` `:514-556`): `nvidia-smi
  --query-gpu=index,uuid,name,memory.total,compute_cap
  --format=csv,noheader,nounits`, 5 s timeout
  (`capability::output_with_timeout`, which leaks the thread and child on
  timeout, `capability.rs:181-190`). `parse_inventory`/`parse_row`
  (`gpu.rs:1652-1722`): any bad identity column → whole inventory unknown
  (WARN); `compute_cap=[N/A]` tolerated per row.
- Ambient `CUDA_VISIBLE_DEVICES` (`gpu.rs:363`, `restrict_to_visible`
  `:830-882`): UUID form narrows; **any index-form entry blanks the whole
  inventory at INFO** → no pinning, no ledger, no calibration. Empty
  string = unrestricted. On ROCm any of `ROCR_VISIBLE_DEVICES`,
  `HIP_VISIBLE_DEVICES`, `CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL`
  does the same (`rocm.rs:52-57, 213-241`).
- Multi-GPU: one `GpuInfo{index, uuid, name, total_mb, compute_cap}` per
  row (`gpu.rs:152-238`); ledger keyed by `uuid`. Default board =
  highest compute cap → largest `placement_total_mb` → lowest index
  (`default_board`, `gpu.rs:1224-1232`). **Every unpinned model lands on
  the same board**; other boards only via registry `devices` pins. No
  headroom-based placement.

### 1.2 Live memory readings

- Host refresh (`gpu.rs:772-786` `query_memory_nvidia_smi`): `nvidia-smi
  --query-gpu=uuid,memory.total,memory.free`, 5 s timeout, all-or-nothing
  parse (`:793-820`). Triggered **only** from `VramLedger::request_grant`
  (`ledger.rs:2808` → `maybe_refresh_external`) when `refresh_due`
  (`:865-880`): no refresh in flight for that board, no failure within
  10 s, freshest free sample older than `EXTERNAL_SAMPLE_MAX_AGE = 10 s`
  (`:150`). Runs in `spawn_blocking`; the grant uses the stale value
  meanwhile. **No periodic poller; `/health` never refreshes.** Design
  doc promised an NVML per-process snapshot; the code reads free/total
  only.
- Worker samples are the primary source: load and every predict response
  carry `memory{free_mb,total_mb,free_source,reserved_mb,allocated_mb}`.
  Recorded at registration (`ledger.rs:2008-2020`) and every settle
  (`:3165-3189`) via `record_free_locked` (`:2386-2456`): authoritative
  labels `nvml|nvidia-smi|amdgpu-sysfs|mps|ram` (`:1003-1008`); once a
  board has seen one, `torch` readings never overwrite `free`
  (`:2438-2443`); older samples ignored; a sample whose own `total_mb`
  disagrees with the board (±5 %/512 MB) is discarded with a once-per-
  (model, board) WARN (`:2399-2437`).
- `external = max(0, total − free − Σ footprint(registered workers))`
  (`:2462-2467`); `footprint = base + max(0, reserved −
  reserved_at_load)` (`:641-655`). `base` is measured **by the worker**.
  Everything unregistered is "external": `none`-class models
  (faster_whisper), replicas refused a board, prewarm-parked workers, any
  worker on an unknown-inventory host.

### 1.3 Worker→board identity

- Pin at spawn (`resolve_pin`, `gpu.rs:1329-1420`):
  `CUDA_VISIBLE_DEVICES=GPU-<uuid>` (canonical inventory spelling; index
  pins translated) plus `PANOPTIKON_DEVICE_PIN=<same>` (`worker.rs:627-631`).
- Ledger identity = what the worker reports (`LoadReport.gpu_uuid`),
  resolved by `resolve_board` (`ledger.rs:1640-1710`): UUID match →
  admit; PCI-address match + total cross-check (`total_tolerance_mb`
  `:130-138`); single-board fallback (no UUID reported, claims a GPU,
  total agrees); else `NoBoard` (DEBUG) → unpriced. Pin ≠ reported board
  → `PinDiverged` WARN only. `gpu_bdf` is absent on the shipped CUDA
  torch 2.7.1.

### 1.4 Ledger state, budgets, grants

- One `StdMutex<LedgerState>` (`ledger.rs:1365-1381`), never held across
  await or subprocess. `LedgerState` (`:1054-1082`): `gpus{uuid →
  GpuLedger}` (`:1010-1052`), `workers{id → WorkerEntry}` (`:562-636`:
  `seed_units, base_mb, reserved_at_load_mb, reserved_mb, grants{id →
  GrantCharge{mb,requests,unit_budget}}, pending_requests, ramp_step,
  deflation, clean_windows, fit_watermark, last_trim_at,
  last_grant_settled_at`), `calibration{(inference_id, board_uuid) →
  ModelCalibration}` (`:893-965`: sample ring 64 `FIT_RING`, transients
  32, `fit`, `fit_is_local`, `max_units_measured` (anchor), `seeded`,
  `local_samples`, throughput ring 128 `KNEE_RING`, `knee_best`,
  `knee_units`, `knee_is_local`, `persisted`), `remembered_bases`,
  `remembered_dtypes`, `pending_trims` (cap 32).
- Budgets: `VramBudget{margin = 0.10, cap_fraction = None}` (`:100,
  325-340`); per-board overrides case-insensitive (`:358-390`); from
  `[inference_local.vram]` (`http.rs:240-258`). CPU board only ships
  `cap_fraction = 0.75` (`:417-441`, `cpu.rs:59`). Validation
  (`config.rs:1367-1404`): margin finite ≥ 0; cap_fraction in (0, 1], so
  no per-board way to switch a global cap off.
- Arithmetic (`:2469-2515`): `limit = min(total×cap_fraction, total −
  ceil(external×(1+margin)))`; `headroom = limit − Σcharge −
  Σload_reservations`; `charge = footprint + max(0, grants_mb −
  pool_growth)` (`:673-677`).
- Effective margin (`:2545-2567`): configured + 0.15 while
  `local_samples < 5` (`LOCAL_CONFIRMATION_SAMPLES` `:224`) or cost
  dimension degraded + clamp(residual/base, ≤ 0.25); increment clamped
  at 0.40.
- Share (`:2626-2696`): hungry = requester + same-board workers with
  `pending_requests > 0 && no grant`; appetite = `slope × min(anchor,
  knee)` post-fit else `base` (or 256); floor = `slope × seed` or
  `SEED_BATCH_FLOOR_MB = 256`; pro-rata when floors oversubscribe.
- Unit budget `admitted_units` (`:787-811`): `max(seed <<
  effective_ramp_step, anchor)`, `min(2×anchor)` if anchor > 0
  (`RATCHET_FACTOR`), `min(knee)`, `>> deflation`, `≥ 1`.
  `effective_ramp_step = max(ramp_step, ramp_floor_step(seed, anchor))`
  (`:716-750`, cap 32).
- Window = `admitted_units × 3` (`WINDOW_DEPTH_MULTIPLIER` `:264`).
  Dispatcher bounds (`dispatch.rs:499-511`): priced `{units:
  window_target, items: cap×3 if capped, bytes: MAX_WINDOW_BYTES}`;
  unpriced path bounds by registry `default_batch_size` else
  `default_max_batch` (32) (`dispatch.rs:37-48`).
- Grant `request_grant` (`:2800-2908`): post-fit `units = min(wanted,
  floor(share/slope))`, `mb = ceil(units × slope)`; pre-fit `units =
  wanted`, `mb = share`. **On a full board pre-fit grants carry `mb = 0`
  and are memory-blind** (`:2855-2867, 2879-2880`); post-fit affordable
  = 1 unit. `Grant{unit_budget, mb, unit, aggregation, user_cap_items}`
  encoded on the predict frame (`worker.rs:1722-1741`); fit snapshot
  attached when its version changed (`encode_fit`).
- Ramp / deflation (`:689-727`): clean window **with ≥ 1 high-water
  sample** → `ramp_step + 1`; clean without measurement → no growth;
  while deflated, 3 clean windows (`CLEAN_WINDOWS_TO_RESTORE` `:156`)
  restore one halving; negative → `deflation + 1` (unbounded
  `saturating_add`), `clean_windows = 0`. Per-replica runtime state.
- Settle (`:2923-3000`, `ingest_locked` `:3094-3366`): telemetry ring 256
  (`worker.rs:427`; gap → WARN); `oom || throughput_collapse` →
  negative, discarded; `peak_reserved > reserved_before` → high-water →
  `FitSample{units, peak − reserved_at_load}`, anchor candidate,
  `local_samples++`; warm batch with `units ≥ 0.8 × granted`
  (`FULL_BATCH_RATIO` `:205`) and `duration_ms > 0` → throughput sample.
  `WorkerDied` → unified boards only: anchor halved + deflate
  (`note_unified_death_locked` `:3039-3077`); discrete boards learn
  nothing. Then `refit_locked` (Theil–Sen `robust_fit` `:4141-4177`: ≥ 3
  samples, distinct x, slope > 0, else the old fit is kept) and
  `refit_knee_locked`.
- Knee `fit_knee` (`:4244-4297`): log2 buckets, median per bucket, ≥ 12
  samples over ≥ 3 buckets, threshold 0.9 × max(ring best, historical
  `knee_best`), knee = first bucket ≥ threshold and < largest bucket,
  returned as `2^(k+1) − 1`. Sticky. A shipped knee is adopted at seed
  time when no local knee exists and can only ratchet down within a run.
- Load reservation `reserve_load` (`:1476-1573`): `max(remembered base,
  store expected_base)` else `CONSERVATIVE_BASE_MB = 4096`; only WARNs
  when it exceeds headroom ("loading this model is expected to need more
  VRAM than the board's remaining headroom"); the load proceeds.
- Idle trim `flag_trims_locked` (`:2727-2768`): other replicas on the
  board with no grant, `pending_requests == 0`, last settle ≥ 5 s ago,
  `pool_growth ≥ 256 MB`, debounce 30 s → `deliver_pending_trims`
  (`manager.rs:1120-1131`) → `try_trim` (`dispatch.rs:762-790`, dropped
  if the replica is busy) → `Worker::trim` with **fatal `TRIM_DEADLINE =
  60 s`** (`worker.rs:113`) → `note_trimmed` (`:3522-3561`).
- Seeding (`register_worker` `:1931-2066` → `seed_calibration_locked`
  `:2123-2238`): once per (model, board) per run; fit adopted if none
  and slope > 0; knee adopted even from a baseline; anchor/ring/
  `local_samples` only from a local profile with the exact torch string.

### 1.5 Persistence (`calibration.rs`)

- Local store `<data_folder>/inferio/calibration.toml`
  (`StorePaths::beside_registry` `:478-486`); shipped baselines
  `<registry_dir>/calibration/*.toml` (built-in
  `python/inferio/config/calibration/`, plus user
  `config/inference/calibration/`), local-authority fields stripped on
  import (`:242-247`). None ship yet.
- `CalibrationProfile` (`:123-207`): key `inference_id, epoch, gpu (model
  name as nvidia-smi prints it), platform (std::env::consts::OS),
  backend, torch, dtype, unit, aggregation`; measurement `base_mb,
  base_method, slope_mb_per_unit, knee_units, samples, residual_mb,
  measured_at, generator`; local-only `max_units_measured,
  local_samples, sample_units[], sample_reserved_mb[]`. `schema = 1`.
- Write policy `pending_update_locked` (`ledger.rs:2266-2328`): needs
  torch, dtype, base_mb, `local_samples > 0`; fires on anchor advance,
  fit version change, local knee change; anchor monotone; debounce 30 s
  (`calibration.rs:104`); atomic temp+rename; merge on same key
  (`:830-869`); flushed on manager shutdown (`manager.rs:1059-1061`).
- Trust: lookup needs torch+dtype; dtype exact, torch exact or
  `major.minor`; stale epoch / unit mismatch silently ignored; ring
  length mismatch → ring dropped; newer schema → file ignored; **invalid
  TOML → treated as empty and overwritten on next write** (`:1219-1230`);
  I/O read error → write deferred. Deletion is honoured at next lookup
  (mtime, `:572-590`) **unless** the process has pending in-memory
  updates (`:586-588`). **Reset = delete the file with the server
  stopped.** No reset or disable endpoint exists.

### 1.6 Failure paths

1. Impl `run_with_oom_retry` (`python/inferio/impl/utils.py:422-545`):
   torch OOM / `MemoryError` / "out of memory" → `clear_cache()`, chunk
   halved, unbounded halvings to 1; single-item OOM →
   `InferenceOOMError("INFERENCE_OOM_BATCH_SIZE_1: …")`. Absorbed
   halvings mark the successful batch `oom = True` → negative sample.
2. Harness `run_window` (`packing.py:685-830`): no in-harness retry; a
   multi-item OOM is prefixed `INFERENCE_OOM_WINDOW:`; `WindowFailure`
   carries the measurements.
3. Host classification `message_reports_oom` (`ledger.rs:4111-4124`):
   the two prefixes, "CUDA out of memory", "HIP out of memory", any line
   containing "out of memory" (case-insensitive), the
   `DefaultCPUAllocator`/"allocate memory" pair; applied by
   `dispatch::error_reports_oom` (`dispatch.rs:982-991`) to message +
   traceback only, never the stderr tail.
4. Dispatcher (`dispatch.rs:836-1161`): merged window failure → WARN
   "merged batch of {n} requests failed, falling back to per-request
   prediction" (`:913`) → **one** sequential per-request retry pass under
   the same grant, `unit_budget / 2` if OOM (`halved_for_retry`); a
   single-request window gets no retry. Worker not restarted on a
   per-request error.
5. Ledger: `Responded{oom: true}` → `deflation + 1`; nothing enters
   fit/anchor.
6. Core (`jobs/extraction.rs`): predict failure → `isolate_inputs` once
   with `max_batch = 1` (`:1402-1425`); still failing → item marked
   transient, job continues; job fails "Systemic" only if every
   attempted item failed for non-media reasons. `inferio_client.rs:383-389`
   retries only 429/502/503/504/connect/timeout, not a 500 from an OOM.
- Worker death mid-window: `roundtrip` EOF → fatal (`worker.rs:1305-1315,
  1422-1444`); dispatcher `End::Fatal` fails the queue, aborts sibling
  windows, kills all replicas; `handle_worker_death`
  (`manager.rs:1068-1085`) WARN "worker died fatally; dropping model from
  all caches". **The next predict respawns immediately, serially under
  `load_lock`, with no counter, backoff or cap.**
- Deadlines (`worker.rs:119-145`): handshake/configure/ping 30 s,
  load/prewarm 600 s, unload_grace 10 s, terminate_grace 5 s
  (`[inference_local] handshake_secs/load_secs/unload_grace_secs/
  terminate_grace_secs`); `TRIM_DEADLINE` 60 s fixed; **predict has no
  deadline**; a hung worker holds its grant forever. The manager's
  `load_lock` is taken at the top of every predict (`manager.rs:1161`).
- Core request sizing: `REQUEST_UNIT_BUDGET = 64` (`extraction.rs:66`)
  is both the per-request chunk and the **total** in-flight unit
  semaphore per job (`:535`); master sized that semaphore by the user's
  batch size. Being replaced by the feedback signal (plan §8 G7). Other
  core bounds: `loader_concurrency` default 8, intermediate byte budget
  default 1024 MB (`config.rs:530-536`).

### 1.7 Control surfaces

- Server TOML: `[inference_local.vram] margin` (default 0.10),
  `cap_fraction` (default off), `[inference_local.vram.gpu."GPU-<uuid>"]
  {margin?, cap_fraction?}`; commented examples in all five profiles.
  `accelerator = "mps"` is a new variant. Worker deadline keys above.
- Registry: `metadata.cost{unit, aggregation, epoch, seed_units}`
  (`cost.rs:1-33, 166-336`; missing → `(item, count)`, seed 4,
  `degraded = true`); `devices = [...]` pins; `metadata.default_batch_size`
  is only the unpriced-window bound now.
- Per-DB config: `cron_jobs[].batch_size` and
  `job_settings[].default_batch_size` are `Option<i64>` caps, `None` =
  auto; cap chain `resolve_job_defaults` (`extraction.rs:1865-1908`):
  request > per-ID setting > group setting. `data_log.batch_size` stores
  0 for auto. `POST /api/jobs/data/extraction?batch_size=` is an optional
  cap. The Desktop wizard cannot clear a stored cap
  (`api/desktop.rs:1132-1146`).
- Env read by host: `CUDA_VISIBLE_DEVICES` (`gpu.rs:363`); ROCm
  visibility vars; ROCm path/MIOpen vars (`accelerator_env.rs:157-202`).
  **No `PANOPTIKON_*`/`INFERIO_*` env var disables calibration.**
- Env set on workers (`worker.rs:597-681`): `INFERIO_WORKER=1`,
  `PYTHONIOENCODING=utf-8`, `PYTHONHOME` removed, `PYTHONPATH`,
  `CUDA_VISIBLE_DEVICES=<uuid>` or `HIP_VISIBLE_DEVICES=<index>`,
  `PANOPTIKON_DEVICE_PIN`, `PANOPTIKON_UNIFIED_GPU=<bdf>` (unified ROCm),
  ROCm: `ROCM_PATH, HIP_PATH, LD_LIBRARY_PATH, MIOPEN_FIND_MODE=FAST,
  MIOPEN_USER_DB_PATH, MIOPEN_CUSTOM_CACHE_DIR`; MPS:
  `PYTORCH_MPS_HIGH/LOW_WATERMARK_RATIO=1.0` unconditionally; CPU:
  `INFERIO_DEVICE=cpu`. `PYTORCH_CUDA_ALLOC_CONF` is never set (inherited
  from the gateway). Model `env` touching visibility vars → WARN.
- `GET /api/inference/health` (`http.rs:718-744`): `gpus: [GpuInfo]`;
  `vram: [{gpu_uuid, gpu_name, total_mb, external_mb, external_known,
  external_source?, external_sample_age_ms?, limit_mb, headroom_mb,
  charges_mb, footprints_mb, load_reservations_mb, grants_mb,
  grants_outstanding, margin, cap_fraction?, workers: [{inference_id,
  footprint_mb, charge_mb, base_mb?, reserved_at_load_mb?, reserved_mb?,
  grants_outstanding, grants_mb, pending_requests, seed_units, ramp_step,
  deflation, clean_windows, unit_budget, max_units_measured, knee_units?,
  knee_is_local, throughput_samples, local_samples, effective_margin,
  fit?{slope_mb_per_unit, intercept_mb, residual_mb, samples,
  transient_samples}}]}]` (`ledger.rs:3654-3732, 4318-4408`); `models[]`
  has `last_grant_units` (renamed from `last_effective_cap`),
  `last_window_items`, `cost{unit, aggregation?, epoch, seed_units?,
  degraded}`, `replicas_detail[{gpu, gpu_uuid, gpu_name, gpu_bdf?,
  torch_version, dtype, base_mb, base_method, free_mb, total_mb,
  free_source, allocated_mb, reserved_mb, reserved_at_load_mb,
  memory_age_ms, measurements_recorded, recent_batches[]}]`
  (`manager.rs:148-179`).
- `GET /api/inference/metadata` (`http.rs:672-700`,
  `calibration::overlay_metadata` `calibration.rs:1108-1168`): per priced
  id a `calibration` key `{status: "local"|"baseline", gpu, dtype,
  base_mb, slope_mb_per_unit, samples, local_samples, max_units_measured,
  knee_units|null}` or `{status: "uncalibrated", gpu}`; from the store,
  default board only; omitted when no inventory.

### 1.8 Log lines (targets are module paths; worker stderr is forwarded at INFO as `worker=<impl_class> "{line}"`, `worker.rs:1551`)

Added by commit `49822c8b` (ledger.rs / calibration.rs):
- DEBUG "issued a memory grant" (model, gpu, unit_budget, mb, share_mb,
  headroom_mb, external_mb, pre_fit, ramp_step, deflation, squeezed,
  window_requests)
- DEBUG "settled a granted window" (model, gpu, outcome
  clean|negative|aborted|worker_died, high_water_samples,
  throughput_samples, ramp_step, deflation, clean_windows,
  max_units_measured); **WARN** with `reason`
  oom|throughput_collapse|unified_board_death on negatives
- DEBUG "refitted the memory cost model" (slope_mb_per_unit,
  intercept_mb, residual_mb, samples, version)
- DEBUG "refreshed the board's free memory from the host probe" (gpu,
  source, free_mb, total_mb, external_mb, previous_age_ms, recorded);
  WARN on the first failure of a streak (backoff_secs), DEBUG thereafter
- DEBUG "admitted a worker to a board's ledger" (model, gpu, replica,
  base_mb, base_method, reserved_at_load_mb, seeded_from_store)
- DEBUG "queued a calibration profile update for the store" (reason
  fit_changed|knee_changed|anchor_advanced, max_units_measured,
  fit_version); INFO "wrote the local calibration store" (path, profiles)

Pre-existing:
- gpu.rs: INFO "detected GPU" (index, uuid, name, total_mb,
  compute_cap); INFO "CUDA_VISIBLE_DEVICES names devices by index;
  leaving the GPU inventory unknown"; WARN "CUDA_VISIBLE_DEVICES names no
  board nvidia-smi reports"; INFO "restricting the GPU inventory to the
  ambient CUDA_VISIBLE_DEVICES"; WARN "…configured for CUDA but
  nvidia-smi was not found"; WARN "nvidia-smi GPU probe failed or timed
  out"; WARN "nvidia-smi exited nonzero"; WARN "unparseable nvidia-smi
  row"; WARN "nvidia-smi reported no GPUs"; WARN "device pin does not
  name a visible GPU…".
- ledger.rs: WARN "loading this model is expected to need more VRAM than
  the board's remaining headroom"; DEBUG "the worker reports no board
  this GPU inventory lists; dispatching this model without VRAM
  admission"; WARN "this worker is on a PCI address no board in the GPU
  inventory has"; WARN "the worker's own total-VRAM reading does not
  agree…" / "this worker reports no total VRAM"; WARN "this replica was
  pinned to one board and came up on another"; DEBUG "seeded calibration
  from a stored profile"; WARN "discarding this worker's free-memory
  samples for the board it was admitted under"; DEBUG "an idle resident
  is holding allocator pool slack while a neighbour's window was
  squeezed; asking it to release the pool"; WARN "batch measurements
  were evicted from this replica's telemetry ring"; DEBUG "fitted a
  throughput knee"; WARN "this replica died while running a granted
  window on a board whose memory is the machine's own…".
- dispatch.rs: DEBUG "the ledger refused a grant; dispatching this
  window on the unpriced path"; WARN "merged batch of {n} requests
  failed, falling back to per-request prediction" (oom field); DEBUG
  "this replica declined to release its allocator pool"; DEBUG
  "splitting {n} inputs across several predict frames".
- manager.rs: DEBUG "resolved cost dimension"; DEBUG "replica loaded";
  WARN "worker died fatally; dropping model from all caches"; DEBUG
  "unloading model".
- worker.rs: DEBUG "worker reported its load footprint" (base_mb,
  base_method, dtype, gpu_uuid, gpu_bdf, gpu_total_mb, torch).
- calibration.rs (WARN): store cannot be read / serialize failure /
  failed to write / failed to read a calibration file / not valid TOML /
  declares a newer schema / "ignoring profile N of M in <path>".
- batch_auto.rs: INFO "batch sizes in the stored job settings and cron
  schedule were reset to auto"; WARN "pre-upgrade batch sizes in {path}
  could not be cleared automatically…".
- Python (forwarded): "GPU OOM on a chunk of %d inputs; retrying at %d."
  (utils.py:531); "free memory fell to %d MiB against a %d MiB grant;
  shrinking this batch's budget from %d to %d units" (packing.py:457);
  "grant fell to %d MiB against %d MiB of releasable slack … released the
  pool (empty_cache)" (:261); "the impl's own halving loop absorbed %d
  out-of-memory condition(s)…" (:798); "batch of %d inputs (%d %s units)
  ran at %.0f units/sec against %.0f …; treating it as a memory spill"
  (:669); "retiring the throughput comparator" (:655); "this impl has its
  own batching disabled; ignoring memory grants" (__main__:298);
  "released the allocator pool on request" (:403); "NVML lists no process
  with pid %d" (memory.py:408).

Filter: `RUST_LOG=info,panoptikon::inferio=trace,panoptikon::db::batch_auto=debug`
and `INFERIO_WORKER_LOG_LEVEL=DEBUG` in the gateway's environment
(workers inherit it).

### 1.9 Migration (`db/batch_auto.rs`, SQL `20260903120000_batch_auto_stamp.sql`)

Table `batch_auto_migration(id = 1, applied_at)` created empty; the Rust
step runs after SQL migrations for index DBs only (startup default DB,
the all-DB sweep, Desktop new-DB, `POST /api/db/create`). Readonly mode
→ skip; stamp present → skip; `!fresh` → `clear_config_batch_sizes`
(reads the sibling `config.toml` directly, `.take()`s every
`job_settings[].default_batch_size` and `cron_jobs[].batch_size`, saves
through `TomlDocument::patch_serialized`, which removes the keys and the
comment directly above each; errors only WARN); then `INSERT OR IGNORE`
the stamp, the only step whose failure blocks startup. No backup, not
reversible.

### 1.10 Suspected weak points (ranked; plan §5 maps each to a scenario)

1. Zero-MB grants on a full board disable the worker's clamp (B1).
2. Index-form `CUDA_VISIBLE_DEVICES` silently disables the feature (B6).
3. Silent respawn loop with no backoff after worker death (B15).
4. `TRIM_DEADLINE = 60 s` fatal for the whole model (B17).
5. Design deviations: nvidia-smi free/total only, grant-driven refresh;
   unpriced path uses `default_max_batch`.
6. Reset while running unreliable; no reset/disable surface (B3).
7. Persisted anchor is a permanent floor across torch patch bumps (B4);
   seeded knee only ratchets down (B5).
8. Unbounded deflation with linear recovery (B8).
9. `REQUEST_UNIT_BUDGET = 64` ceiling (B16; being fixed).
10. `load_lock` on every predict (B18).
11. Container without `--pid=host` degrades base measurement (B9); CPU
    board is cgroup-blind (B19).
12. Stamp insert failure blocks startup; migration irreversible.
13. `/health` `last_effective_cap` → `last_grant_units` rename (B22).
14. `accelerator_backend(Auto) → "cpu"` mis-keying (B23).
15. Post-trim zero-delta fit samples; old fit kept forever (B10).
16. `message_reports_oom` substring false positives (B11).
17. Prewarm-parked workers invisible to the ledger (B21).
18. `predict_chunked` reuses one grant across payload-split frames.
19. MPS watermark env forced unconditionally.
20. ROCm platform assumptions (`gfx_target_version` required, APU =
    `cpu_cores_count > 0`, PCI function forced `.0`, KFD order = HIP
    order unverified).

---

## Part 2: Python worker (`python/inferio_worker/`, `python/inferio/impl/`)

### 2.1 Measurement (`memory.py`)

- Stdlib-only imports; torch used only if already in `sys.modules`
  (`_torch()` :152) and CUDA is initialised (`_torch_cuda()` :156-176).
  `pynvml` lazy, `nvmlInit` memoized (:249-267), handle re-resolved until
  success (:195-246).
- CUDA readings: reserved/allocated/peaks via `torch.cuda.memory_reserved
  / memory_allocated / max_memory_reserved / max_memory_allocated`
  (`_allocator_stats` :1827-1857); `reset_peak_memory_stats`
  (`_reset_peaks` :1817-1824); free/total via
  `nvmlDeviceGetMemoryInfo` (:356-366) then `torch.cuda.mem_get_info`
  (:1941-1950); own-PID via `nvmlDeviceGetComputeRunningProcesses` →
  `usedGpuMemory` for `os.getpid()` (:369-415; a reading ≥ board total is
  rejected); identity via `torch.cuda.get_device_properties(0)`
  (:653-703, :706-773, :796-826). `memory_stats()` unused.
- NVML handle (:270-309): `CUDA_VISIBLE_DEVICES` starting `GPU-`/`MIG-`
  → by UUID (prefix match fallback); else torch UUID after CUDA is live;
  else count == 1 → index 0. **An index pin is never mapped to an NVML
  index**, so on multi-GPU with an index pin and no live CUDA the NVML
  paths are skipped.
- Per batch: `begin_batch()` (:2307-2318) resets peaks and snapshots
  `reserved_before_mb`, `allocated_before_mb`, `started`;
  `measure_batch()` (:2321-2376) emits `items, reserved_before_mb,
  peak_reserved_mb, allocated_before_mb, peak_allocated_mb, duration_ms`
  (predict call only), optional `units`, `oom`, `throughput_collapse`;
  the harness adds `trimmed` on a window's first measurement
  (`packing.py:712-721`). **Caches are not emptied between batches**;
  `empty_cache()` only on an orchestrator `trim` (`__main__.py:381-413`),
  the reactive shrink (`packing.maybe_shrink` :196-274), or inside
  `run_with_oom_retry` after an OOM. Peaks reset at the end of
  `finish_load` (:2080).
- Tiers (`_free_total_mb` :1860-1951, tried by availability):

  | Backend | free/total | pool/allocated | base tiers (`_resolve_base` :2084-2198) |
  |---|---|---|---|
  | CUDA | `nvml` → `torch` | torch allocator | `nvml` own-PID → `free_delta` → `alloc_delta` |
  | ROCm | `amdgpu-sysfs` (+GTT on a verified unified board) → `torch`; NVML refused when `torch.version.hip` or `HIP_VISIBLE_DEVICES` set | torch allocator | `fdinfo` (DRM `drm-resident-vram`, floored at `reserved − 256 MiB`) → `free_delta` → `alloc_delta` |
  | MPS | `mps` = `min(recommended_max_memory, psutil available)` (:1326-1347) | `driver_allocated_memory` / `current_allocated_memory`; **no peak API**, post-batch values reported as peaks | `mps` (`driver_allocated_memory` at load end) |
  | CPU (`INFERIO_DEVICE=cpu`) | `ram` = psutil available/total (:1519-1543) | pool = OS RSS high-water (`VmHWM` Linux, `peak_wset` Windows, `ru_maxrss` else), allocated = live RSS (:1634-1690) | `rss` = load-window RSS growth |

- `base_mb` (`_finish_load` :2017-2081, `_resolve_base`): `touched_gpu`
  = allocated or reserved delta > 0 across the load window, else no base
  at all; CPU → `(alloc_floor, "rss")`; NVML own-PID > 0 → `"nvml"`;
  fdinfo → `"fdinfo"`; MPS → `"mps"`; else `free_delta = before.free −
  free_after` (same source both sides), ceiling `reserved_delta + 500
  (CONTEXT_ESTIMATE_MB) + 2048 (IMPLAUSIBLE_SLACK_MB)`; `free_delta`
  None/≤ 0/> ceiling → `(alloc_floor + 500, "alloc_delta")`; ≥
  `alloc_floor` → `(free_delta, "free_delta")`; else `alloc_delta`.
- **Docker without `--pid=host`**: NVML reports host PIDs, so
  `os.getpid()` is never listed; one INFO line ("NVML lists no process
  with pid … expected in a container started without --pid=host",
  :403-413) and base falls to `free_delta` (board-wide, contaminated by
  concurrent activity in the load window, plausibility-capped) or
  `alloc_delta + 500`. Board-level NVML free/total still works, so
  `free_source` stays `"nvml"`.
- Load response also carries `reserved_at_load_mb`, `dtype`
  (`resolved_dtype_name` :2260-2299), `gpu_uuid` (`GPU-<uuid>`,
  suppressed on HIP), `gpu_name`, `gpu_bdf` (absent on torch 2.7.1),
  `gpu_total_mb`, `torch_version`, `memory`.

### 2.2 Protocol (host↔worker)

- Host → worker `predict` frame: `grant = {unit_budget, mb, unit,
  aggregation, user_cap_items|nil}` (`ledger.rs:3953-3961`;
  `worker.rs:1722-1739`), `fit = {slope_mb_per_unit, intercept_mb,
  residual_mb, samples}` only when the version changed and only on the
  first chunk of a multi-frame window (`dispatch.rs:1140`). A grant is
  sent only when the replica has an admission; `none`-class models never
  get one. After a merged-window OOM, per-request retries get
  `unit_budget / 2`. New `trim` request type.
- Worker (`__main__.py:291-340`): `fit` is **ignored** (advisory in v1).
  Grant present and `packing.batching_disabled(instance)` false →
  `packing.run_window`. No grant, or `enable_batching`/`enable_batch`
  present-and-falsy (`packing.py:509-531`) → single `instance.predict`
  bracketed by `begin_batch/finish_batch`, one measurement, **no
  `units`**; logged once per worker.
- Grantless impls: `MoondreamTagger`/`MoondreamCaptioner`
  (`enable_batching = False` class attr), `EasyOCRModel` with
  `config.enable_batching = false` (all three shipped easyocr ids,
  `inference.toml:257,275,284`), `DotsOCRModel`/`Florence2` if configured
  off (default on), plus every `none`-class id (whisper, tagmatch, jina
  APIs, vlm, moondream taggers).
- Worker → host: `load` ok carries `base_mb, base_method,
  reserved_at_load_mb, dtype, gpu_uuid, gpu_name, gpu_bdf, gpu_total_mb,
  torch_version, memory` (parsed `worker.rs:1672-1681`); `predict` ok
  carries `outputs` (order restored, `packing.py:809-812`),
  `measurements[]` (one per GPU batch), `memory`; `predict` error carries
  `message`, `traceback`, plus `measurements`/`memory` when the exception
  is a `WindowFailure` (grantless-path failures carry none); `trim` ok
  carries a post-`empty_cache` sample.

### 2.3 OOM handling

- Classifiers (three copies, change together): `utils.looks_like_oom`
  (:390-419: "out of memory" case-insensitive, `INFERENCE_OOM`,
  `defaultcpuallocator` + `allocate memory`, over `exc`, `__cause__`,
  `__context__`); `packing._looks_like_oom` (:473-506: same strings plus
  type names `OutOfMemoryError`, `InferenceOOMError`, `MemoryError`);
  Rust `message_reports_oom` (Part 1 §1.6).
- Harness on any exception (`packing.py:736-765`): unpriced measurement
  with `oom` flag if classified or the halving counter moved; multi-item
  OOM → `INFERENCE_OOM_WINDOW:` prefix; `WindowFailure`. A batch that
  succeeded but halved internally: `oom: true`, no `units` (:781-805).
- Survival: predict errors (OOM or not) → error frame, worker keeps
  serving. Exits only on handshake failure (1), `ProtocolError` (2),
  uncaught error outside the loop (3), stdin EOF (0), or a real kill.

### 2.4 Packing (`packing.py`)

- Pricing (`price_inputs` :333-363): `pixel` = `w × h` from
  `PIL.Image.open(BytesIO(file)).size` (header only, **raw submitted
  dimensions**; unreadable → largest priced so far, else
  `UNREADABLE_PIXEL_UNITS = 2_000_000`); `token` = `max(1, utf8 bytes //
  4)` (`BYTES_PER_TOKEN = 4`); `audio-second` = flat 30; `item` = 1.
- Planning (`plan_batches` :384-434): `count` = len; `sum` = greedy FIFO;
  `max-times-count` = sort descending by units then greedy; a single
  over-budget item goes alone; `cap_items` is a separate bound;
  re-planned before every batch.
- Defensive clamp (`clamp_to_live_memory` :437-465): if `grant_mb > 0`
  and live free < grant_mb, `budget = max(1, unit_budget × free /
  grant_mb)`; shrink-only. **No-op when `grant_mb <= 0`.**
- Throughput collapse (`_note_throughput` :622-682): `COLLAPSE_RATIO =
  0.4`, `COMPARATOR_MAX_AGE = 8`; comparable only if pool grew, priced,
  `units ≥ previous`; flagged batch does not become the comparator;
  reset on any `empty_cache()`.
- Reactive shrink (`maybe_shrink` :196-274): `SHRINK_RATIO = 0.8`,
  `SHRINK_WINDOWS = 2`; once per window before the first batch; `slack =
  reserved − allocated`; `grant_mb < 0.8 × slack` for 2 consecutive
  windows → `empty_cache()`, `trimmed: true`. No-op when `grant_mb <= 0`.
- Controlled-cost inputs: pixel = any image header of chosen `w × h`;
  token = text of `4 × k` bytes; item = anything.

### 2.5 Model table (`python/inferio/config/inference.toml`)

| inference_id | impl_class | unit / aggregation | seed | epoch | notes |
|---|---|---|---|---|---|
| tags/wd-{swinv2,convnext,vit,eva02-large,vit-large}-tagger-v3 | wd_tagger | item / count | 8 | 1 | `run_with_oom_retry` |
| tags/moondream-2b-25-03[-clothing] | moondream_tagger | none | – | 2 | `enable_batching = False` |
| tagmatch/danbooru[-saucenao] | danbooru_tagger | none | – | 1 | network |
| doctr/db_resnet50_* (7) | doctr | item / count | 8 | 1 | docTR re-batches internally |
| doctr/dots_ocr | dotsocr | pixel / sum | 2 000 000 | 1 | min CC 8.0, ~6 GB |
| doctr/easyocr_standard_{en,en_ja,en_ch_sim} | easyocr | pixel / max-times-count | 2 000 000 | 1 | **`enable_batching = false`** → grantless |
| florence2/msft_large-* (4) | florence2 | item / count | 4 | 1 | |
| vlm/moondream-2b-25-03-* (5) | moondream_captioner | none | – | 2 | |
| textembed/all-mpnet-base-v2, all-MiniLM-L6-v2, stella_* | sentence_transformers | token / max-times-count | 4000 | 1 | no impl-side OOM retry |
| textembed/jina-embeddings-v3-api | jina-clip-api | none | – | 1 | remote |
| whisper/* (15) | faster_whisper | none | – | 1 | CT2, no torch allocator |
| clip/ViT-H-14-*, PE-Core-*, ViT-B-16-SigLIP2-384, apple_MobileCLIP-{B-LT,S2,S1} | openclip | item / count | 8 | 1 | `run_with_oom_retry` ×2 |
| clip/qwen3-vl-embedding-{8b,2b} | qwen3-vl-embedding | pixel / sum | 2 000 000 | 1 | |
| clip/nemotron-embed-vl-1b-v2 | nemotron-embed-vl | pixel / sum | 2 000 000 | 1 | ~2.5 GB |
| tclip/<openclip ids> | openclip | item / count | 8 | 1 | text tower |
| tclip/qwen3-vl-embedding-{8b,2b} | qwen3-vl-embedding | token / max-times-count | 4000 | 2 | |
| clap/clap-htsat-unfused, larger_clap_* | clap | item / count | 8 | 1 | **no `run_with_oom_retry`** |

Smallest per class: `tags/wd-vit-tagger-v3` (~350 MB),
`clip/apple_MobileCLIP-S1` (~170 MB), `textembed/all-MiniLM-L6-v2`
(~90 MB), `doctr/db_resnet50_crnn_mobilenet_v3_small` (~110 MB),
`doctr/easyocr_standard_en` (~95 MB, batching off by default),
`clip/nemotron-embed-vl-1b-v2` (~2.5 GB), `clap/clap-htsat-unfused`
(~600 MB), `whisper/tiny` (~75 MB), any `vlm/moondream-*` (~3.7 GB).

### 2.6 Tests and fixture impls

- Run: `cd python && uv sync --group test --extra cu128 && .venv/bin/python
  -m pytest tests/inferio_worker tests/inferio/impl -q`. The extra is
  mandatory or torch resolves from PyPI. Integration tests need
  `PANOPTIKON_RUN_INTEGRATION=1`. `test_worker_protocol.py` spawns real
  `python -m inferio_worker` with `PYTHONPATH=python`, `NO_CUDNN=true`,
  `INFERIO_WORKER=1`.
- Fixtures (`python/tests/inferio_worker/fixture_impls/`), discovered by
  `IMPL_CLASS.name()` (`discovery.py:22-54`), stdlib-only, no
  `InferenceModel` subclassing: `nobatching_impl.py`
  (`"nobatching_test"`, `enable_batching = False`, outputs `{"batch":
  n}`); `oom_second_batch_impl.py` (`"oom_second_batch_test"`, raises
  `RuntimeError("CUDA out of memory. Tried to allocate 2.00 GiB
  (fixture)")` on the 2nd predict per instance); `subbatching_impl.py`
  (real `run_with_oom_retry`, `initial_chunk_size=1`); `oom_impl.py`
  (always `INFERENCE_OOM_BATCH_SIZE_1:`); `failbatch_impl.py` (non-OOM
  `ValueError` for batch > 1); `echo_impl.py`, `batchsize_impl.py`,
  `dying_impl.py`, `die_on_flag_impl.py`, `hang_impl.py`,
  `slow_impl.py`, `device_impl.py` (echoes `CUDA_VISIBLE_DEVICES`).
- Registering with a real host: copy into `inferio_custom/` (default
  impl dir; `inferio_custom/README.md`) or set `[inference_local]
  impl_dirs`, then a user registry TOML in `config/inference/` (scanned
  after the built-in dir; see `config/inference/example.toml` and the
  manager's test registry at `manager.rs:1884-1935`), e.g.
  ```toml
  [group.oomtest]
  config.impl_class = "oom_second_batch_test"
  [group.oomtest.metadata]
  default_batch_size = 64
  target_entities = ["items"]
  output_type = "tags"
  input_mime_types = ["image/"]
  [group.oomtest.metadata.cost]
  unit = "item"
  aggregation = "count"
  seed_units = 8
  [group.oomtest.metadata.input_spec]
  handler = "image_frames"
  [group.oomtest.inference_ids.test]
  ```
  Caveats: torch-free fixtures report no `gpu_uuid`/`base_mb`, so on CUDA
  they register only via the single-board fallback (single visible
  board) or run unpriced; a fixture that allocates one CUDA tensor at
  load registers normally. `oom_second_batch` fires once per worker
  lifetime. The extraction job's `output_type` must match what the
  fixture returns.

### 2.7 Env vars honoured by the worker

`INFERIO_DEVICE=cpu` (`utils.py:23-51`, `memory.py:1498-1516`: forces
`get_device()` to cpu and the `ram`/`rss` tiers); `CUDA_VISIBLE_DEVICES`
(NVML handle by UUID when `GPU-`/`MIG-` prefixed); `HIP_VISIBLE_DEVICES`
(non-empty ⇒ NVML refused); `PANOPTIKON_DEVICE_PIN` (if set and torch
is accelerated but `device_count() == 0`, load **fails** with an
actionable error); `PANOPTIKON_UNIFIED_GPU=<bdf>` (GTT counted only when
it equals the worker's own resolved BDF); `PYTORCH_MPS_HIGH/LOW_WATERMARK_RATIO`;
MIOpen/ROCm path vars; `NO_CUDNN`; `INFERIO_WORKER_LOG_LEVEL`;
`PYTORCH_CUDA_ALLOC_CONF` (inherited, never set or read by the repo);
model `config.env`/`env_remove` applied last.

### 2.8 Worker-side fragility

NVML PID mismatch in containers; `free_delta` contamination; fixed
500 MiB context estimate; reserved-vs-allocated quantisation and
`expandable_segments` semantics; cuDNN benchmark workspace spikes on new
shapes; raw-dimension pixel pricing (20 MP charged 10× real cost for
capped VLMs) and `bytes/4` token pricing (CJK under-priced ~3×, long
texts over-priced); measurement brackets CPU decode time (collapse
detector can trip on slow-decoding inputs); no NVML until torch
initialises CUDA on a multi-GPU index pin; `touched_gpu` gate misses
impls that initialise CUDA before `load()`; grantless-path failures carry
no measurements; comparator not reset after an absorbed OOM; MPS peaks
are post-batch values; CPU high-water is lifetime-monotone; `gpu_bdf`
absent on torch 2.7.1 and a MIG UUID may not match the inventory key.

---

## Part 3: Build, run, deploy, drive

### 3.1 Bare Linux

- Build: `cargo build --release -p panoptikon` (plain dev build reads
  `python/`, `config/`, `ui/` from the tree). `.cargo/config.toml` sets
  `LIBSQLITE3_FLAGS=-DSQLITE_ENABLE_MATH_FUNCTIONS`. Features `bundled`,
  `bundled-ui` are the release/CI form, not needed here.
- UI: the gateway itself runs `npm install` → `next build` → `next
  start` on `http://127.0.0.1:6340` when `[upstreams.ui] local = true`
  (`panoptikon/src/ui.rs`); node from the venv's `nodejs-wheel` or PATH.
  Set `[upstreams.ui] local = false` in a copied config for API-only
  runs, or ignore the 502s on `/`.
- Python env: `target/release/panoptikon setup [--accelerator …]
  [--force] [--if-needed]` → `uv venv --python 3.12` + `uv sync --locked
  --extra cu128` in `python/`; auto-runs at startup when the sentinel
  (`python/.venv/.panoptikon-setup-complete`, holds `extra=` and the
  `uv.lock` sha) is stale, serialised on `runtime/setup.lock`. uv: PATH
  0.6.14 (floor 0.6.13) or the pinned 0.11.28 download into
  `runtime/uv/<ver>/`.
- Run: `./start.sh` (needs the release binary) or
  `target/release/panoptikon [--config PATH] [--root DIR]
  [--disable-update-check]`; subcommands `inferio` (inference-only
  service), `setup`, `accelerator` (prints resolved backend + GPUs),
  `update`. Config: `--config`, else `PANOPTIKON_CONFIG_PATH`, else
  `config/server/default.toml` relative to CWD; `.env` at CWD feeds
  `${VAR}` templates (`LOGLEVEL`, `PDFIUM_PATH`, `HTML_RENDERER_PATH`,
  `PANOPTIKON_FONT`, `SAUCENAO_API_KEY`, `JINA_API_KEY`) plus
  `PANOPTIKON_CONFIG_PATH` and `RUST_LOG`. No profile auto-detection.
- `default.toml`: `127.0.0.1:6342` endpoint "default" (policy
  `localhost`, DBs `default`/`default`); extra listeners `test` **6343**
  (policy `test_endpoint`, locked to index/user DB `stdtest`; create with
  `POST http://127.0.0.1:6343/api/db/create`) and `legacy_ui` 6339;
  `[inference_local] enabled = true`; `[inference_local.vram]` empty
  header with commented examples.
- Paths (CWD/`--root` relative): `data/` (`data_folder`), `data/tmp`,
  index DBs `data/index/<name>/{index.db, storage.db, config.toml}`,
  user DBs `data/user_data/<name>/`, log `data/panoptikon.log`, local
  calibration store `data/inferio/calibration.toml`,
  `data/transcode-cache`; `runtime/{setup.lock, uv/}`.
- Inferio defaults: python auto-detected `python/.venv/bin/python`;
  `impl_dirs = ["python/inferio/impl", "inferio_custom"]`; `config_dirs
  = ["python/inferio/config", "config/inference"]`; `default_max_batch =
  32`; `handshake_secs = 30`, `load_secs = 600`; prewarm enabled + lazy.

### 3.2 Docker

- `Dockerfile` (3 stages): node builds the UI standalone; ubuntu 24.04
  `cargo build --release --features bundled,bundled-ui`; runtime ubuntu
  24.04 + ffmpeg, OpenCV libs, node 24, chrome, `uv:0.11.28`, user
  `ubuntu` (uid 1000), `WORKDIR /app`,
  `PANOPTIKON_CONFIG_PATH=/app/config/server/docker.toml`,
  `NVIDIA_DRIVER_CAPABILITIES=compute,utility,video`, `ARG
  ACCELERATOR=cpu` → `panoptikon setup --accelerator …` (venv at
  `/app/runtime/venv`), `EXPOSE 6342 6339`, healthcheck `curl
  http://127.0.0.1:6342/api/client-config`, `ENTRYPOINT ["panoptikon"]`.
  **Variants cpu and cuda only.**
- `docker.toml`: `host = 0.0.0.0`, 6342 "default" → policy `private`;
  6339 "public" → `restricted_demo`; `[logging] file = ""` (stdout);
  `[upstreams.ui] dir = "no-checkout-use-embedded-bundle"`; `[jobs]
  ffmpeg = /usr/bin/ffmpeg`, `pdfium = /app/libpdfium.so`.
- Compose: root `docker-compose.yml` (dev: builds `panoptikon:dev`,
  `ACCELERATOR: ${ACCELERATOR:-cuda}`, nvidia device reservation, ports
  `127.0.0.1:6342`, `6339`, `LOGLEVEL=DEBUG`); `deploy/docker-compose.yml`
  (`ghcr.io/reasv/panoptikon:latest`, CPU) and
  `deploy/docker-compose.cuda.yml` (`:latest-cuda`). Volumes
  `panoptikon-data:/app/data`, `panoptikon-config:/app/config` (seeded
  once), `panoptikon-cache:/home/ubuntu/.cache`. Media bind-mounted
  read-only. **No `pid: host` anywhere.**
- Build: `docker build --build-arg ACCELERATOR=cuda -t panoptikon:cuda .`
  (`.dockerignore` excludes `target`, `python/.venv`, `data`, `runtime`,
  `ui/.next`, `ui/node_modules`, `.env`). Run: `docker run -d --gpus all
  -p 127.0.0.1:6342:6342 -v $PWD/results/x/data:/app/data -v
  /path/media:/media:ro panoptikon:cuda` (data dir writable by uid 1000).
  No panoptikon images or volumes exist locally yet.

### 3.3 API (no auth; DB via `?index_db=` / `?user_data_db=`; docs at `/docs`)

```
B=http://127.0.0.1:6342
curl $B/api/db
curl -X POST "$B/api/db/create?new_index_db=cal&new_user_data_db=cal"
curl "$B/api/jobs/config?index_db=cal" > c.json       # SystemConfig (included_folders, job_settings[], cron_jobs[], …)
jq '.included_folders += ["/abs/corpus"]' c.json > c2.json
curl -X PUT -H 'Content-Type: application/json' --data @c2.json "$B/api/jobs/config?index_db=cal"
curl -X POST "$B/api/jobs/folders/rescan?index_db=cal"                 # 202 JobModel
curl -X POST "$B/api/jobs/data/extraction?index_db=cal&inference_ids=tags/wd-vit-tagger-v3[&batch_size=8]"
curl $B/api/jobs/queue                                                 # {"queue":[…], "outcomes":[…]}
curl "$B/api/jobs/data/history?index_db=cal&page=1&page_size=50"       # LogRecord{batch_size(0=auto), total_segments, errors, data_load_time, inference_time, failed, completed, status}
curl "$B/api/jobs/data/failures?index_db=cal"
curl -X POST "$B/api/jobs/cancel"; curl -X DELETE "$B/api/jobs/queue?queue_ids=3"
curl $B/api/inference/metadata                                         # registry + calibration overlay
curl $B/api/inference/health                                           # vram[], models[], gpus[]
curl -X PUT "$B/api/inference/load/tags/wd-vit-tagger-v3?cache_key=t&lru_size=1&ttl_seconds=60"
curl -X DELETE $B/api/inference/cache/t
curl -F 'data={"inputs":[{}]}' -F 'files=@img.jpg;filename=0' -X POST $B/api/inference/predict/tags/wd-vit-tagger-v3
curl $B/api/client-config                                              # readiness
curl -X POST $B/api/search/pql -H 'Content-Type: application/json' -d '{"query":{},"page_size":10}'
```
No per-job log endpoint: job progress is in the gateway log and in
`LogRecord` afterwards. The job queue runs one job at a time.

### 3.4 Test data and infra

- No usable media corpus in the repo (only `static/screenshot_*.jpg`,
  logos, a two-frame webp, npy fixtures); CI uses a `ci-fixture` release
  asset. Synthesize with Pillow (images at chosen sizes, incl. 2048×1152
  where slicing kicks in, 4096², 8000×6000), ffmpeg (`-f lavfi -i
  sine=frequency=440:duration=30`, `testsrc`), text files, and PIL
  multi-page PDFs.
- cargo: ~1 470 in-tree tests; `cargo test -p panoptikon --release`;
  inferio tests spawn the repo venv Python (override
  `PANOPTIKON_TEST_PYTHON`) with the CPU fixture impls. Ignored
  harnesses: `db/vq_int8_verify_harness.rs` (`PANOPTIKON_INT8_DB=<copy>
  cargo test --release -p panoptikon vq_int8_verify -- --ignored`),
  `pql/explain_plan.rs`, `pql/quant_ab.rs`, `pql/fts_probe.rs`,
  `media_tools/outro_equivalence.rs`.
- CI: `release.yml` on `v*` tags only (3 OS builds + smoke: boot with
  `PANOPTIKON_AUTO_SETUP=false`, `/api/client-config`, UI, then
  `db/create`, folders, rescan, poll queue, PQL, thumbnail, file serve;
  docker cpu+cuda images, 403 on 6339). `nix.yml` manual. **No GPU
  runner, no cargo test/pytest in CI.**
- Model cache: nothing sets `HF_HOME`; HF hub `~/.cache/huggingface/hub`
  (Docker: `/home/ubuntu/.cache` volume), torch hub
  `~/.cache/torch/hub/checkpoints`, EasyOCR `~/.EasyOCR`, MIOpen
  `~/.cache/panoptikon/miopen`. Redirect with `HF_HOME` in the gateway
  env for clean-cache tests.

### 3.5 Logging

`panoptikon/src/logging.rs`: `RUST_LOG` (full directive syntax) wins;
else `[logging].level` (`"${LOGLEVEL:-INFO}"`); console + append-mode
file `[logging].file` (default `data/panoptikon.log`, `""` disables;
docker.toml disables → `docker logs`). No JSON format. Setup/npm/next
child output re-logged at INFO with `setup=… stream=…`. Worker stderr
forwarded line by line at INFO under `panoptikon::inferio::worker`;
Python `logging.basicConfig(level=INFERIO_WORKER_LOG_LEVEL)`, loggers
`inferio_worker`, `inferio_worker.memory`, `inferio_worker.packing`.
