# Batch calibration: code map for the test protocol

Reference material gathered on 2026-09-02 by reading the PR branch
(`claude/batch-calibration-coverage-db9ab9`); every `file:line` and symbol
reference was re-resolved against the tree on 2026-09-04, after the run2
change set (R1-R11) landed. Line numbers drift with every commit; grep for
the quoted message text or function name when they do, and prefer the symbol
name — it is given beside almost every reference for exactly that reason. Three parts: the Rust host, the Python worker, and
build/deploy/API. The plan that uses this is
`docs/batch-calibration-test-protocol.md`.

---

## Part 1: Rust host (`panoptikon/src/inferio/`)

### 1.1 Backend resolution and inventory probe

- Backend (`http.rs:380-386`): user-managed interpreter → config; else
  the venv's setup sentinel `extra=` line (`setup.rs:581-594`); else
  `resolve_accelerator` / `decide_accelerator` (`setup.rs:537-562,
  669-697`): macOS → mps/cpu; `nvidia-smi` on PATH, System32, or
  `/proc/driver/nvidia` → cuda; `/opt/rocm` or `rocm-smi` → rocm; else
  cpu. Filesystem checks only. The one value drives `worker_env`, the pin
  env var, `gpu::probe`, and the calibration `backend` key
  (`accelerator_backend`, `http.rs:506-513`; `Auto → "cpu"`, a
  mis-keying risk on the validation-failure path).
- Probe, **once at startup, nvidia-smi subprocess, no NVML host-side**
  (`gpu.rs:399-416`, `query` `:600-640`): `nvidia-smi
  --query-gpu=index,uuid,name,memory.total,compute_cap
  --format=csv,noheader,nounits`, 5 s timeout
  (`capability::output_with_timeout`, which leaks the thread and child on
  timeout, `capability.rs:204-265`). `parse_inventory`/`parse_row`
  (`gpu.rs:1660-1727`): any bad identity column → whole inventory unknown
  (WARN); `compute_cap=[N/A]` tolerated per row.
- Ambient `CUDA_VISIBLE_DEVICES` (`gpu.rs:414`, `restrict_to_visible`
  `:897-939`): UUID form narrows; **any index-form entry blanks the whole
  inventory at INFO** → no pinning, no ledger, no calibration. Empty
  string = unrestricted. On ROCm any of `ROCR_VISIBLE_DEVICES`,
  `HIP_VISIBLE_DEVICES`, `CUDA_VISIBLE_DEVICES`, `GPU_DEVICE_ORDINAL`
  does the same (`rocm.rs:50-60, 200-231`).
- Multi-GPU: one `GpuInfo{index, uuid, name, total_mb, compute_cap}` per
  row (`gpu.rs:161-224`); ledger keyed by `uuid`. Default board =
  highest compute cap → largest `placement_total_mb` → lowest index
  (`default_board`, `gpu.rs:1227-1235`). **Every unpinned model lands on
  the same board**; other boards only via registry `devices` pins. No
  headroom-based placement.

### 1.2 Live memory readings

- Host refresh (`gpu.rs:778-790` `query_memory_nvidia_smi`): `nvidia-smi
  --query-gpu=uuid,memory.total,memory.free`, 5 s timeout, all-or-nothing
  parse (`parse_memory`, `:796-821`). Triggered **only** from `VramLedger::request_grant`
  (`ledger.rs:3923` → `maybe_refresh_external`, `:5207`) when `refresh_due`
  (`:1340`), now four conditions in order: no refresh in flight for that
  board, no failure within 10 s, **the board carries a departure stamp**,
  else freshest free sample older than `EXTERNAL_SAMPLE_MAX_AGE = 10 s`
  (`:181`). The stamp (`GpuLedger::free_adjusted_at`, `:1660`) is set by
  `forget_worker` (`:2797`) on every worker departure and cleared by the
  next reading that lands, so an unload forces the *next* grant to
  re-probe whatever the sample's age — but the in-flight and
  failure-backoff suppressions still win, so a host whose probe answers
  nothing is not re-probed on every grant. Runs in `spawn_blocking`; the
  grant uses the stale value meanwhile. **No periodic poller; `/health`
  never refreshes** (it does not need to: the departure is accounted for
  synchronously, see § external below). Design doc promised an NVML
  per-process snapshot; the code reads free/total only.
- Worker samples are the primary source: load and every predict response
  carry `memory{free_mb,total_mb,free_source,reserved_mb,allocated_mb}`, and
  since run2 (R5) **every measurement** carries its own pre-batch
  `free_mb`/`free_source` too (`worker.rs:400` `BatchMeasurement`). Both go
  through `record_free_locked` under the same rules; within one response the
  measurements apply in sequence order and the response-level sample last
  (it is taken after the final batch, and `Worker::record_telemetry` stamps
  it last for that reason), so `external_mb` refreshes at response cadence
  rather than at the 10 s staleness timer (finding T3).
  New in run2, same file: `clamped{from_units,to_units,free_mb}`
  (`worker.rs:369`) and `oom_class{source,exception,free_mb_at_failure,device}`
  (`worker.rs:381`).
  Recorded at registration (`ledger.rs:2817`) and every settle
  (`ingest_locked` `:4489`, `:4679`) via `record_free_locked` (`:3299-3386`): authoritative
  labels `nvml|nvidia-smi|amdgpu-sysfs|mps|ram`
  (`free_source_is_authoritative`, `:1604-1609`); once a
  board has seen one, `torch` readings never overwrite `free`
  (`:3351-3356`); older samples ignored; a sample whose own `total_mb`
  disagrees with the board (±5 %/512 MB) is discarded with a once-per-
  (model, board) WARN (`:3325-3340`).
- On departure (`forget_worker`, `ledger.rs:2933`) the board's free sample
  is *credited* with the departing replica's footprint, so `external` does
  not absorb it when the footprint leaves the `Σ` below; skipped (refresh
  still forced) when the sample predates that replica's load, since such a
  reading never counted the footprint. A worker sample captured before the
  departure is refused by `record_free_locked` rather than allowed to undo
  the credit.
- `external = max(0, total − free − Σ footprint(registered workers))`
  (`external_locked`, `:3392-3397`); `footprint = base + max(0, reserved −
  reserved_at_load)` (`footprint_mb`, `:895-899`). `base` is measured **by the worker**.
  Everything unregistered is "external": `none`-class models
  (faster_whisper), replicas refused a board, prewarm-parked workers, any
  worker on an unknown-inventory host.

### 1.3 Worker→board identity

- Pin at spawn (`resolve_pin`, `gpu.rs:1334-1432`):
  `CUDA_VISIBLE_DEVICES=GPU-<uuid>` (canonical inventory spelling; index
  pins translated) plus `PANOPTIKON_DEVICE_PIN=<same>` (`worker.rs:944-948`).
- Ledger identity = what the worker reports (`LoadReport.gpu_uuid`),
  resolved by `resolve_board` (`ledger.rs:2446-2516`): UUID match →
  admit; PCI-address match + total cross-check (`total_tolerance_mb`
  `:161-163`); single-board fallback (no UUID reported, claims a GPU,
  total agrees); else `NoBoard` (DEBUG) → unpriced. Pin ≠ reported board
  → `PinDiverged` WARN only. `gpu_bdf` is absent on the shipped CUDA
  torch 2.7.1.

### 1.4 Ledger state, budgets, grants

- One `StdMutex<LedgerState>` (`ledger.rs:2143`), never held across
  await or subprocess. `LedgerState` (`:1799-1840`): `gpus{uuid →
  GpuLedger}` (`:1746-1796`), `workers{id → WorkerEntry}` (`:872-981`:
  `seed_units, base_mb, reserved_at_load_mb, reserved_mb, grants{id →
  GrantCharge{mb,requests,unit_budget}}, pending_requests, ramp_step,
  deflation, clean_windows, fit_watermark, last_trim_at,
  last_grant_settled_at`), `calibration{(inference_id, board_uuid) →
  ModelCalibration}` (`:1549-1685`: sample ring 64 `FIT_RING`, transients
  32, `fit`, `fit_is_local`, `max_units_measured` (anchor), `seeded`,
  `local_samples`, throughput ring 128 `KNEE_RING`, `knee_best`,
  `knee_units`, `knee_is_local`, run2 `knee_clean_windows` +
  `knee_widened: Option<KneeWidening{bucket, from_seq}>` (`:1691`, R1e —
  was `knee_re_explore_above`), `knee_withdrawn`, `throughput_seq`,
  `persisted`; `ledger.rs:1549`),
  `remembered_bases`,
  `remembered_dtypes`, `pending_trims` (cap 32).
- Budgets: `VramBudget{margin: Option<f64>, cap_fraction: Option<f64>}`
  (`ledger.rs:532`), both `None` by default since run2 (R5) — an **unset**
  margin is a distinct state from one set to `DEFAULT_MARGIN = 0.10`, and
  `VramBudget::margin_in_force` is what resolves it. Per-board overrides
  case-insensitive; from `[inference_local.vram]` (`http.rs`,
  `vram_budgets`). CPU board only ships `cap_fraction = 0.75` (`cpu.rs:59`).
  Validation (`config.rs`, `validate_inference_vram`): a *stated* margin must
  be finite ≥ 0; cap_fraction in (0, 1], so no per-board way to switch a
  global cap off.
- Arithmetic (`reserve_locked` `ledger.rs:3561`, `limit_with_margin_locked`
  `:3439`):
  `limit = min(total×cap_fraction, total − external − reserve)`, where
  run2 (R5) `reserve = ceil(external×margin)` when the user set one
  (`reserve_rule = "user_margin"`) and `min(ceil(external×margin), 1024)`
  when they did not (`"capped_default"`, `DEFAULT_RESERVE_CAP_MB`). The
  user-margin form is the pre-run2 arithmetic to the MiB. `/health` and the
  `issued a memory grant` line both report `reserve_mb` and `reserve_rule`.
  `headroom = limit − Σcharge − Σload_reservations`;
  `charge = footprint + max(0, grants_mb − pool_growth)`.
- Effective margin (`effective_margin_locked` `:3507-3529`): configured +
  0.15 (`UNCONFIRMED_MARGIN_BONUS` `:380`) while
  `local_samples < 5` (`LOCAL_CONFIRMATION_SAMPLES` `:367`) or cost
  dimension degraded + clamp(residual/base, ≤ 0.25); increment clamped
  at 0.40 (`MAX_MARGIN_INCREMENT` `:399`).
- Share (`share_locked` `:3613-3668`): hungry = requester + same-board workers with
  `pending_requests > 0 && no grant`; appetite = `slope × min(anchor,
  knee)` post-fit else `base` (or 256); floor = `slope × seed` or
  `SEED_BATCH_FLOOR_MB = 256`; pro-rata when floors oversubscribe.
- Unit budget `admitted_units` (`:1104-1116`): `max(seed <<
  effective_ramp_step, anchor)`, `min(2×anchor)` if anchor > 0
  (`RATCHET_FACTOR`), `min(knee)`, `>> deflation`, `≥ 1`.
  `effective_ramp_step = max(ramp_step, ramp_floor_step(seed, anchor))`
  (`ramp_floor_step` `:1058-1065`, cap 32).
- Window = `admitted_units × 3` (`WINDOW_DEPTH_MULTIPLIER` `:407`).
  Dispatcher bounds (`dispatch.rs:703-724`): priced `{units:
  window_target, items: cap×3 if capped, bytes: MAX_WINDOW_BYTES}`;
  unpriced path bounds by registry `default_batch_size` else
  `default_max_batch` (32) (`dispatch.rs:37-48`).
- Grant `request_grant` (`ledger.rs:3915-4101`): post-fit `units = min(wanted,
  floor(share/slope))`, `mb = ceil(units × slope)`; pre-fit `units =
  wanted`, `mb = share`. **On a full board pre-fit grants carry `mb = 0`
  and are memory-blind** (the pre-fit arm, `:3857-3868`); post-fit affordable
  = 1 unit. `Grant{unit_budget, mb, unit, aggregation, user_cap_items,
  canvas_pixels}` encoded on the predict frame (`worker.rs:1401`,
  `encode_grant` `:2345-2374`); fit
  snapshot attached when its version changed (`encode_fit`).
- Host-side pricing of a window's content (`dispatch::request_units` →
  `estimate_input_units`, `dispatch.rs:391-399`, `:344-361`): `pixel` = `min(image
  header w × h, canvas_pixels)`, unreadable header → `min(
  PIXEL_FALLBACK_UNITS = 2 000 000, canvas)`; `token` = bytes/4;
  `audio-second` = flat 30; `item`/`none` = 1; then `count` = item count,
  `sum`/`max-times-count` = Σ. This is the number `request_grant` is asked
  for, so the run2 R7 canvas is applied here as well as in the worker's
  `price_inputs` — otherwise the window and the batches inside it are
  denominated differently (F-B), and on the three grantless `easyocr_*` ids
  it is the only cap that ever applies.
- Ramp / deflation (`ledger.rs`: `note_clean_window` `:933`,
  `note_negative_sample` `:973`): clean window **with ≥ 1 high-water sample** → `ramp_step + 1`;
  clean without measurement → no growth; while deflated, 3 clean windows
  (`CLEAN_WINDOWS_TO_RESTORE`) restore one halving; negative →
  `deflation + 1`, `clean_windows = 0`. Run2 (R4): the counter is **capped**
  at `deflation_cap(anchor, seed) = ceil(log2(max(anchor,seed))) + 1`
  (`deflation_cap` `:1039`) and additionally repays **one level per `DEFLATION_REPAY_SECS`
  (30 s = `TRIM_DEBOUNCE`) of wall time** (`repay_deflation_by_time` `:989`,
  driven from `repay_deflation_locked` `:3976` on the grant, settle and
  `/health` paths). Per-replica runtime state, so a respawn clears it.
- Settle (`settle` `:4038-4056` → `settle_locked` `:4058-4205`,
  `ingest_locked` `:4356-4791`): telemetry ring 256
  (`worker.rs:545`; gap → WARN); `oom || throughput_collapse` →
  negative, discarded; `peak_reserved > reserved_before` → high-water →
  `FitSample{units, peak − reserved_at_load}`, anchor candidate,
  `local_samples++`; warm batch with `units ≥ 0.8 × granted`
  (`FULL_BATCH_RATIO`) and `duration_ms > 0` → throughput sample — unless
  run2 (R1a/R1b) excludes it: the window was `squeezed` or memory-blind
  (`knee_admits_window` `ledger.rs:1266`), the measurement carries `clamped`, and
  every admitted sample is tagged with the window's contention count
  (`GrantCharge::peak_occupants`, maintained by `note_occupancy_locked`
  `:4006`). A `throughput_collapse` from a window that was **not** sole
  occupancy is discarded rather than counted as a negative (P5-5) — the
  *verdict* only: an `oom` on the same measurement still deflates
  (`:4110-4115`), which is the shape an impl's own absorbed halving takes.
  `WorkerDied` → unified boards only: anchor halved + deflate
  (`note_unified_death_locked` `:4449-4487`); discrete boards learn
  nothing. Then `refit_locked` (`:4967-5016`, Theil–Sen `robust_fit` `:6367-6403`: ≥ 3
  samples, distinct x, slope > 0, else the old fit is kept) and
  `refit_knee_locked`.
- Knee `fit_knee` (`:6547`, signature `(samples, floor_rate, anchor,
  widened)`): log2 buckets, median per bucket, ≥ 12 samples over ≥ 3 buckets,
  threshold 0.9 × max(ring best, historical `knee_best`), candidate = the
  **smallest** quiet bucket ≥ threshold, returned as `2^(k+1) − 1`. Run2 (R1):
  only **sole-occupancy** samples are fitted (`refit_knee_locked` `:5038`); a
  bucket with fewer than `MIN_KNEE_BUCKET_SAMPLES = 2` observations is
  dropped; and any retained bucket whose **relative MAD** (`relative_mad`
  `:6793`) exceeds `KNEE_MAX_BUCKET_DISPERSION = 0.20` refuses the whole fit,
  `knee_best` included.
  Run2 (R1e, finding F1) replaces the single frontier guard with five vetoes
  on that one candidate — there is no search for a bucket that survives them,
  a veto refuses the fit: (1) the largest bucket *observed* (before the
  two-sample retain) must itself be quiet and must not be the knee; (2) the
  knee may not be the smallest bucket observed either; (3)
  `KNEE_PLATEAU_BUCKETS = 2` quiet buckets must lie above it, none faster by
  `KNEE_RATIO`; (4) a knee below `size_bucket(max(max_units_measured, largest
  ThroughputSample::anchor in the ring))` — the max because DP-2's halving of
  the anchor unmeasures nothing — needs two observations in its own bucket
  taken once the anchor's bucket had already passed it; (5) after a widening, the smallest
  quiet bucket above `knee_widened.bucket` needs two observations with `seq ≥
  from_seq`. R1e also marks a replica's **first settled window**
  (`WorkerEntry::settled_windows`) as `ThroughputSample::warmup` and drops
  those observations entirely.
  Run2 (R1d) makes it expire: `note_knee_window_locked` (`:4343`) counts clean
  windows run **at** the knee with headroom ≥ `RATCHET_FACTOR ×
  appetite_mb_locked` (`:3725`), and at `KNEE_EXPIRY_CLEAN_WINDOWS = 12` —
  `KNEE_SEED_REVALIDATION_WINDOWS = 4` for a knee this process never measured
  (`!knee_is_local`, R1e) — widens it one bucket (`2k+1`), or withdraws it
  once it reaches `uncapped_units` (`:1228-1239`), the budget the ramp and the
  ratchet allow on their own, which is also defined where `anchor == 0` —
  logging `this model has run cleanly at its throughput knee…` at INFO.
  `knee_widened` is set by **both** arms (a withdrawal is a widening with no
  upper bound) and is a permanent sequence mark rather than a flag the ingest
  clears; a withdrawal additionally sets `ModelCalibration::knee_withdrawn`,
  which the store's merge reads as "drop the stored knee" rather than "this
  run fitted none".
  A shipped knee is adopted at seed time when no local knee exists, arrives
  with its persisted `knee_clean_windows`, and can only ratchet down within
  a run.
- Load reservation `reserve_load` (`ledger.rs:2255-2265`): `max(remembered base,
  store expected_base)` else `CONSERVATIVE_BASE_MB = 4096`; only WARNs
  when it exceeds headroom ("loading this model is expected to need more
  VRAM than the board's remaining headroom"); the load proceeds.
- Idle trim `flag_trims_locked` (`ledger.rs:3835-3876`): other replicas on the
  board with no grant, `pending_requests == 0`, last settle ≥ 5 s ago,
  `pool_growth ≥ 256 MB`, debounce 30 s → `deliver_pending_trims`
  (`manager.rs:1674-1685`) → `try_trim` (`dispatch.rs:1066-1088`, dropped
  if the replica is busy) → `Worker::trim` with **fatal `TRIM_DEADLINE =
  60 s`** (`worker.rs:129`) → `note_trimmed` (`ledger.rs:5155-5194`).
- Seeding (`register_worker` `ledger.rs:2737-2891` → `seed_calibration_locked`
  `:3046-3174`): once per (model, board) per run; fit adopted if none
  and slope > 0; knee adopted even from a baseline; anchor/ring/
  `local_samples` only from a local profile with the exact torch string.
- `Grant.squeezed` (`ledger.rs:6032`, set at `:3981-4004`): true when the
  board could afford **less** than the window target the anchor asked
  for. Two consumers, both added during run1: `flag_trims_locked` (a
  squeezed neighbour is what justifies asking an idle resident to release
  its pool) and `dispatch::in_flight_target_units` (`dispatch.rs:619`),
  which publishes the **granted** budget's window depth instead of the
  anchor-derived one and clamps the next window the dispatcher forms —
  the header alone cannot shorten a window that is already formed. Before
  `22eb33f9` a grant squeezed to 11 units was followed by a window of
  1 936 requests that ran 49 s with no grant, no high-water sample and no
  re-pricing (finding T5).

### 1.5 Persistence (`calibration.rs`)

- Local store `<data_folder>/inferio/calibration.toml`
  (`StorePaths::beside_registry` `calibration.rs:527-535`); shipped baselines
  `<registry_dir>/calibration/*.toml` (built-in
  `python/inferio/config/calibration/`, plus user
  `config/inference/calibration/`), local-authority fields stripped on
  import (`strip_local_authority` `:275-281`). None ship yet.
- `CalibrationProfile` (`calibration.rs:123`): key `inference_id, epoch, gpu
  (model name as nvidia-smi prints it), platform (std::env::consts::OS),
  backend, torch, dtype, unit, aggregation` — `dtype` sentinel spelled
  `unstated` since run2 (R11), not `unknown`; measurement `base_mb,
  base_method, dtype_method (run2 R11, additive and never matched on),
  slope_mb_per_unit, knee_units, samples, residual_mb, measured_at,
  generator`; local-only `max_units_measured, local_samples,
  knee_clean_windows (run2 R1d), sample_units[], sample_reserved_mb[]`.
  `schema = 1`. `ProfileUpdate` additionally carries `knee_withdrawn`, the
  one signal that erases a stored knee (the merge otherwise reads an absent
  knee as "nothing fitted this run").
- Write policy `pending_update_locked` (`ledger.rs:3064-3198`): needs
  torch, dtype, base_mb, `local_samples > 0`; fires on anchor advance,
  fit version change, local knee change; anchor monotone; debounce 30 s
  (`WRITE_DEBOUNCE`, `calibration.rs:104`); atomic temp+rename; merge on same
  key (`apply` `:846-941`); flushed on manager shutdown (`manager.rs:1597`).
- Trust: lookup needs torch+dtype; dtype exact, torch exact or
  `major.minor`; stale epoch / unit mismatch silently ignored; ring
  length mismatch → ring dropped; newer schema → file ignored; **invalid
  TOML → treated as empty and overwritten on next write**
  (`calibration.rs:1290-1301`);
  I/O read error → write deferred. Deletion is honoured at next lookup
  (mtime, `load_local_locked` `:656-681`) **unless** the process has pending
  in-memory updates (`:635-637`). **Reset = delete the file with the server
  stopped.** No reset or disable endpoint exists.

### 1.6 Failure paths

1. Impl `run_with_oom_retry` (`python/inferio/impl/utils.py:431-533`):
   torch OOM / `MemoryError` / "out of memory" → `clear_cache()`, chunk
   halved, unbounded halvings to 1; single-item OOM →
   `InferenceOOMError("INFERENCE_OOM_BATCH_SIZE_1: …")`. Absorbed
   halvings mark the successful batch `oom = True` → negative sample.
2. Harness `run_window` (`packing.py:1267-1438`): no in-harness retry; a
   multi-item OOM is prefixed `INFERENCE_OOM_WINDOW:`; `WindowFailure`
   carries the measurements.
3. Host classification, **rewritten in run2 (R3 host half)**. Two paths:
   - **With a measurement**, `oom_verdict` (`ledger.rs:6466`, called from
     `ingest_locked` `:4844`) reads `oom_class.source`:
     `typed_exception`/`marker` (and an unrecognised tier, and a
     measurement with no class at all — a pre-run2 worker) deflate on
     their own; `message_pattern` is **vetoed** when
     `free_mb_at_failure >=` the window's grant `mb` (`mb = 0` states no
     envelope, so it cannot veto). One WARN per window names the figures.
     The verdict also carries **why** it was believed — `trusted` (the
     tier is structural), `corroborated` (`message_pattern` whose free
     reading was *below* the envelope) or `unopposed` (`message_pattern`
     with no reading, or a memory-blind grant) — for the negative's own
     INFO line (defect C2; §1.8).
   - **Without one** (the error frame), `message_oom_tier`
     (`ledger.rs:6620`; `message_reports_oom` is the `cfg(test)` predicate
     form) mirrors the worker's classifier: the two `INFERENCE_OOM_*`
     prefixes → tier `marker`, then per **line** — `OOM_MESSAGE_PATTERNS`
     (`:6335`, ten allocator/driver spellings), the
     `defaultcpuallocator`/"allocate memory" pair, and `out of memory`
     **plus** a whole-word device token (`cuda|hip|rocm|nvml|xpu|sycl`,
     `contains_word` `:6569`) → tier `error_frame`. The bare
     `out of memory` substring is gone (Q1/B11). Applied by
     `dispatch::error_reports_oom` (`dispatch.rs:1290`) to message +
     traceback only, never the stderr tail; the tier it returns rides on
     `WindowOutcome::Responded { oom: Option<ErrorFrameOom> }`.
4. Dispatcher (`run_batch_inner`, `dispatch.rs:1167-1270`): merged window failure → WARN
   "merged batch of {n} requests failed, falling back to per-request
   prediction" (`:1220`) → **one** sequential per-request retry pass under
   the same grant, `unit_budget / 2` if OOM (`halved_for_retry` `:1460-1465`); a
   single-request window gets no retry. Worker not restarted on a
   per-request error.
5. Ledger: `Responded{oom: true}` → `deflation + 1`; nothing enters
   fit/anchor.
6. Core (`jobs/extraction.rs`): predict failure → `isolate_inputs` once
   with `max_batch = 1` (`ISOLATION_MAX_BATCH` `:2467`,
   `isolate_inputs` `:2479-2510`); still failing → item marked
   transient, job continues; job fails "Systemic" only if every
   attempted item failed for non-media reasons. `inferio_client.rs:924-931`
   retries only 429/502/503/504/connect/timeout, not a 500 from an OOM.
- **Death, seen from the job** (R2a): the predict 500 for a request that
  never reached a model now carries a machine-readable kind —
  `{"detail": {"kind": "worker_died", "message", "model", "last_error"}}`,
  built by `structured_error` (`http.rs:297-305`) on the one arm of
  `predict_failure_response`. What earns it is `classify_predict_failure`:
  **the typed `Unattempted` marker first** (`slot_error.rs:119-161`,
  downcast through the whole context chain), then — as the documented
  fallback for an error raised by code predating it — one of the five
  `UNATTEMPTED_REQUEST_MARKERS` — `failed fatally`, `exited while idle`,
  `is dead after a previous fatal error`, `dropped the request`,
  `was unloaded` — each cited to the one place that formats it, because a
  single death renders a *different* string per affected request depending
  on where it was standing. A load failure of **this model's id** keeps
  precedence over both (`LOAD_FAILURE_MARKER`, anchored so a stale line in
  a worker's stderr tail cannot forge one). The marker is attached at three
  places covering all six shapes: `Worker::fatal` and the poisoned
  `Worker::roundtrip` (`worker.rs`), `dispatch::fail_requests` (every
  queue-failing path funnels through it) and `ModelManager::predict`'s two
  arms; it *carries* the message rather than wrapping it, so every
  rendering above is still produced byte for byte. The client parses it into the
  typed `InferenceFailure` (`inferio_client.rs:121`, reached with
  `inference_failure()`), and `jobs::extraction::run_item_inference`
  (`extraction.rs:2179-2228`) re-submits that item's work **once**
  (`classify_item_failure` `:2250-2261`, one retry per item per job, counted
  in `JobCounters::requeued_items`); `run_chunked_inference` skips its
  isolation pass for it (`is_unit_agnostic_failure`), since isolating
  would re-ask a dead worker once per unit. Items that fail again are
  recorded in `data_job_failures` and the job reports **partial**. A
  `load_cooldown` 503 (R9) instead aborts the job through `JobAbort`
  (`:715-762`) with the model, retry instant and last error as the reason.
- Worker death mid-window: `roundtrip` EOF → fatal (`worker.rs:1701-1711,
  1823-1833`); dispatcher `End::Fatal` fails the queue, aborts sibling
  windows, kills all replicas; `handle_worker_death`
  (`manager.rs:1605-1623`) WARN "worker died fatally; dropping model from
  all caches". The next predict respawns — under that **model's own** load
  lock and its board's admission permit (R6), and bounded by the R9
  load-failure cooldown when the respawn itself keeps failing (was: no
  counter, backoff or cap at all).
- **Death record** `WorkerDeath` (`worker.rs:626-656`), built by
  `Worker::record_death` (`:1894-1953`) on **every** fatal path and logged as
  WARN *"an inferio worker process is gone. Cause: …"* with `worker`,
  `pid` (captured at spawn, since the reap clears `Child::id()`),
  `status`, `signal`, `core_dumped`, `killed_by_gateway` and the stderr
  tail. The child is reaped with `FATAL_REAP_GRACE = 5 s` (`:112`);
  `status = None` means it was wedged rather than gone.
  The attribution is sampled **before** the record's own SIGKILL of the
  process group, because that kill happens on every path. It is
  three-valued since `5becf29c` (`DeathAttribution`, `worker.rs:673`,
  logged as `attribution=` and spelled out in the WARN's sentence):
  `reaped_before_signal` and `dying` both mean *the process was already
  going down, so the signal came from outside*, and only `still_running`
  means the gateway's own SIGKILL — `killed_by_gateway()` is exactly that
  third case. The middle value exists because of run1's finding F12:
  `try_wait` cannot report a thread-group leader while its CUDA threads
  are still unwinding a SIGKILL (**475 ms** measured), so the boolean it
  replaced read "the gateway did it" on precisely the externally-killed
  CUDA worker it had been added to explain.
- **Idle liveness sweep**: `DispatchMsg::ReapIdle` (`dispatch.rs:280`,
  handled `:852`/`:900`, acted on `:1010`) `try_wait`s every replica in
  the free pool on the manager's tick (`manager.rs:1651`) and runs
  `Worker::reap_if_exited` (`worker.rs:1968-1993`), which produces the same
  death record a request-path failure would. It is the **only** way a
  dead `none`-cost model is ever noticed: with no request there is no
  read on the pipe, so EOF is never seen and `/health` keeps advertising
  a replica that does not exist (finding P5-6, measured at 13 minutes).
  A worker already poisoned by an earlier fatal path answers `None`, so a
  death is never reported twice. Prewarm-**parked** workers are not swept
  (open item V3).
- **Descriptor budget** (`panoptikon/src/rlimit.rs`, new): local
  inference is loopback HTTP inside one process, so an in-flight predict
  costs **two** sockets in one descriptor table.
  `raise_soft_limit_at_startup` (`main.rs:149`, logged at `:218` once
  logging exists) lifts the soft `RLIMIT_NOFILE` to the hard limit —
  containerd's default OCI spec gives a container soft 1024 / hard
  524 288, the same pair as a bare login shell — and `soft_nofile_limit`
  feeds `jobs::extraction::in_flight_unit_ceiling` a descriptor term.
  **R10' made that term a function of the transport**
  (`InFlightTransport`, `extraction.rs:130`; the ceiling `:224`):
  - `PerRequest` (the HTTP/1.1 fallback) keeps the original
    `by_fds = (soft − FD_RESERVE 256) / FDS_PER_IN_FLIGHT_ITEM 2`
    (`extraction.rs:117,170`), which **caps** the other two terms rather
    than joining the max;
  - `Multiplexed` (h2c) has no per-unit socket cost at all
    (`extraction.rs:134`, `inferio_client.rs:290`), so the window is
    whatever the byte budget and the loader slots allow. What bounds
    sockets there is the client's own gate,
    `INFERENCE_MAX_CONCURRENT_REQUESTS 256`, so the only check left is a
    WARN when `FD_RESERVE 256 + 2 × 256 = 768` does not fit the soft
    limit — the worst case, a peer that allows one stream per
    connection. Against a peer generous with streams the real cost is the
    pool, 8 descriptors. The clamp stays in both modes as defence in
    depth.
  The job picks the mode from `InferencePool::requests_are_multiplexed`
  (`jobs/inference_pool.rs:51`), read after the model load (which is what
  resolves each endpoint's transport); an endpoint nothing has reached
  answers "not multiplexed", the conservative direction.
  `NOFILE_LIMIT_UNKNOWN` makes the term unconditionally non-binding where
  there is no rlimit to read (Windows). Motivating failure: Phase 6
  finding F6.
- **Inference transport** (`inferio_client.rs`, R10'): one
  `EndpointRuntime` per base URL, shared by every `InferenceApiClient`
  for that endpoint (`endpoint_runtime` `inferio_client.rs:373-391`) — a pool that is not
  shared is not a bound. It holds an h2c-prior-knowledge client and an
  HTTP/1.1 client, both with `pool_max_idle_per_host =
  INFERENCE_POOL_CONNECTIONS 4`, plus a semaphore of
  `INFERENCE_MAX_CONCURRENT_REQUESTS 256`
  (`INFERENCE_POOL_CONNECTIONS × H2_STREAMS_PER_CONNECTION 64`, `:326`)
  taken in **both** transports (`active` `:613-626`) — HTTP/1.1 is the one
  where a request costs a whole socket, and it is reachable after a job
  has already sized its window for multiplexing. `Transport` (`:264-269`) is
  resolved once per endpoint by a `GET /cache` probe sent with prior
  knowledge (`transport` `:449-506`): *any* HTTP answer means `H2c`. A
  downgrade is only recorded on **positive evidence** — a failure that
  could be a refusal (`could_be_an_http2_refusal` `:550-552`: not connect,
  not timeout), repeated, and then the peer answering the same request
  over HTTP/1.1 (`peer_answers_http11` `:525-533`). Anything else records
  nothing and re-probes next call, because one wrong memo costs the
  endpoint its multiplexing for the life of the process. The memo is
  cleared on a connect/request error by `predict` and by `checked_send`
  (`checked_send` `:583-594`), which every other call goes through, so it cannot be stale
  upward either. `known_transport` (`:557-563`) reads it without probing, for
  the descriptor budget above. The
  server end needs nothing beyond axum's `http2` feature: `axum::serve`
  builds hyper-util's version-sniffing auto builder (`main.rs:730`, `:796`).
- **`PR_SET_PDEATHSIG` is thread-scoped**, and this was run1's blocker
  (**F11**). On Linux it fires when the **forking thread** exits, not the
  forking process, and the premise that spawns happen on tokio core
  worker threads that live for the life of the runtime stops holding the
  moment anything calls `block_in_place`: Tokio launches its runtime
  workers *through* the blocking pool, and a demoted pool thread exits
  after `KEEP_ALIVE = 10 s` idle. The load-path host probe
  (`ledger.rs`, `refresh_external_for_load`) does exactly that
  milliseconds before a worker is forked, and `refresh_due` arms it for
  the first load after every boot and after every worker departure — so
  the gateway SIGKILLed its own workers ~10 s after each such load, 8/8
  with Δ 1–3 ms from the forking thread's exit, self-sustaining, costing
  15 924 of 16 000 items on a job that still reported *completed*
  (explains P5-1 and F8 in full). **Fixed in `f9cf10fa`**: every command
  armed with `die_with_parent` is now forked from **one permanently
  alive spawner thread** — `process_tree::spawn_supervised` (blocking
  callers, e.g. the transcode runner) and `spawn_supervised_tokio` (every
  inferio worker), the latter creating the child inside the *caller's*
  `Handle::enter()` so the runtime that registers the child with the
  signal/IO drivers is the one that later `wait()`s on it. When reading
  older recordings, note which binary they ran on: everything before
  `8546cd63` has no such probe, and everything between it and `f9cf10fa`
  has the bug.
- Deadlines (`WorkerDeadlines`, `worker.rs:135-149`): handshake/configure/ping 30 s,
  load/prewarm 600 s, unload_grace 10 s, terminate_grace 5 s
  (`[inference_local] handshake_secs/load_secs/unload_grace_secs/
  terminate_grace_secs`); `TRIM_DEADLINE` 60 s fixed; **predict has no
  deadline**; a hung worker holds its grant forever. The manager's global
  `load_lock` is **gone** (R6, finding P5-3/B18): a predict to a resident
  model takes no load-path lock at all (`ensure_loaded`'s fast path,
  `manager.rs:1930-2136`), a load takes the shutdown barrier, that model's own
  lock (`load_locks`, `manager.rs:1222`) and one permit per board from the
  admission gate (`load_admission` `:1229`, `acquire_load_admission`
  `manager.rs:1866-1911`, `[inference_local] max_concurrent_loads`, default 1;
  a replica whose board key does not resolve takes a shared bucket *and*
  every board's permit, since the pin still reaches the backend).
  Lock order and the no-deadlock argument are in the `manager.rs` module
  docs.
- Core request sizing (**changed by the §8 G7 fix; the feedback signal is
  implemented**): `REQUEST_UNIT_BUDGET = 64` (`extraction.rs:70`) is now
  only the per-request chunk. The per-job in-flight total is a resizable
  `UnitBudget` (`extraction.rs:304-308`, state `:311-317`, built at `:1151`) that
  follows a figure the orchestrator publishes on every predict response:
  `observe` (`:353-384`) grows by `add_permits` and shrinks by withholding
  only *free* permits (`forget_permits`), retrying the remainder as
  outstanding permits return (`settle` `:389-392`); it is called from
  `predict_units` (`:2400-2437`, at `:2433`, `settle` on the error path
  `:2434`). Floor
  `MIN_IN_FLIGHT_UNITS = 64` (`:91`, also the starting value, and a
  deadlock bound since one chunk acquires up to 64 permits at once);
  ceiling `in_flight_unit_ceiling` (`:224-281`) =
  `wanted = max(intermediate_budget_kib / NOMINAL_UNIT_KIB,
  loader_concurrency × 64, 64)` with `NOMINAL_UNIT_KIB = 256` (`:100`),
  then `min(wanted, max(by_fds, 64))` on the HTTP/1.1 path and `wanted`
  unchanged on the multiplexed one (R10'; the descriptor term is
  described under §1.6) → **4096 units at the shipped defaults** (before
  R10' the descriptor term bound wherever the *soft* limit survived
  startup below ~8 500; over h2c it never binds).
  Master sized the semaphore by the user's batch size; the cap plays no
  part in either number now. Other core bounds unchanged:
  `loader_concurrency` default 8, intermediate byte budget default
  1024 MB (`config.rs:574-576`).
- The signal itself: `dispatch::desired_in_flight_items`
  (`dispatch.rs:565-588`) — window target units × the just-formed window's
  items-per-unit × `IN_FLIGHT_SLACK = 2` (`:493`), bounded by
  `MAX_WINDOW_BYTES` through that window's bytes-per-item; pre-fit ratio
  from `seed_units_per_item` (`:506-518`, with `TOKEN_SEED_UNITS = 512`
  `:524`). Computed per window formation at `dispatch.rs:812-823` (the
  `admission == None` arm publishes `unpriced_window_items × 2`) and
  stored in `ModelStats::desired_in_flight_items` (`:202`, written `:823`). `ModelManager::desired_in_flight_items`
  (`manager.rs:1390-1397`) reads it; the HTTP layer puts it on the predict
  response as the header `x-panoptikon-desired-in-flight-items`
  (`http.rs:87`, attached by `with_desired_in_flight` `:848-858`, documented
  in the `#[utoipa::path]` for `predict` and hence in `openapi.json`).
  The client reads it back into `PredictResponse.desired_in_flight_items`
  (`inferio_client.rs:78-86`, header constant `:92`, read at `:700`). **Absent = no change**
  from the last figure (an older server, a model with no window yet, or
  the model unloaded in the gap); the caller's initial value is its floor.

### 1.7 Control surfaces

- Server TOML: `[inference_local.vram] margin` (default 0.10),
  `cap_fraction` (default off), `[inference_local.vram.gpu."GPU-<uuid>"]
  {margin?, cap_fraction?}`; commented examples in all five profiles.
  `accelerator = "mps"` is a new variant. Worker deadline keys above.
- Registry: `metadata.cost{unit, aggregation, epoch, seed_units}`
  (`cost.rs:131-155`, `resolve` `:179-194`, `from_tables` `:196-288`; missing → `(item, count)`, seed 4,
  `degraded = true`); `devices = [...]` pins; `metadata.default_batch_size`
  is only the unpriced-window bound now.
- Per-DB config: `cron_jobs[].batch_size` and
  `job_settings[].default_batch_size` are `Option<i64>` caps, `None` =
  auto; cap chain `resolve_job_defaults` (`extraction.rs:2820-2879`):
  request > per-ID setting > group setting. `data_log.batch_size` stores
  0 for auto. `POST /api/jobs/data/extraction?batch_size=` is an optional
  cap. The Desktop wizard cannot clear a stored cap
  (`api/desktop.rs:1128-1146`).
- Env read by host: `CUDA_VISIBLE_DEVICES` (`gpu.rs:414`); ROCm
  visibility vars; ROCm path/MIOpen vars (`accelerator_env.rs:76-86`, `:157-205`).
  **No `PANOPTIKON_*`/`INFERIO_*` env var disables calibration.**
- Env set on workers (`worker.rs:914-998`): `INFERIO_WORKER=1`,
  `PYTHONIOENCODING=utf-8`, `PYTHONHOME` removed, `PYTHONPATH`,
  `CUDA_VISIBLE_DEVICES=<uuid>` or `HIP_VISIBLE_DEVICES=<index>`,
  `PANOPTIKON_DEVICE_PIN`, `PANOPTIKON_UNIFIED_GPU=<bdf>` (unified ROCm),
  ROCm: `ROCM_PATH, HIP_PATH, LD_LIBRARY_PATH, MIOPEN_FIND_MODE=FAST,
  MIOPEN_USER_DB_PATH, MIOPEN_CUSTOM_CACHE_DIR`; MPS:
  `PYTORCH_MPS_HIGH/LOW_WATERMARK_RATIO=1.0` unconditionally; CPU:
  `INFERIO_DEVICE=cpu`. `PYTORCH_CUDA_ALLOC_CONF` is never set (inherited
  from the gateway). Model `env` touching visibility vars → WARN.
- `GET /api/inference/health` (`http.rs:1099-1101`, `ModelManager::health`
  `manager.rs:1449-1537`): `gpus: [GpuInfo]`;
  `vram: [{gpu_uuid, gpu_name, total_mb, external_mb, external_known,
  external_source?, external_sample_age_ms?, limit_mb, headroom_mb,
  charges_mb, footprints_mb, load_reservations_mb, grants_mb,
  grants_outstanding, margin, cap_fraction?, workers: [{inference_id,
  footprint_mb, charge_mb, base_mb?, reserved_at_load_mb?, reserved_mb?,
  grants_outstanding, grants_mb, pending_requests, seed_units, ramp_step,
  deflation, clean_windows, unit_budget, max_units_measured, knee_units?,
  knee_is_local, throughput_samples, local_samples, effective_margin,
  fit?{slope_mb_per_unit, intercept_mb, residual_mb, samples,
  transient_samples}}]}]` (`VramLedger::health` `ledger.rs:5394-5487`); `models[]`
  has `last_grant_units` (renamed from `last_effective_cap`),
  `last_window_items`, `cost{unit, aggregation?, epoch, seed_units?,
  degraded}`, `replicas_detail[{gpu, gpu_uuid, gpu_name, gpu_bdf?,
  torch_version, dtype, base_mb, base_method, free_mb, total_mb,
  free_source, allocated_mb, reserved_mb, reserved_at_load_mb,
  memory_age_ms, measurements_recorded, recent_batches[]}]`
  (`manager.rs:614-653`), and top-level `load_cooldowns[{inference_id,
  failures, last_error, retry_at, retry_after_secs, window_secs}]` (R9,
  `manager.rs:1496-1515`) — the only view of a model whose loads are
  failing, since such a model is never in `models[]`. A predict or
  `PUT /load` during a cooldown answers **503** with `Retry-After` and
  `{"detail": {"kind": "load_cooldown", …}}` (`http.rs`
  `load_cooldown_response` `http.rs:818-843`).
- `GET /api/inference/metadata` (`get_metadata` `http.rs:1029-1053`,
  `calibration::overlay_metadata` `calibration.rs:1179-1239`): per priced
  id a `calibration` key `{status: "local"|"baseline", gpu, dtype,
  base_mb, slope_mb_per_unit, samples, local_samples, max_units_measured,
  knee_units|null}` or `{status: "uncalibrated", gpu}`; from the store,
  default board only; omitted when no inventory.

### 1.8 Log lines (targets are module paths; worker stderr is forwarded at INFO as `worker=<impl_class> "{line}"`, `worker.rs:2133`)

Added by commit `49822c8b` (ledger.rs / calibration.rs):
- DEBUG "issued a memory grant" (model, gpu, unit_budget, mb,
  `canvas_pixels` (run2 R7, the per-item pixel canvas this window was
  *priced* under, from the same `WorkerEntry` field the grant frame is
  filled from; the literal `none` when uncapped — every non-pixel model,
  and every pixel model pre-run2 or with no canvas resolved),
  share_mb, headroom_mb, external_mb, reserve_mb, reserve_rule, pre_fit,
  ramp_step, deflation, squeezed, window_requests). Before the canvas
  field a leg could read the canvas only off the grant frame or the load
  report, never out of the gateway's own log (run2 easyOCR leg).
  `ledger.rs`, `canvas_log_field`
- DEBUG "settled a granted window" (model, gpu, outcome
  clean|negative|aborted|worker_died, high_water_samples,
  throughput_samples, ramp_step, deflation, clean_windows,
  max_units_measured); **WARN** with `reason`
  oom|throughput_collapse|unified_board_death on negatives
- **INFO** "classified this window as an out-of-memory negative" (model,
  gpu, `source` typed_exception|marker|message_pattern|error_frame|
  unclassified|*a tier this host does not recognise*, `exception` (`unknown`
  when the classification named none), `trust`
  trusted|corroborated|unopposed, `free_mb_at_failure` (**-1** when the
  classification carried no live reading), `grant_mb` (the window's
  envelope; 0 is memory-blind), `oom_samples` (measurements carrying a
  trusted OOM; 0 on the error-frame path)) — **one per negative window**,
  emitted immediately before that window's own WARN. Run2 defect **C2**:
  before this, only the *vetoed* `message_pattern` path printed, so a
  deflation the ledger trusted outright could not be attributed to a tier
  from the log at all. `ledger.rs`, `OomNegative::emit`. Read by
  `analyze.py`'s `failures` check, which tallies `source/trust` per
  negative and names any negative the tally could not attribute
  (`; tiers marker/trusted=5954`); the clause and the `oom_tiers` number
  appear only when the recording carries the line, so a pre-C2 leg's
  verdicts are byte-identical
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
- worker.rs: DEBUG "spawned an inferio worker" — `worker=<impl_class>`,
  `pid=Some(N)` and, since run2, `inference_id=<group/name>`
  (`<unconfigured>` on the prewarm path, which has no model at spawn).
  `analyze.py::_worker_spawns` prefers the stated id and falls back to the
  old FIFO pairing against `Configured as …` for run1 recordings.
- worker.rs (added by `c8d64a5a`): **WARN "an inferio worker process is
  gone. Cause: {why}. {attribution}. stderr tail: …"** with `worker`,
  `pid`, `status`, `signal`, `core_dumped`, `killed_by_gateway` — one per
  death, on every fatal path including the requestless idle reap
  ("the worker process exited while idle (no request was in flight)").
- rlimit.rs / extraction.rs (added by `d5e42c78`): INFO "raised the open
  file descriptor soft limit to the hard limit"
  (`soft_nofile_before/after`) or INFO "open file descriptor limit is
  already at its maximum"; WARN "could not raise…" / "could not read…";
  WARN when the descriptor budget would put the in-flight ceiling under
  the floor of 64 (`reserve`, `fds_per_item`).
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

### 1.9b Job failure audit (`db/job_failures.rs`, SQL `20260904120000_job_item_failures.sql`, R2b)

`data_job_failures(job_id, item_id, setter_id, stage, error, requeued,
occurred_at)` — one row per item a job attempted, could not finish, and has
**no verdict** for. Deliberately *not* the retry ledger: nothing joins it, so
it suppresses nothing and the item is selected again next run
(`docs/failed-media-retry-design.md`, "The other half"). `job_id` is not a
foreign key (job rows are deleted); retention is
`prune_orphan_job_failures` (`:151`), run from `remove_incomplete_jobs` at
the start of every extraction job — which under `atomic_extraction_jobs`
(off by default) is also when an unfinished job's rows go. Written once at
the end of the job (`record_job_failures` `:112`), buffered in
`JobCounters::failures` and bounded at `MAX_RECORDED_JOB_FAILURES = 10 000`
(`extraction.rs:591`) — the counts in `data_log` stay exact. Each record
carries the `occurred_at` the job stamped when the item failed
(`note_job_failure` `:600`), not the moment of the batched write.

`data_log` gains `outcome` (`''`/`completed`/`partial`/`failed`/`cancelled`;
`''` on every pre-existing row, rendered as `running`) and
`failure_reason`. Every terminal path writes them: the normal end, the
early-return path through `finalize_unfinished_job` (`extraction.rs:891`,
which is what gives a failed job a real `end_time` — run1 finding T8), and
the cancel path through the `CancelledJobStamp` drop guard (`:812`, which
also flushes the job's buffered failure records) plus
`finalize_cancelled_job` (`db/extraction_write.rs:153`), whose statement is
guarded so either order of the two is correct. A per-item progress update
that lands after any of them is refused rather than reopening the row
(`update_data_log`'s `outcome = ''` guard). The queue's
`JobOutcomeStatus` gains `partial` (`jobs/queue.rs`).

### 1.10 Suspected weak points (ranked; plan §5 maps each to a scenario)

1. Zero-MB grants on a full board disable the worker's clamp (B1) —
   narrowed in run2: R5 caps the default reserve at 1 GiB so `mb = 0` is
   far rarer, and R5's worker half takes the live free reading even on a
   memory-blind grant, though the clamp itself still needs an envelope.
2. Index-form `CUDA_VISIBLE_DEVICES` silently disables the feature (B6).
3. Silent respawn loop with no backoff after worker death (B15) —
   **fixed in run2 (R9)**: per-model cooldown, exponential and capped, and
   a 503 with `Retry-After` while it holds.
4. `TRIM_DEADLINE = 60 s` fatal for the whole model (B17).
5. Design deviations: nvidia-smi free/total only, grant-driven refresh;
   unpriced path uses `default_max_batch`.
6. Reset while running unreliable; no reset/disable surface (B3).
7. Persisted anchor is a permanent floor across torch patch bumps (B4);
   seeded knee only ratchets down (B5).
8. Unbounded deflation with linear recovery (B8) — **fixed in run2 (R4)**:
   capped at `deflation_cap(anchor, seed)` and repaid by wall time as well
   as by clean windows.
9. `REQUEST_UNIT_BUDGET = 64` ceiling (B16) — **fixed** by the §8 G7
   feedback signal (it is only the per-request chunk now), and R10' freed
   the descriptor term over h2c.
10. The manager's global `load_lock` on every predict (B18) — **removed in
    run2 (R6)**: per-model `load_locks` plus a per-board admission gate
    (`max_concurrent_loads`), and a resident model's predict takes neither.
11. Container without `--pid=host` degrades base measurement (B9); CPU
    board is cgroup-blind (B19).
12. Stamp insert failure blocks startup; migration irreversible.
13. `/health` `last_effective_cap` → `last_grant_units` rename (B22).
14. `accelerator_backend(Auto) → "cpu"` mis-keying (B23).
15. Post-trim zero-delta fit samples; old fit kept forever (B10).
16. `message_reports_oom` substring false positives (B11) — **fixed in run2
    (R3)**: the worker classifies structurally (`packing.classify_oom`) and
    the host's bare `out of memory` substring is gone.
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
- Per batch: `begin_batch()` resets peaks and snapshots
  `reserved_before_mb`, `allocated_before_mb`, `started`;
  `measure_batch()` emits `items, reserved_before_mb,
  peak_reserved_mb, allocated_before_mb, peak_allocated_mb, duration_ms`
  (predict call only), optional `units`, `oom`, `throughput_collapse`,
  and since run2 `free_mb`/`free_source` (the clamp's pre-batch reading, R5),
  `clamped` (R5) and `oom_class` (R3);
  the harness adds `trimmed` on a window's first measurement
  (`packing.py:1139-1148`). **Caches are not emptied between batches**;
  `empty_cache()` only on an orchestrator `trim` (`__main__.py:401-433`),
  the reactive shrink (`packing.maybe_shrink` :196-274), or inside
  `run_with_oom_retry` after an OOM. Peaks reset at the end of
  `finish_load` (:2080).
- Tiers (`_free_total_mb` :1860-1951, tried by availability):

  | Backend | free/total | pool/allocated | base tiers (`_resolve_base` :2084-2198) |
  |---|---|---|---|
  | CUDA | `nvml` → `torch` | torch allocator | `nvml` own-PID → `free_delta` → `alloc_delta_measured`/`alloc_delta` |
  | ROCm | `amdgpu-sysfs` (+GTT on a verified unified board) → `torch`; NVML refused when `torch.version.hip` or `HIP_VISIBLE_DEVICES` set | torch allocator | `fdinfo` (DRM `drm-resident-vram`, floored at `reserved − 256 MiB`) → `free_delta` → `alloc_delta` |
  | MPS | `mps` = `min(recommended_max_memory, psutil available)` (:1326-1347) | `driver_allocated_memory` / `current_allocated_memory`; **no peak API**, post-batch values reported as peaks | `mps` (`driver_allocated_memory` at load end) |
  | CPU (`INFERIO_DEVICE=cpu`) | `ram` = psutil available/total (:1519-1543) | pool = OS RSS high-water (`VmHWM` Linux, `peak_wset` Windows, `ru_maxrss` else), allocated = live RSS (:1634-1690) | `rss` = load-window RSS growth |

- `base_mb` (`_finish_load` :2017-2081, `_resolve_base`): `touched_gpu`
  = allocated or reserved delta > 0 across the load window, else no base
  at all; CPU → `(alloc_floor, "rss")`; NVML own-PID > 0 → `"nvml"`;
  fdinfo → `"fdinfo"`; MPS → `"mps"`; else `free_delta = before.free −
  free_after` (same source both sides), ceiling `reserved_delta + context +
  2048 (IMPLAUSIBLE_SLACK_MB)`; `free_delta` None/≤ 0/> ceiling →
  `(alloc_floor + context, alloc_method)`; ≥ `alloc_floor` →
  `(free_delta, "free_delta")`; else the allocator tier. Since run2 (R8)
  `context` is `context_allowance_mb()`: what `_ContextProbe` measured across
  this process's first CUDA initialisation (a daemon thread polling
  `torch.cuda.is_initialized()` every 5 ms, started by `begin_load` only when
  the pre-load reading came from `nvml`/`amdgpu-sysfs`, CUDA is not yet live
  and the host is not RAM-priced; the baseline is re-read every 250 ms
  (`_CONTEXT_BASELINE_SECONDS`) while it waits, so a long load does not put an
  external process's memory in the delta; at the flip the allocator pool is
  read *before* the free memory and subtracted; accepted only within
  `CONTEXT_MIN_MB`..`CONTEXT_MAX_MB` = 64..2048), else the fixed
  `CONTEXT_ESTIMATE_MB = 500`. `base_method` is `"alloc_delta_measured"` for
  the first and `"alloc_delta"` for the second. The probe is collected by
  `finish_load`, or by `memory.abort_load` from `__main__`'s load-failure arm
  (a load that raised never reaches `finish_load`, and its watcher would
  otherwise poll for its whole 600 s deadline).
- **Docker without `--pid=host`**: NVML reports host PIDs, so
  `os.getpid()` is never listed; one INFO line ("NVML lists no process
  with pid … expected in a container started without --pid=host",
  :403-413) and base falls to `free_delta` (board-wide, contaminated by
  concurrent activity in the load window, plausibility-capped) or
  `alloc_delta + 500`. Board-level NVML free/total still works, so
  `free_source` stays `"nvml"`.
- Load response also carries `reserved_at_load_mb`, `dtype`
  (`resolved_dtype_name` :2260-2299), `canvas_pixels` (run2 R7, the
  worker's own tier-2 reading, `packing.impl_canvas_pixels` via
  `__main__.py`'s load arm — the host's only way to learn dots_ocr's
  canvas), `gpu_uuid` (`GPU-<uuid>`, suppressed on HIP), `gpu_name`,
  `gpu_bdf` (absent on torch 2.7.1), `gpu_total_mb`, `torch_version`,
  `memory`.

### 2.2 Protocol (host↔worker)

- Host → worker `predict` frame: `grant = {unit_budget, mb, unit,
  aggregation, user_cap_items|nil, canvas_pixels|nil}` (`canvas_pixels` is
  run2 R7: `canvas_from_tables` resolves `metadata.cost.canvas_pixels` into
  `CostDimension.canvas_pixels`, `manager::canvas_in_force` folds the
  worker's reported canvas in behind it, `WorkerEntry` carries it onto every
  `Grant` and `encode_grant` forwards it) (`cost.rs:278`, `:332-387`;
  `manager.rs:2487-2519`; `ledger.rs:2717`, `:3874`, `:3954-3963`,
  `:5800-5814`; `worker.rs:2345-2374`),
  `fit = {slope_mb_per_unit, intercept_mb,
  residual_mb, samples}` only when the version changed and only on the
  first chunk of a multi-frame window (`dispatch.rs:1444`). A grant is
  sent only when the replica has an admission; `none`-class models never
  get one. After a merged-window OOM, per-request retries get
  `unit_budget / 2`. New `trim` request type.
- Worker (`__main__.py:311-369`): `fit` is **ignored** (advisory in v1).
  Grant present and `packing.batching_disabled(instance)` false →
  `packing.run_window`. No grant, or `enable_batching`/`enable_batch`
  present-and-falsy (`packing.py:936-958`) → single `instance.predict`
  bracketed by `begin_batch/finish_batch`, one measurement, **no
  `units`**; logged once per worker.
- Grantless impls: `MoondreamTagger`/`MoondreamCaptioner`
  (`enable_batching = False` class attr), `EasyOCRModel` with
  `config.enable_batching = false` (all three shipped easyocr ids,
  `inference.toml:277,316,329`), `DotsOCRModel`/`Florence2` if configured
  off (default on), plus every `none`-class id (whisper, tagmatch, jina
  APIs, vlm, moondream taggers).
- Worker → host: `load` ok carries `base_mb, base_method,
  reserved_at_load_mb, dtype, gpu_uuid, gpu_name, gpu_bdf, gpu_total_mb,
  torch_version, memory` (parsed `worker.rs:2254-2267`); `predict` ok
  carries `outputs` (order restored, `packing.py:1258-1261`),
  `measurements[]` (one per GPU batch), `memory`; `predict` error carries
  `message`, `traceback`, plus `measurements`/`memory` when the exception
  is a `WindowFailure` (grantless-path failures carry none); `trim` ok
  carries a post-`empty_cache` sample.

### 2.3 OOM handling

- Classifiers, **deliberately different since run2 (R3)**:
  `utils.looks_like_oom` is unchanged and stays broad ("out of memory"
  case-insensitive, `INFERENCE_OOM`, `defaultcpuallocator` + `allocate
  memory`, over `exc`, `__cause__`, `__context__`) because it only decides
  whether `run_with_oom_retry` halves and retries. `packing.classify_oom`
  replaced `packing._looks_like_oom` and decides what the *orchestrator* is
  told: three tiers over the same chain, in strength order — typed
  (`torch.OutOfMemoryError`, looked up through `sys.modules`; `MemoryError`),
  marker (`InferenceOOMError`, `INFERENCE_OOM` text, or the halving counter
  moving), then driver-shaped text: a listed substring that never says "out
  of memory" itself (`OOM_MESSAGE_PATTERNS`), the `OOM_MESSAGE_PAIRS` pair, or
  the words "out of memory" **with a device-API token as a whole word**
  (`OOM_DEVICE_TOKENS` = cuda|hip|rocm|nvml|xpu|sycl), which covers torch's
  four spellings and CTranslate2's. A bare "out of memory" naming no device is
  excluded (B11). Returns the `oom_class` map
  `{source, exception, free_mb_at_failure, device}` or None, and None now
  means `oom` is absent. Rust `message_reports_oom` (Part 1 §1.6).
- Harness on any exception (`packing.py:953-1204`): unpriced measurement
  with `oom` flag if classified or the halving counter moved; multi-item
  OOM → `INFERENCE_OOM_WINDOW:` prefix; `WindowFailure`. A batch that
  succeeded but halved internally: `oom: true`, no `units` (:781-805).
- Survival: predict errors (OOM or not) → error frame, worker keeps
  serving. Exits only on handshake failure (1), `ProtocolError` (2),
  uncaught error outside the loop (3), stdin EOF (0), or a real kill.

### 2.4 Packing (`packing.py`)

- Pricing (`price_inputs`): `pixel` = `min(w × h, canvas)` from
  `PIL.Image.open(BytesIO(file)).size` (header only; unreadable → largest
  priced so far, else `UNREADABLE_PIXEL_UNITS = 2_000_000`, itself capped).
  `canvas` is run2 R7 (`resolve_canvas_pixels`): `grant.canvas_pixels` →
  the impl's own `canvas_pixels`/`max_pixels`/`image_max_pixels` reached
  through at most two of `processor`/`image_processor`/`embedder`/`model`
  and floored at `CANVAS_FLOOR_PIXELS = 512²` → uncapped; `token` = `max(1, utf8 bytes //
  4)` (`BYTES_PER_TOKEN = 4`); `audio-second` = flat 30; `item` = 1.
  `price_window` returns `PricedWindow(units, raw)` — the capped price and
  the same window uncapped, from **one** header read (`_pixel_readings` →
  `_pixel_units`); `raw` is the packing tiebreaker below and never a price.
- Planning (`plan_batches`): `count` = len; `sum` = greedy FIFO;
  `max-times-count` = sort descending by units then greedy; a single
  over-budget item goes alone; `cap_items` is a separate bound;
  re-planned before every batch. Run2 D1-b adds `tiebreak=` — a descending
  **secondary** key (raw pixels) applied only among equal priced units and
  only for `max-times-count`, because the R7 cap prices every item at or
  above the canvas alike and so erases the size information the bucketing
  sorts on. Never changes a price, an order across prices, or safety;
  ignored when its length does not match.
- Mixed-batch guard (run2 D1-b, `_pads_without_a_canvas`,
  `_warn_mixed_batch_once`): an impl exposing `pads_to_common_size = True`
  (`PADS_TO_COMMON_SIZE_ATTR`) says it builds one tensor at its largest
  member's dimensions. If it also states **no** canvas of its own, and a
  batch under a cap mixes raw areas by more than `MIXED_SIZE_LOG_RATIO = 2`,
  one WARN per process names the ratio. Exposing a canvas is the impl's
  promise to bound its own tensor by it, so `inferio.impl.eocr` — which
  exposes `canvas_pixels = 6 553 600` and enforces it — is exempt.
- Defensive clamp (`clamp_to_live_memory`): returns a `LiveBudget(units,
  free_mb, free_source, clamped)`. It **always** takes the one free reading
  (run2 R5, including when `grant_mb <= 0`, which is the memory-blind case);
  if `grant_mb > 0` and live free < grant_mb, `units = max(1, unit_budget ×
  free / grant_mb)`, shrink-only, and `clamped = {from_units, to_units,
  free_mb}`. All three extra fields ride every measurement of the batch.
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
| doctr/dots_ocr | dotsocr | pixel / sum | 2 000 000 | 2 | min CC 8.0, ~6 GB; no `canvas_pixels` — its cap lives in the downloaded processor, so the worker's tier-2 fallback reads it and **reports it on the load response**, which is what lets the host price it too |
| doctr/easyocr_standard_{en,en_ja,en_ch_sim} | easyocr | pixel / max-times-count | 2 000 000 | 2 | **`enable_batching = false`** → grantless, so the **host** cap is the only cap; `canvas_pixels = 6 553 600` (the CRAFT detector's 2560px canvas), which the impl now **enforces** before it pads a batch (run2 D1-b, `eocr.py::fit_to_canvas`; boxes mapped back by `scale_boxes_to_original`) |
| florence2/msft_large-* (4) | florence2 | item / count | 4 | 1 | |
| vlm/moondream-2b-25-03-* (5) | moondream_captioner | none | – | 2 | |
| textembed/all-mpnet-base-v2, all-MiniLM-L6-v2, stella_* | sentence_transformers | token / max-times-count | 4000 | 1 | no impl-side OOM retry |
| textembed/jina-embeddings-v3-api | jina-clip-api | none | – | 1 | remote |
| whisper/* (15) | faster_whisper | none | – | 1 | CT2, no torch allocator |
| clip/ViT-H-14-*, PE-Core-*, ViT-B-16-SigLIP2-384, apple_MobileCLIP-{B-LT,S2,S1} | openclip | item / count | 8 | 1 | `run_with_oom_retry` ×2 |
| clip/qwen3-vl-embedding-{8b,2b} | qwen3-vl-embedding | pixel / sum | 2 000 000 | 2 | `canvas_pixels = 1 843 200` (MAX_PIXELS = 1800 × 32²) |
| clip/nemotron-embed-vl-1b-v2 | nemotron-embed-vl | pixel / sum | 2 000 000 | 2 | ~2.5 GB; `canvas_pixels = 1 835 008` ((6 tiles + thumbnail) × 512²) |
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
  `IMPL_CLASS.name()` (`discovery.py:22-52`), stdlib-only, no
  `InferenceModel` subclassing: `nobatching_impl.py`
  (`"nobatching_test"`, `enable_batching = False`, outputs `{"batch":
  n}`); `oom_second_batch_impl.py` (`"oom_second_batch_test"`, raises
  `RuntimeError("CUDA out of memory. Tried to allocate 2.00 GiB
  (fixture)")` on the 2nd predict per instance); `subbatching_impl.py`
  (real `run_with_oom_retry`, `initial_chunk_size=1`); `oom_impl.py`
  (always `INFERENCE_OOM_BATCH_SIZE_1:`); `failbatch_impl.py` (non-OOM
  `ValueError` for batch > 1); `echo_impl.py`, `batchsize_impl.py`,
  `dying_impl.py`, `die_on_flag_impl.py`, `hang_impl.py`,
  `slow_impl.py`, `slow_load_impl.py` (`"slow_load_test"`, sleeps
  `config.load_seconds` — default 3 — inside `load()`; the R6 fixture for
  proving a load delays no other model's predicts), `device_impl.py`
  (echoes `CUDA_VISIBLE_DEVICES`).
- Registering with a real host: copy into `inferio_custom/` (default
  impl dir; `inferio_custom/README.md`) or set `[inference_local]
  impl_dirs`, then a user registry TOML in `config/inference/` (scanned
  after the built-in dir; see `config/inference/example.toml` and the
  manager's test registry at `manager.rs:2812-2903`), e.g.
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
  they ~~register only via the single-board fallback (single visible
  board) or~~ run unpriced — **measured in run1: the single-board
  fallback needs a `gpu_bdf` or a `gpu_total_mb` and a torch-free worker
  sends neither, so they are never ledger-admitted on a CUDA host at
  all**; a fixture that allocates one CUDA tensor at load registers
  normally. `oom_second_batch` (the shipped torch-free one) OOMs on
  **every** batch from the second on, not once. The extraction job's
  `output_type` must match what the fixture returns.

**The protocol's own fixtures** (`tools/calibration-protocol/fixtures/`,
installed into `inferio_custom/` + `config/inference/` by
`install-fixtures.sh`; both destinations are git-ignored so the checkout
stays clean). One registry group, `calibfixture`, `item`/`count`,
`seed_units = 8`, `input_spec.handler = "image_frames"`; every CUDA
variant allocates `load_mb` (64) of real device memory at load so it
resolves to a board and is **priced**:

| Inference id | Behaviour | Probes |
|---|---|---|
| `calibfixture/oom_second_batch_cuda` | Succeeds on batch 1, then raises the bare driver OOM text on exactly `oom_batches` (default 1) batches | the OOM backstop, the merged-window fallback, deflation → 0 |
| `calibfixture/oom_cuda` | `INFERENCE_OOM_BATCH_SIZE_1:` every time — "even one item does not fit", which no deflation can rescue | B8's climb |
| `calibfixture/oom_timed_cuda` | Batch-1 OOM for `oom_secs` (120) after load, healthy afterwards | the only way to time deflation's **recovery** on one resident (7.04 levels/s) |
| `calibfixture/failbatch_cuda` | Non-OOM `ValueError` for any merged batch; singles succeed | per-request fallback with **no** deflation |
| `calibfixture/failbatch_oomtext_cuda` | Same impl with `message = "refusing merged batch of {n}: the caption cache is out of memory slots"` | **B11** — does a substring deflate a healthy model? (it does) |
| `calibfixture/dies_on_load_cuda` | Raises inside `load()`; never becomes resident | **B15** respawn cadence on the predict path |
| `calibfixture/dying_cuda` | `os._exit(3)` inside predict | **B7** death mid-window, respawn at the anchor |
| `calibfixture/hang_trim_cuda` | Grows the pool by `pool_mb` (512) per predict so `flag_trims_locked` picks it, and ignores the trim for `hang_secs` (70) by rebinding `inferio_worker.memory.empty_cache` only when no predict is in flight | **B17** — the fatal trim (it kills the worker at ~20 s, not 60) |
| `calibfixture/{oom_second_batch,oom,failbatch,dying}_cpu` | the torch-free originals | the **unpriced path** only (see the caveat above) |

### 2.7 Env vars honoured by the worker

`INFERIO_DEVICE=cpu` (`utils.py:23-51`, `memory.py:1582-1600`: forces
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

NVML PID mismatch in containers; `free_delta` contamination; ~~fixed
500 MiB context estimate~~ (run2 R8 measures it; the constant is now the last
resort); reserved-vs-allocated quantisation and
`expandable_segments` semantics; cuDNN benchmark workspace spikes on new
shapes; ~~raw-dimension pixel pricing (20 MP charged 10× real cost for
capped VLMs)~~ (run2 R7 caps each item at the model's canvas, on both sides:
the host applies the same `min` in `dispatch::estimate_input_units`) and
`bytes/4` token pricing (CJK under-priced ~3×, long
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
curl "$B/api/jobs/data/history?index_db=cal&page=1&page_size=50"       # LogRecord{batch_size(0=auto), total_segments, errors, data_load_time, inference_time, failed, completed, status, outcome, failed_items, failure_reason}  (R2b added the last three; `outcome` is completed|partial|failed|cancelled|running)
curl "$B/api/jobs/data/failures?index_db=cal"   # R2b: {total, failures[], job_failures_total, job_failures[], failed_jobs_total, failed_jobs[]} — the ledger's verdicts, the items a job could not finish and has no verdict for, and the partial/failed/cancelled job records (real end_time, failed_items, reason). run1: stayed {"total":0,"failures":[]} in every leg, including 125 easyOCR OOMs and a whole-job failure (findings T8/Q8)
curl -X POST "$B/api/jobs/cancel"; curl -X DELETE "$B/api/jobs/queue?queue_ids=3"
curl $B/api/inference/metadata                                         # registry + calibration overlay
curl $B/api/inference/health                                           # vram[], models[], gpus[]
curl -X PUT "$B/api/inference/load/tags/wd-vit-tagger-v3?cache_key=t&lru_size=1&ttl_seconds=60"
curl -X DELETE $B/api/inference/cache/t
curl -F 'data={"inputs":[{}]}' -F 'files=@img.jpg;filename=0' -X POST "$B/api/inference/predict/tags/wd-vit-tagger-v3?cache_key=calib&lru_size=1&ttl_seconds=60"   # all three query params are required (PredictParams has no serde default)
curl $B/api/client-config                                              # readiness
curl -X POST $B/api/search/pql -H 'Content-Type: application/json' -d '{"query":{"match_tags":{"tags":["1girl"],"match_any":true}},"page_size":5}'   # `{"query":{}}` is rejected (untagged enum QueryElement); use a real clause as CI does
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
- **Protocol shims** (`tools/calibration-protocol/config/nvidia-smi-shims/`,
  for S13): `slow-all` (sleeps 6 s on every query — also hits the *boot*
  inventory probe, which times out at 5.000 s, and
  `accelerator_report`'s untimed capability query, which waits the full
  6 s and adds ~11 s to boot), `slow-memory` (slow only on
  `--query-gpu=uuid,memory.total,memory.free`, so the inventory is normal
  and only refreshes are slow — this is the one B13 needs) and
  `malformed` (a row that parses nowhere). The **hidden** case is not a
  shim: `find_nvidia_smi` (`capability.rs:148`) walks `PATH` and there is
  no config key, so it needs a scratch mirror of the directory holding
  `nvidia-smi` with that one entry missing; recipe in the shims'
  `README.md`. Never leave a shim on a `PATH` the user inherits.
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
