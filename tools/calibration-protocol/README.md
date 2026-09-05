# Calibration test protocol tooling

The plan this directory serves is `docs/batch-calibration-test-protocol.md`.
`codemap.md` beside this file is the code-level reference (file:line) the
executing agents work from: how the host and the worker measure memory, the
wire fields, the log lines, the API calls, the env vars, the fixture impls, and
the suspected weak points.

Everything here is Python 3.12 on the managed venv (`python/.venv`), stdlib
plus `nvidia-ml-py` (`pynvml`), `torch`, `numpy` and `Pillow` — all already in
`python/uv.lock`. `psutil` and `matplotlib` are used **if importable** and
never required: `/proc` is read directly on Linux, and `analyze.py --plot`
prints a skip note instead of failing. Run every tool with the managed
interpreter:

```bash
V=python/.venv/bin/python
$V tools/calibration-protocol/<tool>.py --help
```

Every tool has a module docstring stating its usage and its exact output
schema; this table is the index, not the reference.

| File | Purpose |
|---|---|
| `vramrec.py` | Out-of-process NVML recorder: per-GPU and per-PID VRAM at 250 ms, `/proc/meminfo`, per-PID RSS/VmHWM, worker attribution via `/proc/<pid>/environ`. JSONL. |
| `hog.py` | External-pressure generator (GPU via torch, RAM via numpy) with schedules and an HTTP control endpoint. |
| `corpus.py` | Deterministic media corpus with a per-item unit-cost manifest. |
| `healthrec.py` | Polls `/api/inference/health` and `/api/jobs/queue` at 500 ms. JSONL. |
| `loadgen.py` | Concurrent `POST /api/inference/predict/<id>` driver for contention scenarios; `--prewarm-only` loads and holds idle instead (the S2-base plateau leg). |
| `ceiling_probe.py` | Ground-truth base/slope/OOM boundary per model, outside the orchestrator. |
| `analyze.py` | Joins the recordings and prints the §6 verdict table. |
| `oracle_calibrate.py` | The §2 instrument calibration: does the oracle see a known allocation? One command, PASS/FAIL. |
| `newrun.py` | Creates `results/<run-id>/<scenario>/`, records `host.json`, seeds `runlog.md`. |
| `runlog.md` | Per-scenario report template (§7). |
| `config/` | Per-configuration server TOMLs and env files (C0–C3, C7) plus `run-gateway.sh`, the C7 registry (`registry-C7/`) and the S13 `nvidia-smi` shims (`nvidia-smi-shims/`). |
| `fixtures/` | CUDA-touching fixture impls, their user registry, and `install-fixtures.sh`. |
| `compose/` | Copies of any docker compose files used for pressure, and the C4/C5/C6 compose files plus the Phase 6 overlays (raised `nofile`, master image); never the user's own files. |

Results go under `results/<run-id>/<scenario>/` (git-ignored), corpora under
`results/corpus/<tier>/`.

## The CLIs

### `newrun.py` — results layout and host facts

```
newrun.py --scenario S2 [--run-id 20260903-c1] [--config C1] [--note "..."]
          [--results DIR] [--repo DIR] [--force] [--json]
newrun.py --run-id R --host-only      # write/refresh host.json only
newrun.py --latest                    # print the newest run id
```

Prints the absolute scenario directory. `host.json` records the git commit and
branch, the `ui` gitlink, `nvidia-smi`'s driver/CUDA/GPU inventory, the
interpreter and its torch, `/proc/meminfo` total, and the calibration-relevant
environment variables.

### `vramrec.py` — the independent oracle

```
vramrec.py --out DIR/vramrec.jsonl [--interval 0.25] [--duration S]
           [--filter 'inferio|panoptikon'] [--gpu N ...] [--env-key VAR ...]
           [--no-env] [--flush-every N] [--quiet]
```

Per sample: every GPU's `total/used/free`, every NVML compute/graphics
process on it (`pid`, `used_mb`, cmdline, `comm`, RSS, VmHWM and the
`CUDA_VISIBLE_DEVICES` / `PANOPTIKON_DEVICE_PIN` / `INFERIO_*` /
`PANOPTIKON_*` variables from `/proc/<pid>/environ`), `/proc/meminfo`
(`MemAvailable`, `MemFree`, `Cached`, swap), and the RSS/VmHWM of every process
whose cmdline matches `--filter`. Runs until SIGINT/SIGTERM or `--duration`.
A per-process `used_mb` of `null` means NVML answered N/A (WDDM, or a container
without `--pid=host`) — it is never silently turned into 0.

**Why a PID's identity is re-read rather than memoised on sight.** NVML lists a
PID as soon as it touches the driver, and a worker touches it *inside* its
fork/exec window, when `/proc/<pid>/cmdline` still reads empty and
`/proc/<pid>/environ` is not yet the child's. Reading then yields the `[comm]`
fallback (`[panoptikon-spaw]`) and an empty env, and memoising that negative
pins it for the process's whole life: run1's S9 lost a nemotron worker that
way — 815 of 815 samples carried `"cmdline": "[panoptikon-spaw]", "env": {}`,
so `analyze.py` never recognised it as ours, counted it as external, and
compared its `base_mb` against a different worker's process (a 346.7 %
`base_accuracy` FAIL that was purely a recorder artefact). `ProcCache`
therefore memoises only a *complete* identity — a real argv, plus a readable
environ when env capture is on — and re-reads anything less on the next
sample, which costs three small `/proc` reads per unresolved PID per sample
and resolves within one sample of the exec. A PID that is *permanently*
unidentifiable (a kernel thread, another user's process) settles into the
cache after `MAX_ATTEMPTS` reads **and** `MIN_RETRY_S` of wall clock. Both
bounds are needed: an attempt count alone measures the retry window in
samples, so 64 attempts is 16 s at the default 4 Hz but only 3.2 s at 20 Hz,
and raising the cadence would silently reintroduce the fault.

### `hog.py` — the external world

```
hog.py [--target gpu|ram] [--device N] [--chunk-mb 128] [--tick 0.5]
       [--reeval 2] [--progress-every 2] [--duration S] [--port N]
       [--out FILE] [--hold-at-end] [--quiet] <schedule>

  hold MB
  step MB,SECONDS [MB,SECONDS ...]
  ramp FROM_MB TO_MB SECONDS
  spike MB --every S --for S [--base-mb N]
  oscillate LO_MB HI_MB --period S
  leave-free MB
  idle
```

Control endpoint on `127.0.0.1:<port>`: `GET /state`, `POST /set?mb=N`,
`POST /set?leave_free=N`, `POST /resume`, `POST /stop`. Every allocation is
touched; every shrink calls `torch.cuda.empty_cache()` so the driver sees the
release. An allocation failure increments `oom`, records `last_error`, holds
what it got and keeps serving.

While a large allocation is still in flight the hog emits `kind: "progress"`
records every `--progress-every` seconds, so a slow ramp-up is still recorded
(on this host, touching fresh anonymous pages runs at 8-30 MiB/s, so an 8 GiB
RAM hog spends minutes inside one tick). `kind: "state"` is one record per
tick; `kind: "final"` is written after everything is released.

### `corpus.py` — deterministic inputs with known units

```
corpus.py --tier {smoke,ramp,text,pixmix,ocr,audio,pdf,soak,poison}
          --out DIR [--seed 20260903] [--scale 1.0] [--jobs N] [--force]
          [--manifest PATH] [--ffmpeg PATH] [--poison-side N] [--dry-run]
corpus.py --list-tiers
```

`manifest.json` gives every item a `path`, `kind`, `format`, `bytes`, and
`width/height/pixels`, `seconds` or `pages`, plus a `units` object with what
each of the four cost dimensions would charge it (`item`, `pixel`, `token` =
`bytes/4`, `audio-second` = the flat 30 the harness charges). Regenerating a
tier with the same seed reproduces the files byte for byte (verified: 205/205
identical sha256 for `smoke`).

Generated in Phase 0: `smoke` (205 items, 19.3 MiB, 5.6 s) and `ramp` (2 000
uniform 1024x1024 JPEGs, 92.3 MiB, 53 s). `soak` is not generated (`--dry-run`
estimates 12 000 items / 2.1 GiB).

### `healthrec.py` — the gateway's own view

```
healthrec.py [--base http://127.0.0.1:6342] --out DIR/healthrec.jsonl
             [--interval 0.5] [--duration S] [--timeout 4] [--no-queue]
             [--full] [--quiet]
```

Flattens `vram[]` into `vram` and `workers` (`external_mb`, `limit_mb`,
`headroom_mb`, `grants_mb`, `unit_budget`, `ramp_step`, `deflation`,
`clean_windows`, `max_units_measured`, `knee_units`, `local_samples`,
`effective_margin`, `fit_*`) and `models[]` into a compact per-replica view.
`--full` also keeps the untouched payload under `health.raw`. A refused
connection is a sample with `ok: false`, never a crash. That per-GPU list was
called `boards` before the 2026-09-05 vocabulary rename (`vram`, not `gpus`,
because `gpus` is already the GPU inventory in the same sample);
`analyze.py::health_gpus` reads either name, so `results/run1` and
`results/run2` stay analysable.

Size: about **3.3 kB per sample** with one GPU and one resident (6.9 kB with
`--full`), so the 12 h S9 soak at 2 Hz produces roughly **280 MB** — fine, but
do not add `--full` to a soak. `vramrec.py` at 4 Hz costs about 1.8 kB per
sample per GPU (~1.7 MB per 250 samples measured), i.e. ~1.5 GB over a 12 h
soak: for S9, raise `--interval` to 1 s unless a sub-second event matters.

### `loadgen.py` — concurrency the job queue cannot produce

```
loadgen.py [--base URL] --out FILE [--corpus manifest.json]
           --model 'id=<inference_id>[,concurrency=N][,items=N][,corpus=PATH]
                    [,group=G][,kind=K][,mode=auto|file|text][,requests=N]
                    [,max_batch=N][,cache_key=S][,lru_size=N][,ttl_seconds=N]
                    [,order=sequential|random][,interval=S]
                    [,data={"threshold":0.1}]'
           [--model ...] [--duration S] [--requests N] [--timeout S]
           [--seed N] [--warmup-load] [--quiet]
```

`interval=S` paces the *starts* of one slot's requests S seconds apart (a
model's rate is `concurrency / interval`), which is what a soak's low-rate
background load needs; without it a slot runs flat out.

Records per request: latency, status, item count, the corpus item ids, the
summed units in every dimension, the output count and any
`desired_in_flight_items` the response carries (body or `x-…` header; absence
is never an error). Ends with a per-model summary (p50/p90/p99, items/s).

**`cache_key`, `lru_size` and `ttl_seconds` are required query parameters** on
`POST /api/inference/predict/{group}/{id}` (`PredictParams` in
`panoptikon/src/inferio/http.rs` has no serde defaults) — `loadgen.py` always
sends them.

### `ceiling_probe.py` — ground truth

```
ceiling_probe.py --model <inference_id> [--corpus manifest.json]
                 [--group G] [--kind K] [--mode auto|file|text] [--data JSON]
                 [--device N] [--batches 1,2,4,...] [--max-batch 64]
                 [--repeats N] [--warmup N] [--bisect-oom] [--bisect-max N]
                 [--repo DIR] [--impl-dir DIR] [--registry FILE]
                 [--out FILE] [--keep-loaded] [--dry-run]
```

Resolves the registry entry exactly as the gateway does (group `config` merged
with the inference id's), pins the GPU with
`CUDA_VISIBLE_DEVICES=GPU-<uuid>` before torch is imported, loads the impl
through `inferio_worker.discovery.find_impl_class`, and measures each batch
with `torch.cuda.max_memory_reserved` / `max_memory_allocated` plus NVML's
per-process figure. Units are priced with the worker's own
`packing.price_inputs`/`batch_units` — including run2's per-item pixel canvas,
resolved from the registry declaration first and the loaded impl's own
attribute second, as a worker under a grant resolves it, and reported as
`cost.canvas_pixels_in_force` — and the slope is fitted with the same
Theil–Sen estimator as `ledger.rs: robust_fit`, so probe and ledger numbers are
directly comparable. A batch counts as an OOM by the worker's own
`packing.classify_oom` — the same three tiers the ledger acts on, imported
rather than copied — and each row carries the `oom_class` that decided it.
`--dry-run` resolves and prints the plan without touching
a GPU. `--bisect-oom` pairs with `hog.py leave-free N` to find the true OOM
boundary at N MiB free.

It also resolves the protocol's own fault-injection fixtures, whether or not
`fixtures/install-fixtures.sh` has been run:

```bash
ceiling_probe.py --model calibfixture/oom_second_batch_cuda \
    --registry tools/calibration-protocol/fixtures/registry/calibration-fixtures.toml \
    --impl-dir tools/calibration-protocol/fixtures/impls --dry-run
```

(After installation the shipped scan of `config/inference/` and
`inferio_custom/` finds both on its own.)

#### The probe's output

`<out>.json` is `{"schema": "ceiling_probe/1", "model", "impl_class",
"config", "torch", "dtype", "python"}` plus these blocks:

| block | fields |
|---|---|
| `cost` | `unit`, `aggregation`, `seed_units`, `epoch`, `canvas_pixels`, `canvas_pixels_in_force` |
| `device` | `index`, `uuid`, `name`, `total_mb`, `cuda_visible_devices` |
| `load` | `seconds`, `base_nvml_mb`, `base_free_delta_mb`, `reserved_at_load_mb`, `allocated_at_load_mb`, `free_before_mb`, `free_after_mb` |
| `batches[]` | `batch`, `repeat`, `units`, `items`, `ok`, `oom`, `error`, `absorbed_halvings`, `index_limit_events`, `duration_ms`, `peak_reserved_mb`, `peak_allocated_mb`, `delta_mb`, `reserved_before_mb`, `reserved_after_mb`, `nvml_own_mb`, `gpu_free_mb`, and `oom_class` (`source`, `exception`, `device`, `free_mb_at_failure`) or `null` |
| `fit` | `slope_mb_per_unit`, `intercept_mb`, `residual_mb`, `samples` — or `null` |
| `bisect` | `free_mb_at_start`, `reserved_at_bisect_start_mb`, `largest_ok_units`, `largest_ok_items`, `first_oom_items`, `first_index_limit_items`, `low_items`, `high_items`, `stopped_early`, `trace[]` — or `null` |

`bisect.free_mb_at_start` is measured *after* the `--batches` sweep, whose
reservations the caching allocator still holds, so the memory a bisect probe
can actually use is `free_mb_at_start + reserved_at_bisect_start_mb`. Compare
a boundary against that sum, never against `free_mb_at_start` alone.

#### A shape ceiling is not an out-of-memory condition

A batch can be refused because a kernel's 32-bit element index cannot address
the tensor the batch builds — with the whole GPU free. The impl then falls
back to smaller work and the probe sees a *slow success*, so a bisect that
only looked at `oom` would report a boundary well above the true one: run2's
S4 had both easyOCR bisects reporting `largest_ok_items: 37` against a true
28, because at 29 and above CRAFT's pooling kernel overflowed its index and
easyOCR fell back to per-image processing. `ceiling_probe.py` therefore diffs
`inferio.impl.utils.total_index_limit_events()` across every call, treats such
a batch as not `ok` for both the sweep and the bisect, and records its
boundary as `first_index_limit_items` rather than `first_oom_items` — the two
are different facts about a model and the ledger acts on them differently.
The same diffing catches `total_oom_halvings()`, the OOMs an impl's own
`run_with_oom_retry` swallowed.

#### Registry and canvas resolution

Two rules the probe copies from the orchestrator rather than approximating:

* An `[group.G.inference_ids.ID]` table **replaces** an earlier definition of
  that id wholesale; only group-level `config` and `metadata` merge key by key
  (`registry.rs: load_file`). Deep-merging the id tables lets a shipped key
  survive an override that deliberately omits it —
  `config/registry-C7nc/` exists precisely to run easyOCR *without*
  `metadata.cost.canvas_pixels`, and under a deep merge the shipped `6553600`
  leaked back in and priced the uncapped control at the cap.
* `metadata.cost.canvas_pixels` is **scale-bound**: `cost.rs:
  canvas_from_tables` reads it only for a `pixel` unit and never inherits a
  group's value into an id that redeclares the unit. `[group.clip]` is
  `item`-priced while its VLM ids are not, so inheriting the CLIP tower's
  canvas there would cap a tiled VLM at 378² and under-price every item.

The canvas actually in force is resolved through the worker's own
`packing.resolve_canvas_pixels` (declaration, then the loaded impl's
attribute, then uncapped) and reported as `cost.canvas_pixels_in_force`.
Pricing raw pixels here while the ledger prices capped ones would put the two
slopes over different denominators, and two slopes in different denominations
cannot be compared at all — the shape of run1's spurious 4.33x disagreement on
nemotron, with the sides swapped.

### `oracle_calibrate.py` — the mandatory instrument calibration (§2)

```
oracle_calibrate.py [--target gpu|ram] [--device N] [--sizes 10240,40960]
                    [--hold 25] [--settle 10] [--tolerance-mb 64]
                    [--ram-tolerance-mb 512] [--chunk-mb 128]
                    [--alloc-timeout S] [--hog-port N] [--out DIR]
                    [--python PATH] [--json]
```

Starts `vramrec.py`, runs `hog.py hold <size>` for each size, and compares what
the oracle saw against what the hog says it held. Exit code 1 if any size
misses its tolerance, so it can gate a run.

- **GPU**: GPU `used` delta and the hog PID's NVML per-process delta, both
  minus the CUDA context (600–700 MiB on this driver) that `hog.py` measures
  once and reports as `context_mb`, so the comparison is payload against
  payload.
- **RAM**: the hog PID's **RSS**, and the **`MemAvailable` recovery at
  release** — the hog frees everything in microseconds, so whatever the kernel
  hands back within a second is the hog's and nothing else's. The naive
  "baseline before vs during the hold" delta is reported but not judged: a
  multi-GB RAM hog takes minutes to fill here, and everything else on the host
  moves during it (it was off by 5 GiB in the Phase-0 run).

```bash
# Phase 2, first thing after SGLang is stopped:
python/.venv/bin/python tools/calibration-protocol/oracle_calibrate.py \
    --target gpu --device 0 --sizes 10240,40960 --hold 30 --settle 10 \
  && echo "oracle trustworthy" || echo "STOP: the oracle cannot see a known allocation"
```

### `analyze.py` — the verdict table

```
analyze.py --scenario results/<run>/<scenario>
           [--checks all|a,b,c] [--list-checks] [--learning]
           [--expect-ooms N] [--expect-deaths N] [--expect-failures N]
           [--expect-failed-jobs N]
           [--baseline-jobs FILE | --baseline-items-per-s F]
           [--throughput-floor 0.9] [--utilization-floor 0.25]
           [--idle-window 60] [--join-tolerance 1.5] [--base-window 10]
           [--worker-pattern RE] [--probe FILE ...]
           [--json FILE] [--plot FILE] [--quiet]
           # or point at each file: --vramrec/--healthrec/--hog/--log/
           #                        --before/--after/--jobs
```

Checks (`--list-checks`): `oracle_agreement`, `base_accuracy`,
`footprint_agreement`, `slope_accuracy`, `grant_safety`, `failures`,
`deflation_recovery`, `idle_liveness`, `utilization`, `throughput`,
`persistence`, `job_outcome`, `ledger_invariant`, `peak_fds`,
`hog_tracking`, `ramp_progress`, `calibration_learned`. A scenario declares
which ones apply by passing `--checks`.
Verdicts are `PASS` / `FAIL` / `WARN` / `INFO` (report-only) / `SKIP` (inputs
absent); the exit code is 1 if anything FAILed. Every row prints the numbers
behind it so a near-miss can be adjudicated by a human.

**The check that decides safety is `grant_safety`, and within it the oracle
clause**, which joins every `issued a memory grant` line to `vramrec.jsonl` and
compares the grant with the GPU's *live free memory* at that instant. That
clause needs `vramrec.jsonl`, and without it `grant_safety` reports **WARN**,
never PASS, so a silently skipped safety clause is visible in the table. Its
other clause — grant ≤ the headroom it was priced against — only re-checks the
ledger's arithmetic against itself, and `ledger_invariant`'s strict form is not
a substitute either: run1's S15 mutation 2 hard-zeroed `external`, so
`limit_mb` became `total_mb` and `ledger_invariant` passed on **0 of 498**
GPU-samples while the oracle clause caught **335 of 335** grants. Record
`vramrec.jsonl` on every leg.

`analyze.py` reconstructs the ledger's behaviour primarily from the structured
log lines added in commit `49822c8b` — it needs
`RUST_LOG=info,panoptikon::inferio=trace,panoptikon::db::batch_auto=debug` and
`INFERIO_WORKER_LOG_LEVEL=DEBUG` in the gateway's environment.

Seven things about the verdicts are worth knowing before reading a table
(all seven come from run1; three of them close the holes the S15 mutation
self-test exposed, and the last one closes a hole run2 found in
`base_accuracy` itself):

- **`--expect-failures` counts failed *items*, `--expect-failed-jobs` counts
  whole jobs whose outcome is not `completed`.** A scenario like S4g, whose
  job is *supposed* to fail (a 2.5 GB model asked to load onto a GPU with
  1 GB free), needs the second knob or it reports `job_outcome FAIL` for
  succeeding at its own point.
- **`ledger_invariant` has two forms and reports both.** The strict form —
  Σ charges + load reservations ≤ `limit_mb` — cannot hold on a nearly-full
  GPU, because `limit = total − external × (1 + margin)` reaches **0** while
  a model we already loaded legitimately holds gigabytes and nothing would
  unload it (findings T6 / P5-2). Breaches in samples whose `limit_mb` is 0
  are therefore **WARN** and the detail says so; a breach against a non-zero
  limit still **FAILs**. The form that must always hold is the one the ledger
  actually enforces and `grant_safety` measures — a grant never exceeds the
  headroom it was priced against, nor the oracle's live free memory — and the
  `ledger_invariant` row now restates it inline so the two are read together.
- **`utilization` wants the bisect probe as well as the sweep**
  (`--probe probe-<m>.json --probe bisect-<m>.json`): the check prefers
  `bisect.largest_ok_units` and only falls back to the sweep's largest batch.
- **`SKIP` never means "the run produced no measurement".** It means *this
  harness did not record the input*, and it never sets the exit code. A fault
  that destroys the measurement usually destroys the evidence with it, which is
  how run1's S15 mutation 1 (the worker halving its reported
  `peak_reserved_mb`) came back with `slope_accuracy SKIP` and
  `persistence SKIP` and was caught by a single row that only existed because
  that leg happened to have been run with `--probe`. So `slope_accuracy`,
  `utilization` and `persistence` now split the two: "the store was never
  written" / "no worker was ever admitted" is **WARN** (**FAIL** under
  `--learning`), while "you gave me no probe file / no log" is **SKIP** with a
  pointer to what to pass.
- **A learning leg must declare itself, with `--learning`** (or, equivalently,
  by naming `calibration_learned` in `--checks`; `--checks all` declares
  nothing). Under that declaration `calibration_learned` FAILs on any of:
  `fit samples == 0`, no `[[profile]]` in `calibration.after.toml`, or a peak
  `unit_budget` that never rose above the first value recorded. The three
  numbers are exactly the ones `ramp_progress` prints as INFO — the check only
  promotes them to a verdict, which is what closes the whole class of "the
  instrument stopped reporting" faults. Undeclared, the row is report-only.
  Pass `--learning` on every S2/S3 cold-ramp leg.
- **`hog_tracking` is INFO with one FAIL form.** `external` is a
  window-boundary quantity with a real staleness (B2/T3), so a GPU that
  updates *late* is behaving as designed and stays INFO. The one shape that is
  not staleness: the hog held **≥ 1 GiB for more than 60 s** and `external_mb`
  **never moved by a single MiB** across the whole recording — that is no
  update at all, and it FAILs. 60 s is the practical threshold, well above the
  ledger's 10 s staleness window plus one admission window (run1's worst
  `external_sample_age_ms` was 166.9 s overall, but every such GPU still
  *moved*). Calibrated against run1: the only FAIL among the 14 legs with a
  hog is S15 mutation 2 (`external_locked` patched to return 0: `0..0 MiB`
  against a hog holding 30 720 MiB for 77 s); the nearest miss is
  `S11-C4-fixed`, a genuinely quiet GPU that held 30 s with a flat
  `external_mb 775..775` and correctly stays INFO.
- **`base_accuracy` only judges a replica it can attribute and time.** Three
  rules, all of them added after run1's S9 reported a 346.7 % FAIL that was
  entirely an instrument artefact (the recorder had memoised a `[comm]`
  cmdline and an empty env for a worker first sighted inside its fork/exec
  window, so the check compared nemotron's `base_mb` against the *MiniLM*
  worker's process). A PID is now recognised as ours by the gateway's own
  `spawned an inferio worker … pid=Some(N)` line as well as by its recorded
  cmdline/environ, so a recording already on disk re-analyses correctly. Among
  our PIDs on the GPU the row takes **the freshest sighting inside
  [spawn, admission]** — the replica is admitted the instant its own worker
  finishes loading, so the newest process is the one that just came up — and
  declines only when nothing on the GPU fits: a GPU holding several of our
  workers is resolved, not waved through and not skipped. And the reading is
  the **minimum over the samples between the load `ok` and the end of the
  replica's idle window**, which closes at its first grant or predict (from
  that instant the process holds the batch's workspace too) or at its
  departure from the GPU (from that instant it is tearing the model down,
  which reads *below* base and would win the minimum outright). Same bounds
  for `oracle_pid_min_mb`, a post-load minimum rather than a lifetime one. A
  demand-driven load starts its first batch tens of milliseconds after the load
  `ok`, so at 1–4 Hz that window is usually **empty**: the row then reports its
  numbers as **INFO** with the reason instead of FAILing on a reading that
  provably contains workspace. To get a judged row, sample faster or run a leg
  that loads without predicting — **`S2-base`** below is that leg, and
  `S6-b18-loadstall` is run1's accidental one (nemotron resident and idle for
  178.5 s over 714 samples at 3 788 MiB, `base_mb` to the megabyte, 0.0 %).

#### Checks, one by one

Each row prints the numbers behind it. Thresholds are the defaults; the flags
that move them are in `analyze.py --help`.

| check | compares | threshold | tiers |
|---|---|---|---|
| `oracle_agreement` | the ledger's `external_mb` against (GPU `used` − the NVML usage of our own worker PIDs) | ±1 GiB or 2 % | PASS/FAIL, SKIP without both recordings |
| `base_accuracy` | a replica's reported `base_mb` against the oracle's per-process reading for *its* process | ±10 % (`nvml` method only) | PASS/FAIL; INFO when the window is empty or the method is not `nvml` |
| `footprint_agreement` | per GPU, `footprints_mb` against the summed NVML usage of our PIDs | ±1 GiB or 2 % | PASS/FAIL |
| `slope_accuracy` | the persisted slope against `ceiling_probe.py`'s | −30 % .. +100 % | PASS/FAIL; WARN (FAIL under `--learning`) when no store was written; SKIP when no probe was passed |
| `grant_safety` | every grant against the headroom it was priced against **and** against the oracle's live free memory | no grant over either | PASS/FAIL; WARN without `vramrec.jsonl` |
| `failures` | OOM negatives, worker deaths and merged-window fallbacks in the log | `--expect-ooms` / `--expect-deaths` | PASS/FAIL |
| `deflation_recovery` | how long deflation takes to return to 0 | 3 clean windows per level | PASS/FAIL |
| `idle_liveness` | `grants_outstanding` in the trailing `--idle-window` | must reach 0 | PASS/FAIL |
| `utilization` | peak admitted `unit_budget` against the probe's OOM boundary (or knee) | `--utilization-floor` (0.25) | PASS/FAIL; the same result-versus-omission split as `slope_accuracy` |
| `throughput` | items/s from the job `LogRecord`s against a C0 baseline | `--throughput-floor` (0.9) | PASS/FAIL; INFO without a baseline |
| `persistence` | the store write against the anchor advance that queued it | within 30 s | PASS/FAIL; same split again |
| `job_outcome` | job outcomes and item failures | `--expect-failures` (items), `--expect-failed-jobs` (whole jobs) | PASS/FAIL |
| `ledger_invariant` | Σ charges + load reservations against `limit_mb` | see below | PASS/FAIL, WARN on a zero limit |
| `peak_fds` | peak open descriptors and sockets against the process's own limit | — | INFO; SKIP when nothing recorded them |
| `hog_tracking` | `external_mb` against what `hog.py` actually held | see below | INFO with one FAIL form |
| `ramp_progress` | `ramp_step` / `unit_budget` / `fit_samples` over time | — | INFO |
| `calibration_learned` | the same three numbers, as a verdict | see below | FAIL only under `--learning` |

`ledger_invariant` has two forms and reports both. The strict form — Σ charges
+ load reservations ≤ `limit_mb` — cannot hold on a nearly-full GPU: `limit =
total − external × (1 + margin)` charges the margin against the neighbour's
level and reaches **0** while a model we already loaded legitimately holds
gigabytes and nothing would unload it (measured at `limit_mb = 2813` with
10 GB free and `0` with 4 GB free, our own residents holding 1.2–3.8 GB;
findings T6 / P5-2). A breach in a sample whose `limit_mb` is 0 is therefore
arithmetic rather than over-commitment and reports **WARN**; a breach against
a non-zero limit **FAILs**. The form that must always hold is the one
`grant_safety` measures, restated inline on this row so the two read together.

`hog_tracking` is INFO because `external` is a window-boundary quantity with a
real staleness: `ledger.rs` refreshes it at grant time with a 10 s staleness
window and nothing polls, so the honest bound is that window plus one
admission window, which can be tens of seconds under load (run1 measured
`external_sample_age_ms` of 85.5 s with a resident and 166.9 s overall,
finding B2/T3). `HOG_STALL_SECONDS = 60` sits far above any staleness seen in
run1 and far below any hog hold a scenario sets up, so a *late* GPU still
reports INFO. The one FAIL form is no update at all: a hog holding at least
`HOG_STALL_MB` (1 GiB — beyond any allocator jitter) for longer than that
while `external_mb` never moved a single MiB across the whole recording.

`calibration_learned` promotes `ramp_progress`'s three numbers to a verdict,
and it exists because a fault that destroys the measurement destroys the
evidence the other checks read: run1's S15 mutation 1 (the worker halving its
reported `peak_reserved_mb`) put the high-water below the post-load baseline,
so the `grew` test never fired — `high_water_samples = 0` on all 96 grants, no
fit, `unit_budget` never left the seed of 8, no store written — and
`slope_accuracy` and `persistence` both SKIPped. It FAILs, rather than
reporting, only for a leg that declares itself a learning scenario, on any of:
`fit samples == 0` for some model, no `[[profile]]` in
`calibration.after.toml`, or a peak `unit_budget` no higher than the first
value recorded. The third reads the first health sample as the seed — at
healthrec's default 500 ms that is within a sample of admission, and a leg
that ramps at all leaves it far behind (run1 S2: 8 → 1024).

`peak_fds` is report-only and exists because of Phase 6's F6: with local
inference every in-flight predict is loopback HTTP inside one process and so
costs **two** sockets in one descriptor table. In the shipped container
(`nofile` soft 1024) a 2 000-item job reached 983 sockets, `accept` began
failing with EMFILE, SQLite could not open its files and 1 849 items went
unprocessed. The branch raises its own soft limit at startup and clamps the
in-flight ceiling by the descriptor budget; this row is how a platform pass
re-verifies that on its own deployment shape.

#### How a replica is tied to a process

`base_accuracy` and `footprint_agreement` only mean anything if the oracle's
per-process figures are matched to the right processes, and three mechanisms
do that.

**Which PIDs are ours.** Any of three routes: the gateway's own
`spawned an inferio worker … pid=Some(N)` line named it; the recorded cmdline
matches `--worker-pattern`; or the recorded environ carries **both**
`INFERIO_WORKER` and `PANOPTIKON_DEVICE_PIN`, the pair the orchestrator sets
on a spawned worker. `ceiling_probe.py` sets the pin alone and `hog.py`
neither, so neither is mistaken for a resident. The log route exists because
the other two believe the recording: a worker first sighted by NVML inside its
own fork/exec window used to be recorded with the spawning helper's `[comm]`
cmdline and an empty env for its whole life (run1/S9, 815 of 815 samples),
which made a resident nemotron worker holding up to 66 GiB count as
*external*. `vramrec.py` no longer memoises that negative, but the log route
is what lets a recording already on disk be re-analysed correctly.

**Which of our PIDs is *this* replica.** The pid the log states, when it
states one: since run2 the spawn line carries `inference_id=` and `pid=` as
two fields of one event, so there is nothing to infer. Otherwise the freshest
sighting inside `[spawn, admission]` — a replica is admitted the instant its
own worker finishes loading, so the process the oracle sighted last is the one
that just came up, and every older PID is a resident that was already there.
A PID first sighted *before* this replica's worker was forked cannot be that
worker (`SPAWN_CLOCK_SLACK_S` absorbs only an NTP step, never a genuinely
older process). A sighting *after* the admission is unhelpful rather than
disqualifying — the oracle samples at 1–4 Hz and a worker allocates a few
hundred ms before its load returns — so with one PID on the GPU it costs
nothing and with several the row is declined rather than guessed. Both failure
modes were measured: run1/S9's 346.7 % FAIL was an older worker being the only
PID the recording could see, and run2's S2-base loaded four models onto one
GPU 7 s apart, where MiniLM's own worker was first sighted 5 ms *after* its
admission and the freshest resident pid was MobileCLIP's (654 MiB compared
against 732 MiB: 10.66 %, pure attribution).

**Pre-run2 recordings** name only the impl class (`worker=nemotron-embed-vl`)
and the OS pid on the spawn line; the worker logs `Configured as <inference
id>` a moment later under the same `worker=` label, so the two are paired FIFO
per impl class. That pairing is fail-closed: one impl class serves many
inference ids, the `Configured as` line carries no pid, and two workers of one
class mid-configure can finish in either order — so whenever such a line
arrives with more than one spawn of its class pending, every spawn in that
queue is marked ambiguous and none takes a model. An unattributed pid costs a
cross-check; a cross-paired one would put a *wrong* spawn time on a replica.
`inference_id=<unconfigured>` is the prewarm path, which really has no model
at spawn, and is read as absent.

**The window `base_accuracy` reads** runs from the load `ok` to the replica's
first grant or predict, or to its departure from the GPU, whichever comes
first, and the reading is the *minimum* over it. Both edges matter: past the
first grant the process holds the batch's cuBLAS/cuDNN workspace as well as
its base, and past the departure it is tearing the model down, which reads
*below* base and would win the minimum outright (S6-b18-loadstall: 3 788 MiB
for 178.5 s, then 550). Departure is detected from the ledger's
`credited a departed replica's footprint` line or from the first health sample
that no longer lists the replica, whichever comes first. When no oracle sample
lands in the window, the nearest reading is still reported but the row is INFO:
it provably contains the first batch's workspace. The single-sample reading is
taken with `_at_or_after` rather than "nearest", because the per-process figure
is still *rising* at the admission — run1/S1 measured 812 MiB 60 ms before the
admission line against a reported 964, and 974 MiB 190 ms after it.

### Recording file descriptors

**No tool in this directory records them** — the gap is deliberate and
documented rather than filled, because the number is only interesting on a
containerised or descriptor-constrained run, and sampling it costs one shell
loop. `analyze.py`'s `peak_fds` row reads either `fds.jsonl`
(`{"iso": …, "fds": N, "sockets": M, "limit": L}` per line) or the plainer
`fdrec.txt` (`<iso> fds=N sockets=M [limit=L]`), whichever the scenario
directory holds, and SKIPs with a pointer to this section when neither is
there. Phase 6 recorded it like this (container case; `PID` is the gateway's
pid, 1 inside the container):

```bash
# The limit that matters is pid 1's, NOT `docker exec … ulimit -n`: an exec is
# a new process and gets the container's configured OCI rlimit (1024 here),
# while the gateway raises its OWN soft limit to the hard one at start-up
# (panoptikon/src/rlimit.rs). Reading the wrong one puts a number 512x too
# small in the `limit=` column, and analyze.py's `peak_fds` row reports the
# percentage against it. Run1 Phase 7b hit exactly this.
LIMIT=$(docker exec <container> awk '/Max open files/{print $4}' /proc/1/limits)
while :; do
  printf '%s fds=%s sockets=%s limit=%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
    "$(docker exec <container> sh -c 'ls /proc/1/fd | wc -l')" \
    "$(docker exec <container> sh -c 'ls -l /proc/1/fd | grep -c socket')" \
    "$LIMIT"
  sleep 0.5
done > "$DIR/fdrec.txt"
```

Bare-host equivalent: `ls /proc/$PID/fd | wc -l`, with the limit from
`awk '/Max open files/{print $4}' /proc/$PID/limits`.

**Container logs need no pre-processing.** `docker.toml` sets
`[logging] file = ""`, so `docker logs <container>` is the only sink, and the
gateway writes ANSI colour to it; `analyze.py` strips those escapes when it
reads the log, so a raw `docker logs > panoptikon.log` capture parses like a
file sink. (Before that was added, a container leg reported `log 0 events` and
silently SKIPped `grant_safety`, `failures` and `persistence`.) Record it on **every
containerised run** and on any bare run whose `unit_budget` passes ~100: with
local inference each in-flight predict is loopback HTTP inside one process and
costs **two** sockets in one descriptor table, which is what made Phase 6's F6
a release blocker (983 sockets against a 1024 soft limit, 1 849 items
unprocessed).

## A scenario, end to end

The base URL differs per configuration — `config/README.md` lists the ports
(C1 6342, C0 6352, C2 6362, C3 6372) and `config/run-gateway.sh` launches one.
The example below uses C1.

```bash
V=python/.venv/bin/python
T=tools/calibration-protocol
RUN=20260903-c1
DIR=$($V $T/newrun.py --scenario S2 --run-id $RUN --config C1 --note "cold ramp")

cp data/inferio/calibration.toml "$DIR/calibration.before.toml" 2>/dev/null || true
$V $T/vramrec.py   --out "$DIR/vramrec.jsonl"   &
$V $T/healthrec.py --out "$DIR/healthrec.jsonl" &
$V $T/hog.py --out "$DIR/hog.jsonl" --port 6401 --target gpu --device 0 leave-free 12288 &

# ... run the scenario ...

kill %1 %2 %3
cp data/inferio/calibration.toml "$DIR/calibration.after.toml"
cp data/panoptikon.log "$DIR/panoptikon.log"
curl -s "http://127.0.0.1:6342/api/jobs/data/history?index_db=cal&page=1&page_size=50" \
    > "$DIR/jobs.json"
# --learning: S2 is a cold ramp, so a leg that learns nothing must FAIL, not SKIP
$V $T/analyze.py --scenario "$DIR" --probe "$DIR/probe-wd.json" --learning \
    --json "$DIR/verdicts.json" --plot "$DIR/timeline.png"
```

### S2-base — the leg that gives `base_accuracy` something to judge

`base_accuracy` can only judge a replica from the samples between its load and
its **first grant or predict**; past that instant the process holds the batch's
workspace as well as its base. A demand-driven load starts its first batch tens
of milliseconds after the load `ok`, so at 1–4 Hz that window is normally empty
and every row is reported unjudged (run1: 42 of 60 legs INFO). `S2-base` is the
leg that fixes that, by loading each model under test and then doing **nothing**
for at least 60 s, which is hundreds of samples of flat plateau at oracle
cadence. No corpus, no job queue, no hog — the whole point is the absence of
work. Run it once per configuration; it takes about two minutes.

```bash
V=python/.venv/bin/python
T=tools/calibration-protocol
RUN=20260904-c1
DIR=$($V $T/newrun.py --scenario S2-base --run-id $RUN --config C1 \
        --note "prewarm and hold: base_accuracy plateau")

# The gateway must be up with the standard calibration logging
# (RUST_LOG=info,panoptikon::inferio=trace ...), and no job may be running.
$V $T/vramrec.py   --out "$DIR/vramrec.jsonl" --interval 0.25 &
$V $T/healthrec.py --out "$DIR/healthrec.jsonl" &

# `lru_size` must be at least the number of models sharing the cache key, or
# the second load evicts the first and only the last model is resident.
$V $T/loadgen.py --base http://127.0.0.1:6342 --out "$DIR/loadgen.jsonl" \
    --prewarm-only --hold 90 \
    --model 'id=tags/wd-vit-tagger-v3,lru_size=4' \
    --model 'id=clip/apple_MobileCLIP-S1,lru_size=4' \
    --model 'id=textembed/all-MiniLM-L6-v2,lru_size=4' \
    --model 'id=clip/nemotron-embed-vl-1b-v2,lru_size=4'

kill %1 %2
cp data/panoptikon.log "$DIR/panoptikon.log"
$V $T/analyze.py --scenario "$DIR" --checks base_accuracy \
    --json "$DIR/verdicts.json"
```

Expected: **`base_accuracy` PASS**, every model judged (no `[not judged: ...]`
and no `[report-only: ...]` on the verdict), each row carrying an
`oracle_window_samples` in the hundreds and an `error_pct` of a few percent or
less. Read the JSON, not just the table: a row is only evidence if
`cadence_blind` is `false`. Three things turn it back into INFO or SKIP, and
each is a mistake in the leg rather than a finding —

* **something predicted.** Anything that touches a model during the hold — a
  running job, a stray `loadgen.py`, the UI — closes its window at the first
  grant. `first_work_dt_ms` in the row names the moment.
* **a model was evicted.** `lru_size` below the model count, or a
  `ttl_seconds` shorter than the hold, ends the window early; the row's
  `oracle_window_samples` collapses and the detail says the window closed at
  "the replica's departure".
* **the load failed.** `loadgen.py` exits non-zero and the `kind: "hold"`
  record lists the model under `models_failed`.

The one thing this leg cannot do is judge a `base_method` that is not `nvml`:
a worker that fell back to a torch-allocator reading is reported and never
judged, because it is not measuring the same quantity as the oracle.

## Phase 0 state

`results/phase0/` holds the instrument-calibration evidence:
`oracle-calibration.md` (the numbers, including the 10 GB / 40 GB GPU
calibrations, which had to wait for SGLang to be stopped in Phase 2a),
`oracle-gpu/`, `oracle-gpu-driver/`, `oracle-gpu-full-dev0/`,
`oracle-gpu-full-dev1/`, `oracle-ram/`, `oracle-ram-16g/`,
`oracle-ram-firstpass/` (the raw recordings) and `probe-minilm-smoke.json`
(a `ceiling_probe.py` run at batch 1–2). **The gate is open on this host:**
GPU `used` is +2 MiB and NVML per-process −6 MiB against known 10 GB and
40 GB allocations on both GPUs, RAM +32 MiB RSS at 16 GiB.

Corpora under `results/corpus/` (all git-ignored, all regenerable from
`corpus.py` with the seed each runlog records): `smoke` (205), `ramp` (2 000),
`ramp8` (16 000 — S4b–S4e need a corpus that outlives a 10-minute profile),
`text` (2 000, for the token model via `loadgen.py`; `.txt` is not indexable,
so it cannot be driven by a job), `poison`, `poisonmix`, `pixmix`, `ocr` and
`audio`. `soak` is not generated.

## Run results

`results/run1/` (git-ignored) holds the first full execution of the protocol,
2026-09-03, one directory per scenario with its `runlog.md`; `results/run1/
README.md` indexes them with each leg's verdict headline, the binary commit it
ran on, and which calibration stores are **poisoned** and must not be used to
seed a later scenario.
