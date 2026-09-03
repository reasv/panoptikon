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
| `vramrec.py` | Out-of-process NVML recorder: per-board and per-PID VRAM at 250 ms, `/proc/meminfo`, per-PID RSS/VmHWM, worker attribution via `/proc/<pid>/environ`. JSONL. |
| `hog.py` | External-pressure generator (GPU via torch, RAM via numpy) with schedules and an HTTP control endpoint. |
| `corpus.py` | Deterministic media corpus with a per-item unit-cost manifest. |
| `healthrec.py` | Polls `/api/inference/health` and `/api/jobs/queue` at 500 ms. JSONL. |
| `loadgen.py` | Concurrent `POST /api/inference/predict/<id>` driver for contention scenarios. |
| `ceiling_probe.py` | Ground-truth base/slope/OOM boundary per model, outside the orchestrator. |
| `analyze.py` | Joins the recordings and prints the §6 verdict table. |
| `oracle_calibrate.py` | The §2 instrument calibration: does the oracle see a known allocation? One command, PASS/FAIL. |
| `newrun.py` | Creates `results/<run-id>/<scenario>/`, records `host.json`, seeds `runlog.md`. |
| `runlog.md` | Per-scenario report template (§7). |
| `config/` | Per-configuration server TOMLs and env files (C0–C3) plus `run-gateway.sh`. |
| `fixtures/` | CUDA-touching fixture impls, their user registry, and `install-fixtures.sh`. |
| `compose/` | Copies of any docker compose files used for pressure; never the user's own files. Created on demand (Phase 3). |

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
branch, the `ui` gitlink, `nvidia-smi`'s driver/CUDA/board inventory, the
interpreter and its torch, `/proc/meminfo` total, and the calibration-relevant
environment variables.

### `vramrec.py` — the independent oracle

```
vramrec.py --out DIR/vramrec.jsonl [--interval 0.25] [--duration S]
           [--filter 'inferio|panoptikon'] [--gpu N ...] [--env-key VAR ...]
           [--no-env] [--flush-every N] [--quiet]
```

Per sample: every board's `total/used/free`, every NVML compute/graphics
process on it (`pid`, `used_mb`, cmdline, `comm`, RSS, VmHWM and the
`CUDA_VISIBLE_DEVICES` / `PANOPTIKON_DEVICE_PIN` / `INFERIO_*` /
`PANOPTIKON_*` variables from `/proc/<pid>/environ`), `/proc/meminfo`
(`MemAvailable`, `MemFree`, `Cached`, swap), and the RSS/VmHWM of every process
whose cmdline matches `--filter`. Runs until SIGINT/SIGTERM or `--duration`.
A per-process `used_mb` of `null` means NVML answered N/A (WDDM, or a container
without `--pid=host`) — it is never silently turned into 0.

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

Flattens `vram[]` into `boards` and `workers` (`external_mb`, `limit_mb`,
`headroom_mb`, `grants_mb`, `unit_budget`, `ramp_step`, `deflation`,
`clean_windows`, `max_units_measured`, `knee_units`, `local_samples`,
`effective_margin`, `fit_*`) and `models[]` into a compact per-replica view.
`--full` also keeps the untouched payload under `health.raw`. A refused
connection is a sample with `ok: false`, never a crash.

Size: about **3.3 kB per sample** with one board and one resident (6.9 kB with
`--full`), so the 12 h S9 soak at 2 Hz produces roughly **280 MB** — fine, but
do not add `--full` to a soak. `vramrec.py` at 4 Hz costs about 1.8 kB per
sample per board (~1.7 MB per 250 samples measured), i.e. ~1.5 GB over a 12 h
soak: for S9, raise `--interval` to 1 s unless a sub-second event matters.

### `loadgen.py` — concurrency the job queue cannot produce

```
loadgen.py [--base URL] --out FILE [--corpus manifest.json]
           --model 'id=<inference_id>[,concurrency=N][,items=N][,corpus=PATH]
                    [,group=G][,kind=K][,mode=auto|file|text][,requests=N]
                    [,max_batch=N][,cache_key=S][,lru_size=N][,ttl_seconds=N]
                    [,order=sequential|random][,data={"threshold":0.1}]'
           [--model ...] [--duration S] [--requests N] [--timeout S]
           [--seed N] [--warmup-load] [--quiet]
```

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
with the inference id's), pins the board with
`CUDA_VISIBLE_DEVICES=GPU-<uuid>` before torch is imported, loads the impl
through `inferio_worker.discovery.find_impl_class`, and measures each batch
with `torch.cuda.max_memory_reserved` / `max_memory_allocated` plus NVML's
per-process figure. Units are priced with the worker's own
`packing.price_inputs`/`batch_units`, and the slope is fitted with the same
Theil–Sen estimator as `ledger.rs: robust_fit`, so probe and ledger numbers are
directly comparable. `--dry-run` resolves and prints the plan without touching
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

- **GPU**: board `used` delta and the hog PID's NVML per-process delta, both
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
           [--checks all|a,b,c] [--list-checks]
           [--expect-ooms N] [--expect-deaths N] [--expect-failures N]
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
`persistence`, `job_outcome`, `ledger_invariant`, `hog_tracking`,
`ramp_progress`. A scenario declares which ones apply by passing `--checks`.
Verdicts are `PASS` / `FAIL` / `WARN` / `INFO` (report-only) / `SKIP` (inputs
absent); the exit code is 1 if anything FAILed. Every row prints the numbers
behind it so a near-miss can be adjudicated by a human.

`analyze.py` reconstructs the ledger's behaviour primarily from the structured
log lines added in commit `49822c8b` — it needs
`RUST_LOG=info,panoptikon::inferio=trace,panoptikon::db::batch_auto=debug` and
`INFERIO_WORKER_LOG_LEVEL=DEBUG` in the gateway's environment.

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
$V $T/analyze.py --scenario "$DIR" --probe "$DIR/probe-wd.json" \
    --json "$DIR/verdicts.json" --plot "$DIR/timeline.png"
```

## Phase 0 state

`results/phase0/` holds the instrument-calibration evidence:
`oracle-calibration.md` (the numbers, and the recipe for the 10 GB / 40 GB GPU
calibrations that are deferred until SGLang is stopped),
`oracle-gpu/`, `oracle-gpu-driver/`, `oracle-ram/`, `oracle-ram-firstpass/`
(the raw recordings) and `probe-minilm-smoke.json` (a `ceiling_probe.py` run at
batch 1–2).
`results/corpus/{smoke,ramp}/` are generated; `soak` is not.
