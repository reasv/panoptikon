# Batch calibration: agent-driven test protocol

Status: **plan approved by the user on 2026-09-02, not yet executed**
(PR #27, branch `claude/batch-calibration-coverage-db9ab9`, master merged
in). Section 0 is the handoff: every decision taken and the exact next
actions, so a fresh session can start the work from this file and
`tools/calibration-protocol/codemap.md` alone.

## 0. Handoff: decisions, state, next actions

### Decisions taken (all by the user, 2026-09-02)

1. **Do not cancel or split the PR.** Test it with this protocol on this
   host first; repeat a subset per platform (§9). It will not make the
   next release; possibly the one after.
2. **Ledger logging first** — done, commit `49822c8b` (§8 G1).
3. **The 64-item in-flight ceiling (G7) gets the principled fix, the
   feedback signal, before the run**, not the quick constant bump. It is
   implemented as its own Opus track, with its own verifier, **in
   parallel with Phase 0 tooling**. The run starts when that verifier
   signs off. Brief in §8 G7.
4. **Opus subagents do all execution** (tools, scenarios, fixes,
   reviews). The main session (Fable) orchestrates, decides and
   adjudicates only.
5. **Fix policy.** Low-discretion fixes are made, then reviewed by a
   separate verifier subagent, iterated, and reported. Anything that
   changes a feature, a default, user-visible behaviour or a design
   decision is written up with options and left to the user.
6. **Tools live in `tools/calibration-protocol/`** on the PR branch so
   they ship for the other platform passes.
7. **SGLang** is used as-is for the "board full" case, then stopped with
   `docker compose down` in `~/docker/dsv4flash` and restarted with
   `docker compose up -d` at the end. Nothing of the user's is edited;
   other resident models (`~/docker/{llamacpp,vllmdsv4,comfy,textgen}`)
   may be used for intermediate VRAM levels via copied compose files.
8. **Master worktree baseline**: `git worktree add ../panoptikon-master
   master`, its own venv and data dir.
9. **Throughput as a secondary calibration target** is post-run work
   (§8b), not part of this run.
10. **Plan and code map are committed** on the PR branch.

### Host state at handoff

- Branch checked out, `ui` submodule at `7d9e1f6e`. Only `target/debug`
  exists (no release build). `python/.venv` was synced on master
  (torch `2.7.1+cu128`, Python 3.12); the branch's `uv.lock` adds
  `nvidia-ml-py`, so the first gateway start re-syncs automatically.
  `ui/.next` absent: first start runs `next build`. No `data/index` yet.
  The pytest group is not installed (`uv sync --group test --extra
  cu128` from `python/`).
- GPUs: two RTX PRO 6000 Blackwell (97 887 MiB, compute cap 12.0),
  driver 590.48.01, `CUDA_DEVICE_ORDER` unset on the host. SGLang holds
  ~95 GB on both. 62 GB RAM, 48 cores, 1.8 TB free on `/`.
- `~/.cache/huggingface/hub` already has `wd-vit-large-tagger-v3` and
  `wd-eva02-large-tagger-v3`; everything else downloads on first use.
- `ffmpeg` on PATH and in the venv; no ImageMagick; Pillow and numpy in
  the venv.
- `~/docker/inferio` is the old Python-era deployment; its `.env` holds a
  real API key. Do not read it into logs or reports.
- Memory notes for Claude exist under the project memory dir
  (`user-hardware-fleet`, `pr27-batch-calibration-testing`).

### Next actions, in order

1. Spawn two Opus tracks in parallel:
   - **Track A, feedback signal** (§8 G7 brief) with a verifier loop;
     commit on the PR branch when approved.
   - **Track B, Phase 0 tooling** (§2 tools, corpora, oracle calibration,
     release builds of branch and master worktree, CUDA-touching fixture
     impl, `results/` layout). Track B must not start the gateway on a
     GPU SGLang is using for anything but S1.
2. When Track A's verifier approves: run Phases 1–9 (§7), one Opus
   subagent per scenario, each returning a filled `runlog.md`; the
   orchestrator adjudicates thresholds and decides fixes per decision 5.
3. Final report per §7 Phase 9; restart SGLang.

This document is the protocol an autonomous agent session follows to
decide whether the batch-calibration feature is shippable on one platform,
and the method that is then repeated on every other platform we ship to.
It is written for the Linux/CUDA host it is first run on (two RTX PRO 6000
Blackwell, 97 GB each, driver 590, 62 GB RAM, Arch Linux, Docker with the
NVIDIA runtime), with a per-platform portability section at the end.

The companion docs are `batch-calibration-design.md` (what the feature is
supposed to do), `unified-memory-admission.md` (MPS / APU / CPU boards)
and `inferio-worker-protocol.md` (the wire fields the protocol reads).

## 1. What "works" means, and why unit tests cannot answer it

The feature has 827 Rust and 215 Python tests, all against fixtures. None
of them can answer the questions that decide whether it ships:

1. **Is the measurement honest on this platform?** The ledger prices every
   batch from numbers the worker reports (torch allocator peaks, NVML
   per-process usage) and from `nvidia-smi` free/total. If those numbers
   are systematically wrong on a given driver, allocator or container
   setup, every downstream mechanism is wrong with them, and nothing
   inside the system can notice.
2. **Does it track a changing world?** External VRAM usage grows, shrinks,
   spikes and oscillates. The design has a specific story for each
   direction (per-window grow, per-batch shrink, OOM backstop, deflation,
   trim). Each is a timing claim that only a live host can test.
3. **Does it degrade instead of break?** Auto is on for everyone with no
   off switch. A wrong number must cost throughput, never a job, and never
   a hung or dead server.
4. **Is it a regression?** Manual batch sizes are gone. Throughput on an
   idle board must be at least what a user got before.

So the protocol is built around **independent oracles**: every quantity
the feature computes is compared against a measurement taken by a
different instrument, from outside the process, and against outcomes the
user can see (job completion, throughput, errors, worker deaths). Where the
oracle is the same driver API the feature uses, the oracle is first
verified against a known allocation, so the chain is anchored in a
physical fact rather than in agreement between two readers of the same
counter.

## 2. Oracles and instruments

All tools live in `tools/calibration-protocol/` (to be created), Python on
the managed venv (`python/.venv`), stdlib + `nvidia-ml-py` + `psutil` +
Pillow + torch, so the same files run on every platform. Each writes
timestamped JSONL so recordings from different instruments can be joined.

| Tool | What it does | Independent of the feature because |
|---|---|---|
| `vramrec.py` | Samples every 250 ms per GPU: total/used/free and **per-process** used memory with PID, command line and the `CUDA_VISIBLE_DEVICES` / `PANOPTIKON_DEVICE_PIN` of each PID (from `/proc/<pid>/environ`), so each worker is attributed to a model and a board. Also samples `/proc/meminfo` (MemAvailable) and per-PID RSS/VmHWM for CPU-board runs. | Reads NVML directly, out of process, at a fixed cadence. The feature reads `nvidia-smi` free/total only on grant requests older than 10 s, and per-process usage only inside the worker. |
| `hog.py` | Controllable external pressure. Allocates real, touched device memory on a chosen GPU (torch) or RAM (numpy) following a schedule: `hold`, `step`, `ramp`, `spike`, `oscillate`, `leave-free N`; plus an HTTP control endpoint (`POST /set?mb=`, `GET /state`) so an agent can change it mid-test. Frees with `empty_cache()` so the driver sees the release. | It *is* the external world the feature must react to. |
| `corpus.py` | Deterministic media corpus with controlled unit cost: images at chosen pixel sizes and formats (including alpha and huge 8000×6000 scans), text files at chosen byte lengths (token pricing is `bytes / 4`), short audio via ffmpeg, multi-page PDFs. Tiers: `smoke` (200), `ramp` (2 000), `soak` (10 000+). Writes a manifest with per-item units. | Ground truth for what the packer should have priced. |
| `healthrec.py` | Polls `GET /api/inference/health` and `GET /api/jobs/queue` every 500 ms into JSONL: per board `external_mb`, `limit_mb`, `headroom_mb`, `grants_mb`, and per worker `unit_budget`, `ramp_step`, `deflation`, `clean_windows`, `max_units_measured`, `knee_units`, `fit`, `base_mb`, `base_method`. | Not independent (it is the feature's own view), but it is the only record of grants and deflation: the ledger logs none of them (see §8, finding G1). |
| `loadgen.py` | Drives `POST /api/inference/predict/<id>` for several models concurrently from corpus files, with a per-model concurrency and request size. Needed because the job queue runs exactly one job at a time, so multi-model contention on one board never arises from jobs alone. | Produces the concurrency the ledger arbitrates. |
| `ceiling_probe.py` | **Ground-truth cost model.** Loads a shipped impl via the `inferio` package outside the orchestrator on a chosen GPU, records NVML own-PID usage after load (true `base`), then runs batches of 1, 2, 4, … items of a fixed unit size, recording `peak_reserved`, `peak_allocated` and NVML process usage per batch; fits its own slope. Optionally, with `hog.py` leaving exactly F GB free, bisects the true OOM boundary in units for that F. | Same torch, same model, no ledger, no packing, no grant: the numbers the ledger *should* converge to, and the boundary its grants must stay under. |
| `analyze.py` | Joins the recordings for one scenario and prints the verdict table (§6) with the numbers behind each pass/fail; optional PNG timelines. | — |
| `runlog.md` template | The per-scenario report an agent fills in: setup, commands, observations, verdicts, anomalies, log excerpts. | — |

Server-side capture for every run:

```
RUST_LOG=info,panoptikon::inferio=trace
INFERIO_WORKER_LOG_LEVEL=DEBUG
```

with `data/panoptikon.log` (bare) or `docker logs` (container) copied into
the scenario's results directory, plus `data/inferio/calibration.toml`
snapshotted before and after.

**Instrument calibration (Phase 0, mandatory).** Before any scenario:
`hog.py hold 10240` on GPU 0 must show +10 240 MiB (±64) in `vramrec.py`
for the hog's PID and in board `used`; the same for 40 GB; a RAM hog of
16 GB must show as −16 GB MemAvailable. If the oracle cannot see a known
allocation, nothing downstream is trustworthy and the run stops.

## 3. Environment and configurations under test

Everything runs from the repo checkout with `--root`-separated data
directories per configuration, so runs never share `calibration.toml`,
index DBs or logs:

```
tools/calibration-protocol/results/<run-id>/<scenario>/{panoptikon.log, worker.log, vramrec.jsonl,
                             healthrec.jsonl, hog.jsonl, calibration.before.toml,
                             calibration.after.toml, jobs.json, runlog.md}
```

Configurations (`C1` is where most scenarios run; the rest repeat a
subset):

| Id | Configuration | Purpose |
|---|---|---|
| C1 | Bare Linux, release build of the PR branch, both GPUs visible, `config/server/default.toml` copy with `RUST_LOG` as above | Primary. |
| C0 | Bare Linux, release build of **master** in a `git worktree` (`../panoptikon-master`, own venv), same corpus, registry default batch sizes | Throughput and behaviour baseline: "before". |
| C2 | C1 with `CUDA_VISIBLE_DEVICES=GPU-<uuid of GPU 1>` (UUID form) | Single-board host, ledger still active. |
| C3 | C1 with `CUDA_VISIBLE_DEVICES=1` (index form) | Documented off-switch: inventory unknown, unpriced path. Must still work. |
| C4 | Docker CUDA image built from the branch (`docker build --build-arg ACCELERATOR=cuda`), root `docker-compose.yml` as shipped (no `pid: host`) | The plug-and-play target. Base measurement degrades to `free_delta`. |
| C5 | C4 plus `pid: host` | Shows what `--pid=host` buys; informs whether the shipped compose should set it. |
| C6 | Docker **CPU** image (`ACCELERATOR=cpu`) with a cgroup memory limit (`mem_limit: 16g`) | The CPU unified board (`CPU` key, `ram`/`rss` tiers, `cap_fraction` 0.75, DP-2 death-as-negative) without exposing the host to the OOM killer, and without clobbering the cu128 venv. |
| C7 | C1 with a user registry (`config/inference/protocol.toml`) that pins one model to GPU 1 via `devices = ["GPU-<uuid>"]` and flips `enable_batching` on for `easyocr_standard_en` | Multi-GPU and the design's own easyOCR acceptance test. |

SGLang (`~/docker/dsv4flash`) currently holds 95 of 97 GB on both boards.
It is used in Phase 1 as the "board nearly full by someone else"
scenario, then stopped (`docker compose down` in that directory, nothing
edited) for the rest of the run and restarted at the end with `docker
compose up -d`. Intermediate "another real model is resident" levels are
better served by loading one of the other models on this host (the
`~/docker` directory has llama.cpp, vLLM, ComfyUI and text-generation
composes) than by SGLang; `hog.py` covers the arbitrary levels in between.
Any compose used for pressure gets its own copy under
`tools/calibration-protocol/compose/`, never an edit of the user's files.

Models (chosen per cost dimension, smallest first; all download on first
use except the two wd taggers already cached):

| Dimension | Model | Why |
|---|---|---|
| item / count | `tags/wd-vit-tagger-v3` (~350 MB), `clip/apple_MobileCLIP-S1` (~170 MB, two `run_with_oom_retry` passes), `clap/clap-htsat-unfused` (~600 MB, **no** impl-side OOM retry) | Fixed-size inputs; cleanest ramp; clap shows the harness-only OOM path. |
| token / max-times-count | `textembed/all-MiniLM-L6-v2` (~90 MB) | Bucketing by length; `bytes/4` pricing. |
| pixel / sum | `clip/nemotron-embed-vl-1b-v2` (~2.5 GB) | Variable resolution; pricing saturation above 1.8 MP. |
| pixel / max-times-count | `doctr/easyocr_standard_en` with `enable_batching` flipped on (C7) | The uniform-dims OOM trap the design was written for. |
| none | `whisper/tiny` (~75 MB) | Grantless path; a resident whose VRAM lands in `external`. |
| fixture | `inferio_custom/` copies of `oom_second_batch_impl.py`, `oom_impl.py`, `failbatch_impl.py`, `dying_impl.py`, registered through a user registry group with `metadata.cost` (shape in `manager.rs` test registry) | Deterministic OOM, batch-1 OOM, non-OOM failure and worker death on demand. Note: torch-free fixtures report no `gpu_uuid`, so on CUDA they register only through the single-board fallback (C2) or run unpriced; a fixture that touches CUDA (allocate one tensor at load) is needed for a priced fixture on C1. Build that variant in Phase 0. |

## 4. Scenario catalogue

Each scenario states setup, action, what to record, and pass criteria.
"Oracle" means `vramrec.py` unless stated. Thresholds are first guesses
to be tightened by the first run; a threshold that is missed by a small
margin is a finding to discuss, not an automatic fail.

### S0 Build and unit baseline
Release build of the branch and of master; `cargo test -p panoptikon
--release`; `uv sync --group test --extra cu128` then pytest for
`tests/inferio_worker` and `tests/inferio/impl`. Pass: green. Record the
durations. This is a sanity floor, not evidence.

### S1 Inventory, identity and a full board (SGLang still running)
Start C1. Record `detected GPU` lines (two boards, UUIDs, name, 97 887
MiB, `compute_cap` 12.0), `/health` `gpus[]` and `vram[]`. Run the smoke
extraction (`tags/wd-vit-tagger-v3`, `smoke` corpus).
Check:
- `vram[].external_mb` ≈ oracle board used minus our workers' NVML usage,
  within ±1 GB, at every health sample.
- Worker registers on the board it was pinned to; `base_method = "nvml"`;
  `base_mb` within ±10 % of oracle per-process usage for that PID.
- With headroom ≈ 0: the load warns ("expected to need more VRAM than the
  board's remaining headroom"), grants are issued with `mb = 0`, batches
  are seed-sized pre-fit, OOMs (if any) deflate and the job still
  completes or fails per item with a readable error. **No hang, no dead
  server, no unbounded worker respawn loop.** Record how long the job
  takes versus C0 under the same SGLang load. This scenario targets
  finding B1 (zero-MB grants disable the worker's clamp).

### S2 Cold ramp on an idle board (SGLang stopped)
Delete `calibration.toml`. C1, `tags/wd-vit-tagger-v3`, `ramp` corpus of
2 000 uniform 1024² images. Run `ceiling_probe.py` for the same model
first (true base, true slope, true OOM boundary at full free memory).
Check:
- Ramp visible in `healthrec`: `unit_budget` 8 → 16 → 32 → … each step
  earned by a window with a high-water sample; `local_samples` grows;
  `fit` appears after ≥ 3 distinct sizes; `knee_units` after ≥ 12
  samples over ≥ 3 buckets or never (state which). **Expect the ramp to
  stop at 64 for item/count models driven by a job**: the extraction
  layer submits at most `REQUEST_UNIT_BUDGET = 64` items per request
  (`jobs/extraction.rs:65`), so a single job can never fill a larger
  window (finding B16). Run the same model through `loadgen.py` with
  256-item requests to see where the ramp goes without that ceiling, and
  record both anchors.
- Fitted `slope_mb_per_unit` within ±30 % of the probe's slope, or higher
  by at most 2× (conservative side allowed); `base_mb` within ±15 % of the
  probe's NVML base.
- Largest admitted batch ≤ probe OOM boundary at the free memory the
  oracle saw, with at least the configured margin to spare, and ≥ 25 % of
  that boundary (or the knee, if fitted) once ramped: the feature must
  neither overshoot nor sandbag.
- Zero OOM, zero worker deaths.
- Throughput (items/s over the whole job, from `LogRecord`) ≥ 0.9 × C0
  with the registry default batch, and the ramp's cost (time to first
  window at ≥ 50 % of the final batch) recorded.
- `calibration.toml` written within 30 s of the last anchor advance;
  entry key matches (`gpu` name exactly as nvidia-smi prints it, `torch`
  `2.7.1+cu128`, `dtype`, `unit`/`aggregation`).
Repeat for `clip/apple_MobileCLIP-S1` and `textembed/all-MiniLM-L6-v2`
(text corpus with lengths drawn from 40 B to 8 kB: check bucketed
batches are length-homogeneous in the worker DEBUG log and priced at
`bytes/4`).

### S3 Restart and resume
Restart C1 after S2; run a fresh `ramp` corpus for the same model.
Check: first window's `unit_budget` equals the persisted anchor (no
re-ramp from seed); `/api/inference/metadata` shows `calibration.status =
"local"`; `local_samples` continues from the stored count; the file's
anchor only ever increases. Then delete the file with the server
**stopped**, restart, confirm re-ramp from seed. Also delete it while the
server is running and observe whether the next debounced write resurrects
it (finding B3).

### S4 External pressure dynamics (the core)
All on C1, GPU 0, `tags/wd-vit-tagger-v3` calibrated by S2, `ramp`
corpus; `hog.py` on GPU 0. One run per profile:

| Profile | Hog schedule | Expected reaction | Pass |
|---|---|---|---|
| S4a constant | `leave-free 12 GB` before start | Grants sized to ≈ (12 GB − margin) | Every grant's `mb` ≤ headroom; batch units ≤ boundary the probe found at 12 GB free; no OOM; utilization ≥ 50 % of that boundary. |
| S4b step up | +30 GB at t = 60 s, between windows | Next grant shrinks; `external_mb` follows within one grant request (≤ 10 s staleness + one window) | Shrink observed in `healthrec` within 15 s; no OOM, or ≤ 1 OOM followed by deflation and full recovery within 3 clean windows. |
| S4c spike | Fill to 2 GB free for 10 s at t = 90 s, then release | Defensive clamp (`free memory fell to … shrinking this batch's budget`) and/or OOM backstop; deflation; recovery | Job completes; worker survives (or is respawned once); `deflation` returns to 0 within 3 × levels clean windows; fit unchanged by the negative (compare `fit.samples` before/after). |
| S4d step down | Start at `leave-free 8 GB`, release all at t = 120 s | Grants grow back | `unit_budget` recovers to the S2 level within 3 windows of the release (the grow direction is per-window and refresh-gated at 10 s, so state the observed latency). |
| S4e oscillate | Toggle 0 ↔ 40 GB every 20 s for 10 min | Tracking without livelock | Throughput ≥ 40 % of S2; `deflation` never exceeds 3; no worker death; `grants_outstanding` returns to 0 between windows. |
| S4f slow creep | +1 GB/min for 40 min | Continuous shrink | External tracked within ±1.5 GB throughout; grants never exceed live headroom at the moment of issue (join `healthrec` and `vramrec` by timestamp). |
| S4g no room to load | `leave-free 1 GB`, then start a job for an unloaded 2.5 GB model (nemotron) | Load reservation warning, load fails inside torch | Job fails with a clear per-model error within `load_secs`; server healthy; other models unaffected; no respawn loop. |

Each run also produces the timeline plot (oracle used, `external_mb`,
`headroom_mb`, `grants_mb`, `unit_budget`, OOM/negative marks).

### S5 OOM backstop and negative samples
C2 (single-board fallback lets torch-free fixtures register) plus the
CUDA-touching fixture variant on C1:
- `oom_second_batch`: second window OOMs once. Check: `WARN merged batch …
  failed, falling back to per-request prediction` with `oom = true`;
  per-request retries carry a halved `unit_budget` (worker DEBUG shows the
  grant); `deflation = 1` then back to 0 after 3 clean windows; fit and
  anchor unchanged by the failed batch.
- `oom_impl` on one poisoned item in a real corpus (mixed with a normal
  model is not possible; instead use `clip/apple_MobileCLIP-S1` with one
  corrupt file and a 30 000 × 30 000 PNG to provoke a genuine batch-1
  OOM): item fails, job continues, item recorded in
  `/api/jobs/data/failures`. Measure how far `deflation` climbs while
  the poisoned items recur across windows (finding B8: unbounded) and how
  long full recovery takes.
- `failbatch` (non-OOM error): per-request fallback, **no** deflation.
- `dying`: worker death mid-window on a discrete board: model respawned,
  next window admitted at the anchor again (finding B7), no synthetic
  negative (the log line for unified boards must **not** appear). Then a
  fixture that dies on **every** load: measure the respawn cadence and
  how long a 64-item job takes to fail as "Systemic" (finding B15: no
  backoff or cap on respawn; each attempt can cost up to `load_secs`).
- Message classifier probe: a model error whose text merely contains "out
  of memory" (a corpus file literally named `out of memory.png` that the
  impl rejects) must not deflate (finding B11).

### S6 Multi-model contention on one board
C1, `loadgen.py` driving `tags/wd-vit-tagger-v3`, `clip/apple_MobileCLIP-S1`
and `textembed/all-MiniLM-L6-v2` concurrently (concurrency 2 each) for
10 minutes, with `whisper/tiny` loaded and idle as a `none`-class
resident, and `hog.py` at `leave-free 20 GB` to make the board tight.
Check:
- Ledger invariant at every health sample: Σ `charge_mb` + load
  reservations ≤ `limit_mb`; Σ oracle usage of our PIDs ≤ `limit_mb` +
  margin allowance; no OOM.
- Each hungry model gets at least its floor (one seed batch); shares
  scale with appetite after fit.
- Idle-resident trim: stop driving `clip`, keep driving `tags` with a
  squeezed grant; within 30 s of the squeeze `clip`'s pool slack drops in
  the oracle (a `trim` reply) and the debug line "an idle resident is
  holding allocator pool slack …" appears.
- Whisper's CT2 VRAM shows up in `external_mb` (by design) and is
  margin-inflated; note the cost.
- Trim latency: the trim round-trip has a fixed 60 s deadline that kills
  the whole model on expiry (finding B17). Record the observed trim
  durations under load; a `hang_impl`-style fixture that ignores the
  trim for 70 s confirms the failure shape.
- Load stall: while a large model (nemotron) is loading, time the
  predict latency of the already-resident `tags` model. The manager
  takes the load lock on every predict (finding B18), so every model
  stalls behind an in-flight load; record the stall length.

### S7 Multi-GPU
C7. Pin `clip/apple_MobileCLIP-S1` to GPU 1; leave `tags` on the default
board. Drive both. Check: oracle shows each worker PID on the pinned
board only; two independent `vram[]` rows; a hog on GPU 0 changes only
GPU 0's grants; `PinDiverged` never logged. Then C3 (index-form
`CUDA_VISIBLE_DEVICES=1`): confirm the INFO line, no `vram[]`, unpriced
dispatch, and that the job still completes with registry defaults.

### S8 Cost dimensions and packing
- pixel/sum: nemotron on a corpus mixing 0.3 MP, 1 MP, 4 MP and 20 MP
  images. Check batches are priced from header dimensions (worker DEBUG),
  20 MP items are over-priced (few per batch), no OOM, and record the
  utilization loss the saturation causes (design's deferred
  `unit_cap_per_item`).
- pixel/max-times-count: **easyOCR acceptance test** per the design doc's
  "Remaining for the easyOCR acceptance test": C7 with batching enabled,
  a corpus from thumbnails to 8000×6000 scans, run as a normal extraction
  job (real core pipelining). Check bucketed batches are size-homogeneous,
  no OOM, no throughput-collapse flags, throughput beats C0's per-image
  path.
- token/max-times-count: MiniLM with CJK text (pricing under-estimates
  ~3×) and with texts longer than `max_seq_length` (over-priced). Record
  whether either direction produces an OOM or only a utilization loss.
- Grantless: whisper and a moondream id: no grant, no `units`, no fit,
  job fine.

### S9 Soak (overnight)
C1, 8–12 h: a loop of extraction jobs over the `soak` corpus alternating
models, `hog.py` on a randomized schedule (steps, spikes, calm periods),
`loadgen.py` at low rate in the background. Check every hour from
`healthrec`: `grants_outstanding` returns to 0 when idle; `deflation`
returns to 0 after calm periods; `calibration.toml` size bounded (ring
64); server RSS flat; no worker deaths except intended; throughput per
job within ±20 % of the S2 figure during calm periods.

### S10 Migration
Create an index DB under C0 (master) with `job_settings[].default_batch_size`
and a cron row `batch_size` set; boot C1 on the same data dir. Check: the
INFO line "batch sizes … were reset to auto", `config.toml` lost only those
keys (byte-compare the rest), `batch_auto_migration` row present; set a cap
of 4 through the API, restart, cap survives. Repeat through the Docker
volume path (C4 image over a volume seeded by a master-built image). Note
for the release notes that the old numbers are not backed up anywhere.

### S11 Docker CUDA
C4 then C5 with the same S2 run. Check: `base_method` is `free_delta` or
`alloc_delta` in C4 and `nvml` in C5; `external_mb` still tracks a hog
running **on the host** (board-level NVML works without `--pid=host`); S4b
and S4c repeated in C4; the health check and the CI smoke script pass;
`docker logs` carries the worker lines. Record the base error C4 incurs
versus the oracle (this decides the `pid: host` recommendation).

### S12 CPU board (Docker CPU image)
C6 with `mem_limit: 16g`. `tags/wd-vit-tagger-v3` on the `smoke` corpus.
Check: board `CPU`, name `CPU (64 GB)` (or the 4 GiB-grid rounding of
whatever the container reports), `cap_fraction` 0.75 in `/health`,
`free_source = "ram"`, `base_method = "rss"`, `INFERIO_DEVICE=cpu` in the
worker's environ; a RAM hog inside the container that pushes the worker
past the cgroup limit gets it OOM-killed and the ledger logs the
unified-board death negative and halves the anchor; the job continues.
Whether the fit ever produces samples (the monotone high-water problem
in the unified doc) is recorded, not judged. **Expected finding (B19):**
the CPU board reads host RAM, not the cgroup limit (`cpu.rs`), so inside
a 16 GB container the board says `CPU (64 GB)` with a 46 GB budget while
the kernel kills at 16 GB. Record the board total and whether the
death-negative path is the only thing standing between a Docker CPU user
and a respawn loop.

### S13 Probe robustness
C1 with `nvidia-smi` hidden from PATH (unpriced with a WARN, jobs work);
with a wrapper `nvidia-smi` that sleeps 6 s (5 s timeouts, 10 s backoff,
grants proceed on stale values, measure how many blocking threads pile
up under S6 load: finding B13); with a wrapper that prints a malformed
row (whole inventory unknown, WARN, unpriced).

### S14 Regression sanity
The CI smoke sequence (`.github/workflows/release.yml`) against C1 and C4:
DB create, folders, rescan, PQL search, thumbnail, file serve, 403 on
port 6339 (the 403 assertion is Docker-only: `docker.toml` gives 6339 the
`public` endpoint with `restricted_demo`, while `default.toml` gives it
`legacy_ui` with the `localhost` policy, which correctly answers 200 on
C1). Plus one run of every shipped model category on the `smoke`
corpus to catch a load-path regression unrelated to calibration.

### S15 Protocol self-test (mutation runs)
The protocol is only useful if it catches what it is meant to catch. Three
deliberate faults, each on a throwaway branch, run through S2 and S4b:
1. Worker under-reports `peak_reserved_mb` by 50 % (patch `measure_batch`).
   Expected catch: slope check in S2 fails; S4b or S4c produces OOMs the
   healthy run did not.
2. Host ignores `external` (patch `external_locked` to return 0).
   Expected catch: S4a grants exceed headroom against the oracle; S4b OOMs.
3. Margin set to −0.5 in the config (if validation lets it through, that
   is itself a finding). Expected catch: S4 series.
A fault the protocol misses is a hole in the protocol and gets a new
check before the platform passes are run.

## 5. Targeted probes from the code reading

The three code maps (host ledger, worker, deployment) surfaced concrete
suspicions. Each is pinned to the scenario that will confirm or clear it,
so the run produces an answer for every one rather than a vague sense of
risk.

| Id | Suspicion (file) | Scenario |
|---|---|---|
| B1 | A full board yields `mb = 0` grants; the worker's `clamp_to_live_memory` and `maybe_shrink` treat `grant_mb <= 0` as "no reservation", so pre-fit units are memory-blind exactly when the board is fullest (`ledger.rs:2879`, `packing.py:450`). | S1, S4c |
| B2 | External refresh is `nvidia-smi` free/total only, grant-triggered, 10 s staleness, no poller: a quiet board's picture can be minutes old (`ledger.rs:865-880`). | S4b, S4d (latency numbers) |
| B3 | Deleting `calibration.toml` under a running server can be undone by the next debounced write; a corrupt file is silently replaced (`calibration.rs:586`, `:1219`). | S3 |
| B4 | A persisted anchor is a permanent floor across driver/torch patch bumps matched by `major.minor`; the OOM backstop is the only protection (`ledger.rs:2295`). | S3 variant: edit the stored anchor to 4× and observe the first window |
| B5 | A shipped baseline knee can only ratchet down within a run (frontier guard) | Not testable until baselines ship; note only |
| B6 | Index-form `CUDA_VISIBLE_DEVICES` silently disables the ledger at INFO (`gpu.rs:846`); Docker users commonly set it. | S7 (C3), S11 |
| B7 | Discrete-board worker death relearns nothing; respawn is admitted at the anchor again (`ledger.rs:741`). | S5 |
| B8 | Deflation is unbounded and recovers one level per 3 clean windows; a few unfittable items can pin a model at 1-unit batches for a long time (`ledger.rs:725`). | S5 |
| B9 | Container without `--pid=host`: base falls to `free_delta`, contaminated by concurrent activity in the load window (`memory.py:403`). | S11 |
| B10 | Post-trim fit samples use a stale `reserved_at_load`; Theil–Sen may refuse and keep an old fit forever (`ledger.rs:3286`, `:3378`). | S6 (trim) then S2 re-run |
| B11 | `message_reports_oom` matches any line containing "out of memory" (`ledger.rs:4119`). | S5 |
| B12 | No log line for grant issued, batch chosen, negative applied, ramp step, refresh result or store write; `/health` polling is the only reconstruction path. | Every scenario; see G1 |
| B13 | A wedged `nvidia-smi` costs a 5 s blocking thread every 10 s per board. | S13 |
| B14 | Single-model hosts never confirm the +0.15 unconfirmed margin (`ledger.rs:3346`). | S2 (record effective margin) |
| B15 | Worker death → immediate respawn on the next predict with no backoff or attempt cap (`manager.rs:1068-1085`, `:1152-1197`); a job of N items can pay N loads before failing. | S5 |
| B16 | `REQUEST_UNIT_BUDGET = 64` caps every item/count window a job can produce, so anchors and knees learned from jobs never exceed 64 regardless of VRAM (`jobs/extraction.rs:65`). | S2 |
| B17 | `TRIM_DEADLINE = 60 s` is fixed and fatal for the whole model (`worker.rs:113`, `dispatch.rs:803`). | S6 |
| B18 | The manager's load lock is taken on every predict (`manager.rs:1161`), so any load (up to `load_secs`) stalls every model. | S6 |
| B19 | The CPU board reads host RAM and is cgroup-blind (`cpu.rs`). | S12 |
| B20 | The unpriced path bounds windows by `default_max_batch` (32) rather than seed-sized batches as the design says (`dispatch.rs:37-48`). | S7 (C3), S13 |
| B21 | Prewarm-parked workers are invisible to the ledger; a `prepare()` that initialises CUDA becomes margin-inflated external usage. | S6 (compare `external_mb` with and without prewarm) |
| B22 | `/health` renamed `last_effective_cap` to `last_grant_units`; check the UI submodule and any consumer. | S14 |
| B23 | `accelerator_backend(Auto)` keys profiles as `cpu` while the probe behaves as CUDA on the validation-failure path (`http.rs:271-278`). | S1 (inspect the `backend` key written to `calibration.toml`) |
| W1 | Pixel pricing uses raw submitted dimensions, not the model's canvas (`packing.py:300`). | S8 |
| W2 | Measurement brackets CPU decode time, so slow-decoding inputs can trip the 0.4 collapse ratio spuriously (`packing.py:622`). | S8 (20 MP PNGs), S4e |
| W3 | After an absorbed OOM the throughput comparator is not reset, so the regrowth batch may be flagged as a collapse (`packing.py`, `utils.py`). | S5 |
| W4 | `alloc_delta` uses a fixed 500 MiB context estimate; Blackwell contexts may exceed it. | S11 (C4 base error) |
| W5 | Ambient `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` changes what `memory_reserved` means and nothing pins or reports it. | S2 repeated once with that env set |

## 6. Verdict table (what `analyze.py` prints per scenario)

| Check | Source | Threshold |
|---|---|---|
| Oracle agreement: `external_mb` vs oracle | healthrec × vramrec | ±1 GB or ±2 % of total, every sample |
| Base accuracy: `base_mb` vs oracle per-process | health × vramrec | ±10 % (nvml), report only (free_delta) |
| Slope accuracy vs `ceiling_probe` | calibration.toml × probe | −30 % … +100 % |
| Safety: grants vs live headroom at issue time | healthrec × vramrec | never exceeded |
| Safety: OOMs, worker deaths | logs | 0 unless the scenario provokes them |
| Recovery: deflation → 0 after calm | healthrec | ≤ 3 × levels clean windows |
| Liveness: `grants_outstanding` at idle | healthrec | 0 |
| Utilization: admitted units vs probe boundary / knee | health × probe | ≥ 25 % ramped (S2), ≥ 50 % (S4a) |
| Throughput vs C0 | LogRecord | ≥ 0.9 × idle board; ≥ 0.4 × under S4e |
| Persistence | calibration.toml | written ≤ 30 s after anchor advance; resumes on restart |
| Job outcome | `/api/jobs/queue` outcomes, failures | completed; failures only for poisoned items |

## 7. Execution plan for the autonomous session

Estimated wall time 24–36 h including the soak; agent-driven with the
main session orchestrating and subagents running one scenario each and
returning a filled `runlog.md`. Phases are sequential; scenarios inside
a phase may run in parallel only when they use different GPUs or
different root directories and do not share `hog.py`.

| Phase | Content | Needs |
|---|---|---|
| 0 Prep (~2 h) | Release build of the branch; master worktree build with its own venv; test-group sync; write the tools in §2; generate the corpora; **oracle calibration** against known allocations; CUDA-touching fixture impl. | Nothing stopped yet. |
| 1 Smoke (~1 h) | S0, S1 (with SGLang running), S14. | — |
| 2 Idle-board truth (~3 h) | Stop SGLang. `ceiling_probe` for the four models. S2 for three models, S3. | SGLang down from here. |
| 3 Pressure (~4 h) | S4a–S4g. | — |
| 4 Faults and dimensions (~3 h) | S5, S8. | — |
| 5 Concurrency (~2 h) | S6, S7. | — |
| 6 Deployment (~4 h incl. image builds) | S10, S11, S12. | Docker builds. |
| 7 Robustness and self-test (~3 h) | S13, S15. | Throwaway branches. |
| 8 Soak (8–12 h) | S9. | Overnight. |
| 9 Report (~1 h) | Aggregate verdicts, findings list with severity, portability notes, recommendation. Restart SGLang. | — |

Rules for the session:
- **Orchestration.** The main session (Fable) only orchestrates, decides
  and adjudicates; every scenario, tool, fix and review is executed by
  Opus subagents to conserve quota.
- **Fix policy.** Low-discretion fixes (a wrong constant, a missing
  clamp, a log line, a crash on an edge the design already covers) are
  made by the executing agent on their own commit, then reviewed by a
  *separate* verifier subagent, iterated until the verifier is satisfied,
  and listed in the final report with the review outcome. Anything that
  changes a feature, a default, a user-visible behaviour or a design
  decision is **not** fixed: it is written up as a finding with options
  and left for the user's decision.
- Every scenario's `runlog.md` is written before the next starts, with
  the exact commands, so any result can be reproduced by hand.
- A scenario that cannot be completed is marked blocked with the reason,
  never skipped silently.
- Nothing in `python/inferio/config/` or `config/server/` is edited; all
  overrides go through user registry files and a copied server TOML.

## 8. Findings already made while writing this plan

These come from reading, not running, and are recorded so the run can
confirm or refute them rather than rediscover them.

- **G1 Observability gap — resolved in `49822c8b`.** The ledger emitted
  no log line when it issued a grant, applied a negative sample, earned a
  ramp step, refitted, refreshed external usage, admitted a worker or
  wrote the store; reconstruction relied on `/health` polling. The commit
  adds structured `debug!` lines for each ("issued a memory grant",
  "settled a granted window" — `warn!` with a `reason` on negatives —
  "refitted the memory cost model", "refreshed the board's free memory
  from the host probe" with a `recorded` flag, "admitted a worker to a
  board's ledger", "queued a calibration profile update for the store")
  and an `info!` "wrote the local calibration store". `healthrec.py`
  stays as the cross-check, and `analyze.py` reconstructs primarily from
  these lines.
- **G2 Docker `pid` namespace.** The shipped compose files do not set
  `pid: host`, so the plug-and-play target always takes the degraded
  base-measurement tier. S11 measures the cost; the likely outcome is a
  recommendation to add it (or to document why not).
- **G3 Full-board behaviour is unclamped pre-fit** (B1). The very first
  windows on a board another process nearly fills are protected only by
  the OOM backstop.
- **G4 Manual override.** There is no configuration switch to disable
  admission; the only off-switches are side effects (index-form
  `CUDA_VISIBLE_DEVICES`, a hidden `nvidia-smi`). Worth a deliberate
  decision before release given the feature is on for everyone.
- **G5 The migration does not preserve the old numbers.** Fine per the
  design's reasoning, but the release notes must say so. The stamp
  insert is the one step whose failure blocks startup.
- **G6 Failure containment is weaker than the design implies.** Worker
  death has no respawn backoff (B15), the trim deadline is fatal (B17),
  and the load lock serialises predicts behind loads (B18). None of these
  is a calibration bug as such, but calibration makes them reachable
  more often (trims and reloads are now routine), so the run measures
  each one.
- **G7 A job alone cannot calibrate past 64 items** (B16), and this is a
  regression against master, not a default. On master the extraction
  job's in-flight unit semaphore was sized by the user's batch size
  (`Semaphore::new(defaults.batch_size)`), so a user who set 512 kept 512
  items in flight and the worker saw 512-item batches. The PR replaces
  that with the constant `REQUEST_UNIT_BUDGET = 64` as both the per-request
  chunk size and the **total** in-flight budget per job, and a cap can
  only lower it. Because the dispatcher only merges requests that are in
  flight, one job can never present the ledger with a window above 64
  items, so for item/count models the ramp, anchor and knee all stop at
  64 regardless of VRAM: on a 97 GB board (or two), a wd-tagger or CLIP
  job runs at batch 64 forever. The other core-side bounds (8 loader
  slots, a 1 GB intermediate byte budget) are not the limiting factor for
  small images. The design doc's own split says core should size requests
  "by keeping the server fed" without learning about VRAM; a constant
  cannot do that. Options, for the user's decision: (a) decouple the two
  meanings, keeping 64 as the per-request chunk and raising the in-flight
  total to something the byte budget already bounds (hundreds to a few
  thousand units); (b) let the in-flight total follow a signal from the
  inference response (the window's admitted units, or a "wanted more"
  flag) so core adapts without learning about VRAM; (c) make it a
  configurable per-server knob. (a) is the low-risk immediate fix; (b) is
  what the design's wording implies. S2 measures the gap by driving the
  same model through `loadgen.py` with large requests.

  **Resolution (user decision, 2026-09-02): implement (b) before the run,
  in parallel with Phase 0 tooling.** Brief for the implementing agent:
  - The orchestrator publishes, per model, a **desired in-flight figure
    in items** (core counts items and PDF pages, never pixels or tokens):
    the dispatcher's window target in units converted through the recent
    units-per-item ratio (`last_window_items` / last window units, or the
    seed for a pre-fit model), times a small slack (≈2) so consecutive
    windows can merge. Bounded by the byte wall the dispatcher already
    applies.
  - Carried on every predict response as an additive, optional field
    (`desired_in_flight_items` or similar; document it in
    `inferio-worker-protocol.md`'s HTTP section and `openapi.json`).
    Absent, as from an older server, means the floor.
  - Core (`jobs/extraction.rs`) resizes its per-job unit semaphore toward
    the figure on each response: add permits to grow; to shrink, hold
    back permits as they return. Floor 64, ceiling derived from the
    intermediate byte budget and the loader slots. The per-request chunk
    of 64 stays: it exists to split a 4000-page PDF, and the dispatcher
    merges requests into windows regardless.
  - Isolation retries (`ISOLATION_MAX_BATCH`) are unaffected.
  - Tests: unit tests for the semaphore resizing in both directions and
    for the absent-field floor; a dispatcher test for the figure's
    derivation; and S2 gains the check "core's in-flight count follows the
    window target within one or two windows and the ramp no longer stops
    at 64".

  **Implemented (2026-09-03)** on this branch, by the commit *"Let core's
  in-flight unit budget follow the server's desired item count"* (the hash
  cannot be written inside the commit that carries this paragraph; it is
  the branch's tip commit touching `inferio/dispatch.rs`,
  `inferio/manager.rs`, `inferio/http.rs`, `inferio_client.rs`,
  `jobs/extraction.rs`, `panoptikon/openapi.json` and
  `docs/inferio-worker-protocol.md`). What was built:
  - **Carrier: an HTTP *response header*,
    `x-panoptikon-desired-in-flight-items`**, not a body field. Predict
    answers in three encodings (`application/octet-stream`,
    `multipart/mixed`, the JSON `{"outputs": [...]}` envelope) and only
    the last could hold a scalar — a body field would be absent for
    exactly the image and embedding models G7 is about. Documented in
    `inferio-worker-protocol.md` ("Desired in-flight items") and in the
    OpenAPI document, whose source of truth is the `#[utoipa::path]`
    annotation in `inferio/http.rs`; `panoptikon/openapi.json` is the
    regenerated fixture.
  - **Derivation** (`dispatch::desired_in_flight_items`, per window
    formation): the ledger's window target in units
    (`admitted_units x WINDOW_DEPTH_MULTIPLIER`) times the just-formed
    window's items-per-unit ratio times a slack of 2, then bounded by
    `MAX_WINDOW_BYTES` converted through that window's bytes-per-item
    (the byte bound takes no slack: a window at the wall cannot merge
    another request anyway). **Amended 2026-09-03 (finding T5):** the
    unit target is the *granted* budget's window depth
    (`unit_budget x WINDOW_DEPTH_MULTIPLIER`) whenever the ledger
    flagged the grant `squeezed`, and the anchor-derived target
    otherwise — `dispatch::in_flight_target_units`, which also clamps
    the next window the dispatcher forms, since the header cannot
    shorten a window that is already formed. The published figure still
    meets core's floor of 64 below, so under a hard squeeze (under ~11
    granted units at one unit per item) the window clamp — which has no
    floor — is what actually shortens the blind window. As shipped on 2026-09-02
    the figure was always the anchor-derived target, so a grant
    squeezed to 11 units was followed by a window of 1 936 requests
    that ran 49 s with no grant, no high-water sample and no
    re-pricing while `/health` and the header kept publishing the
    1 024-unit figure (Phase 3, S4a). Before any window exists the ratio comes
    from a per-unit-class seed — 1 for `item` and for every
    `count`-aggregated model, `PIXEL_FALLBACK_UNITS` (2 MP) for `pixel`,
    a new `TOKEN_SEED_UNITS = 512` for `token`,
    `AUDIO_FALLBACK_SECONDS` (30) for `audio-second`. Unpriced paths
    (the `none` cost class, no inventory, unknown board) publish
    `unpriced_window_items x 2`; the user's `max_batch` cap is
    deliberately not folded in, since a cap bounds GPU batches, never
    how much work the caller keeps in flight.
  - **Core** (`jobs/extraction.rs`): the per-job unit semaphore is now a
    `UnitBudget` that resizes toward the figure on each response. Floor
    64 (`MIN_IN_FLIGHT_UNITS`, which is also the starting value; it is a
    deadlock bound, since one chunked request acquires up to 64 permits
    at once), ceiling `max(intermediate_budget_kib / NOMINAL_UNIT_KIB,
    loader_concurrency x 64)` with `NOMINAL_UNIT_KIB = 256`, i.e. **4096
    units at the shipped defaults**. Growth adds permits; a shrink
    withdraws only free permits and withholds the remainder as
    outstanding permits return, so it never interrupts work in flight.
  - **Absent header = no change** from the last figure, not a drop to the
    floor. Since the budget starts at the floor, a server that never
    sends it leaves the job at 64 for the whole run (the pre-feature
    behaviour), while one that sends it and then misses a response keeps
    what it published.
  - `REQUEST_UNIT_BUDGET = 64` stays as the per-request chunk, and
    `ISOLATION_MAX_BATCH` is untouched. No grant arithmetic, ramp, knee
    or user-visible default changed.
  - **Follow-up, not done here:** `ui/lib/panoptikon.d.ts` is generated
    from `panoptikon/openapi.json` by `npm run gen:api` in the `ui`
    submodule (a separate repository), and the new response header
    changes it. Nothing is broken meanwhile — the UI never calls the
    predict endpoint — but it should be regenerated on the UI side
    before release.

## 8b. Noted for after the run: throughput as a calibration target

The user's framing of the feature: find the optimal batch size, adjust it
dynamically so nothing runs out of memory, keep jobs as fast as possible,
and preserve whatever headroom the user asked for. On a GPU the optimum
is usually "as much memory as possible"; on CPU and MPS it is often much
smaller, where a bigger batch is slower. The current design treats memory
as the target and throughput only as a **cap** (the knee: stop growing
once units/sec plateaus). It never *seeks* the throughput optimum, and on
unified boards the memory-derived budget can be far above the speed
optimum. Making batch speed a secondary calibration target is out of
scope for this run but expected to be necessary for production; S2's
throughput ring and the knee samples on the CPU board (S12) are the data
that will show how far the two optima sit apart on each platform.

## 9. Portability: what changes per platform

The scenarios are identical everywhere; the oracle and the pressure
generator differ. The autonomous run on this host produces the tools and
the scenario definitions; each platform pass then needs only the items
below, and its report uses the same verdict table.

| Platform | Oracle (`vramrec.py`) | Pressure (`hog.py`) | Known differences to expect |
|---|---|---|---|
| Ubuntu NAS, RTX 3090 (Ampere, CUDA) | Same as here | Same | Single board; smaller headroom makes S4 tighter; validates the ramp on a 24 GB card and the shipped baseline story once one exists. |
| Windows desktop, dual RTX 5090 (WDDM) | NVML per-process returns N/A on WDDM; oracle records board-level used/free plus **`PDH` "GPU Process Memory" counters** (or `nvidia-smi --query-compute-apps`, which does work on WDDM for used memory) for attribution; expect base tier `free_delta`. | Same (torch) | Sysmem fallback: over-admission shows as throughput collapse, not OOM; S4c must watch `throughput_collapse` flags and per-batch `duration_ms`; S15 mutation 1 is the key sensitivity test there. Also run once with the driver's "Prefer No Sysmem Fallback". Two boards: S7 is the monitor-asymmetry test the design defers. |
| MacBook Pro M3 Max 128 GB (MPS) | No per-process GPU counter; oracle records `vm_stat`/psutil RAM, `sysctl iogpu.wired_limit_mb`, and the worker's own `driver_allocated` from `/health`; ground truth for base comes from `ceiling_probe.py` in-process (`torch.mps.driver_allocated_memory()` is per-process by construction). | RAM hog (numpy) and an MPS hog (torch tensors on `mps`) | Total adoption from the first load (`GPU-MPS`, recommended-max), re-adoption after raising the wired limit under a running gateway, watermark env 1.0/1.0, near-ceiling GC bias, jetsam death-as-negative (S12 analogue). The field-pass list in `unified-memory-admission.md` maps onto S1/S3/S4/S12. |
| BC-250 (ROCm APU) | amdgpu sysfs (`mem_info_vram_*`, `mem_info_gtt_*`) plus DRM fdinfo per PID; `rocm-smi` as a second reader | torch on HIP | Which total HIP reports (either-of cross-check), GTT spill as slowdown rather than OOM, OOM string forms, `HSA_OVERRIDE_GFX_VERSION` passthrough. The ROCm parity doc's "first numbers to measure" become S1's checklist. |
| Linux Desktop / Nix | Same as here | Same | Only the packaging path differs; S14 plus S2 suffice. |

A platform passes when S1, S2, S3, S4a–d, S5, S14 and the platform's own
field-pass items pass; S6–S13 are run once here and on Windows (the
second multi-GPU host), and S9 once here.
