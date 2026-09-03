# Batch calibration: agent-driven test protocol

Status: **run once on the Linux/CUDA host on 2026-09-03 (run id `run1`)
and revised from what that run measured** (PR #27, branch
`claude/batch-calibration-coverage-db9ab9`, master merged in). Section 0
is the handoff: every decision taken and the exact next actions, so a
fresh session can start the work from this file and
`tools/calibration-protocol/codemap.md` alone. The scenario text in §4,
the probe table in §5 and the portability list in §9 now carry run1's
results; the recordings behind every number are in
`tools/calibration-protocol/results/run1/` (git-ignored; indexed by that
directory's `README.md`) and the per-phase reasoning in the phase reports
the final report cites.

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

### Decisions taken during run1 by the orchestrator (2026-09-03)

Decision 5 above splits every problem into "fix it now, with a separate
verifier" and "write it up with options and leave it to the user". These
are the calls the orchestrator made under that rule during run1, with the
commit subject each landed under and why it was classified as
low-discretion. Everything else run1 found is a finding with options, not
a fix — including every threshold, default and policy question (the knee
estimator, the deflation cap, the margin arithmetic, the OOM message
classifier, pixel pricing, the load lock, the trim deadline, per-window
re-queue on worker death).

| # | Fix (commit subject) | Why low-discretion |
|---|---|---|
| 1 | *Force uv copy link mode in the Docker build so it works on ZFS-backed hosts* (`5b7c7353`), comment fix *Say why the Docker build pins uv's link mode, and that it outlives the build* (`3cff5c8c`) | The shipped `Dockerfile` could not build at all on this host (uv's reflink on overlay2-over-ZFS, `os error 11`). One `ENV`; no runtime behaviour changes; the alternative was to test an image the shipped file cannot produce. |
| 2 | *Infer a model's dtype and log why a calibration update is skipped* (`58dda519`, verified; design-doc clarification `918ec170`) | Phase 1's headline: five of five shipped models report no `dtype`, so `queue_profile_update` bailed and `calibration.toml` was **never written** on any host. The design says the profile persists; the store key's meaning is unchanged (a resolved dtype is inferred from the loaded weights, with a `dtype_method` field saying how), and every early return now logs once per (model, board, reason). Flagged to the user as a reversible judgement: **D1** store `dtype_method` in the profile? **D2** sentinel name `"unknown"` vs `"unstated"`. |
| 3 | *Do not reattribute a departed worker's VRAM to external usage* (`d17e3854`; verified fix-up of `cc78394a`) | N4: after an unload the board kept the departed replica's footprint in `external_mb` (775 → 27 603 MiB against an oracle that saw 638 MiB and no process) until the next grant. An accounting bug with a phantom as its only output; it caused five of seven `oracle_agreement` FAILs in Phase 3 and none of it is a policy choice. |
| 4 | *Probe the board before pricing a load against it* (`8546cd63`, fix-up `ff34b059`) | T2: a board with no resident is never probed, so the `expected > headroom` load guard — the one question asked before a worker exists — could never fire. S4g spent 259 s on four 4 096 MiB reservations against a board holding 95.6 GB of someone else's memory. One NVML call on a path that already costs tens of seconds; the guard already existed. |
| 5 | *Publish the granted budget when a window's grant was squeezed* (`22eb33f9`, fix-up `ff34b059`) | T5: the G7 header published the *anchor-derived* figure while the ledger had just issued a squeezed grant, so a grant of 11 units was followed by a 1 936-request window that ran 49 s blind. The published figure contradicting the grant the same code path had just issued is a defect in the feature added for this run, not a new policy. |
| 6 | *Clear the board's refreshing flag even when the host probe panics* (`8c696ac0`, fix-up *Pin the probe guard's disarm and correct its abandoned-task claim* `eb398d69`) | A missing drop guard: a panicking probe stranded `refreshing = true` and no further refresh could ever start on that board. Textbook low-discretion. |
| 7 | *Report every worker death with pid, status and signal; sweep idle replicas* (`c8d64a5a`, amended from `80a1d836`) | P5-1/P5-6: deaths were logged without a pid and without saying who killed the process, and a dead **grantless** replica went unnoticed for 13 minutes because a `none`-class model forms no window. Log fields plus a liveness sweep on the existing tick; no admission arithmetic changed. (P5-1's premise had to be corrected in the process: the exit status and stderr tail *were* already logged on the predict path; the gaps were the pid, the requestless death and the kill attribution.) |
| 8 | *Bound the in-flight window by the process's descriptor budget* (`d5e42c78`, sweep `13e8850f`, docs `25153334`) | F6, the Phase 6 blocker: the shipped Docker image could not finish a 2 000-item job (`nofile` soft 1024, 983 sockets, `status: -1`, 1 849 items unprocessed) where master finished 2 000/2 000. Classified as **completing G7**, the change the user approved for this run: G7 made the in-flight budget follow the grant and nothing in it knew about descriptors. Two parts: raise the soft `RLIMIT_NOFILE` to the hard limit at start-up, and clamp the ceiling by `(soft − 256) / 2`. The compose-only option (1) and in-process dispatch (4) are left to the user. |
| 9 | *Fork every supervised child from one thread that never exits* (`f9cf10fa`) and *Tell a dying worker from one the gateway killed* (`5becf29c`, F12's three-valued attribution) | The gateway SIGKILLs its own workers ~10 s after any load that re-probed the host — `PR_SET_PDEATHSIG` is thread-scoped and the load-path `block_in_place` added in fix 4 demotes the forking thread into Tokio's blocking pool, which reaps it after 10 s idle. It is a **regression introduced by this run's own fix**, measured 8/8 with Δ 1–3 ms from the forking thread's exit, costing 15 924 of 16 000 items on a job that still reported *completed*. Fixing a regression the run created is not a design change. |

Two decisions were made *not* to change anything:

- **G2, `pid: host` in the shipped compose: do not add it.** It buys 0.00 %
  of base error on this driver (S11) and costs the container's PID
  isolation. The claim is driver-specific, so it is a per-platform check
  (§9), not a shipped default.
- **The file-descriptor exposure found by Track A's verifier** was first
  handled as "every scenario greps for `EMFILE` and records `ulimit -n`"
  (option (a)); Phase 6 found the real limit case and it became fix 8.

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

**Instrument calibration (Phase 0, mandatory).** Before any scenario the
oracle must see a known allocation. **Revised after run1: this is one
command per target**, `oracle_calibrate.py`, which starts `vramrec.py`,
drives `hog.py hold <size>` for each size and exits non-zero if any size
misses its tolerance, so it can gate the run:

```bash
V=python/.venv/bin/python; T=tools/calibration-protocol
$V $T/oracle_calibrate.py --target gpu --device 0 --sizes 10240,40960 \
    --hold 30 --settle 10 --out $T/results/phase0/oracle-gpu-full-dev0
$V $T/oracle_calibrate.py --target gpu --device 1 --sizes 10240,40960 \
    --hold 30 --settle 10 --out $T/results/phase0/oracle-gpu-full-dev1
$V $T/oracle_calibrate.py --target ram --sizes 16384 \
    --out $T/results/phase0/oracle-ram-16g
```

It compares payload against payload: the CUDA context (`context_mb`,
**666–668 MiB** on this driver) is measured once by the hog and subtracted,
and the RAM leg judges the hog's RSS and the `MemAvailable` *recovery at
release* rather than a baseline-vs-hold delta, which on a busy host is off
by gigabytes. Run1 numbers, both boards: board `used` **+2 MiB** and NVML
per-process **−6 MiB** against a known 10 240 and 40 960 MiB (tolerance
±64); RAM +32 MiB RSS, +78 MiB recovery (±512). If the oracle cannot see a
known allocation, nothing downstream is trustworthy and the run stops.
Two traps run1 hit: the GPU legs need SGLang (or whatever else owns the
board) **stopped** for the 40 GB size, and `--alloc-timeout` is a budget,
not an addend to `--hold` (fixed in `e67705c2`; before that a 16 GiB RAM
leg held for 40 minutes after filling in 107 s).

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
| C4 | Docker CUDA image built from the branch (`docker build --build-arg ACCELERATOR=cuda`), root `docker-compose.yml` as shipped (no `pid: host`) | The plug-and-play target. ~~Base measurement degrades to `free_delta`.~~ **Refuted in run1** on driver 590.48.01 with the NVIDIA Container Toolkit: NVML resolves the worker's namespace-local pid, `base_method` stays `nvml` and the base error is 0.00 %. Keep it as a per-platform check, not an assumption. |
| C5 | C4 plus `pid: host` | Shows what `--pid=host` buys; informs whether the shipped compose should set it. **Run1 answer: nothing (0.00 % either way) — do not add it to the shipped compose** (G2, finding F10). |
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

**Operational notes from run1 (2026-09-03).** Each of these cost time in
the first run and is a precondition, not a finding:

- **`--root` is a `chdir`.** Every `[inference_local]` path (`python`,
  `impl_dirs`, `config_dirs`, `pythonpath`) must be absolute in a
  `--root`'d run, and the CWD `.env` auto-load stops working. The configs
  under `tools/calibration-protocol/config/` and `run-gateway.sh` handle
  it; anything else launched with `--root` inherits the trap.
- **Never start a gateway on the shipped default config.** The venv setup
  sentinel goes stale whenever `uv.lock` moves, and a start with the
  shipped config re-syncs the venv and **drops the `test` group**. The
  protocol's configs are immune because they set `python` explicitly.
- **`panoptikon accelerator` ignores `CUDA_VISIBLE_DEVICES`** — it prints
  the raw `nvidia-smi` stack list. Verify C2/C3 from the startup
  `detected GPU` lines and `/api/inference/health`, never from the CLI.
- **The Docker build needs `UV_LINK_MODE=copy`** on a host whose overlay2
  sits on ZFS (uv's reflink fails with `os error 11`). Now an `ENV` in the
  shipped `Dockerfile` (`5b7c7353`); a scratch copy of master's Dockerfile
  needs the same line to build a baseline image on such a host.
- **A container's `nofile` soft limit is 1024** (containerd's default OCI
  rlimit) while the daemon itself has 524 288, and the bare host has
  524 288. This is what made F6 a Docker-only blocker. The gateway now
  raises its own soft limit at start-up and clamps the in-flight ceiling
  by the descriptor budget (`d5e42c78`), but **S11 keeps an explicit
  "peak fds" check**: a deployment shape that pins the *hard* limit low
  is still the untested case.
- **On bare Linux, `LD_LIBRARY_PATH` must carry the venv's
  `nvidia/cudnn/lib`** or CTranslate2 (`whisper/tiny`, the grantless
  `none`-class model) SIGABRTs on load — `Unable to load any of
  {libcudnn_ops.so.9…}`. torch finds its own copy through RPATH; only
  CTranslate2 needs the variable, and the repo sets it on the ROCm path
  only. Shared with master, so not a PR regression, but S6's idle
  resident and S8's grantless leg need it in the scenario env (finding
  F5; it is set in `config/env.C*`).
- **`python/tests/__pycache__` busts the Docker `COPY` layer** — clean it
  before an image build or every build re-runs from that layer.
- **Docker 29 lists no `nvidia` runtime**; `--gpus all` and compose's
  `driver: nvidia` reservation both work through CDI/the hook.

Models (chosen per cost dimension, smallest first; all download on first
use except the two wd taggers already cached):

| Dimension | Model | Why |
|---|---|---|
| item / count | `tags/wd-vit-tagger-v3` (~350 MB), `clip/apple_MobileCLIP-S1` (~170 MB, two `run_with_oom_retry` passes), `clap/clap-htsat-unfused` (~600 MB, **no** impl-side OOM retry) | Fixed-size inputs; cleanest ramp; clap shows the harness-only OOM path. |
| token / max-times-count | `textembed/all-MiniLM-L6-v2` (~90 MB) | Bucketing by length; `bytes/4` pricing. |
| pixel / sum | `clip/nemotron-embed-vl-1b-v2` (~2.5 GB) | Variable resolution; pricing saturation above 1.8 MP. |
| pixel / max-times-count | `doctr/easyocr_standard_en` with `enable_batching` flipped on (C7) | The uniform-dims OOM trap the design was written for. |
| none | `whisper/tiny` (~75 MB) | Grantless path; a resident whose VRAM lands in `external`. |
| fixture | `inferio_custom/` copies of `oom_second_batch_impl.py`, `oom_impl.py`, `failbatch_impl.py`, `dying_impl.py`, registered through a user registry group with `metadata.cost` (shape in `manager.rs` test registry) | Deterministic OOM, batch-1 OOM, non-OOM failure and worker death on demand. Note: torch-free fixtures report no `gpu_uuid`, so on CUDA they ~~register only through the single-board fallback (C2) or~~ run unpriced; a fixture that touches CUDA (allocate one tensor at load) is needed for a priced fixture on C1. Build that variant in Phase 0. **Corrected in run1 (S5-cpu-C2): the single-board fallback is not a route for them** — `resolve_board` needs a `gpu_bdf` or a `gpu_total_mb` and a torch-free worker sends neither, so even on one visible board all four log *"dispatching this model without VRAM admission … boards=1"* and run unpriced (0 grants, 181 unpriced windows). The `*_cpu` family is the **unpriced-path** fixture; every priced fixture must touch CUDA. The CUDA variants and their registry live in `tools/calibration-protocol/fixtures/`. |

## 4. Scenario catalogue

**Revision after run1 (2026-09-03).** Every scenario below was executed
once on this host; the recordings are in
`tools/calibration-protocol/results/run1/<scenario>/` (git-ignored, with a
per-scenario `runlog.md` and an index at `results/run1/README.md`).
Paragraphs marked **Run1** are corrections to the scenario text: legs that
turned out to be impossible as written, criteria that are blocked on this
platform, and numbers that replace a guess. Where a criterion was wrong
rather than merely unmet, the original wording is struck through and the
replacement follows it.

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

**Run1:** PASS, and better than master — the same job in 22 s vs 24 s,
**0 OOMs vs 9**. B1 confirmed on all 51 grants (`mb = 0`,
`headroom_mb = 0`). Two corrections to what this scenario is for:

- **B14 must be restated, and this scenario cannot settle it.** S1
  measured `effective_margin = 0.25` (0.10 plus the never-confirmed 0.15)
  for its whole run and concluded that a single-model host never retires
  the surcharge. S2 then retired it at the **5th fit sample, 2.5 s into
  the job**, on the same single-model host. The surcharge is retired by
  accumulating fit samples, not by host shape, so the risk B14 names is a
  model that **never accumulates samples** — which is exactly what a
  squeezed board produces (S1's windows were clamped to 1 unit and never
  fitted). Record `effective_margin` *together with* `fit.samples`, or
  the number means nothing.
- The first load onto a never-sampled board is **unwarned**: the
  reservation is taken while the board still reads
  `external_known = false, headroom_mb = total`, so the "expected to need
  more VRAM than the board's remaining headroom" warning stays silent on
  the one occasion it exists for (finding F1, the load-path half of which
  is fixed in `8546cd63`). Expect the warning on every *subsequent* load,
  not the first. A model with no measured base reserves a flat
  `expected_base_mb = 4096` against real bases of 654–1 182 MiB.

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
(text corpus with lengths drawn from 40 B to 8 kB: ~~check bucketed
batches are length-homogeneous in the worker DEBUG log~~ and priced at
`bytes/4`).

**Run1.** All three models PASS on slope (+0.04 % / −0.29 % / +52 %
conservative) and base (0 % on all three), and G7 works: the job-driven
ramp reached `unit_budget = 512` with **957 items in flight**, so B16 is
cleared and the "expect the ramp to stop at 64" paragraph above no longer
holds for a server that publishes the header. Four corrections to the
scenario itself:

- **The MiniLM leg cannot be run as a job.** `.txt` is in no branch of
  `build_extension_set` (`jobs/files/mod.rs`), so a text corpus indexes
  **0 items** and the extraction job produces no record. Drive the token
  model through `loadgen.py` in text mode instead (or route it through
  the `ocr` tier behind a doctr pass). Run1 used `loadgen.py`.
- **"Bucketed batches are length-homogeneous in the worker DEBUG log" is
  BLOCKED for `sentence_transformers`.** At `INFERIO_WORKER_LOG_LEVEL=
  DEBUG` that impl emits **zero** bucket lines — only comparator
  retirements and clamps. The bucket contents the codemap promises are
  not on this impl's path, so either the line gets added or the criterion
  is dropped; do not record it as a failure of the feature. Bucketing
  *is* observable for easyOCR in S8, from `/health`'s `recent_batches`.
- **Inspect `knee_units` in the written store, not just in `/health`.**
  This is the scenario that first showed the knee estimator firing on the
  *sample rate* rather than the throughput curve: with large fixed-size
  `loadgen` requests it fitted `knee_units = 31` for MobileCLIP (real
  optimum 128, −19 % throughput) and `7` for wd-vit in ~20 s, and
  **persisted both** — while fitting no knee at all in the job-driven
  runs where a real knee exists (findings N1/T1). A knee is a hard
  ceiling, so a store poisoned this way caps the model on that GPU for
  good. Compare the fitted knee against the probe's throughput curve per
  model: a knee of 1 for wd-vit is *correct* (35.9 items/s at batch 1,
  36.1 at 2 048 — the curve is flat), so the check is "does the knee
  match the curve", never "is the knee small".
- **`sample_units` records the granted budget, not the batch actually
  run** (finding N8), so the stored pairs are not all measurements of
  their own label. Theil–Sen absorbed it here (slope error 0.008 %), but
  a check that reads `sample_units` as a batch size is reading the wrong
  quantity.

**W5 confirmed, and it belongs in this scenario.** Repeating S2 with
`PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` gives slope **32.19**
against **50.56** (**−36 %**) and base 932 vs 964, with an **identical
profile key**. A profile learned with the variable and used without it
admits ~2 986 items where the measured OOM boundary is 2 560–2 816.

**Judge "≥ 25 % of the boundary" against the probe's own conservatism**:
the linear fit under-shoots the true boundary by 17–36 % near the
ceiling, because the caching allocator releases cached blocks and retries
instead of growing (finding A2). Run1's wd-vit reached 20 % by admitted
units / 40 % by budget, and the binding constraint was neither memory nor
the knee but the **job's length** — one doubling per settled window, so
2 000 items buys 10 windows (finding N7).

### S3 Restart and resume
Restart C1 after S2; run a fresh `ramp` corpus for the same model.
Check: first window's `unit_budget` equals the persisted anchor (no
re-ramp from seed); `/api/inference/metadata` shows `calibration.status =
"local"`; `local_samples` continues from the stored count; the file's
anchor only ever increases. Then delete the file with the server
**stopped**, restart, confirm re-ramp from seed. Also delete it while the
server is running and observe whether the next debounced write resurrects
it (finding B3).

**Run1.** Resume PASS: `seeded_from_store=true`, three windows instead of
ten, anchor 512 → 1 024, `local_samples` 10 → 13, `calibration.status =
"local"`. Note that the *first* window is bounded by core's
`MIN_IN_FLIGHT_UNITS = 64` floor, so a resume takes two windows, not one.
Both probes came back with more than the plan expected:

- **B3 confirmed, with a nuance.** The file is back within ~20 s and no
  log line says so — but the profile is read at **worker admission**, not
  at process start, so a delete *before* the first load leaves
  `seeded_from_store=false` and what gets resurrected is a **new, worse**
  profile (anchor 32 where it had been 512, `local_samples` 4 where it
  had been 10, plus a spurious `knee_units = 7`). The write is a
  whole-profile **replacement**, not a merge (finding N6). Stop → delete
  → restart behaves as documented: `status = "uncalibrated"`, re-ramp
  from `seed_units = 8`.
- **B4 confirmed on all three legs, and the anchor is worse than a
  floor.** Editing the stored anchor to 2 048 is acted on immediately (a
  window of 1 853, no ramp, no OOM) and left at 2 048. With the anchor at
  4 096 *and* the slope poisoned to a quarter of truth, the slope
  **self-heals** by refitting over the stored samples with no new
  sample — but the anchor never does: a `unit_budget = 4096` first window
  OOMed, the worker's halving loop absorbed it (both requests HTTP 200,
  6 144 outputs, no death, −28 % throughput for that window), and the
  file afterwards still said **4 096**. So the anchor is a permanent
  floor that is never validated against `sample_units` and is **not
  lowered by an OOM at or below it** (findings N5/B4c). The OOM backstop
  is the only thing under it.

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

**Run1: 419/419 grants safe, 0 OOM, 0 worker deaths, 0 failed items, and
the fitted slope within 0.02 % of ground truth under every profile.**
Four things about *what this scenario is measuring* have to change:

- **The ledger is not the guard; the worker's per-batch clamp is.**
  `external_mb` is a **window-boundary quantity**, not a live one: it is
  refreshed only by a worker report at a window edge, and **zero** host
  probes ran in ~2.5 h of recording (`refresh_due`'s 10 s clock never
  fires, because the only path that can start a probe is a grant, and at
  grant time the worker's report has just refreshed the sample). Measured
  ages: **85.5 s** with a worker resident, 166.9 s overall; a +30 GB step
  took **31.5 s** to reach `/health`; a 53 GB spike lasting 10 s moved
  `external_mb` by 2 MiB. What kept every profile safe is the worker's
  own live NVML clamp, which reacted in **0.60–2.81 s** and fired 33
  times (finding T3). So S4b's "shrink observed in `healthrec` within
  15 s" criterion is measuring the wrong instrument: record **both** the
  ledger's latency and the clamp's, and judge safety on the clamp.
- **`limit` clamps to 0 below ~9 % free, and the last ~9.8 GB of any
  board is unusable by design.** `limit = total − external × (1 + margin)`
  charges the margin against the *neighbour's level*: at 12 GB free on a
  98 GB board the 10 % margin costs 8.58 GB, and at **8.9 GB free the
  limit reaches 0**, after which the ledger issues `mb = 0,
  unit_budget = 1` memory-blind grants (B1) — run1's S4d ran **5 512
  consecutive one-image batches** that way (findings T4/P5-2).
- **S4a's utilization must be judged against the boundary at the hog's
  free level, not the full-board boundary.** At 12 GB free the probe's
  boundary is **220 units**; run1 admitted 47 (21 %), and the 176 batches
  carrying 96.8 % of the job ran at **11 units (5 %)**. Against the
  full-board boundary the same run reads 0.40 and looks fine. State which
  boundary the number is against, every time.
- **Use a 16 000-item corpus for S4b–S4e.** The 2 000-item `ramp` corpus
  is exhausted in ~70 s — before S4c's t = 90 s event ever fires. Run1
  generated `ramp8` (`corpus.py --tier ramp --scale 8`, 16 000 × 1024²,
  1.9 GB) for exactly this. S4f is better driven by `loadgen.py` than by
  a job loop, so the model is never unloaded and the post-unload phantom
  cannot contaminate the tracking measurement.

Smaller results worth carrying: a squeezed grant did **not** squeeze the
in-flight target until `22eb33f9` (T5); grants **alternate** rather than
converge under steady pressure (425/425/906/370/370/892/257 at a pinned
`external_mb`, T9); the worker's clamp has no margin of its own and ran
at 98.2 % of free nine times without an OOM (T10); and S4g's B15 count is
**4 attempts**, `reqwest_retry`'s, after which the whole job aborts — not
one load per item.

### S5 OOM backstop and negative samples
~~C2 (single-board fallback lets torch-free fixtures register)~~ **the
CUDA-touching fixture variants on C1** (`tools/calibration-protocol/
fixtures/`, installed with `install-fixtures.sh`); the C2 leg is worth
running only as the **unpriced path**, since torch-free fixtures are
never ledger-admitted on a CUDA host (see §3):
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
  of memory" ~~(a corpus file literally named `out of memory.png` that the
  impl rejects)~~ must not deflate (finding B11). **The file-name vector
  does not exist**: the worker receives bytes over multipart and never a
  file name, so no file name can reach an error message. Probe it with
  the fixture `calibfixture/failbatch_oomtext_cuda` instead, whose
  `message` key puts the words in a plain `ValueError`.

**Run1.** The backstop works — every real fault was absorbed with no OOM,
no lost job and no unsafe grant — and three of this scenario's own
assumptions were wrong:

- **`oom_second_batch` OOMs forever, not once.** The shipped torch-free
  impl raises on *every* batch from the second on, which is `oom_impl`'s
  job. The CUDA fixture gained an `oom_batches` key (default 1) so the
  "one OOM then healthy" leg is expressible (`17e38e95`).
- **`scan_audio` defaults to `false`** in a fresh per-DB job config, so
  the first whisper attempt indexes 0 of 200 files. Set it before the
  grantless legs.
- The `poison` tier's truncated JPEG/PNG **decode fine** under Pillow
  (it tolerates 50 % truncation); the item that actually fails is the
  256 MP PNG, via `PIL.DecompressionBombError`.

Numbers this scenario produced: **B11 confirmed decisively** — a plain
`ValueError` whose text contains "out of memory" produced **15 negative
settles with `reason="oom"` on a board with 96 GB free**, while the same
failure worded differently produced zero. **B8 confirmed and quantified**
— `deflation` is an uncapped counter (**8 074 levels in 148 s**) that
repays one level per three clean windows (**7.04 levels/s** measured), so
a 2-minute fault costs ~15.6 minutes at **0.43×** throughput. **B7
confirmed twice**, including with a *learned* anchor: after `kill -9` the
respawn's first grant was byte-identical to the run's first and the
anchor was unchanged — the right behaviour on a discrete board. **B15
confirmed for the predict path and restated for jobs**: 93 load attempts
in 182 s, one per request, no backoff and no cap, against 4 attempts
inside a job (§S4g). And a shape the plan did not anticipate: the same
fault costs **100 % or 0 %** of requests depending on the client's
batching, because a single-request window goes to `run_single`, which has
no per-request fallback (276/276 lost vs 0/173, finding Q7).

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

**Run1.** The trim is fast and exact: flagged **1.837 s** after the
squeeze, a **5.8 ms** round trip, and the oracle saw the model's PID drop
by exactly the `slack_mb` the ledger named, inside one 250 ms sample. So
the "within 30 s" criterion is met by an order of magnitude, and the
useful number to record is the flag latency, not the deadline.

**B17 confirmed, but it fires at ~20 s, not at the 60 s
`TRIM_DEADLINE`.** With the `hang_trim_cuda` fixture (ignores the trim
for 70 s) the worker was taken down at **20.26 s** by a
shutdown/teardown path racing the hung trim (10 s `unload_grace` + 5 s
`terminate_grace` is the right magnitude), with `signal: 9 (SIGKILL)` and
the model dropped from all caches. The outcome B17 predicts is confirmed;
it simply arrives sooner. What the client sees is a bare
`{"detail":"Prediction failed"}` 500 after 18.6 s with no hint that
hygiene, not the model, failed; a predict 5 minutes later reloads
transparently in 1.9 s.

**B18 confirmed at 100 % of the load**: an 11.865 s cached nemotron load
stalled every in-flight wd-vit predict for **11.885–11.894 s** against a
p50 of 421 ms — **28×** — and `load_secs` is 600 s.

Two more results belong to this scenario. **Q6 resolves in the
with-neighbour case**: a grantless model's VRAM *does* land in
`external_mb` once anything refreshes the board (whisper's 678 MiB was
inside it); Phase 4's "invisible" reading was the never-refreshed case
only. And **the knee, fitted in the first 30 s of a contended run, pins
the model for the rest of it** — MobileCLIP sat at `unit_budget = 31` and
wd-vit at 15 for ten minutes, where S8 measured MobileCLIP reaching 128
on an idle board (P5-4, the N1/T1 family); `throughput_collapse` also
fires on neighbour contention rather than on batch size (P5-5). B21 is
**unreachable with shipped impls**: no impl defines `prepare()`, so
parked prewarm workers hold 0 MiB of VRAM and cost ~1.7 GiB of host RAM
instead.

### S7 Multi-GPU
C7. Pin `clip/apple_MobileCLIP-S1` to GPU 1; leave `tags` on the default
board. Drive both. Check: oracle shows each worker PID on the pinned
board only; two independent `vram[]` rows; a hog on GPU 0 changes only
GPU 0's grants; `PinDiverged` never logged. Then C3 (index-form
`CUDA_VISIBLE_DEVICES=1`): confirm the INFO line, no `vram[]`, unpriced
dispatch, and that the job still completes with registry defaults.

**Run1: isolation is exact.** Under a hog that took GPU 0 from 96 GB free
to 4 GB, GPU 1's ledger row was **byte-identical** at every sample
(`ext 775, limit 97 034, headroom 96 022`) while GPU 0's limit went
97 036 → 2 813 → 0; every worker PID appeared on exactly one board and
`PinDiverged` never logged. **B20's shape is right but its number is
wrong: the unpriced window is bounded by the registry's
`metadata.default_batch_size` — 64 for the `tags` group — and falls back
to the server-wide `default_max_batch = 32` only where a group declares
none.** Confirmed on two independent paths (C3, and S13's no-inventory
case). T5's open question stays open: no shipped configuration puts one
model's replicas on two boards, so a squeeze on one board clamping the
other's window could not be provoked; it needs a `replica_count > 1`
config spanning boards.

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

**Run1: the easyOCR acceptance test passes by 8.5×** — 54.72 s of
inference against master's 468.20 s on the same 460 segments (**8.02×**
by items/s), 0 errors, 0 OOMs, 0 collapse flags, `deflation` never left
0, and 37 size-homogeneous bucketed batches whose `peak_reserved_mb`
steps monotonically with the bucket (`1×5 2×1 3×10 4×1 … 122×1`).
Caveats to state in the report: **an idle board only** (Phase 1 saw
`deflation = 108` for this model on a *full* board), the winning run used
half of a 97 GB board, and the same bucketing on a 24 GB card is
untested. `slope_accuracy` is **not comparable** here unless the probe is
re-run with the C7 registry: the shipped registry has `enable_batching =
false`, under which easyOCR's memory is flat in batch size.

**W1 confirmed, and it is worse than a pricing quirk: pixel pricing is a
property of the corpus, not of the model.** Pricing is the raw header
`width × height`, so for a model that resizes to a fixed canvas the
*fitted slope* moves with the corpus — nemotron fitted **4.33×** the
probe's slope on a mixed corpus, and the 20 MP items, charged 66× a
thumbnail for ~1× the memory, forced **58 of 110 batches to hold a single
item** (utilization **0.08** of the probe boundary). The knee moves with
it too, since it is expressed in priced units. A profile learned on
thumbnails and used on scans is wrong in both directions.

**W2 is not reproduced and the comparator retires instead.** No
`throughput_collapse` fired in any of the 20 legs; the mixed-resolution
corpus produced *non-comparable* batches, and the worker logged
*"retiring the throughput comparator after 8 non-comparable batches"*
four times. The tier encodes JPEG rather than the PNG the plan names, so
a slower codec might still trip it — but on this evidence W2 retires as a
suspicion. Token pricing over-charges **8×** past `max_seq_length` and
under-charges CJK by 1.37×, which the model's own truncation makes
exactly right; neither direction produced an OOM (1 828 windows, all
clean).

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
C4 then C5 with the same S2 run. Check: ~~`base_method` is `free_delta` or
`alloc_delta` in C4 and `nvml` in C5~~ **`base_method` at load in both, as
a recorded per-platform fact**; `external_mb` still tracks a hog running
**on the host** (board-level NVML works without `--pid=host`); S4b and S4c
repeated in C4; the health check and the CI smoke script pass (including
the **403 on 6339**, which is Docker-only — see S14); `docker logs`
carries the worker lines; and **peak file descriptors and `ulimit -n`
inside the container**. Record the base error C4 incurs versus the oracle
(this decides the `pid: host` recommendation).

**Run1.**

- **B9 is refuted on this platform** and must stay a per-platform check,
  not an assumption: on driver 590.48.01 with the NVIDIA Container
  Toolkit, NVML inside the container resolves the worker's
  namespace-local pid, so `base_method` is **`nvml` in C4 *and* C5** and
  the base error is **0.00 %** either way. `pid: host` therefore buys
  nothing and costs PID isolation — **G2's answer is: do not add it to
  the shipped compose.** One grep decides it on a new platform:
  `base_method` at load, or the presence of the `NVML lists no process
  with pid` warning.
- **W4 stays open, and was not reachable here.** `free_delta` /
  `alloc_delta` were never exercised, so the fixed 500 MiB context
  estimate against a **measured 666–668 MiB** context is still untested.
  It is a live risk wherever NVML *does* hide the pid: older drivers,
  WSL, WDDM, ROCm containers, podman with a different toolkit.
- **The store key is portable bare ↔ Docker** on this host (`gpu`,
  `platform`, `backend`, `torch=2.7.1+cu128`, `dtype`, `base_mb`,
  `base_method` all identical) — because the image's venv comes from the
  same `uv.lock` with the same `cu128` extra.
- **The descriptor criterion is the one that failed, and it was the
  release gate** (F6): the container's `nofile` soft limit is 1024, each
  in-flight predict costs two sockets in one process, and a 2 000-item
  job drove the gateway to 983 sockets / 1024 descriptors, `status: -1`,
  1 849 items unprocessed, 1 240 `Too many open files` lines — where the
  master image finished 2 000/2 000 with a peak of 177. Fixed on the
  branch (`d5e42c78`); **keep the "peak fds" check** so the fix is
  re-verified per platform, and note that the plug-and-play leg passes
  only when the ramp never reaches ~128 units.
- Repeating S4b and S4c inside the container **passed, and faster than
  bare**: the +30 GB step was visible at **+1.2 s** against the bare
  host's 31.5 s (different window lengths, not a different mechanism).

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

**Run1: every stated criterion PASSED, and B19's number is a 2.94×
overcommit** — board total **64 137 MiB**, budget `limit_mb` **48 102
MiB**, cgroup `memory.max` **16 384 MiB**; nothing in the CPU path reads
`memory.max`. The death-negative path is indeed the only thing between
that user and a respawn loop, and **it converges only across job passes,
not within one**: five passes with a ~14 GB hog gave anchors 32 → 16 → 8
→ 4 and 4 deaths / 4 negatives / **14 spawns with no backoff**, each pass
losing a worker, reporting its remainder as `errors` while
`/api/jobs/data/failures` stays `{"total":0}`, and showing the user a
*completed* job that did a fraction of the work. Caveat for the report:
cgroup `oom_kill` stayed at **2**, so deaths 2–4 were *not* OOM kills —
the unified path treats **every** death as a memory negative, so an
unrelated crash also halves the anchor.

### S13 Probe robustness
C1 with `nvidia-smi` hidden from PATH (unpriced with a WARN, jobs work);
with a wrapper `nvidia-smi` that sleeps 6 s (5 s timeouts, 10 s backoff,
grants proceed on stale values, measure how many blocking threads pile
up under S6 load: finding B13); with a wrapper that prints a malformed
row (whole inventory unknown, WARN, unpriced).

The three wrappers are committed as
`tools/calibration-protocol/config/nvidia-smi-shims/{slow-all,slow-memory,
malformed}`. **The "hidden" case needs a `PATH` with no `nvidia-smi` at
all**, not a shim: `find_nvidia_smi` walks `PATH` and there is no config
key for the binary, so the recipe (in that directory's `README.md`) is a
scratch mirror of `/usr/bin` — 2 255 symlinks — with that one entry
missing, and `/usr/bin` dropped from the gateway's `PATH`. Nothing may be
left on a `PATH` the user inherits.

**Run1: PASS on every stated criterion, and B13 is refuted.** With a 6 s
shim under three models at concurrency 2 for 240 s: **2** host probes,
gateway threads **51–53** against a baseline's 52–53, **max 1** concurrent
`nvidia-smi`, and predict p50s within noise of the baseline. Forcing the
worst case (14 load/unload cycles, each departure stamp re-arming the
probe) gave **7** probes, threads 52–54, still max 1. Three reasons: the
refresh is single-flight per board, a failure backs the board off 10 s,
and **nothing polls** — the probe runs only from a grant or a load, and a
board with a live resident is kept fresh by the worker's own NVML
samples. Two residuals to keep: `capability::output_with_timeout`
**abandons** the timed-out child and its reader thread instead of killing
them (measured 1.04 s of overlap; a binary slower than the 10 s backoff
would accumulate one process and one thread per 10 s, finding F13), and
**`accelerator_report`'s own capability query has no timeout** — with
`slow-all` it waited the full 6 s and added ~11 s to boot, so a `slow-all`
run collapses into the "hidden" case at boot anyway (the boot inventory
probe times out at exactly 5.000 s).

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

**Revision after run1 (2026-09-03).** The last column is this host's
answer, with the number that decided it; the evidence is the runlog of
the scenario named in the third column, under
`tools/calibration-protocol/results/run1/`. Five entries changed the
protocol rather than merely answering it: **B9** and **B13** are refuted
*here* and stay as per-platform checks (§9); **B14**, **B15** and **B20**
were confirmed in shape but with a different mechanism or number than the
suspicion states; **B16** is cleared by the G7 fix; **B10**, **W3** and
**W4** are still unanswered and each names the leg that would answer it.

| Id | Suspicion (file) | Scenario | Status after run1 |
|---|---|---|---|
| B1 | A full board yields `mb = 0` grants; the worker's `clamp_to_live_memory` and `maybe_shrink` treat `grant_mb <= 0` as "no reservation", so pre-fit units are memory-blind exactly when the board is fullest (`ledger.rs:2879`, `packing.py:450`). | S1, S4c | **Confirmed.** 51/51 grants `mb = 0` on a full board (S1); 4 more at 8 GB free (S4d), where the ledger's own `unit_budget = 1` — 5 512 one-image batches — was the real cost, not the blinded worker. The worker's live clamp kept working throughout. |
| B2 | External refresh is `nvidia-smi` free/total only, grant-triggered, 10 s staleness, no poller: a quiet board's picture can be minutes old (`ledger.rs:865-880`). | S4b, S4d (latency numbers) | **Confirmed, and worse than stated.** `external_mb` is a *window-boundary* quantity: **0** host probes in ~2.5 h of Phase 3 recording, sample ages of 85.5 s with a worker resident and 166.9 s overall, a +30 GB step taking 31.5 s to reach `/health` and a 53 GB/10 s spike never appearing at all. True of busy boards with long windows, not just quiet ones (T3). |
| B3 | Deleting `calibration.toml` under a running server can be undone by the next debounced write; a corrupt file is silently replaced (`calibration.rs:586`, `:1219`). | S3 | **Confirmed**, with a nuance: the profile is read at worker *admission*, so a delete before the first load resurrects a **new, worse** profile (anchor 512 → 32, samples 10 → 4, a spurious knee). The write is a whole-profile replacement, not a merge (N6). |
| B4 | A persisted anchor is a permanent floor across driver/torch patch bumps matched by `major.minor`; the OOM backstop is the only protection (`ledger.rs:2295`). | S3 variant: edit the stored anchor to 4× and observe the first window | **Confirmed on three legs.** The stored anchor is acted on immediately and is a permanent floor: a poisoned slope self-heals by refitting, the anchor never does, and an OOM *caused by* the anchor did not lower it (N5). Never cross-checked against `sample_units`. |
| B5 | A shipped baseline knee can only ratchet down within a run (frontier guard) | Not testable until baselines ship; note only | Unchanged — **not testable** until a shipped baseline exists. |
| B6 | Index-form `CUDA_VISIBLE_DEVICES` silently disables the ledger at INFO (`gpu.rs:846`); Docker users commonly set it. | S7 (C3), S11 | **Confirmed**, one INFO line, exact wording in the S7-C3 runlog. The off-switch costs budgeting, not placement: the worker still ran on the physical board. |
| B7 | Discrete-board worker death relearns nothing; respawn is admitted at the anchor again (`ledger.rs:741`). | S5 | **Confirmed twice**, including with a *learned* anchor: after `kill -9` the respawn's first grant was byte-identical to the run's first, `seeded_from_store=true`, anchor unchanged. Right behaviour on a discrete board; cost 2 lost requests and 3.4 s. No `unified_board_death` negative ever appeared. |
| B8 | Deflation is unbounded and recovers one level per 3 clean windows; a few unfittable items can pin a model at 1-unit batches for a long time (`ledger.rs:725`). | S5 | **Confirmed and quantified.** Uncapped: **8 074 levels in 148 s** (54.6/s). Recovery: one level per three clean windows, **7.04 levels/s** measured → a 2-minute fault costs ~15.6 min at **0.43×** throughput. Every deflated grant still offered 96 GB of an empty board. |
| B9 | Container without `--pid=host`: base falls to `free_delta`, contaminated by concurrent activity in the load window (`memory.py:403`). | S11 | **Refuted on this platform.** Driver 590.48.01 + NVIDIA Container Toolkit: NVML resolves the namespace-local pid, `base_method = nvml` in C4 *and* C5, base error **0.00 %** either way. Keep it as a per-platform check (§9), never as an assumption. |
| B10 | Post-trim fit samples use a stale `reserved_at_load`; Theil–Sen may refuse and keep an old fit forever (`ledger.rs:3286`, `:3378`). | S6 (trim) then S2 re-run | **Not exercised.** No leg produced a post-trim fit sample against a stale `reserved_at_load`; the S6 trim legs ended in the model being dropped (B17) or the run ending. Still open, and it needs the S2 re-run after a trim that the plan calls for. |
| B11 | `message_reports_oom` matches any line containing "out of memory" (`ledger.rs:4119`). | S5 | **Confirmed decisively.** A plain `ValueError` whose text contains "out of memory" produced **15 negative settles with `reason="oom"` on a board with 96 GB free**; the same fault worded differently produced zero. Probe it with `calibfixture/failbatch_oomtext_cuda` — the `out of memory.png` file-name vector does not exist (the worker gets bytes, never names). |
| B12 | No log line for grant issued, batch chosen, negative applied, ramp step, refresh result or store write; `/health` polling is the only reconstruction path. | Every scenario; see G1 | **Cleared.** The `49822c8b` lines reconstruct a whole run; `analyze.py` is built on them. |
| B13 | A wedged `nvidia-smi` costs a 5 s blocking thread every 10 s per board. | S13 | **Refuted, with numbers.** 2 host probes in 240 s under three-model load (7 in a forced churn case), gateway threads 51–54 with and without a wedged binary, max **1** concurrent `nvidia-smi`, latency within noise. Single-flight per board + a 10 s failure backoff + **nothing polls**. Residual: the timed-out child is abandoned rather than killed (F13). |
| B14 | Single-model hosts never confirm the +0.15 unconfirmed margin (`ledger.rs:3346`). | S2 (record effective margin) | **Restated.** The +0.15 surcharge is retired at the **5th fit sample** (2.5 s into a job) on the same single-model host S1 said could never retire it. The risk is models that **never accumulate fit samples** — a squeezed board, whose windows are clamped to 1 unit — not host shape. Record `effective_margin` with `fit.samples`. |
| B15 | Worker death → immediate respawn on the next predict with no backoff or attempt cap (`manager.rs:1068-1085`, `:1152-1197`); a job of N items can pay N loads before failing. | S5 | **Confirmed on the predict path, restated for jobs.** `dies_on_load` gave 93 load attempts in 182 s, one per request, **no backoff, no cap**. Inside a job the same condition stopped after **4** attempts / 259 s — and that cap is `reqwest_retry`'s three retries, not the manager's; the load failure aborts the whole job. "A job of N items can pay N loads" was not reproduced. |
| B16 | `REQUEST_UNIT_BUDGET = 64` caps every item/count window a job can produce, so anchors and knees learned from jobs never exceed 64 regardless of VRAM (`jobs/extraction.rs:65`). | S2 | **Cleared by G7.** Job-driven in-flight items reached **957** with `unit_budget = 512`; the floor of 64 was left behind at window 7. Jobs now calibrate *better* than fixed large `loadgen` requests (which feed the knee estimator instead). |
| B17 | `TRIM_DEADLINE = 60 s` is fixed and fatal for the whole model (`worker.rs:113`, `dispatch.rs:803`). | S6 | **Confirmed, and it fires at ~20 s, not 60 s.** The worker died at **20.26 s** into a hung trim — a teardown path racing the trim (`unload_grace` 10 s + `terminate_grace` 5 s), not the `TRIM_DEADLINE`. Client got a bare 500 after 18.6 s; a later predict reloaded transparently (P5-7). |
| B18 | The manager's load lock is taken on every predict (`manager.rs:1161`), so any load (up to `load_secs`) stalls every model. | S6 | **Confirmed at 100 % of the load.** An 11.865 s cached load stalled every in-flight predict for **11.885–11.894 s** — **28×** the 421 ms p50 — and `load_secs` is 600 s (P5-3). |
| B19 | The CPU board reads host RAM and is cgroup-blind (`cpu.rs`). | S12 | **Confirmed, with the number.** Board total 64 137 MiB, budget 48 102 MiB, cgroup `memory.max` 16 384 MiB = **2.94× overcommit**; nothing in the CPU path reads `memory.max`. The death-negative path converges (32 → 16 → 8 → 4) but only **across** job passes, and treats every death — not just an OOM kill — as a memory negative. |
| B20 | The unpriced path bounds windows by `default_max_batch` (32) rather than seed-sized batches as the design says (`dispatch.rs:37-48`). | S7 (C3), S13 | **Confirmed in shape, corrected in number: the bound is the registry's `metadata.default_batch_size` (64 for `tags`), not `default_max_batch` (32),** which applies only where a group declares none. Seen on two independent unpriced paths (C3 and the no-inventory case in S13). |
| B21 | Prewarm-parked workers are invisible to the ledger; a `prepare()` that initialises CUDA becomes margin-inflated external usage. | S6 (compare `external_mb` with and without prewarm) | **Unreachable with shipped impls.** No impl defines `prepare()`, so parked prewarm workers never initialise CUDA and hold **0 MiB** of VRAM; the pool's real cost is ~1.7 GiB of host RAM (~400–455 MiB RSS each). Re-test if a CUDA-touching `prepare()` ever ships. |
| B22 | `/health` renamed `last_effective_cap` to `last_grant_units`; check the UI submodule and any consumer. | S14 | **Cleared.** `models[].last_grant_units` is present and populated and nothing in the run read `last_effective_cap`. (Unrelated follow-up: the `ui` submodule's generated types need `npm run gen:api` before release, because G7 added a response header.) |
| B23 | `accelerator_backend(Auto)` keys profiles as `cpu` while the probe behaves as CUDA on the validation-failure path (`http.rs:271-278`). | S1 (inspect the `backend` key written to `calibration.toml`) | **Cleared.** The written profile carries `backend = "cuda"`. |
| W1 | Pixel pricing uses raw submitted dimensions, not the model's canvas (`packing.py:300`). | S8 | **Confirmed, and it is more than a pricing quirk:** the *fitted slope* becomes a function of the corpus (nemotron fitted **4.33×** the probe's on a mixed corpus), utilization fell to **0.08** of the boundary, and 58 of 110 batches held a single item. The knee moves with it, since it is expressed in priced units. |
| W2 | Measurement brackets CPU decode time, so slow-decoding inputs can trip the 0.4 collapse ratio spuriously (`packing.py:622`). | S8 (20 MP PNGs), S4e | **Not reproduced** in 20 legs. The mixed-resolution corpus **disarms** the comparator (4 × "retiring the throughput comparator after 8 non-comparable batches") rather than fooling it, and `throughput_collapse` never fired on decode time. Retires as a suspicion on this evidence; the tier encodes JPEG, so a slower codec is untested. |
| W3 | After an absorbed OOM the throughput comparator is not reset, so the regrowth batch may be flagged as a collapse (`packing.py`, `utils.py`). | S5 | **Open.** Nothing in run1 produced an impl-side absorbed OOM under real pressure: the fixtures' batches take 0.5 ms and the one real failure was a decode bomb on an empty board. Needs a leg that OOMs MobileCLIP for real (hog + large batch), i.e. S4c-style pressure. |
| W4 | `alloc_delta` uses a fixed 500 MiB context estimate; Blackwell contexts may exceed it. | S11 (C4 base error) | **Open, and unreachable on this platform.** The degraded base tiers were never entered (see B9), so the fixed 500 MiB context estimate against a **measured 666–668 MiB** context is still untested. Live wherever NVML hides the pid: older drivers, WSL, WDDM, ROCm containers, podman with another toolkit. |
| W5 | Ambient `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` changes what `memory_reserved` means and nothing pins or reports it. | S2 repeated once with that env set | **Confirmed.** `expandable_segments:True` gives slope **32.19** vs **50.56** (**−36 %**) and base 932 vs 964 under an **identical profile key**; a profile learned with it and used without admits ~2 986 items where the boundary is 2 560–2 816. |

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

| Phase | Content | Needs | Run1 actual |
|---|---|---|---|
| 0 Prep (~2 h) | Release build of the branch; master worktree build with its own venv; test-group sync; write the tools in §2; generate the corpora; **oracle calibration** against known allocations; CUDA-touching fixture impl. | Nothing stopped yet. | Split into three parallel tracks (A feedback signal + verifier, B tooling, C environment). The GPU half of the oracle calibration had to be **deferred to Phase 2a**, because SGLang held 95 GB of each board. |
| 1 Smoke (~1 h) | S0, S1 (with SGLang running), S14. | — | ~1 h. `cargo test --release` 335 s wall (241 s of tests), pytest 9 s, S1's job 22 s, six S14 model jobs. |
| 2 Idle-board truth (~3 h) | Stop SGLang. `ceiling_probe` for the four models. S2 for three models, S3. | SGLang down from here. | Split in two: **2a** (stop SGLang, oracle 10/40 GB on both boards + 16 GiB RAM, bisect smokes, five ceiling probes) ~2 h, and **2b** (S2 ×3 + the W5 and loadgen legs, S3 + B3/B4) ~1 h. A refinement probe near wd-vit's boundary costs 60–170 s, which is why `--bisect-start` / `--bisect-budget` exist. |
| 3 Pressure (~4 h) | S4a–S4g. | — | ~2.5 h of recording for seven profiles — but two are fixed-length by construction (S4e 10 min, S4f 40 min), so the phase cannot be compressed much below that. |
| 4 Faults and dimensions (~3 h) | S5, S8. | — | **20 legs in 1 h 03 min**, because the fixtures fail in milliseconds. The long pole is the easyOCR baseline (C0 takes 468 s where C7 takes 55 s). |
| 5 Concurrency (~2 h) | S6, S7. | — | **5 legs in 32 min.** |
| 6 Deployment (~4 h incl. image builds) | S10, S11, S12. | Docker builds. | ~3.5 h. Image builds with a warm layer cache: `calib-cuda` 349 s / 9.43 GB, `calib-cpu` 54 s / 3.49 GB, master baselines 285 s and 118 s. Rebuild the images at the tip if product commits landed since. |
| 7 Robustness and self-test (~3 h) | S13, S15. | Throwaway branches. | Split: **7a** (S13's three cases + the F8 SIGKILL root-cause investigation the earlier phases had deferred) ~1 h; **7b** (S15 mutations, plus the re-run legs the fixes require) was still to run when this revision was written. |
| 7a F8 investigation | Root-cause the unexplained worker SIGKILLs seen from Phase 5 on: gateway under `strace -f -e trace=clone,clone3,exit,exit_group,kill,tgkill`, a standalone C control for the kernel behaviour being suspected, and negative controls that suppress the suspected trigger. | A reproducer that dies within minutes (here: a cold board + an oscillating hog). | Found it: `PR_SET_PDEATHSIG` is **thread**-scoped and the load-path `block_in_place` demotes the forking thread into Tokio's blocking pool, which reaps it after 10 s idle — 8/8 deaths Δ 1–3 ms from the forking thread's exit (**F11**). Budget a phase like this whenever deaths have no traceback and no kernel OOM. |
| 7b Re-run legs after fixes | Every fix landed mid-run invalidates the legs that ran before it. Re-run: the F8 leg (expect 0 deaths, 16 000/16 000), a cold S2 ramp, and the S11-C4 job on an image rebuilt at the tip (F6 closure: 2 000/2 000, peak fds well under the limit). Then S15. | A release build at the tip. | Pending at the time of this revision. |
| 8 Soak (8–12 h) | S9. | Overnight. | Pending; `vramrec.py --interval 1` (the 4 Hz default writes ~1.5 GB per 12 h). |
| 9 Report (~1 h) | Aggregate verdicts, findings list with severity, portability notes, recommendation. Restart SGLang. | — | Pending. Add: revise this plan from the run (§0/§4/§5/§7/§9), and index the results directory. |

**Run1's own lesson about phase order.** Every product fix landed by an
earlier phase invalidates the legs that ran before it, so the plan needs
an explicit re-run budget (phase 7b above), and every runlog must record
**which binary commit it ran on** — not just the repo commit, since the
release binary is often several commits behind the tree. Run1's phases
ran on `10be7442` (1), `918ec170` (2b, 3), `d17e3854` (4), `443fa088`
(5), `eb398d69` (6) and `b9d32324`/code `c8d64a5a` (7a).

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
  - **Amended 2026-09-03 (Phase 6, finding F6): the ceiling is also
    clamped by the process's file-descriptor budget, and the gateway
    raises that budget at startup.** With local inference every in-flight
    predict is loopback HTTP from the gateway to a listener in the *same*
    process, so it costs two sockets in one descriptor table. Nothing in
    the ramp knew that: in the shipped Docker image, whose `nofile` soft
    limit is containerd's default 1024 (hard 524 288), a 2000-item job
    drove the gateway to 983 sockets / 1024 descriptors, `accept` started
    failing with `EMFILE`, SQLite could not open its files, and the job
    ended `status: -1` with 1849 items unprocessed; the same job on the
    master image peaked at 177 descriptors and finished 2000/2000. Two
    changes, both on this branch. (i) `main` raises the soft
    `RLIMIT_NOFILE` to the hard limit before the tokio runtime is built
    (`panoptikon/src/rlimit.rs`), so every thread and child process
    inherits it; the outcome is logged at INFO once logging exists, at
    WARN if the raise is refused, and a soft limit already at the hard
    one is a silent no-op. This alone fixes the container, which had 524
    288 descriptors available all along. (ii) `in_flight_unit_ceiling`
    gains a third term for the case where the *hard* limit is also small:
    `by_fds = (soft_nofile - FD_RESERVE) / FDS_PER_IN_FLIGHT_ITEM` with
    `FDS_PER_IN_FLIGHT_ITEM = 2` (the client socket plus the accepted
    server socket) and `FD_RESERVE = 256` (the ~50 non-window descriptors
    master held at 177 total, with margin for more databases, listeners,
    worker replicas and decode subprocesses), and the ceiling becomes
    `min(max(by_budget, by_loaders, 64), max(by_fds, 64))` — the
    descriptor term is a **cap** on the other two, not a third term to
    maximise with, because descriptors are the one resource a job can
    exhaust process-wide rather than merely oversubscribe. The floor of
    64 still wins under it (it is a deadlock bound), with a WARN naming
    the limit when the budget is below `64 x 2 + 256 = 384`. On a host
    whose limit is 524 288 the term is not binding and the shipped
    defaults are unchanged (4096 units). On non-Unix there is no
    `RLIMIT_NOFILE` to read and the term is fed a sentinel that can never
    bind. **Not changed:** the server-side header derivation — the
    orchestrator cannot know a remote caller's descriptor budget, so
    bounding the figure by one's own is the caller's job (stated in
    `inferio-worker-protocol.md`). **Considered and not done:** a
    server-side accept guard, because `axum` 0.8 already logs an accept
    error and backs off for one second rather than giving up
    (`serve::listener::handle_accept_error`) — the Phase 6 symptom
    "axum stopped accepting" was that backoff, and the damage in-process
    was SQLite's, which the descriptor clamp is what prevents.
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

### Per-platform checks run1 added

Each of these is a one-line check that run1 found to be **platform- or
deployment-specific** — a fact that cannot be carried over from this
host, and that silently changes what a later scenario is measuring:

1. **`base_method` at load** (`grep 'base_method' `, or the presence of
   the `NVML lists no process with pid` warning). It is driver- and
   container-runtime-dependent, not container-shape-dependent: B9's
   predicted degradation did not happen here, so the *degraded* tiers —
   and with them W4's fixed 500 MiB context estimate against a measured
   666–668 MiB context — are **untested everywhere so far**. A platform
   that reports `free_delta` or `alloc_delta` is the one that finally
   tests W4, and its base error must be recorded against the oracle.
2. **The container's `nofile` soft limit** (`docker exec … ulimit -n`)
   and the gateway's **peak descriptor count** during a job of at least
   ~1 000 items. containerd defaults the soft limit to 1024 while the
   daemon itself has 524 288; the branch now raises its own soft limit to
   the hard one at start-up and clamps the in-flight ceiling by the
   descriptor budget, so what a new platform is really checking is
   whether its *hard* limit is also small (podman, a hardened image, a
   systemd unit with `LimitNOFILE=`, macOS's much lower defaults).
3. **`UV_LINK_MODE=copy` for the image build on a host whose Docker
   storage sits on ZFS** (or any filesystem where uv's reflink fails with
   `os error 11`). Now shipped in the `Dockerfile`; a scratch copy of an
   *older* Dockerfile — a master baseline image, say — still needs the
   line added by hand.
4. **The PDEATHSIG/thread hazard is Linux-only, but the fix is not.**
   `PR_SET_PDEATHSIG` fires when the *forking thread* exits, so a worker
   forked from a thread that Tokio may later reap is killed ~10 s later
   (F11). macOS and Windows have no PDEATHSIG, so that failure class does
   not exist there — but the spawner change (fork every worker from a
   thread that cannot exit) applies everywhere, and any platform pass
   must still watch for **worker deaths with no traceback and no kernel
   OOM**, which is the signature. On Linux, `strace -f -e
   trace=clone,clone3,exit,exit_group,kill,tgkill` and a check of who
   forked the dead pid is the diagnostic; on other platforms the
   equivalent is whatever names the killer.
5. **`LD_LIBRARY_PATH` for CTranslate2 on bare Linux.** `whisper/tiny`
   (the grantless `none`-class model, used by S6 and S8) SIGABRTs on load
   unless the venv's `nvidia/cudnn/lib` is on the loader path; torch
   finds its own copy through RPATH, so only CTranslate2 needs it and the
   repo sets it on the ROCm path only. A platform whose wheels bundle
   cuDNN differently — or a distro package — will not need it; record
   which.

Two further per-platform facts that are cheap to record and were needed
for adjudication here: the **CUDA context size** (`context_mb` from
`oracle_calibrate.py`; 666–668 MiB on this driver, and the term
`base_mb` under-states by ~120 MiB until the first kernel launch), and
the **`nvidia-smi` vs torch total** disagreement (97 887 vs 97 250 MiB
here, 0.7 %, inside the ±5 %/512 MB sample-vs-board check but the kind of
number a threshold gets written against by accident).
