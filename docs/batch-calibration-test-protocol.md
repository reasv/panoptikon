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
   host first, then repeat a subset per platform (§9).
2. **Ledger logging first** — done, `49822c8b` (§8 G1).
3. **The 64-item in-flight ceiling (G7) gets the principled fix**, the
   feedback signal, before the run — not a constant bump (§8 G7).
4. **Subagents do all execution** (tools, scenarios, fixes, reviews); the
   main session orchestrates, decides and adjudicates only.
5. **Fix policy.** Low-discretion fixes are made, then reviewed by a
   separate verifier subagent, iterated and reported. Anything that changes
   a feature, a default, user-visible behaviour or a design decision is
   written up with options and left to the user.
6. **Tools live in `tools/calibration-protocol/`** on the PR branch, so they
   ship for the other platform passes.
7. **SGLang** is used as-is for the "GPU full" case, then stopped and
   restarted with `docker compose` in `~/docker/dsv4flash`; nothing of the
   user's is edited, and other resident models there may be used for
   intermediate VRAM levels via copied compose files.
8. **Master worktree baseline** with its own venv and data dir.
9. **Throughput as a secondary calibration target** is post-run work (§8b).
10. **Plan and code map are committed** on the PR branch.

### Standing host rules

`~/docker` is the user's: SGLang and the other deployments there are used
as-is and never edited, and `~/docker/inferio/.env` (a real API key) is never
read into a log or a report. The master worktree baseline lives at
`/home/admin/projects/panoptikon-master` with its own venv and data dir.

### Decisions taken by the user after run1 (2026-09-04): the run2 change set

The user reviewed the run1 report and approved the following. Rule stated
for all of them: "the most thorough, robust, principled solution always".
Each is implemented by an Opus agent on its own commit(s), reviewed by a
separate verifier, then the affected scenarios are re-run (run2) and the
report gains a run2 section.

| # | Change | Decision and design |
|---|---|---|
| R1 | Throughput knee (T1/N1/P5-4/F-A) | (a) exclude throughput samples from windows the ledger knows were squeezed, memory-blind or clamped by the worker; (d) the knee is a brake that expires after N clean windows at the cap with ample headroom (re-widening), not a permanent ceiling; plus the user's extra failure mode: tag every throughput sample with the GPU's concurrent-active-worker count and fit the knee (and judge throughput collapse, P5-5) only from samples taken while the model was the sole active occupant. |
| R2 | Death blast radius (F7) | Re-queue the in-flight items of a died-on window once instead of recording them as errors; a job whose items still fail after that is reported partial, not completed. Also fix the pre-existing empty `/api/jobs/data/failures` (Q8/T8): failed items and failed jobs must appear there. |
| R3 | OOM classification (Q1) | Structural signal: the worker catches typed OOM exceptions (CUDA, HIP, MPS, CPU allocator) and reports an explicit flag; the host trusts the flag; string matching stays only as a fallback restricted to driver-shaped messages and deflates only when the worker's live free-memory reading at failure time confirms memory was tight. |
| R4 | Deflation (Q2/B8) | Cap the counter at log2(anchor) + 1 (beyond that it is a no-op); repay by time as well as by clean windows; clear on respawn. User note: approved, though "beside the issue" (the classifier is the root). |
| R5 | Margin near a full GPU (T4/P5-2) | The margin exists so a desktop user's variable VRAM use does not spill into ours; a user-set margin is honoured as today. **Default rule:** reserve = min(external × margin, 1024 MiB), so at most 1 GB is ever withheld from the budget; `limit = total − external − reserve`. Orchestrator's call, approved by delegation: the worker reports free memory per batch (not per window), so `external_mb` stops being a window-boundary quantity (T3). |
| R6 | Load lock (B18/P5-3) | Per-model locks with a separate device-admission gate; the global load lock is retired. |
| R7 | Pixel pricing (W1/Q3/F-B) | Implement the design's deferred per-item cap: price each item at min(raw pixels, canvas pixels), canvas declared per model in the registry (fallback: the model's known input resolution); makes the slope corpus-independent and lets large images pack. |
| R8 | Degraded base tier (W4/F9) | Measure the CUDA context (GPU free before/after first CUDA init) instead of the 500 MiB constant; record `base_method` in every platform pass. |
| R9 | Load-failure backoff (B15/Q5) | Per-model cooldown after consecutive load failures, exponential and capped; predicts fail fast with "unavailable until"; jobs abort on the first one. |
| R10 | Socket cost (F6 option 4) | **Withdrawn 2026-09-04 and replaced by R10'.** In-process dispatch only helps the co-located case and creates a second path; real deployments run the gateway and the inference server on different machines (the user's NAS talks to this GPU server). **R10':** keep the single HTTP client path and multiplex requests over a small bounded connection pool using HTTP/2 cleartext (h2c) between gateway and inference server, with automatic fallback to HTTP/1.1 for a server that does not speak it; the descriptor clamp then bounds by pool size (HTTP/1.1 fallback keeps the per-item term). Uniform for local and remote; the existing clamp stays as defence in depth. |
| R11 | Small choices (orchestrator's calls, delegated) | Store `dtype_method` in the profile; rename the dtype sentinel to `unstated`; do not add `pid: host`; regenerate the UI types from `openapi.json`. |
| R12 | Verification | Nemotron's 4.5× base at admission (F-C) is checked as an analysis artefact (oracle sampled while weights streamed) before any change. |

### Run2 status, by milestone

| Milestone | Date | State | Where the numbers are |
|---|---|---|---|
| **Run2 handoff** | 2026-09-04 | The four implementation tracks (L, M, E, P) were briefed and restarted from `910378ff` after a compaction. User corrections to the R table above: R1 gains a **bucket-variance filter** — no permanent and no persisted knee without honest, quiet samples, since an external load can taint the ring undetectably and the bound has to come from exclusion, expiry, the contention tag and the filter together; F-C is a mid-load oracle reading, note only; R10 withdrawn for R10′ | This section's R table |
| **Implementation done** | 2026-09-04 | All four tracks, their verifiers, a cross-track follow-up (R7 was inert without it) and an integration pass landed: **82 commits**, `0d6b36c5` → `65fd2f82`. Suites green bar the two known host artefacts | run2 report §3 (per-R row, with commits and verifier verdicts) and §2 (integration pass) |
| **Phase A, C, D1 and the probes** | 2026-09-04 | Ran on `65fd2f82` and found **eleven defects** (P1, P2, F1, S1/S1b, C2, C4, C5, D1-b, easyOCR's int32 ceiling, the ledger shape ceiling, the typed transport failure). All eleven fixed, verified and integrated — a further **70 commits**, `65fd2f82` → `34a591aa` | run2 report §4.1–§4.4 (per-leg criteria) and §5 (per-defect root cause, fix, verifier, leg expectation) |
| **Phase A′ and S4a** | 2026-09-04 21:44–21:58 | Ran on the rebuilt binary `34a591aa` on the **unmodified** `server-C1.toml`, both GPUs at 2 MiB before every leg. **All three PASS on every stated expectation**, closing P1, P2, F1, S1 and R5 by measurement rather than by test. Two findings, neither a defect: the worker's live clamp now double-counts our own allocator pool, and S3's throughput is 0.88× master at exact run1 parity | run2 report §4.5 and §4.6 |
| **Blocked** | 2026-09-04 21:59 → 2026-09-05 05:13 | The user recreated SGLang (77 702 MiB on both GPUs), so S4b, S4d, S4g, Phase D and Phase E stood **blocked, not failed** for a day: running them against a live SGLang would have been invalid *and* a risk to the user's service | run2 report §4.5, §4.7, §4.8 |
| **Deslopping** | 2026-09-04 → 2026-09-05 | The user reviewed the PR size (+86 051 lines, ~19 000 of them product code) and asked for a comment/test/doc concision pass and an assess-first approach to code bloat first. Phase 1 landed (whole-PR insertions 86 255 → 72 594, production tokens byte-identical); phase 2's candidate list awaits the user's picks | `docs/batch-calibration-deslop-plan.md` |
| **Phase 3 legs measured** | 2026-09-05 05:13–10:18 | The user authorised stopping SGLang; all seven remaining legs ran on the deslopped tip binary `63a8ad73` (image `5b2b3ed166c0` for S11-C4) and SGLang was restarted at the end. **S4g, S4d, S8 pixmix, S11-C4 and the 4 h soak PASS; S4b PASSes but its "within a few seconds" latency is bounded by the in-flight window; the shape ceiling is unreachable through a job on a 98 GB GPU.** No product defect in any leg; two tool fixes (`555213ac`, `176f6ad9`). Three items now need the user (report §6): S4b's expectation, the ceiling's reachability, and the soak's `oracle_agreement` (14.12 % → 13.53 % only) | run2 report §4.5, §4.7, §4.8 (per-leg expectation tables) and §6 |

**Artefacts the legs ran on.** Phases A/C/D1 on `65fd2f82`; Phase A′ and S4a on
release binary `34a591aa` (86 313 672 B, built 21:28:07 UTC) with image
`panoptikon:calib-cuda` `0b2261f94c8f`; **Phase 3 on `63a8ad73`**
(`/home/admin/projects/panoptikon-wt/build-target/release/panoptikon`,
**86 303 120 B**, built 2026-09-05 04:53 UTC, `panoptikon 0.1.8`) — the last
commit on the branch that touches code, so it is the tip's code — with image
`panoptikon:calib-cuda` = **`5b2b3ed166c0`** (built 04:59 UTC from a clean
detached worktree at the same commit). `0b2261f94c8f` is retagged
`panoptikon:calib-cuda-run2b`, beside `-run2a` (`6fe5d86e3a1e`) and `-run1`
(`2a2c93ad6375`); the CPU and master images are still run1's. Nothing is
pushed, and the `ui` submodule commits `9b28044` and `8abf631` are unpushed, so
a clean-worktree image build needs this host's clone as a remote. The
`calib_hostless` workaround is gone from every calibration config
(`ea59a63b`), so **every leg from Phase A′ onwards ran on the shipped policy
shape and recorded that job creation works** — including S11-C4, on the shipped
`docker.toml` inside the image.

**Open items carried into the remaining legs.** The three `easyocr_*` ids
still ship `enable_batching = false`, so their worker takes the grantless path
and fits no slope — S8-ocr-C7 measures the grant, not a fit, until that flag
is flipped. All seven shipped `pixel` ids carry `metadata.cost.epoch = 2`
(`doctr/dots_ocr`, the three `doctr/easyocr_*`,
`clip/qwen3-vl-embedding-{8b,2b}`, `clip/nemotron-embed-vl-1b-v2`), because a
canvas re-denominates what one unit *is*: every run1 profile row for them is
ignored rather than migrated, so a leg on those models starts from an empty
profile even on this host.

### The sequence as it ran (2026-09-05), and what is left

Stopping SGLang is the user's decision, never a leg's; the user gave it for
this phase and the service was stopped 05:13 and restarted 10:18 UTC.

1. **S4g, S4d, S4b** on `63a8ad73`, seeded from
   `results/run2/S2-wdvit-v2/calibration.after.toml` (anchor 439, no knee),
   S4d/S4b over the **`ramp8`** corpus — `results/run2/S4g-v2`, `S4d-v2`,
   `S4b-v2`; all PASS (report §4.5).
2. **Phase D** — S8 pixmix (fit +0.07 % off its own group's probe, the 20 MP
   items packed 46 and 27 to a window), S11-C4 (image `5b2b3ed166c0`, shipped
   `docker.toml`, 80 fds / 37 sockets in the container) and the easyOCR C7
   ceiling leg (**NOT REACHED**: the ledger prices the 28-item batch at
   272 313 MiB against 94 070 measured) — `results/run2/S8-pixmix`, `S11-C4`,
   `S8-ocr-C7-ceiling` (report §4.7).
3. **Phase E** — the 4 h S9 soak on C1 over the `soak` corpus, 29/29 jobs
   `completed`, 0 deaths, no persisted knee — `results/run2/S9` (report §4.8).
4. **Left to do**: the user's decisions (report §6 options table, now including
   S4b's expectation, the ceiling's reachability and the soak's
   `oracle_agreement`), the phase 2 deslop picks, pushing the two `ui`
   submodule commits, and the Nix UI pin at release time. **No host step
   remains.**

### Decisions taken during run1 by the orchestrator (2026-09-03)

The calls made under decision 5 during run1, with the commit subject each
landed under and why it was low-discretion. Everything else run1 found is a
finding with options — every threshold, default and policy question included.

| # | Fix (commit subject) | Why low-discretion |
|---|---|---|
| 1 | *Force uv copy link mode in the Docker build so it works on ZFS-backed hosts* (`5b7c7353`), comment fix *Say why the Docker build pins uv's link mode, and that it outlives the build* (`3cff5c8c`) | The shipped `Dockerfile` could not build at all on this host (uv's reflink on overlay2-over-ZFS, `os error 11`). One `ENV`; no runtime behaviour changes; the alternative was to test an image the shipped file cannot produce. |
| 2 | *Infer a model's dtype and log why a calibration update is skipped* (`58dda519`, verified; design-doc clarification `918ec170`) | Phase 1's headline: five of five shipped models report no `dtype`, so `queue_profile_update` bailed and `calibration.toml` was **never written** on any host. The design says the profile persists; the store key's meaning is unchanged (a resolved dtype is inferred from the loaded weights, with a `dtype_method` field saying how), and every early return now logs once per (model, GPU, reason). Flagged to the user as a reversible judgement: **D1** store `dtype_method` in the profile? **D2** sentinel name `"unknown"` vs `"unstated"`. |
| 3 | *Do not reattribute a departed worker's VRAM to external usage* (`d17e3854`; verified fix-up of `cc78394a`) | N4: after an unload the GPU kept the departed replica's footprint in `external_mb` (775 → 27 603 MiB against an oracle that saw 638 MiB and no process) until the next grant. An accounting bug with a phantom as its only output; it caused five of seven `oracle_agreement` FAILs in Phase 3 and none of it is a policy choice. |
| 4 | *Probe the GPU before pricing a load against it* (`8546cd63`, fix-up `ff34b059`) | T2: a GPU with no resident is never probed, so the `expected > headroom` load guard — the one question asked before a worker exists — could never fire. S4g spent 259 s on four 4 096 MiB reservations against a GPU holding 95.6 GB of someone else's memory. One NVML call on a path that already costs tens of seconds; the guard already existed. |
| 5 | *Publish the granted budget when a window's grant was squeezed* (`22eb33f9`, fix-up `ff34b059`) | T5: the G7 header published the *anchor-derived* figure while the ledger had just issued a squeezed grant, so a grant of 11 units was followed by a 1 936-request window that ran 49 s blind. The published figure contradicting the grant the same code path had just issued is a defect in the feature added for this run, not a new policy. |
| 6 | *Clear the GPU's refreshing flag even when the host probe panics* (`8c696ac0`, fix-up *Pin the probe guard's disarm and correct its abandoned-task claim* `eb398d69`) | A missing drop guard: a panicking probe stranded `refreshing = true` and no further refresh could ever start on that GPU. Textbook low-discretion. |
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

This document is the protocol an autonomous session follows to decide
whether the batch-calibration feature is shippable on one platform, and the
method repeated on every other platform. It is written for the Linux/CUDA
host it is first run on (two RTX PRO 6000 Blackwell, 97 GB each, driver 590,
62 GB RAM, Arch Linux, Docker with the NVIDIA runtime), with a portability
section at the end. Companions: `batch-calibration-design.md` (what the
feature is supposed to do), `unified-memory-admission.md` (MPS / APU / CPU
devices) and `inferio-worker-protocol.md` (the wire fields it reads).

## 1. What "works" means, and why unit tests cannot answer it

The feature has 827 Rust and 215 Python tests, all against fixtures. None can
answer the four questions that decide whether it ships:

1. **Is the measurement honest on this platform?** The ledger prices every
   batch from numbers the worker reports (torch allocator peaks, NVML
   per-process usage) and from `nvidia-smi` free/total. If those are
   systematically wrong on a driver, allocator or container setup, every
   downstream mechanism is wrong with them and nothing inside the system can
   notice.
2. **Does it track a changing world?** External VRAM grows, shrinks, spikes
   and oscillates, and the design's story for each direction (per-window
   grow, per-batch shrink, OOM backstop, deflation, trim) is a timing claim
   only a live host can test.
3. **Does it degrade instead of break?** Auto is on for everyone with no off
   switch, so a wrong number must cost throughput — never a job, never a hung
   or dead server.
4. **Is it a regression?** Manual batch sizes are gone, so throughput on an
   idle GPU must be at least what a user got before.

So the protocol is built around **independent oracles**: every quantity the
feature computes is compared against a measurement taken by a different
instrument from outside the process, and against outcomes the user can see.
Where the oracle is the same driver API the feature uses, it is first
verified against a known allocation, so the chain is anchored in a physical
fact rather than in agreement between two readers of one counter.

## 2. Oracles and instruments

All tools live in `tools/calibration-protocol/`, Python on the managed venv
(`python/.venv`), stdlib + `nvidia-ml-py` + `psutil` + Pillow + torch, so the
same files run on every platform. Each writes timestamped JSONL, so
recordings from different instruments can be joined.

| Tool | What it does | Independent of the feature because |
|---|---|---|
| `vramrec.py` | Samples every 250 ms per GPU: total/used/free and **per-process** used memory with PID, command line and the `CUDA_VISIBLE_DEVICES` / `PANOPTIKON_DEVICE_PIN` of each PID (from `/proc/<pid>/environ`), so each worker is attributed to a model and a GPU. Also samples `/proc/meminfo` (MemAvailable) and per-PID RSS/VmHWM for CPU-GPU runs. | Reads NVML directly, out of process, at a fixed cadence. The feature reads `nvidia-smi` free/total only on grant requests older than 10 s, and per-process usage only inside the worker. |
| `hog.py` | Controllable external pressure. Allocates real, touched device memory on a chosen GPU (torch) or RAM (numpy) following a schedule: `hold`, `step`, `ramp`, `spike`, `oscillate`, `leave-free N`; plus an HTTP control endpoint (`POST /set?mb=`, `GET /state`) so an agent can change it mid-test. Frees with `empty_cache()` so the driver sees the release. | It *is* the external world the feature must react to. |
| `corpus.py` | Deterministic media corpus with controlled unit cost: images at chosen pixel sizes and formats (including alpha and huge 8000×6000 scans), text files at chosen byte lengths (token pricing is `bytes / 4`), short audio via ffmpeg, multi-page PDFs. Tiers: `smoke` (200), `ramp` (2 000), `soak` (10 000+). Writes a manifest with per-item units. | Ground truth for what the packer should have priced. |
| `healthrec.py` | Polls `GET /api/inference/health` and `GET /api/jobs/queue` every 500 ms into JSONL: per GPU `external_mb`, `limit_mb`, `headroom_mb`, `grants_mb`, and per worker `unit_budget`, `ramp_step`, `deflation`, `clean_windows`, `max_units_measured`, `knee_units`, `fit`, `base_mb`, `base_method`. | Not independent (it is the feature's own view), but it is the only record of grants and deflation: the ledger logs none of them (see §8, finding G1). |
| `loadgen.py` | Drives `POST /api/inference/predict/<id>` for several models concurrently from corpus files, with a per-model concurrency and request size. Needed because the job queue runs exactly one job at a time, so multi-model contention on one GPU never arises from jobs alone. | Produces the concurrency the ledger arbitrates. |
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
release* rather than a baseline-vs-hold delta, which on a busy host is off by
gigabytes. Run1 numbers, both GPUs: GPU `used` **+2 MiB** and NVML
per-process **−6 MiB** against a known 10 240 and 40 960 MiB (tolerance ±64);
RAM +32 MiB RSS, +78 MiB recovery (±512). **If the oracle cannot see a known
allocation, nothing downstream is trustworthy and the run stops.** Two traps
run1 hit: the GPU legs need whatever owns the GPU **stopped** for the 40 GB
size, and `--alloc-timeout` is a budget, not an addend to `--hold`.

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
| C2 | C1 with `CUDA_VISIBLE_DEVICES=GPU-<uuid of GPU 1>` (UUID form) | Single-GPU host, ledger still active. |
| C3 | C1 with `CUDA_VISIBLE_DEVICES=1` (index form) | Documented off-switch: inventory unknown, unpriced path. Must still work. |
| C4 | Docker CUDA image built from the branch (`docker build --build-arg ACCELERATOR=cuda`), root `docker-compose.yml` as shipped (no `pid: host`) | The plug-and-play target. ~~Base measurement degrades to `free_delta`.~~ **Refuted in run1** on driver 590.48.01 with the NVIDIA Container Toolkit: NVML resolves the worker's namespace-local pid, `base_method` stays `nvml` and the base error is 0.00 %. Keep it as a per-platform check, not an assumption. |
| C5 | C4 plus `pid: host` | Shows what `--pid=host` buys; informs whether the shipped compose should set it. **Run1 answer: nothing (0.00 % either way) — do not add it to the shipped compose** (G2, finding F10). |
| C6 | Docker **CPU** image (`ACCELERATOR=cpu`) with a cgroup memory limit (`mem_limit: 16g`) | The CPU unified-memory device (`CPU` key, `ram`/`rss` tiers, `cap_fraction` 0.75, DP-2 death-as-negative) without exposing the host to the OOM killer, and without clobbering the cu128 venv. |
| C7 | C1 with a user registry (`config/inference/protocol.toml`) that pins one model to GPU 1 via `devices = ["GPU-<uuid>"]` and flips `enable_batching` on for `easyocr_standard_en` | Multi-GPU and the design's own easyOCR acceptance test. |

SGLang (`~/docker/dsv4flash`) holds 95 of 97 GB on both GPUs. It is used in
Phase 1 as the "GPU nearly full by someone else" scenario, then stopped
(`docker compose down` in that directory, nothing edited) and restarted with
`docker compose up -d`. `hog.py` covers the arbitrary levels in between; any
compose used for pressure gets its own copy under
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
| fixture | `inferio_custom/` copies of `oom_second_batch_impl.py`, `oom_impl.py`, `failbatch_impl.py`, `dying_impl.py`, registered through a user registry group with `metadata.cost` (shape in `manager.rs` test registry) | Deterministic OOM, batch-1 OOM, non-OOM failure and worker death on demand. Note: torch-free fixtures report no `gpu_uuid`, so on CUDA they ~~register only through the single-GPU fallback (C2) or~~ run unpriced; a fixture that touches CUDA (allocate one tensor at load) is needed for a priced fixture on C1. Build that variant in Phase 0. **Corrected in run1 (S5-cpu-C2): the single-GPU fallback is not a route for them** — `resolve_gpu` needs a `gpu_bdf` or a `gpu_total_mb` and a torch-free worker sends neither, so even on one visible GPU all four log *"dispatching this model without VRAM admission … GPUs=1"* and run unpriced (0 grants, 181 unpriced windows). The `*_cpu` family is the **unpriced-path** fixture; every priced fixture must touch CUDA. The CUDA variants and their registry live in `tools/calibration-protocol/fixtures/`. |

## 4. Scenario catalogue

Each scenario states setup, action, what to record and pass criteria.
"Oracle" means `vramrec.py` unless stated; a threshold missed by a small
margin is a finding to discuss, not an automatic fail. Recordings are under
`tools/calibration-protocol/results/<run>/<scenario>/` (git-ignored, one
`runlog.md` each, indexed by that run's `README.md`). Paragraphs marked
**Run1** and **Run2** are corrections to the scenario text — legs impossible
as written, criteria blocked on this platform, numbers that replace a guess —
and point at the report section holding the measurements; where a criterion
was wrong rather than merely unmet, the original is struck through.

### S0 Build and unit baseline
Release build of the branch and of master; `cargo test -p panoptikon
--release`; `uv sync --group test --extra cu128` then pytest for
`tests/inferio_worker` and `tests/inferio/impl`. Pass: green. Record the
durations. This is a sanity floor, not evidence.

### S1 Inventory, identity and a full GPU (SGLang still running)
Start C1. Record `detected GPU` lines (two GPUs, UUIDs, name, 97 887
MiB, `compute_cap` 12.0), `/health` `gpus[]` and `vram[]`. Run the smoke
extraction (`tags/wd-vit-tagger-v3`, `smoke` corpus).
Check:
- `vram[].external_mb` ≈ oracle GPU used minus our workers' NVML usage,
  within ±1 GB, at every health sample.
- Worker registers on the GPU it was pinned to; `base_method = "nvml"`;
  `base_mb` within ±10 % of oracle per-process usage for that PID.
- With headroom ≈ 0: the load warns ("expected to need more VRAM than the
  GPU's remaining headroom"), grants are issued with `mb = 0`, batches
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
  squeezed GPU produces (S1's windows were clamped to 1 unit and never
  fitted). Record `effective_margin` *together with* `fit.samples`, or
  the number means nothing.
- The first load onto a never-sampled GPU is **unwarned**: the
  reservation is taken while the GPU still reads
  `external_known = false, headroom_mb = total`, so the "expected to need
  more VRAM than the GPU's remaining headroom" warning stays silent on
  the one occasion it exists for (finding F1, the load-path half of which
  is fixed in `8546cd63`). Expect the warning on every *subsequent* load,
  not the first. A model with no measured base reserves a flat
  `expected_base_mb = 4096` against real bases of 654–1 182 MiB.

### S2 Cold ramp on an idle GPU (SGLang stopped)
Delete `calibration.toml`. C1, `tags/wd-vit-tagger-v3`, `ramp` corpus of
2 000 uniform 1024² images. Run `ceiling_probe.py` for the same model
first (true base, true slope, true OOM boundary at full free memory).
Check:
- Ramp visible in `healthrec`: `unit_budget` 8 → 16 → 32 → … each step
  earned by a window with a high-water sample; `local_samples` grows; `fit`
  appears after ≥ 3 distinct sizes; `knee_units` after ≥ 12 samples over ≥ 3
  buckets or never (state which). ~~Expect the ramp to stop at 64 for
  item/count models driven by a job (finding B16)~~ — **cleared by G7, and
  the ceiling that replaced it was the transport's, not core's** (finding
  S1): the ramp must now be bounded by the corpus or by memory, and a run
  whose `queue_depth + last_window_items` sums to a constant is the S1
  signature.
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

**Run1: PASS on slope and base for all three models**, and G7 works — the
job-driven ramp reached `unit_budget = 512` with 957 items in flight, so B16
is cleared and the "expect the ramp to stop at 64" paragraph above no longer
holds for a server that publishes the header (run1 report §3, §6). Four
corrections to the scenario itself:

- **The MiniLM leg cannot be run as a job.** `.txt` is in no branch of
  `build_extension_set` (`jobs/files/mod.rs`), so a text corpus indexes **0
  items**. Drive the token model through `loadgen.py` in text mode instead.
- **"Bucketed batches are length-homogeneous in the worker DEBUG log" is
  BLOCKED for `sentence_transformers`**: at `INFERIO_WORKER_LOG_LEVEL=DEBUG`
  that impl emits **zero** bucket lines. Either the line gets added or the
  criterion is dropped; do not record it as a failure of the feature.
  Bucketing *is* observable for easyOCR in S8, from `/health`'s
  `recent_batches`.
- **Inspect `knee_units` in the written store, not just in `/health`**, and
  compare the fitted knee against the probe's throughput curve **per model**:
  a knee of 1 for wd-vit is *correct* (35.9 items/s at batch 1, 36.1 at
  2 048 — the curve is flat), so the check is "does the knee match the
  curve", never "is the knee small".
- **`sample_units` records the granted budget, not the batch actually run**
  (finding N8), so a check that reads it as a batch size is reading the wrong
  quantity.

**W5 confirmed, and it belongs in this scenario.** Repeating S2 with
`PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` gives slope **32.19**
against **50.56** (**−36 %**) and base 932 vs 964, with an **identical
profile key**: a profile learned with the variable and used without it admits
~2 986 items where the measured OOM boundary is 2 560–2 816.

**Judge "≥ 25 % of the boundary" against the probe's own conservatism**: the
linear fit under-shoots the true boundary by 17–36 % near the ceiling (finding
A2), and run1's binding constraint was neither memory nor the knee but the
**job's length** — one doubling per settled window, so 2 000 items buys 10
windows (finding N7).

**Run2** (`65fd2f82`, `results/run2/S2-{wdvit,mobileclip,minilm}/`; run2
report §4.1). wd-vit **FAILed on the knee** and on the frozen anchor
(findings **F1**, **S1**) and lost one item to **P2**; MobileCLIP and MiniLM
PASS, MiniLM showing the R1 bucket-variance filter firing 59 times with its
reason stated. The **Phase A′ re-run on `34a591aa`**
(`results/run2/S2-wdvit-v2/`, report §4.6) is a clean PASS on every check on
the unmodified `server-C1.toml`: no knee at any point, anchor 136 → 439 in 12
windows, the gate following the published figure, and no item lost.
`unstated` still has no producer on this host, so R11's sentinel rename cannot
be verified here.

### S2-base Resident-idle plateau (the `base_accuracy` leg)
Same configuration and models as S2, but **nothing is inferred**. Load
each model under test through `PUT /api/inference/load/{group}/{id}`
(`inferio/http.rs: load_model`, and `loadgen.py --prewarm-only --hold 90`
drives exactly that), then hold every one of them resident and idle for
**≥ 60 s** before any predict, with `vramrec.py` and `healthrec.py`
running and no job in the queue. Two minutes per configuration; run it
once, early, and re-run it after any change to how a worker measures its
own load footprint.

The leg exists because `base_accuracy` had no way to be judged: the only
oracle samples measuring the same quantity as `base_mb` are those between
the replica's load `ok` and its **first grant or predict**, and a
demand-driven load starts its first batch 0–92 ms after admission while the
oracle runs at 1–4 Hz — so in run1 that window was empty in **42 of 60
legs**. Holding the models idle turns it into hundreds of samples. Check:
- `base_accuracy` **PASS**, not INFO: every row judged
  (`cadence_blind: false`, no `[report-only: …]` on the verdict) with
  `oracle_window_samples` in the hundreds, and `error_pct` within a few
  percent — the threshold is 10 %, and the two clean plateaus run1
  recorded by accident both read **0.0 %** (nemotron 3 788 MiB flat over
  714 samples in `S6-b18-loadstall`; wd-vit 964 in `S8-pixmix`).
- Every model resident at once: `lru_size` at least the model count (or
  one `cache_key` each) and `ttl_seconds` longer than the hold, or a
  later load evicts an earlier model and its window closes at the
  departure instead of the end of the hold.
- A row that stays unjudged is a **defect in the leg**, not a finding —
  something predicted (`first_work_dt_ms` names the moment), a model was
  evicted, or the load failed. The one honest exception is a `base_method`
  other than `nvml`, which is not the oracle's quantity.
- Nothing else is asserted here: no ramp, no store, no throughput. The leg's
  whole content is that `base_mb` is what the process holds when it holds
  nothing else.

**Run2: PASS, first run of this leg** (`results/run2/S2-base/`, report §8):
every row judged, `oracle_window_samples` 535 / 504 / 476 / 373, errors 0.0 /
0.0 / 4.47 / 0.0 % — the nemotron row closes R12/F-C. One tool fix was needed
first (`cf1939e6`): the old freshest-pid-in-window rule compared MiniLM's
`base_mb` against MobileCLIP's process and reported 10.66 %, a FAIL that was
pure attribution.

### S3 Restart and resume
Restart C1 after S2; run a fresh `ramp` corpus for the same model.
Check: first window's `unit_budget` equals the persisted anchor (no
re-ramp from seed); `/api/inference/metadata` shows `calibration.status =
"local"`; `local_samples` continues from the stored count; the file's
anchor only ever increases. Then delete the file with the server
**stopped**, restart, confirm re-ramp from seed. Also delete it while the
server is running and observe whether the next debounced write resurrects
it (finding B3).

**Run1: resume PASS.** Note that the *first* window is bounded by core's
`MIN_IN_FLIGHT_UNITS = 64` floor, so a resume takes two windows, not one.
Both probes came back with more than the plan expected:

- **B3 confirmed, with a nuance.** The file is back within ~20 s and no log
  line says so — but the profile is read at **worker admission**, not at
  process start, so a delete *before* the first load leaves
  `seeded_from_store=false` and what gets resurrected is a **new, worse**
  profile (anchor 32 where it had been 512, `local_samples` 4 where it had
  been 10, plus a spurious `knee_units = 7`): the write is a whole-profile
  **replacement**, not a merge (finding N6). Stop → delete → restart behaves
  as documented.
- **B4 confirmed on all three legs, and the anchor is worse than a floor.**
  An edited anchor is acted on immediately and left as it is; with the anchor
  at 4 096 *and* the slope poisoned to a quarter of truth, the slope
  **self-heals** by refitting over the stored samples with no new sample, but
  the anchor never does — a `unit_budget = 4096` first window OOMed, the
  worker's halving loop absorbed it, and the file afterwards still said
  **4 096** (findings N5/B4c). The OOM backstop is the only thing under it.

**Run2** (`results/run2/S3-wdvit/`, `S3-wdvit-kcw/`; report §4.1): resume
PASS, and the new persisted field `knee_clean_windows` **is restored across
the restart**, proved in the `S3-wdvit-kcw` sub-leg (a seed of 11 made the
first widening arrive 1.52 s after admission where the control needed twelve
windows). The leg also showed F-A across a restart — seeded with a
`knee_units = 7` store, a fresh job ran its whole length at 7–31 units,
`utilization` **0.01** (finding **F1**). The **Phase A′ re-run on
`34a591aa`** (`results/run2/S3-wdvit-v2/`, report §4.6) is a PASS on the
leg's own question: no knee comes back with the seed, `utilization` 0.01 →
0.69, three grants for the whole job. Its one FAIL is throughput at 0.88×
master, which is run1 parity (finding N2), not a regression.

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
  `external_mb` is a **window-boundary quantity**: **zero** host probes ran
  in ~2.5 h of recording, because the only path that can start one is a
  grant, and at grant time the worker's report has just refreshed the sample.
  Ages reached 85.5 s with a worker resident and 166.9 s overall; a +30 GB
  step took 31.5 s to reach `/health`. What kept every profile safe is the
  worker's own live NVML clamp, reacting in **0.60–2.81 s** (finding T3). So
  S4b's "shrink observed in `healthrec` within 15 s" measures the wrong
  instrument: record **both** latencies and judge safety on the clamp.
- **`limit` clamps to 0 below ~9 % free** under run1's arithmetic
  (`limit = total − external × (1 + margin)` charges the margin against the
  *neighbour's* level), after which grants go memory-blind (B1, T4/P5-2).
  R5's capped reserve replaces it; S4a on `34a591aa` measured the fix.
- **S4a's utilization must be judged against the boundary at the hog's free
  level, not the full-GPU boundary.** At 12 GB free the probe's boundary is
  **220 units**; against the full-GPU boundary the same run reads 0.40 and
  looks fine. State which boundary the number is against, every time.
- **Use the 16 000-item `ramp8` corpus for S4b–S4e** (`corpus.py --tier ramp
  --scale 8`): the 2 000-item `ramp` is exhausted in ~70 s, before S4c's
  t = 90 s event fires. S4f is better driven by `loadgen.py` than by a job
  loop, so the model is never unloaded and the post-unload phantom cannot
  contaminate the tracking measurement.

Smaller results worth carrying: a squeezed grant did **not** squeeze the
in-flight target until `22eb33f9` (T5); grants **alternate** rather than
converge under steady pressure (425/425/906/370/370/892/257 at a pinned
`external_mb`, T9); the worker's clamp has no margin of its own and ran
at 98.2 % of free nine times without an OOM (T10); and S4g's B15 count is
**4 attempts**, `reqwest_retry`'s, after which the whole job aborts — not
one load per item.

**Run2: S4a on `34a591aa` and PASS; S4g, S4d and S4b on `63a8ad73` and PASS.**
`results/run2/S4a-v2/`, `S4g-v2/`, `S4d-v2/`, `S4b-v2/`; report §4.5.
**T4/P5-2 are closed by measurement**:
`reserve_rule = "capped_default"`, `reserve_mb` exactly 1 024 against run1's
8 579, `limit_mb` 11 071 against 3 517, the executed batch 83 items against
run1's 11, and 147 granted of the probe's 220-unit boundary at that free
level (66.8 % against run1's 21 %). Judge `utilization` against the boundary
**at the hog's free level** — `analyze.py`'s printed 0.34 is against the full
GPU, and the leg reads 0.67 granted / 0.38 executed against the right one.
**T10 now has a number and is the binding constraint**: the worker's live
clamp fired 22 times, comparing NVML's free reading *after* our own caching
allocator took its pool against a grant the ledger had already charged for,
so **44 % of the grant goes unused with nothing external moving** (finding
S4a-A2, a user decision: subtract our own `memory_reserved − memory_allocated`
before the comparison, or price the grant net of the pool).

| Profile | Run2 verdict, on `63a8ad73` | Recording |
|---|---|---|
| **S4g** no room to load | **PASS on every criterion.** A per-model `failure_reason` (model + cooldown + the torch OOM) and `failed_jobs_total 1` — run1's **F1** and **F2** both closed; **2 load attempts / 25.4 s** against run1's 4 / 259.1 s; the server `ok` throughout and wd-vit 180/180 after the release; the cooldown ladder 2 s → 4 s visible in `/health.load_cooldowns[]` | `results/run2/S4g-v2/` |
| **S4d** step down | **PASS on all three stated expectations.** **0 of 18** memory-blind grants, `limit_mb` min **6 975** over 1 533 samples with none at 0, and the budget back to `S2-wdvit-v2`'s published 878 in **2 windows / 6.1 s**. 16 000/16 000, throughput 0.92× of S2. **S1b's shrink lands in the next window** (82 → 43, 85 → 43) | `results/run2/S4d-v2/` |
| **S4b** step up | **PASS except the headline latency.** The +30 GB step reaches the grant line in **22.4 s** and `/health` in 22.5 s (run1: 31.0 s) — but **21.3 s of that is the single in-flight batch**, because per-batch readings are applied at `token.finish`, so "within a few seconds" is unreachable without mid-window publication (finding S4b-A1, a user decision). Budget flat at 561 for seven windows where run1 oscillated; 0 clamps needed; 16 000/16 000 at 0.96× of S2 | `results/run2/S4b-v2/` |

Both remaining criteria of this scenario are settled on this host: judge the
step latency against **one in-flight window**, not a wall-clock "few seconds",
and judge `utilization` against the boundary at the hog's free level.

### S5 OOM backstop and negative samples
~~C2 (single-GPU fallback lets torch-free fixtures register)~~ **the
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
- `dying`: worker death mid-window on a discrete GPU: model respawned,
  next window admitted at the anchor again (finding B7), no synthetic
  negative (the log line for unified-memory devices must **not** appear). Then a
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

**Run1: the backstop works** — every real fault absorbed, no lost job, no
unsafe grant — and three of this scenario's own assumptions were wrong:

- **`oom_second_batch` OOMs forever, not once.** The shipped torch-free impl
  raises on *every* batch from the second on, which is `oom_impl`'s job. The
  CUDA fixture gained an `oom_batches` key (default 1) so the "one OOM then
  healthy" leg is expressible (`17e38e95`).
- **`scan_audio` defaults to `false`** in a fresh per-DB job config, so the
  first whisper attempt indexes 0 of 200 files. Set it before the grantless
  legs.
- The `poison` tier's truncated JPEG/PNG **decode fine** under Pillow; the
  item that actually fails is the 256 MP PNG, via `DecompressionBombError`.

It also produced a shape the plan did not anticipate: the same fault costs
**100 % or 0 %** of requests depending on the client's batching, because a
single-request window goes to `run_single`, which has no per-request fallback
(276/276 lost vs 0/173, finding Q7). B11, B8, B7 and B15 were all confirmed
with numbers (run1 report §4).

**Run2: every clause met, and three run1 findings closed** (binary
`65fd2f82`, `results/run2/S5-*`; report §4.2).

- **`failbatch_oomtext` (B11/Q1): PASS, 0 negatives** of 26 windows on a
  GPU with 96 356 MiB free, against run1's 15; `oom=false` on 25 of 25
  fallbacks; `deflation=0` throughout. **Correction to this scenario's
  wording:** the job leg reports `completed`, not `partial` — `failbatch`
  fails only multi-input batches and the per-request fallback recovers
  every item, so nothing is ever owed.
- **`oom_timed` (B8/Q2): PASS.** Deflation capped at **4** =
  `ceil(log2(seed 8)) + 1` against run1's 6 589 levels in 120 s; repaid
  4 → 0 in 0.5 s on clean windows, and 4 → 0 over 120 s of idleness at
  one level per 30.0 s (a new DEBUG line per repayment).
- **`oom_second_batch`: PASS**, identical to run1 (1 negative of 3 915;
  deflation 1 → 0 after exactly 3 clean windows). Its `oom_class` is
  present and **vetoed** by R3's corroboration rule
  (`free_mb_at_failure 96 518 ≥ grant.mb 96 356`); the deflation came
  from the error frame's `INFERENCE_OOM_WINDOW` marker 61 µs later. On
  an idle GPU the veto will fire on essentially every
  `message_pattern` OOM, because `grant.mb` is the whole GPU.
- **`dies_on_load` (B15/Q5): PASS on the cooldown, FAIL on the reason
  text.** 7 load attempts in 182 s against run1's 93; ladder 2, 4, 8, 16,
  32, 64, 128 s; 13 145 refusals as **503** with `retry-after` and
  `detail.kind = "load_cooldown"`; `/health.load_cooldowns[]` names the
  model, its failure count and `retry_at`. The job aborts in 9.5 s after
  2 attempts (run1: 259 s / 4) but its `failure_reason` still reads
  *"model load failed on all 1 inference endpoints"* — no model id, no
  retry time.
- **`dying` inside a job (F7/R2): PASS with the expectation corrected.**
  2 000 re-queue lines and `requeued=2000`, `job_failures_total: 2000`
  each with a real `occurred_at` and `requeued: true`, job record with
  `end_time != start_time` and `failed_items: 2000`, 63 spawns/deaths for
  2 000 items over 137 s, 0 synthetic negatives. The outcome is
  **`failed`** (Systemic), not `partial`: `dying_cuda` kills *every*
  predict, so nothing survives the retry. `partial` is exercised by
  `S2-wdvit` instead.

### S6 Multi-model contention on one GPU
C1, `loadgen.py` driving `tags/wd-vit-tagger-v3`, `clip/apple_MobileCLIP-S1`
and `textembed/all-MiniLM-L6-v2` concurrently (concurrency 2 each) for
10 minutes, with `whisper/tiny` loaded and idle as a `none`-class
resident, and `hog.py` at `leave-free 20 GB` to make the GPU tight.
Check:
- Ledger invariant at every health sample: Σ `charge_mb` + load
  reservations ≤ `limit_mb`; Σ oracle usage of our PIDs ≤ `limit_mb` +
  margin allowance; no OOM.
- Each hungry model gets at least its floor (one seed batch); shares
  scale with appetite after fit.
- Idle-resident trim: stop driving `clip`, keep driving `tags` with a
  squeezed grant; within 30 s `clip`'s pool slack drops in the oracle and
  the "an idle resident is holding allocator pool slack …" line appears.
  Record the observed trim durations under load — the round trip has a fixed
  60 s deadline that kills the whole model on expiry (B17), and a fixture
  that ignores the trim for 70 s confirms the failure shape.
- Whisper's CT2 VRAM shows up in `external_mb` (by design) and is
  margin-inflated; note the cost.
- Load stall: while nemotron is loading, time the predict latency of the
  already-resident `tags` model and record the stall length (B18).

**Run1.** The trim is fast and exact — flagged **1.837 s** after the squeeze,
a 5.8 ms round trip, the oracle seeing exactly the `slack_mb` the ledger named
— so the "within 30 s" criterion is met by an order of magnitude and the
useful number to record is the flag latency, not the deadline. Three
corrections to what this scenario measures:

- **B17 fires at ~20 s, not at the 60 s `TRIM_DEADLINE`**: with a fixture
  that ignores the trim for 70 s the worker was taken down at **20.26 s** by a
  shutdown/teardown path racing the hung trim (10 s `unload_grace` + 5 s
  `terminate_grace` is the right magnitude), and the client sees a bare
  `{"detail":"Prediction failed"}` 500 after 18.6 s.
- **Q6 resolves in the with-neighbour case**: a grantless model's VRAM *does*
  land in `external_mb` once anything refreshes the GPU; Phase 4's
  "invisible" reading was the never-refreshed case only.
- **B21 is unreachable with shipped impls**: no impl defines `prepare()`, so
  parked prewarm workers hold 0 MiB of VRAM and ~1.7 GiB of host RAM instead.

B18 was confirmed at 100 % of the load and the knee at pinning a contended
model for the whole run (P5-4, P5-5); numbers in run1 report §4.

**Run2: PASS on every clause, with one throughput regression to decide**
(binary `65fd2f82`, `results/run2/S6-contend/`, `S6-b18-loadstall/`;
report §4.2).

- **P5-5 closed**: 0 `throughput_collapse` negatives of 2 610 settles
  (run1: 3), because a collapse verdict is now trusted only from a
  sole-occupancy window.
- **P5-4 closed for this scenario**: wd-vit and MobileCLIP fitted no
  knee under contention and MiniLM's 16 383 was **withdrawn 18 s
  later** — the first firing of `knee_withdrawn` in either run. The
  store carries three profiles and **zero** `knee_units`, where run1's
  equivalent carried 31 / 15 / 4 095 and is on the poisoned list.
  Budgets move with appetite instead: 8 → 128 and 4 000 → 32 634.
- **B18/P5-3 closed**: predict latency during a neighbour's 13.388 s
  load is p50 551 ms, max **951 ms** — **1.86×** the model's own p50 and
  7.1 % of the load, against run1's 28.3× and 100.2 %. The literal
  "p99 < 500 ms" bar is below this leg's undisturbed p50 (511 ms) and is
  unreachable by construction; **record the ratio, not an absolute**.
- **B1 under a squeeze**: 3 memory-blind grants of 2 610 (run1: 113),
  0 samples at `limit_mb = 0` (run1: 367).
- **Finding C6, to decide**: with the GPU squeezed to 4 GB free the R5
  reserve cap admits `unit_budget = 60`, `mb = 3 040` where run1 admitted
  `unit_budget = 1`, `mb = 0` — and wd-vit's throughput **falls to
  0.56×** (21.85 vs 38.98 items/s, p50 5 323 vs 3 199 ms) with nothing
  in the ledger noticing, because the knee compares buckets within one
  run and this run never ran wd-vit small on a squeezed GPU. Safe (0
  OOM, 0 negatives, 0 unsafe grants) and the approved trade, but the
  first measured cost of R5.
- The idle-resident trim flagged **two** residents in the same
  millisecond (6 µs apart), which is why its latency reads 2.888 s
  rather than run1's 1.837 s. Both released, 6.0 and 9.4 ms round trips.

### S7 Multi-GPU
C7. Pin `clip/apple_MobileCLIP-S1` to GPU 1; leave `tags` on the default
GPU. Drive both. Check: oracle shows each worker PID on the pinned
GPU only; two independent `vram[]` rows; a hog on GPU 0 changes only
GPU 0's grants; `PinDiverged` never logged. Then C3 (index-form
`CUDA_VISIBLE_DEVICES=1`): confirm the INFO line, no `vram[]`, unpriced
dispatch, and that the job still completes with registry defaults.

**Run1: isolation is exact.** Under a hog that took GPU 0 from 96 GB free
to 4 GB, GPU 1's ledger row was **byte-identical** at every sample
(`ext 775, limit 97 034, headroom 96 022`) while GPU 0's limit went
97 036 → 2 813 → 0; every worker PID appeared on exactly one GPU and
`PinDiverged` never logged. **B20's shape is right but its number is
wrong: the unpriced window is bounded by the registry's
`metadata.default_batch_size` — 64 for the `tags` group — and falls back
to the server-wide `default_max_batch = 32` only where a group declares
none.** Confirmed on two independent paths (C3, and S13's no-inventory
case). T5's open question stays open: no shipped configuration puts one
model's replicas on two GPUs, so a squeeze on one GPU clamping the
other's window could not be provoked; it needs a `replica_count > 1`
config spanning GPUs.

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

**Run1: the easyOCR acceptance test passes by 8.5×** (54.72 s of inference
against master's 468.20 s on the same 460 segments), with 37 size-homogeneous
bucketed batches, 0 OOMs and 0 collapse flags — on an **idle GPU only**
(Phase 1 saw `deflation = 108` for this model on a full GPU), using half of a
97 GB card, and the same bucketing on a 24 GB card is untested.
`slope_accuracy` is **not comparable** here unless the probe is re-run with
the C7 registry, since the shipped registry has `enable_batching = false`,
under which easyOCR's memory is flat in batch size.

**W1 confirmed, and it is worse than a pricing quirk: pixel pricing is a
property of the corpus, not of the model.** Pricing is the raw header
`width × height`, so for a model that resizes to a fixed canvas the *fitted
slope* moves with the corpus, and the 20 MP items — charged 66× a thumbnail
for ~1× the memory — forced **58 of 110 batches to hold a single item**
(utilization 0.08). The knee moves with it too, since it is expressed in
priced units. (Run2's probes then showed the headline 4.33× was a group
mismatch, below.)

**W2 is not reproduced and the comparator retires instead**: no
`throughput_collapse` fired in any of the 20 legs, and the worker logged
*"retiring the throughput comparator after 8 non-comparable batches"* four
times. The tier encodes JPEG rather than the PNG the plan names, so a slower
codec might still trip it. Token pricing over-charges **8×** past
`max_seq_length` and under-charges CJK by 1.37×, which the model's own
truncation makes exactly right; neither direction produced an OOM.

**Run2, the easyOCR half** (Phase D1, `65fd2f82`, **eleven** legs;
`results/run2/S8-ocr-*`; report §4.3). Every stated clause for this leg is
met: the 2560² canvas is in force and provable from the stored windows,
`grant_safety` PASSes on all eight ledger legs, and the job completes 460/460
on all eleven. Four corrections to the leg as written:

- **"(easyOCR, grantless)" describes the *shipped* registry, not C7.** C7
  sets `enable_batching = true`, so the model **is** granted and **does** fit
  a slope, byte-identical to run1's on three separate legs. The leg the
  parenthetical describes was added (`S8-ocr-C1-grantless`) and there the
  expectation holds exactly: `local_samples 0`, `unit_budget` pinned at the
  seed, **no store written**, `analyze.py` printing *"NOTHING WAS LEARNED"*.
  F-B's *never-learning* half is untouched on the shipped configuration.
- **The wall-clock acceptance criterion is not resolvable on this host.**
  The workload is CPU-bound (0 % GPU utilization in 157 of 184 one-second
  samples) and the host was paging, and the master baseline itself moved
  468 s → 113 s → 185 s for an untouched binary. **Do not read a throughput
  verdict off this scenario without `mem_available`, swap and GPU
  utilization beside it.**
- **What *is* host-independent is the packing decision**: capped legs finish
  the corpus in 49–57 batches against the uncapped legs' 61–64, and per-item
  time on uniform page batches is flat at 126–131 ms/item across every leg
  and configuration, run1 included.
- **The cap cost the bucketing its size information** (finding D1-b), since
  under `min(raw, 6 553 600)` a 8.7 MP scan and a 48 MP sheet price
  identically. Fixed by a raw-size tiebreaker among equally priced items and
  by easyOCR bounding the **detector's batch tensor** before padding — while
  recognition still crops from the raw image, so transcription of small print
  on large scans is unchanged.

**Run2 ground truth: the ceiling probes** (`results/run2/probes/`; report
§4.4 and §8). They settle W1/Q3 and give this scenario the ground truth it
never had:

- **A `pixel`-priced slope is comparable only to a probe of the same image
  group**, and both of run1's disagreements were group mismatches (the
  published 4.33× divided a 0.3 MP-dominated fit by the 1024×1024 probe).
  **`slope_accuracy` must name the group it compares against, or it means
  nothing.** The capped price is 1.4176× the above-canvas probe and all of it
  is arithmetic, so the honest band for a C7 leg is 0.001044 … 0.001480.
- **`ceiling_probe.py` had never been run against the C7 registry**, which is
  what made `slope_accuracy` and `utilization` uninterpretable here (D1-c).
  It has now, and it needed a tool fix first (`6d074f3d`: a registry override
  must *replace* an inference id, as `registry.rs` does, not merge into it).
- **Product finding, since fixed**: easyOCR's batched detector dies of a
  32-bit index overflow in CRAFT's first `MaxPool2d` at batch ≥ 29
  (`64 × ⌊H/2⌋ × ⌊W/2⌋` against 2³¹ − 1 = **28 items / 183 500 800 capped
  units** at 1824×2560) and the impl silently reprocessed the window one
  image at a time — a 3.2× throughput cliff the ledger could not learn. Both
  easyOCR **bisect boundaries in this run are therefore invalid** (37 against
  a true 28), which `utilization` consumes.
- Two instrument facts: a **warm-allocator sweep overstates the requirement
  by up to 1.8×** (the warm figure is the one comparable to a ledger fit; a
  *boundary* must come from the cold series), and **`--bisect-oom` is only
  trustworthy when the impl has no silent fallback**.

**Run2, the pixmix half** (`results/run2/S8-pixmix/`, `63a8ad73`; report §4.7).
**PASS on every packing and canvas expectation.** The fit is **+0.07 %** off
the `img-0.3mp` probe its corpus is dominated by (0.0012809 against 0.0012800),
where the same fit against the `img-1mp` probe reads 4.78× and means nothing;
`canvas_pixels=1835008` appears on all 52 grants, on the load report and in
`/health`; and the 50 × 20 MP items went through **two windows of 46 and 27
items** where run1 gave 58 of 110 batches a single item — the canvas price
1 835 008 against a raw 20 280 000 is an 11.05× reduction, and run1's raw price
exceeded the whole peak budget. `utilization` reads **0.07** and FAILs: the
corpus (638 M priced units, ~2.2× one batch at the probe's boundary) ran out
while the ramp was still doubling, and the knee estimator refuses three times
saying so. **Nemotron's memory is a function of the tile grid its aspect ratio
picks, not of pixel count** — 1 MP and 4 MP squares allocate byte-identically,
and so do 0.3 MP and 20 MP 4:3 images — so even in capped units the per-unit
cost spans **8.35×** over that corpus.

**Run2, the shape-ceiling leg** (`results/run2/S8-ocr-C7-ceiling/`; report
§4.7). Everything passes except the ceiling itself, which is **not reachable
through a job on a 97 887 MiB GPU**: to grant the 28-item batch that triggers
CRAFT's int32 limit (183 500 800 capped units) the ledger must first reserve
what its fit says it costs, `796 + 0.0014796440700825149 × 183 500 800 =
272 313 MiB` — **2.78× the whole GPU** — where the probe measures **94 070 MiB**
for the same batch (~2.9×, rising to 3.8× at batch 37 as easyOCR's memory
flattens and the Theil–Sen line does not). The loop is self-closing: no fit
sample above 34.8 M units exists because no grant above it is issued. So
`shape_ceiling_units` stayed `null` in all 386 samples and `index_limit` never
appeared, while the leg PASSes on 460/460 items, host-side canvas pricing, P1,
grant safety, `oracle_agreement` (worst 19 MiB) and a slope byte-identical to
run1's. **Recognition text is byte-identical to run1's** — with the caveat that
the synthetic corpus's > 2560 px scans yield no recognised text in either run,
so that check is an equality of two empty sets; testing the detect-bounded /
recognise-raw split properly needs a corpus with real text on a large page.

### S9 Soak (overnight)
C1, 8–12 h: a loop of extraction jobs over the `soak` corpus alternating
models, `hog.py` on a randomized schedule (steps, spikes, calm periods),
`loadgen.py` at low rate in the background. Check every hour from
`healthrec`: `grants_outstanding` returns to 0 when idle; `deflation`
returns to 0 after calm periods; `calibration.toml` size bounded (ring
64); server RSS flat; no worker deaths except intended; throughput per
job within ±20 % of the S2 figure during calm periods.

**Run2: PASS on every expectation, at 4 h instead of 8**
(`results/run2/S9/`, `63a8ad73`, 06:01:15 → 10:04:38; report §4.8).
**29/29 jobs `completed`** with 0 `partial`, 0 failed items, 0 `invalid
multipart` and 0 `request_incomplete` — the P2 acceptance test; **0 deaths**
in 33 spawns; **no `knee_units` on any of the four profiles**, where run1
persisted 1 on wd-vit and never refitted for 7 h 55 m; deflation peaked at 1
and returned to 0 within 30 s each time; the store is **5 550 B** with the
64-ring respected; `load_cooldowns` never populated; RSS 5 134 → 7 341 MB with
VmHWM 11 310 MB; `refused_requests` 0; peak descriptors **271** against run1's
8 257; 0 ERROR and 0 panic in 388 623 events. Three `analyze.py` FAILs, all
adjudicated in report §4.8: the 3 332 "memory-blind" grants are the contention
floor (`headroom_mb=0`, run1's 3 912 identical in shape, 0 of 21 206 grants
over their priced headroom); 7 OOM negatives, each carrying its C2 tier line,
against run1's 3 OOM **and 34 collapse** in twice the time; and 7 271
`ledger_invariant` samples of 61 222, 99.7 % of them while `external_mb`
> 10 GiB — the hog stepping up under residents that already hold their grants.
The one number that did not move is `oracle_agreement`: **13.53 %** against
run1's 14.12 %, a 0.6 pp improvement where R12's attribution fix was expected
to give a step change (finding S9-A1, open).

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
**on the host** (GPU-level NVML works without `--pid=host`); S4b and S4c
repeated in C4; the health check and the CI smoke script pass (including
the **403 on 6339**, which is Docker-only — see S14); `docker logs`
carries the worker lines; and **peak file descriptors and `ulimit -n`
inside the container**. Record the base error C4 incurs versus the oracle
(this decides the `pid: host` recommendation).

**Run1.**

- **B9 is refuted on this platform** and must stay a per-platform check, not
  an assumption: on driver 590.48.01 with the NVIDIA Container Toolkit, NVML
  inside the container resolves the worker's namespace-local pid, so
  `base_method` is **`nvml` in C4 *and* C5** and the base error is **0.00 %**
  either way. `pid: host` therefore buys nothing and costs PID isolation —
  **G2's answer is: do not add it to the shipped compose.** One grep decides
  it on a new platform.
- **W4 stays open, and was not reachable here**: `free_delta` / `alloc_delta`
  were never exercised, so the fixed 500 MiB context estimate against a
  measured 666–668 MiB context is still untested. It is live wherever NVML
  *does* hide the pid: older drivers, WSL, WDDM, ROCm containers, podman.
- **The store key is portable bare ↔ Docker** on this host, because the
  image's venv comes from the same `uv.lock` with the same `cu128` extra.
- **The descriptor criterion is the one that failed, and it was the release
  gate** (F6): the container's `nofile` soft limit is 1024 and each in-flight
  predict costs two sockets in one process. Fixed on the branch (`d5e42c78`);
  **keep the "peak fds" check** so the fix is re-verified per platform.
- Repeating S4b and S4c inside the container **passed, and faster than bare**:
  the +30 GB step was visible at **+1.2 s** against the bare host's 31.5 s
  (different window lengths, not a different mechanism).

**Run2 (C4): PASS on every criterion** (`results/run2/S11-C4/`, image
`5b2b3ed166c0`, the shipped `compose/docker-compose.C4.yml` with no `pid: host`
and no nofile overlay; report §4.7). **2 000/2 000, 0 errors, 0 failed items**,
60.39 s of inference. **This is the real P1 test** — the job was created
through the shipped `docker.toml` policy over h2c inside the shipped image:
`EXTRACTION_POST_RC=0`, **0 `no_policy` and 0 × 403** in 2 124 log lines, with
`transport = "h2c"` in 245 of 282 health samples (the 37 `unknown` are the
documented pre-probe fallback). **The descriptor criterion is now met by a wide
margin**: peak **80 fds / 37 sockets** on pid 1 inside the container against
run1's 3 003 / 2 961, with `connections_in_use` peaking at 16 (37 ≈ 2 × 16 + 5),
and the in-flight ceiling sits at the **byte budget** (`max_concurrent_requests`
2 304, peak in flight 1 017, `refused_requests` 0), not at the 384 descriptor
clamp. `/proc/1/limits` still reads `Max open files 524288 524288` — run1's F6
fix re-verified — while `docker exec … ulimit -n` still prints 1 024, run1's
documented nuance. Nine grants ramp 6 → 384 units and stop because the corpus
ran out; four early worker clamps of the S4a-A2 shape are harmless.

### S12 CPU device (Docker CPU image)
C6 with `mem_limit: 16g`. `tags/wd-vit-tagger-v3` on the `smoke` corpus.
Check: device `CPU`, name `CPU (64 GB)` (or the 4 GiB-grid rounding of
whatever the container reports), `cap_fraction` 0.75 in `/health`,
`free_source = "ram"`, `base_method = "rss"`, `INFERIO_DEVICE=cpu` in the
worker's environ; a RAM hog inside the container that pushes the worker
past the cgroup limit gets it OOM-killed and the ledger logs the
unified-memory-device death negative and halves the anchor; the job continues.
Whether the fit ever produces samples (the monotone high-water problem
in the unified doc) is recorded, not judged. **Expected finding (B19):**
the CPU device reads host RAM, not the cgroup limit (`cpu.rs`), so inside
a 16 GB container the GPU says `CPU (64 GB)` with a 46 GB budget while
the kernel kills at 16 GB. Record the GPU total and whether the
death-negative path is the only thing standing between a Docker CPU user
and a respawn loop.

**Run1: every stated criterion PASSED, and B19's number is a 2.94×
overcommit** — GPU total 64 137 MiB, budget 48 102 MiB, cgroup `memory.max`
16 384 MiB, with nothing in the CPU path reading `memory.max`. The
death-negative path is the only thing between that user and a respawn loop,
and **it converges only across job passes, not within one**: five passes with
a ~14 GB hog gave anchors 32 → 16 → 8 → 4 and 4 deaths / 14 spawns with no
backoff, each pass losing a worker and showing the user a *completed* job
that did a fraction of the work. Caveat: cgroup `oom_kill` stayed at **2**, so
deaths 2–4 were not OOM kills — the unified path treats **every** death as a
memory negative, so an unrelated crash also halves the anchor.

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
shim under three models at concurrency 2 for 240 s: **2** host probes, gateway
threads 51–53 against a baseline's 52–53, **max 1** concurrent `nvidia-smi`,
predict p50s within noise. Forcing the worst case (14 load/unload cycles) gave
7 probes and still max 1, because the refresh is single-flight per GPU, a
failure backs the GPU off 10 s, and **nothing polls**. Two residuals to keep:
`capability::output_with_timeout` **abandons** the timed-out child and its
reader thread instead of killing them (finding F13), and
**`accelerator_report`'s own capability query has no timeout** — with
`slow-all` it added ~11 s to boot, so a `slow-all` run collapses into the
"hidden" case at boot anyway.

### S14 Regression sanity
The CI smoke sequence (`.github/workflows/release.yml`) against C1 and C4:
DB create, folders, rescan, PQL search, thumbnail, file serve, 403 on
port 6339 (the 403 assertion is Docker-only: `docker.toml` gives 6339 the
`public` endpoint with `restricted_demo`, while `default.toml` gives it
`legacy_ui` with the `localhost` policy, which correctly answers 200 on
C1). Plus one run of every shipped model category on the `smoke`
corpus to catch a load-path regression unrelated to calibration.

### S15 Protocol self-test (mutation runs)
The protocol is only useful if it catches what it is meant to catch.
Deliberate faults, each on a throwaway branch, run through S2 and S4b.
A fault the protocol misses is a hole in the protocol and gets a new
check before the platform passes are run.

**Revision after run1 (2026-09-03).** All three mutations were caught, but
**not one of them by the check this section predicted**, and m1 exposed a
real hole. The expectations below are the corrected ones; the four holes
(H1–H4) are closed in `analyze.py` and the tooling README, and m4/m5 are
new and unrun.

| # | Mutation | Predicted catch | What actually happened |
|---|---|---|---|
| m1 | Worker under-reports `peak_reserved_mb` by 50 % (patch `measure_batch`) | slope check fails in S2; S4b or S4c OOMs | **Neither.** Halving the reported high-water puts it *below* the post-load baseline (964 MiB base against a halved peak of ≈500), so the `grew` test never fires: `high_water_samples = 0` on all 96 grants, no fit forms, `unit_budget` never leaves the seed of 8 and **no store is written**. The direction is **sandbagging**, so S4b produced 0 OOMs and the job ran *faster* than baseline (`throughput` PASS 1.15×). With no `calibration.after.toml`, `slope_accuracy` and `persistence` both came back **SKIP**, which never sets the exit code; the only FAIL was `utilization` (peak `unit_budget` 8 / boundary 2 560 = 0.00), and only because that leg happened to run with `--probe`. **Correct expected catch: `utilization` *and* `calibration_learned`**, with `slope_accuracy` and `persistence` as WARN/FAIL rather than SKIP. Run every S15 leg with `--learning` |
| m2 | Host ignores `external` (patch `external_locked` to return 0) | S4a grants exceed headroom against the oracle; S4b OOMs | **Confirmed, and earlier than predicted**: three FAILs on the plain S2 leg before any hog starts — `grant_safety` **335 of 335** grants over the oracle's live free memory, `slope_accuracy` (the store it writes is degenerate: slope 0.0, 0 samples, `max_units_measured` 1, now flagged poisoned) and `utilization`. Note what did **not** fire: `ledger_invariant`'s strict form PASSed on **0 of 498** GPU-samples, because zeroing `external` makes `limit_mb == total_mb`. On S4b it also FAILs the new `hog_tracking` form (`external_mb 0..0` against a hog holding 30 720 MiB for 77 s) |
| m3 | Margin set to −0.5 in the config | the S4 series | **The value never reaches a scenario — it is rejected at config load**: `Error: inference_local.vram margin must be a finite number >= 0 (got -0.5); it is a fraction of other processes' VRAM usage, e.g. 0.10 for 10%`. `Settings::validate` names the key, the constraint and an example, and `ledger.rs` carries the same assumption as defence in depth (`margin.max(0.0)`, `cap_fraction.is_finite()`). A hard startup failure is the right trade for an admission-safety knob. **Not a protocol hole**; restate the expectation as *"rejected at config load with a message naming the key; if a future change ever lets it through, the catch is `grant_safety` in the S4 series"* |
| m4 | `cap_fraction = 1.5` | — | *To run in the next pass.* Same shape as m3: does validation reject a fraction above 1, and if not, does the S4 series catch a GPU budgeted at 150 % of itself? |
| m5 | `cap_fraction = NaN` | — | *To run in the next pass.* `ledger.rs` filters on `cap_fraction.is_finite()`, so the question is whether `Settings::validate` rejects it first, and as legibly as the margin case, or the run silently proceeds on the filtered default |

**Holes found by run1's S15, and how they are closed.** All four are in
`tools/calibration-protocol/analyze.py` and its README; §6 carries the
rules.

| Id | Hole | Closure |
|---|---|---|
| H1 | A fault that destroys the measurement destroys the evidence too, and `analyze.py` turned that into `SKIP`, which never sets the exit code. m1 without probe files reports **all green with three SKIPs**. | New check **`calibration_learned`** (FAIL, not SKIP) for a leg that declares itself a learning scenario (`--learning`, or naming the check in `--checks`): FAIL on any of `fit samples == 0`, no `[[profile]]` in `calibration.after.toml`, or a peak `unit_budget` that never left the seed. All three numbers were already printed by `ramp_progress` as INFO. |
| H2 | `slope_accuracy` could not distinguish "no store was written" (a *result*) from "no probe file was passed" (a harness omission). Both were `SKIP`. | The two are split, in `slope_accuracy`, `utilization` and `persistence`: no store / no worker admitted → **WARN**, or **FAIL** under `--learning`; no probe / no log → **SKIP with a pointer** to what to pass. |
| H3 | `hog_tracking` was INFO only. `external_mb 0..0 MiB` while a hog holds 30 720 MiB is not an observation. | A FAIL form that cannot re-fail the known B2 staleness: FAIL only when the hog held **≥ 1 GiB for more than 60 s** *and* `external_mb` **never moved at all** across the recording; a GPU that moves late is still INFO. Calibrated on run1's 14 hog legs — only m2's S4b FAILs. |
| H4 | `ledger_invariant`'s strict form PASSes on a completely broken ledger (m2: 0 of 498). | Documented in `--list-checks` and the tooling README: **the check that decides safety is `grant_safety`'s oracle clause, and it needs `vramrec.jsonl`.** `grant_safety` now reports **WARN**, not PASS, when that clause could not run. |

## 5. Targeted probes from the code reading

The three code maps (host ledger, worker, deployment) surfaced concrete
suspicions. Each is pinned to the scenario that will confirm or clear it,
so the run produces an answer for every one rather than a vague sense of
risk.

**The last column is this host's answer**, with the number that decided it,
extended once per leg group: run1 (2026-09-03), then run2's Phases A and C on
`65fd2f82`, then the Phase D1 leg, the ground-truth probes and the fix round,
then Phase A′ and S4a on `34a591aa`, and finally Phase 3 on `63a8ad73`
(2026-09-05). Evidence is the runlog of the scenario named in the third column,
under `tools/calibration-protocol/results/`; verdicts are collected in the two
reports. Every row now has a leg behind it. A fix that is landed and verified
but not yet leg-measured never marks a row cleared.

| Id | Suspicion (file) | Scenario | Status after run1, then run2 |
|---|---|---|---|
| B1 | A full GPU yields `mb = 0` grants; the worker's `clamp_to_live_memory` and `maybe_shrink` treat `grant_mb <= 0` as "no reservation", so pre-fit units are memory-blind exactly when the GPU is fullest (`ledger.rs:2879`, `packing.py:450`). | S1, S4c | **Confirmed.** 51/51 grants `mb = 0` on a full GPU (S1); 4 more at 8 GB free (S4d), where the ledger's own `unit_budget = 1` — 5 512 one-image batches — was the real cost, not the blinded worker. The worker's live clamp kept working throughout. **Run2 (S6-contend, R5): 3 memory-blind grants of 2 610** against run1's 113, and **0** samples at `limit_mb = 0` against 367 — at 4 GB free the grant is now `unit_budget = 60 mb = 3 040 reserve_mb = 1 024 reserve_rule = "capped_default"`. The cost is throughput (finding C6). **Run2, S4a on `34a591aa` (`S4a-v2`): closed at 12 GB free.** `reserve_rule = "capped_default"`, `reserve_mb` **exactly 1 024** against run1's 8 579, `limit_mb` **11 071** against 3 517, **0** memory-blind grants and **0** samples at `limit_mb = 0`; the executed batch is **83 items where run1 ran 11**. What binds instead is the worker's live clamp, which double-counts our own allocator pool (finding **S4a-A2**, §4 S4). **Run2, S4d on `63a8ad73` (`S4d-v2`): closed at 8 GB free too.** **0 of 18** grants memory-blind (smallest `mb = 53`), `limit_mb` min **6 975** over 1 533 samples with **0** at zero, `reserve_mb` 1 024 while the hog is up and 496–504 after it lets go, and 16 000/16 000 items at 0.92× of S2. S4c (the 10 s spike) is the only profile of this scenario never run. |
| B2 | External refresh is `nvidia-smi` free/total only, grant-triggered, 10 s staleness, no poller: a quiet GPU's picture can be minutes old (`ledger.rs:865-880`). | S4b, S4d (latency numbers) | **Confirmed, and worse than stated.** `external_mb` is a *window-boundary* quantity: **0** host probes in ~2.5 h of Phase 3 recording, sample ages of 85.5 s with a worker resident and 166.9 s overall, a +30 GB step taking 31.5 s to reach `/health` and a 53 GB/10 s spike never appearing at all. True of busy GPUs with long windows, not just quiet ones (T3). **Run2, S4b on `63a8ad73` (`S4b-v2`): measured, and the mechanism is bounded from below by the window in flight.** R5's per-batch `free_mb` ingest works — the +30 GB step reaches the grant line at **22.4 s** and `/health.external_mb` at 22.5 s (`external_sample_age_ms` 43 840 → **278**), against run1's 31.0 s — but the readings are applied at `token.finish`, and the window here was one 878-image batch lasting **21.3 s**, so 22.4 s = 21.3 s of in-flight batch + 1.1 s of ledger work. The suspicion is therefore **restated, not cleared**: `external_mb` is no longer a *window-boundary* quantity by construction, but it is still published at window granularity, and the fix for the residue is a mid-window publication at a batch boundary (finding **S4b-A1**, a user decision). Every one of the 43 `oracle_agreement` breaches (of 2 366) falls inside that same 22.4 s window, and `grant_safety` is 12/12 against live free memory. |
| B3 | Deleting `calibration.toml` under a running server can be undone by the next debounced write; a corrupt file is silently replaced (`calibration.rs:586`, `:1219`). | S3 | **Confirmed**, with a nuance: the profile is read at worker *admission*, so a delete before the first load resurrects a **new, worse** profile (anchor 512 → 32, samples 10 → 4, a spurious knee). The write is a whole-profile replacement, not a merge (N6). |
| B4 | A persisted anchor is a permanent floor across driver/torch patch bumps matched by `major.minor`; the OOM backstop is the only protection (`ledger.rs:2295`). | S3 variant: edit the stored anchor to 4× and observe the first window | **Confirmed on three legs.** The stored anchor is acted on immediately and is a permanent floor: a poisoned slope self-heals by refitting, the anchor never does, and an OOM *caused by* the anchor did not lower it (N5). Never cross-checked against `sample_units`. |
| B5 | A shipped baseline knee can only ratchet down within a run (frontier guard) | Not testable until baselines ship; note only | Unchanged — **not testable** until a shipped baseline exists. |
| B6 | Index-form `CUDA_VISIBLE_DEVICES` silently disables the ledger at INFO (`gpu.rs:846`); Docker users commonly set it. | S7 (C3), S11 | **Confirmed**, one INFO line, exact wording in the S7-C3 runlog. The off-switch costs budgeting, not placement: the worker still ran on the physical GPU. |
| B7 | Discrete-GPU worker death relearns nothing; respawn is admitted at the anchor again (`ledger.rs:741`). | S5 | **Confirmed twice**, including with a *learned* anchor: after `kill -9` the respawn's first grant was byte-identical to the run's first, `seeded_from_store=true`, anchor unchanged. Right behaviour on a discrete GPU; cost 2 lost requests and 3.4 s. No `unified_device_death` negative ever appeared. **Run2 (S5-dying-job): unchanged in the ledger, transformed in the job.** 63 spawns / 63 deaths for 2 000 items over 137 s, still 0 `unified_device_death` and `deflation = 0` — but the died-on window's items are now **re-queued once** (2 000 lines, `requeued=2000`) and, when they fail again, listed individually by `/api/jobs/data/failures` (R2). |
| B8 | Deflation is unbounded and recovers one level per 3 clean windows; a few unfittable items can pin a model at 1-unit batches for a long time (`ledger.rs:725`). | S5 | **Confirmed and quantified.** Uncapped: **8 074 levels in 148 s** (54.6/s). Recovery: one level per three clean windows, **7.04 levels/s** measured → a 2-minute fault costs ~15.6 min at **0.43×** throughput. Every deflated grant still offered 96 GB of an empty GPU. **Run2 (S5-oomtimed): closed by R4.** The counter is capped at `ceil(log2(max(anchor, seed))) + 1` = **4** on that leg, and it now repays **by time** as well as by clean windows: 4 → 0 over 120 s of idleness, one level per 30.0 s with a DEBUG line each, and 4 → 2 → 0 in **0.5 s** once windows flowed again. |
| B9 | Container without `--pid=host`: base falls to `free_delta`, contaminated by concurrent activity in the load window (`memory.py:403`). | S11 | **Refuted on this platform.** Driver 590.48.01 + NVIDIA Container Toolkit: NVML resolves the namespace-local pid, `base_method = nvml` in C4 *and* C5, base error **0.00 %** either way. Keep it as a per-platform check (§9), never as an assumption. |
| B10 | Post-trim fit samples use a stale `reserved_at_load`; Theil–Sen may refuse and keep an old fit forever (`ledger.rs:3286`, `:3378`). | S6 (trim) then S2 re-run | **Not exercised.** No leg produced a post-trim fit sample against a stale `reserved_at_load`; the S6 trim legs ended in the model being dropped (B17) or the run ending. Still open, and it needs the S2 re-run after a trim that the plan calls for. |
| B11 | `message_reports_oom` matches any line containing "out of memory" (`ledger.rs:4119`). | S5 | **Confirmed decisively.** A plain `ValueError` whose text contains "out of memory" produced **15 negative settles with `reason="oom"` on a GPU with 96 GB free**; the same fault worded differently produced zero. Probe it with `calibfixture/failbatch_oomtext_cuda` — the `out of memory.png` file-name vector does not exist (the worker gets bytes, never names). **Run2 (S5-failbatch-oomtext): closed by R3. 0 negatives** of 26 windows, `oom=false` on 25 of 25 fallbacks, `deflation = 0` throughout. The bare substring is gone: classification is by typed exception, then explicit marker, then a **closed** list of driver-shaped messages that also has to be corroborated by the worker's live free reading at the failure. Residue: on an *idle* GPU that corroboration vetoes essentially every `message_pattern` OOM, because `grant.mb` is the whole GPU (S5-oom2nd, finding C3). **Fix round: the classification is now visible** — every OOM negative emits one INFO line naming `source` (`typed_exception` \| `marker` \| `message_pattern` \| `error_frame` \| `unclassified`), `exception`, `trust` (`trusted` \| `corroborated` \| `unopposed`), `free_mb_at_failure` and `grant_mb`, immediately above the window's own WARN, and `analyze.py`'s `failures` check tallies the `source/trust` pairs (`672aa85a`, `18f2aa1b`, `62a092c9`). That is what makes C3's two shapes distinguishable in a recording: `message_pattern/corroborated` on a tight GPU against `message_pattern/unopposed` where there is nothing to weigh. C3 itself is unchanged and stands as a fixture artefact of an idle GPU. |
| B12 | No log line for grant issued, batch chosen, negative applied, ramp step, refresh result or store write; `/health` polling is the only reconstruction path. | Every scenario; see G1 | **Cleared.** The `49822c8b` lines reconstruct a whole run; `analyze.py` is built on them. |
| B13 | A wedged `nvidia-smi` costs a 5 s blocking thread every 10 s per GPU. | S13 | **Refuted, with numbers.** 2 host probes in 240 s under three-model load (7 in a forced churn case), gateway threads 51–54 with and without a wedged binary, max **1** concurrent `nvidia-smi`, latency within noise. Single-flight per GPU + a 10 s failure backoff + **nothing polls**. Residual: the timed-out child is abandoned rather than killed (F13). |
| B14 | Single-model hosts never confirm the +0.15 unconfirmed margin (`ledger.rs:3346`). | S2 (record effective margin) | **Restated.** The +0.15 surcharge is retired at the **5th fit sample** (2.5 s into a job) on the same single-model host S1 said could never retire it. The risk is models that **never accumulate fit samples** — a squeezed GPU, whose windows are clamped to 1 unit — not host shape. Record `effective_margin` with `fit.samples`. |
| B15 | Worker death → immediate respawn on the next predict with no backoff or attempt cap (`manager.rs:1068-1085`, `:1152-1197`); a job of N items can pay N loads before failing. | S5 | **Confirmed on the predict path, restated for jobs.** `dies_on_load` gave 93 load attempts in 182 s, one per request, **no backoff, no cap**. Inside a job the same condition stopped after **4** attempts / 259 s — and that cap is `reqwest_retry`'s three retries, not the manager's; the load failure aborts the whole job. "A job of N items can pay N loads" was not reproduced. **Run2 (S5-dieonload): closed by R9.** 7 attempts in 182 s on a ladder of 2, 4, 8, 16, 32, 64, 128 s; 13 145 refusals as **503** with `retry-after` and `detail.kind = "load_cooldown"`; `/health.load_cooldowns[]` names the model. The job aborts in **9.5 s after 2 attempts**. Residue: the job's `failure_reason` still loses the model id and the retry time (finding C4). **Fix round: C4 is closed** (`20bd1536`) — `load_model_all` keeps the most informative endpoint error rather than the last (a typed cooldown verdict is no longer overwritten by a plain 500 from another endpoint), its context names the model, and the reason is either the structured cooldown (model, consecutive failures, `retry_at`, last error) or the **whole** cause chain. |
| B16 | `REQUEST_UNIT_BUDGET = 64` caps every item/count window a job can produce, so anchors and knees learned from jobs never exceed 64 regardless of VRAM (`jobs/extraction.rs:65`). | S2 | **Cleared by G7.** Job-driven in-flight items reached **957** with `unit_budget = 512`; the floor of 64 was left behind at window 7. Jobs now calibrate *better* than fixed large `loadgen` requests (which feed the knee estimator instead). **Run2: the premise is falsified and a different ceiling took its place.** `REQUEST_UNIT_BUDGET = 64` is a chunk bound *within one item's work units*; an image item has exactly one, so a job sends **one item per request** (1 999 requests for 2 000 items) and 4 096 units means 4 096 concurrent requests. What actually capped the run2 ramp at `max_units_measured = 136` was `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200` over a pool that shares one h2 connection (finding **S1**). Re-measure after the S1 fix. **Fix round: the 200 is now an assertion in the suite** (`4e587635` offers 400 concurrent predicts and measures a peak of 200 over one socket), the server advertises a limit it chose (512), the pool is 64 real connection lanes recruited by load, the gate follows the published desired-in-flight figure within `[256, 4096]`, and a window forms after the previous one's refills land. **Phase A′ measured it, and S1 is closed** (`results/run2/S2-wdvit-v2/`, binary `34a591aa`): `max_units_measured` reached **439** on a monotone ramp (1,2,4,8,16,17,34,68,136,272,439) with the budget published at **878** and the 136/64 alternation gone; the gate follows `desired_in_flight_items` **256 → 3 264** with a peak of **1 382** requests in flight over **22** of 64 lanes; `queue_bound_windows` is **6**, every one a ramp step the job could not yet fill or the corpus-exhausting last window; and the sockets are bounded at **44 established = 2 × 22 lanes** (90 fds of a 524 288 limit). The end of the ramp is now the 2 000-item corpus, not the transport. `/health` answers all of it directly — `healthrec.py` had to be taught to keep those sections (`362ec437`). |
| B17 | `TRIM_DEADLINE = 60 s` is fixed and fatal for the whole model (`worker.rs:113`, `dispatch.rs:803`). | S6 | **Confirmed, and it fires at ~20 s, not 60 s.** The worker died at **20.26 s** into a hung trim — a teardown path racing the trim (`unload_grace` 10 s + `terminate_grace` 5 s), not the `TRIM_DEADLINE`. Client got a bare 500 after 18.6 s; a later predict reloaded transparently (P5-7). |
| B18 | The manager's load lock is taken on every predict (`manager.rs:1161`), so any load (up to `load_secs`) stalls every model. | S6 | **Confirmed at 100 % of the load.** An 11.865 s cached load stalled every in-flight predict for **11.885–11.894 s** — **28×** the 421 ms p50 — and `load_secs` is 600 s (P5-3). **Run2 (S6-b18-loadstall): closed by R6.** A 13.388 s load costs the resident model p50 551 ms, max **951 ms** — **1.86×** its own p50 and **7.1 %** of the load — with 0 requests over 3 s attributable to it. The global lock is retired; loads are per model, admitted per GPU. |
| B19 | The CPU device reads host RAM and is cgroup-blind (`cpu.rs`). | S12 | **Confirmed, with the number.** GPU total 64 137 MiB, budget 48 102 MiB, cgroup `memory.max` 16 384 MiB = **2.94× overcommit**; nothing in the CPU path reads `memory.max`. The death-negative path converges (32 → 16 → 8 → 4) but only **across** job passes, and treats every death — not just an OOM kill — as a memory negative. |
| B20 | The unpriced path bounds windows by `default_max_batch` (32) rather than seed-sized batches as the design says (`dispatch.rs:37-48`). | S7 (C3), S13 | **Confirmed in shape, corrected in number: the bound is the registry's `metadata.default_batch_size` (64 for `tags`), not `default_max_batch` (32),** which applies only where a group declares none. Seen on two independent unpriced paths (C3 and the no-inventory case in S13). |
| B21 | Prewarm-parked workers are invisible to the ledger; a `prepare()` that initialises CUDA becomes margin-inflated external usage. | S6 (compare `external_mb` with and without prewarm) | **Unreachable with shipped impls.** No impl defines `prepare()`, so parked prewarm workers never initialise CUDA and hold **0 MiB** of VRAM; the pool's real cost is ~1.7 GiB of host RAM (~400–455 MiB RSS each). Re-test if a CUDA-touching `prepare()` ever ships. |
| B22 | `/health` renamed `last_effective_cap` to `last_grant_units`; check the UI submodule and any consumer. | S14 | **Cleared.** `models[].last_grant_units` is present and populated and nothing in the run read `last_effective_cap`. (Unrelated follow-up: the `ui` submodule's generated types need `npm run gen:api` before release, because G7 added a response header.) |
| B23 | `accelerator_backend(Auto)` keys profiles as `cpu` while the probe behaves as CUDA on the validation-failure path (`http.rs:271-278`). | S1 (inspect the `backend` key written to `calibration.toml`) | **Cleared.** The written profile carries `backend = "cuda"`. |
| W1 | Pixel pricing uses raw submitted dimensions, not the model's canvas (`packing.py:300`). | S8 | **Confirmed, and it is more than a pricing quirk:** the *fitted slope* becomes a function of the corpus (nemotron fitted **4.33×** the probe's on a mixed corpus), utilization fell to **0.08** of the boundary, and 58 of 110 batches held a single item. The knee moves with it, since it is expressed in priced units. **Run2: the mechanism is in place.** R7 prices every pixel item at `min(raw, canvas_pixels)` on **both** sides (`packing.price_inputs` and `dispatch::estimate_input_units`), the canvas is resolved once per load with the registry winning over the impl's own reading, and it is visible in force — `canvas_pixels=1835008` on nemotron in `S6-b18-loadstall`. All seven shipped `pixel` ids carry `epoch = 2`, so their run1 profiles are ignored and re-measured. **Run2, S8 pixmix on `63a8ad73` (`S8-pixmix`): the slope is decided and W1 is cleared as a defect.** The fit lands **+0.07 %** from the probe of the group the corpus is dominated by (0.0012809 against `img-0.3mp`'s 0.0012800) where the same fit against `img-1mp` reads 4.78×, and the 50 × 20 MP items pack into **two windows of 46 and 27** where run1 gave 58 of 110 batches a single item. `utilization` reads 0.07 and FAILs on a corpus that ran out while the ramp was still doubling, not on the pricing. **Run2, after the D1 leg and the probes: the "corpus-dependent slope" is confirmed as a *measurement* rule, and run1's 4.33× is retracted as a group mismatch.** A `pixel`-priced slope is comparable only to a probe of the same image group: run1's `S8-pixmix` fit is **0.907×** the probe of the group its `sample_units` are multiples of, and run2's easyOCR fit matches its group probe **to seventeen digits** (ratio 1.0000). The cap works and is provable from the stored windows (`S8-ocr-C7-calm-b`: `sample_units` = 5 ×, 9 ×, 11 × 6 553 600), and it packs: 49–57 batches capped against 61–64 uncapped. Two residues: the cap flattened the *price* that `plan_batches` sorts on, so a mixed batch was padded to the largest member's raw dimensions (**D1-b**, fixed — raw-size tiebreaker, and the impl bounds its own detector batch); and for a **tiled VLM** the capped per-unit cost still spans **8.35×** over one corpus, because nemotron's memory follows the tile grid its aspect ratio picks, not the pixel count (a 0.3 MP 4:3 thumbnail costs the same seven tiles as a 20 MP 4:3 sheet). Tile-based pricing would be exact and is a user design decision. |
| W2 | Measurement brackets CPU decode time, so slow-decoding inputs can trip the 0.4 collapse ratio spuriously (`packing.py:622`). | S8 (20 MP PNGs), S4e | **Not reproduced** in 20 legs. The mixed-resolution corpus **disarms** the comparator (4 × "retiring the throughput comparator after 8 non-comparable batches") rather than fooling it, and `throughput_collapse` never fired on decode time. Retires as a suspicion on this evidence; the tier encodes JPEG, so a slower codec is untested. **Run2 (Phase D1): one `throughput_collapse` negative in eleven legs**, in `S8-ocr-C7-calm-b`, inside a single window while the host was reclaiming memory (that leg also carries the run's only `oracle_agreement` breach, 10 249 MiB on 1 of 400 joined samples). Every other leg: 0 negatives, 19 MiB worst disagreement. Not decode time and not the comparator's tier — host pressure, which nothing in `analyze.py` surfaces; the run2 report's instrument-gap list asks for a `host_pressure` row for exactly this reason. A separate, unrelated source of spurious collapse verdicts *was* found and fixed: a batch trimmed by easyOCR's **shape ceiling** runs a fraction of the work and tripped the collapse flag into a deflation on an empty GPU (`37f5c764`). |
| W3 | After an absorbed OOM the throughput comparator is not reset, so the regrowth batch may be flagged as a collapse (`packing.py`, `utils.py`). | S5 | **Open.** Nothing in run1 produced an impl-side absorbed OOM under real pressure: the fixtures' batches take 0.5 ms and the one real failure was a decode bomb on an empty GPU. Needs a leg that OOMs MobileCLIP for real (hog + large batch), i.e. S4c-style pressure. |
| W4 | `alloc_delta` uses a fixed 500 MiB context estimate; Blackwell contexts may exceed it. | S11 (C4 base error) | **Open, and unreachable on this platform.** The degraded base tiers were never entered (see B9), so the fixed 500 MiB context estimate against a **measured 666–668 MiB** context is still untested. Live wherever NVML hides the pid: older drivers, WSL, WDDM, ROCm containers, podman with another toolkit. **Run2: the constant is gone, the tier is still unreached.** R8 replaces the fixed 500 MiB with a measurement — the GPU free-memory delta across this process's first CUDA initialisation, minus the allocator pool at that instant, taken by a thread that *watches* `torch.cuda.is_initialized()` rather than calling into CUDA. On this host `base_method` is still `nvml` on every leg, so the new code is exercised by unit tests only; W4 stays open on every platform. |
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
| Throughput vs C0 | LogRecord | ≥ 0.9 × idle GPU; ≥ 0.4 × under S4e |
| Persistence | calibration.toml | written ≤ 30 s after anchor advance; resumes on restart |
| Job outcome | `/api/jobs/queue` outcomes, failures | completed; failures only for poisoned items |
| Calibration learned (a leg that declares `--learning`) | healthrec × calibration.toml | fit samples > 0, ≥ 1 `[[profile]]` on disk, and peak `unit_budget` above the seed — **FAIL**, never SKIP |
| External tracking under a hog | healthrec × hog.jsonl | report-only, **except**: FAIL when the hog held ≥ 1 GiB for > 60 s and `external_mb` never moved at all |

**The check that decides safety is `grant_safety`, and within it the oracle
clause** — every `issued a memory grant` line joined to `vramrec.jsonl` and
compared with the GPU's *live free memory* at that instant. It needs
`vramrec.jsonl`, so **`grant_safety` reports WARN, not PASS, when that file
is absent**, and a scenario without it has not had its safety verified. Its
other clause only re-checks the ledger's arithmetic against itself, and
`ledger_invariant`'s strict form is no substitute: S15's m2 zeroed
`external`, so `limit_mb` became `total_mb` and `ledger_invariant` PASSed on
0 of 498 GPU-samples while the oracle clause caught 335 of 335 grants.

**`SKIP` means an input was not recorded — never that the run produced no
measurement.** A SKIP never sets the exit code, so any check whose inputs
a *fault* can delete must report WARN or FAIL instead (H1/H2 above).

## 7. Execution plan for the autonomous session

Estimated wall time 24–36 h including the soak; agent-driven with the
main session orchestrating and subagents running one scenario each and
returning a filled `runlog.md`. Phases are sequential; scenarios inside
a phase may run in parallel only when they use different GPUs or
different root directories and do not share `hog.py`.

| Phase | Content | Needs | Run1 actual |
|---|---|---|---|
| 0 Prep (~2 h) | Release build of the branch; master worktree build with its own venv; test-group sync; write the tools in §2; generate the corpora; **oracle calibration** against known allocations; CUDA-touching fixture impl. | Nothing stopped yet. | Split into three parallel tracks (A feedback signal + verifier, B tooling, C environment). The GPU half of the oracle calibration had to be **deferred to Phase 2a**, because SGLang held 95 GB of each GPU. |
| 1 Smoke (~1 h) | S0, S1 (with SGLang running), S14. | — | ~1 h. `cargo test --release` 335 s wall (241 s of tests), pytest 9 s, S1's job 22 s, six S14 model jobs. |
| 2 Idle-GPU truth (~3 h) | Stop SGLang. `ceiling_probe` for the four models. S2 for three models, S3. | SGLang down from here. | Split in two: **2a** (stop SGLang, oracle 10/40 GB on both GPUs + 16 GiB RAM, bisect smokes, five ceiling probes) ~2 h, and **2b** (S2 ×3 + the W5 and loadgen legs, S3 + B3/B4) ~1 h. A refinement probe near wd-vit's boundary costs 60–170 s, which is why `--bisect-start` / `--bisect-budget` exist. |
| 3 Pressure (~4 h) | S4a–S4g. | — | ~2.5 h of recording for seven profiles — but two are fixed-length by construction (S4e 10 min, S4f 40 min), so the phase cannot be compressed much below that. |
| 4 Faults and dimensions (~3 h) | S5, S8. | — | **20 legs in 1 h 03 min**, because the fixtures fail in milliseconds. The long pole is the easyOCR baseline (C0 takes 468 s where C7 takes 55 s). |
| 5 Concurrency (~2 h) | S6, S7. | — | **5 legs in 32 min.** |
| 6 Deployment (~4 h incl. image builds) | S10, S11, S12. | Docker builds. | ~3.5 h. Image builds with a warm layer cache: `calib-cuda` 349 s / 9.43 GB, `calib-cpu` 54 s / 3.49 GB, master baselines 285 s and 118 s. Rebuild the images at the tip if product commits landed since. |
| 7 Robustness and self-test (~3 h) | S13, S15. | Throwaway branches. | Split: **7a** (S13's three cases + the F8 SIGKILL root-cause investigation the earlier phases had deferred) ~1 h; **7b** (S15 mutations, plus the re-run legs the fixes require) was still to run when this revision was written. |
| 7a F8 investigation | Root-cause the unexplained worker SIGKILLs seen from Phase 5 on: gateway under `strace -f -e trace=clone,clone3,exit,exit_group,kill,tgkill`, a standalone C control for the kernel behaviour being suspected, and negative controls that suppress the suspected trigger. | A reproducer that dies within minutes (here: a cold GPU + an oscillating hog). | Found it: `PR_SET_PDEATHSIG` is **thread**-scoped and the load-path `block_in_place` demotes the forking thread into Tokio's blocking pool, which reaps it after 10 s idle — 8/8 deaths Δ 1–3 ms from the forking thread's exit (**F11**). Budget a phase like this whenever deaths have no traceback and no kernel OOM. |
| 7b Re-run legs after fixes | Every fix landed mid-run invalidates the legs that ran before it. Re-run: the F8 leg (expect 0 deaths, 16 000/16 000), a cold S2 ramp, and the S11-C4 job on an image rebuilt at the tip (F6 closure: 2 000/2 000, peak fds well under the limit). Then S15. | A release build at the tip. | Done 2026-09-03 18:11–20:00 UTC on `dc613400`: F8 leg 0 deaths / 16 000 of 16 000 / 0 kill syscalls; S2 byte-identical to Phase 2b; S11-C4 2 000 of 2 000 with soft `nofile` raised to 524 288 in the container; S15 caught 3 of 3 mutations (holes H1–H5 closed in `3bf4ba76`). |
| 8 Soak (8–12 h) | S9. | Overnight. | Done 2026-09-03 19:37 → 2026-09-04 03:47 UTC (8 h 09 m) on `6a5e6799`: 13 cycles, 52 of 52 jobs, 0 deaths, 0 ERROR lines, store 6 635 B with rings ≤ 64, RSS mean-reverting, throughput 1.03×/1.08×; wd-vit `knee_units = 1` persisted for 7 h 55 m (finding F-A, T1/N1). `vramrec.py --interval 1` was used. |
| 9 Report (~1 h) | Aggregate verdicts, findings list with severity, portability notes, recommendation. Restart SGLang. | — | Done 2026-09-04: `docs/batch-calibration-run1-report.md`; plan revised in `4d1f0af5`/`960edf61`/`4ea6d5f6`; results indexed in `results/run1/README.md`; SGLang restarted 03:55 UTC. |

**Run1's own lesson about phase order.** Every product fix landed by an
earlier phase invalidates the legs that ran before it, so the plan needs
an explicit re-run budget (phase 7b above), and every runlog must record
**which binary commit it ran on** — not just the repo commit, since the
release binary is often several commits behind the tree. Run1's phases
ran on `10be7442` (1), `918ec170` (2b, 3), `d17e3854` (4), `443fa088`
(5), `eb398d69` (6) and `b9d32324`/code `c8d64a5a` (7a).

Rules for the session:
- **Fix policy** (decision 5): a low-discretion fix — a wrong constant, a
  missing clamp, a log line, a crash on an edge the design already covers —
  is made by the executing agent on its own commit, reviewed by a *separate*
  verifier and listed in the report with the review outcome. Anything that
  changes a feature, a default, user-visible behaviour or a design decision
  is **not** fixed: it becomes a finding with options.
- Every scenario's `runlog.md` is written before the next starts, with the
  exact commands, so any result can be reproduced by hand.
- A scenario that cannot be completed is marked blocked with the reason,
  never skipped silently.
- Nothing in `python/inferio/config/` or `config/server/` is edited; all
  overrides go through user registry files and a copied server TOML.

## 8. Findings already made while writing this plan

These come from reading, not running, and are recorded so the run can
confirm or refute them rather than rediscover them.

- **G1 Observability gap — resolved in `49822c8b`.** The ledger emitted no
  log line for a grant, a negative sample, a ramp step, a refit, an external
  refresh, a worker admission or a store write, so reconstruction relied on
  `/health` polling. The commit adds one structured line per event (the names
  are in `analyze.py`'s parser, which is now built on them); `healthrec.py`
  stays as the cross-check.
- **G2 Docker `pid` namespace.** The shipped compose files do not set
  `pid: host`, so the plug-and-play target always takes the degraded
  base-measurement tier. S11 measures the cost; the likely outcome is a
  recommendation to add it (or to document why not).
- **G3 Full-GPU behaviour is unclamped pre-fit** (B1): the first windows on
  a GPU another process nearly fills are protected only by the OOM backstop.
- **G4 Manual override.** There is no configuration switch to disable
  admission; the only off-switches are side effects (index-form
  `CUDA_VISIBLE_DEVICES`, a hidden `nvidia-smi`). Worth a deliberate decision
  before release, given the feature is on for everyone.
- **G5 The migration does not preserve the old numbers.** Fine per the
  design's reasoning, but the release notes must say so; the stamp insert is
  the one step whose failure blocks startup.
- **G6 Failure containment is weaker than the design implies** — no respawn
  backoff (B15), a fatal trim deadline (B17), predicts serialised behind
  loads (B18). None is a calibration bug as such, but calibration makes them
  reachable more often, so the run measures each.
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
  64 regardless of VRAM: on a 97 GB GPU (or two), a wd-tagger or CLIP
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

  **Resolution (user decision, 2026-09-02): implement (b) before the run.**
  Implemented on this branch by *"Let core's in-flight unit budget follow the
  server's desired item count"* (`10be7442`) and amended by finding T5
  (`22eb33f9`) and finding F6 (`d5e42c78`, sweep `13e8850f`, docs
  `25153334`). What was built, in one line each — the full description is
  `docs/inferio-worker-protocol.md` ("Desired in-flight items") and
  `openapi.json`:
  - **Carrier**: an HTTP *response header*,
    `x-panoptikon-desired-in-flight-items`, not a body field, because
    predict answers in three encodings and only the JSON envelope could hold
    a scalar — a body field would be absent for exactly the image and
    embedding models G7 is about.
  - **Derivation** (`dispatch::desired_in_flight_items`): the ledger's window
    target in units × the just-formed window's items-per-unit ratio × a slack
    of 2, bounded by `MAX_WINDOW_BYTES` through that window's bytes-per-item.
    Under T5 the unit target is the *granted* budget's window depth whenever
    the grant was flagged `squeezed`.
  - **Core** (`jobs/extraction.rs`): the per-job unit semaphore is a
    `UnitBudget` resized toward the figure on each response. Floor **64**
    (`MIN_IN_FLIGHT_UNITS`, a deadlock bound, since one chunked request
    acquires up to 64 permits at once), ceiling
    `max(intermediate_budget_kib / NOMINAL_UNIT_KIB, loader_concurrency × 64)`
    with `NOMINAL_UNIT_KIB = 256`, i.e. **4 096 units at the shipped
    defaults**. An absent header means no change, not a drop to the floor,
    so a server that never sends it leaves the job at 64 — the pre-feature
    behaviour. `REQUEST_UNIT_BUDGET = 64` stays as the per-request chunk and
    `ISOLATION_MAX_BATCH` is untouched; no grant arithmetic, ramp, knee or
    user-visible default changed.
  - **The F6 amendment**: the ceiling is also clamped by the process's
    descriptor budget, because with local inference every in-flight predict
    is loopback HTTP inside one process and costs **two** sockets in one
    descriptor table. `main` raises the soft `RLIMIT_NOFILE` to the hard
    limit before the runtime is built (`panoptikon/src/rlimit.rs`), and
    `in_flight_unit_ceiling` gains
    `by_fds = (soft_nofile − FD_RESERVE) / FDS_PER_IN_FLIGHT_ITEM` with
    `FDS_PER_IN_FLIGHT_ITEM = 2` and `FD_RESERVE = 256`, applied as a **cap**
    on the other terms rather than a third term to maximise with, because
    descriptors are the one resource a job can exhaust process-wide. The
    floor of 64 still wins under it, with a WARN when the budget is below
    `64 × 2 + 256 = 384`. Not changed: the server-side header derivation — a
    caller bounds the figure by its own descriptor budget, which the
    orchestrator cannot know.

## 8b. Noted for after the run: throughput as a calibration target

The user's framing: find the optimal batch size, adjust it dynamically so
nothing runs out of memory, keep jobs fast, and preserve the headroom the
user asked for. On a GPU the optimum is usually "as much memory as
possible"; on CPU and MPS it is often much smaller. The design treats memory
as the target and throughput only as a **cap** (the knee), never *seeking*
the throughput optimum, so on unified-memory devices the memory-derived
budget can be far above the speed optimum. Out of scope for this run,
expected to be necessary for production; S2's throughput ring and the CPU
device's knee samples (S12) are the data that will show how far the two
optima sit apart per platform.

## 9. Portability: what changes per platform

The scenarios are identical everywhere; the oracle and the pressure
generator differ. The autonomous run on this host produces the tools and
the scenario definitions; each platform pass then needs only the items
below, and its report uses the same verdict table.

| Platform | Oracle (`vramrec.py`) | Pressure (`hog.py`) | Known differences to expect |
|---|---|---|---|
| Ubuntu NAS, RTX 3090 (Ampere, CUDA) | Same as here | Same | Single GPU; smaller headroom makes S4 tighter; validates the ramp on a 24 GB card and the shipped baseline story once one exists. |
| Windows desktop, dual RTX 5090 (WDDM) | NVML per-process returns N/A on WDDM; oracle records GPU-level used/free plus **`PDH` "GPU Process Memory" counters** (or `nvidia-smi --query-compute-apps`, which does work on WDDM for used memory) for attribution; expect base tier `free_delta`. | Same (torch) | Sysmem fallback: over-admission shows as throughput collapse, not OOM; S4c must watch `throughput_collapse` flags and per-batch `duration_ms`; S15 mutation 1 is the key sensitivity test there. Also run once with the driver's "Prefer No Sysmem Fallback". Two GPUs: S7 is the monitor-asymmetry test the design defers. |
| MacBook Pro M3 Max 128 GB (MPS) | No per-process GPU counter; oracle records `vm_stat`/psutil RAM, `sysctl iogpu.wired_limit_mb`, and the worker's own `driver_allocated` from `/health`; ground truth for base comes from `ceiling_probe.py` in-process (`torch.mps.driver_allocated_memory()` is per-process by construction). | RAM hog (numpy) and an MPS hog (torch tensors on `mps`) | Total adoption from the first load (`GPU-MPS`, recommended-max), re-adoption after raising the wired limit under a running gateway, watermark env 1.0/1.0, near-ceiling GC bias, jetsam death-as-negative (S12 analogue). The field-pass list in `unified-memory-admission.md` maps onto S1/S3/S4/S12. |
| BC-250 (ROCm APU) | amdgpu sysfs (`mem_info_vram_*`, `mem_info_gtt_*`) plus DRM fdinfo per PID; `rocm-smi` as a second reader | torch on HIP | Which total HIP reports (either-of cross-check), GTT spill as slowdown rather than OOM, OOM string forms, `HSA_OVERRIDE_GFX_VERSION` passthrough. The ROCm parity doc's "first numbers to measure" become S1's checklist. |
| Linux Desktop / Nix | Same as here | Same | Only the packaging path differs; S14 plus S2 suffice. |

A platform passes when S1, S2, S3, S4a–d, S5, S14 and the platform's own
field-pass items pass; S6–S13 are run once here and on Windows (the
second multi-GPU host), and S9 once here.

### Per-platform checks run1 added

Each is a one-line check of a fact that cannot be carried over from this host
and that silently changes what a later scenario measures. The same five, with
the numbers behind them, are `docs/batch-calibration-run1-report.md` §8; run2
adds six more in `docs/batch-calibration-run2-report.md` §10.

| Check | Why it is platform-specific |
|---|---|
| **`base_method` at load** (`grep 'base_method'`, or the `NVML lists no process with pid` warning) | Driver- and container-runtime-dependent, not container-shape-dependent. B9's predicted degradation did not happen here, so the degraded tiers — and W4's fixed 500 MiB context estimate against a measured 666–668 MiB context — are **untested everywhere so far**. The platform that reports `free_delta` or `alloc_delta` is the one that finally tests W4 |
| **The container's `nofile` soft limit** and the gateway's **peak descriptor count** on a job of ≥ ~1 000 items | containerd defaults the soft limit to 1024 while the daemon has 524 288. The branch raises its own soft limit and clamps the in-flight ceiling by the descriptor budget, so what a new platform really checks is whether its **hard** limit is also small (podman, a hardened image, `LimitNOFILE=`, macOS) |
| **`UV_LINK_MODE=copy` for the image build** | Needed wherever uv's reflink fails (`os error 11`), e.g. Docker storage on ZFS. Shipped in the `Dockerfile`; a scratch copy of an *older* Dockerfile still needs it by hand |
| **Worker deaths with no traceback and no kernel OOM** | The PDEATHSIG/thread hazard is Linux-only (macOS and Windows have none), but the spawner fix applies everywhere and that is the signature. On Linux the diagnostic is `strace -f -e trace=clone,clone3,exit,exit_group,kill,tgkill` plus a check of who forked the dead pid |
| **`LD_LIBRARY_PATH` for CTranslate2 on bare Linux** | `whisper/tiny` (S6's and S8's grantless resident) SIGABRTs on load unless the venv's `nvidia/cudnn/lib` is on the loader path; torch finds its own copy through RPATH and the repo sets the variable on the ROCm path only. A platform whose wheels bundle cuDNN differently will not need it — record which |

Two cheap facts to record everywhere: the **CUDA context size**
(`context_mb`; 666–668 MiB here, and `base_mb` under-states by ~120 MiB until
the first kernel launch) and the **`nvidia-smi` vs torch total** disagreement
(97 887 vs 97 250 MiB, 0.7 %).
