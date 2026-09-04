# Batch calibration: run1 final report (Linux/CUDA, 2026-09-03)

Subject: PR #27, branch `claude/batch-calibration-coverage-db9ab9` (the Rust
port). Protocol: `docs/batch-calibration-test-protocol.md`, revised from this
run. Recordings: `tools/calibration-protocol/results/run1/` (git-ignored;
indexed by that directory's `README.md`). Host: two RTX PRO 6000 Blackwell
(97 887 MiB each, compute capability 12.0), driver 590.48.01, 62 GB RAM, 48
cores, Arch Linux kernel 6.12.73-1-lts, Docker 29.

Every number below comes from a phase report or a runlog. Non-obvious numbers
carry the source report file in a trailing parenthesis. Reports live in the
orchestrator's scratchpad; each cites the runlog that produced it.

Acronyms expanded once: NVML = NVIDIA Management Library; VRAM = video RAM;
OOM = out of memory; PDEATHSIG = the Linux `PR_SET_PDEATHSIG` parent-death
signal; RSS = resident set size; fd = file descriptor; cgroup = Linux control
group; CI = continuous integration.

---

## 1. Verdict and recommendation

**PR #27 is not shippable as it stood at the start of the run, and is
shippable on Linux/CUDA after the nine fixes this run landed. Phase 7b re-ran
both failed legs on a binary at the tip and **both blockers closed**. Phase 8
then confirmed that the last verifier commit is behaviour-preserving — Leg 0's
store is byte-identical to Phase 7b's — and ran the soak to completion: 8 h
09 m, 13 cycles, 52 of 52 jobs, 0 worker deaths, 0 `ERROR` lines, PASS on every
§4 criterion. The run is finished. The only thing still outstanding is the
user answering the open policy questions listed below, and the soak sharpened
the first of them.**

Two release blockers were found, both by scenarios that no unit test can
express, and both are fixed:

| Blocker | What it did | Status |
|---|---|---|
| **F6** descriptor exhaustion | The shipped Docker image could not finish a 2 000-item extraction: `nofile` soft limit 1024, 983 sockets on pid 1, job `status: -1`, **1 849 of 2 000 items unprocessed**, 1 240 `Too many open files` lines. The master image finished 2 000/2 000 at a peak of 177 fds. | **Fixed** in *Bound the in-flight window by the process's descriptor budget* (`d5e42c78`, sweep `13e8850f`, docs `25153334`). Verified. **Closed in Phase 7b**: the rebuilt image finishes 2 000/2 000 with 0 errors, soft `nofile` raised 1024 → 524 288 inside the container, peak 3 003 fds. |
| **F11** the gateway SIGKILLs its own workers | PDEATHSIG is thread-scoped; the load-path `block_in_place` added by this run's own T2 fix demoted the forking thread into Tokio's blocking pool, which reaps it after 10 s. 8/8 deaths at Δ 1–3 ms from the forking thread's exit. Cost **15 924 of 16 000 items** on a job that still reported *completed*. | **Fixed** in *Fork every supervised child from one thread that never exits* (`f9cf10fa`) plus `5becf29c`, `ed0b1f0e`, `c8c26c54`, verifier fix-up `6a5e6799`. **Closed in Phase 7b**: 0 deaths, 16 000/16 000, 0 `kill`/`tgkill` syscalls in 33 minutes under an oscillating hog. |

Three further defects were serious enough to invalidate whole scenarios and
are also fixed:

| Defect | What it did | Status |
|---|---|---|
| **dtype persistence** | Five of five shipped models report no `dtype`, so `queue_profile_update` bailed and `data/inferio/calibration.toml` was **never written, on any host, ever**, with no log line saying why. S2's persistence criterion and the whole of S3 were void. | **Fixed** in *Infer a model's dtype and log why a calibration update is skipped* (`58dda519`). Zero skip lines in Phase 2b. |
| **N4** phantom external usage | After an unload the board kept the departed replica's footprint in `external_mb` (775 → 27 603 MiB against an oracle seeing 638 MiB and no process) until the next grant. Caused five of seven `oracle_agreement` FAILs in Phase 3. | **Fixed** in *Do not reattribute a departed worker's VRAM to external usage* (`d17e3854`). `oracle_agreement` PASSes on both Phase 4 job legs. |
| **T2** unprobed empty board | A board with no resident is never probed, so the "can this model be loaded at all" guard could never fire. S4g spent 259 s on four 4 096 MiB reservations against a board holding 95.6 GB of someone else's memory. | **Fixed** in *Probe the board before pricing a load against it* (`8546cd63`, fix-up `ff34b059`). Confirmed live in Phase 5. |

Plus three smaller landed fixes: **T5** (publish the granted budget when a
window's grant was squeezed, `22eb33f9`), the **refreshing drop guard**
(`8c696ac0`/`eb398d69`), and **P5-1/P5-6** worker-death reporting and the idle
liveness sweep (`c8d64a5a`). One build fix: `UV_LINK_MODE=copy` in the
`Dockerfile` (`5b7c7353`), without which the shipped image cannot build on
this host at all.

### What is measurably better than master

- Same 180-image job, full board: **22 s vs 24 s wall, 0 GPU OOMs vs 9**
  (phase1-report.md).
- The design's own easyOCR acceptance test: **8.56× inference time**
  (54.72 s vs 468.20 s), 0 errors, 0 OOMs, 37 size-homogeneous bucketed
  batches (phase4-report.md).
- The in-flight ceiling of 64 is gone: a 2 000-item job ramped to
  `unit_budget = 512` with **957 items in flight** (phase2b-report.md).
- Learned slope vs independently probed ground truth: **+0.04 %** (wd-vit),
  **−0.29 %** (MobileCLIP), **+52 %** conservative (MiniLM); base error 0 %
  on all three.
- Across 419 grants under seven external-pressure profiles: **419/419 safe,
  0 OOM, 0 worker deaths, 0 failed items** (phase3-report.md).
- An 8 h 09 m soak on the fixed binary, under a randomized 0–93 GB hog:
  **52/52 jobs, 78 789 grants of which 0 exceeded their priced headroom, 0
  worker deaths, 0 `ERROR` lines in 166 MB of log**, a store bounded at 6 635 B
  and an RSS that mean-reverts on a 4.8–7.1 GB band (phase8-report.md).

### What remains open and could still block a release

None of these are code defects the run was authorised to fix; all are policy,
default or design questions left to the user (protocol §0 decision 5). Ranked
by how much damage they can do to a real user.

| Open item | Why it can still block | Worst measured number |
|---|---|---|
| **T1 / N1 / P5-4** knee poisoning | The throughput-knee estimator fires on sample rate and on externally forced batch-size variance, **persists** the result, and has **no re-examination path** once the knee is in the store. A knee is a hard ceiling, so one unlucky workload caps the model on that GPU for the life of the store. The soak is the strongest evidence yet: the knee was fitted four minutes in and stood for the remaining 7 h 55 m across 13 job passes and 56 worker spawns. | `knee_units = 1` fitted from 68 observations and never refitted for **7 h 55 m**, so **4 281 of wd-vit's 4 285 grants ran one image per window** (F-A, phase8-report.md); `= 1` persisted from 5 512 forced one-image batches (S4d); `= 63` from oscillation (S4e); `= 31` for MobileCLIP against a real optimum of 128, −19 % throughput (phase3-report.md, phase2b-report.md) |
| **F7** worker-death blast radius | One worker death now fails a whole in-flight window's items, and the job still reports *completed*. Master capped the blast radius at 64 items. F11's fix removes the main cause of deaths on this host but not the class — though the soak took **0 deaths in 8 h**. | 1 542 items lost from one death (phase6-report.md); 15 924/16 000 when multiplied by F11 (phase7a-report.md) |
| **Q1** OOM message classifier (B11) | Any error text containing the substring "out of memory" deflates a healthy model. A library talking about its own cache is enough. | 15 spurious negatives with `reason="oom"` on a board with **96 GB free** (phase4-report.md) |
| **Q2** unbounded deflation (B8) | The counter has no cap and repays only in clean windows. A brief fault costs many minutes of batch-1 throughput. | 8 074 levels in 148 s; recovery 7.04 levels/s → a 2-minute fault costs **15.6 minutes at 0.43× throughput** (phase4-report.md) |
| **P5-2 / T4** the margin zeroes the budget | `limit = total − external × (1 + margin)` charges the margin against the neighbour's level. The last ~9.8 GB of any board is unusable, and below ~9 % free the limit clamps to 0 and grants go memory-blind. | `limit_mb = 2 813` at 10 GB free, **0** at 4 GB free; utilization 5 % for the 176 batches carrying 96.8 % of S4a's job (phase5-report.md, phase3-report.md) |
| **P5-3** B18 load lock | Every in-flight predict on the host stalls for the full duration of any model load. `load_secs` is 600 s. | 11.885–11.894 s stall for an 11.865 s load = **100.2 % of the load, 28× the p50** (phase5-report.md) |
| **W4** untested degraded base tier | The `free_delta`/`alloc_delta` tiers and their fixed 500 MiB context estimate were never exercised, because B9's predicted degradation did not happen on this driver. The estimate is against a **measured 666–668 MiB** context. | 0 occurrences of the degraded tier in the whole run (phase6-report.md) |

### Recommendation

1. **Merge is defensible on Linux/CUDA.** Phase 7b's two closure legs passed:
   F6 finished 2 000/2 000 at a peak of 3 003 fds against a 524 288 limit, and
   F11 finished 16 000/16 000 with 0 worker deaths, both on the `dc613400`
   binary. Phase 8 then showed the verifier commit above that binary changes
   nothing measurable (Leg 0 byte-identical) and that the feature survives 8 h
   of rotation under a randomized hog. See §2 and §3.
2. **Decide the knee question (T1/N1/P5-4) first.** It is now the top user
   item. It is the one finding that writes a permanent, invisible cap into a
   user's store, and the soak showed there is **no path that ever re-examines
   it**: `knee_units = 1` was fitted four minutes into an 8-hour run and was
   still in force 7 h 55 m later, across 13 job passes and 56 worker spawns,
   pinning 4 281 of 4 285 grants at one image per window (F-A). It cost
   nothing measurable there only because wd-vit's real throughput curve is
   flat; the same estimator cost MobileCLIP −19 %. The orchestrator's
   recommendation, from the Phase 3 options, is still option (a): exclude
   squeezed, memory-blind and clamped windows from the knee's observations,
   which the ledger already knows at grant time. F-A argues for pairing it
   with option (d) or the P5-4 expiry: a knee that can never be revisited is a
   ceiling for the life of the store.
3. **Decide F7 before release.** It is the one open item where a user sees a
   job report success after doing a fraction of the work. The suggested option
   is to re-queue a died-on window's in-flight set rather than record the items
   as errors. The soak took 0 deaths in 8 h, so the exposure is now rare, not
   absent.
4. **Decide Q1 and Q2 before release.** Both are small, bounded changes with a
   clear preferred option (a driver-shaped pattern for Q1; a cap at
   `log2(anchor)` for Q2) and both cost throughput silently today. The soak
   supports the bounded reading of Q2: peak deflation was 1 and every instance
   recovered, the worst in 109 s.
5. **P5-2/T4, P5-3, W4 and the pixel-pricing decoupling (F-B / Q3) can ship as
   documented limitations** provided the release notes say so; none loses data.
   F-B is the soak's new instance: easyOCR's pixel-priced grants ran 23–94 GB
   against as little as 1 986 MiB of free memory without a single OOM, because
   the price and the real footprint are unrelated on that path, and easyOCR
   never accumulates a fit sample at all.
6. Two schema questions from the dtype fix are still open and are cheap:
   **D1** store `dtype_method` in the profile? **D2** sentinel name `"unknown"`
   vs `"unstated"`.
7. **Regenerate the UI's API types** (`ui/lib/panoptikon.d.ts` from
   `panoptikon/openapi.json`) before release: the predict response gained a
   header (trackA-verify.md).

The full list of decisions left to the user is the table in §4; the seven
above are the ones with a release gate attached.

---

## 2. What was run

### Phases, binaries and legs

Phases are sequential. Fixes landed continuously, so each leg describes the
code in **its binary**, which is usually several commits behind the tree. This
was run1's own lesson about phase order and is now a rule in the plan (§7).

| Phase | Content | Binary the legs ran on | Legs |
|---|---|---|---|
| 0 Prep | Three parallel tracks: A (the G7 feedback signal + verifier), B (tooling, corpora, fixtures), C (environment, worktrees, venvs, configs) | n/a | n/a |
| 1 Smoke | S0, S1 (SGLang still holding 95 GB of each board), S14 | `10be7442` (repo `1b5b6850`) | S0, S1, S1-C0, S14 |
| 2a Ground truth | SGLang stopped; oracle calibration at 10/40 GB on both boards and 16 GiB RAM; bisect smokes; five ceiling probes | no gateway, probes in process (repo `e67705c2`) | `probes/`, `phase0/oracle-*` |
| 2b Cold ramp | S2 ×3 plus the W5 and loadgen legs; S3 with B3/B4 | `918ec170` (includes the dtype fix `58dda519`) | S2-\*, S3-\* |
| 3 Pressure | S4a–S4g | `918ec170` | S4a–S4g |
| 4 Faults, dimensions | S5 (12 legs), S8 (8 legs) | `d17e3854` (N4 in; T2/T5 **not**) | S5-\*, S8-\* |
| 5 Concurrency | S6, S7 | `443fa088` (N4 + T2 + T5) | S6-contend, S6-b17, S6-b18, S7-C7, S7-C3 |
| 6 Deployment | S10, S11, S12 | `eb398d69`; images built from a clean worktree at it | S10-bare, S10-docker, S11-C4, S11-C5, S12-C6 |
| 7a Robustness, F8 | S13's three cases plus the F8 SIGKILL root-cause investigation | `b9d32324` (code `c8d64a5a`) | F8-sigkill, S13-nosmi, S13-slowsmi, S13-badsmi |
| 7b Re-run legs, S15 | F8 leg re-run, S2 cold ramp re-run, S11-C4 job on a rebuilt image, S15 mutations | `dc613400` | F8-sigkill-fixed, S2-wdvit-fixed, S11-C4-fixed, S15-m1-S2, S15-m1-S4b, S15-m2-S2, S15-m2-S4b, S15-m3-S2 |
| 8 Soak | Leg 0, the `6a5e6799` confirmation ramp; then S9, the soak | `1f1a69c2` (code `6a5e6799`), sha256 `4ba35498…`, 85 017 176 B | S2-wdvit-6a5e6799, S9 |
| 9 Report | This document; plan revision; SGLang restart | n/a | n/a |

### Phase 7b: the two closure legs

All four Phase 7b legs ran on the release binary built at `dc613400`. The
verifier's fix-up `6a5e6799` landed *during* the phase, so **no result below
tests it**; the verifier's own behavioural analysis concludes no rebuild is
needed, because both of its code changes are reachable only on paths no
scenario takes (f11-verify.md). **Phase 8's Leg 0 did the rebuild and the short
S2 leg anyway, and the argument holds: every learned quantity is
byte-identical.**

**F11 closure** (`F8-sigkill-fixed/`). wd-vit seeded from
`S2-wdvit/calibration.after.toml`, `ramp8` 16 000 items, a hog oscillating
0 ↔ 40 GB every 20 s, the gateway under `strace -f -e trace=clone,clone3,exit,
exit_group,kill,tgkill`.

| Quantity | Phase 7a | Phase 7b |
|---|---|---|
| worker deaths | 4 | **0**. No `attribution=`, no `signal: 9`, no kill-ladder line |
| worker spawns | 4 | **1** (`pid=Some(1873649)`), alive for the whole 1 986 s job |
| worker lifetime | 10.1 / 10.2 / 12.2 / 21.4 s | **1 986 s** |
| items processed | 76 | **16 000 / 16 000**; `errors 0`, `input_errors 0`, `failed 0`, `total_remaining 0`, `status: 1`, `/failures` `{"total":0}` |
| items errored | 15 924 | **0** |
| `kill` / `tgkill` syscalls | 8 kills, 4 SIGKILLs | **0 / 0**, and no `+++ killed by SIGKILL +++` |
| log volume | 250 000 lines, 2.6 MB per 60 s job | 17 254 lines, 157 KB per 33-minute job, **0 `ERROR` lines** |

The `panoptikon-spawner` thread is present in `ps -T` (`1873648
panoptikon-spaw`; the kernel truncates `comm` to 15 characters) mid-job and at
job end, and absent at boot, because the thread is `OnceLock`-lazy. The
gateway's own limit read `Max open files 524288 524288`, and the
`AlreadyRaised` branch logged it. **Peak descriptors 8 239, of which 8 200
sockets**, 9 s into the job: exactly the byte budget's ceiling of 4 096 units ×
2 sockets, and 2 % of the limit. `analyze.py` reported 9 PASS, 5 INFO and one
FAIL; the FAIL is `oracle_agreement` and is B2/T3, not new, because one worker
held one window and the worst sample age is 1 048 s against a 40 GB hog
amplitude. `grant_safety` is 17/17 against both the priced headroom and the
oracle's live free memory.

**F6 closure** (`S11-C4-fixed/`). The image was rebuilt from a clean detached
worktree at `dc613400`, brought up with the shipped `docker-compose.C4.yml`,
and given the same 2 000-item wd-vit job.

| | Phase 6 branch image | **Phase 7b** | Phase 6 master image |
|---|---|---|---|
| `/proc/1/limits` max open files | **1024** / 1048576 | **524 288 / 524 288** | 1024 |
| Job | `status: -1`, 151 done, **1 849 errors** | **`status: 1` completed, 2 000/2 000, 0 errors** | 2 000/2 000 |
| `Too many open files` | **1 240** | **0** | 0 |
| `ERROR` lines | many | **0** | 0 |
| peak fds / sockets | **1 024 (the limit)** / 983 | **3 003 / 2 961** | 177 / 135 |
| worker deaths | several | **0** | n/a |
| peak `unit_budget` | 128, then died | **1 024** | n/a |

The raise is in `docker logs` verbatim: `raised the open file descriptor soft
limit to the hard limit soft_nofile_before=1024 soft_nofile_after=524288`. The
**clamp** was never binding, because at 524 288 the descriptor term allows
262 016 units and the byte budget's 4 096 wins, exactly as the fix report
predicted. The container now calibrates identically to the bare host: slope
**50.5625**, `samples` 10, anchor **512**, base 964 / `nvml`, the same numbers
as `S2-wdvit-fixed`, where Phase 6's C4 got slope 50.79 at anchor 64 only
because the job died early. B9 stays refuted and W4 stays unreached. S4b inside
C4 was attempted and is **not** a result: the corpus was fully extracted before
the hog stepped, so there was no live grant to shrink, and Phase 6's C4 S4b
(+1.2 s, PASS) stands as the last valid measurement.

**The S2 cold ramp re-run** (`S2-wdvit-fixed/`) is the control. Every learned
quantity is **byte-identical to Phase 2b**: the ramp 1, 2, 4, … 512 in ten
windows, `base_mb` 964 / `nvml`, `slope_mb_per_unit` 50.5625, 10 samples,
`residual_mb` 3.71875, anchor 512, no knee, peak `unit_budget` 1 024 at
utilization 0.40, store write delay 29.8 s, 2 000/2 000 with 0 deaths.
Throughput is 0.93× C0 against 0.94×, a 1.5 % move. The async load-path probe
fires where it used to (`no free sample: this board has never had a resident`,
spawn +94 ms later), with no demotion and no death. One check changed verdict:
**`oracle_agreement` FAIL → PASS**, 137 MiB worst and 0 breaches against Phase
2b's 26 965 MiB and 81 breaches. This is the first clean S2 re-measurement of
the N4 phantom fix (`d17e3854`), and the post-unload phantom is gone.
`base_accuracy` still FAILs at 13.15 %, byte-for-byte Phase 2b's number, which
is the A3 artefact. (Phase 8 re-ran the same leg and gets the same store; the
check now reports **PASS at 1.03 %** because `analyze.py` gained a
nearest-oracle-sample join in the meantime. The ledger's `base_mb` is 964 in
both.)

### Phase 8: the confirmation leg and the soak

Both Phase 8 legs ran on the release binary built at `1f1a69c2` (code
`6a5e6799`), sha256 `4ba35498…`, 85 017 176 B, `panoptikon 0.1.8`. No
`cargo build` was run during the phase. SGLang stayed down and both boards read
2 MiB at the start and at the end of both legs (phase8-report.md).

**Leg 0, `S2-wdvit-6a5e6799`** (19:28:52–19:31), is the confirmation Phase 7b
asked for: `6a5e6799` landed during Phase 7b, so no Phase 7b result tested it.
Phase 7b's `leg.sh` verbatim, fresh root, `ramp` 2 000 items, no hog. **Every
learned quantity is byte-identical to Phase 7b**: ramp 1, 2, 4, … 512 in ten
windows, `base_mb` 964 / `nvml`, `slope_mb_per_unit` 50.5625, 10 samples,
`residual_mb` 3.71875, the same ten `sample_reserved_mb` values 40 … 25 864,
anchor 512, no knee, peak `unit_budget` 1 024 at utilization 0.40, throughput
0.93× C0, 2 000/2 000, 0 deaths, 0 `ERROR`, peak 3 002 fds / 2 962 sockets.
`inference_time` moved +0.5 % (56.86 s vs 56.58 s) and
`calibration.after.toml` differs only in `measured_at`. `analyze.py`: **0 FAIL
in 14 rows**. `6a5e6799` is behaviour-preserving, and the verifier's own
analysis (§5, fix 9) is confirmed rather than merely argued.

**Leg 1, S9, the soak** (19:37:54 → 03:47:10, **8 h 09 m**). C1, fresh root,
store seeded from `S2-wdvit-fixed/calibration.after.toml`, default batching, no
C7 registry. The `soak` corpus generated in 96.8 s (12 000 items / 803 MiB, of
which 9 680 are indexable images), so the `ramp8`+`pixmix`+`ocr` fallback was
not needed. **13 rotations** of wd-vit, MobileCLIP, nemotron and easyOCR, each
extraction followed by a deletion job over the same corpus — `DELETE
/api/jobs/data/extraction` (`enqueue_delete_extracted_data`,
`panoptikon/src/api/jobs.rs:192`) exists, so no second hard-linked copy was
needed; 51 deletion jobs at 5–16 s each. `loadgen.py` held MiniLM at 16 items,
concurrency 1, one request every 5 s throughout (**5 756 requests, 5 756 ok**),
and `hogdrv.py` ran 19 randomized phases: calm 10–20 min at 0, steps to a
uniform-random 10–50 GB for 5–15 min, and a 10 s `leave_free 2048` spike every
~40–50 min, every transition timestamped. Recorders: `vramrec` at 1 s,
`healthrec` at 0.5 s, an fd loop at 10 s, an RSS loop at 60 s.

| h | UTC | rings (`sample_units`) | store | gateway RSS (Δh) | log | deaths / ERROR / OOM |
|---|---|---|---|---|---|---|
| 1 | 20:34 | 15 / 51 / 64 / 64 | 5 793 B | 5 558 MB (–) | 20.3 MB | 0 / 0 / 2 |
| 2 | 21:34 | 21 / 64 / 64 / 64 | 6 232 B | 6 499 MB (**+940**) | 46.8 MB | 0 / 0 / 1 |
| 3 | 22:34 | 24 / 64 / 64 / 64 | 6 300 B | 6 482 MB (−17) | 61.4 MB | 0 / 0 / 0 |
| 4 | 23:34 | 30 / 64 / 64 / 64 | 6 358 B | 6 526 MB (+44) | 85.2 MB | 0 / 0 / 0 |
| 5 | 00:34 | 33 / 64 / 64 / 64 | 6 390 B | 6 580 MB (+54) | 100.7 MB | 0 / 0 / 0 |
| 6 | 01:34 | 39 / 64 / 64 / 64 | 6 500 B | 6 684 MB (+104) | 125.1 MB | 0 / 0 / 0 |
| 7 | 02:34 | 43 / 64 / 64 / 64 | 6 554 B | 6 765 MB (+81) | 147.4 MB | 0 / 0 / 0 |
| 8 | 03:34 | 48 / 64 / 64 / 64 | 6 635 B | 6 561 MB (**−204**) | 163.7 MB | 0 / 0 / 0 |
| 9 | 03:48 | 48 / 64 / 64 / 64 | 6 635 B | 6 561 MB (0.0) | 166.1 MB | 0 / 0 / 0 |

Rings are MobileCLIP / nemotron / wd-vit / MiniLM and the cap is 64: none ever
exceeded it. Peak descriptors were **8 257** (8 208 sockets, 2 % of 524 288) in
every hour, and `grants_outstanding` never exceeded 2 per board. The knees held
steady all soak: MobileCLIP 127, nemotron 67 108 863, wd-vit **1**, MiniLM
8 191. Hour 1 covers 19:37–20:34; the hourly checker was validated early and
its RSS column reworked, so the table was regenerated once at 20:34
(`results/run1/S9/hourly.md`).

**Deaths and ERRORs: none.** 0 `an inferio worker process is gone`, 0
`attribution=`, 0 ` ERROR `, 0 `panicked` across 166 113 166 B and 922 411
parsed events, with 56 worker spawns and 13 job rotations.

`analyze.py` reported five FAILs on the soak. Each was adjudicated against the
recordings and **none is a new safety defect**:

1. **`oracle_agreement`** (93 437 MiB worst, 16 641 / 117 852) — **B2/T3, not
   new.** `external_mb` is a window-boundary quantity, worst sample age 27.1 s,
   against a hog that moves up to 93 GB in seconds. Safety did not rest on it:
   **0 of 78 789 grants exceeded the headroom they were priced against**, and
   `hog_tracking` confirms the instrument tracked (hog 0…93 696 MiB,
   `external_mb` 0…95 257 MiB, ≥ 1 GiB held for 10 655 s).
2. **`base_accuracy`** — wd-vit is **0.0 %** (964 vs 964). The FAIL is entirely
   nemotron: `base_mb = 3 788` against an oracle per-process reading of
   **848 MiB at admission + 343 ms**, 346.7 % over, lifetime minimum 654. The
   direction is conservative, so it is not a safety defect (finding F-C).
3. **`grant_safety`** — 78 789 grants, **0 over their priced headroom**, **6
   over the oracle's live free memory** (0.008 %), 3 912 memory-blind
   (`mb = 0`, B1, all during spikes). **All six are
   `doctr/easyocr_standard_en`**: two inside a 10 s `leave_free 2048` spike
   (23 576 and 38 922 MiB granted against 1 986 and 2 100 MiB free), two 24 GB
   overshoots inside a hog step, two 1.3 GB overshoots on a board with 93 GB
   free. **No OOM followed any of them** — easyOCR took zero OOM negatives all
   soak — because its pixel-priced grant of 88–94 GB hugely over-states what it
   actually allocates (finding F-B).
4. **`failures`** — **3 OOM negatives, all nemotron, all under real hog
   pressure** (20:19:34 inside a 31 744 MiB step, 20:23:14 just after a spike
   released, one more in hour 2), each raising `deflation` to 1 and each
   recovering to 0. Plus **34 throughput-collapse negatives, 32 of them
   MiniLM** — the loadgen model, whose fixed 16-item requests every 5 s feed the
   comparator a sample-rate signal unrelated to memory; the comparator retired
   108 times (P5-5 and N1 again). 0 fatal worker deaths, 0 merged-window
   fallbacks.
5. **`ledger_invariant`** — 1 698 of 118 414 board-samples over `limit_mb`, **36
   at `limit_mb = 0`**. **T6 / P5-2, not new**: under a 93 GB hog
   `limit = total − external × (1 + margin)` collapses while our own resident
   legitimately holds gigabytes. The clause with teeth holds: 0 of 78 789 grants
   over their priced headroom.

`deflation_recovery`, `idle_liveness`, `utilization` (0.82), `persistence` (273
writes, worst 29.9 s), `job_outcome` (52/52) and `slope_accuracy` (ledger 57.42
vs probe 50.54 = 1.1362) all PASS.

### Configurations

| Id | What it is |
|---|---|
| C1 | Bare Linux, release build of the branch, both GPUs visible. Primary. Ports 6342/6343/6339 |
| C0 | Bare Linux, release build of **master** in `../panoptikon-master` (`7aa92b20`), own venv. The "before" baseline |
| C2 | C1 with `CUDA_VISIBLE_DEVICES` in UUID form (single visible board) |
| C3 | C1 with `CUDA_VISIBLE_DEVICES=1` (index form), the documented off-switch |
| C4 | Docker CUDA image from the branch, root `docker-compose.yml` as shipped |
| C5 | C4 plus `pid: host` |
| C6 | Docker CPU image with `mem_limit: 16g`, the unified CPU board |
| C7 | C1 with a user registry: `enable_batching = true` on `doctr/easyocr_standard_en`, MobileCLIP pinned to GPU 1 |

Every configuration runs under its own `--root`, so no two legs share a
`calibration.toml`, an index database or a log.

### Wall time

Approximate, from the phase report timestamps and the UTC stamps inside them
(the host clock is UTC). **These are wall-clock spans, not effort, and phases
overlapped with fix and verifier agents running in parallel.**

| Marker | Time (UTC, 2026-09-03) |
|---|---|
| Track C (environment) complete | ~04:49 |
| Track A (feedback signal) complete and verified | ~05:16 |
| Phase 1 report | ~06:17 |
| Phase 2a report (SGLang stopped, oracle passed) | ~07:31 |
| Phase 2b report | ~08:31 |
| Phase 3 report | ~10:45 |
| Phase 4 report (20 legs in 1 h 03 min of recording) | ~12:07 |
| Phase 5 report (5 legs in 32 min) | ~12:49 |
| Phase 6 report (includes four image builds) | ~16:27 |
| Phase 7a report | ~17:24 |
| Phase 7b binary `dc613400` built | ~18:11 |
| Phase 7b S2 cold-ramp re-run leg | ~18:59 |
| Phase 8 binary `1f1a69c2` (code `6a5e6799`) | 19:27 |
| Phase 8 Leg 0, the confirmation ramp | 19:28:52–19:31 |
| Phase 8 S9 soak start | 19:37:54 |
| Phase 8 S9 soak end | **03:47:10** (2026-09-04) |

Roughly **12.5 hours** from the end of preparation to the end of Phase 7a, plus
Phase 7b and the 8 h 09 m soak: about **23 hours** end to end. The plan's
estimate was 24–36 h including the soak, which was right.

Phase 7b's own cost is dominated by two things the plan did not budget for. The
F11 closure leg took **39 minutes** against a 25-minute cap, of which only
551 s was inference and 22 minutes was the tag-write phase (finding F15). The
S11-C4 leg needed a fresh image build, and each of the three S15 mutation
branches built its own `target/` in its own worktree.

### Oracle calibration (the gate that had to pass first)

`oracle_calibrate.py` drives `hog.py` to a known allocation and checks what
every instrument reports (phase2a-report.md).

| Target | Requested | Board delta | NVML per-process delta | Verdict |
|---|---|---|---|---|
| GPU 0 | 10 240 MiB | **+2 MiB** | −6 MiB | PASS |
| GPU 0 | 40 960 MiB | **+2 MiB** | −6 MiB | PASS |
| GPU 1 | 10 240 MiB | +2 MiB | −6 MiB | PASS |
| GPU 1 | 40 960 MiB | +2 MiB | −6 MiB | PASS |
| RAM | 16 384 MiB | RSS +32 MiB, `MemAvailable` recovery +78 MiB | n/a | PASS (tolerance ±512) |

CUDA context measured at **666 MiB** here (668 MiB in the Phase 0 smaller
sizes). `nvidia-smi` total 97 887 MiB vs torch total 97 250 MiB, a 0.7 %
disagreement, inside the sample-vs-board check but the kind of number a
threshold gets written against by accident.

### Corpora and instruments

Corpora generated (git-ignored, `results/corpus/`): `smoke` (205 items),
`ramp` (2 000), `ramp8` (16 000 × 1024² JPEG), `text` (2 000), `poison`,
`poisonmix`, `pixmix`, `ocr`, `audio`, and `soak`, generated at the start of
Phase 8 in **96.8 s** (12 000 items / 803 MiB, of which 9 680 are indexable
images; the `.txt`, `.wav`, `.mp3` and `.pdf` items are not indexed).

Instruments, all committed under `tools/calibration-protocol/`: `vramrec.py`
(NVML at 4 Hz, per-process with `CUDA_VISIBLE_DEVICES` / pin attribution),
`hog.py`, `corpus.py`, `healthrec.py`, `loadgen.py`, `ceiling_probe.py`,
`oracle_calibrate.py`, `analyze.py`, the runlog template, and the S13
`nvidia-smi` shims.

---

## 3. Aggregate verdict per scenario

One number decides each row. Full criteria tables are in the phase reports;
one-line headlines per leg are in `results/run1/README.md`.

| Scenario | Verdict | The deciding number |
|---|---|---|
| **S0** Build and unit baseline | **PASS with WARN** | `cargo test --release` 1 464 pass / **3 fail**; pytest 159 + 73 green. One failure is a host artefact (`batch_auto` assumes a read-only mode bit); two `media_tools::transcode` ffmpeg-budget tests fail **identically on master** |
| **S1** Inventory, identity, full board | **PASS** | 22 s vs master's 24 s, **0 GPU OOMs vs 9**; 51/51 grants safe; base error 1.03 % |
| **S2** Cold ramp, idle board | **PASS** | Learned slope **+0.04 %** vs the independently probed ground truth (wd-vit); −0.29 % MobileCLIP; base 0 % on all three; 957 items in flight |
| **S3** Restart and resume | **PASS**, one criterion FAIL | Resume in **3 windows instead of 10**, anchor 512 → 1 024, `status = "local"`. Throughput **0.89×** (finding N2) |
| **S4** External pressure (a–g) | **PASS** | **419/419 grants safe, 0 OOM, 0 deaths, 0 failed items**; slope within 0.02 %. `utilization` FAILs on S4a at 21 % (finding T4) |
| **S5** OOM backstop, faults | **PASS with findings** | The merged-window fallback lost **0 of 72 539** requests; deflation 1 → 0 after exactly 3 clean windows. Findings Q1 (15 spurious OOM negatives) and Q2 (8 074 levels) |
| **S6** Multi-model contention | **PASS with findings** | 0 OOM across **2 358 clean settles**; the idle-resident trim flagged in **1.837 s** with a 5.8 ms round trip and the oracle saw exactly `slack_mb` come back. Findings P5-2, P5-3, P5-4 |
| **S7** Multi-GPU | **PASS** | GPU 1's ledger row **byte-identical** at every sample under a hog on GPU 0; `PinDiverged` logged **0** times |
| **S8** Cost dimensions and packing | **PASS** | easyOCR acceptance test **8.56×** inference (54.72 s vs 468.20 s), 0 errors. `utilization` FAILs at 0.08 on the mixed-pixel corpus (finding Q3) |
| **S9** Soak | **PASS on every §4 criterion** | 8 h 09 m, 13 cycles, **52 of 52 jobs completed**, 0 failed, `/failures` `{"total":0}`; **0 worker deaths** across 56 spawns; **0 `ERROR` lines and 0 panics** in 166 MB of log; deflation peaked at **1** and always returned to 0, worst recovery **109 s**, nothing deflated at the end; `calibration.toml` bounded at **6 635 B** with every `sample_units` ring **≤ 64**; server RSS fits **+113 MB/h** but mean-reverts on a 4 800–7 107 MB band (VmHWM 11 519 MB set in the first 10 minutes and never re-set); throughput **1.03×** S2 on wd-vit and **1.08×** on MobileCLIP; peak **8 257 fds** = 2 % of the 524 288 limit. Findings F-A (the pinned knee), F-B, F-C, F-D |
| **S10** Migration | **PASS** both paths | Exactly one INFO line across four boots; only the batch-size keys removed; stamp row present; cap survives a restart |
| **S11** Docker CUDA | **FAIL, fixed, closure verified** | **1 849 of 2 000 items unprocessed**, 1 240 `Too many open files`, peak 1 024 fds / 983 sockets, where the master image finished 2 000/2 000 at 177 fds (**F6**). On the rebuilt image: **2 000/2 000, 0 errors, 0 `Too many open files`, peak 3 003 fds against a 524 288 limit**, and calibration identical to the bare host. Everything else PASSed, including the 403-on-6339 CI assertion and base error **0.00 %** |
| **S12** CPU board (Docker CPU) | **PASS on every stated criterion** | Board 64 137 MiB, budget 48 102 MiB, cgroup `memory.max` 16 384 MiB = **2.94× overcommit** (B19); the death-negative path converges 32 → 16 → 8 → 4 but only across job passes |
| **S13** Probe robustness | **PASS all** | B13 refuted: **2 host probes in 240 s** under three-model load, threads 51–53 against a 52–53 baseline, max 1 concurrent probe |
| **S14** Regression sanity | **PASS** (CI smoke), 4 of 6 model jobs clean | Every CI step 200; `whisper/tiny` SIGABRTs on cuDNN (finding F5, shared with master); nemotron correctly deferred; deflation reached **108** on easyOCR (finding F4) |
| **S15** Protocol self-test (mutations) | **PASS with holes** | **3 of 3 mutations caught, none by the check the protocol predicted.** m1 was caught by `utilization` alone (peak `unit_budget` 8 against a probe boundary of 2 560 = 0.00); m2 by three FAILs before any hog started; m3 never reaches a scenario, because the config is **rejected at load**. Five holes found in the protocol's own checks (H1–H5), **all five closed** in `3bf4ba76` and `4ea6d5f6` (§7) |

---

## 4. Findings

All findings, sorted by severity, keeping the original identifiers. "Fixed"
means the fix landed on this branch and a separate verifier agent signed it
off (§5). "User" means the protocol's fix policy classifies it as a feature,
default, user-visible behaviour or design decision, so it was written up with
options rather than changed.

| Id | Sev | Statement | Status |
|---|---|---|---|
| **F6** | BLOCKER | The shipped Docker image cannot finish a 2 000-item job: `nofile` soft 1024, ~2 sockets per in-flight item, 983 sockets, `status: -1`, 1 849 items unprocessed; master finishes 2 000/2 000 | **Fixed** in *Bound the in-flight window by the process's descriptor budget* |
| **F11** | BLOCKER | The gateway SIGKILLs its own workers ~10 s after any load that re-probed the host: PDEATHSIG is thread-scoped and `block_in_place` demotes the forking thread into Tokio's blocking pool. Self-sustaining. 8/8 deaths at Δ 1–3 ms | **Fixed** in *Fork every supervised child from one thread that never exits* |
| **dtype** | HIGH | Five of five shipped models report no `dtype`, so `calibration.toml` was never written on any host, silently | **Fixed** in *Infer a model's dtype and log why a calibration update is skipped* |
| **N4 / T7** | HIGH (was MED) | After an unload the board keeps the departed replica's footprint in `external_mb` (775 → 27 603 MiB) until the next grant; reproduced in every job-driven Phase 3 profile | **Fixed** in *Do not reattribute a departed worker's VRAM to external usage* |
| **T2** | HIGH | A board with no resident is never probed, so the load guard never fires; S4g took four 4 096 MiB reservations against a board holding 95.6 GB | **Fixed** in *Probe the board before pricing a load against it* |
| **F7** | HIGH | One worker death fails a whole in-flight window's items; 1 542 from a single death; the items are not in `/api/jobs/data/failures` and the job reports *completed* | **User** |
| **F8 / P5-1** | HIGH | Unexplained worker SIGKILLs with no traceback and no kernel OOM: 7 in Phase 5, 17 in Phase 6 | **Root-caused as F11 and fixed**; diagnostic half fixed in *Report every worker death with pid, signal and killer* |
| **F4 / Q2 (B8)** | HIGH | Deflation is an uncapped counter: 108 on a shipped model in Phase 1; 8 074 levels in 148 s in Phase 4; repays at 7.04 levels/s, so a 2-minute fault costs 15.6 min at 0.43× | **User** |
| **Q1 (B11)** | HIGH | Any error text containing "out of memory" deflates a healthy model: 15 negatives with `reason="oom"` on a board with 96 GB free; the same fault worded differently, zero | **User** |
| **N1 / T1 / P5-4 / F2** | HIGH | The throughput-knee estimator fires on sample rate and on externally forced variance, and persists a hard ceiling: knee 1 (S4d), 63 (S4e), 7 (loadgen), 31 for MobileCLIP against an optimum of 128. **F-A adds the missing half: once persisted, nothing re-examines it** | **User** (top item) |
| **A1** | HIGH | `clip/apple_MobileCLIP-S1` has a hard **non-OOM** ceiling at exactly 2 048 items (`canUse32BitIndexMath`), reached at 34 GB. Deflation keys on OOM, so this surfaces as a failed batch, not a shrink. Masked by the knee in S2, not cleared | **User** |
| **P5-2 / T4** | HIGH | The margin multiplies `external`, so `limit` reaches 0 once external > total/1.1: `limit_mb` 2 813 at 10 GB free, 0 at 4 GB free; the last ~9.8 GB of any board is unusable; below that, grants go memory-blind | **User** |
| **N2** | MED/HIGH | Bigger batches make wd-vit jobs **slower** in wall time: inference −3.4 %, non-inference wall +165 %, items/s 31.75 → 28.17 | **User** (and §8b of the plan) |
| **N3 (W5)** | MED/HIGH | `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True` changes the learned slope by **−36 %** (32.19 vs 50.56) under an **identical** profile key; using such a profile without the variable admits ~57 % too much | **User** |
| **T3** | MED/HIGH | `external_mb` is a window-boundary quantity: 0 host probes in 2.5 h of Phase 3, ages to 166.9 s, a +30 GB step took 31.5 s to reach `/health`, a 53 GB 10 s spike moved it by 2 MiB. The worker's per-batch clamp (0.60–2.81 s) is the real guard | **User** |
| **Q3 (W1)** | MED/HIGH | Pixel pricing is raw header dimensions, so the *fitted slope* is a function of the corpus: nemotron fitted 4.33× the probe's; 58 of 110 batches held one item; utilization 0.08 | **User** |
| **P5-3 (B18)** | MED/HIGH | Every in-flight predict stalls for the full duration of any load: 11.885–11.894 s for an 11.865 s load, 28× the p50; `load_secs` is 600 s | **User** |
| **F-A** | MED (new evidence for N1/T1) | The soak's knee was fitted **once**, four minutes in, from 68 observations, and **never refitted for 7 h 55 m** across 13 job passes and 56 worker spawns, because it is persisted and every new replica is reseeded from it: `knee_units = 1` on wd-vit, so **4 281 of its 4 285 grants ran at `unit_budget = 1`** — one image per 50 ms batch, `last_grant_units: 1`. Harmless here (wd-vit's real curve is flat, 30.67 it/s = 1.03× S2, `peak_reserved_mb` 454) and −19 % where the curve is not (MobileCLIP). `utilization` reports a peak of 2 110 only because that peak preceded the knee; the steady state is 1 / 2 560 | **User**, the strongest evidence for the T1/N1 decision |
| **F-B** | MED | The pixel-priced OCR path's grant and its real footprint have decoupled: all **6** of the soak's `grant_safety` breaches are easyOCR, granting 23–94 GB against as little as 1 986 MiB of oracle-free memory, and **none OOMed**. Self-consistent for the ledger (0 of 78 789 grants over their priced headroom), wrong against the oracle. easyOCR also **never writes a profile**: 13 272 grants, `fit samples 0`, peak `unit_budget` never leaves the 2 000 000 seed | **User** (the same mechanism as Q3 / W1) |
| **F1** | MED | The first load onto a never-sampled board is unwarned and unpriced (`external_known=false`); an unmeasured model reserves a flat `expected_base_mb = 4096` against real bases of 654–1 182 MiB | **User** (partly mitigated by the T2 fix) |
| **F5** | MED (HIGH for whisper) | `whisper/tiny` SIGABRTs on load: CTranslate2 cannot find cuDNN 9 because the venv's `nvidia/cudnn/lib` is not on `LD_LIBRARY_PATH`. Shared with master, not a PR regression | **User** |
| **F9 (W4)** | MED | The degraded base tiers (`free_delta`/`alloc_delta`) and their fixed 500 MiB context estimate were never exercised, because B9's predicted degradation did not happen on this driver. Measured context: 666–668 MiB | **User / portability** |
| **F12** | MED | `killed_by_gateway` reported `true` on exactly the class of death it was added to explain (`try_wait` cannot see a leader whose CUDA threads are still unwinding; 475 ms measured) | **Fixed** in *Tell a dying worker from one the gateway killed* |
| **N5** | MED | The stored anchor is trusted verbatim, never cross-checked against `sample_units`, and never lowered, not even by the OOM that it caused (B4c left 4 096) | **User** |
| **N6** | MED | The store write is a whole-profile replacement, not a merge: a process that re-learns from scratch overwrites a better profile (anchor 512 → 32, samples 10 → 4, plus a spurious knee) | **User** |
| **T6** | MED | `ledger_invariant` as written cannot hold once `external × (1 + margin) > total`: our own residents are legitimately over a limit that floors at 0 | **Fixed in the tool** (`analyze.py` reports both forms; WARN at `limit_mb == 0`) |
| **Q4** | MED | Token pricing over-charges 8× past `max_seq_length`; CJK under-charges 1.37× but truncation makes it exactly right | **User** |
| **Q5 / B15** | MED | On the predict path a dying-on-load model is retried once per request with no backoff and no cap: 93 loads in 182 s. Inside a job the cap is `reqwest_retry`'s 4 attempts, not the manager's | **User** |
| **Q6** | MED | A grantless model's VRAM is invisible until something refreshes the board (whisper held 796 MiB with `external_mb = 0` for 700 samples). Phase 5 showed the design expectation holds once any refresh happens | **Scoped to the never-refreshed case** |
| **Q7** | MED | A window holding one request has no per-request fallback: the same fault lost 276/276 requests single, 0 of 173 merged | **User** |
| **P5-5** | MED | `throughput_collapse` fires on neighbour contention: 3 negatives on MiniLM purely from sharing a board, 0 alone | **User** |
| **P5-6** | MED | A dead grantless replica went unnoticed for 13 minutes: a `none`-class idle model forms no window, so nothing tests it | **Fixed** (idle liveness sweep in the same commit as P5-1) |
| **F15** | MED | The tag-write phase, not inference, is the long pole on a large tagging job: 16 000 wd-vit items took 551 s of inference and then **22 minutes** of commits at ~6.4 items/s, with `slow statement … COMMIT ≈1.1 s` throughout, for 236 k `tags_items` rows. Never visible before, because F11 destroyed every job that would have reached it | **User** (outside the calibration feature; **not** a calibration defect) |
| **S15 H1–H5** | MED | The protocol's own checks had five holes, found by S15. **H1**: a fault that destroys the measurement also destroys the evidence, and `analyze.py` turns that into SKIP, which never fails a run; m1 reported all green with three SKIPs unless the leg was run with `--probe`. **H2**: `slope_accuracy` cannot tell "no store" (a result) from "no probe" (a harness omission); both are SKIP. **H3**: `hog_tracking` is INFO only, so `external_mb 0..0` against a hog holding 30 720 MiB is not a verdict. **H4**: `ledger_invariant`'s strict form PASSes on a completely broken ledger. **H5**: §4's stated expectations for m1 and m3 are wrong | **Fixed** in `3bf4ba76` (`analyze.py`: the `calibration_learned` check, the SKIP split, the `hog_tracking` FAIL form, the `grant_safety` WARN) and `4ea6d5f6` (the plan's §4 and §6), §7 |
| **A2** | MED | The linear fit is conservative at the ceiling by 17–36 %, because the caching allocator releases cached blocks near the top. The safe direction, and the reason the worker's clamp survives at 98.2 % of free | **Informational** |
| **A3** | MED | `base_mb` under-states the steady-state footprint by **118–128 MiB**, once: the CUDA context is not fully materialised until the first kernel launch (550 MiB at load, 668–678 MiB after) | **Informational**; explains several `base_accuracy` FAILs |
| **N7** | LOW/MED | The anchor a job can reach is a function of the job's length (one doubling per settled window): 2 000 items buys 10 windows and stops at 512 = 20 % of the boundary with 70 GB unused | **User** |
| **P5-7 (B17)** | LOW/MED | A hung idle-resident trim kills the model at ~20 s (a teardown race), not at the 60 s deadline, and the queued client gets a bare 500 after 18.6 s | **User** |
| **T8 / Q8** | LOW/MED | A failed job records `end_time == start_time` and `failed = 0`; a failed item never reaches `/api/jobs/data/failures` (`{"total":0}` in every leg of the run) | **User** (outside the calibration feature) |
| **F-C** | LOW (new) | `nemotron`'s `base_mb` over-states its resident **4.5×** at admission: 3 788 MiB priced against an oracle per-process reading of **848 MiB at admission + 343 ms**, lifetime minimum 654. Conservative, so safe, but it withholds 3.8 GB of headroom from other models for the life of the replica. It is the whole of the soak's `base_accuracy` FAIL (wd-vit is 0.0 %) | **User**; no option proposed, recorded for the next pass (a sibling of A3, opposite direction) |
| **F-D** | LOW (unexplained) | easyOCR degrades from **18.6 to ~12 it/s after cycle 2** and stays there. Not the ledger (`unit_budget` pinned at 2 000 000 throughout), not the hog (the 7.95 it/s pass ran through a calm window), not thermal (58 °C, 2 625/3 090 MHz, `clocks_throttle_reasons.active 0x0`). GPU utilization during OCR was **15 %**, so it is CPU-side | **Recorded, not explained** |
| **F10 / G2** | LOW | `pid: host` buys 0.00 % of base error on this driver and costs the container's PID isolation | **Decision taken: do not add it** |
| **F13** | LOW | `output_with_timeout` abandoned the timed-out `nvidia-smi` child and its reader thread (1.04 s of overlap measured) | **Fixed** in *Kill the probe child when the capability timeout expires* |
| **F14** | LOW | `accelerator_report` prints an unparseable `nvidia-smi` row back as hardware one line before `gpu.rs` rejects it. Two parsers, different strictness; the lax one logs first | **User** (cosmetic) |
| **T9** | LOW | Under constant external pressure the grant size alternates rather than converging (425/425/906/370/370/892/257) because headroom nets off the previous window's grant | **Informational** |
| **T10** | LOW | The worker's defensive clamp has no margin of its own: it ran at 98.2 % of free nine times without an OOM. It works because the fit over-predicts near the ceiling (A2) | **User** |
| **N8 / T11** | LOW | `sample_units` records the **granted** budget, not the batch actually run, so the stored pairs are not all measurements of their label. Theil–Sen absorbed it (0.008 % slope error) | **Informational** |
| **Q9** | LOW | A window whose inference failed for a non-OOM reason settles `clean`, so failures repay a previous OOM's deflation and `analyze.py` reports PASS while 100 % of requests are 500s | **Tool caveat recorded** |
| **Q10** | LOW | Log volume under a stuck model: 57 MB in 118 s (8 forwarded Python traceback lines per failed window); 2.6 MB per 60 s job through the F11 bug | **User** (relevant to the soak) |
| **A5** | LOW | At near-ceiling batches NVML per-process can read **below** `peak_reserved` (83 432 vs 85 078), because the allocator's OOM-retry path already released cached blocks | **Informational**; any check assuming `nvml ≥ reserved` must tolerate it |
| **A6** | LOW | The probe resolves a dtype by reading the instance attribute, a different path from the worker's; S2 confirmed the two agree after the dtype fix | **Closed** |
| **F3 / G3** | INFO | `ledger_invariant` cannot hold on a board another process owns: `charges_mb` 964–1 148 against `limit_mb = 0` | **Expected**, stated numerically |
| **Q11** | INFO | The knee estimator got one right: nemotron `knee_units = 33 554 431` = bucket 24 ≈ 32 items of 1 MP, exactly the probe's plateau. The counter-example to N1/T1 | **Informational** |
| **F-E** | INFO (positive) | The idle-replica sweep works under 8 h of rotation: between every pair of jobs the resident set drops to the loadgen's MiniLM alone and comes back with the next model, **26 times** in `healthrec`; **at most two models were ever resident**. This is P5-6's fix observed in the field | **Confirmed** |
| **F-F** | INFO (positive) | `analyze.py`'s new `calibration_learned` row — the H1 hole closed during Phase 8 — **fires correctly on its first real leg**, naming easyOCR: *"NOTHING WAS LEARNED: fit samples == 0 for doctr/easyocr_standard_en; peak unit_budget never left the seed"*. Report-only on S9, which is not a learning scenario, and it independently found F-B | **Confirmed** |
| **Q12 / N9 / N10 / P5-8 / P5-9 / P5-10** | INFO | Protocol and fixture corrections: torch-free fixtures are never ledger-admitted on CUDA; `oom_second_batch` OOMs forever; the `out of memory.png` vector does not exist (the worker gets bytes, never names); `scan_audio` defaults to false; `.txt` is not indexable so the MiniLM job leg is impossible; `sentence_transformers` emits no bucket DEBUG lines; B20's bound is 64 (the registry's `default_batch_size`), not 32; B21 is unreachable (no shipped `prepare()`); T5's per-model `last_grant` question needs a multi-board replica config | **All corrected in the plan, the codemap and the fixtures** |
| **G7** | INFO | The feedback signal shipped as a response header, `x-panoptikon-desired-in-flight-items`, not a body field: only the JSON envelope has room for a scalar, so a body field would be absent exactly for the image and embedding models G7 is about | **Landed and verified** |
| **B19** | INFO | Docker CPU: board total 64 137 MiB, budget 48 102 MiB, cgroup `memory.max` 16 384 MiB, which is a 2.94× overcommit. Nothing in the CPU path reads `memory.max` | **User** |

### The user-decision items, with the options as the reports phrased them

Options are quoted from the report that raised each finding. The
recommendation column is filled only where `orchestrator-state.md` records
one; blank means the orchestrator deliberately took no position.

| Id | Options | Orchestrator's recommendation |
|---|---|---|
| **F7** | Fail the window, not the job: re-queue the in-flight set of a died-on window instead of recording the items as errors | Decide before release; it is the one open item where a user sees "completed" after a fraction of the work |
| **Q1** | (a) require a driver-shaped pattern (`CUDA out of memory`, `INFERENCE_OOM_*`, `HIP out of memory`, `torch.OutOfMemoryError`), since every real path emits one; (b) corroborate with the worker's live free-memory reading at the failure before deflating; (c) keep the match but log the classification so a spurious deflation is diagnosable | n/a |
| **Q2 / F4** | (a) cap deflation at what takes the budget to 1 (≈ `log2(anchor)` levels); (b) repay by *time* as well as by clean windows; (c) reset when a window settles clean **and** headroom is ample; (d) clear it on respawn | n/a |
| **T1 / N1 / P5-4** | (a) do not feed throughput observations from windows whose budget was `squeezed`, memory-blind, or clamped by the worker, all three of which the ledger already knows at grant time; (b) never persist a locally-fitted knee; (c) require the knee's buckets to span the ramped range (no knee below `max_units_measured/2`); (d) make the knee a growth brake, not a ceiling. For P5-4 additionally: let the knee expire after N clean windows at the cap; do not fit a knee while headroom is below the model's appetite; key the knee on a headroom band | **(a)**, and after F-A also **(d) or the P5-4 expiry**. (a) alone prevents both persisted instances and costs nothing the ledger does not already know; the soak showed that a knee already in the store is never revisited, so a brake or an expiry is what bounds the damage of one that is wrong |
| **P5-2 / T4** | (a) apply the margin to the board *total* (`limit = total × (1−m) − external`) so the reserve is a fixed slice; (b) floor the limit at our own residents' footprints so the invariant is expressible; (c) keep the arithmetic but clamp `mb` to `max(free − reserve, 0)` measured live, so a squeezed grant is still priced rather than blind; (d) scale the margin by observed external volatility with the configured margin as a ceiling | n/a |
| **T3** | (a) have the worker report free memory per batch, not per window, it already reads it for the clamp, and this would have turned S4b's 31 s into ~12 s; (b) run the host probe on `sweep_interval_secs`; (c) document `external_mb` as a per-window quantity | n/a |
| **P5-3 (B18)** | (a) hold the load lock only around the manager's map mutation, not the `load()` round trip; (b) per-model locks with a separate admission gate for the board; (c) if the global lock is deliberate, document it and surface "loading &lt;model&gt;" so the stall is explicable | n/a |
| **A1** | Any model whose batch can reach 2 048 items needs either an absolute item cap or a non-OOM failure path | n/a |
| **N3 (W5)** | (a) put the allocator config in the profile key; (b) record it and invalidate on change; (c) normalise the worker's high-water measurement; (d) document and rely on the backstop | n/a |
| **Q3 (W1)** | (a) implement the design's deferred `unit_cap_per_item` (price `min(raw_pixels, canvas)`), the one change that fixes the slope *and* the utilization; (b) record the resolution mix in the profile key; (c) accept and document that `pixel` profiles are corpus-specific | n/a |
| **F-B** | Q3's three options apply unchanged; the soak adds one of its own: treat "thousands of grants with `fit samples 0`" as a warned condition, because a model that never accumulates a sample can never correct a priced quantity that has drifted this far from its footprint | n/a |
| **Q4** | Cap the token price at the model's `max_seq_length` (a registry key the impl already knows), or document the direction | n/a |
| **Q5** | (a) per-model cooldown after N consecutive load failures; (b) exponential backoff on respawn; (c) surface "model unavailable" for a cooldown window instead of re-loading | n/a |
| **Q7** | Apply the fallback (or at least one halved retry) to single-request windows too; or document that batching clients are strictly better protected | n/a |
| **N5** | (a) clamp the stored anchor to `max(sample_units)` at load; (b) lower it when a window settles negative/oom at or below it; (c) treat it as advisory when the fit says it will not fit | n/a |
| **N6** | Merge on write (keep the higher anchor / more samples), or refuse to write a profile strictly worse than the one on disk | n/a |
| **N7** | Optionally take bigger steps when the fit residual is small and headroom is large | n/a |
| **N2** | Measure properly (§8b); likely a pipelining gap, a 1 853-item window returns in one lump and is written while the GPU idles | Post-run work per plan decision 9 |
| **P5-5** | (a) suppress the collapse verdict while other replicas on the board are running windows; (b) compare against a rate measured in the same contention regime; (c) require a memory signal to corroborate | n/a |
| **P5-7 (B17)** | (a) drop the trim on a busy or unresponsive replica instead of treating a timeout as fatal (the pool release is optional by definition); (b) if it must be fatal, fail the queued requests with a distinguishable error and retry them after the reload | n/a |
| **F1** | Refresh external usage on the load path. This is a behaviour change | Partly mitigated by the T2 fix, which now probes an unsampled board before pricing a load |
| **F5** | Set `LD_LIBRARY_PATH` for CUDA workers as the repo already does on the ROCm path, or document the requirement | Shared with master; not a PR regression, but a shipped-model load-path defect on bare Linux |
| **F6 residual** | (1) `ulimits: nofile` in the shipped compose files; **(4) stop paying two sockets per in-flight item, in-process dispatch rather than loopback HTTP** | Options (2) and (3) were taken as the fix; (1) is optional and (4) is the user's call |
| **F9 (W4)** | Keep W4 open; make `base_method` a recorded field in every platform pass | Adopted into plan §9 |
| **D1 / D2** | D1: store `dtype_method` in the profile? D2: sentinel name `"unknown"` vs `"unstated"` | n/a |
| **B19** | Nothing in the CPU path reads the cgroup `memory.max` | n/a |
| **F14** | Reuse `gpu.rs`'s parse, or print `<unreadable>` | n/a |
| **Q8 / T8** | Report only (outside the batch-calibration feature) | n/a |
| **Multi-replica** | Replicas of one model share one `ModelStats`, so the published in-flight figure is whichever replica formed a window last; a `min` policy is the conservative alternative | Advisory and self-correcting; revisit if the soak shows thrash |

---

## 5. Fixes landed during the run

Nine low-discretion fixes, each made by an executing agent on its own commit
and then reviewed by a **separate** verifier agent, iterated until the verifier
was satisfied. This is protocol §0 decision 5. The full classification
rationale is in the plan's §0 decisions table; the short form is here.

| # | Commit subject | What it does | Verification | User-visible behaviour change? |
|---|---|---|---|---|
| 1 | *Force uv copy link mode in the Docker build so it works on ZFS-backed hosts* (`5b7c7353`); comment fix `3cff5c8c` | One `ENV UV_LINK_MODE=copy` in the `Dockerfile`, before the `panoptikon setup` layer | APPROVED WITH FIXES (dtype-verify.md); comment corrected in its own commit | **No.** Build-only. The shipped `Dockerfile` could not build at all on this host (uv's reflink on overlay2-over-ZFS, `os error 11`); the alternative was to test an image the shipped file cannot produce |
| 2 | *Infer a model's dtype and log why a calibration update is skipped* (`58dda519`; design-doc clarification `918ec170`) | Worker infers a resolved dtype from the loaded weights (selected > attribute > inferred > `"unknown"`), adds a `dtype_method` protocol field, and logs every early return of `queue_profile_update` once per (model, board, reason) | APPROVED WITH FIXES (dtype-verify.md): module-namespace walk guard, adversarial test, README. 376 inferio tests; 237 pytest pass / 13 skipped | **Yes, and intended.** The design says the profile persists; before this it never did. The store key's meaning is unchanged. Flagged as a reversible judgement (D1, D2) |
| 3 | *Do not reattribute a departed worker's VRAM to external usage* (`d17e3854`, verified fix-up of `cc78394a`) | Credits a departed replica's footprint back to the board's free reading, stamps `free_adjusted_at` to force a refresh, and refuses pre-departure samples | APPROVED WITH FIXES (n4-verify.md): one wrong comment fixed, one bounded skew documented, three tests added. 380 inferio tests. Later hardened with a `loaded_at` gate | **No.** An accounting bug whose only output was a phantom; none of it is a policy choice |
| 4 | *Probe the board before pricing a load against it* (`8546cd63`, fix-up `ff34b059`) | One NVML probe inside `reserve_for_load` when the board has no usable sample, on a path that already costs tens of seconds. The guard already existed and could never fire | Accepted with fixes (t2t5-verify.md): the inline query blocked a Tokio worker thread, later removed entirely by `c8c26c54`. 389 inferio tests | **No** to the guard's semantics; it makes an existing warning reachable. (This fix is also what introduced F11, see §7) |
| 5 | *Publish the granted budget when a window's grant was squeezed* (`22eb33f9`, fix-up `ff34b059`) | `Grant.squeezed` plus `in_flight_target_units`, so the published figure follows the grant the same code path just issued | Accepted with fixes (t2t5-verify.md) | **No.** The published figure contradicting the grant is a defect in the feature added for this run, not a new policy |
| 6 | *Clear the board's refreshing flag even when the host probe panics* (`8c696ac0`, fix-up *Pin the probe guard's disarm and correct its abandoned-task claim* `eb398d69`) | A drop guard, so a panicking probe cannot strand `refreshing = true` and block every future refresh on that board | Correct as claimed (refreshing-guard-verify.md); one doc claim corrected, the disarm pinned by a test a mutation showed nothing caught. 390 inferio tests | **No.** A missing drop guard; textbook low-discretion |
| 7 | *Report every worker death with pid, signal and killer; sweep idle replicas* (`c8d64a5a`, amended from `80a1d836`) | A `WorkerDeath` record with pid and signal, a WARN on every death, gateway-kill attribution, and an idle liveness sweep via `DispatchMsg::ReapIdle` | Accept with one fix applied (p5-verify.md): the attribution boolean was wrong on the fatal paths. 393 inferio tests | **No.** Log fields plus a liveness sweep on the existing tick; no admission arithmetic moved. P5-1's premise had to be corrected: the exit status and stderr tail *were* already logged on the predict path; the gaps were the pid, the requestless death and the kill attribution |
| 8 | *Bound the in-flight window by the process's descriptor budget* (`d5e42c78`, sweep `13e8850f`, docs `25153334`) | New `rlimit.rs`: raise `RLIMIT_NOFILE` soft to hard at the first statement of `main()`, and clamp the in-flight ceiling by `(soft − 256) / 2`, with the floor of 64 winning and a WARN | PASS, no bug found (f6-verify.md); fix-up adds two tests (a sweep 384..896, a macOS never-lower test), two comments, an AGENTS.md note. 393 inferio tests | **Yes, and it completes G7.** G7 made the in-flight budget follow the grant and nothing in it knew about descriptors. Failure to raise the limit is never fatal; the process is never shrunk |
| 9 | *Fork every supervised child from one thread that never exits* (`f9cf10fa`), *Tell a dying worker from one the gateway killed* (`5becf29c`), *Kill the probe child when the capability timeout expires* (`ed0b1f0e`), *Await the load-path host probe instead of block_in_place* (`c8c26c54`), plus `532c15d2`, `dc613400` | A permanent `panoptikon-spawner` thread that all four `die_with_parent` sites fork through, so PDEATHSIG can never fire on a reaped pool thread; a three-valued `DeathAttribution`; the timed-out probe child is killed and reaped; no `block_in_place` left anywhere in the tree | Verified (f11-verify.md): two gaps closed in `6a5e6799` (spawner `catch_unwind`; kill the child when the requester is cancelled) plus a stranded-frame attribution test. 399 inferio tests. **No rebuild needed** for the `dc613400` binary — and Phase 8's Leg 0 rebuilt at `6a5e6799` and measured a byte-identical store, so the claim is confirmed rather than argued | **No.** Fixing a regression the run's own fix 4 created is not a design change |

Two decisions were made **not** to change anything:

- **`pid: host` in the shipped compose: do not add it.** It buys 0.00 % of
  base error on this driver and costs the container's PID isolation. The claim
  is driver-specific, so it becomes a per-platform check, not a shipped
  default.
- **The file-descriptor exposure found by Track A's verifier** was first
  handled as "every scenario greps for `EMFILE` and records `ulimit -n`"
  (option (a)). Phase 6 found the real limit case and it became fix 8.

Pre-existing failures that are **not** this PR's: one `db::batch_auto` test
that assumes the process cannot write through a read-only mode bit (a host
artefact), and two `media_tools::transcode` ffmpeg-budget tests that fail
identically on the master worktree. No CI job runs `cargo test`, so nobody had
seen the latter two before (phase1-report.md).

---

## 6. Ground truth measured

Every quantity the feature computes was compared against a measurement taken
by a different instrument, from outside the process. This is the table the
ledger's own numbers were adjudicated against.

### Per-model cost model (`ceiling_probe.py`, GPU 1, 97 249 MiB free before each load)

| Model | unit / aggregation | base (NVML) | base (`free_delta`) | slope MiB/unit | intercept | residual | n | OOM boundary | throughput peak |
|---|---|---|---|---|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | item/count | 964 | 973 | **50.5403** | −11.63 | 2.77 | 12 | **2 560 ok / 2 816 fail** | **38.3 items/s @ 8**, flat 34–38 across 1→2 048 |
| `clip/apple_MobileCLIP-S1` | item/count | 732 | 741 | **16.7539** | −45.0 | 5.13 | 11 | **2 047 ok / 2 048 fail, not an OOM** | 105.0 items/s @ 128 (22.3 @ 1) |
| `textembed/all-MiniLM-L6-v2` | token/max×count | 654 | 663 | **0.0245317** | −0.83 | 4.27 | 13 | 17 749 items = 4 596 991 units | 1 073 items/s @ 2 048 (149 @ 1) |
| `clip/nemotron-embed-vl-1b-v2` | pixel/sum | 3 788 | 3 797 | **0.000267834** | −37.38 | 5.87 | 9 | 468 items = 490 733 568 units | 12.0 items/s @ 32 (9.1 @ 1) |
| `doctr/easyocr_standard_en` | pixel/max×count | 796 | 805 | 3.88e-06 | **1 018.9** | 17.27 | 8 | **none, memory is flat in batch size** | 21.3 items/s @ 64 (20.1 @ 1) |

### Instrument offsets

| Quantity | Value | Consequence |
|---|---|---|
| CUDA context | **666–668 MiB** on this driver | The fixed 500 MiB estimate the degraded base tier uses (W4) is 25 % low against it, and untested |
| `base_free_delta` − `base_nvml` | **uniform +9 MiB** on all five models | Either tier can serve as ground truth for `base_mb` |
| Board `used` vs a known allocation | **+2 MiB** at 10 240 and 40 960 MiB, both boards | The oracle is anchored in a physical fact, not in agreement between two readers |
| NVML per-process vs a known allocation | **−6 MiB** | Same |
| `nvml_own` − `peak_reserved`, steady state | 668–678 MiB at every batch size of every model | The context |
| …at load | **550 MiB** for all four torch models | **A3: `base_mb` under-states the steady state by 118–128 MiB**, paid once on the first batch (wd-vit +126, MobileCLIP +128, MiniLM +118, nemotron +128, easyOCR +0, it runs kernels during load) |
| `nvidia-smi` total vs torch total | 97 887 vs 97 250 MiB (0.7 %) | Inside the sample-vs-board check; record it per platform |
| Steady-state `external_mb` vs oracle with a worker resident | constant **127–129 MiB** in every Phase 3 profile | The A3 workspace, well inside the ±1 GB allowance |
| Linear fit vs measured boundary | conservative by **17–36 %** (wd-vit +34 %, MiniLM +17 %, nemotron +36 %) | The ledger under-shoots the true boundary, which is the safe direction, and it is why the worker's clamp survives at 98.2 % of free |

### What the ledger learned, against that ground truth

| Model | probe slope | learned slope | error | learned base | error |
|---|---|---|---|---|---|
| wd-vit | 50.5403 | **50.5625** | **+0.04 %** | 964 | **0 %** |
| MobileCLIP | 16.7539 | **16.7047** | **−0.29 %** | 732 | **0 %** |
| MiniLM | 0.0245317 | 0.037323 | **+52.1 %** (conservative) | 654 | **0 %** |

The written `sample_reserved_mb` values are **byte-identical to the probe's own
delta column** at every batch size both measured (phase2b-report.md).

---

## 7. Protocol assessment

### What the protocol caught that unit tests could not

The feature has 827 Rust and 215 Python tests, all against fixtures. Here is
the detection path for each blocker and each near-blocker. None of them is
expressible as a unit test.

| Finding | How it was caught |
|---|---|
| **dtype persistence** | S1 finished, every criterion passed, and then a `grep -c 'dtype='` over the run's log returned **0**. The absence of a `tracing` field *is* the evidence; nothing in the code path errors. Only a real host with real shipped models produces this |
| **F6** | S11 ran the same 2 000-item job in the shipped container and against the master-built image, back to back, and counted file descriptors on pid 1. The CI Docker job never runs an extraction, which is exactly why it would not have caught it |
| **F11** | Phase 7a's dedicated investigation leg: the gateway under `strace -f -e trace=clone,clone3,exit,exit_group,kill,tgkill`, a 40-line C control proving the kernel behaviour, and three independent negative controls. The deciding number is Δ 1–3 ms between the forking thread's `exit(0)` and the worker's SIGKILL, 8/8 |
| **N4** | An independent 4 Hz NVML recorder disagreeing with `/health` by 27 GB while `nvidia-smi` showed no process. `oracle_agreement` is the only check that could see it |
| **T2** | S4g put a hog on the board and asked the ledger to load a model that could not fit. The guard's silence (`grep -c "loading this model"` = 0) was the finding |
| **Q1 (B11)** | A purpose-built fixture raising a `ValueError` whose text contains "out of memory", and the same fixture with different wording. 15 negatives versus 0 |
| **A1** | The ground-truth ceiling probe, bisecting to 2 047 ok / 2 048 fail. The ledger's own arithmetic extrapolates to ~5 700 items and would never discover this |
| **P5-3 (B18)** | Latency percentiles of in-flight predicts banded around a `PUT /load`. Needs concurrency the job queue cannot produce, which is what `loadgen.py` exists for |
| **F7** | Counting items processed against items submitted, on a job the server reported as *completed* |
| **F-A** | Eight hours of rotation: 13 job passes and 56 worker spawns against a single knee fitted at minute four, with `/health` reporting `last_grant_units: 1` throughout. No shorter leg can tell "fitted once, correctly" from "fitted once and never revisited"; the store is the same file either way |

### What the protocol missed until a later phase

- **F2 was filed as a defect in Phase 1** (`knee_units = 1`) and vindicated in
  Phase 2a: wd-vit's throughput is flat within noise across a 2 048× range, so
  the knee was correct ground truth. The criterion is now "does the fitted knee
  match the probe's curve for **this** model", never "is the knee small".
- **N1/T1 then showed the same estimator misfiring** for a different reason
  (sample rate, and externally forced variance), and Phase 4 found it getting
  nemotron exactly right (Q11). Three phases to separate the mechanism from the
  number.
- **F8's SIGKILLs were seen from Phase 5 onward** and were deferred twice
  before Phase 7a root-caused them as F11. Budget an investigation phase
  whenever deaths have no traceback and no kernel OOM.
- **F11 was introduced by this run's own fix 4** and was invisible in Phases 2b
  and 3 because a board kept warm by worker NVML samples suppresses the probe
  entirely. It took a cold board plus an oscillating hog to reproduce within
  minutes.
- **The knee's *permanence* needed eight hours to see.** Every phase before the
  soak measured the knee's *value*; only a run long enough to contain 13 job
  passes and 56 worker spawns could show that nothing ever refits it (F-A).
  A scenario that ends when its corpus does cannot express "and then it never
  changed again".
- **W3 is still open**: no leg produced a real impl-side absorbed OOM under
  pressure. It needs a hog plus a large batch on MobileCLIP.
- **W4 is still open** on every platform, because B9's predicted degradation
  did not happen here.

### Tool bugs found in the protocol's own instruments

| Fix | Bug |
|---|---|
| `16e1f2d7` | `check_base_accuracy` joined with the nearest oracle sample, which for a load timestamp picks the one **59 ms before** it (mid-load, 812 MiB) over the one 186 ms after (974 MiB), reporting 18.72 % error where the truth is 1.03 %. NVML's per-process figure rises throughout a load, so the join must not look backwards |
| `a8054b52` | An OOM absorbed by an impl's own retry appeared in the bisect trace as `ok: false, error: null`, indistinguishable from a hard failure with a lost message. Also `free_mb_at_start` was sampled after the sweep and understated what a bisect probe can use |
| `0bea8326` | The bisect always doubled from 1, so on a 97 GB board the grow phase re-ran every cheap size; each refinement near wd-vit's boundary costs 60–170 s. Added `--bisect-start` and `--bisect-budget` |
| `e67705c2` | `oracle_calibrate.py` passed `hold + alloc_timeout` to the hog as its **lifetime**, so `--alloc-timeout 2400` held for 40 minutes after filling in 107 s. A 2 048 MiB RAM leg went from 640 s to 41 s |
| `23e5a468` | `analyze.py` had no way to express "this job is supposed to fail" (`--expect-failed-jobs`); `ledger_invariant` burned a FAIL on the `limit_mb == 0` arithmetic; no `peak_fds` check existed |
| `1f1a69c2` | `analyze.py` parsed **0 events** from a raw `docker logs` capture: the gateway writes ANSI colour and `docker.toml` sets `[logging] file = ""`, so `docker logs` is the only sink, `LOG_LINE` broke on the first escape, and `grant_safety`, `failures` and `persistence` came back as three silent SKIPs. `parse_log` now strips ANSI; the raw C4 capture went 0 → **2 124 events** and the bare-host logs are unchanged. Also: the README's fd-recorder recipe read the wrong limit, because `docker exec … ulimit -n` prints **1024** (an exec is a new process with the container's OCI rlimit) while pid 1 has raised itself to **524 288**; the recipe now reads `/proc/1/limits`, so `peak_fds` stops reporting a percentage against a limit 512× too small |
| `8d0fabfe` | `loadgen.py` had no rate control, so a soak's "low-rate background load" was not expressible at all: a slot could only run flat out. An `interval=` spec field now paces a slot's request *starts*, so a model's rate is `concurrency / interval`, documented in the tools README. S9 used it to hold MiniLM at one 16-item request every 5 s for eight hours (5 756 requests, 5 756 ok) |
| `3bf4ba76`, `4ea6d5f6` | The five holes S15 found in the protocol's own checks (H1–H5): three of `analyze.py`'s verdicts were SKIPs that never fail a run, `hog_tracking` had no FAIL form, `ledger_invariant`'s strict form PASSes on a broken ledger, and the plan's stated S15 expectations were wrong. Closed as described above: the `calibration_learned` check, the SKIP split, the `hog_tracking` FAIL form, the `grant_safety` WARN, and the rewritten plan §4/§6 with m4/m5 suggested |

Two instrument gaps remain: **no file-descriptor recorder was written** (the
`peak_fds` check reads what a scenario records, and the two-line loop Phase 6
used is documented in the README, now corrected to read `/proc/1/limits`), and
`--plot` needs `matplotlib`, which is absent here.

### Corrections already applied to the plan

`4d1f0af5`, `23e5a468` and `960edf61` revised the protocol and the code map
from what this run measured: a §0 decisions table (the nine fixes and the two
non-changes), a single `oracle_calibrate.py` recipe in §2 with its run1
numbers, eight operational notes in §3, Run1 paragraphs on eleven scenarios in
§4, a *Status after run1* column filled for all 28 probes in §5, a *Run1
actual* column plus the 7a/7b rows and the binary-commit rule in §7, and five
new per-platform checks in §9. The code map gained `Grant.squeezed`,
`WorkerDeath`/`DeathAttribution`, `rlimit.rs`, the fixture inventory and the
S13 shims. `4ea6d5f6` then rewrote §4's S15 expectations from what the
mutations actually did, added an H1–H4 table recording each hole and its
closure, added m4/m5 as suggested mutations, and gave §6 a
`calibration_learned` row, an external-tracking row, the `grant_safety` WARN
rule and a "SKIP is not a result" rule.

Eleven contradictions between phase reports were adjudicated and recorded in
`protocol-revision-report.md`; the resolution rule was "the later phase or the
verifier wins". The substantive ones: the knee of 1 (correct for wd-vit, wrong
mechanism elsewhere), B14 (restated: the risk is models that never accumulate
fit samples, not host shape), grantless VRAM (Phase 4's Q6 scoped to the
never-refreshed case), P5-1's premise (the exit status *was* already logged),
B9 versus W4 ("B9 refuted" must not read as "W4 cleared"), B13 refuted while
F13 is real, B20's number (registry-dependent), the C2 fixture leg (torch-free
fixtures are never ledger-admitted on CUDA), and repo commit versus binary
commit.

### S15, the protocol's self-test

Three throwaway branches from `dc613400`, each in its own worktree with its own
`target/`, each running S2 and S4b against the deliberately broken product. All
three worktrees and both `s15-*` branches were removed afterwards. One
deviation applies to all three: S4b's +30 GB step is at **t = 20 s on the
2 000-item `ramp` corpus**, not t = 60 s on `ramp8`, because six legs with a
five-minute rescan each would not fit. The 58-second job still leaves ~35 s of
post-step observation and the hog demonstrably reached 30 720 MiB.

**All three mutations were caught. None was caught by the check the protocol
predicted.** The healthy column is `S2-wdvit-fixed`.

| check | healthy | **m1** worker halves `peak_reserved_mb` | **m2** `external_locked` → 0 | **m3** `margin = -0.5` |
|---|---|---|---|---|
| *startup* | ok | ok | ok | **REJECTED** at config load |
| `grant_safety` | PASS 10/10 | PASS 96/96, 85/85 | **FAIL**: 335 grants, 0 exceeded the priced headroom, **335 exceeded the oracle's live free memory** (both legs) | n/a |
| `slope_accuracy` | PASS 1.0004 | **SKIP** `no calibration.after.toml profiles` | **FAIL** `ledger 0.0 vs probe 50.5403` | n/a |
| `utilization` | PASS 0.40 | **FAIL** peak `unit_budget` 8 / boundary 2 560 = 0.00 | **FAIL** peak 8 (S4b: 2) / 2 560 = 0.00 | n/a |
| `oracle_agreement` | PASS 137 MiB | PASS (S2); FAIL 30 593 MiB (S4b, the hog, B2) | PASS 646 MiB (S2); **FAIL 32 031 MiB, 154 breaches** (S4b) | n/a |
| `hog_tracking` | SKIP | INFO `external_mb 1306..32160` (tracked) | **INFO** `external_mb 0..0 MiB` against a hog holding 0..30 720 MiB | n/a |
| `ramp_progress` | INFO peak 1 024, 10 fit samples | INFO peak 8, **0 fit samples** | INFO peak 8 / 2, 0 fit samples | n/a |
| `persistence` | PASS 29.8 s | **SKIP** `no calibration.after.toml` | PASS 0.0 s (a degenerate profile was written) | n/a |
| `failures` | PASS | PASS 0 OOM, 0 deaths | PASS 0 OOM, 0 deaths | n/a |
| `ledger_invariant` | PASS | PASS | **PASS** (the strict form is blind) | n/a |
| `throughput` | PASS 0.93× | **PASS 1.15×** (the wrong direction) | PASS 1.09× | n/a |
| `job_outcome` | PASS | PASS 2 000/2 000 | PASS 2 000/2 000 | n/a |
| **would it have shipped?** | n/a | **No, but only just** | **No, three FAILs before any hog is started** | **No, the server will not boot** |

**m1 does not produce "slope ≈ half".** Halving the reported high-water puts it
*below* the post-load baseline (964 MiB base against a halved peak of ~500), so
the `grew` test never fires: `high_water_samples=0` on all 96 grants. The anchor
never leaves the seed, `unit_budget` reaches 8, no fit ever forms, and no store
is ever written. The throughput-knee estimator, which is driven by sample
arrival rate rather than memory (finding N1), then fits `knee_units=7` from 13
observations twelve seconds in and pins the model there. The direction is
**sandbagging**, so S4b produced 0 OOMs and the job ran *faster* than baseline.
Both catches the protocol named, a slope FAIL and S4b OOMs, missed.

**m2** put `external_mb=0` on every grant, so `limit = total` and 96 923 MiB of
headroom was offered on a board whose oracle free reading was 96 285 MiB
*before* our own resident. The store it wrote is degenerate (slope 0.0, 0
samples, `max_units_measured` 1) and is now flagged poisoned in the run1
README.

**m3 is rejected at config load** by `Settings::validate`, with the key, the
constraint and an example in the message: `inference_local.vram margin must be
a finite number >= 0 (got -0.5); it is a fraction of other processes' VRAM
usage, e.g. 0.10 for 10%`. `ledger.rs` carries the same assumption as defence
in depth (`margin.max(0.0)`, `cap_fraction.is_finite()`). No scenario is
reachable, so this is **not** a protocol hole; the protocol's "expected catch
in the S4 series" is simply the wrong expectation.

#### The five holes, and the checks proposed

These are findings against the protocol, not against the product. An undetected
or under-graded mutation is a hole in the verdict table. **All five are now
closed**; the closure is described after H5.

**H1, the real one: a fault that destroys the measurement also destroys the
evidence, and `analyze.py` turns that into SKIP, which never fails a run.** m1
made both `slope_accuracy` and `persistence` SKIP rather than FAIL. It was
caught only by `utilization`, and only because the leg was run with `--probe`;
several run1 legs were not. Run m1 without probe files and it reports all green
with three SKIPs.

> **Proposed check `calibration_learned` (FAIL, not SKIP).** For a leg that
> declares itself a learning or cold-ramp scenario, FAIL on any of: 0 fit
> samples, no profile in `calibration.after.toml`, or a peak `unit_budget` that
> never left the seed. **All three numbers are already computed**; the
> `ramp_progress` row prints them as INFO today. Promoting them to a verdict
> costs nothing and closes the whole class of "the instrument stopped
> reporting" faults.

**H2: `slope_accuracy` cannot distinguish "no store" from "no probe".** Both
are SKIP. The first is a result, the second a harness omission. Split the two
verdicts.

**H3: `hog_tracking` is INFO only.** `external_mb 0..0 MiB` while a hog holds
30 720 MiB is not an observation, it is a failure. A safe FAIL form, one that
does not re-fail the known B2 staleness on quiet boards: FAIL when
`external_mb` never moves at all across a recording in which the hog held
≥ 1 GiB for longer than the ledger's staleness window.

**H4, documented rather than checked: `ledger_invariant`'s strict form PASSes
on a completely broken ledger.** m2 is the empirical proof that the run1
README's warning was right: 0 of 498 samples over `limit_mb`, because
`limit_mb` *is* `total`. Only `grant_safety`'s second clause, the one that
joins `healthrec` to `vramrec` and asks whether a grant exceeded the oracle's
live free memory, has teeth. §6 should say so: the check that decides safety is
`grant_safety`, and it needs `vramrec.jsonl`.

**H5: §4's stated S15 expectations are wrong for m1 and m3.** m1's catch is
`utilization` plus the proposed H1 check, not slope, and m1 sandbags rather
than OOMs; m3 never reaches a scenario. Suggested replacement probes for the
same knob, neither exercised in run1: `cap_fraction = 1.5` and
`cap_fraction = NaN`.

#### How the five holes were closed

Two commits, landed during Phase 8 alongside the soak: **`3bf4ba76`** *Turn
analyze.py's silent SKIPs into verdicts (S15 holes H1-H4)*
(`tools/calibration-protocol/analyze.py` +418/−46, its `README.md` +59) and
**`4ea6d5f6`** *Correct S15's expectations from run1 and record holes H1-H4*
(`docs/batch-calibration-test-protocol.md` +96/−9). `analyze.py` was written to
a temp file, `py_compile`d there and `mv`'d into place in one step, so the
soak's hourly run never saw a partial file.

- **H1** is a new check, **`calibration_learned`**. A leg declares itself a
  learning scenario with a new `--learning` flag or by naming the check in
  `--checks`; declared, it **FAILs** on any of `fit samples == 0`, no
  `[[profile]]` in `calibration.after.toml`, or a peak `unit_budget` that never
  left the seed. Undeclared it is INFO with a note saying how to make it a
  verdict. The numbers come from a shared `_budget_rows()` that `ramp_progress`
  now uses too, so the verdict and the report-only row cannot disagree. On
  `S15-m1-S2` **with no probe file at all** — run1's actual blind spot — the
  declared invocation now exits 1 with three FAILs where it used to exit 0, all
  green with three SKIPs. Its first real firing is F-F.
- **H2**, the SKIP split, in `slope_accuracy`, `utilization` and `persistence`:
  "no store was written" / "no worker was ever admitted" is now **WARN** (FAIL
  under `--learning`), while "no probe file" / "no log lines" / "no healthrec"
  is **SKIP with a pointer** naming the flag or the recorder to supply.
- **H3**, a FAIL form for `hog_tracking`: FAIL only when the hog held ≥ 1024 MiB
  for more than **60 s** (`HOG_STALL_SECONDS`, summed over intervals where both
  endpoints were above the threshold, so an oscillating hog is charged only for
  the time it was up) *and* `external_mb` took exactly one distinct value across
  the whole recording. `S15-m2-S4b` FAILs; **S4d, whose worst sample age is
  166.9 s, correctly stays INFO because `external_mb` moved**, and
  `S11-C4-fixed`'s genuinely quiet board is protected by the 60 s threshold at
  30 s held.
- **H4**: `grant_safety` returns **WARN instead of PASS** when zero grants
  joined a `vramrec` sample, distinguishing "no `vramrec.jsonl`" from "nothing
  joined within `--join-tolerance`"; and `--list-checks`, the tools README and
  the plan's §6 now all state that the check deciding safety is
  `grant_safety`'s oracle clause and that it needs `vramrec.jsonl` — with m2's
  numbers as the proof (`ledger_invariant` 0/498 against the oracle clause
  335/335).
- **H5**: the plan's §4 S15 section was rewritten to m1's and m3's real
  behaviour, and **`cap_fraction = 1.5` and `cap_fraction = NaN` are recorded as
  m4/m5, suggested for the next pass and not run**, so what `Settings::validate`
  does with them is stated as a question, not a result.

Regression evidence: **exit codes are identical for all 62 run1 legs with a
`healthrec.jsonl` under `--checks all`**, before and after, so no existing
invocation changes its verdict and the soak's hourly runs were unaffected; the
escalation happens only under `--learning` or an explicit
`--checks calibration_learned`. Across the 14 run1 legs that have a
`hog.jsonl`, exactly one FAILs `hog_tracking`. No leg was re-run: this is
`analyze.py` replayed against the existing recordings. The caveat carried
forward is that the 60 s threshold is calibrated against this host's 14 hog
legs only, so the INFO detail always prints the held-seconds and the
moved/not-moved numbers for a human to adjudicate on a slower platform.

---

## 8. Portability: what each platform pass must check first

The scenarios are identical everywhere; the oracle and the pressure generator
differ. A platform passes when S1, S2, S3, S4a–d, S5, S14 and its own
field-pass items pass; S6–S13 are run here and on the second multi-GPU host,
and S9 once here.

| Platform | Oracle | Pressure | Check first |
|---|---|---|---|
| Ubuntu NAS, RTX 3090 (Ampere) | Same as here | Same | Single board; smaller headroom makes S4 tighter. **P5-2's arithmetic zeroes the budget below ~2.2 GB free on a 24 GB card**. This is the platform where T4/P5-2 hurts most |
| Windows desktop, dual RTX 5090 (WDDM) | NVML per-process returns N/A on WDDM; use PDH "GPU Process Memory" counters or `nvidia-smi --query-compute-apps`; expect base tier `free_delta` | Same (torch) | **This is the platform that finally tests W4.** Sysmem fallback means over-admission shows as throughput collapse, not OOM, so S4c must watch `throughput_collapse` and per-batch `duration_ms`; run once with "Prefer No Sysmem Fallback". S15 mutation 1 is the key sensitivity test. Two boards, so S7 is the monitor-asymmetry test |
| MacBook Pro M3 Max 128 GB (MPS) | No per-process GPU counter; `vm_stat`/psutil, `sysctl iogpu.wired_limit_mb`, and the worker's `driver_allocated`; base ground truth from `ceiling_probe.py` in process | RAM hog plus an MPS hog | Total adoption from the first load, re-adoption after raising the wired limit under a running gateway, near-ceiling GC bias, jetsam death-as-negative. **macOS `nofile` defaults are much lower**, the F6 clamp's macOS fallback (10 240, which is `OPEN_MAX`) is the untested path |
| BC-250 (ROCm APU) | amdgpu sysfs plus DRM fdinfo per pid; `rocm-smi` as a second reader | torch on HIP | Which total HIP reports, GTT spill as slowdown rather than OOM, **OOM string forms, Q1's classifier is string-based, so this platform is where a wrong pattern bites** |
| Linux Desktop / Nix | Same as here | Same | Only packaging differs; S14 plus S2 suffice |

### The five per-platform checks run1 added

1. **`base_method` at load** (`grep 'base_method'`, or the presence of the
   `NVML lists no process with pid` warning). Driver- and container-runtime
   dependent, not container-shape dependent: B9's predicted degradation did not
   happen here, so the degraded tiers, and W4's fixed 500 MiB context estimate
   against a measured 666–668 MiB context, are **untested everywhere so far**.
2. **The container's `nofile` soft limit** (`docker exec … ulimit -n`) and the
   gateway's **peak descriptor count** during a job of at least ~1 000 items.
   containerd defaults the soft limit to 1024 while the daemon has 524 288. The
   branch now raises its own soft limit and clamps the ceiling, so what a new
   platform really checks is whether its **hard** limit is also small (podman, a
   hardened image, a systemd unit with `LimitNOFILE=`, macOS).
3. **`UV_LINK_MODE=copy` for the image build** on any host whose Docker storage
   sits on ZFS, or any filesystem where uv's reflink fails with `os error 11`.
   Now shipped; a scratch copy of an *older* Dockerfile still needs it by hand.
4. **The PDEATHSIG/thread hazard is Linux-only, but the fix is not.** macOS and
   Windows have no PDEATHSIG, so that failure class does not exist there, but
   the spawner change applies everywhere, and any pass must watch for **worker
   deaths with no traceback and no kernel OOM**, which is the signature. On
   Linux the diagnostic is `strace -f -e trace=clone,clone3,exit,exit_group,
   kill,tgkill` plus a check of who forked the dead pid.
5. **`LD_LIBRARY_PATH` for CTranslate2 on bare Linux.** `whisper/tiny` SIGABRTs
   on load unless the venv's `nvidia/cudnn/lib` is on the loader path. torch
   finds its own copy through RPATH, so only CTranslate2 needs it, and the repo
   sets it on the ROCm path only. Record which platforms need it.

Two cheap facts to record everywhere: the **CUDA context size** and the
**`nvidia-smi` vs torch total** disagreement.

---

## 9. Release-note text

### Batch sizes are now automatic (G5 migration)

> **Batch sizes are now automatic.** On first start after upgrading, Panoptikon
> clears every batch size you had saved, the per-model default on the Scan
> page and the `batch_size` on each scheduled job, and sizes batches itself
> from what your GPU can actually hold. **The old numbers are not kept
> anywhere: they are removed from `config.toml` and there is no backup.** If
> you want a record, copy `data/index/<your index>/config.toml` before you
> upgrade. Nothing else in that file is touched. If you still want a ceiling
> for a model, set one after the upgrade and it will be respected as a maximum;
> the migration runs once per index database and will not clear it again.

Verified in S10 on both the bare-binary and the Docker path: exactly one INFO
line across four boots, only those keys removed, hand-written comments
preserved, the stamp row written, a cap set through the API surviving a
restart, and master leaving no stamp table behind.

### File descriptors in containers

> Panoptikon now raises its own open-file limit at start-up and sizes its
> in-flight work to fit the descriptors it actually has. If you run it in a
> container, note that the default soft limit inside a container is often 1024
> even though the host allows far more; Panoptikon raises the soft limit to
> whatever the hard limit permits. If your deployment pins the **hard** limit
> low (a hardened image, a systemd unit with `LimitNOFILE=`, some podman
> setups), Panoptikon will run with a smaller in-flight window rather than
> failing, and will log a warning saying so.

### Calibration profiles are now written and reused

> Panoptikon now records what it learns about each model's memory use in
> `data/inferio/calibration.toml` and reuses it on the next start, so a
> restart no longer re-learns from scratch. The profile is keyed by GPU, torch
> version, platform, backend and the model's data type; a profile learned on
> one machine is not used on a different one. Profiles learned before this
> release do not exist, earlier versions never wrote the file.

### For API clients

> `POST /api/inference/predict/{group}/{id}` now returns a response header,
> `x-panoptikon-desired-in-flight-items`, carrying the number of items the
> server would like to have in flight for that model right now. It is
> advisory: clients that ignore it behave exactly as before, and the header is
> absent when the server has no opinion (nothing dispatched yet, or an unpriced
> model). It is a response header rather than a body field because the predict
> endpoint answers in three encodings and only the JSON envelope has room for a
> scalar, a body field would be missing for exactly the image and embedding
> models the signal is about.

---

## 10. Host restore checklist

**The soak is finished and the run is over.** Nothing of this run is running:
no gateway, recorder, hog, loadgen, driver or calibration container, on the
host or in Docker. The host is clean apart from the items below — the SGLang
deployment the run stopped, four container images, the master worktree and its
venv, and the recordings. Nothing of the user's was edited.

### 1. Restart SGLang (the one mandatory step) — **already done**

```
docker compose -f /home/admin/docker/dsv4flash/docker-compose.yml up -d
```

It was stopped with the matching `down` at the start of Phase 2a: container
`dsv4-flash-sglang` (image `dsv4flash-sglang:dev-fi3989`) and the network
`dsv4flash_default`. It had been holding **95 782 MiB on each board** and about
19 GB of host RAM. The second compose project, `metrics`
(`sglang-grafana`, `sglang-prometheus`, from `/home/admin/docker/metrics/`),
holds no GPU memory and was deliberately left running throughout.

**Status at 04:03 UTC on 2026-09-04**: `dsv4-flash-sglang` is up again and both
boards read **77 702 MiB used**, so the deployment is back. Both boards read
2 MiB with no compute apps at the end of the soak (03:47) and at the end of
every phase before it, which is what every phase report confirms; the run
therefore returned the GPUs empty before SGLang reclaimed them. If the 77 702
figure against the earlier 95 782 matters, it is a property of that
deployment's own configuration: nothing under `~/docker` was read or edited by
this run.

### 2. Container images left behind

| Image | Size | Keep or delete |
|---|---|---|
| `panoptikon:calib-cuda` | 9.43 GB | Delete after the run. Phase 7b rebuilt it at `dc613400` for the F6 closure leg, which has passed |
| `panoptikon:calib-cpu` | 3.49 GB | Delete after the run |
| `panoptikon:calib-master-cuda` | 9.43 GB | Delete after the run, a baseline, not reproducible from the repo (master's Dockerfile needed a hand-added `UV_LINK_MODE=copy`) |
| `panoptikon:calib-master-cpu` | 3.49 GB | Delete after the run |

`docker rmi panoptikon:calib-cuda panoptikon:calib-cpu
panoptikon:calib-master-cuda panoptikon:calib-master-cpu` reclaims ~26 GB. No
calibration container and no `panoptikon-config` volume was left running or
mounted (phase6-report.md), and Phase 8 ran entirely on the bare host, so all
four images are free to delete now: the only containers on the host are the
user's own `dsv4-flash-sglang`, `sglang-grafana` and `sglang-prometheus`.

### 3. Worktrees

| Path | Commit | Disposition |
|---|---|---|
| `/home/admin/projects/panoptikon` | branch tip | The working checkout |
| `/home/admin/projects/panoptikon-master` | `7aa92b20` | The C0 baseline. Keep until the platform passes are done, or `git worktree remove` it plus its own venv |
| `…/scratchpad/p7b/wt-dc613400`, `wt-m1`, `wt-m2` | `dc613400`, branches `s15-m1`, `s15-m2` | Phase 7b's throwaway worktrees and mutation branches. **Already removed**: `git worktree list` and `git branch --list 's15*'` were both verified empty at the end of that phase, and `git worktree list` at the end of the run lists only the two rows above |

### 4. Virtual environments

`python/.venv` on the branch was synced with the `test` group and the `cu128`
extra. The master worktree has its own venv. **Never start a gateway on the
shipped default config**: the venv setup sentinel goes stale whenever
`uv.lock` moves, and a start with the shipped config re-syncs and drops the
`test` group. The protocol's configs are immune because they set `python`
explicitly.

### 5. Recordings

`tools/calibration-protocol/results/` is **5.4 GB** after the soak, and
git-ignored. Largest items: `corpus/ramp8` 508 MB, `corpus/soak` 429 MB,
`run1/S9` 348 MB (the soak's recordings), `corpus/ramp` 63 MB, `corpus/pixmix`
41 MB, `corpus/audio` 25 MB; the rest is the other scenario directories, 71
entries under `run1/`.

- **Keep** `results/run1/` until the next platform pass: it is the only record
  behind every number in this report, and its `README.md` names the poisoned
  calibration stores.
- **Safe to delete**: `results/corpus/` (regenerable from `corpus.py` with the
  recorded seeds, `ramp8` used `--tier ramp --scale 8 --seed 20260904`, `text`
  used seed 20260903).
- **Do not seed a later run from a poisoned store.** The safe seeds are
  `S2-wdvit/calibration.after.toml` (anchor 512, slope 50.5625, no knee),
  `S2-mobileclip/` and `S2-minilm/`. The poisoned ones are listed in
  `results/run1/README.md`: S4d (knee 1), S4e (63), S2-wdvit-loadgen (7),
  S2-mobileclip-A1 (31), S3-wdvit-B3 (the resurrected anchor-32 profile),
  S3-wdvit-B4/B4b/B4c (hand-edited anchors), S6-contend (31/15/4095), S7-C7
  (31/7), S11-C4/hog (1), and **S9** (wd-vit knee 1, plus knees on all four
  profiles fitted under 8 h of hog pressure with a concurrent loadgen — a soak
  result, never a seed). `S2-wdvit-6a5e6799` is byte-identical to
  `S2-wdvit-fixed` and is a safe seed. `S2-wdvit-W5` is not poisoned but carries the
  `expandable_segments` slope under an identical key. `S8-pixmix`'s knee is
  correct but corpus-specific.

### 6. Nothing else to undo

Fixtures were installed into git-ignored destinations (`inferio_custom/`,
`config/inference/`). The S13 `nvidia-smi` shims were never left on any PATH
the user inherits (`command -v nvidia-smi` is `/usr/bin/nvidia-smi`). Nothing
under `~/docker` was edited and `~/docker/inferio/.env` was never read. Every
driver script of every phase, Phase 8's `leg.sh`, `s9/setup.sh`, `run.sh`,
`loadloop.sh`, `hogdrv.py` and `hourly.py` included, lives in the
orchestrator's scratchpad and was never installed anywhere.

---

## Appendix: the commits of this run

In `git log` order, newest first (`65c4fe63..HEAD`). Product commits are
marked **P**; tooling, fixtures and configs **T**; documentation **D**.

| Commit | Kind | Subject |
|---|---|---|
| `4ea6d5f6` | D | Correct S15's expectations from run1 and record holes H1-H4 |
| `3bf4ba76` | T | Turn analyze.py's silent SKIPs into verdicts (S15 holes H1-H4) |
| `8d0fabfe` | T | Let loadgen pace a slot's requests with interval= |
| `1f1a69c2` | T | Strip ANSI from logs and read the container's real fd limit |
| `6a5e6799` | P | Verifier fixes for the spawner, death attribution and probe timeout |
| `dc613400` | D | Say what the spawner thread's stack is actually for |
| `532c15d2` | D | Note the vanished block_in_place in the orchestrator map |
| `c8c26c54` | P | Await the load-path host probe instead of block_in_place |
| `ed0b1f0e` | P | Kill the probe child when the capability timeout expires |
| `960edf61` | D | Record the landed spawner and death-attribution fixes in the protocol docs |
| `23e5a468` | T | Teach analyze.py failed jobs, the restated invariant and peak fds |
| `4d1f0af5` | D | Revise the test protocol and code map from what run1 measured |
| `5becf29c` | P | Tell a dying worker from one the gateway killed |
| `f9cf10fa` | P | Fork every supervised child from one thread that never exits |
| `05c2b868` | T | Add the S13 nvidia-smi shims: slow-all, slow-memory, malformed |
| `13e8850f` | P | Sweep the descriptor clamp and pin the fallback against lowering |
| `25153334` | D | Document the descriptor clamp and the startup nofile raise |
| `d5e42c78` | P | Bound the in-flight window by the process's descriptor budget |
| `b9d32324` | T | Add Phase 6 compose overlays: raised nofile and master-image |
| `c8d64a5a` | P | Report every worker death with pid, signal and killer; sweep idle replicas |
| `eb398d69` | P | Pin the probe guard's disarm and correct its abandoned-task claim |
| `26a43404` | T | Add a fixture that ignores the trim past TRIM_DEADLINE (B17) |
| `8c696ac0` | P | Clear the board's refreshing flag even when the host probe panics |
| `443fa088` | T | Finish the fixture README's unpriced-path correction |
| `612771a3` | T | Correct the torch-free fixtures' single-board fallback claim |
| `0abf3283` | T | Add a B11 probe fixture: non-OOM error text containing 'out of memory' |
| `ff34b059` | P | Verifier fixes for the load-path probe and the squeezed in-flight figure |
| `22eb33f9` | P | Publish the granted budget when a window's grant was squeezed |
| `8546cd63` | P | Probe the board before pricing a load against it |
| `17e38e95` | T | Bound the second-batch OOM fixture to one OOM |
| `1801bce6` | T | Make the poison tier's out-of-memory.png a failing input |
| `8d0eaeb5` | T | Add the C7 config and two Phase 4 fault-injection fixtures |
| `d17e3854` | P | Do not reattribute a departed worker's VRAM to external usage |
| `e67705c2` | T | Stop the oracle hold once the hog reaches its target, not after the budget |
| `0bea8326` | T | Add --bisect-start and --bisect-budget to the ceiling probe |
| `918ec170` | D | Distinguish the absent dtype query from the literal unknown key in the design doc |
| `3cff5c8c` | D | Say why the Docker build pins uv's link mode, and that it outlives the build |
| `58dda519` | P | Infer a model's dtype and log why a calibration update is skipped |
| `a8054b52` | T | Record OOM cause and allocator reserve in the ceiling probe bisect trace |
| `62c9ecb0` | D | Correct the PQL example, note the Docker-only 403 check, add cuDNN to run envs |
| `16e1f2d7` | T | Anchor base_accuracy on the oracle sample after the load, not before |
| `ccc552eb` | T | Add the C4/C5/C6 compose files for the calibration protocol's Docker runs |
| `5b7c7353` | P | Force uv copy link mode in the Docker build so it works on ZFS-backed hosts |
| `1b5b6850` | T | Add the calibration protocol instruments, run configs and fixtures |
| `10be7442` | P | Let core's in-flight unit budget follow the server's desired item count |

Phase 7b landed one commit, `1f1a69c2`, tooling only
(`tools/calibration-protocol/analyze.py` and its `README.md`, staged by
explicit path). It sits above `6a5e6799`, which the verifier landed during
Phase 7b and which is therefore **not** in the `dc613400` binary every Phase 7b
leg ran on. The three S15 mutation branches were throwaway and are deleted;
nothing from them is in this table.

Phase 8 landed three commits, **all tooling or documentation and none of them
in the binary either of its legs ran on** (`1f1a69c2`, code `6a5e6799`, built
at 19:27 before any of them): `8d0fabfe` gave `loadgen.py` its `interval=`
rate control, and `3bf4ba76` plus `4ea6d5f6` closed the five S15 holes in
`analyze.py` and in the plan. Each was staged by explicit path; nothing under
`panoptikon/src/`, `python/`, `config/` or `docs/inferio-worker-protocol.md`
was touched after `6a5e6799`. **The product code of this run is therefore
frozen at `6a5e6799`**, and both Phase 8 legs measured exactly that.
