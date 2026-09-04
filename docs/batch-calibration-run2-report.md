# Batch calibration: run2 report (Linux/CUDA, 2026-09-04)

Subject: PR #27, branch `claude/batch-calibration-coverage-db9ab9` (the Rust
port). Change set: `docs/batch-calibration-test-protocol.md` §0, the table
*"Decisions taken by the user after run1 (2026-09-04): the run2 change set"*
(rows R1–R12, R10') and the *"Run2 handoff"* section after it. Predecessor:
`docs/batch-calibration-run1-report.md`. Recordings:
`tools/calibration-protocol/results/run2/` (git-ignored). Host: two RTX PRO
6000 Blackwell (97 887 MiB each, compute capability 12.0), driver 590.48.01,
62 GB RAM, 48 cores, Arch Linux kernel 6.12.73-1-lts, Docker 29 — the same
host run1 ran on.

Every number below comes from a track, verifier, fix or phase report in the
orchestrator's scratchpad, or from a runlog under `results/run2/`. Non-obvious
numbers carry their source in a trailing parenthesis.

**This report is written while the run is still going.** Sections whose legs
have not run carry a `<!-- PENDING: … -->` marker and state the expectation the
legs plan sets for them, so they can be filled in without re-deriving anything.

Acronyms expanded once, beyond run1's: h2c = HTTP/2 cleartext (prior
knowledge); MAD = median absolute deviation; RFC = the IETF standard of that
number; VLM = vision-language model.

---

## 1. Verdict and state

**The run2 change set is implemented, verified and integrated: 96 commits,
`0d6b36c5` → the tip, four tracks each reviewed by a separate verifier, a
cross-track follow-up, an integration pass with the full suites green, a
release binary at `65fd2f82` and a `panoptikon:calib-cuda` image rebuilt from a
clean worktree. Two of the five leg phases have run. Phase A found one release
blocker (P1, fixed), one intermittent transport fault (P2, fix in progress),
one finding against R1's own knee estimator (F1, fixed) and one surprise that
turned out to be a second, unnamed concurrency ceiling (S1, root-caused, fix in
progress). Phase C met every stated expectation and closed five run1 findings
outright. Phases B, D and E, and the re-run of S2/S3 wd-vit, are pending on a
binary that contains the four post-Phase-A fixes.**

### What the legs have closed

| Run1 finding | Run1 number | Run2 number | Leg |
|---|---|---|---|
| **Q1 / B11** OOM classifier deflates on prose | **15** negatives `reason="oom"` on a board with 96 356 MiB free | **0** negatives; 25 of 25 fallbacks `oom=false`; `deflation=0` throughout | `S5-failbatch-oomtext` |
| **Q2 / B8** deflation uncapped | **6 589** levels in 120 s (54.9/s); a 2-min fault costs 15.6 min at 0.43× | capped at **4** = `ceil(log2 8)+1`; repaid 4 → 0 in **0.5 s** on clean windows and 4 → 0 over **120 s** idle, one level per 30.0 s | `S5-oomtimed`, `S5-oomtimed-idle` |
| **Q5 / B15** no load backoff | **93** load attempts in 182 s, one per request, HTTP 500 | **7** attempts, ladder 2/4/8/16/32/64/128 s, **13 145 × 503** with `retry-after` and `detail.kind = "load_cooldown"`, `/health.load_cooldowns[]` | `S5-dieonload` |
| **P5-3 / B18** global load lock | p50 **11 886 ms**, max 11 894 ms = 28.3× p50 = 100.2 % of the load | p50 **551 ms**, max **951 ms** during a 13.388 s load = **1.86× p50**, 7.1 % of the load | `S6-b18-loadstall` |
| **P5-5** collapse fires on contention | **3** MiniLM `throughput_collapse` negatives | **0** of 2 610 settles | `S6-contend` |
| **P5-4 / F-A** the knee pins a contended model | knees **31 / 15 / 4 095** fitted 20 s in, held 10 min | no knee on wd-vit or MobileCLIP; MiniLM's 16 383 **withdrawn 18 s later**; the store has **zero** `knee_units` | `S6-contend` |
| **F7 / Q8 / T8** death blast radius, empty failures endpoint | one death failed 1 542 items; `/api/jobs/data/failures` `{"total":0}` in every leg; `end_time == start_time` | **2 000** re-queues, `job_failures_total: 2000` with real `occurred_at`, job `failed` with `failed_items: 2000`, `13:59:50 → 14:02:07` | `S5-dying-job` |
| **F-C** nemotron base 4.5× | 3 788 vs "848 MiB" | retracted in run1's report; **3 788 vs 3 788 = 0.0 %** on a judged 373-sample plateau | `S2-base`, R12 |
| **base_accuracy** never judged | 42 of 60 legs INFO | **judged and PASS**: 0.0 / 0.0 / **4.47** / 0.0 % (wd-vit / MobileCLIP / MiniLM / nemotron) | `S2-base` |
| **N4 post-unload phantom** (re-confirmed) | S2 `oracle_agreement` FAIL, 81 of 205 | **PASS, 137 MiB worst, 0 of 410** | `S2-wdvit` |

### What run2 broke, and where each stands

| Id | Sev | What it does | Status |
|---|---|---|---|
| **P1** | **BLOCKER** | R10' made the gateway an h2c client of its own API; `policy::resolve_effective_host` read only the `Host` header, which HTTP/2 does not send, so every self-call was refused **403 `no_policy`** and `POST /api/jobs/data/extraction` answered **500**. No shipped-shape configuration works around it (a policy with neither `hosts` nor `endpoints` is rejected at load; `hosts = ["*"]` does not match a hostless request) | **Fixed** in *Resolve the policy host from the request authority, not just Host* (`74ca202c`) + Desktop parity (`4c2e00b6`). **Verified**: APPROVED, policy 42 tests, desktop 11 (`run2-p1-verify.md`). Not yet exercised by a leg — every Phase A/C job leg used the protocol's own `calib_hostless` workaround |
| **P2** | HIGH | One predict in ~8 000 items failed **400 `invalid multipart body`** from the gateway to itself on the h2c path; the item is lost (`requeued: false`), the job is reported `partial`. Did not recur across the other three Phase A job legs or ~8 400 Phase C job items | **Fix in progress** (`run2-p2-fix-report.md`, agent running). Recorded, not yet closed |
| **F1** | HIGH | R1's knee estimator still fitted a spurious small knee on a flat curve: wd-vit `knee_units = 3` at 14 observations, persisted as 7, and the stored knee then held a **fresh** 2 000-item job at 7–31 units for its whole length (`utilization` 0.01 against run1's 0.80). The expiry lost the race — widen at 12 clean windows, refit within ~1 s | **Fixed** in *Fit a knee only where the curve bends and stays flat above it* (`9cbd6304`, docs `d230d5ba`, code map `19e2bf9f`, prose `ce03bd0d`). Replay: **no knee at any point** on either wd-vit leg. Verifier still running |
| **S1** | HIGH | The ramp anchor froze at **136** and never moved. Not the knee: `max_units_measured` froze at t ≈ +17 s, the knee was not fitted until t ≈ +34.5 s. The cause is `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200`, which `axum::serve` never overrides, over a `hyper-util` pool that shares **one** h2 connection per host — so `INFERENCE_POOL_CONNECTIONS = 4` is really 1 and the job could never put more than 200 predicts on the wire, while the orchestrator was asking for **1 632** | **Root-caused** (`run2-s1-analysis.md`). **Fix in progress**; scope in §6 |
| **S1b** | MED | A `UnitBudget` **shrink can never land in a saturated job**: `drain_shrink` calls `Semaphore::forget_permits`, which can only take *available* permits, and a released permit goes straight to one of ~1 800 waiting item tasks. Observed in-flight stayed at 200 for the whole post-knee phase instead of falling to 64 | **Fix in progress**, folded into the S1 fix |

### What is measurably better than run1 so far

- **Deflation** is bounded and repays on a clock: 4 levels, not 6 589.
- **A dying model costs 7 loads in 182 s, not 93**, and a client is told when
  to come back (`503` + `Retry-After` + `detail.kind`).
- **A neighbour's load costs 1.86× the p50, not 28×.**
- **A worker death no longer loses a window's items silently**: 2 000 of 2 000
  re-queued once, then listed individually by `/api/jobs/data/failures`.
- **`base_mb` is judged for the first time** and is exact on three of four
  models.
- **`base_accuracy`, `oracle_agreement`, `grant_safety` and `failures` all
  PASS on the cold ramps**: 88 grants on wd-vit, 0 over headroom, 0 over live
  free, 0 memory-blind.
- **The reserve cap does what R5 promised** on a squeezed board: `unit_budget
  = 60`, `mb = 3 040`, `reserve_mb = 1 024`, `reserve_rule = "capped_default"`
  where run1 issued `unit_budget = 1`, `mb = 0` (memory-blind).

### What remains open and could still block a release

| Open item | Why it can still block | Worst measured number |
|---|---|---|
| **S1** the 200-stream ceiling | A ceiling no layer can name caps job-driven calibration at ~136 units on **every** model whose appetite exceeds it, independent of board size, model and knee. On a 97 GB board that is roughly one sixteenth of what the policy computed | in-flight pinned at **200** across 70 windows on two independent legs; policy asked for 1 632; anchor frozen at **136** |
| **P2** the intermittent 400 | A silent one-item data loss on the new transport, on the path every local-inference job takes. A soak that reports `partial` jobs may be counting this, not a fixture | 1 item in ~8 000; 1 job leg in 4 |
| **C6** throughput under a hard squeeze | R5's win in *admission* is a loss in *throughput* in the same regime, and nothing in the ledger notices | wd-vit **0.56×** run1 at 4 GB free (21.85 vs 38.98 items/s), p50 5 323 vs 3 199 ms |
| **F1's residue** — MobileCLIP's knee is not restored | Under `KNEE_PLATEAU_BUCKETS = 2` the recorded MobileCLIP ring fits **no** knee where run2 fitted the correct 127. The knee is found late rather than lost, and it was worth nothing measurable (0.94× with it, 1.00× without in run1) — but the stated acceptance criterion is not met literally | one quiet bucket above the bend where the rule wants two |
| **P1 is unexercised by a leg** | The fix is verified by tests, not by a job on the shipped config | 0 legs so far; the C1 workaround is still in place |
| **`unstated` has no producer** | R11's sentinel rename cannot be verified on this host: all four shipped models and every fixture resolve a dtype by inference | the string appears **0** times in five Phase A legs and ten Phase C legs |

### Recommendation

1. **Land the S1 fix and rebuild before Phases B, D and E.** Every remaining
   leg's headline number (S4a's "≈200 items at 12 GB free", the pixmix slope,
   the soak's throughput) is measured through the transport S1 names, and a
   ramp that stops at 136 for a reason outside the ledger makes all of them
   unreadable.
2. **Close P2 before the soak.** It is the one open item that loses a user's
   data silently, and the soak is where it would be counted as something else.
3. **Re-run S2 wd-vit and S3 on the rebuilt binary.** They are the only legs
   whose verdicts were decided by F1 and S1 together.
4. **Decide C6.** It is the first measured cost of the R5 decision the user
   approved, and it is a throughput regression in exactly the regime R5 was
   aimed at. Options in §5.
5. **Decide `KNEE_PLATEAU_BUCKETS`.** 2 refuses wd-vit's spurious knee *and*
   MobileCLIP's real one; 1 restores MobileCLIP and still refuses wd-vit (by
   rules 1 and 2). One named constant, one line.
6. **Push the `ui` submodule commit** (`9b28044` on `batch-calibration-ui`) to
   `reasv/panoptikon-ui`. Until then the gitlink resolves only from this
   host's clone and a clean-worktree image build needs the local remote.

---

## 2. What was run

### Phases, binaries and legs

| Phase | Content | Binary the legs ran on | Legs |
|---|---|---|---|
| Implementation | Four parallel tracks L, M, E, P; one verifier each; R12 analysis and its tool fix; cross-track follow-up; integration pass | n/a (debug `cargo test` only) | n/a |
| A Idle board, cold ramps | S2-base (new), S2 ×3, S3 (+ the `S3-wdvit-kcw` sub-leg) | **`65fd2f82`** (`panoptikon 0.1.8`, 86 397 864 B, built 12:41 UTC) | `S2-base`, `S2-wdvit`, `S2-wdvit-blocked-h2c403`, `S2-mobileclip`, `S2-minilm`, `S3-wdvit`, `S3-wdvit-kcw` |
| C Negatives, deaths, contention | S5 ×5 (+3 sub-legs), S6 contend, S6-b18 | **`65fd2f82`**, not rebuilt | `S5-failbatch-oomtext(-job)`, `S5-oomtimed(-idle)`, `S5-oom2nd`, `S5-dieonload(-job)`, `S5-dying-job`, `S6-contend`, `S6-b18-loadstall` |
| D1 S8 ocr-C7 | easyOCR host-side pricing at the 2560² canvas | `65fd2f82` | in progress |
| B External pressure | S4a, S4d, S4b (+ S4g if time) | **pending the rebuilt binary** | `<!-- PENDING: phase B -->` |
| D Packing, docker | S8 pixmix, S11-C4 | **pending the rebuilt binary** | `<!-- PENDING: phase D -->` |
| A′ Re-run | S2 wd-vit, S3 wd-vit on the F1/S1/P1/P2-fixed binary | **pending the rebuilt binary** | `<!-- PENDING: phase A re-run -->` |
| E Soak | 4 h S9 recipe | **pending the rebuilt binary** | `<!-- PENDING: phase E -->` |

Phase A and Phase C both ran on `65fd2f82`, which contains the whole R1–R12
change set and **none** of the four post-Phase-A fixes. Phase C's report
re-read every source line it quotes from `git show 65fd2f82:…`, because three
fix agents were editing the tree while its legs ran.

### Configurations

Run1's C0–C7 unchanged (`docs/batch-calibration-run1-report.md` §2). Two
run2 deviations, both in the protocol's own configs and both committed by
explicit path:

| Config | Deviation | Why |
|---|---|---|
| `server-C1.toml` (`a39ab8d8`) | a `calib_hostless` policy — `ruleset = "allow_all"`, `[policies.match] endpoints = ["default"]` | the **P1** workaround; without it no job leg can run at all. Gates routing and DB defaults only; changes nothing the ledger measures. **Remove it in the first leg that runs on the P1-fixed binary and record the removal** |
| `server-C7.toml` (`2779a81e`), `registry-C7.toml` (`ea6f94f9`) | the same endpoint-scoped policy; easyOCR's canvas and `epoch = 2` restated in the C7 override registry | the C7 registry override drops `metadata.cost` keys it does not restate |

C0/C2/C3 and the C4/C5/C6 compose overlays are **untouched**, so the shipped
shape is still available to a leg that needs it — which is what makes P1
testable once the rebuilt binary lands.

### Wall time (UTC, 2026-09-04)

| Marker | Time |
|---|---|
| Four tracks launched in parallel | ~05:25 |
| Track P done | ~06:28 |
| Track M done, R12 analysis done | ~06:41 / 05:55 |
| Track E done, Track L done | ~07:30 / ~07:35 |
| Verifiers done (P, M, then E, L after a rate-limit restart) | 07:06 / 07:21 / 11:16 / 11:19 |
| Cross-track follow-up done | ~12:12 |
| Integration pass done; release binary `65fd2f82` built | 12:41–12:48 |
| SGLang stopped for the legs | ~12:50 |
| Phase A done | ~13:27 |
| P1 fixed / verified | 13:43 / 14:22 |
| Phase C done | ~14:30 |
| F1 fixed | ~14:52 |
| S1 root-caused | ~16:01 |

Two session rate limits interrupted the run (10:40 and 15:40 UTC resets). No
commits were lost either time; the E and L verifiers were relaunched with
resume briefs, and P2, the F1 verifier, the S1 analysis and the ocr leg were
relaunched after the second.

---

## 3. The change set, per R row

"Verifier" is the separate agent that reviewed the implementer's commits; its
verdict is quoted from the plan's §0 status block and the verifier report
named there.

| R | What was built | Commits | Verifier | What the legs measured |
|---|---|---|---|---|
| **R1a** exclusions | Windows the ledger knows were **squeezed**, **memory-blind** (`mb == 0`) or **worker-clamped** (per measurement) no longer feed the throughput ring; they still feed the cost fit and the ratchet, because a clean high-water batch's allocator envelope is honest whatever decided its size | `e6abd09e` | one defect: a suppressed collapse verdict also suppressed the OOM riding on the same batch — fixed `2314e609` | no spurious knee from a squeezed window in any Phase A/C leg |
| **R1** contention tag | `GrantCharge.peak_occupants`, a high-water count of *other* replicas holding a window while this one was in flight; the knee fits **only** `occupants == 0` samples, and a collapse verdict is trusted only from a sole-occupancy window (P5-5). The approximation is one-sided: honest samples may be tagged contended, never the reverse | `8f71379a` | approved | **S6-contend: 0 `throughput_collapse` negatives** of 2 610 settles (run1: 3); wd-vit and MobileCLIP fit no knee under contention |
| **R1** variance filter (the user's addition) | Per-bucket **relative MAD**; `MIN_KNEE_BUCKET_SAMPLES = 2` (a singleton's dispersion is 0 by construction, so it is dropped from the fit and from the counts); `KNEE_MAX_BUCKET_DISPERSION = 0.20`, and **any** retained bucket over it refuses the whole fit, `knee_best` included | `9e8d6810` | approved | **fires, with the reason stated**: MiniLM, 59 × `declining to fit a throughput knee … bucket=13 observations=2 dispersion=0.2128157093511856 threshold=0.2` |
| **R1d** expiry (brake, not ceiling) | `knee_clean_windows`, `knee_re_explore_above`; a window counts only if it responded clean, the **knee was binding**, it **carried enough work to reach the cap** and the board had **ample headroom** (`headroom ≥ RATCHET_FACTOR × appetite`). `KNEE_EXPIRY_CLEAN_WINDOWS = MIN_KNEE_SAMPLES = 12` — twelve honest observations buy a cap, so twelve clean windows at it buy a re-test. Withdrawal when the widened knee reaches `uncapped_units` | `f161850f`, withdrawal arm `29f5636b` | approved; the verifier judged the `anchor == 0` withdrawal form **better than either option the brief named** | **works**: 10 widenings across the wd-vit legs, every one at exactly `clean_windows_at_the_knee=12`, every one one log2 bucket. **`knee_withdrawn` first exercised in Phase C** (`S6-contend`, MiniLM, withdrawn 18 s after it was fitted, never came back) |
| **R1e** (new, from F1) | One candidate — the smallest quiet bucket on the plateau — and **five vetoes**: frontier must be quiet, floor must be interior, `KNEE_PLATEAU_BUCKETS = 2` quiet buckets above, no ramp-era knee below the anchor, post-widening evidence judged by a per-(model, board) `seq`. Plus: a replica's first settled window teaches the knee nothing, and a **seeded** knee is provisional for `KNEE_SEED_REVALIDATION_WINDOWS` | `9cbd6304`, `d230d5ba`, `19e2bf9f`, `ce03bd0d` | **running** | replay against the recordings: **no knee at any point** on `S2-wdvit` (218 obs) and `S3-wdvit` (205 obs); MiniLM still refused by variance; **MobileCLIP's 127 not restored** on its ring (§5) |
| **R2a** re-queue + `partial` | A died-on window's in-flight items are re-queued **once**; `data_log.outcome ∈ {'', completed, partial, failed, cancelled}` (`''` renders `running`, so no backfill); every terminal path writes it, including the early-return and cancellation paths (the T8 fix) | `521cca81` | nine defects across two rounds, incl. the death signal matching one rendering of six and the scan-history column still reading "Completed" for a `partial` job — `a3120f12`, `8fef026e`, `168aa9bd`, `207aa3c0`, `c6a7a9ef`, `6dcd82e4`, `6a8fb930`, `299c3c65`, `1e6b80ea`, `793f40a7` | **`S5-dying-job`: 2 000 re-queue lines + `requeued=2000`**; outcome `failed` (Systemic — *every* item died, so `partial` is unreachable there), `end_time != start_time`, 63 spawns / 63 deaths for 2 000 items. **`partial` evidenced in Phase A** (`S2-wdvit`, 1 owed item from P2) |
| **R2b** failures endpoint | `GET /api/jobs/data/failures` keeps `total`/`failures` and gains `job_failures_total`/`job_failures[]` (`JobItemFailure`) and `failed_jobs_total`/`failed_jobs[]` (`FailedJobRecord`); `error_class`/`mime_prefix` answer with the job-side lists empty rather than an unfiltered approximation | `22256f8c`, `fed96ea4`, `618fc76c`, `232f523d`, `ec98f181` | as above | **`job_failures_total: 2000`**, each row with path, sha256, mime, setter, `stage: "inference"`, the `[worker_died]` error, `requeued: true` and a real `occurred_at` (run1: `{"total":0}` in every leg) |
| **R3** structural OOM | Worker: `packing.classify_oom` — three tiers in **strength order over the exception chain**: `typed_exception` (`torch.OutOfMemoryError` via `sys.modules`, `MemoryError`), `marker` (`InferenceOOMError`, `INFERENCE_OOM*`), `message_pattern` (a **closed** list of nine driver-shaped strings plus one two-part CPU-allocator pattern; a bare `out of memory` is deliberately absent). Every classification stamps `free_mb_at_failure` and `device`. Host: trust typed/marker, **veto `message_pattern` when the worker's own live free reading at the failure was at least the whole envelope the window was priced at** | `fffbc947` (worker), `6afed305`, `1031504c`, `5ecba825` (host + device wordings) | six defects in P (the closed list had lost real device wordings); the host half was added in L's verifier pass | **B11 closed**: 0 negatives from prose. **The veto fires** (`S5-oom2nd`, `contradicted=1 free_mb_at_failure=96518 grant_mb=96356`) — see finding **C3** |
| **R4** deflation | `deflation_cap(anchor, seed) = ceil(log2(max(anchor, seed, 1))) + 1`; time repayment `DEFLATION_REPAY_SECS = TRIM_DEBOUNCE = 30 s`, one level per whole interval, advancing the stamp by the intervals consumed; repayment runs where the counter is *read*, not on a timer; clear-on-respawn holds by construction and is now pinned by a test | `83acdbdc` | the ladder's 33rd-consecutive-failure shift overflow fixed by M's verifier (`d8721f90`) | **cap 4, time repayment 4 → 0 over 120 s idle, one DEBUG line per 30.0 s** |
| **R5** margin | `reserve = min(ceil(external × margin), 1024)` when the margin is unset, `ceil(external × margin)` when the user set one; `limit = min(total × cap_fraction, total − external − reserve)`. `margin` became `Option<f64>` so absence tracks the code default. `/health` gains `reserve_mb`, `reserve_rule` (`user_margin` \| `capped_default`); the grant line carries both | `9a10bfe5`, config examples `391b66cb`, `6e515174` | approved; **the widening stays capped** (reasoned, §6) | `/health`: `"reserve_mb": 67, "reserve_rule": "capped_default"`. **On a 4 GB-free board: `unit_budget=60 mb=3040 reserve_mb=1024`** where run1 gave `unit_budget=1 mb=0`; **3** memory-blind grants of 2 610 against run1's 113; **0** samples at `limit_mb = 0` against run1's 367. Cost: finding **C6** |
| **R5** per-batch free (T3) | Every measurement carries `free_mb`/`free_source` (the reading the worker's defensive clamp already takes before that batch) and the host ingests them in sequence order **before** the negative/collapse checks; the response-level sample is applied after, and `record_telemetry` was reordered so capture instants follow the order the worker took the readings | `786fd3b1` (worker), `9a10bfe5` (host) | approved; P's verifier fixed a stale baseline and a load that left the probe polling | `<!-- PENDING: phase B --> S4b will decide it: expect `external_mb` to track a hog step within a few seconds, not run1's 31.5 s` |
| **R6** load locks | Per-model load lock + a **per-board** admission gate; the global lock retired. Four-lock order (barrier > per-model > per-board admission > state) with a no-deadlock proof in the module docs. `max_concurrent_loads` default 1; every unpinned model resolves to the same default board, so the shipped configuration behaves exactly as before | `e6e510d0`, `3ad22c92` | two holes closed: the `""` unresolvable-pin bucket now waits on **every** board's permit (`702ea8ac`); the ladder no longer panics (`d8721f90`) | **B18 closed**: 1.86× p50, 7.1 % of a 13.388 s load (run1: 28.3×, 100.2 %) |
| **R9** load cooldown | `window(n) = min(base × 2^(n−1), max)`, shipped 2 s → 300 s; only `costed_worker` failures arm it (spawn/handshake/configure/load/death-while-streaming), never registry-phase rejections; `Retry-After` never below 1; `/health.load_cooldowns[]` top-level (a cooling model is by construction not in `models[]`); refusal checked **before** the `failed to load model` and `worker_died` arms | `e2679f62` | approved; scope line ("a death *after* a successful load is not counted") judged correct | **7 attempts / ladder 2,4,8,16,32,64,128 / 13 145 × 503 / job aborts in 9.5 s after 2 attempts** (run1: 93 attempts, 500s, 259 s). Residue: finding **C4** |
| **R7** pixel canvas | `metadata.cost.canvas_pixels` on the registry, resolved into `CostDimension` under the same two rules as `seed_units` (pixel-only, never inherited across a unit change); the worker prices `min(raw_pixels, canvas)` including its unreadable-input fallback; three resolution tiers (grant > impl introspection, floored at 512² > uncapped) | `84663c27` (+ follow-up `ee014235`) | approved; the follow-up was needed because **R7 was inert without it** | **`canvas_pixels=1835008` in force on nemotron** with `epoch=2 seed_units=2000000` (`S6-b18-loadstall`). `<!-- PENDING: phase D --> S8 pixmix decides the slope` |
| **R7** wiring (follow-up) | One resolution site (`manager::canvas_in_force`, **registry wins**, load report fills in only where the registry declares nothing), `Grant.canvas_pixels` on the wire as `u32 \| nil`, `dispatch::estimate_input_units` capping the **host's** window pricing at the same figure — which for the three grantless `easyocr_*` ids is the only cap there is — and a new `canvas_pixels` key on the `load ok` response | `ee014235`, probe `893e8a7d`, `9be09e5e`, `18cc2e77` | integration review found two defects, both fixed (`18cc2e77`, `56aec556`) | as above |
| **R8** measured context | `_ContextProbe` measures the board free-memory delta across this process's **first CUDA initialisation** and subtracts the allocator pool at that instant. A **watcher, not a call**: a daemon thread polls `torch.cuda.is_initialized()` every 5 ms rather than initialising CUDA in a process that was never going to touch a board | `daacf07d` | three defects fixed (`f37afe62`, `b34c9ec5`, `344178eb`, `5dedc5e4`) | **not reached on this host**: `base_method = "nvml"` everywhere, exactly as run1's W4 predicted. Exercised by unit tests only |
| **R10′** h2c pool | `Transport::{H2c, Http11}`; **one `EndpointRuntime` per base URL shared by every client** (a pool that is not shared is not a bound); `INFERENCE_POOL_CONNECTIONS = 4`, `H2_STREAMS_PER_CONNECTION = 64`, gate `INFERENCE_MAX_CONCURRENT_REQUESTS = 256`, h2c arm only; probe by `GET /cache` with prior knowledge where **any** HTTP status proves h2c; re-probe on connect/request errors. Descriptor clamp reworked: `Multiplexed` costs a constant 8 descriptors, `PerRequest` unchanged | `ddd44e4a`, `b2723f16` | a mid-stream reset permanently downgrading an endpoint to HTTP/1.1 fixed (`8d352b33`); `c6a7a9ef` never records an unreachable endpoint as HTTP/1.1; gate constant approved in both transports (`6dcd82e4`) | **the two defects of the run**: **P1** (403 on every self-call) and **S1** (an unnamed 200-stream ceiling). Sockets themselves behaved: `<!-- PENDING: phase D --> S11-C4 measures the descriptor count` |
| **R11** small choices | `dtype_method` stored in the profile; sentinel `unknown` → **`unstated`** (constants renamed too, no back-compat alias, so the profile key moves and rows re-measure); no `pid: host`; UI types regenerated | `a1fb91a9`, `649125b9`, `f506dc2b`, `049d271c` | approved; `b0cc2c95` spells the sentinel in the load-report doc comments | **`dtype_method = "inferred"` in all three S2 stores** (`dtype` fp32/fp16/fp32). **`unstated` has no producer on this host** — the string appears 0 times in 15 legs |
| **R12** nemotron base | F-C is an **analysis artefact**: `vramrec.py` had memoised `[panoptikon-spaw]`/empty env for the nemotron pid, so `analyze.py` compared it against the **MiniLM** worker's 848 MiB. Real base 3 788 = 3 201 MiB bf16 weights + allocator slack + ~550 MiB context. Recorder and `analyze.py` fixed; F-C retracted in run1's report; the **S2-base** plateau leg added to the protocol | `03bb8a4d`, `90409b41`, `7b3ea31e`, `0643942c`, `e31fbd4f`, `7a4d38fe` | `6665b931`, `832474b0`, `71cb0827`, `e9e44549` — the identity retry rebounded by wall clock, `base_accuracy` now picks the replica's own pid and closes its window | **0.0 % on nemotron over 373 samples**; `grep -c panoptikon-spaw vramrec.jsonl` = **0** |

**Integration pass** at `65fd2f82`: `cargo test -p panoptikon` **1 586 passed,
2 failed** (both known host artefacts that also fail on master), `pytest tests`
**302 passed, 13 skipped**, `cargo fmt --check` clean, clippy 8 pre-existing
warnings none in a run2 file, `cargo test --bin panoptikon openapi` 5 passed,
and the `ui` gitlink's types byte-identical to a fresh `openapi-typescript`
run.

---

## 4. What the legs measured

### 4.1 Phase A — idle board, cold ramps (`run2-phaseA-report.md`)

Binary `65fd2f82`. SGLang stopped, both boards at 2 MiB before every leg and
after the last. Five legs plus one sub-leg, in the order the brief gave.

#### S2-base (new) — `results/run2/S2-base/`

| Expectation (run2) | Run1 value | Run2 measurement | Verdict |
|---|---|---|---|
| `base_accuracy` judged **PASS** for each model, not INFO | 42 of 60 legs INFO; no judged plateau existed | PASS. wd-vit 964 vs 964 = **0.0 %**; MobileCLIP 732 vs 732 = **0.0 %**; MiniLM 654 vs 626 = **4.47 %**; nemotron 3 788 vs 3 788 = **0.0 %**. `cadence_blind: false`, `first_work_dt_ms: null`, `oracle_window_samples` 535/504/476/373 | **PASS** (after tool fix `cf1939e6`) |
| nemotron ≈ 3 788 MiB on the idle plateau | 3 788 (accidental, `S6-b18-loadstall`) | **3 788, 0.0 %, 373 samples** | **PASS** |
| The spawn line carries `inference_id=` | absent | 8 spawn lines, all with it | **PASS** |
| Every worker pid attributed; no `[panoptikon-spaw]` identities | S9's recorder memoised `[comm]` | `grep -c panoptikon-spaw vramrec.jsonl` = **0**; every row reads `spawn_check: "pid is the one the spawn line names for this inference id"` | **PASS** |
| `/health` carries `reserve_mb`, `reserve_rule`, `load_cooldowns[]` | absent | `"reserve_mb": 67, "reserve_rule": "capped_default", "load_cooldowns": []` | **PASS** |

#### S2 wd-vit — `results/run2/S2-wdvit/`

| Expectation | Run1 | Run2 | Verdict |
|---|---|---|---|
| No OOM, 0 deaths | 0 / 0 | **0 / 0** | **PASS** |
| `grant_safety` | PASS, 10 grants | **PASS, 88 grants**, 0 over headroom, 0 over live free, 0 memory-blind | **PASS** |
| Slope within run1's band | 50.5625 (+0.04 %) | **50.6 (+0.12 %)**, ratio 1.0012 | **PASS** |
| `dtype_method` in the store | field did not exist | **`dtype_method = "inferred"`**, `dtype = "fp32"` | **PASS** |
| `dtype = "unstated"` where an impl states none | n/a | **no producer on this host** | **not exercised** |
| `knee_clean_windows` beside any `knee_units` | n/a | absent here (counter 0 at the last write); present as `knee_clean_windows = 1` in MobileCLIP's and S3's stores | **PASS (conditional)** |
| Knee-expiry INFO within ~12 clean windows, widening one bucket | n/a | **5 widenings**, all at `clean_windows_at_the_knee=12`, each exactly one log2 bucket (3→7 ×4, 7→15) | **PASS** |
| A persisted knee must have passed the variance filter | n/a | the filter never fired (0 `declining to fit` lines); the buckets were quiet and it still produced a knee of 3 on a flat curve | see **F1** |
| **wd-vit's run1 F-A knee (1) must NOT recur** | run1 S2: no knee; S9: knee 1 for 7 h 55 m | **`knee_units = 3` at 14 observations, oscillated 3↔7, persisted as 7**; peak budget 272, last budget 7; `utilization` 0.11 vs run1's 0.40 | **FAIL** |
| Job outcome | 2 000 segments, 0 errors | 2 000 segments, **`outcome: "partial"`, 1 failed item** (defect P2) | **FAIL** |
| Throughput vs master | 0.94× | **1.02×** (32.258 vs 31.746 items/s) | **PASS** |
| `oracle_agreement` | FAIL (81 of 205) | **PASS, 137 MiB worst, 0 of 410** | **PASS** |

#### S2 MobileCLIP — `results/run2/S2-mobileclip/`

| Expectation | Run1 | Run2 | Verdict |
|---|---|---|---|
| No OOM, 0 deaths, `grant_safety` | 0/0, PASS | **0/0, PASS**, 25 grants, 0 unsafe, 0 memory-blind | **PASS** |
| Slope within run1's band | 16.7047 (−0.29 %) | **16.2286 (−3.1 %)**, ratio 0.9686 | **PASS** |
| `dtype_method` | n/a | **`inferred`**, `dtype = "fp16"` | **PASS** |
| `knee_clean_windows` beside `knee_units` | n/a | **`knee_clean_windows = 1`** beside `knee_units = 127` | **PASS** |
| Knee matches the curve (run1 missed a real knee at 128, −8 %) | **no knee fitted** | **`knee_units = 127`**, fitted once at 15 observations, never moved; measured optimum 128 | **PASS, better than run1** |
| Job outcome | completed, 0 errors | **completed, 0 errors** (P2 did not recur) | **PASS** |
| Throughput vs master | 1.00× | **0.94×** (64.516 vs 68.966 items/s) | **PASS** |

#### S2 MiniLM — `results/run2/S2-minilm/` (loadgen-driven; the one leg that never uses the h2c self-call)

| Expectation | Run1 | Run2 | Verdict |
|---|---|---|---|
| No OOM, 0 deaths, `grant_safety` | 0/0, PASS 504 grants | **0/0, PASS, 497 grants**, 0 unsafe, 0 memory-blind; 991/991 requests HTTP 200 | **PASS** |
| Slope within run1's band | 0.037323 (+52.1 %) | **0.032248 (+31.5 %)**, ratio 1.3145 | **PASS** |
| `dtype_method` / `unstated` | n/a | `dtype = "fp32"`, **`dtype_method = "inferred"`**; `unstated` never produced | **PASS / not exercised** |
| No spurious knee; variance filter | never fitted (unexplained) | **never fitted, with a stated reason**: 59 × `declining to fit a throughput knee … bucket=13 observations=2 dispersion=0.2128157093511856 threshold=0.2` | **PASS — the R1 variance filter demonstrably fires** |
| Throughput | 355.8 items/s | **351.6 items/s**, p50 684.9 ms | **PASS** |
| Bucketed batches length-homogeneous in the worker DEBUG log | BLOCKED | **still BLOCKED** (impl unchanged) | **BLOCKED** |

#### S3 wd-vit restart — `results/run2/S3-wdvit/` (+ `S3-wdvit-kcw/`)

Seeded with run2's own S2-wdvit store (anchor 136, slope 50.6, `knee_units = 7`).

| Expectation | Run1 | Run2 | Verdict |
|---|---|---|---|
| The seeded profile resumes; no re-ramp | `seeded_from_store=true`, 3 windows instead of 10 | **`seeded_from_store=true`**, first grant already `ramp_step=5`, second straight to 7 — no 1,2,4,8… sequence; `calibration.status = "local"`, `local_samples` 20 → 32, anchor never decreased | **PASS** |
| `knee_clean_windows` restored | n/a | **PASS, proved in `S3-wdvit-kcw`**: seeded `knee_clean_windows = 11` with `knee_units = 15`, and the first widening arrived **1.52 s after admission** reporting `clean_windows_at_the_knee=12`. The control needed twelve windows, ~10 s | **PASS** |
| A knee withdrawn in-run does not come back | n/a | **not reached** in Phase A — `widened past the point where it could cap anything` appears 0 times; every widening was undone by a refit within ~1–2 s. (Exercised in Phase C) | **not exercised here** |
| No OOM, 0 deaths, job completes | 0/0, 2 000 | **0/0, 2 000/2 000, 0 errors, `completed`** | **PASS** |
| Throughput vs master | 0.89× (FAIL) | **1.09×** (34.483 items/s) | **PASS** |
| (side effect) utilization | 0.80 (peak budget 2 048) | **0.01 (peak budget 31)** — the *stored* knee held a fresh 2 000-item job at 7–31 units for its whole length | **FAIL — F-A across a restart** |

### 4.2 Phase C — negatives, deaths, contention (`run2-phaseC-report.md`)

Binary `65fd2f82`, not rebuilt. Seven briefed legs plus three sub-legs the
expectations forced.

#### S5 failbatch_oomtext — `results/run2/S5-failbatch-oomtext/`

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| OOM negatives from a non-memory error whose text says "out of memory" | **15 of 51 windows** on a board with 96 356 MiB free | **0** | **0.** 26 windows, all `outcome="clean"` | **PASS** |
| error frames classified non-OOM | `oom=true` on 15 of 15 | non-OOM | **`oom=false` on 25 of 25** merged-batch fallbacks | **PASS** |
| deflation | climbed to **3** | none | **`deflation=0`** throughout | **PASS** |
| client cost | 182/182 HTTP 200 | unchanged | 173/173 HTTP 200 | **PASS** |
| "the job reports the items failed (`partial`)" | not run | `partial` | job **`completed`**, 180/180, `errors: 0`, failures endpoint `{"total":0,"job_failures_total":0}` | **NOT APPLICABLE** — `failbatch` fails only multi-input batches and the per-request fallback recovers every item |

#### S5 oom_timed — `results/run2/S5-oomtimed/` (+ `S5-oomtimed-idle/`)

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| deflation cap | **6 589** levels in 120 s (54.9/s), uncapped | `ceil(log2(anchor)) + 1` | **4.** Anchor is the seed (`seed_units 8`) → `ceil(log2 8)+1 = 4` | **PASS** |
| repayment by 3 clean windows | 7.04 levels/s → 15.6 min extrapolated | unchanged | **4 → 2 → 0 in 0.5 s** at the instant the fixture went healthy | **PASS** |
| repayment by time, 1 level / 30 s idle | did not exist | present | **4 lines, one per 30.0 s**, 4 → 0 over 120 s of idleness | **PASS** |
| throughput cost of the fault | **0.43×** for ~15 min | none | no deflated phase exists; healthy phase 2 222 items/s | **PASS** |
| `oom_class.source` on the OOM measurement | field did not exist | present (`marker`) with `free_mb_at_failure` | **not observable** — a *trusted* classification leaves no log line | **INCONCLUSIVE**, finding **C2** |

#### S5 oom_second_batch — `results/run2/S5-oom2nd/`

| Clause | Run1 | Run2 | Verdict |
|---|---|---|---|
| still deflates | 1 negative of 4 462; deflation 1 → 0 after exactly 3 clean windows; budget 8→4→8 | **identical**: 1 negative of 3 915; same recovery; same budget path | **PASS** |
| `oom_class` present, `source="message_pattern"`, `free_mb_at_failure` populated | field did not exist | **present and acted on** — but **vetoed**: `contradicted=1 free_mb_at_failure=96518 grant_mb=96356` | **PASS with a correction** (finding **C3**) |
| client | 1 × 500, 4 461 × 200 | 1 × 500, 3 914 × 200 | **PASS** |

#### S5 dies_on_load — `results/run2/S5-dieonload/` (+ `S5-dieonload-job/`)

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| load attempts in ~182 s | **93**, one per request, every 1.95 s | ≈6 | **7** | **PASS** |
| cooldown ladder | none | 2, 4, 8, … s | **2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0** | **PASS** |
| `/health.load_cooldowns[]` | field did not exist | names the model | one entry: `inference_id`, `failures: 7`, `retry_at`, `retry_after_secs: 78`, `window_secs: 128`, `last_error` | **PASS** |
| predicts answered 503 with `Retry-After` and `detail.kind` | 93 × HTTP 500 `{"detail":"Failed to load model"}` | 503 + header + typed detail | **13 145 × 503** (7 × 500 for the real attempts); `{"detail":{"kind":"load_cooldown",…}}`; header **`retry-after: 4`** | **PASS** |
| job aborts on the first cooldown, model + retry time in the reason | S4g: 4 attempts / 259 s | abort fast, rich reason | aborts in **9.5 s after 2 attempts**; `failure_reason: "Failed to load model: model load failed on all 1 inference endpoints"` — **no model id, no retry time** | **FAIL on the reason text** (finding **C4**) |

#### S5 dying-in-job (new) — `results/run2/S5-dying-job/`

`calibfixture/dying_cuda` inside a **2 000-item** extraction job.

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| the died-on window's items re-queued once | no re-queue existed; each death failed the in-flight request *and* the one behind it (184 requests, all 500) | log "re-queue" | **2 000** re-queue WARNs + one summary `requeued=2000` | **PASS** |
| job outcome | n/a | `partial` | **`failed`** — every item failed, so `JobFailure::Systemic`: `"All 2000 attempted items failed; check the inference server"` | **PASS with the expectation corrected** |
| `job_failures` with a real `occurred_at` | run1's `S5-poison`: `{"total":0}` | present | **`job_failures_total: 2000`**, each with path, sha256, mime, setter, `stage: "inference"`, `[worker_died]`, `requeued: true`, `occurred_at: "2026-09-04T14:02:07"` | **PASS** |
| job record `end_time != start_time`, `failed_items`, `failure_reason` | none existed | present | `13:59:50` → `14:02:07`; `failed_items: 2000`; reason set; `total_remaining: 2000` | **PASS** |
| respawn count bounded ≈ 2 × items/window | **92 deaths / 92 respawns in 120 s**, uncapped | bounded | **63 spawns / 63 deaths / 63 `outcome="worker_died"`** for 2 000 items over 137 s | **PASS** |
| no synthetic negative on a discrete board | 0 | 0 | **0** `unified_board_death`, `deflation=0` | **PASS** |

#### S6 contend — `results/run2/S6-contend/`

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| MiniLM `throughput_collapse` negatives | **3** | **0** (`occupants > 0` excluded) | **0.** 2 610 settles, all clean | **PASS** |
| knee null while contended | knees **31 / 15 / 4 095** fitted 20 s in, pinning all three for 10 min | null | wd-vit and MobileCLIP **never fitted one**; MiniLM fitted 16 383 and **withdrew it 18 s later**; the store has **three profiles and zero `knee_units`** | **PASS** |
| knee fits after the neighbour leaves if the curve is flat | n/a | may fit | none fitted — the safe direction given F1 | **PASS (permissive)** |
| `/health` shows no cooldowns | field did not exist | `[]` | `[]` at every sample | **PASS** |
| shares scale with appetite | FAIL — 31 and 15 for ten minutes | — | wd-vit and MobileCLIP **8 → 128**, MiniLM **4 000 → 32 634** | **PASS** |
| idle-resident trim | flagged 1.837 s after the squeeze, 5.8 ms round trip | unchanged | flagged **2.888 s**, **two** residents in the same millisecond, round trips 6.0 / 9.4 ms | **PASS** (finding C7) |
| worker deaths | **3** | 0 | **0** | **PASS** |
| memory-blind grants under squeeze (B1) | **113**, 111 of them `mb=0 ub=1` | few | **3** of 2 610; phase B grants `unit_budget=60 mb=3040 headroom_mb=3085 reserve_mb=1024 reserve_rule="capped_default"` | **PASS** |
| `limit_mb` clamped to 0 (T6 shape) | 367 of 3 308 samples over the strict invariant, all at `limit_mb = 0` | — | **0 samples at `limit_mb = 0`**; 8 of 3 342 over, all in one 7 s admission burst, worst overshoot 140 MiB | **adjudicated FAIL → accepted** |
| throughput, phase A (all three) | 144 320 items in 600 s | — | **163 008 items in 605 s (1.12×)**; the split shifts to MiniLM (138.7 → 214.7 items/s) at wd-vit's (23.2 → 15.0) and MobileCLIP's (77.8 → 39.7) expense | recorded |
| throughput, phase B (`tags` alone, squeezed to 4 GB) | **38.98 items/s**, p50 3 199 ms, `ub = 1` | — | **21.85 items/s**, p50 5 323 ms, `ub = 60` — **0.56×** | **regression, finding C6** |

#### S6-b18 loadstall — `results/run2/S6-b18-loadstall/`

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| predict latency during a neighbour's load | **p50 11 886 ms, max 11 894 ms** against a 421 ms p50 — **28.3×**, 100.2 % of an 11.865 s load | must not track the load duration | **n=88, ok=88, p50 551 ms, p90 798, p99 951, max 951 ms** during a **13.388 s** load; the 4 in-flight requests finished in **510–518 ms**; requests > 3 s attributable to the load: **0** | **PASS — B18 closed** |
| p99 during the load < 500 ms | — | < 500 ms | **951 ms** — but the *undisturbed* p50 on this leg is **511 ms**, so the literal bar is below the model's own idle latency | **literal bar NOT MET; substance PASS** (1.86× p50 vs 28.3×) |
| worker deaths | 1, 4 of 2 281 requests failed | 0 | **0**; 1 725 requests, 0 failed | **PASS** |
| `base_accuracy` judged | run1 SKIP/INFO | — | **judged PASS**: nemotron 3 788 vs 3 788 = 0.0 %, wd-vit 964 vs 964 = 0.0 % | **PASS** |
| `canvas_pixels` on the load report | field did not exist | present | **`pricing each input at min(raw pixels, 1835008), the canvas the registry states`**, with `epoch=2 seed_units=2000000` | **PASS** |

#### Run2 log fields, checked on every Phase C leg

| Leg | `reserve_mb`/`reserve_rule` on grants | `/health` | `load_cooldowns[]` | `inference_id=` on spawn | `oom_class` | `canvas_pixels` | knee INFO |
|---|---|---|---|---|---|---|---|
| S5-failbatch-oomtext | 26/26 | yes | `[]` | yes | n/a | n/a | n/a |
| S5-failbatch-oomtext-job | 9/9 | yes | `[]` | yes | n/a | n/a | n/a |
| S5-oomtimed | 14 173 | yes | `[]` | yes | **not logged when trusted** (C2) | n/a | n/a |
| S5-oomtimed-idle | 2 940 | yes | `[]` | yes | as above | n/a | n/a |
| S5-oom2nd | 3 915 | yes | `[]` | yes | **yes**, via the veto WARN | n/a | n/a |
| S5-dieonload | 0 grants | yes | **7-entry ladder** | yes (7 spawns) | n/a | n/a | n/a |
| S5-dieonload-job | 0 grants | yes | yes | yes | n/a | n/a | n/a |
| S5-dying-job | 63/63 | yes | `[]` | 63/63 | n/a | n/a | n/a |
| S6-contend | 2 610, all `capped_default`, `reserve_mb = 1024` | yes | `[]` | yes | n/a | n/a | **fitted + withdrawn + declining-to-fit, all three** |
| S6-b18-loadstall | 862, all `capped_default` | yes | `[]` | yes | n/a | **yes, 1 835 008** | n/a |

### 4.3 Phase B — external pressure

<!-- PENDING: phase B -->

Runs only on the rebuilt binary. Expectations from the legs plan:

- **S4a** (hog steps, 12 GB free): batches ≈ **200 items** at 12 GB free (run1:
  single digits, because the fraction reserve ate the headroom);
  `reserve_rule = "capped_default"`, `reserve_mb ≤ 1024`, `limit_mb > 0`.
- **S4d** (8 GB free): **no `mb = 0`** (memory-blind) grants; `limit_mb > 0`.
- **S4b** (hog step mid-window): `external_mb` on the grant line tracks the hog
  step **within a few seconds** (the per-batch `free_mb` ingest), not run1's
  ≥ 31 s; `oracle_agreement` breaches bounded; the worker clamp still fires as
  backstop.
- **S4g** if time allows: the load guard still refuses; not a run2 change.
- Watch **C6**: if S4a's ≈200 lands and throughput is below run1's, the cause
  is likely the same — bigger admitted batches on a tight board are slower for
  models whose curve is flat.

### 4.4 Phase D — packing, docker

<!-- PENDING: phase D -->

Runs only on the rebuilt binary (S8 ocr-C7 ran separately on `65fd2f82`;
`run2-phaseD1-report.md` when it lands).

- **S8 pixmix**: `canvas_pixels` on nemotron's grants and load report, the
  fitted slope **within ~2× of the probe's** (run1: 4.33×), **no single-item
  batches** for 20 MP items (they pack at the canvas), `utilization > 0.3`, the
  epoch-2 profile written fresh.
- **S8 ocr-C7** (easyOCR, grantless): host-side pricing at the **2560²** canvas
  on the grant lines/units, **no 23–94 GB grants** (F-B). It measures the grant,
  not a fit, while `enable_batching = false` stands.
- **S11-C4** (Docker, 2 000-item job): 2 000/2 000; inference sockets bounded by
  the pool (`ls /proc/<pid>/fd | wc -l` inside the container ≈ 2×4 + reserve,
  not hundreds); `known transport h2c` in the log; the job's in-flight ceiling
  at the byte budget (4 096) not the descriptor clamp (384). **The container's
  config is baked from the shipped `docker.toml`, so this leg is the first real
  test of the P1 fix.**

### 4.5 Phase A re-run — S2 wd-vit, S3 wd-vit

<!-- PENDING: phase A re-run -->

On the rebuilt binary, with the `calib_hostless` block removed from
`server-C1.toml` in the same leg and the removal recorded. New expectations:
the ramp reaches **512** on wd-vit again (run1: 10 windows), `/health` shows
the desired-in-flight figure and no queue-bound stall, **no knee on wd-vit at
any point**, a seeded knee is provisional and revalidated within 4 windows, and
a `unit_budget` shrink lands within one window when the target falls.

### 4.6 Phase E — soak

<!-- PENDING: phase E -->

4 h (run1's S9 recipe, 4 h instead of 8), on the rebuilt binary. Expectations:
all jobs completed (no fixture is involved, so **any `partial` is P2**), 0
deaths, **no persisted knee below the ramp's plateau bucket**, deflation
returning to 0, store bounded, no cooldowns, `oracle_agreement` breaches lower
than run1's (the attribution fix), RSS bounded. Then SGLang restart.

---

## 5. Findings

Everything new in run2, sorted by severity, with run1's identifiers kept where
a finding is the same one. "Fixed" means the fix landed on this branch;
"verified" means a separate verifier signed it off. "User" means it is a
policy, default or design decision and is written up with options.

| Id | Sev | Statement | Status |
|---|---|---|---|
| **P1** | **BLOCKER** | With local inference, extraction jobs cannot be created at all on the shipped configuration: `policy::resolve_effective_host` read only `header::HOST`, HTTP/2 carries the authority in `:authority`, so every h2c self-call is 403 `no_policy` and `POST /api/jobs/data/extraction` answers 500. Two aggravators: the transport probe accepts **any** status, so a peer that 403s every h2c request is still selected as an h2c peer; and no shipped-shape config works around it | **Fixed and verified** in `74ca202c` (+ Desktop parity `4c2e00b6`). Rule: trusted `Forwarded` > request-target authority > `Host` > none, which RFC 9112 §3.2.2 makes *mandatory* on HTTP/1.1 absolute-form and RFC 9113 §8.3.1 makes correct on HTTP/2. **Not yet exercised by a leg** |
| **S1** | HIGH | The job could never put more than **200** predicts on the wire: `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200`, which `axum::serve` never overrides, over a `hyper-util` pool that shares **one** h2 connection per host — so a pool of 4 is really 1. Window formation then degenerates into `W → 200 − W`, the stable 136/64 cycle, and `max_units_measured` freezes at 136 while the orchestrator asks for 1 632. The number is in no log line, no `/health` field and no ceiling arithmetic | **Root-caused** (`run2-s1-analysis.md`); **fix in progress**, scope in §6 |
| **S1b** | MED | A `UnitBudget` **shrink never lands in a saturated job**: `forget_permits` can only take *available* permits and a released permit goes straight to a waiting item task. Harmless while the transport was the real bound; a live bug the moment the header is used to *reduce* pressure on a squeezed board, which is what T5 exists for | **Fix in progress**, folded into S1 |
| **P2** | HIGH | One predict in ~8 000 items failed **400 `invalid multipart body`** from the gateway to itself; the item is lost with no retry (the R2a re-queue covers a worker death, not a 400) and the job is reported `partial`. Client and server are the same process and the same encoder served the other 1 999 items, so it reads as a transport-level truncation on the h2c path | **Fix in progress** (`run2-p2-fix-report.md`) |
| **F1** | HIGH | R1's knee estimator still fitted a spurious knee on a flat curve and persisted it across a restart: `knee_units = 3` at 14 observations on wd-vit (whose curve is flat: 35.9 items/s at batch 1, 36.1 at 2 048), persisted as 7, and a **fresh** 2 000-item job then ran its whole length between 7 and 31 units. Diagnosis: the plateau was judged against **the ring's own best**, which was the smallest bucket, so the first candidate compared bucket 1 against itself; the expiry then lost the race (widen at 12 clean windows, refit within ~1 s) | **Fixed** in `9cbd6304` (+`d230d5ba`, `19e2bf9f`, `ce03bd0d`): five vetoes, first-window warm-up exclusion, provisional seeded knee. Replay: **no knee at any point** on either wd-vit leg, on 218 and 205 reconstructed observations that reproduce every logged fit exactly. **Verifier running** |
| **C6** | MED/HIGH | Under a hard squeeze (4 GB free) the R5 reserve cap admits **60× bigger batches** and wd-vit runs at **0.56×** run1's throughput (21.85 vs 38.98 items/s, p50 5 323 vs 3 199 ms) with nothing in the ledger noticing — and nothing could: the knee compares log2 buckets *within one run*, and this run never ran wd-vit small on a squeezed board to compare against. Safe (0 OOM, 0 negatives, 0 unsafe grants); the grant is 3 040 MiB against 3 085 MiB of headroom, so the allocator has nothing to spare | **User.** Expected behaviour of an approved change, not a regression of correctness (run1 issued a *blind* grant in the same place). Options below |
| **F1 residue** | MED | **MobileCLIP's `knee_units = 127` is not restored** under `KNEE_PLATEAU_BUCKETS = 2`: the recorded ring has exactly one quiet bucket above the bend, because the ramp stalled at 136 units for S1's reasons and nothing at 256 was ever measured. Two observations there and the same ring answers 127 (pinned by a test). The knee was worth nothing measurable either way: 0.94× master with it in run2, 1.00× without it in run1 | **User** — a one-line constant. Options below |
| **C2** | MED | A **trusted** `oom_class` leaves no trace: `oom_verdict` returns `Trusted` for `typed_exception` and `marker` and logs nothing, and `oom_class` never reaches `panoptikon.log`, `/health` or `analyze.py`. So the plan's clause "`oom_class.source` present on the OOM measurements" is only verifiable for the *vetoed* case (5 954 marker-classified OOMs in `S5-oomtimed` with zero mentions of `oom_class`, against one mention in `S5-oom2nd`) | **Open, observability.** A one-line `debug!` on the trusted arm, or an `oom_class` column in `analyze.py` |
| **C3** | MED | The `message_pattern` veto fired on a **genuine** OOM. Both rules are individually right (the measurement was vetoed at `free_mb_at_failure 96 518 ≥ grant.mb 96 356`; the error frame's `INFERENCE_OOM_WINDOW` marker deflated 61 µs later), but on an **idle board the veto will fire on essentially every `message_pattern` OOM**, because `grant.mb` is the whole board. The tier is load-bearing only on a tight board — which is where real OOMs happen, so it is defensible — but a model raising bare driver text *without* an `INFERENCE_OOM_*` prefix (a batch-1 failure inside `run_single`) would then not deflate at all | **User / watch.** A fixture artefact on an idle board; worth a look before the soak |
| **C4** | MED | A job's `failure_reason` loses the cooldown detail **and the model id**: `"Failed to load model: model load failed on all 1 inference endpoints"`, where `/health.load_cooldowns[]` and the 503 body both carry `inference_id`, `failures`, `retry_at`, `retry_after_secs`, `window_secs` and the last error. Two causes: `inference_pool` renders any endpoint failure as `inference request failed (<status>): <short detail>`, discarding the structured body; and the job's retry cadence meant the second attempt arrived after the 2 s window and got a plain 500 from a real load | **Open** — the improvement over run1 is real (9.5 s / 2 attempts vs 259 s / 4) but the stated clause is not delivered |
| **C5** | LOW | A systemically failed job logs that its re-queued items "then completed": the branch is `else if guard.requeued_items > 0`, reached whenever there is no *partial* reason — which includes `JobFailure::Systemic`. Two statements later the same code writes `outcome: "failed"`, `failed_items: 2000` | **Open**, log text only |
| **C7** | INFO | Two idle residents were trimmed in the same millisecond (6 µs apart) where run1 trimmed one, which is why the flag latency reads 2.888 s rather than 1.837 s. Both released, 6.0 and 9.4 ms round trips | **Not a fault** |
| **`unstated` unexercised** | INFO | R11's sentinel rename has **no producer on this host**: all four shipped models and every fixture resolve a dtype by inference, so the string appears 0 times in fifteen legs. `oom_class.source = "typed_exception"` is likewise unreachable — no fixture raises `torch.OutOfMemoryError` itself | **Open**; needs a model whose weights state no precision, and a fixture that raises the typed exception |
| **`CostHealth` omits the canvas** | LOW | `/health`'s cost block does not report `canvas_pixels`, so a leg confirming which canvas is in force must read the load-time DEBUG line or the worker's `resolve_canvas_pixels` INFO. Deliberate: adding it is a spec change → `openapi.json` regen → `ui` types regen → gitlink bump | **User** — cheap, but it moves the spec |
| **easyocr `enable_batching = false`** | MED | The three `easyocr_*` ids still ship it, so their worker takes the grantless path, reports no `units` and **fits no slope**. The host cap now bounds their grants (F-B's symptom), but S8-ocr-C7 measures the grant, not a fit, until the flag is flipped | **User** — run1's F-B/F-F, half-addressed |
| **epoch bump** | INFO | All seven shipped `pixel` ids carry `metadata.cost.epoch = 2` (`doctr/dots_ocr`, three `doctr/easyocr_*`, `clip/qwen3-vl-embedding-{8b,2b}`, `clip/nemotron-embed-vl-1b-v2`), because a canvas re-denominates what one unit *is* and the profile key does not carry it. Any run1 profile row for them is **ignored, not migrated** | **Intended.** A run2 leg on those models starts from an empty profile even on this host |
| **`ui` submodule unpushed** | MED (release gate) | Commit `9b28044` on `batch-calibration-ui` is unpushed; the gitlink resolves only from this host's clone, and a clean-worktree image build needs the local remote | **User must push** to `reasv/panoptikon-ui` |
| **B16 premise falsified** | INFO | Track E's arithmetic reason for a fixed 256-request gate — "4 096 units ÷ 64 units per request = 64 concurrent requests" — is false. `REQUEST_UNIT_BUDGET = 64` is a chunk bound *within one item's work units*; an image item has exactly one, so a job sends **one item per request**: 1 999 requests for 2 000 items. 4 096 units is 4 096 concurrent requests for exactly the image taggers and CLIP embedders G7 exists for | **Recorded**; it is why the S1 fix changes the gate |
| **S2 `persistence` reports INFO** | INFO | `S2-minilm` and `S3-wdvit` queued only `fit_changed`/`knee_changed` updates inside the recording, never an `anchor_advanced`, so the check has no advance→write delay to measure. Both stores were written | **Not a defect** in product or tool; flagged so the tables are not misread |

### The user-decision items, with options

| Id | Options | Orchestrator's recommendation |
|---|---|---|
| **C6** (throughput under a hard squeeze) | (a) accept and document: the reserve cap trades throughput on a tight board for admission that is priced rather than blind, which is strictly safer; (b) make the ledger *notice* — compare a squeezed board's throughput against the same model's own unsqueezed buckets rather than only within one run, so the knee/comparator can see it; (c) bound the admitted batch on a board whose headroom is within a small multiple of the grant, i.e. treat "grant ≈ headroom" as its own regime; (d) re-tune the 1 024 MiB cap | (a) for this release, with (b) recorded as follow-up work. The failure the cap prevents (memory-blind grants at `limit_mb = 0`) is worse than a slower batch, and run1's 38.98 items/s was bought with grants the ledger could not price at all |
| **F1 residue** (`KNEE_PLATEAU_BUCKETS`) | (a) keep **2**: a plateau resting on one comparison between two medians is not a plateau, and MobileCLIP's knee is found late rather than lost (two observations at 256 units restore it); (b) set **1**: restores MobileCLIP's 127 on its recorded ring, and wd-vit is *still* refused, by rules 1 and 2 | (a). The asymmetry the whole design rests on points this way: a false negative is a knee found late, a false positive is F-A. The constant is named and its derivation is in the doc comment, so (b) is one line if the user prefers the literal acceptance criterion |
| **C2** (trusted `oom_class` invisible) | (a) one `debug!` on the trusted arm naming source, exception and `free_mb_at_failure`; (b) an `oom_class` column in `analyze.py`; (c) both | (c) — (a) makes it diagnosable in the field, (b) makes it a verdict in the protocol |
| **C3** (the veto's real reach) | (a) corroborate against the *board's* free memory rather than the grant's envelope, so an empty board does not auto-veto; (b) keep the rule and require impls to prefix driver text with `INFERENCE_OOM_*` (which `packing` already does on the window path); (c) accept: the tier is load-bearing exactly where it matters | n/a — the outcome was right on every measured leg; the risk is a batch-1 failure inside `run_single` |
| **C4** (job failure reason) | (a) carry the structured 503 body through `inference_pool` instead of rendering it to a string; (b) have the job's abort path read `/health.load_cooldowns[]` for the model it failed on; (c) accept the generic text and point users at `/health` | (a) — the body already carries every field the reason wants |
| **easyocr `enable_batching`** | (a) flip it to `true` on the three ids now that the host caps their pricing, so they accumulate fit samples; (b) keep the grantless path and treat "thousands of grants with `fit samples 0`" as a warned condition (run1's F-B option) | n/a — (a) is what turns S8-ocr-C7 from a grant measurement into a calibration one |
| **`CostHealth` canvas** | (a) add `canvas_pixels` to the cost block and accept the spec/UI regen; (b) leave it to the log lines | n/a |
| **S1 gate policy** | (a) the fix in §6: real N-connection pool, an explicit server stream limit, and a gate that follows the published desired-in-flight figure with a floor; (b) keep the fixed 256 gate and accept that job-driven calibration is capped there; (c) F1-only (an explicit `max_concurrent_streams`) and leave the gate fixed | (a). It is a deliberate reversal of a Track E decision made on an arithmetic premise this leg falsifies, and it is what lets a 97 GB board be calibrated by a job |

---

## 6. Decisions taken by the orchestrator during run2

Recorded from `orchestrator-state.md` (entries from *RUN2* onward), with the
reasoning as it was given at the time.

| Decision | Reasoning |
|---|---|
| **Pin the cross-track wire and store names in the common brief** before any track started (per-batch `free_mb`/`free_source`, `clamped{from_units,to_units,free_mb}`, `oom_class{source,exception,free_mb_at_failure,device}`, the `unstated` sentinel, `metadata.cost.canvas_pixels`, the 503 `load_cooldown` body and `Retry-After`, the `worker_died` kind) | Four agents were working in one checkout with no way to message each other mid-task. A name invented twice is a merge conflict at best and a silent wire mismatch at worst; pinning them made every track's half of a two-sided change compile against the other's without coordination |
| **R5: the confidence widening stays capped** with the base reserve, rather than capping only the base and leaving the widening uncapped | The user's rule is a flat bound ("at most 1 GB is ever withheld"). The widening multiplies *other processes'* usage, so it vanishes on a headless board where our fit is equally untrustworthy — it is a conservatism knob about somebody else's memory, not a safety term against fit error. The real protections (geometric ramp, extrapolation ratchet on local samples, the worker's per-batch clamp) are untouched. And an uncapped widening reproduces T4: `limit → 0` produces **memory-blind grants**, which is strictly less safe than a slightly smaller reserve |
| **`KNEE_PLATEAU_BUCKETS = 2` stays**, accepting that MobileCLIP's 127 is not restored on its recorded ring | One bucket above the candidate is a single comparison between two medians — the same "two points are not a curve" objection `MIN_KNEE_BUCKETS` answers for the fit as a whole. Two means the flat stretch spans a factor of ≥ 4 in batch size. The shortfall is an artefact of the leg (the ramp stalled at 136 for S1's reasons, so nothing at 256 was measured), the knee bought nothing measurable on either run, and the asymmetry is deliberate: a false negative is a knee found late, a false positive is F-A |
| **C6 is classified as expected behaviour, not a regression** | Run1's 38.98 items/s at 4 GB free was bought with `unit_budget = 1` and `mb = 0` — a memory-blind grant, the condition R5 was approved to remove. Run2 is slower and *priced*: 0 OOM, 0 negatives, 0 unsafe grants. It is a real cost and it is recorded as one, but it is the approved trade, not a defect |
| **The S1 fix scope**: a real N-connection pool (independent clients, least-loaded) + our server advertising an explicit stream limit consistent with the gate + the gate following the **published** desired-in-flight figure (floor 256; HTTP/1.1 fixed 256) + letting refills land before window formation + S1b deficit accounting + `/health` exposing the desired-in-flight figure and a queue-bound counter + a stub test that pins the stream limit. **Rejected**: advancing the ramp on grant fill | Every element removes one layer of the "a ceiling no layer can name" problem: the pool becomes what its constant says, our own server stops imposing a limit our client does not know about, the gate stops being a fixed number chosen on falsified arithmetic, and the counter turns the next occurrence into a five-minute diagnosis. Ramping on grant fill was rejected because it would advance the anchor on evidence no batch actually produced |
| **R6's admission gate is per board, not global** | The R-table calls it a *board*-admission gate, VRAM is a per-board quantity (a global gate serializes loads that cannot interact), and it is a strict superset of the old behaviour on every shipped configuration: every unpinned model resolves to the same default board, so `max_concurrent_loads = 1` reproduces the retired global lock exactly |
| **A new `canvas_pixels` key on the `load ok` response**, parsed host-side and used only where the registry declares nothing | `doctr/dots_ocr`'s ceiling lives in an `AutoProcessor` config downloaded with the weights and is not a fact the registry can state truthfully across model revisions. Making the worker report what it found — behind the registry, never over it — is what makes the introspection tier load-bearing rather than decorative, while keeping a maintainer's declaration correctable from the one place a maintainer can act |
| **Declare the canvas only where it prices something**, and record the rest as comments | For an `item`/`count` or `none`-class model a canvas is inert by construction (`min(1, cap)` is 1), so ~40 live declarations would be lines that look like they do something and do not, each costing a DEBUG line. The numbers themselves are preserved in comments (openclip 224²…448², WD v3 448², Florence-2 768², `db_resnet50` 1024²), one line from being live if an id is ever reclassified to `pixel` |
| **Bump `metadata.cost.epoch` to 2 on all seven shipped `pixel` ids** | The profile key carries `unit`/`aggregation` but **not** the canvas, so a run1 slope in MiB per *raw* pixel would keep matching and be applied to capped units — it under-predicts, which over-admits, the one direction the design says the ledger cannot absorb. `epoch` is the documented lever for "memory behaviour changed without moving a key component" |
| **Type the worker-death marker instead of matching a substring** (`slot_error::Unattempted`) | Track E's death signal was the substring `"failed fatally"`, which matched one rendering of six. A typed marker at the three sites that cover all six shapes makes the classification structural, and `fail_requests` is the single funnel where four of them land |
| **Take the C1/C7 policy workaround as a *tool* change, and only for C1 and C7** | Phase A could not run a single job leg otherwise, and the alternative was to stop the run for a product fix. Scoping it to the two configs that need it leaves C0/C2/C3 and the compose overlays in the shipped shape, which is what makes the P1 fix testable by a later leg instead of permanently papered over |

---

## 7. Ground truth measured

### Per-model base, against a judged idle plateau (S2-base, `vramrec.py` at 4 Hz)

The first judged `base_accuracy` in either run. The window that measures the
same quantity as `base_mb` is between the replica's load `ok` and its **first**
grant or predict; holding the models resident and idle turns run1's empty
window into hundreds of samples.

| Model | ledger `base_mb` | oracle per-process | error | oracle samples in the window |
|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | 964 | 964 | **0.0 %** | 535 |
| `clip/apple_MobileCLIP-S1` | 732 | 732 | **0.0 %** | 504 |
| `textembed/all-MiniLM-L6-v2` | 654 | 626 | **4.47 %** | 476 |
| `clip/nemotron-embed-vl-1b-v2` | 3 788 | 3 788 | **0.0 %** | 373 |

`cadence_blind: false` and `first_work_dt_ms: null` on all four rows, so none
of them is the run1 artefact. Nemotron's 3 788 closes R12: 3 201.1 MiB of bf16
weights (`model.safetensors` = 3 356 585 352 B) + allocator slack
(`reserved_at_load_mb = 3 238`) + ~550 MiB of CUDA context.

### Learned slope against run1's probe

| Model | probe slope (run1) | run1 learned | run2 learned | run2 error |
|---|---|---|---|---|
| wd-vit | 50.5403 | 50.5625 (+0.04 %) | **50.6** | **+0.12 %** (ratio 1.0012) |
| MobileCLIP | 16.7539 | 16.7047 (−0.29 %) | **16.2286** | **−3.1 %** (ratio 0.9686) |
| MiniLM | 0.0245317 | 0.037323 (+52.1 %) | **0.032248** | **+31.5 %** conservative (ratio 1.3145) |

### The throughput ring wd-vit knew when it fitted `knee_units = 3`

Reconstructed from the ledger's own log lines and validated against every
logged `throughput_samples` and `observations=` count (zero mismatches over 88
windows). This is the table the R1e rules were derived against.

| bucket | units | n | median u/s | rel. MAD | in the fit? |
|---|---|---|---|---|---|
| 1 | 2 | 2 | **40.77** | 0.084 | yes — **and the ring's best** |
| 2 | 4 | 2 | 34.96 | 0.158 | yes |
| 3 | 8 | 2 | 40.31 | 0.104 | yes |
| 4 | 16 | 1 | — | — | **dropped** (`MIN_KNEE_BUCKET_SAMPLES`) |
| 6 | 64 | 6 | 39.79 | 0.007 | yes |
| 7 | 136 | 1 | — | — | **dropped — and it was the frontier** |

Every retained bucket is under the 0.20 dispersion threshold, which is why the
variance filter never fired; `knee_best` was `None`, so the reference was the
ring's own best — **bucket 1, compared against itself** at threshold
0.9 × 40.77 = 36.69. That single comparison is F1.

MiniLM's refusal is the same statistic on the other side: bucket 13, two
observations, dispersion **0.2128157093511856** against a threshold of 0.20,
59 times.

### Deflation, cooldown and death, measured (Phase C)

| Quantity | Run1 | Run2 |
|---|---|---|
| deflation cap | none — 6 589 levels in 120 s | **4** = `ceil(log2(max(anchor, seed)))+1`, seed 8 |
| repayment on clean windows | 3 clean windows per level, 7.04 levels/s | unchanged; **4 → 2 → 0 in 0.5 s** |
| repayment by time | did not exist | **one level per 30.0 s idle**, 4 → 0 over 120 s, one DEBUG line each |
| load-failure ladder | none | **2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0 s**; 7 attempts in 182 s (run1: 93) |
| refusals while cooling | 93 × HTTP 500 | **13 145 × 503**, `retry-after: 4`, `detail.kind = "load_cooldown"` |
| B18 predict latency during a neighbour's load | p50 11 886 ms = **28.3× p50**, 100.2 % of the load | p50 551 ms, max 951 ms = **1.86× p50**, **7.1 %** of a 13.388 s load |
| dying-in-job, 2 000 items | 92 deaths / 92 respawns in 120 s; items lost silently | **63 spawns / 63 deaths / 63 `worker_died` settles** over 137 s; **2 000 re-queues**, **2 000 `job_failures`**, job `failed` with `failed_items: 2000` |
| memory-blind grants under a squeeze | 113 of 3 308 (111 at `mb=0 ub=1`) | **3 of 2 610** |
| samples at `limit_mb = 0` | 367 | **0** |

`<!-- PENDING: phase B/D -->` The ceiling-probe ground truth is run1's
(`docs/batch-calibration-run1-report.md` §6) and is unchanged for the item and
token models; **the five pixel models must be re-probed under the canvas**,
because `ceiling_probe.py` now prices its pixels at the canvas it resolves
(`893e8a7d`, `9be09e5e`, `18cc2e77`) and the seven `pixel` ids carry
`epoch = 2`.

---

## 8. Protocol assessment

### What the legs caught that the tracks, the verifiers and 1 586 unit tests did not

| Finding | How it was caught |
|---|---|
| **P1** | Starting a gateway on the shipped-shape config and submitting one real extraction job. Every unit test of the h2c client talks to a service with no policy layer in front of it; every unit test of the policy layer builds its request with a `Host` header |
| **P2** | 8 000 real items through the real encoder. One failure in 8 000 is invisible to any test that sends tens |
| **F1** | Comparing the fitted knee against the model's **measured curve**, per model, exactly as the protocol's own S2 rule says ("does the knee match the curve", never "is the knee small") — and then restarting on the store it wrote, which is the only way to see a knee that is wrong *and* permanent |
| **S1** | Reading `queue_depth + last_window_items` across 70 windows on two independent legs and noticing that the two phases of the cycle sum to exactly 200 every time. No panoptikon-side bound is 200 |
| **C6** | Running the same model, same hog, same client shape as run1's leg, and comparing throughput rather than only safety |
| **C2/C3** | Asking the recordings to *evidence* a field the change set added, rather than asserting the code path exists |

### Tool commits (`tools/calibration-protocol/**` only)

| Commit | What it fixes |
|---|---|
| `03bb8a4d`, `6665b931` | `vramrec.py` memoised a half-read worker identity (`[panoptikon-spaw]`, empty env) inside a fork/exec window, which is the whole of run1's F-C; the retry is now bounded by wall clock rather than sample count |
| `90409b41`, `7b3ea31e`, `0643942c`, `832474b0` | `analyze.py`'s `base_accuracy`: recognise worker pids from the gateway's spawn lines, reject a pid older than the replica's spawn, read the base from the load-to-first-work window rather than one sample, and pick the replica's **own** pid |
| `cf1939e6` | **T1.** `base_accuracy` picked "the freshest of our pids first sighted inside [spawn, admission]"; MiniLM's worker was first sighted 5 ms *after* its admission, so the row compared MiniLM's 654 against MobileCLIP's 732 — **10.66 %, a FAIL that was pure attribution**. The run2 binary states the inference id and the pid on one spawn event, so the fix takes that pid whenever the log names one. S2-base goes FAIL → **PASS** on the same recording |
| `71cb0827`, `e9e44549` | `loadgen.py --prewarm-only/--hold` and the **S2-base** leg in the protocol §4 + README — the leg that made `base_accuracy` judgeable at all |
| `e31fbd4f` | The calibration README's statement of `base_accuracy`'s attribution and cadence rules |
| `7a4d38fe` | run1's report: **F-C retracted** |
| `a39ab8d8`, `2779a81e`, `ea6f94f9` | The C1/C7 P1 workaround, and easyOCR's canvas + epoch restated in the C7 override registry |
| `fc4e1e9e` | The fixture README claimed an extraction job "would reject the payload against the declared `output_type`". On the run2 binary it does not — a 180-item job over `failbatch_oomtext_cuda` completes and the 2 000-item `dying_cuda` job fails with 2 000 `job_failures` rows |

### Instrument gaps still open

- **`analyze.py` has no `reserve_rule`/`reserve_mb`, `oom_class` or
  `knee_expired` column.** Every Phase C measurement of those fields came from
  `grep`.
- **The desired-in-flight figure is invisible.** It is neither logged nor on
  `/health`, so S1's table is a derivation rather than an observation. The
  value already lives in `ModelStats`; exposing it is part of the S1 fix.
- **`oom_class` never reaches the log**, so a trusted classification cannot be
  evidenced (C2).

---

## 9. Portability

Run1's platform table (`docs/batch-calibration-run1-report.md` §8) stands.
Run2 adds five items to what a platform pass must check first.

1. **The transport, and its stream limit.** A gateway that speaks h2c to its
   inference server inherits whatever `SETTINGS_MAX_CONCURRENT_STREAMS` the
   peer advertises. Record the peer's limit and the observed in-flight
   plateau; if they match and the policy asked for more, that is S1 on another
   host. A remote inference server (the user's NAS talking to this GPU box)
   is the case R10' exists for and the one no leg has run yet.
2. **The policy host on HTTP/2.** Any deployment behind a proxy that
   terminates HTTP/2, or any client using prior knowledge, exercises the
   authority path P1 fixed. Record one `curl --http2-prior-knowledge` against
   `/api/inference/cache` per platform.
3. **`base_method`** stays the W4 item, and **R8's measured context is still
   untested** — this driver never enters the degraded tier, so the probe is
   exercised by unit tests only. Windows/WDDM and ROCm containers are where it
   first runs for real, and where the run1 constant (500 MiB against a measured
   666–668 MiB) was wrong.
4. **The OOM classifier's device tier.** R3's `message_pattern` list is closed
   and device-shaped (`hip out of memory`, `mps backend out of memory`,
   `enforce fail at alloc_cpu.cpp`, …). BC-250/ROCm and MPS are the platforms
   where a missing wording now means *no deflation at all*, where run1's bare
   substring meant *too much*. Record which tier fired on every OOM.
5. **The pixel canvas per platform is a no-op** — it is a registry fact, not a
   host fact — but the **epoch bump means every pixel model re-measures on
   every platform**, so budget the ramp time.

---

## 10. Release-note text (draft)

### Jobs now report partial work, and failed items are listed

> A job that could not process every item now reports **partial** instead of
> completed, and every failed item is listed by
> `GET /api/jobs/data/failures` with its path, checksum, the stage that failed
> and when. If an inference worker dies mid-batch, the items that were in
> flight are **re-queued once** rather than recorded as errors; if they fail
> again the job says so. Jobs that failed outright now also appear there, with
> the reason and a real end time — previously a failed job recorded an end
> time equal to its start time and an empty failure list.

New API surface: `JobOutcomeStatus` gains `"partial"`; the failures endpoint
gains `job_failures_total` / `job_failures[]` (`JobItemFailure`) and
`failed_jobs_total` / `failed_jobs[]` (`FailedJobRecord`); job records gain
`outcome`, `failed_items` and `failure_reason`. Existing rows carry an empty
outcome and render as *running*, so nothing is backfilled and no existing
column changes meaning.

### A model that cannot load is refused politely, not hammered

> If a model fails to load, Panoptikon now waits before trying again —
> 2 seconds, then 4, 8, 16 … up to 5 minutes. While it is waiting, requests
> for that model are answered **503 Service Unavailable** with a
> `Retry-After` header and a body naming the model, the number of consecutive
> failures and the time it will try again, instead of a generic 500. `GET
> /health` lists every cooling-down model under `load_cooldowns`. Loading
> another model is unaffected: loads are now serialized per model and admitted
> per GPU, so a load onto one board no longer stalls predictions on another.

New settings, all with defaults (nothing is written to your config file):
`[inference_local] max_concurrent_loads` (default 1),
`load_failure_cooldown_secs` (default 2, **0 disables**),
`load_failure_cooldown_max_secs` (default 300).

### Less memory is held back from your GPU

> Panoptikon reserves a margin over what *other* processes are using on a GPU,
> so your own desktop's VRAM use does not spill into ours. That margin is now
> **capped at 1 GiB when you have not configured one**: on a busy board the
> old rule could withhold tens of gigabytes and, past a point, stop pricing
> batches at all. If you set `margin` yourself it is honoured exactly as
> before, uncapped. `GET /health` now reports `reserve_mb` and `reserve_rule`
> (`user_margin` or `capped_default`) beside each GPU's budget.

### Better handling of out-of-memory errors

> Panoptikon used to treat any error message containing the words "out of
> memory" as an out-of-memory event, and shrink its batches for minutes
> afterwards even when the GPU was empty. It now recognises real allocator
> failures **structurally** — the exception type where the framework raises
> one, an explicit marker otherwise — and falls back to a short list of
> driver-specific messages only when it can also confirm from the worker's own
> live free-memory reading that memory was actually tight. The shrink is also
> **capped** now, and recovers on a clock (one step per 30 seconds) as well as
> on successful batches, so a brief fault no longer costs many minutes of
> small batches.

### Large images are priced at the size the model actually uses

> Models that resize their input (the vision-language embedders and the OCR
> detector) are now charged for **the pixels they will actually process**, not
> the pixels in your file. A 20-megapixel photo fed to a model that works at
> 1.8 megapixels no longer occupies a whole batch by itself. **Panoptikon will
> re-measure these models once after upgrading**: their stored memory profiles
> describe the old pricing and are ignored, not migrated. Affected:
> `doctr/dots_ocr`, the three `doctr/easyocr_*` models,
> `clip/qwen3-vl-embedding-8b` and `-2b`, and
> `clip/nemotron-embed-vl-1b-v2`.

### Talking to the inference server

> Panoptikon now talks to its inference server over **HTTP/2 (cleartext, prior
> knowledge)** where the server supports it, multiplexing all requests over a
> small pool of connections instead of opening one connection per request. A
> server that does not speak HTTP/2 is detected on the first call and served
> over HTTP/1.1 exactly as before. This is what removes the file-descriptor
> pressure of a large job: a window that used to cost two sockets per in-flight
> item now costs eight sockets in total.
>
> If you run Panoptikon **behind a proxy or with a policy configuration**, note
> that HTTP/2 requests carry their host in the `:authority` pseudo-header and
> send no `Host` header. Panoptikon now resolves a request's host from the
> request target's authority first, then `Host` — a trusted `Forwarded` header
> still outranks both when `trust_forwarded_headers` is set. Policies that
> list `hosts` therefore match HTTP/2 requests the way they always matched
> HTTP/1.1 ones.

### For anyone reading calibration profiles

> `data/inferio/calibration.toml` now records `dtype_method` beside `dtype`
> (how the model's precision was determined: `selected`, `attribute`,
> `inferred` or `unstated`), and the sentinel for "the model states no
> precision" is spelled **`unstated`** rather than `unknown` — it is a fact
> about the model, not about Panoptikon. Rows written under the old spelling
> stop matching and are re-measured once.

### Also

- The worker now reports its live free memory **on every batch** rather than
  once per window, so the server's picture of other processes' VRAM use
  refreshes at response cadence.
- The CUDA context size is now **measured** at first initialisation instead of
  assumed to be 500 MiB, on the paths where the driver hides per-process usage.
- The UI's generated API types were regenerated from `openapi.json`
  (submodule commit `9b28044`).

---

## 11. Host restore checklist

**The run is not over.** SGLang is **stopped**. Phases B, D and E, the S2/S3
re-run and the outstanding fixes are still to come, so nothing below is done
yet.

### 1. Restart SGLang — the one mandatory step, **still outstanding**

```
docker compose -f /home/admin/docker/dsv4flash/docker-compose.yml up -d
```

Stopped at ~12:50 UTC on 2026-09-04 for the legs, after run1 restarted it at
03:55. Both boards read **2 MiB** at the end of Phase A and at the end of
Phase C. Nothing under `~/docker` was read or edited; `~/docker/inferio/.env`
was never read.

### 2. Current host state

- **GPUs**: board 0 and board 1 both **2 MiB / 97 887**, idle, no compute apps.
- **Processes**: no gateway, no `inferio_worker`, no `vramrec`/`healthrec`/
  `loadgen`/`hog`. Four recorder processes orphaned by two aborted S2-wdvit
  attempts were found and killed at the end of Phase A; the successful leg's
  JSONLs were verified clean (single header, 0 NUL bytes, 0 unparsable lines,
  monotonic timestamps spanning only that leg).
- **Docker**: only the user's `sglang-grafana` and `sglang-prometheus` (up
  7 days, untouched).
- **Binary**: `target/release/panoptikon` at `65fd2f82`, 86 397 864 B, built
  12:41 UTC, reports `panoptikon 0.1.8`. **A rebuild is owed** before Phases
  B/D/E.
- **Images**: `panoptikon:calib-cuda` = `6fe5d86e3a1e` (9.43 GB, built from a
  clean worktree at `65fd2f82`, 378 s); run1's kept as
  `panoptikon:calib-cuda-run1` (`2a2c93ad6375`); the CPU and master images are
  still run1's.
- **Git**: nothing pushed. The `ui` gitlink `9b28044` on
  `batch-calibration-ui` is **unpushed and the user must push it**.
- **Results**: `tools/calibration-protocol/results/run2/` — Phase A's seven
  directories plus Phase C's ten, 415 MB for run2 so far, git-ignored.

### 3. Stores this run has produced — which are safe to seed from

| Store | Safe? |
|---|---|
| `run2/S2-minilm/calibration.after.toml` | **Yes** — anchor 19 684, slope 0.032248, **no knee**. The only clean store of the run so far |
| `run2/S2-wdvit/calibration.after.toml` | **No** — `knee_units = 7` on a flat-curve model (F1). Used deliberately to seed S3 and nothing else |
| `run2/S3-wdvit/calibration.after.toml` | **No** — `knee_units = 15`, anchor stuck at 136 |
| `run2/S3-wdvit-kcw/calibration.after.toml` | **No** — descended from a hand-edited seed |
| `run2/S2-mobileclip/calibration.after.toml` | **No** — `knee_units = 127`, correct for this model and corpus, but a knee all the same |
| `run2/S6-contend/calibration.after.toml` | **No** — three profiles and **no knee** (a first; run1's equivalent is on the poisoned list), but 4 local samples each, learned under a 20 GB hog and three-way contention |
| `run2/S6-b18-loadstall/calibration.after.toml` | **No** — one profile, `fit samples 0` |
| every S5 leg | no store written (fixture batches move the allocator not at all) |

Run1's safe seeds still hold for the **item and token** models
(`S2-wdvit`, `S2-wdvit-fixed`, `S2-wdvit-6a5e6799`); for the **seven pixel
ids** every run1 row is ignored by key, because `metadata.cost.epoch` is now 2.

### 4. Deviations to undo before the run ends

- **Remove the `calib_hostless` policy block** from
  `tools/calibration-protocol/config/server-C1.toml` (and `server-C7.toml`) in
  the first leg that runs on the P1-fixed binary, and record the removal in
  that leg's runlog. It is the only configuration deviation of the run.

---

## Appendix: the commits of this run

96 commits, `0d6b36c5..HEAD`, grouped by the agent that made them; within a
group, oldest first. Product commits **P**, tooling/fixtures/configs **T**,
documentation **D**. Nothing pushed.

### Track L — R1(a, d, contention tag, variance filter), R4, R5 ledger half, R11 store

| Commit | Kind | Subject |
|---|---|---|
| `e6abd09e` | P | Keep squeezed, blind and clamped batches out of the throughput knee |
| `8f71379a` | P | Fit the knee only from samples taken with the board to one replica |
| `9e8d6810` | P | Refuse a throughput knee while a size bucket disagrees with itself |
| `f161850f` | P | Make the throughput knee a brake that expires instead of a ceiling |
| `83acdbdc` | P | Cap the deflation counter and repay it by time as well as by windows |
| `9a10bfe5` | P | Cap the default VRAM reserve and refresh external usage per batch |
| `a1fb91a9` | P | Store dtype_method in the profile and rename the sentinel to unstated |
| `d911b676` | P | Silence the clippy lints the run2 ledger changes introduced |
| `40aa1d0e` | D | Code map: the run2 ledger changes (knee, deflation, reserve, per-batch free) |
| `b0cc2c95` | D | Spell the dtype sentinel unstated in the load report's doc comments |

### Track L verifier

| Commit | Kind | Subject |
|---|---|---|
| `29f5636b` | P | Fix the knee expiry's withdrawal arm: guard, ceiling and store signal |
| `2314e609` | P | Suppress the collapse verdict, not the OOM riding on the same batch |
| `6afed305` | P | Read the worker's OOM class on the host and stop deflating on wording |
| `1031504c` | P | Pin the OOM tiers the host trusts without corroborating them |
| `c0896110` | D | Name the per-board load admission gate in reserve_load's doc |
| `75016a3f` | T | Name the model on the spawn line, and let analyze.py prefer it |
| `6e515174` | D | Say in the shipped configs what an unset VRAM margin now means |
| `00798eda` | P | Match the OOM tiers by name so all three are live, not just one |
| `0b166de1` | D | Code map: the R3 host classifier and Track L's moved line numbers |
| `9a9b5612` | D | Code map: the four line numbers the OOM tier match moved |

### Track M — R6 per-model locks and board gate, R9 load cooldown

| Commit | Kind | Subject |
|---|---|---|
| `e6e510d0` | P | Give every model its own load lock and gate loads per board |
| `e2679f62` | P | Cool a model down after a failed load instead of respawning per request |
| `3ad22c92` | D | Code map: the load lock is per model now, plus the load cooldown |

### Track M verifier

| Commit | Kind | Subject |
|---|---|---|
| `d8721f90` | P | Stop the load-failure ladder panicking on the 33rd failure |
| `702ea8ac` | P | Make an unresolvable board pin wait for every board's load permit |
| `391b66cb` | T | Ship the R6/R9 load-policy keys as commented examples |
| `804f5148` | D | Code map: refresh the manager line numbers after the R6/R9 fixes |

### Track E — R2a re-queue and `partial`, R2b failures endpoint, R10' h2c pool, R11 UI types

| Commit | Kind | Subject |
|---|---|---|
| `521cca81` | P | Re-queue a died-on window's items once and report the job partial |
| `22256f8c` | P | List failed items and the jobs behind them in the failures endpoint |
| `fed96ea4` | P | Cover the partial job outcome end to end in the queue tests |
| `ddd44e4a` | P | Multiplex inference requests over a bounded h2c connection pool |
| `f506dc2b` | T | Bump the UI submodule to the regenerated API types |
| `b2723f16` | D | Document the job failure audit, the partial outcome and the h2c pool |
| `049d271c` | T | Re-bump the UI submodule after the run2 spec additions |

### Track E verifier

| Commit | Kind | Subject |
|---|---|---|
| `a3120f12` | P | Classify all four shapes of a worker death, not just the fatal one |
| `8fef026e` | T | Bump the UI submodule for the partial outcome badge |
| `168aa9bd` | P | Close the last uncovered early return before a job owns its log row |
| `207aa3c0` | P | Write a cancelled job's buffered failure records instead of dropping them |
| `c6a7a9ef` | P | Never record an unreachable endpoint as an HTTP/1.1 one |
| `6dcd82e4` | P | Gate inference concurrency in both transports, not just h2c |
| `6a8fb930` | P | Stop a late progress update reopening a job that recorded its ending |
| `618fc76c` | D | Say what the failures endpoint's filters and shortfalls actually mean |
| `299c3c65` | P | Stamp a job failure when it happens, not when the batch is written |
| `1e6b80ea` | P | Re-queue the tail of a died-on window, not just what the fatal arm failed |
| `8d352b33` | P | Downgrade an endpoint to HTTP/1.1 only on evidence the peer refused h2 |
| `232f523d` | P | Let a stale h2c memo be cleared by every call, not only by predict |
| `ec98f181` | T | Regenerate the spec for the corrected failure-audit descriptions |
| `793f40a7` | T | Bump the UI submodule for the job outcome column and the spec regen |
| `c6761368` | D | Code map: refresh Track E's line numbers and the claims that moved |

### Track P — protocol doc, R3 worker half, R5 worker half, R7 canvas, R8 context, R11 sentinel

| Commit | Kind | Subject |
|---|---|---|
| `793f869a` | D | Document run2's new worker wire fields and the unstated dtype sentinel |
| `fffbc947` | P | Classify a batch failure as OOM structurally and report why |
| `786fd3b1` | P | Report the clamp's pre-batch free reading on every measurement |
| `84663c27` | P | Price each pixel item at the model's canvas, not its raw size |
| `daacf07d` | P | Measure this process's accelerator context instead of assuming 500 MiB |
| `649125b9` | P | Rename the dtype sentinel from unknown to unstated |
| `a0d5bdf4` | D | Code map: record Track P's worker and registry changes |

### Track P verifier

| Commit | Kind | Subject |
|---|---|---|
| `5ecba825` | P | Keep every device wording of out of memory an OOM, not just the listed ones |
| `f37afe62` | D | Protocol changelog: five new keys, and base_method's new value is a change too |
| `b34c9ec5` | P | Context probe: keep the baseline fresh and read the pool before the free memory |
| `344178eb` | P | Collect the context probe when a load fails instead of leaving it polling |
| `5dedc5e4` | P | Keep the pre-batch reading on a measurement whose allocator query failed |
| `baf3f0f2` | D | cost.rs: unwrap the canvas log lines that lost their line continuations |
| `fb202822` | P | Keep the worker unit tests off the machine's real GPU under a full-suite run |
| `75bc41e9` | D | Code map: the verifier's classifier and context-probe corrections |

### R12 — the nemotron base check, and the instruments behind it

| Commit | Kind | Subject |
|---|---|---|
| `03bb8a4d` | T | vramrec: never memoise a half-read worker identity |
| `90409b41` | T | analyze: recognise worker PIDs from the gateway's spawn lines |
| `7b3ea31e` | T | analyze: base_accuracy rejects a PID older than the replica's spawn |
| `0643942c` | T | analyze: read base from the load-to-first-work window, not one sample |
| `e31fbd4f` | T | calibration README: document base_accuracy's attribution and cadence rules |
| `7a4d38fe` | D | run1 report: retract F-C, it was a recorder attribution artefact |
| `6665b931` | T | vramrec: bound the identity retry by wall clock, not sample count |
| `832474b0` | T | analyze: base_accuracy picks the replica's own PID and closes its window |
| `71cb0827` | T | loadgen: add --prewarm-only/--hold, the S2-base idle plateau leg |
| `e9e44549` | D | protocol: add the S2-base resident-idle plateau leg and its runbook |

### Cross-track follow-up

| Commit | Kind | Subject |
|---|---|---|
| `ee014235` | P | Wire the per-item pixel canvas end to end, host pricing included |
| `9229edc5` | P | Type the unattempted request, and classify it by type not by wording |
| `893e8a7d` | T | Let the ceiling probe call the worker's OOM classifier instead of a substring |
| `9be09e5e` | T | Price the ceiling probe's pixels at the canvas the ledger prices them at |

### Integration pass

| Commit | Kind | Subject |
|---|---|---|
| `18cc2e77` | T | Make the ceiling probe price with the canvas it resolves, not without it |
| `56aec556` | P | Give the ROCm inventory helper back the doc comment R7's test took |
| `022b4210` | D | Say in the store's README that a canvas change needs an epoch bump |
| `65fd2f82` | D | Code map: re-resolve every line and symbol reference at the run2 tip |
| `9bb7531f` | D | Plan: record the run2 implementation status and the legs' open items |

### The legs' own tool commits

| Commit | Kind | Subject |
|---|---|---|
| `cf1939e6` | T | Attribute a replica to the pid its spawn line names |
| `a39ab8d8` | T | C1: add an endpoint-scoped policy so h2c self-calls are not 403 |
| `fc4e1e9e` | T | Fixtures: an extraction job does drive them, correct the note |
| `2779a81e` | T | C7: add the endpoint-scoped policy so h2c self-calls are not 403 |
| `ea6f94f9` | T | C7 registry: restate easyOCR's canvas and epoch, which override drops |

### Post-Phase-A fixes

| Commit | Kind | Subject |
|---|---|---|
| `74ca202c` | P | Resolve the policy host from the request authority, not just Host |
| `4c2e00b6` | P | Judge the Desktop same-origin guard by the request authority too |
| `9cbd6304` | P | Fit a knee only where the curve bends and stays flat above it |
| `d230d5ba` | D | Write up the knee's R1e rules and the provisional seeded knee |
| `19e2bf9f` | D | Code map: re-resolve the knee's symbols and lines after R1e |
| `ce03bd0d` | D | Say in the knee's prose what the code does: one candidate, vetoes |

The P2 fix and the S1 fix are **not** in this table: they were still being
written when it was compiled. `<!-- PENDING: the P2 and S1 fix commits, the
rebuilt binary's commit and image id, and the commits of Phases B, D and E -->`
