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
have not run carry a `<!-- BLOCKED: … -->` marker, state the expectation the
legs plan sets for them and give the runbook to run them cold, so they can be
filled in without re-deriving anything. As of this revision that is **the rest
of Phase B (S4b, S4d, S4g), Phase D and Phase E** — all three **blocked, not
failed**: the GPUs are no longer free (§1, §12). Everything else — the
implementation, Phases A, C and D1, the ground-truth probes, the whole fix
round, **Phase A′ and S4a** — is measured.

Acronyms expanded once, beyond run1's: h2c = HTTP/2 cleartext (prior
knowledge); MAD = median absolute deviation; RFC = the IETF standard of that
number; VLM = vision-language model.

---

## 1. Verdict and state

**The run2 change set is implemented, verified and integrated — and so is the
fix round the first legs forced: 157 commits, `0d6b36c5` → the tip, four tracks
each reviewed by a separate verifier, a cross-track follow-up, two integration
passes with the full suites green, a release binary at `34a591aa`
and a `panoptikon:calib-cuda` image (`0b2261f94c8f`) rebuilt from a clean
worktree. Six leg groups have run: Phase A, Phase C, the Phase D1 easyOCR leg
(eleven legs), the ground-truth ceiling probes, **Phase A′ and S4a**. Between them they found
eleven defects — P1, P2, F1, S1/S1b, C2, C4, C5, D1-b, easyOCR's int32 detector
ceiling, the ledger's missing shape ceiling and an untyped client-side
transport failure — and **every one is fixed and signed off by a separate
verifier** (§5). Phase C met every stated expectation and closed five run1
findings outright; Phase D1 met every stated expectation for its leg; the
probes retracted run1's two headline slope disagreements as measurement
artefacts. **Phase A′ (the S2/S3 wd-vit re-run) and Phase B's S4a have now run
on the rebuilt binary `34a591aa`, without the `calib_hostless` workaround, and
they PASS on every check** — P1, P2, F1, S1 and R5 are confirmed on the wire by
a job rather than by a test. The remaining GPU legs — S4b, S4d, S4g, Phase D
and Phase E — are **blocked, not failed**: the GPUs were taken back at
21:59 UTC (below).**

### Phase A′ and S4a on `34a591aa`, and why the rest stopped

**Phase A′ is a clean pass on every check** (§4.6), on the unmodified
`server-C1.toml`:

- **No knee is ever fitted** on wd-vit — `fitted a throughput knee` appears
  **0** times in either leg, the store carries no `knee_units` and
  `/api/inference/metadata` answers `"knee_units": null` — with the estimator
  stating its refusal in words (*"the plateau starts at the smallest batch size
  measured, so no bend was observed"*). **F1 closed by measurement.**
- **The ramp is free again**: anchor **136 → 439** in 12 windows, monotone
  (1, 2, 4, 8, 16, 17, 34, 68, 136, 272, 439), budget published **878**, and the
  bound at the end is the 2 000-item corpus, not the transport. The gate
  **follows the published figure** (256 → 3 264, tracking
  `desired_in_flight_items` 48 → 3 264) instead of a hidden 200, peak in-flight
  **1 382**, and the sockets stay bounded: **44 established = 2 × 22 lanes** in
  use, of a 64-lane pool. **S1 closed by measurement.**
- **P1 and P2 are confirmed fixed on the unmodified config**: three job-driven
  legs created their jobs on the shipped policy shape (`EXTRACTION_POST_RC=0`,
  **zero** `reason="no_policy"` lines in 6 300 log events) and lost **0 of
  6 000** predicts — `invalid multipart` and `request_incomplete` are 0 each,
  every job `completed`, `failures.json` empty.
- **S3 utilization 0.01 → 0.69** across the restart: the seeded profile resumes
  at `ramp_step=6`, three grants (1, 63, 878) carry the whole job, and no
  spurious knee comes back with the seed.
- **S4a confirms R5's capped reserve** under an 85.8 GB neighbour:
  `reserve_rule = "capped_default"`, `reserve_mb` **exactly 1 024** where run1
  withheld **8 579**, `limit_mb` **11 071** where run1 had **3 517**, and the
  executed batch is **83 items where run1 ran 11**.

Two findings came out of it, neither a regression and neither a product defect:

1. **The worker's live clamp now double-counts our own allocator pool.** On the
   pressured GPU the clamp — not the ledger's margin — is the binding
   constraint, and it compares NVML's free reading *after* our caching allocator
   took its pool against a grant the ledger already charged us for: `free memory
   fell to 4371 MiB against a 7729 MiB grant; shrinking this batch's budget from
   147 to 83 units`, twenty-two times, **44 % of the grant left unused with
   nothing external moving**. This is run1's T10 with a number. Safe, known, and
   a decision rather than a fix (§6).
2. **S3 throughput is 0.88× master, at exact run1 parity** (run1: 0.89×, the
   same FAIL for the same reason — wd-vit's curve is flat, so one 1 936-item
   window pipelines slightly worse than many small ones). The old binary's
   1.09× was *caused by* the spurious knee holding it to 7–31-unit windows.

**Why the rest is blocked.** At **21:59 UTC on 2026-09-04**, between the S4a
leg and the S4d leg, **the user recreated SGLang** (`dsv4-flash-sglang`,
**77 702 MiB on both GPUs**; the user has been logged in since 20:09 UTC and
the SGLang bench directory was modified at 22:02; there is no timer or cron
behind it). It was not the run: both GPUs read 2 MiB before every leg and
after S4a. The orchestrator therefore **did not stop it again** — it is the
user's service and the user's decision — so S4b (whose defining +30 GB step is
arithmetically impossible with ~20 GB free), S4d (whose criterion is the
recovery to a 95 GB-free GPU), S4g, Phase D and Phase E are **blocked until
the GPUs are idle, not failed**. §4.5, §4.7 and §4.8 carry the exact commands, configs
and checks to finish each of them cold.

### What the legs have closed

| Run1 finding | Run1 number | Run2 number | Leg |
|---|---|---|---|
| **Q1 / B11** OOM classifier deflates on prose | **15** negatives `reason="oom"` on a GPU with 96 356 MiB free | **0** negatives; 25 of 25 fallbacks `oom=false`; `deflation=0` throughout | `S5-failbatch-oomtext` |
| **Q2 / B8** deflation uncapped | **6 589** levels in 120 s (54.9/s); a 2-min fault costs 15.6 min at 0.43× | capped at **4** = `ceil(log2 8)+1`; repaid 4 → 0 in **0.5 s** on clean windows and 4 → 0 over **120 s** idle, one level per 30.0 s | `S5-oomtimed`, `S5-oomtimed-idle` |
| **Q5 / B15** no load backoff | **93** load attempts in 182 s, one per request, HTTP 500 | **7** attempts, ladder 2/4/8/16/32/64/128 s, **13 145 × 503** with `retry-after` and `detail.kind = "load_cooldown"`, `/health.load_cooldowns[]` | `S5-dieonload` |
| **P5-3 / B18** global load lock | p50 **11 886 ms**, max 11 894 ms = 28.3× p50 = 100.2 % of the load | p50 **551 ms**, max **951 ms** during a 13.388 s load = **1.86× p50**, 7.1 % of the load | `S6-b18-loadstall` |
| **P5-5** collapse fires on contention | **3** MiniLM `throughput_collapse` negatives | **0** of 2 610 settles | `S6-contend` |
| **P5-4 / F-A** the knee pins a contended model | knees **31 / 15 / 4 095** fitted 20 s in, held 10 min | no knee on wd-vit or MobileCLIP; MiniLM's 16 383 **withdrawn 18 s later**; the store has **zero** `knee_units` | `S6-contend` |
| **F7 / Q8 / T8** death blast radius, empty failures endpoint | one death failed 1 542 items; `/api/jobs/data/failures` `{"total":0}` in every leg; `end_time == start_time` | **2 000** re-queues, `job_failures_total: 2000` with real `occurred_at`, job `failed` with `failed_items: 2000`, `13:59:50 → 14:02:07` | `S5-dying-job` |
| **F-C** nemotron base 4.5× | 3 788 vs "848 MiB" | retracted in run1's report; **3 788 vs 3 788 = 0.0 %** on a judged 373-sample plateau | `S2-base`, R12 |
| **base_accuracy** never judged | 42 of 60 legs INFO | **judged and PASS**: 0.0 / 0.0 / **4.47** / 0.0 % (wd-vit / MobileCLIP / MiniLM / nemotron) | `S2-base` |
| **N4 post-unload phantom** (re-confirmed) | S2 `oracle_agreement` FAIL, 81 of 205 | **PASS, 137 MiB worst, 0 of 410** | `S2-wdvit` |
| **W1 / Q3** the fitted pixel slope moves with the corpus | nemotron fitted **4.33×** the probe's slope | a **group mismatch**, not a defect: run1's fit and run1's probe were measured on different image groups. Against its own group run1's fit is **0.907×**, and run2's easyOCR fit matches its group probe **to seventeen digits** | `probes/` |
| **F-B** easyOCR grants 23–94 GB against 1 986 MiB of oracle-free memory | 6 `grant_safety` breaches in the soak | `grant_safety` **PASS on all eight ledger legs** (14–205 grants), 0 over the priced headroom, 0 over live free, 0 memory-blind. The *never-learning* half is untouched on the shipped registry (D1-d) | `S8-ocr-C7*` |

### What run2 broke, and where each stands

Every row is now closed. "Verified" means a separate agent reviewed the fix,
re-derived its evidence and signed it off; §5 gives each one in full.

| Id | Sev | What it does | Status |
|---|---|---|---|
| **P1** | **BLOCKER** | R10' made the gateway an h2c client of its own API; `policy::resolve_effective_host` read only the `Host` header, which HTTP/2 does not send, so every self-call was refused **403 `no_policy`** and `POST /api/jobs/data/extraction` answered **500**. No shipped-shape configuration works around it (a policy with neither `hosts` nor `endpoints` is rejected at load; `hosts = ["*"]` does not match a hostless request) | **Fixed and verified** (`74ca202c` + Desktop parity `4c2e00b6`; policy 42 tests, desktop 11). The `calib_hostless` workaround has been removed from every calibration config (`ea59a63b`), so **the next job-driven leg is the test** |
| **P2** | HIGH | One predict in ~8 000 items failed **400 `invalid multipart body`** from the gateway to itself on the h2c path; the item was lost (`requeued: false`) and the job reported `partial`. It recurred in Phase D1 — 4 items in 11 legs — and only on the legs sending 28–85-item multipart bodies | **Fixed and verified** (`7e96de62`, bound `79488c92`). Root cause reproduced verbatim: **381 / 300 032** before, **0 / 300 032** after |
| **F1** | HIGH | R1's knee estimator still fitted a spurious small knee on a flat curve: wd-vit `knee_units = 3` at 14 observations, persisted as 7, and the stored knee then held a **fresh** 2 000-item job at 7–31 units for its whole length (`utilization` 0.01 against run1's 0.80). The expiry lost the race — widen at 12 clean windows, refit within ~1 s | **Fixed and verified** (`9cbd6304` + five verifier commits). Independent replay: **no knee at any point** on either wd-vit leg, from a second implementation of the estimator |
| **S1** | HIGH | The job could never put more than **200** predicts on the wire: `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200`, which `axum::serve` never overrides, over a `hyper-util` pool that shares **one** h2 connection per host — so `INFERENCE_POOL_CONNECTIONS = 4` was really 1, window formation degenerated into `W → 200 − W` and `max_units_measured` froze at 136 while the orchestrator asked for **1 632** | **Fixed and verified** (`4e587635`…`085e9cd7`, plus seven verifier commits). The 200 is now a test assertion; the client offers 400 and 256 land, over **4** sockets |
| **S1b** | MED | A `UnitBudget` **shrink can never land in a saturated job**: `drain_shrink` calls `Semaphore::forget_permits`, which can only take *available* permits, and a released permit goes straight to one of ~1 800 waiting item tasks. Observed in-flight stayed at 200 for the whole post-knee phase instead of falling to 64 | **Fixed and verified** (`fae83107`): deficit accounting on the release path; 136 releases reach **0** waiters and the budget settles at exactly 64 |
| **D1-b** | MED | R7's canvas cap makes every item at or above the canvas price *identically*, which removes the size information `packing.plan_batches` sorts on; `eocr.py` then padded a mixed batch to the largest member's **raw** dimensions — the "uniform dims" cost `enable_batching = false` exists to avoid, and the property run1's S8 PASS rested on | **Fixed and verified** (`2b8499ce`, `44a7babf` + six verifier commits). The tiebreak restores homogeneity; the detector is bounded by the canvas before padding, and the verifier **reverted the quality half** so recognition still crops from the raw image (§5) |

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
- **The reserve cap does what R5 promised** on a squeezed GPU: `unit_budget
  = 60`, `mb = 3 040`, `reserve_mb = 1 024`, `reserve_rule = "capped_default"`
  where run1 issued `unit_budget = 1`, `mb = 0` (memory-blind).
- **The transport is now a thing the system can name.** `/health` reports the
  published desired-in-flight figure, the endpoint's transport, its lanes and
  connections in use, its gate, the queue-bound window count, the predict body
  budget and the shape ceiling — every number the S1 diagnosis had to
  reconstruct from log archaeology.
- **easyOCR is priced at the canvas, provably**: `S8-ocr-C7-calm-b`'s stored
  `sample_units` are exact multiples of 6 553 600 and the uncapped control's
  are not, and `grant_safety` PASSes on all eight ledger legs.

### What remains open and could still block a release

| Open item | Why it can still block | Worst measured number |
|---|---|---|
| **Three leg groups are blocked on an idle GPU** | Phase A′ and S4a ran on `34a591aa` and pass; **S4b, S4d, S4g, Phase D and Phase E have no measurement on it at all**, so the shape ceiling, the Docker socket count, pixmix's slope, the moving-GPU `external_mb` latency and the soak's `partial`-free claim are still verified by tests only. SGLang holds both GPUs | 3 of 8 planned legs on the rebuilt binary; 77 702 MiB per GPU |
| **The worker's live clamp double-counts our own pool** | On a pressured GPU the clamp is now the binding constraint and prices our own allocator pool twice — once as `charge_mb` in the grant, once as memory missing from the live free reading. It is safe by construction, and it costs throughput exactly where R5 was meant to buy it | **44 %** of the grant unused with nothing external moving (147 → 83 units, 22 times, S4a) |
| **C6** throughput under a hard squeeze | R5's win in *admission* is a loss in *throughput* in the same regime, and nothing in the ledger notices | wd-vit **0.56×** run1 at 4 GB free (21.85 vs 38.98 items/s), p50 5 323 vs 3 199 ms |
| **F1's residue** — MobileCLIP's knee is not restored | Under `KNEE_PLATEAU_BUCKETS = 2` the recorded MobileCLIP ring fits **no** knee where run2 fitted the correct 127. The knee is found late rather than lost, and it was worth nothing measurable (0.94× with it, 1.00× without in run1) — but the stated acceptance criterion is not met literally | one quiet bucket above the bend where the rule wants two |
| **nemotron's price spans 8.35× across one corpus** | Its memory is a function of the **tile grid the aspect ratio picks**, not of pixel count: 1 MP and 4 MP square images allocate byte-identically, and so do 0.3 MP and 20 MP 4:3 images. The canvas closed the top of the range (66× → 8.35× in capped units); nothing closes the bottom | 0.000153 … 0.001280 MiB/unit on the pixmix corpus |
| **P1 is still unexercised by a leg** | The fix is verified by tests, not by a job on the shipped policy shape — though the workaround is now gone, so the next job-driven leg either works or fails loudly | 0 legs so far |
| **The shape ceiling only ratchets downward inside one process** | A pessimistic report — a mixed batch whose one oversized page sets the padded frame — holds until the model is reloaded or the canvas/epoch moves. The cost is throughput, never a failure; climbing back out needs a knee-style expiry probe that does not exist | one over-wide window per reload is the only way back up |
| **The easyOCR fidelity trade was decided by the orchestrator** | A quality regression is a user-facing design change. The trade was taken on the user's behalf and then **reversed**, so the shipped behaviour is run1's; it is flagged here for confirmation rather than left implicit | at 30 px text on an 8000 px sheet the bounded arm returned garbage where the raw arm reads cleanly |
| **`unstated` has no producer** | R11's sentinel rename cannot be verified on this host: all four shipped models and every fixture resolve a dtype by inference | the string appears **0** times in five Phase A legs and ten Phase C legs, and the D1 legs' easyOCR replica reports `dtype fp32` |
| **`codemap.md` drift** | A mechanical check at the tip flagged **68** references whose target text had changed since `65fd2f82`; the map's whole value is that its references resolve. **A full re-resolution sweep landed as `4898d0a8`** while the A′/B legs ran, so this row is closed unless a later leg moves code again | 68 refs before the sweep, in `http.rs`, `worker.rs`, `dispatch.rs`, `manager.rs`, `calibration.rs`, `packing.py`, `utils.py` |
| **`ui` submodule commits unpushed** | `9b28044` **and** `8abf631` on `batch-calibration-ui` exist only in this host's clone, so the gitlink does not resolve for anyone else and every clean-worktree image build needs a local remote | 2 commits |

### Recommendation

1. **Finish S4b, S4d, S4g, Phase D and Phase E on `34a591aa` and nothing
   else**, once the GPUs are idle again — SGLang is the user's and is not to
   be stopped by the run. The fix round changed what every one of them
   measures; a number from `65fd2f82` is a number about a binary that will not
   ship. §4.5, §4.7 and §4.8 hold the commands.
2. **Phase A′ has answered the S1 acceptance test.** `queue_depth +
   last_window_items` no longer sums to 200, the 136/64 alternation is gone,
   `max_units_measured` reached **439**, and `/health`'s
   `desired_in_flight_items` and `inference_clients[].in_flight_requests`
   agree — 3 264 desired against a 1 382 peak in flight over 22 lanes.
3. **Treat Phase E as the P2 acceptance test.** No fixture is involved, so any
   `partial` job is P2 or its successor; a `request_incomplete` or `transport`
   failure must be re-queued once and reach `job_failures` only if it fails
   twice. P2 has already survived 6 000 predicts in Phase A′ / S4a.
4. **Decide the clamp's pool accounting** (§6): the worker knows its own
   `memory_reserved − memory_allocated` and can add it back before comparing
   against the grant, or the grant can be priced net of the pool. One of the
   two, and it is a user call.
5. **Decide C6.** It is the first measured cost of the R5 decision the user
   approved, and it is a throughput regression in exactly the regime R5 was
   aimed at. Options in §6.
6. **Decide `KNEE_PLATEAU_BUCKETS`.** 2 refuses wd-vit's spurious knee *and*
   MobileCLIP's real one; 1 restores MobileCLIP and still refuses wd-vit (by
   rules 1 and 2). One named constant, one line.
7. **Confirm the easyOCR fidelity call**, which the orchestrator took on the
   user's behalf: the canvas bounds the *detector's batch tensor* only, and
   recognition still reads crops from the raw image, so OCR quality above
   2560 px is unchanged from run1. The cheaper alternative — bounding
   everything — was measured to destroy fine print for no memory saving.
8. **Decide nemotron's pricing.** Tile-based pricing (`tiles × 512²` rather
   than `min(raw, canvas)`) would be exact for the VLM class and is a design
   change; capped pixels are a 8.35× approximation of it.
9. **Push the `ui` submodule commits** `9b28044` and `8abf631` on
   `batch-calibration-ui` to `reasv/panoptikon-ui`.
10. **`codemap.md` is re-resolved** at the settled tip (`4898d0a8`); re-run the
   check if the blocked legs force any further code change.

---

## 2. What was run

### Phases, binaries and legs

| Phase | Content | Binary the legs ran on | Legs |
|---|---|---|---|
| Implementation | Four parallel tracks L, M, E, P; one verifier each; R12 analysis and its tool fix; cross-track follow-up; integration pass | n/a (debug `cargo test` only) | n/a |
| A Idle GPU, cold ramps | S2-base (new), S2 ×3, S3 (+ the `S3-wdvit-kcw` sub-leg) | **`65fd2f82`** (`panoptikon 0.1.8`, 86 397 864 B, built 12:41 UTC) | `S2-base`, `S2-wdvit`, `S2-wdvit-blocked-h2c403`, `S2-mobileclip`, `S2-minilm`, `S3-wdvit`, `S3-wdvit-kcw` |
| C Negatives, deaths, contention | S5 ×5 (+3 sub-legs), S6 contend, S6-b18 | **`65fd2f82`**, not rebuilt | `S5-failbatch-oomtext(-job)`, `S5-oomtimed(-idle)`, `S5-oom2nd`, `S5-dieonload(-job)`, `S5-dying-job`, `S6-contend`, `S6-b18-loadstall` |
| D1 S8 ocr-C7 | easyOCR host-side pricing at the 2560² canvas, eleven legs including a C7nc uncapped control, two master baselines and the shipped grantless registry | **`65fd2f82`** (master baselines on `7aa92b20`) | `S8-ocr-C7(-repeat,-cold,-calm,-calm-b)`, `S8-ocr-C7nc(-b,-aborted-registry)`, `S8-ocr-C1-grantless(-b)`, `S8-ocr-C0(-cold)` |
| Probes Ground truth | `ceiling_probe.py` against the impls directly: easyOCR under C7 and C7nc, nemotron on four image groups | no gateway — the probe drives the Python impl in-process | `probes/easyocr-C7`, `probes/easyocr-C7nc`, `probes/nemotron` |
| Fix round | Eleven defects fixed, each with a separate verifier; second integration pass; binary and image rebuilt | n/a (debug `cargo test`, `pytest`) | n/a |
| A′ Re-run | S2 wd-vit, S3 wd-vit on the fixed estimator and transport | **`34a591aa`** | `S2-wdvit-v2`, `S3-wdvit-v2` — **done, PASS** |
| B External pressure | S4a done; S4d, S4b, S4g stopped by the host state | **`34a591aa`** | `S4a-v2` — **done, PASS**; `<!-- BLOCKED: S4d, S4b, S4g — SGLang holds both gpus -->` |
| D Packing, docker | S8 pixmix, S11-C4, the easyOCR shape-ceiling leg | **`34a591aa`** / image `0b2261f94c8f` | `<!-- BLOCKED: phase D — needs idle gpus; ~93 GB free for the shape-ceiling leg -->` |
| E Soak | 4 h S9 recipe | **`34a591aa`** | `<!-- BLOCKED: phase E — needs both gpus for 4 h -->` |

Phase A, Phase C and Phase D1 all ran on `65fd2f82`, which contains the whole
R1–R12 change set and **none** of the eleven fixes. Phase C's report re-read
every source line it quotes from `git show 65fd2f82:…`, because three fix
agents were editing the tree while its legs ran; Phase D1's did the same.

The rebuilt artefacts are `target/release/panoptikon` at **`34a591aa`**
(86 313 672 B, built 21:28:07 UTC, `panoptikon 0.1.8` — `34a591aa` is the last
commit on the branch that touches code, so the binary is the tip's code) and
`panoptikon:calib-cuda` = **`0b2261f94c8f`** (9.43 GB, built 21:33:56 UTC from
a clean detached worktree in 353 s). The previous image is kept as
`panoptikon:calib-cuda-run2a` (`6fe5d86e3a1e`) beside run1's
`panoptikon:calib-cuda-run1` (`2a2c93ad6375`).

### Configurations

Run1's C0–C7 unchanged (`docs/batch-calibration-run1-report.md` §2). Two
run2 deviations, both in the protocol's own configs and both committed by
explicit path:

| Config | Deviation | Why |
|---|---|---|
| `server-C1.toml` (`a39ab8d8`), `server-C7.toml` (`2779a81e`), `server-C7nc.toml` (`c4d6fd3c`) | a `calib_hostless` policy — `ruleset = "allow_all"`, `[policies.match] endpoints = ["default"]` | the **P1** workaround; without it no job leg could run at all on `65fd2f82`. **Removed in `ea59a63b`**, once the fix was in the binary: each block is replaced by a one-line CALIB comment naming the defect and the two commits that fix it, and all five branch-derived configs now list exactly the two shipped policies (`test_endpoint`, `localhost`). Every leg from here on runs on the shipped policy shape |
| `registry-C7.toml` (`ea6f94f9`) | easyOCR's canvas and `epoch = 2` restated in the C7 override registry | the C7 registry override drops `metadata.cost` keys it does not restate — without this restatement C7 would silently have run **uncapped**, i.e. measured nothing |
| `server-C7nc.toml` / `registry-C7nc.toml` (`c4d6fd3c`, `0f8d04d7`) | **new**: C7 with the canvas key removed and `config.canvas_size = 40000`, ports moved to 6392/6393/6389 | the control that separates R7's per-item cap from the `enable_batching` flag. Both file headers say it is a **diagnostic, not a proposed configuration** (running easyOCR uncapped is F-B/W1). After the D1-b fix the registry key alone no longer frees it — the impl states its own canvas and the worker reports it — so the impl's canvas has to be raised too |

C0/C2/C3 and the C4/C5/C6 compose overlays were **never** touched, and the
`calib_hostless` blocks are gone, so from the rebuilt binary onwards there is
no configuration deviation left in the run.

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
| F1 fixed / verified | ~14:52 / 16:18 |
| S1 root-caused | ~16:01 |
| C2 fixed / verified | ~16:39 / 16:52 |
| Phase D1 (S8 ocr-C7, eleven legs) done | ~16:47 |
| P2 fixed | ~16:55 |
| Grant-line canvas | ~17:00 |
| D1-b fixed / verified | ~17:07 / 17:36 |
| Ground-truth probes done | ~18:49 |
| S1 / S1b / C4 / C5 / health fixed | ~19:06 |
| easyOCR int32 ceiling fixed / verified | ~19:14 / 19:32 |
| Combined verifier (P2 + S1 + grant log + `clamped.reason`) done | ~20:25 |
| Ledger shape ceiling done | ~20:52 |
| Typed transport failure done | ~21:14 |
| Second integration pass; binary `34a591aa`, image `0b2261f94c8f` | 21:28 / 21:34 |
| Phase A′: `S2-wdvit-v2` 21:44:26→21:46:38, `S3-wdvit-v2` 21:50:29→21:53:07 | 21:44–21:53 |
| Phase B: `S4a-v2` 21:55:36→21:57:57 | 21:56–21:58 |
| **SGLang recreated by the user** (77 702 MiB on both GPUs); S4d/S4b/S4g, Phase D and Phase E blocked | **21:59** |

(Times are when each agent's report landed in the orchestrator's scratchpad.)

Three session rate limits interrupted the run (10:40, 15:40 and 20:40 UTC
resets). No commits were lost. The E and L verifiers were relaunched with
resume briefs after the first; P2, the F1 verifier, the S1 analysis and the ocr
leg after the second; the S1 fix (which had produced no commits) and the D1-b
verifier after the third — the S1 fix was then re-briefed to commit per item
and write its report incrementally, which is why its eight commits each carry
their own section of evidence.

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
| **R1d** expiry (brake, not ceiling) | `knee_clean_windows`, `knee_re_explore_above`; a window counts only if it responded clean, the **knee was binding**, it **carried enough work to reach the cap** and the GPU had **ample headroom** (`headroom ≥ RATCHET_FACTOR × appetite`). `KNEE_EXPIRY_CLEAN_WINDOWS = MIN_KNEE_SAMPLES = 12` — twelve honest observations buy a cap, so twelve clean windows at it buy a re-test. Withdrawal when the widened knee reaches `uncapped_units` | `f161850f`, withdrawal arm `29f5636b` | approved; the verifier judged the `anchor == 0` withdrawal form **better than either option the brief named** | **works**: 10 widenings across the wd-vit legs, every one at exactly `clean_windows_at_the_knee=12`, every one one log2 bucket. **`knee_withdrawn` first exercised in Phase C** (`S6-contend`, MiniLM, withdrawn 18 s after it was fitted, never came back) |
| **R1e** (new, from F1) | One candidate — the smallest quiet bucket on the plateau — and **five vetoes**: frontier must be quiet, floor must be interior, `KNEE_PLATEAU_BUCKETS = 2` quiet buckets above, no ramp-era knee below the anchor, post-widening evidence judged by a per-(model, GPU) `seq`. Plus: a replica's first settled window teaches the knee nothing, and a **seeded** knee is provisional for `KNEE_SEED_REVALIDATION_WINDOWS` | `9cbd6304`, `d230d5ba`, `19e2bf9f`, `ce03bd0d` | **FIXED** — one real defect (rule 4 read the *live* anchor, which a unified-memory-device death halves; `36a8cb77`), plus `846806f0`, `9a507c82`, `493c7f0b`, `f0c74915` | replay against the recordings: **no knee at any point** on `S2-wdvit` (218 obs) and `S3-wdvit` (205 obs), reproduced independently by the verifier's own transcription of the estimator, verdict census matching to the unit; MiniLM still refused by variance; **MobileCLIP's 127 not restored** on its ring (§6) |
| **R2a** re-queue + `partial` | A died-on window's in-flight items are re-queued **once**; `data_log.outcome ∈ {'', completed, partial, failed, cancelled}` (`''` renders `running`, so no backfill); every terminal path writes it, including the early-return and cancellation paths (the T8 fix) | `521cca81` | nine defects across two rounds, incl. the death signal matching one rendering of six and the scan-history column still reading "Completed" for a `partial` job — `a3120f12`, `8fef026e`, `168aa9bd`, `207aa3c0`, `c6a7a9ef`, `6dcd82e4`, `6a8fb930`, `299c3c65`, `1e6b80ea`, `793f40a7` | **`S5-dying-job`: 2 000 re-queue lines + `requeued=2000`**; outcome `failed` (Systemic — *every* item died, so `partial` is unreachable there), `end_time != start_time`, 63 spawns / 63 deaths for 2 000 items. **`partial` evidenced in Phase A** (`S2-wdvit`, 1 owed item from P2) |
| **R2b** failures endpoint | `GET /api/jobs/data/failures` keeps `total`/`failures` and gains `job_failures_total`/`job_failures[]` (`JobItemFailure`) and `failed_jobs_total`/`failed_jobs[]` (`FailedJobRecord`); `error_class`/`mime_prefix` answer with the job-side lists empty rather than an unfiltered approximation | `22256f8c`, `fed96ea4`, `618fc76c`, `232f523d`, `ec98f181` | as above | **`job_failures_total: 2000`**, each row with path, sha256, mime, setter, `stage: "inference"`, the `[worker_died]` error, `requeued: true` and a real `occurred_at` (run1: `{"total":0}` in every leg) |
| **R3** structural OOM | Worker: `packing.classify_oom` — three tiers in **strength order over the exception chain**: `typed_exception` (`torch.OutOfMemoryError` via `sys.modules`, `MemoryError`), `marker` (`InferenceOOMError`, `INFERENCE_OOM*`), `message_pattern` (a **closed** list of nine driver-shaped strings plus one two-part CPU-allocator pattern; a bare `out of memory` is deliberately absent). Every classification stamps `free_mb_at_failure` and `device`. Host: trust typed/marker, **veto `message_pattern` when the worker's own live free reading at the failure was at least the whole envelope the window was priced at** | `fffbc947` (worker), `6afed305`, `1031504c`, `5ecba825` (host + device wordings) | six defects in P (the closed list had lost real device wordings); the host half was added in L's verifier pass | **B11 closed**: 0 negatives from prose. **The veto fires** (`S5-oom2nd`, `contradicted=1 free_mb_at_failure=96518 grant_mb=96356`) — see finding **C3** |
| **R4** deflation | `deflation_cap(anchor, seed) = ceil(log2(max(anchor, seed, 1))) + 1`; time repayment `DEFLATION_REPAY_SECS = TRIM_DEBOUNCE = 30 s`, one level per whole interval, advancing the stamp by the intervals consumed; repayment runs where the counter is *read*, not on a timer; clear-on-respawn holds by construction and is now pinned by a test | `83acdbdc` | the ladder's 33rd-consecutive-failure shift overflow fixed by M's verifier (`d8721f90`) | **cap 4, time repayment 4 → 0 over 120 s idle, one DEBUG line per 30.0 s** |
| **R5** margin | `reserve = min(ceil(external × margin), 1024)` when the margin is unset, `ceil(external × margin)` when the user set one; `limit = min(total × cap_fraction, total − external − reserve)`. `margin` became `Option<f64>` so absence tracks the code default. `/health` gains `reserve_mb`, `reserve_rule` (`user_margin` \| `capped_default`); the grant line carries both | `9a10bfe5`, config examples `391b66cb`, `6e515174` | approved; **the widening stays capped** (reasoned, §7) | `/health`: `"reserve_mb": 67, "reserve_rule": "capped_default"`. **On a 4 GB-free GPU: `unit_budget=60 mb=3040 reserve_mb=1024`** where run1 gave `unit_budget=1 mb=0`; **3** memory-blind grants of 2 610 against run1's 113; **0** samples at `limit_mb = 0` against run1's 367. Cost: finding **C6** |
| **R5** per-batch free (T3) | Every measurement carries `free_mb`/`free_source` (the reading the worker's defensive clamp already takes before that batch) and the host ingests them in sequence order **before** the negative/collapse checks; the response-level sample is applied after, and `record_telemetry` was reordered so capture instants follow the order the worker took the readings | `786fd3b1` (worker), `9a10bfe5` (host) | approved; P's verifier fixed a stale baseline and a load that left the probe polling | `<!-- BLOCKED: phase B --> S4b still decides it — blocked on an idle gpu: expect `external_mb` to track a hog step within a few seconds, not run1's 31.5 s` |
| **R6** load locks | Per-model load lock + a **per-GPU** admission gate; the global lock retired. Four-lock order (barrier > per-model > per-device admission > state) with a no-deadlock proof in the module docs. `max_concurrent_loads` default 1; every unpinned model resolves to the same default GPU, so the shipped configuration behaves exactly as before | `e6e510d0`, `3ad22c92` | two holes closed: the `""` unresolvable-pin bucket now waits on **every** GPU's permit (`702ea8ac`); the ladder no longer panics (`d8721f90`) | **B18 closed**: 1.86× p50, 7.1 % of a 13.388 s load (run1: 28.3×, 100.2 %) |
| **R9** load cooldown | `window(n) = min(base × 2^(n−1), max)`, shipped 2 s → 300 s; only `costed_worker` failures arm it (spawn/handshake/configure/load/death-while-streaming), never registry-phase rejections; `Retry-After` never below 1; `/health.load_cooldowns[]` top-level (a cooling model is by construction not in `models[]`); refusal checked **before** the `failed to load model` and `worker_died` arms | `e2679f62` | approved; scope line ("a death *after* a successful load is not counted") judged correct | **7 attempts / ladder 2,4,8,16,32,64,128 / 13 145 × 503 / job aborts in 9.5 s after 2 attempts** (run1: 93 attempts, 500s, 259 s). Residue: finding **C4** |
| **R7** pixel canvas | `metadata.cost.canvas_pixels` on the registry, resolved into `CostDimension` under the same two rules as `seed_units` (pixel-only, never inherited across a unit change); the worker prices `min(raw_pixels, canvas)` including its unreadable-input fallback; three resolution tiers (grant > impl introspection, floored at 512² > uncapped) | `84663c27` (+ follow-up `ee014235`) | approved; the follow-up was needed because **R7 was inert without it** | **`canvas_pixels=1835008` in force on nemotron** with `epoch=2 seed_units=2000000` (`S6-b18-loadstall`), and **proven from the stored windows on easyOCR**: `S8-ocr-C7-calm-b`'s `sample_units` hold 5 ×, 9 × and 11 × 6 553 600 where the uncapped control's hold none. Cost: **D1-b** (§5), and the canvas is only true if the impl honours it. `<!-- BLOCKED: phase D --> S8 pixmix still decides the slope` |
| **R7** wiring (follow-up) | One resolution site (`manager::canvas_in_force`, **registry wins**, load report fills in only where the registry declares nothing), `Grant.canvas_pixels` on the wire as `u32 \| nil`, `dispatch::estimate_input_units` capping the **host's** window pricing at the same figure — which for the three grantless `easyocr_*` ids is the only cap there is — and a new `canvas_pixels` key on the `load ok` response | `ee014235`, probe `893e8a7d`, `9be09e5e`, `18cc2e77` | integration review found two defects, both fixed (`18cc2e77`, `56aec556`) | as above |
| **R8** measured context | `_ContextProbe` measures the GPU free-memory delta across this process's **first CUDA initialisation** and subtracts the allocator pool at that instant. A **watcher, not a call**: a daemon thread polls `torch.cuda.is_initialized()` every 5 ms rather than initialising CUDA in a process that was never going to touch a GPU | `daacf07d` | three defects fixed (`f37afe62`, `b34c9ec5`, `344178eb`, `5dedc5e4`) | **not reached on this host**: `base_method = "nvml"` everywhere, exactly as run1's W4 predicted. Exercised by unit tests only |
| **R10′** h2c pool | `Transport::{H2c, Http11}`; **one `EndpointRuntime` per base URL shared by every client** (a pool that is not shared is not a bound); `INFERENCE_POOL_CONNECTIONS = 4`, `H2_STREAMS_PER_CONNECTION = 64`, gate `INFERENCE_MAX_CONCURRENT_REQUESTS = 256`, h2c arm only; probe by `GET /cache` with prior knowledge where **any** HTTP status proves h2c; re-probe on connect/request errors. Descriptor clamp reworked: `Multiplexed` costs a constant 8 descriptors, `PerRequest` unchanged | `ddd44e4a`, `b2723f16` | a mid-stream reset permanently downgrading an endpoint to HTTP/1.1 fixed (`8d352b33`); `c6a7a9ef` never records an unreachable endpoint as HTTP/1.1; gate constant approved in both transports (`6dcd82e4`) | **the three defects of the run**: **P1** (403 on every self-call), **P2** (a handler that answered with the request stream open) and **S1** (an unnamed 200-stream ceiling). All three fixed in the fix round (§5): 64 real connection lanes built on recruitment, a server stream limit this process chooses, a gate that follows the published figure, and a typed transport failure that costs no item. Sockets themselves behaved: **Phase A′ measured them on the bare host**: peak **44 established sockets = 2 × 22 lanes** on S2, 65 = 2 × 32 + 1 on S3, at 90–111 fds of a 524 288 limit. `<!-- BLOCKED: phase D --> S11-C4 still measures the in-container descriptor count` |
| **R11** small choices | `dtype_method` stored in the profile; sentinel `unknown` → **`unstated`** (constants renamed too, no back-compat alias, so the profile key moves and rows re-measure); no `pid: host`; UI types regenerated | `a1fb91a9`, `649125b9`, `f506dc2b`, `049d271c` | approved; `b0cc2c95` spells the sentinel in the load-report doc comments | **`dtype_method = "inferred"` in all three S2 stores** (`dtype` fp32/fp16/fp32). **`unstated` has no producer on this host** — the string appears 0 times in 15 legs |
| **R12** nemotron base | F-C is an **analysis artefact**: `vramrec.py` had memoised `[panoptikon-spaw]`/empty env for the nemotron pid, so `analyze.py` compared it against the **MiniLM** worker's 848 MiB. Real base 3 788 = 3 201 MiB bf16 weights + allocator slack + ~550 MiB context. Recorder and `analyze.py` fixed; F-C retracted in run1's report; the **S2-base** plateau leg added to the protocol | `03bb8a4d`, `90409b41`, `7b3ea31e`, `0643942c`, `e31fbd4f`, `7a4d38fe` | `6665b931`, `832474b0`, `71cb0827`, `e9e44549` — the identity retry rebounded by wall clock, `base_accuracy` now picks the replica's own pid and closes its window | **0.0 % on nemotron over 373 samples**; `grep -c panoptikon-spaw vramrec.jsonl` = **0** |

**Integration pass** at `65fd2f82`: `cargo test -p panoptikon` **1 586 passed,
2 failed** (both known host artefacts that also fail on master), `pytest tests`
**302 passed, 13 skipped**, `cargo fmt --check` clean, clippy 8 pre-existing
warnings none in a run2 file, `cargo test --bin panoptikon openapi` 5 passed,
and the `ui` gitlink's types byte-identical to a fresh `openapi-typescript`
run.

**Second integration pass** at `34a591aa`, after the fix round: `cargo test -p
panoptikon` **1 648 passed, 1 failed, 10 ignored** — the one failure is the
known host artefact (`db::batch_auto::tests::an_unwritable_config_warns_
stamps_and_is_left_intact`: this host lets the owner write a file whose
permissions were just set read-only), and the `media_tools::transcode`
ffmpeg-budget tests passed this time; `pytest tests` **366 passed, 14
skipped**; `cargo fmt --check` clean; clippy **8** warnings, the same
pre-existing set and none in a file run2 touched; `openapi` 5 passed. The `ui`
types were regenerated against the moved spec (**+164 / −1** lines: the new
`InferenceTransportHealth` and `PredictBodyBudgetHealth` schemas,
`InferenceHealth.inference_clients` / `.predict_body_budget`,
`CostHealth.canvas_pixels`, `LedgerWorkerHealth.shape_ceiling_units`),
`tsc --noEmit` clean, submodule commit `8abf631`, gitlink `0998e6ca`.

---

## 4. What the legs measured

### 4.1 Phase A — idle GPU, cold ramps (`run2-phaseA-report.md`)

Binary `65fd2f82`. SGLang stopped, both GPUs at 2 MiB before every leg and
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
| OOM negatives from a non-memory error whose text says "out of memory" | **15 of 51 windows** on a GPU with 96 356 MiB free | **0** | **0.** 26 windows, all `outcome="clean"` | **PASS** |
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
| no synthetic negative on a discrete GPU | 0 | 0 | **0** `unified_device_death`, `deflation=0` | **PASS** |

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

### 4.3 Phase D1 — S8 ocr-C7, easyOCR at the canvas (`run2-phaseD1-report.md`)

Binary `65fd2f82`, master baselines on the untouched `panoptikon-master`
worktree at `7aa92b20`. **Eleven legs** over the same 400-image corpus (460
segments), because the leg's own expectations forced controls the brief did not
name: a C7nc arm with the canvas key removed, a shipped-registry arm (the
"grantless" configuration the brief's parenthetical actually describes), and
two master baselines. Primary record:
`results/run2/S8-ocr-C7/runlog.md`.

| Clause | Run1 | Run2 expectation | Run2 measurement | Verdict |
|---|---|---|---|---|
| Host-side pricing at the 2560² canvas | raw header pixels, epoch 1: an 8000×6000 sheet cost 48 000 000 units | every item at `min(raw, 6 553 600)` | **proven from the stored windows**: `S8-ocr-C7-calm-b`'s `sample_units` hold **32 768 000 = 5 ×**, **58 982 400 = 9 ×**, **72 089 600 = 11 ×** 6 553 600; `S8-ocr-C7-repeat`'s hold 7 ×, 4 ×, 3 ×. The uncapped control `S8-ocr-C7nc-b` has **no** multiple of it (its samples are multiples of 2 174 960, the raw page) | **PASS** |
| `canvas_pixels` on the load report and the grant | field did not exist | present | load report **yes** (`pricing each input at min(raw pixels, 6553600), the canvas the registry states`), grant **frame** yes; the ledger's `issued a memory grant` **log line** did not carry it, and `/health` did not | **PASS on the wire; MISS on the log line** — fixed in the round (`2e5c782b`, `feb0a5a9`) |
| Grants priced by `slope × units` | same | same | first five grants `pre_fit=true` = the whole headroom (96 282 → 51 501 MiB); from the sixth `mb = 46 992 = 796 + 0.0014796 × 31 758 989`. Ladder identical to run1's, grant for grant | **PASS** |
| **No 23–94 GB grants (F-B)** | soak: 6 breaches, 23–94 GB against as little as 1 986 MiB oracle-free | none | grants of 46 992–96 282 MiB *were* issued — and on an idle GPU that is the real free memory: `grant_safety` **PASS on every leg**, 0 over the priced headroom **or** over the oracle's live free reading, 0 memory-blind. F-B's unsafety needs the stale-`external_mb` condition, which an idle GPU cannot produce | **PASS as far as this leg can measure**; the mechanism is not closed |
| Is a slope fitted? | 0.0014796440700825149, 5 samples | the brief expected none ("grantless") | **C7 fits** — `enable_batching = true` in that registry — slope **byte-identical to run1's** on three separate legs. The **shipped** registry does not: `S8-ocr-C1-grantless` has `fit_slope null`, `local_samples 0`, `max_units_measured 0`, `unit_budget` pinned at the 2 000 000 seed across 205 grants and **no store written**; `analyze.py` prints *"NOTHING WAS LEARNED"* | **PASS — the expectation is right for the shipped registry, wrong for C7** |
| `grant_safety` | PASS, 12 grants | PASS | **PASS on all eight ledger legs** (14, 14, 14, 15, 15, 16, 183, 205 grants) | **PASS** |
| Job completes | 460 segments, 0 errors | completes | **460/460 on all eleven legs.** Nine `completed`; two `partial` — 3 items in `-cold`, 1 in `S8-ocr-C7nc`, all four to **P2**, each reported correctly with `outcome`, `failed_items`, `failure_reason` and a `job_failures` row | **PASS; P2 recurred** |
| `/health` for the easyOCR replica | — | — | `cost_unit "pixel"`, `cost_aggregation "max-times-count"`, **`cost_epoch 2`**, `cost_seed_units 2000000`, `last_grant_units 31 758 989`, `last_window_items 28`; `base_mb 796 (nvml)`, `dtype fp32`; worker row `unit_budget 69 598 720, ramp_step 6, deflation 0, clean_windows 14, max_units_measured 34 799 360, knee_units null, fit_samples 5, effective_margin 0.1021`; GPUs `reserve_rule "capped_default"`, `load_cooldowns: []`. **No canvas anywhere in `/health`** | **PASS (canvas gap noted, since closed)** |
| Throughput against the per-image path | 54.72 s vs master's 468.20 s = **8.56×** | beat the per-image path | **not resolvable on this host**: capped 71.68 / 63.53 s, uncapped 59.19 / 99.77 s, shipped per-image 55.52 / 64.68 s, master **113.32 / 185.06 s** — all on the same corpus within forty minutes | **INCONCLUSIVE (host, not product)** |

**Why the throughput row is inconclusive, and what survives it.** The workload
is **CPU-bound** — `gpuclk.txt` shows **0 % GPU utilization in 157 of 184**
one-second samples during the job, 112 W mean, no throttle reason ever set —
so the host sets the wall time, and the host was paging: the alarming legs ran
at 7–17 GB `mem_available` with up to **340 GB of swap in use**. The master
baseline moved the same way (468 s in run1 → 113 s → 185 s for a binary
untouched since run1), so any cross-day ratio built on it — run1's headline
8.56× included — inherits that. What *is* reproducible is host-independent:
the capped legs finish the corpus in **49–57 batches** against the uncapped
legs' **61–64**, because the 100 items at or above 2560² pack **4 to a batch**
where uncapped they packed 3 and 1; and per-item time on the uniform page
batches is flat at **126–131 ms/item** across every leg and every
configuration, run1 included.

### 4.4 Ground-truth ceiling probes (`run2-probes-report.md`)

No gateway: `ceiling_probe.py` drives the Python impl in-process on GPU 0, with
`vramrec.py` at 4 Hz over every sweep and bisect. Peak per-PID NVML equals the
probe's own `nvml_own_mb` **to the megabyte** on all four judgeable recordings;
GPU `used` runs a constant 646 MiB above it; `base_free_delta − base_nvml =
9 MiB` on both models, the same systematic offset run1 and Phase 0 recorded.
The measurements themselves are §8; what the leg *decided*:

- **Both of run1's slope disagreements were group mismatches.** A
  `pixel`-priced slope is comparable only to a slope measured on the **same
  image group**. run1's easyOCR ledger fit matches its group's probe **to
  seventeen digits** (ratio 1.0000), and run1's nemotron `S8-pixmix` fit is
  **0.907×** the probe of the group its `sample_units` are multiples of. The
  published 4.33× divided a 640×480-dominated fit by a 1024×1024 probe.
- **The canvas price is 1.4176× the above-canvas probe, and all of it is
  arithmetic**: `min(raw, 2560²)` charges a square canvas, while an
  aspect-preserving fit of a 1:1.414 page occupies 4 669 440 px — a built-in
  **1.4035×** conservatism, times the small group's 1.0099× padding round-up.
- **A product finding**: easyOCR's batched detector dies of a **32-bit index
  overflow**, not of OOM, at batch ≥ 29 — and `eocr.py` caught it, logged one
  line without the traceback and silently reprocessed the window one image at a
  time. True ceiling **28 items = 183 500 800 capped units**, identical under
  C7 and C7nc; the only symptom was throughput (3.10 items/s at batch 16
  against 0.91–0.96 from 29 up, a **3.2×** loss). Fixed in the round (§5).
- **Two instrument facts to carry forward.** A **warm-allocator sweep
  overstates the requirement by up to 1.8×** (easyOCR batch 12: 79 864 MiB warm
  against 43 964 MiB in a fresh process) — the warm number is what a long-lived
  worker produces and is the one comparable to a ledger fit, but a *boundary*
  must be drawn from the cold series. And **`--bisect-oom` is only trustworthy
  when the impl has no silent fallback**: both easyOCR bisects reported
  `largest_ok_items: 37` against a true 28.
- **Tool fix `6d074f3d`**: `ceiling_probe.py` deep-merged registry documents
  key by key, where `registry.rs` **replaces** an inference id wholesale. The
  C7nc registry, whose entire purpose is to omit `canvas_pixels`, therefore
  still resolved to the shipped 6 553 600 — **probe 2 would have measured the
  capped configuration and reported it as the uncapped control.** Caught on the
  dry run, before any GPU time was spent on it.
- `clip/qwen3-vl-embedding-2b` was **not probed**: the weights are absent from
  the local Hugging Face cache and nothing was downloaded.

### 4.5 Phase B — external pressure (`run2-phaseAB-report.md`)

Binary `34a591aa`, unmodified `server-C1.toml`. **S4a ran and passed; S4d, S4b
and S4g are blocked.**

#### S4a — `results/run2/S4a-v2/` — PASS on all three stated expectations

`hog.py --reeval 999999 leave-free 12288` (filled to **12 228 MiB free**,
byte-for-byte run1's figure, held flat at 84 352 MiB for all 281 samples),
store seeded from `S2-wdvit-v2` (anchor 439, no knee), `ramp` corpus.
2026-09-04 21:55:36Z–21:57:57Z. Job: 2 000/2 000, `completed`, 0 errors.
**Three grants: 1, 63, 147.**

| Expectation (run2) | Run1 | Measured on `34a591aa` | Verdict |
|---|---|---|---|
| **`reserve_rule = "capped_default"`** | rule did not exist (fraction reserve) | **`capped_default`** on all 3 grants and in all 285 `/health` GPU samples | **PASS** |
| **`reserve_mb ≤ 1024`** | 8 579 MiB withheld | **exactly 1 024** while the hog is up (the cap binds: `min(85 792 × 0.10, 1024)`); 0 on the idle GPU | **PASS** |
| **`limit_mb > 0`** | 3 517 | **11 071** throughout (`97 887 − 85 792 − 1 024`), minimum over the run 11 071 | **PASS** |
| **Batches ≈ 200 at 12 GB free** | **47 granted, 11 executed** (5.0 % of the 220-item boundary) | **147 granted (66.8 %), 83 executed (37.7 %)** for the 21 batches carrying 87 % of the job — **3.1× run1's grant, 7.5× run1's executed batch** | **PASS in direction and magnitude; short of the literal 200, and the shortfall is fully explained (below)** |
| Every grant ≤ headroom; batch ≤ the 12 GB boundary | 3/3, 47 ≤ 220 | **3/3** (53 ≤ 10 235, 3 319 ≤ 10 069, 7 729 ≤ 7 741); largest batch **147 ≤ 220** | **PASS** |
| No OOM / deaths / negatives | 0/0/0 | **0/0/0** (22 worker clamps, none an OOM) | **PASS** |
| No knee under pressure | no knee | **0 `fitted a throughput knee` lines**, `knee_units` null in all samples | **PASS** |
| Throughput | 0.97× of run1 S2 | **0.90×** of this run's S2 (27.397 vs 30.303 items/s) | **PASS (at the floor)** |
| `oracle_agreement` (run1's N4 phantom) | **FAIL, 83 of 210** | **PASS, worst 134 MiB, 0 of 456** | **PASS** |
| transport / sockets | n/a | `h2c`, peak lanes 32, gate 256→4 096, peak in-flight 1 999, `queue_bound_windows` 3; peak fds **110** / 70 sockets / **65 established** | **PASS** |
| P1 / P2 | n/a | `invalid multipart` / `no_policy` / `request_incomplete` / `out of memory` = **0 each** | **PASS** |

`analyze.py`'s printed `utilization FAIL (0.34, floor 0.50)` uses the
*full-GPU* boundary (2 560) and the anchor-derived budget (878) — the same
artefact run1 called out; the scenario's criterion is against the boundary at
12 GB free and is recomputed by hand above.

**Why 147 and not 220** (no defect): the probe's 220 is priced against the
GPU's *whole* free memory, the ledger against `limit − our charge`:
`limit 11 071 − charge 3 330 = headroom 7 741`, `7 741 / 51.61 = 149 → 147`.
~20 units are the 1 024 MiB reserve; ~50 are memory the worker already holds.

**Finding S4a-A2 (MEDIUM, not a regression) — the worker's live clamp is now
the binding constraint, and it double-counts our own pool.** Twenty-two
identical clamps, one per batch of the big window:

```
21:56:12.864 [INFO] inferio_worker.packing: free memory fell to 4371 MiB against a
             7729 MiB grant; shrinking this batch's budget from 147 to 83 units
```

4 371 MiB is NVML's free reading *after* our caching allocator took its pool —
the same pool the ledger already charged us for and which the allocator would
reuse rather than grow. Our footprint is charged twice: once as `charge_mb` in
the grant, once as memory missing from the live free reading, and **44 % of the
grant is left on the table with nothing external moving**. Safe (that is the
clamp's job, and it is what kept every run1 profile safe) and not a run2
regression — but in run1 the ledger's margin was the binding constraint and now
the clamp is. Recorded as a user decision in §6.

#### S4d, S4b, S4g — BLOCKED

<!-- BLOCKED: S4d, S4b, S4g — SGLang holds both GPUs -->

Not run, and **not attempted against a live SGLang**. At 21:59 UTC the user
recreated `dsv4-flash-sglang`, which holds **77 702 MiB on each GPU**:

- **S4b** (`hold 0`, then **+30 GB** at t = 60 s) is arithmetically impossible
  with ~20 GB free — the step the leg is defined by cannot be taken.
- **S4d** (`leave-free 8192`, release everything at t = 120 s) could be forced
  to its starting free level, but its criterion is the **recovery** when the hog
  releases: here the GPU would recover to ~20 GB free, not 95 GB, and
  `external_mb` would be tracking a live inference server instead of a hog whose
  schedule the run controls.
- Beyond validity there is a **risk to the user's service**: the worker
  allocates into whatever headroom is left, and on a GPU SGLang is serving
  from that can starve it.

**To finish them**, wait until both GPUs are idle (SGLang is the user's to
stop and start — the run must not stop it), then run, in order, with the seed
store named:

```bash
S=<session scratchpad>/bin
R=/home/admin/projects/panoptikon/tools/calibration-protocol/results

SEED_CAL=$R/run2/S2-wdvit-v2/calibration.after.toml \
HEALTH_EXTRA=--full HOG_SCHED="leave-free 8192" HOG_EXTRA="--reeval 999999" \
HOG_WAIT=180 DRIVE=$S/drive-d.sh JOB_CAP=1800 \
  $S/runjob2.sh S4d-v2 tags/wd-vit-tagger-v3 $R/corpus/ramp8

SEED_CAL=$R/run2/S2-wdvit-v2/calibration.after.toml \
HEALTH_EXTRA=--full HOG_SCHED="hold 0" DRIVE=$S/drive-b.sh JOB_CAP=1800 \
  $S/runjob2.sh S4b-v2 tags/wd-vit-tagger-v3 $R/corpus/ramp8
```

`drive-d.sh` releases the hog at t = 120 s; `drive-b.sh` steps it to
30 720 MiB at t = 60 s; both are run1's, copied verbatim. `runjob2.sh` is
run1's `p2b/runjob.sh` with `--run-id run2`, an optional hog and the descriptor
recorder added. The **`ramp8` corpus (16 000 items)** is what run1 used for
S4b–S4e — the 2 000-item `ramp` is exhausted before either event fires.
S4g (`leave-free 1024`, then a job for the unloaded nemotron) is run1's leg
unchanged; nothing in run2 changed its expectation (the load guard still
refuses), and it is the cheapest of the three to run.

What each still has to answer: **S4d** — no `mb = 0` (memory-blind) grants,
`limit_mb > 0`, and the budget recovering to the S2 level within 3 windows of
the release; **S4b** — `external_mb` on the grant line tracking the hog step
**within a few seconds** (the per-batch `free_mb` ingest, R5/T3) rather than
run1's 31.5 s, bounded `oracle_agreement` breaches, and the worker clamp still
firing as backstop; **S4g** — a clear per-model error, a healthy server and no
respawn loop. Watch **C6** throughout: bigger admitted batches on a tight GPU
are slower for a model whose curve is flat, and S4a already reads 0.90×.

### 4.6 Phase A′ re-run — S2 wd-vit, S3 wd-vit (`run2-phaseAB-report.md`)

Binary `34a591aa`, **unmodified** `server-C1.toml` (no `calib_hostless`), both
GPUs at 2 MiB before every leg. This is the acceptance test for the fix
round's two largest items, and it passes on every check.

#### S2 wd-vit — `results/run2/S2-wdvit-v2/` — PASS, no defects

Cold ramp, empty store. 2026-09-04 21:44:26Z–21:46:38Z. Job: 2 000/2 000,
`outcome: "completed"`, 0 errors, 0 failed items, `inference_time` 57.89 s.
`analyze.py --learning`: every check that can pass, passes.

| Expectation (run2) | Run1 | Run2 on the OLD binary `65fd2f82` | Measured on `34a591aa` | Verdict |
|---|---|---|---|---|
| Job creation works on the shipped policy shape (P1) | n/a | **500; needed the `calib_hostless` policy** | `EXTRACTION_POST_RC=0` on unmodified C1; **zero `reason="no_policy"` lines** in 2 123 log events | **PASS** |
| Ramp reaches ~512 in ~10 windows, no `W → 200 − W` cycle (S1) | anchor 512, 10 windows | **anchor froze at 136**, 88 grants, budget cycling 3/7 | **anchor 439 in 12 windows**, budget published **878**; monotone 1,2,4,8,16,17,34,68,136,272,439; the bound is the corpus (last window asked 439 with 439 items left), not the transport | **PASS** |
| **No knee on wd-vit at any point** (F1) | no knee (correct) | **`knee_units = 3` fitted, persisted as 7** | **`fitted a throughput knee` appears 0 times.** Two refusals, F1 rule 1: `declining to fit a throughput knee: the plateau starts at the smallest batch size measured, so no bend was observed bucket=1 observed_floor=1 observed_top=8 anchor=272 observations=14`. Store carries **no `knee_units`**; `/api/inference/metadata` → `"knee_units": null` | **PASS — F1 fixed** |
| No `partial` from `invalid multipart body` (P2) | completed | **`partial`, 1 of 2 000 lost to a 400** | `grep -c "invalid multipart"` = **0**, `request_incomplete` = 0, `failures.json` empty on every list, job `completed` | **PASS — P2 fixed** |
| `/health.inference_clients` shows `h2c` | n/a | n/a | `{"transport":"h2c","pool_connections":64,"connections_in_use":1,"max_concurrent_requests":256,"in_flight_requests":64}`; only pre-probe samples read `"unknown"` (the documented `try_read` fallback) | **PASS** |
| lanes in use small | n/a | n/a | peak `connections_in_use` = **22** of a 64-lane pool | **PASS** |
| gate follows the published figure | pinned 200 | pinned 200 | gate **256 → 3 264**, tracking `desired_in_flight_items` 48→192→408→816→1 632→3 264; peak `in_flight_requests` **1 382** | **PASS** |
| `queue_bound` low | n/a | n/a | **6** over the whole job, each one a ramp-step window the job could not yet fill or the final corpus-exhausting window; the desired figure climbed across every one | **PASS** |
| sockets ≤ 2 × lanes-in-use + reserve | n/a | n/a | peak fds **90**, of which **50 sockets**; `ss -tnH state established '( sport = :6342 or dport = :6342 )'` peaked at **44 = 2 × 22 lanes** (loopback: each lane is a client and a server socket in one process). 0 % of the 524 288 soft limit | **PASS** |
| `grant_safety`, OOM, deaths | PASS/0/0 (10 grants) | PASS/0/0 (88 grants) | **PASS on 12 grants**, 0 over headroom, 0 over live free, 0 memory-blind; 0 OOM, 0 deaths | **PASS** |
| Slope vs probe | 50.5625 (+0.04 %) | 50.6 (+0.12 %) | **52.6818 (+4.24 %)**, ratio 1.0424 — band a little wider because two anchors are not powers of two (17, 439) | **PASS** |
| `base_mb` vs probe | 964 (0 %) | 964 (0 %) | **964 (0 %)**, `base_method="nvml"` | **PASS** |
| `dtype_method` in the store | absent | `"inferred"` | **`dtype_method = "inferred"`**, `dtype = "fp32"` | **PASS** |
| `dtype = "unstated"` | n/a | no producer | **still no producer on this host** (`unstated` appears 0 times) | **not exercised** |
| `reserve_rule` / `reserve_mb` / `limit_mb` | n/a | `capped_default`, 79–197 | **`capped_default` on all 12 grants, `reserve_mb` 79–197**; `/health` `limit_mb` never below **97 034** | **PASS** |
| `canvas_pixels` on the grant line | n/a | n/a | **`canvas_pixels=none`** — correct for an `item`/`count` model | **PASS** |
| `load_cooldowns[]` | n/a | `[]` | **`[]` in all 262 health samples** | **PASS** |
| `utilization` | 0.40 | **0.11 (knee-pinned)** | **0.34** (peak budget 878 / boundary 2 560) | **PASS** |
| Throughput vs master C0 | 0.94x | 1.02x | **0.95x** (30.303 vs 31.746 items/s) | **PASS** |
| `oracle_agreement` | FAIL (81 of 205) | PASS (0 of 410) | **PASS**, worst 137 MiB, 0 of 432 | **PASS** |
| `unit_budget` shrink lands within one window | n/a | could not land | **not exercised** — idle GPU, no shrink called for; exercised in S4b/S4d | **not reached** |

No product defect, no deviation, no unexpected log line on this leg.

#### S3 wd-vit restart — `results/run2/S3-wdvit-v2/` — PASS on the leg's own question; one accepted FAIL at run1 parity

Fresh root seeded with `S2-wdvit-v2/calibration.after.toml` (anchor 439, slope
52.68, **no knee**). 2026-09-04 21:50:29Z–21:53:07Z. Job: 2 000/2 000,
`completed`, 0 errors, `inference_time` 56.27 s. **Three grants for the whole
job: 1, 63, 878.**

| Expectation (run2) | Run1 | Run2 on the OLD binary | Measured on `34a591aa` | Verdict |
|---|---|---|---|---|
| Seeded profile resumes, no re-ramp | `seeded_from_store=true`, 3 windows | `seeded_from_store=true`, `ramp_step=5` | **`seeded_from_store=true`**, first grant already `ramp_step=6`, then 63, then 878 (= 2 × the seeded anchor); no 1,2,4,8… | **PASS** |
| **No knee caps the fresh job** | no knee | **stored knee 7 pinned the whole job at 7–31 units** | **0 `fitted a throughput knee` lines; `knee_units` is `null` in all 275 health samples and absent from the store** — the seed carried none to restore | **PASS — F1 fixed across a restart** |
| **utilization ≈ run1's 0.80** | 0.80 (peak budget 2 048) | **0.01 (peak budget 31)** | **0.69** (peak budget 1 756 / boundary 2 560). The gap to run1 is exactly the seeded anchor: run1 resumed from 512 → 1 024, this leg from 439 → 878 | **PASS** |
| `calibration.status = "local"`, `local_samples` continues, anchor only rises | local, 10→13, 512→1 024 | local, 20→32, 136→136 | `status: "local"`, `local_samples` **11 → 15**, anchor **439 → 878** | **PASS** |
| `knee_clean_windows` restored / `knee_withdrawn` | n/a | not exercised | **not exercised, correctly** — no knee exists to count or withdraw. The restoration path itself was proved on the old binary in `S3-wdvit-kcw` | **not exercised** |
| `/health` clients, lanes, gate, `queue_bound` | n/a | n/a | `h2c`, pool 64, peak lanes **32**, gate **256 → 4 096** (the ceiling; `desired_in_flight_items` reached 5 268), peak in-flight **1 999**, `queue_bound_windows` **3** (one per window, all corpus-bounded) | **PASS** |
| sockets ≤ 2 × lanes + reserve | n/a | n/a | peak fds **111**, sockets **70**, `ss` established on :6342 peak **65 = 2 × 32 + 1** | **PASS** |
| `reserve_rule` / `reserve_mb` / `limit_mb` | n/a | `capped_default` | **`capped_default`** on all 3 grants, `reserve_mb` 78–103, `limit_mb` never below **97 034** | **PASS** |
| P1 / P2 do not recur | n/a | both fired | `invalid multipart` / `no_policy` / `request_incomplete` = **0 each** | **PASS** |
| No OOM, 0 deaths, `grant_safety` | 0/0 PASS | 0/0 PASS | **0/0, PASS on 3 grants** | **PASS** |
| Throughput ≥ 0.9× C0 | **0.89× (FAIL)** | 1.09× | **0.88×** (27.778 vs 31.746 items/s) | **FAIL, accepted — run1 parity** |

**Adjudication of the one FAIL.** Run1's own S3 read 0.89× and failed the same
floor for the same reason (finding N2: wd-vit's curve is flat, so one
1 936-item window pipelines slightly worse than many small ones, and the whole
job is three windows). The old binary's 1.09× was *caused by* the spurious knee
holding it to 7–31-unit windows; restoring the big windows necessarily gives
that back. Utilization is the number this leg exists to measure, and it moved
0.01 → 0.69.

### 4.7 Phase D — packing, docker — BLOCKED

<!-- BLOCKED: phase D — needs idle GPUs; the shape-ceiling leg needs ~93 GB free -->

Not run: SGLang holds 77 702 MiB on both GPUs from 21:59 UTC (§1). Nothing
about the expectations has changed — they are restated below together with the
runbook to execute each leg cold. Everything runs on the rebuilt binary
`34a591aa` and, for S11-C4, the rebuilt image `0b2261f94c8f`; the S8 ocr-C7 leg
of §4.3 was a *different*, already-completed leg on `65fd2f82`.

**S8 pixmix.** Config **C1** (`tools/calibration-protocol/config/server-C1.toml`
+ `env.C1`, `run-gateway.sh`), model `clip/nemotron-embed-vl-1b-v2`, corpus
`results/corpus/pixmix`, **empty store** (the epoch-2 bump means every run1
nemotron row is ignored anyway); run1's verbatim commands are in
`results/run1/S8-pixmix/runlog.md`. Check: `canvas_pixels` on nemotron's grant
lines **and** on the load report and in `/health.models[].cost.canvas_pixels`;
**no single-item batches** for the 20 MP items (they must pack at the canvas);
`utilization > 0.3`; the epoch-2 profile written fresh. Judge the fitted slope
against the probe **of the image group the corpus is dominated by**
(`results/run2/probes/summary.md`), never against a single number: a
0.3 MP-dominated fit should land within ~10 % of **0.00128**, and against the
`img-20mp` probe the same fit reads ≈ 6.0×, which would mean nothing — exactly
as run1's 4.33× meant nothing. Remember the canvas-capped price is ~1.40×
conservative for a 1:1.414 page by construction.

**S11-C4 (Docker).** The C4 compose overlay under
`tools/calibration-protocol/config/compose/`, image **`0b2261f94c8f`** tagged
`panoptikon:calib-cuda`, the shipped `docker.toml` policy (the image carries
the P1 fix, so **this leg is the real test of it**), a 2 000-item job on the
`ramp` corpus; run1's commands are in `results/run1/S11-C4-fixed/runlog.md`.
Check: **2 000/2 000**; `known transport h2c` in the log; the gateway's
inference sockets bounded by the lanes actually recruited — sample
`ls /proc/<pid>/fd | wc -l` *inside* the container during the job, expect
≈ 2 × lanes-in-use + reserve, not hundreds; the job's in-flight ceiling at the
**byte budget**, not the descriptor clamp (now **384** = `FD_RESERVE + 2 × 64`);
`/proc/1/limits` still showing the raised `nofile` (run1's F6).

**The easyOCR shape-ceiling leg.** Config **C7**
(`server-C7.toml` + `registry-C7.toml`, which restates easyOCR's canvas and
`epoch = 2`), `doctr/easyocr_*` on the `ocr` corpus of >2560 px scans; run1's
S8-ocr-C7 runlog is the recipe. It needs a **large idle GPU — ~93 GB free,
because the ceiling only appears once a batch of 28 items of 1824×2560 can be
granted**. Check: the ceiling at **28 items ≈ 183 500 800 units** in
`/health.models[].shape_ceiling_units` and in the INFO line naming
`action`/`cause`; `clamped.reason = "index_limit"` on the measurement; **no
deflation** from it; and recognition text on >2560 px scans **unchanged from
run1** (the D1-b verifier reverted the quality half: the detector runs bounded,
the recogniser still crops from the raw image). Note that both run2 bisect
boundaries from `ceiling_probe.py` (37 against a true 28) must be treated as
**invalid**, not merely noisy — §4.4.

### 4.8 Phase E — soak — BLOCKED

<!-- BLOCKED: phase E — needs both GPUs for 4 h -->

Not run, for the same reason, and it is the leg that needs the host most: the
S9 recipe drives **both GPUs** for its whole length with a hog and a
concurrent loadgen, so it cannot share a GPU with SGLang at all.

**Runbook.** Run1's `results/run1/S9/runlog.md` verbatim, on binary
`34a591aa`, config **C1**, the `soak` corpus, **4 h instead of 8** — jobs
cycling over the four shipped models with `hog.py` and `loadgen.py` alongside,
`vramrec.py`/`healthrec.py --full`/`fdrec` running throughout, then
`analyze.py`. Expect: **all jobs completed** — no fixture is involved, so **any
`partial` is P2 or its successor**, and a re-queued `transport` /
`request_incomplete` failure should reach `job_failures` only if it failed
twice; **0 deaths**; **no persisted knee below the ramp's plateau bucket**
(run1's S9 fitted `knee_units = 1` on wd-vit and never refitted for 7 h 55 m);
deflation returning to 0; the store bounded; no cooldowns
(`/health.load_cooldowns[]` empty at every sample); `oracle_agreement` breaches
**lower than run1's** (the R12 attribution fix); RSS bounded; and
`predict_body_budget.refused_requests` at **0**. Then, and only then, SGLang is
the user's to restart — in this session the user has already done it.

## 5. The fix round

Twelve items, each fixed by one agent and reviewed by a different one, between
Phase C and the second integration pass. Every entry gives the root cause in
one paragraph with the evidence it was established from, the commits, what the
verifier changed, and the leg expectation it sets — which is how Phases B, A′,
D and E know what they are testing.

### P1 — the policy layer could not see an HTTP/2 request's host

`policy::resolve_effective_host` read only `header::HOST`. HTTP/2 carries the
authority in the `:authority` pseudo-header and sends no `Host` at all (RFC
9113 §8.3.1), so every h2c request resolved to a hostless `None`;
`select_policy` requires a matching host whenever a policy states any, so the
shipped `hosts = ["localhost", "127.0.0.1"]` policy declined, nothing matched,
and the request was refused 403 `no_policy`. R10' had just made
`inferio_client` an h2c-prior-knowledge client of the gateway's *own* inference
surface, which turned that into a 500 on every extraction-job creation.
Evidence: `results/run2/S2-wdvit-blocked-h2c403/defect-h2c403.txt` — the
`no_policy` denials for `/api/inference/cache` and `/api/inference/
external-inputs` sit immediately beside `multiplexing inference requests over
HTTP/2 cleartext`, then the 500. A load-time invariant already asserted the
opposite (`config.rs::validate_loopback_inference_policy` checks at config load
that a policy matches the loopback host), which is further evidence the defect
was at the right layer.

- **Fix** `74ca202c` + Desktop parity `4c2e00b6`. The effective host is now the
  first of: trusted `Forwarded`/`X-Forwarded-Host` (only under
  `trust_forwarded_headers`) > the **request target's authority** (h2
  `:authority`, h1 absolute-form) > `Host` > none. RFC 9112 §3.2.2 makes the
  authority *mandatory* over `Host` on HTTP/1.1 absolute-form; RFC 9113 §8.3.1
  makes it correct on HTTP/2. No new reach: all three sources are
  client-chosen, and the non-spoofable dimension is the listener `endpoint`,
  untouched.
- **Verifier: APPROVED, and it fixed the same blind spot in the Desktop
  bridge** (`api/desktop.rs` read `header::HOST` directly and failed *closed*
  on h2). It re-ran the negative control itself — disabling the authority
  branch fails 4 policy tests and 2 desktop tests, the h2c one with the exact
  reported symptom — and pinned the shared rule in
  `policy::request_authority`. Policy 42 tests, desktop 11.
- **Leg expectation**: the `calib_hostless` workaround is gone (`ea59a63b`), so
  **every job-driven leg from here on is the test**; S11-C4 exercises it on the
  shipped `docker.toml` inside the image.

### P2 — a predict handler that answered with the request stream still open

The handler parsed the multipart body **as a stream**; multer stops at the
closing delimiter and never polls the frame after it, so the handler answered
while the request stream was open. hyper must then reset the stream (RFC 9113
§8.1); the client's terminal DATA frame lands on a closed stream; h2 counts
that as a library-initiated reset in `num_local_error_reset_streams`, which
**has no decrement site anywhere in h2** — verified in the vendored 0.4.13
source by both the fixer and the verifier — and at
`max_local_error_reset_streams = 1024` the connection is torn down with
`GOAWAY(ENHANCE_YOUR_CALM, "too_many_internal_resets")`. That is the *one* h2c
connection the self-call used for a whole job, so every request mid-parse on it
failed at once, and `MultipartError`'s Display flattens every cause into the
single sentence the gateway logged as `400 invalid multipart body`. Reproduced
verbatim by a harness driving the production body-handling code over real
h2c: **381 server parse failures and 456 client failures in 300 032 requests**,
clustered in one ordinal band; the HTTP/1.1 control was clean, which matches
the field observation that P2 appeared only after R10'.

- **Fix** `7e96de62`: the body is collected before it is parsed, so the stream
  reaches its end and no counted reset accumulates. With the whole body in
  hand the split is asked **of the bytes** — does what arrived carry the
  closing delimiter of the boundary this request declared? — rather than of
  multer's error variant: no → `400` with `detail.kind = "request_incomplete"`
  and the full cause chain; yes → an ordinary `400`. `is_unattempted()` then
  buys the item one re-queue. Post-fix at the identical configuration:
  **0 / 300 032**.
- **Verifier: APPROVED, with a bound added on top** (`79488c92`): `512 streams
  × 2 GiB` is not a memory bound at all, since nothing bounds how many
  connections a peer opens. A **process-wide 4 GiB predict-body budget**
  (`= 2 × PREDICT_BODY_LIMIT`, so a maximal body can always be admitted beside
  another) is charged before the bytes are read and refuses with **503 +
  `Retry-After: 1` + `kind = "body_budget_exhausted"`** — which the client
  already knew what to do with. `/health` gains `predict_body_budget`.
- **Leg expectation**: Phase E. No fixture is involved, so **any `partial` job
  is this**; a `request_incomplete` failure must be re-queued once and reach
  `job_failures` only on a second failure.

### F1 — the knee estimator judged a plateau against the ring's own smallest bucket

The plateau test was self-referential and one-sided: the knee is "the smallest
bucket within `KNEE_RATIO` of the best", the best was taken over the same ring,
and on a flat curve the smallest bucket therefore clears **its own** threshold.
wd-vit's ring at the first fit (14 observations, reconstructed from the ledger's
own log lines and validated against every logged `throughput_samples` and
`observations=` count, zero mismatches over 88 windows) has bucket 1 as both
candidate and reference: `40.77 ≥ 0.9 × 40.77`. Two structural amplifiers made
that the *default* outcome on a ramp: the ring is dense at the bottom and
sparse at the top, and the frontier guard used the largest **retained** bucket,
so the ring's real frontier — bucket 7, one sample at 136 units — was dropped
by the two-sample retain and bucket 6 silently inherited the title.

- **Fix** `9cbd6304` (+ `d230d5ba`, `19e2bf9f`, `ce03bd0d`): one candidate and
  five vetoes — the observed frontier must be quiet and must not be the knee;
  the floor must be interior; `KNEE_PLATEAU_BUCKETS = 2` quiet buckets above,
  none faster by the ratio; no ramp-era knee below the anchor (judged per
  sample by the anchor stamped at ingest); after a widening the evidence must
  be newer than the widening (a permanent `seq` mark, not a flag the ingest
  consumes). Plus: a replica's first settled window teaches the knee nothing,
  and a knee this process never measured is **provisional** —
  `KNEE_SEED_REVALIDATION_WINDOWS = 4` instead of 12.
- **Verifier: FIXED — one real defect.** Rule 4's gate read the *live*
  `max_units_measured`, which a unified-memory-device death **halves**; two halvings
  switch the rule off for the highest candidate rules 1 and 3 allow, admitting
  a knee at ≈ half the largest measured batch on wholly ramp-era evidence
  (reproduced red: `Some(31)` where `None` is right). It is now held up by the
  largest anchor the ring's own samples were taken under (`36a8cb77`), plus
  `846806f0`, `9a507c82`, `493c7f0b`, `f0c74915`. The verifier also transcribed
  the estimator independently and reproduced the fix report's verdict census to
  the unit: `S2-wdvit` 126 × rule 1, 61 × gates, 24 × rule 3, 7 × rule 2;
  `S3-wdvit` 128 × rule 2, 77 × gates — **no knee at any point** in either.
- **Leg expectation**: Phase A′ — no knee on wd-vit at any point; a seeded knee
  provisional and re-validated within 4 windows; `knee_withdrawn` may fire.
  MobileCLIP fitting none is accepted (§6, F1 residue).

### S1 — an in-flight ceiling of 200 that no layer could name

`hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200`, which
`axum::serve` never overrides, sat over a `hyper-util` pool that hands every h2
caller the **same** connection (`Reservation::Shared`, readiness = "the
dispatch channel is open"), so `INFERENCE_POOL_CONNECTIONS = 4` bought exactly
one socket. The job could never put more than 200 predicts on the wire while
the orchestrator was asking for 1 632, and window formation degenerated into
the involution `W → C − W` — the stable 136/64 cycle — so `max_units_measured`
froze at 136 and the ramp advanced one step per *two* windows. The diagnosis
came from reading `queue_depth + last_window_items` across 70 windows on two
independent legs and noticing that the two phases sum to exactly 200 every
time; it is now a **test assertion** rather than an inference (`4e587635`
offers 400 concurrent predicts and measures peak 200, one peer address).

- **Fix**, one commit per item: `75ada20a` (our own serve loop, so the server
  advertises `MAX_CONCURRENT_STREAMS = 512 = 8 × H2_STREAMS_PER_CONNECTION`,
  plus an explicit `PREDICT_BODY_LIMIT`), `bc483729` (**64 real connection
  lanes**, one client each with `pool_max_idle_per_host = 1`, dispatched
  least-loaded *within the recruited prefix* `k = ceil((in_flight + 1) / 64)`
  so socket cost tracks work; two gates — HTTP/1.1 fixed at 256 forever, h2c
  following the published desired-in-flight figure clamped to `[256, 4096]`),
  `44bdd895` (a bounded settle so a window forms **after** the previous
  window's refills land: `WINDOW_SETTLE_QUIET = 2 ms`, `WINDOW_SETTLE_MAX =
  20 ms`, never on an idle model, never when the queue already carries the
  budget), `feb0a5a9` (`/health` reports the published figure, the queue-bound
  window count, the client section and the effective canvas), `085e9cd7`.
  A real defect surfaced by the lanes: hyper opens up to
  `DEFAULT_INITIAL_MAX_SEND_STREAMS = 100` before the peer's SETTINGS arrive,
  so any peer or proxy advertising fewer refuses streams on first contact —
  `REFUSED_STREAM` is now retried by type (RFC 9113 §8.7) and **keeps** the
  h2c memo, since only an h2 peer can send it.
- **Verifier: FIXED — four defects.** Lane clients were built eagerly, costing
  **58.6 MiB per endpoint** measured at `VmRSS` (now built on recruitment:
  13.0 MiB for the first endpoint, **1.4 MiB** for each further one, ~620 KiB
  per recruited lane — `e6378f78`); a retry held its gate permit and lane
  across the backoff, precisely when the server had just said it was overloaded
  (`9924044c`); `/health` computed in-flight as `target − available`, which
  reports a saturated endpoint mid-shrink as **idle** (`a90856f9`); and a test
  bound an ephemeral port and dropped it, so "nothing is listening" was
  occasionally a neighbour's stub (`e3deabcf`). Plus the body budget above and
  `6aa2c171`, `8d8ed09c`.
- **Leg expectation**: Phase A′ — `queue_depth + last_window_items` stops
  summing to 200, the 136/64 alternation is gone, `max_units_measured` moves
  past 136, `/health`'s published figure and the client's in-flight agree, and
  `connections_in_use` stays a handful.

### S1b — a unit-budget shrink that could never land

`Semaphore::forget_permits` removes only permits that are *available*. A
saturated job has none, and a permit released by `drop` is handed to one of the
hundreds of waiting item tasks before any resize can see it, so a shrink was
described as deferred and was in fact never applied. Measured on `S2-wdvit`:
after the knee the published figure fell to the floor of 64, `observe` set
`pending_shrink = 136`, and in-flight stayed at **200** for the whole post-knee
phase.

- **Fix** `fae83107`: deficit accounting on the **release** path —
  `UnitBudget::release` takes `retired = min(pending_shrink, held)`, forgets the
  whole permit and re-issues only the remainder. The invariant `permits in
  existence == target + pending_shrink` is unchanged. Test: saturated at 200
  with 200 waiters, target dropped to 64 — after 136 releases **0** reached a
  waiter; under the old mechanism 61 of them leaked.
- **Verifier: APPROVED**; the same rule was applied to the h2c gate's own
  shrink.
- **Leg expectation**: Phase A′ — a `unit_budget` shrink lands within one
  window when the target falls.

### C2 — a trusted OOM classification left no trace

`oom_verdict` returned `Trusted` for `typed_exception`, `marker`, an
unrecognised tier and a pre-run2 worker's bare flag, and logged nothing; only
the `Contradicted` arm printed. So a deflation the ledger *believed* left the
log with `outcome="negative" reason="oom"` and no statement of who classified
it: `S5-oomtimed`'s 5 954 marker-classified OOMs mention `oom_class` zero
times, against one mention in `S5-oom2nd` (the veto). The plan's clause was
verifiable only for the refused case.

- **Fix** `672aa85a`: one INFO line per OOM negative, immediately above that
  window's WARN and keyed off the very `negative_reason` the WARN prints, with
  `source` (`typed_exception` | `marker` | `message_pattern` | `error_frame` |
  `unclassified` | a tier this host does not recognise, verbatim), `exception`,
  `trust`, `free_mb_at_failure` (`-1` sentinel), `grant_mb` and `oom_samples`.
  `trust` is a closed three-value vocabulary: `trusted`, `corroborated` (the
  worker's free reading was **below** the window's grant) and `unopposed` (no
  reading, or a memory-blind grant) — the brief's binary was reported as three
  because `unopposed` is genuinely a different epistemic state.
- **Verifier: APPROVED, two defects fixed.** A tier the worker states as an
  **empty string** rendered as a bare `source=`, which `analyze.py`'s field
  regex does not parse — the field would have vanished from the one line whose
  purpose is to name it (`18f2aa1b`). And `analyze.py` did not read the line at
  all: `check_failures` now tallies `source/trust` pairs and names any
  unattributed negative rather than rounding it away (`62a092c9`), verified
  byte-identical against both legs' committed `verdicts.json` and smoke-run
  over all 52 recorded scenarios.
- **Leg expectation**: every OOM negative in Phases B–E carries its tier line,
  and `analyze.py`'s `failures` row tallies the tiers.

### C4 — a job aborted by a model load said neither which model nor for how long

Phase C measured the entire `failure_reason` as *"Failed to load model: model
load failed on all 1 inference endpoints"*, where `/health.load_cooldowns[]`
and the 503 body both carry the inference id, the consecutive-failure count,
`retry_at`, `retry_after_secs`, `window_secs` and the last error. Two causes:
`load_model_all` kept the **last** endpoint's error rather than the most
informative one, so a typed cooldown verdict was overwritten by a plain 500;
and the non-cooldown reason rendered `{err}`, i.e. only the outermost context.

- **Fix** `20bd1536`: prefer an error whose `InferenceFailure` is a load
  cooldown (ties still go to the last), name the model in the context, and
  build the reason in one place — `cooldown_reason(failure)` when it is a
  cooldown, `{err:#}` (the whole cause chain) otherwise.
- **Verifier: APPROVED**; the tests assert both halves, including two stub
  endpoints where "keep the last" and "keep the most informative" disagree.
- **Leg expectation**: any leg that meets a load failure records a reason
  naming the model and the retry instant.

### C5 — a systemically failed job was told its re-queued items completed

The branch was `else if guard.requeued_items > 0`, reached whenever there was
no *partial* reason — which includes `JobFailure::Systemic` and an abort. Phase
C caught it logging that 2 000 re-queued items "then completed" two statements
before the same code wrote `outcome: "failed", failed_items: 2000`.

- **Fix** `f153a739`: a pure `requeue_summary` built from **the same two inputs
  the outcome itself is chosen from**, so the sentence and the record cannot
  disagree — aborted, systemic and completed each get their own wording, and
  the severity follows the outcome.
- **Verifier: APPROVED**, with the head sentence corrected (`79488c92`): it
  said items were re-queued "after an inference worker died", but since P2 a
  re-queue also happens when a request body never arrived, and now when the
  server had no room to read one.
- **Leg expectation**: Phase E — no job log claims a completion its own record
  contradicts.

### D1-b — the canvas cap removed the size information the packer sorts on

With `min(raw, 6 553 600)` a 2480×3508 scan and an 8000×6000 sheet price
**identically**, so `packing.plan_batches` — which buckets a `max-times-count`
model largest-first by *priced* units — can put them in one batch, and
`eocr.py::pad_images_to_same_size` then padded every member to the largest
member's **raw** dimensions. That is the "uniform dims" cost
`enable_batching = false` exists to avoid, and size-homogeneous bucketing is
the property run1's S8 PASS rested on. Supporting evidence, suggestive rather
than conclusive on this host: six 4-item batches at 2 209–4 538 ms in
`S8-ocr-C7-calm` against the same leg's other 4-item batches at 1 146–1 236 ms.

- **Fix** `2b8499ce` (a descending **raw-size tiebreaker** applied only among
  items whose priced units are equal, and only for `max-times-count` — it never
  changes a batch's price, count or safety), `44a7babf` (bound each input by
  the canvas *before* padding), `e3b6cacc` (the protocol rule: a declared
  canvas obliges the impl to bound its batch tensor).
- **Verifier: FIXED — and it reversed a quality regression.** Reading easyocr
  1.7.2 shows the detector's tensor was *always* canvas-bounded
  (`detection.py::test_net` resizes every member before the tensor exists) and
  the recogniser's is a fixed `(N, 1, 64, W)` set by crop **aspect ratios**,
  independent of page resolution. So bounding the array the recogniser crops
  from saved nothing the ledger prices — and cost transcription: measured on
  CPU with the real weights, at 30 px text on an 8000 px sheet the bounded arm
  returned garbage (`PANOJIIKCN CALIURAIION`) where the raw arm reads cleanly,
  recovering 8 boxes against 11, because `min_size = 20` had silently come to
  mean 62 raw pixels. The shipped behaviour is now **detect on the bounded,
  padded batch; recognise from the raw image; map the boxes back; filter
  `min_size` in submitted pixels** (`22845fdb`), plus `ccd14ad6` (the
  mixed-batch guard is exempted only by a canvas the impl states on **itself**,
  not one found inside somebody else's processor), `0f8d04d7` (C7nc),
  `96338378`, `3bc4c751` (2 000 randomised windows proving the tiebreak only
  ever changes the order), `238a0601`.
- **Leg expectation**: Phase D — bucketed batches size-homogeneous under the
  cap, and recognition text on > 2560 px scans **unchanged from run1**.

### easyOCR's int32 index ceiling — a silent 3.2× throughput cliff

At batch ≥ 29 on 2480×3508 scans, `Reader.detect` raises `RuntimeError:
integer out of range` from `torch.max_pool2d` inside CRAFT's VGG backbone —
`at::native::safe_downcast` on the pooling **output element count**, which the
CUDA kernel launches over as a signed `int32`. `eocr.py` caught it with a bare
`except Exception`, logged one line without the traceback and reprocessed the
window one image at a time, so the ledger could never learn the ceiling: no OOM
measurement, no deflation, and `unit_budget` free to widen past a batch the
impl cannot execute. The only symptom was throughput — 3.10 items/s at batch 16
against 0.91–0.96 from batch 29 up. The ceiling is exactly computable:
`per_item = 64 × ⌊H/2⌋ × ⌊W/2⌋` over the padded detector tensor, `max_batch =
(2³¹ − 1) // per_item`, which gives **28** at 1824×2560 — the measured boundary,
derived rather than fitted (and 20 at a square 2560², 61 at 1248×1760).

- **Fix** `99e6c39b` (halve on an index ceiling **without** calling it an OOM,
  no `clear_cache`, traceback at WARN, propagate at a single item), `ea6c8409`
  (the formula, and `max_batch_for(shapes)` as the harness hook), `8f5e0a97`
  (ask the impl before the timed section, trim, and report the trim as
  `clamped.reason = "index_limit"` — a pre-capped batch runs **whole**, so it
  stays a priced sample), `0438a3c6` (the probe must stop scoring an
  index-limited batch as "ok", or the fix would re-create the bad boundary),
  `d4089ce7`, `09cfede2`, `bab05a7d`, `26a0ccd9`.
- **Verifier: APPROVED 8/8 with three defects fixed.** The ceiling was charged
  on **every device**, but there is no numel downcast on the CPU path at all
  (`MaxPoolKernel.cpp`: zero `safe_downcast`), so a CPU-budgeted worker would
  have been pinned at 28 and would have asserted a *permanent* execution
  ceiling it does not have — now gated to CUDA/HIP, answering "yes" unless it
  can positively establish otherwise (`1d47384e`); a ceiling-clamped batch that
  executed **nothing** priced as if it ran whole (`ca4cfbcb`); and the hook's
  contract did not say what it cannot see (a per-request `canvas_size` or
  `mag_ratio`). It also marked both invalid bisect boundaries in the probe
  summary. No other shipped impl can reach the ceiling — checked exhaustively;
  the reactive halving covers them all anyway.
- **Leg expectation**: Phase D — the ceiling visible on easyOCR under C7 at
  **28 items ≈ 183 500 800 units**, `clamped.reason = "index_limit"` on the
  measurement, and **no deflation** from it.

### The shape ceiling — the ledger half of the same fact

An `index_limit` clamp was invisible to admission, and worse: the worker's
**throughput-collapse** flag fires on such a batch (200 units trimmed to 28 runs
a fraction of the work at a fraction of the amortization), and that verdict
became a negative sample and deflation — on an empty GPU, forever, on every
batch of similar pages. A second, live defect underneath it: `ClampReport`
required `free_mb`, which a shape clamp carries only by chance, so an
`index_limit` clamp with no reading parsed as **no clamp at all** and the batch
went back into the throughput-knee ring.

- **Fix** `07356690` (the parser: `free_mb` optional, `reason` read, the settle
  line carries `clamped_samples` and `clamped=<none|memory|index_limit|a+b>`),
  `37f5c764`, `535dfc66`: a runtime-only `ShapeCeiling { units, canvas_pixels,
  epoch, observed_at }` on `ModelCalibration`, checked for identity on **read
  and write** (a ceiling recorded under another canvas or epoch is a number in
  another currency), applied as a pure `min` in `admitted_units` at all four
  call sites, refusing the ramp's doubling while it binds, keeping a clipped
  window out of the knee's expiry, suppressing **only** the `index_limit`
  collapse verdict (a real `oom` on the same measurement still deflates), and
  reported as `/health` `shape_ceiling_units` plus one INFO per change
  (`action` = set | lowered | cleared, `cause` = index_limit_clamp |
  canvas_or_epoch_changed | ran_wider_uncut).
- **Verifier: APPROVED, no defects**, with two further checks it did not claim:
  `ran_wider_uncut` really is a statement about what *executed* (the worker
  computes `units` after both clamps trimmed the batch), and the clamp evidence
  is collected before the negative branch, so a clipped batch that then OOMed
  still teaches the ceiling.
- **Known limitation, recorded not fixed**: inside one process the ceiling only
  ratchets **downward** — while it caps admission no larger batch can be
  granted, so a pessimistic report holds until the model is reloaded or the
  canvas/epoch moves. Climbing out would need a knee-style expiry probe.
- **Leg expectation**: Phase D, as above; and no deflation attributable to it
  in any leg.

### The typed transport failure — the last way an unattempted item was lost

A `REFUSED_STREAM`, or any connection error, that survived the client's three
retries returned an **untyped** `reqwest` error, and `classify_item_failure`
acts only on typed failures — so an item that never reached a model was
recorded as a media failure. P2's 300 k pre-fix run produced **456** of exactly
these beside the 381 server-side 400s.

- **Fix** `bfd2018b`: `kind = "transport"` with a `TransportPhase` the client
  types itself at the point it observed the error — `connect` (not a byte
  left), `send` (the connection existed, no response head came of it),
  `headers` (delivered, no head ever arrived), `body` (the server answered and
  **this end lost the answer**). `is_unattempted()` covers the first three;
  `warrants_resubmission()` adds `body`, because the question the job is
  actually asking is *was this item's work left undone?* The phase is
  unforgeable — `parse` sets it `None` whatever a peer's body claims — and no
  status ever buys a retry. A transport failure is also exempt from unit-by-unit
  isolation, which would have cost up to 4n requests against a socket that is
  simply gone.
- **Verifier: APPROVED, no defects**; it re-checked the forgery guard, the
  one-retry-per-item budget (a job of N items still costs at most 2N requests)
  and that `should_retry_status` and the retry budgets are byte-for-byte
  untouched.
- **Leg expectation**: Phase E — a `transport` failure re-queued once, reaching
  `job_failures` only if it fails twice; and the WARN naming `kind` and
  `phase`.

---

## 6. Findings

Everything new in run2, sorted by severity, with run1's identifiers kept where
a finding is the same one. "Fixed" means the fix landed on this branch;
"verified" means a separate verifier signed it off. "User" means it is a
policy, default or design decision and is written up with options.

| Id | Sev | Statement | Status |
|---|---|---|---|
| **P1** | **BLOCKER** | With local inference, extraction jobs cannot be created at all on the shipped configuration: `policy::resolve_effective_host` read only `header::HOST`, HTTP/2 carries the authority in `:authority`, so every h2c self-call is 403 `no_policy` and `POST /api/jobs/data/extraction` answers 500. Two aggravators: the transport probe accepts **any** status, so a peer that 403s every h2c request is still selected as an h2c peer; and no shipped-shape config works around it | **Fixed and verified** in `74ca202c` (+ Desktop parity `4c2e00b6`). Rule: trusted `Forwarded` > request-target authority > `Host` > none, which RFC 9112 §3.2.2 makes *mandatory* on HTTP/1.1 absolute-form and RFC 9113 §8.3.1 makes correct on HTTP/2. **Not yet exercised by a leg** |
| **S1** | HIGH | The job could never put more than **200** predicts on the wire: `hyper`'s default server `SETTINGS_MAX_CONCURRENT_STREAMS = 200`, which `axum::serve` never overrides, over a `hyper-util` pool that shares **one** h2 connection per host — so a pool of 4 is really 1. Window formation then degenerates into `W → 200 − W`, the stable 136/64 cycle, and `max_units_measured` freezes at 136 while the orchestrator asks for 1 632. The number is in no log line, no `/health` field and no ceiling arithmetic | **Fixed and verified** (§5): the 200 is now an assertion in the suite, the server chooses its own limit, the pool is 64 real lanes recruited by load, the gate follows the published figure, and a window forms after the previous one's refills land. Phase A′ measures it |
| **S1b** | MED | A `UnitBudget` **shrink never lands in a saturated job**: `forget_permits` can only take *available* permits and a released permit goes straight to a waiting item task. Harmless while the transport was the real bound; a live bug the moment the header is used to *reduce* pressure on a squeezed GPU, which is what T5 exists for | **Fixed and verified** (`fae83107`): deficit accounting on the release path — 136 releases reach 0 waiters and the budget settles at exactly 64, where the old mechanism leaked 61 |
| **P2** | HIGH | Not a truncation and not the encoder: the predict handler **answered with the request stream still open**, hyper reset the stream, and h2's `num_local_error_reset_streams` — which has no decrement site — reached `max_local_error_reset_streams = 1024` and killed the one shared connection with `GOAWAY(ENHANCE_YOUR_CALM)`, failing everything mid-parse on it at once. Reproduced at **381 / 300 032**, clean over HTTP/1.1, which is why it appeared only after R10' | **Fixed and verified** (`7e96de62`, bound `79488c92`): buffered body, `request_incomplete` typed and re-queued once, a process-wide 4 GiB body budget answering 503. **0 / 300 032** after |
| **F1** | HIGH | R1's knee estimator still fitted a spurious knee on a flat curve and persisted it across a restart: `knee_units = 3` at 14 observations on wd-vit (whose curve is flat: 35.9 items/s at batch 1, 36.1 at 2 048), persisted as 7, and a **fresh** 2 000-item job then ran its whole length between 7 and 31 units. Diagnosis: the plateau was judged against **the ring's own best**, which was the smallest bucket, so the first candidate compared bucket 1 against itself; the expiry then lost the race (widen at 12 clean windows, refit within ~1 s) | **Fixed and verified** in `9cbd6304` (+ `d230d5ba`, `19e2bf9f`, `ce03bd0d`) and six verifier commits: five vetoes, first-window warm-up exclusion, provisional seeded knee, and rule 4's gate re-keyed to the ring's own anchors after the verifier found a death-halved anchor could switch the rule off (`36a8cb77`). Replay from two independent implementations: **no knee at any point** on either wd-vit leg |
| **D1-b** | MED | R7's canvas cap makes every item at or above the canvas price identically, so `plan_batches` cannot tell a 8.7 MP scan from a 48 MP sheet and `eocr.py` padded the mixed batch to the largest member's **raw** dimensions. Bucket size-homogeneity — the criterion run1's S8 PASS rested on — was lost, and the batch was also under-priced against the padding it materialised | **Fixed and verified** (§5): a raw-size tiebreaker among equally priced items, and the detector's inputs bounded before padding. The fix's *quality* half was reverted by the verifier — see the fidelity row below |
| **easyOCR int32 ceiling** | HIGH | CRAFT's first `MaxPool2d` overflows a signed int32 at 29 items of 1824×2560 (`64 × ⌊H/2⌋ × ⌊W/2⌋` output elements against `2³¹ − 1`), and the impl converted the failure into a **slower success** — one ERROR line without a traceback, then per-image reprocessing. Nothing could learn it: no OOM measurement, no deflation, `unit_budget` free to widen past a batch the impl cannot execute; the only symptom was **3.10 → 0.91 items/s**, a 3.2× cliff that looks like a slow window | **Fixed and verified** (§5), on both sides: the impl caps itself and reports `clamped.reason = "index_limit"`, and the ledger keeps a runtime `ShapeCeiling` that bounds admission, stops the ramp doubling and never deflates |
| **Transport failures cost the item** | MED | A connection error surviving the client's three retries returned an untyped `reqwest` error, so `classify_item_failure` — which acts only on typed failures — recorded an item that was never attempted as a media failure. 456 such in P2's 300 k pre-fix run | **Fixed and verified** (`bfd2018b`): `kind = "transport"` with a phase the client types itself and no peer can forge; `warrants_resubmission()` buys the same single re-queue as a worker death |
| **The h2 constants, and why** | INFO | The fix round chose four numbers that will outlive this run: server **`MAX_CONCURRENT_STREAMS = 512`** = 8 × the 64 streams our own client offers one connection, chosen above every common server default (nginx 128, Envoy 100, hyper 200) so this server is never the tightest limit in a chain, with the 8× for a proxy fanning several clients onto one upstream; **`INFERENCE_CONNECTION_LANES = 64`**, the multiplier that makes the gate ceiling (`lanes × 64 = 4 096` requests) reach the job's own `in_flight_unit_ceiling`, since for an image tagger one unit is one request; the **gate `[256, 4096]`**, floored at the constant every existing deployment already runs at so the change can only ever raise it; and **`PREDICT_BODY_LIMIT = 2 GiB`** (the largest legitimate *single-input* request, since `check_frame_budget` already refuses anything above it) beside a process-wide **4 GiB** aggregate — one gateway job's item bytes are bounded by `intermediate_data_budget_mb` at 1 GiB, four gateways against one GPU box is the deployment this exists for, and `4 = 2 × 2` so a maximal body can always be admitted beside another | **Recorded**; all four are named constants with their derivations in doc comments, and three are on `/health` |
| **C6** | MED/HIGH | Under a hard squeeze (4 GB free) the R5 reserve cap admits **60× bigger batches** and wd-vit runs at **0.56×** run1's throughput (21.85 vs 38.98 items/s, p50 5 323 vs 3 199 ms) with nothing in the ledger noticing — and nothing could: the knee compares log2 buckets *within one run*, and this run never ran wd-vit small on a squeezed GPU to compare against. Safe (0 OOM, 0 negatives, 0 unsafe grants); the grant is 3 040 MiB against 3 085 MiB of headroom, so the allocator has nothing to spare | **User.** Expected behaviour of an approved change, not a regression of correctness (run1 issued a *blind* grant in the same place). Options below |
| **S4a-A2** the live clamp double-counts our own pool | MED | On a pressured GPU the **worker's live clamp**, not the ledger's margin, is the binding constraint — and it compares NVML's free reading *after* our own caching allocator took its pool against a grant the ledger already charged us for. Our footprint is priced twice: `free memory fell to 4371 MiB against a 7729 MiB grant; shrinking this batch's budget from 147 to 83 units`, twenty-two identical clamps in one window, **44 % of the grant unused with nothing external moving**. It is run1's **T10** ("the clamp has no margin of its own and ran at 98.2 % of free nine times") with a number, and run1's S4d pool-accounting clamps at 36 GB free are the same shape | **Open, user decision.** Not a regression and not unsafe — the clamp is what kept every run1 profile safe — but in run1 the ledger was the binding constraint and now the clamp is, so the cost is paid on every pressured window. Options below |
| **SGLang restarted mid-run** | INFO | At **21:59 UTC on 2026-09-04**, between the S4a leg and the S4d leg, the user recreated `dsv4-flash-sglang` on this host; it took **77 702 MiB on each GPU**. Evidence that it was the user and not the run: the user has been logged in since 20:09 UTC, the SGLang bench directory was modified at 22:02, there is no timer or cron behind the container, and both GPUs read 2 MiB before every leg the run started and after S4a. The run **did not stop it again** — SGLang is the user's service, and the brief makes stopping and starting it the orchestrator's, not a leg's | **Not a fault.** Consequence: S4b, S4d, S4g, Phase D and Phase E are **blocked, not failed**, and §4.5/§4.7/§4.8 carry their runbooks |
| **F1 residue** | MED | **MobileCLIP's `knee_units = 127` is not restored** under `KNEE_PLATEAU_BUCKETS = 2`: the recorded ring has exactly one quiet bucket above the bend, because the ramp stalled at 136 units for S1's reasons and nothing at 256 was ever measured. Two observations there and the same ring answers 127 (pinned by a test). The knee was worth nothing measurable either way: 0.94× master with it in run2, 1.00× without it in run1 | **User** — a one-line constant. Options below |
| **C2** | MED | A **trusted** `oom_class` leaves no trace: `oom_verdict` returns `Trusted` for `typed_exception` and `marker` and logs nothing, and `oom_class` never reaches `panoptikon.log`, `/health` or `analyze.py`. So the plan's clause "`oom_class.source` present on the OOM measurements" is only verifiable for the *vetoed* case (5 954 marker-classified OOMs in `S5-oomtimed` with zero mentions of `oom_class`, against one mention in `S5-oom2nd`) | **Fixed and verified** (`672aa85a`, `18f2aa1b`, `62a092c9`): one INFO line per OOM negative naming `source`, `exception`, `trust`, `free_mb_at_failure`, `grant_mb` and `oom_samples`, and `analyze.py`'s `failures` row tallies `source/trust` pairs. Both options were taken |
| **C3** | MED | The `message_pattern` veto fired on a **genuine** OOM. Both rules are individually right (the measurement was vetoed at `free_mb_at_failure 96 518 ≥ grant.mb 96 356`; the error frame's `INFERENCE_OOM_WINDOW` marker deflated 61 µs later), but on an **idle GPU the veto will fire on essentially every `message_pattern` OOM**, because `grant.mb` is the whole GPU. The tier is load-bearing only on a tight GPU — which is where real OOMs happen, so it is defensible — but a model raising bare driver text *without* an `INFERENCE_OOM_*` prefix (a batch-1 failure inside `run_single`) would then not deflate at all | **User / watch.** A fixture artefact on an idle GPU; worth a look before the soak |
| **C4** | MED | A job's `failure_reason` loses the cooldown detail **and the model id**: `"Failed to load model: model load failed on all 1 inference endpoints"`, where `/health.load_cooldowns[]` and the 503 body both carry `inference_id`, `failures`, `retry_at`, `retry_after_secs`, `window_secs` and the last error. Two causes: `inference_pool` renders any endpoint failure as `inference request failed (<status>): <short detail>`, discarding the structured body; and the job's retry cadence meant the second attempt arrived after the 2 s window and got a plain 500 from a real load | **Fixed and verified** (`20bd1536`): the most informative endpoint error wins, the context names the model, and the reason is either the structured cooldown or the whole cause chain. The retry-cadence half is now harmless, because the plain 500's own detail survives to the record |
| **C5** | LOW | A systemically failed job logs that its re-queued items "then completed": the branch is `else if guard.requeued_items > 0`, reached whenever there is no *partial* reason — which includes `JobFailure::Systemic`. Two statements later the same code writes `outcome: "failed"`, `failed_items: 2000` | **Fixed and verified** (`f153a739`, prose corrected in `79488c92`): the sentence is built from the same two inputs the outcome is chosen from, so the two cannot disagree |
| **C7** | INFO | Two idle residents were trimmed in the same millisecond (6 µs apart) where run1 trimmed one, which is why the flag latency reads 2.888 s rather than 1.837 s. Both released, 6.0 and 9.4 ms round trips | **Not a fault** |
| **`unstated` unexercised** | INFO | R11's sentinel rename has **no producer on this host**: all four shipped models and every fixture resolve a dtype by inference, so the string appears 0 times in twenty-six legs. `oom_class.source = "typed_exception"` is likewise unreachable — no fixture raises `torch.OutOfMemoryError` itself | **Open**; needs a model whose weights state no precision, and a fixture that raises the typed exception |
| **`CostHealth` omitted the canvas** | LOW | `/health`'s cost block did not report `canvas_pixels`, so the D1 leg confirming which canvas was in force had to read the load-time DEBUG line. It also meant `last_grant_units` was ambiguous: under a canvas the same unit budget describes a very different batch | **Fixed** (`feb0a5a9`, and `2e5c782b` for the grant log line). The spec, the `ui` types and the gitlink moved with it, at the second integration pass |
| **easyocr `enable_batching = false`** | MED | The three `easyocr_*` ids still ship it, so their worker takes the grantless path, reports no `units` and **fits no slope**. D1 measured both halves: **C7 (batching on) fits 0.0014796440700825149 on 5 samples, byte-identical to run1's**, while the shipped registry produced **183 and 205 grants, `local_samples 0`, `max_units_measured 0`, `unit_budget` pinned at the 2 000 000 seed and no store file at all** (D1-d) — every grant the whole headroom, which is exactly the shape that produced the soak's 23–94 GB grants once `external_mb` went stale | **User** — run1's F-B/F-F, and now measured on both sides |
| **D1-a** P2 correlates with the request shape | MED | Across eleven D1 legs and 5 060 segments, **4 items** were lost to P2 — 3 in `S8-ocr-C7-cold` inside one second, 1 in `S8-ocr-C7nc` — and **only** on the `enable_batching = true` legs, which send 28–85-item multipart bodies containing 8000×6000 images. The four legs whose windows hold 1–4 items lost nothing | **Closed with P2**; the correlation is consistent with the reset-accounting mechanism (stream churn on one connection) rather than with any file |
| **D1-c** `utilization`/`slope_accuracy` were uninterpretable for easyOCR | MED | The run1 probe was measured through the **shipped** registry (`enable_batching = false`, memory flat in batch size), and run2 added a second incommensurability: the probe's units were **raw** pixels (epoch 1), the ledger's **capped** (epoch 2). `slope_accuracy` FAILed at ratios of 194–382 and `utilization` read 1.07 on a recording whose `grant_safety` and strict `ledger_invariant` both PASS | **Resolved by the probes** (§4.4, §8): with a C7-registry probe on the matching image group the ratio is **1.0000**, and the honest band for the leg is 0.001044 (all above-canvas) … 0.001480 (all below-canvas) |
| **D1-e** a bad user registry takes the whole registry down | INFO | A C7nc registry file missing `allow_override = true` was rejected **whole**, so both redefinitions were lost *and* the shipped entries did not take over; the extraction job was created and then sat at 0 items with `failed to resolve external inputs` | **Not a defect** (the key is documented) — but the failure mode is silent from the job's side. Directory kept as `S8-ocr-C7nc-aborted-registry` |
| **D1-f** one collapse negative and one oracle breach under host pressure | INFO | `S8-ocr-C7-calm-b` recorded 1 `throughput_collapse` negative and 1 `oracle_agreement` breach (10 249 MiB, 1 of 400 joined samples), both inside one window while the host was reclaiming memory. Every other leg: 0 negatives, 19 MiB worst disagreement | **Not a fault**; it is the strongest argument for the `host_pressure` instrument gap (§9) |
| **nemotron's memory is aspect-ratio, not area** | MED | `img-4mp` (2048²) and `img-1mp` (1024²) produce **byte-identical** `delta_mb` at all seven batch sizes, and so do `img-0.3mp` (640×480) and `img-20mp` (5200×3900) — pairs that share an aspect ratio and differ in area by 4× and **66×**. `dynamic_preprocess` picks a tile *grid* from the aspect ratio (`max_input_tiles = 6` plus a thumbnail): 1:1 → 5 tiles → 280.5 MiB/item, 4:3 → 7 tiles → 392.3 MiB/item. Pixel count enters nowhere. In **capped** units the per-unit cost still spans **8.35×** across the pixmix corpus (0.000153 … 0.001280): the canvas closes the top of the range, nothing closes the bottom, because a 0.3 MP 4:3 thumbnail costs the same seven tiles as a 20 MP 4:3 sheet | **User.** Not a defect and nothing changed; it is why a single "slope" for a tiled VLM is under-determined and why `slope_accuracy` must compare group with group. **Tile-based pricing** (`tiles × 512²`) would be exact and is a design change |
| **The shape ceiling ratchets only downward** | MED | While a ceiling caps admission no larger batch can be granted, so `units > ceiling` arrives only from a window granted before the ceiling existed or after a canvas/epoch change. A pessimistic report — a mixed batch whose one oversized page sets the padded frame — therefore holds until the model is reloaded. Denominating it in *units* keeps the cost small for a `sum` model (`B × 16·H·W ≤ 2³¹` ⟹ `units ≲ 2³¹/16`, near shape-invariant) | **Open by design**, recorded as the design doc's "Known limitation". The way out is a knee-style **expiry probe** — one deliberately over-wide window every N, which the impl trims harmlessly — which is not implemented |
| **The easyOCR fidelity trade** | MED | Bounding easyOCR's inputs by the canvas before padding also bounded the array the **recogniser** crops from, at ~0.32× resolution on an 8000 px sheet, with `min_size = 20` silently meaning 62 raw pixels. Measured on CPU with the real weights: at 30 px text the bounded arm returns `PANOJIIKCN CALIURAIION` where the raw arm reads `PANOPTIKON CALIBRATION`, and recovers 8 boxes against 11 — while the recogniser's device tensor is `(N, 1, 64, W)` either way, so the regression bought nothing the ledger prices | **Decided by the orchestrator on the user's behalf, and flagged for confirmation.** The standing rule is that a silent quality regression is a user-facing design change, not a fix: the verifier was required to restore recognition-from-raw if the recogniser's memory is box-bounded, and it is. Shipped behaviour = run1's transcription |
| **`codemap.md` drift** | MED | A mechanical check at the tip (compare each `file:line` reference's target text at `65fd2f82` against the tip) flags **68** references whose target has changed — `http.rs`, `worker.rs`, `dispatch.rs`, `manager.rs`, `calibration.rs`, `packing.py`, `utils.py` and some of `extraction.rs`. Many will be benign, but the map's value is that its references resolve, and the fix round moved a great deal of code | **Open**; a dedicated re-resolve sweep is running. The second integration pass deliberately re-resolved only what the commits under its review moved (`34a591aa`) |
| **epoch bump** | INFO | All seven shipped `pixel` ids carry `metadata.cost.epoch = 2` (`doctr/dots_ocr`, three `doctr/easyocr_*`, `clip/qwen3-vl-embedding-{8b,2b}`, `clip/nemotron-embed-vl-1b-v2`), because a canvas re-denominates what one unit *is* and the profile key does not carry it. Any run1 profile row for them is **ignored, not migrated** | **Intended.** A run2 leg on those models starts from an empty profile even on this host |
| **`ui` submodule unpushed** | MED (release gate) | **Two** commits on `batch-calibration-ui` are now unpushed — `9b28044` (the run2 spec) and `8abf631` (the fix round's transport health, body budget and shape ceiling). The gitlink resolves only from this host's clone: the image rebuild had to add a `localsrc` remote pointing at it, because `origin` answers `upload-pack: not our ref 8abf631b…` | **User must push** to `reasv/panoptikon-ui` |
| **B16 premise falsified** | INFO | Track E's arithmetic reason for a fixed 256-request gate — "4 096 units ÷ 64 units per request = 64 concurrent requests" — is false. `REQUEST_UNIT_BUDGET = 64` is a chunk bound *within one item's work units*; an image item has exactly one, so a job sends **one item per request**: 1 999 requests for 2 000 items. 4 096 units is 4 096 concurrent requests for exactly the image taggers and CLIP embedders G7 exists for | **Recorded**; it is why the S1 fix changes the gate |
| **S2 `persistence` reports INFO** | INFO | `S2-minilm` and `S3-wdvit` queued only `fit_changed`/`knee_changed` updates inside the recording, never an `anchor_advanced`, so the check has no advance→write delay to measure. Both stores were written | **Not a defect** in product or tool; flagged so the tables are not misread |

### The user-decision items, with options

| Id | Options | Orchestrator's recommendation |
|---|---|---|
| **C6** (throughput under a hard squeeze) | (a) accept and document: the reserve cap trades throughput on a tight GPU for admission that is priced rather than blind, which is strictly safer; (b) make the ledger *notice* — compare a squeezed GPU's throughput against the same model's own unsqueezed buckets rather than only within one run, so the knee/comparator can see it; (c) bound the admitted batch on a GPU whose headroom is within a small multiple of the grant, i.e. treat "grant ≈ headroom" as its own regime; (d) re-tune the 1 024 MiB cap | (a) for this release, with (b) recorded as follow-up work. The failure the cap prevents (memory-blind grants at `limit_mb = 0`) is worse than a slower batch, and run1's 38.98 items/s was bought with grants the ledger could not price at all |
| **S4a-A2** (the clamp double-counts our own allocator pool) | (a) **subtract our own pool in the worker**: add `torch.cuda.memory_reserved − torch.cuda.memory_allocated` back to the live free reading before comparing it against the grant — the worker knows its own pool exactly, and the ledger has already charged for it; (b) **price the grant net of the pool** on the host, so the clamp's comparison is apples to apples from the other side; (c) accept the 44 % and document that a pressured GPU runs at the clamp, not at the grant | (a) is the smaller change and is local to the process that owns the number; (b) is the more principled one if the ledger should always describe *new* memory. **User's call** — either changes how much a squeezed GPU admits, which is exactly the regime R5 was approved for |
| **F1 residue** (`KNEE_PLATEAU_BUCKETS`) | (a) keep **2**: a plateau resting on one comparison between two medians is not a plateau, and MobileCLIP's knee is found late rather than lost (two observations at 256 units restore it); (b) set **1**: restores MobileCLIP's 127 on its recorded ring, and wd-vit is *still* refused, by rules 1 and 2 | (a). The asymmetry the whole design rests on points this way: a false negative is a knee found late, a false positive is F-A. The constant is named and its derivation is in the doc comment, so (b) is one line if the user prefers the literal acceptance criterion |
| **C2** (trusted `oom_class` invisible) | (a) one `debug!` on the trusted arm naming source, exception and `free_mb_at_failure`; (b) an `oom_class` column in `analyze.py`; (c) both | (c) — **done**: the INFO line and the `analyze.py` tally both shipped in the fix round |
| **C3** (the veto's real reach) | (a) corroborate against the *GPU's* free memory rather than the grant's envelope, so an empty GPU does not auto-veto; (b) keep the rule and require impls to prefix driver text with `INFERENCE_OOM_*` (which `packing` already does on the window path); (c) accept: the tier is load-bearing exactly where it matters | n/a — the outcome was right on every measured leg; the risk is a batch-1 failure inside `run_single` |
| **C4** (job failure reason) | (a) carry the structured 503 body through `inference_pool` instead of rendering it to a string; (b) have the job's abort path read `/health.load_cooldowns[]` for the model it failed on; (c) accept the generic text and point users at `/health` | (a) — the body already carries every field the reason wants |
| **easyocr `enable_batching`** | (a) flip it to `true` on the three ids now that the host caps their pricing **and the impl bounds its own detector batch**, so they accumulate fit samples; (b) keep the grantless path and treat "thousands of grants with `fit samples 0`" as a warned condition (run1's F-B option) | (a) is now better supported than it was: D1 shows C7 (batching on) fits a slope byte-identical to run1's and PASSes `grant_safety`, and the int32 ceiling that made a large batch silently slow is fixed and reported. (b) leaves the shipped ids issuing whole-headroom grants forever |
| **`CostHealth` canvas** | (a) add `canvas_pixels` to the cost block and accept the spec/UI regen; (b) leave it to the log lines | (a) — **done** in `feb0a5a9`, together with the grant log line (`2e5c782b`) |
| **S1 gate policy** | (a) the fix in §7: real N-connection pool, an explicit server stream limit, and a gate that follows the published desired-in-flight figure with a floor; (b) keep the fixed 256 gate and accept that job-driven calibration is capped there; (c) F1-only (an explicit `max_concurrent_streams`) and leave the gate fixed | (a) — **done**. It is a deliberate reversal of a Track E decision made on an arithmetic premise the leg falsifies, and it is what lets a 97 GB GPU be calibrated by a job |
| **nemotron's pricing** | (a) accept capped pixels and document that a tiled VLM's per-unit cost still spans 8.35× on one corpus, comparing group with group whenever a slope is judged; (b) **price by tiles** — the grid `dynamic_preprocess` picks from the aspect ratio, times 512², which is what the model actually materialises; (c) key the profile by aspect-ratio class | (b) is exact and is a design change (a new cost dimension for the VLM class, and an epoch bump). (a) is what run2 ships; the number to quote is the 8.35×, not the 66× |
| **easyOCR fidelity** (taken, needs confirmation) | (a) bound only the **detector's batch tensor**, recognise from the raw image — run1's transcription, at the cost of the raw array staying resident (it is resident anyway: `decode_image_inputs` decodes at full resolution before any impl code runs); (b) bound everything, accept the transcription loss, save nothing the ledger prices | (a), and it is what is shipped. Recorded here because the orchestrator decided a **user-facing quality question** on the user's behalf, under the rule that a silent quality regression is a design change rather than a fix |
| **The shape ceiling's expiry probe** | (a) leave it ratcheting downward within a process (a reload, a canvas change or an epoch change is the only reset); (b) add a knee-style probe — one deliberately over-wide window every N, which the impl trims harmlessly — so a pessimistic ceiling can be retired | (a) for this release; (b) is the honest fix and it is a ledger design change, recorded as a known limitation in the design doc |

---

## 7. Decisions taken by the orchestrator during run2

Recorded from `orchestrator-state.md` (entries from *RUN2* onward), with the
reasoning as it was given at the time.

| Decision | Reasoning |
|---|---|
| **Pin the cross-track wire and store names in the common brief** before any track started (per-batch `free_mb`/`free_source`, `clamped{from_units,to_units,free_mb}`, `oom_class{source,exception,free_mb_at_failure,device}`, the `unstated` sentinel, `metadata.cost.canvas_pixels`, the 503 `load_cooldown` body and `Retry-After`, the `worker_died` kind) | Four agents were working in one checkout with no way to message each other mid-task. A name invented twice is a merge conflict at best and a silent wire mismatch at worst; pinning them made every track's half of a two-sided change compile against the other's without coordination |
| **R5: the confidence widening stays capped** with the base reserve, rather than capping only the base and leaving the widening uncapped | The user's rule is a flat bound ("at most 1 GB is ever withheld"). The widening multiplies *other processes'* usage, so it vanishes on a headless GPU where our fit is equally untrustworthy — it is a conservatism knob about somebody else's memory, not a safety term against fit error. The real protections (geometric ramp, extrapolation ratchet on local samples, the worker's per-batch clamp) are untouched. And an uncapped widening reproduces T4: `limit → 0` produces **memory-blind grants**, which is strictly less safe than a slightly smaller reserve |
| **`KNEE_PLATEAU_BUCKETS = 2` stays**, accepting that MobileCLIP's 127 is not restored on its recorded ring | One bucket above the candidate is a single comparison between two medians — the same "two points are not a curve" objection `MIN_KNEE_BUCKETS` answers for the fit as a whole. Two means the flat stretch spans a factor of ≥ 4 in batch size. The shortfall is an artefact of the leg (the ramp stalled at 136 for S1's reasons, so nothing at 256 was measured), the knee bought nothing measurable on either run, and the asymmetry is deliberate: a false negative is a knee found late, a false positive is F-A |
| **C6 is classified as expected behaviour, not a regression** | Run1's 38.98 items/s at 4 GB free was bought with `unit_budget = 1` and `mb = 0` — a memory-blind grant, the condition R5 was approved to remove. Run2 is slower and *priced*: 0 OOM, 0 negatives, 0 unsafe grants. It is a real cost and it is recorded as one, but it is the approved trade, not a defect |
| **The S1 fix scope**: a real N-connection pool (independent clients, least-loaded) + our server advertising an explicit stream limit consistent with the gate + the gate following the **published** desired-in-flight figure (floor 256; HTTP/1.1 fixed 256) + letting refills land before window formation + S1b deficit accounting + `/health` exposing the desired-in-flight figure and a queue-bound counter + a stub test that pins the stream limit. **Rejected**: advancing the ramp on grant fill | Every element removes one layer of the "a ceiling no layer can name" problem: the pool becomes what its constant says, our own server stops imposing a limit our client does not know about, the gate stops being a fixed number chosen on falsified arithmetic, and the counter turns the next occurrence into a five-minute diagnosis. Ramping on grant fill was rejected because it would advance the anchor on evidence no batch actually produced |
| **R6's admission gate is per GPU, not global** | The R-table calls it a *GPU*-admission gate, VRAM is a per-GPU quantity (a global gate serializes loads that cannot interact), and it is a strict superset of the old behaviour on every shipped configuration: every unpinned model resolves to the same default GPU, so `max_concurrent_loads = 1` reproduces the retired global lock exactly |
| **A new `canvas_pixels` key on the `load ok` response**, parsed host-side and used only where the registry declares nothing | `doctr/dots_ocr`'s ceiling lives in an `AutoProcessor` config downloaded with the weights and is not a fact the registry can state truthfully across model revisions. Making the worker report what it found — behind the registry, never over it — is what makes the introspection tier load-bearing rather than decorative, while keeping a maintainer's declaration correctable from the one place a maintainer can act |
| **Declare the canvas only where it prices something**, and record the rest as comments | For an `item`/`count` or `none`-class model a canvas is inert by construction (`min(1, cap)` is 1), so ~40 live declarations would be lines that look like they do something and do not, each costing a DEBUG line. The numbers themselves are preserved in comments (openclip 224²…448², WD v3 448², Florence-2 768², `db_resnet50` 1024²), one line from being live if an id is ever reclassified to `pixel` |
| **Bump `metadata.cost.epoch` to 2 on all seven shipped `pixel` ids** | The profile key carries `unit`/`aggregation` but **not** the canvas, so a run1 slope in MiB per *raw* pixel would keep matching and be applied to capped units — it under-predicts, which over-admits, the one direction the design says the ledger cannot absorb. `epoch` is the documented lever for "memory behaviour changed without moving a key component" |
| **Type the worker-death marker instead of matching a substring** (`slot_error::Unattempted`) | Track E's death signal was the substring `"failed fatally"`, which matched one rendering of six. A typed marker at the three sites that cover all six shapes makes the classification structural, and `fail_requests` is the single funnel where four of them land |
| **Take the C1/C7 policy workaround as a *tool* change, and only for C1 and C7** | Phase A could not run a single job leg otherwise, and the alternative was to stop the run for a product fix. Scoping it to the two configs that need it leaves C0/C2/C3 and the compose overlays in the shipped shape, which is what makes the P1 fix testable by a later leg instead of permanently papered over. It was removed (`ea59a63b`) the moment the fix was in the binary |
| **Refuse the easyOCR fidelity trade, and say so out loud.** The D1-b fix agent bounded easyOCR's inputs by the canvas unconditionally and recorded, honestly, that transcription on > 2560 px images would change; the verifier was instructed to **restore recognition from the raw image if the recogniser's memory is bounded by the box rather than by the page**, and only to escalate as a user decision if it is not | A quality regression is a user-facing design change, not an implementation detail a calibration run may take in passing. And here it was not even a trade: the recogniser's device tensor is `(N, 1, 64, W)` with `W` set by crop aspect ratio, so it does not depend on the page's resolution at all, while the "7.3× under-pricing" the fix report cited was **host RAM** (and the raw array is resident regardless, decoded before any impl code runs). The measurement settled it — same tensors in both arms, garbage transcription in one. **This decision is flagged in §6 for the user to confirm**, because the orchestrator made a call about output quality on the user's behalf |
| **A per-request body limit is not a memory bound; require a process-wide one** | The S1 fix set `PREDICT_BODY_LIMIT = 2 GiB` and stated the residual honestly (`512 streams × 2 GiB`). That product is not a ceiling at all — nothing bounds how many *connections* a peer opens — so the verifier was directed to decide a byte budget rather than leave a number nobody can act on. It chose **`PREDICT_INFLIGHT_BODY_BYTES = 4 GiB`**, charged before the bytes are read, refusing (never waiting) with 503 + `Retry-After` + `kind = "body_budget_exhausted"`, and put it on `/health` — because a bound nobody can see is indistinguishable from a bug, which is the whole lesson of S1 |
| **Derive the transport constants from something, and write the derivation on the constant** | Every number the fix round introduced is a number some future leg will have to explain: `MAX_CONCURRENT_STREAMS = 512` (8 × our own 64 streams per connection, above nginx's 128, Envoy's 100 and hyper's 200, so this server is never the tightest limit in a chain, with the 8× reserved for a proxy fanning clients onto one upstream); `INFERENCE_CONNECTION_LANES = 64` (the multiplier that makes the gate ceiling reach the job's own in-flight ceiling of 4 096 requests); the gate floored at the **old** constant 256, so the change can only raise it; `PREDICT_BODY_LIMIT = 2 GiB` derived from the largest legitimate *single-input* request rather than from "64 inputs × the largest input", which bounds nothing; and 4 GiB = 2 × that, so a maximal body is always admissible beside another. S1 existed because 200 was a number nobody had chosen |
| **Build a connection lane's client only when the lane is recruited** | 64 eager `reqwest` clients cost a **measured** 58.6 MiB of RSS per inference endpoint, allocated the first time anything touched it, for lanes `pick_lane` will not recruit below thousands of concurrent requests. Lane 0 and the HTTP/1.1 client stay eager so "can this process talk to this endpoint at all" is still answered at registration. After the change: 13.0 MiB for the first endpoint in a process (11 of it one-off TLS init), **1.4 MiB** per further endpoint, ~620 KiB per recruited lane |

---

## 8. Ground truth measured

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

### The run2 ceiling probes, verbatim (`results/run2/probes/summary.md`)

Run1's probe table (`docs/batch-calibration-run1-report.md` §6) is unchanged
for the item and token models. The pixel models were **re-probed under the
canvas**, because `ceiling_probe.py` now prices its pixels at the canvas it
resolves (`893e8a7d`, `9be09e5e`, `18cc2e77`, `6d074f3d`) and the seven `pixel`
ids carry `epoch = 2`:

| # | Model / registry | image group | canvas in force | slope (MiB/unit) | base | OOM boundary | ledger comparison |
|---|---|---|---|---|---|---|---|
| 1 | `doctr/easyocr_standard_en`, `registry-C7` | `scan-2480x3508` (above canvas) | 6 553 600 | **0.0010438024347478694** (n=5, res 0.70) | 796 nvml / 805 free-delta | **28 ok / 29 fails** (bisect said 37/38 — contaminated) = 183 500 800 units | ledger `S8-ocr-C7*` 0.0014796440700825149 → **1.4176×**; `S8-ocr-C7-calm-b` 0.0009402463669844309 → **0.901×** |
| 1b | same, `scan-1240x1754` (below canvas) | | 6 553 600 (not binding) | **0.0014796440700825149** (n=5, res 1.67) | 796 / 805 | not probed | ledger 0.0014796440700825149 → **1.0000×** |
| 2 | same, `registry-C7nc` (`canvas_size = 40000`) | `scan-2480x3508` | 1 600 000 000 = uncapped | **0.0007862976372397235** (n=5, res 0.70) | 796 / 805 | 37/38 (same contamination) | ledger `S8-ocr-C7nc-b` 0.0007543529536175378 → **0.959×** |
| 3 | `clip/nemotron-embed-vl-1b-v2`, shipped | `img-20mp` (above canvas) | 1 835 008 | **0.00021419819386350598** (n=8, res 2.22) | 3 788 / 3 797 | **320 ok / 384 OOM** = 587 202 560 units | see below |
| 3b | same | `img-1mp` (run1 control) | 1 835 008 (not binding) | **0.00026783375512985956** (n=9) | 3 788 / 3 797 | run1: 468/470 | **byte-identical to run1's probe** |
| 3c | same | `img-4mp` | 1 835 008 | **0.0001532781691778274** (n=7) | 3 788 / 3 797 | — | — |
| 3d | same | `img-0.3mp` | 1 835 008 (not binding) | **0.0012799685846560846** (n=7) | 3 788 / 3 797 | — | run1 `S8-pixmix` 0.0011603882303639847 → **0.907×** |
| 4 | `clip/qwen3-vl-embedding-2b` | — | — | **NOT RUN** — `Qwen/Qwen3-VL-Embedding-2B` is absent from `~/.cache/huggingface/hub`, nothing downloaded | | | |

**The rule this table establishes: a `pixel`-priced slope is only comparable
to another slope measured on the same image group.** Neither of run1's
disagreements survives the correction. `S8-ocr-C7`'s stored `sample_units` are
every one a multiple of 2 174 960 — windows made **only** of `scan-1240x1754`
pages — and the probe on *that* group returns the ledger's stored slope to
seventeen digits and its `sample_reserved_mb` to the megabyte. run1's
`S8-pixmix` `sample_units` are all multiples of 307 200 — windows made only of
the 640×480 items — and its fit is **0.907×** the `img-0.3mp` probe; the
published 4.33× had divided it by the 1024×1024 probe. So run1's ledger was
right in both cases, and D1-c's "uninterpretable" verdict is resolved: it is
interpretable, and it passes.

The 1.4176× on row 1 is the canvas price itself, not error: `min(raw, 2560²)`
charges an above-canvas item for a **square** canvas, while an
aspect-preserving fit of a 1:1.414 page onto it occupies 1824 × 2560 =
4 669 440 px — a built-in **1.4035×** conservatism, times the small group's
1.0099× padding round-up = 1.4174 of the observed 1.4176. Per **padded** pixel
the two groups agree to four decimals (0.0014651 vs 0.0014650). The honest
comparison band for a C7 leg is therefore **0.001044** (all above-canvas) …
**0.001480** (all below-canvas).

Two cautions the probes attach to their own numbers. The **bisect boundaries
for easyOCR are invalid, not noisy** — both report `largest_ok_items: 37`
against a true 28, because the impl absorbed the index-limit failure and the
probe scored the per-image fallback as "ok"; `ceiling_probe.py` now refuses
such a batch (`0438a3c6`) and `summary.md` is annotated, but `analyze.py`'s
`utilization` reads exactly that field. And a **warm-allocator sweep overstates
the requirement by up to 1.8×** (easyOCR at batch 12: 79 864 MiB warm vs 43 964
MiB cold): the warm figure is what a long-lived worker produces and is the one
comparable to a ledger fit, but a boundary must come from the cold series —
easyOCR's cold curve is convex-down, so a linear fit over-predicts its true
boundary by **+98 %**, well outside run1's 17–36 % band. nemotron's +30 % at
its own boundary is inside it.

---

## 9. Protocol assessment

### What the legs caught that the tracks, the verifiers and 1 648 unit tests did not

| Finding | How it was caught |
|---|---|
| **P1** | Starting a gateway on the shipped-shape config and submitting one real extraction job. Every unit test of the h2c client talks to a service with no policy layer in front of it; every unit test of the policy layer builds its request with a `Host` header |
| **P2** | 8 000 real items through the real encoder. One failure in 8 000 is invisible to any test that sends tens |
| **F1** | Comparing the fitted knee against the model's **measured curve**, per model, exactly as the protocol's own S2 rule says ("does the knee match the curve", never "is the knee small") — and then restarting on the store it wrote, which is the only way to see a knee that is wrong *and* permanent |
| **S1** | Reading `queue_depth + last_window_items` across 70 windows on two independent legs and noticing that the two phases of the cycle sum to exactly 200 every time. No panoptikon-side bound is 200 |
| **C6** | Running the same model, same hog, same client shape as run1's leg, and comparing throughput rather than only safety |
| **C2/C3** | Asking the recordings to *evidence* a field the change set added, rather than asserting the code path exists |
| **D1-b** | Reading `plan_batches` and `pad_images_to_same_size` **against each other** after R7 changed what "largest first" sorts on. The batch histograms are consistent with it; the timings only suggest it; the source settles it |
| **The easyOCR int32 ceiling** | Wrapping `logging.Logger.error` from *outside* the impl during a probe sweep, because the impl discards the traceback. Then fresh single-batch runs at 28…37 items, where the bisect's own answer (37) was an artefact of the fallback it could not see |
| **The nemotron tile finding, and both group mismatches** | Probing **four image groups** of one model instead of one, and then noticing that two pairs of groups produce byte-identical memory at every batch size |
| **D1's throughput non-result** | Recording `/proc/meminfo` and `nvidia-smi` utilization beside the job and refusing to publish a ratio the host had decided. Run1's own 8.56× headline inherits the same instability, which only the second measurement of the same baseline could show |

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
| `c4d6fd3c`, `0f8d04d7` | **C7nc**, the uncapped control that separates R7's per-item cap from the `enable_batching` flag: C7 with `metadata.cost.canvas_pixels` removed and ports moved. After the D1-b fix the registry key alone no longer frees it — the impl states its own canvas and the worker reports it on the `load ok` response — so `config.canvas_size = 40000` was added, chosen to exceed every corpus image's longest side while staying inside the `Option<u32>` the wire carries. Both file headers say it is a **diagnostic, not a proposed configuration** |
| `6d074f3d` | `ceiling_probe.py::load_registries` **deep-merged** registry documents key by key, where `registry.rs::load_file` **replaces** an inference id wholesale. So `registry-C7nc.toml`, whose entire purpose is to omit the canvas, still resolved to the shipped 6 553 600 — probe 2 would have measured the capped configuration and reported it as the uncapped control. Caught on the dry run |
| `0438a3c6` | The probe scored an index-limited batch as "ok", which is how both easyOCR bisects reported 37 against a true 28. `ran_whole_batch()` now replaces `ok and not oom` at all four decision points, and a failing probe is recorded under `first_index_limit_items` |
| `62a092c9` | `analyze.py`'s `failures` check tallies the OOM tier lines by `source/trust` and names any unattributed negative rather than rounding it away |
| `362ec437` | `healthrec.py`'s `flatten_health` dropped every section the run2 legs are judged on — `inference_clients[]` (transport, pool, lanes in use, gate), `load_cooldowns[]`, `predict_body_budget`, the GPUs' `reserve_mb`/`reserve_rule`, the workers' `shape_ceiling_units` and the models' `desired_in_flight_items`, `queue_bound_windows` and `cost.canvas_pixels`. Without it Phase A′ could only read them out of `--full`'s raw payload |

### Instrument gaps still open

- **`analyze.py` has no `reserve_rule`/`reserve_mb` or `knee_expired` column.**
  Every Phase C measurement of those fields came from `grep`, and so did Phase
  A′'s and S4a's — `healthrec` now *records* them (`362ec437`), but nothing
  judges them. (`oom_class` is closed: the tier line and its tally shipped with
  C2.)
- **`utilization` has no way to know which boundary it is against.** S4a's
  printed `utilization FAIL (0.34, floor 0.50)` divides the anchor-derived
  budget by the *full-GPU* boundary while the scenario's criterion is the
  boundary at the hog's free level, against which the same leg reads **0.67
  granted / 0.38 executed**. Run1 called this out and run2 had to recompute it
  by hand again; the check could take the boundary as an input.
- **No `host_pressure` row.** `vramrec.jsonl` already records `mem_available`
  and swap at 4 Hz and `gpuclk.txt` records GPU utilization, but `analyze.py`
  surfaces neither, so a leg that reports a throughput regression may be
  reporting the box. Phase D1 lost several hours to exactly this; a median
  `mem_available`, peak swap-in-use and median GPU utilization row would have
  settled it immediately.
- **`bisect.largest_ok_units` from run2's two easyOCR bisects is invalid**
  (37 against a true 28) and nothing enforces the annotation; `analyze.py`'s
  `utilization` divides by that field. It could refuse a bisect whose
  `first_index_limit_items` is set, or whose record predates `0438a3c6`.
- Closed since the last revision of this report: **the desired-in-flight figure
  is now published** (`/health.models[].desired_in_flight_items`), together with
  `queue_bound_windows`, the `inference_clients[]` section (transport, lanes,
  connections in use, gate, in-flight), `predict_body_budget`,
  `cost.canvas_pixels` and `shape_ceiling_units` — every number S1 had to
  reconstruct from the logs.

---

## 10. Portability

Run1's platform table (`docs/batch-calibration-run1-report.md` §8) stands.
Run2 adds five items to what a platform pass must check first.

1. **The transport, and its stream limit.** A gateway that speaks h2c to its
   inference server inherits whatever `SETTINGS_MAX_CONCURRENT_STREAMS` the
   peer advertises — our own server now advertises **512**, but a proxy in
   front of it may advertise less, and hyper's client opens up to 100 streams
   before the peer's SETTINGS arrive (which is why `REFUSED_STREAM` is
   retried). Record the peer's limit, `/health`'s
   `inference_clients[].max_concurrent_requests` and the observed in-flight
   plateau; if the plateau matches the peer's limit and the policy asked for
   more, that is S1 on another host. A remote inference server (the user's NAS
   talking to this GPU box) is the case R10' exists for and the one no leg has
   run yet — and it is also where `WINDOW_SETTLE_QUIET = 2 ms` should be
   confirmed, since it was sized for a loopback refill and a LAN adds an RTT.
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
6. **The shape ceiling is a statement about a CUDA kernel, not about a model.**
   `max_pool2d_with_indices` downcasts its output element count to a signed
   int32 on the CUDA (and therefore HIP) path; the CPU kernel has no such
   downcast at all. easyOCR's ceiling is charged only where the kernel has one,
   and answers "yes" for an unloaded GPU-configured model, because a missing
   cap costs a failed batch while a needless one costs a smaller batch. On a
   CPU-budgeted worker (backend C, `INFERIO_DEVICE=cpu`) no `index_limit` clamp
   should ever appear — if one does, the gate is wrong. MPS is untested and
   relies on the reactive halving, which is unconditional.

---

## 11. Release-note text (draft)

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
> per GPU, so a load onto one GPU no longer stalls predictions on another.

New settings, all with defaults (nothing is written to your config file):
`[inference_local] max_concurrent_loads` (default 1),
`load_failure_cooldown_secs` (default 2, **0 disables**),
`load_failure_cooldown_max_secs` (default 300).

### Less memory is held back from your GPU

> Panoptikon reserves a margin over what *other* processes are using on a GPU,
> so your own desktop's VRAM use does not spill into ours. That margin is now
> **capped at 1 GiB when you have not configured one**: on a busy GPU the
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

Which size a model is being charged at is now visible: `canvas_pixels` appears
on the model's load report, on every memory grant in the log, and in `GET
/health`'s cost block.

### OCR on very large scans is faster, and reads exactly the same

> When several images are OCR'd together, Panoptikon now fits each one to the
> text detector's own working size **before** they are batched, instead of
> padding every image in the batch out to the largest one's original
> dimensions. A page next to a large sheet no longer costs the sheet's memory,
> and mixed batches pack better. **Recognition is unchanged**: the text itself
> is still read from your original image at full resolution, so transcription
> of small print on large scans is exactly what it was.

### A batch a model physically cannot run is no longer tried

> Some models have a hard limit on how many images they can process at once
> that has nothing to do with memory — easyOCR's text detector, for instance,
> overflows a 32-bit index inside its pooling kernel at around 29 large pages.
> Previously this failed silently: the worker caught the error, reprocessed the
> batch one image at a time and carried on more than three times slower.
> Panoptikon now computes that limit up front, keeps the batch inside it,
> reports the trim as `clamped.reason = "index_limit"` and remembers it for
> that model, so it stops asking for batches the model cannot run. It is
> **never** mistaken for an out-of-memory event, so it does not shrink your
> batches for anything else, and `GET /health` shows it as
> `shape_ceiling_units`. The limit is a property of the GPU kernel, so it is
> not applied on CPU.

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
> HTTP/1.1 ones, and the Desktop bridge's same-origin check reads the same
> authority. **Without this, local inference over HTTP/2 is refused by your own
> policy layer and no extraction job can be created.**

Panoptikon's own HTTP server now advertises an explicit HTTP/2 stream limit
(512 concurrent streams per connection) instead of inheriting hyper's silent
200, and the inference client keeps a pool of independent connections whose
size follows the work — one connection per 64 requests in flight, built as they
are needed. A large local job is no longer capped at 200 requests on the wire
by a number nothing reported.

### The inference server will not be asked to hold more than it can

> Panoptikon buffers each `predict` request body before parsing it, so a
> single request is limited to 2 GiB and **all in-flight predict bodies
> together** to 4 GiB. A request over the per-request limit is answered
> **413**; one that arrives while the process-wide budget is full is answered
> **503** with a `Retry-After` header — and is then retried and, if it still
> cannot be served, re-queued once, so no item is lost. `GET /health` reports
> `predict_body_budget` (the limits, the bytes in flight and how many requests
> have been refused), and `inference_clients[]` reports each endpoint's
> transport, connections in use and concurrency.

### A request that never reached a model is retried, not blamed on your file

> When an inference request fails, Panoptikon now distinguishes *"this file
> could not be processed"* from *"this request never got there"*. A worker that
> died, a request body that never fully arrived (`request_incomplete`), a
> server with no room to read one (`body_budget_exhausted`) and a connection
> that failed before an answer came back (`transport`) all mean the item's work
> was simply not done: each buys the item **one** re-submission, and only a
> second failure records it as failed. An ordinary error answer still fails the
> item immediately, as before, so a genuinely bad file is not retried forever.

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
- A job that failed to load its model now says **which** model, how many
  consecutive failures it has had and when it will be retried, instead of
  "model load failed on all 1 inference endpoints".
- The UI's generated API types were regenerated from `openapi.json` twice —
  submodule commits `9b28044` (the run2 surface) and `8abf631` (transport
  health, the predict body budget and the shape ceiling).

---

## 12. Host restore checklist

**The run is not over, and the host is no longer the run's.** Phase A′ and S4a
are done; **S4b, S4d, S4g, Phase D and Phase E are blocked** on idle GPUs.
The fix round is complete, the binary and image are rebuilt, and the one
configuration deviation of the run has been removed.

### 1. Restart SGLang — **done, by the user**

```
docker compose -f /home/admin/docker/dsv4flash/docker-compose.yml up -d
```

Stopped at ~12:50 UTC on 2026-09-04 for the legs, after run1 restarted it at
03:55 — and **recreated by the user at 21:59 UTC**, between the S4a leg and the
S4d leg, holding **77 702 MiB on each GPU** (`dsv4-flash-sglang`, image
`dsv4flash-sglang:dev-fi3989`). It was not the run: both GPUs read **2 MiB**
at the end of Phase A, of Phase C, of the D1 legs, of the probes, of Phase A′
and of S4a, and the run's own processes were all stopped between legs. Nothing
under `~/docker` was read or edited; `~/docker/inferio/.env` was never read.

**The mandatory step is therefore satisfied, and it inverts the remaining
work**: the blocked legs need both GPUs idle, and stopping SGLang again is
the user's decision, not the run's.

### 2. Current host state

- **GPUs**: GPU 0 and GPU 1 both **77 702 MiB / 97 887** — the user's
  SGLang (`sglang::scheduler_TP0` and `_TP1`, 77 692 MiB each). Nothing of the
  run's is on either GPU.
- **Processes**: no gateway, no `inferio_worker`, no `vramrec`/`healthrec`/
  `loadgen`/`hog`/`ceiling_probe`. Four recorder processes orphaned by two
  aborted S2-wdvit attempts were found and killed at the end of Phase A; the
  successful leg's JSONLs were verified clean (single header, 0 NUL bytes, 0
  unparsable lines, monotonic timestamps spanning only that leg). One leg the
  D1 agent had to kill by hand (`S8-ocr-C7nc-aborted-registry`) was verified
  dead with its GPUs at 2 MiB before the re-run.
- **Docker**: the user's `dsv4-flash-sglang` (up since 21:59 UTC),
  `sglang-grafana` and `sglang-prometheus` (up 7 days, untouched). No container
  was started by the run or by the image rebuild.
- **Binary**: `target/release/panoptikon` at **`34a591aa`**, 86 313 672 B,
  built 21:28:07 UTC, reports `panoptikon 0.1.8`. `34a591aa` is the last
  commit on the branch that touches code — everything after it is
  documentation — so the binary is the tip's code.
- **Images**: `panoptikon:calib-cuda` = **`0b2261f94c8f`** (9.43 GB, built from
  a clean detached worktree at `34a591aa` in 353 s); the `65fd2f82` image kept
  as `panoptikon:calib-cuda-run2a` (`6fe5d86e3a1e`); run1's kept as
  `panoptikon:calib-cuda-run1` (`2a2c93ad6375`); the CPU and master images are
  still run1's.
- **Git**: nothing pushed. The `ui` gitlink is at `8abf631`, and **both**
  `9b28044` and `8abf631` on `batch-calibration-ui` are unpushed — a
  clean-worktree image build therefore needs this host's clone added as a
  remote (`origin` answers `upload-pack: not our ref 8abf631b…`).
- **Results**: `tools/calibration-protocol/results/run2/` — Phase A's seven
  directories, Phase C's ten, Phase D1's twelve, the probes' three and Phase
  A′/B's three (`S2-wdvit-v2`, `S3-wdvit-v2`, `S4a-v2`); **682 MB** on disk for
  run2, git-ignored, indexed by `results/run2/README.md`.
- **Tree**: clean; the run's commits are `ba03a570` (plan), `92102301` (this
  report), `362ec437` (the `healthrec.py` flattening fix the A′/B legs needed)
  and this session's documentation commits. Nothing pushed.

### 3. Stores this run has produced — which are safe to seed from

| Store | Safe? |
|---|---|
| `run2/S2-wdvit-v2/calibration.after.toml` | **Yes — the seed to use.** Binary `34a591aa`, anchor **439**, slope 52.6818, `local_samples` 11, **no knee**. This is what S3-v2 and S4a-v2 were seeded from |
| `run2/S3-wdvit-v2/calibration.after.toml` | **Yes** — the same profile resumed: anchor **878**, `local_samples` 15, **no knee** |
| `run2/S4a-v2/calibration.after.toml` | **No** — learned under an 85.8 GB hog, so its samples describe a 12 GB-free GPU |
| `run2/S2-minilm/calibration.after.toml` | **Yes** — anchor 19 684, slope 0.032248, **no knee**. The only clean store of the `65fd2f82` legs |
| `run2/S2-wdvit/calibration.after.toml` | **No** — `knee_units = 7` on a flat-curve model (F1). Used deliberately to seed S3 and nothing else |
| `run2/S3-wdvit/calibration.after.toml` | **No** — `knee_units = 15`, anchor stuck at 136 |
| `run2/S3-wdvit-kcw/calibration.after.toml` | **No** — descended from a hand-edited seed |
| `run2/S2-mobileclip/calibration.after.toml` | **No** — `knee_units = 127`, correct for this model and corpus, but a knee all the same |
| `run2/S6-contend/calibration.after.toml` | **No** — three profiles and **no knee** (a first; run1's equivalent is on the poisoned list), but 4 local samples each, learned under a 20 GB hog and three-way contention |
| `run2/S6-b18-loadstall/calibration.after.toml` | **No** — one profile, `fit samples 0` |
| every S5 leg | no store written (fixture batches move the allocator not at all) |
| `run2/S8-ocr-C7*/calibration.after.toml` | **No** — easyOCR only, `epoch 2`, learned from one corpus; pixel units are corpus-shaped even capped (a batch's price is `max × count`). The three that fit 5 samples are byte-identical to run1's |
| `run2/S8-ocr-C7nc*/calibration.after.toml` | **Never** — learned **uncapped** under `epoch = 2`: the epoch says "capped" and the numbers are not |
| `run2/S8-ocr-C1-grantless*` | no store exists — that is the result (D1-d) |

Run1's safe seeds still hold for the **item and token** models
(`S2-wdvit`, `S2-wdvit-fixed`, `S2-wdvit-6a5e6799`); for the **seven pixel
ids** every run1 row is ignored by key, because `metadata.cost.epoch` is now 2.

### 4. Deviations to undo before the run ends

- **Done.** The `calib_hostless` policy block — the run's only configuration
  deviation — was found in **three** configs (`server-C1.toml`,
  `server-C7.toml` and `server-C7nc.toml`) and removed from all of them in
  `ea59a63b`, each replaced by a one-line CALIB comment naming the defect and
  the two commits that fix it. All five branch-derived configs parse and list
  exactly the two shipped policies. Every leg from here on must record that job
  creation works on the shipped policy shape.
- `C7nc` remains on disk as a **diagnostic** configuration, labelled as one in
  both file headers; nothing should be seeded or shipped from it.

---

## Appendix: the commits of this run

**157 commits**, `0d6b36c5` → `362ec437` (plus this revision's own
documentation commits), grouped by the agent that made them; within a group,
oldest first. Product commits **P**, tooling/fixtures/configs **T**,
documentation **D**. Nothing pushed.

### Track L — R1(a, d, contention tag, variance filter), R4, R5 ledger half, R11 store

| Commit | Kind | Subject |
|---|---|---|
| `e6abd09e` | P | Keep squeezed, blind and clamped batches out of the throughput knee |
| `8f71379a` | P | Fit the knee only from samples taken with the GPU to one replica |
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
| `c0896110` | D | Name the per-GPU load admission gate in reserve_load's doc |
| `75016a3f` | T | Name the model on the spawn line, and let analyze.py prefer it |
| `6e515174` | D | Say in the shipped configs what an unset VRAM margin now means |
| `00798eda` | P | Match the OOM tiers by name so all three are live, not just one |
| `0b166de1` | D | Code map: the R3 host classifier and Track L's moved line numbers |
| `9a9b5612` | D | Code map: the four line numbers the OOM tier match moved |

### Track M — R6 per-model locks and GPU gate, R9 load cooldown

| Commit | Kind | Subject |
|---|---|---|
| `e6e510d0` | P | Give every model its own load lock and gate loads per GPU |
| `e2679f62` | P | Cool a model down after a failed load instead of respawning per request |
| `3ad22c92` | D | Code map: the load lock is per model now, plus the load cooldown |

### Track M verifier

| Commit | Kind | Subject |
|---|---|---|
| `d8721f90` | P | Stop the load-failure ladder panicking on the 33rd failure |
| `702ea8ac` | P | Make an unresolvable GPU pin wait for every GPU's load permit |
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
| `c4d6fd3c` | T | Add C7nc: C7 without the canvas cap, to isolate R7 in the S8 leg |
| `6d074f3d` | T | Let a probe registry override replace an inference id, not merge into it |

### Post-Phase-A fixes: P1 and F1, with the F1 verifier

| Commit | Kind | Subject |
|---|---|---|
| `74ca202c` | P | Resolve the policy host from the request authority, not just Host |
| `4c2e00b6` | P | Judge the Desktop same-origin guard by the request authority too |
| `9cbd6304` | P | Fit a knee only where the curve bends and stays flat above it |
| `d230d5ba` | D | Write up the knee's R1e rules and the provisional seeded knee |
| `19e2bf9f` | D | Code map: re-resolve the knee's symbols and lines after R1e |
| `ce03bd0d` | D | Say in the knee's prose what the code does: one candidate, vetoes |
| `36a8cb77` | P | Hold rule 4's gate up with the ring, not a death-halved anchor |
| `846806f0` | P | Pin S6-contend's real census, and both reasons it reaches no knee |
| `9a507c82` | D | Name what the rule 4 control actually varies: the per-sample anchors |
| `493c7f0b` | D | Code map: finish re-resolving the knee bullet's neighbouring lines |
| `f0c74915` | D | Resolve the rule 4 test's doc links from inside the test module |

### The fix round

Forty-six commits, in branch order. The **Item** column names the defect of §5
each one belongs to; a commit whose item is suffixed *(v)* was made by that
item's verifier rather than by its implementer.

| Commit | Kind | Item | Subject |
|---|---|---|---|
| `672aa85a` | P | C2 | Name the tier that classified every out-of-memory negative |
| `7e96de62` | P | P2 | Read a predict's body to its end, and re-queue one that never arrived |
| `18f2aa1b` | P | C2 (v) | Never log an out-of-memory tier the worker stated as an empty string |
| `62a092c9` | T | C2 (v) | analyze: tally the tier that classified each out-of-memory negative |
| `2e5c782b` | P | canvas | Name the pixel canvas a window was priced under on the grant line |
| `2b8499ce` | P | D1-b | Keep the raw size as the packing tiebreaker under the canvas cap |
| `44a7babf` | P | D1-b | Bound easyOCR's inputs by the detector's canvas before it pads a batch |
| `e3b6cacc` | D | D1-b | Document that a declared canvas obliges the impl to bound its tensor |
| `4e587635` | P | S1-1 | Measure what bounds concurrent predicts: hyper's 200 streams, one socket |
| `22845fdb` | P | D1-b (v) | Recognise easyOCR crops from the raw image, not the canvas-bounded one |
| `ccd14ad6` | P | D1-b (v) | Exempt the mixed-batch guard only on a canvas the impl states itself |
| `0f8d04d7` | T | D1-b (v) | C7nc: raise the impl's own canvas, since the registry key no longer frees it |
| `96338378` | D | D1-b (v) | Document that the canvas binds the batch tensor, not every array |
| `3bc4c751` | P | D1-b (v) | Check by exhaustion that the packing tiebreak only changes the order |
| `238a0601` | D | D1-b (v) | Fix the docstring cross-reference left by the box mapper rename |
| `75ada20a` | P | S1-2 | Advertise our own HTTP/2 stream limit instead of inheriting hyper's 200 |
| `bc483729` | P | S1-3 | Give each endpoint real h2 connections and a gate that follows the budget |
| `44bdd895` | P | S1-4 | Let a window form after the previous one's refills land, not before |
| `fae83107` | P | S1b | Retire a shrunk unit permit on release so a saturated job can shrink |
| `feb0a5a9` | P | health | Report the published figure, the transport and the canvas on /health |
| `20bd1536` | P | C4 | Name the model, the retry and the cause when a model load aborts a job |
| `f153a739` | P | C5 | Stop telling a failed job that its re-queued items then completed |
| `99e6c39b` | P | int32 | Halve on a kernel's 32-bit index ceiling without calling it an OOM |
| `ea6c8409` | P | int32 | Cap easyOCR's detector batch at CRAFT's 32-bit pooling index ceiling |
| `8f5e0a97` | P | int32 | Ask the impl for its shape ceiling and report the trim as clamped |
| `085e9cd7` | P | S1 | Silence two clippy lints introduced by the S1 changes |
| `0438a3c6` | T | int32 | Probe: a batch cut short by a shape ceiling is not an ok batch |
| `d4089ce7` | P | int32 | Pin the index ceiling: the formula, the halving, and clamped.reason |
| `09cfede2` | P | int32 | Name a padded detector tensor width-first, as the run2 reports do |
| `bab05a7d` | D | int32 | Document the shape ceiling hook and clamped.reason |
| `26a0ccd9` | D | int32 | codemap: record the shape ceiling and its wire report |
| `07356690` | P | S1/P2 (v) | Read a shape-ceiling clamp that carries no free reading, and name it |
| `1d47384e` | P | int32 (v) | Charge the index ceiling only where the CUDA pooling kernel has one |
| `ca4cfbcb` | P | int32 (v) | Price a ceiling-clamped batch that executed nothing as zero, not whole |
| `56a5bf81` | D | int32 (v) | Say which device a shape ceiling is a statement about |
| `8f8893d5` | D | int32 (v) | codemap: note the shape ceiling is gated on the CUDA kernel |
| `79488c92` | P | P2 (v) | Bound the predict bodies this process holds, not just each one |
| `e3deabcf` | P | S1 (v) | Take an unbindable port for the unreachable-endpoint test, not a freed one |
| `9924044c` | P | S1 (v) | Let a retry wait out its backoff without a gate permit or a lane |
| `a90856f9` | P | S1 (v) | Report a shrinking endpoint's real in-flight count, not target minus free |
| `e6378f78` | P | S1 (v) | Build a connection lane's client when the lane is recruited |
| `6aa2c171` | D | S1 (v) | Say that a desired-in-flight figure is published to every caller at once |
| `8d8ed09c` | P | P2 (v) | Assert the body-budget constants in const blocks, not at runtime |
| `37f5c764` | P | ceiling | Cap the unit budget at a batch size the impl says it cannot execute |
| `535dfc66` | D | ceiling | Document the shape ceiling: a third brake, and why it never persists |
| `bfd2018b` | P | transport | Type a predict's transport failure so a lost request never costs the item |

### Second integration pass

| Commit | Kind | Subject |
|---|---|---|
| `ea59a63b` | T | Drop the calib_hostless workaround now that P1 is fixed in the binary |
| `0998e6ca` | T | Bump the ui pin to the regenerated API types |
| `34a591aa` | D | codemap: re-resolve the client and job refs the run2 fixes moved |

### This report, and the plan

| Commit | Kind | Subject |
|---|---|---|
| `f7396cc7` | D | Add the run2 report: implementation, Phases A and C, what is pending |
| `ea62aecf` | D | Record run2's Phase A and C outcomes in the scenarios and the probes |
| `fee92ffc` | D | Plan: record the fix round, the rebuilt binary and image, and the legs left |
| `92102301` | D | Fold the fix round, the D1 leg and the probes into the run2 report |
| `ba03a570` | D | Record the D1 leg, the probes and the fix round in the scenarios |

### The Phase A′ and B legs

| Commit | Kind | Subject |
|---|---|---|
| `362ec437` | T | healthrec: record the run2 health sections the legs are judged on |
| `4898d0a8` | T | codemap: re-resolve every reference at the tip and add the run2 fix symbols |

`flatten_health` was dropping every section the run2 legs are judged on —
`inference_clients[]` (transport, pool, lanes in use, the gate),
`load_cooldowns[]`, `predict_body_budget`, the GPUs' `reserve_mb` /
`reserve_rule`, the workers' `shape_ceiling_units`, and the models'
`desired_in_flight_items`, `queue_bound_windows` and `cost.canvas_pixels`.
Without it a leg could only read those numbers out of `--full`'s raw payload.
`analyze.py` reads the flattened keys it already knew, so existing recordings
and checks are unaffected. The legs themselves produced **no product commits**:
nothing they measured was a defect. `4898d0a8` is the final `codemap.md` sweep,
made by a separate session in the same checkout and landed while the A′/B legs
ran; it is listed here only because it is a commit of this run.

`<!-- BLOCKED: the commits of S4b/S4d/S4g, Phase D and Phase E, and of the
final codemap sweep -->`
