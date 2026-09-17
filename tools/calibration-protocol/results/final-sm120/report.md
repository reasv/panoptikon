# final-sm120 — the regression pass on the final tip, side by side with run4

Host `gcs`: 2 × NVIDIA RTX PRO 6000 Blackwell Workstation Edition (sm_120,
97 887 MiB each), driver 590.48.01 / CUDA 13.1, Linux 6.12.73-1-lts, 48 cores,
128 649 MiB RAM. Tip **`2133563f`** (`final/local`; `ui` gitlink `96eb4e67`),
release binary built in `/home/admin/projects/panoptikon-wt/final-local`
(2 m 38 s). Run id `final-sm120`; ports 6842/6843/6839, hog 6802, mixed leg
6852/6853/6849. Comparand: `../run4-sm120` at `0d7f5671` on this same host.

**Wall clock: 00:53 → 02:34 UTC, 1 h 41 m**, including the build, the corpora,
the instruments, the probes, 22 legs and the analysis.

**Verdict: the final tip is clean on this architecture, and strictly better
than run4.** Of the 21 legs run4 also ran, **19 reproduce run4's verdict on
every check both runs selected**. Two moved, both in the right direction or
for a named tooling reason:

* `S14-main` and `S14-textembed` `job_outcome` **FAIL → PASS** — run4's cuDNN
  9.5.1/9.7.1 mismatch (run4 §6.2) is gone; the venv now carries the lock's
  9.7.1.26 and docTR loads on the config-pinned interpreter. The `-cu128`
  workaround legs run4 needed are not needed here.
* `S5-dieonload` `job_outcome` **PASS → FAIL** — a **tooling** move, not a
  product one. `analyze.py` gained an unconditional "NO ITEMS" clause after
  run4 (`00f22c55`); the underlying numbers are byte-for-byte run4's. §3.2.
* `S4c` `base_accuracy` **info → PASS** — an oracle sample happened to fall
  inside the judging window this time. Timing, not code.

Both known host residues reproduce exactly and are named as such:
`oracle_agreement` on the four hog legs, `utilization` against the wrong
denominator on S4a, `ledger_invariant` `limit_fell` with 0 `over_grant`, and
the three by-design fixture FAILs. **0 OOM negatives on every leg that is not
a fault-injection fixture.**

Three legs the branch added since run4 — the shipped-baseline-seeded `S2`, the
seeded `S4d`, and the mixed CUDA+CPU leg — all pass and are the evidence for
the four confirmations in §4.

Two tooling defects introduced after run4 blocked four legs on the first
attempt and are written up in §5.

## 1. Instruments

`selftest.py --induce-oom` — **`VERDICT: no degraded tiers`**.

| fact | final-sm120 | run4-sm120 |
|---|---|---|
| `free_source` | `nvml` 95 381 / 97 887 MiB; `torch` tier agrees at 95 381 / 97 250 | identical |
| `base_method` | **`nvml`**, `base_mb` **964** for `tags/wd-vit-tagger-v3` (`free_delta` 969, `alloc_delta` 861) | identical |
| CUDA context size | **665 MiB** (`oracle_calibrate.py`, both sizes) | 665 MiB |
| `nvidia-smi` vs torch total | 97 887 vs 97 250 = **0.65 %** | 0.65 % |
| induced OOM | `OutOfMemoryError` after 95 232 MiB of filler; `classify_oom` → `source: "typed_exception"`, `free_mb_at_failure` 23 | `typed_exception`, 94 208 MiB filler |
| teardown | 95 649 MiB free, nothing left allocated | 94 569 MiB |

`pytest tools/calibration-protocol/tests` — **204 passed, 1 skipped**
(run4: 166 passed, 1 skipped; the branch added tooling tests since).
`oracle_calibrate.py --target gpu --device 0 --sizes 10240,40960` —
**PASS / PASS**, GPU delta 10 240 / 40 960 MiB, PID delta 10 235 / 40 955,
0 OOM, context 665 MiB on both.

Ground truth on the `ramp` tier (`probes/probe-wd.json`,
`probes/bisect-wd.json`): `base(nvml) = 964 MiB`, **slope 29.8594 MiB/item**,
intercept 369.4, residual **0.42**, n = 20; bisect `largest_ok_items` **1024**,
no OOM, `stopped_early: false`. Every figure is run2's and run4's to the
digit, and `peak_reserved` is identical at every batch from 1 to 512.

Venv: `torch 2.7.1+cu128`, `nvidia-cudnn-cu12 9.7.1.26` — the lock's pin, i.e.
run4 §6.2 is closed. `torch.backends.cudnn.version()` reports 90701.

Corpora: regenerated at `generator = 2` into `corpus/` under this run
(`smoke` 200, `ramp` 2 000, `ramp8` 16 000, `text` 2 300 including the 300
scanned pages) — the shared `results/corpus/` tiers on disk are unstamped and
the current `legs.py` refuses them.

## 2. Verdict table, side by side with run4

Each cell is this run's verdict; `run4 → final` where the two differ on a
check both runs selected. Blank = the leg did not select the check.
`S14-main` and `S14-textembed` are compared against run4's `-cu128` re-runs
(its healthy-interpreter controls); against run4's own same-named legs both
`job_outcome` cells read **FAIL → PASS**.

| leg | oracle&#8203;_agreement | base&#8203;_accuracy | footprint&#8203;_agreement | slope&#8203;_accuracy | grant&#8203;_safety | failures | deflation&#8203;_recovery | idle&#8203;_liveness | utilization | throughput | persistence | job&#8203;_outcome | ledger&#8203;_invariant | peak&#8203;_fds | hog&#8203;_tracking | ramp&#8203;_progress | calibration&#8203;_learned | alloc&#8203;_retries |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `S1` | PASS | info | info |  | PASS | PASS |  |  |  |  |  | PASS | PASS | info |  |  |  |  |
| `S2` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | skip | info | PASS | PASS |
| `S2-seeded` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | skip | info | PASS | PASS |
| `S3` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | PASS | PASS | PASS | info | skip | info | PASS | PASS |
| `S4a` | **FAIL** | info | info | PASS | PASS | PASS | PASS | PASS | **FAIL** | info | info | PASS | WARN | info | info | info | PASS | PASS |
| `S4b` | **FAIL** | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | info | info | PASS | PASS |
| `S4c` | **FAIL** | info → PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | info | info | PASS | PASS |
| `S4d` | **FAIL** | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | WARN | info | info | info | PASS | PASS |
| `S4d-seeded` | PASS | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | WARN | info | info | info | PASS | PASS |
| `S5-oom2nd` | PASS | skip | info | skip | PASS | WARN | skip | PASS | WARN | info | PASS | PASS | PASS | info | skip | skip | info | PASS |
| `S5-oomcuda` | PASS | info | info | WARN | PASS | WARN | **FAIL** | PASS | skip | info | WARN | PASS | PASS | info | skip | info | info | PASS |
| `S5-oomtimed` | PASS | info | info | WARN | PASS | WARN | **FAIL** | PASS | skip | info | WARN | PASS | PASS | info | skip | info | info | PASS |
| `S5-oomtimed-long` | PASS | info | info | WARN | PASS | WARN | **FAIL** | PASS | skip | info | WARN | PASS | PASS | info | skip | info | info | PASS |
| `S5-oomtimed-recover` |  |  |  |  | PASS | WARN | PASS | PASS |  |  |  |  | PASS |  |  |  |  |  |
| `S5-failbatch` | PASS | info | info | skip | PASS | PASS | PASS | PASS | skip | info | PASS | PASS | PASS | info | skip | info | info | PASS |
| `S5-failbatch-oomtext` | PASS | info | info | skip | PASS | PASS | PASS | PASS | skip | info | PASS | PASS | PASS | info | skip | info | info | PASS |
| `S5-dieonload` | PASS | skip | info | WARN | skip | PASS | skip | PASS | WARN | info | WARN | PASS → **FAIL** | PASS | info | skip | skip | info | skip |
| `S5-dying` | PASS | info | info | WARN | PASS | WARN | PASS | PASS | skip | info | WARN | PASS | PASS | info | skip | info | info | skip |
| `S14-main` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `S14-florence2` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `S14-textembed` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `mixed-cuda-cpu` | PASS | info |  |  | PASS | PASS | PASS | PASS |  |  |  |  | PASS |  |  |  |  |  |

Where run4's S5 legs were analysed with a narrower `--checks` list, the checks
run4 did not select are shown without an arrow: they are new information, not
a move. The S5 legs here were analysed with run4's own `--expect-ooms` /
`--expect-deaths` / `--expect-failures` / `--expect-failed-jobs` thresholds,
because `legs.py`'s stock S5 `analyze_command` is a flat `--expect-ooms 1` for
every fixture and would report a FAIL for a fixture doing exactly what it was
written to do.

## 3. Every moved check, with the numbers

### 3.1 `S14-main` / `S14-textembed` `job_outcome` FAIL → PASS

run4 §6.2: the shared venv held `nvidia-cudnn-cu12 9.5.1.17` under a
`torch 2.7.1+cu128` compiled against 9.7.1, so docTR raised
`cuDNN version incompatibility` and both legs failed on the config-pinned
interpreter. That venv now carries **9.7.1.26** (the lock's linux + cu128
pin), and both legs pass on it with no re-pointing:

| leg | run4 (pinned venv) | run4 (`-cu128` control) | final-sm120 |
|---|---|---|---|
| `S14-main` | **FAIL** — 1 of 11 queue outcomes `failed` (docTR) | PASS, 11/11 | **PASS, 11 of 11 completed**, 0 item errors, 8 grants; tags + clip + docTR + whisper (`--scan-audio`, 180 indexed) |
| `S14-textembed` | **FAIL** — 1 of 6 `failed`, `grant_safety` SKIP | PASS, 7/7 | **PASS, 7 of 7 completed**, 0 errors, 6 grants; docTR over 300 scanned pages → `textembed/all-MiniLM-L6-v2` |

Both extra listeners answer 200 on every S14 leg (`test` 6843, `legacy_ui`
6839). `S14-florence2`: 180/180 captioned, 0 errors, 3 grants.

### 3.2 `S5-dieonload` `job_outcome` PASS → FAIL — a tooling move

The measured behaviour is identical to run4's: **1 job record, 0 completed,
0 failed items, 0 errors, 1 of 5 queue outcomes `failed`**, `failures` PASS
(0 OOM negatives, 0 deaths), `idle_liveness` PASS. run4 read that as PASS.

`analyze.py` now adds, unconditionally:

```
; NO ITEMS: calibfixture/dies_on_load_cuda ran on 0 items, so nothing here
  measures anything - check the corpus and, for a derived setter, that its
  source setter ran first
```

`dies_on_load_cuda` raises inside `load()` by construction, so the job can
never process an item — `total_segments = 0` **is** the leg's result. The
clause has no expectation flag to acknowledge it (`analyze.py:1694–1701`:
`empty` enters the verdict with no `--expect-…` escape, unlike `over` and
`over_jobs`), so this leg can no longer pass. §5.2.

### 3.3 `S4c` `base_accuracy` info → PASS

run4: `info` — "no oracle sample fell between the load and this replica's
first work". Here an oracle sample did land in that window:
**`base_mb` 964 (nvml) vs oracle PID 964 MiB at admission+95 ms = 0.0 %**
[threshold 10 %]. Sampling luck; the number itself is run4's.

### 3.4 The residues, reproduced and named

**`oracle_agreement` FAIL on the four hogged legs — the documented host
residue, bounded to hog transitions.** Every worst breach lands within two
seconds of a hog transition, all conservative (`external_mb` high while the
oracle has already seen the release), none in steady-state job execution:

| leg | breaches / joined | run4 | worst sample | nearest hog event | lag |
|---|---|---|---|---|---|
| `S4a` | **6 / 460** | 6 / 444 | 01:11:53.668, external 85 791 vs oracle 1 536 (allowance 1 958) | `hog_stop_requested` 01:11:51.87 | +1.8 s |
| `S4b` | **7 / 2 246** | 9 / 2 084 | 01:40:42.599, external 33 055 vs oracle 1 536 | `hog_stop_requested` 01:40:40.77 | +1.8 s |
| `S4c` | **26 / 2 286** | 26 / 2 278 | 01:44:48.843, external 69 791 vs oracle 2 204 | spike `release` 01:44:48.00 | +0.8 s |
| `S4d` | **5 / 2 472** | 4 / 2 828 | 01:57:06.227, external 89 887 vs oracle 2 204 | `release` 01:57:05.05 | +1.2 s |

`grant_safety` PASSes on all four (0 of 6 / 13 / 13 / 20 grants over the
oracle's live free memory), and `hog_tracking` correctly stays INFO.
`S4d-seeded`, driven through the same schedule, had **0 breaches of 2 196**.

**`utilization` FAIL on S4a (0.24 vs floor 0.25)** is the open instrument gap
run2 §9 and run4 §3 already record: the check divides the anchor-derived
budget by the *full-GPU* probe boundary (1 024 units) while S4a's own
criterion is the boundary at the hog's free level. Largest granted budget
**249** of a published **498** against a 12 288 MiB free level — a near-miss
under the wrong denominator, the same shape as run4's 246 / 492 / 0.24.

**`ledger_invariant` WARN is the `limit_fell` class, 0 `over_grant`
everywhere**: S4a 46 of 792, S4d 190 of 4 545, S4d-seeded 211 of 4 170
(run4: S4a 43 of 522, S4d 175 of 3 392). Every other leg is 0 of everything,
including S4b and S4c at 0 of 4 251 each.

**The three fixture `deflation_recovery` FAILs are the fixtures working.**
`oom_cuda` / `oom_timed_cuda` OOM at batch 1 for the whole job, so no clean
window ever arrives to repay deflation and the recording ends at the cap of
**4** = `ceil(log2 8) + 1`. `S5-oomtimed-recover` is the leg that shows the
repayment, and it PASSes (§3.5).

### 3.5 The deflation-recovery loadgen leg, reproduced exactly

`S5-oomtimed-recover`: one resident `calibfixture/oom_timed_cuda` replica
(`lru_size=1`, `ttl 3600`, `cache_key=recover`), driven at one request every
2 s for 300 s plus a 60 s idle tail, so the recording spans the fixture's
120 s OOM phase and the repayment after it.

| | final-sm120 | run4-sm120 |
|---|---|---|
| requests / failed | 151 / 62 | 151 / 62 |
| grants | 151, 0 over headroom, 0 over the oracle's free memory | 151, 0 / 0 |
| OOM negatives | 62, all tier `marker`/`trusted` | 62, `marker`/`trusted` |
| peak deflation | **4** | 4 |
| deflated at end | **0** | 0 |
| `deflation_recovery` | **PASS** | PASS |
| `ledger_invariant` | PASS, 0 of 2 160 | PASS, 0 of 1 232 |

### 3.6 The other fixture legs, numbers unchanged

| leg | final-sm120 | run4-sm120 |
|---|---|---|
| `S5-oom2nd` | 1 OOM negative of 12 grants, tier `marker`/`trusted`, 1 merged-window fallback, job **completed** 180/180 | 1 / 1 / completed |
| `S5-oomcuda` | 57 negatives, 56 fallbacks, all `marker`/`trusted`, 0 over-grants of 57, deflation capped at 4, job fails 180/180 by design | 57 / 56 / 4 / 180 |
| `S5-oomtimed` | 57 negatives, 56 fallbacks, deflation 4, 180/180 failed | identical |
| `S5-oomtimed-long` | **664** negatives, **662** fallbacks, deflation 4, **2 000/2 000** failed | 664 / 662 / 4 / 2 000 |
| `S5-failbatch` | 30 merged-window fallbacks, **0 OOM negatives**, job completed 180/180 | 30 / 0 / completed |
| `S5-failbatch-oomtext` | B11: a non-memory error whose text contains "out of memory" → **0 OOM negatives**, deflation never left 0, completed | 0 / 0 / completed |
| `S5-dying` | **6 fatal worker deaths for 180 items**, 180 failed items, 0 unified-device death negatives, deflation never left 0 | 6 / 180 / 0 |
| `S5-dieonload` | job aborts, 0 OOM negatives, 0 items failed, `idle_liveness` PASS; `job_outcome` FAIL only on the new NO-ITEMS clause | same numbers, PASS |

## 4. The four confirmations

### 4.1 The shipped-baseline seeded S2 confers the anchor and never sets the budget

`S2-seeded` = `legs.py --scenario S2 --seed-calibration
python/inferio/config/calibration/sm_120-linux-cuda.toml` (**168 rows**;
`calibration.before.toml` in the leg directory is that file verbatim).

The shipped row for `tags/wd-vit-tagger-v3` carries
`max_units_measured = 192`, `slope 29.865328820116055`, `samples 5`,
`base_mb 964`. The registry seed is `seed_units = 64`.

* The gateway logs `admitted a worker … base_mb=Some(964) base_method="nvml"
  seeded_from_store=true`, and **the very first settled window already reads
  `max_units_measured=192`** — the anchor travels before this host has
  measured anything.
* The granted budgets are the ramp's, never the anchor:
  **1 → 63 → 128 → 256 → 256 → 400** (`ramp_step` 1 → 1 → 1 → 2 → 2 → 3).
  192 is never granted and is never the starting budget; the run peaks at a
  published 512 exactly as the unseeded `S2` does.
* The anchor then advances on this host's own clean batches:
  `max_units_measured` 192 → 192 → 192 → 256 → 256 → 400 over the six settles.
* The seeded anchor is not written back as this machine's own. After the leg
  the tagger row reads `max_units_measured 256`, `local_samples 11`,
  `measured_at 2026-09-17T01:05:05Z`, `slope 29.860821044240733` — this run's
  measurement, in a store that is still **168 rows** (no duplicate row, no
  foreign anchor preserved).
* Control: the unseeded `S2` reads the same shipped baseline out of
  `config_dirs` and behaves identically — `seeded_from_store=true`, first
  settle at `max_units_measured=192`, budgets **1 → 64 → 128 → 256 → 256 →
  399**, store after the leg 1 profile. `--seed-calibration` changes what the
  *local store* starts with (168 rows vs none), not what the anchor confers.
* Both legs: `slope_accuracy` PASS at ratio 1.0000 against the probe
  (29.859060 and 29.860821 vs 29.859375), `grant_safety` PASS with 6 grants
  and 0 breaches, `ledger_invariant` 0 of 798 / 0 of 762.

### 4.2 The S4d seeded leg does not freeze — the anchor re-probe

`S4d-seeded` runs the `leave-free 8 192` schedule with the 168-row baseline in
the store. Timeline from `panoptikon.log` and `legs.json`:

```
02:05:29  hog filled: 87 552 MiB held, 8 133 MiB free
02:07:55  job posted (16 000 items, ramp8)
02:08:30  "holding the throughput ramp at this rung: the rung is the top of a
           measured plateau"   units=256 certified=true knee_binds=false
02:09:55  hog release (t+120 s): held_mb 0
02:10:30  "fitted a throughput knee"  knee_units=127  previous=None  observations=29
02:10:41  "holding the throughput ramp at this rung: a knee caps the sizes a
           doubling would have to measure at"  units=512 certified=true knee_binds=true
02:12:54  "…the knee is worth re-testing; widening the cap by one batch-size step"
           knee_units_before=127  knee_units_after=255
           clean_windows_at_the_knee=12  last_grant_units=127
02:16:22  job drained, 16 000 items, 31.683 items/s — the fastest of the four S4 legs
```

The granted budget series shows the hold and the release of the hold, not a
freeze: `1, 63, 128, 124 × 8, 256, 256, 127 × 12, 255 × 10`. The leg ends with
`utilization` **PASS at 1.00** ("held at `knee_units=127`, rung 256"),
`ramp_progress` `128 → peak 256 (last 255, fit samples 12, knee 255, low 127)`,
`calibration_learned` PASS, `oracle_agreement` PASS with **0 breaches of
2 196**, `grant_safety` PASS with 35 grants and 0 breaches, 0 OOM, 0 allocator
retries over 35 windows. The only non-PASS is the `limit_fell`
`ledger_invariant` WARN (211 of 4 170, 0 `over_grant`).

The unseeded `S4d` under the same schedule squeezes to 119, holds, and grows
back to **714** after the release with no knee fitted — also no freeze, 20
grants, 0 over-grants.

### 4.3 0 OOM negatives outside the fixtures

Every leg's `failures` check, `negative_reasons.oom`:

| leg | OOM negatives | throughput collapses | fatal worker deaths |
|---|---|---|---|
| `S1`, `S2`, `S2-seeded`, `S3` | **0** | 0 | 0 |
| `S4a`, `S4b`, `S4c`, `S4d`, `S4d-seeded` | **0** | 0 | 0 |
| `S14-main`, `S14-florence2`, `S14-textembed` | **0** | 0 | 0 |
| `mixed-cuda-cpu` | **0** | 0 | 0 |
| `S5-dieonload`, `S5-failbatch`, `S5-failbatch-oomtext` | 0 | 0 | 0 |
| `S5-dying` | 0 | 0 | 6 (the fixture's own `os._exit(3)`) |
| `S5-oom2nd` / `-oomcuda` / `-oomtimed` / `-oomtimed-long` / `-oomtimed-recover` | 1 / 57 / 57 / 664 / 62 | 0 | 0 |

Not one OOM negative and not one throughput collapse outside
`calibfixture/*`, on a pass that put the board under a hog four times and ran
16 000-item jobs at budgets up to 3 230 units.

### 4.4 The CPU-pinned replica is priced under CPU beside the GPU rows

`mixed-cuda-cpu`: the C8 registry override pins
`textembed/all-MiniLM-L6-v2` to `devices = ["cpu"]` on a CUDA host;
`tags/wd-vit-tagger-v3` runs on the GPU; `loadgen.py` drives both at once
(10 × 32 image requests and 20 × 32 text requests, 4 threads).
Final `/health` `vram[]`:

```
CPU       cpu   arch cpu     total 128649  ext 38927 ram          limit 88698  fp 396   cap 0.75
  textembed/all-MiniLM-L6-v2  fp 396  base 235  unit_budget 4224  clean 17  fit slope 0.03447 MB/token (7 samples)
GPU-01c6  cuda  arch sm_120  total  97887  ext  1568 nvidia-smi   limit 96162  fp 0     cap none
GPU-942d  cuda  arch sm_120  total  97887  ext  1668 nvml         limit 96052  fp 964   cap none
  tags/wd-vit-tagger-v3       fp 964  base 964  unit_budget 64    clean  9  fit slope 29.86533 MB/item (5 samples)
```

Three device rows, one per device, each priced on its own backend: the CPU row
takes its `external` from **`ram`** and its base from RSS, the GPU rows from
**`nvml`** and **`nvidia-smi`**. The CPU replica's 396 MiB is charged to the
CPU device only and never appears in either GPU's `external`. 30 requests, **0
failed**; `failures`, `grant_safety` (26 grants, 0 breaches),
`deflation_recovery`, `ledger_invariant` (0 of 465), `idle_liveness` and
`oracle_agreement` (worst 127 MiB of 308 joined samples) all **PASS**, and
**0 WARN lines in the whole gateway log** — no "names no device at all"
escalation. This reproduces `results/run5-mixed/cuda-cpu` on the final tip.

## 5. Two tooling defects introduced after run4

Neither is a product defect; both blocked legs of the standard set on this
tip and both come from `af331a91` / `00f22c55` (`fix/tools-run4`).

### 5.1 `legs.py` demands a `ramp8` corpus tier that `corpus.py` cannot produce

S4b, S4c and S4d declare `corpus="ramp8"` (`legs.py:259,275,293`), and
`corpus_complaint` (`legs.py:703–706`) refuses any corpus whose manifest
`tier` is not that string. But `corpus.py` has **no `ramp8` tier** — its
`--tier` choices are `smoke, ramp, text, pixmix, ocr, audio, pdf, soak,
poison` — and the manifest always stamps `args.tier` (`corpus.py:703`). The
README's own recipe,

```
corpus.py --tier ramp --scale 8 --out $T/results/corpus/ramp8
```

therefore produces a corpus stamped `tier: "ramp"` that the three legs reject,
and the error message tells the operator to run
`corpus.py --tier ramp8 … --force`, which exits 2 on an invalid choice. Three
of the eight platform-pass legs cannot be started from a clean tree.

The same shape hits any leg run on a deliberately larger corpus than its
scenario's tier: `S5-oomtimed-long` is the S5 scenario (tier `smoke`) on the
2 000-item ramp corpus, which run4 ran and this check now refuses.

Worked around here by restamping the manifests (`tier: "ramp8"` on the
`ramp --scale 8` corpus; a `smoke-long` copy of `ramp` stamped `smoke`), both
recorded in the manifests' `tier_note`. The fix is either a real `ramp8` tier
in `corpus.py`, or a `--tier-name` stamp, or accepting `ramp` + `scale 8`.

### 5.2 `analyze.py`'s NO-ITEMS clause has no expectation escape

`job_outcome` (`analyze.py:1694–1701`) computes

```python
empty   = [setter for record in records if not int(record.get("total_segments") or 0)]
over    = failed > ctx.args.expect_failures
expected_bad = ctx.args.expect_failed_jobs
over_jobs = len(bad_outcomes) > expected_bad
verdict = "FAIL" if (over or over_jobs or empty) else "PASS"
```

`over` and `over_jobs` are both declarable; `empty` is not. A fixture whose
whole purpose is that the model never becomes resident —
`calibfixture/dies_on_load_cuda` — records `total_segments = 0` by
construction and so can never pass, whatever `--expect-…` is passed. The
clause is right for its motivating case (run4-deploy's stale-corpus
`S14-textembed`, which drained green on `total_available: 0`); it needs an
`--expect-empty-setters`, or an exemption gated on `--expect-failed-jobs`.

A third, smaller one: `legs.py`'s stock S5 `analyze_command` is
`--checks all --expect-ooms 1` for every fixture, so seven of the eight S5
legs report a FAIL out of the box for behaving as designed. run4's per-fixture
thresholds had to be reconstructed by hand from its verdict details.

## 6. Host conditions

A sibling agent ran `panoptikon-wt/final-deploy` gateways on this box through
parts of the pass (seen by the pre-flight `pgrep` on several legs). It never
collided: **every leg bound 6842/6843/6839** (confirmed from each `legs.json`
`base_url`), and a sweep of all 22 recordings for a `-m inferio_worker` PID
that is not in that leg's own `spawned an inferio worker … pid=Some(N)` lines
found **0 foreign workers on any leg** — so run4 §6.1's attribution
contamination did not recur. The only non-ours GPU tenants throughout were the
host's two permanent ~900 MiB processes on GPU 0 and GPU 1.

`newrun.py`'s `host.json` still records the calling shell's environment rather
than the gateway's (run4 §6.4), so its `RUST_LOG` / `CUDA_VISIBLE_DEVICES`
read `null`; the gateway's own values come from `config/env.C1F` and are in
each `legs.json`.

GPUs at the end of the pass: **900 MiB and 932 MiB**, i.e. the host baseline,
with nothing of this pass resident.

## 7. Files

* `instruments/` — `platform-selftest.json`, `oracle-calibration.json`
* `probes/` — `probe-wd.json`, `bisect-wd.json`, `probe-console.txt`
* `config/` — `server-C1F.toml` / `env.C1F` (the pass configuration, ports
  6842/6843/6839) and `server-C8F.toml` / `env.C8F` (the mixed leg, 6852/6853/6849)
* `<leg>/verdicts.json`, `analyze.txt`, `legs.json`, `calibration.after.toml`,
  `calibration.before.toml`, `jobs.json`, `failures*.json`, `health-*.json`,
  `smoke.json`, `runlog.md` — committed
* `mixed-cuda-cpu/health-boot.json`, `health-final.json`, `loadgen.log`,
  `status.txt` and `S5-oomtimed-recover/loadgen.err`, `status.txt` — committed
* `<leg>/vramrec.jsonl`, `healthrec.jsonl`, `panoptikon.log`, `root/`,
  `corpus/` — left on disk, not committed (3.0 GB)
