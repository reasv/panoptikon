# run4 — sm_120 regression pass, the owed baseline rows, and the F4 re-run

Host `gcs`: 2 × NVIDIA RTX PRO 6000 Blackwell Workstation Edition (sm_120,
97 887 MiB each), driver 590.48.01 / CUDA 13.1, Linux 6.12.73-1-lts, 48 cores,
128 649 MiB RAM. Tip **`0d7f5671`** (`run4/local`, the merged batch-calibration
branch; `ui` gitlink `ce7506ae`), release binary built in
`/home/admin/projects/panoptikon-wt/run4-local` (2 m 50 s). Run id
`run4-sm120`; recordings under this directory.

**Verdict: the merged tip is clean on this architecture.** Every scenario
`legs.py` offers passes. The only FAILs are the two documented host residues
(`oracle_agreement` under a moving hog, `utilization` against the wrong
denominator on S4a) plus three by-design fixture FAILs; none is attributable to
the 43 master commits the merge brought in. Two of the four owed baseline rows
were genuinely missing and are now measured and shipped; the other two were
already in the committed file and re-measure to within the unit's own noise.
**F4 did not reproduce in two attempts**, and the code path that can produce its
shape is identified below.

Three host events perturbed the pass and are accounted for in §6: a concurrent
sibling agent on the same box, a shared-venv cuDNN downgrade, and a host-wide
`pkill -f panoptikon` at 19:59:54Z.

## 1. Instruments

`selftest.py --induce-oom` — **`VERDICT: no degraded tiers`**.

| fact | value |
|---|---|
| `free_source` | `nvml` (95 381 / 97 887 MiB); `torch` tier agrees at 95 381 / 97 250 |
| `base_method` | **`nvml`**, `base_mb` 964 for `tags/wd-vit-tagger-v3` (`free_delta` 969, `alloc_delta` 861 below it) |
| CUDA context size | **665 MiB** (`oracle_calibrate.py`, both sizes) — run2 measured 666–668 |
| `nvidia-smi` vs torch total | 97 887 vs 97 250 MiB = **0.65 %** |
| induced OOM | `OutOfMemoryError` after 94 208 MiB of filler; `classify_oom` → `source: "typed_exception"`, `free_mb_at_failure` 991 |
| teardown | 94 569 MiB free (nothing left allocated) |

`pytest tools/calibration-protocol/tests` — **166 passed, 1 skipped**.
`oracle_calibrate.py --target gpu --device 0 --sizes 10240,40960` —
**PASS/PASS**, GPU delta 10 240 / 40 960 MiB, PID delta 10 235 / 40 955, 0 OOM.

Ground truth for the S2/S3/S4 model, on the `ramp` tier
(`probes/probe-wd.json`, `probes/bisect-wd.json`):
`base(nvml) = 964 MiB`, **slope 29.8594 MiB/item**, intercept 369.4,
residual 0.42, n = 20 — run2's sweep figure (29.8594) reproduced exactly, and
`peak_reserved` identical at every batch from 1 to 512.

## 2. Verdict table

`analyze.py --checks all --learning --probe probe-wd.json --probe bisect-wd.json`
for the S1–S4 legs, the scenario's own check list for S5/S14. Blank = the
scenario did not select the check.

| leg | oracle&#8203;_agreement | base&#8203;_accuracy | footprint&#8203;_agreement | slope&#8203;_accuracy | grant&#8203;_safety | failures | deflation&#8203;_recovery | idle&#8203;_liveness | utilization | throughput | persistence | job&#8203;_outcome | ledger&#8203;_invariant | peak&#8203;_fds | hog&#8203;_tracking | ramp&#8203;_progress | calibration&#8203;_learned | alloc&#8203;_retries |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `S1` | PASS | info | info |  | PASS | PASS |  |  |  |  |  | PASS | PASS | info |  |  |  |  |
| `S14-florence2` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `S14-main` |  |  |  |  | PASS | PASS |  |  |  |  |  | **FAIL** |  | info |  |  |  |  |
| `S14-main-cu128` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `S14-textembed` |  |  |  |  | skip | PASS |  |  |  |  |  | **FAIL** |  | info |  |  |  |  |
| `S14-textembed-cu128` |  |  |  |  | PASS | PASS |  |  |  |  |  | PASS |  | info |  |  |  |  |
| `S2` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | skip | info | PASS | PASS |
| `S2-cu128` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | skip | info | PASS | PASS |
| `S3` | PASS | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | PASS | PASS | PASS | info | skip | info | PASS | PASS |
| `S4a` | **FAIL** | info | info | PASS | PASS | PASS | PASS | PASS | **FAIL** | info | info | PASS | WARN | info | info | info | PASS | PASS |
| `S4b` | **FAIL** | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | info | info | PASS | PASS |
| `S4c` | **FAIL** | info | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | info | info | PASS | PASS |
| `S4c-contaminated` | **FAIL** | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | PASS | info | info | info | PASS | PASS |
| `S4d` | **FAIL** | PASS | info | PASS | PASS | PASS | PASS | PASS | PASS | info | info | PASS | WARN | info | info | info | PASS | PASS |
| `S5-dieonload` |  |  |  |  | skip | PASS | skip | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-dying` |  |  |  |  | PASS | WARN | PASS | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-failbatch` |  |  |  |  | PASS | PASS | PASS | PASS |  |  |  | PASS | PASS |  |  |  |  |  |
| `S5-failbatch-oomtext` |  |  |  |  | PASS | PASS | PASS | PASS |  |  |  | PASS | PASS |  |  |  |  |  |
| `S5-oom2nd` |  |  |  |  | PASS | WARN | skip | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-oomcuda` |  |  |  |  | PASS | WARN | **FAIL** | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-oomtimed` |  |  |  |  | PASS | WARN | **FAIL** | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-oomtimed-long` |  |  |  |  | PASS | WARN | **FAIL** | PASS |  |  |  | PASS | PASS | info |  |  |  |  |
| `S5-oomtimed-recover` |  |  |  |  | PASS | WARN | PASS | PASS |  |  |  |  | PASS |  |  |  |  |  |
Legs whose names end `-cu128` are re-runs on a healthy interpreter (§6.2);
`S4c-contaminated` is the recording a sibling's worker polluted (§6.1);
`S14-main` and `S14-textembed` are the docTR-blocked originals, superseded by
their `-cu128` re-runs.

### Every FAIL and WARN, named

```
S14-main                 job_outcome          FAIL 1 job record(s): 1 completed, 0 failed item(s) (expected <= 0), 0 errors; queue outcomes: ['completed', 'completed', 'completed', 'completed', 'completed', 'completed', 'completed', 'failed', 'complet
S14-textembed            job_outcome          FAIL 1 job record(s): 0 completed, 0 failed item(s) (expected <= 0), 0 errors; queue outcomes: ['completed', 'completed', 'completed', 'failed', 'completed', 'completed'] (1 not completed, expected <= 0)
S4a                      oracle_agreement     FAIL worst |external_mb - oracle| = 84309 MiB over 444 joined GPU-samples; 6 outside the allowance
S4a                      utilization          FAIL tags/wd-vit-tagger-v3: largest granted unit_budget 246 (published 492) / probe boundary 1024 = 0.24  [floor 0.25]
S4a                      ledger_invariant     WARN strict: 43 of 522 GPU-samples had charges + load reservations > limit_mb (0 over_grant, a grant issued in that sample beyond its priced headroom; 43 limit_fell, the limit dropping under a footprint or
S4b                      oracle_agreement     FAIL worst |external_mb - oracle| = 31573 MiB over 2084 joined GPU-samples; 9 outside the allowance
S4c                      oracle_agreement     FAIL worst |external_mb - oracle| = 67587 MiB over 2278 joined GPU-samples; 26 outside the allowance
S4c-contaminated         oracle_agreement     FAIL worst |external_mb - oracle| = 67641 MiB over 2142 joined GPU-samples; 183 outside the allowance
S4d                      oracle_agreement     FAIL worst |external_mb - oracle| = 87737 MiB over 2828 joined GPU-samples; 4 outside the allowance
S4d                      ledger_invariant     WARN strict: 175 of 3392 GPU-samples had charges + load reservations > limit_mb (0 over_grant, a grant issued in that sample beyond its priced headroom; 175 limit_fell, the limit dropping under a footprint
S5-dying                 failures             WARN 0 OOM negatives (expected <= 0), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 6 fatal worker deaths (expected <= 200), 0 merged-window fallbacks (0 OOM)
S5-oom2nd                failures             WARN 1 OOM negatives (expected <= 1), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 0 fatal worker deaths (expected <= 0), 1 merged-window fallbacks (0 OOM); tiers marker/truste
S5-oomcuda               failures             WARN 57 OOM negatives (expected <= 60), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 0 fatal worker deaths (expected <= 0), 56 merged-window fallbacks (0 OOM); tiers marker/tru
S5-oomcuda               deflation_recovery   FAIL peak deflation 4 (calibfixture/oom_cuda@GPU-942d9e56-4e82-54d7-21a9-d5fb466cfd1a=4); at the end of the recording 1 worker(s) were still deflated ({'calibfixture/oom_cuda@GPU-942d9e56-4e82-54d7-21a9-d5
S5-oomtimed              failures             WARN 57 OOM negatives (expected <= 60), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 0 fatal worker deaths (expected <= 0), 56 merged-window fallbacks (0 OOM); tiers marker/tru
S5-oomtimed              deflation_recovery   FAIL peak deflation 4 (calibfixture/oom_timed_cuda@GPU-942d9e56-4e82-54d7-21a9-d5fb466cfd1a=4); at the end of the recording 1 worker(s) were still deflated ({'calibfixture/oom_timed_cuda@GPU-942d9e56-4e82-
S5-oomtimed-long         failures             WARN 664 OOM negatives (expected <= 700), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 0 fatal worker deaths (expected <= 0), 662 merged-window fallbacks (0 OOM); tiers marker/
S5-oomtimed-long         deflation_recovery   FAIL peak deflation 4 (calibfixture/oom_timed_cuda@GPU-942d9e56-4e82-54d7-21a9-d5fb466cfd1a=4); at the end of the recording 1 worker(s) were still deflated ({'calibfixture/oom_timed_cuda@GPU-942d9e56-4e82-
S5-oomtimed-recover      failures             WARN 62 OOM negatives (expected <= 200), 0 throughput-collapse negatives, 0 unified-memory-device death negatives, 0 fatal worker deaths (expected <= 0), 0 merged-window fallbacks (0 OOM); tiers marker/tru```

## 3. Reading the FAILs

**`oracle_agreement` FAIL on all four S4 legs — the documented host residue,
bounded to hog transitions and teardown.** run2's own final-tip legs on this
host FAILed the same way: `S4b-v2` 43 breaches, `S4d-v2` 4, `S4g-v2` 19
(`results/run2/*/verdicts.json`). Run4 is the same or better, and every breach
is a staleness artefact at a moment the hog moved — `external` is a
window-boundary quantity (B2/T3), and `hog_tracking` correctly stays INFO.
Recomputed per sample with `analyze.py`'s own join:

| leg | breaches / joined | when | direction |
|---|---|---|---|
| `S4a` | 6 / 444 | all within 17:54:07.1–17:54:09.6, the last 3 s before teardown, after the hog released | all conservative (`external_mb` 85 845 vs oracle 1 536) |
| `S4b` | 9 / 2 084 | 3 in the 1.4 s after the `POST /set` step at 17:57:37.3; 6 in the 2.5 s after `hog_stop_requested` | 3 optimistic (step not yet seen), 6 conservative |
| `S4c` | 26 / 2 278 | **all 26** within 19:10:18–19:10:37, spanning the spike up (19:10:17.7) and its release (19:10:27.7) | 7 optimistic during the fill, 19 conservative during the release lag |
| `S4d` | 4 / 2 828 | all at the t+120 s hog release, 18:20:58.7–18:20:59.7 | conservative |

No breach falls in steady-state job execution on any leg, and `grant_safety`
PASSes on all four (0 of 6 / 21 / 14 / 15 grants over the oracle's live free
memory). The optimistic samples carried no grant.

**`utilization` FAIL on S4a (0.24 vs floor 0.25) is the open instrument gap,
not a regression.** run2 §9 already records it: the check divides the
anchor-derived budget by the *full-GPU* probe boundary (1 024 units) while
S4a's own criterion is the boundary at the hog's free level. The leg's
`PRECONDITION:` line says so. The largest budget a grant carried was 246 of a
published 492 against a 12 288 MiB free level — a near-miss under the wrong
denominator.

**`ledger_invariant` WARN on S4a (43 of 522) and S4d (175 of 3 392) is the
`limit_fell` class**, 0 `over_grant` on both, which is the WARN-by-design shape
(T6/P5-2). Every other leg is 0 of everything.

**The three fixture FAILs are the fixtures working.** `calibfixture/oom_cuda`
and `calibfixture/oom_timed_cuda` OOM at batch 1 for the whole job, so no clean
window ever arrives to repay deflation and the recording ends with the worker
still at the cap. `S5-oomtimed-recover` is the leg that shows the repayment.

**`S14-main` / `S14-textembed` `job_outcome` FAIL is §6.2**, the venv's cuDNN;
both pass on a healthy interpreter.

## 4. What the scenarios measured

| leg | headline |
|---|---|
| `S1` | inventory and a full board: 2 GPUs detected by UUID, 180 items, 3 grants, `oracle_agreement` worst 188 MiB of 232 samples |
| `S2` | cold ramp: budget 128 → 512, 6 fit samples, ledger slope **29.859155** against the probe's **29.859375** (ratio **1.0000**), base 964 = oracle 964 |
| `S2-cu128` | the same leg on the cu128 interpreter: base 964, `nvml`, slope **29.860821** (ratio 1.0000), `max_units_measured` 256 — the control for §6.2 |
| `S3` | restart and resume: 12 grants, `persistence` **PASS** (worst anchor-advance → store-write 5.3 s of a 30 s threshold), 6 store writes, resumes at the persisted anchor |
| `S4a` | constant `leave-free 12 288`: budget 128 → 492, 9 fit samples, 2 allocator retries, 0 OOM |
| `S4b` | +30 720 MiB step at t+60 s: 21 grants, knee 511, 33.5 items/s, `base_accuracy` **judged PASS at 0.0 %** |
| `S4c` | 2 048 MiB spike for 10 s at t+90 s: 0 OOM, 0 collapse, deflation never left 0, budget reached 1 024, `utilization` 1.00 |
| `S4d` | `leave-free 8 192` released at t+120 s: budget grows back to **3 830**, 16 fit samples, 15 grants, 0 over-grants |
| `S5-oom2nd` | 1 OOM negative of 12 grants, tier `marker`/`trusted`, job **completed** 180/180, budget recovers |
| `S5-oomcuda` | batch-1 OOM always: 57 negatives, all `marker`/`trusted`, deflation capped at **4** = `ceil(log2 8) + 1`, job fails 180/180 as designed, 0 over-grants of 57 |
| `S5-failbatch` | 30 merged-window fallbacks, **0 OOM negatives**, job completed 180/180 |
| `S5-failbatch-oomtext` | B11: a non-memory error whose text contains "out of memory" → **0 OOM negatives** of 31 grants, deflation never left 0, job completed. run1's 15-of-51 is still closed |
| `S5-oomtimed` / `-long` | 57 / 664 negatives, deflation capped at 4; the job drains before the fixture goes healthy, so recovery is not observable through a job leg |
| `S5-oomtimed-recover` | the loadgen shape that holds the replica resident: deflation **0→1→2→3→4 in 3.07 s**, holds 122 windows, then **4→3→2→1→0 at 20:20:57.7 / 21:03.7 / 21:09.7 / 21:15.7** — one level per 3 clean windows, 18 s in all. `deflation_recovery` **PASS** |
| `S5-dieonload` | job aborts, 0 OOM negatives, 0 items failed, `idle_liveness` PASS |
| `S5-dying` | 180 re-queue WARNs + `requeued=180`; 180 `job_failures` each with path, sha256, mime, setter, `stage: "inference"`, `[worker_died]`; **6 spawns for 180 items**; 0 unified-device death negatives, deflation never left 0 |
| `S14-main-cu128` | tags + clip + docTR + whisper (`--scan-audio`, 185 indexed): **11 of 11 queue outcomes completed**, 0 item errors, both extra listeners (6743, 6739) answer 200, `file:` serves 1 065 119 B |
| `S14-florence2` | **180/180 captioned in 102 s**, 0 errors, 3 grants, 0 deaths |
| `S14-textembed-cu128` | the OCR chain on the regenerated `text` tier: 300 scanned pages → docTR **300 text rows, 0 errors** → `textembed/all-MiniLM-L6-v2`, 7 of 7 outcomes completed. **T7 is closed on this host** |

## 5. The owed baseline rows

Method per §4.24: a fresh store per leg, one model resident (`lru_size = 1`),
`loadgen.py` against the gateway, `calibration.after.toml` out. The shipped
baseline directory was masked for these legs (`nobaseline-config`, a mirror of
`python/inferio/config` without its `calibration/` subdirectory) so no prior
seeded the ramp.

| leg | id | unit | base_mb / method | slope MiB/unit | residual_mb | samples | max_units | knee |
|---|---|---|---|---|---|---|---|---|
| `B3-clipqwen8b-pkilled` | `clip/qwen3-vl-embedding-8b` | pixel | 31752 / nvml | 0.0 | 0.0 | 0 | 1048576 | - |
| `B3-clipqwen8b-scan` | `clip/qwen3-vl-embedding-8b` | pixel | 31752 / nvml | 0.00034207525655644244 | 0.0 | 9 | 8156100 | 4194303 |
| `B3-clipqwen8b` | `clip/qwen3-vl-embedding-8b` | pixel | 31752 / nvml | 0.0 | 0.0 | 0 | 1048576 | - |
| `B3-stella15` | `textembed/stella_en_1.5B_v5` | token | 6558 / nvml | 0.15012919896640828 | 4.7999999999999545 | 11 | 16254 | 8191 |
| `B3-stella400` | `textembed/stella_en_400M_v5` | token | 2236 / nvml | 0.07469054763690922 | 1.895161290322676 | 9 | 16512 | 8191 |
| `B3-tclipqwen8b` | `tclip/qwen3-vl-embedding-8b` | token | 31752 / nvml | 0.1867732558139535 | 13.625 | 7 | 7740 | - |
| `F4-tclipB` | `tclip/ViT-L-14_openai` | item | 1386 / nvml | 0.0 | 0.0 | 0 | 8 | - |
| `F4-tclipC-deep` | `tclip/PE-Core-bigG-14-448_meta` | item | 5538 / nvml | 2.1145833333333335 | 0.3333333333333144 | 7 | 256 | - |
| `F4-tclipC` | `tclip/PE-Core-bigG-14-448_meta` | item | 5538 / nvml | 2.09375 | 0.625 | 4 | 32 | - |

`doctr/dots_ocr` was **not attempted**: `flash_attn` does not import in the
venv (`ModuleNotFoundError`), which is the condition §4.24 names.

### What is new, and what was already there

Only **two** of the four ids were actually missing from the committed
`sm_120-linux-cuda.toml` at `0d7f5671`; the other two had been measured in
run2's sweep and shipped.

| id | status before | run4 | shipped now |
|---|---|---|---|
| `textembed/stella_en_400M_v5` | **no row** (its registry fix landed after the sweep) | base **2 236** / `nvml`, slope **0.07469055** MiB/token, residual **1.895**, **9** samples, knee 8 191, max_units 16 512, 832 local samples | **added** |
| `clip/qwen3-vl-embedding-8b` | **no row** (F1, the pre-fit clamp trap) | base **31 752** / `nvml`, slope **0.000342075** MiB/pixel, residual **0.0**, **9** samples, knee 4 194 303, max_units 8 156 100 | **added** |
| `tclip/qwen3-vl-embedding-8b` | shipped (0.17399691, 19 samples) | base 31 752, slope **0.18677326**, residual 13.63, 7 samples — **1.073×** the shipped figure | unchanged |
| `textembed/stella_en_1.5B_v5` | shipped (0.18033176, 41 samples) | base **6 558** (identical), slope **0.15012920**, residual 4.80, 11 samples, same knee 8 191 — **0.833×** | unchanged |

Both re-measured ids are **token-priced**, which §4.23/§4.24 already name as the
noisy unit on both cards (mpnet travelled at 0.906 between architectures). The
shipped rows carry more samples (19 and 41 against 7 and 11), so they stand;
run4 confirms their base to the MiB and their knee exactly.

### The clamp trap is fixed; `clip/qwen3-vl-embedding-8b` needed a different corpus

On the `ramp` tier (1024 × 1024 JPEGs) the id still fits nothing, and the
reason is **no longer F1's clamp**:

* 119 settled windows, **`clamped=none` on all 119**, `unit_budget=2000000` on
  all 119 — the registry seed in full, never halved. run2's F1 signature was a
  budget taken to 1 by the clamp; that is gone.
* But one 1024 × 1024 image prices at 1 048 576 pixels = **52.4 %** of the
  2 000 000-pixel seed, and two images (2 097 152) do not fit. So every window
  ran exactly one image, `budget_floor = ceil(0.8 × 2 000 000) = 1 600 000`
  (`ledger.rs:5023`, `FULL_BATCH_RATIO = 0.8` at `ledger.rs:162`) is never
  reached, the window is never "at budget", the ramp never steps, and the fit
  ring — one entry per distinct `units` — holds a single size. Hence
  `samples = 0`, `slope = 0`, `sample_units = [1048576]`.
* Driven over the `text` tier's 620 × 877 scans (543 740 px each, so three fit
  one seed budget at 81.6 %), the same id ramps 3 262 440 → 4 194 303 →
  8 699 840 units and fits **9 distinct sizes with residual 0.0**. That is the
  row now shipped.

This is design behaviour, not a defect — but it is worth recording that a
pixel-priced id whose single item lands between 50 % and 80 % of its seed budget
can never leave the seed on that corpus, and so can never be fitted from it.

### The top-up

`baselines.py --store merged-store.toml --registry python/inferio/config/inference.toml
--out python/inferio/config/calibration/sm_120-linux-cuda.toml` →
**87 measured, 1 generated**. Verified:

* a **strict superset** of the committed file — 86 of 86 existing rows present
  and **byte-identical on every field**, 2 added
  (`clip/qwen3-vl-embedding-8b`, `textembed/stella_en_400M_v5`, both
  `platform = "linux"`);
* **idempotent** — regenerating from its own output reproduces it byte for byte.

## 6. Host events that perturbed this pass

### 6.1 A concurrent sibling agent on the same box

Another session was running panoptikon on this host throughout. Three
collisions, all accounted for:

* **Ports.** My first two `S1` attempts (17:39, 17:42) failed to bind — the
  sibling's gateway held 6342 and then 6442 — and talked to *its* gateway
  instead. **Both leg directories were deleted before any analysis**; every
  leg in this report ran on **6742/6743/6739** with a pre-flight `ss` check,
  confirmed from each `legs.json`'s `base_url` (19 of 19 legs read
  `http://127.0.0.1:6742`). The `cal` database and two jobs my aborted attempts
  posted into the sibling's instance are the contamination it reported.
* **A foreign worker on GPU 0.** `S4c`'s first recording contains PID
  **2 981 472**, a `python -m inferio_worker` that is not one of mine
  (my only spawn in that leg is 2 974 427), holding up to **25 354 MiB** from
  **18:12:38 to 18:13:59**. It accounts for 156 of that leg's 183
  `oracle_agreement` breaches. I swept **all 19 legs** for a foreign
  `-m inferio_worker` PID against each leg's own
  `spawned an inferio worker … pid=Some(N)` lines: **S4c was the only one**.
  It was re-run clean (26 breaches, all at the hog transitions) and the
  polluted recording is kept as `S4c-contaminated/`.
* **`pkill -f panoptikon`, host-wide, at 19:59:54Z.** Exactly one of my legs
  spans that instant — `B3-clipqwen8b` (19:57:50 → 20:00:02), whose gateway logs
  `shutdown signal received` at **19:59:54.353** followed by four
  `worker_died` 500s. Every other leg ended before 19:57:50 or started at/after
  20:00:10. It was re-run uninterrupted (119 windows, same conclusion); the
  killed recording is kept as `B3-clipqwen8b-pkilled/`.

**Instrument gap this exposes.** `analyze.py`'s `our_pids_mb` claims a PID as
ours on any of three routes, one of which is a cmdline match against
`--worker-pattern` (default `inferio_worker`, `analyze.py:2263`). That route
cannot tell a *concurrent, unrelated* panoptikon instance's worker from our
own, so on a shared host it silently subtracts a stranger's footprint from
`gpu used` and manufactures an `oracle_agreement` disagreement. Only the
spawn-line route is safe. Worth a note in the README, or a flag to restrict
attribution to spawned PIDs.

### 6.2 The shared venv's CUDA stack was downgraded mid-pass

`/home/admin/projects/panoptikon/python/.venv` — the interpreter
`server-C1.toml:119` pins — holds **`nvidia-cudnn-cu12 9.5.1.17`**
(`dist-info` mtime **2026-09-16 17:27**, five minutes before this pass began)
together with the whole CUDA **12.6** runtime, under a `torch 2.7.1+cu128`
compiled against cuDNN **9.7.1**. `python/uv.lock` pins **9.7.1.26** for the
linux + cu128 extra; 9.5.1.17 is what the no-extra resolution picks, so the
venv looks to have been re-synced without the CUDA extra. Consequence:

```
RuntimeError: cuDNN version incompatibility: PyTorch was compiled against
(9, 7, 1) but found runtime version (9, 5, 1).
```

Every torch model that touches cuDNN fails to load — in this pass, **docTR**,
which failed `S14-main` and `S14-textembed`. **It cannot be worked around from
the leg's environment**: `accelerator_env.rs:63–94` prepends the interpreter's
own `site-packages/nvidia/*/lib` to the worker's `LD_LIBRARY_PATH` and appends
the ambient value after it (`merge_ld_library_path`, `accelerator_env.rs:296`),
so the venv's own 9.5.1 always wins.

Both legs were re-run with `inference_local.python` pointed at the
`panoptikon-master` worktree's venv, which carries the lock's CUDA 12.8 stack
and cuDNN 9.7.1.26, plus a one-file `pynvml` shim on `pythonpath` so the worker
keeps its NVML base tier. Both then **PASS**, docTR included.

**The control says the runtime mismatch does not move the memory model.** The
same `S2` leg on both interpreters: `base_mb` **964** / `base_method` `nvml` /
`max_units_measured` **256** on both, slope **29.859155** (12.6 stack) vs
**29.860821** (12.8 stack) — a ratio of 1.00006, both within 0.005 % of the
probe's 29.859375. The baseline rows in §5 were therefore measured on the
config-pinned venv, as the rest of the pass was.

Repointing the shared venv is not something this pass should do while another
session is using it; it is filed as a separate task.

### 6.3 The `text` corpus tier on disk predates its scanned pages

`results/corpus/text` was generated 2026-09-03 and holds only the 2 000 `.txt`
files, so `S14-textembed` indexed **0 files** and drained on nothing. The
current `corpus.py --tier text` emits 2 300 items including **300 scanned
pages** (`scan-620x877`, `scan-1240x1754`); regenerated into `corpus-text/` and
used for the re-run and for the `clip/qwen3-vl-embedding-8b` leg. Not a product
issue — the shared corpus is simply stale.

### 6.4 Two tooling observations

* **`legs.py --config C1` pins the interpreter over `--python`.**
  `server-C1.toml:119` sets `inference_local.python` to the main tree's venv
  absolutely; `legs.py --python` (line 1209) only chooses the interpreter for
  the *recorder* subprocesses. A leg cannot be moved to another venv without
  editing the config — which is what §6.2 had to do.
* **`host.json` does not reflect the leg's own environment.** `newrun.py`
  records `os.environ.get(key)` for its `ENV_KEYS` (`newrun.py:160`) in the
  process `legs.py` spawns *before* applying the per-configuration env file, so
  `RUST_LOG`, `CUDA_VISIBLE_DEVICES` and friends in `host.json` are the calling
  shell's, not the gateway's. (`read_env_file` itself is fine: it preserves an
  empty `CUDA_VISIBLE_DEVICES=` as `''` and passes it through.)

## 7. F4 — `tclip/PE-Core-bigG-14-448_meta`

**Question:** does the persisted row again show `max_units_measured` stuck at 8
while the windows ran at 192?

**Answer: no. It did not reproduce in either attempt.**

| attempt | leg shape | settled windows | budgets seen | persisted row |
|---|---|---|---|---|
| 1 | solo, `concurrency=4 items=8` (32 items in flight) | **7 319** | 8 (27), 16 (20), 24 (12), **32 (7 260)** | slope 2.09375, 4 samples, residual 0.625, **`max_units_measured = 32`** |
| 2 | solo, `concurrency=4 items=64` (256 in flight) | **3 209** | 8, 16, 32, 64 (1 259), 128 (690), **192 (1 252)**, 256 (4) | slope **2.1145833**, 7 samples, residual 0.333, **`max_units_measured = 256`**, 15 store writes |

Attempt 2 reproduces the sweep's stated conditions (a budget of 192 held for
1 252 windows, well past the 150 the finding cites) and the persisted row is
**correct** — 256, matching the shipped row's 256 and its slope 2.118714 to
0.2 %. In attempt 1 the persisted 32 is also correct: 32 is the largest batch
the queue ever offered, and the anchor is defined as what a clean batch on this
GPU actually ran.

**The shape did appear once in this run, on a different id, and it names the
mechanism.** A six-model `tclip` leg (`F4-tclipB`, my own mis-shaping — all six
shared one `cache_key` at `lru_size = 1`, so they thrashed) produced exactly
**one** settled window in 480 s, for `tclip/ViT-L-14_openai`, and its store row
reads `slope 0.0, samples 0, max_units_measured 8, local_samples 1` — F4's row
to the letter.

The path, in `panoptikon/src/inferio/ledger.rs` at `0d7f5671`:

* **`ledger.rs:5283–5291`** — `anchor` is raised only by a batch that carried a
  priced measurement (`units`, `peak_allocated_mb`, `allocated_at_load`); it is
  the largest *batch*, never the budget.
* **`ledger.rs:5472–5481`** — the runtime `cal.max_units_measured` advances when
  `anchor >= cal.max_units_measured`, and `anchor_measured_here` is set only out
  of a clean window. This is the "absorbed-OOM split": the runtime anchor may
  also be halved, at **`ledger.rs:4844–4857`** (`lower_seeded_anchor_locked`,
  seeded anchors only) and **`ledger.rs:4871–4905`**
  (`note_unified_death_locked`, unified-memory devices only) — neither fires on
  a discrete CUDA board with a locally measured anchor, and neither is what F4
  shows.
* **`ledger.rs:5487–5494`** — the figure that is *persisted* is
  `cal.max_units_measured_here` (`persistable_anchor`, **`ledger.rs:927–929`**),
  and it advances only under a four-way gate:
  `clean_window && anchor > here && !queue_bound && (reached_anchor || anchor >= budget_floor)`,
  where `budget_floor = ceil(unit_budget × FULL_BATCH_RATIO)` with
  `FULL_BATCH_RATIO = 0.8` (**`ledger.rs:5023–5024`**, **`ledger.rs:162`**), and
  `queue_bound` is true whenever the window carried no grant
  (**`ledger.rs:5033`**).
* **`ledger.rs:3323–3329`** — the store write is suppressed unless the fit
  version, the persistable anchor or the knee moved. So the first row written
  survives untouched for as long as none of the three changes.

Put together: a replica whose windows are *queue*-shaped rather than
budget-shaped runs batches far below `0.8 × unit_budget`, so
`max_units_measured_here` freezes at whatever the early windows reached — 8 is
the seed rung — while `/health` and the log go on publishing the ramped budget.
With only one distinct batch size the fit never forms either, so `fit_version`
stays 0 and no further write is ever queued. That produces run2's row exactly:
`max_units_measured = 8`, `slope = 0`, `samples = 0`, alongside 151 windows
logged at a budget of 192.

It is not a defect in the anchor — the anchor is documented as "what a clean
batch on this GPU actually ran", and holding it back from a budget the host
never filled is the safe direction. What the finding really shows is that
**`max_units_measured` in a persisted row must not be read as "the budget this
host reached"**, and that a sweep leg whose queue is shallower than its budget
will ship a row that understates the anchor. The run2 sweep's `B3-tclip*` legs
put several ids behind one queue; the id whose requests were rarest would be
the one to freeze.

No product code was changed.

## 8. Files

* `instruments/` — `platform-selftest.json`, `oracle-calibration.json`
* `probes/` — `probe-wd.json`, `bisect-wd.json` and their console output
* `<leg>/verdicts.json`, `legs.json`, `calibration.after.toml`, `jobs.json`,
  `failures*.json`, `health-*.json`, `runlog.md` — committed
* `<leg>/vramrec.jsonl`, `healthrec.jsonl`, `panoptikon.log`, `root/` — left on
  disk, not committed (2.4 GB)
* `merged-store.toml` — the committed baseline plus the two new rows, the input
  to `baselines.py`
* `config/`, `nobaseline-config/`, `pynvml-shim/`, `corpus-text/` — the leg
  configuration this pass ran with
