# final — sm_86 regression pass on the merged tip `2133563f`

RTX 3090 (24 576 MiB, sm_86), `systemvi`, Ubuntu 24.04.4 / 6.8.0-138,
driver 580.159.03 / CUDA 13.0, 48 cores, 377 GB RAM, torch 2.7.1+cu128,
python 3.12.10. Tip **`2133563f`** ("Design doc: what the reload does, and
what clears a condemnation") — the merged branch tip with every fix of
2026-09-16/17 in it — pushed to the host as `final` and built there with the
`~/.cache/ossl` workaround (run4 §5, F5): 1 m 27 s,
`target/release/panoptikon`, **84 065 560 B**, 2026-09-17 02:56 local.
Config C1 with the host's own path overrides (unchanged from run4, plus
`HF_HOME`). `ui` gitlink `ce7506ae`.

**Wall clock.** Checkout and build 00:54–00:56 UTC; instruments 00:57–01:00;
corpora 01:02–01:05; the 28-leg pass 01:09:43 → 02:19:18 UTC (**69 m 35 s**);
two re-runs to 02:23. Host total **1 h 29 m**. The GPU was left at **1 MiB**
with no process of ours running.

Comparands: `../run4/report.md` (49 legs at `0d7f5671`) and
`../run5-anchor/report.md` (the F1 fix pass at `c7130a19`). `root/` (per-leg
databases and thumbnails) was left on the host and is not committed; the four
recordings are gzipped, as in run4.

## 1. Instruments

| Fact | final (`2133563f`) | run4 (`0d7f5671`) |
|---|---|---|
| `pytest tools/calibration-protocol/tests` | **204 passed, 1 skipped** | 166 passed, 1 skipped |
| `selftest.py` verdict | **no degraded tiers** | same |
| `base_mb` / `base_method` (wd-vit) | 670 / `nvml` | 670 / `nvml` |
| `allocated_at_load_mb` / `reserved_at_load_mb` | 361 / 414 | 361 / 414 |
| free tiers | `nvml` 23 445, `free_delta` 678, `alloc_delta` 861 | same |
| induced over-allocation | typed `torch.OutOfMemoryError`, `oom_class.source = typed_exception`, **835 MiB** free at failure | identical |
| `oracle_calibrate.py --sizes 2560,10240` | **PASS / PASS**, payload deltas **2 563 / 10 243**, 0 OOM | PASS / PASS, 2 563 / 10 243 |
| `ceiling_probe` ground truth | run4's `probe-wd.json` / `bisect-wd.json` reused verbatim (slope 29.859375 MiB/item, boundary 639) | measured here |

The instruments reproduce run4 to the digit. The probe was **not** re-measured:
run4's files are from this same card and reusing them keeps every
`utilization` denominator (639) and `slope_accuracy` ratio directly
comparable.

## 2. Side by side, leg by leg

`PASS*` = the leg passes with one adjudicated check, named in the last column.

| Leg | final | run4 | The number that moved |
|---|---|---|---|
| `S1` (external hog leaves 2 048 MiB) | **PASS\*** | PASS\* | 8 grants, 1 → 8 units at `mb ≤ 299`; 0 over-grant, 0 blind, 0 OOM; 180/180. `ledger_invariant` WARN **22 of 254** `limit_fell` (run4 21 of 127), 0 `over_grant` |
| `S2-wdvit` (seeded) | **PASS** | PASS | slope **29.858268** vs probe 29.859375 = **1.0000**; max granted **256 units** (run4 **399**), published 512 both; `utilization` **0.40** (run4 0.62); 7 grants (6); 25.316 items/s (25.641) |
| `S2-wdvit-unseeded` | **PASS** | PASS | ramp **2 → 32**, knee **31**, `utilization` **1.00 at the learned knee** (run4: ramp 2 → 256, 0.40); 70 grants (13); slope 29.909 = 1.0017; 26.667 items/s (24.390) |
| `S3-wdvit` (restart + resume) | **PASS** | PASS | resume reaches **256 units**; `persistence` PASS, worst anchor→write **1.1 s** (1.6 s); 12 grants (11); 25.000 items/s |
| `S4a` (hog leaves 3 085 MiB) | **PASS\*** | PASS\* | 21 grants, **0 blind**, 0 unsafe; max 35 units; 2 000/2 000 at **26.667** items/s (24.390). `oracle_agreement` FAIL **2 of 233** (2 of 246); `utilization` FAIL 0.05 against the full-card boundary |
| `S4b` (step up at t+60 s) | **PASS\*** | PASS\* | 14 grants (13), 0 unsafe, 8 000/8 000 at **27.027** items/s (26.490); `utilization` 0.46 at 293 units — unchanged. `oracle_agreement` FAIL **16 of 678** (18 of 690) |
| `S4c` (spike to 2 048 MiB at t+90 s) | **PASS\*** | PASS\* | granted up to **615 units / 22 983 MiB** (run4 528 / 22 970), published 1 024 (926), min headroom **1 582 MiB** (56), **0 OOM**, 0 collapse; 26.490 items/s. `oracle_agreement` FAIL **21 of 687** (20 of 674) |
| `S4d` (hog releases at t+120 s) | **PASS\*** | **FAIL (F1)** | **the hold lifts**: 64 → **128** with one re-test line, `held_certified` **true** for 313 samples; `utilization` **0.20** at the 639 boundary (run4 0.10; 0.25 at run5-anchor's 512 boundary, its PASS figure); 180 grants, 0 unsafe, 0 OOM; **28.070** items/s (27.397) |
| `S4d-repeat` | **PASS\*** | FAIL | identical: 64 → 128, one re-test line, `held_certified` true for 306 samples, 179 grants, 28.571 items/s |
| `S5-oom2` | **PASS\*** | PASS\* | exactly **1** OOM negative, 1 fallback, tier `marker/trusted`, job completed, 0 items lost |
| `S5-oom1` | **PASS\*** | PASS\* | 57 OOM negatives, 56 fallbacks, deflation 1 → 4, job `failed`, 180/180 items failed, **0 worker deaths**, 0 unsafe |
| `S5-failbatch` | **PASS** | PASS | 30 fallbacks, **0** OOM negatives, deflation never left 0 |
| `S5-oomtext` (B11) | **PASS** | PASS | 30 fallbacks whose text says "out of memory", **0** OOM negatives |
| `S5-oomtimed` | **PASS\*** | PASS\* | 57 negatives, deflation 4, job `failed` — the recovery half is still unreachable on the job path |
| `S5-dying` | **PASS\*** | PASS\* | **6** fatal worker deaths, each respawned, 180 items in `/failures`, no respawn storm, 0 synthetic negatives |
| `S5-diesonload` | **PASS\*** | PASS\* | cooldown ladder **2.0 s → 4.0 s**, job `failed`, 0 fatal deaths. `job_outcome` **PASS → FAIL**: the new zero-item rule, see T3 |
| `S5-oomimpl` (MobileCLIP + poison) | **PASS\*** | PASS\* | 1 item failed, job **`partial`**, `oom=false`, deflation 0. Needed the T2 workaround to start |
| `S14-tags` | **PASS** | PASS\* | **0 throughput-collapse negatives** (run4: 1) — F3 fixed, see §4 |
| `S14-clip` | **PASS** | PASS | 180/180, 0 errors, 4 grants, all five smoke assertions 200 |
| `S14-ocr` | **PASS** | PASS | 180/180, 0 errors, 3 grants |
| `S14-whisper` (`--scan-audio`) | **PASS** | PASS | 5 audio items, 0 errors, model unpriced (`grant_safety` SKIP by design) |
| `S14-florence2` | **PASS** | PASS | 180/180, 0 errors, 5 grants, max 16 units / 7 121 MiB |
| `S14-textembed` | **PASS** | FAIL → PASS on re-run (F4) | on a freshly generated `text` corpus: doctr **300/300** pages, textembed **300/300** rows, 0 errors, first time |
| `sc8-S2` (wd-vit seeded, 8 GB free) | **PASS\*** | PASS\* | max granted **145 units / 6 426 MiB** (run4 132 / 6 420); 0 blind/OOM/over_grant; 25.974 items/s. `utilization` FAIL 0.23 (0.21); `ledger_invariant` **PASS → WARN**, 101 `limit_fell`, **0 `over_grant`** |
| `sc8-S2-unseeded` | **PASS** | PASS | **128** grants (69), max **64 units / 6 499 MiB**, knee **15** (31), `utilization` 1.00 at the knee, 26.316 items/s |
| `sc8-S2-vith` (ViT-H, 8 GB free) | **PASS\*** | PASS\* | max granted **348 units / 4 808 MiB** (run4 331 / 4 805; run5-anchor 348), 4 grants, **0 OOM**, 0 re-test lines; **35.088** items/s (32.258). `calibration_learned` FAIL — nothing learned under a conferred 512, unchanged |
| `sc8-S14-flor` (florence2, 8 GB free) | **PASS** | PASS | max **12 units / 5 026 MiB**, identical to run4 to the MiB; 6 grants, 0 OOM/blind |
| `sc8-S4a` (wd-vit, 982 MiB free) | **FAIL** | PASS\* | **the load is refused**: 0 grants, 0 items, job `failed`. run4 ran 2 000/2 000 at 22.727 items/s on 648 memory-blind one-item grants. **Finding P1** |

`sc8-S4a-repeat` reproduces the refusal exactly (982 MiB free, same message,
3 refusals through the cooldown ladder).

## 3. The confirmations asked for

**0 OOM / 0 `over_grant` / 0 memory-blind on the small-card legs.** Over the
four small-card legs that admitted work (`sc8-S2`, `sc8-S2-unseeded`,
`sc8-S2-vith`, `sc8-S14-flor`): **145 grants**, `grant_safety` PASS on all
four — 0 above their priced headroom, 0 above the oracle's live free memory
plus their own pool, **0 memory-blind**, **0 OOM negatives**, **0
`over_grant`** in `ledger_invariant`. The `limit_fell` counts (101 on
`sc8-S2`, 60 on `sc8-S2-vith`, 0 on the other two) are the known residue and
carry no `over_grant`. The fifth leg, `sc8-S4a`, has 0 of everything because
nothing was ever admitted — see P1; that is a job failure, not a memory
breach.

**The `S14-tags` heterogeneous smoke leg no longer deflates a healthy
replica.** The worker's comparator still fires, once, exactly where run4 saw
it — the second window, 116 units against the previous 63:

```
[WARNING] inferio_worker.packing: batch of 116 inputs (116 item units) ran at
14 units/sec against 35 for the previous growing batch of 63 units; treating
it as a memory spill (driver sysmem fallback)
```

and the ledger now discards it instead of counting it:

```
DEBUG panoptikon::inferio::ledger: ignored this window's throughput-collapse
flags: the pool grew by less than the device had free, so no batch this
window ran spilled to host memory and the rate drop is the impl's own decode
cost. Discarded rather than counted clean
model=tags/wd-vit-tagger-v3 uncorroborated_collapses=1
```

Result: `failures` PASS with **0 throughput-collapse negatives** (run4: 1),
deflation never leaves 0, and the replica is not deflated. Across all 29 legs
of this pass the spill warning appears in **exactly one** log
(`S14-tags`), and the `uncorroborated_collapses` line appears in the same
one — no other leg produced a candidate, and none was ever counted.

**Cold-cache behaviour: nothing re-downloaded.** `HF_HOME` is the warm
`~/.cache/panoptikon-hf` (85 GB) and `~/.cache/doctr` (915 MB) is populated;
docTR logs "Using downloaded & verified file" for both of its weights. Load
times are the warm column of run4 §4: florence2 spawn → footprint **11.5 s**
(run4 warm 15.19 s, cold 24.65 s), ViT-H **19.0 s** (warm 20.63 s, cold
35.47 s). `load_cooldowns` is empty in every leg except the
`dies_on_load_cuda` fixture's deliberate 2 s → 4 s and the three refusals in
`sc8-S4a` (P1). No handshake or `load_secs` timeout fired anywhere.

## 4. Findings

**P1 — a model that fits on the card is refused because the reserve ate the
budget, and the job fails.** `sc8-S4a`, reproduced in `sc8-S4a-repeat`. The
external hog leaves 8 192 MiB free, the leg's own hog (scaled against
`--gpu-total-mb 8192`) presses to **981 MiB** free, and the gateway answers:

```
ERROR panoptikon::inferio::http: failed to load model
  model=tags/wd-vit-tagger-v3
  error=failed to load model tags/wd-vit-tagger-v3: model
  tags/wd-vit-tagger-v3 needs about 670 MiB on GPU GPU-c120e653-…,
  which has room for 0 MiB; not loading it
WARN  panoptikon::inferio::manager: load failed; refusing further loads of
  this model until the cooldown expires failures=1 cooldown_secs=2.0
ERROR panoptikon::jobs::queue: job failed … status: 500
```

The card physically has **981 MiB** free and the model needs **670**. The
"room" the refusal is priced against is `refusal_room_locked` →
`limit_locked` (`ledger.rs` ~4092), i.e. `room − external − reserve` with
`reserve = min(external × 0.10, 1 024)` = the full `DEFAULT_RESERVE_CAP_MB`,
so `limit = max(0, 981 − 1 024) = 0`. This is `9e018083` ("refuse a model the
GPU cannot hold instead of OOMing per item") firing on a model the GPU *can*
hold. run4 at the same pressure completed the job: 2 000/2 000 items,
22.727 items/s, 648 memory-blind one-item grants, **0 OOM**. So on this leg
the new rule does not prevent an OOM that was happening — it converts a slow
but correct job into a hard 500. run4 §3 already named the flat 1 024 MiB
reserve as "a large fraction of a small card"; this tip turns that from a
throughput cost into an availability one. The three `analyze.py` checks that
went `PASS → SKIP/WARN/FAIL` on this leg (`grant_safety`, `utilization`,
`job_outcome`) all say the same thing: nothing was admitted.

**T1 — `corpus.py` cannot regenerate the `pdf` and two `junk` items.**
`_text_page` has returned `(image, meta)` since `d97094ad` (2026-09-06), but
`gen_pdf` and `gen_junk` still call `_base_image(..., text_page=True)` and
use the result as an image: `AttributeError: 'tuple' object has no attribute
'save' / 'convert'`. It never fired before because no corpus had been
regenerated since; the new corpus stamp (T2) forces regeneration and exposes
it. Cost here: the `smoke` tier is **200 items instead of 205** (its 5 PDFs
are gone) and the `poison` tier's manifest is **2 items instead of 4** (the
truncated-JPEG and named-OOM junk). Neither touches a measurement — every
S14 and S5 leg runs the tier's 180 images, and `S5-oomimpl`'s huge PNG is
intact (the leg still indexed 3 files and still ends `partial` with 1 failed
item, exactly as run4).

**T2 — `legs.py`'s new corpus check refuses two legitimate corpora.**
`corpus_complaint` (`00f22c55`) compares the manifest's stamped **tier**
against `Scenario.corpus`, which is a **directory name**:

* S4b/S4c/S4d declare `ramp8`. `corpus.py` has no `ramp8` tier, so the check
  refuses any corpus for them, and the remedy it prints
  (`corpus.py --tier ramp8 …`) is rejected by argparse. **All three legs are
  unstartable at this tip.**
* It also ignores `--corpus`. `S5-oomimpl`, run on `poison` exactly as run4
  ran it, was refused with *"corpus …/poison is the 'poison' tier, this leg
  needs 'smoke'"* — the documented flag cannot be used for another tier.

Worked around without touching the repository, by a wrapper
(`runleg.py`, committed beside the results) that strips the trailing scale
digits before the comparison and, when `--corpus` was given explicitly, keeps
the generator-version clause and drops the tier clause. The one-line fix the
defect needs is the same: compare `tier.rstrip("0123456789")`, and take the
tier from the manifest when `--corpus` names the directory.

**T3 — the new zero-item rule mis-fires on `dies_on_load`.** `S5-diesonload`
is *defined* by a model that never loads, so its setter legitimately records
0 items; `legs.py` now marks the leg `no_items` and exits 1, and
`analyze.py`'s `job_outcome` FAILs with "NO ITEMS … nothing here measures
anything". The recordings are complete and every other check reads as in
run4 (cooldown ladder 2.0 → 4.0 s, job `failed`, 0 fatal deaths). The rule
wants an opt-out for a scenario whose expectation is zero items —
`--expect-failed-jobs` already exists and could carry it.

**F2 is fixed.** `peak_fds` now prices the peak against the gateway's
post-raise limit: every leg reports *"soft limit **1048576** (0% of it)"*
against run4's *"soft limit 1024 (5–11 %)"*. Peaks themselves are unchanged
(48–153 descriptors).

**F1 is fixed, on both seeded S4d legs.** Confirmed above and in §2; the
mechanism matches run5-anchor exactly (`HOLD_REPROBE_WINDOWS = 4`, ceiling
`min(205, 64 << 1) = 128`).

**F3 is fixed.** Confirmed in §3.

**F4 does not recur,** and the guard that would have caught it is now in
place (T3 is its over-reach).

### Known residues, unchanged

* **`oracle_agreement` FAIL under a moving hog**: S4a 2 of 233, S4b 16 of
  678, S4c 21 of 687, S4d 1 of 654, S4d-repeat 2 of 646, sc8-S4a 6 of 90.
  Every idle-card leg is PASS with a worst disagreement of 55–67 MiB.
* **`ledger_invariant` WARN with `limit_fell` only**: S1 22, S4a 134, S4d
  227, S4d-repeat 226, sc8-S2 101, sc8-S2-vith 60 — **0 `over_grant`
  everywhere**, on every one of this pass's 864 grants.
* **`utilization` judged against the full-card boundary** on hogged legs;
  §2 gives the per-leg numbers.
* **`base_accuracy`** stays INFO ("not judged") on idle legs; run4's two
  hogged FAILs are gone (`sc8-S2` FAIL → INFO at 0.59 %), and S1 now reads
  87 % against an 8 ms-old sample — the same per-PID NVML lag, on the other
  leg.
* **`sc8-S2-vith` learns nothing** under a conferred anchor of 512 the card
  can never measure: `calibration_learned` FAIL, 0 re-test lines (the probe
  cannot arm on a board the anchor does not fit). Identical to run5-anchor.
* **`calibfixture/oom_timed_cuda`'s recovery half** is still not reachable on
  the job path.
* `S5-oom2`'s `deflation_recovery` / `ramp_progress` went PASS/INFO → SKIP:
  the 500 ms health poll caught a worker in **0 of 100** samples this time
  against **1 of 100** in run4, on a job that lasts ~10 s. Sampling luck, not
  a product change.

## 5. Deviations from run4's procedure

* The corpora were **regenerated** (`corpus.py` now stamps
  `generator = 2` and `legs.py` refuses an unstamped one). Same seeds, so the
  images are byte-identical; the losses are T1's.
* `S4b`/`S4c`/`S4d` ran on `ramp4` (8 000 items), as run4 and run5-anchor
  did, not the scenario table's `ramp8`.
* The **unseeded** legs ran against `server-C1u.toml`, a copy of C1 whose
  first `config_dirs` entry is `noseed-config/` — a directory holding a
  symlink to `inference.toml` and **no `calibration/`** — so the shipped
  sm_86 baseline (now **162 rows**) is invisible. The repository working tree
  was not touched.
* `ceiling_probe.py` was not re-run; run4's files were reused (§1).
* `S14-tags` was run as the pipeline validation leg at 01:07 UTC, before the
  driver started, on the same binary, config and corpus.
* `analyze.py` was given run4's per-leg `--expect-*` flags so the S5 verdicts
  are comparable.

## 6. Layout

```
final/
  report.md                  this file
  platform-selftest.json     selftest.py --induce-oom
  oracle.json/               oracle_calibrate.py
  probe-wd.json bisect-wd.json   run4's ceiling_probe ground truth, reused
  drive.sh analyse.sh runleg.py  the driver, the analysis, the T2 wrapper
  server-C1u.toml env.C1u noseed-config/   the unseeded configuration
  analyse-all.txt drive.log  the whole pass's analysis and timing
  <leg>/<scenario>/          legs.json, panoptikon.log.gz, vramrec.jsonl.gz,
                             healthrec.jsonl.gz, hog.jsonl.gz, fds.jsonl,
                             jobs/failures/metadata/health snapshots,
                             calibration.after.toml, analyze.txt, verdicts.json
  <leg>.log                  the driver's own output for that leg
```

`gunzip` the four recordings before re-running `analyze.py --scenario
<leg>/<scenario>`, which reads the plain names.
