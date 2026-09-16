# run4 — sm_86 regression pass, small-card simulation and first-use downloads

RTX 3090 (24 576 MiB, sm_86), `systemvi`, Ubuntu 24.04 / 6.8.0-138, driver
580.159.03 / CUDA 13.0, 48 cores, 377 GB RAM, torch 2.7.1+cu128, python
3.12.10. Tip `0d7f5671` (master merged into the batch-calibration branch on
2026-09-10), binary built **on this host** (`target/release/panoptikon`,
83 796 168 B, 2026-09-16 18:01 UTC). Config C1 with the host's own paths.
`ui` gitlink `ce7506ae`. 2026-09-16, 17:29–20:30 UTC.

Everything under this directory is a recording; `root/` (per-leg databases and
thumbnails, 1.1 GB) was left on the host and is not committed. Every leg was
driven by `legs.py` (`<leg>/<scenario>/legs.json` has the exact command line)
and read by `analyze.py` (`analyze.txt`, `verdicts.json` per leg).

**What is new about this tip on this card:** the binary now ships
`python/inferio/config/calibration/sm_86-linux-cuda.toml`, the 83 rows measured
on *this* 3090 in run2 §4.23. Every leg below is therefore **seeded from a
shipped baseline** unless its name says `-unseeded`, which is a different
condition from the §4.13 Ampere pass. Where it changes a verdict, both legs
were run.

## 1. Instruments (§0 of the platform pass)

| Fact | run4 | §4.13 Ampere pass |
|---|---|---|
| `selftest.py` verdict | **no degraded tiers** | same |
| `base_mb` / `base_method` (wd-vit) | **670** / `nvml` | 670 / `nvml` |
| `allocated_at_load_mb` / `reserved_at_load_mb` | 361 / 414 | 361 / 414 |
| CUDA context (`base − allocated`) | **309** | 305–309 |
| `nvidia-smi` vs torch total | 24 576 vs 24 124 MiB — **451.8 MiB, 1.84 %** | same |
| free tiers | `nvml` 23 445 / 24 576 and `torch` 23 445 / 24 124 agree | same |
| induced over-allocation | **typed** `torch.OutOfMemoryError`, `oom_class.source = typed_exception`, 835 MiB free at failure | typed |
| `pytest tools/calibration-protocol/tests` | **166 passed, 1 skipped** | — |
| `oracle_calibrate.py` 2 560 / 10 240 MiB | **PASS / PASS**, payload deltas 2 563 / 10 243, 0 OOM | PASS both |
| `ceiling_probe` sweep (allocated basis) | **29.859375** MiB/item, residual 0.4219, n = 20, intercept 369.4 | 29.859375, residual 0.42, n = 20 |
| `ceiling_probe` reserved basis | 37.5625 | 37.563 |
| `ceiling_probe --bisect-oom` | **639 OK / 640 OOM** | 639 / 640 |

The instruments reproduce the Ampere pass to the digit, including the bisect
boundary and the probe fit; `probe-wd.json` / `bisect-wd.json` are in this
directory and were passed to every wd-vit leg's `analyze.py`.

## 2. Regression pass, full 24 GB card

Per-leg verdicts are in `<leg>/<scenario>/analyze.txt`. `PASS*` means one or
more rows were adjudicated; the adjudication is named in the last column and
justified in §5.

| Leg | Verdict | The number that decides it | Adjudicated rows |
|---|---|---|---|
| `S1` (external hog leaves 2 048 MiB free) | **PASS*** | 8 grants, 0 over the priced headroom, 0 over the oracle's live free, 0 memory-blind; 180/180 items, 0 OOM, 0 deaths; grants 1 → 8 units at `mb ≤ 299` against `limit 971` | `ledger_invariant` WARN: 21 of 127 samples `limit_fell`, **0 `over_grant`** |
| `S2-wdvit` (cold ramp, seeded) | **PASS** | ledger **29.859060** vs probe 29.859375 = **1.0000**; ramp 128 → 512 published, granted to **399 units / 18 923 MiB**; 0 OOM, 0 deaths, `calibration_learned` PASS | — |
| `S2-wdvit-unseeded` (no shipped baseline) | **PASS** | ramp 1 → 256 from the seed; ledger slope 29.8601 = 1.0000; 13 grants, 0 unsafe; 24.390 items/s against the seeded leg's 25.641 | — |
| `S3-wdvit` (restart + resume) | **PASS** | second gateway seeds `local=true fit_is_local=true confirms=true` from its own store and reaches **256 units in 2.0 s** against 16 s cold; `persistence` PASS (6 writes, worst anchor→write 1.6 s); store ends at `max_units_measured = 256`, `local_samples = 21` | — |
| `S4a` (constant, hog leaves 3 019 MiB free) | **PASS*** | 21 grants, **0 memory-blind** (§4.13's D2 had 2 613 of 2 615), 0 unsafe, 2 000/2 000 at 24.390 items/s | `utilization` FAIL 0.05 against the **full-card** boundary; at the hog's free level the boundary is ~78 units and 35 were granted = **0.45**. `oracle_agreement` FAIL 2 of 246 |
| `S4b` (step up +7 713 MiB at t+60 s) | **PASS*** | 13 grants, 0 unsafe, 8 000/8 000 at 26.490 items/s; the step reaches `/health` **8.1 s** after the hog filled — inside one in-flight window (293 units ≈ 11 s), which is the rule the protocol sets | `oracle_agreement` FAIL 18 of 690 |
| `S4c` (spike to 2 048 MiB free at t+90 s) | **PASS*** | granted up to **528 units / 22 970 MiB**, min headroom 56 MiB, **0 OOM**, 0 throughput-collapse, 8 000/8 000 at 27.027 items/s | `oracle_agreement` FAIL 20 of 674 |
| `S4d` (hog releases at t+120 s) | **FAIL** | the budget does **not** grow back: the ramp is held at **64 units** from t+2.6 s to the end of the job, including ~3 minutes with `headroom_mb = 19 922`. Reproduced in `S4d-repeat`; the `S4d-unseeded` control recovers. **Finding F1** | `oracle_agreement` FAIL 1 of 670; `ledger_invariant` WARN 227 `limit_fell`, 0 `over_grant` |
| `S5-oom2` (`oom_second_batch_cuda`) | **PASS*** | exactly **1** OOM negative, 1 merged-window fallback, tier `marker/trusted`, job **completed**, 0 items lost | `failures` WARN is the check's shape for any expected negative |
| `S5-oom1` (`oom_cuda`, batch-1 OOM always) | **PASS*** (fixture's intent) | 57 OOM negatives, 56 fallbacks, deflation 1 → **4** and stays, job `failed`, 180/180 items failed, **0 worker deaths**, 0 unsafe grants | `deflation_recovery` FAIL is the fixture: nothing can rescue a batch-1 OOM |
| `S5-failbatch` | **PASS** | 30 merged-window fallbacks, **0** OOM negatives, deflation never left 0, job completed | — |
| `S5-oomtext` (B11) | **PASS** | 30 fallbacks whose text says "out of memory", **0** OOM negatives, deflation 0 | — |
| `S5-oomtimed` (120 s of batch-1 OOM) | **PASS*** (see §5) | 57 negatives, deflation 4, job `failed`. Re-run on 8 000 items (`S5-oomtimed-8k`) still fails every item inside the fixture's 120 s, so the *recovery* half is not reachable on the job path | recovery half **not covered** |
| `S5-dying` | **PASS*** | **6** fatal worker deaths, each respawned, 180 items in `/failures`, job `failed`, no respawn storm, 0 synthetic negatives | `failures` WARN counts the expected deaths |
| `S5-diesonload` | **PASS** | cooldown ladder **2.0 s → 4.0 s**, loads and predicts refused with `retry_after_secs`, job `failed`, 0 fatal deaths; run2's F6 does not reproduce | — |
| `S5-oomimpl` (MobileCLIP + poison corpus) | **PASS*** | 1 item failed, job **`partial`**, `oom=false`, deflation 0 | `job_outcome` counts `partial` as not-completed |
| `S14-tags` | **PASS*** | 180/180, 0 errors; pql 200, thumbnail 200, file 200, endpoints 6343 → 200 and 6339 → 200 | 1 `throughput_collapse` negative — **Finding F3** |
| `S14-clip` (`clip/apple_MobileCLIP-S1`) | **PASS** | 180/180, 0 errors, 4 grants, 0 unsafe; all five smoke assertions 200 | — |
| `S14-ocr` (`doctr/db_resnet50_crnn_mobilenet_v3_small`) | **PASS** | 180/180, 0 errors | — |
| `S14-whisper` (`--scan-audio`, `whisper/tiny`) | **PASS** | 5 audio items transcribed, 0 errors, model unpriced (`unit = "none"`) as designed; loads with the C1 `LD_LIBRARY_PATH` | — |
| `S14-florence2` (`msft_large-caption`) | **PASS** | 180/180, 0 errors, base 1 738 MiB, `inference_work_secs` 1 753 over 24.6 s busy | — |
| `S14-textembed` (as shipped corpus) | **FAIL → PASS on re-run** | the box's `text` corpus predates the scanned pages: `total_available = 0`, **0 job records**, and every check still PASSed. Re-generated (`S14-textembed-r4`): doctr **300/300** pages, textembed **300/300** rows, 0 errors. **Finding F4** | — |

Nothing in the pass produced an OOM outside a fixture, a worker death outside a
fixture, an `over_grant`, or a grant above the oracle's live free memory: over
**1 900 grants** across 30 legs, `grant_safety` is PASS or SKIP everywhere and
its oracle clause joined every grant it was given.

## 3. Small-card simulation (8 GB and 12 GB free)

**How the figure was produced.** An external `hog.py --target gpu --device 0
--reeval 999999 leave-free <L>` is filled *before* the gateway starts, so the
card presents `L` MiB free to everything that follows: `leave-free` reads NVML
free once and then pins (`--reeval 999999`), which is what keeps our own pool
growth from making the hog take more. Measured fill: **8 200 MiB** and
**12 296 MiB** free (targets 8 192 / 12 288), and `/health` then reports
`total − external` = 8 139 / 12 235 MiB. Legs that carry their own hog schedule
(S4a, S4c) additionally get `--gpu-total-mb <L>`, so `legs.py`'s scaling rule
resolves their fractions against the *simulated* board: S4a's `leave-free`
becomes 1 029 MiB at 8 GB and 1 543 MiB at 12 GB, and S4c's spike stays the
absolute 2 048 MiB the defensive clamp is defined against. Their own hog then
presses the card the rest of the way down, on top of the external one.

**Why the simulation is faithful.** The ledger's limit is
`room − external − reserve` with `reserve = min(external × margin, 1 024 MiB)`
(`DEFAULT_RESERVE_CAP_MB`), and `.min(total)` is the only term keyed to the
board's own size — non-binding whenever the hog holds the difference. So the
quantities that price a window (free, external, reserve, headroom) take exactly
the values a real 8/12 GB card would show. What the simulation cannot
reproduce is anything keyed to `total_mb` alone; no such term binds here.

| Leg | model | free (MiB) | limit (MiB) | shipped anchor | published `unit_budget` | **max granted units** | max granted MiB | min headroom | blind | OOM | `over_grant` | collapse | items/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `sc8-S2` | wd-vit | 8 139 | 7 115 | 205 | 128 → 410 | **132** | 6 420 | 207 | 0 | 0 | 0 | 0 | 25.641 |
| `sc8-S2-unseeded` | wd-vit | 8 139 | 7 115 | — | 4 → 64 | **32** | 6 499 | 4 761 | 0 | 0 | 0 | 0 | 26.316 |
| `sc8-S2-vith` | ViT-H-14-quickgelu_dfn5b | 8 133 | 7 109 | 512 | 331 → 512 | **331** | 4 805 | 0 | 0 | 0 | 0 | 0 | 32.258 |
| `sc8-S4a` | wd-vit | 922 | 0 | 205 | 128 | **1** | 0 | 0 | 648 | 0 | 0 | 0 | 22.727 |
| `sc8-S4a-unseeded` | wd-vit | 922 | 0 | — | 4 → 64 | **4** | 0 | 0 | 664 | 0 | 0 | 0 | 23.529 |
| `sc8-S4c` | wd-vit | 7 834 → 2 048 spike | 6 810 | 205 | 128 → 410 | **138** | 6 134 | 0 | 0 | 0 | 0 | 0 | 27.586 |
| `sc8-S14-tags` | wd-vit | 8 139 | 7 115 | 205 | 64 → 128 | **116** | 4 431 | 4 029 | 0 | 0 | 0 | 1 | — |
| `sc8-S14-clip` | MobileCLIP-S1 | 8 131 | 7 107 | 64 | 31 | **31** | 325 | 6 401 | 0 | 0 | 0 | 0 | — |
| `sc8-S14-flor` | florence2 large-caption | 8 129 | 7 105 | 28 | 16 → 56 | **12** | 5 026 | 0 | 0 | 0 | 0 | 0 | — |
| `sc12-S2` | wd-vit | 12 235 | 11 211 | 205 | 128 → 468 | **234** | 10 512 | 327 | 0 | 0 | 0 | 0 | 25.000 |
| `sc12-S4a` | wd-vit | 1 434 | 410 | 205 | 128 | **1** | 0 | 0 | 647 | 0 | 0 | 0 | 23.529 |
| `sc12-S4c` | wd-vit | 11 930 → 2 048 spike | 10 906 | 205 | 128 → 502 | **251** | 10 225 | 0 | 0 | 0 | 0 | 26.403 |
| `sc12-S14-tags` | wd-vit | 12 235 | 11 211 | 205 | 64 → 128 | **115** | 4 378 | 8 091 | 0 | 0 | 0 | 1 | — |
| `sc12-S14-clip` | MobileCLIP-S1 | 12 227 | 11 203 | 64 | 31 | **31** | 325 | 10 501 | 0 | 0 | 0 | 0 | — |
| `sc12-S14-flor` | florence2 large-caption | 12 225 | 11 201 | 28 | 16 | **16** | 7 121 | 2 333 | 0 | 0 | 0 | 0 | — |

Answering the four questions the scenario asks, per leg:

* **Did the ramp hold below the free memory?** Yes, on every leg. The largest
  granted window is always under the free the card presented (8 GB: 6 420 of
  8 139; 12 GB: 10 512 of 12 235; florence2 5 026 of 8 129), and
  `grant_safety`'s oracle clause joined **every** grant of every leg with
  **0 over the oracle's live free memory plus the replica's own pool**.
* **Any OOM?** None. 0 OOM negatives on all 15 small-card legs, including
  florence2 at 8 GB free (2 GB base + 382.5 MiB/item) and ViT-H (2 294 MiB
  base, shipped anchor 512) — the two worst realistic cases.
* **Any `over_grant`?** None: 0 in `ledger_invariant`'s classification on every
  leg, and 0 grants above their priced headroom.
* **Any `throughput_collapse`?** One on each `S14-tags` leg, at both levels and
  on the full card — Finding F3, a corpus artefact rather than a small-card
  effect.
* **Max batch per model:** wd-vit **132** @ 8 GB / **234** @ 12 GB;
  MobileCLIP-S1 **31** at both (its own knee, 31, travels in the shipped row);
  florence2 **12** @ 8 GB / **16** @ 12 GB; ViT-H **331** @ 8 GB.

**Seeded vs unseeded, which is the claim under test.** The shipped row is read
and logged on every load — `seeded calibration from a stored profile …
local=false fit_is_local=false exact_torch=true confirms=false
slope_mb_per_unit=29.861313868613138 samples=14 max_units_measured=205` — and
`admitted a worker … seeded_from_store=true`. What it confers:

* The **slope and the base travel** and are right: the fit in force on a
  freshly-installed 8 GB-equivalent card is the measured 29.8613 MiB/item, and
  the load reservation is the shipped `base_mb` (670 for wd-vit, 2 294 for
  ViT-H, 1 738 for florence2) instead of the 4 096 MiB placeholder an unseeded
  install uses — measured: `load_reservations_mb` peaks at **670** on
  `S2-wdvit` and at **4 096** on `S2-wdvit-unseeded` and `sc8-S2-unseeded`.
* The **conferred anchor sets the published `unit_budget`**, and it is *not*
  clamped to what the small card can run: wd-vit opens at 128 (from 205),
  ViT-H opens at **512**, florence2's published budget reaches **56** against a
  conferred 28. `/health`'s figure therefore over-states the runnable window
  by up to **4.7×** on a squeezed card (florence2: published 56, granted 12).
* The **admitted window is re-derived from `share.mb` / slope every time**, and
  that is what keeps the card safe: first grant 1 unit on every leg, then
  1 → 63 → 128 → 132 (wd-vit @ 8 GB), 1 → … → 331 (ViT-H, from a published
  512), 1 → … → 12 (florence2). No shipped anchor ever produced a window the
  card could not hold, and nothing OOMed.
* The cost is **utilisation and learning**, not safety: the unseeded control at
  the same 8 GB reaches only 32 units (its own fitted knee 7 → 31) against the
  seeded 132, at the same throughput (26.3 vs 25.6 items/s — wd-vit's curve is
  flat), and under *pressure* the conferred anchor is what freezes the ramp
  (Finding F1, and `sc8-S2-vith`'s `calibration_learned` FAIL: held at 331
  under a conferred 512 that the card can never measure).

**The 1 024 MiB reserve is a large fraction of a small card.** `S4a` scaled to
8 GB presses the card to 922 MiB free, and to 1 434 MiB at 12 GB. Both are at
or under `reserve + base`, so `limit` is 0 / 410 MiB, `headroom` is 0, and
**every** window is memory-blind at 1 unit (648 and 647 of them). That is safe
— 2 000/2 000 items, 0 OOM, 22.7–23.5 items/s against 25.6 unhogged, so the
per-request path costs ~11 % — but a real 8 GB card under that much external
pressure runs batch-1 for the whole job where ~8 units would have fitted. The
flat `DEFAULT_RESERVE_CAP_MB = 1024` is what decides this, and it is not scaled
to the board.

## 4. First-use download inside a running job

`env.C1` was pointed at an **empty** `HF_HOME` (`~/.cache/panoptikon-hf-cold`)
and each category re-run (`cold-S14-*`), so every load below is a cold
download inside a running extraction job. `load s` is the gateway's own
`spawned an inferio worker` → `worker reported its load footprint` interval.

| Model | HF bytes downloaded | cold load | warm load (phase B) | load reservation held | cooldown / timeout |
|---|---|---|---|---|---|
| `tags/wd-vit-tagger-v3` | 362 MB | **9.70 s** | 6.38 s | 670 MiB for ~9.5 s | none |
| `clip/apple_MobileCLIP-S1` | 325 MB | **11.81 s** | 8.18 s | 438 MiB for ~12.0 s | none |
| `doctr/db_resnet50_crnn_mobilenet_v3_small` | 0 MB (docTR uses `~/.cache/doctr`, 915 MB, not `HF_HOME`) | 4.97 s | 5.60 s | 432 MiB for ~5.0 s | none |
| `whisper/tiny` | 73 MB | **5.12 s** | 3.82 s | none (`unit = "none"`, unpriced) | none |
| `florence2/msft_large-caption` | 1 473 MB | **24.65 s** | 15.19 s | 1 738 MiB for ~24.5 s | none |
| `clip/ViT-H-14-quickgelu_dfn5b` | 3 766 MB | **35.47 s** | 20.63 s | 2 294 MiB for ~35.5 s | none |
| `textembed/all-MiniLM-L6-v2` (after doctr in one chain) | 87 MB | **10.48 s** | — | 432 MiB for ~15.5 s (the chain) | none |

* **No `load_secs` / handshake timeout fired** on any model, cold or warm: the
  worker handshake lands within a second of the spawn in every leg, and the
  download happens *after* it, inside `load()`.
* **The load-failure cooldown ladder never engaged** — `load_cooldowns` is
  empty in all 7 cold legs' `/health` samples. The only ladder in the whole run
  is the `dies_on_load_cuda` fixture's deliberate 2 s → 4 s.
* **The load reservation is held for the whole download.** It is the shipped
  baseline's `base_mb`, and it stays on the ledger from the spawn until the
  worker reports: 2 294 MiB for 35.5 s (ViT-H), 1 738 MiB for 24.5 s
  (florence2). Nothing else was loading in these legs, so nothing was blocked
  — but on a card with 8 GB free that is 21–29 % of the board withheld from any
  other tenant for half a minute, on the *first* use of a model, and it scales
  with the download, not with the model's admission. Worth knowing before a
  multi-model first run on a small card.
* Cold costs roughly **+3 s per GB** on this host's link (florence2 +9.5 s,
  ViT-H +14.8 s over warm), and the jobs completed normally throughout.

## 5. Findings

**F1 — a conferred anchor the card cannot reach freezes the throughput ramp,
and the hold never lifts.** `S4d` (seeded), reproduced in `S4d-repeat`:

```
18:44:53.740  INFO ledger: holding the throughput ramp at this rung: the ring
              cannot certify this rung yet  units=64 certified=false knee_binds=false
18:48:59.892 DEBUG ledger: issued a memory grant unit_budget=64 mb=3071
              share_mb=23004 headroom_mb=19922 external_mb=819
```

The hold engages 2.6 s into the job, while the hog still holds the card and the
memory clamp is running 7-unit windows; it is never followed by a *"the
throughput ramp is free to grow again"* line, so the budget is still 64 units
three minutes after the hog released, with 19 922 MiB of headroom. Grant
budgets over the job: `1×1, 7×157, 8×1, 21×1, 64×24`.

The mechanism is in the code: the hold's rung is `anchor.min(reached)` = 64,
but certification is `ring_certifies_reached(&samples, anchor)` with
`anchor = cal.max_units_measured` = **205**, the *conferred* anchor from the
shipped sm_86 row (`ledger.rs` ~5610 and ~7312). A bucket at 205 can only be
filled by running a 205-unit window, which the hold itself forbids — so
`certified` is false for ever. `analyze.py` states the same thing from the
outside: *"the throughput brake held it at 64 for 566 sample(s), a rung the
ring never certified: nothing was measured there"*.

The control decides it: `S4d-unseeded`, same hog, same corpus, nothing
conferred — the early hold **lifts** (`the throughput ramp is free to grow
again`), the ramp then grows 5 → 14 → 15 → 31 → 56 and holds at *"the rung is
the top of a measured plateau"*, `utilization` PASS 1.00 at a learned knee 31.

Impact here is utilisation and learning, not safety: 0 OOM, 0 unsafe grants,
and wd-vit's flat curve means throughput is unchanged (27.397 seeded vs 27.211
unseeded items/s). A model with a real knee above the pinned rung would pay for
it, and `calibration_learned` reports "NOTHING WAS LEARNED" on both seeded
legs. The same shape appears on the small card without any hog at all:
`sc8-S2-vith` holds at 331 under a conferred 512 for the rest of its job.

**F2 — `legs.py` records the descriptor limit from before the gateway raises
it.** `fds.jsonl` carries `"limit": 1024` on every sample of every leg, while
the gateway's own log says

```
INFO panoptikon::rlimit: raised the open file descriptor soft limit to the hard
limit soft_nofile_before=1024 soft_nofile_after=1048576
```

and `/proc/<gateway>/limits` read live during a leg says `1048576`.
`FdRecorder.__init__` calls `fd_limit(pid)` **once**, milliseconds after the
spawn and ~12 ms before `rlimit::raise_soft_limit_at_startup` lands (first
sample 18:14:19.472, raise 18:14:19.484), and reuses that value for the whole
leg. Every `peak_fds` row in this run therefore reports a percentage 1 024×
too large (e.g. "peak 94 … soft limit 1024 (9 %)" where the true figure is
0.009 %). This is exactly the class of error run1's Phase 7b hit and that the
tool's own docstring warns about; it is tooling, not product.

**F3 — the worker's throughput comparator reads item-cost heterogeneity as a
memory spill.** On the `smoke` tier, wd-vit's second window is scored a
synthetic negative on all three `S14-tags` legs (full card, 8 GB, 12 GB):

```
[WARNING] inferio_worker.packing: batch of 116 inputs (116 item units) ran at
13 units/sec against 34 for the previous growing batch of 63 units; treating it
as a memory spill (driver sysmem fallback)
```

13/34 = 0.38, just under `COLLAPSE_RATIO = 0.4` — on a card with ~18 GB free
and a 4.4 GB grant, where no spill is possible. The cause is the corpus: the
`smoke` tier mixes 8000×6000 JPEGs with small PNGs, so a larger *unit* count is
not a comparable *work* count. The ledger records `outcome="negative"
reason="throughput_collapse"` and deflates the replica once (`deflation=1`).
0 such negatives on every homogeneous-corpus leg in this run (ramp / ramp4 /
poison / text), 3 of 3 on the smoke tier. Benign here — it lands on the last
window of a 180-item job — but this comparator is, per the protocol, the *only*
over-admission instrument on WDDM, where a false positive deflates a healthy
model.

**F4 — a stale corpus produces a leg that passes every check on no data.**
`S14-textembed` ran to `outcome=drained` with `folders.json`
`total_available: 0`, `jobs.json == []`, and `failures`, `job_outcome` and
`peak_fds` all PASS. The `text` corpus on this host was generated before the
tier gained its 300 scanned pages (`Counter({'text': 2000})`, no images), which
is the T7/T5 trap the README documents — and nothing in the leg or the verdict
table says so. Re-generated with this tip's `corpus.py` (2 300 items, 300
pages) the leg is a real one: doctr 300/300 pages, `textembed/all-MiniLM-L6-v2`
300/300 rows, 0 errors (`S14-textembed-r4`, `cold-S14-textembed`). Suggested
fix, for the owner: have `legs.py` refuse an `S14-textembed` whose corpus
manifest holds no image items, or stamp a tier version in `manifest.json`.

**F5 — the tip does not build on a stock Ubuntu without OpenSSL headers.**
`cargo build --release -p panoptikon` fails in `openssl-sys v0.9.111`
("Could not find directory of OpenSSL installation"), reached through
`reqwest → hyper-tls → native-tls`. The host has `libssl.so.3` but no
`libssl-dev` and no root. Worked around with the tree the §4.13 pass left in
`~/.cache/ossl` (`OPENSSL_INCLUDE_DIR` / `OPENSSL_LIB_DIR`), after which the
build takes 1 m 31 s. `native-tls` is already in `Cargo.lock` at `5c778e48`, so
this is not new at this tip — but it is a first-build blocker on any Linux box
without the dev package, and nothing in the build output points at the fix
beyond openssl-sys's own message.

### Known residues, confirmed not new

* **`oracle_agreement` FAILs under a hog that never stands still** — §4.23's
  open residue. Here: S4a 2 of 246 joined samples, S4b 18 of 690, S4c 20 of
  674, S4d 1 of 670, `sc8-S4a` 7 of 266, `sc12-S4a` 6 of 257. Every breach is a
  transient at a hog step; the idle-card legs (S2, S3) are PASS with a worst
  disagreement of 55 MiB.
* **`ledger_invariant` WARN with `limit_fell` only** — 0 `over_grant` on every
  leg of the run, including the 648-grant memory-blind small-card legs.
* **`utilization` judged against the full-card boundary** on any hogged leg —
  the README's own instruction is to judge at the hog's free level; §2 and §3
  give both numbers.
* **`base_accuracy`** is INFO ("not judged") on the idle legs because the first
  predict starts ~27 ms after the load, and FAILs on two hogged legs
  (`sc8-S2` 98 %, `sc12-S4a` 87 %) where the only sample inside the window is
  8 ms old and reads 338–358 MiB against a base of 670 — a per-PID NVML lag at
  admission, not a footprint disagreement; `footprint_agreement` is INFO by
  design.
* **D2 (pool pinning) does not reproduce**: 0 memory-blind grants on S4a
  against §4.13's 2 613 of 2 615, and `charges = footprints` never pins.

### Not covered

`calibfixture/oom_timed_cuda`'s *recovery* half: the fixture is healthy after
120 s, but a job of 2 000 or 8 000 items fails every item well inside that
window (`S5-oomtimed`, `S5-oomtimed-8k`), so deflation never gets a healthy
window to repay against on the job path. It needs `loadgen.py` pacing, not a
job. `calibfixture/hang_trim_cuda` (B17) needs a squeezed neighbour, which is
an S6-shaped leg and outside the platform-pass set.

## 6. Layout

```
run4/
  report.md                     this file
  platform-selftest.json        selftest.py --induce-oom
  probe-wd.json bisect-wd.json  ceiling_probe.py ground truth (wd-vit)
  <leg>/<scenario>/             legs.json, panoptikon.log.gz, vramrec.jsonl.gz,
                                healthrec.jsonl.gz, fds.jsonl, hog.jsonl.gz,
                                jobs/failures/metadata/health snapshots,
                                calibration.after.toml, analyze.txt, verdicts.json
  <leg>.log                     the driver's own output for that leg
```

The four recordings are gzipped (`gunzip` them before re-running
`analyze.py --scenario <leg>/<scenario>`, which reads the plain names);
`gateway.out`, a byte-duplicate of `panoptikon.log`, and the per-leg `root/`
(databases and thumbnails, 1.1 GB) were not kept.

Legs: `S1`, `S2-wdvit`, `S2-wdvit-unseeded`, `S3-wdvit`, `S4a`, `S4b`, `S4c`,
`S4d`, `S4d-repeat`, `S4d-unseeded`, `S5-{oom2,oom1,failbatch,oomtext,oomtimed,
oomtimed-long,oomtimed-8k,dying,diesonload,oomimpl}`, `S14-{tags,clip,ocr,
whisper,florence2,textembed,textembed-r4}`, `sc8-*`, `sc12-*`, `cold-S14-*`.
