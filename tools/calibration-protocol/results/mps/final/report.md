# final — the MPS regression pass on `2133563f`, at the default wired limit

2026-09-17 00:54–02:38 UTC, over SSH, on the MacBook Pro M3 Max 128 GB
(`moot@10.11.11.3`). Tip under test **`2133563f`** ("Design doc: what the
reload does, and what clears a condemnation"), the final tip of
`claude/batch-calibration-coverage-db9ab9` with every fix of the branch merged.

Comparands: run4's fourteen-leg set on **`0d7f5671`**
(`results/mps/run4/report.md`); **run4-holdfix**, the `r4fix-*` five CLIP ramps
and five MiniLM repeats on the same tip plus `fix/early-hold`; and
**run5-mixed-fix**. All three were re-analysed here with **one** `analyze.py`
— the one at `2133563f` — so every verdict below is like-for-like. The
published run4 verdicts came from an older analyzer and are not quoted as the
comparand.

## Platform

| | run4 (`0d7f5671`) | final (`2133563f`) |
|---|---|---|
| `iogpu.wired_limit_mb` | 0 | **0 — the driver default** |
| `recommended_max_memory()` adopted | 110 100 | **110 100** |
| `hw.memsize_mb` | 131 072 | 131 072 |
| `--gpu-total-mb` seeded into `vramrec.py` | 110 100 | 110 100 |
| torch / macOS | 2.7.1 / 26.6.2 (25G83) | 2.7.1 / 26.6.2 (25G83) |
| `PYTORCH_MPS_*_WATERMARK_RATIO` | unset | unset (torch default) |
| `ulimit -n` / `-Hn` in the leg shell | 256 / unlimited | 256 / unlimited |
| cargo | 1.97.1 | 1.97.1 |

`sysctl` needs sudo and was not available, so the limit stayed at 0 throughout:
**every number here is at wired limit 0**, as in run4. The machine was idle,
each leg ran alone, one gateway at a time. `caffeinate -is` of this pass's own
held it awake and was killed at the end. `vm.swapusage` was **0.00 M before,
during and after every leg**.

### Instruments

`cargo build --release -p panoptikon` exit 0 in **49.3 s**.
`uv sync --locked --extra cpu --group test` exit 0.
`oracle_calibrate.py` is not run on MPS (no per-process GPU counter).

`python -m pytest tools/calibration-protocol/tests -q` → **205 passed, 0
failed**, against run4's **1 failed, 166 passed**. Run4's F-run4-1
(`test_no_reading_at_all_leaves_the_total_null_never_zero` could not pass on a
Mac, because `MpsOracle` used `None` both for "no reading" and for "read the
host") is fixed by `1a7b872f`, which gives it a distinct `READ_HOST` sentinel.
**First moved check of this pass.**

`selftest.py`: **`VERDICT: no degraded tiers`**. `free_source` `mps`
107 456 / 110 100 MiB; `base_method` **`mps` 1 384 MiB** on wd-vit with the
`alloc_delta` tier under it answering **860** (−38 %, the §4.16 figure to the
point); `gpu_total_mb` 110 100; fp32, load 2.5 s; `empty_cache` true, 1 848 →
1 840 MiB reserved and 107 456 MiB free after teardown. Both watermark ratios
unset.

`selftest.py --induce-oom --mps-watermark 0.05 --oom-cap-mb 8192`: after 3 072
MiB of filler, `RuntimeError: MPS backend out of memory (MPS allocated: 4.35
GB, other allocations: 456.73 MB, max allowed: 5.38 GB)`, and `classify_oom`
returned `source: "message_pattern"`, `free_mb_at_failure: 592`,
`device: "mps"`. The classifier tier is intact.

### Two deviations from run4's inputs, both forced

1. **The corpora had to be regenerated.** `00f22c55` added
   `CORPUS_GENERATOR = 2` and legs.py now refuses a corpus stamped below it;
   run4's were unstamped. Regenerated at the same seed 20260903:
   `ramp` and `ramp4` came back **byte-identical to run4's** (2 000 /
   96 762 319 B and 8 000 / 387 766 010 B, every item id and size equal), so
   every ramp leg is exactly like-for-like. `smoke` is **200 of 205 items
   byte-identical**; the 5 PDFs changed, because `d97094ad` made PDF pages
   carry printed words (20 186 346 → 23 268 745 B). `text` gained 300 scanned
   pages beside its 2 000 `.txt` files, which are byte-identical; the MiniLM
   leg is pinned to `kind=text`, so it drives run4's exact 2 000 files.
2. **Two tooling defects had to be fixed to run the set at all** — F-final-1
   and F-final-2 below. Neither touches the binary; the product commit under
   test is `2133563f` unmodified.

## 1. Verdict table — this tip against `0d7f5671`

Only checks that are **not** PASS on at least one tip. Full tables:
`analysis/analyze-2133563f.txt`, `analysis/analyze-0d7f5671.txt`,
`analysis/analyze-holdfix-0d7f5671.txt`.

| leg | `0d7f5671` non-PASS | `2133563f` non-PASS | same? | items/s old→new | `grant_safety` over_oracle/grants |
|---|---|---|---|---|---|
| S1 | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | – | 5/6 → 5/6 |
| S2-wdvit | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | 28.169 → **28.169** | 3/149 → 4/149 |
| S2-clip (R1 *a*) | grant_safety FAIL | grant_safety FAIL | yes | 105.263 → **114.286** (1.086×) | 1/70 → 3/76 |
| S2-textembed | calibration_learned FAIL | calibration_learned FAIL, **grant_safety FAIL** | **NO (worse)** | 224.7 → **260.4** † | 0/318 → **2/187** |
| S3-clip | grant_safety FAIL | grant_safety FAIL | yes | 90.909 → **95.238** (1.048×) | 3/33 → 3/33 |
| S3-wdvit | grant_safety FAIL, ledger_invariant WARN | grant_safety FAIL, ledger_invariant WARN | yes | 28.169 → **28.169** | 4/303 → 3/312 |
| S4a-mps | ledger_invariant WARN | ledger_invariant WARN | yes | 25.723 → **28.674** (1.115×) ‡ | 0/411 → 0/643 |
| S4a-ram | ledger_invariant WARN | ledger_invariant WARN | yes | 28.674 → **29.197** (1.018×) | 0/643 → 0/643 |
| S4d-mps | ledger_invariant WARN | ledger_invariant WARN | yes | 28.777 → **29.197** (1.015×) | 0/643 → 0/643 |
| S14-tags | grant_safety FAIL | grant_safety FAIL | yes | – | 5/6 → 4/6 |
| S14-clip | grant_safety FAIL | grant_safety FAIL | yes | – | 3/6 → 3/6 |
| S14-whisper | grant_safety SKIP | grant_safety SKIP | yes | – | no grant lines (CPU/CTranslate2) |
| S14-ocr | grant_safety FAIL | grant_safety FAIL | yes | – | 3/17 → 3/17 |
| S14-chain | grant_safety FAIL | grant_safety FAIL | yes | – | 3/19 → 3/17 |

† the loadgen figure, not the analyzer's; see §3. ‡ this recovers run4's
F-run4-3 in full: 25.723 → 28.674 is above run4's own `27ff0790` comparand of
28.369.

**Thirteen of fourteen legs reproduce their `0d7f5671` verdict set check for
check.** One moved, and it moved the wrong way: `grant_safety` PASS → FAIL on
S2-textembed run *a* (finding F-final-5). **Nothing else moved in either
direction**, and every leg is equal or faster: the eleven legs with a
throughput figure are 1.000–1.115× (median 1.029), none slower.

Invariants that held on both tips:

* **`over_headroom` — a grant beyond the headroom it was priced against — is
  0 on every grant on both tips**: 4 501 grants here, 3 300 (run4) + 1 382
  (holdfix) there. The clause the ledger enforces never moved.
* `limit = min(recommended_max, memsize − external − reserve)` on **8 170 of
  8 170** health samples here and **5 899 of 5 899** there — **100 % both**,
  with totals 98 304 / 110 100 / **131 072** (the CPU device row, which every
  host now carries, `a781a9d4`) and 98 304 / 110 100.
  `analysis/limit-formula-*.json`.
* `ledger_invariant`: `over_grant` — the FAIL class — is **0 everywhere on
  both tips**. `limit_fell`, the documented WARN class, is **11 of 14 202**
  GPU-samples here against 11 of 5 660 there: same absolute count over 2.5×
  the samples.
* **0 worker deaths, 0 OOM negatives, 0 throughput-collapse negatives, 0
  failed items, 0 failed jobs** across all 24 legs.
* S3 resumes on the persisted knee, not the seed: `seeded_from_store=true` on
  the post-restart load, CLIP **31** and wd-vit **3**.
* The stored knees are unchanged: `calibration.after.toml` carries wd-vit
  **3** and CLIP **31** on both tips, leg for leg.
* `hog_tracking`: `external_mb` moved with every hold — S4a-mps 0..89 088 MiB
  over 636 joined samples, S4d 0..89 600 over 624, S4b 0..34 560 over 679.
* `persistence` PASS on every leg that has it.
* Descriptors: peak 43–83 of an unlimited hard limit; `peak_fds` INFO
  throughout.

Florence2 was excluded from every leg but the mixed one, where it is pinned to
the CPU (unbounded MPS growth per batch).

### Legs with no run4 comparand

* **S4b** (step up at t+60 s) ran for the first time on this platform — it was
  unrunnable before F-final-2. 8 000 items drained, hog raised to 34 553 MiB
  at t+60 s and acked in 12 ms, `grant_safety` FAIL (4/396, the 98 304 seed),
  `ledger_invariant` WARN, everything else PASS. 26.403 items/s under
  pressure.
* **S14-textembed** on the `text` tier: `job_outcome` PASS, 1 job completed,
  0 failed items — the derived-setter chain works when the OCR has pages with
  words on them (F-final-3).
* **S4c** stays excluded: its "~2 GB free" squeeze means leaving *the laptop*
  with 2 GB, which is the memory-pressure experiment this pass was told not to
  run. **S5 proper** stays excluded: its fixtures are the CUDA-touching
  `calibfixture/*_cuda` impls, which have no MPS variant. The MPS-applicable
  fixture leg is A1 (§5).

## 2. R1 — five consecutive cold CLIP ramps

Fresh store each, corpus `ramp4`, 8 000 items, `clip/apple_MobileCLIP-S1`.
Ramp *a* is the one inside the fourteen-leg sequence; *b–e* ran back to back.

**`2133563f` — 5 of 5 pass. No ramp ran away.**

| run | knee first | knee last | held rung | `ramp_held` samples | `held_certified` | peak `unit_budget` | max units measured | peak pool (MB) | items/s |
|---|---|---|---|---|---|---|---|---|---|
| fin-2long (a) | 31 | 63 | **128** | 120 | **120 of 120** | 192 | 64 | **3 271** | 114.286 |
| fin-2long-b | 31 | 63 | **128** | 119 | **119 of 119** | 192 | 64 | **3 271** | 117.647 |
| fin-2long-c | 31 | 63 | **128** | 119 | **119 of 119** | 64 | 64 | **3 271** | 115.942 |
| fin-2long-d | 31 | 63 | **128** | 120 | **120 of 120** | 192 | 64 | **3 271** | 115.942 |
| fin-2long-e | 31 | 31 | **128** | 121 | **121 of 121** | 127 | 127 | **4 344** | 115.942 |

run4-holdfix (`r4fix-2long-*`), the matched five: held rung 128, 120–121
`ramp_held` samples, all certified, peak budget 64–192, peak pool 3 271 MB.
**Identical behaviour.**

**Pass criterion met.** Every one of the five fitted a knee at 31 on its first
fit, every one declared the hold at rung **128**, and **every hold was
certified on every sample it was published in** — `held_certified` equals
`ramp_held` exactly, 5 times out of 5. Peak pool 3 271–4 344 MB against the
original defect's 65 893 MB, a **15–20× reduction**, and no run visited a rung
above 192. Throughput averages **115.95** items/s here against run4's 111.5
and run4-holdfix's own five — **1.04×**, and ramp *a* is no longer the slow
slot (114.286 against run4's 105.263).

## 3. MiniLM loadgen, five repeats

`textembed/all-MiniLM-L6-v2`, 4 threads × 64 items, 180 s, `kind=text` over
run4's exact 2 000 text files.

| run | hold log lines | lift log lines | worker samples | `ramp_held` samples | final `unit_budget` | items/s | requests | `window_requests` histogram |
|---|---|---|---|---|---|---|---|---|
| fin-2t-loadgen (a) | **0** | 0 | 427 | **0** | 88 576 | 260.4 | 737 | 4×182, 3×2, 1×3 |
| fin-2t-loadgen-b | **0** | 0 | 425 | **0** | 79 872 | 245.7 | 694 | 2×345, 4×1 |
| fin-2t-loadgen-c | **0** | 0 | 426 | **0** | 90 112 | 269.8 | 760 | 4×187, 3×3, 1×3 |
| fin-2t-loadgen-d | **0** | 0 | 427 | **0** | 89 088 | 251.3 | 711 | 2×217, 4×67, 3×2, 1×3 |
| fin-2t-loadgen-e | **0** | 0 | 426 | **0** | 92 160 | 287.9 | 812 | 4×203 |

**Mean 263.0 items/s ≥ 256.** 0 failed requests on all five.

**There is no hold to answer.** The criterion "each hold line followed by a
lift or certified within 10 samples" is satisfied vacuously: `39f7bb2c` makes
a replica whose last two clean windows were sized by the queue report
`ramp_held = false`, and on this tip the leg logs **no hold line at all**.
The comparands each logged exactly one hold per run — run4's three
(held 421 / 13 / 4 samples) and run4-holdfix's five (5 / 7 / 1 / 30 / 10) —
and run4's run *a* is the one that never lifted, pinning the published budget
at 79 360 for 421 of 427 worker-samples. **Run4's F-run4-2 is closed:** no run
here is pinned, and the five final budgets are 79 872–92 160 against run4 *a*'s
frozen 79 360.

Throughput against the comparands: run4's three averaged 256.8 (224.7 / 264.4
/ 281.2) and run4-holdfix's five averaged **282.6** (268.3 / 282.2 / 286.3 /
289.2 / 286.7). This pass's five average **263.0**, i.e. **1.024×** run4 but
**0.931×** run4-holdfix. The gap is entirely the merge width, and the
histogram says so: run4-holdfix merged all four concurrent requests into one
window on every run, while two of five here (b: 345 of 346 windows at 2
requests; d: 217 of 289) merged only two — and b and d are exactly the two
slow runs. The three runs that merged 4-wide average **272.7**, 0.965× the
comparand. n = 5 per side, ranges overlapping; this is a merge-width
observation, not a demonstrated throughput regression, and it is the thing to
watch next.

## 4. The mixed leg — CLIP on Metal beside Florence2 pinned to the CPU

`.mac/run5/server-C8-mac.toml`, whose registry pins
`florence2/msft_large-caption` to `devices = ["cpu"]`. 150 images each,
side by side, 0 failed requests; then unload → load → 2-item predict.

Two device rows, and **each device's headroom is its own limit less *both*
replicas**:

| snapshot | device | total | external | limit | charges | headroom | limit − both charges |
|---|---|---|---|---|---|---|---|
| both loaded | CPU | 131 072 | 14 697 ram | 98 304 | 13 537 | **83 606** | 98 304 − 13 537 − 1 161 = **83 606** ✓ |
| both loaded | GPU-MPS | 110 100 | 16 069 mps | 110 100 | 1 161 | **95 402** | 110 100 − 1 161 − 13 537 = **95 402** ✓ |
| after reload | CPU | 131 072 | 21 780 ram | 98 304 | 3 232 | **93 911** | 98 304 − 3 232 − 1 161 = **93 911** ✓ |
| after reload | GPU-MPS | 110 100 | 26 374 mps | 103 674 | 1 161 | **99 281** | 103 674 − 1 161 − 3 232 = **99 281** ✓ |
| final | CPU | 131 072 | 22 201 ram | 98 304 | 4 479 | **92 664** | 98 304 − 4 479 − 1 161 = **92 664** ✓ |
| final | GPU-MPS | 110 100 | 25 127 mps | 104 921 | 1 161 | **99 281** | 104 921 − 1 161 − 4 479 = **99 281** ✓ |

**Exact on all six rows.** Each `external_mb` carries only its own domain —
the Metal row's 16 069 does not include the CPU replica's RSS, which is the
run5-mixed defect `9f2f628a` fixed. Both rows carry the `device_kind` field
`752447f0` added (`cpu` / `mps`), which is what makes the two readable apart.

**The CPU pin holds across the reload.** Load 1 and load 2 both land on the
CPU device:

```
02:26:59  florence2/msft_large-caption base_mb=3232 base_method="rss"
          gpu_name="CPU (128 GB)" gpu_arch="cpu"  -> gpu=CPU
02:35:55  florence2/msft_large-caption base_mb=3232 base_method="rss"
          gpu_name="CPU (128 GB)" gpu_arch="cpu"  -> gpu=CPU  seeded_from_store=true
```

`claimed a prewarmed worker` appears **0 times** in the whole leg; the only
parked worker is CLIP's (`impl_class=openclip`), so the cpu-pinned replica is
neither claim-eligible nor does it lazily warm the pool. CLIP stayed on Metal
throughout (`base_method="mps"`, `gpu_arch="apple-m3"`, `gpu=GPU-MPS`).

**No OOM, no failed load, no failed request. `vm.swapusage` 0.00 M and
`memory_pressure` "System-wide memory free percentage: 97%" before the leg,
after the 300 items, and after the reload.**

## 5. A1 — a host-RAM out-of-memory on a CPU-priced Mac

`calibfixture/cpu_alloc_oom` (raises torch's `DefaultCPUAllocator: can't
allocate memory` and allocates nothing), driven by `loadgen` at 8 items × 6
requests in a priced window, against two configs that differ only in
`[inference_local.python_env] accelerator`.

| | `accelerator = "cpu"` | `accelerator = "auto"` (negative control) |
|---|---|---|
| device the replica was admitted to | **CPU** | **GPU-MPS** |
| grant `mb` | **98 304** (the CPU limit) | **109 231** (Metal headroom) |
| `free_mb_at_failure` the worker reported | **110 253** | **110 100** |
| what that figure is | host RAM available (`vm_stat` free+inactive was 112 119 MiB at t0 and falling) | `recommended_max_memory()` **exactly** |
| `oom_verdict` | **Contradicted** (free ≥ grant) | **Contradicted** (free ≥ grant) |
| tier that did deflate | `marker` (`INFERENCE_OOM_WINDOW`), trusted outright | `marker`, trusted outright |
| `unit_budget` over the six grants | **8 → 4 → 2 → 1 → 1 → 1** | **8 → 4 → 2 → 1 → 1 → 1** |
| `deflation` counter | 0 → 1 → 2 → 3 → 4 → 4 | 0 → 1 → 2 → 3 → 4 → 4 |
| swap during | 0.00 M | 0.00 M |

**The thing the leg exists to prove is proved: the free figure follows the
currency.** Under `accelerator = "cpu"` the worker weighed the failure against
**110 253 MiB of host RAM** — a live reading that tracks `vm_stat` — and under
`auto` against **110 100 MiB**, which is `recommended_max_memory()` to the MiB
with an empty pool. `986865f8` does what it says. The budget halves on every
grant, 8 → 1, on both legs.

Two of the README's stated pass criteria are **not** met, and both are
defects in the criteria rather than in the code — see F-final-4.

## 6. Findings

### F-final-1 — `corpus.py` cannot generate any PDF (tooling, new on this branch)

Every PDF in the `smoke` and `pdf` tiers failed with
`AttributeError: 'tuple' object has no attribute 'convert'`, so `smoke` came
back with 200 of its 205 items and no PDFs at all. `d97094ad` made
`_base_image` return `(image, page-meta)`; `gen_image` was updated to unpack
it and `gen_pdf` was not, and it takes `.convert("RGB")` on the tuple.
Introduced 2026-09-06, never caught because no pass had regenerated a corpus
since.

**Fixed here**, one line plus a comment, in `tools/calibration-protocol/corpus.py`:

```python
    images = [
        _base_image(width, height, _rng(spec["seed"], spec["index"] * 977 + page),
                    False, True)[0].convert("RGB")
        for page in range(pages)
    ]
```

`smoke` then regenerated at 205 items, 5 PDFs, 0 errors.

### F-final-2 — S4b, S4c, S4d and S4e were unrunnable on every platform

S4d exited 1 in 26 s:

```
legs.py: corpus .../results/corpus/ramp4 is the 'ramp' tier, this leg needs
'ramp8' - generate it with `corpus.py --tier ramp8 --out ... --force`
```

The advice cannot be followed: `ramp8` is not a `corpus.py` tier and never
was (`TIERS` is smoke, ramp, text, pixmix, ocr, audio, pdf, soak, poison). It
is a *directory* convention for the `ramp` tier at `--scale 8`, which is how
`results/corpus/ramp8` was generated and what the README calls it. `00f22c55`
added a tier check that compares the scenario's whole corpus name against the
manifest's `tier`, so the four scenarios that name `ramp8` refuse every corpus
in existence and print a command that exits 2. Nothing on any platform could
have run S4b/S4c/S4d/S4e since 2026-09-16; run4 ran S4d because the check did
not exist yet.

**Fixed here** in `tools/calibration-protocol/legs.py`: a scenario's corpus
name is split into a tier and a scale (no tier name ends in a digit, so a
trailing integer is always the scale), the tier is what the manifest is
checked against, the regenerate hint names a real command, and a corpus of the
right tier at a *smaller* scale runs with an announced
`PRECONDITION:` line and a `corpus_scale_short` event in `legs.json` — the
same treatment the hog floor gets, because a short job is a weaker measurement
and not an invalid one. `pytest tools/calibration-protocol/tests -q` is 205
passed with the change. S4d then re-ran clean and S4b ran for the first time,
both on `ramp4`, both recording the shortfall:

```
PRECONDITION: this leg is defined on the ramp tier at --scale 8 and
.../corpus/ramp4 is --scale 4, so its job is shorter than the scenario's
profile window
```

### F-final-3 — S14-chain's textembed sub-job was always a no-op, and this tip is the first to say so

S14-chain exits **1** on this tip. The leg itself is fine — 185 files indexed, 195 OCR
items, 5 whisper items, every smoke assertion 200 — but its third model finds
nothing:

```
job_no_items {"model": "textembed/all-MiniLM-L6-v2", "record": "none",
 "hint": "the model found nothing to run on - a derived text setter needs
 extracted_text rows another setter wrote first, ..."}
```

`smoke`'s 180 images are gradients, so `doctr` reads no words off them and
writes no `extracted_text` rows; `smoke`'s 15 text items are `.txt`, which no
file scan indexes. The sub-job has been draining on zero items on every pass
that ever ran it, including run4, where the analyzer reported `job_outcome
PASS`. `0a337893` ("an empty jobs.json is a queued-nothing failure, not a
skip") is what turned the silence into an exit code. **Not a regression — a
four-month-old blind spot the branch closed.**

Confirmed by running the leg that was designed for it: **S14-textembed on the
`text` tier** (300 scanned pages with real words) is `job_outcome` PASS, 1 job
completed, 0 failed items. The chain works; `smoke` was the wrong corpus.

### F-final-4 — two of the A1 leg's stated pass criteria cannot be met by its own fixture

`tools/calibration-protocol/README.md` asks A1 for `oom_verdict` =
`Trusted(Corroborated)` and says of the negative control that the Metal
headroom is a figure "which no host-RAM figure can be mistaken for on a 128
GiB machine". Both are wrong, and the leg measured why.

* **`Corroborated` is unreachable.** `oom_verdict` returns
  `Trusted(Corroborated)` only when a `message_pattern` classification carries
  `free_mb_at_failure < grant.mb`. The fixture allocates **nothing**, so at the
  instant it raises there is 110 253 MiB free against a 98 304 MiB grant, the
  veto fires, and the verdict is **`Contradicted`** — which is the ledger
  behaving exactly as designed (`ledger.rs`: "A batch this size was not what
  the GPU ran out of"). A corroborated verdict needs a fixture that actually
  consumes the memory it claims to want.
* **The negative control barely discriminates on this host.** 110 253 MiB of
  available RAM against 110 100 MiB of Metal headroom is a **153 MiB, 0.14 %**
  separation, because `recommended_max` is 0.84 × RAM and an idle 128 GiB Mac
  has about 0.84 of its RAM available. The two are distinguishable only
  because the Metal figure is the `gpu_total_mb` constant to the MiB while the
  RAM figure drifts with `vm_stat`. The control is sound in kind and weak in
  margin, and the README overstates it.

The criterion that does discriminate cleanly, and that this pass records
instead, is the **device and the grant**: CPU / 98 304 against GPU-MPS /
109 231. No product change is proposed; the README's A1 paragraph should be
rewritten around what is observable.

### F-final-5 — the first two pre-fit windows of a cold text replica price above live free

The one check that moved the wrong way. S2-textembed run *a*, `grant_safety`
PASS → FAIL, 2 of 187 grants:

| iso | grant `mb` | `oracle_free_mb` | own pool | over by |
|---|---|---|---|---|
| 01:06:27.462 | 108 410 | 106 978 | 0 | **1 432 MiB** |
| 01:06:28.320 | 106 969 | 104 763 | 2 018 | **188 MiB** |

Both are in the leg's first two seconds, before any fit exists, and both are
priced against a *live* MPS reading — unlike every other `grant_safety` FAIL
in this pass, whose `oracle_free_mb` is **98 304 exactly**, the known seed
residue. `over_headroom` is 0 for both: the ledger's own arithmetic is intact,
and no allocation of that size is ever made — the envelope is a reservation.

It is visible now **because** F-run4-2 was fixed: run4's run *a* was pinned at
79 360 units by the spurious hold, so its windows were small and it scored
0/318. With the brake gone the ramp reaches 100 k-token windows immediately,
and MiniLM's 120 000-token seed prices the first one at essentially the whole
device. The four repeats are 0/346, 0/193, 0/289, 0/203 — all PASS — so n = 1
for the condition, and `fde87cad` ("a bounded pre-fit one-item price") is the
rule this sits closest to. Watch it; do not yet call it a defect.

### Known residues that reproduced unchanged

* **`grant_safety` FAIL on 16 legs; on 15 of them the oracle clause alone,
  with `oracle_free_mb` = 98 304 exactly** — `vramrec.py`'s 0.75-of-`hw.memsize` seed pricing the
  grants issued before the gateway's `/health` publishes the adopted total
  (§4.16 T2 / §4.20). Counts are in run4's range leg for leg (S2-wdvit
  3/149 → 4/149, S14-ocr 3/17 → 3/17, S3-clip 3/33 → 3/33).
* **`ledger_invariant` WARN, `limit_fell` only** — 11 samples of 14 202, 0
  `over_grant`. WARN by construction on a nearly-full unified device.
* **`calibration_learned` FAIL on all five MiniLM repeats** — the seed
  (120 000 tokens) *is* MiniLM's peak, so "peak never left the seed" fires on
  a model with nothing to learn. Pre-existing, both tips.
* **`oracle_agreement`, `base_accuracy`, `footprint_agreement` SKIP** on every
  leg, both tips — no per-process GPU counter on MPS, by design.
* **`slope_accuracy`, `utilization`, `alloc_retries` SKIP** — no `--probe`
  files were passed on either pass.
* **`job_outcome` SKIP** on the five loadgen legs — `minilm.sh` runs no job
  queue, by design.
* **S14-whisper has no grant lines** — CTranslate2 takes CPU on macOS, so
  whisper has no MPS replica (§4.16); `grant_safety` SKIP on both tips.
* **Hog decay (F1)** — on the MPS legs `external_mb` still runs far above what
  the hog holds (S4a-mps: hog 0..89 088 MiB, `external_mb` 110 174..114 067
  over 636 joined samples); `--target ram` remains the better pressure source
  on this platform.

## 7. What is in this directory

```
report.md                       this file
legs/fin-*/                     24 leg recordings (14 regression + S4b +
                                S14-textembed + 4 extra CLIP ramps + 4 extra
                                MiniLM repeats). `root/` is not kept; files
                                over 100 KB are gzipped. verdicts.json beside
                                each is analyze.py's output.
mixed/                          the CLIP-on-Metal + Florence2-on-CPU leg
a1-cpu/, a1-auto/               the A1 fixture leg and its negative control
instruments/                    selftest JSON+log (ambient and induced-OOM),
                                platform.txt, pytest.log, build.log, every
                                driver script and extractor, the three server
                                configs and the C8 registry, and the per-leg
                                driver logs
analysis/analyze-*.txt          analyze.py over all three tips, one analyzer
analysis/r1-*.json              the R1 ramp extraction, both tips
analysis/minilm-*.json          the MiniLM hold/lift/histogram extraction
analysis/limit-formula-*.json   the limit identity over every health sample
analysis/manifest-old-*.json.gz run4's corpus manifests, for the byte-compare
```

The Mac's own copies stay under `~/projects/panoptikon-pr27/.mac/final/` with
the leg trees (including `root/`) under
`~/projects/panoptikon-pr27/tools/calibration-protocol/results/fin-*`.

## 8. Wall clock

| task | span (UTC) | elapsed |
|---|---|---|
| push, checkout, `cargo build --release`, `uv sync` | 00:54:05–00:54:54 | **49 s** |
| instruments: platform, pytest, selftest ×2 | 00:55:54–00:56:08 | **14 s** |
| corpora regenerated (ramp, ramp4, text, smoke ×2) | 00:58:04–00:59:5x | **~2 min** |
| the fourteen legs | 00:59:59–01:37:57 | **37 min 58 s** |
| S4b (extra) | 01:37:57–01:44:31 | **6 min 34 s** |
| S4d re-run after F-final-2 | 01:45:06–01:50:49 | **5 min 43 s** |
| R1 ramps b–e (ramp *a* is inside the fourteen) | 01:50:49–02:01:38 | **10 min 49 s** |
| MiniLM repeats b–e (*a* is inside the fourteen) | 02:01:38–02:18:37 | **16 min 59 s** |
| S14-textembed (extra) | 02:18:37–02:26:20 | **7 min 43 s** |
| the mixed leg | 02:26:50–02:36:23 | **9 min 33 s** |
| A1 `accelerator = cpu` | 02:36:23–02:37:09 | **46 s** |
| A1 `accelerator = auto` | 02:37:09–02:37:55 | **46 s** |
| analyze.py over three tips + five extractors | 02:37:55–02:38:22 | **27 s** |
| **total** | **00:54:05–02:38:25** | **1 h 44 min 20 s** |

Run4's fourteen legs took 44 min 52 s against this pass's 37 min 58 s, the
same set on the same machine: **0.846×**.
