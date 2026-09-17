# final-p1 — the P1 refusal-room fix, verified on the 3090

Verification of `fix/refusal-room` (`2e2eb34e` "Refuse a load against the room
the card has, not the reserve" + `51a9f64d` "Gate the cgroup readers to Linux
so Windows builds them clean", on `92d091b5`) against **P1** of
`../final/report.md` §2 / §4.

RTX 3090 (24 576 MiB, sm_86), `systemvi`, the same host, driver and venv as
`../final`. Built on the host as branch `p1` at **`d3da925d`** — the two fix
commits plus a **local cherry-pick of `60e0a4e1`** (the `legs.py` tier/scale
fix), which is on the branch tip but not in `fix/refusal-room`'s ancestry;
without it the tooling is the pre-fix `legs.py` the final pass had to shim
with `runleg.py`. The cherry-pick is behaviour-neutral for every leg here
(`S2`, `S4a` and `S14` all declare scale-1 tiers, and none passes `--corpus`),
and it exists only on the host clone.

Build with the `~/.cache/ossl` workaround (run4 §1): **1 m 26 s**,
`target/release/panoptikon`, **84 064 160 B** (final: 84 065 560 B).
Config C1 with the host's own path overrides, unchanged. Corpora as the final
pass regenerated them. Pass **05:06:37 -> 05:21:19 local, 14 m 42 s**; the GPU
was left at **1 MiB**.

Scope: the **sc8 set only** — the legs the fix can move. The rest of the
regression is `../final`'s and is not re-run here.

## 1. The P1 leg

The fix is confirmed. On the leg that failed the final pass:

* **0 refusals** and **0 load cooldowns** in either `sc8-S4a` log
  (final: 3 of `model tags/wd-vit-tagger-v3 needs about 670 MiB on GPU ...,
  which has room for 0 MiB; not loading it`).
* **648 grants, all 648 memory-blind** (`mb=0`, `unit_budget=1`), with
  `reserve_mb=1024 reserve_rule="capped_default"` still on every grant line —
  the reserve governs the batch budget, exactly as it did; it no longer
  governs the load. `external_mb` 23 600 -> **976 MiB free** at the first
  grant.
* **2 000/2 000 items, 0 errors, job `completed`**, at **23.256** items/s
  (repeat: 23.810; run4: 22.727 on the same 648 grants).
* **0 OOM negatives, 0 `over_grant`, 0 worker deaths.**

## 2. Side by side, leg by leg

`PASS*` = passes with an adjudicated check, named in the last column.

| Leg | p1 (`d3da925d`) | final (`2133563f`) | run4 (`0d7f5671`) | The number that moved |
|---|---|---|---|---|
| `sc8-S2` | **PASS\*** | PASS\* | PASS\* | **run4 to the digit**: 7 grants, max **132** units (final 145), 25.641 items/s (final 25.974), `utilization` FAIL 0.21 (final 0.23). `ledger_invariant` **WARN -> PASS**, 0 `limit_fell` (final 101; run4 0). New: `base_accuracy` FAIL, see §3 |
| `sc8-S2-unseeded` | **PASS** | PASS | PASS | also run4's shape: **69** grants (final 128), max **32** units (64), knee **31** (15), `utilization` 1.00 at the knee, 26.667 items/s (26.316) |
| `sc8-S2-vith` | **PASS\*** | PASS\* | PASS\* | max granted **348** units, 4 grants, **0 OOM**, 33.898 items/s (final 35.088, run4 32.258). `calibration_learned` FAIL under a conferred 512 — unchanged. `ledger_invariant` WARN 63 `limit_fell` (60), 0 `over_grant` |
| `sc8-S14-flor` | **PASS** | PASS | PASS | max **11 units / 5 023 MiB** (final 12 / 5 026), 6 grants, 0 OOM/blind, 180/180 |
| `sc8-S4a` | **PASS\*** | **FAIL (P1)** | PASS\* | **the load is admitted**: 648 grants, 648 memory-blind, 0 OOM/over_grant, **2 000/2 000** at **23.256** items/s. final: 0 grants, 0 items, job `failed`. `oracle_agreement` FAIL 7 of 258 and `utilization` FAIL 0.00 — run4's, unchanged. New: `base_accuracy` FAIL, see §3 |
| `sc8-S4a-repeat` | **PASS\*** | **FAIL (P1)** | — | identical: 648 grants, 648 blind, 2 000/2 000, **23.810** items/s, 0 OOM; `base_accuracy` INFO here |

`slope_accuracy` FAIL and `calibration_learned` "nothing was learned" on both
`sc8-S4a` legs are run4's figures for a memory-blind leg: a blind grant prices
nothing, so there is no slope to compare and no rung to certify. They are the
*expected* shape of the behaviour P1 was blocking, not a regression.

## 3. The one new check verdict

`base_accuracy` **FAIL** on `sc8-S2` (60.29%) and `sc8-S4a` (87.15%) is an
oracle **sampling race**, not a memory fault. In both, the oracle's per-PID
poll landed *inside* the load — admission+19 ms and admission+5 ms — and read
418 MiB and 358 MiB of a 670 MiB model still allocating. Because a sample fell
in the window, the check judged it instead of reporting it; where no sample
fell in the window (`sc8-S4a-repeat`, +31 ms; `sc8-S2-unseeded`, +223 ms) it
is INFO and "not judged", and run4's `sc8-S4a` sample at +110 ms read 0.0%.
The ledger's own `base_mb=670 (nvml)` is identical across every leg and every
pass. Nothing in either commit touches a measurement path.

## 4. Deviations from the final pass's procedure

* The sc8 set only, six legs; `drive.sh` here is `../final/drive.sh` cut down
  to them, with the same hog floor and the same per-leg flags.
* `legs.py` was called directly instead of through `../final/runleg.py`: the
  tier/scale defect that wrapper shimmed is fixed in the cherry-picked
  `60e0a4e1`, and `runleg.py` is not on this branch.
* `server-C1u.toml`, `env.C1u`, `noseed-config/`, `probe-wd.json` and
  `bisect-wd.json` were reused in place from the final pass's host directory.
* `analyse.sh` is `../final/analyse.sh`'s rows for these six legs, verbatim.
* The external hog's recording was copied into each leg directory as
  `hog.jsonl` after the pass, as the final pass did, so `hog_tracking`
  reports rather than SKIPs.

## 5. Layout

```
final-p1/
  report.md                  this file
  drive.sh analyse.sh        the driver and the analysis
  analyse-all.txt drive.log  the whole pass's analysis and timing
  sc8-hog.jsonl.gz           the external hog to 8 192 MiB free
  <leg>/<scenario>/          legs.json, panoptikon.log.gz, vramrec.jsonl.gz,
                             healthrec.jsonl.gz, hog.jsonl.gz, fds.jsonl,
                             jobs/failures/metadata/health snapshots,
                             calibration.after.toml, analyze.txt, verdicts.json
  <leg>.log                  the driver's own output for that leg
```

`gunzip` the four recordings before re-running `analyze.py --scenario
<leg>/<scenario>`, which reads the plain names.
