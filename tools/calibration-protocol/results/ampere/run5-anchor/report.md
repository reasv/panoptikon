# run5-anchor — F1 verification pass on `fix/unreachable-anchor`

RTX 3090 (24 576 MiB, sm_86), `systemvi`, same host, driver and venv as
`../run4` (see its §1 for the instruments; nothing about the host changed).
Tip **`c7130a19`** ("Ledger: a rung the squeeze left below the anchor is
re-tested, not a cap") on `e697a6b5`, pushed to the host as `run5-anchor` and
built there with the `~/.cache/ossl` workaround (1 m 28 s,
`target/release/panoptikon`, 83 877 880 B, 2026-09-16 23:32 local).
Config C1 with the host's own path overrides. 2026-09-16, 21:34–22:05 UTC.

`root/` (per-leg databases) was left on the host and is not committed.
The four legs are the F1 shape, its repeat, the unseeded control and the
small-card ViT-H shape that must stay untouched.

## 1. S4d — the finding under test

`legs.py --scenario S4d --corpus ramp4`, seeded from the shipped sm_86 row
(wd-vit anchor 205), external hog `leave-free 2057` released at t + 120 s.

| | run4 (`e697a6b5`-era tip) | run5-anchor (`c7130a19`) |
|---|---|---|
| grant budgets over the job | `1×1, 7×157, 8×1, 21×1, 64×24` | `1×1, 7×161, 8×1, 21×1, 64×4, 128×11` |
| windows at 64 after the hog released | 24, to the end of the job | **4** |
| re-test line | — | **1**, `units=128 from=64` |
| `held_certified` | false for all 566 samples | **true** for 243 of 271 samples at 128 |
| `utilization` | FAIL 64 / 639 = 0.10 | **PASS** 128 / 512 = 0.25 |
| `calibration_learned` | "NOTHING WAS LEARNED … a rung the ring never certified" | "held by the throughput brake at a rung the ring **certified**" |
| throughput | 27.397 items/s | 28.169 items/s |
| `grant_safety` / `failures` | PASS / PASS | PASS (179 grants, 0 unsafe) / PASS (0 OOM) |

Timeline around the release (`unit_budget` / `headroom_mb` / `window_requests`):

```
21:38:11.707  7    headroom 0      requests 21     <- hog still holding
21:38:12.433  21   headroom 22673  requests 21     <- hog releases
21:38:13.158  64   headroom 22036  requests 192
21:38:19.979  64   headroom 19922  requests 192    <- hold announced, units=64
21:38:26.646  64   headroom 19922  requests 192
21:38:33.375  64   headroom 19922  requests 192
21:38:40.132  128  headroom 19922  requests 384    <- re-test, units=128 from=64
… 128 for the remaining 10 windows
```

Four clean at-rung windows, then the doubling — `HOLD_REPROBE_WINDOWS = 4`
exactly. The hold LOG line does print, at 64: 39f7bb2c suppresses it only
while the windows are queue-sized, and the release makes them at-budget two
windows before the probe fires (`QUEUE_BOUND_HOLD_WINDOWS = 2`).

`S4d-repeat` reproduces it to the window: `64×4, 128×10`, one re-test line
`units=128 from=64`, `utilization` PASS 0.25, 28.070 items/s, 181 grants and
0 unsafe.

128 and not 205: `ramp_floor_step(64, 205) = 1`, so the ramp's own term is
`64 << 1 = 128` and the probe's ceiling is `min(205, 128)`. Going past it
needs a measured gain wd-vit's curve does not offer.

## 2. Controls

**`S4d-unseeded`** (shipped baseline moved aside) — matches run4:

| | run4 | run5-anchor |
|---|---|---|
| budgets | `1, 2, 4, 5×217, 7, 14×44, 15×45, 28, 31×7, 56` | `1, 2, 4×2, 5×226, 7, 14×44, 15×43, 28, 31×7, 56` |
| sequence | hold at 14 → lifts → plateau hold | identical |
| re-test lines | — | **0** |
| `utilization` | PASS 1.00 at knee 31 | PASS 1.00, knee 31, rung 56 |
| throughput | — | 28.369 items/s |

Nothing confers an anchor, so the hold sits at the ramp's own term and the
probe is inert — which is every replica running without a shipped profile.

**`sc8-S2-vith`** (ViT-H under an external hog leaving 8 192 MiB free, the
worst realistic small-card shape) — unchanged:

| | run4 | run5-anchor |
|---|---|---|
| held rung | 331 | 348 (the hog settled at 8 200 rather than 8 192 free) |
| re-test lines | — | **0** |
| grants / unsafe / OOM | 4 / 0 / 0 | 4 / 0 / 0 |
| `calibration_learned` | FAIL, nothing learned | FAIL, nothing learned |
| throughput | 32.258 items/s | 33.898 items/s |

The probe cannot arm here and does not need the rung to be out of reach to
stay out: `ample_headroom` is priced at `RATCHET_FACTOR × slope ×
min(anchor, affordable)`, and on a board the conferred 512 does not fit
`affordable` *is* the board — so twice it is more room than exists.

## 3. Known residues, unchanged from run4

* `oracle_agreement` FAIL 1 of 650 joined samples on S4d (run4: 1 of 670) —
  a transient at the hog step, §5 of run4's report.
* `ledger_invariant` WARN with `limit_fell` only: 228 of 813 samples,
  **0 `over_grant`** (run4: 227, 0).
* `peak_fds` still reports against the pre-raise soft limit (run4 F2).
* `utilization`'s probe boundary is 512 here rather than run4's 639, because
  `analyze.py` takes no `--bisect`; at the same 512 denominator run4's S4d is
  0.125 and still under the 0.25 floor.
