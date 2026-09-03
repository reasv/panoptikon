# Scenario <SCENARIO> - run <RUN-ID>

Template per `docs/batch-calibration-test-protocol.md` §7. Filled in by the
agent that ran the scenario, **before the next scenario starts**. Every command
is written out in full so any result can be reproduced by hand.

| | |
|---|---|
| Scenario | <SCENARIO> |
| Configuration | <CONFIG> (see protocol §3) |
| Run id | <RUN-ID> |
| Date (UTC) | <DATE> |
| Host | <HOST> |
| Branch / commit | <BRANCH> / <COMMIT> |
| Driver | <DRIVER> |
| GPUs | <GPUS> |
| Note | <NOTE> |
| Status | **not started** / running / complete / **blocked** |

## 1. Setup

What was running before the scenario started, and what this scenario changed.

- SGLang / other resident models:
- Server started as (exact command, config path, `--root`):
- Environment (`RUST_LOG`, `INFERIO_WORKER_LOG_LEVEL`, `CUDA_VISIBLE_DEVICES`,
  `PYTORCH_CUDA_ALLOC_CONF`, anything else non-default):
- Corpus (tier, path, item count, manifest sha):
- Calibration store state at the start (present / deleted / edited; snapshot
  copied to `calibration.before.toml`):
- Models used and whether they were already in the HF cache:

## 2. Commands

Verbatim, in order, including the recorder invocations.

```bash
# results dir
DIR=$(python/.venv/bin/python tools/calibration-protocol/newrun.py --scenario <SCENARIO> --run-id <RUN-ID>)

# recorders (background)
python/.venv/bin/python tools/calibration-protocol/vramrec.py   --out "$DIR/vramrec.jsonl"   &
python/.venv/bin/python tools/calibration-protocol/healthrec.py --out "$DIR/healthrec.jsonl" &
python/.venv/bin/python tools/calibration-protocol/hog.py --out "$DIR/hog.jsonl" --port 6401 ... &

# the scenario itself
...

# teardown
kill %1 %2 %3
cp data/inferio/calibration.toml "$DIR/calibration.after.toml"
cp data/panoptikon.log "$DIR/panoptikon.log"
curl -s "$B/api/jobs/data/history?index_db=cal&page=1&page_size=50" > "$DIR/jobs.json"
python/.venv/bin/python tools/calibration-protocol/analyze.py --scenario "$DIR" --checks ... \
    --json "$DIR/verdicts.json" --plot "$DIR/timeline.png"
```

## 3. Timeline of observations

Wall-clock notes taken while the scenario ran: what was done, what was seen,
and anything surprising. One line per event.

| t (s) | Event | Observation |
|---|---|---|

## 4. Verdict table

Paste `analyze.py`'s output verbatim, then one line per row saying whether the
verdict is accepted, and why if it is not.

```
CHECK                VERDICT  DETAIL
```

Adjudication:

- `oracle_agreement`:
- `base_accuracy`:
- `grant_safety`:
- ...

## 5. Scenario-specific pass criteria

The criteria this scenario states in §4, each with the number that decided it.

| Criterion (from §4) | Measured | Pass? |
|---|---|---|

## 6. Targeted probes (§5)

Findings this scenario is meant to confirm or clear (B1, B2, ...): what was
observed for each.

| Id | Confirmed / cleared / inconclusive | Evidence |
|---|---|---|

## 7. Anomalies

Anything unexpected, whether or not it failed a check. A missed threshold with
a small margin is a finding to discuss, not an automatic fail (§4).

## 8. Log excerpts

The lines that carry the evidence, with enough context to read them.

```
```

## 9. Reproduction

The shortest command sequence that reproduces the result from a clean tree,
plus anything about the host state that must hold for it to reproduce.

## 10. Files

| File | Size | Note |
|---|---|---|
| `vramrec.jsonl` | | |
| `healthrec.jsonl` | | |
| `hog.jsonl` | | |
| `panoptikon.log` | | |
| `calibration.before.toml` | | |
| `calibration.after.toml` | | |
| `jobs.json` | | |
| `verdicts.json` | | |
| `timeline.png` | | |
