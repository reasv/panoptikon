#!/bin/bash
# analyze.py per leg, run4's checks and probe ground truth.
set -u
cd /home/moot/projects/panoptikon || exit 1
V=/home/moot/projects/panoptikon/python/.venv/bin/python
A=/home/moot/projects/panoptikon/tools/calibration-protocol/analyze.py
F=/home/moot/projects/panoptikon/tools/calibration-protocol/results/final
P="--probe $F/probe-wd.json --probe $F/bisect-wd.json"
SMOKE=failures,job_outcome,grant_safety,peak_fds

while IFS='|' read -r leg sc checks extra; do
  [ -z "${leg:-}" ] && continue
  case "$leg" in \#*) continue ;; esac
  checks=${checks/SMOKE/$SMOKE}
  extra=${extra//PROBE/$P}
  D="$F/$leg/$sc"
  if [ ! -d "$D" ]; then echo "=== $leg MISSING ($D)"; continue; fi
  echo "=== $leg"
  # shellcheck disable=SC2086
  "$V" "$A" --scenario "$D" --checks "$checks" $extra \
      --json "$D/verdicts.json" > "$D/analyze.txt" 2>&1
  sed -n '/^CHECK/,$p' "$D/analyze.txt"
done <<'ROWS'
S1|S1|oracle_agreement,base_accuracy,footprint_agreement,grant_safety,failures,job_outcome,ledger_invariant,peak_fds|PROBE
S2-wdvit|S2|all|--learning PROBE
S2-wdvit-unseeded|S2|all|--learning PROBE
S3-wdvit|S3|all|PROBE
S4a|S4a|all|PROBE
S4b|S4b|all|PROBE
S4c|S4c|all|PROBE
S4d|S4d|all|PROBE
S4d-repeat|S4d|all|PROBE
S5-oom2|S5|all|--expect-ooms 1
S5-oom1|S5|all|--expect-ooms 300 --expect-failures 300 --expect-failed-jobs 1
S5-failbatch|S5|all|--expect-failures 300
S5-oomtext|S5|all|--expect-failures 300
S5-oomtimed|S5|all|--expect-ooms 300 --expect-failures 300 --expect-failed-jobs 1
S5-dying|S5|all|--expect-deaths 10 --expect-failures 300 --expect-failed-jobs 1
S5-diesonload|S5|all|--expect-deaths 10 --expect-failures 300 --expect-failed-jobs 1
S5-oomimpl|S5|all|--expect-failures 5 --expect-failed-jobs 1
S14-tags|S14|SMOKE|
S14-clip|S14|SMOKE|
S14-ocr|S14|SMOKE|
S14-whisper|S14|SMOKE|
S14-florence2|S14|SMOKE|
S14-textembed|S14-textembed|SMOKE|
sc8-S2|S2|all|--learning PROBE
sc8-S2-unseeded|S2|all|--learning PROBE
sc8-S2-vith|S2|all|--learning
sc8-S14-flor|S14|SMOKE|
sc8-S4a|S4a|all|PROBE
sc8-S4a-repeat|S4a|all|PROBE
ROWS
