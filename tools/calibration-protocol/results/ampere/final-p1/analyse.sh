#!/bin/bash
# analyze.py per leg, the final pass's checks and probe ground truth.
set -u
cd /home/moot/projects/panoptikon || exit 1
V=/home/moot/projects/panoptikon/python/.venv/bin/python
A=/home/moot/projects/panoptikon/tools/calibration-protocol/analyze.py
R=/home/moot/projects/panoptikon/tools/calibration-protocol/results
F=$R/final-p1
P="--probe $R/final/probe-wd.json --probe $R/final/bisect-wd.json"
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
sc8-S2|S2|all|--learning PROBE
sc8-S2-unseeded|S2|all|--learning PROBE
sc8-S2-vith|S2|all|--learning
sc8-S14-flor|S14|SMOKE|
sc8-S4a|S4a|all|PROBE
sc8-S4a-repeat|S4a|all|PROBE
ROWS
