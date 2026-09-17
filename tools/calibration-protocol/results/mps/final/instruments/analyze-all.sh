#!/bin/sh
# usage: .mac/final/analyze-all.sh <prefix>   (fin)
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
P=/Users/moot/projects/panoptikon-pr27
V=$P/python/.venv/bin/python
T=$P/tools/calibration-protocol
PFX=$1
OUT=$P/.mac/final/analyze-$PFX
mkdir -p $OUT
cd $P
for d in $T/results/$PFX-*/*/; do
  rid=$(basename $(dirname $d)); sc=$(basename $d)
  case "$rid/$sc" in *-mixed/*|*-a1-*/*|*/root) continue ;; esac
  L="$d/legs.json"
  if [ -f "$L" ]; then
    $V - "$L" <<'PY' > /tmp/ac.$$ 2>/dev/null
import json,sys,shlex
o=json.load(open(sys.argv[1]))
print(" ".join(shlex.quote(x) for x in o["analyze_command"]))
PY
    CMD=$(cat /tmp/ac.$$); rm -f /tmp/ac.$$
  else
    CMD="$V $T/analyze.py --scenario $d --checks all --learning --json $d/verdicts.json"
  fi
  echo "### $rid/$sc"
  echo "\$ $CMD"
  sh -c "$CMD" 2>&1
  echo
done > $OUT/analyze.txt 2>&1
echo "wrote $OUT/analyze.txt"
