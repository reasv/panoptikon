#!/bin/sh
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
R=/Users/moot/projects/panoptikon-pr27
V=$R/python/.venv/bin/python
T=$R/tools/calibration-protocol
S=$R/.mac/final/corpora.status
: > $S
cd $R
for spec in "smoke 1.0 smoke" "ramp 1.0 ramp" "ramp 4.0 ramp4" "text 1.0 text"; do
  set -- $spec
  tier=$1; scale=$2; dir=$3
  cp $T/results/corpus/$dir/manifest.json $R/.mac/final/manifest-old-$dir.json 2>/dev/null
  rm -rf $T/results/corpus/$dir
  $V $T/corpus.py --tier $tier --scale $scale --out $T/results/corpus/$dir --force \
      > $R/.mac/final/corpus-$dir.log 2>&1
  echo "$dir exit=$? $(date -u +%FT%TZ)" >> $S
done
echo "DONE $(date -u +%FT%TZ)" >> $S
