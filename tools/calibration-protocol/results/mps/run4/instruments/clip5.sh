#!/bin/sh
# run4 R1 proof: repeats b..e of the S2-clip cold ramp (a = r4-2long from legs.sh).
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
ROOT=/Users/moot/projects/panoptikon-pr27
T=$ROOT/tools/calibration-protocol
S=$ROOT/.mac/run4/clip5.status
: > $S
echo "started $(date -u +%FT%TZ)" >> $S
for r in b c d e; do
  sh $ROOT/.mac/run4/leg.sh S2-clip-long-$r --scenario S2 --run-id r4-2long-$r \
    --model clip/apple_MobileCLIP-S1 --corpus $T/results/corpus/ramp4 \
    --note "run4 tip 0d7f5671 wired 0: CLIP cold ramp, R1 repeat $r"
  echo "r4-2long-$r exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
done
echo ALLDONE >> $S
