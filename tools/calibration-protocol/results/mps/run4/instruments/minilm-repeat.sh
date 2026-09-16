#!/bin/sh
# run4: two more MiniLM loadgen repeats, to match the old pass's N=3.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
P=/Users/moot/projects/panoptikon-pr27
S=$P/.mac/run4/minilm-repeat.status
: > $S
echo "started $(date -u +%FT%TZ)" >> $S
for r in b c; do
  RUNID=r4-2t-loadgen-$r sh $P/.mac/minilm.sh > $P/.mac/run4/S2-text-loadgen-$r.log 2>&1
  echo "r4-2t-loadgen-$r exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
done
echo ALLDONE >> $S
