#!/bin/sh
# Wait for legs.sh, then run everything else in sequence, one gateway at a time.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
R=/Users/moot/projects/panoptikon-pr27
F=$R/.mac/final
S=$F/pipeline.status
: > $S
echo "waiting for legs.sh $(date -u +%FT%TZ)" >> $S
while ! grep -q ALLDONE $F/legs.status 2>/dev/null; do sleep 20; done
echo "legs done $(date -u +%FT%TZ)" >> $S
sleep 30

sh $F/extras.sh
echo "extras exit=$? $(date -u +%FT%TZ)" >> $S
sleep 30

sh $F/mixed.sh > $F/mixed.log 2>&1
echo "mixed exit=$? $(date -u +%FT%TZ)" >> $S
sleep 30

sh $F/a1.sh cpu > $F/a1-cpu.log 2>&1
echo "a1-cpu exit=$? $(date -u +%FT%TZ)" >> $S
sleep 30

sh $F/a1.sh auto > $F/a1-auto.log 2>&1
echo "a1-auto exit=$? $(date -u +%FT%TZ)" >> $S
sleep 20

# Analysis: this tip's analyze.py over this pass and over both comparands.
sh $F/analyze-all.sh fin   > $F/analyze-fin.out 2>&1
echo "analyze-fin exit=$? $(date -u +%FT%TZ)" >> $S
sh $F/analyze-all.sh r4    > $F/analyze-r4.out 2>&1
echo "analyze-r4 exit=$? $(date -u +%FT%TZ)" >> $S
sh $F/analyze-all.sh r4fix > $F/analyze-r4fix.out 2>&1
echo "analyze-r4fix exit=$? $(date -u +%FT%TZ)" >> $S

V=$R/python/.venv/bin/python
T=$R/tools/calibration-protocol
cd $R
$V $F/r1.py "$T/results/fin-2long*/S2" > $F/r1-2133563f.json 2>&1
$V $F/r1.py "$T/results/r4-2long*/S2" "$T/results/r4fix-2long-*/S2" > $F/r1-0d7f5671.json 2>&1
$V $F/minilm.py "$T/results/fin-2t-loadgen*/S2-text" > $F/minilm-2133563f.json 2>&1
$V $F/minilm.py "$T/results/r4-2t-loadgen*/S2-text" "$T/results/r4fix-2t-loadgen-*/S2-text" > $F/minilm-0d7f5671.json 2>&1
$V $F/limitcheck.py "$T/results/fin-*/*" > $F/limit-formula-2133563f.json 2>&1
$V $F/limitcheck.py "$T/results/r4-*/*" > $F/limit-formula-0d7f5671.json 2>&1
echo "extractors done $(date -u +%FT%TZ)" >> $S

sh $F/collect.sh > $F/collect.out 2>&1
echo "collect exit=$? $(date -u +%FT%TZ)" >> $S
echo "PIPELINE-DONE $(date -u +%FT%TZ)" >> $S
