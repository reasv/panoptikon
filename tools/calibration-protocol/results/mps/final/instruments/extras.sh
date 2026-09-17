#!/bin/sh
# R1: four more CLIP cold ramps (b-e; a is inside legs.sh).
# Then four more MiniLM loadgen repeats (b-e; a is inside legs.sh).
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
ROOT=/Users/moot/projects/panoptikon-pr27
T=$ROOT/tools/calibration-protocol
C4=$T/results/corpus/ramp4
S=$ROOT/.mac/final/extras.status
: > $S
echo "started $(date -u +%FT%TZ)" >> $S

# S4d, re-run: the first attempt was refused by the ramp8 tier check (F-final-2).
rm -rf $T/results/fin-4d
sh $ROOT/.mac/final/leg.sh S4d-mps --scenario S4d --run-id fin-4d \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "final tip 2133563f wired 0: step down, hog released at t=120s"
echo "S4d-mps-rerun exit=$? $(date -u +%FT%TZ)" >> $S
sleep 25

for r in b c d e; do
  sh $ROOT/.mac/final/leg.sh S2-clip-long-$r --scenario S2 --run-id fin-2long-$r \
    --model clip/apple_MobileCLIP-S1 --corpus $C4 \
    --note "final tip 2133563f wired 0: R1 CLIP cold ramp $r"
  echo "clip-$r exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
done
echo "R1-DONE $(date -u +%FT%TZ)" >> $S

for r in b c d e; do
  RUNID=fin-2t-loadgen-$r sh $ROOT/.mac/final/minilm.sh \
      > $ROOT/.mac/final/S2-text-loadgen-$r.log 2>&1
  echo "minilm-$r exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
done

# S14-textembed on the text tier: the leg whose OCR actually writes the rows
# textembed then embeds. S14-chain runs the same three models on smoke,
# where the images are gradients and the OCR writes nothing (F-final-3).
sh $ROOT/.mac/final/leg.sh S14-textembed --scenario S14-textembed \
  --run-id fin-14te --corpus $T/results/corpus/text \
  --note "final tip 2133563f wired 0: derived-setter chain on the text tier"
echo "S14-textembed exit=$? $(date -u +%FT%TZ)" >> $S

echo "ALLDONE $(date -u +%FT%TZ)" >> $S
