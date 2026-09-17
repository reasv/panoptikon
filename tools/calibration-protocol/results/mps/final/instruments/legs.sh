#!/bin/sh
# FINAL MPS regression pass on tip 2133563f at iogpu.wired_limit_mb=0.
# The same 14 legs as run4 (.mac/run4/legs.sh), run-ids fin-*, plus S4b.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
ROOT=/Users/moot/projects/panoptikon-pr27
T=$ROOT/tools/calibration-protocol
C=$T/results/corpus/ramp
C4=$T/results/corpus/ramp4
CS=$T/results/corpus/smoke
S=$ROOT/.mac/final/legs.status
cd $ROOT
: > $S
echo "started $(date -u +%FT%TZ) head=$(git rev-parse HEAD) wired_limit=$(sysctl -n iogpu.wired_limit_mb)" >> $S

N="final tip 2133563f wired 0"
run() {   # $1 = log name, rest = leg args
  L=$1
  sh $ROOT/.mac/final/leg.sh "$@"
  echo "$L exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
}

run S1 --scenario S1 --run-id fin-1 \
  --model tags/wd-vit-tagger-v3 --corpus $CS \
  --note "$N: inventory, identity, total adoption"

run S2-wdvit --scenario S2 --run-id fin-2w \
  --model tags/wd-vit-tagger-v3 --corpus $C \
  --note "$N: cold ramp on an idle GPU (hog-free external control)"

run S2-clip-long --scenario S2 --run-id fin-2long \
  --model clip/apple_MobileCLIP-S1 --corpus $C4 \
  --note "$N: ramp4 8000 items, R1 repeat a"

RUNID=fin-2t-loadgen sh $ROOT/.mac/final/minilm.sh > $ROOT/.mac/final/S2-text-loadgen.log 2>&1
echo "S2-text-loadgen exit=$? $(date -u +%FT%TZ)" >> $S
sleep 25

run S3-clip --scenario S3 --run-id fin-3 \
  --model clip/apple_MobileCLIP-S1 --corpus $C \
  --note "$N: warm restart on the CLIP store"

run S3-wdvit --scenario S3 --run-id fin-3w \
  --model tags/wd-vit-tagger-v3 --corpus $C \
  --note "$N: warm restart on the wd-vit store"

run S4a-mps --scenario S4a --run-id fin-4a-mps \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "$N: constant MPS pressure, min-free 20480"

run S4a-ram --scenario S4a --run-id fin-4a-ram \
  --hog-target ram --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "$N: constant RAM pressure, min-free 20480"

run S4d-mps --scenario S4d --run-id fin-4d \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "$N: step down, hog released at t=120s"

run S14-tags --scenario S14 --run-id fin-14t \
  --models tags/wd-vit-tagger-v3 --corpus $CS \
  --note "$N: S14 CI smoke + tags"

run S14-clip --scenario S14 --run-id fin-14c \
  --models clip/apple_MobileCLIP-S1 --corpus $CS \
  --note "$N: S14 load-path clip"

run S14-whisper --scenario S14 --run-id fin-14w \
  --models whisper/tiny --scan-audio --corpus $CS \
  --note "$N: S14 load-path whisper (CTranslate2 device on macOS)"

run S14-ocr --scenario S14 --run-id fin-14o \
  --models doctr/db_resnet50_crnn_mobilenet_v3_small --corpus $CS \
  --note "$N: S14 load-path doctr OCR"

run S14-chain --scenario S14 --run-id fin-14ch \
  --models doctr/db_resnet50_crnn_mobilenet_v3_small,whisper/tiny,textembed/all-MiniLM-L6-v2 \
  --scan-audio --corpus $CS \
  --note "$N: S14 derived-setter chain; Florence2 deliberately excluded"

echo "FOURTEEN-DONE $(date -u +%FT%TZ)" >> $S

# Extra, no run4 comparand: S4b's step-up at t+60 s. Safe on this machine
# (it holds a fraction of the adopted total, not the 2 GB-free squeeze of S4c).
run S4b-mps --scenario S4b --run-id fin-4b \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "$N: step up, hog raised at t=60s (no run4 comparand)"

echo "ALLDONE $(date -u +%FT%TZ)" >> $S
