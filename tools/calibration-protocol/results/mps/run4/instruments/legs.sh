#!/bin/sh
# run4: MPS regression pass on tip 0d7f5671 at iogpu.wired_limit_mb=0.
# Same 14 legs as the 27ff0790 pass (.mac/final-legs.sh), run-ids r4-*.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
ROOT=/Users/moot/projects/panoptikon-pr27
T=$ROOT/tools/calibration-protocol
C=$T/results/corpus/ramp
C4=$T/results/corpus/ramp4
CS=$T/results/corpus/smoke
S=$ROOT/.mac/run4/legs.status
cd $ROOT
: > $S
echo "started $(date -u +%FT%TZ) head=$(git rev-parse HEAD) wired_limit=$(sysctl -n iogpu.wired_limit_mb)" >> $S

run() {   # $1 = log name, rest = leg args
  L=$1
  sh $ROOT/.mac/run4/leg.sh "$@"
  echo "$L exit=$? $(date -u +%FT%TZ)" >> $S
  sleep 25
}

run S1 --scenario S1 --run-id r4-1 \
  --model tags/wd-vit-tagger-v3 --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: inventory, identity, total adoption"

run S2-wdvit --scenario S2 --run-id r4-2w \
  --model tags/wd-vit-tagger-v3 --corpus $C \
  --note "run4 tip 0d7f5671 wired 0: cold ramp on an idle GPU (hog-free external control)"

run S2-clip-long --scenario S2 --run-id r4-2long \
  --model clip/apple_MobileCLIP-S1 --corpus $C4 \
  --note "run4 tip 0d7f5671 wired 0: ramp4 8000 items, R1 repeat a"

RUNID=r4-2t-loadgen sh $ROOT/.mac/minilm.sh > $ROOT/.mac/run4/S2-text-loadgen.log 2>&1
echo "S2-text-loadgen exit=$? $(date -u +%FT%TZ)" >> $S
sleep 25

run S3-clip --scenario S3 --run-id r4-3 \
  --model clip/apple_MobileCLIP-S1 --corpus $C \
  --note "run4 tip 0d7f5671 wired 0: warm restart on the CLIP store"

run S3-wdvit --scenario S3 --run-id r4-3w \
  --model tags/wd-vit-tagger-v3 --corpus $C \
  --note "run4 tip 0d7f5671 wired 0: warm restart on the wd-vit store"

run S4a-mps --scenario S4a --run-id r4-4a-mps \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "run4 tip 0d7f5671 wired 0: constant MPS pressure, min-free 20480"

run S4a-ram --scenario S4a --run-id r4-4a-ram \
  --hog-target ram --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "run4 tip 0d7f5671 wired 0: constant RAM pressure, min-free 20480"

run S4d-mps --scenario S4d --run-id r4-4d \
  --hog-target mps --min-free-mb 20480 --models tags/wd-vit-tagger-v3 \
  --corpus $C4 --note "run4 tip 0d7f5671 wired 0: step down, hog released at t=120s"

run S14-tags --scenario S14 --run-id r4-14t \
  --models tags/wd-vit-tagger-v3 --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: S14 CI smoke + tags"

run S14-clip --scenario S14 --run-id r4-14c \
  --models clip/apple_MobileCLIP-S1 --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: S14 load-path clip"

run S14-whisper --scenario S14 --run-id r4-14w \
  --models whisper/tiny --scan-audio --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: S14 load-path whisper (CTranslate2 device on macOS)"

run S14-ocr --scenario S14 --run-id r4-14o \
  --models doctr/db_resnet50_crnn_mobilenet_v3_small --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: S14 load-path doctr OCR"

run S14-chain --scenario S14 --run-id r4-14ch \
  --models doctr/db_resnet50_crnn_mobilenet_v3_small,whisper/tiny,textembed/all-MiniLM-L6-v2 \
  --scan-audio --corpus $CS \
  --note "run4 tip 0d7f5671 wired 0: S14 derived-setter chain; Florence2 deliberately excluded"

echo "ALLDONE $(date -u +%FT%TZ)" >> $S
