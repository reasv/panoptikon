#!/bin/bash
# final/3090 regression pass at 2133563f. Sequential; one leg at a time.
set -u
cd /home/moot/projects/panoptikon || exit 1

V=/home/moot/projects/panoptikon/python/.venv/bin/python
RL=/home/moot/projects/panoptikon/tools/calibration-protocol/results/final/runleg.py
BIN=/home/moot/projects/panoptikon/target/release/panoptikon
R=/home/moot/projects/panoptikon/tools/calibration-protocol/results
F=$R/final
CORP=$R/corpus
UCFG=$F/server-C1u.toml
HOG=/home/moot/projects/panoptikon/tools/calibration-protocol/hog.py
LOG=$F/drive.log

say() { echo "### $(date -Is) $*" >> "$LOG"; }

run() {
  name=$1; shift
  if [ -f "$F/$name/.done" ]; then say "SKIP $name"; return; fi
  say "START $name"
  timeout 3600 "$V" "$RL" --bin "$BIN" "$@" --run-id "final/$name" \
      > "$F/$name.log" 2>&1
  rc=$?
  mkdir -p "$F/$name" && echo "$rc" > "$F/$name/.done"
  say "END $name rc=$rc"
  sleep 8
}

start_hog() {  # port leave-free out
  say "HOG START port=$1 leave-free=$2"
  nohup "$V" "$HOG" --target gpu --device 0 --port "$1" --reeval 999999 \
      --out "$3" leave-free "$2" > "$3.log" 2>&1 &
  echo $! > "/tmp/hog-$1.pid"
  sleep 20
  nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader >> "$LOG"
}

stop_hog() {
  say "HOG STOP port=$1"
  kill -TERM "$(cat /tmp/hog-$1.pid)" 2>/dev/null
  sleep 8
  nvidia-smi --query-gpu=memory.used --format=csv,noheader >> "$LOG"
}

say "PASS START $(git -C /home/moot/projects/panoptikon rev-parse --short HEAD)"

# ---- S1: the neighbour is still resident (external hog leaves 2048 MiB) ----
start_hog 6501 2048 "$F/S1-hog.jsonl"
run S1 --scenario S1 --config C1
stop_hog 6501

# ---- full-card regression ----
run S2-wdvit           --scenario S2  --config C1
run S2-wdvit-unseeded  --scenario S2  --config "$UCFG"
run S3-wdvit           --scenario S3  --config C1
run S4a                --scenario S4a --config C1
run S4b                --scenario S4b --config C1 --corpus "$CORP/ramp4"
run S4c                --scenario S4c --config C1 --corpus "$CORP/ramp4"
run S4d                --scenario S4d --config C1 --corpus "$CORP/ramp4"
run S4d-repeat         --scenario S4d --config C1 --corpus "$CORP/ramp4"

# ---- S5 fixtures ----
run S5-oom2        --scenario S5 --config C1 --model calibfixture/oom_second_batch_cuda
run S5-oom1        --scenario S5 --config C1 --model calibfixture/oom_cuda
run S5-failbatch   --scenario S5 --config C1 --model calibfixture/failbatch_cuda
run S5-oomtext     --scenario S5 --config C1 --model calibfixture/failbatch_oomtext_cuda
run S5-oomtimed    --scenario S5 --config C1 --model calibfixture/oom_timed_cuda
run S5-dying       --scenario S5 --config C1 --model calibfixture/dying_cuda
run S5-diesonload  --scenario S5 --config C1 --model calibfixture/dies_on_load_cuda
run S5-oomimpl     --scenario S5 --config C1 --model clip/apple_MobileCLIP-S1 --corpus "$CORP/poison"

# ---- S14 ----
run S14-tags       --scenario S14 --config C1
run S14-clip       --scenario S14 --config C1 --model clip/apple_MobileCLIP-S1
run S14-ocr        --scenario S14 --config C1 --model doctr/db_resnet50_crnn_mobilenet_v3_small
run S14-whisper    --scenario S14 --config C1 --model whisper/tiny --scan-audio
run S14-florence2  --scenario S14 --config C1 --model florence2/msft_large-caption
run S14-textembed  --scenario S14-textembed --config C1

# ---- small card: 8 GB free ----
start_hog 6502 8192 "$F/sc8-hog.jsonl"
run sc8-S2           --scenario S2  --config C1
run sc8-S2-unseeded  --scenario S2  --config "$UCFG"
run sc8-S2-vith      --scenario S2  --config C1 --model clip/ViT-H-14-quickgelu_dfn5b
run sc8-S14-flor     --scenario S14 --config C1 --model florence2/msft_large-caption
run sc8-S4a          --scenario S4a --config C1 --gpu-total-mb 8192
stop_hog 6502

say "PASS DONE"
nvidia-smi --query-gpu=memory.used --format=csv,noheader >> "$LOG"
echo DONE > "$F/.pass-done"
