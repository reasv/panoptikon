#!/bin/bash
# fix/refusal-room (p1) verification: the sc8 set only, on the fix binary.
# Same procedure as final/drive.sh, same hog floor, same flags.
set -u
cd /home/moot/projects/panoptikon || exit 1

V=/home/moot/projects/panoptikon/python/.venv/bin/python
LEGS=/home/moot/projects/panoptikon/tools/calibration-protocol/legs.py
BIN=/home/moot/projects/panoptikon/target/release/panoptikon
R=/home/moot/projects/panoptikon/tools/calibration-protocol/results
F=$R/final-p1
UCFG=$R/final/server-C1u.toml
HOG=/home/moot/projects/panoptikon/tools/calibration-protocol/hog.py
LOG=$F/drive.log

mkdir -p "$F"
say() { echo "### $(date -Is) $*" >> "$LOG"; }

run() {
  name=$1; shift
  if [ -f "$F/$name/.done" ]; then say "SKIP $name"; return; fi
  say "START $name"
  timeout 3600 "$V" "$LEGS" --bin "$BIN" "$@" --run-id "final-p1/$name" \
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

start_hog 6502 8192 "$F/sc8-hog.jsonl"
run sc8-S2           --scenario S2  --config C1
run sc8-S2-unseeded  --scenario S2  --config "$UCFG"
run sc8-S2-vith      --scenario S2  --config C1 --model clip/ViT-H-14-quickgelu_dfn5b
run sc8-S14-flor     --scenario S14 --config C1 --model florence2/msft_large-caption
run sc8-S4a          --scenario S4a --config C1 --gpu-total-mb 8192
run sc8-S4a-repeat   --scenario S4a --config C1 --gpu-total-mb 8192
stop_hog 6502

say "PASS DONE"
nvidia-smi --query-gpu=memory.used --format=csv,noheader >> "$LOG"
echo DONE > "$F/.pass-done"
