#!/bin/sh
# The mixed leg: CLIP on Metal beside Florence2 pinned devices = ["cpu"],
# 150 images each, then a load/unload/load of Florence2 to prove the pin holds.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
export HF_HOME=/Users/moot/projects/panoptikon-pr27/.mac/hf
R=/Users/moot/projects/panoptikon-pr27
T=$R/tools/calibration-protocol
V=$R/python/.venv/bin/python
O=$T/results/fin-mixed
B=http://127.0.0.1:6342
rm -rf $O; mkdir -p $O/root
: > $O/status.txt
echo "started $(date -u +%FT%TZ) head=$(cd $R && git rev-parse HEAD)" >> $O/status.txt
echo "before memory_pressure=$(memory_pressure 2>/dev/null | tail -1)" >> $O/status.txt
echo "before swapusage=$(sysctl -n vm.swapusage)" >> $O/status.txt
echo "before wired_limit=$(sysctl -n iogpu.wired_limit_mb)" >> $O/status.txt

RUST_LOG="info,panoptikon::inferio=trace" INFERIO_WORKER_LOG_LEVEL=DEBUG \
  $R/target/release/panoptikon --config $R/.mac/run5/server-C8-mac.toml \
  --root $O/root --disable-update-check > $O/gateway.log 2>&1 &
GW=$!
i=0; while [ $i -lt 120 ]; do curl -sf $B/api/inference/health >/dev/null 2>&1 && break; sleep 2; i=$((i+1)); done
echo "health up after ${i}x2s, gateway pid=$GW" >> $O/status.txt
curl -s $B/api/inference/health > $O/health-boot.json

$V $T/healthrec.py --base $B --out $O/health.jsonl --interval 0.5 --quiet > $O/healthrec.log 2>&1 &
HR=$!
$V $T/vramrec.py --out $O/vramrec.jsonl --interval 0.5 --quiet > $O/vramrec.log 2>&1 &
VR=$!

# 150 images on each device, side by side.
$V $T/loadgen.py --base $B --out $O/loadgen.jsonl \
  --corpus $T/results/corpus/ramp4/manifest.json \
  --model "id=clip/apple_MobileCLIP-S1,concurrency=2,items=25,requests=6,cache_key=mix,lru_size=4,ttl_seconds=3600" \
  --model "id=florence2/msft_large-caption,concurrency=1,items=10,requests=15,cache_key=mix,lru_size=4,ttl_seconds=3600" \
  > $O/loadgen.log 2>&1 &
LG=$!

( while kill -0 $LG 2>/dev/null; do
    echo "$(date -u +%FT%TZ) $(memory_pressure 2>/dev/null | tail -1) | swapused=$(sysctl -n vm.swapusage)" >> $O/pressure.log
    sleep 10
  done ) &
WATCH=$!
wait $LG
echo "loadgen exit=$? $(date -u +%FT%TZ)" >> $O/status.txt
kill $WATCH 2>/dev/null
curl -s $B/api/inference/health > $O/health-both.json
echo "after-load memory_pressure=$(memory_pressure 2>/dev/null | tail -1)" >> $O/status.txt
echo "after-load swapusage=$(sysctl -n vm.swapusage)" >> $O/status.txt

# The reload: does the cpu pin survive unload -> load with a pool worker parked?
echo "--- florence2 unload" >> $O/status.txt
curl -s -X DELETE "$B/api/inference/cache/mix/florence2/msft_large-caption" >> $O/status.txt
echo >> $O/status.txt
sleep 5
curl -s $B/api/inference/health > $O/health-unloaded.json
echo "--- florence2 load 2" >> $O/status.txt
curl -s -X PUT "$B/api/inference/load/florence2/msft_large-caption?cache_key=mix&lru_size=4&ttl_seconds=3600" >> $O/status.txt
echo >> $O/status.txt
sleep 8
curl -s $B/api/inference/health > $O/health-reload.json
# and one predict on the reloaded replica, to prove it still works on the CPU
$V $T/loadgen.py --base $B --out $O/loadgen-reload.jsonl \
  --corpus $T/results/corpus/ramp4/manifest.json \
  --model "id=florence2/msft_large-caption,concurrency=1,items=2,requests=1,cache_key=mix,lru_size=4,ttl_seconds=3600" \
  > $O/loadgen-reload.log 2>&1
echo "reload predict exit=$? $(date -u +%FT%TZ)" >> $O/status.txt
curl -s $B/api/inference/health > $O/health-final.json
curl -s $B/api/inference/metadata > $O/metadata.json

kill $HR $VR 2>/dev/null
sleep 2
kill $GW 2>/dev/null
i=0; while kill -0 $GW 2>/dev/null && [ $i -lt 40 ]; do sleep 1; i=$((i+1)); done
kill -9 $GW 2>/dev/null
sleep 2
cp $O/root/data/panoptikon.log $O/panoptikon.log 2>/dev/null
echo "after memory_pressure=$(memory_pressure 2>/dev/null | tail -1)" >> $O/status.txt
echo "after swapusage=$(sysctl -n vm.swapusage)" >> $O/status.txt
echo "ALLDONE $(date -u +%FT%TZ)" >> $O/status.txt
