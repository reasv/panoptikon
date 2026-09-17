#!/bin/bash
# run5-mixed (CUDA host): the CUDA devices and the CPU device side by side.
# tags/wd-vit-tagger-v3 on the GPU, textembed/all-MiniLM-L6-v2 pinned to the
# CPU device (devices = ["cpu"]) through the C8 registry override.
set -u
T=/home/admin/projects/panoptikon-wt/verify-mixed
D=$T/tools/calibration-protocol
O=$D/results/run5-mixed/${LEG:-cuda-cpu}
V=/home/admin/projects/panoptikon/python/.venv/bin/python
export TMPDIR=$HOME/tmp-verify-mixed
mkdir -p $O $O/root
: > $O/status.txt
echo "started $(date -u +%FT%TZ) CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES-<unset>}" >> $O/status.txt

RUST_LOG="info,panoptikon::inferio=trace" INFERIO_WORKER_LOG_LEVEL=DEBUG \
LD_LIBRARY_PATH=/home/admin/projects/panoptikon/python/.venv/lib/python3.12/site-packages/nvidia/cudnn/lib \
  $T/target/release/panoptikon --config $D/config/server-C8.toml \
  --root $O/root --disable-update-check > $O/gateway.log 2>&1 &
GW=$!
echo "gateway pid=$GW" >> $O/status.txt
for i in $(seq 1 120); do curl -sf http://127.0.0.1:6382/api/inference/health > /dev/null 2>&1 && break; sleep 2; done
curl -s http://127.0.0.1:6382/api/inference/health > $O/health-boot.json
$V $D/healthrec.py --base http://127.0.0.1:6382 --out $O/health.jsonl > $O/healthrec.log 2>&1 &
HR=$!
$V $D/loadgen.py --base http://127.0.0.1:6382 --out $O/loadgen.jsonl \
  --model "id=tags/wd-vit-tagger-v3,concurrency=2,items=32,requests=10,corpus=/home/admin/projects/panoptikon/tools/calibration-protocol/results/corpus/ramp/manifest.json,cache_key=run5,lru_size=4,ttl_seconds=3600" \
  --model "id=textembed/all-MiniLM-L6-v2,concurrency=2,items=32,requests=20,corpus=/home/admin/projects/panoptikon/tools/calibration-protocol/results/corpus/text/manifest.json,cache_key=run5,lru_size=4,ttl_seconds=3600" \
  > $O/loadgen.log 2>&1
echo "loadgen exit=$? $(date -u +%FT%TZ)" >> $O/status.txt
curl -s http://127.0.0.1:6382/api/inference/health > $O/health-final.json
kill $HR 2>/dev/null; sleep 1; kill $GW 2>/dev/null
for i in $(seq 1 30); do kill -0 $GW 2>/dev/null || break; sleep 1; done
kill -9 $GW 2>/dev/null
echo "ALLDONE $(date -u +%FT%TZ)" >> $O/status.txt
