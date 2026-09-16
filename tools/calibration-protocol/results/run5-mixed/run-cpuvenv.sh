#!/bin/bash
# run5-mixed leg 3: a +cpu venv on a host whose resolved accelerator is cuda,
# with no pin and no registry override. Nothing in the environment says "cpu";
# torch does (commit d1dc5197 / f2804990).
set -u
T=/home/admin/projects/panoptikon-wt/verify-mixed; D=$T/tools/calibration-protocol; O=/home/admin/projects/panoptikon-wt/verify-mixed/tools/calibration-protocol/results/run5-mixed/cpu-venv
V=/home/admin/projects/panoptikon/python/.venv/bin/python
export TMPDIR=$HOME/tmp-verify-mixed
: > $O/status.txt; echo "started $(date -u +%FT%TZ)" >> $O/status.txt
RUST_LOG="info,panoptikon::inferio=trace" INFERIO_WORKER_LOG_LEVEL=DEBUG \
  $T/target/release/panoptikon --config $D/config/server-C9.toml --root $O/root \
  --disable-update-check > $O/gateway.log 2>&1 &
GW=$!
for i in $(seq 1 120); do curl -sf http://127.0.0.1:6392/api/inference/health >/dev/null 2>&1 && break; sleep 2; done
curl -s http://127.0.0.1:6392/api/inference/health > $O/health-boot.json
$V $D/loadgen.py --base http://127.0.0.1:6392 --out $O/loadgen.jsonl \
  --model "id=textembed/all-MiniLM-L6-v2,concurrency=2,items=32,requests=20,corpus=/home/admin/projects/panoptikon/tools/calibration-protocol/results/corpus/text/manifest.json,cache_key=run5,lru_size=4,ttl_seconds=3600" \
  > $O/loadgen.log 2>&1
echo "loadgen exit=$? $(date -u +%FT%TZ)" >> $O/status.txt
curl -s http://127.0.0.1:6392/api/inference/health > $O/health-final.json
kill $GW 2>/dev/null; for i in $(seq 1 30); do kill -0 $GW 2>/dev/null || break; sleep 1; done; kill -9 $GW 2>/dev/null
echo "ALLDONE $(date -u +%FT%TZ)" >> $O/status.txt
