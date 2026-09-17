#!/bin/sh
# A1 -- a host-RAM OOM on a CPU-priced Mac. $1 = cpu | auto (the accelerator,
# and so the currency: host RAM, or Metal's headroom for the negative control).
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
export HF_HOME=/Users/moot/projects/panoptikon-pr27/.mac/hf
ACC=$1
R=/Users/moot/projects/panoptikon-pr27
T=$R/tools/calibration-protocol
V=$R/python/.venv/bin/python
O=$T/results/fin-a1-$ACC
B=http://127.0.0.1:6342
rm -rf $O; mkdir -p $O/root
: > $O/status.txt
echo "started $(date -u +%FT%TZ) accelerator=$ACC head=$(cd $R && git rev-parse HEAD)" >> $O/status.txt
echo "hw.memsize_mb=$(( $(sysctl -n hw.memsize) / 1048576 ))" >> $O/status.txt

# A 1 Hz host-RAM sampler, so free_mb_at_failure has a comparand at the instant
# of the failure. `ram_available_mb` is what the worker itself reports.
( while true; do
    echo "$(date -u +%FT%T.%3NZ) $(vm_stat | tr '\n' ' ')" >> $O/vmstat.log
    sleep 1
  done ) &
VM=$!

RUST_LOG="info,panoptikon::inferio=trace" INFERIO_WORKER_LOG_LEVEL=DEBUG \
  $R/target/release/panoptikon --config $R/.mac/final/server-C1-a1$ACC.toml \
  --root $O/root --disable-update-check > $O/gateway.log 2>&1 &
GW=$!
i=0; while [ $i -lt 120 ]; do curl -sf $B/api/inference/health >/dev/null 2>&1 && break; sleep 2; i=$((i+1)); done
echo "health up after ${i}x2s" >> $O/status.txt
curl -s $B/api/inference/health > $O/health-boot.json
curl -s $B/api/inference/metadata > $O/metadata.json

$V $T/healthrec.py --base $B --out $O/health.jsonl --interval 0.5 --quiet > $O/healthrec.log 2>&1 &
HR=$!
$V $T/vramrec.py --out $O/vramrec.jsonl --interval 0.5 --quiet > $O/vramrec.log 2>&1 &
VR=$!

$V $T/loadgen.py --base $B --out $O/loadgen.jsonl \
  --corpus $T/results/corpus/ramp4/manifest.json \
  --model "id=calibfixture/cpu_alloc_oom,concurrency=1,items=8,requests=6,cache_key=a1,lru_size=2,ttl_seconds=3600" \
  > $O/loadgen.log 2>&1
echo "loadgen exit=$? $(date -u +%FT%TZ)" >> $O/status.txt
sleep 8
curl -s $B/api/inference/health > $O/health-final.json
echo "free_pages_now=$(vm_stat | head -2 | tail -1)" >> $O/status.txt

kill $HR $VR $VM 2>/dev/null
sleep 2
kill $GW 2>/dev/null
i=0; while kill -0 $GW 2>/dev/null && [ $i -lt 40 ]; do sleep 1; i=$((i+1)); done
kill -9 $GW 2>/dev/null
sleep 2
cp $O/root/data/panoptikon.log $O/panoptikon.log 2>/dev/null
echo "swapusage=$(sysctl -n vm.swapusage)" >> $O/status.txt
echo "ALLDONE $(date -u +%FT%TZ)" >> $O/status.txt
