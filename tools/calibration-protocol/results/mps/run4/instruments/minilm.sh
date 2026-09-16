#!/bin/sh
# S2-text on MPS driven by loadgen, because the extraction route finds no
# items (textembed is a derived setter; the `text` corpus tier extracts none).
# Modelled on <scratch>/tf-minilm.sh from run2.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
P=/Users/moot/projects/panoptikon-pr27
V=$P/python/.venv/bin/python
T=$P/tools/calibration-protocol
D=$T/results/${RUNID:-mps-2t-loadgen}/S2-text
B=http://127.0.0.1:6342
export HF_HOME=$P/.mac/hf
export RUST_LOG=info,panoptikon::inferio=trace
export INFERIO_WORKER_LOG_LEVEL=DEBUG

rm -rf "$D"; mkdir -p "$D/root/data/inferio"
cd "$P"
$V $T/vramrec.py --out "$D/vramrec.jsonl" --interval 0.25 --quiet &
VR=$!
$P/target/release/panoptikon --config $P/.mac/server-C1-mac.toml \
    --root "$D/root" --disable-update-check > "$D/gateway.out" 2>&1 &
GW=$!
i=0
while [ $i -lt 240 ]; do
  curl -fsS "$B/api/inference/health" >/dev/null 2>&1 && break
  i=$((i + 1)); sleep 1
done
$V $T/healthrec.py --base $B --out "$D/healthrec.jsonl" --interval 0.5 --quiet &
HR=$!
echo "gateway $GW vramrec $VR healthrec $HR dir=$D"
curl -fsS "$B/api/inference/health" > "$D/health-t0.json"
curl -fsS -X POST "$B/api/db/create?new_index_db=cal&new_user_data_db=cal" \
     > "$D/dbcreate.json"

$V $T/loadgen.py --base $B --out "$D/loadgen.jsonl" \
   --corpus $T/results/corpus/text/manifest.json \
   --model "id=textembed/all-MiniLM-L6-v2,concurrency=4,items=64,mode=text,order=random" \
   --duration "${DUR:-180}" --seed 7 2>&1 | tee "$D/loadgen.txt"

curl -fsS "$B/api/inference/metadata" > "$D/metadata-after.json"
sleep 40
curl -fsS "$B/api/inference/health" > "$D/health-end.json"
cp "$D/root/data/inferio/calibration.toml" "$D/calibration.after.toml" 2>/dev/null
kill -TERM $VR $HR 2>/dev/null
sleep 2
kill -TERM $GW 2>/dev/null
sleep 10
kill -KILL $GW 2>/dev/null
sleep 2
cp "$D/root/data/panoptikon.log" "$D/panoptikon.log" 2>/dev/null
echo "DONE $D"
