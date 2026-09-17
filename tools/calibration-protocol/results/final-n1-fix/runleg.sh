#!/bin/bash
# runleg.sh <run-id> [hog]  -- one S2 wd-vit ramp leg on the quiet box,
# against the fix/cpu-knee-gate binary (verify/cpu-knee-gate worktree).
set -u
RUN=$1
HOGMODE=${2:-}
OUT=/home/admin/tmp-vkg/$RUN.driver.log
export TMPDIR=/home/admin/tmp-verify-kneegate
{
echo "=== $RUN start $(date -Is)"
/home/admin/tmp-vkg/quiet.sh || { echo "ABORT: box not quiet"; exit 1; }
echo "--- free -m BEFORE"; free -m
echo "--- uptime BEFORE"; uptime
HOG=""
if [ "$HOGMODE" = "hog" ]; then
  python3 /home/admin/tmp-vkg/ramhog.py 1200 &
  HOG=$!
  echo "--- ramhog pid $HOG (6 GB alloc/touch/free every 5 s, 1200 s)"
fi
cd /home/admin/tmp-vkg
python3 /home/admin/projects/panoptikon-wt/verify-kneegate/tools/calibration-protocol/legs.py \
  --scenario S2 --model tags/wd-vit-tagger-v3 \
  --bin /home/admin/projects/panoptikon-wt/verify-kneegate/target/release/panoptikon \
  --config /home/admin/tmp-vkg/legs-config/server-N1FIX.toml \
  --python /home/admin/projects/panoptikon-wt/final-deploy/pyenv/.venv/bin/python \
  --results /home/admin/tmp-vkg/legs --run-id "$RUN" \
  --corpus /home/admin/tmp-final-deploy/corpus/ramp \
  --port 6922 --gpu-total-mb 128649 --no-dotenv \
  --note "N1-fix quiet-box leg $RUN"
echo "--- legs.py exit=$?"
[ -n "$HOG" ] && { kill $HOG 2>/dev/null; wait $HOG 2>/dev/null; echo "--- ramhog stopped"; }
echo "--- free -m AFTER"; free -m
echo "--- uptime AFTER"; uptime
echo "=== $RUN done $(date -Is)"
} >"$OUT" 2>&1
