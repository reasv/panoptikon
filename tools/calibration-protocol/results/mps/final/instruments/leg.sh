#!/bin/sh
# usage: .mac/final/leg.sh <logname> <extra legs.py args...>
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
cd /Users/moot/projects/panoptikon-pr27
export HF_HOME=/Users/moot/projects/panoptikon-pr27/.mac/hf
LOG=$1; shift
exec python/.venv/bin/python tools/calibration-protocol/legs.py \
  --bin target/release/panoptikon --config .mac/server-C1-mac.toml \
  --results tools/calibration-protocol/results --gpu-total-mb 110100 "$@" \
  > .mac/final/$LOG.log 2>&1
