#!/bin/sh
# final pass instruments: pytest, selftest (ambient + induced OOM), platform facts.
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
R=/Users/moot/projects/panoptikon-pr27
V=$R/python/.venv/bin/python
T=$R/tools/calibration-protocol
O=$R/.mac/final
S=$O/instruments.status
export HF_HOME=$R/.mac/hf
mkdir -p $O
: > $S
echo "started $(date -u +%FT%TZ) head=$(cd $R && git rev-parse HEAD)" >> $S

cd $R
{
  echo "date_utc=$(date -u +%FT%TZ)"
  echo "head=$(git rev-parse HEAD)"
  echo "sw_vers=$(sw_vers -productVersion) build=$(sw_vers -buildVersion)"
  echo "uname=$(uname -m)"
  echo "iogpu.wired_limit_mb=$(sysctl -n iogpu.wired_limit_mb)"
  echo "hw.memsize=$(sysctl -n hw.memsize)"
  echo "hw.memsize_mb=$(( $(sysctl -n hw.memsize) / 1048576 ))"
  echo "swapusage=$(sysctl -n vm.swapusage)"
  echo "ulimit_n=$(ulimit -n)"
  echo "ulimit_Hn=$(ulimit -Hn)"
  echo "cargo=$(cargo --version)"
  echo "torch=$($V -c 'import torch;print(torch.__version__)' 2>&1)"
  echo "uv=$(uv --version 2>&1)"
  echo "memory_pressure=$(memory_pressure 2>/dev/null | tail -1)"
} > $O/platform.txt 2>&1
echo "platform $(date -u +%FT%TZ)" >> $S

$V -m pytest $T/tests -q > $O/pytest.log 2>&1
echo "pytest exit=$? $(date -u +%FT%TZ)" >> $S

$V $T/selftest.py --json $O/mps-selftest.json > $O/selftest.log 2>&1
echo "selftest exit=$? $(date -u +%FT%TZ)" >> $S

$V $T/selftest.py --induce-oom --mps-watermark 0.05 --oom-cap-mb 8192 \
    --json $O/mps-selftest-oom.json > $O/selftest-oom.log 2>&1
echo "selftest-oom exit=$? $(date -u +%FT%TZ)" >> $S

echo "DONE $(date -u +%FT%TZ)" >> $S
