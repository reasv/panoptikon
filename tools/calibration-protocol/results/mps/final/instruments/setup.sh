#!/bin/sh
set -u
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
R=/Users/moot/projects/panoptikon-pr27
S=$R/.mac/final/setup.status
: > $S
echo "started $(date -u +%FT%TZ) head=$(cd $R && git rev-parse HEAD) wired=$(sysctl -n iogpu.wired_limit_mb)" >> $S
cd $R
cargo build --release -p panoptikon > $R/.mac/final/build.log 2>&1
echo "cargo exit=$? $(date -u +%FT%TZ)" >> $S
uv sync --locked --extra cpu --group test --directory $R/python > $R/.mac/final/uvsync.log 2>&1
echo "uvsync exit=$? $(date -u +%FT%TZ)" >> $S
echo "DONE $(date -u +%FT%TZ)" >> $S
