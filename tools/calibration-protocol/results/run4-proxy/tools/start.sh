#!/bin/bash
export CUDA_VISIBLE_DEVICES=""
export TMPDIR=$HOME/tmp-run4-proxy
export RUST_LOG="info,panoptikon::policy=debug"
MB=/home/admin/projects/panoptikon-master/target/release/panoptikon
BB=/home/admin/projects/panoptikon-wt/run4-upgrade/target/release/panoptikon
run() { bin=$1; name=$2;
  nohup $bin --root $HOME/tmp-run4-proxy/$name --config $HOME/tmp-run4-proxy/cfg/$name.toml --disable-update-check \
    > $HOME/tmp-run4-proxy/logs/$name.stdout 2>&1 &
  echo "$name pid=$!"
}
run $MB m_trustfalse
run $MB m_trusttrue
run $MB m_strict
run $BB b_trustfalse
run $BB b_trusttrue
run $BB b_strict
