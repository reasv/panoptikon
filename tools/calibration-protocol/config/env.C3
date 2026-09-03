# Environment for one calibration-protocol gateway run. Sourced by
# run-gateway.sh; every line is `KEY=value` (no `export`, no quoting tricks)
# because the script exports the whole file with `set -a`.
#
# The two logging variables are mandatory for every run
# (docs/batch-calibration-test-protocol.md §2, "Server-side capture"):
#   RUST_LOG                 -- the ledger's grant/settle/refit/refresh/admit
#                               lines live at trace on panoptikon::inferio
#                               (added in 49822c8b, finding G1). RUST_LOG wins
#                               over [logging].level in the TOML.
#   INFERIO_WORKER_LOG_LEVEL -- Python logging.basicConfig level in the worker;
#                               the packing/memory DEBUG lines (batch plans,
#                               bucket contents, base tier chosen) come from
#                               here and are forwarded into the gateway log at
#                               INFO as `worker=<impl_class> "{line}"`.
#
# C3: the documented off-switch. Any index-form entry in CUDA_VISIBLE_DEVICES
# blanks the whole inventory at INFO (gpu.rs restrict_to_visible), so there is
# no pinning, no ledger and no calibration: the unpriced path. "1" is GPU 1.
# Everything must still work, just without budgets.
PANOPTIKON_BIN=/home/admin/projects/panoptikon/target/release/panoptikon
PANOPTIKON_TREE=/home/admin/projects/panoptikon
RUST_LOG=info,panoptikon::inferio=trace
INFERIO_WORKER_LOG_LEVEL=DEBUG
CUDA_VISIBLE_DEVICES=1
