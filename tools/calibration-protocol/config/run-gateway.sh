#!/usr/bin/env bash
# Start one calibration-protocol gateway. Phase 0 of
# docs/batch-calibration-test-protocol.md (§2 logging, §3 configurations).
#
#   run-gateway.sh <C0|C1|C2|C3> <root-dir> [-- extra panoptikon args]
#
#   <root-dir>  becomes the process's --root: panoptikon chdirs into it at
#               startup, so everything CWD-relative lands there and nothing is
#               shared between configurations --
#                 <root>/data/panoptikon.log          gateway + worker log
#                 <root>/data/inferio/calibration.toml local calibration store
#                 <root>/data/index/<name>/            index DBs
#                 <root>/data/user_data/<name>/
#                 <root>/data/tmp, <root>/data/transcode-cache
#               The chdir is also why server-C*.toml pins python/impl_dirs/
#               config_dirs/pythonpath to absolute paths (see the header there).
#
# Runs in the FOREGROUND on purpose: the caller decides whether to background
# it, and the console copy of the log is what a scenario agent tails. Stop it
# with SIGINT/SIGTERM.
#
# Examples
#   run-gateway.sh C1 "$PWD/results/run-1/S2"
#   run-gateway.sh C0 "$PWD/results/run-1/S2-baseline"
#   run-gateway.sh C2 "$PWD/results/run-1/S10" -- --disable-update-check
#
# Ports (from the matching server-C*.toml): C1 6342/6343/6339,
# C0 6352/6353/6349, C2 6362/6363/6359, C3 6372/6373/6369.
set -euo pipefail

usage() { sed -n '2,30p' "$0"; exit "${1:-2}"; }

ID="${1:-}"; ROOT="${2:-}"
[ -n "$ID" ] && [ -n "$ROOT" ] || usage
shift 2
[ "${1:-}" = "--" ] && shift

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$HERE/server-$ID.toml"
ENVFILE="$HERE/env.$ID"
[ -f "$CONFIG" ]  || { echo "no such config: $CONFIG" >&2; exit 2; }
[ -f "$ENVFILE" ] || { echo "no such env file: $ENVFILE" >&2; exit 2; }

# env.<ID> carries RUST_LOG, INFERIO_WORKER_LOG_LEVEL, any
# CUDA_VISIBLE_DEVICES restriction, and PANOPTIKON_BIN / PANOPTIKON_TREE.
set -a
# shellcheck disable=SC1090
. "$ENVFILE"
set +a

# The repo's own .env normally auto-loads from the CWD, but --root chdirs
# away from it, so the ${PDFIUM_PATH:-} / ${HTML_RENDERER_PATH:-} /
# ${PANOPTIKON_FONT:-} / ${SAUCENAO_API_KEY} / ${JINA_API_KEY} templates in
# the config would all fall back to empty. Export it here instead. Never
# echoed: it holds API keys. CALIB_LOAD_DOTENV=0 skips it.
if [ "${CALIB_LOAD_DOTENV:-1}" = "1" ] && [ -f "$PANOPTIKON_TREE/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$PANOPTIKON_TREE/.env"
  set +a
fi
# env.<ID>'s RUST_LOG must win over anything .env sets.
set -a
# shellcheck disable=SC1090
. "$ENVFILE"
set +a

[ -x "$PANOPTIKON_BIN" ] || {
  echo "release binary missing: $PANOPTIKON_BIN (cargo build --release -p panoptikon in $PANOPTIKON_TREE)" >&2
  exit 3
}

mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"

echo "config=$ID bin=$PANOPTIKON_BIN root=$ROOT" >&2
echo "RUST_LOG=$RUST_LOG INFERIO_WORKER_LOG_LEVEL=$INFERIO_WORKER_LOG_LEVEL CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES-<unset>}" >&2
echo "log=$ROOT/data/panoptikon.log  calibration=$ROOT/data/inferio/calibration.toml" >&2

# --disable-update-check: no GitHub call at startup; it would add latency and
# a network dependency to every scenario's t=0.
exec "$PANOPTIKON_BIN" \
  --config "$CONFIG" \
  --root "$ROOT" \
  --disable-update-check \
  "$@"
