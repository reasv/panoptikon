#!/usr/bin/env bash
# Install the calibration-protocol fixtures into the locations the shipped
# loader scans by default. Phase 0 of docs/batch-calibration-test-protocol.md.
#
#   install-fixtures.sh [--tree <checkout>] [--uninstall]
#
# Default tree: the checkout this script lives in.
#
# WHERE THE LOADER ACTUALLY LOOKS
#   Impl classes. The gateway sends the worker a list of impl dirs in the spawn
#   handshake; the worker scans them in
#   python/inferio_worker/discovery.py::find_impl_class -- every *.py in each
#   dir in order, loaded as a standalone module by file location, matching
#   IMPL_CLASS.name(). The list comes from [inference_local].impl_dirs, and
#   when that key is empty it defaults to
#   panoptikon/src/resources.rs::default_impl_dirs =
#       ["python/inferio/impl", "inferio_custom"]
#   resolved against the process CWD. Earlier dirs win, so a custom file can
#   never shadow a built-in impl class. Because each file is loaded standalone,
#   the fixtures must be self-contained: no relative imports between them.
#     -> installs to  <tree>/inferio_custom/
#
#   Registry TOMLs. [inference_local].config_dirs, defaulting to
#   ["python/inferio/config", "config/inference"]
#   (panoptikon/src/inferio/registry.rs, RegistryConfig::default), also CWD-
#   relative; the user dir is scanned after the built-in one.
#     -> installs to  <tree>/config/inference/
#
# CAVEAT ABOUT --root: `panoptikon --root <dir>` chdirs into <dir> at startup,
# so those CWD-relative defaults resolve UNDER THE RESULTS DIR, not under the
# checkout. The protocol's configs (../config/server-C*.toml) therefore pin
# impl_dirs/config_dirs to absolute paths in the checkout, which is what makes
# this install visible to a --root'd run. If you would rather not write into
# the checkout at all, skip this script and instead append these two absolute
# paths to the config's existing lists:
#     impl_dirs   += ".../tools/calibration-protocol/fixtures/impls"
#                    ".../python/tests/inferio_worker/fixture_impls"
#     config_dirs += ".../tools/calibration-protocol/fixtures/registry"
# The two approaches are equivalent; the second leaves the tree clean.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TREE="$(cd "$HERE/../../.." && pwd)"
UNINSTALL=0
while [ $# -gt 0 ]; do
  case "$1" in
    --tree) TREE="$2"; shift 2 ;;
    --uninstall) UNINSTALL=1; shift ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

SRC_FIXTURES="$TREE/python/tests/inferio_worker/fixture_impls"
DST_IMPL="$TREE/inferio_custom"
DST_CFG="$TREE/config/inference"

# The four behaviours the protocol needs, torch-free originals.
ORIGINALS=(oom_second_batch_impl.py oom_impl.py failbatch_impl.py dying_impl.py)
# The CUDA-touching variants, which are the ones that get priced on C1.
VARIANTS=(oom_second_batch_cuda_impl.py oom_cuda_impl.py failbatch_cuda_impl.py dying_cuda_impl.py
          oom_timed_cuda_impl.py dies_on_load_cuda_impl.py)
REGISTRY=calibration-fixtures.toml

if [ "$UNINSTALL" = 1 ]; then
  for f in "${ORIGINALS[@]}" "${VARIANTS[@]}"; do rm -fv "$DST_IMPL/$f"; done
  rm -fv "$DST_CFG/$REGISTRY"
  echo "uninstalled from $TREE"
  exit 0
fi

[ -d "$DST_IMPL" ] || { echo "no impl dir: $DST_IMPL" >&2; exit 3; }
[ -d "$DST_CFG" ]  || { echo "no registry dir: $DST_CFG" >&2; exit 3; }

for f in "${ORIGINALS[@]}"; do cp -v "$SRC_FIXTURES/$f" "$DST_IMPL/$f"; done
for f in "${VARIANTS[@]}";  do cp -v "$HERE/impls/$f"   "$DST_IMPL/$f"; done
cp -v "$HERE/registry/$REGISTRY" "$DST_CFG/$REGISTRY"

echo
echo "installed into $TREE"
echo "  impls    -> $DST_IMPL"
echo "  registry -> $DST_CFG/$REGISTRY"
echo "inference ids: calibfixture/{oom_second_batch,oom,failbatch,dying}_{cuda,cpu}"
echo "               calibfixture/{oom_timed,dies_on_load}_cuda"
echo "verify with: curl \$B/api/inference/metadata | jq '.. | .calibfixture? // empty'"
