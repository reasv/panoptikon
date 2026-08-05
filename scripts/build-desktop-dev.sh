#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/desktop-dev-macos-lib.sh"

skip_npm_ci=false
skip_ui_build=false
release_desktop=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-npm-ci) skip_npm_ci=true ;;
        --skip-ui-build) skip_ui_build=true ;;
        --release-desktop) release_desktop=true ;;
        -h|--help)
            echo "Usage: scripts/build-desktop-dev.sh [--skip-npm-ci] [--skip-ui-build] [--release-desktop]"
            exit 0
            ;;
        *) desktop_dev_die "unknown argument: $1" ;;
    esac
    shift
done

desktop_dev_init
if [[ "$skip_ui_build" == "true" ]]; then
    desktop_dev_require_standalone
elif [[ "$skip_npm_ci" == "true" ]]; then
    desktop_dev_build_ui false
else
    desktop_dev_build_ui true
fi
desktop_dev_build_sidecar release

tauri_args=(build --bundles app,dmg --config src-tauri/tauri.dev.conf.json)
profile=debug
if [[ "$release_desktop" == "false" ]]; then
    tauri_args+=(--debug)
else
    profile=release
fi

echo "Building Panoptikon Desktop Dev app and DMG ($profile)..."
(cd "$DESKTOP_ROOT" && npx --yes @tauri-apps/cli@2.11.4 "${tauri_args[@]}")

bundle_dir="$REPO_ROOT/target/$profile/bundle/dmg"
dmg="$(find "$bundle_dir" -maxdepth 1 -type f -name '*.dmg' -print -quit 2>/dev/null || true)"
[[ -n "$dmg" ]] || desktop_dev_die "no Desktop Dev DMG was found under $bundle_dir"
echo "Desktop Dev DMG: $dmg"
