#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/desktop-dev-macos-lib.sh"

skip_ui_build=false
skip_npm_ci=false
bundled_app=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-ui-build) skip_ui_build=true ;;
        --skip-npm-ci) skip_npm_ci=true ;;
        --bundled-app) bundled_app=true ;;
        -h|--help)
            echo "Usage: scripts/run-desktop-dev.sh [--skip-ui-build] [--skip-npm-ci] [--bundled-app]"
            exit 0
            ;;
        *) desktop_dev_die "unknown argument: $1" ;;
    esac
    shift
done

desktop_dev_init
if [[ "$skip_ui_build" == "true" ]]; then
    desktop_dev_require_standalone
else
    run_npm_ci=false
    if [[ "$skip_npm_ci" == "false" && ! -d "$UI_ROOT/node_modules" ]]; then
        run_npm_ci=true
    fi
    desktop_dev_build_ui "$run_npm_ci"
fi
desktop_dev_build_sidecar debug

if [[ "$bundled_app" == "true" ]]; then
    echo "Building the debug Panoptikon Desktop Dev app bundle..."
    (cd "$DESKTOP_ROOT" && npx --yes @tauri-apps/cli@2.11.4 build \
        --bundles app --debug --config src-tauri/tauri.dev.conf.json)
    app_bundle="$REPO_ROOT/target/debug/bundle/macos/Panoptikon Desktop Dev.app"
    [[ -d "$app_bundle" ]] || desktop_dev_die "the Desktop Dev app was not found at $app_bundle"
    echo "Launching bundled Panoptikon Desktop Dev (quit the app to stop)..."
    exec open -W -n "$app_bundle"
fi

echo "Starting unpackaged Panoptikon Desktop Dev (Ctrl+C to stop)..."
cd "$DESKTOP_ROOT"
exec npx --yes @tauri-apps/cli@2.11.4 dev --config src-tauri/tauri.dev.conf.json
