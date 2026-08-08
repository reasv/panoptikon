#!/usr/bin/env bash
set -euo pipefail

force=false
dry_run=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) force=true ;;
        --dry-run) dry_run=true ;;
        -h|--help)
            echo "Usage: scripts/reset-desktop-dev.sh [--force] [--dry-run]"
            exit 0
            ;;
        *) echo "error: unknown argument: $1" >&2; exit 1 ;;
    esac
    shift
done

[[ "$(uname -s)" == "Darwin" ]] || { echo "error: this script supports macOS only" >&2; exit 1; }
identifier="app.panoptikon.desktop.dev"
app_support="$HOME/Library/Application Support"
logs="$HOME/Library/Logs"
targets=("$app_support/$identifier" "$logs/$identifier")

for target in "${targets[@]}"; do
    case "$target" in
        "$app_support/$identifier"|"$logs/$identifier") ;;
        *) echo "error: refusing unsafe Desktop Dev reset target: $target" >&2; exit 1 ;;
    esac
done

if pgrep -x panoptikon-desktop >/dev/null 2>&1 || pgrep -x panoptikon >/dev/null 2>&1; then
    echo "error: quit Panoptikon Desktop and Panoptikon Server before resetting state" >&2
    exit 1
fi

echo "Panoptikon Desktop Dev state to remove:"
printf '  %s\n' "${targets[@]}"
if [[ "$dry_run" == "true" ]]; then
    echo "Dry run: no files were removed."
    exit 0
fi
if [[ "$force" == "false" ]]; then
    read -r -p "Type RESET to permanently remove this development state: " answer
    [[ "$answer" == "RESET" ]] || { echo "Reset cancelled."; exit 1; }
fi

for target in "${targets[@]}"; do
    if [[ -e "$target" ]]; then
        rm -rf -- "$target"
        echo "Removed $target"
    else
        echo "Already absent: $target"
    fi
done
echo "Panoptikon Desktop Dev will run first-time setup on its next launch."
