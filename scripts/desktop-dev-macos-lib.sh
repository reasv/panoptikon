#!/usr/bin/env bash

# Shared implementation for the macOS Desktop Dev scripts. This file is meant
# to be sourced, not executed directly.

desktop_dev_die() {
    echo "error: $*" >&2
    exit 1
}

desktop_dev_require() {
    command -v "$1" >/dev/null 2>&1 || desktop_dev_die "required command not found: $1"
}

desktop_dev_init() {
    [[ "$(uname -s)" == "Darwin" ]] || desktop_dev_die "this script supports macOS only"

    desktop_dev_require xcodebuild
    local license_output
    if ! license_output="$(xcodebuild -license check 2>&1)"; then
        echo "$license_output" >&2
        desktop_dev_die \
            "Xcode is not ready; review and accept its license with 'sudo xcodebuild -license'"
    fi

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
    UI_ROOT="$REPO_ROOT/ui"
    STANDALONE="$UI_ROOT/.next/standalone"
    DESKTOP_ROOT="$REPO_ROOT/panoptikon-desktop"

    desktop_dev_require npm
    desktop_dev_require npx
    desktop_dev_require python3

    if command -v rustup >/dev/null 2>&1; then
        local requested="${RUSTUP_TOOLCHAIN:-stable}"
        local selected="$requested"
        local cargo_minor
        cargo_minor="$(rustup run "$selected" cargo --version 2>/dev/null | sed -nE 's/^cargo 1\.([0-9]+).*/\1/p')"
        if [[ -z "$cargo_minor" || "$cargo_minor" -lt 85 ]]; then
            while IFS= read -r candidate; do
                candidate="${candidate%% *}"
                [[ "$candidate" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || continue
                cargo_minor="$(rustup run "$candidate" cargo --version 2>/dev/null | sed -nE 's/^cargo 1\.([0-9]+).*/\1/p')"
                if [[ -n "$cargo_minor" && "$cargo_minor" -ge 85 ]]; then
                    selected="$candidate"
                    break
                fi
            done < <(rustup toolchain list)
        fi
        cargo_minor="$(rustup run "$selected" cargo --version 2>/dev/null | sed -nE 's/^cargo 1\.([0-9]+).*/\1/p')"
        [[ -n "$cargo_minor" && "$cargo_minor" -ge 85 ]] || desktop_dev_die \
            "Rust 1.85 or newer is required; update stable with 'rustup update stable'"
        local sysroot
        sysroot="$(rustup run "$selected" rustc --print sysroot)"
        export PATH="$sysroot/bin:$PATH"
        echo "Using Rust toolchain $selected"
    fi

    desktop_dev_require cargo
    desktop_dev_require rustc
    local cargo_minor
    cargo_minor="$(cargo --version | sed -nE 's/^cargo 1\.([0-9]+).*/\1/p')"
    [[ -n "$cargo_minor" && "$cargo_minor" -ge 85 ]] || desktop_dev_die \
        "Cargo 1.85 or newer is required for this Rust 2024 workspace"

    HOST_TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"
    [[ "$HOST_TRIPLE" == *-apple-darwin ]] || desktop_dev_die \
        "the selected Rust toolchain is not a macOS host toolchain: $HOST_TRIPLE"
    STAGED_SIDECAR="$DESKTOP_ROOT/src-tauri/binaries/panoptikon-$HOST_TRIPLE"
}

desktop_dev_ensure_ui() {
    if [[ ! -f "$UI_ROOT/package.json" ]]; then
        echo "Initializing the pinned UI submodule..."
        git -C "$REPO_ROOT" submodule update --init --recursive ui
    fi
    [[ -f "$UI_ROOT/package.json" ]] || desktop_dev_die \
        "the UI checkout is unavailable at $UI_ROOT"
}

desktop_dev_build_ui() {
    local run_npm_ci="$1"
    desktop_dev_ensure_ui
    if [[ "$run_npm_ci" == "true" ]]; then
        (cd "$UI_ROOT" && npm ci)
    elif [[ ! -d "$UI_ROOT/node_modules" ]]; then
        desktop_dev_die "UI dependencies are absent; rerun without --skip-npm-ci"
    fi

    echo "Building the standalone Panoptikon UI..."
    (cd "$UI_ROOT" && BUILD_STANDALONE=true npm run build)
    [[ -f "$STANDALONE/server.js" ]] || desktop_dev_die \
        "the standalone UI build did not produce $STANDALONE/server.js"

    rm -rf -- "$STANDALONE/.next/static"
    mkdir -p "$STANDALONE/.next"
    cp -R "$UI_ROOT/.next/static" "$STANDALONE/.next/static"
    if [[ -d "$UI_ROOT/public" ]]; then
        rm -rf -- "$STANDALONE/public"
        cp -R "$UI_ROOT/public" "$STANDALONE/public"
    fi
}

desktop_dev_require_standalone() {
    desktop_dev_ensure_ui
    [[ -f "$STANDALONE/server.js" ]] || desktop_dev_die \
        "no standalone UI exists; rerun without --skip-ui-build"
}

desktop_dev_build_sidecar() {
    local profile="$1"
    local cargo_args=(build -p panoptikon --features bundled,bundled-ui)
    if [[ "$profile" == "release" ]]; then
        cargo_args+=(--release)
    fi

    echo "Staging the pinned PDFium runtime..."
    (cd "$REPO_ROOT" && python3 scripts/stage-pdfium.py --target "$HOST_TRIPLE")

    echo "Building the bundled Panoptikon Server sidecar ($profile)..."
    (cd "$REPO_ROOT" && PANOPTIKON_UI_BUNDLE="$STANDALONE" cargo "${cargo_args[@]}")

    local sidecar="$REPO_ROOT/target/$profile/panoptikon"
    [[ -f "$sidecar" ]] || desktop_dev_die "the Server build did not produce $sidecar"
    mkdir -p "$(dirname "$STAGED_SIDECAR")"
    install -m 755 "$sidecar" "$STAGED_SIDECAR"
    echo "Staged sidecar: $STAGED_SIDECAR"
}
