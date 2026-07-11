#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1

if [ ! -x target/release/panoptikon ]; then
    echo "target/release/panoptikon not found."
    echo "Build it first with: cargo build --release -p panoptikon"
    exit 1
fi

exec target/release/panoptikon --config config/gateway/local.toml "$@"
