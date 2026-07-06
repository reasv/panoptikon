#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1

if [ ! -x target/release/gateway ]; then
    echo "target/release/gateway not found."
    echo "Build it first with: cargo build --release -p gateway"
    exit 1
fi

exec target/release/gateway --config config/gateway/local.toml
