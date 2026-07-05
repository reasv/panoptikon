"""Minimal Python worker harness for the Rust inferio orchestrator.

Implements docs/inferio-worker-protocol.md (v2). This package must stay
import-light: stdlib + msgpack only at startup. It must never import the
legacy `inferio` package (impl modules themselves may, once instantiated).
"""
