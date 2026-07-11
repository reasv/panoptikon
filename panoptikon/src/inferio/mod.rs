//! Rust port of the inferio inference-service orchestration layer.
//!
//! Phase 1 (see docs/inferio-rust-orchestrator-design.md): Rust owns model
//! registry parsing and hands workers a resolved `impl_class` + config kwargs
//! in the spawn handshake; workers never read TOML themselves.
//!
//! Layers: the registry (`registry`), worker supervision (`worker`), the
//! model manager with dispatch-time batching (`manager` + `dispatch`), and
//! the wire-compatible HTTP surface (`http`) mounted under
//! `/api/inference` when `[inference_local].enabled` (or via the `inferio`
//! subcommand).

pub mod dispatch;
pub mod http;
pub mod manager;
pub mod prewarm;
pub mod registry;
pub mod worker;
