//! Rust port of the inferio inference-service orchestration layer.
//!
//! Phase 1 (see docs/inferio-rust-orchestrator-design.md): Rust owns model
//! registry parsing and hands workers a resolved `impl_class` + config kwargs
//! in the spawn handshake; workers never read TOML themselves.
//!
//! Implemented so far: the registry (`registry`), worker supervision
//! (`worker`), and the model manager with dispatch-time batching
//! (`manager` + `dispatch`); the HTTP surface consumes them as it lands.
#![allow(dead_code)] // Consumed by the orchestrator HTTP layer once it lands.

pub mod dispatch;
pub mod manager;
pub mod registry;
pub mod worker;
