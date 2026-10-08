//! Rust port of the inferio inference-service orchestration layer.
//!
//! Phase 1 (see docs/inferio-rust-orchestrator-design.md): Rust owns model
//! registry parsing and hands workers a resolved `impl_class` + config kwargs
//! in the spawn handshake; workers never read TOML themselves.
//!
//! Layers: the registry (`registry`), worker supervision (`worker`), the
//! model manager with dispatch-time batching (`manager` + `dispatch`), the
//! wire vocabulary of typed per-item error slots (`slot_error`), and
//! the wire-compatible HTTP surface (`http`) mounted under
//! `/api/inference` when `[inference_local].enabled` (or via the `inferio`
//! subcommand). Alongside: `capability` (compute capability floors), `gpu`
//! (GPU identities and pinning), `cost`, `ledger` (per-GPU memory budget)
//! and `calibration` (docs/batch-calibration-design.md).

pub mod calibration;
pub mod capability;
pub mod cost;
/// `gpu` backend for host RAM as one device; reached only through `gpu`.
mod cpu;
pub mod dispatch;
pub mod gpu;
pub mod http;
pub mod ledger;
pub mod manager;
/// `gpu` backend for Apple unified memory; reached only through `gpu`.
mod mps;
pub mod prewarm;
pub mod registry;
/// `gpu` backend over KFD/amdgpu sysfs; reached only through `gpu`.
mod rocm;
pub mod slot_error;
pub mod worker;
