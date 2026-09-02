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
//! subcommand). Hardware awareness sits alongside: `capability` (compute
//! capability floors, GPU-compatibility package 1), `gpu` (board identities
//! and worker→GPU pinning), `cost` (per-model cost dimensions), `ledger`
//! (the per-GPU VRAM budget that grants every dispatch window its memory)
//! and `calibration` (the profile store those budgets are primed from and
//! persisted to) — the last four are batch calibration
//! (docs/batch-calibration-design.md).

pub mod calibration;
pub mod capability;
pub mod cost;
/// The CPU half of `gpu`: one synthetic unified-memory board over the host's
/// own RAM, for a machine with no accelerator at all. Private for the same
/// reason `mps` and `rocm` are — `gpu` is the only entry point any backend is
/// reached through.
mod cpu;
pub mod dispatch;
pub mod gpu;
pub mod http;
pub mod ledger;
pub mod manager;
/// The MPS half of `gpu`: one synthetic unified-memory board from macOS
/// kernel facts. Private for the same reason `rocm` is — `gpu` is the only
/// entry point any backend is reached through.
mod mps;
pub mod prewarm;
pub mod registry;
/// The ROCm half of `gpu`: KFD/amdgpu sysfs instead of nvidia-smi. Private
/// because `gpu` is the only entry point either backend is reached through.
mod rocm;
pub mod slot_error;
pub mod worker;
