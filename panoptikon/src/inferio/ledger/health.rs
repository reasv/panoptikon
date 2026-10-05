//! The ledger's `GET /health` snapshot and its response shapes.

use super::*;

impl VramLedger {
    /// Read-only ledger snapshot for `GET /health`.
    pub fn health(&self) -> Vec<GpuBudgetHealth> {
        let mut state = self.lock();
        // Refresh pools first, so `external_mb` is not computed from pool
        // readings older than the free reading.
        Self::refresh_pools_locked(&mut state);
        // Settle time-based deflation repayment before reporting it.
        let workers: Vec<WorkerId> = state.workers.keys().copied().collect();
        for worker in workers {
            Self::repay_deflation_locked(&mut state, worker);
        }
        let state = &*state;
        let mut gpus: Vec<GpuBudgetHealth> = state
            .gpus
            .iter()
            .map(|(uuid, gpu)| {
                let external = Self::external_locked(state, uuid);
                let (reserve, reserve_rule) = self.reserve_locked(
                    state,
                    uuid,
                    external.unwrap_or(0),
                    self.budgets.for_gpu(uuid).margin_in_force(),
                );
                let mut workers: Vec<LedgerWorkerHealth> = state
                    .workers
                    .values()
                    .filter(|entry| &entry.gpu == uuid)
                    .map(|entry| {
                        let cal = cal_locked(state, entry);
                        let anchor = cal.map(|cal| cal.max_units_measured).unwrap_or(0);
                        let knee = cal.and_then(|cal| cal.knee_units).filter(|knee| *knee > 0);
                        let shape_ceiling = shape_ceiling_for(cal, entry);
                        LedgerWorkerHealth {
                            inference_id: entry.inference_id.clone(),
                            footprint_mb: entry.footprint_mb(),
                            charge_mb: entry.charge_mb(),
                            base_mb: entry.base_mb,
                            reserved_at_load_mb: entry.reserved_at_load_mb,
                            reserved_mb: entry.reserved_mb,
                            alloc_retries_last_window: entry.alloc_retries_last_window,
                            alloc_retries_total: entry.alloc_retries_total,
                            pool_releases: entry.pool_releases,
                            last_release_mb: entry.last_release_mb,
                            last_release_ms: entry.last_release_ms,
                            last_regrow_mb: entry.last_regrow_mb,
                            last_regrow_batch_ms: entry.last_regrow_batch_ms,
                            grants_outstanding: entry.grants.len(),
                            grants_mb: entry.grants_mb(),
                            pending_requests: entry.pending_requests,
                            seed_units: entry.seed_units,
                            deflation: entry.deflation,
                            clean_windows: entry.clean_windows,
                            unit_budget: Self::budget_locked(state, entry),
                            max_units_measured: anchor,
                            knee_units: knee,
                            shape_ceiling_units: shape_ceiling,
                            death_cap_units: cal.and_then(|cal| cal.death_cap_units),
                            knee_is_local: cal.is_some_and(|cal| cal.knee_is_local),
                            trial_units: cal.and_then(|cal| cal.probe).map(|probe| probe.run),
                            retest_after_windows: cal.map_or(0, |cal| cal.retest_after),
                            throughput_samples: cal.map_or(0, |cal| {
                                cal.evidence
                                    .values()
                                    .map(|size| size.windows as usize)
                                    .sum()
                            }),
                            local_samples: cal.map(|cal| cal.local_samples).unwrap_or(0),
                            effective_margin: self.effective_margin_locked(state, entry),
                            ram_resident_mb: entry.has_ram_side().then(|| entry.ram_resident_mb()),
                            ram_mb_per_unit: cal
                                .and_then(|cal| cal.ram_cost)
                                .map(|cost| cost.mb_per_unit),
                            ram_booked_mb: entry.ram_booked_mb(),
                            ram_ceiling_binding: entry.ram_bound,
                            fit: cal.and_then(|cal| cal.fit).map(|fit| FitHealth {
                                slope_mb_per_unit: fit.slope_mb_per_unit,
                                intercept_mb: fit.intercept_mb,
                                residual_mb: fit.residual_mb,
                                samples: fit.samples,
                                pool_margin: Self::pool_margin_locked(state, entry),
                            }),
                        }
                    })
                    .collect();
                workers.sort_by(|a, b| a.inference_id.cmp(&b.inference_id));
                GpuBudgetHealth {
                    gpu_uuid: uuid.clone(),
                    gpu_name: gpu.name.clone(),
                    device_kind: state.inventory.device_kind(uuid).to_owned(),
                    gpu_arch: gpu.arch.clone(),
                    total_mb: gpu.total_mb,
                    external_mb: external.unwrap_or(0),
                    external_known: external.is_some(),
                    external_source: gpu.free.as_ref().map(|sample| sample.source.clone()),
                    external_sample_age_ms: gpu
                        .free
                        .as_ref()
                        .map(|sample| sample.at.elapsed().as_millis() as u64),
                    limit_mb: self.limit_locked(state, uuid),
                    reserve_mb: reserve,
                    reserve_rule: reserve_rule.to_owned(),
                    headroom_mb: self.headroom_locked(state, uuid),
                    charges_mb: Self::charges_locked(state, uuid),
                    footprints_mb: Self::footprints_locked(state, uuid),
                    load_reservations_mb: gpu.load_reservations.values().copied().sum(),
                    grants_mb: Self::grants_locked(state, uuid),
                    grants_outstanding: workers
                        .iter()
                        .map(|worker| worker.grants_outstanding)
                        .sum(),
                    margin: self.budgets.for_gpu(uuid).margin_in_force(),
                    cap_fraction: self.budgets.for_gpu(uuid).cap_fraction,
                    sizing: self
                        .budgets
                        .for_gpu(uuid)
                        .sizing
                        .unwrap_or_default()
                        .as_str()
                        .to_owned(),
                    workers,
                }
            })
            .collect();
        gpus.sort_by(|a, b| a.gpu_uuid.cmp(&b.gpu_uuid));
        gpus
    }
}

/// One GPU's ledger state in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuBudgetHealth {
    pub gpu_uuid: String,
    pub gpu_name: String,
    /// `"cuda"`, `"rocm"`, `"mps"` or `"cpu"`; a host can list several kinds.
    pub device_kind: String,
    /// Calibration profile key (`sm_120`, `gfx1100`, `apple-m3`, `cpu`); `null`
    /// until a load report names one.
    pub gpu_arch: Option<String>,
    pub total_mb: u64,
    /// `max(0, total − free − Σ our footprints)`: what other processes hold.
    /// On unified memory the footprints of both devices sharing the RAM count.
    pub external_mb: u64,
    /// False when no free reading exists yet and `external_mb` is assumed 0.
    pub external_known: bool,
    /// Source of the freshest free reading: `"nvml"`, `"torch"`,
    /// `"nvidia-smi"` or `"amdgpu-sysfs"`.
    pub external_source: Option<String>,
    pub external_sample_age_ms: Option<u64>,
    /// The admission budget: `min(total × cap_fraction,
    /// total − external − reserve_mb)`.
    pub limit_mb: u64,
    /// The reserve applied to this GPU on top of `external_mb`.
    pub reserve_mb: u64,
    /// `"user_margin"` (configured, uncapped), `"capped_default"` (default
    /// fraction, clamped), `"gpu_floor"` (3 % of the card, at most 1 GiB,
    /// where the default fraction gives less; not on Apple Silicon),
    /// `"flat_default"` (the cap itself, on a CUDA GPU that spills to system
    /// RAM) or `"ram_floor"` (the CPU device's minimum: a tenth of RAM, at
    /// most 16 GiB, at least 2 GiB or a quarter of RAM).
    pub reserve_rule: String,
    /// `limit − Σ charges − Σ load reservations`; on unified memory the
    /// charges of both devices sharing the RAM.
    pub headroom_mb: u64,
    /// `Σ` per-worker `footprint + max(0, grants − pool growth)`; what
    /// `headroom_mb` subtracts. On the CPU device it includes GPU replicas'
    /// resident sets and RAM bookings, as `footprints_mb` and `grants_mb` do.
    pub charges_mb: u64,
    pub footprints_mb: u64,
    pub load_reservations_mb: u64,
    pub grants_mb: u64,
    pub grants_outstanding: usize,
    pub margin: f64,
    pub cap_fraction: Option<f64>,
    /// How the batch size trades memory for speed: `balanced` or `throughput`.
    pub sizing: String,
    pub workers: Vec<LedgerWorkerHealth>,
}

/// Overwrite the inventory's totals with the ledger's, which may have adopted
/// a worker's figure on a unified-memory host, so `/health` shows one total.
pub(crate) fn publish_adopted_totals(gpus: &mut [super::gpu::GpuInfo], vram: &[GpuBudgetHealth]) {
    for gpu in gpus {
        if let Some(budget) = vram.iter().find(|budget| budget.gpu_uuid == gpu.uuid) {
            gpu.total_mb = budget.total_mb;
        }
    }
}

/// One resident replica's ledger state.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct LedgerWorkerHealth {
    pub inference_id: String,
    /// `base + max(0, reserved − reserved_at_load)`: this resident's footprint.
    pub footprint_mb: u64,
    /// `footprint + max(0, grants − pool growth)`: this replica's charge now.
    /// On the CPU device `footprint + grants`.
    pub charge_mb: u64,
    pub base_mb: Option<u64>,
    /// The allocator pool at load and now; on the CPU device the replica's
    /// resident set.
    pub reserved_at_load_mb: Option<u64>,
    pub reserved_mb: Option<u64>,
    /// Allocator retries in the last window that reported them, and the total.
    /// Absent off CUDA; absent is not zero.
    pub alloc_retries_last_window: Option<u64>,
    pub alloc_retries_total: Option<u64>,
    /// Trims that freed memory, and the last one's MiB and `empty_cache()`
    /// time. Absent off CUDA and MPS.
    pub pool_releases: Option<u64>,
    pub last_release_mb: Option<u64>,
    pub last_release_ms: Option<f64>,
    /// The first batch after a host-requested release: MiB the pool grew back,
    /// and that batch's whole duration.
    pub last_regrow_mb: Option<u64>,
    pub last_regrow_batch_ms: Option<f64>,
    pub grants_outstanding: usize,
    pub grants_mb: u64,
    /// Demand signal behind the contention split.
    pub pending_requests: usize,
    pub seed_units: u64,
    /// Halvings currently applied by OOM / throughput-collapse deflation.
    pub deflation: u32,
    /// Consecutive clean windows since the last negative sample.
    pub clean_windows: u32,
    /// The unit budget as of this snapshot: the batch size under the ratchet.
    pub unit_budget: u64,
    /// Ratchet anchor: largest locally measured clean priced batch.
    pub max_units_measured: u64,
    /// The working batch size: where the evidence per size put it, or the
    /// size this replica opened at.
    pub knee_units: Option<u64>,
    /// `knee_units` was opened or moved on this machine, in this run or the
    /// one that stored it. `false` for a size seeded from a shipped profile.
    pub knee_is_local: bool,
    /// The batch size the probe in progress runs next; absent between probes.
    pub trial_units: Option<u64>,
    /// Windows at `knee_units` still to run before the next probe.
    pub retest_after_windows: u32,
    /// Shape ceiling from `index_limit` clamps: caps `unit_budget`; runtime-only.
    pub shape_ceiling_units: Option<u64>,
    /// Half the batch a replica of this model was running here when its
    /// process died mid-window: caps `unit_budget` until the server restarts.
    pub death_cap_units: Option<u64>,
    /// Windows counted in the evidence per batch size, over every run.
    pub throughput_samples: usize,
    /// Local fit samples, including restored ones; the margin widens below
    /// `LOCAL_CONFIRMATION_SAMPLES`.
    pub local_samples: u32,
    /// The GPU's margin, widened while the fit is unconfirmed or scattered.
    pub effective_margin: f64,
    /// A GPU replica's host RAM, booked on the CPU device: its resident set
    /// (absent when its RAM is not booked), the MiB per unit its grants book
    /// (absent until a batch measured it), what its outstanding grants hold
    /// booked, and whether host RAM capped its last grant.
    pub ram_resident_mb: Option<u64>,
    pub ram_mb_per_unit: Option<f64>,
    pub ram_booked_mb: u64,
    pub ram_ceiling_binding: bool,
    pub fit: Option<FitHealth>,
}

/// The fitted cost model in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FitHealth {
    pub slope_mb_per_unit: f64,
    pub intercept_mb: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// Observed reserved/allocated ratio, raised a tenth (three times at
    /// most) by out-of-memory windows at the limit of the device's room; a
    /// grant is `(max(0, intercept) + slope × units) × pool_margin`.
    /// Runtime-only.
    pub pool_margin: f64,
}
