use super::*;

impl VramLedger {
    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    /// Read-only ledger snapshot for `GET /health`.
    pub fn health(&self) -> Vec<GpuBudgetHealth> {
        let mut state = self.lock();
        // Every in-flight resident's pool figure first: `external_mb` is the
        // reported number, and reporting it against pool readings older than
        // the free reading is the whole of the defect.
        Self::refresh_pools_locked(&mut state);
        // `/health` reads the deflation counter, so it settles the time repayment
        // first rather than reporting a level the next grant is about to hand
        // back.
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
                        let held = entry.hold_reported();
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
                            ramp_step: entry.ramp_step,
                            deflation: entry.deflation,
                            clean_windows: entry.clean_windows,
                            unit_budget: admitted_units(entry, anchor, knee, shape_ceiling),
                            ramp_held: held,
                            held_units: held.then_some(entry.held_units).flatten(),
                            held_certified: held && entry.held_certified,
                            max_units_measured: anchor,
                            knee_units: knee,
                            shape_ceiling_units: shape_ceiling,
                            knee_is_local: cal.is_some_and(|cal| cal.knee_is_local),
                            throughput_samples: cal.map(|cal| cal.throughput.len()).unwrap_or(0),
                            local_samples: cal.map(|cal| cal.local_samples).unwrap_or(0),
                            effective_margin: self.effective_margin_locked(state, entry),
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
                    workers,
                }
            })
            .collect();
        gpus.sort_by(|a, b| a.gpu_uuid.cmp(&b.gpu_uuid));
        gpus
    }
}

// ----------------------------------------------------------------------
// Health shapes
// ----------------------------------------------------------------------

/// One GPU's ledger state in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuBudgetHealth {
    pub gpu_uuid: String,
    pub gpu_name: String,
    /// Which kind of device this row is: `"cuda"`, `"rocm"`, `"mps"` or
    /// `"cpu"`. Every host carries the CPU device beside its accelerators, and
    /// a replica is admitted against the device its own load report named, so
    /// one `/health` can hold rows of more than one kind.
    pub device_kind: String,
    /// The calibration profile keyspace for this card (`sm_120`, `gfx1100`,
    /// `apple-m3`, `cpu`). `null` until a load report on it names one.
    pub gpu_arch: Option<String>,
    pub total_mb: u64,
    /// `max(0, total − free − Σ our footprints)`: what other processes hold.
    /// On a unified-memory host the footprints are the whole RAM domain's —
    /// this device's and its peer's alike, since the Metal device and the CPU
    /// device read one pool of physical RAM.
    pub external_mb: u64,
    /// False when no free-memory reading is known yet, in which case
    /// `external_mb` is 0 by assumption rather than by measurement.
    pub external_known: bool,
    /// Which driver answered the freshest free reading: `"nvml"` or
    /// `"torch"` from a worker, `"nvidia-smi"` for a ledger-side staleness
    /// refresh, and `"amdgpu-sysfs"` on ROCm hosts, where it is both.
    pub external_source: Option<String>,
    pub external_sample_age_ms: Option<u64>,
    /// The admission budget: `min(total × cap_fraction,
    /// total − external − reserve_mb)`.
    pub limit_mb: u64,
    /// The VRAM withheld from the budget on top of `external_mb` itself: the
    /// reserve **actually applied** to this GPU, in MiB.
    pub reserve_mb: u64,
    /// Which rule produced `reserve_mb`: `"user_margin"` (the GPU's configured
    /// margin, honoured verbatim and uncapped) or `"capped_default"` (nobody
    /// configured this GPU, so the default fraction applies and is clamped).
    pub reserve_rule: String,
    /// `limit − Σ charges − Σ load reservations`, and on a unified-memory
    /// host those of the **pair**: the Metal device and the CPU device spend
    /// one pool of RAM, so each charges the other's residents. `limit_mb`
    /// stays this device's own ceiling.
    pub headroom_mb: u64,
    /// What the residents actually cost the GPU: `Σ` per-worker
    /// `footprint + max(0, grants − pool growth)`. This, not
    /// `footprints_mb + grants_mb`, is what `headroom_mb` derives from: a grant
    /// is denominated in the same memory the pool-growth term already counts.
    pub charges_mb: u64,
    pub footprints_mb: u64,
    pub load_reservations_mb: u64,
    pub grants_mb: u64,
    pub grants_outstanding: usize,
    pub margin: f64,
    pub cap_fraction: Option<f64>,
    pub workers: Vec<LedgerWorkerHealth>,
}

/// Republish the inventory with the ledger's total per device.
///
/// One device must have one total. Where the ledger adopted a worker's figure
/// (DP-4, a unified-memory host), the probe's row still carries the seed it
/// replaced, and `/health` published both: `98 304` in `gpus` beside `110 100`
/// in `vram`, for the life of the process (MPS pass F6).
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
    /// `footprint + max(0, grants − pool growth)`: what this replica charges the
    /// GPU right now, grant overlap netted out.
    pub charge_mb: u64,
    pub base_mb: Option<u64>,
    pub reserved_at_load_mb: Option<u64>,
    pub reserved_mb: Option<u64>,
    /// Allocator retries the last window that **reported** the counter, and
    /// this replica's running total. Both absent off CUDA, which keeps no such
    /// counter: absent is not zero — a replica reading 0 was measured and was
    /// never short of memory. A window that stretched with no retry was not
    /// short of memory either.
    pub alloc_retries_last_window: Option<u64>,
    pub alloc_retries_total: Option<u64>,
    /// Trim replies that handed memory back (`released_mb > 0`), and what the
    /// most recent release measured: MiB returned and the `empty_cache()`
    /// call's own wall time. Absent on a replica whose pool cannot be
    /// measured, which is every replica off CUDA and MPS.
    pub pool_releases: Option<u64>,
    pub last_release_mb: Option<u64>,
    pub last_release_ms: Option<f64>,
    /// The first batch after a release **the host asked for**: the MiB it grew
    /// the pool back by, and that batch's whole duration. Not a re-grow time —
    /// the `cudaMalloc`s run inside `predict`. The diagnosis path for a search
    /// query that suddenly got slower.
    pub last_regrow_mb: Option<u64>,
    pub last_regrow_batch_ms: Option<f64>,
    pub grants_outstanding: usize,
    pub grants_mb: u64,
    /// Demand signal behind the contention split.
    pub pending_requests: usize,
    pub seed_units: u64,
    /// Doublings earned by clean windows.
    pub ramp_step: u32,
    /// Halvings currently applied by OOM / throughput-collapse deflation.
    pub deflation: u32,
    /// Consecutive clean windows since the last negative sample.
    pub clean_windows: u32,
    /// The ramp+ratchet-bounded unit budget as of this snapshot.
    pub unit_budget: u64,
    /// The throughput brake: the last clean window refused this replica its next
    /// doubling, and the rung the hold was declared on. Without them a held
    /// replica is indistinguishable from an idle one — a frozen `unit_budget`.
    pub ramp_held: bool,
    pub held_units: Option<u64>,
    /// Whether the ring certified that rung: a knee or a measured plateau is a
    /// hold on evidence, and only that kind of hold says the calibration
    /// learned where this replica stands. `false` whenever nothing is held.
    pub held_certified: bool,
    /// Ratchet anchor: largest locally measured clean priced batch.
    pub max_units_measured: u64,
    /// Throughput knee: the largest batch size worth admitting, whatever
    /// memory allows. `None` until one is fitted or seeded from a profile.
    pub knee_units: Option<u64>,
    /// Whether that knee was fitted on this machine (as opposed to seeded
    /// from a profile, which may cap but never travels back to the store).
    pub knee_is_local: bool,
    /// Shape ceiling: a batch size this model's own kernels have said they cannot
    /// execute at the shapes this corpus feeds them, reported as
    /// `clamped.reason = "index_limit"`. It caps `unit_budget` and stops the
    /// ramp, never deflates anything, and is runtime-only.
    pub shape_ceiling_units: Option<u64>,
    /// Warm-pool throughput observations behind the knee fit. Runtime-only:
    /// the store persists the fitted knee, not the series.
    pub throughput_samples: usize,
    /// Local clean fit samples behind this model's fit, including any a
    /// local calibration profile restored. Below `LOCAL_CONFIRMATION_SAMPLES`
    /// the effective margin is widened.
    pub local_samples: u32,
    /// The margin this model's windows are actually priced under: the GPU's
    /// configured margin, widened while the fit is unconfirmed or scattered.
    pub effective_margin: f64,
    pub fit: Option<FitHealth>,
}

/// The fitted cost model in `GET /health`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FitHealth {
    pub slope_mb_per_unit: f64,
    pub intercept_mb: f64,
    pub residual_mb: f64,
    pub samples: usize,
    /// The reserved/allocated ratio **this process** has observed for this
    /// (model, GPU); a grant is `slope × units × pool_margin`. Runtime-only and
    /// never persisted — the ratio does not reproduce across runs.
    pub pool_margin: f64,
}
