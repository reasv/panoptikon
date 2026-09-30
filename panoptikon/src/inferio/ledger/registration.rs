//! Worker registration: placing a load report on a ledger device, adopting
//! masked GPUs and unified totals, and the per-replica [`Admission`] handle.

use super::*;

/// The `device_kind` a worker on the CPU reports; the host places by it.
const DEVICE_KIND_CPU: &str = "cpu";

/// One log line about a registration decision, emitted after the ledger lock
/// is dropped (hence owned strings).
pub(super) enum GpuLog {
    /// The worker's PCI address matches no GPU, on an inventory with
    /// addresses.
    BdfOutsideInventory {
        worker_bdf: String,
        worker_uuid: Option<String>,
        gpus: usize,
        /// The GPU the pin named, if known.
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// The total-VRAM cross-check on a non-UUID match failed, or the worker
    /// reported no total.
    TotalDisagrees {
        matched_by: &'static str,
        gpu: String,
        gpu_bdf: Option<String>,
        gpu_total_mb: u64,
        /// A unified ROCm GPU's carve-out, the other accepted total.
        gpu_carveout_mb: Option<u64>,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
        worker_total_mb: Option<u64>,
        tolerance_mb: u64,
        /// The GPU the pin named, if known: a refused replica never reaches
        /// [`Self::PinDiverged`].
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// Nothing matched: a remote-API worker, or a GPU outside the inventory.
    NoGpu {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        gpus: usize,
    },
    /// [`Self::NoGpu`] for a worker that names a GPU, escalated to WARN once
    /// per card: every model on it runs unpriced.
    UnadmittedGpuWorker {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        gpus: usize,
        adoptable: usize,
    },
    /// [`Self::NoGpu`] for a worker that names no device at all, escalated
    /// to WARN once.
    UnadmittedDevicelessWorker { gpus: usize },
    /// A GPU hidden by an unmappable ambient mask was adopted because a load
    /// report named it by UUID.
    MaskedGpuAdopted {
        gpu: String,
        name: String,
        total_mb: u64,
        adoptable: usize,
    },
    /// A unified-memory device's total was replaced by the worker's figure.
    UnifiedTotalAdopted {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// A later replica reported a different, sane total: the new one wins.
    UnifiedTotalReadopted {
        gpu: String,
        previous_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// A reported unified total outside `(0, host RAM]`, ignored.
    UnifiedTotalRejected {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// Admitted under a different GPU than the pin named: the inventory's
    /// row order is not the backend's device order.
    PinDiverged {
        expected: String,
        expected_bdf: Option<String>,
        expected_total_mb: Option<u64>,
        resolved: String,
        resolved_bdf: Option<String>,
        resolved_total_mb: u64,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
    },
}

impl GpuLog {
    pub(super) fn emit(self, inference_id: &str) {
        match self {
            Self::BdfOutsideInventory {
                worker_bdf,
                worker_uuid,
                gpus,
                expected_gpu,
                expected_bdf,
            } => tracing::warn!(
                model = %inference_id,
                worker_bdf = %worker_bdf,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                gpus,
                expected_gpu = expected_gpu.as_deref().unwrap_or("<none>"),
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                "this worker is on a PCI address no GPU in the GPU \
                 inventory has — the inventory's row order may not be the \
                 HIP device order it is pinned by. Dispatching this model \
                 without VRAM admission rather than pricing it against a \
                 GPU it is not on"
            ),
            Self::TotalDisagrees {
                matched_by,
                gpu,
                gpu_bdf,
                gpu_total_mb,
                gpu_carveout_mb,
                worker_bdf,
                worker_uuid,
                worker_total_mb,
                tolerance_mb,
                expected_gpu,
                expected_bdf,
            } => {
                let message = if worker_total_mb.is_some() {
                    "the worker's own total-VRAM reading does not agree with \
                     the GPU it was matched to; dispatching this model \
                     without VRAM admission rather than pricing it against a \
                     GPU it may not be on"
                } else {
                    "this worker reports no total VRAM, so the GPU it was \
                     matched to cannot be cross-checked; dispatching this \
                     model without VRAM admission (only an exact UUID match \
                     is admitted without one)"
                };
                tracing::warn!(
                    model = %inference_id,
                    matched_by,
                    gpu = %gpu,
                    gpu_bdf = gpu_bdf.as_deref().unwrap_or("<none>"),
                    gpu_total_mb,
                    gpu_carveout_mb = ?gpu_carveout_mb,
                    worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                    worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                    worker_total_mb = ?worker_total_mb,
                    tolerance_mb,
                    expected_gpu = expected_gpu.as_deref().unwrap_or("<none>"),
                    expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                    "{message}"
                );
            }
            Self::NoGpu {
                worker_uuid,
                worker_bdf,
                gpus,
            } => tracing::debug!(
                model = %inference_id,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                gpus,
                "the worker reports no GPU this GPU inventory lists; \
                 dispatching this model without VRAM admission"
            ),
            Self::UnadmittedDevicelessWorker { gpus } => tracing::warn!(
                model = %inference_id,
                gpus,
                "this worker names no device at all — an impl that never \
                 imported torch (a remote API), or a worker older than the \
                 load report's device_kind field — so there is no device to \
                 place it on and it is dispatched without VRAM admission: no \
                 grants, no batch ramp and no calibration profiles, for every \
                 model it runs. A worker that names one is priced against \
                 that device, the CPU device included. Logged once"
            ),
            Self::UnadmittedGpuWorker {
                worker_uuid,
                worker_bdf,
                gpus,
                adoptable,
            } => tracing::warn!(
                model = %inference_id,
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                gpus,
                adoptable,
                "this worker runs on a GPU that is in neither this host's GPU \
                 inventory nor the rows an ambient device mask hid, so it is \
                 dispatched without VRAM admission: no grants, no batch ramp \
                 and no calibration profiles, for every model on that GPU. \
                 Name the GPU by UUID in CUDA_VISIBLE_DEVICES (nvidia-smi -L \
                 lists them) or unset the variable; an index-form mask needs \
                 no change — the ledger adopts the GPU the first load report \
                 names. Logged once per GPU"
            ),
            Self::MaskedGpuAdopted {
                gpu,
                name,
                total_mb,
                adoptable,
            } => tracing::info!(
                model = %inference_id,
                gpu = %gpu,
                gpu_name = %name,
                total_mb,
                adoptable,
                "adopting the GPU this worker reports into the ledger: an \
                 ambient device mask left the inventory unknown, and the load \
                 report names by UUID which GPU this host actually runs on. \
                 VRAM admission applies to it from now on"
            ),
            Self::UnifiedTotalAdopted {
                gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                gpu = %gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
                "this unified-memory device's admission total is now the figure the \
                 worker's own runtime reports, which is what its allocations \
                 are actually judged against; the probe's seed was a default \
                 fraction of host RAM and a raised GPU memory limit moves the \
                 real figure well away from it"
            ),
            Self::UnifiedTotalReadopted {
                gpu,
                previous_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::info!(
                model = %inference_id,
                gpu = %gpu,
                previous_total_mb,
                reported_total_mb,
                ram_mb,
                "unified total re-adopted: this worker reports a different \
                 figure than the one already in force, which is what raising \
                 (or lowering) the GPU memory limit under a running gateway \
                 looks like. Taking the new figure — refusing the replica for \
                 disagreeing would leave a tuned machine unpriced until a \
                 restart"
            ),
            Self::UnifiedTotalRejected {
                gpu,
                seed_total_mb,
                reported_total_mb,
                ram_mb,
            } => tracing::warn!(
                model = %inference_id,
                gpu = %gpu,
                total_mb = seed_total_mb,
                reported_total_mb,
                ram_mb,
                "ignoring this worker's total-memory report for a unified \
                 GPU: it is not inside (0, host RAM], so it cannot be this \
                 GPU's share of the machine's memory — keeping the total \
                 already in force"
            ),
            Self::PinDiverged {
                expected,
                expected_bdf,
                expected_total_mb,
                resolved,
                resolved_bdf,
                resolved_total_mb,
                worker_bdf,
                worker_uuid,
            } => tracing::warn!(
                model = %inference_id,
                expected_gpu = %expected,
                expected_bdf = expected_bdf.as_deref().unwrap_or("<none>"),
                expected_total_mb = ?expected_total_mb,
                resolved_gpu = %resolved,
                resolved_bdf = resolved_bdf.as_deref().unwrap_or("<none>"),
                resolved_total_mb,
                worker_bdf = worker_bdf.as_deref().unwrap_or("<none>"),
                worker_uuid = worker_uuid.as_deref().unwrap_or("<none>"),
                "this replica was pinned to one GPU and came up on another: \
                 the GPU-row order the pin was derived from is not the \
                 device order the backend enumerated. Admitting it under the \
                 GPU it is actually on (which is the correct pricing), but \
                 its *load* reservation was taken against the GPU the pin \
                 named and therefore protected the wrong card"
            ),
        }
    }
}

impl VramLedger {
    /// Which ledger device a load report belongs to, plus the line to log.
    /// In order: `device_kind = cpu`; a UUID match; a PCI address match that
    /// passes the total cross-check; the only accelerator (the CPU device on
    /// a host with none), for a report that claims a GPU but no UUID, with no
    /// adoptable GPU left, again through the total cross-check. A PCI address
    /// matching no row of an inventory with addresses is refused. Anything
    /// else is unpriced. `expected_gpu` (the pin) is diagnostic only.
    pub(super) fn resolve_gpu(
        state: &LedgerState,
        report: &LoadReport,
        expected_gpu: Option<&str>,
    ) -> GpuResolution {
        // No total cross-check for the CPU device: under a cgroup limit the
        // host and the worker read RAM differently.
        if report.device_kind.as_deref() == Some(DEVICE_KIND_CPU)
            && let Some(gpu) = state.gpus.get(super::cpu::DEVICE_KEY)
        {
            return GpuResolution {
                admit: Some((super::cpu::DEVICE_KEY.to_owned(), gpu.name.clone())),
                log: None,
            };
        }
        if let Some(uuid) = report.gpu_uuid.as_deref()
            && let Some(gpu) = state.gpus.get(uuid)
        {
            return Self::admit_gpu(state, uuid, gpu, report, expected_gpu);
        }
        let inventory_has_bdfs = state.accelerators().any(|(_, gpu)| gpu.bdf.is_some());
        if let Some(bdf) = report.gpu_bdf.as_deref() {
            let wanted = bdf.to_ascii_lowercase();
            let matched = state
                .accelerators()
                .find(|(_, gpu)| gpu.bdf.as_deref() == Some(wanted.as_str()));
            if let Some((key, gpu)) = matched {
                return match Self::cross_check_total(
                    state,
                    report,
                    key,
                    gpu,
                    "the PCI address the worker reports",
                    expected_gpu,
                ) {
                    None => Self::admit_gpu(state, key, gpu, report, expected_gpu),
                    Some(log) => GpuResolution::refused(log),
                };
            }
            if inventory_has_bdfs {
                return GpuResolution::refused(GpuLog::BdfOutsideInventory {
                    worker_bdf: bdf.to_owned(),
                    worker_uuid: report.gpu_uuid.clone(),
                    gpus: state.accelerators().count(),
                    expected_gpu: expected_gpu.map(str::to_owned),
                    expected_bdf: Self::gpu_bdf(state, expected_gpu),
                });
            }
        }
        let claims_a_gpu = report.gpu_bdf.is_some() || report.gpu_total_mb.is_some();
        // The only accelerator, or on a host with none the CPU device.
        let accelerators: Vec<(&String, &GpuLedger)> = state.accelerators().collect();
        let only = match accelerators.as_slice() {
            [(key, gpu)] => Some((*key, *gpu)),
            [] => state.gpus.get_key_value(super::cpu::DEVICE_KEY),
            _ => None,
        };
        // With adoptable GPUs left, "the only GPU" is not a host fact.
        if let Some((key, gpu)) = only
            && state.adoptable.is_empty()
            && claims_a_gpu
            && report.gpu_uuid.is_none()
        {
            return match Self::cross_check_total(
                state,
                report,
                key,
                gpu,
                "this host's only GPU",
                expected_gpu,
            ) {
                None => GpuResolution {
                    admit: Some((key.clone(), gpu.name.clone())),
                    log: None,
                },
                Some(log) => GpuResolution::refused(log),
            };
        }
        GpuResolution::refused(GpuLog::NoGpu {
            worker_uuid: report.gpu_uuid.clone(),
            worker_bdf: report.gpu_bdf.clone(),
            gpus: accelerators.len(),
        })
    }

    /// Admit under `key`, with [`GpuLog::PinDiverged`] if the pin named
    /// another GPU.
    fn admit_gpu(
        state: &LedgerState,
        key: &str,
        gpu: &GpuLedger,
        report: &LoadReport,
        expected_gpu: Option<&str>,
    ) -> GpuResolution {
        let log = expected_gpu
            .filter(|expected| *expected != key)
            .map(|expected| GpuLog::PinDiverged {
                expected: expected.to_owned(),
                expected_bdf: state.gpus.get(expected).and_then(|row| row.bdf.clone()),
                expected_total_mb: state.gpus.get(expected).map(|row| row.total_mb),
                resolved: key.to_owned(),
                resolved_bdf: gpu.bdf.clone(),
                resolved_total_mb: gpu.total_mb,
                worker_bdf: report.gpu_bdf.clone(),
                worker_uuid: report.gpu_uuid.clone(),
            });
        GpuResolution {
            admit: Some((key.to_owned(), gpu.name.clone())),
            log,
        }
    }

    /// `None` when the worker's total agrees with the GPU's
    /// ([`totals_agree`]), or on a unified ROCm GPU with its carve-out;
    /// otherwise the refusal. An absent or zero total refuses.
    fn cross_check_total(
        state: &LedgerState,
        report: &LoadReport,
        key: &str,
        gpu: &GpuLedger,
        matched_by: &'static str,
        expected_gpu: Option<&str>,
    ) -> Option<GpuLog> {
        let tolerance = total_tolerance_mb(gpu.total_mb);
        if let Some(total) = report.gpu_total_mb.filter(|total| *total > 0) {
            let agrees = |figure: u64| totals_agree(figure, total);
            if agrees(gpu.total_mb) || gpu.vram_carveout_mb.is_some_and(agrees) {
                return None;
            }
        }
        Some(GpuLog::TotalDisagrees {
            matched_by,
            gpu: key.to_owned(),
            gpu_bdf: gpu.bdf.clone(),
            gpu_total_mb: gpu.total_mb,
            gpu_carveout_mb: gpu.vram_carveout_mb,
            worker_bdf: report.gpu_bdf.clone(),
            worker_uuid: report.gpu_uuid.clone(),
            worker_total_mb: report.gpu_total_mb,
            tolerance_mb: tolerance,
            expected_gpu: expected_gpu.map(str::to_owned),
            expected_bdf: Self::gpu_bdf(state, expected_gpu),
        })
    }

    /// The PCI address of a device key, when the ledger holds one for it.
    fn gpu_bdf(state: &LedgerState, key: Option<&str>) -> Option<String> {
        state.gpus.get(key?).and_then(|gpu| gpu.bdf.clone())
    }

    /// Adopt a unified-memory device's total from the worker's report (on
    /// Apple Silicon, Metal's `recommendedMaxWorkingSetSize`, which moves with
    /// the wired limit). Accepted if `0 < reported ≤ host RAM`; a later figure
    /// out of tolerance re-adopts. Only for a single unified accelerator with
    /// no PCI address and a report naming no GPU. Runs before
    /// [`Self::resolve_gpu`].
    fn adopt_unified_total_locked(state: &mut LedgerState, report: &LoadReport) -> Option<GpuLog> {
        if !state.adopts_worker_total {
            return None;
        }
        let reported = report.gpu_total_mb?;
        if report.gpu_uuid.is_some() || report.gpu_bdf.is_some() {
            return None;
        }
        if report.device_kind.as_deref() == Some(DEVICE_KIND_CPU) {
            return None;
        }
        if state.accelerators().count() != 1 {
            return None;
        }
        let (key, gpu) = state
            .gpus
            .iter_mut()
            .find(|(key, _)| key.as_str() != super::cpu::DEVICE_KEY)
            .expect("one accelerator, just counted");
        if gpu.bdf.is_some() {
            return None;
        }
        let ram_mb = gpu.unified_ram_mb?;
        let previous_total_mb = gpu.total_mb;
        if reported == 0 || reported > ram_mb {
            return Some(GpuLog::UnifiedTotalRejected {
                gpu: key.clone(),
                seed_total_mb: previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        if gpu.total_adopted {
            if totals_agree(previous_total_mb, reported) {
                return None;
            }
            gpu.total_mb = reported;
            return Some(GpuLog::UnifiedTotalReadopted {
                gpu: key.clone(),
                previous_total_mb,
                reported_total_mb: reported,
                ram_mb,
            });
        }
        gpu.total_mb = reported;
        gpu.total_adopted = true;
        Some(GpuLog::UnifiedTotalAdopted {
            gpu: key.clone(),
            seed_total_mb: previous_total_mb,
            reported_total_mb: reported,
            ram_mb,
        })
    }

    /// Move the GPU this report names by UUID from the adoptable set (hidden
    /// by an unmappable `CUDA_VISIBLE_DEVICES`) into the ledger and the
    /// inventory. Runs before [`Self::resolve_gpu`].
    fn adopt_masked_gpu_locked(state: &mut LedgerState, report: &LoadReport) -> Option<GpuLog> {
        let uuid = report.gpu_uuid.as_deref()?;
        if state.gpus.contains_key(uuid) {
            return None;
        }
        let gpu = state.adoptable.remove(uuid)?;
        let (name, total_mb) = (gpu.name.clone(), gpu.total_mb);
        state.gpus.insert(uuid.to_owned(), gpu);
        state.inventory.adopt(uuid);
        Some(GpuLog::MaskedGpuAdopted {
            gpu: uuid.to_owned(),
            name,
            total_mb,
            adoptable: state.adoptable.len(),
        })
    }

    /// The shared [`LedgerState::unpriced_warned`] slot for workers that
    /// report no device.
    const NO_DEVICE_REPORTED: &str = "<no device>";

    /// Escalate an unpriced refusal to WARN once per reported GPU, and once
    /// for workers naming no device. Other refusals stay at DEBUG.
    pub(super) fn escalate_first_unpriced(
        state: &mut LedgerState,
        resolution: GpuResolution,
        report: &LoadReport,
    ) -> GpuResolution {
        let names_a_gpu =
            report.gpu_uuid.is_some() || report.gpu_bdf.is_some() || report.gpu_total_mb.is_some();
        let GpuResolution {
            admit: None,
            log: Some(GpuLog::NoGpu { .. }),
        } = &resolution
        else {
            return resolution;
        };
        if !names_a_gpu {
            if report.device_kind.is_some()
                || !state
                    .unpriced_warned
                    .insert(Self::NO_DEVICE_REPORTED.to_owned())
            {
                return resolution;
            }
            return GpuResolution::refused(GpuLog::UnadmittedDevicelessWorker {
                gpus: state.gpus.len(),
            });
        }
        let card = report
            .gpu_uuid
            .clone()
            .or_else(|| report.gpu_bdf.clone())
            .unwrap_or_else(|| "<unidentified>".to_owned());
        if !state.unpriced_warned.insert(card) {
            return resolution;
        }
        GpuResolution::refused(GpuLog::UnadmittedGpuWorker {
            worker_uuid: report.gpu_uuid.clone(),
            worker_bdf: report.gpu_bdf.clone(),
            gpus: state.gpus.len(),
            adoptable: state.adoptable.len(),
        })
    }

    /// Register a freshly loaded replica, or `None` when it takes the unpriced
    /// path (a `none`-class model, or no device the ledger knows). The device
    /// is the one the worker reported ([`Self::resolve_gpu`]); `expected_gpu`
    /// is diagnostic only.
    pub fn register_worker(
        self: &Arc<Self>,
        inference_id: &str,
        cost: CostDimension,
        telemetry: &TelemetryHandle,
        expected_gpu: Option<&str>,
    ) -> Option<Admission> {
        let aggregation = cost.aggregation?;
        if !cost.scales() {
            return None;
        }
        let seed_units = u64::from(cost.seed_units.unwrap_or(1)).max(1);
        let stamped = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            telemetry.load.clone()
        }?;
        let loaded_at = stamped.captured_at;
        let report = stamped.value;
        let (adoption, masked, resolution) = {
            let mut state = self.lock();
            let adoption = Self::adopt_unified_total_locked(&mut state, &report);
            let masked = Self::adopt_masked_gpu_locked(&mut state, &report);
            let resolution = Self::resolve_gpu(&state, &report, expected_gpu);
            let resolution = Self::escalate_first_unpriced(&mut state, resolution, &report);
            (adoption, masked, resolution)
        };
        // Logged with the lock dropped, and before the `?` so refusals log.
        for log in adoption.into_iter().chain(masked).chain(resolution.log) {
            log.emit(inference_id);
        }
        let (gpu, gpu_name) = resolution.admit?;
        // The card's architecture (the profile key): first answer wins.
        let (gpu_arch, arch_disagreement) = {
            let mut state = self.lock();
            let reported = report.gpu_arch.clone().filter(|arch| !arch.is_empty());
            let entry = state.gpus.get_mut(&gpu)?;
            if entry.arch.is_none() {
                entry.arch = reported.clone();
            }
            let arch = entry.arch.clone();
            let mut disagreement = None;
            if let (Some(seeded), Some(said)) = (arch.as_deref(), reported.as_deref())
                && seeded != said
                && state.arch_mismatch_logged.insert(gpu.clone())
            {
                disagreement = Some((seeded.to_owned(), said.to_owned()));
            }
            (arch, disagreement)
        };
        if let Some((seeded, said)) = arch_disagreement {
            tracing::warn!(
                gpu = %gpu,
                seeded_arch = %seeded,
                reported_arch = %said,
                "this host derived GPU architecture {seeded} where the worker reports {said}; \
                 calibration profiles key on {seeded}, so with HSA_OVERRIDE_GFX_VERSION set the \
                 overridden kernels' measurements land in the physical target's entry"
            );
        }
        // Outside the ledger lock: the store may read files.
        let seed = self
            .profiles
            .as_ref()
            .zip(gpu_arch.as_deref())
            .and_then(|(profiles, arch)| {
                profiles.lookup(&ProfileQuery {
                    inference_id,
                    epoch: cost.epoch,
                    arch,
                    unit: cost.unit.as_str(),
                    aggregation: aggregation.as_str(),
                    torch: report.torch_version.as_deref(),
                    dtype: report.dtype.as_deref(),
                })
            });
        let mut state = self.lock();
        let key = (inference_id.to_owned(), gpu.clone());
        // A load reporting no base never erases a known one.
        let known_base = state.remembered_bases.get(&key).copied().flatten();
        if report.base_mb.is_some() || known_base.is_none() {
            state.remembered_bases.insert(key.clone(), report.base_mb);
        }
        if let Some(dtype) = report.dtype.clone() {
            state.remembered_dtypes.insert(key.clone(), dtype);
        }
        // The load report's memory sample may be the GPU's only reading yet.
        if let Some(sample) = report.memory.as_ref()
            && let (Some(free), Some(source)) = (sample.free_mb, sample.free_source.clone())
        {
            Self::record_free_locked(
                &mut state,
                &gpu,
                free,
                source,
                loaded_at,
                sample.total_mb,
                Some(inference_id),
                RamBasis::of(sample),
            );
        }
        let seeded_from_store = seed.is_some();
        Self::seed_calibration_locked(
            &mut state,
            &key,
            self.profiles.is_some(),
            seed,
            inference_id,
            &gpu,
        );
        // Host RAM is booked on the CPU device for a replica on a GPU with its
        // own memory; a unified device (the CPU device, MPS, an APU) is priced
        // in RAM already.
        let ram_at_load_mb = report.rss_at_load_mb.filter(|_| {
            state.gpus.contains_key(cpu::DEVICE_KEY)
                && state
                    .gpus
                    .get(&gpu)
                    .is_some_and(|device| device.unified_ram_mb.is_none())
        });
        if let Some(rss) = ram_at_load_mb {
            Self::shift_free_locked(&mut state, cpu::DEVICE_KEY, 0, rss, loaded_at);
        }
        let id = state.next_id();
        let logged_gpu = gpu.clone();
        state.workers.insert(
            id,
            WorkerEntry {
                inference_id: inference_id.to_owned(),
                gpu,
                gpu_name,
                gpu_arch,
                loaded_at,
                telemetry: Arc::clone(telemetry),
                unit: cost.unit,
                aggregation,
                epoch: cost.epoch,
                degraded: cost.degraded,
                canvas_pixels: cost.canvas_pixels,
                max_tokens: cost.max_tokens,
                torch: report.torch_version.clone(),
                dtype: report.dtype.clone(),
                dtype_method: report.dtype_method.clone(),
                base_method: report.base_method.clone(),
                seed_units,
                base_mb: report.base_mb,
                base_recorded: report.base_mb.is_some(),
                reserved_at_load_mb: report.reserved_at_load_mb,
                allocated_at_load_mb: report.allocated_at_load_mb,
                reserved_mb: report.reserved_at_load_mb,
                reserved_seen_at: None,
                grants: HashMap::new(),
                pending_requests: 0,
                ramp_step: 0,
                ramp_held: false,
                held_units: None,
                held_certified: false,
                oom_at_floor: 0,
                windows_queue_bound: 0,
                hold_announced: false,
                hold_reprobe_windows: 0,
                deflation: 0,
                deflation_repaid_at: None,
                clean_windows: 0,
                settled_windows: 0,
                ran_batches: 0,
                fit_watermark: 0,
                fit_version_sent: 0,
                last_trim_at: None,
                last_grant_settled_at: None,
                alloc_retries_last_window: None,
                alloc_retries_total: None,
                idle_release_gave_nothing: false,
                pool_releases: None,
                last_release_mb: None,
                last_release_ms: None,
                last_regrow_mb: None,
                last_regrow_batch_ms: None,
                ram_at_load_mb,
                ram_base_mb: ram_at_load_mb,
                ram_mb: None,
                ram_bound: false,
                item_cap: ram_at_load_mb.map(|_| 1),
            },
        );
        drop(state);
        tracing::debug!(
            model = %inference_id,
            gpu = %logged_gpu,
            replica = id,
            base_mb = ?report.base_mb,
            base_method = report.base_method.as_deref().unwrap_or("<none>"),
            reserved_at_load_mb = ?report.reserved_at_load_mb,
            seeded_from_store,
            "admitted a worker to a GPU's ledger"
        );
        Some(Admission {
            ledger: Arc::clone(self),
            worker: id,
        })
    }

    /// Forget a replica and its grants ([`Admission`]'s `Drop`). Its
    /// footprint is credited back to the GPU's free reading, and a GPU
    /// replica's resident set to the CPU device's, unless that reading
    /// predates its load, so it is not reattributed to external usage; the
    /// departure is stamped so the next grant request refreshes the reading
    /// ([`super::external_memory::refresh_due`]) and older samples are
    /// refused.
    fn forget_worker(&self, worker: WorkerId) {
        let mut state = self.lock();
        let Some(entry) = state.workers.remove(&worker) else {
            return;
        };
        let departed = [
            (entry.gpu.clone(), entry.footprint_mb()),
            (cpu::DEVICE_KEY.to_owned(), entry.ram_resident_mb()),
        ];
        let mut credits = Vec::new();
        for (device, footprint_mb) in departed {
            if footprint_mb == 0 {
                continue;
            }
            let Some(gpu) = state.gpus.get_mut(&device) else {
                continue;
            };
            let total_mb = gpu.total_mb;
            let Some(sample) = gpu.free.as_mut() else {
                continue;
            };
            let credited = sample.at >= entry.loaded_at;
            // Capped at the total; that only binds where `external` is already 0.
            if credited {
                sample.free_mb = sample.free_mb.saturating_add(footprint_mb).min(total_mb);
            }
            let adjusted_free_mb = sample.free_mb;
            gpu.free_adjusted_at = Some(Instant::now());
            credits.push((device, footprint_mb, adjusted_free_mb, credited));
        }
        drop(state);
        let model = entry.inference_id;
        for (gpu, footprint_mb, adjusted_free_mb, credited) in credits {
            if credited {
                tracing::debug!(
                    model = %model,
                    gpu = %gpu,
                    footprint_mb,
                    adjusted_free_mb,
                    "credited a departed replica's footprint back to the GPU's \
                     free reading, so its memory is not reattributed to external \
                     usage, and flagged the reading for a refresh"
                );
            } else {
                tracing::debug!(
                    model = %model,
                    gpu = %gpu,
                    footprint_mb,
                    free_mb = adjusted_free_mb,
                    "a replica departed a GPU whose freshest free reading predates \
                     its load, so there is no footprint in that reading to credit \
                     back; leaving it as it stands — external usage reads high until \
                     the refresh this flagged settles it"
                );
            }
        }
    }
}

/// One replica's handle into the ledger, for sizing windows and obtaining
/// grants. Dropping it forgets the replica.
pub struct Admission {
    ledger: Arc<VramLedger>,
    worker: WorkerId,
}

impl Admission {
    /// This replica's ledger id, as a [`TrimRequest`] names it.
    pub fn worker_id(&self) -> u64 {
        self.worker
    }

    /// The GPU this replica was admitted to; `None` once forgotten.
    pub fn gpu(&self) -> Option<String> {
        self.ledger
            .lock()
            .workers
            .get(&self.worker)
            .map(|entry| entry.gpu.clone())
    }

    /// Record a `trim` answer ([`VramLedger::note_trimmed`]).
    pub fn note_trimmed(&self, reply: TrimReply) {
        self.ledger.note_trimmed(self.worker, reply);
    }

    /// Record that this replica declined a `trim`; the debounce applies as for
    /// a release.
    pub fn note_trim_declined(&self) {
        self.ledger.note_trim_declined(self.worker);
    }

    /// Units to aim for in the next window (see [`WINDOW_DEPTH_MULTIPLIER`]).
    pub fn window_target_units(&self) -> u64 {
        self.ledger.window_target_units(self.worker)
    }

    /// Items the next window may hold ([`VramLedger::window_item_bound`]).
    pub fn window_item_bound(&self) -> usize {
        self.ledger.window_item_bound(self.worker)
    }

    /// [`Self::request_grant_byte_bound`] with `byte_bound = false`.
    #[cfg(test)]
    pub fn request_grant(
        &self,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
    ) -> Option<GrantToken> {
        self.request_grant_byte_bound(
            window_units,
            user_cap_items,
            window_requests,
            queued_behind,
            false,
        )
    }

    /// Reserve headroom for one window. Demand is `window_requests +
    /// queued_behind`; the window's own requests retire when it settles.
    /// `byte_bound`: the byte limit, not the queue, closed the window.
    pub fn request_grant_byte_bound(
        &self,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
        byte_bound: bool,
    ) -> Option<GrantToken> {
        self.ledger.request_grant(
            self.worker,
            window_units,
            user_cap_items,
            window_requests,
            queued_behind,
            byte_bound,
        )
    }

    /// The fit snapshot to ride the next request frame, if it moved.
    pub fn fit_to_send(&self) -> Option<FitSnapshot> {
        self.ledger.fit_to_send(self.worker)
    }

    /// Update the demand signal (0 when the queue drains).
    pub fn note_demand(&self, pending: usize) {
        let mut state = self.ledger.lock();
        if let Some(entry) = state.workers.get_mut(&self.worker) {
            entry.pending_requests = pending;
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.ledger.forget_worker(self.worker);
    }
}
