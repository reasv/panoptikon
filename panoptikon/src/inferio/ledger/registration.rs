use super::*;

/// The `device_kind` a worker reports when it ran on the CPU — the one value
/// the host places a replica by rather than merely recording.
const DEVICE_KIND_CPU: &str = "cpu";

/// One line about a registration decision, emitted after the ledger lock is
/// dropped. Every variant owns its strings for exactly that reason.
pub(super) enum GpuLog {
    /// The worker's PCI address matches no GPU, on an inventory whose rows
    /// *do* carry addresses.
    BdfOutsideInventory {
        worker_bdf: String,
        worker_uuid: Option<String>,
        gpus: usize,
        /// The GPU the *pin* believed this replica was on, when the caller
        /// knew it — see [`Self::TotalDisagrees`] for why a refusal needs it.
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// The total-VRAM cross-check that guards a non-UUID match failed: the two
    /// totals disagree, or the worker reported no total to check against.
    TotalDisagrees {
        matched_by: &'static str,
        gpu: String,
        gpu_bdf: Option<String>,
        gpu_total_mb: u64,
        /// The other figure a unified ROCm GPU's total was allowed to match (its
        /// carve-out); `None` on every discrete GPU. Named in the refusal so a
        /// field report shows both candidates.
        gpu_carveout_mb: Option<u64>,
        worker_bdf: Option<String>,
        worker_uuid: Option<String>,
        worker_total_mb: Option<u64>,
        tolerance_mb: u64,
        /// The GPU the orchestrator's *pin* named for this replica, when the
        /// caller knew it, and that GPU's PCI address. Carried on a **refusal**
        /// because the cross-check runs before admission, so a replica on a
        /// mis-ordered enumeration never reaches [`Self::PinDiverged`] and the
        /// "the pin believed GPU A" half of the alarm would be missing.
        expected_gpu: Option<String>,
        expected_bdf: Option<String>,
    },
    /// Nothing matched and no fallback applied — the ordinary CPU/remote-API
    /// worker, and the GPU-outside-the-inventory case.
    NoGpu {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        gpus: usize,
    },
    /// [`Self::NoGpu`] for a worker that *does* name a GPU: the first one this
    /// process refuses **for that card**, escalated to WARN with the remedy,
    /// since it means every model on that GPU runs unpriced for the life of
    /// the process.
    UnadmittedGpuWorker {
        worker_uuid: Option<String>,
        worker_bdf: Option<String>,
        gpus: usize,
        adoptable: usize,
    },
    /// [`Self::NoGpu`] for a worker that names **no device at all** — no
    /// `device_kind`, no identity, no total. Nothing can place it, so every
    /// model it runs is unpriced for the life of the process; escalated to
    /// WARN for the same reason as [`Self::UnadmittedGpuWorker`].
    UnadmittedDevicelessWorker { gpus: usize },
    /// A GPU an unmappable ambient mask hid was adopted into the ledger
    /// because a worker's load report named it by UUID.
    MaskedGpuAdopted {
        gpu: String,
        name: String,
        total_mb: u64,
        adoptable: usize,
    },
    /// A unified-memory device's admission total was replaced by the figure the
    /// worker's own runtime reports.
    UnifiedTotalAdopted {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// A later replica reported a *different* — and sane — total for an
    /// already-adopted unified-memory device: the memory limit moved under a
    /// running gateway. The new figure wins rather than refusing every replica.
    UnifiedTotalReadopted {
        gpu: String,
        previous_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The same report, refused: outside `(0, host RAM]`, so it describes
    /// something other than this GPU's budget. The total in force stands.
    UnifiedTotalRejected {
        gpu: String,
        seed_total_mb: u64,
        reported_total_mb: u64,
        ram_mb: u64,
    },
    /// The replica was admitted, but under a **different** GPU than the one the
    /// pin believed: the enumeration-order diagnostic. Not a refusal — the
    /// replica is physically on the resolved GPU — but the one signal that the
    /// row order the pin came from is not the backend's device order.
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
    // ------------------------------------------------------------------
    // Worker registration
    // ------------------------------------------------------------------

    /// Which ledger GPU a load report belongs to — plus the line to log about
    /// it, which the caller emits once the lock is dropped.
    ///
    /// The report carries up to three independent facts about the GPU — a UUID,
    /// a PCI address, and torch's own total-memory figure — and the arms are
    /// ordered by how much each can be trusted to *identify*:
    ///
    /// 1. **UUID matching a GPU**, with no memory check: NVML UUIDs are unique
    ///    and byte-identical on both sides, so a match is proof.
    /// 2. **PCI address matching a GPU's**, which is only as good as the
    ///    assumption that the inventory's row order is the backend's device
    ///    order, so it must survive a cross-check against the worker's
    ///    `gpu_total_mb` ([`total_tolerance_mb`]). No total at all refuses.
    /// 3. **The single-GPU fallback**, when nothing matched, the host has one
    ///    GPU and no adoptable one left, the report says *something* about a
    ///    GPU, no BDF could have matched, and the worker reported **no UUID at
    ///    all**.
    ///
    /// Everything else falls to unpriced dispatch, including — deliberately — a
    /// BDF matching no row on a host whose rows *do* carry addresses.
    ///
    /// `expected_gpu` is the device key the pin named, a **diagnostic input
    /// only**: a divergence raises [`GpuLog::PinDiverged`], and the replica is
    /// still admitted under the *resolved* GPU, where it physically is.
    pub(super) fn resolve_gpu(
        state: &LedgerState,
        report: &LoadReport,
        expected_gpu: Option<&str>,
    ) -> GpuResolution {
        // The CPU device, ahead of every accelerator arm: a worker that says
        // it ran on the CPU belongs to it whatever accelerator this host
        // resolved for *itself*, and there is exactly one such device to place
        // it on. No total cross-check — the reported kind is the
        // identification, and under a cgroup limit the two sides read RAM in
        // different namespaces (the host's total is the limit, the worker's
        // psutil figure the machine's).
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
        // A report with nothing to say about a GPU at all is the CPU/MPS/
        // remote-API worker, not a failed identification: it falls to the debug
        // line below rather than through a check it was never a candidate for,
        // which would warn on every CPU model this host loads.
        let claims_a_gpu = report.gpu_bdf.is_some() || report.gpu_total_mb.is_some();
        // The one device this report could be about: this host's only
        // accelerator, or — on a host that has none — the CPU device, which is
        // how a worker too old to send `device_kind` is admitted on a CPU-only
        // host, exactly as it was before that field existed.
        let accelerators: Vec<(&String, &GpuLedger)> = state.accelerators().collect();
        let only = match accelerators.as_slice() {
            [(key, gpu)] => Some((*key, *gpu)),
            [] => state.gpus.get_key_value(super::cpu::DEVICE_KEY),
            _ => None,
        };
        // A non-empty `adoptable` means a mask hid cards this host reported,
        // so "the only GPU" is a fact about the ledger, not about the host:
        // two identical cards pass the total cross-check by construction.
        if let Some((key, gpu)) = only
            && state.adoptable.is_empty()
            && claims_a_gpu
            && report.gpu_uuid.is_none()
        {
            // No divergence check here, and none is possible: with one GPU in
            // the ledger, an `expected_gpu` from the same inventory is that GPU.
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

    /// Admit under `key`, and raise the mis-order alarm when the orchestrator
    /// believed it had pinned this replica somewhere else. Admission is under
    /// the **resolved** GPU either way — the replica is physically there — and
    /// the alarm is the field diagnostic; the pin's own *load reservation* was
    /// already taken against the believed GPU and stays there.
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

    /// `None` when the worker's own total-memory reading agrees with the GPU it
    /// is about to be admitted under ([`total_tolerance_mb`]); otherwise the
    /// refusal's log line. An **absent** total fails: this check is the only
    /// evidence that a non-UUID identification is the right GPU at all, and the
    /// cost of a false refusal is one unpriced replica against every grant on
    /// that GPU. On a **unified ROCm GPU** a report matching *either* the
    /// admission total or the BIOS carve-out is accepted, HIP's APU
    /// `total_memory` being unverified (docs/unified-memory-admission.md).
    fn cross_check_total(
        state: &LedgerState,
        report: &LoadReport,
        key: &str,
        gpu: &GpuLedger,
        matched_by: &'static str,
        expected_gpu: Option<&str>,
    ) -> Option<GpuLog> {
        let tolerance = total_tolerance_mb(gpu.total_mb);
        // A reported **zero** is refused on every GPU: it is the shape of a
        // driver that answered without knowing, and on a small enough figure a
        // tolerance window would otherwise reach down to it.
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
            // The pin's belief travels with the refusal: on a host of unequal
            // GPUs a mis-ordered enumeration is refused here and never reaches
            // `PinDiverged`, so without this the loudest evidence of a wrong
            // row order would name only the GPU the worker turned out to be on.
            expected_gpu: expected_gpu.map(str::to_owned),
            expected_bdf: Self::gpu_bdf(state, expected_gpu),
        })
    }

    /// The PCI address of a device key, when the ledger holds one for it.
    fn gpu_bdf(state: &LedgerState, key: Option<&str>) -> Option<String> {
        state.gpus.get(key?).and_then(|gpu| gpu.bdf.clone())
    }

    /// Adopt a unified-memory device's **authoritative** total from the first
    /// load report that carries one, and say so.
    ///
    /// On such a device `total` is a *policy* number, not a device fact — on
    /// Apple Silicon it is Metal's `recommendedMaxWorkingSetSize`, which moves
    /// when the user raises the GPU wired limit — and only the worker can read
    /// the moved figure, so the worker's number wins outright. The check is a
    /// **sanity bound and nothing else**: `0 < reported ≤ host RAM`. A later
    /// sane figure out of tolerance **re-adopts**, the wired limit being a live
    /// sysctl; one inside tolerance changes nothing.
    ///
    /// It runs **before** [`Self::resolve_gpu`], which is the whole reason it is
    /// a separate step: the same report is then cross-checked against the total
    /// it just supplied. Scoped as tightly as the facts allow — one GPU in the
    /// ledger, unified, carrying **no PCI address**, and a report naming **no
    /// other GPU** — because a unified ROCm GPU's HIP total may be its BIOS
    /// carve-out, and a CPU device's total is physical RAM the kernel already
    /// reported (`GpuInventory::adopts_worker_total`).
    fn adopt_unified_total_locked(state: &mut LedgerState, report: &LoadReport) -> Option<GpuLog> {
        if !state.adopts_worker_total {
            return None;
        }
        let reported = report.gpu_total_mb?;
        if report.gpu_uuid.is_some() || report.gpu_bdf.is_some() {
            return None;
        }
        // A CPU replica's total is RAM, which says nothing about the Metal
        // device it is running beside.
        if report.device_kind.as_deref() == Some(DEVICE_KIND_CPU) {
            return None;
        }
        // The one **accelerator**, not the one device: every host also carries
        // the CPU device, whose total is physical RAM the kernel reported.
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

    /// Move the GPU this load report names out of the adoptable set and into
    /// the ledger, and into the inventory's adopted set with it. An ambient `CUDA_VISIBLE_DEVICES` we could not map left the
    /// inventory unknown, but nvidia-smi's rows were kept: the report's UUID
    /// says which of them this replica is on, which is exactly the index->GPU
    /// mapping no static rule can make. Runs **before** [`Self::resolve_gpu`],
    /// whose UUID arm then matches.
    fn adopt_masked_gpu_locked(state: &mut LedgerState, report: &LoadReport) -> Option<GpuLog> {
        let uuid = report.gpu_uuid.as_deref()?;
        if state.gpus.contains_key(uuid) {
            return None;
        }
        let gpu = state.adoptable.remove(uuid)?;
        let (name, total_mb) = (gpu.name.clone(), gpu.total_mb);
        state.gpus.insert(uuid.to_owned(), gpu);
        // The same event on the inventory side, so `/metadata`'s calibration
        // overlay and `/health`'s `gpus[]` name the card the ledger is now
        // writing profiles for.
        state.inventory.adopt(uuid);
        Some(GpuLog::MaskedGpuAdopted {
            gpu: uuid.to_owned(),
            name,
            total_mb,
            adoptable: state.adoptable.len(),
        })
    }

    /// The [`LedgerState::unpriced_warned`] slot for the workers that report no
    /// device at all: they cannot be told apart, and the remedy is one host
    /// fact, so they share one. No UUID or PCI address can collide with it.
    const NO_DEVICE_REPORTED: &str = "<no device>";

    /// Say once **per reported GPU**, at WARN, that a worker on it is running
    /// unpriced — a respawn on that card is silent, a second card is not. The
    /// refusal itself is a DEBUG line because it also covers every CPU, MPS
    /// and remote-API replica, which are not failed identifications; a report
    /// that names a GPU is one, and it costs that GPU the whole feature.
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
            // A worker that *named* a device kind was placeable in principle
            // and its refusal is an ordinary one. One that names none at all —
            // no torch, or a worker too old for the field — can match nothing
            // this ledger holds, which costs it the whole feature: say so once.
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
        // A worker that names a total and nothing else cannot be told apart
        // from the next one, so they share the one `<unidentified>` slot.
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

    /// Register a freshly loaded replica and return its admission handle, or
    /// `None` when it is not admissible: a `none`-class model, a worker that
    /// reported no GPU at all, or a GPU the ledger does not know, all of which
    /// take the unpriced dispatch path. [`Self::resolve_gpu`] holds the table.
    /// The GPU is whatever the *worker* reported, which is authoritative — the
    /// spawn pin may be an index, absent, or a UUID CUDA reordered — and
    /// `expected_gpu` is a **diagnostic input only**, never a filter.
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
        // The device key, plus its *model name* — the profile's provenance. The
        // name comes from the inventory rather than from the worker's
        // `gpu_name`, so every profile this host writes records the string the
        // probe derived, whatever torch calls the card. The profile *key* is
        // the architecture, which only the worker can read (below).
        let (adoption, masked, resolution) = {
            let mut state = self.lock();
            // Before the join, not after: on a unified-memory device the total
            // the join cross-checks against is the one this call adopts, and a
            // masked GPU is not in the ledger for the join to find at all.
            let adoption = Self::adopt_unified_total_locked(&mut state, &report);
            let masked = Self::adopt_masked_gpu_locked(&mut state, &report);
            let resolution = Self::resolve_gpu(&state, &report, expected_gpu);
            let resolution = Self::escalate_first_unpriced(&mut state, resolution, &report);
            (adoption, masked, resolution)
        };
        // Emitted with the lock **dropped**, or every concurrent grant request
        // would queue behind a log write. Before the `?` below, so a refusal
        // still says why.
        for log in adoption.into_iter().chain(masked).chain(resolution.log) {
            log.emit(inference_id);
        }
        let (gpu, gpu_name) = resolution.admit?;
        // Learn this card's architecture from the report, first one winning:
        // silicon does not change, and a later report disagreeing would re-key
        // every profile the card has written. A short lock, no I/O.
        let (gpu_arch, arch_disagreement) = {
            let mut state = self.lock();
            let reported = report.gpu_arch.clone().filter(|arch| !arch.is_empty());
            let entry = state.gpus.get_mut(&gpu)?;
            if entry.arch.is_none() {
                entry.arch = reported.clone();
            }
            let arch = entry.arch.clone();
            let mut disagreement = None;
            // The seed already won, so without this the two derivations
            // disagreeing is invisible.
            if let (Some(seeded), Some(said)) = (arch.as_deref(), reported.as_deref())
                && seeded != said
                && state.arch_mismatch_logged.insert(gpu.clone())
            {
                disagreement = Some((seeded.to_owned(), said.to_owned()));
            }
            (arch, disagreement)
        };
        // Emitted with the lock dropped, like every other registration log.
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
        // Consulted **outside** the ledger lock: the store stats and may parse
        // files, and blocking every concurrent grant request behind that would
        // put file I/O on the dispatch path by the back door.
        let seed = self
            .profiles
            .as_ref()
            .zip(gpu_arch.as_deref())
            .and_then(|(profiles, arch)| {
                profiles.lookup(&ProfileQuery {
                    inference_id,
                    epoch: cost.epoch,
                    arch,
                    // The dimension in force *now*. A stored profile measured under
                    // another one prices a different quantity, so it must not
                    // match — see `CalibrationProfile::matches_key`.
                    unit: cost.unit.as_str(),
                    aggregation: aggregation.as_str(),
                    torch: report.torch_version.as_deref(),
                    dtype: report.dtype.as_deref(),
                })
            });
        let mut state = self.lock();
        let key = (inference_id.to_owned(), gpu.clone());
        // Record-once semantics, and never downgrade: a later load reporting no
        // base at all must not erase a footprint expectation an earlier measured
        // load taught us, or `reserve_load` would fall back to the conservative
        // constant for a model whose real base is known.
        let known_base = state.remembered_bases.get(&key).copied().flatten();
        if report.base_mb.is_some() || known_base.is_none() {
            state.remembered_bases.insert(key.clone(), report.base_mb);
        }
        if let Some(dtype) = report.dtype.clone() {
            state.remembered_dtypes.insert(key.clone(), dtype);
        }
        // The load response carries a memory sample, and it is the *only* reading
        // this GPU may have for a while: samples otherwise arrive on predict
        // responses, so without this the first window after a load prices
        // `external` as 0 until the staleness refresh happens to land.
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
        let id = state.next_id();
        // Cloned for the admission line below, which is emitted with the lock
        // dropped (the same reason the alarms above are).
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

    /// Forget a replica: its footprint stops being charged and any grant it
    /// still holds disappears with it. Runs from [`Admission`]'s `Drop`.
    ///
    /// The GPU's free reading is adjusted by the departed footprint at the same
    /// moment: `external = total − free − Σ footprint(residents)`, and the
    /// freshest free sample predates the unload by construction, so dropping the
    /// footprint from the sum while the sample still counts that memory as in
    /// use would reattribute it to *external*. The departure is stamped on the
    /// GPU, so the next grant request refreshes immediately ([`refresh_due`])
    /// and a sample captured *before* it is refused
    /// ([`Self::record_free_locked`]). The credit itself is skipped where the
    /// reading predates the load, which would *under*-state `external`.
    fn forget_worker(&self, worker: WorkerId) {
        let mut state = self.lock();
        let Some(entry) = state.workers.remove(&worker) else {
            return;
        };
        let footprint_mb = entry.footprint_mb();
        if footprint_mb == 0 {
            return;
        }
        let Some(gpu) = state.gpus.get_mut(&entry.gpu) else {
            return;
        };
        let total_mb = gpu.total_mb;
        let Some(sample) = gpu.free.as_mut() else {
            // Nothing to adjust and nothing to flag: a GPU with no reading at
            // all is already due a refresh, and reports no `external`.
            return;
        };
        // A reading from before this replica loaded never counted its footprint,
        // so there is nothing to give back — force the refresh and leave the
        // figure alone (see above).
        let credited = sample.at >= entry.loaded_at;
        // Bounded by the GPU's total so the credit cannot walk a reading past the
        // memory that exists. Inert wherever it could change `external`, which
        // is positive only when `free + Σ ours < total` and this footprint is
        // one term of that `Σ`; it binds only where `external` is already pinned
        // at 0, and truncating there costs nothing.
        if credited {
            sample.free_mb = sample.free_mb.saturating_add(footprint_mb).min(total_mb);
        }
        let adjusted_free_mb = sample.free_mb;
        gpu.free_adjusted_at = Some(Instant::now());
        let (model, gpu) = (entry.inference_id, entry.gpu);
        // Snapshotted under the lock, emitted with it dropped, as every other
        // ledger log line is.
        drop(state);
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

/// One replica's handle into the ledger: everything the dispatcher needs to
/// size windows and obtain grants. `None` from
/// [`VramLedger::register_worker`] for replicas with no admission.
pub struct Admission {
    ledger: Arc<VramLedger>,
    worker: WorkerId,
}

impl Admission {
    /// This replica's ledger id, which is what a [`TrimRequest`] names. The
    /// dispatcher matches it against its own replicas to find the one being
    /// asked to release its pool.
    pub fn worker_id(&self) -> u64 {
        self.worker
    }

    /// The GPU this replica was admitted to; `None` once the ledger has
    /// forgotten the entry. Read on the death path, which has to name the
    /// card the verdict was passed on.
    pub fn gpu(&self) -> Option<String> {
        self.ledger
            .lock()
            .workers
            .get(&self.worker)
            .map(|entry| entry.gpu.clone())
    }

    /// Record that this replica just answered a `trim`: its fresh memory
    /// sample is already in the shared telemetry, and this is what makes the
    /// ledger see the released slack (see [`VramLedger::note_trimmed`]).
    pub fn note_trimmed(&self, reply: TrimReply) {
        self.ledger.note_trimmed(self.worker, reply);
    }

    /// Record that this replica *declined* a `trim`. The ledger waits out the
    /// debounce on a decline exactly as it does on a release: the replica was
    /// asked, and asking again at once would only repeat the answer.
    pub fn note_trim_declined(&self) {
        self.ledger.note_trim_declined(self.worker);
    }

    /// Units to aim for in the next window (see [`WINDOW_DEPTH_MULTIPLIER`]).
    pub fn window_target_units(&self) -> u64 {
        self.ledger.window_target_units(self.worker)
    }

    /// Reserve headroom for one window. The demand signal behind the contention
    /// split is `window_requests + queued_behind`, passed separately because the
    /// window's own requests are retired when it settles while whatever was
    /// queued behind it is still demand.
    ///
    /// The dispatcher always knows whether the byte wall closed the window it
    /// is asking for, so it calls [`Self::request_grant_byte_bound`]; this is
    /// the shorthand the tests ask through.
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

    /// The same, for a caller that knows whether the byte wall — and not the
    /// queue running dry — is what closed the window it is asking for.
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

    /// Update the demand signal (e.g. to 0 when the queue drains), so
    /// contention shares stop counting this replica as hungry.
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
