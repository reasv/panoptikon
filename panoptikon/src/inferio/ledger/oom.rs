//! Out-of-memory evidence: classification, the seeded-anchor backstop and
//! condemning a replica that cannot run one item.

use super::*;

/// How the host read an out-of-memory condition from an error frame (which
/// carries no `oom_class`). Both are trusted; the distinction is for the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFrameOom {
    /// The worker's own `INFERENCE_OOM_*` sentinel. Logged as `marker`.
    Marker,
    /// The message matched [`message_oom_tier`]'s patterns. Logged as
    /// `error_frame`.
    Prose,
}

impl ErrorFrameOom {
    /// The tier's name in the log.
    fn as_str(self) -> &'static str {
        match self {
            Self::Marker => OOM_SOURCE_MARKER,
            Self::Prose => OOM_SOURCE_ERROR_FRAME,
        }
    }
}

/// A replica died mid-window on a unified-memory device and its anchor was
/// halved. Logged after the lock drops.
pub(super) struct DeathNegative {
    inference_id: String,
    gpu: String,
    ram_mb: u64,
    anchor_before: u64,
    anchor_after: u64,
}

impl DeathNegative {
    pub(super) fn emit(self) {
        tracing::warn!(
            model = %self.inference_id,
            gpu = %self.gpu,
            unified_ram_mb = self.ram_mb,
            anchor_units_before = self.anchor_before,
            anchor_units_after = self.anchor_after,
            negative_sample = "unified-memory-device worker death",
            "this replica died while running a granted window on a GPU whose \
             memory is the machine's own; recording it as a memory negative \
             (an out-of-memory kill there is a signal from the OS, which no \
             in-process handler can catch) and halving the batch size the next \
             replica of this model is admitted for"
        );
    }
}

/// A believed out-of-memory classification, for the negative's log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OomEvidence {
    /// The worker's `oom_class.source`, or `unclassified` for a bare `oom`.
    source: String,
    /// The worker's `oom_class.exception`, or `unknown` when it sent no class.
    exception: String,
    free_mb_at_failure: Option<u64>,
    trust: OomTrust,
}

impl VramLedger {
    /// Count consecutive out-of-memory windows that carried one item into less
    /// room than one item costs ([`Self::one_unit_appetite_mb_locked`]); at
    /// [`OOM_WINDOWS_AT_FLOOR`] the replica is unrunnable. A clean window
    /// clears the count; an aborted one neither counts nor clears, nor does
    /// one that failed while macOS was paging, which leaves every window that
    /// little room.
    ///
    /// A worker that `died` running one unit counts whatever room the ledger
    /// saw, where a death may be a host RAM kill ([`death_may_be_ram`]): the
    /// batch cannot shrink further. The count passes to the next replica.
    /// A one-unit window whose batch `spilled` to system RAM right after a
    /// pool release counts whatever the room: that spill is live memory that
    /// does not fit.
    ///
    /// Condemning remembers the model's working set on this GPU: the next load
    /// is refused while the refusal room ([`Self::refusal_room_locked`],
    /// reserve not deducted) is below it.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn note_floor_oom_locked(
        &self,
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        failed: bool,
        spilled: bool,
        died: bool,
        clean: bool,
    ) -> Option<UnrunnableReplica> {
        let entry = state.workers.get(&worker)?;
        let one_unit = self.one_unit_appetite_mb_locked(state, entry);
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let at_floor = charge
            .filter(|charge| !charge.pressure.paging())
            .is_some_and(|charge| {
                charge.unit_budget <= 1
                    && (spilled
                        || failed
                            && ((charge.room as f64) < one_unit
                                || (died && death_may_be_ram(state, &key.1, &charge))))
            });
        let entry = state.workers.get_mut(&worker)?;
        if clean {
            entry.oom_at_floor = 0;
            // A clean window, not a load, clears the working set: a load only
            // proves the weights fit.
            state.remembered_working_sets.remove(&key);
            state.death_verdicts.remove(&key);
            if let Some(cal) = state.calibration.get_mut(&key) {
                cal.floor_strikes = 0;
            }
            return None;
        }
        if !at_floor {
            return None;
        }
        entry.oom_at_floor = entry.oom_at_floor.saturating_add(1);
        let strikes = entry.oom_at_floor;
        if died {
            state.calibration.entry(key).or_default().floor_strikes = strikes;
        }
        let entry = state.workers.get(&worker)?;
        if strikes < OOM_WINDOWS_AT_FLOOR {
            return None;
        }
        let inference_id = entry.inference_id.clone();
        let gpu = entry.gpu.clone();
        let base_mb = entry.base_mb.unwrap_or(0);
        // A death says nothing about what the model needs: no working set.
        if died {
            state
                .death_verdicts
                .insert((inference_id.clone(), gpu.clone()), Instant::now());
            return Some(UnrunnableReplica {
                inference_id,
                room_mb: self.limit_locked(state, &gpu),
                gpu,
                base_mb,
                needs_mb: 0,
                died: true,
            });
        }
        // A lower bound: base plus more than the failed window's room, and
        // above the current refusal room so an unchanged card refuses it.
        let needs_mb = base_mb
            .saturating_add(charge.map_or(0, |charge| charge.room).saturating_add(1))
            .max(self.refusal_room_locked(state, &gpu).saturating_add(1));
        state
            .remembered_working_sets
            .insert((inference_id.clone(), gpu.clone()), needs_mb);
        Some(UnrunnableReplica {
            inference_id,
            room_mb: self.limit_locked(state, &gpu),
            gpu,
            base_mb,
            needs_mb,
            died: false,
        })
    }

    /// An out-of-memory window halves a **seeded** anchor; one this GPU
    /// reached in a clean window is kept. Runtime only:
    /// [`super::calibration_store::persistable_anchor`] never writes a seeded
    /// anchor.
    pub(super) fn lower_seeded_anchor_locked(state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        if cal.anchor_measured_here || cal.max_units_measured == 0 {
            return;
        }
        let before = cal.max_units_measured;
        // Floored at 1: zero turns the ratchet ceiling off.
        cal.max_units_measured = (before / 2).max(1);
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            anchor_before = before,
            anchor_after = cal.max_units_measured,
            "halved a seeded ratchet anchor after an out-of-memory window"
        );
    }

    /// A window of more than one unit that the device's room sized
    /// ([`GrantCharge::room_bound`]) ran out of memory: its batch needed more
    /// than its price. The (model, device)'s pool margin is raised by
    /// [`OOM_MARGIN_STEP`], at most [`OOM_MARGIN_MAX_STEPS`] times, so the
    /// same room buys a smaller batch from now on, for every replica of the
    /// model here. Not while macOS is paging, which leaves any batch too
    /// little room.
    pub(super) fn raise_pool_margin_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: GrantCharge,
    ) {
        if !charge.room_bound || charge.unit_budget <= 1 || charge.pressure.paging() {
            return;
        }
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let before = Self::pool_margin_locked(state, entry);
        let cal = state.calibration.entry(key.clone()).or_default();
        if cal.oom_margin_steps >= OOM_MARGIN_MAX_STEPS {
            return;
        }
        cal.oom_margin_steps += 1;
        let after = state
            .workers
            .get(&worker)
            .map_or(before, |entry| Self::pool_margin_locked(state, entry));
        tracing::info!(
            model = %key.0,
            gpu = %key.1,
            failed_at_units = charge.unit_budget,
            room_mb = charge.room,
            pool_margin_before = before,
            pool_margin = after,
            "a window sized by this device's room ran out of memory; raised \
             this model's pool margin here until the server restarts"
        );
    }

    /// A replica whose process died holding a granted window (`charge`).
    ///
    /// Where the death may be a host RAM kill ([`death_may_be_ram`]), the
    /// (model, device) is capped at half that window's unit budget for the
    /// life of this process, at least one unit. Without the cap the next
    /// replica is admitted for the batch that died, and dies again. A window
    /// the queue sized sets no cap: its size says nothing about the batch
    /// the model can run. An item-capped window does, since the cap sized it.
    ///
    /// On a unified-memory device the death is also a negative: the replica
    /// is deflated and its (model, GPU) anchor halved, for this run only.
    /// `None` on a discrete GPU, without a grant, or for a forgotten replica.
    pub(super) fn note_death_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
    ) -> Option<DeathNegative> {
        let charge = charge?;
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let unified_ram_mb = state.gpus.get(&key.1)?.unified_ram_mb;
        let sized_by_queue = charge.queue_bound && !charge.squeezed && charge.item_cap.is_none();
        if death_may_be_ram(state, &key.1, &charge) && !sized_by_queue {
            let cap = (charge.unit_budget / 2).max(1);
            let cal = state.calibration.entry(key.clone()).or_default();
            let cap = cal.death_cap_units.map_or(cap, |held| held.min(cap));
            cal.death_cap_units = Some(cap);
            tracing::warn!(
                model = %key.0,
                gpu = %key.1,
                died_at_units = charge.unit_budget,
                batch_cap_units = cap,
                "a worker died while running a granted window; this model's \
                 batches on this device are capped at half that batch until \
                 the server restarts"
            );
        }
        let ram_mb = unified_ram_mb?;
        let anchor_before = Self::anchor_locked(state, entry);
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.note_negative_sample(anchor_before);
        }
        // Floored at 1, since zero means "never measured" and turns the ratchet
        // ceiling off; an anchor that was already zero stays zero.
        let anchor_after = if anchor_before > 0 {
            (anchor_before / 2).max(1)
        } else {
            0
        };
        if let Some(cal) = state.calibration.get_mut(&key) {
            cal.max_units_measured = anchor_after;
        }
        Some(DeathNegative {
            inference_id: key.0,
            gpu: key.1,
            ram_mb,
            anchor_before,
            anchor_after,
        })
    }
}

/// Whether a death holding `charge` on `gpu` may be the kernel killing the
/// worker for host RAM: on a unified-memory device, or with host RAM booked.
fn death_may_be_ram(state: &LedgerState, gpu: &str, charge: &GrantCharge) -> bool {
    charge.ram_mb > 0
        || state
            .gpus
            .get(gpu)
            .is_some_and(|gpu| gpu.unified_ram_mb.is_some())
}

/// A replica that ran out of memory, or died, [`OOM_WINDOWS_AT_FLOOR`] windows
/// running at one item. [`GrantToken::finish`] hands it to the dispatcher, which fails
/// the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrunnableReplica {
    pub inference_id: String,
    pub gpu: String,
    /// The measured base; `0` when the load reported none.
    pub base_mb: u64,
    /// Lower bound on the model's working set here, for the next load's
    /// refusal.
    pub needs_mb: u64,
    /// The GPU's limit with the reserve deducted, which a window is priced
    /// against; the refusal room and `needs_mb` leave the reserve out.
    pub room_mb: u64,
    /// The last strike was a worker death, not an out-of-memory error:
    /// `needs_mb` is 0 and the refusal lapses ([`DEATH_VERDICT_LAPSE`]).
    pub died: bool,
}

impl std::fmt::Display for UnrunnableReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.died {
            return write!(
                f,
                "the worker of model {} died {} times in a row running a \
                 single item on GPU {}; it is not loaded there again for {} s",
                self.inference_id,
                OOM_WINDOWS_AT_FLOOR,
                self.gpu,
                DEATH_VERDICT_LAPSE.as_secs()
            );
        }
        write!(
            f,
            "model {} did not fit in memory (out of memory, or spilled to \
             system RAM) on GPU {} at a one-item batch {} windows running: \
             its base is {} MiB of the {} MiB this GPU lends a window after \
             its reserve, and one item on top of it did not fit; the next \
             load of it here is refused unless the card has {} MiB free \
             before the reserve",
            self.inference_id,
            self.gpu,
            OOM_WINDOWS_AT_FLOOR,
            self.base_mb,
            self.room_mb,
            self.needs_mb
        )
    }
}

/// Allocator and driver failures worded without "out of memory". Mirrors the
/// worker's `packing.OOM_MESSAGE_PATTERNS`, lower-cased.
const OOM_MESSAGE_PATTERNS: [&str; 10] = [
    "mps backend out of memory",
    "enforce fail at alloc_cpu.cpp",
    "cublas_status_alloc_failed",
    "cudnn_status_alloc_failed",
    "cusolver_status_alloc_failed",
    "cusparse_status_alloc_failed",
    "cufft_alloc_failed",
    "cudaerrormemoryallocation",
    "hiperroroutofmemory",
    "hiperrormemoryallocation",
];

/// Fragment pairs that must share a line (`packing.OOM_MESSAGE_PAIRS`).
const OOM_MESSAGE_PAIRS: [(&str, &str); 1] = [("defaultcpuallocator", "allocate memory")];

/// The device-scoped form of "out of memory": the words **plus** a device-API
/// token as a whole word in the same line (`packing.OOM_DEVICE_TOKENS`).
const OOM_DEVICE_PHRASE: &str = "out of memory";
const OOM_DEVICE_TOKENS: [&str; 6] = ["cuda", "hip", "rocm", "nvml", "xpu", "sycl"];

/// The `oom_class.source` values the protocol defines
/// (docs/inferio-worker-protocol.md).
pub const OOM_SOURCE_TYPED: &str = "typed_exception";
pub const OOM_SOURCE_MARKER: &str = "marker";
pub const OOM_SOURCE_MESSAGE_PATTERN: &str = "message_pattern";
/// The host's own tier: an error frame matched [`message_oom_tier`]. No worker
/// sends it.
pub const OOM_SOURCE_ERROR_FRAME: &str = "error_frame";
/// A measurement that claimed `oom` with no class: an older worker.
pub const OOM_SOURCE_UNCLASSIFIED: &str = "unclassified";
/// What the log prints for an exception type no classification named.
const OOM_EXCEPTION_UNKNOWN: &str = "unknown";

/// Why the ledger believed an out-of-memory report; logged on every negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OomTrust {
    /// `typed_exception`, `marker`, an unknown tier, a bare `oom` flag, or the
    /// host's own error-frame read.
    Outright,
    /// `message_pattern`, with free memory at failure below the window's grant.
    Corroborated,
    /// `message_pattern` with no free reading or an unpriced grant.
    Unopposed,
}

impl OomTrust {
    fn as_str(self) -> &'static str {
        match self {
            Self::Outright => "trusted",
            Self::Corroborated => "corroborated",
            Self::Unopposed => "unopposed",
        }
    }
}

/// What the ledger makes of one measurement's out-of-memory claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OomVerdict {
    /// No out-of-memory condition claimed.
    None,
    /// Believed: the window deflates.
    Trusted(OomTrust),
    /// A `message_pattern` claim with at least the grant free at failure. Not
    /// a negative.
    Contradicted { free_mb: u64, grant_mb: u64 },
}

/// Whether a measurement's `oom` flag is evidence to deflate on.
/// `typed_exception` and `marker` are trusted outright; `message_pattern` is
/// trusted unless `free_mb_at_failure` is at least the window's grant `mb` (a
/// veto, not a requirement; `mb == 0` cannot veto). No class, or an unknown
/// source, is trusted. See docs/batch-calibration-design.md, "What counts as
/// an out-of-memory condition at all".
pub(super) fn oom_verdict(
    measurement: &BatchMeasurement,
    window: Option<&GrantCharge>,
) -> OomVerdict {
    if !measurement.oom {
        return OomVerdict::None;
    }
    let Some(class) = measurement.oom_class.as_ref() else {
        // An older worker's bare `oom` flag.
        return OomVerdict::Trusted(OomTrust::Outright);
    };
    match class.source.as_str() {
        OOM_SOURCE_TYPED | OOM_SOURCE_MARKER => OomVerdict::Trusted(OomTrust::Outright),
        OOM_SOURCE_MESSAGE_PATTERN => {
            let (Some(free_mb), Some(grant_mb)) = (
                class.free_mb_at_failure,
                window.map(|charge| charge.mb).filter(|mb| *mb > 0),
            ) else {
                return OomVerdict::Trusted(OomTrust::Unopposed);
            };
            if free_mb >= grant_mb {
                OomVerdict::Contradicted { free_mb, grant_mb }
            } else {
                OomVerdict::Trusted(OomTrust::Corroborated)
            }
        }
        // An unknown tier is believed.
        _ => OomVerdict::Trusted(OomTrust::Outright),
    }
}

/// Whether a batch's pool growth (`peak − reserved_before`) exceeds the free
/// memory before it plus [`SPILL_SLACK_MB`], corroborating a collapse as a
/// spill. Uses the peak, not the post-batch pool. A missing figure is not
/// corroboration. See docs/batch-calibration-design.md, "The worker's verdict
/// is a candidate".
pub(super) fn pool_grew_past_free(
    measurement: &BatchMeasurement,
    free_before: Option<u64>,
) -> bool {
    let (Some(peak), Some(before), Some(free)) = (
        measurement
            .peak_reserved_mb
            .max(measurement.reserved_after_mb),
        measurement.reserved_before_mb,
        free_before,
    ) else {
        return false;
    };
    peak.saturating_sub(before) > free.saturating_add(SPILL_SLACK_MB)
}

/// `value`, or `fallback` when it is empty: an empty `tracing` field would
/// print as a bare `source=`.
fn named(value: &str, fallback: &'static str) -> String {
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

/// What the log says of a measurement whose out-of-memory the ledger believed.
pub(super) fn oom_evidence(measurement: &BatchMeasurement, trust: OomTrust) -> OomEvidence {
    match measurement.oom_class.as_ref() {
        Some(class) => OomEvidence {
            source: named(&class.source, OOM_SOURCE_UNCLASSIFIED),
            exception: named(&class.exception, OOM_EXCEPTION_UNKNOWN),
            free_mb_at_failure: class.free_mb_at_failure,
            trust,
        },
        // An older worker's bare `oom` flag.
        None => OomEvidence {
            source: OOM_SOURCE_UNCLASSIFIED.to_owned(),
            exception: OOM_EXCEPTION_UNKNOWN.to_owned(),
            free_mb_at_failure: None,
            trust,
        },
    }
}

/// The log line for a window recorded as an out-of-memory negative; `None`
/// when it is not one. A measurement's classification is named in preference
/// to the error frame's.
pub(super) fn oom_negative(
    inference_id: &str,
    gpu: &str,
    evidence: Option<&OomEvidence>,
    frame: Option<ErrorFrameOom>,
    grant_mb: u64,
    oom_samples: usize,
) -> Option<OomNegative> {
    let (source, exception, free_mb_at_failure, trust) = match (evidence, frame) {
        (Some(evidence), _) => (
            evidence.source.clone(),
            evidence.exception.clone(),
            evidence.free_mb_at_failure,
            evidence.trust,
        ),
        (None, Some(tier)) => (
            tier.as_str().to_owned(),
            OOM_EXCEPTION_UNKNOWN.to_owned(),
            None,
            OomTrust::Outright,
        ),
        (None, None) => return None,
    };
    Some(OomNegative {
        inference_id: inference_id.to_owned(),
        gpu: gpu.to_owned(),
        source,
        exception,
        trust: trust.as_str(),
        free_mb_at_failure: free_mb_at_failure
            .map_or(-1, |mb| i64::try_from(mb).unwrap_or(i64::MAX)),
        grant_mb,
        oom_samples,
    })
}

/// Whether `token` occurs in `line` as a whole word (`\b…\b`), so "chip"
/// does not match "hip".
fn contains_word(line: &str, token: &str) -> bool {
    fn is_word(character: char) -> bool {
        character.is_alphanumeric() || character == '_'
    }
    line.match_indices(token).any(|(start, _)| {
        let end = start + token.len();
        line[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !is_word(c))
            && line[end..].chars().next().is_none_or(|c| !is_word(c))
    })
}

/// Whether a worker error message names an out-of-memory condition:
/// [`ErrorFrameOom::Marker`] for an `INFERENCE_OOM_*` prefix,
/// [`ErrorFrameOom::Prose`] for a recognised wording, `None` otherwise.
///
/// Must match the worker's `packing._pattern_oom` exactly. A bare "out of
/// memory" only counts beside a device-API token, and every rule is tested
/// per line, so a traceback's file path cannot supply the token.
pub fn message_oom_tier(message: &str) -> Option<ErrorFrameOom> {
    if message.contains("INFERENCE_OOM_BATCH_SIZE_1:") || message.contains("INFERENCE_OOM_WINDOW:")
    {
        return Some(ErrorFrameOom::Marker);
    }
    let prose = message.lines().any(|line| {
        let lowered = line.to_ascii_lowercase();
        OOM_MESSAGE_PATTERNS
            .iter()
            .any(|pattern| lowered.contains(pattern))
            || OOM_MESSAGE_PAIRS
                .iter()
                .any(|(first, second)| lowered.contains(first) && lowered.contains(second))
            || (lowered.contains(OOM_DEVICE_PHRASE)
                && OOM_DEVICE_TOKENS
                    .iter()
                    .any(|token| contains_word(&lowered, token)))
    });
    prose.then_some(ErrorFrameOom::Prose)
}

/// [`message_oom_tier`] as a predicate, for the parity tests.
#[cfg(test)]
pub fn message_reports_oom(message: &str) -> bool {
    message_oom_tier(message).is_some()
}
