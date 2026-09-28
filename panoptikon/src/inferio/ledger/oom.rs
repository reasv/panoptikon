use super::*;

/// Which host-side tier read an out-of-memory condition out of a window's
/// **error frame** — the path that carries no measurement and therefore none of
/// the worker's own `oom_class`. Both are trusted; the distinction is for the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFrameOom {
    /// This project's own `INFERENCE_OOM_*` sentinel, which the worker emits
    /// only after classifying the failure itself, so the host is reading a
    /// *classification* rather than prose. Named `marker` in the log.
    Marker,
    /// The frame's message or traceback matched the host's allocator/driver
    /// patterns ([`message_oom_tier`]). Named `error_frame`.
    Prose,
}

impl ErrorFrameOom {
    /// The tier's name in the log, alongside the worker's own
    /// `oom_class.source` spellings.
    fn as_str(self) -> &'static str {
        match self {
            Self::Marker => OOM_SOURCE_MARKER,
            Self::Prose => OOM_SOURCE_ERROR_FRAME,
        }
    }
}

/// A replica died mid-window on a unified-memory device and the ledger halved
/// its model's budget for it. Owns its strings; formatted after the lock drops.
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

/// One measurement's out-of-memory classification, as the ingest believed it,
/// carried out so the settle path can name the tier on the negative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OomEvidence {
    /// The worker's `oom_class.source`, or `unclassified` for a pre-run2
    /// worker's bare `oom` flag.
    source: String,
    /// The worker's `oom_class.exception`, or `unknown` when it sent no class.
    exception: String,
    free_mb_at_failure: Option<u64>,
    trust: OomTrust,
}

impl VramLedger {
    /// Count this replica's consecutive out-of-memory windows that carried
    /// **one item** into less room than one item costs
    /// ([`Self::one_unit_appetite_mb_locked`]), and declare it unrunnable at
    /// [`OOM_WINDOWS_AT_FLOOR`]. A clean window clears the count; an aborted or
    /// cancelled one reports no failure and neither counts nor clears.
    ///
    /// The comparand is the room, not `mb == 0`: once the model is resident its
    /// footprint is *ours*, so `external` falls and the card reports a nominal
    /// few hundred MiB of share — 292 MiB against a base of 31 150 on the 5090,
    /// where every window still ran out of memory.
    ///
    /// Condemning also remembers the least this model can run in on this GPU
    /// — its **working set**, which is what the next load of it is refused
    /// against: the weights fit, one item on top of them did not.
    pub(super) fn note_floor_oom_locked(
        &self,
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        failed: bool,
        clean: bool,
    ) -> Option<UnrunnableReplica> {
        let entry = state.workers.get(&worker)?;
        let one_unit = self.one_unit_appetite_mb_locked(state, entry);
        let at_floor = failed
            && charge
                .is_some_and(|charge| charge.unit_budget <= 1 && (charge.room as f64) < one_unit);
        let entry = state.workers.get_mut(&worker)?;
        if clean {
            entry.oom_at_floor = 0;
            // A window ran here: whatever an earlier replica of this model
            // proved about this card, it no longer holds. Cleared on a clean
            // window rather than on a successful load, because a load only
            // proves the weights fit — which the condemnation already granted.
            let key = (entry.inference_id.clone(), entry.gpu.clone());
            state.remembered_working_sets.remove(&key);
            return None;
        }
        if !at_floor {
            return None;
        }
        entry.oom_at_floor = entry.oom_at_floor.saturating_add(1);
        if entry.oom_at_floor < OOM_WINDOWS_AT_FLOOR {
            return None;
        }
        let inference_id = entry.inference_id.clone();
        let gpu = entry.gpu.clone();
        let base_mb = entry.base_mb.unwrap_or(0);
        // What this model needs here, as a **lower bound** and all of it
        // measured: its base, plus more room than the window that failed was
        // given — and never the whole appetite, because a card that frees up
        // later has to be allowed to try this model again. Floored just over
        // the room the reload will be judged against, so an unchanged card
        // refuses it next cycle: on a memory-blind grant the window's room is
        // 0 and the base alone would re-admit it forever.
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
        })
    }

    /// The backstop under a **seeded** anchor: a window that ran out of memory —
    /// by its own error frame, by a batch's, or by killing the worker — halves it.
    ///
    /// Deflation already shrinks the grant below the anchor and repays itself
    /// over wall time, so on its own it cycles back into the same OOM. An anchor
    /// a clean batch on this GPU has reached is a batch size it has actually run
    /// and no OOM unmeasures it (run2 finding B4/N5), but a seeded one is a claim
    /// about another card, and an OOM is the evidence against it. Runtime only,
    /// like the death halving: [`persistable_anchor`] never writes a seeded
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
        // Floored at one unit for the same reason the death halving is: zero is
        // the sentinel that turns the ratchet ceiling off, not a small anchor.
        cal.max_units_measured = (before / 2).max(1);
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            anchor_before = before,
            anchor_after = cal.max_units_measured,
            "halved a seeded ratchet anchor after an out-of-memory window"
        );
    }

    /// A replica that died with a granted window in flight, on a GPU whose memory
    /// is the machine's, is one synthetic negative sample. `None` on a discrete
    /// GPU (a mid-window death there has too many non-memory causes), on a window
    /// that held no grant, and on a replica the ledger has already forgotten.
    ///
    /// The dying entry is deflated, and the (model, GPU) **ratchet anchor is
    /// halved** — the half that does the work, deflation being per-replica
    /// runtime state while the anchor is a *floor* on the next replica's budget.
    /// Nothing reaches the fit or the store, [`Self::pending_update_locked`]
    /// persisting the anchor **monotonically**, so the correction is scoped to
    /// this run.
    pub(super) fn note_unified_death_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        held_grant: bool,
    ) -> Option<DeathNegative> {
        if !held_grant {
            return None;
        }
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let ram_mb = state.gpus.get(&key.1)?.unified_ram_mb?;
        let anchor_before = Self::anchor_locked(state, entry);
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.note_negative_sample(anchor_before);
        }
        // Floored at one unit, because zero is not "a very small anchor" — it is
        // the sentinel for *no local measurement at all*, and [`admitted_units`]
        // turns the ratchet ceiling **off** when it sees one. A GPU that never
        // measured anything keeps its zero: an invented anchor of 1 would clamp
        // a fresh model to a single unit forever.
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

/// A replica that ran out of memory [`OOM_WINDOWS_AT_FLOOR`] windows running
/// on a **memory-blind one-item** grant: there is no room to wait for and no
/// smaller batch, so what is left is to stop dispatching to it.
/// [`GrantToken::finish`] hands it to the dispatcher, which fails the model
/// rather than the next item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrunnableReplica {
    pub inference_id: String,
    pub gpu: String,
    /// The measured base, and `0` when the load reported none.
    pub base_mb: u64,
    /// The least this model can be run in on this GPU: its base plus more
    /// room than the window that failed had. Remembered for the next load's
    /// refusal, and a *bound* rather than a measurement of one item's cost —
    /// which is why a clean window on that card later clears it.
    pub needs_mb: u64,
    /// The GPU's whole limit, the **reserve deducted** — unlike
    /// [`OversizedLoad::room_mb`] and [`Self::needs_mb`], this one is what a
    /// window is priced against, which is why the sentence names both.
    pub room_mb: u64,
}

impl std::fmt::Display for UnrunnableReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "model {} ran out of memory on GPU {} at a one-item batch {} \
             windows running: its base is {} MiB of the {} MiB this GPU lends \
             a window after its reserve, and one item on top of it did not \
             fit; the next load of it here is refused unless the card has {} \
             MiB free before the reserve",
            self.inference_id,
            self.gpu,
            OOM_WINDOWS_AT_FLOOR,
            self.base_mb,
            self.room_mb,
            self.needs_mb
        )
    }
}

/// Allocator and driver failures that never say "out of memory" at all, so each
/// spelling has to be listed. The mirror of the worker's
/// `packing.OOM_MESSAGE_PATTERNS`, lower-cased.
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

/// Two fragments that must appear in the same line. CPU torch's classic
/// allocator failure is one string in practice, but its middle varies by torch
/// version and neither half alone is specific enough (`packing.OOM_MESSAGE_PAIRS`).
const OOM_MESSAGE_PAIRS: [(&str, &str); 1] = [("defaultcpuallocator", "allocate memory")];

/// The device-scoped form of "out of memory": the words **plus** a device-API
/// token as a whole word in the same line (`packing.OOM_DEVICE_TOKENS`).
const OOM_DEVICE_PHRASE: &str = "out of memory";
const OOM_DEVICE_TOKENS: [&str; 6] = ["cuda", "hip", "rocm", "nvml", "xpu", "sycl"];

/// The three `oom_class.source` values the protocol defines, as the worker
/// spells them (docs/inferio-worker-protocol.md; `packing.OOM_SOURCE_*`).
pub const OOM_SOURCE_TYPED: &str = "typed_exception";
pub const OOM_SOURCE_MARKER: &str = "marker";
pub const OOM_SOURCE_MESSAGE_PATTERN: &str = "message_pattern";
/// The host's own tier, for a window that failed with no measurement to carry a
/// class: the error frame's prose matched [`message_oom_tier`]. Not a value any
/// worker sends — it names the host as the classifier.
pub const OOM_SOURCE_ERROR_FRAME: &str = "error_frame";
/// A measurement that claimed `oom` and carried no class at all: a pre-run2
/// worker, whose bare flag is the contract it was written to.
pub const OOM_SOURCE_UNCLASSIFIED: &str = "unclassified";
/// What the log prints for an exception type no classification named.
const OOM_EXCEPTION_UNKNOWN: &str = "unknown";

/// Why the ledger believed an out-of-memory report it acted on. Logged on every
/// negative so the tier that classified it is evidenced in the gateway log
/// rather than inferable only from the worker's wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OomTrust {
    /// The tier is structural on its own and there is nothing to corroborate:
    /// `typed_exception`, `marker`, a tier this host does not recognise, a
    /// pre-run2 worker's bare `oom` flag, or the host's own error-frame read.
    Outright,
    /// `message_pattern`, and the worker's live free reading at the moment of
    /// the failure was **below** the envelope this window was priced at — the
    /// GPU's own arithmetic agrees a batch this size was too big.
    Corroborated,
    /// `message_pattern` with nothing to weigh it against: the worker took no
    /// free reading, or the grant was memory-blind and states no envelope.
    /// Believed, the free reading being a **veto** and not a requirement
    /// ([`oom_verdict`]), but no independent evidence backs it.
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

/// What the ledger makes of one measurement's out-of-memory claim (run2
/// change R3, host half).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OomVerdict {
    /// No out-of-memory condition claimed.
    None,
    /// Claimed and believed: the window deflates. Carries *why* it was
    /// believed, for the negative's log line.
    Trusted(OomTrust),
    /// Claimed from a **message pattern** alone, and the worker's own live
    /// free reading at the instant of the failure says the GPU had at least
    /// the whole envelope this window was priced at. Not a negative.
    Contradicted { free_mb: u64, grant_mb: u64 },
}

/// Whether a measurement's `oom` flag is evidence to deflate on.
///
/// Three tiers, exactly as the worker classified them: **`typed_exception`**, a
/// real allocator error type the interpreter itself named; **`marker`**, this
/// project's own `INFERENCE_OOM_*` sentinel, emitted only after classifying the
/// failure as one of those; and **`message_pattern`**, the tier that reads
/// prose. The last is trusted but **vetoed** by `free_mb_at_failure`, the
/// worker's live free reading at the moment the batch failed: if the GPU had at
/// least `grant.mb` free right then, no batch size we could have chosen was the
/// problem.
///
/// A veto and not a requirement, deliberately: demanding positive corroboration
/// would refuse a real out-of-memory whenever the worker could take no free
/// reading, and whenever an allocator failed with memory free but **fragmented**.
/// The reading is whatever the allocator itself compared against, which on MPS
/// is its watermark ceiling and not free RAM (`memory.free_at_failure_mb`);
/// otherwise this rule vetoes every MPS failure on a Mac with RAM to spare.
/// The comparand is the window's grant `mb`, which is what deflation acts on;
/// `mb == 0` states no envelope and cannot contradict anything, as in
/// [`knee_admits_window`]. A measurement with no `oom_class`, and an
/// unrecognised `source`, are both trusted: the safe direction for an unknown
/// memory signal is to believe it.
pub(super) fn oom_verdict(
    measurement: &BatchMeasurement,
    window: Option<&GrantCharge>,
) -> OomVerdict {
    if !measurement.oom {
        return OomVerdict::None;
    }
    let Some(class) = measurement.oom_class.as_ref() else {
        // A pre-run2 worker, whose bare `oom` is the contract it was
        // written to.
        return OomVerdict::Trusted(OomTrust::Outright);
    };
    match class.source.as_str() {
        OOM_SOURCE_TYPED | OOM_SOURCE_MARKER => OomVerdict::Trusted(OomTrust::Outright),
        OOM_SOURCE_MESSAGE_PATTERN => {
            let (Some(free_mb), Some(grant_mb)) = (
                class.free_mb_at_failure,
                window.map(|charge| charge.mb).filter(|mb| *mb > 0),
            ) else {
                // Nothing independent to weigh it against; the veto cannot
                // fire and the classification stands.
                return OomVerdict::Trusted(OomTrust::Unopposed);
            };
            if free_mb >= grant_mb {
                OomVerdict::Contradicted { free_mb, grant_mb }
            } else {
                OomVerdict::Trusted(OomTrust::Corroborated)
            }
        }
        // A tier a future worker invented. The safe direction for an
        // unknown memory signal is to believe it.
        _ => OomVerdict::Trusted(OomTrust::Outright),
    }
}

/// Does this batch's own pool growth corroborate the worker's collapse verdict?
///
/// The growth (`peak − reserved_before`) against the memory the device had
/// free before the same batch, one batch and one memory domain: to grow the
/// pool on the device by more than that, something had to go to host memory,
/// which is the spill the verdict claims. No wall-clock ratio can make that
/// claim — the worker times `predict`, which an item-priced impl decodes and
/// resizes inside (design doc, "The worker's verdict is a candidate").
///
/// The **peak**, not the pool the batch ended on: an allocator that released
/// blocks mid-batch reports a small after-figure, and that population is
/// exactly the one under memory pressure. A missing figure → not corroborated.
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

/// A wire string as the log may print it, or `fallback` when it is empty. The
/// msgpack decode reads an absent `exception` as `""`, and a `tracing` field
/// with an empty value renders as a bare `source=` that the protocol tooling
/// drops when it splits the line into fields — and the line whose whole job is
/// to name the tier must not lose it to a worker that under-fills the map.
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
        // A pre-run2 worker's bare `oom` flag, believed as it always was. It
        // names neither a tier nor an exception, which is worth seeing: it dates
        // the worker on the other end.
        None => OomEvidence {
            source: OOM_SOURCE_UNCLASSIFIED.to_owned(),
            exception: OOM_EXCEPTION_UNKNOWN.to_owned(),
            free_mb_at_failure: None,
            trust,
        },
    }
}

/// The line one settled window logs when it is recorded as an out-of-memory
/// negative; `None` when it is not one. A **measurement's** classification is
/// preferred over the host's read of the error frame whenever the window carried
/// one — it is the more specific statement, made in the process that raised the
/// failure. Both are trusted, so the preference changes nothing about the
/// verdict, only about who the log credits with it.
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

/// Whether `token` occurs in `line` bounded by non-word characters on both
/// sides — the host's `\b…\b`, so "chip", "ship" and "relationship" cannot
/// stand in for "hip".
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

/// Which tier of an error message from a worker names an out-of-memory condition
/// the ledger should treat as a negative sample — [`ErrorFrameOom::Marker`] for
/// the project's own sentinel, [`ErrorFrameOom::Prose`] for a recognised
/// wording, `None` for neither. Which tier matched changes no verdict; it exists
/// so the negative's log line can name its classifier.
///
/// Both `INFERENCE_OOM_*` prefixes are contract
/// (docs/inferio-worker-protocol.md). Everything below them is the
/// **error-frame** path — a `predict` that failed with no measurement to
/// classify — and it mirrors the worker's own classifier
/// (`packing._pattern_oom`) exactly, since a wording only one side recognises
/// deflates on one side of the wire only.
///
/// **The bare `out of memory` substring is deliberately gone**: an impl wording
/// an unrelated failure as "out of memory slots" deflated a healthy model on a
/// GPU with 96 GB free. What replaces it is the closed list **plus** the words
/// scoped to a device-API token, the closed list alone having lost real
/// conditions. Every rule is tested **per line**, the device-token rule included:
/// a Python traceback names `torch/cuda/__init__.py` in its frames and `/` is a
/// word boundary, so a whole-blob test would match a token from a file path.
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

/// [`message_oom_tier`] as the predicate the worker-protocol parity tests
/// assert against. The dispatcher takes the tier itself.
#[cfg(test)]
pub fn message_reports_oom(message: &str) -> bool {
    message_oom_tier(message).is_some()
}
