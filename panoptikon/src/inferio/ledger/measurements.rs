use super::*;

/// Whether a settling window's batches may describe this model's throughput
/// curve at all. One window-wide disqualification, and it is not about size:
/// **memory-blind** (`mb == 0`), a pre-fit grant that ran unpriced. A
/// **squeezed** window is admitted — the squeeze is the budget that card ran,
/// and [`FULL_BATCH_RATIO`] is taken over the *admitted* units, so its batches
/// are honest samples of the size they ran at. The other exclusion, a batch the
/// worker's own clamp shrank, lives in [`VramLedger::ingest_locked`]; both
/// still feed the cost fit.
pub(super) fn knee_admits_window(charge: &GrantCharge) -> bool {
    charge.mb > 0
}

/// The one clamp reason this host acts on beyond the log: a size-dependent,
/// **non-memory** kernel ceiling cut the batch, so it feeds the
/// per-(model, GPU) [`ShapeCeiling`]. Any other reason is printed verbatim and
/// otherwise treated like the memory clamp.
pub(super) const CLAMP_REASON_INDEX_LIMIT: &str = "index_limit";

/// Whether this measurement was cut by a clamp naming `reason`. A clamp that
/// names **no** reason is the defensive memory clamp, so it never answers `true`
/// for a named reason and an unrecognised reason gets `false`.
fn clamp_reason_is(measurement: &BatchMeasurement, reason: &str) -> bool {
    measurement
        .clamped
        .as_ref()
        .and_then(|clamp| clamp.reason.as_deref())
        .is_some_and(|named| named == reason)
}

/// How many measurements the telemetry ring dropped before the ledger read
/// them: 0 when the retained history is continuous with the watermark.
pub(super) fn watermark_gap(oldest_retained: Option<u64>, watermark: u64) -> u64 {
    oldest_retained
        .unwrap_or(0)
        .saturating_sub(watermark)
        .saturating_sub(1)
}

/// What one settle did to a (model, GPU)'s [`ShapeCeiling`]. Logged at INFO once
/// per change — never per window — because a ceiling that moves is the
/// operator's only notice that a model is held below its memory budget by its
/// own kernels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ShapeCeilingEvent {
    inference_id: String,
    gpu: String,
    /// `set`, `lowered` or `cleared`.
    action: &'static str,
    /// The ceiling now in force; `None` on `cleared`.
    units: Option<u64>,
    /// The ceiling that stood before this change; `None` on `set`.
    previous_units: Option<u64>,
    /// Why, for the two clearing causes: [`CEILING_CAUSE_PROFILE`] or
    /// [`CEILING_CAUSE_RAN_WIDER`]; [`CEILING_CAUSE_REPORTED`] for a clamp.
    cause: &'static str,
    canvas_pixels: Option<u32>,
    max_tokens: Option<u32>,
    epoch: u32,
    /// Age of the ceiling being replaced, in seconds. `None` on `set`.
    previous_age_secs: Option<u64>,
}

impl ShapeCeilingEvent {
    pub(super) fn emit(self) {
        tracing::info!(
            model = %self.inference_id,
            gpu = %self.gpu,
            action = self.action,
            shape_ceiling_units = self.units.map_or(-1i64, |units| units as i64),
            previous_units = self.previous_units.map_or(-1i64, |units| units as i64),
            cause = self.cause,
            canvas_pixels = self.canvas_pixels.map_or(-1i64, i64::from),
            max_tokens = self.max_tokens.map_or(-1i64, i64::from),
            epoch = self.epoch,
            previous_age_secs = self.previous_age_secs.map_or(-1i64, |secs| secs as i64),
            "this model's own kernels named a batch size they cannot execute \
             at this corpus's shapes; the unit budget will not widen past it \
             and the ramp takes no step beyond it. A shape ceiling is not a \
             memory condition and never deflates anything — it is runtime-only \
             state, re-learned after a restart and dropped the moment the \
             canvas, the cost epoch or the corpus moves (run2 S1)"
        );
    }
}

/// The `cause` of a [`ShapeCeilingEvent`]: the worker reported the clamp.
pub(super) const CEILING_CAUSE_REPORTED: &str = "index_limit_clamp";
/// The replica's canvas, token window or cost epoch is not the one the ceiling
/// was observed under, so its unit figure no longer denominates anything.
pub(super) const CEILING_CAUSE_PROFILE: &str = "canvas_or_epoch_changed";
/// A batch **larger** than the ceiling ran without the impl cutting it: these
/// are not the dims the ceiling was measured at any more.
pub(super) const CEILING_CAUSE_RAN_WIDER: &str = "ran_wider_uncut";

/// Fold this window's `index_limit` evidence into a (model, GPU)'s shape
/// ceiling, returning what changed (`None` = nothing did). Four rules, in this
/// order because one window can carry more than one: **invalidate on identity**
/// (a ceiling under another canvas or cost epoch is a number in another
/// currency, and converting would need padded dims the ledger never sees);
/// **clear when contradicted** by a larger batch that ran uncut; **record, or
/// lower**, the binding frame being the element-wise max of the batch; and
/// **never raise in place**, which would pin the budget at the size just
/// demonstrated and make the next demonstration impossible.
pub(super) fn update_shape_ceiling(
    cal: &mut ModelCalibration,
    canvas_pixels: Option<u32>,
    max_tokens: Option<u32>,
    epoch: u32,
    reported: Option<u64>,
    ran_wider_uncut: u64,
    now: Instant,
) -> Option<ShapeCeilingChange> {
    // `(cause, previous units, previous age in seconds)` for the invalidation,
    // taken before the write so the borrow of `cal.shape_ceiling` ends first.
    let cleared = match &cal.shape_ceiling {
        Some(current)
            if current.canvas_pixels != canvas_pixels
                || current.max_tokens != max_tokens
                || current.epoch != epoch =>
        {
            Some((
                CEILING_CAUSE_PROFILE,
                current.units,
                now.saturating_duration_since(current.observed_at).as_secs(),
            ))
        }
        Some(current) if ran_wider_uncut > current.units => Some((
            CEILING_CAUSE_RAN_WIDER,
            current.units,
            now.saturating_duration_since(current.observed_at).as_secs(),
        )),
        _ => None,
    };
    if cleared.is_some() {
        cal.shape_ceiling = None;
    }
    let reported = reported.filter(|units| *units > 0);
    let standing = cal.shape_ceiling;
    match (reported, standing) {
        // A clamp with nothing standing — nothing was ever recorded, or this
        // same window's evidence just retired what was — is a `set`, with the
        // dropped figure reported beside it. A clamp *below* the one in force
        // is a `lowered`: the binding frame is bigger than we knew, and the
        // smaller figure is the one that holds for every batch.
        (Some(units), current) if current.is_none_or(|standing| units < standing.units) => {
            cal.shape_ceiling = Some(ShapeCeiling {
                units,
                canvas_pixels,
                max_tokens,
                epoch,
                observed_at: now,
            });
            // What this displaced: the standing ceiling on a `lowered`, and on a
            // `set` whatever the invalidation above dropped, if anything.
            let displaced = current
                .map(|standing| {
                    (
                        standing.units,
                        now.saturating_duration_since(standing.observed_at)
                            .as_secs(),
                    )
                })
                .or_else(|| cleared.map(|(_, units, age)| (units, age)));
            Some(ShapeCeilingChange {
                action: if current.is_some() { "lowered" } else { "set" },
                cause: CEILING_CAUSE_REPORTED,
                units: Some(units),
                previous_units: displaced.map(|(units, _)| units),
                previous_age_secs: displaced.map(|(_, age)| age),
            })
        }
        // A clamp at or above the one in force teaches nothing: a batch of
        // smaller pages fits more of them under the same element limit.
        (Some(_), _) => None,
        (None, _) => cleared.map(|(cause, units, age)| ShapeCeilingChange {
            action: "cleared",
            cause,
            units: None,
            previous_units: Some(units),
            previous_age_secs: Some(age),
        }),
    }
}

impl VramLedger {
    /// Drain this worker's new telemetry into the ledger by watermark. `window`
    /// is the settling window's own grant, and it gates the **throughput ring
    /// only** ([`FULL_BATCH_RATIO`], [`knee_admits_window`]): the cost fit and
    /// the ratchet take every clean priced batch. One approximation errs the
    /// safe way — an ingest can pick up batches an *aborted* window left above
    /// the watermark, which a forward-only ramp under-admits at worst.
    pub(super) fn ingest_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        window: Option<GrantCharge>,
        window_failed: bool,
    ) -> Ingested {
        let Some(entry) = state.workers.get(&worker) else {
            return Ingested::default();
        };
        let watermark = entry.fit_watermark;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let gpu = entry.gpu.clone();
        let telemetry = Arc::clone(&entry.telemetry);
        let base_recorded = entry.base_recorded;
        let mut reserved_at_load = entry.reserved_at_load_mb;
        let mut allocated_at_load = entry.allocated_at_load_mb;

        let (load, memory, samples, oldest_retained) = {
            let telemetry = match telemetry.lock() {
                Ok(telemetry) => telemetry,
                Err(poisoned) => poisoned.into_inner(),
            };
            let samples: Vec<_> = telemetry
                .measurements()
                .filter(|sample| sample.seq > watermark)
                .cloned()
                .collect();
            let oldest = telemetry.measurements().next().map(|sample| sample.seq);
            (
                telemetry.load.as_ref().map(|stamped| stamped.value.clone()),
                telemetry.memory.clone(),
                samples,
                oldest,
            )
        };
        // Reading by watermark is what makes ring overflow *visible*: an oldest
        // retained sequence past the watermark means measurements were evicted
        // between reads and the fit has a hole. Nothing breaks, but a silent
        // hole is how a fit quietly stops tracking a model, so it gets named.
        let gap = watermark_gap(oldest_retained, watermark);
        if gap > 0 {
            tracing::warn!(
                model = %key.0,
                gpu = %gpu,
                gap,
                watermark,
                ring = super::worker::WorkerTelemetry::RING,
                "batch measurements were evicted from this replica's telemetry \
                 ring before the ledger read them; the cost fit is missing that \
                 many samples"
            );
        }

        // A base that only arrived after registration (a late load response, a
        // claimed prewarmed worker) is recorded once and never moved.
        if !base_recorded && let Some(base) = load.as_ref().and_then(|report| report.base_mb) {
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.base_mb = Some(base);
                entry.base_recorded = true;
            }
            state.remembered_bases.insert(key.clone(), Some(base));
        }
        if reserved_at_load.is_none() {
            reserved_at_load = load.as_ref().and_then(|report| report.reserved_at_load_mb);
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.reserved_at_load_mb = reserved_at_load;
            }
        }
        if allocated_at_load.is_none() {
            allocated_at_load = load.as_ref().and_then(|report| report.allocated_at_load_mb);
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.allocated_at_load_mb = allocated_at_load;
            }
        }

        // The GPU total this response claims, reused as the currency check for
        // the per-batch readings below: they come from the same worker in the
        // same response, so a total that does not describe the GPU condemns them
        // exactly as it condemns the response-level one.
        let reported_total_mb = memory.as_ref().and_then(|stamped| stamped.value.total_mb);
        let model = state
            .workers
            .get(&worker)
            .map(|entry| entry.inference_id.clone());
        // The currency any `index_limit` clamp in this window is denominated in.
        // Both are fixed for the life of a [`WorkerEntry`], so every window this
        // replica ran was priced under exactly these — which is what makes
        // stamping the ceiling with them correct. A replica the ledger has
        // forgotten reports neither, and its clamps establish no ceiling: a
        // number in an unknown currency is worse than no number.
        let profile = state
            .workers
            .get(&worker)
            .map(|entry| (entry.canvas_pixels, entry.max_tokens, entry.epoch));

        let mut negative = false;
        let mut saw_oom = false;
        let mut saw_collapse = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut margin_samples: Vec<(u64, f64)> = Vec::new();
        let mut throughput: Vec<ThroughputSample> = Vec::new();
        let mut anchor = 0u64;
        // The one definition of "ran at its budget", read by the ramp's gate and
        // by the knee's sample rule alike: [`FULL_BATCH_RATIO`] of the
        // **admitted** unit budget, which is already post-squeeze — a squeeze is
        // the budget that card ran. `None` when there is no window to measure
        // against.
        let budget_floor = window
            .map(|charge| ((charge.unit_budget as f64 * FULL_BATCH_RATIO).ceil() as u64).max(1));
        // The ring's own window-wide exclusion, which is not about size at all
        // ([`knee_admits_window`]).
        let full_batch =
            budget_floor.filter(|_| window.is_some_and(|charge| knee_admits_window(&charge)));
        // And the ramp's own extra gate, over that same floor: a window the
        // **queue** sized never reached the rung the ramp put in force, so it is
        // no evidence for the next one. Its batches still feed the ring — they
        // are honest samples of the size they ran at, and the ring buckets by
        // size ([`Ingested::at_budget`]).
        let queue_bound = window.is_none_or(|charge| charge.queue_bound);
        // A window the byte wall closed ran everything that fit, so it is
        // evidence of the size this GPU reached even though the unit budget
        // went unspent. Only `max_units_measured_here` is relaxed for it: the
        // wall guarantees the next window cannot test a wider rung, so the ramp
        // still earns no step ([`Ingested::at_budget`]).
        let byte_bound = window.is_some_and(|charge| charge.byte_bound);
        // The window's contention tag, carried onto every throughput sample it
        // produces and consulted for the collapse verdict below. An ingest with
        // no window behind it is treated as contended: only a positive statement
        // that the GPU was quiet admits a sample to the knee.
        let occupants = window
            .map(|charge| charge.peak_occupants)
            .unwrap_or(u32::MAX);
        let sole_occupancy = occupants == 0;
        // Is this the replica's *first* settled window? Its batches are warm-up
        // whatever the allocator says, and are marked so the knee fit can drop
        // them ([`ThroughputSample::warmup`]). A forgotten replica is treated as
        // warming up: it costs at most one window.
        let warmup_window = state
            .workers
            .get(&worker)
            .is_none_or(|entry| entry.settled_windows == 0);
        // Batches run before this window, counted on past the first window so
        // that a model whose first window is one batch still gets a warm-up
        // ([`KNEE_WARMUP_BATCHES`]).
        let mut ran_batches = state
            .workers
            .get(&worker)
            .map_or(0, |entry| entry.ran_batches);
        let mut suppressed_collapses = 0usize;
        // `(free at failure, the window's granted envelope)` for every
        // message-pattern OOM this window's own free readings contradicted.
        let mut contradicted_ooms: Vec<(u64, u64)> = Vec::new();
        // The out-of-memory classifications this window's measurements carried
        // and the ledger believed, for the negative's log line. The first is the
        // one named; the count is reported beside it.
        let mut trusted_oom: Option<OomEvidence> = None;
        let mut trusted_ooms = 0usize;
        // Every clamp this window's measurements reported, for the settle line.
        // Collected rather than counted because the *reason* is the new half:
        // a memory clamp is a transient of a busy GPU, a shape ceiling is
        // permanent for these shapes ([`clamp_log_field`]).
        let mut clamps: Vec<Option<String>> = Vec::new();
        // The shape-ceiling evidence this window carried: the **smallest**
        // `to_units` any `index_limit` clamp reported (the binding padded frame
        // is the element-wise max over a batch, so the smallest report holds for
        // every batch), and the **largest** batch that executed without the impl
        // cutting it, which contradicts a ceiling that no longer describes these
        // dims.
        let mut index_limit_to: Option<u64> = None;
        let mut ran_wider_uncut = 0u64;
        // Summed over the window, `None` while no batch reported the counter.
        let mut alloc_retries: Option<u64> = None;
        // `(MiB the pool grew back, that batch's own wall time)` from the first
        // batch after a release the **host asked for**.
        let mut regrow: Option<(u64, Option<f64>)> = None;
        // Throughput-collapse verdicts dropped because the batch was cut by the
        // impl's own shape ceiling rather than by anything about its rate.
        let mut clipped_collapses = 0usize;
        // …and because the batch's own memory figures do not show a spill.
        let mut uncorroborated_collapses = 0usize;
        for sample in samples {
            new_watermark = new_watermark.max(sample.seq);
            let measurement = &sample.measurement;
            if let Some(retries) = measurement.alloc_retries {
                alloc_retries = Some(alloc_retries.unwrap_or(0).saturating_add(retries));
            }
            // The last one this window reported wins. Only a release the host
            // asked for: a reactive shrink's re-grow is the worker's own
            // hysteresis, and `pool_releases` never counted it.
            if let Some(mb) = measurement.regrow_mb
                && measurement.regrow_after.as_deref() == Some(HOST_ASKED_RELEASE)
            {
                regrow = Some((mb, measurement.duration_ms));
            }
            // Per-batch free. The worker's defensive clamp already reads live
            // free memory before every batch; reporting it turns `external_mb`
            // from a window-boundary quantity into one that refreshes at response
            // cadence. Ingested **before** the negative check below, because a
            // window that just OOMed is when the freshest reading is worth most.
            // Ordering is by sequence number and `record_free_locked` keeps the
            // freshest by capture instant, so the response-level sample wins;
            // source precedence and the departed-worker rule apply unchanged.
            if let (Some(free), Some(source)) =
                (measurement.free_mb, measurement.free_source.clone())
            {
                Self::record_free_locked(
                    state,
                    &gpu,
                    free,
                    source,
                    sample.captured_at,
                    reported_total_mb,
                    model.as_deref(),
                    // The RAM domain this same reading was clipped from, when
                    // the frame states one: a Metal frame does, so it is priced
                    // in the domain `external` is summed in rather than 8 192
                    // MiB away down the no-basis fallback.
                    RamBasis::of_batch(measurement),
                );
            }
            // And this replica's own pool beside it, from the same measurement:
            // the pool the batch *left behind*, never the peak it touched. A
            // high-water charged as the resident's footprint never falls back,
            // so the external term it is netted out of decays away under a hog
            // that released nothing — round 4's real defect. The response-level
            // sample below supersedes this, so the case it actually moves is the
            // reply that carried measurements but no `memory` map (a worker
            // whose allocator answers and whose driver does not).
            // Freshness-guarded as `note_trimmed` is.
            if let Some(pool) = measurement
                .reserved_after_mb
                .or(measurement.peak_reserved_mb)
                && let Some(entry) = state.workers.get_mut(&worker)
                && entry
                    .reserved_seen_at
                    .is_none_or(|at| sample.captured_at > at)
            {
                entry.reserved_mb = Some(pool);
                entry.reserved_seen_at = Some(sample.captured_at);
            }
            // A throughput collapse is a *comparison* between two of this
            // window's batches, and a comparison is only meaningful inside one
            // occupancy regime: a neighbour's window arriving between batch N−1
            // and batch N halves the rate with nothing wrong at all. So the
            // verdict is trusted only from a window that had the GPU to itself
            // throughout — the same tag the knee is fitted under — and a
            // suppressed collapse is discarded whole rather than counted clean.
            //
            // Suppression is of the *verdict*, never of the measurement: one
            // batch can carry both flags, since an impl that absorbed an
            // out-of-memory inside its own halving loop runs its retries inside
            // the same wall clock. A batch the impl's **shape ceiling** cut is
            // the second thing a verdict cannot be read across — the drop from
            // 200 units to the 28 the kernel allows is arithmetic, not a spill,
            // and the ceiling carries no `oom` by definition.
            let clipped = clamp_reason_is(measurement, CLAMP_REASON_INDEX_LIMIT);
            // Recorded before the negative branch below, deliberately: the clamp
            // states what the impl *executed*, which is true whatever the batch
            // then went on to do.
            if clipped && let Some(clamp) = &measurement.clamped {
                let to_units = clamp.to_units;
                if to_units > 0 {
                    index_limit_to =
                        Some(index_limit_to.map_or(to_units, |seen: u64| seen.min(to_units)));
                }
            }
            let collapse_suppressed =
                measurement.throughput_collapse && (!sole_occupancy || clipped);
            if collapse_suppressed {
                if clipped {
                    clipped_collapses += 1;
                } else {
                    suppressed_collapses += 1;
                }
            }
            // The third thing a verdict cannot be read across, and the one no
            // ratio separates, so the batch's own memory figures decide
            // ([`pool_grew_past_free`]) — weighed against the free reading
            // this measurement just refreshed, which is the one taken before
            // the batch it rides on.
            let uncorroborated = measurement.throughput_collapse
                && !collapse_suppressed
                && !pool_grew_past_free(measurement, Self::free_before_locked(state, &gpu));
            if uncorroborated {
                uncorroborated_collapses += 1;
            }
            let collapse =
                measurement.throughput_collapse && !collapse_suppressed && !uncorroborated;
            // The worker's structural OOM classification, read for what it is
            // (see [`oom_verdict`]). A message-only classification the GPU's own
            // free reading contradicts is not a negative.
            let oom = match oom_verdict(measurement, window.as_ref()) {
                OomVerdict::None => false,
                OomVerdict::Trusted(trust) => {
                    trusted_ooms += 1;
                    trusted_oom.get_or_insert_with(|| oom_evidence(measurement, trust));
                    true
                }
                OomVerdict::Contradicted { free_mb, grant_mb } => {
                    contradicted_ooms.push((free_mb, grant_mb));
                    false
                }
            };
            if oom || collapse {
                // A negative sample is evidence that a batch this size did NOT
                // work. Its `peak_reserved` under-states the batch's real cost,
                // so feeding it to the fit would drag the slope into
                // over-admission, and advancing the ratchet anchor on it would
                // enshrine the failing size as the floor the ramp resumes at.
                // The sample deflates and is then discarded.
                negative = true;
                saw_oom |= oom;
                saw_collapse |= collapse;
                continue;
            }
            // Discarded whole, and without deflating: a collapse nothing
            // corroborates is not evidence that the size worked either.
            if collapse_suppressed || uncorroborated {
                continue;
            }
            let units = measurement.units.filter(|units| *units > 0);
            // The contradiction that retires a shape ceiling: a batch bigger than
            // the ceiling in force that ran uncut, with no out-of-memory and no
            // collapse, so the dims moved ([`update_shape_ceiling`], rules 2 and
            // 4). A *memory*-clamped batch still counts: it executed the units
            // it reports.
            if !clipped {
                ran_wider_uncut = ran_wider_uncut.max(units.unwrap_or(0));
            }
            // Three states, not two: a measurement carrying no allocator reading
            // says nothing about the pool either way, and must not be read as
            // "warm" (see the warm-pool exclusion below). The reading is the
            // **post-batch** pool: MPS's `peak_reserved` is sampled at 20 ms
            // during the batch and so exceeds it by construction, which made
            // every MPS batch look pool-growing and emptied the knee ring
            // (914 samples on the control against 0 on the fix). A worker too
            // old to report the post-batch pool falls back to the peak.
            //
            // **This rule is not MPS-scoped, and the CUDA change is large.**
            // `peak_reserved` there is `max_memory_reserved()` after a
            // per-batch reset, above the post-batch pool whenever torch
            // released cached blocks mid-batch to retry an allocation — such a
            // batch now rings as warm at the rate the retry stalled. Measured
            // (round-6 verification §3): S2 wd-vit's largest granted budget
            // fell 718 -> 48 and its published one 1 024 -> 64, at **1.119x**
            // the items/s, over 0 squeezed windows on either binary; MiniLM,
            // GPU-bound, held 128 ring samples throughout and moved 1.011x.
            // The ramp brakes where more batch pays nothing, which is the
            // ruled behaviour on every platform, not an MPS side effect.
            let pool_after = measurement
                .reserved_after_mb
                .or(measurement.peak_reserved_mb);
            let grew_pool = match (pool_after, measurement.reserved_before_mb) {
                (Some(after), Some(before)) => Some(after > before),
                _ => None,
            };
            let high_water = grew_pool == Some(true);
            let warm = grew_pool == Some(false);
            // Counted for every batch that ran, priced or not: what settles a
            // runtime is work, and a batch excluded below still did some
            // ([`KNEE_WARMUP_BATCHES`]).
            ran_batches = ran_batches.saturating_add(1);
            // Throughput for the knee, in units/sec. Six exclusions: negative
            // samples (the `continue` above) measure the failure, not the curve;
            // an unpriceable batch has no `units` to bucket by; a batch with **no
            // allocator reading** is excluded rather than assumed warm; a
            // **pool-growing** batch pays `cudaMalloc` for the pool it grows,
            // which is the cost of *reaching* that size, and since every ramp
            // step is high-water, including them would manufacture a knee out of
            // allocator behaviour; a batch that did not spend its granted budget
            // ([`FULL_BATCH_RATIO`]) ran small because there was nothing bigger;
            // and a batch the worker **clamped**, for either reason it clamps
            // for, could not use an honest budget. All still feed the cost fit,
            // which is a statement about memory and true at whatever size ran.
            if let Some(clamp) = &measurement.clamped {
                clamps.push(clamp.reason.clone());
            }
            if warm
                && measurement.clamped.is_none()
                && let (Some(units), Some(duration_ms), Some(full_batch)) =
                    (units, measurement.duration_ms, full_batch)
                && duration_ms > 0.0
                && units >= full_batch
            {
                throughput.push(ThroughputSample {
                    units,
                    units_per_sec: units as f64 * 1000.0 / duration_ms,
                    occupants,
                    // Both filled in below, where the (model, GPU)'s calibration
                    // — which owns the sequence counter and the anchor — is in
                    // hand.
                    seq: 0,
                    anchor: 0,
                    warmup: warmup_window,
                    warmup_tail: !warmup_window && ran_batches <= KNEE_WARMUP_BATCHES,
                });
            }
            // Every clean priced batch is a fit sample: `max_memory_allocated`
            // has none of the caching allocator's hysteresis, so a warm-pool
            // repeat is as honest a point as the batch that grew the pool. The
            // formula is the envelope `peak_allocated − allocated_at_load`,
            // which is what a grant reserves, never a per-batch delta.
            if let (Some(units), Some(peak), Some(at_load)) =
                (units, measurement.peak_allocated_mb, allocated_at_load)
            {
                fit_samples.push(FitSample {
                    units,
                    delta_mb: peak.saturating_sub(at_load),
                });
                anchor = anchor.max(units);
            }
            // What the allocator's pool took over what the batch actually held,
            // measurable only where the pool grew. Big deltas only: under
            // [`POOL_MARGIN_MIN_DELTA_MB`] the ratio is block granularity.
            if high_water
                && let (
                    Some(units),
                    Some(peak_reserved),
                    Some(reserved_base),
                    Some(peak_allocated),
                    Some(allocated_base),
                ) = (
                    units,
                    measurement.peak_reserved_mb,
                    reserved_at_load,
                    measurement.peak_allocated_mb,
                    allocated_at_load,
                )
            {
                let allocated = peak_allocated.saturating_sub(allocated_base);
                if allocated >= POOL_MARGIN_MIN_DELTA_MB {
                    let reserved = peak_reserved.saturating_sub(reserved_base);
                    margin_samples.push((units, reserved as f64 / allocated as f64));
                }
            }
        }
        // The response-level sample last, because it is the freshest reading the
        // response carries: the worker takes it after the final batch, while
        // every `free_mb` above was taken *before* the batch it rides on. It
        // updates our own pool size as well as the GPU's free reading.
        if let Some(stamped) = memory {
            if let Some(reserved) = stamped.value.reserved_mb
                && let Some(entry) = state.workers.get_mut(&worker)
            {
                entry.reserved_mb = Some(reserved);
                entry.reserved_seen_at = Some(stamped.captured_at);
            }
            if let (Some(free), Some(source)) =
                (stamped.value.free_mb, stamped.value.free_source.clone())
            {
                Self::record_free_locked(
                    state,
                    &gpu,
                    free,
                    source,
                    stamped.captured_at,
                    stamped.value.total_mb,
                    model.as_deref(),
                    RamBasis::of(&stamped.value),
                );
            }
        }
        if let Some((free_mb, grant_mb)) = contradicted_ooms.first().copied() {
            tracing::warn!(
                model = %key.0,
                gpu = %gpu,
                contradicted = contradicted_ooms.len(),
                free_mb_at_failure = free_mb,
                grant_mb,
                "not deflating on this window's out-of-memory report: the \
                 worker classified it from the failure's *wording* alone, and \
                 its own live reading at that instant had at least the whole \
                 envelope this window was priced at still free. A batch this \
                 size was not what the GPU ran out of, so halving the budget \
                 would cost throughput and fix nothing (run2 change R3, \
                 finding Q1/B11)"
            );
        }
        if suppressed_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                suppressed_collapses,
                occupants,
                "ignored this window's throughput-collapse flags: another \
                 replica held a window on the same GPU while it ran, so the \
                 rate drop the worker compared against has a neighbour to \
                 explain it and is not evidence about the batch size (P5-5)"
            );
        }
        if uncorroborated_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                uncorroborated_collapses,
                "ignored this window's throughput-collapse flags: the pool grew \
                 by less than the device had free, so no batch this window ran \
                 spilled to host memory and the rate drop is the impl's own \
                 decode cost. Discarded rather than counted clean"
            );
        }
        if clipped_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                clipped_collapses,
                "ignored this window's throughput-collapse flags: the impl's \
                 own shape ceiling cut these batches (clamped.reason = \
                 index_limit), so the rate the worker compared against was \
                 taken over a fraction of the work and the drop is arithmetic \
                 rather than a spill. A shape ceiling carries no out-of-memory \
                 and never deflates anything (run2 S1)"
            );
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_watermark = new_watermark;
            // Counted here, after `warmup_window` was read, so the first
            // window's own samples carry the mark and the second window's do not.
            entry.settled_windows = entry.settled_windows.saturating_add(1);
            entry.ran_batches = ran_batches;
            // Kept even when this window reported none: "the last window
            // retried zero times" is the reading the starvation trigger needs,
            // and it differs from "no window has ever reported".
            if let Some(retries) = alloc_retries {
                entry.alloc_retries_last_window = Some(retries);
                entry.alloc_retries_total = Some(
                    entry
                        .alloc_retries_total
                        .unwrap_or(0)
                        .saturating_add(retries),
                );
            }
            if let Some((mb, batch_ms)) = regrow {
                entry.last_regrow_mb = Some(mb);
                entry.last_regrow_batch_ms = batch_ms;
            }
        }
        let fit_sample_count = fit_samples.len();
        let throughput_samples = throughput.len();
        let ceiling_identity = key.clone();
        let cal = state.calibration.entry(key).or_default();
        // The shape ceiling, before anything else this window taught: it is read
        // by the very next grant and by the ramp accounting this settle is about
        // to do, and unlike the fit or the knee it needs no ring and no refit.
        let shape_ceiling = profile.and_then(|(canvas_pixels, max_tokens, epoch)| {
            update_shape_ceiling(
                cal,
                canvas_pixels,
                max_tokens,
                epoch,
                index_limit_to,
                ran_wider_uncut,
                Instant::now(),
            )
            .map(|change| ShapeCeilingEvent {
                inference_id: ceiling_identity.0.clone(),
                gpu: ceiling_identity.1.clone(),
                action: change.action,
                units: change.units,
                previous_units: change.previous_units,
                cause: change.cause,
                canvas_pixels,
                max_tokens,
                epoch,
                previous_age_secs: change.previous_age_secs,
            })
        });
        for sample in fit_samples {
            // One entry per distinct `units`, refreshed in place: allocated peaks
            // reproduce, so a newer reading loses nothing, and a steady state at
            // one size can no longer evict the ramp's diverse points.
            if let Some(pos) = cal
                .samples
                .iter()
                .position(|held| held.units == sample.units)
            {
                cal.samples.remove(pos);
            }
            cal.samples.push_back(sample);
            while cal.samples.len() > FIT_RING {
                cal.samples.pop_front();
            }
        }
        for sample in margin_samples {
            // Same one-entry-per-distinct-`units` rule as the fit ring, for the
            // same reason and one more: `pool_margin_locked` reads the
            // largest-`units` entry, and small batches carry a lower ratio, so a
            // long steady state at one small size must not evict the ramp's
            // largest sample and drop the price.
            if let Some(pos) = cal.margin_ring.iter().position(|held| held.0 == sample.0) {
                cal.margin_ring.remove(pos);
            }
            cal.margin_ring.push_back(sample);
            while cal.margin_ring.len() > FIT_RING {
                cal.margin_ring.pop_front();
            }
        }
        // The ratchet counts only *local* clean priced batches. Ahead of the
        // throughput ring so this window's own samples are stamped with the
        // anchor **including** this window's largest batch: a sample and the
        // largest size measured by the time it was taken have to be read off the
        // same instant, or a ramp step would look like evidence against itself.
        let clean_window = !window_failed && !saw_oom;
        let reached_anchor = anchor > 0 && anchor >= cal.max_units_measured;
        if reached_anchor {
            cal.max_units_measured = anchor;
            // Local evidence has reached the seeded claim, so the anchor is this
            // GPU's own and stops being the OOM backstop's business — but only
            // out of a window that did not fail, here or in its own batches.
            if clean_window {
                cal.anchor_measured_here = true;
            }
        }
        // What this GPU actually ran, whether or not the conferred anchor was
        // ever reached — a host squeezed to 295 units under a seeded 3 072 keeps
        // its 295 — and only out of a window that ran at its budget
        // ([`Ingested::at_budget`]): a window the queue sized to one item lowers
        // `budget_floor` with it, so its one unit would otherwise stand as the
        // largest size this replica ran, and hold the ramp there for the job.
        if clean_window
            && anchor > cal.max_units_measured_here
            && (!queue_bound || byte_bound)
            && (reached_anchor || budget_floor.is_some_and(|floor| anchor >= floor))
        {
            cal.max_units_measured_here = anchor;
        }
        // Every observation is stamped with its place in this pair's stream and
        // with the anchor in force, which makes "taken after the widening" and
        // "taken while the ramp was still climbing" decidable per sample rather
        // than per ring. The post-widening guard itself lives in [`fit_knee`].
        for mut sample in throughput {
            sample.seq = cal.throughput_seq;
            sample.anchor = cal.max_units_measured;
            cal.throughput_seq = cal.throughput_seq.saturating_add(1);
            cal.throughput.push_back(sample);
            while cal.throughput.len() > KNEE_RING {
                cal.throughput.pop_front();
            }
        }
        // And so does the confirmation gate: every sample counted here was
        // measured on this machine, which is what confirms a profile this machine
        // did not produce. Ingested samples, not ring entries — the ring keeps
        // one per distinct size, and confirmation is about how much this machine
        // has seen.
        cal.local_samples = cal
            .local_samples
            .saturating_add(fit_sample_count.min(u32::MAX as usize) as u32);
        Ingested {
            negative,
            fit_samples: fit_sample_count,
            at_budget: !queue_bound && budget_floor.is_some_and(|floor| anchor >= floor),
            throughput_samples,
            oom: saw_oom,
            throughput_collapse: saw_collapse,
            oom_evidence: trusted_oom,
            oom_samples: trusted_ooms,
            clamps,
            shape_ceiling,
            alloc_retries,
        }
    }

    pub(super) fn refit_locked(state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get(&key) else {
            return;
        };
        let samples: Vec<FitSample> = cal.samples.iter().copied().collect();
        let previous = cal.fit;
        let Some(mut fit) = robust_fit(&samples) else {
            return;
        };
        // "Changed" has to mean the whole snapshot, not just the slope: the
        // intercept and the residual ride the wire too, and a refit moving only
        // those would otherwise never reach the worker or bump the version the
        // store's write policy watches.
        let unchanged = previous.is_some_and(|old| {
            (old.slope_mb_per_unit - fit.slope_mb_per_unit).abs() < f64::EPSILON
                && (old.intercept_mb - fit.intercept_mb).abs() < f64::EPSILON
                && (old.residual_mb - fit.residual_mb).abs() < f64::EPSILON
                && old.samples == fit.samples
        });
        if unchanged {
            return;
        }
        state.next_fit_version += 1;
        fit.version = state.next_fit_version;
        if let Some(cal) = state.calibration.get_mut(&key) {
            cal.fit = Some(fit);
            // Computed from this machine's own ring: from here on the fit may be
            // persisted as local evidence.
            cal.fit_is_local = true;
        }
        // Under the lock for the same reason `pending_update_locked`'s line is:
        // the `unchanged` gate above has already returned for every settle that
        // re-derived the same fit, so this is a change event.
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            slope_mb_per_unit = fit.slope_mb_per_unit,
            intercept_mb = fit.intercept_mb,
            residual_mb = fit.residual_mb,
            samples = fit.samples,
            version = fit.version,
            "refitted the memory cost model"
        );
    }

    /// The fit snapshot to attach to the next request frame, or `None` when this
    /// worker already has the current one. Snapshots ride request frames, so
    /// "changed since last send" is tracked per worker.
    pub(super) fn fit_to_send(&self, worker: WorkerId) -> Option<FitSnapshot> {
        let mut state = self.lock();
        let entry = state.workers.get(&worker)?;
        let sent = entry.fit_version_sent;
        let fit = Self::fit_locked(&state, entry)?;
        if fit.version <= sent {
            return None;
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_version_sent = fit.version;
        }
        Some(fit)
    }
}

/// Robust two-parameter fit of `delta_mb ≈ intercept + slope × units` over
/// fit samples. Theil–Sen: the slope is the **median of all pairwise
/// slopes**, so one contaminated sample moves the median by one rank rather than
/// by its magnitude; the intercept is the median of `y − slope·x` and the
/// residual the median absolute deviation from the fitted line, which is the
/// confidence number margins widen on. O(n²) in the sample ring.
///
/// `None` for degenerate inputs: fewer than [`MIN_FIT_SAMPLES`] samples, no two
/// samples with distinct unit counts, or a non-positive fitted slope.
pub(super) fn robust_fit(samples: &[FitSample]) -> Option<FitSnapshot> {
    if samples.len() < MIN_FIT_SAMPLES {
        return None;
    }
    let mut slopes: Vec<f64> = Vec::new();
    for (index, left) in samples.iter().enumerate() {
        for right in &samples[index + 1..] {
            let dx = right.units as f64 - left.units as f64;
            if dx == 0.0 {
                continue;
            }
            slopes.push((right.delta_mb as f64 - left.delta_mb as f64) / dx);
        }
    }
    let slope = median(&mut slopes)?;
    if !slope.is_finite() || slope <= 0.0 {
        return None;
    }
    let mut intercepts: Vec<f64> = samples
        .iter()
        .map(|sample| sample.delta_mb as f64 - slope * sample.units as f64)
        .collect();
    let intercept = median(&mut intercepts)?;
    let mut residuals: Vec<f64> = samples
        .iter()
        .map(|sample| (sample.delta_mb as f64 - (intercept + slope * sample.units as f64)).abs())
        .collect();
    let residual = median(&mut residuals).unwrap_or(0.0);
    Some(FitSnapshot {
        slope_mb_per_unit: slope,
        intercept_mb: intercept,
        residual_mb: residual,
        samples: samples.len(),
        // Assigned by the caller, which owns the monotonic counter.
        version: 0,
    })
}
