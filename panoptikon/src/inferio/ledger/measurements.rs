//! Ingesting batch telemetry: the cost fit, the throughput ring, the shape
//! ceiling.

use super::*;

/// Whether a window's batches may feed the throughput ring: not when it ran
/// unpriced (`mb == 0`) or under memory pressure. A window the room or host
/// RAM cut is admitted; its budget is what the device ran. Excluded windows
/// still feed the cost fit.
pub(super) fn ring_admits_window(charge: &GrantCharge) -> bool {
    charge.mb > 0 && charge.pressure == mps::MemoryPressure::Normal
}

/// Add a fit sample to a ring holding at most one per distinct `units`, so a
/// steady state at one size cannot evict the other sizes' points.
fn push_fit_sample(ring: &mut VecDeque<FitSample>, sample: FitSample) {
    if let Some(pos) = ring.iter().position(|held| held.units == sample.units) {
        ring.remove(pos);
    }
    ring.push_back(sample);
    while ring.len() > FIT_RING {
        ring.pop_front();
    }
}

/// The host RAM a GPU replica books, from its samples. Every size is priced
/// at an upper bound, since the per-unit cost varies with the input and this
/// is a safety ceiling; no batch books less than the costliest one measured
/// at a size no larger, or than the smallest one measured.
///
/// Up to the largest batch measured it books the Theil–Sen fit, once two
/// sizes ran: its fixed part plus per unit the largest cost above it among
/// batches within [`RATCHET_FACTOR`] of the largest, or the slope if higher.
/// Past the largest, a fixed part read too high would leave part of the
/// per-unit cost out, so each of those batches is extended from its own
/// growth at its whole growth per unit, the highest of them, or the slope if
/// higher. Past [`RamCost::fitted_reach`], where a few small batches price a
/// far larger one, the rate is at least the one-size rate below.
///
/// Samples are measured over a load level that includes what a replica's
/// first batch (`first_units`) kept (`first_kept_mb`), which may be that
/// batch's own memory, reused by later ones. So batches no larger than the
/// first are left out, and a batch's one-size rate
/// is the lower of its growth plus the kept memory over its units and its
/// growth over the units beyond the first batch's: either bounds the cost per
/// unit, whichever the kept memory is, when every input costs the same. A
/// one-size cost books its growth up to the size measured and is extended at
/// that rate. `None` with no sample, or a per-unit cost of 0: unknown, not
/// free.
pub(super) fn ram_cost(
    samples: &[FitSample],
    first_units: u64,
    first_kept_mb: u64,
) -> Option<RamCost> {
    let mut samples: Vec<FitSample> = samples
        .iter()
        .copied()
        .filter(|sample| sample.units > first_units)
        .collect();
    samples.sort_by_key(|sample| sample.units);
    let largest = samples.last()?.units;
    let fit = theil_sen(&samples);
    let fixed_mb = fit.map_or(0.0, |fit| fit.intercept_mb.max(0.0));
    let slope = fit.map_or(0.0, |fit| fit.slope_mb_per_unit);
    let mut mb_per_unit = slope;
    let mut lines = Vec::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.units.saturating_mul(RATCHET_FACTOR) >= largest)
    {
        let (units, delta) = (sample.units as f64, sample.delta_mb as f64);
        let whole = delta / units;
        let one_size = ((delta + first_kept_mb as f64) / units)
            .min(delta / (sample.units - first_units) as f64)
            .max(whole);
        let (fitted, near) = match fit {
            Some(_) => ((delta - fixed_mb) / units, whole),
            None => (one_size, one_size),
        };
        mb_per_unit = mb_per_unit.max(fitted);
        lines.push(RamLine {
            units: sample.units,
            delta_mb: delta,
            near_mb_per_unit: near.max(slope),
            far_mb_per_unit: one_size.max(slope),
        });
    }
    let mut floor: Vec<FitSample> = Vec::new();
    for sample in samples {
        if floor
            .last()
            .is_none_or(|top| sample.delta_mb > top.delta_mb)
        {
            floor.push(sample);
        }
    }
    Some(RamCost {
        fixed_mb,
        mb_per_unit,
        lines,
        floor,
        startup_mb: 0.0,
        fitted: fit.is_some(),
        measured_units: largest,
    })
    .filter(|cost| cost.mb_per_unit > 0.0)
}

/// The clamp reason for a non-memory kernel limit; it feeds the per-(model,
/// GPU) [`ShapeCeiling`]. Any other reason is treated like the memory clamp.
pub(super) const CLAMP_REASON_INDEX_LIMIT: &str = "index_limit";

/// Whether this measurement was cut by a clamp naming `reason`. A clamp with
/// no reason is the memory clamp and matches no named reason.
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

/// What one settle did to a (model, GPU)'s [`ShapeCeiling`]; logged at INFO
/// once per change, never per window.
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
    /// One of the `CEILING_CAUSE_*` constants.
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
             and no larger size is tried beyond it. A shape ceiling is not a \
             memory condition and never deflates anything — it is runtime-only \
             state, re-learned after a restart and dropped the moment the \
             canvas, the cost epoch or the corpus moves"
        );
    }
}

/// The `cause` of a [`ShapeCeilingEvent`]: the worker reported the clamp.
pub(super) const CEILING_CAUSE_REPORTED: &str = "index_limit_clamp";
/// The replica's canvas, token window or cost epoch changed since the ceiling
/// was observed.
pub(super) const CEILING_CAUSE_PROFILE: &str = "canvas_or_epoch_changed";
/// A batch larger than the ceiling ran without the impl cutting it.
pub(super) const CEILING_CAUSE_RAN_WIDER: &str = "ran_wider_uncut";

/// Fold this window's `index_limit` evidence into a (model, GPU)'s shape
/// ceiling, returning what changed. Rules, in order: clear on a canvas, token
/// window or epoch change; clear when a larger batch ran uncut; record or
/// lower on a clamp; never raise in place. See docs/batch-calibration-design.md,
/// "Shape ceiling: the third brake".
pub(super) fn update_shape_ceiling(
    cal: &mut ModelCalibration,
    canvas_pixels: Option<u32>,
    max_tokens: Option<u32>,
    epoch: u32,
    reported: Option<u64>,
    ran_wider_uncut: u64,
    now: Instant,
) -> Option<ShapeCeilingChange> {
    // `(cause, previous units, previous age in seconds)` of a cleared ceiling.
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
        // A clamp with nothing standing is a `set`; one below the standing
        // ceiling is a `lowered`.
        (Some(units), current) if current.is_none_or(|standing| units < standing.units) => {
            cal.shape_ceiling = Some(ShapeCeiling {
                units,
                canvas_pixels,
                max_tokens,
                epoch,
                observed_at: now,
            });
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
        // A clamp at or above the standing ceiling changes nothing.
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
    /// Drain this worker's new telemetry into the ledger by watermark.
    /// `window` is the settling window's grant; it gates the throughput ring,
    /// `max_units_measured_here` and the contention tag (no window counts as
    /// busy). The cost fit and the anchor take every clean priced batch.
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
        let mut ram_at_load = entry.ram_at_load_mb;
        let mut ram_started = entry.ram_started;
        let known_startup = cal_locked(state, entry).map_or(0, |cal| cal.ram_startup_mb);
        // What the replica's first batch kept: start-up memory.
        let mut startup_mb: Option<u64> = None;
        let mut first_units = 0u64;
        let mut ram_before = entry.ram_resident_mb();
        let mut ram_base = entry.ram_base_mb;

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
        // An oldest retained sequence past the watermark means the ring
        // evicted measurements before this read.
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

        // A base that arrives after registration is recorded once.
        if !base_recorded && let Some(base) = load.as_ref().and_then(|report| report.base_mb) {
            if let Some(entry) = state.workers.get_mut(&worker) {
                entry.base_mb = Some(base);
                entry.base_recorded = true;
            }
            state.remembered_bases.insert(key.clone(), Some(base));
        }
        if reserved_at_load.is_none() {
            reserved_at_load = load
                .as_ref()
                .and_then(|report| pool_at_load_mb(&gpu, report));
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

        // The response's GPU total also validates the per-batch free readings.
        let reported_total_mb = memory.as_ref().and_then(|stamped| stamped.value.total_mb);
        let model = state
            .workers
            .get(&worker)
            .map(|entry| entry.inference_id.clone());
        // The canvas, token window and epoch this replica's clamps are
        // measured in, fixed for its life. A forgotten replica sets no ceiling.
        let profile = state
            .workers
            .get(&worker)
            .map(|entry| (entry.canvas_pixels, entry.max_tokens, entry.epoch));

        let mut negative = false;
        let mut saw_oom = false;
        let mut saw_collapse = false;
        let mut saw_spill = false;
        let mut new_watermark = watermark;
        let mut fit_samples: Vec<FitSample> = Vec::new();
        let mut ram_samples: Vec<FitSample> = Vec::new();
        let mut ram_after: Option<(u64, Instant)> = None;
        let mut margin_samples: Vec<(u64, f64)> = Vec::new();
        let mut throughput: Vec<ThroughputSample> = Vec::new();
        let mut anchor = 0u64;
        // "Ran at its budget", for the gain rule and the throughput ring:
        // [`FULL_BATCH_RATIO`] of the admitted (post-squeeze) unit budget, or
        // a batch the next item would have pushed past it.
        let budget_floor = window
            .map(|charge| ((charge.unit_budget as f64 * FULL_BATCH_RATIO).ceil() as u64).max(1));
        let full_batch =
            budget_floor.filter(|_| window.is_some_and(|charge| ring_admits_window(&charge)));
        // A clean priced batch of this window ran at its budget.
        let mut ran_full = false;
        // A window the queue sized did not run at its budget; its batches
        // still feed the throughput ring.
        let queue_bound = window.is_none_or(|charge| charge.queue_bound);
        // A window the byte wall closed still counts toward
        // `max_units_measured_here`, but is no window at its budget.
        let byte_bound = window.is_some_and(|charge| charge.byte_bound);
        // A window host RAM sized counts toward neither; nor does one run
        // under memory pressure, which also feeds no throughput sample.
        let ram_bound = window.is_some_and(|charge| charge.ram_bound);
        let pressure = window.is_some_and(|charge| charge.pressure != mps::MemoryPressure::Normal);
        let item_capped = window.is_some_and(|charge| charge.item_cap.is_some());
        // The largest units per item an item-capped batch ran, if one ran,
        // and whether a batch filled the cap.
        let mut item_units: Option<u64> = None;
        let mut filled_cap = false;
        // Contention tag for the throughput samples and the collapse verdict. No
        // window counts as contended.
        let occupants = window
            .map(|charge| charge.peak_occupants)
            .unwrap_or(u32::MAX);
        let sole_occupancy = occupants == 0;
        // The replica's first settled window is warm-up.
        let warmup_window = state
            .workers
            .get(&worker)
            .is_none_or(|entry| entry.settled_windows == 0);
        // Batches run before this window, for [`KNEE_WARMUP_BATCHES`].
        let mut ran_batches = state
            .workers
            .get(&worker)
            .map_or(0, |entry| entry.ran_batches);
        let mut suppressed_collapses = 0usize;
        let mut pressure_collapses = 0usize;
        // `(free at failure, granted envelope)` per contradicted OOM.
        let mut contradicted_ooms: Vec<(u64, u64)> = Vec::new();
        // Trusted OOMs, for the negative's log line: the first and the count.
        let mut trusted_oom: Option<OomEvidence> = None;
        let mut trusted_ooms = 0usize;
        // Every clamp reason this window reported, for the settle line.
        let mut clamps: Vec<Option<String>> = Vec::new();
        // Shape-ceiling evidence: the smallest `index_limit` `to_units`, and
        // the largest batch that ran uncut.
        let mut index_limit_to: Option<u64> = None;
        let mut ran_wider_uncut = 0u64;
        // Summed over the window, `None` while no batch reported the counter.
        let mut alloc_retries: Option<u64> = None;
        // `(MiB the pool grew back, that batch's wall time)` after a release
        // the host asked for.
        let mut regrow: Option<(u64, Option<f64>)> = None;
        // Collapse verdicts dropped: batch cut by the shape ceiling, or not
        // corroborated by its memory figures.
        let mut clipped_collapses = 0usize;
        let mut uncorroborated_collapses = 0usize;
        // The window's time from grant to settle over the time its batches
        // ran, at least 1: each batch is charged its share of the rest.
        let batch_ms: f64 = samples
            .iter()
            .filter_map(|sample| sample.measurement.duration_ms)
            .sum();
        let wall_ratio = window
            .map(|charge| charge.granted_at.elapsed().as_secs_f64() * 1000.0 / batch_ms)
            .filter(|ratio| ratio.is_finite())
            .map_or(1.0, |ratio| ratio.max(1.0));
        for sample in samples {
            new_watermark = new_watermark.max(sample.seq);
            let measurement = &sample.measurement;
            if let Some(retries) = measurement.alloc_retries {
                alloc_retries = Some(alloc_retries.unwrap_or(0).saturating_add(retries));
            }
            // A reactive shrink's re-grow is not counted.
            if let Some(mb) = measurement.regrow_mb
                && measurement.regrow_after.as_deref() == Some(HOST_ASKED_RELEASE)
            {
                regrow = Some((mb, measurement.duration_ms));
            }
            // Per-batch free reading, taken before the batch. Recorded before
            // the negative check: an OOM window needs the freshest reading.
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
                    // The RAM domain the reading was clipped from, if stated.
                    RamBasis::of_batch(measurement),
                );
            }
            // This replica's pool as the batch left it, never its peak: a peak
            // never falls back, so `external` would decay under a real hog.
            // On the CPU device that is the resident set after the batch.
            let pool = if gpu == cpu::DEVICE_KEY {
                measurement.rss_after_mb
            } else {
                measurement
                    .reserved_after_mb
                    .or(measurement.peak_reserved_mb)
            };
            if let Some(pool) = pool
                && let Some(entry) = state.workers.get_mut(&worker)
                && entry
                    .reserved_seen_at
                    .is_none_or(|at| sample.captured_at > at)
            {
                entry.reserved_mb = Some(pool);
                entry.reserved_seen_at = Some(sample.captured_at);
            }
            // A GPU replica's resident set before this batch, and as the
            // window left it.
            let (batch_ram_before, batch_ram_base) = (ram_before, ram_base);
            let mut first_batch = false;
            if let Some(rss) = measurement.rss_after_mb.filter(|_| ram_at_load.is_some()) {
                ram_after = Some((rss, sample.captured_at));
                // What the first batch after load keeps is start-up (libraries,
                // kernels, allocator set-up): it joins the load level and
                // gives no sample. Once the cost is known, only up to the
                // start-up measured before counts as such.
                if !ram_started {
                    ram_started = true;
                    first_batch = true;
                    let kept = rss.saturating_sub(ram_before);
                    let startup = if item_capped {
                        startup_mb = Some(kept);
                        first_units = measurement.units.unwrap_or(0);
                        kept
                    } else {
                        kept.min(known_startup)
                    };
                    let level = ram_before + startup;
                    ram_at_load = ram_at_load.map(|at_load| at_load.max(level));
                    ram_base = Some(level);
                }
                ram_before = rss;
                // Below the baseline the replica released memory (load-time,
                // or only for now): samples are measured from there on. Its
                // resident growth stays measured over the load level, so a
                // dip that comes back cannot inflate its own credit.
                ram_base = ram_base.map(|base| base.min(rss));
            }
            // A collapse verdict counts only from a window with the GPU to
            // itself and a batch the shape ceiling did not cut. A suppressed
            // or uncorroborated collapse drops the sample without deflating.
            let clipped = clamp_reason_is(measurement, CLAMP_REASON_INDEX_LIMIT);
            // Before the negative branch: the clamp states what executed.
            if clipped && let Some(clamp) = &measurement.clamped {
                let to_units = clamp.to_units;
                if to_units > 0 {
                    index_limit_to =
                        Some(index_limit_to.map_or(to_units, |seen: u64| seen.min(to_units)));
                }
            }
            let collapse_suppressed =
                measurement.throughput_collapse && (!sole_occupancy || clipped || pressure);
            if collapse_suppressed {
                if clipped {
                    clipped_collapses += 1;
                } else if pressure {
                    pressure_collapses += 1;
                } else {
                    suppressed_collapses += 1;
                }
            }
            // Corroboration: the pool grew past the free reading taken before
            // the batch ([`pool_grew_past_free`]).
            let uncorroborated = measurement.throughput_collapse
                && !collapse_suppressed
                && !pool_grew_past_free(measurement, Self::free_before_locked(state, &gpu));
            if uncorroborated {
                uncorroborated_collapses += 1;
            }
            let collapse =
                measurement.throughput_collapse && !collapse_suppressed && !uncorroborated;
            // A message-only OOM the free reading contradicts is not a
            // negative ([`oom_verdict`]).
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
            if oom || collapse || measurement.spilled {
                // A negative (trusted OOM, corroborated collapse or spill)
                // deflates and is discarded: its peak under-states the cost,
                // and the anchor must not advance on a failing size.
                negative = true;
                saw_oom |= oom;
                saw_collapse |= collapse;
                saw_spill |= measurement.spilled;
                continue;
            }
            if collapse_suppressed || uncorroborated {
                continue;
            }
            let units = measurement.units.filter(|units| *units > 0);
            // The batch's envelope in host RAM, over the baseline. A batch
            // that peaked no higher than the resident set before it ran in
            // memory kept from an earlier one: its own cost is unknown.
            if let (Some(units), Some(peak), Some(base)) =
                (units, measurement.peak_rss_mb, batch_ram_base)
                && peak > batch_ram_before
                && !first_batch
            {
                ram_samples.push(FitSample {
                    units,
                    delta_mb: peak.saturating_sub(base),
                });
            }
            if item_capped {
                let per_item = units
                    .unwrap_or(1)
                    .div_ceil(measurement.items.filter(|items| *items > 0).unwrap_or(1));
                item_units = Some(item_units.unwrap_or(0).max(per_item));
                filled_cap |= window
                    .and_then(|charge| charge.item_cap)
                    .is_some_and(|cap| measurement.items.unwrap_or(0) >= u64::from(cap));
            }
            // A memory-clamped batch still counts as uncut.
            if !clipped {
                ran_wider_uncut = ran_wider_uncut.max(units.unwrap_or(0));
            }
            // Pool growth compares the post-batch pool (the peak only from an
            // older worker) with the pool before; `None` when either is absent,
            // which is not "warm". On every platform, CUDA included: a mid-batch
            // peak would mark every MPS batch, and every CUDA batch that
            // retried an allocation, as pool-growing. See
            // docs/batch-calibration-design.md, "Batch size: what counts as
            // a measurement".
            let pool_after = measurement
                .reserved_after_mb
                .or(measurement.peak_reserved_mb);
            let grew_pool = match (pool_after, measurement.reserved_before_mb) {
                (Some(after), Some(before)) => Some(after > before),
                _ => None,
            };
            let high_water = grew_pool == Some(true);
            // Every batch that ran counts toward the warm-up, except negatives
            // and dropped collapses (skipped above).
            ran_batches = ran_batches.saturating_add(1);
            // Throughput samples (units/sec) exclude negatives, unpriced
            // batches, batches below the full-batch floor, and clamped
            // batches. All still feed the fit.
            if let Some(clamp) = &measurement.clamped {
                clamps.push(clamp.reason.clone());
            }
            if measurement.clamped.is_none()
                && let (Some(units), Some(duration_ms), Some(full_batch)) =
                    (units, measurement.duration_ms, full_batch)
                && duration_ms > 0.0
                && (units >= full_batch || measurement.next_over_budget)
            {
                throughput.push(ThroughputSample {
                    units,
                    units_per_sec: units as f64 * 1000.0 / (duration_ms * wall_ratio),
                    occupants,
                    grew_pool,
                    warmup: warmup_window || ran_batches <= KNEE_WARMUP_BATCHES,
                });
            }
            // Every clean priced batch is a fit sample of the envelope
            // `peak_allocated − allocated_at_load`, which is what a grant reserves.
            if let (Some(units), Some(peak), Some(at_load)) =
                (units, measurement.peak_allocated_mb, allocated_at_load)
            {
                fit_samples.push(FitSample {
                    units,
                    delta_mb: peak.saturating_sub(at_load),
                });
                anchor = anchor.max(units);
                let full = budget_floor
                    .is_some_and(|floor| units >= floor || measurement.next_over_budget);
                ran_full |= full;
            }
            // Pool-over-allocated ratio, only where the pool grew and the delta
            // reaches [`POOL_MARGIN_MIN_DELTA_MB`].
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
        // One shift for the window, so batches stamped alike cannot skip one.
        if let Some((rss, at)) = ram_after
            && let Some(entry) = state.workers.get_mut(&worker)
        {
            let before = entry.ram_resident_mb();
            entry.ram_mb = Some(rss);
            entry.ram_base_mb = ram_base;
            entry.ram_at_load_mb = ram_at_load;
            entry.ram_started = ram_started;
            Self::shift_free_locked(state, cpu::DEVICE_KEY, before, rss, at);
        }
        // The response-level reading last: it is taken after the final batch.
        if let Some(stamped) = memory {
            if let Some(reserved) = sample_pool_mb(&gpu, &stamped.value)
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
                 would cost throughput and fix nothing"
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
                 explain it and is not evidence about the batch size"
            );
        }
        if pressure_collapses > 0 {
            tracing::debug!(
                model = %key.0,
                gpu = %gpu,
                pressure_collapses,
                "ignored this window's throughput-collapse flags: macOS reported \
                 memory pressure while it ran, so the rate drop is not evidence \
                 about the batch size"
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
                 and never deflates anything"
            );
        }
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.fit_watermark = new_watermark;
            entry.settled_windows = entry.settled_windows.saturating_add(1);
            entry.ran_batches = ran_batches;
            // Doubled after a batch that filled it. It ends once it would
            // hold a seed batch: from there the unit budget bounds the batch
            // as it does for any replica.
            if let Some(item_units) = item_units.filter(|_| filled_cap) {
                let seed_items = entry.seed_units.div_ceil(item_units.max(1));
                entry.item_cap = entry
                    .item_cap
                    .map(|cap| cap.saturating_mul(2))
                    .filter(|cap| u64::from(*cap) < seed_items);
            }
            // A window reporting zero retries is kept: the starvation trigger
            // tells it apart from no report.
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
        // The shape ceiling first.
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
            push_fit_sample(&mut cal.samples, sample);
        }
        if let Some(startup) = startup_mb {
            cal.ram_startup_mb = cal.ram_startup_mb.max(startup);
            cal.ram_first_units = cal.ram_first_units.max(first_units);
        }
        if !ram_samples.is_empty() {
            for sample in ram_samples {
                // The costlier of two batches at one size stays, so the
                // booking covers the costliest input measured there.
                let costliest = cal
                    .ram_samples
                    .iter()
                    .find(|held| held.units == sample.units && held.delta_mb > sample.delta_mb)
                    .copied()
                    .unwrap_or(sample);
                push_fit_sample(&mut cal.ram_samples, costliest);
            }
            cal.ram_cost = ram_cost(
                cal.ram_samples.make_contiguous(),
                cal.ram_first_units,
                cal.ram_startup_mb,
            );
        }
        for sample in margin_samples {
            // Same rule: `pool_margin_locked` reads the largest-`units` entry.
            if let Some(pos) = cal.margin_ring.iter().position(|held| held.0 == sample.0) {
                cal.margin_ring.remove(pos);
            }
            cal.margin_ring.push_back(sample);
            while cal.margin_ring.len() > FIT_RING {
                cal.margin_ring.pop_front();
            }
        }
        // The ratchet counts local clean priced batches.
        let clean_window = !window_failed && !saw_oom;
        let reached_anchor = anchor > 0 && anchor >= cal.max_units_measured;
        if reached_anchor {
            cal.max_units_measured = anchor;
            // Local evidence reached the seeded anchor in a clean window.
            if clean_window {
                cal.anchor_measured_here = true;
            }
        }
        // The largest size this GPU ran, from a clean window at its budget
        // (or closed by the byte wall), whether or not the seeded anchor was
        // reached.
        if clean_window
            && anchor > cal.max_units_measured_here
            && (!queue_bound || byte_bound)
            && !ram_bound
            && !pressure
            && (reached_anchor || ran_full)
        {
            cal.max_units_measured_here = anchor;
        }
        for sample in throughput {
            cal.throughput.push_back(sample);
            while cal.throughput.len() > KNEE_RING {
                cal.throughput.pop_front();
            }
        }
        // Local samples ingested, not ring entries, for the confirmation gate.
        cal.local_samples = cal
            .local_samples
            .saturating_add(fit_sample_count.min(u32::MAX as usize) as u32);
        Ingested {
            negative,
            fit_samples: fit_sample_count,
            at_budget: !queue_bound && !pressure && ran_full,
            filled: !queue_bound && !ram_bound && ran_full,
            throughput_samples,
            oom: saw_oom,
            throughput_collapse: saw_collapse,
            spill: saw_spill,
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
        // Compare the whole snapshot: intercept and residual are sent too.
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
            // From this machine's own ring, so it may be persisted as local.
            cal.fit_is_local = true;
        }
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

/// Theil–Sen fit of `delta_mb ≈ intercept + slope × units`: slope is the
/// median pairwise slope, intercept the median of `y − slope·x`, residual the
/// median absolute deviation. `None` with fewer than [`MIN_FIT_SAMPLES`]
/// samples, no two distinct unit counts, or a non-positive slope.
pub(super) fn robust_fit(samples: &[FitSample]) -> Option<FitSnapshot> {
    if samples.len() < MIN_FIT_SAMPLES {
        return None;
    }
    theil_sen(samples)
}

/// The intercept of the line of `slope` through `samples`: the median of
/// `delta − slope × units`. `None` without samples.
pub(super) fn intercept_at(samples: &[FitSample], slope: f64) -> Option<f64> {
    let mut intercepts: Vec<f64> = samples
        .iter()
        .map(|sample| sample.delta_mb as f64 - slope * sample.units as f64)
        .collect();
    median(&mut intercepts)
}

/// [`robust_fit`] from any two distinct unit counts on.
fn theil_sen(samples: &[FitSample]) -> Option<FitSnapshot> {
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
    let intercept = intercept_at(samples, slope)?;
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
