use super::*;

/// The throughput knee reached its expiry and was re-widened (or withdrawn).
/// Owns its strings so the line is formatted after the lock is dropped.
pub(super) struct KneeExpired {
    inference_id: String,
    gpu: String,
    from_units: u64,
    /// `None` when the widened cap could no longer bind and the knee was
    /// withdrawn outright.
    to_units: Option<u64>,
    windows: u32,
    granted_units: u64,
}

impl KneeExpired {
    pub(super) fn emit(self) {
        match self.to_units {
            Some(to_units) => tracing::info!(
                model = %self.inference_id,
                gpu = %self.gpu,
                knee_units_before = self.from_units,
                knee_units_after = to_units,
                clean_windows_at_the_knee = self.windows,
                last_grant_units = self.granted_units,
                "this model has run cleanly at its throughput knee for long \
                 enough, with memory to spare, that the knee is worth \
                 re-testing; widening the cap by one batch-size step. A knee \
                 is a brake, not a ceiling: if the curve really does flatten \
                 here, the next fit from honest samples puts it back"
            ),
            None => tracing::info!(
                model = %self.inference_id,
                gpu = %self.gpu,
                knee_units_before = self.from_units,
                clean_windows_at_the_knee = self.windows,
                last_grant_units = self.granted_units,
                "this model's throughput knee has widened past the point where \
                 it could cap anything and has been withdrawn; the ramp and \
                 the extrapolation ratchet govern its batch size from here"
            ),
        }
    }
}

impl VramLedger {
    /// Advance (or reset) the knee's expiry counter for one settled window, and
    /// widen the knee when it has been earned.
    ///
    /// A window counts only when all four hold: it responded, it was clean, the
    /// **knee** is what held its batch size back, and the requester's own room
    /// (headroom plus its own free pool) held [`RATCHET_FACTOR`] times this
    /// model's appetite while it ran. A negative
    /// window resets the counter. The widening is by one log2 bucket, and once
    /// the widened cap can no longer bind (it has reached [`uncapped_units`]) the
    /// knee is **withdrawn** outright.
    ///
    /// **Both branches leave [`ModelCalibration::knee_widened`] set**, withdrawal
    /// being a widening to infinity: the ring at that instant is what it was
    /// under the old cap, so a refit later in this same settle would otherwise
    /// reinstall the number that just expired. See the design doc, R1 (d).
    pub(super) fn note_knee_window_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        negative: bool,
    ) -> Option<KneeExpired> {
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let anchor = Self::anchor_locked(state, entry);
        // The budget with no knee and no deflation in it: what a widened knee
        // has to reach before it stops being able to cap anything.
        let ceiling = uncapped_units(entry, anchor);
        let cal = state.calibration.get_mut(&key)?;
        let knee = cal.knee_units.filter(|knee| *knee > 0)?;
        if negative {
            cal.knee_clean_windows = 0;
            return None;
        }
        let charge = charge.filter(|charge| charge.knee_bound && charge.ample_headroom)?;
        // A knee this process never measured is **provisional** and is re-tested
        // far sooner. "Never measured here" is exactly `!knee_is_local`: the
        // store and seed paths set it false, and only `refit_knee_locked` sets
        // it true.
        let expiry = if cal.knee_is_local {
            KNEE_EXPIRY_CLEAN_WINDOWS
        } else {
            KNEE_SEED_REVALIDATION_WINDOWS
        };
        cal.knee_clean_windows = cal.knee_clean_windows.saturating_add(1);
        if cal.knee_clean_windows < expiry {
            return None;
        }
        let windows = cal.knee_clean_windows;
        cal.knee_clean_windows = 0;
        // `knee` is `2^(b+1) − 1`; the top of the next bucket is `2k + 1`, and
        // it cannot overflow for any knee the fit can produce (`b < 63`).
        let widened = knee.saturating_mul(2).saturating_add(1);
        let withdrawn = widened >= ceiling;
        if withdrawn {
            cal.knee_units = None;
            cal.knee_fitted_units = None;
            cal.knee_is_local = false;
            // The store keeps whatever knee is on disk when an update brings
            // none, so the withdrawal has to be stated here, where it happens: a
            // knee this run *seeded* is not `knee_is_local`, and its
            // disappearance is otherwise "this run fitted none".
            cal.knee_withdrawn = true;
        } else {
            cal.knee_units = Some(widened);
        }
        // The samples in the ring were all taken under the old cap, so a refit
        // would hand the same number straight back. The model has to run at the
        // wider size first, and a withdrawal is a widening with no upper bound,
        // so it waits on the same evidence.
        cal.knee_widened = Some(KneeWidening {
            bucket: size_bucket(knee),
            from_seq: cal.throughput_seq,
        });
        Some(KneeExpired {
            inference_id: key.0,
            gpu: key.1,
            from_units: knee,
            to_units: (!withdrawn).then_some(widened),
            windows,
            granted_units: charge.unit_budget,
        })
    }

    pub(super) fn refit_knee_locked(&self, state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let band = self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force();
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get(&key) else {
            return;
        };
        // Sole-occupancy samples only: a rate measured while a neighbour was
        // running windows on the same GPU is a rate for *that* GPU state. The
        // tag is carried rather than filtered at ingest, so `/health`'s
        // `throughput_samples` still reports everything.
        let samples: Vec<ThroughputSample> = cal
            .throughput
            .iter()
            .filter(|sample| sample.occupants == 0)
            .copied()
            .collect();
        let floor = cal.knee_best.map(|(_, rate)| rate).unwrap_or(0.0);
        // The ratchet anchor and the widening mark are inputs to the fit, not
        // post-hoc filters on it: they are per-sample tests inside a bucket, so
        // only the fit can apply them. Either one disqualifying the candidate
        // refuses the whole fit. See [`fit_knee`] rules 4 and 5.
        let Some(fit) = fit_knee(
            &samples,
            floor,
            cal.max_units_measured,
            cal.knee_widened,
            band,
        ) else {
            return;
        };
        let previous = cal.knee_units;
        let unchanged = cal.knee_units == fit.knee_units && cal.knee_is_local;
        let Some(cal) = state.calibration.get_mut(&key) else {
            return;
        };
        // The anchor moves *before* the knee decision short-circuits: a refit
        // that produced no knee still witnessed this ring's peak, and that is
        // the number later fits are held to.
        if fit.best.1 > floor {
            cal.knee_best = Some(fit.best);
        }
        let Some(knee) = fit.knee_units else {
            return;
        };
        if unchanged {
            return;
        }
        cal.knee_units = Some(knee);
        // The number the store gets: the widenings below move `knee_units` and
        // leave this one where the ring put it.
        cal.knee_fitted_units = Some(knee);
        // This run measured it, so it may travel to the store — and it is no
        // longer *provisional*, which is what a seeded knee is until this
        // machine's own observations have spoken.
        cal.knee_is_local = true;
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            knee_units = knee,
            previous = ?previous,
            observations = samples.len(),
            "fitted a throughput knee; batches larger than this are no longer \
             admitted however much memory is free"
        );
    }
}

/// Which log2 bucket a batch size falls in. Buckets are the natural x axis
/// because the ramp is geometric: a linear binning would leave every bucket but
/// one empty, and a per-size grouping one sample per group.
pub(super) fn size_bucket(units: u64) -> u32 {
    units.max(1).ilog2()
}

/// The ring's rates grouped by log2 batch-size bucket, as `(units/sec, the
/// ratchet anchor when it was taken, its sequence number)`. The two tags ride
/// along because [`fit_knee`]'s rules 4 and 5 are per-sample tests inside a
/// bucket. Warm-up and non-finite samples are dropped here, before any rule.
///
/// `drop_tail` additionally drops [`ThroughputSample::warmup_tail`], which only
/// [`fit_knee`] does: a permanent cap read off a median may not be read off the
/// runtime still settling, while the ramp needs the ring to hold something.
pub(super) fn bucket_rates(
    samples: &[ThroughputSample],
    drop_tail: bool,
) -> BTreeMap<u32, Vec<(f64, u64, u64)>> {
    let mut buckets: BTreeMap<u32, Vec<(f64, u64, u64)>> = BTreeMap::new();
    for sample in samples {
        if !sample.units_per_sec.is_finite()
            || sample.units_per_sec <= 0.0
            || sample.warmup
            || (drop_tail && sample.warmup_tail)
        {
            continue;
        }
        buckets.entry(size_bucket(sample.units)).or_default().push((
            sample.units_per_sec,
            sample.anchor,
            sample.seq,
        ));
    }
    buckets
}

/// One median units/sec per bucket, in size order — and `None` when any bucket
/// disagrees with itself by more than `band`, since something outside this
/// ledger was moving throughput while those rates were taken and no rule may
/// read them. `band` is this device's
/// ([`VramBudget::knee_dispersion_in_force`]): a quiet CPU's throughput floor
/// is an order of magnitude above a quiet GPU's.
pub(super) fn quiet_medians(
    buckets: &BTreeMap<u32, Vec<(f64, u64, u64)>>,
    band: f64,
) -> Option<Vec<(u32, f64)>> {
    let mut medians: Vec<(u32, f64)> = Vec::with_capacity(buckets.len());
    for (bucket, rates) in buckets {
        let mut only_rates: Vec<f64> = rates.iter().map(|(rate, _, _)| *rate).collect();
        medians.push((*bucket, median(&mut only_rates).unwrap_or(0.0)));
        let dispersion = relative_mad(&mut only_rates)?;
        if dispersion > band {
            tracing::debug!(
                bucket,
                observations = rates.len(),
                dispersion,
                threshold = band,
                "declining to read this model's throughput curve: the \
                 observations in one batch-size bucket disagree with each other \
                 by more than the knee's own decision band, so something \
                 outside this ledger was moving throughput while they were taken"
            );
            return None;
        }
    }
    Some(medians)
}

/// Whether the [`KNEE_PLATEAU_BUCKETS`] doublings *immediately* above `bucket`
/// beat `rate`, and `None` when one of them holds no quiet observation: a
/// bucket the ring never measured is **unknown**, which is neither a plateau
/// nor a gain, and the two callers need to tell those apart.
pub(super) fn plateau_above(medians: &[(u32, f64)], bucket: u32, rate: f64) -> Option<bool> {
    let mut flat = true;
    for step in 1..=KNEE_PLATEAU_BUCKETS as u32 {
        let (_, other_rate) = medians.iter().find(|(other, _)| *other == bucket + step)?;
        flat &= rate >= *other_rate * KNEE_RATIO;
    }
    Some(flat)
}

/// The plateau a knee at `bucket` claims: the doublings immediately above it
/// all measured, and none of them faster. [`fit_knee`]'s rule 2/4 exception,
/// where an unmeasured doubling withholds the exception exactly as a faster one
/// does — the claim is unproven either way.
pub(super) fn flat_above(medians: &[(u32, f64)], bucket: u32, rate: f64) -> bool {
    plateau_above(medians, bucket, rate) == Some(true)
}

/// Fit the throughput knee: the smallest batch size at which the model is
/// already within [`KNEE_RATIO`] of the best units/sec it has ever shown.
///
/// The curve is summarized as a **median per log2 bucket**, so one batch that
/// raced a compositor redraw cannot move a permanent cap. `floor_rate` is the
/// best bucket median this model has shown in any earlier fit: the live ring
/// ages by eviction and a knee removes the very sizes that set the peak, so the
/// threshold is taken against `max(ring best, floor_rate)`.
///
/// Two gates decide whether the ring may be read as a curve at all:
/// [`MIN_KNEE_SAMPLES`] observations across at least [`MIN_KNEE_BUCKETS`]
/// distinct **quiet** buckets. Then five rules decide where the knee may go, all
/// of them one principle: *a knee is a claim about the curve above it, and may
/// only be made from honest, quiet samples taken in the regime the model is in.*
/// (1) the frontier must be quiet and the knee may not be it; (2) the floor must
/// be interior too, unless the [`KNEE_PLATEAU_BUCKETS`] doublings immediately
/// above the candidate were all measured flat ([`flat_above`]), which exempts it
/// from rule 4 as well, at the floor and anywhere above it;
/// (3) [`KNEE_PLATEAU_BUCKETS`] quiet buckets must lie strictly
/// above the candidate; (4) no ramp-era knee below the anchor; (5) after a
/// widening, the evidence must be newer than the widening
/// ([`ModelCalibration::knee_widened`]). Samples marked
/// [`ThroughputSample::warmup`] or [`ThroughputSample::warmup_tail`] never
/// reach any of this.
///
/// The knee is returned as the **top of its bucket**, every size in a bucket
/// being equally supported by the one median summarizing it. There is exactly
/// one candidate — the smallest quiet bucket already on the plateau — and the
/// five rules are vetoes on it, never a search for a bucket that survives them.
/// See docs/batch-calibration-design.md "Throughput knee: what run2 changed
/// again (R1e)" for the derivation and the replayed rings.
pub(super) fn fit_knee(
    samples: &[ThroughputSample],
    floor_rate: f64,
    anchor: u64,
    widened: Option<KneeWidening>,
    band: f64,
) -> Option<KneeFit> {
    let mut buckets = bucket_rates(samples, true);
    // Read *before* the retain below: the frontier rule is about the largest and
    // smallest sizes the ring actually holds, and a bucket dropped for being
    // unmeasurable is still a size that was run.
    let observed_top = *buckets.keys().next_back()?;
    let observed_floor = *buckets.keys().next()?;
    // A bucket that cannot be *tested* for noise cannot be certified quiet, so
    // it takes no part in the fit — not even in the sample and bucket counts
    // below, which would otherwise let two singletons stand in for a curve.
    buckets.retain(|_, rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES);
    let observations = buckets.values().map(Vec::len).sum::<usize>();
    if observations < MIN_KNEE_SAMPLES || buckets.len() < MIN_KNEE_BUCKETS {
        // R1 returned here for four windows running, silently: 11 quiet
        // observations against 12, one bucket dropped for holding a single one.
        tracing::debug!(
            observations,
            min_observations = MIN_KNEE_SAMPLES,
            buckets = buckets.len(),
            min_buckets = MIN_KNEE_BUCKETS,
            "declining to fit a throughput knee: the ring's quiet buckets hold \
             too few observations to read as a curve"
        );
        return None;
    }
    // One noisy bucket refuses the whole fit rather than excusing itself: the
    // knee is the *smallest* bucket on the plateau, so dropping a noisy one
    // would silently move the answer to its neighbour. Refusing also leaves
    // `knee_best` where it was.
    let medians = quiet_medians(&buckets, band)?;
    // Which bucket carries the peak is reported but never *used*: the threshold
    // is a rate, and the guard below is on the knee bucket.
    let best = medians
        .iter()
        .copied()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))?;
    // Every rate that reached a bucket is finite and positive, so a
    // non-positive reference means the medians are degenerate rather than NaN.
    let reference = if floor_rate.is_finite() {
        best.1.max(floor_rate)
    } else {
        best.1
    };
    if reference <= 0.0 {
        return None;
    }
    let threshold = reference * KNEE_RATIO;
    // Rule 4's gate is a *historical* question — had the ramp already gone past
    // this size when these rates were taken — so it is held up by the largest
    // anchor the ring's own observations were taken under, not only by the
    // anchor in force now, which a death mid-window can have halved.
    let anchor_bucket = size_bucket(
        samples
            .iter()
            .map(|sample| sample.anchor)
            .max()
            .unwrap_or(0)
            .max(anchor)
            .max(1),
    );
    // The knee is, by definition, the **smallest** quiet bucket already on the
    // plateau. That is the candidate; there is exactly one, and the rules below
    // are vetoes on it rather than a search for a bucket that survives them.
    let candidate = medians.iter().copied().find(|(_, rate)| *rate >= threshold);
    let veto = |bucket: u32, rate: f64| -> Option<&'static str> {
        // Rules 2 and 4 share one exception ([`flat_above`]): the
        // `KNEE_PLATEAU_BUCKETS` doublings *immediately* above the candidate
        // were all measured and neither beats it.
        let plateau_here = flat_above(&medians, bucket, rate);
        // Rules 1 (second half) and 2: the knee must be interior to the range
        // actually measured, at both ends. A bend at the frontier is the
        // frontier, and a plateau starting at the smallest size ever measured is
        // a statement about the range rather than about a size — unless the
        // range's own bottom is contiguously flat, which is that statement made
        // about the floor.
        if bucket <= observed_floor && !plateau_here {
            return Some(
                "the plateau starts at the smallest batch size measured \
                         and the doublings above it were not all measured flat",
            );
        }
        if bucket >= observed_top {
            return Some(
                "the plateau starts at the largest batch size measured, \
                         which is the frontier and not a bend",
            );
        }
        // Rule 3: established above the knee.
        let above: Vec<(u32, f64)> = medians
            .iter()
            .copied()
            .filter(|(other, _)| *other > bucket)
            .collect();
        if above.len() < KNEE_PLATEAU_BUCKETS {
            return Some(
                "the plateau is not established above the knee yet: \
                         fewer quiet buckets above it than the rule asks for",
            );
        }
        if !above
            .iter()
            .all(|(_, other_rate)| rate >= *other_rate * KNEE_RATIO)
        {
            return Some(
                "a larger batch size is materially faster, so this is \
                         not where the curve stops gaining",
            );
        }
        // Rule 4: a knee below the anchor may not rest on ramp-era evidence. An
        // observation is ramp-era *for its own bucket* when the ramp had not yet
        // reached a strictly larger bucket when it was taken. A plateau is
        // exempt: the standing evidence rule 4 waits for is the ramp's next
        // steps, and those are the flat buckets that made the exception.
        if !plateau_here
            && bucket < anchor_bucket
            && buckets.get(&bucket).is_none_or(|rates| {
                rates
                    .iter()
                    .filter(|(_, sample_anchor, _)| size_bucket(*sample_anchor) > bucket)
                    .count()
                    < MIN_KNEE_BUCKET_SAMPLES
            })
        {
            return Some(
                "every observation behind this knee was taken while the \
                         ramp was still climbing past it",
            );
        }
        // Rule 5: after a widening, the evidence must be newer than it. The
        // bucket that has to prove itself is the smallest quiet one *above* the
        // one the knee was widened away from — the size the model was let out to
        // run at, and the only one whose fresh behaviour is news.
        if let Some(widening) = widened
            && bucket <= widening.bucket
            && !buckets
                .iter()
                .find(|(other, _)| **other > widening.bucket)
                .is_some_and(|(_, rates)| {
                    rates
                        .iter()
                        .filter(|(_, _, seq)| *seq >= widening.from_seq)
                        .count()
                        >= MIN_KNEE_BUCKET_SAMPLES
                })
        {
            return Some(
                "this knee last expired and was widened, and the ring \
                         has not yet seen enough of the wider size to put it back",
            );
        }
        None
    };
    // Rule 1, first half: the frontier itself has to be quiet before anything
    // below it may be called a plateau. A frontier holding one lone sample is a
    // curve whose top end is unknown, and an unknown top end may be climbing.
    let knee = match candidate {
        _ if !buckets.contains_key(&observed_top) => {
            tracing::debug!(
                observed_top,
                observations = samples.len(),
                minimum = MIN_KNEE_BUCKET_SAMPLES,
                "declining to fit a throughput knee: the largest batch size in \
                 the ring has too few observations to be certified quiet, so \
                 nothing below it can be called a plateau yet"
            );
            None
        }
        Some((bucket, rate)) => match veto(bucket, rate) {
            Some(why) => {
                tracing::debug!(
                    bucket,
                    observed_floor,
                    observed_top,
                    anchor,
                    observations = samples.len(),
                    "declining to fit a throughput knee: {why}"
                );
                None
            }
            // `bucket < observed_top <= 63`, so the shift cannot overflow.
            None => Some((1u64 << (bucket + 1)) - 1),
        },
        None => None,
    };
    Some(KneeFit {
        knee_units: knee,
        best,
    })
}

/// `MAD / median` of one bucket's rates: how far a typical observation sits from
/// the bucket's own summary, as a fraction of it (see
/// [`KNEE_MAX_BUCKET_DISPERSION`]). `None` for an empty set or a non-positive
/// median, which the caller reads as "cannot certify this quiet".
pub(super) fn relative_mad(values: &mut [f64]) -> Option<f64> {
    let centre = median(values)?;
    if !centre.is_finite() || centre <= 0.0 {
        return None;
    }
    let mut deviations: Vec<f64> = values.iter().map(|value| (value - centre).abs()).collect();
    let mad = median(&mut deviations)?;
    Some(mad / centre)
}

pub(super) fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}
