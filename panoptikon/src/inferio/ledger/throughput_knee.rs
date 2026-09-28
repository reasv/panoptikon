//! The throughput knee: fit, expiry and widening. See
//! docs/batch-calibration-design.md, "Throughput knee: the fit itself".

use super::*;

/// The knee expired and was widened or withdrawn. Logged after the lock drops.
pub(super) struct KneeExpired {
    inference_id: String,
    gpu: String,
    from_units: u64,
    /// `None` when the knee was withdrawn.
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
    /// Count one settled window toward the knee's expiry, and widen the knee
    /// by one log2 bucket when it expires. A window counts when it was clean,
    /// knee-bound, and had room for [`RATCHET_FACTOR`] times the model's
    /// appetite; a negative resets the count. A knee that reaches
    /// [`uncapped_units`] is withdrawn. Both set
    /// [`ModelCalibration::knee_widened`], so a refit waits for samples at the
    /// wider size.
    pub(super) fn note_knee_window_locked(
        state: &mut LedgerState,
        worker: WorkerId,
        charge: Option<GrantCharge>,
        negative: bool,
    ) -> Option<KneeExpired> {
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let anchor = Self::anchor_locked(state, entry);
        let ceiling = uncapped_units(entry, anchor);
        let cal = state.calibration.get_mut(&key)?;
        let knee = cal.knee_units.filter(|knee| *knee > 0)?;
        if negative {
            cal.knee_clean_windows = 0;
            return None;
        }
        let charge = charge.filter(|charge| charge.knee_bound && charge.ample_headroom)?;
        // A knee this process did not fit expires sooner.
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
        // `knee` is `2^(b+1) − 1`, so `2k + 1` tops the next bucket.
        let widened = knee.saturating_mul(2).saturating_add(1);
        let withdrawn = widened >= ceiling;
        if withdrawn {
            cal.knee_units = None;
            cal.knee_fitted_units = None;
            cal.knee_is_local = false;
            // Tells the store to drop its knee; an absent knee does not.
            cal.knee_withdrawn = true;
        } else {
            cal.knee_units = Some(widened);
        }
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

    /// Re-fit the knee from the ring. A knee is only replaced here, never
    /// withdrawn: a knee in force stops the ramp above it, so a refit that
    /// declines means nothing.
    pub(super) fn refit_knee_locked(&self, state: &mut LedgerState, worker: WorkerId) {
        let Some(entry) = state.workers.get(&worker) else {
            return;
        };
        let band = self.budgets.for_gpu(&entry.gpu).knee_dispersion_in_force();
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let Some(cal) = state.calibration.get(&key) else {
            return;
        };
        // Sole-occupancy samples only; `/health` still counts every sample.
        let samples: Vec<ThroughputSample> = cal
            .throughput
            .iter()
            .filter(|sample| sample.occupants == 0)
            .copied()
            .collect();
        let floor = cal.knee_best.map(|(_, rate)| rate).unwrap_or(0.0);
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
        // The best rate is kept even when no knee is fitted.
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
        // What the store persists; widenings move only `knee_units`.
        cal.knee_fitted_units = Some(knee);
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

/// The log2 bucket a batch size falls in; the ramp is geometric.
pub(super) fn size_bucket(units: u64) -> u32 {
    units.max(1).ilog2()
}

/// The ring's rates by log2 bucket, as `(units/sec, anchor at the time,
/// sequence)`. Drops warm-up and non-finite samples, and with `drop_tail`
/// (the knee fit only) [`ThroughputSample::warmup_tail`] samples too.
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

/// The median units/sec per bucket, in size order; `None` when any bucket's
/// relative MAD exceeds `band`. `band` is per device kind
/// ([`VramBudget::knee_dispersion_in_force`]): a CPU's is wider than a GPU's.
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

/// Whether the [`KNEE_PLATEAU_BUCKETS`] doublings immediately above `bucket`
/// are all within [`KNEE_RATIO`] of `rate`; `None` when one is unmeasured.
pub(super) fn plateau_above(medians: &[(u32, f64)], bucket: u32, rate: f64) -> Option<bool> {
    let mut flat = true;
    for step in 1..=KNEE_PLATEAU_BUCKETS as u32 {
        let (_, other_rate) = medians.iter().find(|(other, _)| *other == bucket + step)?;
        flat &= rate >= *other_rate * KNEE_RATIO;
    }
    Some(flat)
}

/// [`plateau_above`], with an unmeasured doubling counting as not flat.
pub(super) fn flat_above(medians: &[(u32, f64)], bucket: u32, rate: f64) -> bool {
    plateau_above(medians, bucket, rate) == Some(true)
}

/// Fit the throughput knee: the top of the smallest log2 bucket whose median
/// is within [`KNEE_RATIO`] of `max(ring best, floor_rate)`, where
/// `floor_rate` is the best median of any earlier fit.
///
/// The caller passes sole-occupancy samples; warm-up ones (the first window
/// and [`KNEE_WARMUP_BATCHES`]) are dropped. Buckets need
/// [`MIN_KNEE_BUCKET_SAMPLES`] samples, the ring [`MIN_KNEE_SAMPLES`] across
/// [`MIN_KNEE_BUCKETS`] buckets, and every bucket's dispersion within `band`.
/// Five rules veto the one candidate: (1) the frontier is quiet and not the
/// knee; (2) the knee is above the smallest bucket, unless the doublings above
/// it are flat ([`flat_above`], which also exempts rule 4); (3)
/// [`KNEE_PLATEAU_BUCKETS`] buckets above it, none faster; (4) below the
/// anchor, not from ramp-era samples only; (5) after a widening, fresh samples
/// at the wider size. See docs/batch-calibration-design.md, "Throughput knee:
/// the fit itself".
pub(super) fn fit_knee(
    samples: &[ThroughputSample],
    floor_rate: f64,
    anchor: u64,
    widened: Option<KneeWidening>,
    band: f64,
) -> Option<KneeFit> {
    let mut buckets = bucket_rates(samples, true);
    // Before the retain: an under-measured bucket is still a size that ran.
    let observed_top = *buckets.keys().next_back()?;
    let observed_floor = *buckets.keys().next()?;
    // A bucket too small to test for noise takes no part, not even in counts.
    buckets.retain(|_, rates| rates.len() >= MIN_KNEE_BUCKET_SAMPLES);
    let observations = buckets.values().map(Vec::len).sum::<usize>();
    if observations < MIN_KNEE_SAMPLES || buckets.len() < MIN_KNEE_BUCKETS {
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
    // One noisy bucket refuses the whole fit; dropping it would move the knee.
    let medians = quiet_medians(&buckets, band)?;
    let best = medians
        .iter()
        .copied()
        .max_by(|(_, left), (_, right)| left.total_cmp(right))?;
    let reference = if floor_rate.is_finite() {
        best.1.max(floor_rate)
    } else {
        best.1
    };
    if reference <= 0.0 {
        return None;
    }
    let threshold = reference * KNEE_RATIO;
    // Rule 4 uses the largest anchor any sample was taken under, since the
    // current one may have been halved.
    let anchor_bucket = size_bucket(
        samples
            .iter()
            .map(|sample| sample.anchor)
            .max()
            .unwrap_or(0)
            .max(anchor)
            .max(1),
    );
    let candidate = medians.iter().copied().find(|(_, rate)| *rate >= threshold);
    let veto = |bucket: u32, rate: f64| -> Option<&'static str> {
        let plateau_here = flat_above(&medians, bucket, rate);
        // Rules 1 (second half) and 2: interior to the measured range.
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
        // Rule 4: a sample is ramp-era when its anchor was not yet in a
        // larger bucket.
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
        // Rule 5: the smallest bucket above the widened one needs samples
        // newer than the widening.
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
    // Rule 1, first half: the frontier must be quiet.
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

/// `MAD / median` of one bucket's rates; `None` for an empty set or a
/// non-positive median.
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
