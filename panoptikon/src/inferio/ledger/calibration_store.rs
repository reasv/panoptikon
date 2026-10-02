//! Seeding calibration from the store and the write policy back to it. See
//! docs/batch-calibration-design.md, "Calibration store".

use super::*;

/// The anchor this entry may write into the local store: what a clean batch
/// on this GPU ran, never a seeded anchor. Zero means nothing to write.
pub(super) fn persistable_anchor(cal: &ModelCalibration) -> u64 {
    cal.max_units_measured_here
}

/// The trial cadence as it is stored: the trials in a row that left the
/// working size in place, and the wait for the next rounded down to whole
/// [`RETEST_WINDOWS`], so a wait is written once per that many windows.
fn stored_cadence(cal: &ModelCalibration) -> (u32, u32) {
    (
        cal.failed_trials,
        cal.retest_after / RETEST_WINDOWS * RETEST_WINDOWS,
    )
}

impl VramLedger {
    /// Prime a (model, GPU)'s calibration from a matched profile. A profile
    /// confers the fit always; the anchor (as a seeded claim) and the working
    /// size when it carries a fit; the sample ring only when local;
    /// `local_samples` only when local
    /// with the exact torch string. Runs once per (model, GPU) per run: the
    /// first attempt sets the flag even without a match, so a reload cannot
    /// duplicate the ring.
    pub(super) fn seed_calibration_locked(
        state: &mut LedgerState,
        key: &(String, String),
        attempted: bool,
        seed: Option<ProfileSeed>,
        inference_id: &str,
        gpu: &str,
    ) {
        if !attempted || state.calibration.get(key).is_some_and(|cal| cal.seeded) {
            return;
        }
        let Some(seed) = seed else {
            state.calibration.entry(key.clone()).or_default().seeded = true;
            return;
        };
        let confirms = seed.local && seed.exact_torch;
        let adopt_fit = state
            .calibration
            .get(key)
            .is_none_or(|cal| cal.fit.is_none())
            && seed.slope_mb_per_unit > 0.0;
        // Only an adopted fit spends a version number.
        let version = if adopt_fit {
            state.next_fit_version += 1;
            state.next_fit_version
        } else {
            0
        };
        let cal = state.calibration.entry(key.clone()).or_default();
        cal.seeded = true;
        // Never overwrite a working size this machine measured. Like the
        // anchor, a stored one is ignored without a fit: it is the size the
        // run opens at, and nothing could price it. A local one was left in
        // place by a trial here, and the wait for the next trial carries on.
        if !cal.knee_is_local {
            cal.knee_units = seed.knee_units.filter(|_| seed.slope_mb_per_unit > 0.0);
            if let Some(knee) = cal.knee_units.filter(|_| seed.local) {
                cal.knee_is_local = true;
                cal.failed_trials = seed.knee_trials.failed;
                cal.retest_after = seed.knee_trials.retest_after;
                // No larger size was ever measured here: memory, or the end
                // of the run, cut the climb at this one.
                cal.room_cut = seed.max_units_measured as f64 * SAME_SIZE_RATIO <= knee as f64;
            }
        }
        if adopt_fit {
            cal.fit = Some(FitSnapshot {
                slope_mb_per_unit: seed.slope_mb_per_unit,
                // Not stored: read off the local ring, 0 without one.
                intercept_mb: measurements::intercept_at(&seed.ring, seed.slope_mb_per_unit)
                    .unwrap_or(0.0),
                residual_mb: seed.residual_mb,
                samples: seed.samples,
                version,
            });
            // Only a fit measured here, under the exact torch, is written back.
            cal.fit_is_local = seed.fit_is_local && seed.exact_torch;
        }
        // Any matching profile with a fit confers its anchor, always as a
        // seeded claim until a clean batch here reaches it.
        if seed.max_units_measured > cal.max_units_measured && seed.slope_mb_per_unit > 0.0 {
            cal.max_units_measured = seed.max_units_measured;
            cal.anchor_measured_here = false;
        }
        if seed.local {
            for sample in seed.ring {
                cal.samples.push_back(sample);
                while cal.samples.len() > FIT_RING {
                    cal.samples.pop_front();
                }
            }
            if confirms {
                cal.local_samples = cal.local_samples.max(seed.local_samples);
            }
            // Mark as persisted so the write policy does not write it back.
            cal.persisted = Some(Persisted {
                anchor: persistable_anchor(cal),
                fit_version: cal.fit.map(|fit| fit.version).unwrap_or(0),
                knee: cal.knee_units.filter(|_| cal.knee_is_local),
                cadence: stored_cadence(cal),
            });
        }
        tracing::debug!(
            model = %inference_id,
            gpu = %gpu,
            local = seed.local,
            fit_is_local = seed.fit_is_local,
            exact_torch = seed.exact_torch,
            confirms,
            slope_mb_per_unit = seed.slope_mb_per_unit,
            samples = seed.samples,
            local_samples = seed.local_samples,
            max_units_measured = seed.max_units_measured,
            "seeded calibration from a stored profile"
        );
    }

    /// The write policy, once per settled window: an update when the anchor
    /// advanced, or the fit, the working size or the trial cadence changed.
    /// Requires known `arch`, `torch`, `dtype` and `base_mb`, and
    /// `local_samples > 0`. The fit fields are empty until a local fit exists.
    pub(super) fn pending_update_locked(
        state: &mut LedgerState,
        worker: WorkerId,
    ) -> Option<ProfileUpdate> {
        let entry = state.workers.get(&worker)?;
        let key = (entry.inference_id.clone(), entry.gpu.clone());
        let identity = (
            entry.inference_id.clone(),
            entry.epoch,
            entry.gpu_name.clone(),
            entry.unit.as_str(),
            entry.aggregation.as_str(),
            entry.base_method.clone(),
            entry.dtype_method.clone(),
        );
        let (arch, torch, dtype, base) = (
            entry.gpu_arch.clone(),
            entry.torch.clone(),
            entry.dtype.clone(),
            entry.base_mb,
        );
        let (arch, torch, dtype, base_mb) = match (arch, torch, dtype, base) {
            (Some(arch), Some(torch), Some(dtype), Some(base_mb)) => (arch, torch, dtype, base_mb),
            (arch, torch, dtype, _) => {
                let reason = if arch.is_none() {
                    "no_arch"
                } else if torch.is_none() {
                    "no_torch"
                } else if dtype.is_none() {
                    "no_dtype"
                } else {
                    "no_base"
                };
                Self::note_unpersistable_locked(&mut state.profile_skip_logged, &key, reason);
                return None;
            }
        };
        let Some(cal) = state.calibration.get_mut(&key) else {
            Self::note_unpersistable_locked(&mut state.profile_skip_logged, &key, "no_calibration");
            return None;
        };
        if cal.local_samples == 0 {
            Self::note_unpersistable_locked(
                &mut state.profile_skip_logged,
                &key,
                "no_local_samples",
            );
            return None;
        }
        let before = cal.persisted;
        let fit_version = cal.fit.map(|fit| fit.version).unwrap_or(0);
        // Only a working size a trial here measured is written; `None` leaves
        // the stored one as it is.
        let knee = cal.knee_units.filter(|_| cal.knee_is_local);
        let cadence = stored_cadence(cal);
        let anchor = persistable_anchor(cal);
        if before.is_some_and(|before| {
            before.fit_version == fit_version
                && before.anchor >= anchor
                && (knee.is_none() || before.knee == knee)
                && before.cadence == cadence
        }) {
            return None;
        }
        // The persisted anchor only moves forward; halvings stay runtime-only.
        let max_units_measured = before.map_or(anchor, |before| before.anchor.max(anchor));
        cal.persisted = Some(Persisted {
            anchor: max_units_measured,
            fit_version,
            knee,
            cadence,
        });
        let fit = cal.fit.filter(|_| cal.fit_is_local);
        let reason = match before {
            Some(before) if before.fit_version != fit_version => "fit_changed",
            Some(before) if knee.is_some() && before.knee != knee => "knee_changed",
            Some(before) if before.cadence != cadence => "trial_cadence",
            Some(_) => "anchor_advanced",
            None if fit_version > 0 => "fit_changed",
            None => "anchor_advanced",
        };
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            reason,
            max_units_measured,
            fit_version,
            "queued a calibration profile update for the store"
        );
        Some(ProfileUpdate {
            inference_id: identity.0,
            epoch: identity.1,
            arch,
            gpu_name: identity.2,
            torch,
            dtype,
            unit: identity.3,
            aggregation: identity.4,
            base_mb,
            base_method: identity.5,
            dtype_method: identity.6,
            slope_mb_per_unit: fit.map(|fit| fit.slope_mb_per_unit).unwrap_or(0.0),
            residual_mb: fit.map(|fit| fit.residual_mb).unwrap_or(0.0),
            samples: fit.map(|fit| fit.samples).unwrap_or(0),
            knee_units: knee,
            knee_trials: TrialCadence {
                failed: cadence.0,
                retest_after: cadence.1,
            },
            max_units_measured,
            local_samples: cal.local_samples,
            ring: cal.samples.iter().copied().collect(),
        })
    }

    /// Log once per `(model, gpu, reason)` why a settled window wrote nothing
    /// to the store. The unchanged no-op is not logged.
    fn note_unpersistable_locked(
        logged: &mut HashSet<(String, String, &'static str)>,
        key: &(String, String),
        reason: &'static str,
    ) {
        if !logged.insert((key.0.clone(), key.1.clone(), reason)) {
            return;
        }
        let because = match reason {
            "no_torch" => "the worker reported no torch version",
            "no_dtype" => "the worker reported no dtype",
            "no_base" => "the worker reported no load footprint",
            "no_calibration" => "this replica has no calibration state on the GPU yet",
            "no_local_samples" => "nothing has been measured locally yet",
            "no_arch" => {
                "the worker reported no GPU architecture and the host could not derive one"
            }
            other => other,
        };
        tracing::debug!(
            model = %key.0,
            gpu = %key.1,
            reason,
            "skipped the calibration store update: {because}"
        );
    }
}
