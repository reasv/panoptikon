use super::*;

/// The anchor this entry may write into the **local** store: what a clean batch
/// on this GPU actually ran, never a seeded claim — which travels no further
/// than the entry, exactly as a seeded knee and a seeded fit do. A host the
/// conferred anchor is too large for still records the size it reached. Zero is
/// the store's "nothing to say" and the merge keeps whatever the file holds.
pub(super) fn persistable_anchor(cal: &ModelCalibration) -> u64 {
    cal.max_units_measured_here
}

impl VramLedger {
    // ------------------------------------------------------------------
    // Calibration store: seeding and persistence
    // ------------------------------------------------------------------

    /// Prime a (model, GPU)'s calibration from a matched profile, once.
    ///
    /// What a profile may confer is the crux of the design: **pricing** — the
    /// fit — always; the **ratchet anchor** from any matching profile that also
    /// carries a fit, as a seeded claim the OOM backstop can undo; **growth** —
    /// the sample ring — only from a **local** profile; and **confidence** —
    /// `local_samples` — only when local *and* matched on the exact torch
    /// string.
    ///
    /// Seeding happens once per (model, GPU) per run, and the flag is set on the
    /// first **attempt**, not on the first match: setting it on a match is how a
    /// re-seed duplicates the ring, since after a TTL unload the reload's lookup
    /// answers with the samples still in memory. The corollary is that an
    /// attempt that could not be *keyed* still consumes the pair's one seed
    /// attempt, which is deliberate.
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
            // The store was consulted and had nothing (or the key was
            // incomplete). Still an attempt: whatever this run measures is the
            // only truth for this pair, and a reload must not re-import it.
            state.calibration.entry(key.clone()).or_default().seeded = true;
            return;
        };
        // A profile confers confirmation only when this machine measured it
        // under the software environment now running.
        let confirms = seed.local && seed.exact_torch;
        let adopt_fit = state
            .calibration
            .get(key)
            .is_none_or(|cal| cal.fit.is_none())
            && seed.slope_mb_per_unit > 0.0;
        // Only a fit that is actually adopted spends a version number: an unspent
        // one would leave `persisted` pointing at a version nothing holds, and
        // the first settled window would write the file back unchanged.
        let version = if adopt_fit {
            state.next_fit_version += 1;
            state.next_fit_version
        } else {
            0
        };
        let cal = state.calibration.entry(key.clone()).or_default();
        cal.seeded = true;
        // A profile's knee is adopted only where this machine has not fitted one.
        // Seeding normally runs before any local evidence exists, but it is
        // reachable afterwards, and both directions of the unguarded assignment
        // are wrong: it would overwrite a measured local knee with a stranger's,
        // and — `knee_is_local` staying true — launder the stranger's number
        // into local provenance on the next write.
        if !cal.knee_is_local {
            cal.knee_units = seed.knee_units;
            cal.knee_fitted_units = seed.knee_units;
            // Explicit rather than implied by the branch: a seeded knee is a
            // foreign measurement and may never travel back out.
            cal.knee_is_local = false;
            // A seeded knee arrives with its expiry progress, which is
            // local-only and therefore zero from anything but this machine's own
            // store. Without it a restart would hand a persisted knee a fresh
            // set of [`KNEE_EXPIRY_CLEAN_WINDOWS`] windows to be right in.
            cal.knee_clean_windows = seed.knee_clean_windows;
        }
        if adopt_fit {
            cal.fit = Some(FitSnapshot {
                slope_mb_per_unit: seed.slope_mb_per_unit,
                // The intercept is diagnostic only (admission uses the slope),
                // which is why the file format has no field for it. A local
                // profile's sample ring reproduces it on the first refit; a
                // shipped one never had one to share.
                intercept_mb: 0.0,
                residual_mb: seed.residual_mb,
                samples: seed.samples,
                version,
            });
            // Whose fit this is decides whether it may ever travel back into the
            // local store (see `pending_update_locked`): neither a **shipped**
            // baseline's slope nor a local one reached through the `major.minor`
            // fallback may, both having been measured elsewhere.
            // `fit_is_local` rather than `local`, because a local entry with no
            // fit of its own borrows one from a shipped baseline.
            cal.fit_is_local = seed.fit_is_local && seed.exact_torch;
        }
        // The anchor is conferred by **any** matching profile, and always as a
        // seeded claim: a card name is not a gate on it, since any card becomes
        // "the same architecture with less memory" as soon as another process is
        // on it, and the store's own key is the architecture, so even a local row
        // may name a bigger card of this machine. Only a clean batch this GPU
        // runs at it makes it measured here. Without a fit there is no slope to
        // bound the anchor in MB with, so it confers nothing at all.
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
            // Nothing has moved since the file was written, so the write policy
            // must not immediately write it back. The version recorded is the
            // one in force (0 when no fit was adopted) and the knee is `None`,
            // a seeded knee never being written: both sides of the comparison
            // have to describe the same quantity.
            let in_force = cal.fit.map(|fit| fit.version).unwrap_or(0);
            cal.persisted = Some((persistable_anchor(cal), in_force, None));
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

    /// The write policy, evaluated once per settled window: hand the store an
    /// update when the ratchet anchor advanced or the fit meaningfully changed —
    /// never per batch, and never for state carrying no local evidence.
    ///
    /// Five guards, each load-bearing: `arch`/`torch`/`dtype` must be known, or
    /// the entry could not be keyed and could never be read back; `base_mb` must be
    /// known, or the profile would claim a base of 0 and later suppress a real
    /// load reservation; `local_samples > 0`, so a shipped baseline is never
    /// copied in as if this machine had measured it; and something must actually
    /// have changed. The **fit fields are separate**, since the first local
    /// sample can advance the anchor several windows before [`MIN_FIT_SAMPLES`]
    /// produces a refit: until then the update carries no fit at all.
    pub(super) fn pending_update_locked(
        state: &mut LedgerState,
        worker: WorkerId,
    ) -> Option<ProfileUpdate> {
        // A replica deregistered between the settle and here has no model to name
        // and nothing left to persist; every other exit below says why it took
        // itself out.
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
        // The key guards, and the one place in this design where doing nothing is
        // invisible: a model whose worker reports no dtype writes no profile on
        // any host, ever, and the only evidence is a store file that never
        // appears. Each reason is explained once per model and GPU.
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
        // Read before the write below moves it on, so the log can say which of
        // the three watched quantities actually changed.
        let previously_persisted = cal.persisted;
        let fit_version = cal.fit.map(|fit| fit.version).unwrap_or(0);
        // Only a knee this machine fitted travels, for the same reason only a
        // local fit does, and it travels *as fitted*: the expiry's widenings are
        // this process's own re-test of it. Quantized to a bucket edge, so
        // "changed at all" and "changed materially" are the same test.
        let knee = cal.knee_fitted_units.filter(|_| cal.knee_is_local);
        // A knee that expired past the point of capping anything: the store has
        // to be told, because a `None` knee otherwise reads as "nothing fitted
        // this run" and the merge keeps what is on disk.
        let knee_withdrawn = cal.knee_withdrawn;
        let current = (persistable_anchor(cal), fit_version, knee);
        if !knee_withdrawn
            && cal.persisted.is_some_and(|persisted| {
                persisted.1 == current.1 && persisted.0 >= current.0 && persisted.2 == current.2
            })
        {
            // A withdrawal is never suppressed by the write policy: nothing the
            // policy watches has to have moved for it, and an unwritten
            // withdrawal is a stored knee outliving its own expiry.
            return None;
        }
        cal.knee_withdrawn = false;
        // The **persisted** anchor only ever moves forward, which the suppression
        // predicate above cannot achieve on its own: being a conjunction, a fit
        // or knee change riding along with a lowered anchor would write the
        // lowered figure. A stored anchor is a claim about a batch size this
        // machine once ran, which no death unmeasures, so the runtime halving
        // stays runtime-only.
        let max_units_measured = cal
            .persisted
            .map_or(current.0, |persisted| persisted.0.max(current.0));
        cal.persisted = Some((max_units_measured, current.1, current.2));
        // Only a locally derived fit travels; see the note above.
        let fit = cal.fit.filter(|_| cal.fit_is_local);
        let reason = match previously_persisted {
            Some(persisted) if persisted.1 != current.1 => "fit_changed",
            Some(persisted) if persisted.2 != current.2 => "knee_changed",
            Some(_) => "anchor_advanced",
            None if current.1 > 0 => "fit_changed",
            None => "anchor_advanced",
        };
        // Emitted under the ledger lock, unlike the settle line this rides
        // inside: the suppression predicate above has already returned for every
        // unchanged settle, so this fires only when something really moved.
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
            knee_withdrawn,
            max_units_measured,
            local_samples: cal.local_samples,
            // Expiry progress rides along with whatever else triggered this write
            // rather than triggering one of its own: a counter that moved every
            // window would defeat the write policy's point, and losing a
            // restart's worth of it costs windows, not permanence.
            knee_clean_windows: cal.knee_clean_windows,
            ring: cal.samples.iter().copied().collect(),
        })
    }

    /// Say, **once** per `(model, gpu, reason)`, why a settled window handed the
    /// store nothing — the key and the store state, deliberately not the write
    /// policy's own no-op, which is the designed steady state of every healthy
    /// model. `no_torch`, `no_dtype` and `no_base` are properties of the worker
    /// build and do mean "and it will go on writing nothing"; `no_calibration`
    /// and `no_local_samples` can clear on a later settle. Takes the log set
    /// rather than the whole state so it can be called while the calibration
    /// entry is borrowed.
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
