//! Issuing grants and settling the windows they cover.

use std::sync::LazyLock;

use super::*;
use crate::log_throttle::LogThrottle;

/// The settle line's `clamped` field: `none`, or each clamp reason in
/// first-seen order joined by `+` (a clamp naming no reason is `memory`).
pub(super) fn clamp_log_field(clamps: &[Option<String>]) -> String {
    let mut seen: Vec<&str> = Vec::new();
    for clamp in clamps {
        let reason = clamp.as_deref().unwrap_or(CLAMP_REASON_MEMORY);
        if !seen.contains(&reason) {
            seen.push(reason);
        }
    }
    if seen.is_empty() {
        return "none".to_owned();
    }
    seen.join("+")
}

/// A clamp that named no reason: the worker's defensive memory clamp.
const CLAMP_REASON_MEMORY: &str = "memory";

/// The line a RAM-capped grant logs, at most once per model and GPU per
/// [`crate::log_throttle::LOG_REPEAT_WINDOW`].
static RAM_BOUND_LOG: LazyLock<LogThrottle> =
    LazyLock::new(|| LogThrottle::new("grants capped by host RAM", tracing::Level::INFO));

impl VramLedger {
    /// Units the dispatcher should aim to put in one window.
    pub(super) fn window_target_units(&self, worker: WorkerId) -> u64 {
        let mut state = self.lock();
        // Repay first: this is the first read of the deflation counter for
        // an idle replica's next window.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get(&worker) else {
            return 1;
        };
        // The knee caps the batch; a window is still several batches deep.
        admitted_units(
            entry,
            Self::anchor_locked(&state, entry),
            Self::knee_locked(&state, entry),
            Self::shape_ceiling_locked(&state, entry),
        )
        .saturating_mul(WINDOW_DEPTH_MULTIPLIER)
        .max(1)
    }

    /// Reserve headroom for one window and hand back the grant.
    /// `window_units` is the dispatcher's estimate; safety does not depend on
    /// it, since the worker packs within the grant using exact counts.
    pub(super) fn request_grant(
        self: &Arc<Self>,
        worker: WorkerId,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
        byte_bound: bool,
    ) -> Option<GrantToken> {
        // Fold in neighbours' per-batch pool growth before pricing, or it reads
        // as external usage. Before the probe, which reads the same clock.
        {
            let mut state = self.lock();
            Self::refresh_pools_locked(&mut state);
        }
        self.maybe_refresh_external(worker);
        self.refresh_host_ram_now(worker);
        let mut state = self.lock();
        Self::repay_deflation_locked(&mut state, worker);
        let gpu = state.workers.get(&worker)?.gpu.clone();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.pending_requests = window_requests.saturating_add(queued_behind);
        }
        // Priced under this model's effective (possibly widened) margin.
        let margin = {
            let entry = state.workers.get(&worker)?;
            self.effective_margin_locked(&state, entry)
        };
        let signed_headroom = self.overdraft_with_margin_locked(&state, &gpu, margin);
        let headroom = signed_headroom.max(0) as u64;
        let share = self.share_locked(&state, worker, signed_headroom);
        let (
            mut unit_budget,
            mut mb,
            unit,
            aggregation,
            canvas_pixels,
            max_tokens,
            squeezed,
            knee_bound,
            ample_headroom,
            queue_bound,
            ram_mb,
            ram_bound,
            ram_mb_per_unit,
        ) = {
            let entry = state.workers.get(&worker)?;
            let anchor = Self::anchor_locked(&state, entry);
            let slope = Self::grant_slope_locked(&state, entry);
            let ceiling = Self::shape_ceiling_locked(&state, entry);
            let capped = admitted_units(entry, anchor, Self::knee_locked(&state, entry), ceiling);
            let wanted = capped.min(window_units.max(1)).max(1);
            // The knee decided this window's size: it bit (compared with the
            // shape ceiling still applied) and the work in hand reached it.
            let knee_bound = capped < admitted_units(entry, anchor, None, ceiling)
                && wanted >= capped
                && capped > 0;
            // Room for `RATCHET_FACTOR` × the appetite, measured against the
            // requester's own room (its pool included).
            let ample_headroom = (share.room as f64)
                >= self.appetite_mb_locked(&state, entry) * RATCHET_FACTOR as f64;
            let mut units = wanted;
            let mut mb = share.mb;
            // Memory, not the ramp, ratchet or queue, held this window back.
            let squeezed = if let Some(slope) = slope {
                // Post-fit the unit budget is what the share affords.
                let affordable = ((share.mb as f64) / slope).floor().max(1.0) as u64;
                let squeezed = affordable < wanted;
                units = units.min(affordable).max(1);
                mb = ((units as f64) * slope).ceil() as u64;
                squeezed
            } else {
                // Pre-fit the ramp value is the unit budget; with no share at
                // all, one unit, as post-fit.
                if share.mb == 0 {
                    units = 1;
                }
                // Pre-fit, squeezed means held at the floor while the floors
                // do not all fit.
                share.mb <= share.floor && headroom < share.floor_sum
            };
            // Host RAM caps a GPU replica's batch as the edge of a full card
            // would; the GPU side above is unchanged.
            let ram = self.ram_ceiling_locked(&state, entry);
            let ram_bound = ram.is_some_and(|ram| ram.units < units);
            if let Some(ram) = ram.filter(|_| ram_bound) {
                units = ram.units;
                if let Some(slope) = slope {
                    mb = ((units as f64) * slope).ceil() as u64;
                }
            }
            let ram_cost = ram.and_then(|ram| ram.cost);
            let ram_mb = ram_cost.map_or(0, |cost| cost.booking_mb(units));
            let ram_mb_per_unit = ram_cost.map(|cost| cost.mb_per_unit);
            (
                units,
                mb,
                entry.unit,
                entry.aggregation,
                entry.canvas_pixels,
                entry.max_tokens,
                squeezed,
                knee_bound,
                ample_headroom && !squeezed && !ram_bound,
                // queue_bound: less work in hand than the ramp admits.
                wanted < capped,
                ram_mb,
                ram_bound,
                ram_mb_per_unit,
            )
        };
        // At least one unit, or the queue stalls; the MB side has no floor.
        unit_budget = unit_budget.max(1);
        mb = mb.min(share.mb);
        if squeezed {
            // Starved (memory-blind, no headroom): the largest free pool on
            // the GPU is asked to trim too, busy or not.
            let starved = mb == 0 && headroom == 0;
            let busy_holder = starved.then(|| Self::largest_free_pool_locked(&state, &gpu, worker));
            Self::flag_trims_locked(&mut state, &gpu, worker, starved, busy_holder.flatten());
        }
        let grant_id = state.next_id();
        state
            .workers
            .get_mut(&worker)
            .expect("presence checked above")
            .grants
            .insert(
                grant_id,
                GrantCharge {
                    mb,
                    room: share.room,
                    requests: window_requests,
                    unit_budget,
                    squeezed,
                    peak_occupants: 0,
                    knee_bound,
                    ample_headroom,
                    queue_bound,
                    byte_bound,
                    ram_mb,
                    ram_bound,
                },
            );
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.ram_bound = ram_bound;
        }
        Self::note_occupancy_locked(&mut state, &gpu);
        // Logged after the lock is dropped.
        let external_mb = Self::external_locked(&state, &gpu).unwrap_or(0);
        let (reserve_mb, reserve_rule) = self.reserve_locked(&gpu, external_mb, margin);
        let issued = state.workers.get(&worker).map(|entry| {
            let anchor = Self::anchor_locked(&state, entry);
            (
                entry.inference_id.clone(),
                entry.effective_ramp_step(anchor),
                entry.deflation,
                Self::pricing_fit_locked(&state, entry).is_none(),
            )
        });
        drop(state);
        if let Some((model, ramp_step, deflation, pre_fit)) = issued {
            let canvas = canvas_log_field(canvas_pixels);
            tracing::debug!(
                model = %model,
                gpu = %gpu,
                unit_budget,
                mb,
                canvas_pixels = %canvas,
                share_mb = share.mb,
                room_mb = share.room,
                headroom_mb = headroom,
                external_mb,
                reserve_mb,
                reserve_rule,
                pre_fit,
                ramp_step,
                deflation,
                squeezed,
                window_requests,
                ram_mb,
                ram_bound,
                "issued a memory grant"
            );
            if ram_bound && RAM_BOUND_LOG.admit_for(&format!("{model} {gpu}")) {
                tracing::info!(
                    model = %model,
                    gpu = %gpu,
                    unit_budget,
                    ram_mb,
                    ram_mb_per_unit = ?ram_mb_per_unit,
                    "host RAM capped this window below what the GPU could hold"
                );
            }
        }
        Some(GrantToken {
            ledger: Arc::clone(self),
            worker,
            grant_id,
            grant: Grant {
                unit_budget,
                mb,
                unit,
                aggregation,
                user_cap_items,
                canvas_pixels,
                max_tokens,
                squeezed: squeezed || ram_bound,
            },
            settled: false,
        })
    }

    /// Repay deflation for elapsed wall time ([`DEFLATION_REPAY_SECS`]).
    /// Called wherever the counter is about to be read, not on a timer.
    pub(super) fn repay_deflation_locked(state: &mut LedgerState, worker: WorkerId) {
        let now = Instant::now();
        let Some(entry) = state.workers.get_mut(&worker) else {
            return;
        };
        let before = entry.deflation;
        if entry.repay_deflation_by_time(now) == 0 {
            return;
        }
        tracing::debug!(
            model = %entry.inference_id,
            gpu = %entry.gpu,
            deflation_before = before,
            deflation = entry.deflation,
            repay_secs = DEFLATION_REPAY_SECS.as_secs(),
            "repaid deflation by elapsed time; clean windows are not the only \
             way back from a fault storm, and an idle replica has none to offer"
        );
    }

    /// Raise every outstanding window's contention tag on `gpu` to the
    /// current occupancy. Called on grant issue, the only time it can rise.
    fn note_occupancy_locked(state: &mut LedgerState, gpu: &str) {
        let occupied = state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu && !entry.grants.is_empty())
            .count();
        let Some(others) = occupied.checked_sub(1).filter(|others| *others > 0) else {
            return;
        };
        let others = u32::try_from(others).unwrap_or(u32::MAX);
        for entry in state.workers.values_mut().filter(|entry| entry.gpu == gpu) {
            for charge in entry.grants.values_mut() {
                charge.peak_occupants = charge.peak_occupants.max(others);
            }
        }
    }

    /// Release a grant and account for its window ([`GrantToken::finish`], or
    /// its `Drop` on abort). Telemetry is ingested on every outcome, or the
    /// next window's settle would pick up an aborted window's OOM.
    fn settle(
        &self,
        worker: WorkerId,
        grant_id: u64,
        outcome: WindowOutcome,
    ) -> Option<UnrunnableReplica> {
        let settled = self.settle_locked(worker, grant_id, outcome);
        // Logs and the store write happen with the ledger lock released.
        if let Some(death) = settled.death {
            death.emit();
        }
        if let Some(expiry) = settled.knee_expiry {
            expiry.emit();
        }
        // Ceiling and OOM tier lines precede the window line they explain.
        if let Some(ceiling) = settled.shape_ceiling {
            ceiling.emit();
        }
        if let Some(oom) = settled.oom {
            oom.emit();
        }
        if let Some(window) = settled.window {
            window.emit();
        }
        if let (Some(update), Some(profiles)) = (settled.update, self.profiles.as_ref()) {
            profiles.record(update);
        }
        if let Some(verdict) = settled.unrunnable.as_ref() {
            tracing::warn!(
                model = %verdict.inference_id,
                gpu = %verdict.gpu,
                base_mb = verdict.base_mb,
                room_mb = verdict.room_mb,
                windows = OOM_WINDOWS_AT_FLOOR,
                "this model cannot run a single item on this GPU; failing it \
                 instead of dispatching to it again"
            );
        }
        settled.unrunnable
    }

    fn settle_locked(&self, worker: WorkerId, grant_id: u64, outcome: WindowOutcome) -> Settled {
        let mut state = self.lock();
        // Time repayment first, whatever the outcome.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get_mut(&worker) else {
            return Settled::default();
        };
        // This window's requests leave the demand signal on every outcome.
        let charge = entry.grants.remove(&grant_id);
        if let Some(charge) = charge {
            entry.pending_requests = entry.pending_requests.saturating_sub(charge.requests);
        }
        let granted_units = charge;
        // The trim path's idle clock starts at settle, on every outcome.
        entry.last_grant_settled_at = Some(Instant::now());
        // A window has run since, so another idle release is worth asking.
        entry.idle_release_gave_nothing = false;
        // Anything but a clean response may not have applied the fit
        // snapshot this window carried, so re-send it.
        if !matches!(outcome, WindowOutcome::Responded { oom: None }) {
            entry.fit_version_sent = 0;
        }
        // A failed window may not mark the anchor "measured here".
        let window_failed = matches!(
            outcome,
            WindowOutcome::WorkerDied | WindowOutcome::Responded { oom: Some(_) }
        );
        let ingested = Self::ingest_locked(&mut state, worker, granted_units, window_failed);
        // Allocator retries: the card is full now, so ask neighbours now.
        if ingested.alloc_retries.is_some_and(|retries| retries > 0) {
            Self::flag_starved_neighbours_locked(&mut state, worker);
        }
        let mut knee_expiry: Option<KneeExpired> = None;
        let mut responded_negative = false;
        let frame_oom = match outcome {
            WindowOutcome::Responded { oom } => oom,
            _ => None,
        };
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom.is_some();
            responded_negative = negative;
            // Read after the ingest, which may move both.
            let (anchor, ceiling) = match state.workers.get(&worker) {
                Some(entry) => (
                    Self::anchor_locked(&state, entry),
                    Self::shape_ceiling_locked(&state, entry),
                ),
                None => (0, None),
            };
            // A binding knee stops the exponent too, or doublings would bank
            // under it and be spent at once when it is withdrawn.
            let gate = self.ramp_gate_locked(&state, worker, anchor);
            let knee_binds = Self::knee_binds_locked(&state, worker);
            let may_grow = gate.gains && !knee_binds;
            // An uncertified hold (no knee in force, anchor > 0) is held at the
            // largest batch this GPU ran, else the seed; never at the conferred
            // anchor. See docs/batch-calibration-design.md, "Throughput knee:
            // the fit itself" ("And the stop is durable").
            let reached_here = state
                .workers
                .get(&worker)
                .and_then(|entry| cal_locked(&state, entry))
                .map(|cal| cal.max_units_measured_here)
                .unwrap_or(0);
            let seed_units = state
                .workers
                .get(&worker)
                .map(|entry| entry.seed_units)
                .unwrap_or(0);
            let rung = if reached_here > 0 {
                reached_here
            } else {
                seed_units
            };
            let hold_rung =
                (anchor > 0 && !gate.gains && !gate.certified && !knee_binds).then_some(rung);
            if let Some(entry) = state.workers.get_mut(&worker) {
                if negative {
                    entry.note_negative_sample(anchor);
                } else {
                    entry.note_clean_window(
                        ingested.fit_samples > 0,
                        ingested.at_budget,
                        anchor,
                        ceiling,
                        may_grow,
                        hold_rung,
                    );
                    // A knee or a measured plateau certifies the hold.
                    entry.held_certified = entry.ramp_held && (gate.certified || knee_binds);
                    entry.windows_queue_bound = if ingested.at_budget {
                        0
                    } else {
                        entry.windows_queue_bound.saturating_add(1)
                    };
                }
            }
            Self::reprobe_hold_locked(&mut state, worker, charge, negative);
            Self::log_ramp_hold_locked(&mut state, worker, gate, knee_binds);
            knee_expiry = Self::note_knee_window_locked(&mut state, worker, charge, negative);
        }
        let died = matches!(outcome, WindowOutcome::WorkerDied);
        let death = died
            .then(|| Self::note_unified_death_locked(&mut state, worker, charge.is_some()))
            .flatten();
        // Any OOM or death lowers a seeded anchor, unless the unified-memory
        // death path already halved it.
        if death.is_none() && (frame_oom.is_some() || ingested.oom || died) {
            Self::lower_seeded_anchor_locked(&mut state, worker);
        }
        // A one-item OOM with less room than one item costs; see
        // [`OOM_WINDOWS_AT_FLOOR`].
        let unrunnable = self.note_floor_oom_locked(
            &mut state,
            worker,
            charge,
            frame_oom.is_some() || ingested.oom || died,
            matches!(outcome, WindowOutcome::Responded { .. }) && !responded_negative,
        );
        Self::refit_locked(&mut state, worker);
        self.refit_knee_locked(&mut state, worker);
        // No store, no write policy: it would move `cal.persisted` for nothing.
        let update = self
            .profiles
            .is_some()
            .then(|| Self::pending_update_locked(&mut state, worker))
            .flatten();
        // The state the next window is priced against.
        let window = state.workers.get(&worker).map(|entry| WindowSettled {
            inference_id: entry.inference_id.clone(),
            gpu: entry.gpu.clone(),
            outcome: match outcome {
                WindowOutcome::Responded { .. } if responded_negative => "negative",
                WindowOutcome::Responded { .. } => "clean",
                WindowOutcome::Aborted => "aborted",
                WindowOutcome::WorkerDied => "worker_died",
            },
            negative_reason: if responded_negative {
                if frame_oom.is_some() || ingested.oom {
                    Some("oom")
                } else if ingested.spill {
                    Some("spill")
                } else {
                    Some("throughput_collapse")
                }
            } else if death.is_some() {
                Some("unified_device_death")
            } else {
                None
            },
            fit_samples: ingested.fit_samples,
            throughput_samples: ingested.throughput_samples,
            ramp_step: entry.ramp_step,
            deflation: entry.deflation,
            clean_windows: entry.clean_windows,
            max_units_measured: Self::anchor_locked(&state, entry),
            clamped_samples: ingested.clamps.len(),
            clamped_reason: clamp_log_field(&ingested.clamps),
            alloc_retries: ingested.alloc_retries,
        });
        // Keyed off the window line's own `negative_reason`, so they agree.
        let oom = window
            .as_ref()
            .filter(|window| window.negative_reason == Some("oom"))
            .and_then(|window| {
                oom_negative(
                    &window.inference_id,
                    &window.gpu,
                    ingested.oom_evidence.as_ref(),
                    frame_oom,
                    charge.map_or(0, |charge| charge.mb),
                    ingested.oom_samples,
                )
            });
        Settled {
            update,
            death,
            knee_expiry,
            window,
            oom,
            shape_ceiling: ingested.shape_ceiling,
            unrunnable,
        }
    }
}

/// The grant line's `canvas_pixels` field: the canvas, or `none` if uncapped.
pub(super) fn canvas_log_field(canvas_pixels: Option<u32>) -> String {
    canvas_pixels.map_or_else(|| "none".to_owned(), |pixels| pixels.to_string())
}

/// A window's memory grant: a MiB reservation (the ledger currency) and a
/// unit budget (the packing currency).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grant {
    pub unit_budget: u64,
    pub mb: u64,
    pub unit: CostUnit,
    pub aggregation: CostAggregation,
    /// The user's per-request max batch size, in items; never converted.
    pub user_cap_items: Option<u32>,
    /// Per-item pixel cap; `None` = uncapped. Worker and host both price an
    /// input at `min(raw_pixels, canvas_pixels)`.
    pub canvas_pixels: Option<u32>,
    /// Per-item token cap; `None` = uncapped. The `token`-unit twin of
    /// [`Self::canvas_pixels`].
    pub max_tokens: Option<u32>,
    /// Memory (the GPU's or host RAM), not the ramp, ratchet or queue, held
    /// this window back.
    pub squeezed: bool,
}

/// A held grant. Dropping it releases the reservation as an abort;
/// [`GrantToken::finish`] also accounts for the window. A hung worker holds
/// its grant indefinitely: `predict` has no deadline.
pub struct GrantToken {
    ledger: Arc<VramLedger>,
    worker: WorkerId,
    grant_id: u64,
    grant: Grant,
    settled: bool,
}

impl GrantToken {
    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    /// Release the grant and record the window's outcome. `Some` when this
    /// replica cannot run even one item.
    pub fn finish(mut self, outcome: WindowOutcome) -> Option<UnrunnableReplica> {
        self.settled = true;
        self.ledger.settle(self.worker, self.grant_id, outcome)
    }

    /// [`Self::finish`], handing the caller what the settle *produced* rather
    /// than logging it, so a test can assert on a diagnostic as the decision it
    /// is. The accounting is identical, but the post-lock hand-offs are the
    /// caller's: no alarm is emitted and no store update is recorded.
    #[cfg(test)]
    pub(super) fn finish_for_test(mut self, outcome: WindowOutcome) -> Settled {
        self.settled = true;
        self.ledger
            .settle_locked(self.worker, self.grant_id, outcome)
    }
}

impl Drop for GrantToken {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self
                .ledger
                .settle(self.worker, self.grant_id, WindowOutcome::Aborted);
        }
    }
}
