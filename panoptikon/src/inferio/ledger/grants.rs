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
        // A window is several batches deep.
        Self::budget_locked(&state, entry)
            .saturating_mul(WINDOW_DEPTH_MULTIPLIER)
            .max(1)
    }

    /// Items the dispatcher may put in one window. Under an item cap
    /// ([`Self::item_cap_locked`]) that is [`WINDOW_DEPTH_MULTIPLIER`]
    /// batches of it, whatever the unit budget, and one item while the cap
    /// is one, the replica's first window; else no bound.
    pub(super) fn window_item_bound(&self, worker: WorkerId) -> usize {
        let state = self.lock();
        let cap = state
            .workers
            .get(&worker)
            .and_then(|entry| Self::item_cap_locked(&state, entry));
        match cap {
            None => usize::MAX,
            Some(1) => 1,
            Some(cap) => usize::try_from(u64::from(cap).saturating_mul(WINDOW_DEPTH_MULTIPLIER))
                .unwrap_or(usize::MAX),
        }
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
        let pressure = self.memory_pressure();
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
        // The batch size, and what of it the window's content asks for. An
        // item cap (the user's included) limits the content like a short
        // queue: for a count-priced model it is a unit count.
        let (size_asked, capped, wanted, item_cap) = {
            let entry = state.workers.get(&worker)?;
            let capped = Self::budget_locked(&state, entry);
            let item_cap = Self::item_cap_locked(&state, entry)
                .map(|cap| user_cap_items.map_or(cap, |user| cap.min(user)));
            let content = match item_cap {
                Some(cap) if entry.aggregation == CostAggregation::Count => {
                    window_units.min(u64::from(cap))
                }
                _ => window_units,
            };
            (
                Self::size_locked(&state, entry),
                capped,
                capped.min(content.max(1)).max(1),
                item_cap,
            )
        };
        let share = self.share_locked(&state, worker, signed_headroom, wanted);
        let (
            mut unit_budget,
            mut mb,
            unit,
            aggregation,
            canvas_pixels,
            max_tokens,
            squeezed,
            queue_bound,
            ram_mb,
            ram_bound,
            ram_mb_per_unit,
            fixed_mb,
        ) = {
            let entry = state.workers.get(&worker)?;
            let price = Self::grant_price_locked(&state, entry);
            let mut units = wanted;
            let mut mb = share.mb;
            // Memory, not the batch size, ratchet or queue, held this window back.
            let squeezed = if let Some(price) = price {
                // Post-fit the unit budget is what the share affords.
                let affordable = price.units(share.mb).max(1);
                let squeezed = affordable < wanted;
                units = units.min(affordable).max(1);
                mb = price.cost_mb(units);
                squeezed
            } else {
                // Pre-fit the batch size is the unit budget; with no share at
                // all, one unit, as post-fit.
                if share.mb == 0 {
                    units = 1;
                }
                // Beside another replica's reservation, or once two sizes of
                // this model measured its price here, at most the batch that
                // the share and what the replica holds cover at its pre-fit
                // price.
                let within = share
                    .mb
                    .saturating_add(entry.growth_in_use_mb())
                    .max(entry.pool_growth_mb());
                let price = Self::pre_fit_price_locked(&state, entry);
                let covered = price.units(within, units);
                let cut = covered < units
                    && (price.has_measured_rise()
                        || Self::neighbour_reserved_locked(&state, worker));
                if cut {
                    units = Self::cut_size_locked(&state, entry, covered);
                }
                // Otherwise squeezed means held at the floor while the floors
                // do not all fit.
                cut || (share.mb <= share.floor && headroom < share.floor_sum)
            };
            // Host RAM caps a GPU replica's batch as the edge of a full card
            // would; the GPU side above is unchanged.
            let ram = self.ram_ceiling_locked(&state, entry);
            let ram_bound = ram.is_some_and(|ram| ram.units < units);
            if let Some(ram) = ram.filter(|_| ram_bound) {
                units = ram.units;
                if let Some(price) = price {
                    mb = price.cost_mb(units);
                }
            }
            // A trial's look-ahead is run in full or not at all.
            if units < wanted
                && (squeezed || ram_bound)
                && let Some(working) = Self::look_ahead_from_locked(&state, entry)
                && working < units
            {
                units = working;
                if let Some(price) = price {
                    mb = price.cost_mb(units);
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
                // queue_bound: less work in hand than the batch size admits.
                wanted < capped,
                ram_mb,
                ram_bound,
                ram_mb_per_unit,
                price.map_or(0, |price| price.fixed_mb as u64),
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
        // The room itself, through the fitted price, set this batch's size.
        let room_bound = squeezed
            && share.mb == share.room
            && state
                .workers
                .get(&worker)
                .and_then(|entry| Self::grant_price_locked(&state, entry))
                .is_some_and(|price| unit_budget == price.units(share.mb).max(1));
        // What the booking may take out of free host RAM: the replica reuses
        // the growth it already holds.
        let ram_new_mb = state.workers.get(&worker).map_or(0, |entry| {
            let held = entry.ram_growth_mb().saturating_sub(entry.ram_booked_mb());
            ram_mb.saturating_sub(held)
        });
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
                    size_asked,
                    granted_at: Instant::now(),
                    squeezed,
                    room_bound,
                    peak_occupants: 0,
                    queue_bound,
                    byte_bound,
                    ram_mb,
                    ram_bound,
                    pressure,
                    item_cap,
                },
            );
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.ram_bound = ram_bound;
        }
        Self::note_occupancy_locked(&mut state, &gpu);
        // Logged after the lock is dropped.
        let external_mb = Self::external_locked(&state, &gpu).unwrap_or(0);
        let (reserve_mb, reserve_rule) = self.reserve_locked(&state, &gpu, external_mb, margin);
        // The worker's clamp keeps the host RAM reserve free: its device's
        // own when that is host RAM, else the CPU device's for a RAM booking.
        let ram_reserve_mb = if Self::host_ram_mb_locked(&state, &gpu).is_some() {
            reserve_mb
        } else if ram_new_mb > 0 {
            let external = Self::external_locked(&state, cpu::DEVICE_KEY).unwrap_or(0);
            let margin = self.budgets.for_gpu(cpu::DEVICE_KEY).margin_in_force();
            self.reserve_locked(&state, cpu::DEVICE_KEY, external, margin)
                .0
        } else {
            0
        };
        let issued = state.workers.get(&worker).map(|entry| {
            (
                entry.inference_id.clone(),
                Self::knee_locked(&state, entry).unwrap_or(0),
                entry.deflation,
                Self::pricing_fit_locked(&state, entry).is_none(),
            )
        });
        drop(state);
        if let Some((model, working_units, deflation, pre_fit)) = issued {
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
                ram_reserve_mb,
                pre_fit,
                working_units,
                deflation,
                squeezed,
                window_requests,
                ram_mb,
                ram_bound,
                memory_pressure = ?pressure,
                item_cap = ?item_cap,
                "issued a memory grant"
            );
            if ram_bound && item_cap.is_none() && RAM_BOUND_LOG.admit_for(&format!("{model} {gpu}"))
            {
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
                user_cap_items: item_cap.or(user_cap_items),
                canvas_pixels,
                max_tokens,
                squeezed: squeezed || ram_bound,
                fixed_mb: fixed_mb.min(mb),
                ram_mb: ram_new_mb,
                ram_reserve_mb,
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
                died = verdict.died,
                windows = OOM_WINDOWS_AT_FLOOR,
                "this model cannot run a single item on this GPU; failing it \
                 instead of dispatching to it again"
            );
        }
        settled.unrunnable
    }

    fn settle_locked(&self, worker: WorkerId, grant_id: u64, outcome: WindowOutcome) -> Settled {
        let pressure = self.memory_pressure();
        let mut state = self.lock();
        // Time repayment first, whatever the outcome.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get_mut(&worker) else {
            return Settled::default();
        };
        // This window's requests leave the demand signal on every outcome.
        // Pressure that began while the window was out counts as well.
        let charge = entry.grants.remove(&grant_id).map(|charge| GrantCharge {
            pressure: charge.pressure.max(pressure),
            ..charge
        });
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
        let mut responded_negative = false;
        let frame_oom = match outcome {
            WindowOutcome::Responded { oom } => oom,
            _ => None,
        };
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom.is_some();
            responded_negative = negative;
            // Read after the ingest, which may move it.
            let anchor = state
                .workers
                .get(&worker)
                .map_or(0, |entry| Self::anchor_locked(&state, entry));
            if let Some(entry) = state.workers.get_mut(&worker) {
                if negative {
                    entry.note_negative_sample(anchor);
                } else {
                    entry.note_clean_window();
                }
            }
            if let Some(charge) = charge {
                let filled = !negative && ingested.filled;
                Self::note_pressure_size_locked(&mut state, worker, charge, filled);
            }
        }
        let died = matches!(outcome, WindowOutcome::WorkerDied);
        let death = died
            .then(|| Self::note_death_locked(&mut state, worker, charge))
            .flatten();
        if !matches!(outcome, WindowOutcome::Aborted) {
            let failed = responded_negative || died;
            self.note_gain_locked(&mut state, worker, charge, ingested.at_budget, failed);
        }
        // Any OOM or death lowers a seeded anchor, unless the unified-memory
        // death path already halved it.
        if death.is_none() && (frame_oom.is_some() || ingested.oom || died) {
            Self::lower_seeded_anchor_locked(&mut state, worker);
        }
        // An out-of-memory window the room sized was priced too low.
        if let Some(charge) =
            charge.filter(|_| responded_negative && (frame_oom.is_some() || ingested.oom))
        {
            Self::raise_pool_margin_locked(&mut state, worker, charge);
        }
        // A one-item OOM with less room than one item costs; see
        // [`OOM_WINDOWS_AT_FLOOR`].
        let unrunnable = self.note_floor_oom_locked(
            &mut state,
            worker,
            charge,
            frame_oom.is_some() || ingested.oom || died,
            died,
            matches!(outcome, WindowOutcome::Responded { .. }) && !responded_negative,
        );
        Self::refit_locked(&mut state, worker);
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
            working_units: Self::knee_locked(&state, entry).unwrap_or(0),
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
    /// The user's per-request max batch size, in items; never converted. At
    /// most the item cap while the host RAM cost is unknown.
    pub user_cap_items: Option<u32>,
    /// Per-item pixel cap; `None` = uncapped. Worker and host both price an
    /// input at `min(raw_pixels, canvas_pixels)`.
    pub canvas_pixels: Option<u32>,
    /// Per-item token cap; `None` = uncapped. The `token`-unit twin of
    /// [`Self::canvas_pixels`].
    pub max_tokens: Option<u32>,
    /// Memory (the GPU's or host RAM), not the batch size, ratchet or queue,
    /// held this window back.
    pub squeezed: bool,
    /// The part of `mb` a batch costs whatever its size; 0 pre-fit.
    pub fixed_mb: u64,
    /// Host RAM a GPU replica's window may add to its resident set: its
    /// booking on the CPU device less the growth it already holds; 0 if none.
    pub ram_mb: u64,
    /// Free host RAM the worker's live clamp leaves alone: the reserve of the
    /// grant's device when that device is host RAM (the CPU device, or the
    /// Mac GPU against `hw.memsize`), otherwise the CPU device's reserve for
    /// a RAM booking; 0 when neither.
    pub ram_reserve_mb: u64,
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

    /// Make the grant `ms` older, as if its window had been out that long.
    #[cfg(test)]
    pub(super) fn age_for_test(&self, ms: f64) {
        let mut state = self.ledger.lock();
        let charge = state
            .workers
            .get_mut(&self.worker)
            .and_then(|entry| entry.grants.get_mut(&self.grant_id));
        if let Some(charge) = charge {
            let age = Duration::from_secs_f64(ms / 1000.0);
            charge.granted_at = charge.granted_at.checked_sub(age).expect("a recent boot");
        }
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
