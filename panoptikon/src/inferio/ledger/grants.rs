use super::*;

/// The `clamped` field of the settle line: what shrank this window's batches
/// below the budget they were granted — `"none"`, `"memory"` (the defensive
/// clamp, which is what a clamp naming no reason is), the reason the worker
/// named verbatim, or `"a+b"` in first-seen order. A free function so the
/// rendering is assertable as the decision it is, like [`canvas_log_field`].
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

/// How the settle line spells a clamp that named no reason: the wire's absence
/// means the defensive memory clamp (docs/inferio-worker-protocol.md).
const CLAMP_REASON_MEMORY: &str = "memory";

impl VramLedger {
    // ------------------------------------------------------------------
    // Grants
    // ------------------------------------------------------------------

    /// Units the dispatcher should aim to put in one window.
    pub(super) fn window_target_units(&self, worker: WorkerId) -> u64 {
        let mut state = self.lock();
        // This reads the deflation counter through `admitted_units`, and it is
        // the *first* thing an idle replica's next window asks — before the
        // grant path, which repays too late to size this window. A stale counter
        // here shrinks the window's content, which then bounds the grant.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get(&worker) else {
            return 1;
        };
        // The knee caps the *batch*, not the window: a window is still several
        // admitted batches deep, which is what gives bucketing material and
        // amortizes the round trip.
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
    ///
    /// `window_units` is the dispatcher's *estimate* of the window's priced
    /// content. Safety never depends on it: an over-estimate yields a bigger
    /// grant still clamped by headroom, an under-estimate more GPU batches per
    /// window — the worker packs within the grant using exact counts either way.
    pub(super) fn request_grant(
        self: &Arc<Self>,
        worker: WorkerId,
        window_units: u64,
        user_cap_items: Option<u32>,
        window_requests: usize,
        queued_behind: usize,
        byte_bound: bool,
    ) -> Option<GrantToken> {
        // Before anything prices against `headroom`: a neighbour mid-window has
        // been growing its pool since its last reply, and until that growth is
        // charged to it, it is charged to "some other process" and this window
        // is priced against a GPU that reads full. Ahead of the probe trigger,
        // which reads the same staleness clock these frames settle — one pass
        // per grant either way.
        {
            let mut state = self.lock();
            Self::refresh_pools_locked(&mut state);
        }
        self.maybe_refresh_external(worker);
        let mut state = self.lock();
        // Before anything reads the deflation counter to size this window.
        Self::repay_deflation_locked(&mut state, worker);
        let gpu = state.workers.get(&worker)?.gpu.clone();
        if let Some(entry) = state.workers.get_mut(&worker) {
            entry.pending_requests = window_requests.saturating_add(queued_behind);
        }
        // The headroom this window is priced against is the *requesting model's*:
        // an unconfirmed or scattered fit sees a widened margin, so it asks for
        // less of a GPU it may be mispricing. Every other worker's charge is
        // unaffected — their footprints are measured.
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
        ) = {
            let entry = state.workers.get(&worker)?;
            let anchor = Self::anchor_locked(&state, entry);
            let slope = Self::grant_slope_locked(&state, entry);
            let ceiling = Self::shape_ceiling_locked(&state, entry);
            let capped = admitted_units(entry, anchor, Self::knee_locked(&state, entry), ceiling);
            let wanted = capped.min(window_units.max(1)).max(1);
            // Did the *knee* decide this window's size? Both halves matter to
            // the expiry: the cap has to have bitten (`capped < uncapped`) and
            // the window has to have carried enough work to reach it, or a short
            // queue would count as a window run at the cap. The comparand keeps
            // the **shape ceiling** applied and drops only the knee, so clipped
            // windows cannot walk `knee_clean_windows` to its threshold.
            let knee_bound = capped < admitted_units(entry, anchor, None, ceiling)
                && wanted >= capped
                && capped > 0;
            // Was there room to have run wider? Against the requester's own
            // room, not the saturated headroom: a knee on a card its own pool
            // filled would otherwise never earn a widening. The comparand is
            // `RATCHET_FACTOR` times `slope × min(anchor, knee)`.
            let ample_headroom = (share.room as f64)
                >= self.appetite_mb_locked(&state, entry) * RATCHET_FACTOR as f64;
            let mut units = wanted;
            let mut mb = share.mb;
            // Whether *memory* is what held this window back, as opposed to the
            // ramp, the ratchet or the amount of work in hand — only the first
            // is worth trimming a neighbour for. `slope` comes from a *pricing*
            // fit, so a degenerate one is `None` here and the pre-fit branch runs
            // rather than leaving `squeezed` stuck at false and disabling the trim.
            let squeezed = if let Some(slope) = slope {
                // Post-fit the unit budget derives from the MB side via the
                // slope; pre-fit there is no slope, so the ramp value *is* the
                // unit budget and `share` is the contention share held while
                // that step is measured.
                let affordable = ((share.mb as f64) / slope).floor().max(1.0) as u64;
                let squeezed = affordable < wanted;
                units = units.min(affordable).max(1);
                mb = ((units as f64) * slope).ceil() as u64;
                squeezed
            } else {
                // A share of nothing prices nothing, so the ramp value would be
                // a memory-blind grant of the whole seed batch on a card with
                // no room for it. One unit is where the post-fit side lands on
                // the same share, and it is the smallest batch there is.
                if share.mb == 0 {
                    units = 1;
                }
                // Pre-fit there is nothing to convert MB into units with, so the
                // only visible squeeze is the contention floor. A share sitting
                // *at* its floor is not by itself evidence — an
                // appetite-weighted split on a wide-open GPU routinely clamps a
                // small claimant back up. The floor binds *because the GPU is
                // full* only when the floors do not all fit in the headroom.
                share.mb <= share.floor && headroom < share.floor_sum
            };
            (
                units,
                mb,
                entry.unit,
                entry.aggregation,
                entry.canvas_pixels,
                entry.max_tokens,
                squeezed,
                knee_bound,
                // A squeezed window never had room to spare, whatever the
                // arithmetic above says about the GPU as a whole.
                ample_headroom && !squeezed,
                // Less work in hand than the ramp would have admitted: this
                // window is about to run at the queue's size, not its own rung.
                wanted < capped,
            )
        };
        // The unit budget always admits at least one unit: a batch is never
        // smaller than one item, and a grant admitting zero would stall the
        // queue. The **MB** side carries no such floor — a worker whose share
        // rounded to nothing is charged nothing, which is honest.
        unit_budget = unit_budget.max(1);
        mb = mb.min(share.mb);
        if squeezed {
            // A **memory-blind** window (`mb == 0`) on a GPU with no headroom
            // left is priced against nothing, so the requester cannot ramp its
            // way back out. Its own pool makes it a trim candidate; a
            // neighbour's makes that neighbour one, busy or not, because the
            // credit means the holder is no longer squeezed into trimming
            // itself.
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
                },
            );
        // Now that this window is outstanding, every window on the GPU —
        // including this one — has one more overlapping neighbour than it may
        // have recorded.
        Self::note_occupancy_locked(&mut state, &gpu);
        // Snapshotted under the lock and emitted with it dropped, as the
        // registration and settle paths do: a `tracing` event formatted under
        // the ledger mutex puts every concurrent grant request behind a write.
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
                "issued a memory grant"
            );
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
                squeezed,
            },
            settled: false,
        })
    }

    /// Repay one level of deflation per [`DEFLATION_REPAY_SECS`] of wall time for
    /// one replica, and say so once per repayment. Called wherever the counter is
    /// about to be *read* for a decision rather than on a timer, so an idle
    /// replica's repayment lands the moment something asks.
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

    /// Bring every outstanding window on `gpu` up to date with the GPU's current
    /// occupancy (the contention tag). Called once per grant issue, the only
    /// moment occupancy can *rise*; falls are irrelevant, the tag being a
    /// high-water mark over the window's life. O(replicas on the GPU), and a GPU
    /// holds a handful.
    fn note_occupancy_locked(state: &mut LedgerState, gpu: &str) {
        let occupied = state
            .workers
            .values()
            .filter(|entry| entry.gpu == gpu && !entry.grants.is_empty())
            .count();
        // Each of those windows has `occupied − 1` neighbours right now, and
        // they all have the same number of them.
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

    /// Release a grant and account for its window. Called by
    /// [`GrantToken::finish`] and by its `Drop` (the abort path).
    ///
    /// **Telemetry is ingested on both outcomes; only the *accounting* differs.**
    /// An aborted window teaches the ledger nothing about the ramp, but its
    /// batches really did run and their samples sit above the watermark, where
    /// the *next* window's settle would pick them up and deflate an innocent
    /// window on an aborted one's OOM.
    fn settle(
        &self,
        worker: WorkerId,
        grant_id: u64,
        outcome: WindowOutcome,
    ) -> Option<UnrunnableReplica> {
        let settled = self.settle_locked(worker, grant_id, outcome);
        // Both handed over **after** the ledger lock is released: the store takes
        // its own lock and may schedule a write, and a `tracing` event formatted
        // under the ledger mutex puts every concurrent grant request behind it.
        if let Some(death) = settled.death {
            death.emit();
        }
        if let Some(expiry) = settled.knee_expiry {
            expiry.emit();
        }
        // Before the window's own line too: the ceiling is what explains the
        // `clamped=index_limit` field that line is about to carry.
        if let Some(ceiling) = settled.shape_ceiling {
            ceiling.emit();
        }
        // Before the window's own line, so the classification reads as the
        // reason for the negative that follows it.
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
        // Before the clean/negative bookkeeping below reads or moves it: a window
        // that ran longer than `DEFLATION_REPAY_SECS` has earned its time
        // repayment whatever its outcome was.
        Self::repay_deflation_locked(&mut state, worker);
        let Some(entry) = state.workers.get_mut(&worker) else {
            return Settled::default();
        };
        // Demand: this window's own requests are done with, whatever happened.
        // Without this a busy replica's demand signal stays frozen at its
        // grant-time value until the dispatcher calls `note_demand` again.
        let charge = entry.grants.remove(&grant_id);
        if let Some(charge) = charge {
            entry.pending_requests = entry.pending_requests.saturating_sub(charge.requests);
        }
        // What this window's batches were free to reach; see
        // [`FULL_BATCH_RATIO`] and [`knee_admits_window`].
        let granted_units = charge;
        // The idle clock the trim path reads starts here, not when the grant map
        // happens to be empty: a replica working through a queue is grantless
        // between every pair of windows. Stamped on every outcome — an aborted
        // window still had the pool.
        entry.last_grant_settled_at = Some(Instant::now());
        // And the window this replica just ran is what makes another idle
        // release worth asking for: the pool it could not hand back before has
        // been through a batch since.
        entry.idle_release_gave_nothing = false;
        // Any outcome other than a clean response means the fit snapshot this
        // window carried may never have been applied, and `fit_version_sent` is
        // bumped when the snapshot is *read*, so without this the worker would
        // never see it. Re-sending is free.
        if !matches!(outcome, WindowOutcome::Responded { oom: None }) {
            entry.fit_version_sent = 0;
        }
        // The fold-in may not promote the anchor to "measured here" out of a
        // window that failed: a lucky first batch at a size the window then
        // died at is not evidence for that size.
        let window_failed = matches!(
            outcome,
            WindowOutcome::WorkerDied | WindowOutcome::Responded { oom: Some(_) }
        );
        let ingested = Self::ingest_locked(&mut state, worker, granted_units, window_failed);
        // This window's worker had to free its allocator cache and retry a
        // `cudaMalloc`: the card is full *now*, so the neighbours are asked now
        // rather than at the 30 s idle release.
        if ingested.alloc_retries.is_some_and(|retries| retries > 0) {
            Self::flag_starved_neighbours_locked(&mut state, worker);
        }
        // The knee's expiry, if this window tripped it. Emitted with the
        // ledger lock dropped, like every other alarm here.
        let mut knee_expiry: Option<KneeExpired> = None;
        // Hoisted for the settle log only; the accounting below is unchanged.
        let mut responded_negative = false;
        // Which tier read the window's own error frame, when that is what
        // classified it.
        let frame_oom = match outcome {
            WindowOutcome::Responded { oom } => oom,
            _ => None,
        };
        if let WindowOutcome::Responded { oom } = outcome {
            let negative = ingested.negative || oom.is_some();
            responded_negative = negative;
            // Read *after* the ingest: this window's own priced batches have
            // moved the anchor, and the ramp grows from the exponent that anchor
            // implies. The ceiling is read here for the same reason — this
            // window's own `index_limit` clamps have already established or
            // retired the ceiling the ramp is about to be judged against.
            let (anchor, ceiling) = match state.workers.get(&worker) {
                Some(entry) => (
                    Self::anchor_locked(&state, entry),
                    Self::shape_ceiling_locked(&state, entry),
                ),
                None => (0, None),
            };
            // Read from the same ring and with the same sole-occupancy filter as
            // the knee fit, and after the ingest: this window's own rates are
            // part of the answer.
            // A knee stops the exponent too: the sizes a doubling would have to
            // prove itself at are the ones it refuses to grant, so an exponent
            // raised under it is only banked — and spent all at once, at
            // `RATCHET_FACTOR` × anchor a window, when the knee is withdrawn.
            // Withdrawal takes a wider window that measured a gain, which is
            // the ramp's way back up.
            let gate = self.ramp_gate_locked(&state, worker, anchor);
            let knee_binds = Self::knee_binds_locked(&state, worker);
            let may_grow = gate.gains && !knee_binds;
            // A gate that refused because the ring cannot yet *certify* the
            // size the ramp reached measured nothing there, and no evidence of
            // gain is no growth: that hold's rung is what **this replica ran**,
            // never the ratchet's next step. Only with no knee in force, where the
            // hold is the brake — under one the rung is the room the widening
            // probes in, and the frontier is uncertified because the cap has
            // held every grant below it until its samples aged out. A gate that
            // refused on a *measured* plateau keeps the wider rung either way.
            // `anchor == 0` is the sentinel that turns the ratchet ceiling off
            // altogether: nothing clean has been priced, so there is no
            // conceded doubling to take back.
            // Never the *anchor*: a profile confers one whatever this card's
            // headroom allows, so a seeded 512 on a host squeezed to 70 units
            // would be spent in one step the moment memory frees.
            let reached_here = state
                .workers
                .get(&worker)
                .and_then(|entry| cal_locked(&state, entry))
                .map(|cal| cal.max_units_measured_here)
                .unwrap_or(0);
            // With nothing measured at budget yet the rung is the **seed**: the
            // ramp's start and the contention floor, which only deflation goes
            // under. Never the conferred anchor, and never a window the queue
            // sized — a job's first window holds one item while the scanner
            // fills, and that one unit is evidence of nothing.
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
                    // Why this hold stands, for the one reader that has to tell
                    // a measurement from a silence: a knee or a measured plateau
                    // is learning, a rung the ring cannot certify is not.
                    entry.held_certified = entry.ramp_held && (gate.certified || knee_binds);
                    // A window the queue sized tested no rung, and a hold over
                    // such windows is not what this replica is waiting on.
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
        // Outside the `Responded` arm: a discrete-card out-of-memory hard enough
        // to kill the worker is the harshest form of what the backstop exists
        // for. A cancelled window lowers nothing — it reports no failure — and a
        // death the unified-memory path already halved is not halved twice.
        if death.is_none() && (frame_oom.is_some() || ingested.oom || died) {
            Self::lower_seeded_anchor_locked(&mut state, worker);
        }
        // Under the anchor and under deflation both: a window carrying one
        // item that failed for memory with less room than one item costs has
        // neither room to wait for nor a smaller batch to fall back on. Both
        // halves are needed — a one-item OOM on a card with room to spare is
        // the backstop's ordinary business (`calibfixture/oom_*`), and it
        // recovers.
        let unrunnable = self.note_floor_oom_locked(
            &mut state,
            worker,
            charge,
            frame_oom.is_some() || ingested.oom || died,
            matches!(outcome, WindowOutcome::Responded { .. }) && !responded_negative,
        );
        Self::refit_locked(&mut state, worker);
        self.refit_knee_locked(&mut state, worker);
        // No store, no write policy: there is nothing to hand an update to, and
        // evaluating it anyway would move `cal.persisted` to describe a write
        // that can never happen.
        let update = self
            .profiles
            .is_some()
            .then(|| Self::pending_update_locked(&mut state, worker))
            .flatten();
        // Read after every update this settle performs, so the line describes the
        // state the next window is priced against. Formatted by [`Self::settle`]
        // once the lock is dropped.
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
        // Keyed off the very `negative_reason` the window's own WARN prints, so
        // the tier line and the negative it explains can never disagree about
        // whether this window was an out-of-memory at all.
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

/// How the `issued a memory grant` line names the pixel canvas the window was
/// priced under: the canvas in force, or `none` for uncapped. Neither the grant
/// frame nor the load report is in the gateway's log, so without this field a
/// leg cannot evidence which canvas a window was priced at. A function rather
/// than an inline format so a test can pin what the line will carry.
pub(super) fn canvas_log_field(canvas_pixels: Option<u32>) -> String {
    canvas_pixels.map_or_else(|| "none".to_owned(), |pixels| pixels.to_string())
}

/// A window's memory grant: an MB reservation (the ledger currency) and a
/// unit budget (the packing currency).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Grant {
    pub unit_budget: u64,
    pub mb: u64,
    pub unit: CostUnit,
    pub aggregation: CostAggregation,
    /// The user's per-request "max batch size", forwarded as an item-count
    /// constraint. Never converted to units.
    pub user_cap_items: Option<u32>,
    /// The model's per-item **pixel canvas**: the largest number of decoded
    /// pixels one input can cost it, whatever resolution it was submitted at;
    /// `None` = uncapped. The worker prices every input at
    /// `min(raw_pixels, canvas_pixels)` before packing this budget and the host
    /// applies the same `min` in `dispatch::estimate_input_units`, so the two
    /// sides denominate one quantity by construction.
    pub canvas_pixels: Option<u32>,
    /// The model's per-item **token window**: the most tokens of one input
    /// that ever reach the GPU at once; `None` = uncapped. The `token`-unit
    /// twin of [`Self::canvas_pixels`], and capped on both sides for the same
    /// reason.
    pub max_tokens: Option<u32>,
    /// Whether *memory* is what held this window back, as opposed to the ramp,
    /// the ratchet or the amount of work in hand (the same flag that decides
    /// whether an idle neighbour is asked to trim). The dispatcher reads it to
    /// publish an in-flight figure derived from what the GPU could afford.
    pub squeezed: bool,
}

/// A held grant. Dropping it releases the reservation (the abort path);
/// [`GrantToken::finish`] releases it *and* accounts for the window. A **hung**
/// worker holds its grant indefinitely, deliberately: `predict` has no deadline
/// by standing policy, the memory genuinely is unavailable, and the contention
/// floors keep neighbours running until the operator restarts.
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

    /// Release the grant and record the window's outcome. `Some` when the
    /// settle found this replica unable to run even one item.
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
