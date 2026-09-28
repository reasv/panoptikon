use super::*;

/// The trim trigger: a squeezed window plus an **idle** resident holding pool slack
/// on the same GPU raises a routing signal for the manager.
#[test]
fn a_squeezed_window_flags_an_idle_resident_holding_pool_slack() {
    let ledger = ledger(10_000, no_margin());
    // The idle resident: 4000 base plus 1000 MiB of retained pool.
    let idle = loaded(Some(4000), Some(0));
    let _idle = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    // The hungry one: 4800 base, no pool of its own yet.
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();
    // footprints = (4000 + 1000) + 4800 = 9800; external = 10000 - 200 -
    // 9800 = 0; limit = 10000; headroom = 200 — below the 256 MiB
    // pre-fit contention floor, i.e. squeezed.
    assert_eq!(ledger.headroom_mb(GPU), 200);
    assert!(
        ledger.take_pending_trims().is_empty(),
        "nothing is flagged until someone actually comes up short"
    );

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let trims = ledger.take_pending_trims();
    assert_eq!(trims.len(), 1, "the idle resident is flagged, once");
    assert_eq!(trims[0].inference_id, "g/idle");
    assert_eq!(trims[0].worker, _idle.worker_id());
    assert!(
        ledger.take_pending_trims().is_empty(),
        "the queue is drained, not copied"
    );
    drop(token);

    // A flag nobody delivered leaves the resident a candidate: it still
    // holds every MiB, and the squeeze still needs it.
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(
        ledger.take_pending_trims().len(),
        1,
        "the undelivered flag cost the replica nothing, so it costs the \
         next squeeze nothing"
    );
    drop(token);

    // Debounce: once it has answered, a squeezed window right away
    // re-flags nothing.
    push_memory(&idle, 1200, 0);
    _idle.note_trimmed(released(1000));
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "the same resident is not re-flagged within TRIM_DEBOUNCE"
    );
    drop(token);
}

/// The three ways an idle resident is *not* worth trimming, each of which
/// would otherwise cost a resident its whole working set for nothing.
#[test]
fn trims_are_not_flagged_without_a_squeeze_slack_and_idleness() {
    // 1.
    let roomy = ledger(10_000, no_margin());
    let idle = loaded(Some(1000), Some(0));
    let _idle = roomy
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(1000), Some(0));
    let asking = roomy
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 7000, 1000);
    push_memory(&hungry, 7000, 0);
    roomy.ingest_all_for_test();
    assert_eq!(roomy.headroom_mb(GPU), 7000);
    let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        roomy.take_pending_trims().is_empty(),
        "a comfortable GPU never trims, however much pool a neighbour holds"
    );
    drop(token);

    // 2.
    let tight = ledger(10_000, no_margin());
    let idle = loaded(Some(4900), Some(0));
    let _idle = tight
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4900), Some(0));
    let asking = tight
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 100, TRIM_SLACK_MB - 1);
    push_memory(&hungry, 100, 0);
    tight.ingest_all_for_test();
    let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        tight.take_pending_trims().is_empty(),
        "below TRIM_SLACK_MB the trade is not worth making"
    );
    drop(token);

    // 3.
    let busy_gpu = ledger(10_000, no_margin());
    let busy = loaded(Some(4000), Some(0));
    let busy_admission = busy_gpu
        .register_worker("g/busy", item_cost(4), &busy, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = busy_gpu
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&busy, 200, 1000);
    push_memory(&hungry, 200, 0);
    busy_gpu.ingest_all_for_test();
    let held = busy_admission.request_grant(u64::MAX, None, 1, 0).unwrap();
    let token = asking.request_grant(u64::MAX, None, 1, 0).unwrap();
    assert!(
        busy_gpu.take_pending_trims().is_empty(),
        "a replica with a window in flight is never flagged"
    );
    drop(token);
    drop(held);
}

/// The relief path the credit would otherwise have removed. The resident
/// whose pool filled the card is no longer squeezed into self-trimming, so
/// the starved neighbour's own request is what reaches that pool: a
/// requester priced at `mb = 0` with no headroom flags the largest free
/// pool on the GPU, idle or not. The idle rule alone would never fire —
/// a replica running back-to-back windows is never idle for
/// [`IDLE_BEFORE_TRIM`].
#[test]
fn a_starved_neighbour_reaches_a_busy_residents_pool_through_a_trim() {
    let ledger = ledger(10_000, no_margin());
    let pinned = loaded(Some(1000), Some(0));
    let pinned_admission = ledger
        .register_worker("g/pinned", item_cost(4), &pinned, None)
        .unwrap();
    let neighbour = loaded(Some(200), Some(0));
    let neighbour_admission = ledger
        .register_worker("g/neighbour", item_cost(4), &neighbour, None)
        .unwrap();
    neighbour_admission.note_demand(1);
    push_memory(&pinned, 0, 8500);
    push_memory(&neighbour, 0, 0);
    ledger.ingest_all_for_test();

    // A window of the resident's own: it is not squeezed any more — that is
    // the credit — so it will not self-trim. Settling it leaves the pool
    // where it is and the resident *not* idle, exactly as a batch job
    // between two windows leaves it.
    let held = pinned_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(!held.grant().squeezed, "no longer its own squeeze");
    drop(held);
    assert!(
        ledger.take_pending_trims().is_empty(),
        "the resident's own window asks nothing of anyone"
    );

    let starved = neighbour_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(starved.grant().mb, 0);
    assert!(
        starved.grant().squeezed,
        "the neighbour is the squeezed one"
    );
    let trims = ledger.take_pending_trims();
    assert_eq!(
        trims
            .iter()
            .map(|trim| trim.inference_id.as_str())
            .collect::<Vec<_>>(),
        vec!["g/pinned"],
        "the busy resident holding the 8500 MiB is asked for it"
    );
    drop(starved);

    // The debounce still bounds it once the resident has answered: the next
    // squeezed window re-flags nothing, so a starved neighbour cannot trim
    // a resident per window.
    pinned_admission.note_trimmed(released(0));
    let starved = neighbour_admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(ledger.take_pending_trims().is_empty());
    drop(starved);
}

/// D2, now reached only by a genuine external squeeze: with the limit under
/// this replica's *base*, even its own pool cannot price a window, so the
/// blind grant stands and the requester is flagged for its own trim.
#[test]
fn a_memory_blind_window_flags_the_resident_whose_pool_filled_the_gpu() {
    let ledger = ledger(69_500, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/pinned", item_cost(4), &handle, None)
        .unwrap();
    // 1000 base + 8500 pool against a 60 000 MiB external tenant, whose
    // pre-fit margin bonus reserves a further 9000: limit = 500, under the
    // base alone, so the pool credit still leaves nothing to grant.
    push_memory(&handle, 0, 8500);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].headroom_mb, 0);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().mb, 0, "memory-blind, the D2 signature");
    assert!(token.grant().squeezed);
    let trims = ledger.take_pending_trims();
    assert_eq!(trims.len(), 1, "the requester is its own trim candidate");
    assert_eq!(trims[0].inference_id, "g/pinned");
    assert_eq!(trims[0].worker, admission.worker_id());
    drop(token);

    // And bounded, once it has answered, by the same debounce a
    // neighbour's trim is.
    push_memory(&handle, 8500, 0);
    admission.note_trimmed(released(8500));
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "not re-flagged on every window within TRIM_DEBOUNCE"
    );
    drop(token);
}

/// The other half of that rule: a resident squeezed to `mb = 0` by somebody
/// *else's* memory holds no pool worth releasing, and asking it to drop the
/// working set it is about to need again would buy the GPU nothing.
#[test]
fn a_memory_blind_window_does_not_flag_a_resident_holding_no_pool() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let admission = ledger
        .register_worker("g/starved", item_cost(4), &handle, None)
        .unwrap();
    // 1000 base + 100 pool; the other 8900 MiB is an external process.
    push_memory(&handle, 0, 100);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].headroom_mb, 0);

    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert_eq!(token.grant().mb, 0);
    assert!(
        ledger.take_pending_trims().is_empty(),
        "below TRIM_SLACK_MB the pool is not what filled this card"
    );
    drop(token);
}

/// After a trim lands, the released slack must stop being charged.
#[test]
fn a_trim_reply_releases_the_slack_from_the_footprint() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(4000), Some(0));
    let admission = ledger
        .register_worker("g/idle", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 5000, 1000);
    ledger.ingest_all_for_test();
    assert_eq!(
        ledger.health()[0].workers[0].footprint_mb,
        5000,
        "4000 base + 1000 pool growth"
    );

    // The worker answered `trim` and its reply's sample is in telemetry.
    push_memory(&handle, 6000, 0);
    admission.note_trimmed(released(1000));
    assert_eq!(
        ledger.health()[0].workers[0].footprint_mb,
        4000,
        "the pool is gone; only the base is still charged"
    );
    assert_eq!(
        ledger.health()[0].workers[0].reserved_mb,
        Some(0),
        "and the ledger's view of the pool matches what the worker reported"
    );
}

/// A pre-fit share landing on its contention floor is **not** a squeeze on its own.
#[test]
fn a_lopsided_pre_fit_split_on_a_wide_open_gpu_is_not_a_squeeze() {
    let ledger = ledger(200_000, no_margin());
    // The trim candidate: idle, and holding 1000 MiB of pool slack.
    let idle = loaded(Some(1000), Some(0));
    let _idle = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    // Two hungry pre-fit models, appetites 1 vs 4000.
    let small = loaded(Some(1), Some(0));
    let asking = ledger
        .register_worker("g/small", item_cost(4), &small, None)
        .unwrap();
    let big = loaded(Some(4000), Some(0));
    let other = ledger
        .register_worker("g/big", item_cost(4), &big, None)
        .unwrap();
    other.note_demand(3);
    // footprints = (1000 + 1000) + 1 + 4000 = 6001; external = 0.
    push_memory(&idle, 193_999, 1000);
    push_memory(&small, 193_999, 0);
    push_memory(&big, 193_999, 0);
    ledger.ingest_all_for_test();
    assert_eq!(
        ledger.headroom_mb(GPU),
        193_999,
        "nearly the whole 200 GB GPU is unclaimed"
    );

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(
        token.grant().mb <= SEED_BATCH_FLOOR_MB,
        "the premise: this share really did land on its floor ({} MiB)",
        token.grant().mb
    );
    assert!(
        ledger.take_pending_trims().is_empty(),
        "a floor reached by an uneven split on an empty GPU is not a squeeze"
    );
    drop(token);
}

/// Post-fit, the squeeze question is answered in units: the slice buys fewer units
/// than this window wanted.
#[test]
fn post_fit_a_squeeze_is_affordability_not_the_ramp() {
    // The ramp/ratchet case first: a GPU with room to spare, a fitted
    // model, and a budget bounded by what it has measured.
    let roomy = ledger(200_000, no_margin());
    let idle = loaded(Some(1000), Some(0));
    let _idle = roomy
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let handle = loaded(Some(1000), Some(0));
    let admission = roomy
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&idle, 190_000, 1000);
    push_memory(&handle, 190_000, 0);
    roomy.ingest_all_for_test();
    for units in [4, 8, 16] {
        measured_window(&handle, &admission, units);
    }
    let slope = roomy.health()[0]
        .workers
        .iter()
        .find(|worker| worker.inference_id == "g/a")
        .and_then(|worker| worker.fit.as_ref())
        .expect("fitted by now")
        .slope_mb_per_unit;
    assert!(slope > 0.0);
    roomy.take_pending_trims();
    let token = admission
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    assert!(
        (token.grant().unit_budget as f64) * slope < roomy.headroom_mb(GPU) as f64,
        "the premise: memory was nowhere near the binding constraint"
    );
    assert!(
        roomy.take_pending_trims().is_empty(),
        "a ratchet-bounded window must not trim a neighbour: freeing pool \
         cannot buy it a single extra unit"
    );
    drop(token);

    // And the real thing: the same fitted model on a GPU with almost
    // nothing left, where the slice genuinely cannot pay for the window.
    let tight = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let _idle = tight
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let handle = loaded(Some(4980), Some(0));
    let admission = tight
        .register_worker("g/a", item_cost(4), &handle, None)
        .unwrap();
    // footprints = (4000 + 1000) + 4980 = 9980; external = 0; headroom = 20.
    push_memory(&idle, 20, 1000);
    push_memory(&handle, 20, 0);
    tight.ingest_all_for_test();
    assert_eq!(tight.headroom_mb(GPU), 20);
    // 10 MiB/unit against a 20 MiB slice buys 2 units where even the seed
    // batch wants 4: memory, and nothing else, is the binding constraint.
    tight.install_fit_for_test(
        "g/a",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 10.0,
            intercept_mb: 0.0,
            residual_mb: 0.0,
            samples: 8,
            version: 1,
        },
    );
    tight.take_pending_trims();
    let token = admission
        .request_grant(1_000_000, None, 1, 0)
        .expect("granted");
    let trims = tight.take_pending_trims();
    assert_eq!(trims.len(), 1, "memory is what held this window back");
    assert_eq!(trims[0].inference_id, "g/idle");
    drop(token);
}

/// A fit whose slope is not positive prices nothing, so the pre-fit rule has to
/// take over.
#[test]
fn a_degenerate_fit_falls_back_to_the_pre_fit_squeeze_rule() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let _idle = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();
    ledger.install_fit_for_test(
        "g/hungry",
        GPU,
        FitSnapshot {
            slope_mb_per_unit: 0.0,
            intercept_mb: 0.0,
            residual_mb: 0.0,
            samples: 8,
            version: 1,
        },
    );

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(
        ledger.take_pending_trims().len(),
        1,
        "a slope of zero is 'no slope', which is exactly the pre-fit case"
    );
    drop(token);
}

/// Option 3: a replica that has stopped gives its pool back on the sweep,
/// with nobody squeezed and nobody asking. The timeout is the whole of the
/// rule, so it must also hold before it expires.
#[test]
fn a_stopped_replica_releases_its_pool_after_the_idle_timeout() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/stopped", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();
    clean_window(&resident);

    ledger.flag_idle_pool_releases();
    assert!(
        ledger.take_pending_trims().is_empty(),
        "a window settled a moment ago is between windows, not stopped"
    );

    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    let trims = ledger.take_pending_trims();
    assert_eq!(trims.len(), 1, "it has stopped and is holding 1000 MiB");
    assert_eq!(trims[0].inference_id, "g/stopped");
}

/// The idle release never touches a replica that is working: a grant
/// outstanding or a request queued is enough to keep the pool.
#[test]
fn a_working_replica_is_never_flagged_for_an_idle_release() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let busy = ledger
        .register_worker("g/busy", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();
    clean_window(&busy);
    ledger.age_trim_clocks_for_test(busy.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));

    // A window in flight: the clocks are old, the replica is not idle.
    let token = busy.request_grant(u64::MAX, None, 1, 0).expect("granted");
    ledger.flag_idle_pool_releases();
    assert!(
        ledger.take_pending_trims().is_empty(),
        "a replica holding a grant is running, whatever its clock says"
    );
    token.finish(WindowOutcome::Responded { oom: None });

    // And a queue behind it, with no grant outstanding at this instant.
    ledger.age_trim_clocks_for_test(busy.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));
    busy.note_demand(3);
    ledger.flag_idle_pool_releases();
    assert!(
        ledger.take_pending_trims().is_empty(),
        "requests are queued for it; it is between windows"
    );
}

/// A pool under [`TRIM_SLACK_MB`] is not worth a `cudaMalloc` to get back,
/// and the debounce bounds how often a replica that stays stopped is asked.
#[test]
fn the_idle_release_respects_the_slack_floor_and_the_debounce() {
    let ledger = ledger(10_000, no_margin());
    let small = loaded(Some(1000), Some(0));
    let thin = ledger
        .register_worker("g/thin", item_cost(4), &small, None)
        .unwrap();
    push_memory(&small, 6000, TRIM_SLACK_MB - 1);
    ledger.ingest_all_for_test();
    clean_window(&thin);
    ledger.age_trim_clocks_for_test(thin.worker_id(), IDLE_POOL_RELEASE + Duration::from_secs(1));
    ledger.flag_idle_pool_releases();
    assert!(
        ledger.take_pending_trims().is_empty(),
        "below TRIM_SLACK_MB the re-grow costs more than the pool is worth"
    );

    let fat = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/fat", item_cost(4), &fat, None)
        .unwrap();
    push_memory(&fat, 6000, 1000);
    ledger.ingest_all_for_test();
    clean_window(&resident);
    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    assert_eq!(ledger.take_pending_trims().len(), 1, "flagged once");
    // It answered, handing back 400 of the 1000 MiB.
    push_memory(&fat, 6400, 600);
    resident.note_trimmed(released(400));
    ledger.flag_idle_pool_releases();
    assert!(
        ledger.take_pending_trims().is_empty(),
        "the debounce holds: it is still stopped, and it just answered"
    );
    ledger.age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE);
    ledger.flag_idle_pool_releases();
    assert_eq!(
        ledger.take_pending_trims().len(),
        1,
        "the debounce is a delay, not a verdict"
    );
}

/// Option 2: a window whose worker paid allocator retries on a card with
/// nothing free asks its idle neighbours for their pools at once, without
/// waiting out [`IDLE_POOL_RELEASE`].
#[test]
fn a_window_that_paid_allocator_retries_flags_its_idle_neighbours() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(1000), Some(0));
    let neighbour = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let working = loaded(Some(1000), Some(0));
    let worker = ledger
        .register_worker("g/working", item_cost(4), &working, None)
        .unwrap();
    // The card has less free than the smallest pool worth reclaiming, and
    // the neighbour is holding 1000 MiB of it.
    push_memory(&idle, TRIM_SLACK_MB - 1, 1000);
    push_memory(&working, TRIM_SLACK_MB - 1, 0);
    ledger.ingest_all_for_test();
    clean_window(&neighbour);
    ledger.age_trim_clocks_for_test(
        neighbour.worker_id(),
        IDLE_BEFORE_TRIM + Duration::from_secs(1),
    );
    ledger.take_pending_trims();

    // A card this full squeezes the grant, so the *grant* path flags the
    // neighbour too. Draining and re-arming between the two halves is what
    // isolates the settle path this test is about.
    let quiet = TRIM_DEBOUNCE + IDLE_BEFORE_TRIM + Duration::from_secs(1);
    let settle_with = |retries: u64| {
        working
            .lock()
            .unwrap()
            .record_measurements(vec![BatchMeasurement {
                alloc_retries: Some(retries),
                ..measurement(4, 0, 10)
            }]);
        let token = worker.request_grant(u64::MAX, None, 1, 0).expect("granted");
        ledger.take_pending_trims();
        ledger.age_trim_clocks_for_test(neighbour.worker_id(), quiet);
        token.finish(WindowOutcome::Responded { oom: None });
        ledger.take_pending_trims()
    };

    assert!(
        settle_with(0).is_empty(),
        "no retries: the allocator was never short"
    );
    let trims = settle_with(3);
    assert_eq!(trims.len(), 1, "the idle neighbour is asked at once");
    assert_eq!(trims[0].inference_id, "g/idle");
}

/// The free-memory guard is the half that decides: a retry on a card with
/// room to spare is the allocator defragmenting, not a neighbour holding
/// the memory.
#[test]
fn allocator_retries_on_a_roomy_card_flag_nobody() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(1000), Some(0));
    let neighbour = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let working = loaded(Some(1000), Some(0));
    let worker = ledger
        .register_worker("g/working", item_cost(4), &working, None)
        .unwrap();
    push_memory(&idle, 6000, 1000);
    push_memory(&working, 6000, 0);
    ledger.ingest_all_for_test();
    clean_window(&neighbour);
    ledger.age_trim_clocks_for_test(
        neighbour.worker_id(),
        IDLE_BEFORE_TRIM + Duration::from_secs(1),
    );
    ledger.take_pending_trims();

    working
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            alloc_retries: Some(9),
            ..measurement(4, 0, 10)
        }]);
    worker
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted")
        .finish(WindowOutcome::Responded { oom: None });
    assert!(
        ledger.take_pending_trims().is_empty(),
        "6000 MiB free: nothing on this card is starved"
    );
}

/// The starvation trigger needs no exemption for the requester: the window
/// it just settled stamps `last_grant_settled_at`, so `idle_for` reads
/// false for it — even when it is the only replica on the card holding a
/// pool worth asking for.
#[test]
fn a_starved_requester_is_never_its_own_candidate() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let working = ledger
        .register_worker("g/working", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, TRIM_SLACK_MB - 1, 1000);
    ledger.ingest_all_for_test();
    ledger.take_pending_trims();

    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            alloc_retries: Some(3),
            ..measurement(4, 0, 10)
        }]);
    let token = working
        .request_grant(u64::MAX, None, 1, 0)
        .expect("granted");
    ledger.take_pending_trims();
    ledger.age_trim_clocks_for_test(working.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));
    token.finish(WindowOutcome::Responded { oom: None });
    assert!(
        ledger.take_pending_trims().is_empty(),
        "the replica that paid the retries holds the pool its next window \
         will use"
    );
}

/// The idle sweep spends at most [`MAX_IDLE_TRIMS_PER_SWEEP`] of the one
/// [`MAX_PENDING_TRIMS`] queue, and shares it between the cards: a card
/// full of stopped residents cannot leave another card's squeeze — which
/// has somebody waiting on the memory — without a slot.
#[test]
fn idle_flags_leave_the_shared_cap_for_another_cards_squeeze() {
    const A: &str = "GPU-aaaa";
    const B: &str = "GPU-bbbb";
    const RESIDENTS: usize = MAX_PENDING_TRIMS;
    let ledger = VramLedger::for_test(
        &[(A, "TEST 9000", 20_000), (B, "TEST 9000", 10_000)],
        no_margin(),
    );
    let handles: Vec<TelemetryHandle> = (0..RESIDENTS)
        .map(|_| loaded_on(A, Some(1), Some(0)))
        .collect();
    let residents: Vec<Admission> = handles
        .iter()
        .enumerate()
        .map(|(index, handle)| {
            ledger
                .register_worker(&format!("a/idle{index}"), item_cost(4), handle, None)
                .unwrap()
        })
        .collect();
    // Card B: a full card, a resident that stopped a moment ago (so the
    // squeeze path would take it) and a neighbour about to come up short.
    let on_b = loaded_on(B, Some(4000), Some(0));
    let resident_b = ledger
        .register_worker("b/idle", item_cost(4), &on_b, None)
        .unwrap();
    let hungry = loaded_on(B, Some(4800), Some(0));
    let asking = ledger
        .register_worker("b/hungry", item_cost(4), &hungry, None)
        .unwrap();
    for handle in &handles {
        push_memory(handle, 9000, 300);
    }
    push_memory(&on_b, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();
    for resident in &residents {
        clean_window(resident);
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
    }
    clean_window(&resident_b);
    ledger.age_trim_clocks_for_test(
        resident_b.worker_id(),
        IDLE_BEFORE_TRIM + Duration::from_secs(1),
    );
    ledger.take_pending_trims();

    // The sweep flags card A's stopped residents, then card B's squeeze
    // arrives before the manager has drained anything.
    ledger.flag_idle_pool_releases();
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let trims = ledger.take_pending_trims();
    assert!(
        trims.len() <= MAX_IDLE_TRIMS_PER_SWEEP + 1,
        "the sweep spent its budget, not the whole queue: {}",
        trims.len()
    );
    assert!(
        trims.iter().any(|trim| trim.inference_id == "b/idle"),
        "card B's squeeze found a slot for the neighbour holding its pool"
    );
    drop(token);

    // And with both cards holding stopped residents, the budget is split:
    // card A's 32 do not spend card B's share of it either.
    ledger.age_trim_clocks_for_test(
        resident_b.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    let trims = ledger.take_pending_trims();
    assert_eq!(
        trims
            .iter()
            .filter(|trim| trim.inference_id.starts_with("a/"))
            .count(),
        MAX_IDLE_TRIMS_PER_SWEEP.div_ceil(2),
        "card A took half the budget, not all of it"
    );
    assert!(trims.iter().any(|trim| trim.inference_id == "b/idle"));
}

/// An idle flag the dispatcher drops costs the replica nothing, so it must
/// cost the next squeeze nothing either: `try_trim` returns without acting
/// whenever the model has work queued or the replica is not in the free
/// pool, and the request is never re-queued.
#[test]
fn an_undelivered_idle_flag_does_not_burn_the_debounce_a_squeeze_needs() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let resident = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();
    clean_window(&resident);
    ledger.take_pending_trims();

    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    assert_eq!(ledger.take_pending_trims().len(), 1, "flagged as idle");
    // Dropped on the floor, as `try_trim` does with a busy replica.

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let trims = ledger.take_pending_trims();
    assert_eq!(
        trims.len(),
        1,
        "the squeeze reaches the neighbour still holding its whole pool"
    );
    assert_eq!(trims[0].worker, resident.worker_id());
    drop(token);

    // A decline is an answer, and does start the debounce.
    resident.note_trim_declined();
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "it was asked and it said no; asking again now repeats the answer"
    );
    drop(token);
}

/// A flag still sitting in the queue is not raised a second time: the
/// debounce no longer stands in for that, and the sweep runs every tick.
#[test]
fn a_flag_already_queued_is_not_queued_again() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/idle", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();
    clean_window(&resident);
    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );

    ledger.flag_idle_pool_releases();
    ledger.flag_idle_pool_releases();
    ledger.flag_idle_pool_releases();
    assert_eq!(
        ledger.take_pending_trims().len(),
        1,
        "three sweeps with nobody draining leave one request, not three"
    );
}

/// A release that handed nothing back stops the idle asking until the
/// replica settles a window. `empty_cache()` frees only wholly-unused
/// segments, so a stopped resident's remainder does not shrink by being
/// asked again: S6-contend-idle asked MobileCLIP four times in two minutes
/// and was told "handed back 0 MiB" every time.
#[test]
fn a_stopped_replica_whose_pool_returns_nothing_is_asked_once() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/pinned", item_cost(4), &handle, None)
        .unwrap();
    // `reserved == allocated`: `empty_cache` can hand back nothing, while
    // `pool_growth_mb` (reserved − reserved_at_load) reads 1000.
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();
    clean_window(&resident);

    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    assert_eq!(ledger.take_pending_trims().len(), 1, "asked once");
    // The worker replies ok with an unchanged pool, which is what it does
    // when every segment still holds a live tensor.
    push_memory(&handle, 6000, 1000);
    resident.note_trimmed(released(0));

    for round in 0..3 {
        ledger.age_trim_clocks_for_test(
            resident.worker_id(),
            IDLE_POOL_RELEASE + Duration::from_secs(1),
        );
        ledger.flag_idle_pool_releases();
        assert!(
            ledger.take_pending_trims().is_empty(),
            "round {round}: asked again although the last release returned \
             nothing"
        );
    }
    assert_eq!(
        ledger.health()[0].workers[0].pool_releases,
        Some(0),
        "measured, and none of it counted as a release"
    );

    // A settled window is the evidence that the pool has been through a
    // batch since, so the ask is worth making again.
    clean_window(&resident);
    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_POOL_RELEASE + Duration::from_secs(1),
    );
    ledger.flag_idle_pool_releases();
    assert_eq!(ledger.take_pending_trims().len(), 1, "asked after a window");
}

/// The latch is on the *idle* trigger alone: a neighbour that is actually
/// short still gets to ask, because a squeeze has somebody paying for the
/// silence.
#[test]
fn a_latched_resident_is_still_a_candidate_for_a_squeeze() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let resident = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();
    clean_window(&resident);
    push_memory(&idle, 200, 1000);
    resident.note_trimmed(released(0));
    ledger.take_pending_trims();
    ledger.age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let trims = ledger.take_pending_trims();
    assert_eq!(trims.len(), 1, "the squeeze reaches it anyway");
    assert_eq!(trims[0].worker, resident.worker_id());
    drop(token);
}

/// `pool_releases` counts MiB handed back, not replies: `trim` answers
/// `ok` from a CPU-priced host and from a pool whose every segment still
/// holds a live tensor. S6-contend-idle counted 5 releases, 4 of which
/// returned nothing.
#[test]
fn a_release_that_handed_nothing_back_is_not_counted_as_one() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/pinned", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();

    resident.note_trimmed(released(0));
    assert_eq!(
        ledger.health()[0].workers[0].pool_releases,
        Some(0),
        "the worker replied ok and handed back nothing"
    );
    assert_eq!(ledger.health()[0].workers[0].last_release_mb, Some(0));

    push_memory(&handle, 6600, 400);
    resident.note_trimmed(released(600));
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(
        worker.pool_releases,
        Some(1),
        "this one gave the card 600 MiB"
    );
    assert_eq!(worker.last_release_mb, Some(600));
    assert_eq!(worker.last_release_ms, Some(12.0));
}

/// The re-grow fields describe one population: the first batch after a
/// release the **host** asked for. The worker's own reactive shrink also
/// re-grows, and `pool_releases` never counted it, so reporting it here
/// would show a re-grow with no release beside it.
#[test]
fn a_reactive_shrinks_regrow_is_not_reported_as_a_trims() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(1000), Some(0));
    let resident = ledger
        .register_worker("g/self", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 6000, 1000);
    ledger.ingest_all_for_test();

    // Released inside `maybe_shrink`: the host was never asked.
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            regrow_mb: Some(410),
            regrow_after: Some("shrink".to_owned()),
            duration_ms: Some(542.9),
            ..measurement(4, 0, 900)
        }]);
    clean_window(&resident);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.last_regrow_mb, None, "nobody asked for that pool");
    assert_eq!(
        worker.pool_releases, None,
        "nothing was ever asked of it, so nothing was measured"
    );

    // The batch after a trim, which is what the fields are for.
    handle
        .lock()
        .unwrap()
        .record_measurements(vec![BatchMeasurement {
            regrow_mb: Some(866),
            regrow_after: Some("trim".to_owned()),
            duration_ms: Some(979.6),
            ..measurement(4, 0, 900)
        }]);
    clean_window(&resident);
    let worker = &ledger.health()[0].workers[0];
    assert_eq!(worker.last_regrow_mb, Some(866));
    assert_eq!(
        worker.last_regrow_batch_ms,
        Some(979.6),
        "that batch's whole wall time, which contains the cudaMallocs"
    );
}

/// Idleness is "has held no grant for a while", not "holds none at this instant".
#[test]
fn a_replica_between_windows_is_not_yet_idle_enough_to_trim() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let resident = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();

    // The resident just finished a window: grantless, but not idle.
    clean_window(&resident);
    ledger.take_pending_trims();

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "a replica that settled a window a moment ago is between windows, \
         not finished with them"
    );
    drop(token);

    // Once the quiet period has passed, the same squeeze does flag it.
    ledger.age_trim_clocks_for_test(
        resident.worker_id(),
        IDLE_BEFORE_TRIM + Duration::from_secs(1),
    );
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let trims = ledger.take_pending_trims();
    assert_eq!(trims.len(), 1, "it has now genuinely stopped");
    assert_eq!(trims[0].inference_id, "g/idle");
    drop(token);
}

/// The debounce is a delay, not a verdict: a resident that goes on squeezing its
/// neighbours is asked again once [`TRIM_DEBOUNCE`] has passed.
#[test]
fn the_trim_debounce_expires_and_the_resident_is_asked_again() {
    let ledger = ledger(10_000, no_margin());
    let idle = loaded(Some(4000), Some(0));
    let resident = ledger
        .register_worker("g/idle", item_cost(4), &idle, None)
        .unwrap();
    let hungry = loaded(Some(4800), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    push_memory(&idle, 200, 1000);
    push_memory(&hungry, 200, 0);
    ledger.ingest_all_for_test();

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(ledger.take_pending_trims().len(), 1, "flagged once");
    drop(token);
    resident.note_trimmed(released(0));
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert!(
        ledger.take_pending_trims().is_empty(),
        "and not again inside the debounce"
    );
    drop(token);

    ledger.age_trim_clocks_for_test(resident.worker_id(), TRIM_DEBOUNCE + Duration::from_secs(1));
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(
        ledger.take_pending_trims().len(),
        1,
        "the squeeze is still on, so the resident is asked again"
    );
    drop(token);
}

/// The pending-trim queue is bounded.
#[test]
fn the_pending_trim_queue_is_capped_and_the_rest_are_flagged_next_time() {
    const RESIDENTS: usize = MAX_PENDING_TRIMS + 8;
    // Cheap residents: 1 MiB of base each, 300 MiB of pool slack.
    let footprints = (RESIDENTS as u64) * 301 + 1;
    let ledger = ledger(footprints + 159, no_margin());
    let handles: Vec<TelemetryHandle> = (0..RESIDENTS).map(|_| loaded(Some(1), Some(0))).collect();
    let _residents: Vec<Admission> = handles
        .iter()
        .enumerate()
        .map(|(index, handle)| {
            ledger
                .register_worker(&format!("g/idle{index}"), item_cost(4), handle, None)
                .unwrap()
        })
        .collect();
    let hungry = loaded(Some(1), Some(0));
    let asking = ledger
        .register_worker("g/hungry", item_cost(4), &hungry, None)
        .unwrap();
    for handle in &handles {
        push_memory(handle, 159, 300);
    }
    push_memory(&hungry, 159, 0);
    ledger.ingest_all_for_test();
    assert_eq!(ledger.headroom_mb(GPU), 159, "the GPU is full");

    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    let flagged = ledger.take_pending_trims();
    assert_eq!(
        flagged.len(),
        MAX_PENDING_TRIMS,
        "the queue is capped, not unbounded"
    );
    drop(token);
    // Each of those answered — with nothing to give, which still starts
    // its debounce and leaves the card as full as it was.
    for trim in &flagged {
        let index: usize = trim.inference_id["g/idle".len()..].parse().unwrap();
        _residents[index].note_trimmed(released(0));
    }
    let token = asking.request_grant(u64::MAX, None, 1, 0).expect("granted");
    assert_eq!(
        ledger.take_pending_trims().len(),
        RESIDENTS - MAX_PENDING_TRIMS,
        "the residents that did not fit are picked up next squeeze; the ones \
         that did are inside their debounce"
    );
    drop(token);
}

/// The trim's memory fold is freshness-guarded on **both** halves.
#[test]
fn a_stale_sample_never_re_charges_a_trimmed_pool() {
    let ledger = ledger(10_000, no_margin());
    let handle = loaded(Some(4000), Some(0));
    let admission = ledger
        .register_worker("g/idle", item_cost(4), &handle, None)
        .unwrap();
    push_memory(&handle, 5000, 1000);
    let pre_trim = handle.lock().unwrap().memory.clone().expect("a sample");
    ledger.ingest_all_for_test();
    assert_eq!(ledger.health()[0].workers[0].footprint_mb, 5000);

    // A trim whose reply carried a fresh sample: the pool is gone.
    push_memory(&handle, 6000, 0);
    admission.note_trimmed(released(1000));
    assert_eq!(ledger.health()[0].workers[0].footprint_mb, 4000);

    // A second trim, answered by a worker that could measure nothing: the
    // freshest sample in telemetry is still the pre-trim one.
    handle.lock().unwrap().memory = Some(pre_trim);
    admission.note_trimmed(TrimReply::default());
    assert_eq!(
        ledger.health()[0].workers[0].footprint_mb,
        4000,
        "the older reading must not re-charge the released slack"
    );
    assert_eq!(ledger.health()[0].workers[0].reserved_mb, Some(0));
}
