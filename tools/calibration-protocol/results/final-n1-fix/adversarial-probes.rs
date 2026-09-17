// Verifier adversarial probes for fix/cpu-knee-gate (ca228f26), inserted into
// `mod tests` in panoptikon/src/inferio/ledger.rs and run with
// `cargo test -p panoptikon v_probe -- --nocapture` (and v_probe2). NOT part
// of the commit; the tree was restored with `git checkout` afterwards.
// Recorded output: adversarial-probes.out.

    // ================= VERIFIER ADVERSARIAL PROBES (not for merge) =========

    /// A ledger whose single test GPU carries an explicit knee band.
    fn banded_ledger(total_mb: u64, band: f64) -> Arc<VramLedger> {
        VramLedger::for_test(
            &[(GPU, "TEST 9000", total_mb)],
            VramBudget {
                margin: Some(0.0),
                cap_fraction: None,
                knee_max_bucket_dispersion: Some(band),
            },
        )
    }

    /// Run a whole job: `first` is the warm-up window, `tail` the window after
    /// it, then four batches each at every plateau rung.
    fn job_knee(
        band: f64,
        first: &[(u64, f64)],
        tail: &[(u64, f64)],
        plateau: &[(u64, f64)],
    ) -> Option<u64> {
        let ledger = banded_ledger(100_000, band);
        let handle = loaded(Some(1000), Some(0));
        let admission = ledger
            .register_worker("g/a", item_cost(1), &handle, None)
            .unwrap();
        push_memory(&handle, 90_000, 1000);
        warm_window(&handle, &admission, first);
        warm_window(&handle, &admission, tail);
        for (units, rate_) in plateau {
            warm_window(&handle, &admission, &[(*units, *rate_); 4]);
        }
        ledger.health()[0].workers[0].knee_units
    }

    #[test]
    fn v_probe() {
        const N1_TAIL: [(u64, f64); 3] = [(2, 2.12), (2, 3.00), (2, 4.08)];
        const N1_TAIL4: [(u64, f64); 4] = [(2, 2.12), (2, 3.00), (2, 4.08), (2, 2.30)];
        let plateau = [(8u64, 100.0), (16, 100.0), (32, 100.0), (64, 100.0)];

        let mut t4 = N1_TAIL4.iter().map(|(_, r)| *r).collect::<Vec<_>>();
        println!("P0 relmad tail3={:?} tail4={:?}",
            relative_mad(&mut N1_TAIL.iter().map(|(_, r)| *r).collect::<Vec<_>>()),
            relative_mad(&mut t4));

        // (a) deep vs thin first window, honest second window that is the only
        // evidence for its bucket.
        let honest = [(8u64, 100.0); 3];
        let up = [(16u64, 100.0), (32, 100.0), (64, 100.0)];
        println!("P1 deep-first + honest w2, band .20 -> {:?}",
            job_knee(0.20, &[(4u64, 40.0); 3], &honest, &up));
        println!("P2 thin-first + honest w2, band .20 -> {:?}",
            job_knee(0.20, &[(4u64, 40.0)], &honest, &up));

        // (a) the N1 shape at both bands and both first-window depths.
        println!("P3 thin-first + N1 3-batch tail, band .20 -> {:?}",
            job_knee(0.20, &[(1u64, 2.0)], &N1_TAIL, &plateau));
        println!("P4 deep-first + N1 3-batch tail, band .20 -> {:?}",
            job_knee(0.20, &[(1u64, 2.0); 3], &N1_TAIL, &plateau));
        println!("P5 deep-first + N1 3-batch tail, band .35 -> {:?}",
            job_knee(0.35, &[(1u64, 2.0); 3], &N1_TAIL, &plateau));
        println!("P6 thin-first + N1 4-batch tail, band .20 -> {:?}",
            job_knee(0.20, &[(1u64, 2.0)], &N1_TAIL4, &plateau));
        println!("P7 thin-first + N1 4-batch tail, band .35 -> {:?}",
            job_knee(0.35, &[(1u64, 2.0)], &N1_TAIL4, &plateau));

        // (b) band width: a plateau whose per-bucket scatter is 0.30.
        let scatter = |m: f64| vec![m * 0.7, m, m * 1.3];
        let ring = |rows: &[(u64, f64)]| -> Vec<ThroughputSample> {
            let mut out = Vec::new();
            for (units, m) in rows {
                for r in scatter(*m) {
                    out.extend(rate(*units, r, 1));
                }
            }
            out
        };
        let flat = ring(&[(4, 40.0), (8, 100.0), (16, 100.0), (32, 100.0), (64, 100.0)]);
        let (flat, fa) = stamped(&flat);
        println!("P8 plateau+0.30 scatter band .20 -> {:?} | band .35 -> {:?}",
            fit_knee(&flat, 0.0, fa, None, 0.20).and_then(|f| f.knee_units),
            fit_knee(&flat, 0.0, fa, None, 0.35).and_then(|f| f.knee_units));
        let rising = ring(&[(4, 50.0), (8, 100.0), (16, 200.0), (32, 400.0), (64, 800.0)]);
        let (rising, ra) = stamped(&rising);
        println!("P9 rising+0.30 scatter band .35 -> knee {:?} gains {}",
            fit_knee(&rising, 0.0, ra, None, 0.35).and_then(|f| f.knee_units),
            ramp_still_gains(&rising, ra, 1, 0.35));
        let creeping = ring(&[(4, 50.0), (8, 100.0), (16, 115.0), (32, 132.0), (64, 152.0)]);
        let (creeping, ca) = stamped(&creeping);
        println!("P10 +15%/doubling +0.30 scatter band .35 -> knee {:?} gains {}",
            fit_knee(&creeping, 0.0, ca, None, 0.35).and_then(|f| f.knee_units),
            ramp_still_gains(&creeping, ca, 1, 0.35));

        // (c) how long a transient noisy bucket holds the ramp.
        let mut noisy: Vec<ThroughputSample> = Vec::new();
        noisy.extend(rate(8, 70.0, 2));
        noisy.extend(rate(8, 130.0, 2));
        noisy.extend(rate(16, 100.0, 3));
        noisy.extend(rate(32, 140.0, 3));
        let (base, na) = stamped(&noisy);
        println!("P11 transient noisy low bucket, anchor {} -> gains {}",
            na, ramp_still_gains(&base, na, 1, 0.20));
        // Keep running at the frontier; the ring evicts at KNEE_RING.
        let mut ring_now = base.clone();
        let mut windows = 0;
        let mut freed = None;
        while windows < 200 {
            windows += 1;
            for _ in 0..3 {
                ring_now.push(ThroughputSample {
                    units: 32,
                    units_per_sec: 140.0,
                    occupants: 0,
                    seq: 1000 + windows as u64,
                    anchor: na,
                    warmup: false,
                    warmup_tail: false,
                });
            }
            while ring_now.len() > KNEE_RING {
                ring_now.remove(0);
            }
            if ramp_still_gains(&ring_now, na, 1, 0.20) {
                freed = Some(windows);
                break;
            }
        }
        println!("P12 clean 3-batch windows at the frontier until gains again: {freed:?}");

        // (d) mixed host: CUDA + CPU.
        let mixed = crate::inferio::gpu::GpuInventory::known(vec![nvidia(
            0, "GPU-1a2b", "TEST 9000", 32_607,
        )])
        .with_cpu(CPU_RAM_MB, crate::inferio::cpu::MemRoots::default());
        let resolved = with_shipped_gpu_defaults(&mixed, VramBudgets::default());
        println!("P13 mixed host: gpu band {} cpu band {} cpu cap {:?} gpu cap {:?}",
            resolved.for_gpu("GPU-1a2b").knee_dispersion_in_force(),
            resolved.for_gpu("CPU").knee_dispersion_in_force(),
            resolved.for_gpu("CPU").cap_fraction,
            resolved.for_gpu("GPU-1a2b").cap_fraction);
        let led = VramLedger::new(&mixed, VramBudgets::default(), None);
        println!("P14 via VramLedger::new: gpu {} cpu {}",
            led.budgets.for_gpu("GPU-1a2b").knee_dispersion_in_force(),
            led.budgets.for_gpu("CPU").knee_dispersion_in_force());
        // section-wide user value reaches the CPU too
        let section = with_shipped_gpu_defaults(
            &mixed,
            VramBudgets::uniform(VramBudget {
                knee_max_bucket_dispersion: Some(0.28),
                ..VramBudget::default()
            }),
        );
        println!("P15 section-wide 0.28: gpu {} cpu {}",
            section.for_gpu("GPU-1a2b").knee_dispersion_in_force(),
            section.for_gpu("CPU").knee_dispersion_in_force());
    }

    #[test]
    fn v_probe2() {
        // (c) noise in the FRONTIER bucket instead of a low one: later clean
        // samples land in the same bucket and the median-based MAD collapses.
        let mut noisy: Vec<ThroughputSample> = Vec::new();
        noisy.extend(rate(8, 100.0, 3));
        noisy.extend(rate(16, 120.0, 3));
        noisy.extend(rate(32, 100.0, 2));
        noisy.extend(rate(32, 190.0, 2));
        let (base, na) = stamped(&noisy);
        println!("Q1 noise AT the frontier ({na}) -> gains {}", ramp_still_gains(&base, na, 1, 0.20));
        let mut ring_now = base.clone();
        let mut freed = None;
        for w in 1..=200u64 {
            for _ in 0..3 {
                ring_now.push(ThroughputSample { units: 32, units_per_sec: 145.0, occupants: 0,
                    seq: 1000 + w, anchor: na, warmup: false, warmup_tail: false });
            }
            while ring_now.len() > KNEE_RING { ring_now.remove(0); }
            if ramp_still_gains(&ring_now, na, 1, 0.20) { freed = Some(w); break; }
        }
        println!("Q2 clean windows until the frontier bucket reads again: {freed:?}");

        // (b) the hard half: does 0.35 admit a FAKE plateau that 0.20 refuses?
        // Asymmetric noise that drags a rising bucket's own median down onto
        // its neighbour's. True curve 100 -> 150 a doubling.
        let bucket = |units: u64, rates: &[f64]| -> Vec<ThroughputSample> {
            rates.iter().flat_map(|r| rate(units, *r, 1)).collect()
        };
        let mut fake: Vec<ThroughputSample> = Vec::new();
        fake.extend(bucket(4, &[40.0, 40.0, 40.0]));
        fake.extend(bucket(8, &[100.0, 100.0, 100.0]));
        fake.extend(bucket(16, &[100.0, 100.0, 100.0, 100.0, 133.0, 150.0, 150.0]));
        fake.extend(bucket(32, &[100.0, 100.0, 100.0, 100.0, 133.0, 150.0, 150.0]));
        fake.extend(bucket(64, &[100.0, 100.0, 100.0]));
        let (fake, fa) = stamped(&fake);
        let mut b16 = vec![100.0, 100.0, 100.0, 100.0, 133.0, 150.0, 150.0];
        println!("Q3 displaced-median bucket relmad {:?}", relative_mad(&mut b16));
        println!("Q3 fake plateau: band .20 -> {:?} | band .35 -> {:?}",
            fit_knee(&fake, 0.0, fa, None, 0.20).and_then(|f| f.knee_units),
            fit_knee(&fake, 0.0, fa, None, 0.35).and_then(|f| f.knee_units));

        // (b) the honest control at the same shape.
        let mut honest: Vec<ThroughputSample> = Vec::new();
        honest.extend(bucket(4, &[40.0, 40.0, 40.0]));
        honest.extend(bucket(8, &[100.0, 100.0, 100.0]));
        honest.extend(bucket(16, &[105.0, 150.0, 195.0]));
        honest.extend(bucket(32, &[157.0, 225.0, 292.0]));
        honest.extend(bucket(64, &[236.0, 337.0, 438.0]));
        let (honest, ha) = stamped(&honest);
        println!("Q4 honest rising, 0.30 scatter: band .35 knee {:?} gains {}",
            fit_knee(&honest, 0.0, ha, None, 0.35).and_then(|f| f.knee_units),
            ramp_still_gains(&honest, ha, 1, 0.35));

        println!("Q5 KNEE_WARMUP_BATCHES = {KNEE_WARMUP_BATCHES}, WINDOW_DEPTH_MULTIPLIER = {WINDOW_DEPTH_MULTIPLIER}");
    }
