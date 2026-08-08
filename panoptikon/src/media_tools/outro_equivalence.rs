//! Dev-only driver for the outro-detector equivalence harness
//! (`tools/outro-equivalence`, design §12): runs `detect_outro` over a list of
//! real files and writes one machine-readable record per file, so the same
//! sample can be pushed through the Python reference of §3.3 and the two
//! verdict/K streams compared exactly.
//!
//! **Why an `#[ignore]`d test and not an example or a dev bin.** `panoptikon`
//! is a bin-only crate (no `[lib]`, no `lib.rs`), so an `examples/` target or
//! a second `src/bin` entry would be a separate crate root with no access to
//! `media_tools::outro` — every item of which is `pub(crate)`. A unit test
//! inside the crate is the only form with that access, and being `cfg(test)`
//! it adds nothing at all to the shipped binary's surface.
//!
//! Driven entirely by environment variables so it stays a leaf with no
//! argument parsing and no CLI surface:
//!
//! | var | meaning |
//! |---|---|
//! | `OUTRO_EQUIV_INPUT` | TSV manifest, one job per line: `width<TAB>height<TAB>path`. The dims are the item's stored (coded) ones and may be empty. |
//! | `OUTRO_EQUIV_OUTPUT` | JSONL results, one object per input line, in input order. |
//! | `OUTRO_EQUIV_FFMPEG` / `OUTRO_EQUIV_FFPROBE` | pin the toolchain so both sides of the comparison shell out to the *same* binaries. Set both: leaving one unset sends `media_tools::resolve` looking for a venv. |
//! | `OUTRO_EQUIV_JOBS` | worker threads (default 8). The probe is process-spawn bound (§5), so this is nearly linear. |
//!
//! Run it as:
//!
//! ```text
//! cargo test --bin panoptikon outro_equivalence -- --ignored --nocapture
//! ```

use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use super::outro::{OutroProbeError, OutroVerdict, detect_outro};

struct Job {
    path: PathBuf,
    dims: Option<(u32, u32)>,
}

/// One input line: `width<TAB>height<TAB>path`. The path is last so it may
/// contain anything but a newline (Windows paths, spaces, unicode).
fn parse_job(line: &str) -> Option<Job> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return None;
    }
    let mut fields = line.splitn(3, '\t');
    let width = fields.next()?.trim();
    let height = fields.next()?.trim();
    let path = fields.next()?;
    if path.is_empty() {
        return None;
    }
    let dims = match (width.parse::<u32>(), height.parse::<u32>()) {
        (Ok(width), Ok(height)) if width > 0 && height > 0 => Some((width, height)),
        _ => None,
    };
    Some(Job {
        path: PathBuf::from(path),
        dims,
    })
}

fn record(job: &Job, elapsed_ms: u128, outcome: &Result<OutroVerdict, OutroProbeError>) -> String {
    let value = match outcome {
        Ok(OutroVerdict::TiktokCard { k_seconds }) => serde_json::json!({
            "path": job.path.to_string_lossy(),
            "status": "ok",
            "verdict": "tiktok_card",
            // Exactly as computed (frames / 30); serde_json's shortest
            // round-trip float repr parses back to the same double in Python,
            // so the comparison can demand bit equality.
            "k": k_seconds,
            "gate": true,
            "kind_value": OutroVerdict::TiktokCard { k_seconds: *k_seconds }.kind_value(),
            "ms": elapsed_ms as u64,
        }),
        Ok(OutroVerdict::None(reason)) => serde_json::json!({
            "path": job.path.to_string_lossy(),
            "status": "ok",
            "verdict": "none",
            "reason": reason.as_str(),
            // Stage 1 is the only rejector that runs before stage 2, so the
            // reason is exactly what the gate decided.
            "gate": reason.as_str() != "gate",
            "k": serde_json::Value::Null,
            "ms": elapsed_ms as u64,
        }),
        Err(error) => serde_json::json!({
            "path": job.path.to_string_lossy(),
            "status": "error",
            "class": match error {
                OutroProbeError::Spawn(_) => "spawn",
                OutroProbeError::Decode(_) => "decode",
            },
            "error": error.to_string(),
            "ms": elapsed_ms as u64,
        }),
    };
    value.to_string()
}

#[test]
#[ignore = "equivalence harness (design §12): needs OUTRO_EQUIV_INPUT and real media"]
fn outro_equivalence_harness() {
    let input = std::env::var("OUTRO_EQUIV_INPUT")
        .expect("OUTRO_EQUIV_INPUT: path to the TSV manifest (width<TAB>height<TAB>path)");
    let output = std::env::var("OUTRO_EQUIV_OUTPUT")
        .expect("OUTRO_EQUIV_OUTPUT: path to write the JSONL results to");

    // Pin the toolchain before anything resolves it, so the reference and the
    // detector are demonstrably shelling out to the same ffmpeg. Both paths
    // must be set or `resolve` goes venv-hunting for the missing one.
    if let (Ok(ffmpeg), Ok(ffprobe)) = (
        std::env::var("OUTRO_EQUIV_FFMPEG"),
        std::env::var("OUTRO_EQUIV_FFPROBE"),
    ) {
        crate::config::install_runtime_for_tests(crate::config::RuntimeConfig {
            ffmpeg: Some(PathBuf::from(&ffmpeg)),
            ffprobe: Some(PathBuf::from(ffprobe)),
            ..Default::default()
        });
        // `install_runtime_for_tests` swallows an already-set `OnceLock`, and
        // `resolved()` caches on first use — so a pin can silently lose to
        // whatever ran first and the comparison would quietly be against a
        // different ffmpeg build. Fail loudly instead of measuring the wrong
        // thing.
        assert_eq!(
            super::ffmpeg(),
            std::ffi::OsStr::new(&ffmpeg),
            "the pinned ffmpeg lost to an earlier resolution"
        );
    }
    let resolved = PathBuf::from(super::ffmpeg());
    eprintln!("outro-equivalence: ffmpeg = {}", resolved.display());

    let manifest = std::fs::read_to_string(&input).expect("the manifest is readable");
    let jobs: Vec<Job> = manifest.lines().filter_map(parse_job).collect();
    assert!(!jobs.is_empty(), "the manifest listed no files");
    let workers: usize = std::env::var("OUTRO_EQUIV_JOBS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|count| *count > 0)
        .unwrap_or(8);
    eprintln!(
        "outro-equivalence: {} files over {workers} workers",
        jobs.len()
    );

    let results: Vec<Mutex<String>> = jobs.iter().map(|_| Mutex::new(String::new())).collect();
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(job) = jobs.get(index) else { break };
                    let at = Instant::now();
                    let outcome = detect_outro(&job.path, job.dims);
                    let line = record(job, at.elapsed().as_millis(), &outcome);
                    *results[index].lock().expect("no worker panicked") = line;
                    let seen = done.fetch_add(1, Ordering::Relaxed) + 1;
                    if seen % 50 == 0 {
                        eprintln!(
                            "outro-equivalence: {seen}/{} ({:.1}s)",
                            jobs.len(),
                            started.elapsed().as_secs_f64()
                        );
                    }
                }
            });
        }
    });

    let mut file =
        std::io::BufWriter::new(std::fs::File::create(&output).expect("the output opens"));
    // A header record — no `path`, so the comparison reads it as provenance
    // rather than a result — carrying the ffmpeg this run actually resolved.
    // Without it the pin is only asserted in a log line nobody keeps.
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "harness": "outro_equivalence",
            "ffmpeg": resolved.to_string_lossy(),
            "files": jobs.len(),
            "workers": workers,
        })
    )
    .expect("the output is writable");
    for result in &results {
        let line = result.lock().expect("no worker panicked");
        writeln!(file, "{line}").expect("the output is writable");
    }
    file.flush().expect("the output is writable");
    eprintln!(
        "outro-equivalence: wrote {} records to {output} in {:.1}s",
        jobs.len(),
        started.elapsed().as_secs_f64()
    );
}
