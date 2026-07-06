use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Per-phase timer for pipelined jobs whose items are processed by many
/// concurrent workers. Summing per-item wall-clock spans into one total would
/// count the same second once per in-flight worker (and, worse, include time
/// spent queued on semaphores), so totals could exceed the job duration by
/// orders of magnitude.
///
/// Tracks two totals instead:
/// - busy: the union of intervals during which at least one span was open —
///   "wall-clock time this phase was active", never exceeds job duration.
/// - work: the sum of individual span durations — "aggregate worker time";
///   work / busy is the phase's average parallelism.
///
/// Clones share the same totals, so the timer can be handed to blocking
/// worker closures. Lock scope is a few arithmetic ops; contention is
/// bounded by worker count.
#[derive(Clone, Default)]
pub(crate) struct PhaseTimer {
    inner: Arc<Mutex<PhaseTimerInner>>,
}

#[derive(Default)]
struct PhaseTimerInner {
    in_flight: u32,
    span_start: Option<Instant>,
    busy_secs: f64,
    work_secs: f64,
}

impl PhaseTimer {
    /// Opens a span; it closes when the returned guard drops.
    pub(crate) fn start(&self) -> PhaseSpan {
        let mut inner = self.inner.lock().expect("phase timer poisoned");
        if inner.in_flight == 0 {
            inner.span_start = Some(Instant::now());
        }
        inner.in_flight += 1;
        PhaseSpan {
            timer: self.clone(),
            start: Instant::now(),
        }
    }

    /// Wall-clock seconds during which at least one span was open, including
    /// the currently open one — safe to read for mid-job progress updates.
    pub(crate) fn busy_secs(&self) -> f64 {
        let inner = self.inner.lock().expect("phase timer poisoned");
        match inner.span_start {
            Some(start) => inner.busy_secs + start.elapsed().as_secs_f64(),
            None => inner.busy_secs,
        }
    }

    /// Summed duration of all closed spans (aggregate worker time).
    pub(crate) fn work_secs(&self) -> f64 {
        self.inner.lock().expect("phase timer poisoned").work_secs
    }

    fn finish_span(&self, span_work: f64) {
        let mut inner = self.inner.lock().expect("phase timer poisoned");
        inner.work_secs += span_work;
        inner.in_flight = inner.in_flight.saturating_sub(1);
        if inner.in_flight == 0 {
            if let Some(start) = inner.span_start.take() {
                inner.busy_secs += start.elapsed().as_secs_f64();
            }
        }
    }
}

pub(crate) struct PhaseSpan {
    timer: PhaseTimer,
    start: Instant,
}

impl Drop for PhaseSpan {
    fn drop(&mut self) {
        self.timer.finish_span(self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Overlapping spans must count shared wall-clock once for busy but per
    // span for work.
    #[test]
    fn overlapping_spans_union_busy_time() {
        let timer = PhaseTimer::default();
        let a = timer.start();
        let b = timer.start();
        std::thread::sleep(std::time::Duration::from_millis(30));
        drop(a);
        drop(b);
        let busy = timer.busy_secs();
        let work = timer.work_secs();
        assert!(busy >= 0.03, "busy {busy} should cover the sleep");
        assert!(
            work >= busy * 1.5,
            "work {work} should roughly double busy {busy}"
        );
    }

    // A gap between spans must not count toward busy time.
    #[test]
    fn gaps_between_spans_are_excluded() {
        let timer = PhaseTimer::default();
        drop(timer.start());
        let before = timer.busy_secs();
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(timer.busy_secs(), before);
    }

    // busy_secs must include the currently open span for progress reads.
    #[test]
    fn open_span_counts_toward_busy() {
        let timer = PhaseTimer::default();
        let _span = timer.start();
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(timer.busy_secs() >= 0.02);
    }
}
