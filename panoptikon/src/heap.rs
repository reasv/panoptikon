//! Returning freed heap memory to the OS (glibc on Linux only).
//!
//! glibc keeps freed blocks in its arenas and hands the pages back only when
//! asked. By default it runs up to eight arenas per core, one per thread in
//! practice, and a block freed in one arena cannot serve a thread bound to
//! another, so a job's buffers, freed on some fifty runtime threads, stayed
//! resident after the job. The Windows heap and macOS malloc release freed
//! pages on their own; there both calls are no-ops.
//!
//! Memory is returned when a job ends and, for work outside jobs (inference
//! served to other gateways, search, the API), once the server has been idle
//! for [`IDLE_TRIM_AFTER`].

use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How long the server must be idle (no request in flight, no job running)
/// before it returns what the finished work freed.
const IDLE_TRIM_AFTER: Duration = Duration::from_secs(10);

/// The server's in-flight requests and running jobs.
pub static ACTIVITY: LazyLock<Activity> = LazyLock::new(Activity::new);

/// glibc arenas for the whole process. Few enough that freed blocks are
/// reused across threads rather than kept per thread.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const ARENA_MAX: libc::c_int = 4;

/// Cap glibc's arena count at [`ARENA_MAX`], unless the operator set
/// `MALLOC_ARENA_MAX`. Call before any thread starts. Returns whether glibc
/// accepted the cap.
pub fn limit_arenas() -> bool {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
            return true;
        }
        // SAFETY: `mallopt` only changes allocator parameters.
        unsafe { libc::mallopt(libc::M_ARENA_MAX, ARENA_MAX) == 1 }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        true
    }
}

/// Hand every arena's free pages back to the OS (`malloc_trim(0)`). Takes up
/// to a few hundred milliseconds after a large job: call it off the async
/// runtime.
pub fn return_freed_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: `malloc_trim` only releases pages no allocation uses.
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Work in progress, and whether any has finished since the last idle trim.
pub struct Activity {
    started: Instant,
    busy: AtomicUsize,
    /// Milliseconds after `started` when the last piece of work finished.
    last_done_ms: AtomicU64,
    /// Work finished since the last idle trim.
    dirty: AtomicBool,
}

/// One request or job; it counts as work until dropped.
pub struct Busy<'a>(&'a Activity);

impl Activity {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            busy: AtomicUsize::new(0),
            last_done_ms: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
        }
    }

    pub fn enter(&self) -> Busy<'_> {
        self.busy.fetch_add(1, Ordering::SeqCst);
        Busy(self)
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// True once per idle period: nothing in flight, work finished since the
    /// last time, and none for `quiet`.
    fn take_idle(&self, quiet: Duration) -> bool {
        let quiet_ms = u64::try_from(quiet.as_millis()).unwrap_or(u64::MAX);
        self.busy.load(Ordering::SeqCst) == 0
            && self
                .elapsed_ms()
                .saturating_sub(self.last_done_ms.load(Ordering::SeqCst))
                >= quiet_ms
            && self.dirty.swap(false, Ordering::SeqCst)
    }
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0
            .last_done_ms
            .store(self.0.elapsed_ms(), Ordering::SeqCst);
        self.0.dirty.store(true, Ordering::SeqCst);
        self.0.busy.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Middleware: every request counts as work while its handler runs.
pub async fn track_request(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _busy = ACTIVITY.enter();
    next.run(request).await
}

/// Return freed memory whenever the server has been idle for
/// [`IDLE_TRIM_AFTER`] after some work. Call once, inside the runtime.
pub fn spawn_idle_trim() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    tokio::spawn(async {
        let mut tick = tokio::time::interval(IDLE_TRIM_AFTER / 2);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if ACTIVITY.take_idle(IDLE_TRIM_AFTER) {
                let _ = tokio::task::spawn_blocking(return_freed_memory).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_trim_is_due_once_after_work_and_never_during_it() {
        let activity = Activity::new();
        assert!(!activity.take_idle(Duration::ZERO), "no work yet");
        let request = activity.enter();
        let job = activity.enter();
        drop(request);
        assert!(!activity.take_idle(Duration::ZERO), "a job is running");
        drop(job);
        assert!(
            !activity.take_idle(Duration::from_secs(3600)),
            "not quiet long enough"
        );
        assert!(activity.take_idle(Duration::ZERO));
        assert!(!activity.take_idle(Duration::ZERO), "once per idle period");
        drop(activity.enter());
        assert!(activity.take_idle(Duration::ZERO), "new work, a new period");
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn glibc_accepts_the_arena_cap() {
        assert!(limit_arenas());
    }

    /// Pages of `[start, start + len)` that are resident, whole pages only.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn resident_pages(start: usize, len: usize) -> (usize, usize) {
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let first = start.div_ceil(page) * page;
        let end = (start + len) / page * page;
        if end <= first {
            return (0, 0);
        }
        let pages = (end - first) / page;
        let mut vec = vec![0u8; pages];
        // SAFETY: the range lies inside a live heap mapping; `vec` has one
        // byte per page.
        let rc =
            unsafe { libc::mincore(first as *mut libc::c_void, end - first, vec.as_mut_ptr()) };
        assert_eq!(rc, 0, "mincore failed");
        (vec.iter().filter(|byte| **byte & 1 == 1).count(), pages)
    }

    /// Blocks freed between live ones stay resident, since glibc returns only
    /// the top of a heap by itself; the trim releases their pages.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn the_trim_releases_blocks_freed_between_live_ones() {
        std::thread::spawn(|| {
            const BLOCK: usize = 100 * 1024;
            let mut blocks: Vec<Vec<u8>> = (0..256).map(|_| vec![1u8; BLOCK]).collect();
            let freed: Vec<usize> = blocks
                .iter()
                .step_by(2)
                .map(|b| b.as_ptr() as usize)
                .collect();
            let mut index = 0;
            blocks.retain(|_| {
                index += 1;
                index % 2 == 0
            });
            let resident = || {
                freed
                    .iter()
                    .map(|start| resident_pages(*start, BLOCK))
                    .fold((0, 0), |(r, t), (dr, dt)| (r + dr, t + dt))
            };
            let (kept, total) = resident();
            return_freed_memory();
            let (left, _) = resident();
            drop(blocks);
            assert!(total > 0);
            assert!(
                kept * 4 > total * 3,
                "only {kept} of {total} freed pages stayed resident"
            );
            assert!(
                left * 4 < total,
                "{left} of {total} freed pages are still resident"
            );
        })
        .join()
        .unwrap();
    }
}
