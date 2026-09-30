//! Returning freed heap memory to the OS (glibc on Linux only).
//!
//! glibc keeps freed blocks in its arenas and hands the pages back only when
//! asked. By default it runs up to eight arenas per core, one per thread in
//! practice, and a block freed in one arena cannot serve a thread bound to
//! another, so a job's buffers, freed on some fifty runtime threads, stayed
//! resident after the job. The Windows heap and macOS malloc release freed
//! pages on their own; there both calls are no-ops.

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

#[cfg(all(test, target_os = "linux", target_env = "gnu"))]
mod tests {
    use super::*;

    #[test]
    fn glibc_accepts_the_arena_cap() {
        assert!(limit_arenas());
    }

    /// Pages of `[start, start + len)` that are resident, whole pages only.
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
