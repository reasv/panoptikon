//! Apple Silicon (MPS) GPU facts, read from the macOS kernel.
//!
//! One synthetic unified-memory device per host, with a constant key, named
//! from `machdep.cpu.brand_string` and sized from `hw.memsize`. Live free
//! memory comes from `host_statistics64`. Off macOS every reader returns
//! `None`. See docs/unified-memory-admission.md "Backend A: MPS (Apple
//! Silicon)".

use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::gpu::{GpuInfo, GpuMemory};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// The one device key an MPS host has, and the string a user types into
/// `[inference_local.vram.gpu."GPU-MPS"]`.
pub(super) const DEVICE_KEY: &str = "GPU-MPS";

/// The two kernel facts an MPS device is derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HostFacts {
    /// `machdep.cpu.brand_string`, e.g. `Apple M3 Max`.
    pub chip: String,
    /// `hw.memsize`, physical RAM in bytes.
    pub ram_bytes: u64,
}

/// This host's facts, or `None` off Apple Silicon (including an Intel Mac
/// configured for `mps`) or when the sysctls do not answer.
pub(super) fn probe() -> Option<HostFacts> {
    #[cfg(target_os = "macos")]
    {
        if !cfg!(target_arch = "aarch64") {
            return None;
        }
        Some(HostFacts {
            chip: sysctl_string("machdep.cpu.brand_string")?,
            ram_bytes: sysctl_u64("hw.memsize").filter(|bytes| *bytes > 0)?,
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

/// The MPS device. `total_mb` is a seed (Metal's default working-set limit,
/// 75 % of RAM); the real `recommendedMaxWorkingSetSize`, which
/// `iogpu.wired_limit_mb` changes, is taken from the first load report.
pub(super) fn gpu(facts: &HostFacts) -> GpuInfo {
    let ram_mb = facts.ram_bytes / MIB;
    GpuInfo {
        index: 0,
        uuid: DEVICE_KEY.to_owned(),
        name: gpu_name(&facts.chip, facts.ram_bytes),
        total_mb: seed_total_mb(ram_mb),
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        unified_ram_mb: Some(ram_mb),
        vram_carveout_mb: None,
    }
}

/// Metal's default recommended working-set size: three quarters of RAM.
fn seed_total_mb(ram_mb: u64) -> u64 {
    ram_mb / 4 * 3
}

/// The device name: `Apple M3 Max (128 GB)`, RAM rounded to the nearest GiB.
pub(super) fn gpu_name(chip: &str, ram_bytes: u64) -> String {
    let gb = ((ram_bytes + GIB / 2) / GIB).max(1);
    format!("{chip} ({gb} GB)")
}

/// The device's live free reading, or `None` when RAM statistics could not
/// be read. Not clamped to the device total, which may since have been
/// replaced by the worker's figure; the ledger clamps.
pub(super) fn query_memory(key: &str, ram_mb: u64) -> Option<Vec<GpuMemory>> {
    let available = ram_available_mb()?;
    Some(vec![GpuMemory {
        uuid: key.to_owned(),
        // Physical RAM, not the device total; the refresh reads only `free_mb`.
        total_mb: ram_mb,
        free_mb: free_mb(ram_mb, available),
    }])
}

/// What the OS could deliver, bounded by physical RAM.
fn free_mb(ram_mb: u64, ram_available_mb: u64) -> u64 {
    ram_available_mb.min(ram_mb)
}

/// This Mac's physical RAM in MiB, or `None` off macOS.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) fn physical_ram_mb() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        sysctl_u64("hw.memsize")
            .filter(|bytes| *bytes > 0)
            .map(|bytes| bytes / MIB)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

/// macOS's memory pressure, from `kern.memorystatus_vm_pressure_level` and
/// whether the kernel is swapping pages out ([`Swapouts::pressure`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(super) enum MemoryPressure {
    #[default]
    Normal,
    /// Much of memory is held compressed, and nothing is being swapped out.
    Warning,
    /// The warning level while the kernel is swapping pages out.
    Paging,
    /// The kernel is about to kill processes for memory.
    Critical,
}

impl MemoryPressure {
    /// From the sysctl's value (1 normal, 2 warning, 4 critical; a value
    /// between two levels counts as the lower one, 0 as normal) and whether
    /// the kernel is paging.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    fn from_level(level: u32, paging: bool) -> Self {
        match level {
            4.. => Self::Critical,
            2..=3 if paging => Self::Paging,
            2..=3 => Self::Warning,
            _ => Self::Normal,
        }
    }

    /// Memory is being swapped out: nothing is available.
    pub(super) fn paging(self) -> bool {
        self >= Self::Paging
    }
}

/// The memory pressure now; `Normal` off macOS or when unreadable.
/// Unused by macOS test builds, where the ledger reads its stub.
#[cfg_attr(all(test, target_os = "macos"), allow(dead_code))]
pub(super) fn memory_pressure() -> MemoryPressure {
    pressure(None)
}

/// [`memory_pressure`], and at least paging when the swap-out counter rose
/// after `since` at the warning level or above ([`Swapouts::pressure`]).
#[cfg_attr(all(test, target_os = "macos"), allow(dead_code))]
pub(super) fn memory_pressure_since(since: Instant) -> MemoryPressure {
    pressure(Some(since))
}

#[cfg_attr(all(test, target_os = "macos"), allow(dead_code))]
fn pressure(since: Option<Instant>) -> MemoryPressure {
    #[cfg(target_os = "macos")]
    {
        memory_facts(since).map_or(MemoryPressure::Normal, |facts| facts.pressure)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = since;
        MemoryPressure::Normal
    }
}

/// The kernel counters the free reading is computed from, in bytes.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MemoryFacts {
    /// `hw.memsize`: the RAM that physically exists.
    pub ram: u64,
    /// `wire_count`: pages that cannot be paged out, including Metal buffers.
    pub wired: u64,
    /// `compressor_page_count`: what the compressor's own store holds.
    pub compressed: u64,
    /// `internal_page_count`: anonymous pageable pages, wired ones excluded.
    pub anonymous: u64,
    /// `external_page_count`: the file cache.
    pub file_backed: u64,
    /// Read with the counters.
    pub pressure: MemoryPressure,
}

/// How long after the swap-out counter last rose the kernel counts as
/// paging. Must match `memory.py::MAC_PAGING_SECONDS`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const PAGING_WINDOW: Duration = Duration::from_secs(10);

/// How often a background thread reads the swap-out counter
/// ([`follow_swapouts`]), so a rise is dated within one tick before a grant
/// however long the gateway was idle.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
const SWAPOUT_TICK: Duration = Duration::from_secs(2);

/// The swap-out counter at the last reading and when that was, and the time
/// of the earlier reading of the most recent pair whose counter rose: the
/// rise happened after it. `paged_after` is the same for the most recent
/// rise whose later reading had the pressure level at warning or above.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
struct Swapouts {
    last: Option<(u64, Instant)>,
    rose_after: Option<Instant>,
    paged_after: Option<Instant>,
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
impl Swapouts {
    /// Record a reading of the counter and the sysctl pressure level taken
    /// at `at`; a reading older than the last is ignored.
    fn record(&mut self, count: u64, level: u32, at: Instant) {
        if self.last.is_some_and(|(_, read_at)| at < read_at) {
            return;
        }
        if let Some((previous, read_at)) = self.last
            && count > previous
        {
            self.rose_after = Some(read_at);
            if level >= 2 {
                self.paged_after = Some(read_at);
            }
        }
        self.last = Some((count, at));
    }

    /// The pressure at sysctl `level` and `now`, paging if the counter rose
    /// within [`PAGING_WINDOW`] before `now`. A rise after `since` read at
    /// the warning level or above makes it at least `Paging`, whatever the
    /// level is now.
    fn pressure(&self, level: u32, now: Instant, since: Option<Instant>) -> MemoryPressure {
        let recent = self
            .rose_after
            .is_some_and(|after| now.saturating_duration_since(after) <= PAGING_WINDOW);
        let pressure = MemoryPressure::from_level(level, recent);
        if since.is_some_and(|since| self.paged_after.is_some_and(|after| after >= since)) {
            pressure.max(MemoryPressure::Paging)
        } else {
            pressure
        }
    }
}

/// Feed `swapouts` a reading every [`SWAPOUT_TICK`] until `wait` returns
/// true. `read` answers the counter, the pressure level and when they were
/// read, or `None`.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn follow_swapouts(
    swapouts: &Mutex<Swapouts>,
    mut read: impl FnMut() -> Option<(u64, u32, Instant)>,
    mut wait: impl FnMut(Duration) -> bool,
) {
    while !wait(SWAPOUT_TICK) {
        if let Some((count, level, at)) = read()
            && let Ok(mut swapouts) = swapouts.lock()
        {
            swapouts.record(count, level, at);
        }
    }
}

/// This process's [`Swapouts`].
#[cfg(target_os = "macos")]
static SWAPOUTS: Mutex<Swapouts> = Mutex::new(Swapouts {
    last: None,
    rose_after: None,
    paged_after: None,
});

/// Start [`follow_swapouts`] on [`SWAPOUTS`] for the life of the process,
/// once.
#[cfg(target_os = "macos")]
fn start_following_swapouts() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        let spawned = std::thread::Builder::new()
            .name("swap-outs".to_owned())
            .spawn(|| {
                follow_swapouts(
                    &SWAPOUTS,
                    || sys::swapouts().map(|count| (count, sys::pressure_level(), Instant::now())),
                    |tick| {
                        std::thread::sleep(tick);
                        false
                    },
                );
            });
        if let Err(err) = spawned {
            tracing::warn!(
                error = %err,
                "could not start reading the swap-out counter in the background; \
                 a grant after idle judges paging from the last reading before it"
            );
        }
    });
}

/// RAM a new allocation could get: RAM minus wired, compressed and anonymous
/// pages (Activity Monitor's "used"). File cache counts as available at
/// normal pressure. Must not use `free + inactive`: macOS moves pages
/// another process still holds onto the inactive queue, so that figure rises
/// without anything freed.
///
/// At warning the file cache is taken too: macOS then makes room by
/// compressing and swapping other memory, not only by dropping it. 0 while
/// the kernel is paging ([`MemoryPressure::paging`]): it keeps several GiB
/// of file cache while it swaps.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn available_bytes(facts: &MemoryFacts) -> u64 {
    if facts.pressure.paging() {
        return 0;
    }
    let mut taken = facts
        .wired
        .saturating_add(facts.compressed)
        .saturating_add(facts.anonymous);
    if facts.pressure == MemoryPressure::Warning {
        taken = taken.saturating_add(facts.file_backed);
    }
    facts.ram.saturating_sub(taken)
}

/// RAM the OS could deliver now, in MiB ([`available_bytes`]); `None` off
/// macOS. The worker computes the same figure under the `"mps"` label.
pub(super) fn ram_available_mb() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        memory_facts(None).map(|facts| available_bytes(&facts) / MIB)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
mod sys {
    //! The three syscalls, kept together so the cfg gate is one block.

    use std::ffi::CString;
    use std::ptr;
    use std::time::Instant;

    pub(super) fn sysctl_string(name: &str) -> Option<String> {
        let name = CString::new(name).ok()?;
        let mut len: libc::size_t = 0;
        // SAFETY: a null `oldp` with a valid `oldlenp` is the documented way
        // to ask sysctl for the value's length; nothing is written.
        let sized = unsafe {
            libc::sysctlbyname(name.as_ptr(), ptr::null_mut(), &mut len, ptr::null_mut(), 0)
        };
        if sized != 0 || len == 0 {
            return None;
        }
        let mut buffer = vec![0u8; len];
        // SAFETY: `buffer` is `len` bytes, the size sysctl just asked for;
        // `len` may only shrink on the second call.
        let read = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                &mut len,
                ptr::null_mut(),
                0,
            )
        };
        if read != 0 {
            return None;
        }
        buffer.truncate(len);
        // The value is a C string: drop the terminator and anything after it.
        let text = match buffer.iter().position(|byte| *byte == 0) {
            Some(end) => &buffer[..end],
            None => &buffer[..],
        };
        let text = String::from_utf8(text.to_vec()).ok()?;
        let text = text.trim().to_owned();
        (!text.is_empty()).then_some(text)
    }

    pub(super) fn sysctl_u64(name: &str) -> Option<u64> {
        let name = CString::new(name).ok()?;
        let mut value: u64 = 0;
        let mut len: libc::size_t = std::mem::size_of::<u64>();
        // SAFETY: `oldp` points at a `u64` and `oldlenp` says so; sysctl
        // writes at most that many bytes.
        let read = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                ptr::from_mut(&mut value).cast(),
                &mut len,
                ptr::null_mut(),
                0,
            )
        };
        (read == 0 && len == std::mem::size_of::<u64>()).then_some(value)
    }

    pub(super) fn sysctl_u32(name: &str) -> Option<u32> {
        let name = CString::new(name).ok()?;
        let mut value: u32 = 0;
        let mut len: libc::size_t = std::mem::size_of::<u32>();
        // SAFETY: `oldp` points at a `u32` and `oldlenp` says so; sysctl
        // writes at most that many bytes.
        let read = unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                ptr::from_mut(&mut value).cast(),
                &mut len,
                ptr::null_mut(),
                0,
            )
        };
        (read == 0 && len == std::mem::size_of::<u32>()).then_some(value)
    }

    // `mach_host_self` is deprecated in libc in favour of `mach2`; one call
    // does not earn a dependency.
    #[allow(deprecated)]
    fn vm_statistics() -> Option<libc::vm_statistics64> {
        // SAFETY: zeroed is a valid `vm_statistics64` (plain integers), and
        // the kernel overwrites it wholesale on success.
        let mut stats: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
        let mut count = libc::HOST_VM_INFO64_COUNT;
        // SAFETY: the out-buffer is a whole `vm_statistics64` and `count` is
        // its size in `integer_t` units, as `host_statistics64` documents.
        let result = unsafe {
            libc::host_statistics64(
                libc::mach_host_self(),
                libc::HOST_VM_INFO64,
                ptr::from_mut(&mut stats).cast(),
                &mut count,
            )
        };
        (result == 0).then_some(stats)
    }

    /// Pages swapped out since boot.
    pub(super) fn swapouts() -> Option<u64> {
        vm_statistics().map(|stats| stats.swapouts)
    }

    /// `kern.memorystatus_vm_pressure_level`, 0 when unreadable.
    pub(super) fn pressure_level() -> u32 {
        sysctl_u32("kern.memorystatus_vm_pressure_level").unwrap_or(0)
    }

    /// The counters now; the pressure as [`super::Swapouts::pressure`] with
    /// `since`.
    pub(super) fn memory_facts(since: Option<Instant>) -> Option<super::MemoryFacts> {
        super::start_following_swapouts();
        let stats = vm_statistics()?;
        // SAFETY: sysconf takes a name and returns a long; no pointers.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page = u64::try_from(page).ok().filter(|page| *page > 0)?;
        let pages = |count: u32| u64::from(count).saturating_mul(page);
        let level = pressure_level();
        let now = Instant::now();
        let pressure = super::SWAPOUTS.lock().map_or(
            super::MemoryPressure::from_level(level, false),
            |mut swapouts| {
                swapouts.record(stats.swapouts, level, now);
                swapouts.pressure(level, now, since)
            },
        );
        Some(super::MemoryFacts {
            ram: sysctl_u64("hw.memsize").filter(|bytes| *bytes > 0)?,
            wired: pages(stats.wire_count),
            compressed: pages(stats.compressor_page_count),
            anonymous: pages(stats.internal_page_count),
            file_backed: pages(stats.external_page_count),
            pressure,
        })
    }
}

#[cfg(target_os = "macos")]
use sys::{memory_facts, sysctl_string, sysctl_u64};

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(chip: &str, gib: u64) -> HostFacts {
        HostFacts {
            chip: chip.to_owned(),
            ram_bytes: gib * GIB,
        }
    }

    /// The GPU is one constant-keyed row whose name is the calibration
    /// keyspace and whose total is the 75 % seed — deterministic from the
    /// two kernel facts and nothing else.
    #[test]
    fn the_gpu_is_derived_from_the_two_kernel_facts() {
        let gpu = gpu(&facts("Apple M3 Max", 128));
        assert_eq!(gpu.uuid, "GPU-MPS");
        assert_eq!(gpu.name, "Apple M3 Max (128 GB)");
        assert_eq!(gpu.index, 0);
        assert_eq!(gpu.total_mb, 128 * 1024 / 4 * 3, "≈75% of RAM");
        assert_eq!(
            gpu.unified_ram_mb,
            Some(128 * 1024),
            "the unified flag, and the only sanity bound on an adopted total"
        );
        assert!(gpu.unified());
        assert_eq!(gpu.compute_cap, None, "no CUDA analogue exists");
        assert_eq!(gpu.bdf, None);
        assert_eq!(gpu.gfx_target_version, None);
        // A small Mac: the seed still lands on a whole number of MiB.
        assert_eq!(super::gpu(&facts("Apple M2", 8)).total_mb, 6 * 1024);
    }

    /// The name carries the capacity, rounded to the nearest GiB — the same
    /// convention the ROCm names use, for the same reason (two Macs of one
    /// chip and different RAM do not price alike).
    #[test]
    fn the_name_carries_the_chip_and_the_capacity() {
        assert_eq!(gpu_name("Apple M3 Max", 128 * GIB), "Apple M3 Max (128 GB)");
        assert_eq!(gpu_name("Apple M1", 16 * GIB), "Apple M1 (16 GB)");
        // Rounds to nearest, and never to zero.
        assert_eq!(gpu_name("Apple M4", 36 * GIB - 1), "Apple M4 (36 GB)");
        assert_eq!(gpu_name("Apple M4", GIB / 4), "Apple M4 (1 GB)");
    }

    /// The refresh hands the ledger the RAM the OS says it could deliver,
    /// bounded only by the RAM that exists — the per-GPU clamp to the
    /// admission total is the ledger's `external` arithmetic, which tracks
    /// the worker-reported total this query cannot see.
    #[test]
    fn free_is_available_ram_bounded_by_physical_ram() {
        let ram_mb = 128 * 1024;
        assert_eq!(free_mb(ram_mb, 40 * 1024), 40 * 1024);
        assert_eq!(free_mb(ram_mb, 0), 0, "a machine under real pressure");
        assert_eq!(
            free_mb(ram_mb, ram_mb + 4096),
            ram_mb,
            "no reading may exceed the RAM that physically exists"
        );
    }

    const RAM_MB: u64 = 128 * 1024;

    fn facts_mb(wired_mb: u64, compressed_mb: u64, anonymous_mb: u64) -> MemoryFacts {
        MemoryFacts {
            ram: RAM_MB * MIB,
            wired: wired_mb * MIB,
            compressed: compressed_mb * MIB,
            anonymous: anonymous_mb * MIB,
            file_backed: 0,
            pressure: MemoryPressure::Normal,
        }
    }

    fn available_mb(facts: &MemoryFacts) -> u64 {
        available_bytes(facts) / MIB
    }

    /// Wired pages, the compressor's store and everyone's anonymous pages are
    /// taken; the file cache and free memory are not.
    #[test]
    fn available_is_the_ram_nobody_is_holding() {
        assert_eq!(available_mb(&facts_mb(3_000, 2_325, 71_500)), 54_247);
        // A machine holding nothing has all of it; one holding everything has
        // none of it, and the arithmetic saturates rather than wrapping.
        assert_eq!(available_mb(&facts_mb(0, 0, 0)), RAM_MB);
        assert_eq!(available_mb(&facts_mb(RAM_MB, 4_096, 4_096)), 0);
    }

    /// Nothing is available at critical pressure, or at warning while the
    /// kernel is paging. Warning without paging takes the ~9 GiB of file
    /// cache macOS kept out of the formula's figure.
    #[test]
    fn nothing_is_available_while_the_kernel_pages_under_pressure() {
        use MemoryPressure::{Critical, Normal, Paging, Warning};
        let counters = MemoryFacts {
            file_backed: 9_000 * MIB,
            ..facts_mb(5_189, 55_599, 60_321)
        };
        for (level, paging, pressure, available) in [
            (0, true, Normal, 9_963),
            (1, false, Normal, 9_963),
            (1, true, Normal, 9_963),
            (2, false, Warning, 963),
            (2, true, Paging, 0),
            (3, false, Warning, 963),
            (3, true, Paging, 0),
            (4, false, Critical, 0),
            (4, true, Critical, 0),
            (8, false, Critical, 0),
        ] {
            assert_eq!(MemoryPressure::from_level(level, paging), pressure);
            assert_eq!(pressure.paging(), available == 0);
            let facts = MemoryFacts {
                pressure,
                ..counters
            };
            assert_eq!(
                available_mb(&facts),
                available,
                "level {level}, paging {paging}"
            );
        }
    }

    /// The counter read as `count(secs)` at 0 s and then every tick through
    /// `to` s, as the background thread reads it.
    fn followed(start: Instant, count: impl Fn(u64) -> u64, to: u64) -> Swapouts {
        let swapouts = Mutex::new(Swapouts {
            last: None,
            rose_after: None,
            paged_after: None,
        });
        swapouts.lock().unwrap().record(count(0), 2, start);
        let secs = std::cell::Cell::new(0);
        let at = |offset: u64| start + Duration::from_secs(offset);
        follow_swapouts(
            &swapouts,
            || Some((count(secs.get()), 2, at(secs.get()))),
            |tick| {
                secs.set(secs.get() + tick.as_secs());
                secs.get() > to
            },
        );
        swapouts.into_inner().unwrap()
    }

    /// A rise is dated by the earlier reading of the pair that saw it. Paging
    /// is a rise within the window before now, or one after a given instant,
    /// such as the grant of the window being settled, however long it ran.
    #[test]
    fn paging_is_a_rise_within_the_window_or_after_an_instant() {
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        // Two 90 s batches, each settled from a reading at its end, while
        // the counter rises through both.
        let mut swapouts = Swapouts {
            last: None,
            rose_after: None,
            paged_after: None,
        };
        swapouts.record(500, 2, at(0));
        assert!(
            !swapouts.pressure(2, at(0), Some(at(0))).paging(),
            "one reading cannot tell"
        );
        swapouts.record(900, 2, at(90));
        assert!(
            swapouts.pressure(2, at(90), Some(at(0))).paging(),
            "rose after batch 1 was granted"
        );
        assert!(
            !swapouts.pressure(2, at(90), None).paging(),
            "the rise is dated 90 s ago, outside the window"
        );
        swapouts.record(1300, 2, at(180));
        assert!(
            swapouts.pressure(2, at(180), Some(at(90))).paging(),
            "rose after batch 2 was granted"
        );
        swapouts.record(100, 2, at(181));
        swapouts.record(100, 2, at(200));
        assert!(
            !swapouts.pressure(2, at(200), Some(at(181))).paging(),
            "a counter that fell did not rise"
        );

        // A burst 20-24 s into a 45 s window, read every tick.
        let mut swapouts = followed(start, |secs| 500 + secs.clamp(20, 24) - 20, 44);
        swapouts.record(504, 2, at(45));
        assert!(
            swapouts.pressure(2, at(45), Some(at(0))).paging(),
            "rose after the window was granted"
        );
        assert!(
            !swapouts.pressure(2, at(45), None).paging(),
            "the rise is dated 23 s ago, outside the window"
        );
        swapouts.record(505, 2, at(55));
        assert!(
            swapouts.pressure(2, at(55), None).paging(),
            "a rise dated exactly 10 s ago is paging"
        );

        // After an instant, only a rise read at warning or above counts, and
        // it counts whatever the level is now.
        let mut swapouts = Swapouts {
            last: None,
            rose_after: None,
            paged_after: None,
        };
        swapouts.record(500, 2, at(0));
        swapouts.record(600, 1, at(60));
        assert_eq!(
            swapouts.pressure(2, at(90), Some(at(0))),
            MemoryPressure::Warning,
            "a rise read at normal"
        );
        swapouts.record(700, 2, at(120));
        assert_eq!(
            swapouts.pressure(1, at(150), Some(at(0))),
            MemoryPressure::Paging,
            "a rise read at warning, normal now"
        );
        assert_eq!(
            swapouts.pressure(4, at(150), Some(at(0))),
            MemoryPressure::Critical
        );
        swapouts.record(800, 2, at(100));
        assert_eq!(
            swapouts.pressure(2, at(125), None),
            MemoryPressure::Warning,
            "a reading older than the last is ignored"
        );
    }

    /// The background readings date a rise that happened while the gateway
    /// was idle, so the next grant, which asks only about the window, judges
    /// it by when it happened and not by when the last grant was.
    #[test]
    fn background_readings_date_a_rise_before_a_grant() {
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        // A rise in the last 5 s of a 120 s idle wait.
        let rose_at_115 = |secs: u64| if secs >= 115 { 600 } else { 500 };
        let mut swapouts = followed(start, rose_at_115, 119);
        swapouts.record(600, 2, at(120));
        assert!(
            swapouts.pressure(2, at(120), None).paging(),
            "dated 6 s before the grant by the background readings"
        );
        let mut unfollowed = followed(start, rose_at_115, 0);
        unfollowed.record(600, 2, at(120));
        assert!(
            !unfollowed.pressure(2, at(120), None).paging(),
            "without them it is dated at the reading 120 s before"
        );

        // A rise 50 s before a job.
        let mut swapouts = followed(start, |secs| if secs >= 5 { 600 } else { 500 }, 54);
        swapouts.record(600, 2, at(55));
        assert!(
            !swapouts.pressure(2, at(55), None).paging(),
            "dated 51 s before the grant, outside the window"
        );
        swapouts.record(600, 2, at(65));
        assert!(
            !swapouts.pressure(2, at(65), Some(at(55))).paging(),
            "the rise came before the job's first window was granted"
        );
    }

    /// A recorded trace of a process holding 61 440 MiB for 167.5 s and
    /// releasing nothing: free + speculative + inactive rose 11 888 MiB (free
    /// flat at 47 000, speculative at 1 252, the rise all on the inactive
    /// queue), while the counters this formula reads did not move.
    #[test]
    fn a_hog_that_frees_nothing_does_not_free_memory() {
        // (seconds, the recorded free + speculative + inactive, in MiB)
        let recorded = [
            (3.0, 73_909),
            (22.7, 75_072),
            (42.9, 76_334),
            (63.2, 76_832),
            (83.4, 79_072),
            (103.7, 81_464),
            (123.9, 82_038),
            (144.1, 83_560),
            (164.4, 85_709),
            (170.5, 85_797),
        ];
        let (free_mb, speculative_mb) = (47_000, 1_252);
        let mut readings = Vec::new();
        for (_, recorded_mb) in recorded {
            let inactive_mb = recorded_mb - free_mb - speculative_mb;
            // Ageing moves pages between the active and inactive queues; the
            // hog's 61 440 MiB are anonymous on both, so `anonymous` is flat.
            readings.push((
                free_mb + inactive_mb,
                available_mb(&facts_mb(3_000, 2_325, 71_500)),
            ));
        }
        assert_eq!(recorded[0].1 - free_mb - speculative_mb, 25_657, "inactive");
        let old: Vec<u64> = readings.iter().map(|reading| reading.0).collect();
        assert_eq!(
            old.last().unwrap() - old.first().unwrap(),
            11_888,
            "what the old reading handed back over 167.5 s"
        );
        let new: Vec<u64> = readings.iter().map(|reading| reading.1).collect();
        assert!(
            new.iter().all(|available| *available == new[0]),
            "nothing was released, so nothing became available: {new:?}"
        );
    }

    /// A recorded trace of one process allocating 4 → 24 GiB on MPS. Its
    /// buffers were wired while in use, so this formula follows the allocation
    /// within 1 %, while psutil's `available` froze at 111 196 MiB for the
    /// last five steps.
    #[test]
    fn the_reading_falls_with_a_process_of_our_own() {
        // (GiB allocated, wired_mb, compressor_mb, psutil's available_mb)
        let recorded = [
            (0, 2_997, 372, 115_482),
            (4, 7_276, 372, 111_195),
            (8, 11_377, 372, 111_196),
            (12, 15_477, 372, 111_196),
            (16, 19_577, 372, 111_196),
            (20, 23_677, 372, 111_196),
            (24, 27_777, 372, 111_196),
        ];
        // Not recorded per row, and it did not move: `inactive` is identical
        // on all seven rows and `free` falls one-for-one with `wired`.
        let anonymous_mb = 17_000;
        let baseline = available_mb(&facts_mb(recorded[0].1, recorded[0].2, anonymous_mb));
        for (allocated_gib, wired_mb, compressor_mb, psutil_mb) in recorded {
            let available = available_mb(&facts_mb(wired_mb, compressor_mb, anonymous_mb));
            let taken = baseline - available;
            let allocated_mb = allocated_gib * 1024;
            // Within 5 %: the first 4 GiB step also wires torch's own Metal
            // bookkeeping, and every later step is inside 0.9 %.
            assert!(
                taken >= allocated_mb && taken <= allocated_mb + allocated_mb / 20,
                "{allocated_gib} GiB allocated, reading fell {taken} MiB"
            );
            if allocated_gib >= 8 {
                assert_eq!(psutil_mb, 111_196, "the reading that stopped moving");
            }
        }
    }

    /// Off macOS every syscall path answers "unknown", which is what leaves
    /// such a host on the unpriced path instead of inventing a GPU.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn nothing_is_probed_off_macos() {
        assert_eq!(probe(), None);
        assert_eq!(ram_available_mb(), None);
        assert_eq!(query_memory(DEVICE_KEY, 128 * 1024), None);
        assert_eq!(memory_pressure(), MemoryPressure::Normal);
        assert_eq!(
            memory_pressure_since(Instant::now()),
            MemoryPressure::Normal
        );
    }
}
