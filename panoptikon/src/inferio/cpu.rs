//! CPU-only host device facts, read from the OS's memory statistics.
//!
//! The degenerate instance of the unified-device model: one synthetic device
//! whose memory is the host's RAM, with no accelerator pool to intersect, so
//! `free = min(total, ram_available)`. There is no identity to read and
//! nothing to pin — a constant device key, a name derived from capacity, and
//! two numbers: physical RAM (`MemTotal`, `ullTotalPhys`, `hw.memsize`) and
//! what the OS could deliver right now (`MemAvailable`, `ullAvailPhys`,
//! macOS's free+inactive pages by way of `mps.rs`), the same reading the
//! worker's `psutil` tier reports under the same `"ram"` label. Everything
//! but the platform readers is a pure function of an injected RAM figure.
//! See docs/unified-memory-admission.md "Backend C: CPU".

use std::path::PathBuf;

use super::gpu::{GpuInfo, GpuMemory};
use super::rocm::capacity_gb_up_4;

/// The one device key a CPU-only host ever has: a constant, and the string a
/// user types into `[inference_local.vram.gpu."CPU"]`. It omits the `GPU-`
/// prefix, which is what tells the pin and registration resolvers a string
/// is a CUDA UUID.
pub(super) const DEVICE_KEY: &str = "CPU";

/// The shipped hard ceiling on a CPU device, as a fraction of RAM. Every
/// other device ships with the cap off, because over-admission there ends in
/// a catchable allocation failure; running out of RAM is an OS process kill.
/// A shipped default, not a config line: a user override wins and absence
/// tracks this constant (unified-memory doc, DP-8).
pub(super) const DEFAULT_CAP_FRACTION: f64 = 0.75;

/// Where this host's RAM statistics are read from, so the refresh reads the
/// same file the probe did and the parse runs from a fixture everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MemRoots {
    /// `MemTotal` is the capacity and the name, `MemAvailable` the live free
    /// reading. Ignored off Linux, where a syscall answers both.
    pub meminfo: PathBuf,
}

impl Default for MemRoots {
    fn default() -> Self {
        Self {
            meminfo: PathBuf::from("/proc/meminfo"),
        }
    }
}

/// This host's physical RAM in MiB, or `None` when it could not be read.
pub(super) fn probe(roots: &MemRoots) -> Option<u64> {
    ram_total_mb(roots).filter(|mb| *mb > 0)
}

/// The single synthetic device that RAM figure describes. `total_mb` is the
/// whole of it and, unlike MPS's, is **not** a seed: the kernel already told
/// us, so nothing is adopted from a worker later. [`DEFAULT_CAP_FRACTION`]
/// is a budget, not a smaller total, so `/health` reports what the machine
/// has.
pub(super) fn gpu(ram_mb: u64) -> GpuInfo {
    GpuInfo {
        index: 0,
        uuid: DEVICE_KEY.to_owned(),
        name: gpu_name(ram_mb),
        total_mb: ram_mb,
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        // The unified flag, and all it buys here: DP-2's
        // death-as-negative-sample, the only memory signal on this device.
        unified_ram_mb: Some(ram_mb),
        // No carve-out split exists: the device is the machine's RAM.
        vram_carveout_mb: None,
    }
}

/// The display *and* calibration-profile name: `CPU (64 GB)`. Built from a
/// kernel fact alone, so it cannot move with the environment and orphan the
/// profiles keyed by it; the ISA level is absent because the key already
/// carries `platform` and the worker's torch build.
pub(super) fn gpu_name(ram_mb: u64) -> String {
    format!("CPU ({} GB)", capacity_gb_up_4(ram_mb))
}

/// The device's live free reading, or `None` when RAM statistics could not
/// be read. `free` is `ram_available` bounded by physical RAM; the clamp to
/// the *admission* total is the ledger's own arithmetic, as on MPS. The
/// refresh reads `free_mb` and nothing else.
pub(super) fn query_memory(key: &str, ram_mb: u64, roots: &MemRoots) -> Option<Vec<GpuMemory>> {
    let available = ram_available_mb(roots)?;
    Some(vec![GpuMemory {
        uuid: key.to_owned(),
        total_mb: ram_mb,
        free_mb: free_mb(ram_mb, available),
    }])
}

/// What the OS could deliver, bounded by the RAM that exists.
fn free_mb(ram_mb: u64, ram_available_mb: u64) -> u64 {
    ram_available_mb.min(ram_mb)
}

/// Physical RAM in MiB. `None` with no reader, or on a reader that failed.
fn ram_total_mb(roots: &MemRoots) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        return super::rocm::meminfo_mb(&roots.meminfo, "MemTotal");
    }
    #[cfg(target_os = "windows")]
    {
        let _ = roots;
        sys::total_mb()
    }
    #[cfg(target_os = "macos")]
    {
        let _ = roots;
        return super::mps::physical_ram_mb();
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        let _ = roots;
        None
    }
}

/// RAM the OS says it could deliver right now, in MiB — the same answer the
/// worker's `psutil.virtual_memory().available` gives, under the same
/// `"ram"` label. Over-stating availability would under-state external
/// pressure, so the tighter figure wins where a platform offers both.
fn ram_available_mb(roots: &MemRoots) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        return super::rocm::meminfo_mb(&roots.meminfo, "MemAvailable");
    }
    #[cfg(target_os = "windows")]
    {
        let _ = roots;
        sys::available_mb()
    }
    #[cfg(target_os = "macos")]
    {
        let _ = roots;
        return super::mps::ram_available_mb();
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        let _ = roots;
        None
    }
}

#[cfg(target_os = "windows")]
mod sys {
    //! The one syscall, and the only code here that is not a pure function
    //! of a file or an injected number. `windows-sys` was already a direct
    //! Windows dependency; this adds a feature, not a crate.

    use std::ptr;

    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    const MIB: u64 = 1024 * 1024;

    fn status() -> Option<MEMORYSTATUSEX> {
        // SAFETY: zeroed is a valid `MEMORYSTATUSEX` (plain integers);
        // `dwLength` is the only field the API reads rather than writes.
        let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        status.dwLength = u32::try_from(std::mem::size_of::<MEMORYSTATUSEX>()).ok()?;
        // SAFETY: the out-buffer is a whole `MEMORYSTATUSEX` and its
        // `dwLength` says so, as `GlobalMemoryStatusEx` documents.
        let ok = unsafe { GlobalMemoryStatusEx(ptr::from_mut(&mut status)) };
        (ok != 0).then_some(status)
    }

    pub(super) fn total_mb() -> Option<u64> {
        status()
            .map(|status| status.ullTotalPhys / MIB)
            .filter(|mb| *mb > 0)
    }

    pub(super) fn available_mb() -> Option<u64> {
        status().map(|status| status.ullAvailPhys / MIB)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 64 GiB machine as its kernel counts it: a percent or so short of the
    /// sticker capacity, because firmware took its reservations before the
    /// kernel counted.
    const RAM_MB: u64 = 64 * 1024 - 700;

    /// The GPU is one constant-keyed row whose name is the calibration
    /// keyspace and whose total is the machine's RAM — deterministic from that
    /// one fact and nothing else.
    #[test]
    fn the_gpu_is_derived_from_the_hosts_ram() {
        let gpu = gpu(RAM_MB);
        assert_eq!(gpu.uuid, "CPU");
        assert_eq!(gpu.name, "CPU (64 GB)");
        assert_eq!(gpu.index, 0);
        assert_eq!(
            gpu.total_mb, RAM_MB,
            "the total is RAM itself: nothing here is a seed"
        );
        assert_eq!(
            gpu.unified_ram_mb,
            Some(RAM_MB),
            "the unified flag, which is what DP-2's death negative reads"
        );
        assert!(gpu.unified());
        assert_eq!(gpu.compute_cap, None, "no CUDA analogue exists");
        assert_eq!(gpu.bdf, None);
        assert_eq!(gpu.gfx_target_version, None);
        assert_eq!(gpu.vram_carveout_mb, None, "the GPU is the machine");
    }

    /// The name carries the capacity on a 4 GiB grid, so the kernel's own
    /// reservations — which move with a kernel update or a boot parameter —
    /// cannot split one machine's profiles in two.
    #[test]
    fn the_name_rounds_capacity_up_to_a_four_gib_grid() {
        assert_eq!(gpu_name(RAM_MB), "CPU (64 GB)");
        assert_eq!(gpu_name(64 * 1024), "CPU (64 GB)");
        assert_eq!(gpu_name(16 * 1024 - 400), "CPU (16 GB)");
        assert_eq!(gpu_name(8 * 1024 - 300), "CPU (8 GB)");
        // Never zero, and never a figure below the grid.
        assert_eq!(gpu_name(1), "CPU (4 GB)");
        // A 65 GiB machine is not a 64 GiB one: the grid separates sizes, it
        // does not collapse them.
        assert_eq!(gpu_name(65 * 1024), "CPU (68 GB)");
    }

    /// The refresh hands the ledger what the OS says it could deliver,
    /// bounded only by the RAM that exists.
    #[test]
    fn free_is_available_ram_bounded_by_physical_ram() {
        assert_eq!(free_mb(RAM_MB, 20 * 1024), 20 * 1024);
        assert_eq!(free_mb(RAM_MB, 0), 0, "a machine under real pressure");
        assert_eq!(
            free_mb(RAM_MB, RAM_MB + 4096),
            RAM_MB,
            "no reading may exceed the RAM that physically exists"
        );
    }

    /// The Linux reader is the `/proc/meminfo` parser `rocm.rs` already owns,
    /// asked for the two rows this backend needs. Driven from a fixture so it
    /// is exercised on every platform, not only the one it runs on.
    #[test]
    fn the_two_meminfo_rows_are_read_in_mib() {
        use super::super::rocm::meminfo_mb;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meminfo");
        std::fs::write(
            &path,
            "MemTotal:       65806848 kB\nMemFree:         1234567 kB\n\
             MemAvailable:   41943040 kB\nBuffers:           98765 kB\n",
        )
        .expect("write");
        assert_eq!(meminfo_mb(&path, "MemTotal"), Some(65_806_848 / 1024));
        assert_eq!(meminfo_mb(&path, "MemAvailable"), Some(40 * 1024));
        assert_eq!(meminfo_mb(&path, "MemUnknown"), None);
    }

    /// A machine whose RAM cannot be read is unknown, not a zero-sized GPU:
    /// a GPU with no memory would admit nothing while looking priced.
    #[test]
    fn an_unreadable_host_is_unknown() {
        let roots = MemRoots {
            meminfo: PathBuf::from("this/path/does/not/exist"),
        };
        // Only Linux consults the path; every other platform answers from a
        // syscall that does not care about it, so this is asserted where it
        // is the actual reader.
        #[cfg(target_os = "linux")]
        assert_eq!(probe(&roots), None);
        #[cfg(target_os = "linux")]
        assert_eq!(query_memory(DEVICE_KEY, RAM_MB, &roots), None);
        #[cfg(not(target_os = "linux"))]
        let _ = roots;
    }

    /// On the platforms with a reader, the real host answers a plausible
    /// figure — the one thing a fixture cannot check.
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    #[test]
    fn this_host_reports_its_own_memory() {
        let roots = MemRoots::default();
        let total = probe(&roots).expect("this platform has a RAM reader");
        assert!(total >= 512, "a machine with under 512 MiB of RAM: {total}");
        let sample = query_memory(DEVICE_KEY, total, &roots).expect("a live free reading");
        assert_eq!(sample.len(), 1);
        assert_eq!(sample[0].uuid, DEVICE_KEY);
        assert_eq!(sample[0].total_mb, total);
        assert!(sample[0].free_mb <= total);
    }
}
