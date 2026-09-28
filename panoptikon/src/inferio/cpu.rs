//! The CPU device: one synthetic device whose memory is the host's RAM.
//!
//! Total is physical RAM (`MemTotal`, `ullTotalPhys`, `hw.memsize`) and free
//! is what the OS could deliver now (`MemAvailable`, `ullAvailPhys`, macOS
//! free+inactive pages), matching the worker's `"ram"` reading. On Linux
//! both are bounded by the cgroup memory limit, since `/proc/meminfo` is not
//! namespaced. See docs/unified-memory-admission.md "Backend C: CPU".

use std::path::PathBuf;

use super::gpu::{GpuInfo, GpuMemory};
use super::rocm::capacity_gb_up_4;

/// The CPU device's key, as used in `[inference_local.vram.gpu."CPU"]`. It
/// must not start with `GPU-`, which marks a CUDA UUID.
pub(super) const DEVICE_KEY: &str = "CPU";

/// Default hard ceiling on the CPU device, as a fraction of RAM. Other
/// devices default to no cap because their OOM is catchable; running out of
/// RAM is an OS process kill. A config value overrides it.
pub(super) const DEFAULT_CAP_FRACTION: f64 = 0.75;

/// Default knee bucket-dispersion band on the CPU device, wider than
/// [`super::ledger::KNEE_MAX_BUCKET_DISPERSION`] because a quiet CPU host's
/// buckets already reach about 0.2. A config value overrides it.
pub(super) const DEFAULT_KNEE_MAX_BUCKET_DISPERSION: f64 = 0.35;

/// Where this host's RAM statistics are read from (injectable for tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MemRoots {
    /// `MemTotal` and `MemAvailable`. Linux only.
    pub meminfo: PathBuf,
    /// The cgroup filesystem root, read as the container's own cgroup (true
    /// under the default cgroup namespace); a limit on an outer cgroup is not
    /// seen. Linux only.
    pub cgroup: PathBuf,
}

impl Default for MemRoots {
    fn default() -> Self {
        Self {
            meminfo: PathBuf::from("/proc/meminfo"),
            cgroup: PathBuf::from("/sys/fs/cgroup"),
        }
    }
}

/// This cgroup's memory limit in MiB: v2's `memory.max`, else v1's
/// `memory.limit_in_bytes`. `None` when absent or `max`; v1's unlimited
/// sentinel is larger than RAM and drops out of the `min`.
#[cfg(target_os = "linux")]
fn cgroup_limit_mb(roots: &MemRoots) -> Option<u64> {
    bytes_file_mb(&roots.cgroup.join("memory.max"))
        .or_else(|| bytes_file_mb(&roots.cgroup.join("memory/memory.limit_in_bytes")))
}

/// This cgroup's usage in MiB minus reclaimable page cache
/// (`active_file + inactive_file`, as `MemAvailable` counts it), so a job
/// that reads many files does not look out of memory. `inactive_file` alone
/// is not enough: it can be 0 while `active_file` holds hundreds of MB.
#[cfg(target_os = "linux")]
fn cgroup_used_mb(roots: &MemRoots) -> Option<u64> {
    let v2 = bytes_file_mb(&roots.cgroup.join("memory.current"));
    if let Some(used) = v2 {
        let stat = roots.cgroup.join("memory.stat");
        return Some(used.saturating_sub(file_lru_mb(&stat, FILE_LRU_V2)));
    }
    let used = bytes_file_mb(&roots.cgroup.join("memory/memory.usage_in_bytes"))?;
    let stat = roots.cgroup.join("memory/memory.stat");
    Some(used.saturating_sub(file_lru_mb(&stat, FILE_LRU_V1)))
}

/// The two file-LRU rows of a `memory.stat`, under v2's names and v1's.
#[cfg(target_os = "linux")]
const FILE_LRU_V2: [&str; 2] = ["active_file", "inactive_file"];
#[cfg(target_os = "linux")]
const FILE_LRU_V1: [&str; 2] = ["total_active_file", "total_inactive_file"];

/// A cgroup file holding one byte count, in MiB. `None` for `max` and for
/// anything that is not a number.
#[cfg(target_os = "linux")]
fn bytes_file_mb(path: &std::path::Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    text.trim().parse::<u64>().ok().map(|bytes| bytes / MIB)
}

/// The named `key value` rows of a `memory.stat`, summed, in MiB. An absent
/// file or row contributes zero: less cache subtracted is the safe direction.
#[cfg(target_os = "linux")]
fn file_lru_mb(path: &std::path::Path, keys: [&str; 2]) -> u64 {
    let Ok(text) = std::fs::read_to_string(path) else {
        return 0;
    };
    text.lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            keys.contains(&name)
                .then(|| value.trim().parse::<u64>().ok())?
        })
        .sum::<u64>()
        / MIB
}

#[cfg(target_os = "linux")]
const MIB: u64 = 1024 * 1024;

/// This host's physical RAM in MiB, or `None` when it could not be read.
pub(super) fn probe(roots: &MemRoots) -> Option<u64> {
    ram_total_mb(roots).filter(|mb| *mb > 0)
}

/// The CPU device for `ram_mb` MiB of RAM. `total_mb` is all of it;
/// [`DEFAULT_CAP_FRACTION`] is applied as a budget, not a smaller total.
pub(super) fn gpu(ram_mb: u64) -> GpuInfo {
    GpuInfo {
        index: 0,
        uuid: DEVICE_KEY.to_owned(),
        name: gpu_name(ram_mb),
        total_mb: ram_mb,
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        // Unified, so a replica death counts as a negative sample.
        unified_ram_mb: Some(ram_mb),
        vram_carveout_mb: None,
    }
}

/// The device name: `CPU (64 GB)`, RAM rounded up to 4 GiB.
pub(super) fn gpu_name(ram_mb: u64) -> String {
    format!("CPU ({} GB)", capacity_gb_up_4(ram_mb))
}

/// The device's live free reading (`ram_available` bounded by physical RAM),
/// or `None` when RAM statistics could not be read.
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

/// Physical RAM in MiB, or `None`. Only Linux reads `roots`.
#[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
fn ram_total_mb(roots: &MemRoots) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let total = super::rocm::meminfo_mb(&roots.meminfo, "MemTotal")?;
        Some(cgroup_limit_mb(roots).map_or(total, |limit| limit.min(total)))
    }
    #[cfg(target_os = "windows")]
    {
        sys::total_mb()
    }
    #[cfg(target_os = "macos")]
    {
        return super::mps::physical_ram_mb();
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

/// RAM the OS could deliver now, in MiB, as `psutil.virtual_memory()
/// .available` reports it; on Linux also bounded by the cgroup limit.
#[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
fn ram_available_mb(roots: &MemRoots) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let available = super::rocm::meminfo_mb(&roots.meminfo, "MemAvailable")?;
        let Some(limit) = cgroup_limit_mb(roots) else {
            return Some(available);
        };
        let used = cgroup_used_mb(roots).unwrap_or(0);
        Some(available.min(limit.saturating_sub(used)))
    }
    #[cfg(target_os = "windows")]
    {
        sys::available_mb()
    }
    #[cfg(target_os = "macos")]
    {
        return super::mps::ram_available_mb();
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "windows")]
mod sys {
    //! `GlobalMemoryStatusEx`.

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

    /// Fixture roots for one cgroup layout: `files` is written under a
    /// temporary cgroup tree beside a 64 GiB `/proc/meminfo`.
    #[cfg(target_os = "linux")]
    fn roots_with(dir: &std::path::Path, case: &str, files: &[(&str, &str)]) -> MemRoots {
        let dir = &dir.join(case);
        std::fs::create_dir_all(dir).expect("mkdir");
        let meminfo = dir.join("meminfo");
        std::fs::write(
            &meminfo,
            format!(
                "MemTotal:       {} kB\nMemAvailable:   {} kB\n",
                RAM_MB * 1024,
                40 * 1024 * 1024
            ),
        )
        .expect("write meminfo");
        let cgroup = dir.join("cgroup");
        for (name, body) in files {
            let path = cgroup.join(name);
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
            std::fs::write(&path, body).expect("write");
        }
        std::fs::create_dir_all(&cgroup).expect("mkdir");
        MemRoots { meminfo, cgroup }
    }

    /// `/proc/meminfo` is not namespaced, so a container would otherwise read
    /// the whole machine. The cgroup limit bounds both the device total and
    /// its free reading, on v2 and on v1, and an unlimited or absent cgroup
    /// leaves the host's own figures alone.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_cgroup_limit_bounds_the_device() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gib16 = 16 * 1024;

        // cgroup v2: a 16 GiB limit, 9 GiB of it spent, 3 of those in the page
        // cache the kernel reclaims before it kills anything.
        let v2 = roots_with(
            dir.path(),
            "v2",
            &[
                ("memory.max", "17179869184\n"),
                ("memory.current", "9663676416\n"),
                (
                    "memory.stat",
                    "anon 1234\nactive_file 2147483648\ninactive_file 1073741824\nslab 99\n",
                ),
            ],
        );
        assert_eq!(cgroup_limit_mb(&v2), Some(gib16));
        assert_eq!(cgroup_used_mb(&v2), Some(6 * 1024), "the working set");

        // cgroup v1: the same facts under the controller's own names.
        let v1 = roots_with(
            dir.path(),
            "v1",
            &[
                ("memory/memory.limit_in_bytes", "17179869184\n"),
                ("memory/memory.usage_in_bytes", "9663676416\n"),
                (
                    "memory/memory.stat",
                    "total_active_file 2147483648\ntotal_inactive_file 1073741824\n",
                ),
            ],
        );
        assert_eq!(cgroup_limit_mb(&v1), Some(gib16));
        assert_eq!(cgroup_used_mb(&v1), Some(6 * 1024));

        // The reclaimable half is **both** file LRUs. A live container under
        // `--memory 16g` measured `inactive_file 0 / active_file 543 MB`:
        // subtracting only the inactive list would leave every one of those
        // pages priced as spent, on the one reading admission turns on.
        let active_only = roots_with(
            dir.path(),
            "active-only",
            &[
                ("memory.max", "17179869184\n"),
                ("memory.current", "9663676416\n"),
                ("memory.stat", "inactive_file 0\nactive_file 3221225472\n"),
            ],
        );
        assert_eq!(cgroup_used_mb(&active_only), Some(6 * 1024));

        // Unlimited (v2 writes `max`), and no cgroup files at all.
        let unlimited = roots_with(dir.path(), "unlimited", &[("memory.max", "max\n")]);
        assert_eq!(cgroup_limit_mb(&unlimited), None);
        let absent = roots_with(dir.path(), "absent", &[]);
        assert_eq!(cgroup_limit_mb(&absent), None);

        assert_eq!(ram_total_mb(&v2), Some(gib16), "the total is the limit");
        assert_eq!(
            ram_available_mb(&v2),
            Some(gib16 - 6 * 1024),
            "and free is what the limit leaves, not the host's 40 GiB"
        );
        assert_eq!(ram_total_mb(&v1), Some(gib16));
        assert_eq!(ram_available_mb(&v1), Some(gib16 - 6 * 1024));
        for roots in [&unlimited, &absent] {
            assert_eq!(ram_total_mb(roots), Some(RAM_MB));
            assert_eq!(ram_available_mb(roots), Some(40 * 1024));
        }
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
            ..MemRoots::default()
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
