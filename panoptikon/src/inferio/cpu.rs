//! CPU-only host device facts, read from the OS's memory statistics.
//!
//! The degenerate instance of the unified-device model: one synthetic device
//! whose memory is the host's RAM, with no accelerator pool to intersect, so
//! `free = min(total, ram_available)`. There is no identity to read and
//! nothing to pin — a constant device key, a name derived from capacity, and
//! two numbers: physical RAM (`MemTotal`, `ullTotalPhys`, `hw.memsize`) and
//! what the OS could deliver right now (`MemAvailable`, `ullAvailPhys`,
//! macOS's free+inactive pages by way of `mps.rs`), the same reading the
//! worker's `psutil` tier reports under the same `"ram"` label. Neither is
//! namespaced, so on Linux both are bounded by the cgroup memory limit where
//! one applies: in a container the machine is the limit. Everything
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

/// The shipped knee bucket-variance band on a CPU device, against
/// [`super::ledger::KNEE_MAX_BUCKET_DISPERSION`] elsewhere. That band was
/// derived from quiet GPU series at 0.003 and 0.052; a quiet CPU host running
/// wd-vit measures 0.13–0.20 in the buckets the ramp lives in — highest quiet
/// bucket 0.196, over three identical 2 000-item runs and a control
/// (`final-n1`) — so 0.20 leaves it no headroom at all and one honest bucket
/// refuses every fit for the job. 0.35 is that quiet ceiling with ~1.8x over
/// it, and the reconstruction under-states what the ring sees. The headroom is
/// not free: the control run of `final-n1`, a 6 GB allocate/touch/free every
/// 5 s beside the worker, drove a bucket to 0.2227 — a genuine refusal at
/// 0.20, which 0.35 admits. Under that same hog the band at 0.35 fitted knee
/// 15 / max_units 32 / 1.9 GB RSS, identical to the quiet runs: the motion
/// 0.20 refused over had not displaced the knee it was refusing to read. A
/// shipped default, not a config line: a user override wins and absence
/// tracks this constant.
pub(super) const DEFAULT_KNEE_MAX_BUCKET_DISPERSION: f64 = 0.35;

/// Where this host's RAM statistics are read from, so the refresh reads the
/// same file the probe did and the parse runs from a fixture everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MemRoots {
    /// `MemTotal` is the capacity and the name, `MemAvailable` the live free
    /// reading. Ignored off Linux, where a syscall answers both.
    pub meminfo: PathBuf,
    /// The cgroup filesystem root. `/proc/meminfo` is not namespaced, so in a
    /// container it reports the machine the container runs on; this is where
    /// the limit that machine's kernel actually enforces is read from. Under
    /// the default cgroup namespace — Docker's, and every compose file's —
    /// the root *is* the container's own cgroup, so no path is resolved
    /// through `/proc/self/cgroup`: a limit set on an outer cgroup the
    /// namespace hides is not read. Ignored off Linux.
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

/// The memory limit in force on this cgroup, in MiB: v2's `memory.max`, else
/// v1's `memory.limit_in_bytes`. `None` when no file exists, the value is
/// unreadable, or it is the unlimited spelling — v2 writes `max`, and v1 a
/// sentinel so large that the `min` against physical RAM drops it anyway.
#[cfg(target_os = "linux")]
fn cgroup_limit_mb(roots: &MemRoots) -> Option<u64> {
    bytes_file_mb(&roots.cgroup.join("memory.max"))
        .or_else(|| bytes_file_mb(&roots.cgroup.join("memory/memory.limit_in_bytes")))
}

/// What this cgroup has already spent of that limit, in MiB, less the page
/// cache the kernel reclaims before it ever OOM-kills — `memory.current`
/// minus the working set. Counting the cache as spent would drive the free
/// reading to zero on any job that touches many files, and stall admission on
/// a container that is nowhere near its limit.
///
/// Reclaimable is `active_file + inactive_file`: cgroup-v2's memory.stat
/// documents those two as the file-backed pages on the reclaim algorithm's own
/// LRU lists (mlocked pages are on `unevictable` instead and stay counted),
/// and they are the same two counters `MemAvailable` — the other half of the
/// `min` — treats as available on the host. `inactive_file` alone is not the
/// cache: a live container measured `inactive_file 0 / active_file 543 MB`.
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
/// Linux is the only platform that reads `roots`; the rest answer from a
/// syscall, hence the `unused_variables` allow.
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

/// RAM the OS says it could deliver right now, in MiB — the same answer the
/// worker's `psutil.virtual_memory().available` gives, under the same
/// `"ram"` label. Over-stating availability would under-state external
/// pressure, so the tighter figure wins where a platform offers both.
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

    /// D5/B19: `/proc/meminfo` is not namespaced, so a container under
    /// `mem_limit: 16g` read the whole machine and priced itself 5.89x over
    /// what the kernel would let it have. The limit the kernel enforces
    /// bounds both the device total and its free reading, on v2 and on v1,
    /// and an unlimited or absent cgroup leaves the host's own figures alone.
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
