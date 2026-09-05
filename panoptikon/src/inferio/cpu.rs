//! CPU-only host device facts, read from the OS's memory statistics.
//!
//! The CPU half of `gpu`, and the third and last instance of the **unified
//! GPU** model (docs/unified-memory-admission.md, backend C): one synthetic
//! GPU whose memory is the host's RAM, shared with the OS and every other
//! process. It is the *degenerate* instance — the one where `pool_free` does
//! not exist at all, so the design's
//! `free = max(0, min(total, pool_free, ram_available))` collapses to
//! `ram_available` alone, and `total` is simply the RAM the machine has.
//!
//! Which is why there is no identity to read and nothing to pin: a constant
//! device key, a name derived from capacity, and two numbers.
//!
//! - **Total**: physical RAM (`MemTotal` on Linux, `GlobalMemoryStatusEx`'s
//!   `ullTotalPhys` on Windows, `hw.memsize` on macOS).
//! - **Free**: what the OS says it could deliver right now (`MemAvailable`,
//!   `ullAvailPhys`, and macOS's free+inactive pages by way of `mps.rs`) —
//!   the same reading the worker's `psutil` tier reports under the same
//!   `"ram"` label, which is what keeps one memory vocabulary per host.
//!
//! Everything except the platform readers is a pure function of an injected
//! RAM figure, so the GPU construction, the naming and the refresh
//! arithmetic are tested on every platform; only [`probe`] and the two
//! readers know which OS they are on, and on an OS with no reader they answer
//! `None` — which leaves such a host unpriced, exactly as it was before this
//! module existed.

use std::path::PathBuf;

use super::gpu::{GpuInfo, GpuMemory};
use super::rocm::capacity_gb_up_4;

/// The one device key a CPU-only host ever has.
///
/// A constant, not a hardware identity, for the same reasons `GPU-MPS` is
/// (`mps::DEVICE_KEY`): there is one of it per host, per-GPU budget
/// overrides live in that host's own config file, and this is the string a
/// user can actually type into `[inference_local.vram.gpu."CPU"]`. It
/// deliberately does **not** take the `GPU-` prefix every other device key
/// carries — this is not a GPU, and the prefix is also what tells the
/// registration and pin resolvers a string is a CUDA UUID.
pub(super) const DEVICE_KEY: &str = "CPU";

/// The shipped hard ceiling on a CPU device, as a fraction of RAM (DP-8).
///
/// Every other GPU ships with the cap **off** (`None`), because a dGPU's
/// over-admission ends in a catchable `cudaMalloc` failure and the margin plus
/// the OOM backstop are enough. Here they are not: running the machine out of
/// RAM is answered by the OS killing a process — a SIGKILL no handler sees,
/// which DP-2 can only record *after* the replica is gone — and the process it
/// picks may not even be ours. So the CPU device keeps a quarter of the machine
/// out of the budget by default.
///
/// A shipped default, not a live config line: a user override in
/// `[inference_local.vram.gpu."CPU"]` (or the section-wide `cap_fraction`)
/// wins, and absence tracks this constant, per the config-authoring rules.
pub(super) const DEFAULT_CAP_FRACTION: f64 = 0.75;

/// Where this host's RAM statistics are read from.
///
/// One path today, and it is only meaningful on Linux; it exists so the
/// refresh reads the same file the probe did (the same reason
/// `rocm::SysfsRoots` is carried through `MemoryQuery::RocmSysfs`) and so the
/// parse is exercised from a fixture on every platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MemRoots {
    /// `/proc/meminfo`: `MemTotal` is the GPU's capacity and its name,
    /// `MemAvailable` is its live free reading. Ignored off Linux, where the
    /// same two facts come from a syscall.
    pub meminfo: PathBuf,
}

impl Default for MemRoots {
    fn default() -> Self {
        Self {
            meminfo: PathBuf::from("/proc/meminfo"),
        }
    }
}

/// This host's physical RAM in MiB, or `None` when it could not be read —
/// which leaves the inventory unknown rather than half-known, exactly as a
/// failed sysctl does on MPS.
pub(super) fn probe(roots: &MemRoots) -> Option<u64> {
    ram_total_mb(roots).filter(|mb| *mb > 0)
}

/// The single synthetic device that RAM figure describes.
///
/// `total_mb` is the whole of it, and unlike MPS's it is **not** a seed: a
/// CPU host's admission total is a fact the kernel already told us, so
/// nothing about it is adopted from a worker later (DP-4 is scoped out of
/// this backend — see `ledger::VramLedger::adopt_unified_total_locked`). The
/// ceiling that keeps admission off the last quarter of the machine is
/// [`DEFAULT_CAP_FRACTION`], applied as a budget rather than by shrinking the
/// total, so `/health` still reports what the machine actually has.
///
/// No capability (the shipped floors are CUDA-specific), no PCI address, no
/// gfx target and no pin: `GpuInventory` treats a CPU inventory as no-pin
/// everywhere, because there is no device to select and any value written
/// into a visibility variable could only hide one.
pub(super) fn gpu(ram_mb: u64) -> GpuInfo {
    GpuInfo {
        index: 0,
        uuid: DEVICE_KEY.to_owned(),
        name: gpu_name(ram_mb),
        total_mb: ram_mb,
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        // The unified flag, and the whole of what it buys here: DP-2's
        // death-as-negative-sample, which is *the* memory signal on a device
        // whose over-admission is answered by the OS killing the process.
        unified_ram_mb: Some(ram_mb),
        // No carve-out split exists: the device is the machine's RAM.
        vram_carveout_mb: None,
    }
}

/// The display *and* calibration-profile name: `CPU (64 GB)`.
///
/// Same convention as the ROCm and MPS derived names: built from a kernel
/// fact alone, so it is byte-identical on every host with that much memory
/// and can never appear or disappear with the environment — which would
/// orphan every local profile, ratchet anchor and knee keyed by it. The ISA
/// level (AVX-512, the core count, the memory bandwidth) is deliberately
/// absent: the profile key already carries `platform` and the worker's torch
/// build, and a CPU model string is neither stable across kernels nor
/// comparable between vendors.
///
/// Rounded **up to the nearest 4 GiB**, the rule `rocm::apu_device_name`
/// introduced and for the same reason. What every platform reports as "total
/// RAM" is what the OS could count *after* firmware reservations — 1.5–2 GiB
/// on a real machine, and not constant across a kernel update, a
/// `crashkernel=` change or a BIOS setting. Rounded to nearest, a 64 GiB box
/// would name itself `(63 GB)` today and could silently become `(64 GB)`
/// tomorrow, splitting its own profiles. The 4 GiB grid swallows that drift
/// and lands on the capacity the machine is sold with, while still separating
/// the sizes that price differently. Budgets are untouched: this is the
/// calibration key and nothing else.
pub(super) fn gpu_name(ram_mb: u64) -> String {
    format!("CPU ({} GB)", capacity_gb_up_4(ram_mb))
}

/// The GPU's live free reading, or `None` when RAM statistics could not be
/// read.
///
/// `free` is `ram_available` bounded by the RAM that physically exists, and
/// nothing else — there is no accelerator pool to intersect it with, which is
/// exactly what makes this backend the degenerate instance of the model. The
/// clamp to the GPU's *admission* total is the ledger's own arithmetic
/// (`external = total − free − ours` saturates at zero), as on MPS.
///
/// The `total_mb` reported alongside is that same physical bound, which here
/// happens to equal the GPU's total; the ledger's refresh reads `free_mb`
/// from this and nothing else in either case (a memory refresh may not move a
/// GPU's total).
pub(super) fn query_memory(key: &str, ram_mb: u64, roots: &MemRoots) -> Option<Vec<GpuMemory>> {
    let available = ram_available_mb(roots)?;
    Some(vec![GpuMemory {
        uuid: key.to_owned(),
        total_mb: ram_mb,
        free_mb: free_mb(ram_mb, available),
    }])
}

/// The free reading, given RAM statistics: what the OS says it could deliver
/// right now, bounded by the RAM that exists.
fn free_mb(ram_mb: u64, ram_available_mb: u64) -> u64 {
    ram_available_mb.min(ram_mb)
}

/// Physical RAM in MiB, per this platform's own accounting. `None` on a
/// platform with no reader, and on a reader that failed.
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

/// RAM the OS says it could deliver right now, in MiB.
///
/// Each platform's own answer to that question, and deliberately the same one
/// the worker's `psutil.virtual_memory().available` gives, because the two
/// readings share a `free_source` label (`"ram"`) and the ledger's
/// source-precedence rule assumes they measure the same thing. `MemAvailable`
/// *is* what psutil reports on Linux and `ullAvailPhys` is what it reports on
/// Windows; the macOS path goes through `mps::ram_available_mb`, which sums
/// the free+inactive pages psutil sums there.
///
/// Over-stating availability here would under-state external pressure, which
/// is the one error direction the ledger cannot absorb — so where a platform
/// offers a looser figure, the tighter one is taken.
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
    //! The one syscall, and the only code in this module that is not a pure
    //! function of a file or an injected number.
    //!
    //! `windows-sys` was already a direct Windows dependency (job objects,
    //! `LockFileEx`); this adds the `Win32_System_SystemInformation` feature
    //! and no new crate, which is what the design's "no new crates" rule
    //! asks for.

    use std::ptr;

    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    const MIB: u64 = 1024 * 1024;

    fn status() -> Option<MEMORYSTATUSEX> {
        // SAFETY: zeroed is a valid `MEMORYSTATUSEX` (all fields are plain
        // integers); `dwLength` is then set, which is the only field the API
        // reads rather than writes.
        let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
        status.dwLength = u32::try_from(std::mem::size_of::<MEMORYSTATUSEX>()).ok()?;
        // SAFETY: the out-buffer is a whole `MEMORYSTATUSEX` and its
        // `dwLength` says so, which is the contract `GlobalMemoryStatusEx`
        // documents.
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
