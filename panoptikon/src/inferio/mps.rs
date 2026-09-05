//! Apple Silicon (MPS) GPU facts, read from the macOS kernel.
//!
//! One synthetic unified-memory device whose memory is the host's RAM. There
//! is one Metal device per host, no visibility variable to pin with and no
//! UUID to key by, so the inventory is a constant key plus two kernel facts:
//! `machdep.cpu.brand_string` (with the capacity, the calibration profile
//! name) and `hw.memsize` (physical RAM, both the seed for the device total
//! and the only sanity bound on the authoritative figure the first worker
//! reports back). Live free memory is `host_statistics64`'s view of RAM — no
//! accelerator counter would say a browser is eating 40 GB. Off macOS the
//! readers answer `None`.
//! See docs/unified-memory-admission.md "Backend A: MPS (Apple Silicon)".

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

/// This host's facts, or `None` off Apple Silicon and on a Mac whose sysctls
/// did not answer. The **architecture** gate matters as much as the OS one: a
/// user can hand-write `accelerator = "mps"` on an Intel Mac.
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

/// The single synthetic device these facts describe. `total_mb` is a
/// **seed**: Metal's `recommendedMaxWorkingSetSize` defaults to ≈75 % of RAM
/// but moves with `iogpu.wired_limit_mb`, so the real figure is adopted from
/// the first worker's load report (DP-4).
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
        // No carve-out/GTT split on Apple Silicon: one pool, of which the
        // total above is the policy budget.
        vram_carveout_mb: None,
    }
}

/// Metal's default recommended working-set size: three quarters of RAM.
fn seed_total_mb(ram_mb: u64) -> u64 {
    ram_mb / 4 * 3
}

/// The display *and* calibration-profile name: `Apple M3 Max (128 GB)`.
/// Built from kernel facts alone, so it cannot move with the environment and
/// orphan the profiles keyed by it; the capacity is in the key because a
/// 128 GB M3 Max and a 36 GB one do not price alike, rounded to the nearest
/// GiB (exact on every shipping Mac).
pub(super) fn gpu_name(chip: &str, ram_bytes: u64) -> String {
    let gb = ((ram_bytes + GIB / 2) / GIB).max(1);
    format!("{chip} ({gb} GB)")
}

/// The device's live free reading, or `None` when RAM statistics could not
/// be read. `free` is deliberately **not** clamped to the admission total:
/// the ledger's arithmetic saturates at zero anyway, whereas clamping here
/// would use the probe's *seed* and price phantom external usage on every
/// host that adopted a larger total.
pub(super) fn query_memory(key: &str, ram_mb: u64) -> Option<Vec<GpuMemory>> {
    let available = ram_available_mb()?;
    Some(vec![GpuMemory {
        uuid: key.to_owned(),
        // Deliberately **not** the device's total: this is physical RAM,
        // the bound `free_mb` was computed against, and is safe only because
        // the refresh consumes `free_mb` alone.
        total_mb: ram_mb,
        free_mb: free_mb(ram_mb, available),
    }])
}

/// What the OS could deliver, bounded by physical RAM.
fn free_mb(ram_mb: u64, ram_available_mb: u64) -> u64 {
    ram_available_mb.min(ram_mb)
}

/// This Mac's physical RAM in MiB, or `None` off macOS. Shared with
/// `cpu.rs`; **not** the device total, a policy figure over it.
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

/// RAM the OS says it could deliver right now, in MiB. `None` off macOS.
/// Deliberately conservative — free and inactive pages, nothing else —
/// because over-stating availability understates external pressure, the one
/// error direction the ledger cannot absorb. It is also what the worker
/// reports under the same `"mps"` label.
pub(super) fn ram_available_mb() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        ram_available_bytes().map(|bytes| bytes / MIB)
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

    // `mach_host_self` is deprecated in libc in favour of `mach2`; one call
    // does not earn a dependency.
    #[allow(deprecated)]
    pub(super) fn ram_available_bytes() -> Option<u64> {
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
        if result != 0 {
            return None;
        }
        // SAFETY: sysconf takes a name and returns a long; no pointers.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page = u64::try_from(page).ok().filter(|page| *page > 0)?;
        // psutil's macOS `available` is free + inactive, and the worker's
        // sample and this refresh have to be the same reading.
        let pages = u64::from(stats.free_count).saturating_add(u64::from(stats.inactive_count));
        Some(pages.saturating_mul(page))
    }
}

#[cfg(target_os = "macos")]
use sys::{ram_available_bytes, sysctl_string, sysctl_u64};

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
            "the unified flag, and DP-4's only sanity bound"
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
    /// admission total is the ledger's `external` arithmetic, which tracks a
    /// DP-4 adoption this query cannot see.
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

    /// Off macOS every syscall path answers "unknown", which is what leaves
    /// such a host on the unpriced path instead of inventing a GPU.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn nothing_is_probed_off_macos() {
        assert_eq!(probe(), None);
        assert_eq!(ram_available_mb(), None);
        assert_eq!(query_memory(DEVICE_KEY, 128 * 1024), None);
    }
}
