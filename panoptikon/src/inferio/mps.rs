//! Apple Silicon (MPS) GPU facts, read from the macOS kernel.
//!
//! One synthetic unified-memory device per host, with a constant key, named
//! from `machdep.cpu.brand_string` and sized from `hw.memsize`. Live free
//! memory comes from `host_statistics64`. Off macOS every reader returns
//! `None`. See docs/unified-memory-admission.md "Backend A: MPS (Apple
//! Silicon)".

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
}

/// RAM a new allocation could get: RAM minus wired, compressed and anonymous
/// pages (Activity Monitor's "used"). File cache counts as available. Must
/// not use `free + inactive`: macOS moves pages another process still holds
/// onto the inactive queue, so that figure rises without anything freed.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn available_bytes(facts: &MemoryFacts) -> u64 {
    let taken = facts
        .wired
        .saturating_add(facts.compressed)
        .saturating_add(facts.anonymous);
    facts.ram.saturating_sub(taken)
}

/// RAM the OS could deliver now, in MiB ([`available_bytes`]); `None` off
/// macOS. The worker computes the same figure under the `"mps"` label.
pub(super) fn ram_available_mb() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        memory_facts().map(|facts| available_bytes(&facts) / MIB)
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
    pub(super) fn memory_facts() -> Option<super::MemoryFacts> {
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
        let pages = |count: u32| u64::from(count).saturating_mul(page);
        Some(super::MemoryFacts {
            ram: sysctl_u64("hw.memsize").filter(|bytes| *bytes > 0)?,
            wired: pages(stats.wire_count),
            compressed: pages(stats.compressor_page_count),
            anonymous: pages(stats.internal_page_count),
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
    }
}
