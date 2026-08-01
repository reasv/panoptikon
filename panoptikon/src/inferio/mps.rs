//! Apple Silicon (MPS) board facts, read from the macOS kernel.
//!
//! The MPS half of `gpu`, and the first instance of the **unified board**
//! model (docs/unified-memory-admission.md, backend A): one synthetic board
//! whose memory is the host's RAM, shared with the OS and every other
//! process. There is exactly one Metal device per host, no visibility
//! variable to pin with and no UUID to key by, so the whole inventory is a
//! constant key plus two kernel facts:
//!
//! - `machdep.cpu.brand_string` — the chip (`Apple M3 Max`), which with the
//!   RAM capacity is the calibration profile name. Deterministic from kernel
//!   facts and identical on every host with that silicon, exactly like the
//!   ROCm derived names (`rocm.rs::board_name`);
//! - `hw.memsize` — physical RAM, which is both the seed for the board's
//!   total (Metal's `recommendedMaxWorkingSetSize` defaults to ≈75 % of it)
//!   and the only sanity bound on the *authoritative* figure the first
//!   worker reports back (DP-4).
//!
//! Live free memory is `host_statistics64`'s view of RAM, which is what
//! makes external pressure on a unified board visible at all: a browser
//! eating 40 GB has to show up the way a game eating VRAM shows up on a
//! dGPU, and no accelerator-level counter would ever say so.
//!
//! Everything except the three syscalls is a pure function of injected
//! facts, so the board construction, the naming and the refresh arithmetic
//! are tested on Windows and Linux as well; only [`probe`] and
//! [`ram_available_mb`] know they are on macOS, and off macOS they answer
//! `None` — which leaves such a host with an unknown MPS inventory, i.e.
//! exactly the unpriced behaviour it had before this module existed.

use super::gpu::{GpuInfo, GpuMemory};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// The one board key an MPS host ever has.
///
/// A constant, not a hardware identity: there is exactly one device per
/// host, per-board budget overrides live in that host's own config file, and
/// this is the string a user can actually type into
/// `[inference_local.vram.gpu."GPU-MPS"]`. It keeps the `GPU-` prefix
/// convention every other board key follows.
pub(super) const BOARD_KEY: &str = "GPU-MPS";

/// The two kernel facts an MPS board is derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HostFacts {
    /// `machdep.cpu.brand_string`, e.g. `Apple M3 Max`.
    pub chip: String,
    /// `hw.memsize` — physical RAM in bytes.
    pub ram_bytes: u64,
}

/// This host's facts, or `None` off Apple Silicon (and on an Apple Silicon
/// Mac whose sysctls did not answer, which leaves the inventory unknown
/// rather than half-known).
///
/// The **architecture** gate matters as much as the OS one. MPS exists only
/// on Apple Silicon — `setup.rs::macos_default` resolves the accelerator on
/// exactly that invariant, and releases build macOS aarch64 only — but
/// `accelerator = "mps"` is a value a user can hand-write into config on an
/// Intel Mac, where `effective_accelerator` swallows the resolve error. With
/// only the OS gate that host would fabricate a `GPU-MPS` board out of its
/// RAM and price grants against a Metal device torch will never use.
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

/// The single synthetic board these facts describe.
///
/// `total_mb` is a **seed**: Metal's `recommendedMaxWorkingSetSize` — the
/// figure allocations are actually judged against — defaults to ≈75 % of RAM
/// but moves with the GPU wired limit (`iogpu.wired_limit_mb`, a standard
/// tweak on Macs used for local ML), so the exact number is adopted from the
/// first worker's load report and this only has to keep budgets defined
/// until then (DP-4, `ledger::VramLedger::adopt_unified_total_locked`).
///
/// No capability (the shipped floors are CUDA-specific), no PCI address, and
/// no pin: `GpuInventory` treats an MPS inventory as no-pin everywhere,
/// because there is one device and no visibility variable that could select
/// it.
pub(super) fn board(facts: &HostFacts) -> GpuInfo {
    let ram_mb = facts.ram_bytes / MIB;
    GpuInfo {
        index: 0,
        uuid: BOARD_KEY.to_owned(),
        name: board_name(&facts.chip, facts.ram_bytes),
        total_mb: seed_total_mb(ram_mb),
        compute_cap: None,
        bdf: None,
        gfx_target_version: None,
        unified_ram_mb: Some(ram_mb),
    }
}

/// Metal's default recommended working-set size: three quarters of RAM.
fn seed_total_mb(ram_mb: u64) -> u64 {
    ram_mb / 4 * 3
}

/// The display *and* calibration-profile name: `Apple M3 Max (128 GB)`.
///
/// Same convention as the ROCm derived names (`rocm.rs::board_name`): built
/// from kernel facts alone, so it is byte-identical on every host with that
/// silicon and can never appear or disappear with the environment — which
/// would orphan every local profile, ratchet anchor and knee keyed by it.
/// The capacity belongs in the key because it changes admission behaviour:
/// a 128 GB M3 Max and a 36 GB one do not price alike.
///
/// Rounded to the **nearest** GiB, as on ROCm. Physical RAM is a whole
/// number of GiB on every shipping Mac, so the rounding is exact rather than
/// merely close.
pub(super) fn board_name(chip: &str, ram_bytes: u64) -> String {
    let gb = ((ram_bytes + GIB / 2) / GIB).max(1);
    format!("{chip} ({gb} GB)")
}

/// The board's live free reading, or `None` when RAM statistics could not be
/// read (off macOS, or a failed `host_statistics64`).
///
/// `free` is **not** clamped to the board's admission total here, and that is
/// deliberate. The clamp the design specifies —
/// `free = max(0, min(total, ram_available))` — is applied by the ledger's
/// own arithmetic: `external = total − free − ours` saturates at zero, which
/// is the same number for any `free ≥ total`. Clamping here would have to use
/// the total this query was *built* with, i.e. the probe's seed, and would
/// then keep pricing phantom external usage on every host whose real total
/// was adopted upward from it (DP-4) — the tuned machines, precisely.
///
/// `ram_mb` is therefore only a physical sanity bound (RAM available can
/// never exceed RAM), and the `total_mb` reported alongside is that same
/// bound: the ledger's refresh reads `free_mb` from this and nothing else,
/// and a board's total is not a thing a memory *refresh* is allowed to move.
pub(super) fn query_memory(key: &str, ram_mb: u64) -> Option<Vec<GpuMemory>> {
    let available = ram_available_mb()?;
    Some(vec![GpuMemory {
        uuid: key.to_owned(),
        // Deliberately **not** the board's total: this is physical RAM, the
        // sanity bound `free_mb` was computed against. It is safe only
        // because the ledger's refresh consumes `free_mb` and nothing else
        // (a memory refresh may not move a board's total, and the total in
        // force here may have been adopted upward from the seed — DP-4). Do
        // not wire this field through to anything that treats it as the
        // board total.
        total_mb: ram_mb,
        free_mb: free_mb(ram_mb, available),
    }])
}

/// The free reading, given RAM statistics: what the OS says it could deliver
/// right now, bounded by the RAM that physically exists.
fn free_mb(ram_mb: u64, ram_available_mb: u64) -> u64 {
    ram_available_mb.min(ram_mb)
}

/// RAM the OS says it could deliver right now, in MiB. `None` off macOS.
///
/// Deliberately conservative about what counts as available: free and
/// inactive pages, and nothing else. Compressed, file-backed and purgeable
/// pages *can* often be reclaimed — but a page the compressor has to
/// decompress under pressure is not memory a Metal allocation gets cheaply,
/// and over-stating availability here understates external pressure, which is
/// the one error direction the ledger cannot absorb (the margin lever and the
/// collapse detector are the containment for whatever this still gets wrong —
/// the design's "honest limits").
///
/// `free + inactive` is also exactly what the worker's `psutil` reports as
/// macOS `available`, and the two readings share a `free_source` label
/// (`"mps"`), so the ledger's source-precedence rule assumes they measure the
/// same thing. Adding purgeable pages here — the one term psutil leaves out —
/// would make the orchestrator's refresh systematically the *looser* of the
/// two, which is the forbidden direction twice over.
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
    //! The three syscalls, and the only code in this module that is not a
    //! pure function. Kept together so the cfg gate is one block rather than
    //! a scatter of attributes.

    use std::ffi::CString;
    use std::ptr;

    pub(super) fn sysctl_string(name: &str) -> Option<String> {
        let name = CString::new(name).ok()?;
        let mut len: libc::size_t = 0;
        // SAFETY: a null `oldp` with a valid `oldlenp` is the documented way
        // to ask sysctl only for the value's length; nothing is written to
        // the (absent) buffer.
        let sized = unsafe {
            libc::sysctlbyname(name.as_ptr(), ptr::null_mut(), &mut len, ptr::null_mut(), 0)
        };
        if sized != 0 || len == 0 {
            return None;
        }
        let mut buffer = vec![0u8; len];
        // SAFETY: `buffer` is `len` bytes and `len` is the size sysctl just
        // asked for; it may only shrink on the second call.
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

    // `mach_host_self` is deprecated in libc in favour of the `mach2` crate.
    // One call does not earn a dependency, and the function is not going
    // anywhere (it is the mach ABI); allowed here so the macOS build stays
    // warning-free, which is the whole policy.
    #[allow(deprecated)]
    pub(super) fn ram_available_bytes() -> Option<u64> {
        // SAFETY: zeroed is a valid `vm_statistics64` (all fields are plain
        // integers), and the kernel overwrites it wholesale on success.
        let mut stats: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
        let mut count = libc::HOST_VM_INFO64_COUNT;
        // SAFETY: the out-buffer is a whole `vm_statistics64` and `count`
        // is its size in `integer_t` units, which is the contract
        // `host_statistics64` documents.
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
        // psutil's macOS `available` is free + inactive; the worker's sample
        // and this refresh have to be the same reading (see the doc above).
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

    /// The board is one constant-keyed row whose name is the calibration
    /// keyspace and whose total is the 75 % seed — deterministic from the
    /// two kernel facts and nothing else.
    #[test]
    fn the_board_is_derived_from_the_two_kernel_facts() {
        let board = board(&facts("Apple M3 Max", 128));
        assert_eq!(board.uuid, "GPU-MPS");
        assert_eq!(board.name, "Apple M3 Max (128 GB)");
        assert_eq!(board.index, 0);
        assert_eq!(board.total_mb, 128 * 1024 / 4 * 3, "≈75% of RAM");
        assert_eq!(
            board.unified_ram_mb,
            Some(128 * 1024),
            "the unified flag, and DP-4's only sanity bound"
        );
        assert!(board.unified());
        assert_eq!(board.compute_cap, None, "no CUDA analogue exists");
        assert_eq!(board.bdf, None);
        assert_eq!(board.gfx_target_version, None);
        // A small Mac: the seed still lands on a whole number of MiB.
        assert_eq!(super::board(&facts("Apple M2", 8)).total_mb, 6 * 1024);
    }

    /// The name carries the capacity, rounded to the nearest GiB — the same
    /// convention the ROCm names use, for the same reason (two Macs of one
    /// chip and different RAM do not price alike).
    #[test]
    fn the_name_carries_the_chip_and_the_capacity() {
        assert_eq!(board_name("Apple M3 Max", 128 * GIB), "Apple M3 Max (128 GB)");
        assert_eq!(board_name("Apple M1", 16 * GIB), "Apple M1 (16 GB)");
        // Rounds to nearest, and never to zero.
        assert_eq!(board_name("Apple M4", 36 * GIB - 1), "Apple M4 (36 GB)");
        assert_eq!(board_name("Apple M4", GIB / 4), "Apple M4 (1 GB)");
    }

    /// The refresh hands the ledger the RAM the OS says it could deliver,
    /// bounded only by the RAM that exists — the per-board clamp to the
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
    /// such a host on the unpriced path instead of inventing a board.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn nothing_is_probed_off_macos() {
        assert_eq!(probe(), None);
        assert_eq!(ram_available_mb(), None);
        assert_eq!(query_memory(BOARD_KEY, 128 * 1024), None);
    }
}
