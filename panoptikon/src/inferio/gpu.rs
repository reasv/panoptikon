//! GPU identity enumeration and worker→GPU pin resolution.
//!
//! Batch calibration keys budgets by GPU *instance* — the board UUID
//! (`GPU-…`) NVML/nvidia-smi report — because a CUDA device index is not an
//! identity: it moves across reboots and with every `CUDA_VISIBLE_DEVICES`
//! change (docs/batch-calibration-design.md, "Two keyspaces"). This module
//! is the source of those identities, and the one place that turns a
//! registry `devices` entry (or the absence of one) into the concrete
//! `CUDA_VISIBLE_DEVICES` value a worker is spawned with.
//!
//! Probing reuses the Package-1 philosophy from `capability.rs`: one short
//! `nvidia-smi` call with a timeout, and any unparseable *identity* makes
//! the whole result unknown. Unknown never changes behaviour — pins pass
//! through exactly as they did before this existed (raw index strings, or
//! no pin at all), which is what keeps CPU/MPS/ROCm hosts and hosts without
//! nvidia-smi on today's code path.
//!
//! The `compute_cap` column is the one exception, because it is the one
//! field that is *separably* useless: vGPU slices and a few datacenter SKUs
//! print `[N/A]` there while every identity column is perfectly good, and
//! discarding those rows wholesale would take pinning, the ledger and the
//! board list down with them. Such a board keeps its identity and is simply
//! never chosen by capability-ranked default placement.
//!
//! There is exactly **one** probe for both hardware facts the server needs
//! (board identities here, compute capabilities in `capability.rs`): they
//! come from one `--query-gpu` invocation, so the two views can never
//! disagree about which board is which, and boot pays one subprocess
//! instead of two. Rows are matched positionally, so an inventory index and
//! a capability always describe the same physical board.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::capability::{
    HostComputeCaps, find_nvidia_smi, output_with_timeout, parse_compute_cap,
};

/// One visible NVIDIA board.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GpuInfo {
    /// nvidia-smi enumeration index. Useful only for resolving registry
    /// `devices = ["3"]` pins into a UUID; never an identity.
    pub index: u32,
    /// Board UUID (`GPU-…`), the budget/ledger key and the pin form CUDA
    /// accepts directly in `CUDA_VISIBLE_DEVICES`.
    pub uuid: String,
    /// Marketing name, e.g. `NVIDIA GeForce RTX 5090`; the cost-profile key.
    pub name: String,
    pub total_mb: u64,
    /// Compute capability as `major.minor` (`"12.0"`), the same value
    /// `HostComputeCaps` filters models with — per board here, because
    /// default placement picks the fastest one. `None` when nvidia-smi could
    /// not report it for this board (`[N/A]` on vGPU slices and some
    /// datacenter SKUs): the board is still a usable, pinnable identity, it
    /// just cannot be ranked or used to unlock a capability-gated model.
    pub compute_cap: Option<String>,
}

impl GpuInfo {
    /// `major * 10 + minor`, the comparable form, or `None` for a board
    /// whose capability nvidia-smi did not report. Every stored string
    /// parsed when it was accepted, so `None` here means "not reported",
    /// never "unreadable" — and it must never collapse to `0`, which would
    /// make an unknown board look like the slowest one rather than an
    /// unranked one.
    fn cap_tenths(&self) -> Option<u32> {
        parse_compute_cap(self.compute_cap.as_deref()?).map(|(major, minor)| major * 10 + minor)
    }
}

/// The visible GPUs, or `None` for "unknown host" (no nvidia-smi, probe
/// failure, or any unparseable output). Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct GpuInventory(Option<Arc<[GpuInfo]>>);

/// Everything one `nvidia-smi` call tells us about this host's boards.
pub struct HostGpus {
    /// Compute-capability floors for `/metadata` availability filtering.
    pub caps: HostComputeCaps,
    /// Board identities for worker→GPU pinning and the per-GPU ledger.
    pub inventory: GpuInventory,
}

/// Probe once at startup; never fails. There is no state in which we trust a
/// row's capability but not its identity — but the reverse exists twice over:
/// a board can report an identity and no capability (see [`GpuInfo`]), and an
/// ambient index-form `CUDA_VISIBLE_DEVICES` blanks the inventory while
/// leaving the capabilities perfectly knowable (see [`build`]).
pub fn probe() -> HostGpus {
    // The ambient value matters: nvidia-smi *ignores* CUDA_VISIBLE_DEVICES
    // and reports every board, so an operator who launched the gateway with
    // a restriction would otherwise see us pin workers to boards they
    // deliberately hid (see `restrict_to_visible`).
    let visible = std::env::var("CUDA_VISIBLE_DEVICES").ok();
    build(query().as_deref(), visible.as_deref())
}

/// Run the single query. `None` on any failure — no nvidia-smi, timeout,
/// non-zero exit.
fn query() -> Option<String> {
    let smi = find_nvidia_smi()?;
    let mut cmd = Command::new(smi);
    cmd.args([
        "--query-gpu=index,uuid,name,memory.total,compute_cap",
        "--format=csv,noheader,nounits",
    ]);
    let Some(output) = output_with_timeout(cmd, Duration::from_secs(5)) else {
        tracing::warn!(
            "nvidia-smi GPU probe failed or timed out; workers will not be \
             pinned to a specific GPU and model availability will not be \
             capability-filtered"
        );
        return None;
    };
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Turn probe output plus the ambient `CUDA_VISIBLE_DEVICES` into both
/// views. Pure, so tests drive it without a GPU and without mutating the
/// process environment.
///
/// The two views degrade independently in exactly one direction. A
/// restriction we cannot map to boards (index form) blanks the **inventory**
/// only — nobody gets pinned, as before pinning existed — while the
/// capability view keeps every row nvidia-smi reported, which is what
/// Package 1's availability gate did before this module existed (it ignored
/// `CUDA_VISIBLE_DEVICES` outright). Blanking both would silently un-gate
/// every capability-floored model on hosts that merely set an index
/// restriction. A UUID-form restriction that *resolves* does narrow both:
/// there we know exactly which boards are hidden, and a hidden board must
/// not unlock a gated model it will never run. One that resolves to nothing
/// is the unmappable case again rather than "no boards" — a `MIG-…` pin is
/// legitimate and never appears among these rows, so its physical board's
/// capability is still the right answer for gating.
fn build(stdout: Option<&str>, visible: Option<&str>) -> HostGpus {
    let Some(gpus) = stdout.and_then(parse_inventory) else {
        return HostGpus {
            caps: HostComputeCaps::unknown(),
            inventory: GpuInventory(None),
        };
    };
    let all_caps = caps_of(&gpus);
    let Some(gpus) = restrict_to_visible(gpus, visible) else {
        return HostGpus {
            caps: HostComputeCaps::from_caps(all_caps),
            inventory: GpuInventory(None),
        };
    };
    for gpu in &gpus {
        tracing::info!(
            index = gpu.index,
            uuid = %gpu.uuid,
            name = %gpu.name,
            total_mb = gpu.total_mb,
            compute_cap = gpu.compute_cap.as_deref().unwrap_or("unknown"),
            "detected GPU"
        );
    }
    HostGpus {
        caps: HostComputeCaps::from_caps(caps_of(&gpus)),
        inventory: GpuInventory(Some(gpus.into())),
    }
}

/// The capabilities of the boards that reported one. An all-capless (or
/// empty) row set yields an empty vec, which `HostComputeCaps::from_caps`
/// turns into "unknown" — a host with nothing readable cannot filter. Kept
/// free of `HostComputeCaps` construction so `build` pays (and logs) exactly
/// one capability view per call.
fn caps_of(gpus: &[GpuInfo]) -> Vec<(u32, u32)> {
    gpus.iter()
        .filter_map(|gpu| parse_compute_cap(gpu.compute_cap.as_deref()?))
        .collect()
}

/// Apply the operator's ambient `CUDA_VISIBLE_DEVICES` to the board list
/// nvidia-smi reported (it ignores the variable entirely).
///
/// - unset, or set to the empty string → no restriction (the empty value is
///   treated as "not configured" rather than "hide everything");
/// - all entries in UUID form (`GPU-…`/`MIG-…`, abbreviated forms included,
///   which CUDA accepts) → keep exactly those boards, in nvidia-smi order;
/// - **any** index-form entry → `None`, i.e. the inventory becomes unknown
///   (the capability view does not — see [`build`]). Ambient indices are in
///   *CUDA* order (affected by `CUDA_DEVICE_ORDER`), which we cannot map to
///   nvidia-smi rows, and pinning a worker to the wrong board is worse than
///   not pinning: with an unknown inventory workers simply inherit the
///   ambient restriction, which is what they did before pinning existed.
fn restrict_to_visible(gpus: Vec<GpuInfo>, visible: Option<&str>) -> Option<Vec<GpuInfo>> {
    let entries: Vec<&str> = visible
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect();
    if entries.is_empty() {
        return Some(gpus);
    }
    if !entries.iter().all(|entry| is_uuid_pin(entry)) {
        tracing::info!(
            visible_devices = %visible.unwrap_or(""),
            "CUDA_VISIBLE_DEVICES names devices by index; leaving the GPU \
             inventory unknown (indices are in CUDA order and cannot be \
             mapped to boards) — workers inherit the restriction as-is"
        );
        return None;
    }
    let restricted: Vec<GpuInfo> = gpus
        .into_iter()
        .filter(|gpu| {
            entries.iter().any(|entry| {
                let entry = entry.to_ascii_uppercase();
                gpu.uuid.to_ascii_uppercase().starts_with(&entry)
            })
        })
        .collect();
    if restricted.is_empty() {
        tracing::warn!(
            visible_devices = %visible.unwrap_or(""),
            "CUDA_VISIBLE_DEVICES names no board nvidia-smi reports; leaving \
             the GPU inventory unknown"
        );
        return None;
    }
    tracing::info!(
        visible_devices = %visible.unwrap_or(""),
        boards = restricted.len(),
        "restricting the GPU inventory to the ambient CUDA_VISIBLE_DEVICES"
    );
    Some(restricted)
}

impl GpuInventory {
    /// Explicitly-unknown inventory: the state every non-CUDA host is in.
    /// Only tests construct one without probing; production goes through
    /// [`probe`] (same convention as `HostComputeCaps`).
    #[cfg(test)]
    pub fn unknown() -> Self {
        Self(None)
    }

    /// Construct a known inventory (tests only; the probe path builds it
    /// directly).
    #[cfg(test)]
    pub fn known(gpus: Vec<GpuInfo>) -> Self {
        if gpus.is_empty() {
            Self(None)
        } else {
            Self(Some(gpus.into()))
        }
    }

    /// The boards, or `None` when the host is unknown.
    pub fn gpus(&self) -> Option<&[GpuInfo]> {
        self.0.as_deref()
    }

    /// Default placement: the **highest-compute-capability** board, ties
    /// broken by the lowest nvidia-smi index. Boards whose capability
    /// nvidia-smi did not report rank *last* rather than lowest — unknown is
    /// not slow, and a host where nothing reported a capability still needs a
    /// default (the lowest index, as any tie does).
    ///
    /// This is rough parity with what an unpinned worker got before pinning
    /// existed: torch's default device order is CUDA's `FASTEST_FIRST`, so
    /// "no pin, impls run on `devices[0]`" already meant the fastest visible
    /// board, not the lowest one on the bus. Ordering by nvidia-smi index
    /// instead would silently move every unpinned model onto the slow card
    /// of a mixed host. Headroom-based placement is a follow-up once the
    /// ledgers exist (docs/batch-calibration-design.md).
    ///
    /// One behavioural nuance remains: "usable" is a torch-build property
    /// (kernel coverage per compute capability) that this side cannot see,
    /// so on a host whose fastest board has no kernels the impl-side filter
    /// falls back to CPU instead of silently using another board.
    pub fn default_pin(&self) -> Option<String> {
        let gpus = self.0.as_deref()?;
        gpus.iter()
            .min_by_key(|gpu| (std::cmp::Reverse(gpu.cap_tenths()), gpu.index))
            .map(|gpu| gpu.uuid.clone())
    }

    /// Resolve one replica's registry pin into the `CUDA_VISIBLE_DEVICES`
    /// value it is spawned with:
    ///
    /// - unknown inventory → the request verbatim (`None` stays `None`):
    ///   exactly today's behaviour, which is what CPU/MPS/ROCm hosts need;
    /// - no request → the default board's UUID (universal pinning);
    /// - a UUID request (`GPU-…`/`MIG-…`) → verbatim; CUDA accepts it;
    /// - an index request → that board's UUID, so the ledger key is stable
    ///   even though the index is not;
    /// - anything else (an index we cannot see, a comma-separated list, a
    ///   templated leftover) → verbatim with a warning. Passing it through
    ///   preserves whatever the operator meant; guessing would not.
    pub fn resolve_pin(&self, requested: Option<&str>) -> Option<String> {
        let Some(gpus) = self.0.as_deref() else {
            return requested.map(str::to_owned);
        };
        let Some(requested) = requested else {
            return self.default_pin();
        };
        let trimmed = requested.trim();
        if is_uuid_pin(trimmed) {
            return Some(trimmed.to_owned());
        }
        if let Ok(index) = trimmed.parse::<u32>() {
            if let Some(gpu) = gpus.iter().find(|gpu| gpu.index == index) {
                return Some(gpu.uuid.clone());
            }
        }
        tracing::warn!(
            pin = %requested,
            "device pin does not name a visible GPU; passing it to \
             CUDA_VISIBLE_DEVICES unchanged"
        );
        Some(requested.to_owned())
    }
}

/// `GPU-…` (and MIG's `MIG-…`) are the forms CUDA accepts in
/// `CUDA_VISIBLE_DEVICES` verbatim.
fn is_uuid_pin(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.starts_with("GPU-") || upper.starts_with("MIG-")
}

/// One GPU per line, `index, uuid, name, total, compute_cap`
/// (`--format=csv,noheader,nounits`). Any line whose **identity** columns
/// (index, uuid, name, total) do not parse — or whose column count is not
/// five — makes the whole probe unknown: a partial picture must not drive
/// pinning or filter models, exactly as before the two probes were merged.
///
/// The capability column alone is per-row optional (`[N/A]` on vGPU slices
/// and some datacenter SKUs). Dropping such a host's whole inventory would
/// cost it pinning and the ledger over a field only default placement and
/// model gating use, so the row keeps its identity with no capability.
fn parse_inventory(stdout: &str) -> Option<Vec<GpuInfo>> {
    let mut gpus = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split(',');
        let index = fields.next()?.trim().parse::<u32>().ok()?;
        let uuid = fields.next()?.trim().to_owned();
        let name = fields.next()?.trim().to_owned();
        let total_mb = fields.next()?.trim().parse::<u64>().ok()?;
        let cap_field = fields.next()?.trim().to_owned();
        if fields.next().is_some() || !is_uuid_pin(&uuid) || name.is_empty() {
            return None;
        }
        let compute_cap = if parse_compute_cap(&cap_field).is_some() {
            Some(cap_field)
        } else {
            tracing::info!(
                uuid = %uuid,
                compute_cap = %cap_field,
                "nvidia-smi did not report this board's compute capability; it \
                 stays pinnable but is not used for default placement and \
                 cannot satisfy a model's capability floor"
            );
            None
        };
        gpus.push(GpuInfo {
            index,
            uuid,
            name,
            total_mb,
            compute_cap,
        });
    }
    if gpus.is_empty() {
        None
    } else {
        Some(gpus)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(index: u32, uuid: &str, cap: &str) -> GpuInfo {
        GpuInfo {
            index,
            uuid: uuid.into(),
            name: "NVIDIA GeForce RTX 5090".into(),
            total_mb: 32607,
            compute_cap: (!cap.is_empty()).then(|| cap.to_owned()),
        }
    }

    fn inventory() -> GpuInventory {
        GpuInventory::known(vec![gpu(0, "GPU-1111", "12.0"), gpu(3, "GPU-3333", "12.0")])
    }

    const TWO_BOARDS: &str = "0, GPU-1a2b, NVIDIA GeForce RTX 5090, 32607, 12.0\n\
                              1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n";

    #[test]
    fn parses_nvidia_smi_inventory() {
        let gpus = parse_inventory(TWO_BOARDS).expect("parses");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].uuid, "GPU-1a2b");
        assert_eq!(gpus[0].name, "NVIDIA GeForce RTX 5090");
        assert_eq!(gpus[0].total_mb, 32607);
        assert_eq!(gpus[0].compute_cap.as_deref(), Some("12.0"));
        assert_eq!(gpus[1].index, 1);
    }

    /// The merged probe feeds both views from the same rows, so an index in
    /// the inventory and a capability in the filter always describe the same
    /// physical board.
    #[test]
    fn one_probe_builds_both_views() {
        let host = build(Some(TWO_BOARDS), None);
        assert_eq!(
            host.inventory
                .gpus()
                .expect("known")
                .iter()
                .map(|gpu| gpu.uuid.as_str())
                .collect::<Vec<_>>(),
            vec!["GPU-1a2b", "GPU-3c4d"]
        );
        // 12.0 satisfies an sm_80 floor, 8.6 does too; a 9.0 floor is met by
        // exactly one board, which is what "ANY device" means.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(true));
        assert_eq!(host.caps.meets_floor(12.1), Some(false));
    }

    #[test]
    fn garbage_in_the_identity_columns_makes_both_views_unknown() {
        assert!(parse_inventory("").is_none());
        assert!(parse_inventory("N/A\n").is_none());
        assert!(
            parse_inventory("Failed to initialize NVML: Driver error\n").is_none(),
            "driver error text must not become a GPU"
        );
        // One good line plus one bad line: the whole probe is unknown.
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607, 8.6\nN/A, N/A, N/A, N/A, N/A\n").is_none());
        // A non-UUID identity column is not something we can key a ledger
        // by, so it is unknown rather than half-trusted.
        assert!(parse_inventory("0, 0, RTX, 32607, 8.6\n").is_none());
        // Missing/extra columns mean the query changed shape under us.
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607\n").is_none());
        assert!(parse_inventory("0, GPU-1a2b, RTX, 32607, 8.6, extra\n").is_none());

        let host = build(Some("N/A\n"), None);
        assert!(host.inventory.gpus().is_none());
        assert_eq!(host.caps.meets_floor(8.0), None, "capabilities go with it");
        let missing_smi = build(None, None);
        assert!(missing_smi.inventory.gpus().is_none());
        assert_eq!(missing_smi.caps.meets_floor(8.0), None);
    }

    /// The capability column is the one separably-useless field: vGPU slices
    /// and some datacenter SKUs print `[N/A]` there with every identity
    /// column intact. Dropping the row would cost the host pinning and the
    /// per-GPU ledger over a field only placement and gating read.
    #[test]
    fn an_unreported_capability_keeps_the_board_identity() {
        let mixed = "0, GPU-1a2b, NVIDIA A100-SXM4-40GB MIG 1g.5gb, 4864, [N/A]\n\
                     1, GPU-3c4d, NVIDIA RTX A2000, 6138, 8.6\n";
        let host = build(Some(mixed), None);
        let gpus = host.inventory.gpus().expect("identities are all good");
        assert_eq!(gpus.len(), 2);
        assert_eq!(gpus[0].compute_cap, None);
        assert_eq!(gpus[0].uuid, "GPU-1a2b", "still a pinnable ledger identity");
        assert_eq!(
            host.inventory.resolve_pin(Some("0")),
            Some("GPU-1a2b".to_string()),
            "index pins still resolve through it"
        );
        // Capabilities come from the boards that reported one.
        assert_eq!(host.caps.meets_floor(8.0), Some(true));
        assert_eq!(host.caps.meets_floor(9.0), Some(false));
        // Unknown is not slow: the capless board is unranked, not preferred
        // and not treated as compute capability 0.
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "a board of unknown speed must not win default placement"
        );

        // No board reports one: identities stay, the capability view is
        // unknown (and so filters nothing), and placement still has to pick
        // something — the lowest index, as any tie does.
        let capless = build(
            Some(
                "1, GPU-3c4d, NVIDIA RTX A2000, 6138, [N/A]\n\
                 0, GPU-1a2b, NVIDIA RTX A2000, 6138, N/A\n",
            ),
            None,
        );
        assert_eq!(capless.inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        assert_eq!(capless.caps.meets_floor(8.0), None);
        assert_eq!(capless.inventory.default_pin(), Some("GPU-1a2b".to_string()));
    }

    /// nvidia-smi ignores `CUDA_VISIBLE_DEVICES`, so an operator's ambient
    /// restriction has to be applied here — otherwise pin resolution would
    /// hand a worker a board the operator deliberately hid, and the worker
    /// (which *does* honour the variable) would fail or land elsewhere.
    #[test]
    fn ambient_visible_devices_restricts_the_inventory() {
        // UUID form: keep exactly the named boards, in nvidia-smi order.
        let host = build(Some(TWO_BOARDS), Some("GPU-3c4d"));
        let gpus = host.inventory.gpus().expect("known");
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].uuid, "GPU-3c4d");
        assert_eq!(
            host.inventory.default_pin(),
            Some("GPU-3c4d".to_string()),
            "placement can only choose among visible boards"
        );
        assert_eq!(
            host.caps.meets_floor(12.0),
            Some(false),
            "the hidden board's capability must not filter models either"
        );

        // Abbreviated UUIDs are legal for CUDA, so they are honoured here.
        let abbreviated = build(Some(TWO_BOARDS), Some("GPU-1a"));
        assert_eq!(
            abbreviated.inventory.default_pin(),
            Some("GPU-1a2b".to_string())
        );

        // Unset and empty both mean "no restriction".
        assert_eq!(build(Some(TWO_BOARDS), Some("")).inventory.gpus().map(<[GpuInfo]>::len), Some(2));
        assert_eq!(build(Some(TWO_BOARDS), Some(" , ")).inventory.gpus().map(<[GpuInfo]>::len), Some(2));
    }

    /// An unmappable ambient restriction blanks the **inventory only**. The
    /// capability view keeps every board nvidia-smi reported, which is what
    /// Package 1's availability gate saw before this module existed (it never
    /// looked at `CUDA_VISIBLE_DEVICES`); taking it down with the inventory
    /// would silently un-gate every capability-floored model on any host that
    /// merely restricts by index.
    #[test]
    fn an_unmappable_restriction_blanks_only_the_inventory() {
        // Index form: unmappable (CUDA order != nvidia-smi order), so the
        // inventory goes unknown and workers inherit the restriction as-is.
        let indexed = build(Some(TWO_BOARDS), Some("1"));
        assert!(indexed.inventory.gpus().is_none());
        assert_eq!(indexed.inventory.resolve_pin(None), None, "no pinning");
        assert_eq!(
            indexed.caps.meets_floor(12.0),
            Some(true),
            "model availability is still capability-filtered"
        );
        assert_eq!(indexed.caps.meets_floor(12.1), Some(false));
        // Mixed forms are index-form as far as safety goes.
        let mixed = build(Some(TWO_BOARDS), Some("GPU-1a2b,1"));
        assert!(mixed.inventory.gpus().is_none());
        assert_eq!(mixed.caps.meets_floor(12.0), Some(true));
        // A UUID restriction naming nothing we listed is unknown, not empty:
        // a legitimate `MIG-…` pin never appears among these rows, so this is
        // "cannot map", not "no boards", and the physical capabilities stand.
        let nothing = build(Some(TWO_BOARDS), Some("MIG-abcd"));
        assert!(nothing.inventory.gpus().is_none());
        assert_eq!(nothing.caps.meets_floor(12.0), Some(true));
    }

    /// Default placement is the fastest board (parity with CUDA's
    /// FASTEST_FIRST ordering, which is what unpinned workers saw), not the
    /// lowest index.
    #[test]
    fn default_pin_is_the_fastest_board() {
        let mixed = GpuInventory::known(vec![gpu(0, "GPU-slow", "8.6"), gpu(1, "GPU-fast", "12.0")]);
        assert_eq!(mixed.default_pin(), Some("GPU-fast".to_string()));
        // 10.x is above 9.x, not lexicographically below it.
        let blackwell = GpuInventory::known(vec![gpu(0, "GPU-9", "9.0"), gpu(1, "GPU-12", "12.0")]);
        assert_eq!(blackwell.default_pin(), Some("GPU-12".to_string()));
        // Ties (the common homogeneous host) go to the lowest index.
        assert_eq!(inventory().default_pin(), Some("GPU-1111".to_string()));
    }

    #[test]
    fn unpinned_replica_resolves_to_the_default_board() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(None),
            Some("GPU-1111".to_string()),
            "universal pinning: no pin means the default board's UUID"
        );
    }

    #[test]
    fn index_pins_map_to_uuids() {
        let inventory = inventory();
        assert_eq!(inventory.resolve_pin(Some("3")), Some("GPU-3333".to_string()));
        assert_eq!(inventory.resolve_pin(Some(" 0 ")), Some("GPU-1111".to_string()));
    }

    #[test]
    fn uuid_pins_pass_through_verbatim() {
        let inventory = inventory();
        assert_eq!(
            inventory.resolve_pin(Some("GPU-9999")),
            Some("GPU-9999".to_string()),
            "an explicit UUID is accepted even for a board we cannot see"
        );
        assert_eq!(
            inventory.resolve_pin(Some("MIG-abc")),
            Some("MIG-abc".to_string())
        );
    }

    #[test]
    fn unresolvable_pins_pass_through() {
        let inventory = inventory();
        // Index nobody reported, a multi-device list, and a non-numeric
        // string all reach CUDA_VISIBLE_DEVICES unchanged.
        assert_eq!(inventory.resolve_pin(Some("7")), Some("7".to_string()));
        assert_eq!(inventory.resolve_pin(Some("0,3")), Some("0,3".to_string()));
        assert_eq!(inventory.resolve_pin(Some("cpu")), Some("cpu".to_string()));
    }

    #[test]
    fn unknown_inventory_changes_nothing() {
        let unknown = GpuInventory::unknown();
        assert!(unknown.gpus().is_none());
        assert_eq!(unknown.default_pin(), None);
        assert_eq!(unknown.resolve_pin(None), None, "no pin, as before");
        assert_eq!(
            unknown.resolve_pin(Some("3")),
            Some("3".to_string()),
            "raw index passthrough, as before"
        );
    }
}
