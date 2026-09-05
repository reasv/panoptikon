//! Host accelerator environment for inference workers and setup probes.
//!
//! Callers pass a **resolved** [`Accelerator`] (not `auto` — use
//! [`crate::setup::effective_accelerator`]). `cuda` stays empty so host HIP
//! trees do not alter linking. [`probe_after_setup`] is the extension point
//! for post-sync validation.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use crate::config::Accelerator;

/// The MPS allocator's ceiling, as a fraction of Metal's
/// `recommendedMaxWorkingSetSize` — the figure the ledger's MPS device is
/// budgeted against. Pinned to 1.0 so torch's hard out-of-memory error fires
/// exactly at that boundary: the build default drifts and can sit *above*
/// 1.0, inside the regime where macOS compresses and swaps instead of
/// failing. The **low** watermark is pinned with it because torch asserts
/// `high >= low` at allocator init (unified-memory doc, backend A).
const MPS_WATERMARK_ENV: [(&str, &str); 2] = [
    ("PYTORCH_MPS_HIGH_WATERMARK_RATIO", "1.0"),
    ("PYTORCH_MPS_LOW_WATERMARK_RATIO", "1.0"),
];

/// The device an impl must run on, read by `inferio.impl.utils.get_device`
/// before it probes for one itself. `get_device()` asks about the *machine*
/// while the orchestrator's pricing asks about the **installed wheels** and
/// the user's config; on a host where those differ the model would run on a
/// device nothing was budgeted against, so the answer is written down rather
/// than hoped for. `cpu` is the only value defined today, and an unknown one
/// is ignored worker-side with a warning.
/// See docs/unified-memory-admission.md "Backend C: CPU".
pub const DEVICE_ENV_VAR: &str = "INFERIO_DEVICE";

/// Env vars for an inference worker for a **resolved** accelerator.
/// HIP/HSA injection only for [`Accelerator::Rocm`], the MPS watermarks only
/// for [`Accelerator::Mps`], [`DEVICE_ENV_VAR`] only for
/// [`Accelerator::Cpu`]; `auto` is empty (resolve first) and explicit `cuda`
/// never injects, even with `/opt/rocm` on the host.
///
/// The CPU arm keys off the same resolved accelerator `gpu::probe` builds
/// the CPU device from, which makes "priced against RAM" and "runs on the
/// CPU" one decision. It is written even on a CPU host that has no device at
/// all: coherence does not depend on pricing having succeeded.
pub fn worker_env(accelerator: Accelerator) -> Vec<(String, String)> {
    match accelerator {
        Accelerator::Rocm => hip_worker_env(),
        Accelerator::Mps => MPS_WATERMARK_ENV
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        Accelerator::Cpu => vec![(DEVICE_ENV_VAR.to_owned(), "cpu".to_owned())],
        Accelerator::Cuda | Accelerator::Auto => Vec::new(),
    }
}

/// Post-`uv sync` accelerator checks: no-op except on ROCm, which runs a
/// trivial HIP kernel probe (soft-ok with no GPU).
pub async fn probe_after_setup(accelerator: Accelerator, interpreter: &Path) -> anyhow::Result<()> {
    match accelerator {
        Accelerator::Rocm => probe_rocm_torch(interpreter).await,
        Accelerator::Cpu | Accelerator::Cuda | Accelerator::Mps | Accelerator::Auto => Ok(()),
    }
}

// The HIP helpers below are only reachable from the `target_os = "linux"`
// arm of `hip_worker_env` (and its tests); allow dead_code elsewhere so
// non-Linux builds stay warning-free.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn hip_library_dirs() -> Vec<PathBuf> {
    #[cfg(not(target_os = "linux"))]
    {
        Vec::new()
    }
    #[cfg(target_os = "linux")]
    {
        let mut candidates = Vec::new();
        for key in ["ROCM_PATH", "HIP_PATH"] {
            if let Ok(root) = env::var(key) {
                candidates.push(PathBuf::from(root).join("lib"));
            }
        }
        candidates.extend([
            PathBuf::from("/opt/rocm/lib"),
            PathBuf::from("/run/current-system/sw/lib"),
            PathBuf::from("/run/opengl-driver/lib"),
        ]);
        select_existing_hip_lib_dirs(&candidates)
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn select_existing_hip_lib_dirs(candidates: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in candidates {
        if !dir.is_dir() || !is_hip_related_lib_dir(dir) {
            continue;
        }
        if !out.iter().any(|seen| seen == dir) {
            out.push(dir.clone());
        }
    }
    out
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn is_hip_related_lib_dir(dir: &Path) -> bool {
    const MARKERS: &[&str] = &[
        "libamdhip64.so",
        "libamdhip64.so.6",
        "libamdhip64.so.7",
        "libhsa-runtime64.so",
        "libhsa-runtime64.so.1",
    ];
    if MARKERS.iter().any(|name| dir.join(name).is_file()) {
        return true;
    }
    // NixOS Mesa/AMD client libs (no HIP .so markers of their own).
    dir.ends_with("opengl-driver/lib")
}

/// Prepend HIP dirs to `LD_LIBRARY_PATH`; default `ROCM_PATH`/`HIP_PATH` to
/// `/opt/rocm` when unset. Also sets MIOpen find/cache defaults so conv/GEMM
/// solver search does not stall (EasyOCR/CRAFT: IsEnoughWorkspace size 0).
/// Empty on non-Linux.
fn hip_worker_env() -> Vec<(String, String)> {
    #[cfg(not(target_os = "linux"))]
    {
        Vec::new()
    }
    #[cfg(target_os = "linux")]
    {
        let mut out = Vec::new();
        if let Some(joined) = merge_ld_library_path(&hip_library_dirs()) {
            out.push((
                "LD_LIBRARY_PATH".to_owned(),
                joined.to_string_lossy().into_owned(),
            ));
        }
        if env::var_os("ROCM_PATH").is_none() && Path::new("/opt/rocm").is_dir() {
            out.push(("ROCM_PATH".to_owned(), "/opt/rocm".to_owned()));
        }
        if env::var_os("HIP_PATH").is_none() {
            if let Ok(rocm) = env::var("ROCM_PATH") {
                out.push(("HIP_PATH".to_owned(), rocm));
            } else if Path::new("/opt/rocm").is_dir() {
                out.push(("HIP_PATH".to_owned(), "/opt/rocm".to_owned()));
            }
        }
        // MIOpen defaults (only if unset so operators can override):
        // FAST (2): FindDb hit or immediate fallback — avoids exhaustive
        // GemmFwdRest evaluation with workspace ptr=0 that stalls OCR for
        // tens of seconds until the unload grace kills the worker.
        // See ROCm/TheRock#3077, rocm-libraries#4071.
        if env::var_os("MIOPEN_FIND_MODE").is_none() {
            out.push(("MIOPEN_FIND_MODE".to_owned(), "FAST".to_owned()));
        }
        if let Some(cache) = miopen_cache_dir() {
            if env::var_os("MIOPEN_USER_DB_PATH").is_none() {
                out.push((
                    "MIOPEN_USER_DB_PATH".to_owned(),
                    cache.join("db").to_string_lossy().into_owned(),
                ));
            }
            if env::var_os("MIOPEN_CUSTOM_CACHE_DIR").is_none() {
                out.push((
                    "MIOPEN_CUSTOM_CACHE_DIR".to_owned(),
                    cache.join("cache").to_string_lossy().into_owned(),
                ));
            }
        }
        out
    }
}

/// Writable MIOpen FindDb/kernel cache root (`$XDG_CACHE_HOME/panoptikon/miopen`
/// or `~/.cache/panoptikon/miopen`). Best-effort create; `None` if HOME/XDG
/// unavailable.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn miopen_cache_dir() -> Option<PathBuf> {
    let root = env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .or_else(|| env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))?;
    let dir = root.join("panoptikon").join("miopen");
    std::fs::create_dir_all(dir.join("db")).ok()?;
    std::fs::create_dir_all(dir.join("cache")).ok()?;
    Some(dir)
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn merge_ld_library_path(prepend: &[PathBuf]) -> Option<OsString> {
    if prepend.is_empty() {
        return None;
    }
    let mut entries: Vec<PathBuf> = prepend.to_vec();
    if let Some(existing) = env::var_os("LD_LIBRARY_PATH") {
        entries.extend(env::split_paths(&existing));
    }
    env::join_paths(entries).ok()
}

// Exit 0: ok or no GPU. Non-zero: GPU present but HIP kernel fails.
const ROCM_TORCH_PROBE: &str = r#"
import sys
import torch

ver = getattr(torch, "__version__", "")
print(f"torch {ver}")
print(f"hip {getattr(torch.version, 'hip', None)}")
if ".lw." in ver:
    print("note: AMD .lw wheels often lack consumer GPU code objects", file=sys.stderr)
if not torch.cuda.is_available():
    print("no HIP GPU visible (ok on headless hosts)")
    raise SystemExit(0)
print(f"device0 {torch.cuda.get_device_name(0)} arch={torch.cuda.get_device_properties(0).gcnArchName}")
try:
    t = torch.zeros(8, device="cuda")
    float(t.sum())
except Exception as exc:
    print(f"GPU kernel launch failed: {exc}", file=sys.stderr)
    print("hint: use pytorch.org multi-arch rocm7.2 wheels + ROCm 7.2.x userspace", file=sys.stderr)
    raise SystemExit(2)
print("rocm_gpu_probe_ok")
"#;

/// Soft-ok if no GPU; Err if a trivial HIP kernel fails on a visible device.
async fn probe_rocm_torch(interpreter: &Path) -> anyhow::Result<()> {
    let output = tokio::process::Command::new(interpreter)
        .arg("-c")
        .arg(ROCM_TORCH_PROBE)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(hip_worker_env())
        .output()
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "failed to run ROCm torch probe with '{}': {err}",
                interpreter.display()
            )
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stdout.trim().is_empty() {
        tracing::info!(probe = %stdout.trim(), "ROCm torch probe");
    }
    let stderr_trim = stderr.trim();
    if !stderr_trim.is_empty()
        && !stderr_trim
            .lines()
            .all(|line| line.contains("(null): No such file or directory"))
    {
        tracing::warn!(probe = %stderr_trim, "ROCm torch probe stderr");
    }
    if output.status.success() {
        return Ok(());
    }
    let code = output.status.code().unwrap_or(-1);
    anyhow::bail!(
        "ROCm torch GPU probe failed (exit {code}). \
         stdout:\n{stdout}\nstderr:\n{stderr}\n\
         Use pytorch.org multi-arch rocm7.2 wheels and ROCm 7.2.x userspace."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn worker_env_only_for_resolved_rocm() {
        assert!(worker_env(Accelerator::Cuda).is_empty());
        // Unresolved auto must not inject; callers resolve first.
        assert!(worker_env(Accelerator::Auto).is_empty());
        // `cpu` carries the device marker and nothing else — no HIP paths,
        // no MPS watermarks.
        assert_eq!(
            worker_env(Accelerator::Cpu),
            vec![("INFERIO_DEVICE".to_string(), "cpu".to_string())]
        );
        // Rocm may be empty of HIP libs on hosts without ROCm, but on Linux
        // still carries MIOpen defaults when those env vars are unset. Off
        // Linux the whole HIP env is empty by design.
        let rocm = worker_env(Accelerator::Rocm);
        #[cfg(target_os = "linux")]
        if env::var_os("MIOPEN_FIND_MODE").is_none() {
            assert!(
                rocm.iter()
                    .any(|(k, v)| k == "MIOPEN_FIND_MODE" && v == "FAST"),
                "expected MIOPEN_FIND_MODE=FAST in {rocm:?}"
            );
        }
        #[cfg(not(target_os = "linux"))]
        assert!(rocm.is_empty(), "non-Linux HIP env must be empty: {rocm:?}");
    }

    /// An MPS worker gets the allocator watermarks that make torch raise at
    /// the recommended-max boundary instead of running on into macOS's
    /// compression/swap regime, where nothing raises at all — **both** of
    /// them, because torch asserts `high >= low` at allocator init and an
    /// ambient low above 1.0 would otherwise fail every worker at startup.
    #[test]
    fn an_mps_worker_gets_the_allocator_watermarks() {
        assert_eq!(
            worker_env(Accelerator::Mps),
            vec![
                (
                    "PYTORCH_MPS_HIGH_WATERMARK_RATIO".to_string(),
                    "1.0".to_string()
                ),
                (
                    "PYTORCH_MPS_LOW_WATERMARK_RATIO".to_string(),
                    "1.0".to_string()
                )
            ]
        );
        for accelerator in [Accelerator::Cpu, Accelerator::Cuda, Accelerator::Auto] {
            assert!(
                !worker_env(accelerator)
                    .iter()
                    .any(|(key, _)| key.starts_with("PYTORCH_MPS")),
                "{accelerator:?} must not carry MPS tuning"
            );
        }
    }

    /// Device coherence (docs/unified-memory-admission.md, backend C): a host
    /// priced against system RAM must run its impls on the CPU, and no other
    /// host may be told to. The `mps` case is the one that would otherwise
    /// bite — an `accelerator = "cpu"` Mac is priced as a CPU device (DP-3)
    /// and would run on Metal without this, while an `mps` Mac must not be
    /// forced off it.
    #[test]
    fn only_a_cpu_host_pins_the_workers_device() {
        assert_eq!(
            worker_env(Accelerator::Cpu)
                .iter()
                .find(|(key, _)| key == DEVICE_ENV_VAR)
                .map(|(_, value)| value.as_str()),
            Some("cpu")
        );
        for accelerator in [
            Accelerator::Cuda,
            Accelerator::Rocm,
            Accelerator::Mps,
            Accelerator::Auto,
        ] {
            assert!(
                !worker_env(accelerator)
                    .iter()
                    .any(|(key, _)| key == DEVICE_ENV_VAR),
                "{accelerator:?} must not pin the worker's device"
            );
        }
    }

    #[test]
    fn select_existing_keeps_hip_and_driver_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let hip = tmp.path().join("hip/lib");
        fs::create_dir_all(&hip).unwrap();
        fs::write(hip.join("libamdhip64.so.7"), b"").unwrap();

        let empty = tmp.path().join("empty/lib");
        fs::create_dir_all(&empty).unwrap();

        let driver = tmp.path().join("run/opengl-driver/lib");
        fs::create_dir_all(&driver).unwrap();

        let selected = select_existing_hip_lib_dirs(&[
            hip.clone(),
            empty,
            driver.clone(),
            tmp.path().join("missing/lib"),
            hip.clone(),
        ]);
        assert_eq!(selected, vec![hip, driver]);
    }

    #[test]
    fn merge_ld_library_path_prepends() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a");
        let b = tmp.path().join("b");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        let prev = env::var_os("LD_LIBRARY_PATH");
        // SAFETY: test-only; restored below.
        unsafe {
            env::set_var("LD_LIBRARY_PATH", &b);
        }
        let joined = merge_ld_library_path(std::slice::from_ref(&a)).expect("join");
        let parts: Vec<_> = env::split_paths(&joined).collect();
        assert_eq!(parts.first().map(Path::new), Some(a.as_path()));
        assert!(parts.iter().any(|p| p == &b));
        unsafe {
            match prev {
                Some(v) => env::set_var("LD_LIBRARY_PATH", v),
                None => env::remove_var("LD_LIBRARY_PATH"),
            }
        }
    }
}
