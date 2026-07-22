//! Host HIP/HSA discovery for Linux workers and setup probes.
//! Workers use [`worker_env`]; setup `--accelerator rocm` runs [`probe_rocm_torch`].

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

/// Existing HIP/HSA (or NixOS opengl-driver) lib dirs, discovery order.
pub fn rocm_library_dirs() -> Vec<PathBuf> {
    #[cfg(not(target_os = "linux"))]
    {
        return Vec::new();
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
        select_existing_rocm_lib_dirs(&candidates)
    }
}

/// Keep dirs that exist and look like HIP/HSA or the NixOS driver tree.
pub fn select_existing_rocm_lib_dirs(candidates: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in candidates {
        if !dir.is_dir() || !is_rocm_related_lib_dir(dir) {
            continue;
        }
        if !out.iter().any(|seen| seen == dir) {
            out.push(dir.clone());
        }
    }
    out
}

fn is_rocm_related_lib_dir(dir: &Path) -> bool {
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
/// `/opt/rocm` when unset. Empty on non-Linux.
pub fn worker_env() -> Vec<(String, String)> {
    #[cfg(not(target_os = "linux"))]
    {
        return Vec::new();
    }
    #[cfg(target_os = "linux")]
    {
        let mut out = Vec::new();
        if let Some(joined) = merge_ld_library_path(&rocm_library_dirs()) {
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
        out
    }
}

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
pub async fn probe_rocm_torch(interpreter: &Path) -> anyhow::Result<()> {
    let output = tokio::process::Command::new(interpreter)
        .arg("-c")
        .arg(ROCM_TORCH_PROBE)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(worker_env())
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
    fn select_existing_keeps_hip_and_driver_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let hip = tmp.path().join("hip/lib");
        fs::create_dir_all(&hip).unwrap();
        fs::write(hip.join("libamdhip64.so.7"), b"").unwrap();

        let empty = tmp.path().join("empty/lib");
        fs::create_dir_all(&empty).unwrap();

        let driver = tmp.path().join("run/opengl-driver/lib");
        fs::create_dir_all(&driver).unwrap();

        let selected = select_existing_rocm_lib_dirs(&[
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
        let joined = merge_ld_library_path(&[a.clone()]).expect("join");
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
