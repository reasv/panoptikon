//! Whether the web UI shows the NVIDIA sysmem fallback notice: on Windows,
//! when local inference reports an NVIDIA GPU, until the user dismisses it.
//! The notice text lives in `ui/components/DesktopUpdateRibbon.tsx`.

use crate::supervisor::Supervisor;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, Manager as _};

#[derive(Debug, Default, Serialize)]
pub struct SysmemFallbackNotice {
    pub visible: bool,
    /// The interpreter the inference workers run as, for an NVIDIA program
    /// setting; `None` when it cannot be read.
    pub worker_python: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Health {
    gpus: Vec<HealthGpu>,
}

#[derive(Debug, Deserialize)]
struct HealthGpu {
    /// Reported only for NVIDIA GPUs.
    compute_cap: Option<String>,
}

pub async fn status(app: &AppHandle) -> SysmemFallbackNotice {
    let supervisor = app.state::<Arc<Supervisor>>();
    let dismissed = supervisor
        .settings
        .lock()
        .await
        .typed
        .notices
        .sysmem_fallback_dismissed;
    // With remote inference, `/api/inference/health` describes the remote host.
    let health = if cfg!(windows)
        && !dismissed
        && crate::server_config::local_inference_enabled(
            &supervisor.paths.server_root,
            &supervisor.server_config_path(),
        )
        .unwrap_or(false)
    {
        fetch_health(supervisor.snapshot().await.port).await
    } else {
        None
    };
    if !shows(dismissed, health.as_ref()) {
        return SysmemFallbackNotice::default();
    }
    SysmemFallbackNotice {
        visible: true,
        worker_python: worker_python(&supervisor.paths.server_root.join("runtime/venv"))
            .map(|path| path.display().to_string()),
    }
}

pub async fn dismiss(app: &AppHandle) -> anyhow::Result<()> {
    let supervisor = app.state::<Arc<Supervisor>>();
    let mut document = supervisor.settings.lock().await;
    let previous = document.clone();
    document.typed.notices.sysmem_fallback_dismissed = true;
    if let Err(error) = document.save() {
        *document = previous;
        return Err(error);
    }
    Ok(())
}

async fn fetch_health(port: u16) -> Option<Health> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .ok()?;
    let response = client
        .get(crate::local_browser_url(port, "/api/inference/health"))
        .send()
        .await
        .ok()?;
    response.error_for_status().ok()?.json().await.ok()
}

/// Shown until dismissed once the Server reports an NVIDIA GPU. The driver
/// setting cannot be read, so it is never inferred.
fn shows(dismissed: bool, health: Option<&Health>) -> bool {
    !dismissed
        && health.is_some_and(|health| health.gpus.iter().any(|gpu| gpu.compute_cap.is_some()))
}

/// The base interpreter behind a Windows venv. The venv's own `python.exe`
/// only launches `<home>\python.exe` as a child process, and that child is
/// the one that uses the GPU.
fn worker_python(venv: &Path) -> Option<PathBuf> {
    let config = std::fs::read_to_string(venv.join("pyvenv.cfg")).ok()?;
    let home = config.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "home").then(|| value.trim())
    })?;
    // Resolves a version link (e.g. `cpython-3.12-…`) to the real directory.
    let path = std::fs::canonicalize(Path::new(home).join("python.exe")).ok()?;
    let text = path.to_str()?;
    Some(PathBuf::from(text.strip_prefix(r"\\?\").unwrap_or(text)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(caps: &[Option<&str>]) -> Health {
        Health {
            gpus: caps
                .iter()
                .map(|cap| HealthGpu {
                    compute_cap: cap.map(str::to_owned),
                })
                .collect(),
        }
    }

    #[test]
    fn shows_for_an_nvidia_gpu_until_dismissed() {
        let nvidia = health(&[None, Some("12.0")]);
        assert!(shows(false, Some(&nvidia)));
        assert!(!shows(true, Some(&nvidia)));
    }

    /// CPU, ROCm and MPS devices report no compute capability.
    #[test]
    fn hidden_without_an_nvidia_gpu_or_an_answer() {
        assert!(!shows(false, Some(&health(&[None]))));
        assert!(!shows(false, Some(&health(&[]))));
        assert!(!shows(false, None));
    }

    #[test]
    fn dismissal_survives_a_restart() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("desktop.toml");
        let mut document = crate::settings::SettingsDocument::defaults(path.clone()).unwrap();
        assert!(!document.typed.notices.sysmem_fallback_dismissed);
        document.typed.notices.sysmem_fallback_dismissed = true;
        document.save().unwrap();
        let restored = crate::settings::SettingsDocument::load_path(&path).unwrap();
        assert!(restored.typed.notices.sysmem_fallback_dismissed);
    }

    #[test]
    fn health_report_parses_only_the_gpu_fields() {
        let report: Health = serde_json::from_str(
            r#"{"status":"ok","gpus":[{"uuid":"CPU-0","compute_cap":null},
                {"uuid":"GPU-1","name":"NVIDIA GeForce RTX 5090","compute_cap":"12.0"}]}"#,
        )
        .unwrap();
        assert!(shows(false, Some(&report)));
    }

    #[test]
    fn worker_python_is_the_venv_home_interpreter() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("cpython-3.12.9");
        std::fs::create_dir(&home).unwrap();
        std::fs::write(home.join("python.exe"), b"").unwrap();
        let venv = temp.path().join("venv");
        std::fs::create_dir(&venv).unwrap();
        std::fs::write(
            venv.join("pyvenv.cfg"),
            format!("home = {}\nversion_info = 3.12.9\n", home.display()),
        )
        .unwrap();
        let expected = std::fs::canonicalize(home.join("python.exe")).unwrap();
        let expected = expected.to_str().unwrap();
        assert_eq!(
            worker_python(&venv),
            Some(PathBuf::from(
                expected.strip_prefix(r"\\?\").unwrap_or(expected)
            ))
        );
        assert_eq!(worker_python(&temp.path().join("missing")), None);
    }
}
