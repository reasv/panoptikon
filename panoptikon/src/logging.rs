//! Console + file logging setup, configured by the `[logging]` config
//! section: `level` sets the default level and `file` the log file path,
//! defaulting to `<data_folder>/panoptikon.log`. Setting `file` to an empty
//! string disables file logging. The `RUST_LOG` env var takes precedence over
//! `[logging].level` when set, so targeted per-module directives keep
//! working — it is deliberately NOT absorbed into the config file.

use std::env;
use std::fs;
use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::Settings;

fn env_filter(configured_level: &str) -> EnvFilter {
    if env::var("RUST_LOG").is_ok_and(|value| !value.trim().is_empty()) {
        return EnvFilter::from_default_env();
    }
    let level = configured_level.trim().to_ascii_lowercase();
    if level.is_empty() {
        return EnvFilter::new("info");
    }
    EnvFilter::try_new(&level).unwrap_or_else(|err| {
        eprintln!("invalid logging.level {level:?} ({err}); falling back to info");
        EnvFilter::new("info")
    })
}

/// The log file to append to, from `[logging].file`: absent means the
/// default under the data folder, an explicit empty (or blank) string
/// disables file logging entirely.
fn logs_file_path(settings: &Settings) -> Option<PathBuf> {
    match &settings.logging.file {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(PathBuf::from(value)),
        None => Some(settings.data_folder.join("panoptikon.log")),
    }
}

fn open_logs_file(settings: &Settings) -> Option<(PathBuf, fs::File)> {
    let path = logs_file_path(settings)?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(err) = fs::create_dir_all(parent)
    {
        eprintln!(
            "failed to create log directory {}: {err}; file logging disabled",
            parent.display()
        );
        return None;
    }
    match fs::OpenOptions::new().append(true).create(true).open(&path) {
        Ok(file) => Some((path, file)),
        Err(err) => {
            eprintln!(
                "failed to open log file {}: {err}; file logging disabled",
                path.display()
            );
            None
        }
    }
}

/// Initializes tracing with a console layer and, unless disabled, an appending
/// file layer. A failure to open the log file degrades to console-only rather
/// than refusing to start. The returned guard must stay alive for the process
/// lifetime; dropping it flushes buffered file output.
pub(crate) fn init(settings: &Settings) -> Option<WorkerGuard> {
    let registry = tracing_subscriber::registry()
        .with(env_filter(&settings.logging.level))
        .with(tracing_subscriber::fmt::layer());

    match open_logs_file(settings) {
        Some((path, file)) => {
            let (writer, guard) = tracing_appender::non_blocking(file);
            let file_layer = tracing_subscriber::fmt::layer()
                .with_writer(writer)
                .with_ansi(false);
            registry.with(file_layer).init();
            tracing::info!(path = %path.display(), "logging to file");
            Some(guard)
        }
        None => {
            registry.init();
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings_with(logging: crate::config::LoggingConfig, data_folder: &str) -> Settings {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gw.toml");
        std::fs::write(
            &path,
            r#"
[server]
host = "127.0.0.1"
port = 9155

[upstreams.ui]
base_url = "http://127.0.0.1:6339"

[upstreams.api]
base_url = "http://127.0.0.1:6342"
"#,
        )
        .unwrap();
        // Settings::load resolves ${VAR} templates from the environment:
        // hold the shared env lock so tests mutating env vars cannot
        // interleave.
        let settings = {
            let _env = crate::test_utils::env_lock();
            Settings::load(Some(path)).unwrap()
        };
        let mut settings = settings;
        settings.logging = logging;
        settings.data_folder = PathBuf::from(data_folder);
        settings
    }

    /// `[logging].file` resolution preserves the old LOGS_FILE semantics:
    /// absent -> `<data_folder>/panoptikon.log`, explicit path ->
    /// that path, empty/blank string -> file logging disabled.
    #[test]
    fn logs_file_path_semantics() {
        use crate::config::LoggingConfig;

        let settings = settings_with(
            LoggingConfig {
                file: None,
                level: "INFO".into(),
            },
            "d:/pan",
        );
        assert_eq!(
            logs_file_path(&settings),
            Some(PathBuf::from("d:/pan").join("panoptikon.log"))
        );

        let settings = settings_with(
            LoggingConfig {
                file: Some("logs/custom.log".into()),
                level: "INFO".into(),
            },
            "d:/pan",
        );
        assert_eq!(
            logs_file_path(&settings),
            Some(PathBuf::from("logs/custom.log"))
        );

        for disabled in ["", "   "] {
            let settings = settings_with(
                LoggingConfig {
                    file: Some(disabled.into()),
                    level: "INFO".into(),
                },
                "d:/pan",
            );
            assert_eq!(logs_file_path(&settings), None, "{disabled:?} disables");
        }
    }
}
