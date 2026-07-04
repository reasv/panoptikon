//! Console + file logging setup.
//!
//! Port of `panoptikon.log.setup_logging`: `LOGLEVEL` sets the default level
//! and `LOGS_FILE` the log file path, defaulting to
//! `$DATA_FOLDER/panoptikon-gateway.log`. The file name is gateway-specific on
//! purpose — while the Python server still runs alongside the gateway it keeps
//! appending to `panoptikon.log`, and two processes interleaving one file
//! makes both unreadable. Setting `LOGS_FILE` to an empty string disables file
//! logging. `RUST_LOG` takes precedence over `LOGLEVEL` when set, so targeted
//! per-module directives keep working.

use std::env;
use std::fs;
use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn env_filter() -> EnvFilter {
    if env::var("RUST_LOG").is_ok_and(|value| !value.trim().is_empty()) {
        return EnvFilter::from_default_env();
    }
    let level = env::var("LOGLEVEL").unwrap_or_default();
    let level = level.trim().to_ascii_lowercase();
    if level.is_empty() {
        return EnvFilter::new("info");
    }
    EnvFilter::try_new(&level).unwrap_or_else(|err| {
        eprintln!("invalid LOGLEVEL {level:?} ({err}); falling back to info");
        EnvFilter::new("info")
    })
}

fn logs_file_path() -> Option<PathBuf> {
    match env::var("LOGS_FILE") {
        // Explicitly empty disables file logging entirely.
        Ok(value) if value.trim().is_empty() => None,
        Ok(value) => Some(PathBuf::from(value)),
        Err(_) => {
            let data_dir = env::var("DATA_FOLDER").unwrap_or_else(|_| "data".to_string());
            Some(PathBuf::from(data_dir).join("panoptikon-gateway.log"))
        }
    }
}

fn open_logs_file() -> Option<(PathBuf, fs::File)> {
    let path = logs_file_path()?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(err) = fs::create_dir_all(parent) {
                eprintln!(
                    "failed to create log directory {}: {err}; file logging disabled",
                    parent.display()
                );
                return None;
            }
        }
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
pub(crate) fn init() -> Option<WorkerGuard> {
    let registry = tracing_subscriber::registry()
        .with(env_filter())
        .with(tracing_subscriber::fmt::layer());

    match open_logs_file() {
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
