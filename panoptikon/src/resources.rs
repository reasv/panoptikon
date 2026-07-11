//! Resource resolution and embedded-release materialization
//! (docs/architecture.md "Self-contained releases").
//!
//! Two layouts exist for the Python source set (worker package, impl
//! classes, built-in registry TOMLs, pyproject/uv.lock):
//!
//! - **Dev**: the `python/` source tree next to the binary's working
//!   directory — the only layout plain (non-`bundled`) builds know.
//! - **Extracted**: `bundled` builds embed the set and materialize it to
//!   `runtime/pysrc/<crate version>/` on first run when the dev tree is
//!   absent. The managed venv then lives at `runtime/venv` (outside the
//!   version-keyed dir, so a version bump re-extracts sources but keeps the
//!   venv; the setup sentinel's uv.lock hash triggers a re-sync when the
//!   extracted lock changes).
//!
//! Resolution order everywhere: explicit config beats both; the dev tree
//! beats the extracted set when both exist ([`py_source_mode`]). All paths
//! are CWD-relative like the rest of the config; `--root` re-anchors the
//! CWD itself at startup.
//!
//! Extraction is atomic: the archive is unpacked into a temp sibling, a
//! marker file recording the archive hash is written, and the directory is
//! renamed into place — a corrupted or interrupted extraction never carries
//! the marker, so it is redone on the next start.

use std::path::{Path, PathBuf};

#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
use anyhow::{Context as _, Result};

// A `bundled-ui` build embeds a UI bundle produced at build time; build.rs
// sets `cfg(ui_bundle_present)` only after validating the env var.
#[cfg(all(feature = "bundled-ui", not(ui_bundle_present)))]
compile_error!(
    "feature `bundled-ui` requires the PANOPTIKON_UI_BUNDLE environment variable to point \
     at a fully assembled Next.js standalone output directory (containing server.js, with \
     .next/static copied in) at build time"
);

/// The crate version keying the extracted resource directories
/// (`runtime/pysrc/<version>`, `runtime/ui/<version>`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Marker file written inside a completed extraction, recording the SHA-256
/// of the embedded archive it came from. Absent or mismatched (partial
/// extraction, changed embedded content) means the extraction is redone.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
const EXTRACT_MARKER: &str = ".panoptikon-extracted";

/// Where the Python source set comes from (see module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PySourceMode {
    /// The `python/` dev source tree (always the answer in plain builds).
    Dev,
    /// The embedded set, extracted to `runtime/pysrc/<version>` (`bundled`
    /// builds running outside a source checkout).
    Extracted,
}

/// The active mode: the dev tree wins whenever `python/inferio_worker`
/// exists; plain builds never use the extracted layout.
pub fn py_source_mode() -> PySourceMode {
    if cfg!(feature = "bundled") && !Path::new("python/inferio_worker").is_dir() {
        PySourceMode::Extracted
    } else {
        PySourceMode::Dev
    }
}

/// The extracted Python source set directory (version-keyed, binary-owned).
pub fn extracted_pysrc_dir() -> PathBuf {
    Path::new("runtime/pysrc").join(VERSION)
}

/// The Python project directory: contains `pyproject.toml` + `uv.lock` and
/// the `inferio_worker` / `inferio` packages; `uv` runs here and workers get
/// it on their PYTHONPATH.
pub fn python_project_dir(mode: PySourceMode) -> PathBuf {
    match mode {
        PySourceMode::Dev => PathBuf::from("python"),
        PySourceMode::Extracted => extracted_pysrc_dir(),
    }
}

/// The managed venv `panoptikon setup` creates and syncs. In extracted mode
/// it lives *outside* the version-keyed dir on purpose (module docs).
pub fn managed_venv_dir(mode: PySourceMode) -> PathBuf {
    match mode {
        PySourceMode::Dev => PathBuf::from("python/.venv"),
        PySourceMode::Extracted => PathBuf::from("runtime/venv"),
    }
}

/// The interpreter path inside a venv, relative to the venv root.
pub fn venv_python_relpath() -> &'static Path {
    Path::new(if cfg!(windows) {
        "Scripts/python.exe"
    } else {
        "bin/python"
    })
}

/// Default worker interpreter when `[inference_local].python` is not set:
/// the managed venv for the mode. Dev mode keeps the legacy fallback — a
/// pre-restructure root `.venv` is used when the managed venv is absent.
pub fn default_worker_python(mode: PySourceMode) -> PathBuf {
    let managed = managed_venv_dir(mode).join(venv_python_relpath());
    match mode {
        PySourceMode::Extracted => managed,
        PySourceMode::Dev => {
            if managed.is_file() {
                managed
            } else {
                let legacy = Path::new(".venv").join(venv_python_relpath());
                if legacy.is_file() { legacy } else { managed }
            }
        }
    }
}

/// Default impl-class search dirs when `[inference_local].impl_dirs` is
/// empty: the mode's built-in impl dir, then the user's `inferio_custom/`.
pub fn default_impl_dirs(mode: PySourceMode) -> Vec<PathBuf> {
    vec![
        python_project_dir(mode).join("inferio/impl"),
        PathBuf::from("inferio_custom"),
    ]
}

/// Default worker PYTHONPATH prepends when `[inference_local].pythonpath`
/// is empty: the project dir, so `inferio_worker` / `inferio` resolve.
pub fn default_pythonpath(mode: PySourceMode) -> Vec<PathBuf> {
    vec![python_project_dir(mode)]
}

/// The built-in inference registry TOML directory for the mode (the user
/// dir, `config/inference`, is mode-independent).
pub fn builtin_registry_dir(mode: PySourceMode) -> PathBuf {
    python_project_dir(mode).join("inferio/config")
}

/// First-run materialization for `bundled` builds, called by main before
/// `Settings::load` (the dumped config must exist to be loaded, and the
/// extracted uv.lock is what the setup sentinel is judged against):
///
/// - When the gateway config is not explicitly located (no `--config` /
///   `GATEWAY_CONFIG_PATH`), the embedded default configs are written to
///   their default paths — each file only if absent, never overwriting.
/// - When the dev Python tree is absent ([`py_source_mode`] =
///   [`PySourceMode::Extracted`]), the embedded Python source set is
///   extracted to `runtime/pysrc/<version>` (atomic, marker-verified).
///
/// Messages describing what happened are printed to stderr immediately
/// (logging is not up yet) and returned so main can also log them properly.
/// Plain builds: a no-op returning no messages.
pub fn materialize_first_run(explicit_config: bool) -> anyhow::Result<Vec<String>> {
    #[cfg(not(feature = "bundled"))]
    {
        let _ = explicit_config;
        Ok(Vec::new())
    }
    #[cfg(feature = "bundled")]
    {
        let mut messages = Vec::new();
        if !explicit_config {
            messages.extend(write_default_configs_in(Path::new("."))?);
        }
        if py_source_mode() == PySourceMode::Extracted {
            let dest = std::path::absolute(extracted_pysrc_dir())
                .context("failed to resolve the extracted Python source set path")?;
            if ensure_extracted_archive(embedded::PYSRC_TAR_GZ, &dest, "Python source set")? {
                messages.push(format!(
                    "first run: extracted the embedded Python source set to '{}'",
                    dest.display()
                ));
            }
        }
        for message in &messages {
            eprintln!("{message}");
        }
        Ok(messages)
    }
}

/// The embedded resources of a `bundled` build.
#[cfg(feature = "bundled")]
mod embedded {
    /// The Python source set staged by build.rs (see build.rs for the
    /// exact contents).
    pub static PYSRC_TAR_GZ: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/pysrc.tar.gz"));
    /// The default gateway config, written to `config/gateway/default.toml`
    /// on first run when absent.
    pub static GATEWAY_DEFAULT_TOML: &str =
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../config/gateway/default.toml"));
    /// The example user inference registry, written to
    /// `config/inference/example.toml` on first run when absent.
    pub static INFERENCE_EXAMPLE_TOML: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../config/inference/example.toml"
    ));
}

/// Write the embedded default configs under `base` — each file only when it
/// does not exist yet (`create_new`: an existing user file is never touched,
/// even in a race). Returns one message per file written.
#[cfg(feature = "bundled")]
fn write_default_configs_in(base: &Path) -> Result<Vec<String>> {
    let mut messages = Vec::new();
    for (rel, content, what) in [
        (
            "config/gateway/default.toml",
            embedded::GATEWAY_DEFAULT_TOML,
            "default gateway config",
        ),
        (
            "config/inference/example.toml",
            embedded::INFERENCE_EXAMPLE_TOML,
            "example inference registry",
        ),
    ] {
        let path = base.join(rel);
        if let Some(written) = write_if_absent(&path, content)? {
            messages.push(format!(
                "first run: wrote the {what} to '{}' (edit it and restart to reconfigure; it \
                 will never be overwritten)",
                written.display()
            ));
        }
    }
    Ok(messages)
}

/// Create-new write: `Ok(Some(path))` when the file was created, `Ok(None)`
/// when it already existed (including losing a creation race).
#[cfg(feature = "bundled")]
fn write_if_absent(path: &Path, content: &str) -> Result<Option<PathBuf>> {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create '{}'", parent.display()))?;
    }
    let mut file = match std::fs::File::options().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to create '{}'", path.display()));
        }
    };
    file.write_all(content.as_bytes())
        .with_context(|| format!("failed to write '{}'", path.display()))?;
    Ok(Some(path.to_path_buf()))
}

/// SHA-256 (hex) of an embedded archive, recorded in the extraction marker.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
fn archive_hash(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Whether `dest` holds a completed extraction of the archive with `hash`.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
fn marker_matches(dest: &Path, hash: &str) -> bool {
    let Ok(content) = std::fs::read_to_string(dest.join(EXTRACT_MARKER)) else {
        return false;
    };
    content
        .lines()
        .any(|line| line.trim().strip_prefix("archive_sha256=") == Some(hash))
}

/// Extract an embedded tar.gz to `dest` unless a completed extraction of
/// this exact archive is already there. Atomic: unpack into a temp sibling,
/// write the marker, rename into place — so `dest` either carries a valid
/// marker or is fair game to be replaced (stale version content, partial
/// state from a meddled-with dir). Returns `true` when a fresh extraction
/// happened.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
pub(crate) fn ensure_extracted_archive(
    archive_gz: &[u8],
    dest: &Path,
    what: &str,
) -> Result<bool> {
    let hash = archive_hash(archive_gz);
    if marker_matches(dest, &hash) {
        return Ok(false);
    }
    let parent = dest
        .parent()
        .with_context(|| format!("extraction destination '{}' has no parent", dest.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create '{}'", parent.display()))?;
    let name = dest
        .file_name()
        .with_context(|| format!("extraction destination '{}' has no name", dest.display()))?
        .to_string_lossy()
        .into_owned();
    // Pid-suffixed temp dir: concurrent processes each unpack their own and
    // race only on the final rename, which the marker check below settles.
    let temp = parent.join(format!(".tmp-{name}-{}", std::process::id()));
    if temp.exists() {
        std::fs::remove_dir_all(&temp)
            .with_context(|| format!("failed to clear stale '{}'", temp.display()))?;
    }
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(archive_gz));
    archive
        .unpack(&temp)
        .with_context(|| format!("failed to extract the embedded {what} to '{}'", temp.display()))?;
    std::fs::write(
        temp.join(EXTRACT_MARKER),
        format!("archive_sha256={hash}\n"),
    )
    .with_context(|| format!("failed to write the extraction marker in '{}'", temp.display()))?;
    let committed = commit_extraction(&temp, dest, &hash, what)?;
    if committed {
        sweep_stale_temp_dirs(parent, STALE_TEMP_MAX_AGE);
    }
    Ok(committed)
}

/// Move a fully populated temp extraction into place. The initial marker
/// check happened before the (slow) unpack, so `dest` must be RE-checked
/// here: a concurrent process can have completed the same extraction in
/// the meantime, and a now-valid marker means its byte-identical work is
/// already committed — possibly in use — and must not be deleted. Only a
/// dest without a valid marker (stale version content, partial state from
/// a meddled-with dir) is replaced. Returns `true` when OUR extraction was
/// the one committed.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
fn commit_extraction(temp: &Path, dest: &Path, hash: &str, what: &str) -> Result<bool> {
    if dest.exists() {
        if marker_matches(dest, hash) {
            // Concurrent winner: identical content is already in place.
            let _ = std::fs::remove_dir_all(temp);
            return Ok(false);
        }
        std::fs::remove_dir_all(dest).with_context(|| {
            format!(
                "failed to remove the stale extracted {what} at '{}'",
                dest.display()
            )
        })?;
    }
    match std::fs::rename(temp, dest) {
        Ok(()) => Ok(true),
        Err(err) => {
            // A concurrent extraction can still win the rename race itself;
            // its content is byte-identical if its marker matches.
            let concurrent_won = marker_matches(dest, hash);
            let _ = std::fs::remove_dir_all(temp);
            if concurrent_won {
                Ok(false)
            } else {
                Err(err).with_context(|| {
                    format!(
                        "failed to move the extracted {what} into place at '{}'",
                        dest.display()
                    )
                })
            }
        }
    }
}

/// How old an orphaned `.tmp-*` extraction dir must be before the sweeper
/// removes it: young ones may belong to a concurrent extraction that is
/// still running.
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
const STALE_TEMP_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

/// Best-effort cleanup of `.tmp-*` extraction leftovers from crashed runs:
/// siblings of the destination with the temp prefix, not owned by this
/// process, and older than `max_age`. Nothing else is ever touched — in
/// particular other version dirs, which may still serve an older binary
/// that is running right now. Failures are ignored (another process may be
/// sweeping the same dir).
#[cfg(any(feature = "bundled", feature = "bundled-ui", test))]
fn sweep_stale_temp_dirs(parent: &Path, max_age: std::time::Duration) {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let own_suffix = format!("-{}", std::process::id());
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(".tmp-") || name.ends_with(&own_suffix) {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_dir() {
            continue;
        }
        let stale = meta
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age >= max_age);
        if stale {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// The embedded UI bundle of a `bundled-ui` build (build.rs validated the
/// `PANOPTIKON_UI_BUNDLE` directory and set `cfg(ui_bundle_present)`).
#[cfg(all(feature = "bundled-ui", ui_bundle_present))]
static UI_TAR_GZ: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/ui_bundle.tar.gz"));

/// Materialize the embedded UI bundle at `runtime/ui/<version>` (no-op when
/// already extracted) and return its absolute path. Called by the UI
/// supervisor when `[upstreams.ui] local = true` and the configured checkout
/// dir does not exist.
#[cfg(all(feature = "bundled-ui", ui_bundle_present))]
pub(crate) fn ensure_ui_bundle_extracted() -> Result<PathBuf> {
    let dest = std::path::absolute(Path::new("runtime/ui").join(VERSION))
        .context("failed to resolve the extracted UI bundle path")?;
    if ensure_extracted_archive(UI_TAR_GZ, &dest, "UI bundle")? {
        tracing::info!(dir = %dest.display(), "extracted the embedded UI bundle");
    }
    Ok(dest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The per-mode path table: dev mode is the source tree the plain build
    /// has always used; extracted mode is version-keyed under runtime/ with
    /// the venv outside the version dir (kept across version bumps).
    #[test]
    fn per_mode_path_table() {
        use PySourceMode::*;
        assert_eq!(python_project_dir(Dev), Path::new("python"));
        assert_eq!(
            python_project_dir(Extracted),
            Path::new("runtime/pysrc").join(VERSION)
        );
        assert_eq!(managed_venv_dir(Dev), Path::new("python/.venv"));
        assert_eq!(managed_venv_dir(Extracted), Path::new("runtime/venv"));
        assert_eq!(
            default_impl_dirs(Dev),
            [
                PathBuf::from("python/inferio/impl"),
                PathBuf::from("inferio_custom")
            ]
        );
        assert_eq!(
            default_impl_dirs(Extracted),
            [
                Path::new("runtime/pysrc").join(VERSION).join("inferio/impl"),
                PathBuf::from("inferio_custom")
            ]
        );
        assert_eq!(default_pythonpath(Dev), [PathBuf::from("python")]);
        assert_eq!(
            default_pythonpath(Extracted),
            [Path::new("runtime/pysrc").join(VERSION)]
        );
        assert_eq!(
            builtin_registry_dir(Dev),
            Path::new("python/inferio/config")
        );
        assert_eq!(
            builtin_registry_dir(Extracted),
            Path::new("runtime/pysrc").join(VERSION).join("inferio/config")
        );
        // Extracted mode never falls back to a legacy root .venv: the
        // interpreter is always the runtime/venv one.
        assert_eq!(
            default_worker_python(Extracted),
            Path::new("runtime/venv").join(venv_python_relpath())
        );
    }

    /// In a plain (non-bundled) build the mode is Dev unconditionally —
    /// resolution is byte-identical to the pre-bundling behavior.
    #[cfg(not(feature = "bundled"))]
    #[test]
    fn plain_builds_always_resolve_dev() {
        assert_eq!(py_source_mode(), PySourceMode::Dev);
    }

    /// Build a small tar.gz archive in memory for extraction tests.
    fn tar_gz(files: &[(&str, &str)]) -> Vec<u8> {
        let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut tar = tar::Builder::new(gz);
        for (name, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            tar.append_data(&mut header, name, content.as_bytes())
                .unwrap();
        }
        tar.into_inner().unwrap().finish().unwrap()
    }

    /// Extraction happy path + idempotence: a fresh extract unpacks the
    /// files and writes the marker; a second call with the same archive is
    /// a no-op that leaves user-visible state alone.
    #[test]
    fn extraction_extracts_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let dest = root.path().join("pysrc").join("1.0.0");
        let archive = tar_gz(&[("pyproject.toml", "x = 1"), ("inferio/__init__.py", "")]);

        assert!(ensure_extracted_archive(&archive, &dest, "test set").unwrap());
        assert_eq!(
            std::fs::read_to_string(dest.join("pyproject.toml")).unwrap(),
            "x = 1"
        );
        assert!(dest.join("inferio/__init__.py").is_file());
        let marker = std::fs::read_to_string(dest.join(EXTRACT_MARKER)).unwrap();
        assert!(marker.contains(&archive_hash(&archive)), "{marker}");

        // Second run: valid marker, nothing re-extracted.
        assert!(!ensure_extracted_archive(&archive, &dest, "test set").unwrap());
        // No temp leftovers either way.
        let names: Vec<_> = std::fs::read_dir(root.path().join("pysrc"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["1.0.0"], "no temp dirs left behind");
    }

    /// A dest without a valid marker — partial extraction, corrupted marker,
    /// or content from a different archive — is replaced wholesale.
    #[test]
    fn extraction_redoes_partial_or_stale_dests() {
        let root = tempfile::tempdir().unwrap();
        let dest = root.path().join("set");
        let archive = tar_gz(&[("a.txt", "new")]);

        // Partial state: files but no marker (e.g. a meddled-with dir).
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("stale.txt"), "old").unwrap();
        assert!(ensure_extracted_archive(&archive, &dest, "test set").unwrap());
        assert!(!dest.join("stale.txt").exists(), "stale content replaced");
        assert_eq!(std::fs::read_to_string(dest.join("a.txt")).unwrap(), "new");

        // Marker for a *different* archive: also redone.
        let changed = tar_gz(&[("a.txt", "changed")]);
        assert!(ensure_extracted_archive(&changed, &dest, "test set").unwrap());
        assert_eq!(
            std::fs::read_to_string(dest.join("a.txt")).unwrap(),
            "changed"
        );

        // Corrupted marker: redone too.
        std::fs::write(dest.join(EXTRACT_MARKER), "garbage").unwrap();
        assert!(ensure_extracted_archive(&changed, &dest, "test set").unwrap());
        assert!(marker_matches(&dest, &archive_hash(&changed)));
    }

    /// Concurrent completion: when another process finishes the SAME
    /// extraction between our initial marker check and our commit, the
    /// now-valid dest must be left alone (it may already be in use) and our
    /// temp dir discarded — never delete-and-replace a valid extraction.
    #[test]
    fn commit_leaves_a_concurrently_completed_extraction_alone() {
        let root = tempfile::tempdir().unwrap();
        let archive = tar_gz(&[("a.txt", "content")]);
        let hash = archive_hash(&archive);

        // The concurrent winner's completed extraction at dest...
        let dest = root.path().join("set");
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("a.txt"), "content").unwrap();
        std::fs::write(dest.join("winner-scratch.bin"), "in use").unwrap();
        std::fs::write(
            dest.join(EXTRACT_MARKER),
            format!("archive_sha256={hash}\n"),
        )
        .unwrap();
        // ...and our own fully populated temp dir, about to commit.
        let temp = root.path().join(".tmp-set-123");
        std::fs::create_dir_all(&temp).unwrap();
        std::fs::write(temp.join("a.txt"), "content").unwrap();
        std::fs::write(
            temp.join(EXTRACT_MARKER),
            format!("archive_sha256={hash}\n"),
        )
        .unwrap();

        let committed = commit_extraction(&temp, &dest, &hash, "test set").unwrap();
        assert!(!committed, "the concurrent winner's extraction stands");
        assert!(
            dest.join("winner-scratch.bin").is_file(),
            "the in-use extraction was not replaced"
        );
        assert!(!temp.exists(), "our redundant temp dir is discarded");

        // Whereas a dest with a NON-matching marker is still replaced.
        let temp = root.path().join(".tmp-set-456");
        std::fs::create_dir_all(&temp).unwrap();
        std::fs::write(temp.join("a.txt"), "content").unwrap();
        std::fs::write(
            temp.join(EXTRACT_MARKER),
            format!("archive_sha256={hash}\n"),
        )
        .unwrap();
        std::fs::write(dest.join(EXTRACT_MARKER), "archive_sha256=other\n").unwrap();
        let committed = commit_extraction(&temp, &dest, &hash, "test set").unwrap();
        assert!(committed, "a stale dest is replaced");
        assert!(!dest.join("winner-scratch.bin").exists());
        assert!(marker_matches(&dest, &hash));
    }

    /// The stale-temp sweeper removes only old `.tmp-*` orphans: never this
    /// process's own temp dir, never young temp dirs (a concurrent
    /// extraction in flight), and never anything without the temp prefix
    /// (version dirs may serve a still-running older binary).
    #[test]
    fn sweeper_removes_only_stale_foreign_temp_dirs() {
        let root = tempfile::tempdir().unwrap();
        let foreign_temp = root.path().join(".tmp-set-99999999");
        let own_temp = root
            .path()
            .join(format!(".tmp-set-{}", std::process::id()));
        let version_dir = root.path().join("1.0.0");
        for dir in [&foreign_temp, &own_temp, &version_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }

        // Every dir is younger than a huge threshold: nothing is removed.
        sweep_stale_temp_dirs(root.path(), std::time::Duration::from_secs(3600));
        assert!(foreign_temp.exists() && own_temp.exists() && version_dir.exists());

        // Zero threshold makes every dir "stale": only the foreign temp
        // goes; our own temp and the version dir are untouchable.
        sweep_stale_temp_dirs(root.path(), std::time::Duration::ZERO);
        assert!(!foreign_temp.exists(), "stale foreign temp removed");
        assert!(own_temp.exists(), "own temp dir never swept");
        assert!(version_dir.exists(), "version dirs never swept");
    }

    /// The config dump writes each default config only when absent and
    /// never overwrites — a user-edited file survives any number of runs.
    #[cfg(feature = "bundled")]
    #[test]
    fn config_dump_writes_once_and_never_overwrites() {
        let root = tempfile::tempdir().unwrap();

        let messages = write_default_configs_in(root.path()).unwrap();
        assert_eq!(messages.len(), 2, "{messages:?}");
        let gateway = root.path().join("config/gateway/default.toml");
        let example = root.path().join("config/inference/example.toml");
        assert_eq!(
            std::fs::read_to_string(&gateway).unwrap(),
            embedded::GATEWAY_DEFAULT_TOML
        );
        assert_eq!(
            std::fs::read_to_string(&example).unwrap(),
            embedded::INFERENCE_EXAMPLE_TOML
        );

        // User edits both; a re-run writes nothing and changes nothing.
        std::fs::write(&gateway, "# user edited").unwrap();
        std::fs::write(&example, "# user edited too").unwrap();
        let messages = write_default_configs_in(root.path()).unwrap();
        assert!(messages.is_empty(), "{messages:?}");
        assert_eq!(std::fs::read_to_string(&gateway).unwrap(), "# user edited");
        assert_eq!(
            std::fs::read_to_string(&example).unwrap(),
            "# user edited too"
        );

        // One file deleted: only that one is re-created.
        std::fs::remove_file(&example).unwrap();
        let messages = write_default_configs_in(root.path()).unwrap();
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert_eq!(std::fs::read_to_string(&gateway).unwrap(), "# user edited");
        assert_eq!(
            std::fs::read_to_string(&example).unwrap(),
            embedded::INFERENCE_EXAMPLE_TOML
        );
    }

    /// The embedded Python source set contains what the resolution paths
    /// expect — worker package, impl dir, built-in registry, project files —
    /// and none of what must never ship (tests, venv, bytecode caches).
    #[cfg(feature = "bundled")]
    #[test]
    fn embedded_pysrc_archive_contents() {
        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(embedded::PYSRC_TAR_GZ));
        let names: Vec<String> = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();
        for required in [
            "pyproject.toml",
            "uv.lock",
            "inferio_worker/__main__.py",
            "inferio/impl/utils.py",
            "inferio/config/inference.toml",
        ] {
            assert!(
                names.iter().any(|name| name == required),
                "missing '{required}' in embedded pysrc archive"
            );
        }
        for (what, banned) in [
            ("bytecode", ".pyc"),
            ("bytecode cache", "__pycache__"),
            ("venv", ".venv"),
        ] {
            assert!(
                !names.iter().any(|name| name.contains(banned)),
                "{what} ('{banned}') leaked into the embedded pysrc archive"
            );
        }
        assert!(
            !names.iter().any(|name| name.starts_with("tests/")),
            "python tests leaked into the embedded pysrc archive"
        );
    }
}
