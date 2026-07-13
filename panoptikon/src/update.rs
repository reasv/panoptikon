//! Release updater: fetch the release updater manifest from GitHub and either
//! (startup path) log a single prominent notice when a newer version exists,
//! or (`panoptikon update` path) download the published build for this host,
//! verify its SHA-256, and swap it in for the running executable.
//!
//! The startup check is best-effort and non-annoying: it never blocks startup,
//! fails silently offline, and throttles the network to at most one manifest
//! fetch every [`CHECK_INTERVAL_SECS`] by caching the last result under the
//! data folder. Between fetches a still-relevant cached result is reused, so an
//! available update is shown on *every* startup while the network is hit only a
//! few times a day.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use futures_util::StreamExt as _;

/// GitHub permalink to the newest non-prerelease release's `latest.json`
/// asset (302-redirects to the versioned download; reqwest follows it).
const MANIFEST_URL: &str =
    "https://github.com/reasv/panoptikon/releases/latest/download/latest.json";
/// Human-facing releases page, used as the fallback "where to look" URL.
const RELEASES_URL: &str = "https://github.com/reasv/panoptikon/releases/latest";

/// Throttle the startup manifest fetch to at most once per this many seconds;
/// between fetches the cached result is reused. Eight hours ⇒ ≤3 GETs/day.
const CHECK_INTERVAL_SECS: u64 = 8 * 60 * 60;

/// Cache-file basename under `data_folder`.
const CACHE_FILENAME: &str = ".update-check.json";

/// Temp filename for the streamed binary download (sits next to the exe).
const DOWNLOAD_TEMP: &str = ".panoptikon-update-download";

#[derive(Debug, serde::Deserialize)]
struct UpdateManifest {
    version: String,
    // `pub_date` is carried by the published manifest but not consumed here.
    #[allow(dead_code)]
    pub_date: Option<String>,
    notes: Option<String>,
    platforms: HashMap<String, PlatformEntry>,
}

#[derive(Debug, serde::Deserialize)]
struct PlatformEntry {
    url: String,
    sha256: String,
}

/// Persisted result of the last startup check, so the network is only hit
/// every [`CHECK_INTERVAL_SECS`]. All I/O on this is best-effort.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct UpdateCache {
    last_checked_unix: u64,
    latest_version: String,
    notes: Option<String>,
}

/// Our release label for the current host, matching the manifest's platform
/// keys. `None` on hosts we don't publish binaries for.
fn current_platform_key() -> Option<&'static str> {
    platform_key_for(std::env::consts::OS, std::env::consts::ARCH)
}

/// The pure mapping behind [`current_platform_key`], split out so tests can
/// exercise every branch without depending on the build host.
fn platform_key_for(os: &str, arch: &str) -> Option<&'static str> {
    match (os, arch) {
        ("windows", "x86_64") => Some("windows-x86_64"),
        ("linux", "x86_64") => Some("linux-x86_64"),
        ("macos", "aarch64") => Some("macos-aarch64"),
        _ => None,
    }
}

/// Parse a `major.minor.patch` version, taking the leading ascii digits of
/// each part so suffixed patches like "4rc1" still parse (mirrors setup.rs's
/// `version_triple`).
fn parse_triple(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split('.');
    let major = leading_digits(parts.next()?)?;
    let minor = leading_digits(parts.next()?)?;
    let patch = leading_digits(parts.next()?)?;
    Some((major, minor, patch))
}

fn leading_digits(part: &str) -> Option<u64> {
    let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// True only when both versions parse and `remote` is strictly newer.
fn is_newer(remote: &str, current: &str) -> bool {
    match (parse_triple(remote), parse_triple(current)) {
        (Some(remote), Some(current)) => remote > current,
        _ => false,
    }
}

async fn fetch_manifest() -> anyhow::Result<UpdateManifest> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()?;
    let manifest = client
        .get(MANIFEST_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(manifest)
}

/// Current wall-clock time in unix seconds (0 if the clock predates the epoch).
fn current_unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Path of the on-disk update-check cache under the data folder.
fn cache_path() -> std::path::PathBuf {
    crate::config::runtime().data_folder.join(CACHE_FILENAME)
}

/// Read the cached check result. Best-effort: a missing/unreadable/malformed
/// file is treated as "no cache" so the caller fetches.
fn read_cache() -> Option<UpdateCache> {
    let bytes = std::fs::read(cache_path()).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Persist the cache. Best-effort: a write failure (e.g. a read-only data dir)
/// is swallowed — we simply won't throttle as tightly next time.
fn write_cache(cache: &UpdateCache) {
    if let Ok(bytes) = serde_json::to_vec(cache) {
        let _ = std::fs::write(cache_path(), bytes);
    }
}

/// Whether the network should be hit now: no cache, or the cached result is at
/// least `interval` seconds old. Pure so the throttle logic is unit-testable.
fn should_fetch(cache: Option<&UpdateCache>, now: u64, interval: u64) -> bool {
    match cache {
        None => true,
        Some(cache) => now.saturating_sub(cache.last_checked_unix) >= interval,
    }
}

/// Fire-and-forget the throttled startup update check. Never blocks startup,
/// never errors: offline runs fail silently, up-to-date runs log at debug, and
/// a genuinely newer release prints one prominent banner to stderr.
/// `current` is passed as `crate::resources::VERSION`.
pub fn spawn_startup_check(current: &'static str) {
    tokio::spawn(async move {
        let now = current_unix_time();
        let cached = read_cache();
        // (version, notes) of the newest release we know about, from a fresh
        // fetch when the throttle allows, else the cached result.
        let latest: Option<(String, Option<String>)> =
            if should_fetch(cached.as_ref(), now, CHECK_INTERVAL_SECS) {
                match fetch_manifest().await {
                    Ok(manifest) => {
                        write_cache(&UpdateCache {
                            last_checked_unix: now,
                            latest_version: manifest.version.clone(),
                            notes: manifest.notes.clone(),
                        });
                        Some((manifest.version, manifest.notes))
                    }
                    Err(err) => {
                        tracing::debug!(error = %err, "update check failed (offline?)");
                        // Fall back to a stale cached result if we have one.
                        cached.map(|c| (c.latest_version, c.notes))
                    }
                }
            } else {
                cached.map(|c| (c.latest_version, c.notes))
            };

        if let Some((new, notes)) = latest {
            if is_newer(&new, current) {
                print_update_banner(current, &new, notes.as_deref().unwrap_or(RELEASES_URL));
            } else {
                tracing::debug!(latest = %new, "no newer release available");
            }
        }
    });
}

/// Prominent multi-line startup banner (stderr, not tracing). Uses horizontal
/// rules with left-anchored text so it stays aligned regardless of the
/// version/URL lengths.
fn print_update_banner(current: &str, new: &str, url: &str) {
    eprintln!();
    eprintln!("  ════════════════════════════════════════════════════════════════");
    eprintln!("     ▲  P A N O P T I K O N   —   update available");
    eprintln!("  ────────────────────────────────────────────────────────────────");
    eprintln!("     You are running {current}.  Version {new} is available.");
    eprintln!();
    eprintln!("     ▸ Upgrade:  panoptikon update");
    eprintln!("     ▸ Docker:   pull the latest image");
    eprintln!("     ▸ Notes:    {url}");
    eprintln!("  ════════════════════════════════════════════════════════════════");
    eprintln!();
}

/// `panoptikon update`: fetch the manifest (ignoring the startup throttle),
/// and if a newer build is published for this host, download it, verify its
/// SHA-256, and swap it in for the running executable (backing the old one up).
///
/// User-facing: talks via `println!`/`eprintln!`, not tracing.
pub async fn run_update_command(current: &str, assume_yes: bool) -> anyhow::Result<()> {
    let manifest = fetch_manifest()
        .await
        .map_err(|e| anyhow::anyhow!("couldn't reach GitHub to check for updates: {e}"))?;

    if !is_newer(&manifest.version, current) {
        println!("Panoptikon {current} is already the latest version.");
        // Refresh the cache so the next startup check need not re-fetch.
        write_cache(&UpdateCache {
            last_checked_unix: current_unix_time(),
            latest_version: manifest.version.clone(),
            notes: manifest.notes.clone(),
        });
        return Ok(());
    }

    let notes_url = manifest.notes.as_deref().unwrap_or(RELEASES_URL);
    println!("Update available: {current} → {}", manifest.version);
    println!("Release notes: {notes_url}");

    let key = current_platform_key();
    let entry = match key.and_then(|key| manifest.platforms.get(key)) {
        Some(entry) => entry,
        None => anyhow::bail!(
            "no published build for {}/{}; update manually: {RELEASES_URL}",
            std::env::consts::OS,
            std::env::consts::ARCH,
        ),
    };

    if !assume_yes {
        print!("Update now? [y/N] ");
        std::io::stdout().flush().ok();
        let mut answer = String::new();
        std::io::stdin()
            .read_line(&mut answer)
            .context("failed to read confirmation from stdin")?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let current_exe = std::env::current_exe().context("failed to locate the running executable")?;
    let exe_dir = current_exe
        .parent()
        .context("running executable has no parent directory")?;

    // Stream the new binary to a temp file next to the exe, hashing as we go.
    let temp = exe_dir.join(DOWNLOAD_TEMP);
    println!("Downloading {}...", entry.url);
    let actual_sha256 = download_to_file(&entry.url, &temp)
        .await
        .with_context(|| format!("failed to download {}", entry.url))?;

    if !actual_sha256.eq_ignore_ascii_case(&entry.sha256) {
        std::fs::remove_file(&temp).ok();
        anyhow::bail!("checksum mismatch — download corrupted or tampered; aborted");
    }

    // Make the downloaded binary executable before swapping it in.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to set permissions on '{}'", temp.display()))?;
    }

    // Back up the current exe, then swap the new one in. Moving the *running*
    // executable aside is permitted on both Windows and Unix.
    let backups = exe_dir.join("backups");
    std::fs::create_dir_all(&backups).with_context(|| {
        format!(
            "failed to create backup directory '{}' — check that the install \
             directory is writable, or update manually",
            backups.display()
        )
    })?;
    let backup_path = backups.join(format!("panoptikon-{current}{}", std::env::consts::EXE_SUFFIX));
    // A leftover backup from a previous run would block the rename below.
    std::fs::remove_file(&backup_path).ok();

    std::fs::rename(&current_exe, &backup_path).with_context(|| {
        format!(
            "failed to move the current executable aside to '{}' — check that \
             the install directory is writable, or update manually",
            backup_path.display()
        )
    })?;
    if let Err(err) = std::fs::rename(&temp, &current_exe) {
        // Roll the backup back into place so we don't leave the install broken.
        std::fs::rename(&backup_path, &current_exe).ok();
        std::fs::remove_file(&temp).ok();
        return Err(err).with_context(|| {
            format!(
                "failed to install the new executable at '{}'; the previous \
                 version was restored",
                current_exe.display()
            )
        });
    }

    // Refresh the cache so the next startup check doesn't re-nag.
    write_cache(&UpdateCache {
        last_checked_unix: current_unix_time(),
        latest_version: manifest.version.clone(),
        notes: manifest.notes.clone(),
    });

    println!(
        "Updated {current} → {}. Restart Panoptikon to run the new version.",
        manifest.version
    );
    println!("Previous version backed up at {}", backup_path.display());
    Ok(())
}

/// Stream `url` into `dest`, returning the hex SHA-256 of the bytes written.
/// Uses a long timeout suited to a full binary (the 8s manifest timeout would
/// abort mid-download). Mirrors the streaming+hashing pattern in setup.rs.
async fn download_to_file(url: &str, dest: &Path) -> anyhow::Result<String> {
    use sha2::Digest as _;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()?;
    let response = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("download request for {url} failed"))?;

    let mut file = std::fs::File::create(dest)
        .with_context(|| format!("failed to create '{}'", dest.display()))?;
    let mut stream = response.bytes_stream();
    let mut hasher = sha2::Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("download of {url} failed mid-stream"))?;
        file.write_all(&chunk)
            .with_context(|| format!("failed to write '{}'", dest.display()))?;
        hasher.update(&chunk);
    }
    file.flush().ok();
    drop(file);
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_newer_detects_upgrades() {
        assert!(is_newer("0.1.4", "0.1.3"));
        assert!(is_newer("0.2.0", "0.1.9"));
        assert!(is_newer("1.0.0", "0.9.9"));
    }

    #[test]
    fn is_newer_rejects_equal_older_and_malformed() {
        assert!(!is_newer("0.1.3", "0.1.3"));
        assert!(!is_newer("0.1.2", "0.1.3"));
        assert!(!is_newer("garbage", "0.1.3"));
        assert!(!is_newer("0.1.4", "garbage"));
    }

    #[test]
    fn platform_key_maps_supported_hosts() {
        assert_eq!(platform_key_for("windows", "x86_64"), Some("windows-x86_64"));
        assert_eq!(platform_key_for("linux", "x86_64"), Some("linux-x86_64"));
        assert_eq!(platform_key_for("macos", "aarch64"), Some("macos-aarch64"));
        assert_eq!(platform_key_for("plan9", "sparc"), None);
    }

    #[test]
    fn manifest_deserializes() {
        let json = r#"{
            "version": "0.2.0",
            "pub_date": "2026-07-13T00:00:00Z",
            "notes": "https://example.com/notes",
            "platforms": {
                "windows-x86_64": {
                    "url": "https://example.com/panoptikon-windows-x86_64.zip",
                    "sha256": "abc123"
                }
            }
        }"#;
        let manifest: UpdateManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.version, "0.2.0");
        let entry = manifest.platforms.get("windows-x86_64").unwrap();
        assert_eq!(entry.url, "https://example.com/panoptikon-windows-x86_64.zip");
        assert_eq!(entry.sha256, "abc123");
    }

    #[test]
    fn should_fetch_with_no_cache_is_true() {
        assert!(should_fetch(None, 1_000_000, CHECK_INTERVAL_SECS));
    }

    #[test]
    fn should_fetch_respects_the_interval() {
        let cache = UpdateCache {
            last_checked_unix: 1_000_000,
            latest_version: "0.1.3".to_string(),
            notes: None,
        };
        // Fresh: less than an interval has elapsed ⇒ reuse cache, no fetch.
        assert!(!should_fetch(
            Some(&cache),
            1_000_000 + CHECK_INTERVAL_SECS - 1,
            CHECK_INTERVAL_SECS
        ));
        // Boundary: exactly one interval old ⇒ fetch.
        assert!(should_fetch(
            Some(&cache),
            1_000_000 + CHECK_INTERVAL_SECS,
            CHECK_INTERVAL_SECS
        ));
        // Stale: well past the interval ⇒ fetch.
        assert!(should_fetch(
            Some(&cache),
            1_000_000 + CHECK_INTERVAL_SECS * 3,
            CHECK_INTERVAL_SECS
        ));
    }

    #[test]
    fn should_fetch_handles_clock_going_backwards() {
        let cache = UpdateCache {
            last_checked_unix: 2_000_000,
            latest_version: "0.1.3".to_string(),
            notes: None,
        };
        // `now` before `last_checked` must not panic and must not fetch.
        assert!(!should_fetch(Some(&cache), 1_000_000, CHECK_INTERVAL_SECS));
    }

    #[test]
    fn update_cache_serde_round_trip() {
        let cache = UpdateCache {
            last_checked_unix: 1_752_400_000,
            latest_version: "0.2.0".to_string(),
            notes: Some("https://example.com/notes".to_string()),
        };
        let json = serde_json::to_string(&cache).unwrap();
        let restored: UpdateCache = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_checked_unix, 1_752_400_000);
        assert_eq!(restored.latest_version, "0.2.0");
        assert_eq!(restored.notes.as_deref(), Some("https://example.com/notes"));
    }

    // The network download (`download_to_file`) and the executable swap in
    // `run_update_command` are intentionally not unit-tested: they require a
    // real published release asset plus filesystem/process side effects.
}
