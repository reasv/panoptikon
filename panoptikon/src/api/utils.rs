use axum::http::header;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::system_config::{SystemConfig, SystemConfigStore};

/// Ceiling on the outro gate's filesystem work, mirroring the file-serving
/// paths' `FILE_IO_TIMEOUT` (`api/items.rs`): the data folder can live on a
/// network mount, and a search must not hang behind a wedged stat.
const CONFIG_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether the request's index database may serve the outro playback
/// metadata (`content_end_ms`, `outro_kind`).
///
/// `detect_outros` off means the whole outro feature is off for that
/// database — including boundaries that were already detected while it was
/// on. Serving them as null is what turns the feature off for every client
/// at once, with no config plumbing in the player
/// (`docs/video-outro-skip-design.md` §6). Deliberately keyed on
/// `detect_outros` alone: the folded `scan_video && detect_outros` is a
/// scan-side concern.
///
/// `carries_metadata` says whether the response actually has one of the two
/// values in it. When it does not the answer cannot change what is
/// serialized — nulling an already-null field is a no-op — so the lookup is
/// skipped entirely. That makes the gate's cost, not its verdict,
/// data-dependent, which is benign here precisely because the lookup it
/// skips is harmless in all three ways that matter: it only ever *reads*
/// (never creates or writes a config file), it *fails open* (any stat, read
/// or parse trouble serves the metadata and logs, rather than failing the
/// request), and it is *cached* against the file's stamp, so the common case
/// is one stat rather than a full parse. A request that carries no outro
/// metadata therefore skips nothing it could have observed.
pub(crate) async fn serve_outro_metadata(index_db: &str, carries_metadata: bool) -> bool {
    if !carries_metadata {
        return true;
    }
    detect_outros_enabled(index_db).await
}

/// `config.toml` as it stamped when read. Length is carried alongside the
/// modification time because coarse mtime granularity can hide a same-second
/// rewrite that changed the file's size.
#[derive(Clone, Copy, PartialEq, Eq)]
struct ConfigStamp {
    modified: Option<SystemTime>,
    len: u64,
}

/// What one stat of `config.toml` established.
enum ConfigStat {
    /// No file: the database runs entirely on serde defaults, which is the
    /// answer outright — nothing to read, nothing worth caching.
    Missing,
    Present(ConfigStamp),
    /// Could not be stated at all (permissions, a wedged mount, a timeout).
    Unknown,
}

/// A remembered verdict, and the file state it was read from.
struct CachedGate {
    stamp: ConfigStamp,
    detect_outros: bool,
}

/// Index database name → its last-read `detect_outros`, keyed by the stamp
/// of the file it came from. Process-global like the other in-process caches
/// (`db::connection`'s read pools, `db::local_dbs`' identity probes).
fn gate_cache() -> &'static Mutex<HashMap<String, CachedGate>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedGate>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_gate_cache() -> std::sync::MutexGuard<'static, HashMap<String, CachedGate>> {
    gate_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// This database's `detect_outros`, read at most once per version of its
/// `config.toml`.
///
/// Per request this is one async stat; the read and the TOML parse happen
/// only when the stamp moved, and then off the async worker. A config save
/// rewrites the file, so its new stamp makes the very next request re-read —
/// which is what keeps §6's "takes effect immediately" true without paying
/// for a parse on every search.
///
/// Fails open at every step, and never caches a failure: a hand-broken
/// config must not take search or item metadata down with it (same stance as
/// `jobs::continuous_scan`, which refuses to abort a whole resync over one
/// unreadable config). The toggle is a playback preference; availability
/// outranks it.
async fn detect_outros_enabled(index_db: &str) -> bool {
    let config_path = SystemConfigStore::from_env().config_path(index_db);
    let stamp = match stat_config(&config_path).await {
        ConfigStat::Missing => return SystemConfig::default().detect_outros,
        ConfigStat::Unknown => {
            tracing::warn!(
                index_db = %index_db,
                path = %config_path.display(),
                "could not stat the database config; serving outro metadata"
            );
            return true;
        }
        ConfigStat::Present(stamp) => stamp,
    };

    // A stamp with no modification time can never be expired by one, so it
    // is never allowed to answer (nor to be stored): the file would pin its
    // first verdict for the life of the process.
    let cacheable = stamp.modified.is_some();
    if cacheable
        && let Some(cached) = lock_gate_cache().get(index_db)
        && cached.stamp == stamp
    {
        return cached.detect_outros;
    }

    let owned_db = index_db.to_string();
    let read = tokio::task::spawn_blocking(move || {
        SystemConfigStore::from_env()
            .load_readonly(&owned_db)
            .map(|config| config.detect_outros)
    });
    let detect_outros = match tokio::time::timeout(CONFIG_IO_TIMEOUT, read).await {
        Ok(Ok(Ok(detect_outros))) => detect_outros,
        Ok(Ok(Err(err))) => {
            tracing::warn!(
                index_db = %index_db,
                error = ?err,
                "could not read the database config; serving outro metadata"
            );
            return true;
        }
        Ok(Err(err)) => {
            tracing::warn!(
                index_db = %index_db,
                error = %err,
                "database config read task failed; serving outro metadata"
            );
            return true;
        }
        Err(_) => {
            tracing::warn!(
                index_db = %index_db,
                "timed out reading the database config; serving outro metadata"
            );
            return true;
        }
    };

    if cacheable {
        lock_gate_cache().insert(
            index_db.to_string(),
            CachedGate {
                stamp,
                detect_outros,
            },
        );
    }
    detect_outros
}

async fn stat_config(config_path: &Path) -> ConfigStat {
    match tokio::time::timeout(CONFIG_IO_TIMEOUT, tokio::fs::metadata(config_path)).await {
        Ok(Ok(meta)) => ConfigStat::Present(ConfigStamp {
            modified: meta.modified().ok(),
            len: meta.len(),
        }),
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::NotFound => ConfigStat::Missing,
        Ok(Err(_)) | Err(_) => ConfigStat::Unknown,
    }
}

pub(crate) fn content_disposition_value(kind: &str, filename: &str) -> Option<header::HeaderValue> {
    let mut value = Vec::new();
    value.extend_from_slice(kind.as_bytes());
    value.extend_from_slice(b"; filename=\"");
    value.extend_from_slice(&latin1_bytes(filename));
    value.extend_from_slice(b"\"");
    header::HeaderValue::from_bytes(&value).ok()
}

pub(crate) fn strip_non_latin1_chars(input: &str) -> String {
    input.chars().filter(|ch| (*ch as u32) <= 0xFF).collect()
}

pub(crate) fn iso_to_system_time(value: &str) -> Option<SystemTime> {
    let trimmed = value.trim_end_matches('Z');
    let mut parts = trimmed.split('T');
    let date = parts.next()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut date_parts = date.split('-');
    let year: i32 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour: u32 = time_parts.next()?.parse().ok()?;
    let minute: u32 = time_parts.next()?.parse().ok()?;
    let second: u32 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600)?
        .checked_add(i64::from(minute) * 60)?
        .checked_add(i64::from(second))?;

    if seconds < 0 {
        return None;
    }

    Some(UNIX_EPOCH + Duration::from_secs(seconds as u64))
}

fn latin1_bytes(value: &str) -> Vec<u8> {
    value
        .chars()
        .filter_map(|ch| {
            if (ch as u32) <= 0xFF {
                Some(ch as u8)
            } else {
                None
            }
        })
        .collect()
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut y = year;
    let m = month as i32;
    let d = day as i32;
    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146097 + doe - 719468) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_path(index_db: &str) -> std::path::PathBuf {
        SystemConfigStore::from_env().config_path(index_db)
    }

    /// A config that cannot be parsed must not take the request with it: the
    /// gate logs and serves. A 500 here would break search and item metadata
    /// for a whole database over a hand-edit, and data-dependently at that
    /// (only rows carrying outro metadata reach the read).
    #[tokio::test]
    async fn malformed_config_fails_open() {
        let _env = crate::test_utils::test_data_dir();
        let path = config_path("utils-outro-malformed");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "detect_outros = = broken\n[unterminated\n").unwrap();

        assert!(
            serve_outro_metadata("utils-outro-malformed", true).await,
            "an unparseable config serves the metadata rather than failing"
        );
    }

    /// A database whose `config.toml` does not exist yet reads as the serde
    /// default (detection on) and — the point of the test — the gate creates
    /// nothing. A GET must never write to the data folder: a read-only
    /// server refuses exactly this, and the write would also race a
    /// concurrent config save.
    #[tokio::test]
    async fn missing_config_is_not_created_by_the_gate() {
        let _env = crate::test_utils::test_data_dir();
        let path = config_path("utils-outro-absent");

        assert!(serve_outro_metadata("utils-outro-absent", true).await);
        assert!(
            !path.exists(),
            "the serving gate must not materialize a config file"
        );
        assert!(
            !path.parent().unwrap().exists(),
            "nor the directory it would live in"
        );
    }

    /// The cached bit is keyed on the file's stamp, so a saved config takes
    /// effect on the very next request rather than at some expiry.
    #[tokio::test]
    async fn a_rewritten_config_is_picked_up_on_the_next_call() {
        let _env = crate::test_utils::test_data_dir();
        let index_db = "utils-outro-rewrite";
        crate::test_utils::write_detect_outros_config(index_db, true);
        assert!(serve_outro_metadata(index_db, true).await);
        // Warm the cache a second time: the repeat must agree with itself.
        assert!(serve_outro_metadata(index_db, true).await);

        crate::test_utils::write_detect_outros_config(index_db, false);
        assert!(
            !serve_outro_metadata(index_db, true).await,
            "the rewritten file stamps differently, so it is re-read"
        );
    }

    /// The skip is a cost optimization only: with nothing to withhold the
    /// answer is fixed, whatever the config says.
    #[tokio::test]
    async fn a_response_without_outro_metadata_never_reads_the_config() {
        let _env = crate::test_utils::test_data_dir();
        crate::test_utils::write_detect_outros_config("utils-outro-skip", false);
        assert!(
            serve_outro_metadata("utils-outro-skip", false).await,
            "nothing to null, so the config is not consulted"
        );
    }
}
