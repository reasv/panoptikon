use axum::http::header;
use std::collections::{HashMap, HashSet};
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

/// A remembered verdict, and the file state it was read from. A verdict that
/// came from a *failed* read is remembered the same way: it is still tied to
/// the stamp that produced it, so a fixed file re-reads on the next request.
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

/// True the first time this `key` is seen, false ever after.
///
/// The gate runs on the request path, so any condition it cannot cache away
/// would otherwise log at request rate. Those get one warning per process
/// (and a `debug!` afterwards, for anyone who does want the per-request
/// trace) rather than a permanent stream of identical lines.
fn first_time(key: String) -> bool {
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key)
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
/// Fails open at every step: a hand-broken config must not take search or
/// item metadata down with it (same stance as `jobs::continuous_scan`, which
/// refuses to abort a whole resync over one unreadable config). The toggle is
/// a playback preference; availability outranks it.
///
/// The fail-open verdict is cached like any other, against the stamp that
/// produced it — a broken config must not make every request re-read and
/// re-parse it, nor re-log (this repository has already been burned once by a
/// warning that fired on every config load forever). Recovery is unchanged:
/// fixing the file moves its stamp, and the next request re-reads.
async fn detect_outros_enabled(index_db: &str) -> bool {
    let config_path = SystemConfigStore::from_env().config_path(index_db);
    let stamp = match stat_config(&config_path).await {
        ConfigStat::Missing => return SystemConfig::default().detect_outros,
        // No stamp to key on, so this one genuinely cannot be cached and
        // recurs per request: it is loud once, then quiet.
        ConfigStat::Unknown => {
            if first_time(format!("stat:{index_db}")) {
                tracing::warn!(
                    index_db = %index_db,
                    path = %config_path.display(),
                    "could not stat the database config; serving outro metadata \
                     (logged once per database, retried on every request)"
                );
            } else {
                tracing::debug!(
                    index_db = %index_db,
                    path = %config_path.display(),
                    "could not stat the database config; serving outro metadata"
                );
            }
            return true;
        }
        ConfigStat::Present(stamp) => stamp,
    };

    // A stamp with no modification time can never be expired by one, so it
    // is never allowed to answer (nor to be stored): the file would pin its
    // first verdict for the life of the process. That silently costs a read
    // and a parse per request, so say so once — otherwise the degradation is
    // invisible.
    let cacheable = stamp.modified.is_some();
    if !cacheable && first_time(format!("mtime:{index_db}")) {
        tracing::warn!(
            index_db = %index_db,
            path = %config_path.display(),
            "the database config reports no modification time; the outro gate \
             cannot cache it and re-reads it on every request"
        );
    }
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
    let read = match tokio::time::timeout(CONFIG_IO_TIMEOUT, read).await {
        Ok(Ok(Ok(detect_outros))) => Ok(detect_outros),
        Ok(Ok(Err(err))) => Err(format!("could not read the database config: {err:?}")),
        Ok(Err(err)) => Err(format!("database config read task failed: {err}")),
        Err(_) => Err("timed out reading the database config".to_string()),
    };
    let detect_outros = match read {
        Ok(detect_outros) => detect_outros,
        Err(reason) => {
            // Fail open, and remember that. Reaching here means the cache had
            // no entry for this stamp, so the insert is exactly once per
            // broken revision of the file — and so is the warning.
            let loud = if cacheable {
                lock_gate_cache().insert(
                    index_db.to_string(),
                    CachedGate {
                        stamp,
                        detect_outros: true,
                    },
                );
                true
            } else {
                first_time(format!("read:{index_db}"))
            };
            if loud {
                tracing::warn!(
                    index_db = %index_db,
                    reason = %reason,
                    "serving outro metadata despite an unusable database config"
                );
            } else {
                tracing::debug!(
                    index_db = %index_db,
                    reason = %reason,
                    "serving outro metadata despite an unusable database config"
                );
            }
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

/// Builds a `Content-Disposition` value naming `filename`.
///
/// The quoted `filename=` parameter is Latin-1 only, so any name the quoted
/// form cannot carry verbatim additionally gets an RFC 8187
/// `filename*=UTF-8''…` — the form every current browser prefers, and the only
/// one that can carry the name intact. That is every non-ASCII name (a raw
/// Latin-1 byte such as `é` is ambiguous to browsers, so ASCII is the bar, not
/// Latin-1) plus any name holding a control character, which
/// [`latin1_fallback`] has to drop. A plain printable-ASCII name produces
/// exactly the historical single-parameter output, so the common response is
/// unchanged byte for byte.
pub(crate) fn content_disposition_value(kind: &str, filename: &str) -> Option<header::HeaderValue> {
    let mut value = Vec::new();
    value.extend_from_slice(kind.as_bytes());
    value.extend_from_slice(b"; filename=\"");
    value.extend_from_slice(&latin1_fallback(filename));
    value.push(b'"');
    let fallback_is_lossy = filename
        .chars()
        .any(|ch| !ch.is_ascii() || is_dropped_control(ch));
    if fallback_is_lossy {
        value.extend_from_slice(b"; filename*=UTF-8''");
        value.extend_from_slice(percent_encode_attr_chars(filename).as_bytes());
    }
    header::HeaderValue::from_bytes(&value).ok()
}

/// Characters `HeaderValue` refuses, and which the quoted fallback therefore
/// cannot contain. Tab is included: it is legal in a header value but
/// meaningless in a filename.
fn is_dropped_control(ch: char) -> bool {
    let code = ch as u32;
    code < 0x20 || code == 0x7F
}

/// The quoted-string fallback for legacy clients: characters outside Latin-1
/// have no representation there and are dropped, and `"`/`\` are escaped so a
/// name containing them cannot end the parameter early.
///
/// Control characters are dropped too. `HeaderValue` rejects them outright, so
/// a single `\n` in a filename would otherwise cost the *entire* header — the
/// download would lose its name and its `attachment` disposition. Dropping
/// them keeps the header, and `filename*` (which %-encodes them) still carries
/// the exact name for anything that reads it. Tab is dropped with the rest: it
/// is legal in a header value but meaningless in a filename.
fn latin1_fallback(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len());
    for ch in value.chars() {
        let code = ch as u32;
        if code > 0xFF {
            continue;
        }
        if is_dropped_control(ch) {
            continue;
        }
        if ch == '"' || ch == '\\' {
            out.push(b'\\');
        }
        out.push(code as u8);
    }
    out
}

/// Percent-encodes the UTF-8 bytes over RFC 8187's `attr-char` set.
fn percent_encode_attr_chars(value: &str) -> String {
    const EXTRA: &[u8] = b"!#$&+-.^_`|~";
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || EXTRA.contains(&byte) {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0x0F)] as char);
        }
    }
    out
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

    fn disposition_bytes(kind: &str, filename: &str) -> Vec<u8> {
        content_disposition_value(kind, filename)
            .expect("a valid header value")
            .as_bytes()
            .to_vec()
    }

    /// Only for cases whose output is pure ASCII — the Latin-1 fallback is not
    /// UTF-8 in general, so the byte-level assertion is used where it matters.
    fn disposition(kind: &str, filename: &str) -> String {
        String::from_utf8_lossy(&disposition_bytes(kind, filename)).into_owned()
    }

    /// The overwhelmingly common case must not grow a second parameter: an
    /// ASCII name is representable in the quoted form, so nothing is added.
    #[test]
    fn ascii_names_keep_the_historical_single_parameter_form() {
        assert_eq!(
            disposition("inline", "file.png"),
            r#"inline; filename="file.png""#
        );
        assert_eq!(
            disposition("attachment", "My Vacation (2024) [1].mp4"),
            r#"attachment; filename="My Vacation (2024) [1].mp4""#
        );
    }

    /// A name with no ASCII at all survives only in `filename*`; the quoted
    /// fallback is left empty rather than mangled, because Latin-1 has no
    /// character for any of it.
    #[test]
    fn cjk_name_travels_in_the_extended_parameter() {
        assert_eq!(
            disposition("inline", "写真.jpg"),
            "inline; filename=\".jpg\"; filename*=UTF-8''%E5%86%99%E7%9C%9F.jpg"
        );
    }

    #[test]
    fn emoji_name_is_percent_encoded_over_its_utf8_bytes() {
        assert_eq!(
            disposition("attachment", "🎉.png"),
            "attachment; filename=\".png\"; filename*=UTF-8''%F0%9F%8E%89.png"
        );
    }

    /// Mixed scripts are exactly where the old single-parameter output lost
    /// data silently: the ASCII part still reads, and the rest now arrives.
    /// Latin-1-but-not-ASCII characters (`é`) stay in the fallback as their
    /// single Latin-1 byte while the extended form spells them in UTF-8.
    #[test]
    fn mixed_name_keeps_a_readable_fallback_and_the_full_name() {
        let mut expected = b"inline; filename=\"caf".to_vec();
        expected.push(0xE9); // Latin-1 'é', deliberately not UTF-8
        expected.extend_from_slice(
            b"  01.jpg\"; filename*=UTF-8''caf%C3%A9%20%E5%86%99%E7%9C%9F%2001.jpg",
        );
        assert_eq!(disposition_bytes("inline", "café 写真 01.jpg"), expected);
    }

    /// Quotes and backslashes would otherwise end the quoted string early and
    /// let the rest of the name be read as header syntax.
    #[test]
    fn quotes_and_backslashes_are_escaped_in_the_fallback() {
        assert_eq!(
            disposition("attachment", r#"a"b\c.png"#),
            r#"attachment; filename="a\"b\\c.png""#
        );
    }

    /// A control character in the name must not cost the whole header.
    /// `HeaderValue` rejects them, so the quoted fallback drops them and the
    /// response keeps its disposition and a usable name; `filename*` still
    /// spells the byte out, so nothing is actually lost.
    #[test]
    fn control_characters_are_dropped_from_the_fallback_not_the_header() {
        let value = content_disposition_value("attachment", "evil\nname.png")
            .expect("a control character does not destroy the header");
        let bytes = value.as_bytes();
        assert!(
            !bytes.iter().any(|byte| *byte < 0x20 || *byte == 0x7F),
            "no control byte survives into the header"
        );
        let text = String::from_utf8_lossy(bytes).into_owned();
        assert!(
            text.starts_with(r#"attachment; filename="evilname.png""#),
            "the quoted fallback reads without the control character: {text}"
        );
        assert!(
            text.contains("filename*=UTF-8''evil%0Aname.png"),
            "the extended parameter carries the exact name: {text}"
        );
    }

    /// The disposition type is passed through untouched in both forms.
    #[test]
    fn disposition_type_is_passed_through() {
        assert!(disposition("attachment", "写真.jpg").starts_with("attachment; "));
        assert!(disposition("inline", "写真.jpg").starts_with("inline; "));
    }

    /// A config that cannot be parsed must not take the request with it: the
    /// gate logs and serves. A 500 here would break search and item metadata
    /// for a whole database over a hand-edit, and data-dependently at that
    /// (only rows carrying outro metadata reach the read).
    ///
    /// And it must not pay for the broken file twice: the fail-open verdict
    /// is cached against the same stamp as a good one, so a hand-broken
    /// config costs one read, one parse and one warning per revision rather
    /// than one per request.
    #[tokio::test]
    async fn malformed_config_fails_open() {
        let _env = crate::test_utils::test_data_dir();
        let index_db = "utils-outro-malformed";
        let path = config_path(index_db);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "detect_outros = = broken\n[unterminated\n").unwrap();

        assert!(
            serve_outro_metadata(index_db, true).await,
            "an unparseable config serves the metadata rather than failing"
        );

        // Flipping the remembered bit under the unchanged stamp makes the
        // second call's source observable: a re-read of the broken file would
        // fail open to true again, so `false` can only have come from cache.
        {
            let mut cache = lock_gate_cache();
            let cached = cache
                .get_mut(index_db)
                .expect("the fail-open verdict is cached against the file's stamp");
            assert!(cached.detect_outros, "and it is the fail-open verdict");
            cached.detect_outros = false;
        }
        assert!(
            !serve_outro_metadata(index_db, true).await,
            "the second call is answered from the cache, without re-parsing"
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
