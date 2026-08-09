//! Relay share cache: Relay-managed copies of files the browser pushed
//! because no local mapping resolved.
//!
//! Layout is `<root>/<sha256>/<sanitized-filename>` — the original filename
//! rides along in the path so whatever consumes the pasted clipboard entry
//! sees the real name. Last use is the entry's mtime, refreshed on every
//! [`ShareCache::lookup`] hit; eviction runs on insert and is size-capped.
//!
//! Entries that are never evicted: an in-flight upload (`.tmp-<action_id>-<n>`
//! in the cache root) and every path the Relay recently handed to a local
//! command — those may be live on the system clipboard, and evicting one would
//! turn a later paste into a silent no-op.
//!
//! Uploads that die without their handler running (a crash, a killed process)
//! leave their temporary file behind. [`ShareCache::sweep_temps`] removes those
//! on construction and before every insert, using an age floor so that a
//! long-running upload is never mistaken for debris.

use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

/// Prefix of an in-flight upload in the cache root. Temporary files live in
/// the root rather than in an entry directory so that they are trivially
/// distinguishable from cache entries during eviction.
const TEMP_PREFIX: &str = ".tmp-";

/// Age past which a temporary file is assumed to be debris from an interrupted
/// upload. A live upload refreshes its mtime with every write, so only an
/// upload that has been silent for an hour is swept — and never one whose
/// action is still claimed as in flight.
const TEMP_MAX_AGE: Duration = Duration::from_secs(60 * 60);

/// Per-component byte ceiling of every filesystem the Relay writes its cache
/// to. Windows, ext4, APFS and ZFS all stop at 255.
const MAX_COMPONENT_BYTES: usize = 255;

/// Characters that cannot appear in a Windows path component, plus the
/// control range (which no platform wants in a filename).
const INVALID_FILENAME_CHARS: &[char] = &['/', '\\', ':', '*', '?', '"', '<', '>', '|'];

/// Windows device names. Reserved with or without an extension, in any case.
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Disambiguates concurrent temporary files for one action id. The in-flight
/// claim set already makes a collision impossible; this plus `create_new` is
/// the backstop that keeps two writers from ever sharing a file handle.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct ShareCache {
    root: PathBuf,
}

impl ShareCache {
    pub fn new(root: PathBuf) -> Self {
        let cache = Self { root };
        // Nothing can be in flight before the Relay has served a request, so
        // every temporary file present at startup is debris by definition —
        // subject to the same age floor, which costs one extra hour at worst.
        cache.sweep_temps(&HashSet::new());
        cache
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// A fresh, unused destination for an in-flight upload. The action id is
    /// embedded so that [`sweep_temps`](Self::sweep_temps) can recognise a
    /// temporary file whose upload is still claimed.
    pub fn new_temp_path(&self, action_id: Uuid) -> PathBuf {
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(format!("{TEMP_PREFIX}{action_id}-{counter}"))
    }

    pub fn entry_path(&self, sha256: &str, filename: &str) -> PathBuf {
        self.root
            .join(sha256)
            .join(sanitize_filename(filename, sha256))
    }

    /// The cached copy for this content, if one is present and complete.
    ///
    /// A size mismatch is treated as a miss: the entry is either a truncated
    /// leftover or a filename collision under a different hash, and either way
    /// the correct repair is to re-upload over it. A hit refreshes the entry's
    /// mtime, which is what keeps eviction LRU rather than FIFO.
    pub fn lookup(&self, sha256: &str, filename: &str, size: u64) -> Option<PathBuf> {
        let path = self.entry_path(sha256, filename);
        let metadata = fs::metadata(&path).ok()?;
        if !metadata.is_file() || metadata.len() != size {
            return None;
        }
        touch(&path);
        Some(path)
    }

    /// Moves a completed upload into place, replacing any existing entry, and
    /// then evicts down to `max_bytes`.
    ///
    /// `keep` are paths the Relay recently handed to a local command; they and
    /// the entry being inserted survive eviction regardless of age. `in_flight`
    /// are the action ids currently uploading, whose temporary files are
    /// neither swept nor evicted.
    ///
    /// Eviction failures are reported to the caller only through the log: a
    /// full cache is a housekeeping problem, not a reason to fail an action
    /// whose bytes are already on disk.
    pub fn insert(
        &self,
        temp: &Path,
        sha256: &str,
        filename: &str,
        max_bytes: u64,
        keep: &[PathBuf],
        in_flight: &HashSet<Uuid>,
    ) -> io::Result<PathBuf> {
        self.sweep_temps(in_flight);
        let entry = self.entry_path(sha256, filename);
        if let Some(parent) = entry.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(temp, &entry)?;
        touch(&entry);
        self.evict(max_bytes, keep, &entry);
        Ok(entry)
    }

    /// Removes temporary files left behind by uploads that never finished.
    ///
    /// Both guards must hold before a file is removed: its action id is not
    /// claimed as in flight, and it has not been written to for
    /// [`TEMP_MAX_AGE`]. A temporary file whose name does not parse back to an
    /// action id is debris from no known writer and is swept on age alone.
    pub fn sweep_temps(&self, in_flight: &HashSet<Uuid>) {
        let Ok(directory) = fs::read_dir(&self.root) else {
            return;
        };
        let now = SystemTime::now();
        for entry in directory.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(rest) = name.strip_prefix(TEMP_PREFIX) else {
                continue;
            };
            if temp_action_id(rest).is_some_and(|id| in_flight.contains(&id)) {
                continue;
            }
            let stale = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|modified| now.duration_since(modified).ok())
                .is_some_and(|age| age > TEMP_MAX_AGE);
            if !stale {
                continue;
            }
            if let Err(error) = fs::remove_file(entry.path()) {
                tracing::warn!(%error, path = %entry.path().display(), "failed to sweep a stale Relay share upload");
            }
        }
    }

    /// Removes oldest-first until the cache fits in `max_bytes`.
    ///
    /// Temporary files in the root count toward the total — they are real
    /// bytes on the user's disk — but are never removed here; reclaiming them
    /// is [`sweep_temps`](Self::sweep_temps)' age-guarded job. Per-entry IO
    /// errors skip that entry rather than abandoning the pass, so one
    /// unreadable directory cannot disable eviction entirely.
    fn evict(&self, max_bytes: u64, keep: &[PathBuf], inserted: &Path) {
        let mut entries: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
        let mut total: u64 = 0;
        let directory = match fs::read_dir(&self.root) {
            Ok(directory) => directory,
            Err(error) => {
                tracing::warn!(%error, path = %self.root.display(), "failed to read the Relay share cache");
                return;
            }
        };
        for item in directory.flatten() {
            let Ok(file_type) = item.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                // A root-level file is either an in-flight upload or debris
                // awaiting the sweep. Either way its bytes are occupying the
                // cache and must be counted against the ceiling.
                if let Ok(metadata) = item.metadata()
                    && metadata.is_file()
                {
                    total = total.saturating_add(metadata.len());
                }
                continue;
            }
            let Ok(files) = fs::read_dir(item.path()) else {
                continue;
            };
            for file in files.flatten() {
                let Ok(metadata) = file.metadata() else {
                    continue;
                };
                if !metadata.is_file() {
                    continue;
                }
                total = total.saturating_add(metadata.len());
                entries.push((
                    metadata.modified().unwrap_or(UNIX_EPOCH),
                    metadata.len(),
                    file.path(),
                ));
            }
        }
        entries.sort_by_key(|entry| entry.0);
        for (_, size, path) in entries {
            if total <= max_bytes {
                break;
            }
            if path == inserted || keep.iter().any(|held| held == &path) {
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => {
                    total = total.saturating_sub(size);
                    if let Some(parent) = path.parent() {
                        // Only succeeds once the directory is empty, which is
                        // exactly when it should disappear.
                        let _ = fs::remove_dir(parent);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "failed to evict a Relay share cache entry");
                }
            }
        }
    }
}

/// Recovers the action id from a temporary file name's suffix
/// (`<uuid>-<counter>`).
fn temp_action_id(suffix: &str) -> Option<Uuid> {
    // A hyphenated UUID is exactly 36 bytes, and the counter that follows is
    // separated from it by one more hyphen.
    if suffix.as_bytes().get(36) != Some(&b'-') {
        return None;
    }
    Uuid::parse_str(suffix.get(..36)?).ok()
}

/// Marks an entry as most recently used. Best effort: a filesystem that
/// refuses the timestamp write only costs LRU accuracy.
fn touch(path: &Path) {
    if let Ok(file) = fs::OpenOptions::new().write(true).open(path) {
        let _ = file.set_modified(SystemTime::now());
    }
}

/// Reduces a browser-supplied filename to one safe path component.
///
/// Path separators, NULs and other control characters are dropped rather than
/// substituted, `%` is mapped to `_` (see [`strip_unsafe`]), trailing dots and
/// spaces are trimmed (Windows silently strips them, which would desynchronize
/// the stored name from the looked-up one), and Windows device names are
/// prefixed. A name that survives none of that falls back to the content hash,
/// so the entry is still addressable. The result is finally capped at the
/// 255-byte per-component filesystem limit, truncating the stem so the
/// extension — which decides how the pasted file is treated by its
/// destination — survives.
pub fn sanitize_filename(filename: &str, sha256: &str) -> String {
    let cleaned = strip_unsafe(filename);
    if cleaned.is_empty() {
        // Defensive: the extension survives the filter in every name seen so
        // far, so this branch is normally the bare-hash one. It exists so a
        // future tightening of the filter cannot silently drop extensions.
        let stem: String = sha256.chars().take(10).collect();
        let extension = filename
            .rsplit_once('.')
            .map(|(_, extension)| strip_unsafe(extension))
            .unwrap_or_default();
        return truncate_component(
            if extension.is_empty() {
                stem
            } else {
                format!("{stem}.{extension}")
            },
            sha256,
        );
    }
    let stem = cleaned.split('.').next().unwrap_or_default();
    if RESERVED_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
    {
        return truncate_component(format!("_{cleaned}"), sha256);
    }
    truncate_component(cleaned, sha256)
}

/// `%` becomes `_` rather than being dropped: the relay's own cache copy is the
/// filename a `cmd.exe` clipboard command re-reads, where `%NAME%` expands even
/// inside a quoted region (and cannot be escaped there). Neutralizing it in the
/// name the relay stores means the cache copy can never carry a `%`, so no
/// legitimate name is corrupted and no env-var value can be spliced in. This
/// only renames the relay's local copy; the browser's original is untouched.
fn strip_unsafe(value: &str) -> String {
    let filtered: String = value
        .chars()
        .filter_map(|character| {
            if character == '%' {
                Some('_')
            } else if INVALID_FILENAME_CHARS.contains(&character) || character.is_control() {
                None
            } else {
                Some(character)
            }
        })
        .collect();
    filtered.trim().trim_end_matches(['.', ' ']).to_owned()
}

/// Caps one path component at [`MAX_COMPONENT_BYTES`], cutting the stem at a
/// UTF-8 boundary and keeping the extension.
fn truncate_component(name: String, sha256: &str) -> String {
    if name.len() <= MAX_COMPONENT_BYTES {
        return name;
    }
    let (stem, extension) = match name.rfind('.') {
        // A leading dot is part of the stem, not an extension separator.
        Some(index) if index > 0 => (&name[..index], &name[index..]),
        _ => (name.as_str(), ""),
    };
    // An "extension" that alone fills the budget is not one worth keeping.
    let extension = if extension.len() > MAX_COMPONENT_BYTES / 2 {
        ""
    } else {
        extension
    };
    let mut end = stem.len().min(MAX_COMPONENT_BYTES - extension.len());
    while end > 0 && !stem.is_char_boundary(end) {
        end -= 1;
    }
    let truncated = format!("{}{extension}", &stem[..end]);
    if truncated.is_empty() || truncated == extension {
        // Every byte of the stem was a multi-byte character that did not fit;
        // fall back to the addressable name.
        return format!("{}{extension}", sha256.chars().take(10).collect::<String>());
    }
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// The sanitizer table: separators and control characters vanish, Windows
    /// trailing punctuation is trimmed, device names are escaped, and nothing
    /// can reduce to an empty or traversing component.
    #[test]
    fn sanitizer_table() {
        for (input, expected) in [
            ("photo.png", "photo.png"),
            ("sub/dir/photo.png", "subdirphoto.png"),
            ("..\\..\\evil.exe", "....evil.exe"),
            ("a\0b\tc.png", "abc.png"),
            ("trailing.  ", "trailing"),
            ("CON", "_CON"),
            ("con.txt", "_con.txt"),
            ("Com1.png", "_Com1.png"),
            ("COM10.png", "COM10.png"),
            ("nul", "_nul"),
            // `%` is neutralized to `_` so the relay's cache copy can never
            // carry a cmd-expandable `%NAME%`.
            (
                "a%PROCESSOR_ARCHITECTURE%.png",
                "a_PROCESSOR_ARCHITECTURE_.png",
            ),
            ("100%.png", "100_.png"),
            ("///.png", ".png"),
            ("..", "0123456789"),
            ("   ", "0123456789"),
            ("", "0123456789"),
            ("///.", "0123456789"),
            ("/*?.mp4", ".mp4"),
        ] {
            assert_eq!(sanitize_filename(input, SHA), expected, "input {input:?}");
        }
    }

    /// An over-long name is cut to the filesystem's per-component ceiling with
    /// its extension intact, on byte boundaries a multi-byte name respects.
    #[test]
    fn over_long_names_are_truncated_to_the_component_ceiling() {
        let long = format!("{}.png", "a".repeat(400));
        let sanitized = sanitize_filename(&long, SHA);
        assert_eq!(sanitized.len(), MAX_COMPONENT_BYTES);
        assert!(sanitized.ends_with(".png"));
        assert!(sanitized.starts_with("aaaa"));

        // Reserved-name escaping happens first, so its underscore is inside
        // the budget rather than pushing the result past it.
        let reserved = format!("con.{}.txt", "b".repeat(400));
        let sanitized = sanitize_filename(&reserved, SHA);
        assert_eq!(sanitized.len(), MAX_COMPONENT_BYTES);
        assert!(sanitized.starts_with("_con."));
        assert!(sanitized.ends_with(".txt"));

        // Multi-byte characters are never cut mid-sequence.
        let wide = format!("{}.jpg", "é".repeat(400));
        let sanitized = sanitize_filename(&wide, SHA);
        assert!(sanitized.len() <= MAX_COMPONENT_BYTES);
        assert!(sanitized.ends_with(".jpg"));
        assert!(std::str::from_utf8(sanitized.as_bytes()).is_ok());

        // A name that is one enormous "extension" keeps the ceiling, not the
        // extension.
        let silly = format!("a.{}", "c".repeat(400));
        assert!(sanitize_filename(&silly, SHA).len() <= MAX_COMPONENT_BYTES);
    }

    fn write_entry(cache: &ShareCache, sha: &str, name: &str, size: usize, age: Duration) {
        let path = cache.entry_path(sha, name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, vec![b'x'; size]).unwrap();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now() - age).unwrap();
    }

    fn hash(index: u8) -> String {
        std::iter::repeat_n(char::from(b'a' + index), 64).collect()
    }

    fn empty_cache(temp: &tempfile::TempDir) -> ShareCache {
        let cache = ShareCache::new(temp.path().join("share-cache"));
        fs::create_dir_all(cache.root()).unwrap();
        cache
    }

    /// Eviction is oldest-mtime first and stops as soon as the cache fits.
    #[test]
    fn evicts_oldest_first_until_under_cap() {
        let temp = tempfile::tempdir().unwrap();
        let cache = empty_cache(&temp);
        write_entry(&cache, &hash(0), "old.bin", 100, Duration::from_secs(300));
        write_entry(&cache, &hash(1), "mid.bin", 100, Duration::from_secs(200));
        write_entry(&cache, &hash(2), "new.bin", 100, Duration::from_secs(100));

        cache.evict(250, &[], Path::new(""));

        assert!(!cache.entry_path(&hash(0), "old.bin").exists());
        assert!(cache.entry_path(&hash(1), "mid.bin").exists());
        assert!(cache.entry_path(&hash(2), "new.bin").exists());
        // The emptied hash directory goes with its entry.
        assert!(!cache.root().join(hash(0)).exists());
    }

    /// The skip rules: in-flight uploads are invisible to eviction but their
    /// bytes count against the ceiling, and every recently handed-out path
    /// survives even when it is the oldest entry.
    #[test]
    fn eviction_counts_uploads_and_keeps_live_clipboard_paths() {
        let temp = tempfile::tempdir().unwrap();
        let cache = empty_cache(&temp);
        let action = Uuid::new_v4();
        let in_flight = cache.new_temp_path(action);
        fs::write(&in_flight, vec![b'x'; 500]).unwrap();
        write_entry(&cache, &hash(0), "held.bin", 100, Duration::from_secs(300));
        write_entry(&cache, &hash(1), "other.bin", 100, Duration::from_secs(200));
        write_entry(&cache, &hash(2), "spare.bin", 100, Duration::from_secs(100));

        let held = cache.entry_path(&hash(0), "held.bin");
        // 800 bytes present against a 700 ceiling: exactly one entry must go,
        // which is only true if the 500-byte upload is counted.
        cache.evict(700, std::slice::from_ref(&held), Path::new(""));

        assert!(in_flight.exists(), "in-flight upload must never be evicted");
        assert!(held.exists(), "a live clipboard path must never be evicted");
        assert!(!cache.entry_path(&hash(1), "other.bin").exists());
        assert!(cache.entry_path(&hash(2), "spare.bin").exists());
    }

    /// The sweep reclaims abandoned uploads only: a fresh temporary file and a
    /// claimed one both survive regardless of age.
    #[test]
    fn sweep_removes_only_abandoned_uploads() {
        let temp = tempfile::tempdir().unwrap();
        let cache = empty_cache(&temp);
        let abandoned = cache.new_temp_path(Uuid::new_v4());
        fs::write(&abandoned, b"debris").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&abandoned)
            .unwrap()
            .set_modified(SystemTime::now() - TEMP_MAX_AGE - Duration::from_secs(60))
            .unwrap();

        let fresh = cache.new_temp_path(Uuid::new_v4());
        fs::write(&fresh, b"writing").unwrap();

        let claimed_id = Uuid::new_v4();
        let claimed = cache.new_temp_path(claimed_id);
        fs::write(&claimed, b"slow upload").unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&claimed)
            .unwrap()
            .set_modified(SystemTime::now() - TEMP_MAX_AGE - Duration::from_secs(60))
            .unwrap();

        write_entry(
            &cache,
            &hash(0),
            "entry.bin",
            10,
            Duration::from_secs(9_000),
        );

        cache.sweep_temps(&HashSet::from([claimed_id]));

        assert!(!abandoned.exists(), "stale unclaimed upload must be swept");
        assert!(fresh.exists(), "a live upload must never be swept");
        assert!(claimed.exists(), "a claimed upload must never be swept");
        assert!(
            cache.entry_path(&hash(0), "entry.bin").exists(),
            "the sweep must not touch cache entries"
        );
    }

    /// Insert replaces an existing entry and a lookup only matches when the
    /// stored size is the expected one.
    #[test]
    fn insert_then_lookup_matches_on_size_only() {
        let temp = tempfile::tempdir().unwrap();
        let cache = empty_cache(&temp);
        let upload = cache.new_temp_path(Uuid::new_v4());
        fs::write(&upload, b"12345").unwrap();

        let entry = cache
            .insert(&upload, SHA, "photo.png", 1024, &[], &HashSet::new())
            .unwrap();
        assert_eq!(entry, cache.entry_path(SHA, "photo.png"));
        assert!(!upload.exists());
        assert_eq!(cache.lookup(SHA, "photo.png", 5), Some(entry));
        assert_eq!(cache.lookup(SHA, "photo.png", 6), None);
        assert_eq!(cache.lookup(SHA, "other.png", 5), None);
    }

    /// A path handed to a local command survives an insert big enough to
    /// evict everything else: the clipboard still points at real bytes.
    #[test]
    fn a_large_insert_cannot_evict_a_live_clipboard_path() {
        let temp = tempfile::tempdir().unwrap();
        let cache = empty_cache(&temp);
        write_entry(&cache, &hash(0), "live.bin", 100, Duration::from_secs(600));
        let live = cache.entry_path(&hash(0), "live.bin");

        let upload = cache.new_temp_path(Uuid::new_v4());
        fs::write(&upload, vec![b'y'; 400]).unwrap();
        let inserted = cache
            .insert(
                &upload,
                &hash(1),
                "big.bin",
                200,
                std::slice::from_ref(&live),
                &HashSet::new(),
            )
            .unwrap();

        assert!(live.exists(), "the held path must survive its own eviction");
        assert!(inserted.exists(), "the fresh entry must survive too");
    }

    /// Temporary names carry their action id back out for the sweep.
    #[test]
    fn temp_names_round_trip_the_action_id() {
        let temp = tempfile::tempdir().unwrap();
        let cache = ShareCache::new(temp.path().join("share-cache"));
        let id = Uuid::new_v4();
        let first = cache.new_temp_path(id);
        let second = cache.new_temp_path(id);
        assert_ne!(first, second, "temporary names must never collide");
        for path in [first, second] {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let suffix = name.strip_prefix(TEMP_PREFIX).unwrap();
            assert_eq!(temp_action_id(suffix), Some(id));
        }
        assert_eq!(temp_action_id("not-a-uuid"), None);
    }
}
