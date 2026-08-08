//! The instance identity file (`<data_folder>/instance_id`).
//!
//! One UUID per installation, in a plain file at the data folder root —
//! sibling of `index/` and `user_data/`, deliberately outside every database
//! so that it survives any database being deleted and remade.
//!
//! It answers the one question a stored `(db_uuid, db_name)` pair cannot:
//! *who stamped this association*. That is the only bit distinguishing "my
//! rebuilt `default`" from "another instance's `default`" — both of which
//! present as a dangling UUID plus a matching name — and it is what lets a
//! user_data database be shared between instances without one instance
//! adopting the other's boards.
//!
//! Degradation is deliberate: on a deployment where the file cannot be
//! created — including read-only mode, where this never even tries — this
//! reads `None` and the name-fallback clause of the association rule simply
//! never fires. Nothing else depends on it.

use std::io::Write as _;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use uuid::Uuid;

use super::identity::is_identity_uuid;

const FILE_NAME: &str = "instance_id";

/// Warns the first time and drops to debug afterwards. Every `None` path in
/// [`instance_uuid_in`] is retried on the next call by design (a transient
/// failure must not be cached), which without this would turn a *persistent*
/// failure into one warn per call for the life of the process. Each
/// expansion carries its own flag, so one site going quiet never silences
/// another.
macro_rules! warn_once_then_debug {
    ($($arg:tt)*) => {{
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!($($arg)*);
        } else {
            tracing::warn!($($arg)*);
        }
    }};
}

/// This instance's UUID, minted on first use if the file does not exist yet
/// (never in read-only mode, which only ever reads an existing file).
///
/// Cached for the life of the process: the file is written once and never
/// rewritten, so re-reading it per call would only buy a syscall. Only a
/// *success* is cached — a read that failed for a transient reason (or a
/// read-only server whose file appears later) is retried on the next call.
/// `None` means no identity could be obtained this call.
#[allow(dead_code)] // Consumed by the pinboard association match rule (step 2).
pub(crate) fn instance_uuid() -> Option<&'static str> {
    static INSTANCE_UUID: OnceLock<String> = OnceLock::new();
    static INIT_LOCK: Mutex<()> = Mutex::new(());
    cached(&INSTANCE_UUID, &INIT_LOCK, || {
        instance_uuid_in(
            &crate::config::runtime().data_folder,
            crate::db::readonly_mode(),
        )
    })
}

/// `OnceLock::get_or_init` for a fallible initializer: it can only cache
/// `Option<String>` *including* the `None`, which would freeze a transient
/// failure (a locked file, a folder not mounted yet) for the life of the
/// process. This caches successes only; the mutex keeps two racing callers
/// from both minting a fresh identity.
fn cached<F>(
    cell: &'static OnceLock<String>,
    init_lock: &'static Mutex<()>,
    compute: F,
) -> Option<&'static str>
where
    F: FnOnce() -> Option<String>,
{
    if let Some(cached) = cell.get() {
        return Some(cached.as_str());
    }
    let _guard = init_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cell.get() {
        return Some(cached.as_str());
    }
    let fresh = compute()?;
    let _ = cell.set(fresh);
    cell.get().map(String::as_str)
}

/// The identity in `data_folder`, minting and writing it if needed. The
/// uncached core of [`instance_uuid`]; separate so it can be exercised
/// against a temp folder (the runtime config is process-global) and with
/// `readonly` forced (it is a process-global too).
///
/// `readonly` mirrors [`crate::db::readonly_mode`]: a read-only server reads
/// an existing identity but must not create, mint, move or write anything,
/// so a missing or unusable file simply reads as `None`.
fn instance_uuid_in(data_folder: &Path, readonly: bool) -> Option<String> {
    let path = data_folder.join(FILE_NAME);
    // Bytes, not `read_to_string`: a file saved as UTF-16 (Notepad's other
    // encoding) is not UTF-8, and an `InvalidData` error here would read as
    // "could not read" — a permanent `None` for a file that is simply
    // unusable content and should take the set-aside-and-re-mint path.
    match std::fs::read(&path) {
        Ok(bytes) => {
            let contents = String::from_utf8_lossy(&bytes);
            // Normalize before judging: a hand-edited file may carry the
            // hyphenated or upper-case spelling of the same UUID, or a BOM.
            // Accepted as-is — the file is *not* rewritten to canonicalize it.
            if let Some(value) = normalized_identity(&contents) {
                return Some(value);
            }
            // Genuinely unusable: an unmatchable identity is worse than a
            // fresh one, since every stamp keyed on it matches nothing.
            if readonly {
                // Permanent for this process: read-only mode will never fix
                // it, and this runs on every call, so say it once.
                warn_once_then_debug!(
                    path = %path.display(),
                    "instance id file is empty or malformed and read-only mode forbids re-minting; \
                     this instance will have no identity"
                );
                return None;
            }
            tracing::warn!(
                path = %path.display(),
                "instance id file is empty or malformed; minting a new instance identity"
            );
            // Kept, not clobbered: the old contents are the only evidence of
            // what the stamps in user_data were keyed on, and a human (or a
            // later support question) may need them.
            set_aside_corrupt(&path);
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if readonly {
                tracing::debug!(
                    path = %path.display(),
                    "no instance id file and read-only mode forbids minting one; \
                     this instance will have no identity"
                );
                return None;
            }
        }
        Err(err) => {
            // Also reached on every call for as long as the condition lasts
            // (a permissions problem, a folder gone missing), and the caller
            // deliberately does not cache the failure — so warn once.
            warn_once_then_debug!(
                path = %path.display(),
                error = %err,
                "could not read the instance id file; this instance will have no identity"
            );
            return None;
        }
    }
    mint(&path)
}

/// The canonical form of a stored identity, or `None` if it cannot be one.
/// Whitespace (a trailing newline from an editor), hyphens and upper case are
/// all spellings of a valid UUID, not corruption — and neither is a leading
/// BOM, which is what Notepad writes and what `str::trim` does *not* strip.
fn normalized_identity(contents: &str) -> Option<String> {
    let normalized: String = contents
        .trim_start_matches('\u{feff}')
        .trim()
        .chars()
        .filter(|ch| *ch != '-')
        .flat_map(char::to_lowercase)
        .collect();
    is_identity_uuid(&normalized).then_some(normalized)
}

/// Best-effort move of an unusable identity file out of the way, so minting
/// a replacement does not destroy it.
fn set_aside_corrupt(path: &Path) {
    let aside = path.with_extension("corrupt");
    match std::fs::rename(path, &aside) {
        Ok(()) => tracing::warn!(
            path = %aside.display(),
            "kept the unusable instance id file"
        ),
        Err(err) => tracing::warn!(
            path = %aside.display(),
            error = %err,
            "could not keep the unusable instance id file; it will be replaced"
        ),
    }
}

/// Writes a fresh identity to `path`. `simple()` formats the UUID as 32
/// lowercase hex characters, matching `lower(hex(randomblob(16)))` — every
/// identity in this system is written in one shape.
///
/// A minted UUID that cannot be *persisted* is still returned and cached in
/// memory rather than discarded: stamps written with it are keyed on it for
/// this process, so clause (a) of the match rule and the manual editor stay
/// intact; only cross-restart continuity of clause (b) is lost, which is
/// exactly the degradation the design already accepts for an instance with
/// no identity at all. `None` is reserved for "no identity this call" — the
/// data folder itself could not be created, which is worth retrying later.
fn mint(path: &Path) -> Option<String> {
    let uuid = Uuid::new_v4().simple().to_string();
    if let Some(parent) = path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            // Retried on every call too (the folder may appear later).
            warn_once_then_debug!(
                path = %parent.display(),
                error = %err,
                "could not create the data folder for the instance id file"
            );
            return None;
        }
    }
    match persist(path, &uuid) {
        Ok(()) => tracing::info!(path = %path.display(), "minted this instance's identity"),
        Err(err) => tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not write the instance id file; this instance's identity is in memory only \
             and will not survive a restart"
        ),
    }
    Some(uuid)
}

/// Write-fsync-then-rename, like the Relay pairing store: a half-written
/// identity file would read as corrupt and re-mint on the next start,
/// silently orphaning every stamp made before the crash. The `sync_all` is
/// what makes that true — a rename can otherwise land ahead of the bytes.
fn persist(path: &Path, uuid: &str) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    if let Err(err) = write_synced(&tmp, uuid) {
        std::fs::remove_file(&tmp).ok();
        return Err(err);
    }
    if let Err(err) = std::fs::rename(&tmp, path) {
        // Same retry as `api/relay.rs::save`: on Windows a rename over a
        // target another process holds open fails, and removing it first is
        // the only way through.
        if !path.exists() {
            std::fs::remove_file(&tmp).ok();
            return Err(err);
        }
        std::fs::remove_file(path).ok();
        if let Err(retry) = std::fs::rename(&tmp, path) {
            std::fs::remove_file(&tmp).ok();
            return Err(retry);
        }
    }
    Ok(())
}

fn write_synced(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance_id_path(data_folder: &Path) -> std::path::PathBuf {
        data_folder.join(FILE_NAME)
    }

    #[test]
    fn identity_is_minted_once_and_then_stable() {
        let dir = tempfile::tempdir().unwrap();
        let first =
            instance_uuid_in(dir.path(), false).expect("a writable folder must get an identity");
        assert!(is_identity_uuid(&first), "unexpected shape: {first}");
        assert_eq!(
            std::fs::read_to_string(instance_id_path(dir.path())).unwrap(),
            first,
            "the identity must be persisted verbatim"
        );

        let second =
            instance_uuid_in(dir.path(), false).expect("the stored identity must be read back");
        assert_eq!(first, second, "the identity must not change once minted");
    }

    // Whitespace (a trailing newline from an editor, say) is not corruption,
    // and neither is the hyphenated or upper-case spelling of a real UUID:
    // those are normalized and accepted, leaving the file untouched.
    #[test]
    fn stored_identity_is_normalized_on_read() {
        let canonical = "0123456789abcdef0123456789abcdef";
        for stored in [
            format!("  {canonical}\r\n"),
            "0123456789ABCDEF0123456789ABCDEF".to_string(),
            "01234567-89ab-cdef-0123-456789abcdef".to_string(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(instance_id_path(dir.path()), &stored).unwrap();
            assert_eq!(
                instance_uuid_in(dir.path(), false).as_deref(),
                Some(canonical),
                "should have been accepted as {canonical}: {stored:?}"
            );
            assert_eq!(
                std::fs::read_to_string(instance_id_path(dir.path())).unwrap(),
                stored,
                "an accepted identity file must not be rewritten"
            );
        }
    }

    // Notepad writes a BOM; `str::trim` does not strip it. A hand-placed
    // identity must not be condemned as corrupt over three invisible bytes.
    #[test]
    fn a_byte_order_mark_is_not_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let canonical = "0123456789abcdef0123456789abcdef";
        let stored = format!("\u{feff}{canonical}\r\n");
        std::fs::write(instance_id_path(dir.path()), &stored).unwrap();

        assert_eq!(
            instance_uuid_in(dir.path(), false).as_deref(),
            Some(canonical)
        );
        assert_eq!(
            std::fs::read_to_string(instance_id_path(dir.path())).unwrap(),
            stored,
            "an accepted identity file must not be rewritten"
        );
        assert!(
            !dir.path().join("instance_id.corrupt").exists(),
            "a BOM must not send the file to the corrupt path"
        );
    }

    // A file that is not even UTF-8 (Notepad's UTF-16 save) must take the
    // corrupt path, not the unreadable path: the latter returns `None` for
    // ever, since nothing ever fixes the file.
    #[test]
    fn a_non_utf8_file_is_kept_aside_and_reminted() {
        let dir = tempfile::tempdir().unwrap();
        let utf16: Vec<u8> = "\u{feff}not-a-uuid"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        std::fs::write(instance_id_path(dir.path()), &utf16).unwrap();

        let minted = instance_uuid_in(dir.path(), false).expect("a UTF-16 file must re-mint");
        assert!(is_identity_uuid(&minted), "unexpected shape: {minted}");
        assert_eq!(
            std::fs::read(dir.path().join("instance_id.corrupt")).unwrap(),
            utf16,
            "the unreadable file must be kept byte-for-byte, not clobbered"
        );
        assert_eq!(
            instance_uuid_in(dir.path(), false).as_deref(),
            Some(minted.as_str())
        );
    }

    // A file that cannot be a valid identity is re-minted rather than
    // returned: stamps keyed on garbage would never match anything. The old
    // contents are moved aside, never overwritten.
    #[test]
    fn corrupt_identity_is_kept_aside_and_reminted() {
        for garbage in ["", "   ", "not-a-uuid", "0123456789abcdef"] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(instance_id_path(dir.path()), garbage).unwrap();

            let minted = instance_uuid_in(dir.path(), false).expect("corrupt must re-mint");
            assert!(is_identity_uuid(&minted), "unexpected shape: {minted}");
            assert_ne!(minted, garbage);
            assert_eq!(
                std::fs::read_to_string(dir.path().join("instance_id.corrupt")).unwrap(),
                garbage,
                "the unusable file must be kept, not clobbered"
            );
            // ...and the re-mint is what the next read returns.
            assert_eq!(
                instance_uuid_in(dir.path(), false).as_deref(),
                Some(minted.as_str())
            );
        }
    }

    // Read-only mode reads an existing identity and never creates one: no
    // mint, no data folder, no moving a corrupt file aside.
    #[test]
    fn readonly_mode_reads_but_never_writes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            instance_uuid_in(dir.path(), true),
            None,
            "a missing file must not be minted in read-only mode"
        );
        assert!(
            !instance_id_path(dir.path()).exists(),
            "read-only mode must not create the identity file"
        );

        let missing_folder = dir.path().join("not-created-yet");
        assert_eq!(instance_uuid_in(&missing_folder, true), None);
        assert!(
            !missing_folder.exists(),
            "read-only mode must not create the data folder"
        );

        let stored = "0123456789abcdef0123456789abcdef";
        std::fs::write(instance_id_path(dir.path()), stored).unwrap();
        assert_eq!(
            instance_uuid_in(dir.path(), true).as_deref(),
            Some(stored),
            "an existing identity must still be read in read-only mode"
        );

        std::fs::write(instance_id_path(dir.path()), "not-a-uuid").unwrap();
        assert_eq!(
            instance_uuid_in(dir.path(), true),
            None,
            "a corrupt file must not be re-minted in read-only mode"
        );
        assert_eq!(
            std::fs::read_to_string(instance_id_path(dir.path())).unwrap(),
            "not-a-uuid",
            "read-only mode must not move the corrupt file aside"
        );
    }

    // A folder that cannot be created (here: a *file* standing where the
    // data folder should be) yields no identity instead of an error — the
    // association rule's name fallback just never fires.
    #[test]
    fn unwritable_folder_yields_no_identity() {
        let dir = tempfile::tempdir().unwrap();
        let blocked = dir.path().join("not-a-folder");
        std::fs::write(&blocked, b"").unwrap();
        assert_eq!(instance_uuid_in(&blocked, false), None);
    }

    // Minting works even when the write cannot land: the identity is real
    // for this process (stamps key on it), it just will not survive a
    // restart. Here the temp file's path is occupied by a directory, so the
    // write fails while the folder itself is perfectly writable.
    #[test]
    fn unpersistable_identity_is_still_returned() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_blocker = dir
            .path()
            .join(format!("{FILE_NAME}.{}.tmp", std::process::id()));
        std::fs::create_dir(&tmp_blocker).unwrap();

        let minted =
            instance_uuid_in(dir.path(), false).expect("a failed write must not lose the identity");
        assert!(is_identity_uuid(&minted), "unexpected shape: {minted}");
        assert!(
            !instance_id_path(dir.path()).exists(),
            "nothing should have been committed"
        );
    }

    // The process-global cache keeps successes only: a failed attempt must
    // not freeze `None` for the life of the process.
    #[test]
    fn only_successful_lookups_are_cached() {
        static CELL: OnceLock<String> = OnceLock::new();
        static LOCK: Mutex<()> = Mutex::new(());

        assert_eq!(cached(&CELL, &LOCK, || None), None);
        assert_eq!(
            cached(&CELL, &LOCK, || Some("first".to_string())),
            Some("first"),
            "a later call must retry after a failure"
        );
        assert_eq!(
            cached(&CELL, &LOCK, || Some("second".to_string())),
            Some("first"),
            "a cached success must never be recomputed"
        );
    }
}
