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
//! created, this reads `None` and the name-fallback clause of the
//! association rule simply never fires. Nothing else depends on it.

use std::path::Path;
use std::sync::OnceLock;

use uuid::Uuid;

use super::identity::is_identity_uuid;

const FILE_NAME: &str = "instance_id";

/// This instance's UUID, minted on first use if the file does not exist yet.
///
/// Cached for the life of the process: the file is written once and never
/// rewritten, so re-reading it per call would only buy a syscall. `None`
/// means the file is absent and could not be created (read-only deployment).
#[allow(dead_code)] // Consumed by the pinboard association match rule (step 2).
pub(crate) fn instance_uuid() -> Option<&'static str> {
    static INSTANCE_UUID: OnceLock<Option<String>> = OnceLock::new();
    INSTANCE_UUID
        .get_or_init(|| instance_uuid_in(&crate::config::runtime().data_folder))
        .as_deref()
}

/// The identity in `data_folder`, minting and writing it if needed. The
/// uncached core of [`instance_uuid`]; separate so it can be exercised
/// against a temp folder (the runtime config is process-global).
fn instance_uuid_in(data_folder: &Path) -> Option<String> {
    let path = data_folder.join(FILE_NAME);
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            let value = contents.trim();
            if is_identity_uuid(value) {
                return Some(value.to_string());
            }
            // Empty or corrupt reads as absent: an unusable identity is
            // worse than a fresh one, since every stamp keyed on it is
            // unmatchable anyway.
            tracing::warn!(
                path = %path.display(),
                "instance id file is empty or malformed; minting a new instance identity"
            );
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                error = %err,
                "could not read the instance id file; this instance will have no identity"
            );
            return None;
        }
    }
    mint(&path)
}

/// Writes a fresh identity to `path`. `simple()` formats the UUID as 32
/// lowercase hex characters, matching `lower(hex(randomblob(16)))` — every
/// identity in this system is written in one shape.
fn mint(path: &Path) -> Option<String> {
    let uuid = Uuid::new_v4().simple().to_string();
    if let Some(parent) = path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            tracing::warn!(
                path = %parent.display(),
                error = %err,
                "could not create the data folder for the instance id file"
            );
            return None;
        }
    }
    // Write-then-rename, like the Relay pairing store: a half-written
    // identity file would read as corrupt and re-mint on the next start,
    // silently orphaning every stamp made before the crash.
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    if let Err(err) = std::fs::write(&tmp, &uuid) {
        tracing::warn!(
            path = %tmp.display(),
            error = %err,
            "could not write the instance id file; this instance will have no identity"
        );
        return None;
    }
    if let Err(err) = std::fs::rename(&tmp, path) {
        tracing::warn!(
            path = %path.display(),
            error = %err,
            "could not commit the instance id file; this instance will have no identity"
        );
        std::fs::remove_file(&tmp).ok();
        return None;
    }
    tracing::info!(path = %path.display(), "minted this instance's identity");
    Some(uuid)
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
        let first = instance_uuid_in(dir.path()).expect("a writable folder must get an identity");
        assert!(is_identity_uuid(&first), "unexpected shape: {first}");
        assert_eq!(
            std::fs::read_to_string(instance_id_path(dir.path())).unwrap(),
            first,
            "the identity must be persisted verbatim"
        );

        let second = instance_uuid_in(dir.path()).expect("the stored identity must be read back");
        assert_eq!(first, second, "the identity must not change once minted");
    }

    // Whitespace (a trailing newline from an editor, say) is not corruption.
    #[test]
    fn stored_identity_is_trimmed_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let stored = "0123456789abcdef0123456789abcdef";
        std::fs::write(instance_id_path(dir.path()), format!("  {stored}\r\n")).unwrap();
        assert_eq!(instance_uuid_in(dir.path()).as_deref(), Some(stored));
    }

    // A file that cannot be a valid identity is re-minted rather than
    // returned: stamps keyed on garbage would never match anything.
    #[test]
    fn corrupt_identity_is_reminted() {
        let dir = tempfile::tempdir().unwrap();
        for garbage in ["", "   ", "not-a-uuid", "0123456789ABCDEF0123456789ABCDEF"] {
            std::fs::write(instance_id_path(dir.path()), garbage).unwrap();
            let minted = instance_uuid_in(dir.path()).expect("corrupt must re-mint");
            assert!(is_identity_uuid(&minted), "unexpected shape: {minted}");
            assert_ne!(minted, garbage);
            // ...and the re-mint is what the next read returns.
            assert_eq!(instance_uuid_in(dir.path()).as_deref(), Some(minted.as_str()));
        }
    }

    // A folder that cannot be created (here: a *file* standing where the
    // data folder should be) yields no identity instead of an error — the
    // association rule's name fallback just never fires.
    #[test]
    fn unwritable_folder_yields_no_identity() {
        let dir = tempfile::tempdir().unwrap();
        let blocked = dir.path().join("not-a-folder");
        std::fs::write(&blocked, b"").unwrap();
        assert_eq!(instance_uuid_in(&blocked), None);
    }
}
