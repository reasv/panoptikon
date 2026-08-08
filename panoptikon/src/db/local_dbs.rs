//! What the index databases on this machine currently claim as their
//! identity — the input to the "not claimed elsewhere" gate of the pinboard
//! association rule's clause (b).
//!
//! Clause (b) carries an association across a delete-and-remake from TOML,
//! which mints a fresh database UUID: a stamp whose `db_uuid` belongs to no
//! existing local database, whose name matches, and which this very instance
//! wrote, is the rebuilt database. The gate is what keeps that from also
//! matching a database that is merely *renamed* — its UUID still lives here,
//! under another folder name, so the stamp follows it by UUID instead.
//!
//! The answers are cached per process because a pinboard listing must not
//! turn into a probe of every local database on every call, and re-validated
//! against the folder listing on each call because folders come and go out of
//! band. Probes never go through the normal open path: that runs migrations
//! plus a post-migration ANALYZE (see [`probe_index_db_uuid`]).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use super::identity::{DbIdentityProbe, probe_index_db_uuid};

/// Folder name → what probing that database established. Only the two *real*
/// answers are ever stored: an [`Unknown`](DbIdentityProbe::Unknown) is a
/// transient condition (a locked or busy file), and caching it would freeze
/// the fail-closed verdict for the life of the process.
fn probe_cache() -> &'static Mutex<HashMap<String, DbIdentityProbe>> {
    static CACHE: OnceLock<Mutex<HashMap<String, DbIdentityProbe>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_cache() -> std::sync::MutexGuard<'static, HashMap<String, DbIdentityProbe>> {
    probe_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What the local index databases claim, as of one call.
pub(crate) struct LocalDbIdentities {
    /// Folder name → the identity it claims. Databases that claim nothing
    /// (pre-upgrade, never opened) are simply absent.
    claimed: HashMap<String, String>,
    /// Whether some local database could not be interrogated at all. Not a
    /// detail: it makes "this UUID lives nowhere here" unprovable, which is
    /// exactly what the gate needs to know.
    any_unknown: bool,
}

impl LocalDbIdentities {
    /// Whether `uuid` belongs to no existing local index database.
    ///
    /// Fails closed: a database that could not be read might be holding
    /// exactly this UUID, so an unaccounted-for UUID plus any unreadable
    /// database is *not* dangling. Folding the two together would let a
    /// momentarily locked database hand its boards to a same-named one — the
    /// collision the instance identity exists to prevent.
    pub(crate) fn is_dangling(&self, uuid: &str) -> bool {
        if self.claims(uuid) {
            return false;
        }
        !self.any_unknown
    }

    /// Whether some local index database claims `uuid`.
    pub(crate) fn claims(&self, uuid: &str) -> bool {
        self.claimed.values().any(|claimed| claimed == uuid)
    }

    /// A synthetic set, so the match rule can be exercised without a data
    /// folder full of databases.
    #[cfg(test)]
    pub(crate) fn for_tests(claimed: &[(&str, &str)], any_unknown: bool) -> Self {
        Self {
            claimed: claimed
                .iter()
                .map(|(name, uuid)| ((*name).to_string(), (*uuid).to_string()))
                .collect(),
            any_unknown,
        }
    }
}

/// The identities of every index database that exists right now, probing the
/// ones this process has not seen yet.
///
/// Eager fill on miss is deliberate: with a lazy cache, a gateway restart
/// leaves the set empty until each database happens to be opened, and the
/// gate would admit clause (b) for a stamp whose UUID is alive and well in a
/// renamed folder nobody has touched yet.
///
/// A data folder that cannot even be listed yields an all-unknown answer, so
/// the gate refuses rather than guessing.
pub(crate) async fn local_index_db_identities() -> LocalDbIdentities {
    let names = match super::info::db_lists() {
        Ok((index_dbs, _)) => index_dbs,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not list the local index databases; \
                 pinboard associations will not use the name fallback"
            );
            return LocalDbIdentities {
                claimed: HashMap::new(),
                any_unknown: true,
            };
        }
    };

    // Under the lock: forget folders that are gone, and collect the misses.
    // The probes themselves run outside it — they open files, and no other
    // caller should wait on that.
    let unseen: Vec<String> = {
        let mut cache = lock_cache();
        cache.retain(|name, _| names.iter().any(|existing| existing == name));
        names
            .iter()
            .filter(|name| !cache.contains_key(*name))
            .cloned()
            .collect()
    };

    let mut probed = Vec::with_capacity(unseen.len());
    for name in unseen {
        let paths = super::connection::index_storage_paths_unchecked(&name);
        let probe = probe_index_db_uuid(&paths.index_db_file).await;
        probed.push((name, probe));
    }

    let mut results: HashMap<String, DbIdentityProbe> = {
        let mut cache = lock_cache();
        for (name, probe) in &probed {
            match probe {
                DbIdentityProbe::Claimed(uuid) => {
                    warn_on_duplicate_identity(&cache, name, uuid);
                    cache.insert(name.clone(), probe.clone());
                }
                // A real answer that cannot change without a migration.
                DbIdentityProbe::ClaimsNothing => {
                    cache.insert(name.clone(), probe.clone());
                }
                // Transient by definition: re-probed on the next call.
                DbIdentityProbe::Unknown => {}
            }
        }
        names
            .iter()
            .filter_map(|name| cache.get(name).map(|probe| (name.clone(), probe.clone())))
            .collect()
    };
    // The unknowns are part of this call's answer even though nothing cached
    // them. A name a concurrent caller already resolved keeps the real answer.
    for (name, probe) in probed {
        results.entry(name).or_insert(probe);
    }

    let any_unknown = results
        .values()
        .any(|probe| matches!(probe, DbIdentityProbe::Unknown));
    let claimed = results
        .into_iter()
        .filter_map(|(name, probe)| match probe {
            DbIdentityProbe::Claimed(uuid) => Some((name, uuid)),
            _ => None,
        })
        .collect();
    LocalDbIdentities {
        claimed,
        any_unknown,
    }
}

/// Two folders holding the same identity means one is a copy of the other (a
/// copied folder, or a `VACUUM INTO`). Clause (a) then matches both
/// incarnations and the owning-database badge may point at the stale one —
/// accepted, since associations are hints and the manual editor is the fix
/// path, but this is the only place the duplication is visible at all.
///
/// Warned at probe time rather than per call: probes only happen on a cache
/// miss, so a lasting duplication says so once instead of on every listing.
fn warn_on_duplicate_identity(cache: &HashMap<String, DbIdentityProbe>, name: &str, uuid: &str) {
    for (other, probe) in cache {
        if other != name && matches!(probe, DbIdentityProbe::Claimed(seen) if seen == uuid) {
            tracing::warn!(
                database = name,
                duplicate_of = other,
                "two index databases share one identity; one is a copy of the other, \
                 so pinboard associations cannot tell them apart"
            );
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_index_db_file;

    fn cached_probe(name: &str) -> Option<DbIdentityProbe> {
        lock_cache().get(name).cloned()
    }

    /// Creates `<data folder>/index/<name>/index.db`, migrated (so it has an
    /// identity) or as the given raw bytes (so it cannot be read at all).
    async fn plant_db(root: &std::path::Path, name: &str, garbage: Option<&[u8]>) {
        let dir = root.join("index").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("index.db");
        match garbage {
            Some(bytes) => std::fs::write(&path, bytes).unwrap(),
            None => migrate_index_db_file(&path).await.unwrap(),
        }
    }

    fn remove_db(root: &std::path::Path, name: &str) {
        std::fs::remove_dir_all(root.join("index").join(name)).ok();
    }

    // The three things the gate depends on: a real identity is found and
    // cached, an unreadable database reads as unknown and is NOT cached (so a
    // transient lock does not freeze the fail-closed verdict), and a folder
    // that disappears is forgotten.
    #[tokio::test]
    async fn identities_are_cached_except_the_unknowns() {
        let env = crate::test_utils::test_data_dir();
        let root = env.path();
        plant_db(root, "pbassoc-real", None).await;
        plant_db(root, "pbassoc-unreadable", Some(b"not a database at all")).await;

        let identities = local_index_db_identities().await;
        let uuid = match cached_probe("pbassoc-real") {
            Some(DbIdentityProbe::Claimed(uuid)) => uuid,
            other => panic!("expected a cached identity, got {other:?}"),
        };
        assert!(identities.claims(&uuid));
        assert!(
            !identities.is_dangling(&uuid),
            "a UUID that lives here is not dangling"
        );
        assert_eq!(
            cached_probe("pbassoc-unreadable"),
            None,
            "an unknown probe must be retried, not cached"
        );
        // ...and while it is there, nothing else can be proven absent.
        assert!(!identities.is_dangling("ffffffffffffffffffffffffffffffff"));

        // With the unreadable database gone, absence becomes provable again.
        remove_db(root, "pbassoc-unreadable");
        let identities = local_index_db_identities().await;
        assert!(identities.is_dangling("ffffffffffffffffffffffffffffffff"));
        assert!(identities.claims(&uuid));

        remove_db(root, "pbassoc-real");
        let identities = local_index_db_identities().await;
        assert_eq!(
            cached_probe("pbassoc-real"),
            None,
            "a vanished folder must be dropped from the cache"
        );
        assert!(
            identities.is_dangling(&uuid),
            "the identity of a deleted database no longer claims anything"
        );
    }

    // The gate's fail-closed rule, in isolation from any filesystem.
    #[test]
    fn unknown_probes_refuse_the_name_fallback() {
        let known = LocalDbIdentities::for_tests(&[("photos", "uuid_p")], false);
        assert!(!known.is_dangling("uuid_p"));
        assert!(known.is_dangling("uuid_gone"));

        let uncertain = LocalDbIdentities::for_tests(&[("photos", "uuid_p")], true);
        assert!(!uncertain.is_dangling("uuid_p"));
        assert!(
            !uncertain.is_dangling("uuid_gone"),
            "an unreadable database might be the claimant"
        );
    }
}
