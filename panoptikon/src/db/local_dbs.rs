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
//! turn into a probe of every local database on every call. Two things keep
//! the cache honest: entries are dropped for folders that vanish, and each
//! carries the stamp of the file it was read from, so a database *replaced
//! in place* is re-probed rather than answered from a stale entry. Probes
//! never go through the normal open path: that runs migrations plus a
//! post-migration ANALYZE (see [`probe_index_db_uuid`]).

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use super::identity::{DbIdentityProbe, probe_index_db_uuid};

/// How long the index-folder listing (and the file stamps taken with it) is
/// reused before the data folder is walked again. Short enough that a
/// database created or deleted out of band shows up promptly; long enough
/// that a screenful of pinboard requests walks it once — this deployment
/// keeps its data on an SMB-mounted NAS, where every directory read and stat
/// is a network round trip.
const LISTING_TTL: Duration = Duration::from_secs(2);

/// How long an [`Unknown`](DbIdentityProbe::Unknown) probe is remembered
/// before another open is attempted. It is still never an *answer* — the gate
/// stays closed for as long as it is remembered — this only rate-limits the
/// retry, so a database that is unreadable for good costs one SQLite open
/// every half minute instead of one on every request.
const UNKNOWN_RETRY: Duration = Duration::from_secs(30);

/// A cheap "is this still the same file" stamp.
///
/// Not a content hash: the point is to notice an `index.db` that was replaced
/// *in place* — delete-and-recreate through `POST /api/db/create`, a restore
/// from backup, a `VACUUM INTO` over the top — which a name-keyed cache would
/// otherwise answer from a stale entry for the life of the process. A stale
/// `Claimed` is the dangerous one: it makes the gate call a rebuilt database's
/// old UUID "still claimed here", defeating clause (b) in exactly the
/// rebuilt-from-TOML case the clause exists for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileStamp {
    /// No file at that path. Itself an answer: nothing there claims anything.
    Missing,
    /// Present, with its modification time and length. A platform that cannot
    /// supply a modification time degrades to comparing lengths.
    Present(Option<SystemTime>, u64),
}

/// The stamp of `path`, or `None` when it could not be determined at all — a
/// permissions failure, say. `None` never compares equal to a cached entry's
/// stamp, so it forces a re-probe rather than pinning an answer.
fn file_stamp(path: &Path) -> Option<FileStamp> {
    match std::fs::metadata(path) {
        Ok(meta) => Some(FileStamp::Present(meta.modified().ok(), meta.len())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Some(FileStamp::Missing),
        Err(err) => {
            tracing::debug!(
                path = %path.display(),
                error = %err,
                "could not stamp a database file; its identity will be re-probed"
            );
            None
        }
    }
}

/// One index folder as the listing saw it: its name and the stamp of its
/// `index.db`.
type ListingEntry = (String, Option<FileStamp>);

struct Listing {
    entries: Vec<ListingEntry>,
    at: Instant,
}

fn listing_cache() -> &'static Mutex<Option<Listing>> {
    static LISTING: OnceLock<Mutex<Option<Listing>>> = OnceLock::new();
    LISTING.get_or_init(|| Mutex::new(None))
}

/// The index folders that exist, with their file stamps, re-walked at most
/// once per [`LISTING_TTL`]. A failure is never cached — the next call
/// retries — and every caller here shares the one listing.
fn current_listing() -> anyhow::Result<Vec<ListingEntry>> {
    {
        let cached = listing_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(listing) = cached.as_ref()
            && listing.at.elapsed() < LISTING_TTL
        {
            return Ok(listing.entries.clone());
        }
    }

    let (index_dbs, _) = super::info::db_lists()?;
    let entries: Vec<ListingEntry> = index_dbs
        .into_iter()
        .map(|name| {
            let paths = super::connection::index_storage_paths_unchecked(&name);
            let stamp = file_stamp(&paths.index_db_file);
            (name, stamp)
        })
        .collect();

    let mut cached = listing_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cached = Some(Listing {
        entries: entries.clone(),
        at: Instant::now(),
    });
    Ok(entries)
}

/// A cached probe, and what it was probed against.
struct CacheEntry {
    probe: DbIdentityProbe,
    /// The file as it stamped when probed. `None` only ever pairs with
    /// [`Unknown`](DbIdentityProbe::Unknown): a real answer is cached only
    /// when the file it came from could be stamped, or it would never expire.
    stamp: Option<FileStamp>,
    probed_at: Instant,
}

impl CacheEntry {
    /// Whether this entry still answers for a file that stamps as `current`.
    fn is_fresh(&self, current: Option<FileStamp>, now: Instant) -> bool {
        // A replaced (or newly created, or since-deleted) file is a different
        // database, whatever the folder is still called.
        if self.stamp != current {
            return false;
        }
        match self.probe {
            // Remembered, never believed: this only bounds the retry rate.
            DbIdentityProbe::Unknown => {
                now.saturating_duration_since(self.probed_at) < UNKNOWN_RETRY
            }
            // A real answer is only ever kept against a stamp it can be
            // expired by; without one it would answer for ever.
            _ => current.is_some(),
        }
    }
}

/// Folder name → what probing that database established, and when.
fn probe_cache() -> &'static Mutex<HashMap<String, CacheEntry>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_cache() -> std::sync::MutexGuard<'static, HashMap<String, CacheEntry>> {
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

    #[cfg(test)]
    pub(crate) fn any_unknown(&self) -> bool {
        self.any_unknown
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
/// ones this process has not seen — or has seen a different file for.
///
/// Eager fill on miss is deliberate: with a lazy cache, a gateway restart
/// leaves the set empty until each database happens to be opened, and the
/// gate would admit clause (b) for a stamp whose UUID is alive and well in a
/// renamed folder nobody has touched yet.
///
/// A data folder that cannot even be listed yields an all-unknown answer, so
/// the gate refuses rather than guessing.
pub(crate) async fn local_index_db_identities() -> LocalDbIdentities {
    let listing = match current_listing() {
        Ok(listing) => listing,
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

    // Under the lock: forget folders that are gone, and collect what needs
    // probing — never seen, replaced since, or a remembered Unknown that is
    // due for another try. The probes themselves run outside it: they open
    // files, and no other caller should wait on that.
    let now = Instant::now();
    let unresolved: Vec<ListingEntry> = {
        let mut cache = lock_cache();
        cache.retain(|name, _| listing.iter().any(|(existing, _)| existing == name));
        listing
            .iter()
            .filter(|(name, stamp)| {
                !cache
                    .get(name)
                    .is_some_and(|entry| entry.is_fresh(*stamp, now))
            })
            .cloned()
            .collect()
    };

    let mut probed = Vec::with_capacity(unresolved.len());
    for (name, stamp) in unresolved {
        let paths = super::connection::index_storage_paths_unchecked(&name);
        let probe = probe_index_db_uuid(&paths.index_db_file).await;
        probed.push((name, stamp, probe));
    }

    let probed_at = Instant::now();
    let mut results: HashMap<String, DbIdentityProbe> = {
        let mut cache = lock_cache();
        for (name, stamp, probe) in &probed {
            // Whatever was there answered for a file that is gone; dropping
            // it first also keeps a stale entry from outranking a fresh probe
            // that is deliberately not cached below.
            cache.remove(name);
            // A real answer is only worth caching when the file it came from
            // could be stamped: without a stamp there is nothing to notice a
            // replacement by, and the entry would never expire.
            if matches!(probe, DbIdentityProbe::Unknown) || stamp.is_some() {
                if let DbIdentityProbe::Claimed(uuid) = probe {
                    warn_on_duplicate_identity(&cache, name, uuid);
                }
                cache.insert(
                    name.clone(),
                    CacheEntry {
                        probe: probe.clone(),
                        stamp: *stamp,
                        probed_at,
                    },
                );
            }
        }
        listing
            .iter()
            .filter_map(|(name, _)| {
                cache
                    .get(name)
                    .map(|entry| (name.clone(), entry.probe.clone()))
            })
            .collect()
    };
    // A probe whose answer was deliberately not cached still answers for this
    // call. A name a concurrent caller already resolved keeps the cached one.
    for (name, _, probe) in probed {
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

/// The folder spelling of `index_db`.
///
/// The name a request runs under is not necessarily the folder's own
/// spelling: an explicitly passed `index_db` is checked against the listing
/// (so it matches exactly), but the configured default is trusted unchecked,
/// and on Windows a config saying `Default` opens the folder `default`
/// perfectly happily. Left alone, the two spellings would split clause (b)'s
/// name comparison — and the stamps written under each — down the middle.
///
/// An exact match always wins; otherwise a single case-insensitive match
/// supplies the canonical spelling, and anything ambiguous (two folders
/// differing only in case, which only a case-sensitive filesystem can hold)
/// is left exactly as it came in. ASCII case folding only: a name whose
/// spellings differ outside ASCII is not worth a Unicode-casing dependency.
pub(crate) fn canonical_index_db_name(index_db: &str) -> String {
    let Ok(listing) = current_listing() else {
        return index_db.to_string();
    };
    if listing.iter().any(|(name, _)| name == index_db) {
        return index_db.to_string();
    }
    let mut folded = listing
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(index_db));
    match (folded.next(), folded.next()) {
        (Some((name, _)), None) => name.clone(),
        _ => index_db.to_string(),
    }
}

/// Two folders holding the same identity means one is a copy of the other (a
/// copied folder, or a `VACUUM INTO`). Clause (a) then matches both
/// incarnations and the owning-database badge may point at the stale one —
/// accepted, since associations are hints and the manual editor is the fix
/// path, but this is the only place the duplication is visible at all.
///
/// Warned at probe time rather than per call: probes happen on a cache miss
/// or a changed file, so a lasting duplication says so once instead of on
/// every listing.
fn warn_on_duplicate_identity(cache: &HashMap<String, CacheEntry>, name: &str, uuid: &str) {
    for (other, entry) in cache {
        if other != name && matches!(&entry.probe, DbIdentityProbe::Claimed(seen) if seen == uuid) {
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
        lock_cache().get(name).map(|entry| entry.probe.clone())
    }

    /// Drops the listing cache, so a test's filesystem change is visible on
    /// the next call instead of up to `LISTING_TTL` later.
    fn forget_listing() {
        *listing_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Creates or replaces `<data folder>/index/<name>/index.db`, migrated
    /// (so it has an identity) or as the given raw bytes (so it cannot be
    /// read at all).
    async fn plant_db(root: &std::path::Path, name: &str, garbage: Option<&[u8]>) {
        let dir = root.join("index").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("index.db");
        // The sidecars go too: a `-wal` left by the previous incarnation must
        // never be replayed into the file that replaces it.
        for stale in ["index.db", "index.db-wal", "index.db-shm"] {
            std::fs::remove_file(dir.join(stale)).ok();
        }
        match garbage {
            Some(bytes) => std::fs::write(&path, bytes).unwrap(),
            None => migrate_index_db_file(&path).await.unwrap(),
        }
        forget_listing();
    }

    fn remove_db(root: &std::path::Path, name: &str) {
        std::fs::remove_dir_all(root.join("index").join(name)).ok();
        forget_listing();
    }

    fn claimed_uuid(name: &str) -> String {
        match cached_probe(name) {
            Some(DbIdentityProbe::Claimed(uuid)) => uuid,
            other => panic!("expected a cached identity for {name}, got {other:?}"),
        }
    }

    // The cache's whole contract, over one folder this test owns (the data
    // root is shared with every other test, so nothing here may assert about
    // databases it did not plant): an identity is found and cached, a
    // database REPLACED IN PLACE is re-probed rather than answered from the
    // stale entry — the case that would otherwise defeat clause (b) after a
    // delete-and-remake — an unreadable file is remembered as Unknown without
    // ever becoming an answer, and a vanished folder is forgotten.
    #[tokio::test]
    async fn a_replaced_database_is_reprobed_not_answered_from_cache() {
        let env = crate::test_utils::test_data_dir();
        let root = env.path();
        let name = "pbassoc-replaced";
        plant_db(root, name, None).await;

        let identities = local_index_db_identities().await;
        let first = claimed_uuid(name);
        assert!(identities.claims(&first));
        assert!(
            !identities.is_dangling(&first),
            "a UUID that lives here is not dangling"
        );

        // Replaced in place with something unreadable: the old identity must
        // stop being claimed, and the gate must close.
        plant_db(root, name, Some(b"not a database at all")).await;
        let identities = local_index_db_identities().await;
        assert!(!identities.claims(&first));
        assert!(identities.any_unknown());
        assert!(
            matches!(cached_probe(name), Some(DbIdentityProbe::Unknown)),
            "an unknown is remembered for the retry backoff"
        );

        // Replaced again, by a real database with a fresh identity: the new
        // one is what the gate sees.
        plant_db(root, name, None).await;
        let identities = local_index_db_identities().await;
        let second = claimed_uuid(name);
        assert_ne!(second, first, "a remade database mints a new identity");
        assert!(identities.claims(&second));
        assert!(!identities.claims(&first));

        remove_db(root, name);
        let identities = local_index_db_identities().await;
        assert_eq!(
            cached_probe(name),
            None,
            "a vanished folder must be dropped from the cache"
        );
        assert!(!identities.claims(&second));
    }

    // The staleness rules, without a filesystem: a changed file invalidates
    // any answer, an unstampable file is never answered from cache, and a
    // remembered Unknown expires on the retry timer while the two real
    // answers do not expire at all.
    #[test]
    fn cache_entries_expire_on_a_changed_file_or_the_unknown_timer() {
        let now = Instant::now();
        let stamp = |len| Some(FileStamp::Present(None, len));
        let entry = |probe, at| CacheEntry {
            probe,
            stamp: stamp(10),
            probed_at: at,
        };

        let claimed = entry(DbIdentityProbe::Claimed("uuid".into()), now);
        assert!(claimed.is_fresh(stamp(10), now + UNKNOWN_RETRY * 100));
        assert!(!claimed.is_fresh(stamp(11), now), "a replaced file is stale");
        assert!(
            !claimed.is_fresh(Some(FileStamp::Missing), now),
            "a deleted file is stale"
        );
        assert!(
            !claimed.is_fresh(None, now),
            "a file that cannot be stamped must be re-probed"
        );

        // The `Missing` answer of a folder whose index.db is not there yet —
        // the window a database creation runs in — must not survive the file
        // appearing.
        let nothing = CacheEntry {
            probe: DbIdentityProbe::ClaimsNothing,
            stamp: Some(FileStamp::Missing),
            probed_at: now,
        };
        assert!(nothing.is_fresh(Some(FileStamp::Missing), now));
        assert!(!nothing.is_fresh(stamp(10), now));

        let unknown = entry(DbIdentityProbe::Unknown, now);
        assert!(unknown.is_fresh(stamp(10), now + UNKNOWN_RETRY / 2));
        assert!(
            !unknown.is_fresh(stamp(10), now + UNKNOWN_RETRY),
            "the retry timer must expire a remembered unknown"
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

    // The name a request runs under is canonicalized to the folder's own
    // spelling, so a config default that differs only in case cannot split
    // clause (b)'s name comparison (and, in the next step, the stamps).
    #[tokio::test]
    async fn the_context_name_takes_the_folders_spelling() {
        let env = crate::test_utils::test_data_dir();
        let root = env.path();
        plant_db(root, "pbassoc-Cased", None).await;

        assert_eq!(canonical_index_db_name("pbassoc-cased"), "pbassoc-Cased");
        assert_eq!(canonical_index_db_name("PBASSOC-CASED"), "pbassoc-Cased");
        assert_eq!(canonical_index_db_name("pbassoc-Cased"), "pbassoc-Cased");
        // A name that resolves to nothing is left exactly as it came in.
        assert_eq!(canonical_index_db_name("pbassoc-absent"), "pbassoc-absent");

        remove_db(root, "pbassoc-Cased");
        assert_eq!(canonical_index_db_name("pbassoc-cased"), "pbassoc-cased");
    }
}
