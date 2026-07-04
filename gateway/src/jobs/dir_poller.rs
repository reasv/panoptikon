//! Hierarchical directory-mtime poller backing continuous scan's poll mode.
//!
//! Instead of re-stating every file each tick (notify's `PollWatcher`), a tick
//! stats only directories and re-enumerates just the ones whose mtime changed.
//! An idle tree costs one stat per directory per tick regardless of file count,
//! which is what makes poll mode usable on large folders over network mounts.
//!
//! Relies on POSIX/NTFS semantics: creating, removing, or renaming an entry
//! updates the parent directory's mtime. In-place content edits do NOT bump the
//! parent's mtime and are therefore invisible to the poller until the next full
//! scan — continuous scan is a latency optimization, cron full scans remain the
//! ground truth. (FAT-family filesystems don't maintain directory mtimes at all
//! and are not supported by poll mode.)
//!
//! The poller favors false negatives: any stat or enumeration error marks the
//! pass as degraded and suppresses the stale-directory cleanup, so a transient
//! network failure can never translate into a flood of file removals.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::jobs::files::{
    format_system_time, has_allowed_extension, is_excluded, is_hidden_or_temp,
};

/// Path filters mirroring `should_process_path` in the continuous scan actor.
pub(crate) struct PollFilters {
    pub roots: Vec<PathBuf>,
    pub excluded_roots: Vec<PathBuf>,
    pub allowed_extensions: HashSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileMeta {
    /// Same string format as `files.last_modified` in the DB.
    pub last_modified: String,
    /// None for DB-seeded entries (the files table stores no size), which
    /// makes the comparison mtime-only, matching full-scan dedup semantics.
    pub size: Option<i64>,
}

#[derive(Default)]
struct DirSnapshot {
    /// Dir mtime at the last successful enumeration; None forces enumeration.
    dir_modified: Option<SystemTime>,
    files: HashMap<OsString, FileMeta>,
    subdirs: HashSet<OsString>,
}

#[derive(Default)]
pub(crate) struct PollerSnapshot {
    dirs: HashMap<PathBuf, DirSnapshot>,
}

impl PollerSnapshot {
    #[cfg(test)]
    pub(crate) fn dir_count(&self) -> usize {
        self.dirs.len()
    }
}

pub(crate) struct PollChange {
    pub path: PathBuf,
    pub meta: FileMeta,
}

pub(crate) struct PollOutcome {
    pub snapshot: PollerSnapshot,
    /// New or changed files, as observed at enumeration time. Callers should
    /// settle (re-stat after a delay) before processing, since a file may
    /// still be mid-write when first seen.
    pub changes: Vec<PollChange>,
    /// Files that disappeared. Callers must re-check existence before acting.
    pub removals: Vec<PathBuf>,
    /// True when any stat/enumeration failed; stale-dir cleanup was skipped.
    pub degraded: bool,
}

/// Builds the initial snapshot from DB rows of `(path, last_modified)`, so the
/// first pass diffs the disk against the index instead of treating every file
/// as new. Directories get `dir_modified: None` and are enumerated once; only
/// files genuinely absent from or newer than the index surface as changes.
pub(crate) fn seed_snapshot(rows: &[(String, String)], filters: &PollFilters) -> PollerSnapshot {
    let mut snapshot = PollerSnapshot::default();
    for (path_str, last_modified) in rows {
        let path = PathBuf::from(path_str);
        if !filters.roots.iter().any(|root| path.starts_with(root)) {
            continue;
        }
        if is_excluded(&path, &filters.excluded_roots) || is_hidden_or_temp(&path) {
            continue;
        }
        if !has_allowed_extension(&path, &filters.allowed_extensions) {
            continue;
        }
        let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
            continue;
        };
        snapshot
            .dirs
            .entry(parent.to_path_buf())
            .or_default()
            .files
            .insert(
                name.to_os_string(),
                FileMeta {
                    last_modified: last_modified.clone(),
                    size: None,
                },
            );
    }
    snapshot
}

struct DirListing {
    files: HashMap<OsString, FileMeta>,
    subdirs: HashSet<OsString>,
}

fn enumerate_dir(dir: &Path, filters: &PollFilters) -> std::io::Result<DirListing> {
    let mut files = HashMap::new();
    let mut subdirs = HashSet::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let path = entry.path();
        // DirEntry::metadata is free on Windows (comes from the enumeration
        // itself), so this stays at O(1) network round-trips per directory
        // batch. Symlinks need a real stat to resolve the target.
        let metadata = match entry.file_type() {
            Ok(file_type) if file_type.is_symlink() => match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            },
            Ok(_) => match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            },
            Err(_) => continue,
        };
        if metadata.is_dir() {
            subdirs.insert(name);
        } else if metadata.is_file() {
            if is_hidden_or_temp(&path)
                || !has_allowed_extension(&path, &filters.allowed_extensions)
                || is_excluded(&path, &filters.excluded_roots)
            {
                continue;
            }
            let Some(last_modified) = metadata.modified().ok().and_then(format_system_time)
            else {
                continue;
            };
            files.insert(
                name,
                FileMeta {
                    last_modified,
                    size: Some(metadata.len() as i64),
                },
            );
        }
    }
    Ok(DirListing { files, subdirs })
}

/// Protects a subtree we could not inspect this pass from the stale-dir
/// cleanup by marking every snapshot dir under it as visited.
fn mark_subtree_visited(snapshot: &PollerSnapshot, dir: &Path, visited: &mut HashSet<PathBuf>) {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        if let Some(snap) = snapshot.dirs.get(&current) {
            for sub in &snap.subdirs {
                let child = current.join(sub);
                if !visited.contains(&child) {
                    stack.push(child);
                }
            }
        }
        visited.insert(current);
    }
}

/// One poll pass: walk from the roots, stat each directory, and enumerate only
/// directories whose mtime changed since the snapshot was taken. Consumes the
/// previous snapshot and returns the updated one alongside the observed diff.
pub(crate) fn run_poll_pass(mut snapshot: PollerSnapshot, filters: &PollFilters) -> PollOutcome {
    let mut changes = Vec::new();
    let mut removals = Vec::new();
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let mut degraded = false;

    let mut stack: Vec<PathBuf> = filters.roots.clone();
    while let Some(dir) = stack.pop() {
        if is_excluded(&dir, &filters.excluded_roots) {
            continue;
        }
        if !visited.insert(dir.clone()) {
            continue;
        }

        // Stat before enumerating: if the dir changes mid-enumeration we
        // record the older mtime and simply re-enumerate next tick.
        let dir_modified = match std::fs::metadata(&dir).and_then(|meta| meta.modified()) {
            Ok(modified) => modified,
            Err(_) => {
                degraded = true;
                mark_subtree_visited(&snapshot, &dir, &mut visited);
                continue;
            }
        };

        let unchanged = snapshot
            .dirs
            .get(&dir)
            .and_then(|snap| snap.dir_modified)
            .is_some_and(|previous| previous == dir_modified);
        if unchanged {
            if let Some(snap) = snapshot.dirs.get(&dir) {
                for sub in &snap.subdirs {
                    stack.push(dir.join(sub));
                }
            }
            continue;
        }

        let listing = match enumerate_dir(&dir, filters) {
            Ok(listing) => listing,
            Err(_) => {
                degraded = true;
                mark_subtree_visited(&snapshot, &dir, &mut visited);
                continue;
            }
        };

        let old = snapshot.dirs.remove(&dir).unwrap_or_default();
        for (name, meta) in &listing.files {
            let known_unchanged = old.files.get(name).is_some_and(|prev| {
                prev.last_modified == meta.last_modified
                    && (prev.size.is_none() || prev.size == meta.size)
            });
            if !known_unchanged {
                changes.push(PollChange {
                    path: dir.join(name),
                    meta: meta.clone(),
                });
            }
        }
        for name in old.files.keys() {
            if !listing.files.contains_key(name) {
                removals.push(dir.join(name));
            }
        }

        for sub in &listing.subdirs {
            stack.push(dir.join(sub));
        }
        snapshot.dirs.insert(
            dir,
            DirSnapshot {
                dir_modified: Some(dir_modified),
                files: listing.files,
                subdirs: listing.subdirs,
            },
        );
    }

    if !degraded {
        let stale: Vec<PathBuf> = snapshot
            .dirs
            .keys()
            .filter(|dir| !visited.contains(*dir))
            .cloned()
            .collect();
        for dir in stale {
            if let Some(snap) = snapshot.dirs.remove(&dir) {
                for name in snap.files.keys() {
                    removals.push(dir.join(name));
                }
            }
        }
    }

    PollOutcome {
        snapshot,
        changes,
        removals,
        degraded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::files::get_last_modified_time_and_size;
    use std::fs;
    use tempfile::TempDir;

    fn png_filters(root: &Path) -> PollFilters {
        PollFilters {
            roots: vec![root.to_path_buf()],
            excluded_roots: Vec::new(),
            allowed_extensions: HashSet::from([".png".to_string()]),
        }
    }

    fn change_paths(outcome: &PollOutcome) -> HashSet<PathBuf> {
        outcome.changes.iter().map(|c| c.path.clone()).collect()
    }

    #[test]
    fn initial_pass_reports_all_matching_files() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.png"), "a").unwrap();
        fs::write(root.join("ignored.txt"), "x").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join("b.png"), "b").unwrap();

        let filters = png_filters(root);
        let outcome = run_poll_pass(PollerSnapshot::default(), &filters);

        assert!(!outcome.degraded);
        assert!(outcome.removals.is_empty());
        assert_eq!(
            change_paths(&outcome),
            HashSet::from([root.join("a.png"), root.join("sub").join("b.png")])
        );
    }

    #[test]
    fn quiet_second_pass_reports_nothing() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.png"), "a").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join("b.png"), "b").unwrap();

        let filters = png_filters(root);
        let first = run_poll_pass(PollerSnapshot::default(), &filters);
        let second = run_poll_pass(first.snapshot, &filters);

        assert!(second.changes.is_empty());
        assert!(second.removals.is_empty());
        assert!(!second.degraded);
    }

    #[test]
    fn db_seeded_snapshot_skips_unchanged_and_flags_stale() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let fresh = root.join("fresh.png");
        let stale = root.join("stale.png");
        fs::write(&fresh, "a").unwrap();
        fs::write(&stale, "b").unwrap();

        let (fresh_mtime, _) = get_last_modified_time_and_size(&fresh).unwrap();
        let rows = vec![
            (fresh.to_string_lossy().to_string(), fresh_mtime),
            (
                stale.to_string_lossy().to_string(),
                "2000-01-01T00:00:00".to_string(),
            ),
        ];

        let filters = png_filters(root);
        let snapshot = seed_snapshot(&rows, &filters);
        let outcome = run_poll_pass(snapshot, &filters);

        assert_eq!(change_paths(&outcome), HashSet::from([stale]));
        assert!(outcome.removals.is_empty());
    }

    #[test]
    fn new_file_detected_on_next_pass() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.png"), "a").unwrap();

        let filters = png_filters(root);
        let first = run_poll_pass(PollerSnapshot::default(), &filters);
        fs::write(root.join("b.png"), "b").unwrap();
        let second = run_poll_pass(first.snapshot, &filters);

        assert_eq!(change_paths(&second), HashSet::from([root.join("b.png")]));
        assert!(second.removals.is_empty());
    }

    #[test]
    fn removed_file_and_removed_subtree_detected() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.png"), "a").unwrap();
        let sub = root.join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("b.png"), "b").unwrap();

        let filters = png_filters(root);
        let first = run_poll_pass(PollerSnapshot::default(), &filters);
        fs::remove_file(root.join("a.png")).unwrap();
        fs::remove_dir_all(&sub).unwrap();
        let second = run_poll_pass(first.snapshot, &filters);

        assert!(second.changes.is_empty());
        assert_eq!(
            second.removals.iter().cloned().collect::<HashSet<_>>(),
            HashSet::from([root.join("a.png"), sub.join("b.png")])
        );
    }

    // Documents the known poll-mode limitation: in-place content edits do not
    // bump the parent dir's mtime, so they surface at the next full scan, not
    // through the poller.
    #[test]
    fn in_place_modify_without_dir_change_is_not_detected() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let file = root.join("a.png");
        fs::write(&file, "a").unwrap();

        let filters = png_filters(root);
        let first = run_poll_pass(PollerSnapshot::default(), &filters);
        // Rewrite contents without adding/removing directory entries.
        fs::write(&file, "different contents").unwrap();
        let second = run_poll_pass(first.snapshot, &filters);

        assert!(second.changes.is_empty());
        assert!(second.removals.is_empty());
    }

    #[test]
    fn excluded_dirs_are_skipped() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let excluded = root.join("excluded");
        fs::create_dir(&excluded).unwrap();
        fs::write(excluded.join("hidden.png"), "x").unwrap();
        fs::write(root.join("a.png"), "a").unwrap();

        let mut filters = png_filters(root);
        filters.excluded_roots = vec![excluded.clone()];
        let outcome = run_poll_pass(PollerSnapshot::default(), &filters);

        assert_eq!(change_paths(&outcome), HashSet::from([root.join("a.png")]));
    }

    #[test]
    fn unreachable_root_degrades_without_removals() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join("a.png"), "a").unwrap();

        let filters = png_filters(root);
        let first = run_poll_pass(PollerSnapshot::default(), &filters);
        let dir_count = first.snapshot.dir_count();

        // Point the poller at a root that no longer exists.
        let missing = root.join("gone");
        let filters = PollFilters {
            roots: vec![missing],
            excluded_roots: Vec::new(),
            allowed_extensions: HashSet::from([".png".to_string()]),
        };
        let second = run_poll_pass(first.snapshot, &filters);

        assert!(second.degraded);
        assert!(second.removals.is_empty());
        // Snapshot for the unreachable tree is retained, not dropped.
        assert_eq!(second.snapshot.dir_count(), dir_count);
    }

    #[test]
    fn seed_snapshot_applies_filters() {
        let root = PathBuf::from("C:\\watch");
        let filters = PollFilters {
            roots: vec![root.clone()],
            excluded_roots: vec![root.join("excluded")],
            allowed_extensions: HashSet::from([".png".to_string()]),
        };
        let mtime = "2024-01-01T00:00:00".to_string();
        let rows = vec![
            (
                root.join("keep.png").to_string_lossy().to_string(),
                mtime.clone(),
            ),
            (
                root.join("skip.txt").to_string_lossy().to_string(),
                mtime.clone(),
            ),
            (
                root.join("excluded")
                    .join("no.png")
                    .to_string_lossy()
                    .to_string(),
                mtime.clone(),
            ),
            ("C:\\outside\\no.png".to_string(), mtime.clone()),
        ];

        let snapshot = seed_snapshot(&rows, &filters);
        assert_eq!(snapshot.dir_count(), 1);
        let dir = snapshot.dirs.get(&root).unwrap();
        assert_eq!(dir.files.len(), 1);
        assert!(dir.files.contains_key(&OsString::from("keep.png")));
    }
}
