//! The transcode artifact cache: a directory of finished files plus a sidecar
//! SQLite index (docs/video-transcoding-design.md §3).
//!
//! Write order is the whole correctness argument. The encoder writes into
//! `.tmp-<pid>-<key>.<ext>`, the file is fsynced and renamed into place, and
//! only then is the row inserted — an orphan *file* is swept, whereas an
//! orphan *row* would 404 every request for an artifact that looks cached.
//! Startup reconciles both directions and sweeps abandoned temporaries.
//!
//! The pool is standalone rather than one of `db/connection.rs`'s: this
//! database has no relationship to any index or user-data DB, no attachments,
//! and its own migrator.
// The cache lands ahead of the two things that call it: the worker pool
// (pool.rs) commits and negative-caches, and `GET /api/video/artifact` looks
// up. Until they exist the surface is exercised only by this module's tests,
// which is not liveness as far as the bin target's dead-code analysis is
// concerned. Remove this allow with pool.rs.
#![allow(dead_code)]

use anyhow::{Context, Result};
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::jobs::files::{current_iso_timestamp, iso_timestamp_ago};

/// Max pooled connections. The cache is touched once per job and once per
/// artifact request; four covers a reconciliation pass racing a burst.
const MAX_CONNECTIONS: u32 = 4;
/// SQLite busy handler timeout. Writers here are short (one row) and rare.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The MFU nudge (design §3): an entry hit at least this often *and* touched
/// inside [`HOT_WINDOW`] is skipped by the first eviction pass.
const HOT_HIT_COUNT: i64 = 8;
const HOT_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Victims examined per eviction round.
const EVICT_BATCH: i64 = 32;

/// How old an abandoned `.tmp-*` file must be before the sweeper removes it.
/// Young ones may belong to an encode running in another process right now.
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// A second failure settles the verdict; the first only records it. ffmpeg
/// does its own file I/O, where a corrupt file and a dropped mount are
/// indistinguishable (`SKIP_AFTER_AMBIGUOUS`, same reasoning).
const FAILURE_ATTEMPT_LIMIT: i64 = 2;

/// The sidecar database file name; reconciliation must not mistake it (or its
/// WAL sidecars) for an orphaned artifact.
const DB_FILE_NAME: &str = "cache.db";
/// Prefix of an in-progress encode's output.
const TEMP_PREFIX: &str = ".tmp-";

static MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    crate::db::migrations::normalize_line_endings(sqlx::migrate!("migrations/transcode_cache"))
});

/// A cached artifact as the serving path needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedArtifact {
    pub(crate) key: String,
    pub(crate) path: PathBuf,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
}

/// The row to insert once an encode has produced its bytes.
#[derive(Debug, Clone)]
pub(crate) struct NewArtifact<'a> {
    pub(crate) key: &'a str,
    pub(crate) source_sha256: &'a str,
    pub(crate) params_hash: &'a str,
    pub(crate) preset: &'a str,
    pub(crate) file_name: &'a str,
    pub(crate) mime_type: &'a str,
    pub(crate) transcoder_version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheStats {
    pub(crate) entries: i64,
    pub(crate) pinned_entries: i64,
    pub(crate) used_bytes: i64,
    pub(crate) budget_bytes: u64,
    pub(crate) limit_bytes: u64,
}

pub(crate) struct TranscodeCache {
    dir: PathBuf,
    pool: SqlitePool,
    /// Runtime-resizable (`PUT /api/video/cache`), so it is read on every
    /// eviction rather than captured at open time.
    budget_bytes: AtomicU64,
    limit_bytes: u64,
}

impl TranscodeCache {
    /// Opens the cache configured by `[transcode]`, running migrations,
    /// startup reconciliation and the stale-temporary sweep.
    pub(crate) async fn open_from_config() -> Result<Self> {
        let transcode = &crate::config::runtime().transcode;
        Self::open(
            transcode.resolved_cache_dir(),
            transcode.cache_size_mb,
            transcode.cache_size_max_mb,
        )
        .await
    }

    pub(crate) async fn open(dir: PathBuf, budget_mb: u64, limit_mb: u64) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create transcode cache dir {}", dir.display()))?;
        let options = SqliteConnectOptions::new()
            .filename(dir.join(DB_FILE_NAME))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open the transcode cache db in {}", dir.display()))?;
        MIGRATOR
            .run(&pool)
            .await
            .context("failed to migrate the transcode cache db")?;

        let limit_bytes = mb_to_bytes(limit_mb);
        let cache = Self {
            dir,
            pool,
            budget_bytes: AtomicU64::new(mb_to_bytes(budget_mb).min(limit_bytes)),
            limit_bytes,
        };
        cache.reconcile().await?;
        cache.sweep_stale_temp_files();
        Ok(cache)
    }

    /// Where an encode writes before [`commit`](Self::commit) renames it into
    /// place. Pid-tagged so concurrent processes never collide on it and the
    /// sweeper can tell a live temporary from an abandoned one.
    pub(crate) fn temp_path(&self, key: &str, ext: &str) -> PathBuf {
        self.dir
            .join(format!("{TEMP_PREFIX}{}-{key}.{ext}", std::process::id()))
    }

    /// Looks a key up, recording the hit. `None` covers both "never encoded"
    /// and "the row is there but the file is not", which reconciliation would
    /// have cleaned up anyway.
    pub(crate) async fn lookup(&self, key: &str) -> Option<CachedArtifact> {
        let row = sqlx::query("SELECT file_name, mime_type, size_bytes FROM artifacts WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| tracing::warn!(error = %err, "transcode cache lookup failed"))
            .ok()??;
        let file_name: String = row.get(0);
        let path = self.dir.join(&file_name);
        if !path.is_file() {
            tracing::warn!(key, "cached artifact row without a file; dropping the row");
            let _ = self.delete_row(key).await;
            return None;
        }
        self.record_hit(key).await;
        Some(CachedArtifact {
            key: key.to_string(),
            path,
            mime_type: row.get(1),
            size_bytes: row.get(2),
        })
    }

    /// Bumps recency and the hit counter, which together decide eviction
    /// order. Best-effort: a lost bump costs a slightly worse victim choice.
    pub(crate) async fn record_hit(&self, key: &str) {
        if let Err(err) = sqlx::query(
            "UPDATE artifacts SET last_access = ?, hit_count = hit_count + 1 WHERE key = ?",
        )
        .bind(current_iso_timestamp())
        .bind(key)
        .execute(&self.pool)
        .await
        {
            tracing::warn!(error = %err, key, "failed to record a transcode cache hit");
        }
    }

    /// Publishes a finished encode: fsync, rename, then the row. Runs an
    /// eviction pass, which can never choose the artifact just written.
    pub(crate) async fn commit(
        &self,
        new: NewArtifact<'_>,
        temp: &Path,
    ) -> Result<CachedArtifact> {
        // Opened for write, not read: `sync_all` needs write access on
        // Windows (a read handle fails it with ERROR_ACCESS_DENIED).
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(temp)
            .await
            .with_context(|| format!("failed to open the encoded file {}", temp.display()))?;
        let size_bytes = file
            .metadata()
            .await
            .with_context(|| format!("failed to stat the encoded file {}", temp.display()))?
            .len() as i64;
        // The rename is only atomic with respect to bytes that reached the
        // disk; without this an unclean shutdown can leave a correctly named,
        // correctly rowed, truncated artifact.
        file.sync_all()
            .await
            .with_context(|| format!("failed to flush the encoded file {}", temp.display()))?;
        drop(file);

        let path = self.dir.join(new.file_name);
        tokio::fs::rename(temp, &path)
            .await
            .with_context(|| format!("failed to publish the artifact {}", path.display()))?;

        let now = current_iso_timestamp();
        sqlx::query(
            r#"
            INSERT INTO artifacts (
                key, source_sha256, params_hash, preset, file_name, mime_type,
                size_bytes, transcoder_version, created_at, last_access, hit_count, pinned
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
            ON CONFLICT(key) DO UPDATE SET
                file_name = excluded.file_name,
                mime_type = excluded.mime_type,
                size_bytes = excluded.size_bytes,
                last_access = excluded.last_access
            "#,
        )
        .bind(new.key)
        .bind(new.source_sha256)
        .bind(new.params_hash)
        .bind(new.preset)
        .bind(new.file_name)
        .bind(new.mime_type)
        .bind(size_bytes)
        .bind(new.transcoder_version)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await
        .context("failed to record the artifact")?;

        // A verdict is superseded by the bytes that prove it wrong.
        let _ = self.clear_failure(new.key).await;
        self.evict(Some(new.key)).await?;

        Ok(CachedArtifact {
            key: new.key.to_string(),
            path,
            mime_type: new.mime_type.to_string(),
            size_bytes,
        })
    }

    /// The byte budget, in megabytes. Clamped to the configured ceiling and
    /// applied immediately; not persisted (the TOML value is what a restart
    /// returns to), exactly like the search result cache.
    pub(crate) async fn set_budget_mb(&self, size_mb: u64) -> Result<()> {
        self.budget_bytes
            .store(mb_to_bytes(size_mb).min(self.limit_bytes), Ordering::Relaxed);
        self.evict(None).await
    }

    pub(crate) fn budget_bytes(&self) -> u64 {
        self.budget_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn limit_mb(&self) -> u64 {
        self.limit_bytes / (1024 * 1024)
    }

    pub(crate) async fn stats(&self) -> Result<CacheStats> {
        let row = sqlx::query(
            "SELECT COUNT(*), COALESCE(SUM(size_bytes), 0), COALESCE(SUM(pinned), 0) FROM artifacts",
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to read transcode cache stats")?;
        Ok(CacheStats {
            entries: row.get(0),
            used_bytes: row.get(1),
            pinned_entries: row.get(2),
            budget_bytes: self.budget_bytes(),
            limit_bytes: self.limit_bytes,
        })
    }

    /// Records an encode verdict. The **first** one only counts; the caller
    /// short-circuits on the second (see [`Self::known_failure`]).
    ///
    /// Never called for a spawn failure: a missing ffmpeg is a verdict on this
    /// machine, and recording it would suppress the file forever once the
    /// toolchain is installed.
    pub(crate) async fn record_failure(
        &self,
        key: &str,
        source_sha256: &str,
        preset: &str,
        error: &str,
        transcoder_version: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO transcode_failures (
                key, source_sha256, preset, error, attempts, last_attempt, transcoder_version
            )
            VALUES (?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                error = excluded.error,
                attempts = transcode_failures.attempts + 1,
                last_attempt = excluded.last_attempt
            "#,
        )
        .bind(key)
        .bind(source_sha256)
        .bind(preset)
        .bind(error)
        .bind(current_iso_timestamp())
        .bind(transcoder_version)
        .execute(&self.pool)
        .await
        .context("failed to record a transcode failure")?;
        Ok(())
    }

    /// The recorded error once the verdict is settled, so a submit can be
    /// answered without spawning ffmpeg again.
    pub(crate) async fn known_failure(&self, key: &str) -> Option<String> {
        let row = sqlx::query("SELECT error, attempts FROM transcode_failures WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| tracing::warn!(error = %err, "transcode failure lookup failed"))
            .ok()??;
        let attempts: i64 = row.get(1);
        (attempts >= FAILURE_ATTEMPT_LIMIT).then(|| row.get::<String, _>(0))
    }

    async fn clear_failure(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM transcode_failures WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("failed to clear a transcode failure")?;
        Ok(())
    }

    async fn delete_row(&self, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM artifacts WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await
            .context("failed to delete an artifact row")?;
        Ok(())
    }

    /// Startup consistency pass over the two halves of the cache, followed by
    /// one eviction pass (the budget may have shrunk since the last run).
    async fn reconcile(&self) -> Result<()> {
        let rows = sqlx::query("SELECT key, file_name FROM artifacts")
            .fetch_all(&self.pool)
            .await
            .context("failed to read the artifact index")?;
        let mut claimed: HashSet<String> = HashSet::new();
        let mut orphan_rows: Vec<String> = Vec::new();
        for row in rows {
            let key: String = row.get(0);
            let file_name: String = row.get(1);
            if self.dir.join(&file_name).is_file() {
                claimed.insert(file_name);
            } else {
                orphan_rows.push(key);
            }
        }
        for key in &orphan_rows {
            self.delete_row(key).await?;
        }
        if !orphan_rows.is_empty() {
            tracing::info!(
                rows = orphan_rows.len(),
                "dropped transcode cache rows whose artifact file was gone"
            );
        }

        let mut orphan_files = 0usize;
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                // `.tmp-*` belongs to the sweeper (it may be a live encode);
                // cache.db and its WAL sidecars are the index itself.
                if name.starts_with(TEMP_PREFIX) || name.starts_with(DB_FILE_NAME) {
                    continue;
                }
                if !entry.metadata().is_ok_and(|meta| meta.is_file()) || claimed.contains(&name) {
                    continue;
                }
                if std::fs::remove_file(entry.path()).is_ok() {
                    orphan_files += 1;
                }
            }
        }
        if orphan_files > 0 {
            tracing::info!(
                files = orphan_files,
                "removed transcode cache files that no row claimed"
            );
        }

        self.evict(None).await
    }

    /// Removes `.tmp-*` leftovers from crashed encodes: not this process's,
    /// and old enough that no other process can still be writing them.
    fn sweep_stale_temp_files(&self) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        let own_prefix = format!("{TEMP_PREFIX}{}-", std::process::id());
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with(TEMP_PREFIX) || name.starts_with(&own_prefix) {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let stale = meta
                .modified()
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age >= STALE_TEMP_MAX_AGE);
            if stale {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    /// Byte-budgeted LRU with the MFU nudge. `protect` is the key of an
    /// artifact that must survive this pass — the one just committed, which is
    /// by definition the least recently *used* entry even though it is the
    /// one the caller is about to serve.
    async fn evict(&self, protect: Option<&str>) -> Result<()> {
        let budget = self.budget_bytes();
        let mut used = self.used_bytes().await?;
        if used <= budget {
            return Ok(());
        }
        let hot_since = iso_timestamp_ago(HOT_WINDOW);
        let mut skip_hot = true;
        while used > budget {
            let victims = self.evict_candidates(skip_hot, &hot_since, protect).await?;
            if victims.is_empty() {
                if skip_hot {
                    // Everything left is hot: the nudge is a preference, not a
                    // reservation, so the budget wins on the second pass.
                    skip_hot = false;
                    continue;
                }
                tracing::warn!(
                    used_bytes = used,
                    budget_bytes = budget,
                    "transcode cache is over budget with nothing evictable \
                     (pinned artifacts, or one artifact larger than the budget)"
                );
                break;
            }
            for (key, file_name, size_bytes) in victims {
                if used <= budget {
                    break;
                }
                let _ = tokio::fs::remove_file(self.dir.join(&file_name)).await;
                self.delete_row(&key).await?;
                used = used.saturating_sub(size_bytes.max(0) as u64);
            }
        }
        Ok(())
    }

    async fn evict_candidates(
        &self,
        skip_hot: bool,
        hot_since: &str,
        protect: Option<&str>,
    ) -> Result<Vec<(String, String, i64)>> {
        let mut sql = String::from("SELECT key, file_name, size_bytes FROM artifacts WHERE pinned = 0");
        if skip_hot {
            sql.push_str(" AND NOT (hit_count >= ? AND last_access > ?)");
        }
        if protect.is_some() {
            sql.push_str(" AND key <> ?");
        }
        sql.push_str(" ORDER BY last_access ASC LIMIT ?");

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        if skip_hot {
            query = query.bind(HOT_HIT_COUNT).bind(hot_since);
        }
        if let Some(key) = protect {
            query = query.bind(key);
        }
        let rows = query
            .bind(EVICT_BATCH)
            .fetch_all(&self.pool)
            .await
            .context("failed to select eviction candidates")?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    async fn used_bytes(&self) -> Result<u64> {
        let total: i64 = sqlx::query_scalar("SELECT COALESCE(SUM(size_bytes), 0) FROM artifacts")
            .fetch_one(&self.pool)
            .await
            .context("failed to total the transcode cache")?;
        Ok(total.max(0) as u64)
    }
}

fn mb_to_bytes(mb: u64) -> u64 {
    mb.saturating_mul(1024 * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cache with a byte budget expressed directly, so the tests can work
    /// with a handful of bytes instead of megabytes.
    async fn cache_with_budget(dir: &Path, budget_bytes: u64) -> TranscodeCache {
        let cache = TranscodeCache::open(dir.to_path_buf(), 1, 1).await.unwrap();
        cache.budget_bytes.store(budget_bytes, Ordering::Relaxed);
        cache
    }

    /// Writes `bytes` through the real temp-file protocol and commits it.
    async fn commit(cache: &TranscodeCache, key: &str, bytes: &[u8]) -> CachedArtifact {
        let temp = cache.temp_path(key, "mp4");
        std::fs::write(&temp, bytes).unwrap();
        cache
            .commit(
                NewArtifact {
                    key,
                    source_sha256: "sha",
                    params_hash: "hash",
                    preset: "clip",
                    file_name: &format!("{key}.mp4"),
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap()
    }

    async fn touch(cache: &TranscodeCache, key: &str, last_access: &str, hit_count: i64) {
        sqlx::query("UPDATE artifacts SET last_access = ?, hit_count = ? WHERE key = ?")
            .bind(last_access)
            .bind(hit_count)
            .bind(key)
            .execute(&cache.pool)
            .await
            .unwrap();
    }

    async fn keys(cache: &TranscodeCache) -> Vec<String> {
        sqlx::query_scalar("SELECT key FROM artifacts ORDER BY key")
            .fetch_all(&cache.pool)
            .await
            .unwrap()
    }

    /// The write protocol end to end: the temporary is consumed by the
    /// rename, the row records the real byte count, and a lookup both finds
    /// the file and counts the hit.
    #[tokio::test]
    async fn commit_publishes_and_lookup_records_hits() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        let temp = cache.temp_path("k1", "mp4");
        let artifact = commit(&cache, "k1", b"0123456789").await;
        assert!(!temp.exists(), "the temporary is renamed, not copied");
        assert!(artifact.path.is_file());
        assert_eq!(artifact.size_bytes, 10);
        assert_eq!(artifact.mime_type, "video/mp4");

        let hit = cache.lookup("k1").await.expect("a committed key hits");
        assert_eq!(hit, artifact);
        assert!(cache.lookup("nothing").await.is_none());

        let hit_count: i64 = sqlx::query_scalar("SELECT hit_count FROM artifacts WHERE key = 'k1'")
            .fetch_one(&cache.pool)
            .await
            .unwrap();
        assert_eq!(hit_count, 1, "the commit itself is not a hit");
        cache.lookup("k1").await.unwrap();
        let hit_count: i64 = sqlx::query_scalar("SELECT hit_count FROM artifacts WHERE key = 'k1'")
            .fetch_one(&cache.pool)
            .await
            .unwrap();
        assert_eq!(hit_count, 2);

        let stats = cache.stats().await.unwrap();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.used_bytes, 10);
        assert_eq!(stats.pinned_entries, 0);

        // A row whose file vanished is not a hit, and it takes the row with it.
        std::fs::remove_file(&artifact.path).unwrap();
        assert!(cache.lookup("k1").await.is_none());
        assert!(keys(&cache).await.is_empty());
    }

    /// LRU order decides victims, and the artifact that was just committed is
    /// never one of them however far over budget the commit puts the cache.
    #[tokio::test]
    async fn eviction_takes_the_coldest_and_spares_the_new_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 25).await;

        commit(&cache, "old", &[0u8; 10]).await;
        touch(&cache, "old", "2020-01-01T00:00:00", 0).await;
        commit(&cache, "mid", &[0u8; 10]).await;
        touch(&cache, "mid", "2024-01-01T00:00:00", 0).await;
        assert_eq!(keys(&cache).await, ["mid", "old"]);

        // 30 bytes against a 25-byte budget: the coldest goes.
        commit(&cache, "new", &[0u8; 10]).await;
        assert_eq!(keys(&cache).await, ["mid", "new"]);

        // One artifact larger than the whole budget still lands (and warns),
        // because the pass may not delete what it was asked to protect.
        commit(&cache, "huge", &[0u8; 100]).await;
        assert_eq!(keys(&cache).await, ["huge"]);
    }

    /// The MFU nudge skips frequently-used entries on the first pass, and
    /// gives up on skipping when the budget cannot be met without them.
    #[tokio::test]
    async fn hot_entries_are_skipped_until_nothing_else_is_left() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 25).await;

        commit(&cache, "hot", &[0u8; 10]).await;
        touch(&cache, "hot", &current_iso_timestamp(), HOT_HIT_COUNT).await;
        commit(&cache, "cold", &[0u8; 10]).await;
        // Colder than the hot one but *newer* than the eviction cursor would
        // pick if the nudge were ignored.
        touch(&cache, "cold", "2024-01-01T00:00:00", 0).await;

        commit(&cache, "new", &[0u8; 10]).await;
        assert_eq!(
            keys(&cache).await,
            ["hot", "new"],
            "the hot entry outranks an older-but-cold one"
        );

        // Now only hot entries remain besides the protected commit: the
        // fallback pass runs without the hot-skip clause.
        touch(&cache, "new", &current_iso_timestamp(), HOT_HIT_COUNT).await;
        commit(&cache, "newest", &[0u8; 20]).await;
        assert_eq!(keys(&cache).await, ["newest"]);
    }

    /// Pinned rows are the share-link guarantee: never evicted, not even when
    /// the budget cannot otherwise be met.
    #[tokio::test]
    async fn pinned_artifacts_are_never_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 15).await;

        let pinned = commit(&cache, "pinned", &[0u8; 10]).await;
        sqlx::query("UPDATE artifacts SET pinned = 1, last_access = '2000-01-01T00:00:00' WHERE key = 'pinned'")
            .execute(&cache.pool)
            .await
            .unwrap();

        commit(&cache, "new", &[0u8; 10]).await;
        assert_eq!(keys(&cache).await, ["new", "pinned"]);
        assert!(pinned.path.is_file());
    }

    /// Reconciliation closes both halves of a crash: a row whose file never
    /// landed, and a file whose row never did.
    #[tokio::test]
    async fn reconciliation_drops_orphan_rows_and_orphan_files() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = cache_with_budget(dir.path(), 1024).await;
            let kept = commit(&cache, "kept", &[0u8; 10]).await;
            let lost = commit(&cache, "lost", &[0u8; 10]).await;
            std::fs::remove_file(&lost.path).unwrap();
            assert!(kept.path.is_file());
        }
        // A file nobody claims, and a temporary, which reconciliation must
        // leave to the sweeper.
        std::fs::write(dir.path().join("unclaimed.mp4"), b"junk").unwrap();
        let temp = dir.path().join(format!(".tmp-{}-live.mp4", std::process::id()));
        std::fs::write(&temp, b"encoding").unwrap();

        let cache = cache_with_budget(dir.path(), 1024).await;
        assert_eq!(keys(&cache).await, ["kept"]);
        assert!(!dir.path().join("unclaimed.mp4").exists());
        assert!(dir.path().join("kept.mp4").is_file());
        assert!(temp.is_file(), "an in-progress encode is not an orphan file");
    }

    /// The sweeper only takes temporaries that cannot belong to a running
    /// encode: another process's, and old enough.
    #[tokio::test]
    async fn sweeper_spares_own_and_young_temporaries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = TranscodeCache {
            dir: dir.path().to_path_buf(),
            pool: SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            budget_bytes: AtomicU64::new(0),
            limit_bytes: 0,
        };

        let own = cache.temp_path("mine", "mp4");
        let young = dir.path().join(".tmp-999999-young.mp4");
        let old = dir.path().join(".tmp-999999-old.mp4");
        let artifact = dir.path().join("artifact.mp4");
        for path in [&own, &young, &old, &artifact] {
            std::fs::write(path, b"x").unwrap();
        }
        let stale = std::time::SystemTime::now() - STALE_TEMP_MAX_AGE - Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(stale))
            .unwrap();

        cache.sweep_stale_temp_files();
        assert!(own.is_file(), "this process's temporary is a live encode");
        assert!(young.is_file(), "a young temporary may still be written");
        assert!(!old.exists(), "an abandoned temporary is swept");
        assert!(artifact.is_file(), "the sweeper only touches temporaries");
    }

    /// Two-strike: one verdict allows a retry, the second settles it, and a
    /// later success clears the record.
    #[tokio::test]
    async fn failures_need_two_strikes_and_are_cleared_by_success() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        cache
            .record_failure("k1", "sha", "clip", "ffmpeg exited 1", 1)
            .await
            .unwrap();
        assert!(
            cache.known_failure("k1").await.is_none(),
            "one failure is not a verdict"
        );

        cache
            .record_failure("k1", "sha", "clip", "ffmpeg exited 1 again", 1)
            .await
            .unwrap();
        assert_eq!(
            cache.known_failure("k1").await.as_deref(),
            Some("ffmpeg exited 1 again"),
            "the second failure settles it, carrying the latest message"
        );
        assert!(cache.known_failure("other").await.is_none());

        commit(&cache, "k1", &[0u8; 4]).await;
        assert!(
            cache.known_failure("k1").await.is_none(),
            "bytes on disk supersede the verdict"
        );
    }

    /// The budget is runtime-resizable and clamped to the configured ceiling,
    /// and shrinking it evicts immediately.
    #[tokio::test]
    async fn resizing_the_budget_evicts_and_respects_the_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let cache = TranscodeCache::open(dir.path().to_path_buf(), 1, 2)
            .await
            .unwrap();
        assert_eq!(cache.budget_bytes(), 1024 * 1024);
        assert_eq!(cache.limit_mb(), 2);

        commit(&cache, "a", &[0u8; 10]).await;
        touch(&cache, "a", "2020-01-01T00:00:00", 0).await;
        commit(&cache, "b", &[0u8; 10]).await;

        cache.set_budget_mb(0).await.unwrap();
        assert!(keys(&cache).await.is_empty(), "a zero budget empties it");

        // Over the ceiling clamps rather than failing.
        cache.set_budget_mb(64).await.unwrap();
        assert_eq!(cache.budget_bytes(), 2 * 1024 * 1024);
    }
}
