//! The transcode artifact cache: a directory of finished files plus a sidecar
//! SQLite index (docs/video-transcoding-design.md §3).
//!
//! Write order is the whole correctness argument. The encoder writes into
//! `.tmp-<pid>-<nonce>.<ext>`, the file is fsynced and renamed into place, and
//! only then is the row inserted — an orphan *file* is swept, whereas an
//! orphan *row* would 404 every request for an artifact that looks cached.
//! Startup reconciles both directions and sweeps abandoned temporaries.
//!
//! The pool is standalone rather than one of `db/connection.rs`'s: this
//! database has no relationship to any index or user-data DB, no attachments,
//! and its own migrator.
//!
//! One subdirectory lives alongside the artifacts: `share/<key>/<name>`, the
//! humanly named view of an artifact that the OS needs when a path is handed
//! to the clipboard (an artifact file is named `<key>.<ext>`, which is a hash,
//! not a filename). Its entries are hardlinks, they are created on demand, and
//! they are removed with the row that owns them — see
//! [`TranscodeCache::materialize_share`].

use anyhow::{Context, Result};
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::io::Read as _;
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
/// Young ones may belong to an encode running right now, here or in another
/// process; age is the only signal, since a pid is reused after a crash.
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// How old an artifact file no row claims must be before reconciliation
/// removes it. A publish is a rename followed by an insert, so a file that
/// another process is committing right now is briefly rowless.
const UNCLAIMED_ARTIFACT_MIN_AGE: Duration = Duration::from_secs(5 * 60);

/// A second failure settles the verdict; the first only records it. ffmpeg
/// does its own file I/O, where a corrupt file and a dropped mount are
/// indistinguishable (`SKIP_AFTER_AMBIGUOUS`, same reasoning).
const FAILURE_ATTEMPT_LIMIT: i64 = 2;

/// The sidecar database file name; reconciliation must not mistake it (or its
/// WAL sidecars) for an orphaned artifact.
const DB_FILE_NAME: &str = "cache.db";
/// Prefix of an in-progress encode's output.
const TEMP_PREFIX: &str = ".tmp-";
/// Subdirectory holding the named views of artifacts (`share/<key>/<name>`).
/// Reconciliation must never mistake it for an artifact, and nothing outside
/// [`TranscodeCache::materialize_share`] and the removal paths writes into it.
const SHARE_DIR_NAME: &str = "share";

/// Read buffer for hashing a finished artifact. The file was written moments
/// ago and is still in the page cache, so this is a sequential read of warm
/// pages, not an I/O-bound pass worth tuning further.
const HASH_BUFFER_BYTES: usize = 64 * 1024;

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
    /// The preset that produced it. Carried so the serving path can name the
    /// download without re-deriving anything from the key, which encodes the
    /// *hash* of the settings, not the settings.
    pub(crate) preset: String,
    /// Lowercase hex sha256 of the **artifact's own** bytes, computed by
    /// [`TranscodeCache::commit`] from the finished file. Not the source hash
    /// the key carries: a receiver that verifies what it was handed needs the
    /// digest of the encoded output.
    ///
    /// `None` for a row committed before the column existed. Every consumer
    /// must treat that as "no integrity claim", never as a mismatch.
    pub(crate) sha256: Option<String>,
    /// The name a download of this artifact carries, as computed at publish
    /// time. `None` for pre-column rows, where the on-disk `<key>.<ext>` name
    /// is the only name there is.
    pub(crate) download_name: Option<String>,
}

/// The row to insert once an encode has produced its bytes.
#[derive(Debug, Clone)]
pub(crate) struct NewArtifact<'a> {
    pub(crate) key: &'a str,
    pub(crate) source_sha256: &'a str,
    pub(crate) params_hash: &'a str,
    pub(crate) preset: &'a str,
    pub(crate) file_name: &'a str,
    /// The human name a download carries. Stored rather than recomputed
    /// because it cannot be rebuilt from the row: it depends on the request's
    /// path, which the key does not encode.
    ///
    /// First submitter wins, and that is *weaker* than it looks: two files
    /// with identical bytes and identical settings share one cache key, so
    /// this is one of their stems, chosen by whoever encoded first. It is not
    /// what `ArtifactRef::filename` carries either — that is rebuilt from the
    /// current request's stem on every cache hit, and only a *joined* in-flight
    /// job inherits the first submitter's name. Treat this column as the
    /// fallback for callers with no request context to name the file from
    /// (see [`TranscodeCache::materialize_share`]).
    pub(crate) download_name: &'a str,
    pub(crate) mime_type: &'a str,
    pub(crate) transcoder_version: i64,
}

/// Why a resize was refused. The two cases are different answers to the
/// client: the requested size being over the configured ceiling is the
/// client's own number and is echoed back with a 422, while a failure of the
/// eviction pass behind it is this machine's problem — and its `anyhow` chain
/// names the cache directory, which no HTTP response should carry.
#[derive(Debug)]
pub(crate) enum ResizeError {
    AboveCeiling(String),
    Internal(anyhow::Error),
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
        cache.sweep_stale_temp_files().await;
        Ok(cache)
    }

    /// Where an encode writes before [`commit`](Self::commit) renames it into
    /// place. The nonce, not the key, is what makes it unique: two encodes of
    /// the same key (a retry racing the job that abandoned it, or two
    /// processes) must not share a temporary. Nothing reads the name back.
    pub(crate) fn temp_path(&self, ext: &str) -> PathBuf {
        self.dir.join(format!(
            "{TEMP_PREFIX}{}-{}.{ext}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ))
    }

    /// Looks a key up, recording the hit. `None` covers both "never encoded"
    /// and "the row is there but the file is not", which reconciliation would
    /// have cleaned up anyway.
    pub(crate) async fn lookup(&self, key: &str) -> Option<CachedArtifact> {
        let row = sqlx::query(
            "SELECT file_name, mime_type, size_bytes, preset, sha256, download_name \
             FROM artifacts WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| tracing::warn!(error = %err, "transcode cache lookup failed"))
        .ok()??;
        let file_name: String = row.get(0);
        let path = self.dir.join(&file_name);
        if !tokio::fs::metadata(&path)
            .await
            .is_ok_and(|meta| meta.is_file())
        {
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
            preset: row.get(3),
            sha256: row.get(4),
            download_name: row.get(5),
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

    /// The filesystem half of [`commit`](Self::commit): fsync, hash, rename.
    /// Separated so its caller can clean the temporary up on any failure —
    /// once this returns `Ok` the temporary is gone, having become the
    /// artifact. Returns the published path, its size and the hex sha256 of
    /// its bytes, the last of which is `None` if hashing failed.
    ///
    /// The hash is taken here, from the finished temporary, rather than
    /// anywhere a caller could supply it: it must describe the bytes that were
    /// actually published, and a value threaded in from an encoder can drift
    /// from them (a retry, a joined job, a truncated write) with nothing to
    /// catch it.
    async fn publish(
        &self,
        key: &str,
        temp: &Path,
        file_name: &str,
    ) -> Result<(PathBuf, i64, Option<String>)> {
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
        // Dropped before the hashing read handle rather than kept alongside
        // it: a second handle on the same file is legal everywhere, but there
        // is no reason to hold a writable one across the read.
        drop(file);

        // Deliberately non-fatal. The bytes are finished and the encode behind
        // them can have run for minutes; a read that fails *here* — antivirus
        // holding the handle open, an SMB hiccup, a mount that blinked — is no
        // reason to discard them, for a column whose own contract says NULL is
        // the normal "makes no integrity claim" state. The rename below still
        // happens, and a later re-commit heals the row.
        let to_hash = temp.to_path_buf();
        let sha256 = match tokio::task::spawn_blocking(move || hash_file(&to_hash)).await {
            Ok(Ok(digest)) => Some(digest),
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    key,
                    file = %temp.display(),
                    "failed to hash a finished artifact; publishing it without a digest"
                );
                None
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    key,
                    file = %temp.display(),
                    "the artifact hashing task failed; publishing it without a digest"
                );
                None
            }
        };

        let path = self.dir.join(file_name);
        tokio::fs::rename(temp, &path)
            .await
            .with_context(|| format!("failed to publish the artifact {}", path.display()))?;
        Ok((path, size_bytes, sha256))
    }

    /// Publishes a finished encode: fsync, rename, then the row. Runs an
    /// eviction pass, which can never choose the artifact just written.
    pub(crate) async fn commit(
        &self,
        new: NewArtifact<'_>,
        temp: &Path,
    ) -> Result<CachedArtifact> {
        let (path, size_bytes, sha256) = match self.publish(new.key, temp, new.file_name).await {
            Ok(published) => published,
            Err(err) => {
                // Everything up to the rename leaves the temporary behind,
                // and no one else will claim it for a day.
                let _ = tokio::fs::remove_file(temp).await;
                return Err(err);
            }
        };

        let now = current_iso_timestamp();
        sqlx::query(
            r#"
            INSERT INTO artifacts (
                key, source_sha256, params_hash, preset, file_name, mime_type,
                size_bytes, transcoder_version, created_at, last_access, hit_count, pinned,
                sha256, download_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                file_name = excluded.file_name,
                mime_type = excluded.mime_type,
                size_bytes = excluded.size_bytes,
                last_access = excluded.last_access,
                -- Re-committing the same key republishes the bytes, so the
                -- digest and the name are refreshed with them. This is the
                -- only way a pre-column (or hash-failed) row can heal, and in
                -- single-process operation it is only reachable *after*
                -- `lookup` has dropped the row for a missing file — the
                -- conflict itself is a second process, or a race with that
                -- repair, rather than something a re-encode normally hits.
                sha256 = excluded.sha256,
                download_name = excluded.download_name
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
        .bind(sha256.as_deref())
        .bind(new.download_name)
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
            preset: new.preset.to_string(),
            sha256,
            download_name: Some(new.download_name.to_string()),
        })
    }

    /// A path to `artifact`'s bytes under a *human* file name, for handing to
    /// something that reads the name — the OS clipboard, a drag payload, a
    /// file manager. The artifact itself is named `<key>.<ext>`, which is a
    /// hash: correct for a content-addressed store, useless as a filename.
    ///
    /// The entry is `share/<key>/<sanitized name>`, a hardlink to the
    /// artifact. Hardlinking is the point: it is atomic, costs no bytes, and
    /// cannot desynchronize from the artifact's content. Filesystems that
    /// refuse it (a ReFS dedup edge, an exFAT stick, a Samba mount without
    /// unix extensions) fall back to a copy through a temporary, so a crash
    /// mid-copy can never leave a short file under the final name.
    ///
    /// `download_name` is the *caller's* name for this request, and it wins
    /// over the stored one. The stored column is the first submitter's stem,
    /// and one cache key can be shared by several byte-identical sources — so
    /// pasting the stored name would hand the user another file's name. A
    /// caller with no request to name the file from passes `None` and gets the
    /// stored one. Several names per key simply coexist under `share/<key>/`
    /// and die together with the directory.
    ///
    /// **Not accounted against the cache budget, deliberately.** In the normal
    /// hardlink case there are no extra bytes to account for; in the fallback
    /// case there are, but they are bounded by the artifact set (one copy per
    /// artifact, at most) and every one of them is removed with the row that
    /// owns it. Charging them would make the budget depend on which filesystem
    /// the cache landed on, for a quantity that cannot outlive the artifacts
    /// the budget already covers.
    // Exercised by the tests below; the HTTP route that hands the path to the
    // host clipboard is the next step of this feature.
    #[allow(dead_code)]
    pub(crate) async fn materialize_share(
        &self,
        artifact: &CachedArtifact,
        download_name: Option<&str>,
    ) -> Result<PathBuf> {
        let dir = self.share_dir(&artifact.key);
        // Computed from the directory this entry lands in, not from the
        // component limit alone: see [`share_name_ceiling`].
        let ceiling = share_name_ceiling(&dir);
        let name = download_name
            .or(artifact.download_name.as_deref())
            // Below the floor there is no room for a name worth reading, so
            // the fallback below (short, and already known to fit next to the
            // artifact) is the better answer than a two-character stem.
            .filter(|_| ceiling >= MIN_SHARE_NAME_BYTES)
            .and_then(|name| sanitize_share_name_within(name, ceiling))
            // A pre-column row, or a name that sanitized away entirely: the
            // on-disk `<key>.<ext>` is unlovely but it is a real name, and it
            // is safe by construction (hex plus an extension).
            .or_else(|| {
                artifact
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| artifact.key.clone());

        let target = dir.join(&name);
        // Same key means the same bytes (the key is content-addressed on both
        // halves), so a full-size entry is already the right answer. A
        // *wrong*-size one is a truncated leftover from a crashed copy — the
        // only way this path can produce a short file — and is replaced.
        match tokio::fs::metadata(&target).await {
            Ok(meta) if meta.is_file() && meta.len() == artifact.size_bytes.max(0) as u64 => {
                return Ok(target);
            }
            Ok(_) => {
                let _ = tokio::fs::remove_file(&target).await;
            }
            Err(_) => {}
        }

        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("failed to create the share dir {}", dir.display()))?;

        match tokio::fs::hard_link(&artifact.path, &target).await {
            Ok(()) => return Ok(target),
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    key = artifact.key,
                    "hardlinking a share entry failed; copying instead"
                );
            }
        }

        copy_into_place(&artifact.path, &target, &dir).await?;
        Ok(target)
    }

    fn share_dir(&self, key: &str) -> PathBuf {
        self.dir.join(SHARE_DIR_NAME).join(key)
    }

    /// Drops the named views of an artifact that is going away. Best-effort:
    /// the share entries are derived state, and a leftover directory is
    /// reclaimed by the next reconciliation, whereas failing a delete here
    /// would leave the row and its file in an inconsistent pair.
    async fn remove_share_dir(&self, key: &str) {
        let dir = self.share_dir(key);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(error = %err, key, "failed to remove a transcode share directory");
            }
        }
    }

    /// The byte budget, in megabytes. Applied immediately and not persisted
    /// (the TOML value is what a restart returns to), exactly like the search
    /// result cache. A request above the configured ceiling is refused rather
    /// than silently clamped, so the resize route can answer 422.
    pub(crate) async fn set_budget_mb(
        &self,
        size_mb: u64,
    ) -> std::result::Result<(), ResizeError> {
        let requested = mb_to_bytes(size_mb);
        if requested > self.limit_bytes {
            return Err(ResizeError::AboveCeiling(format!(
                "size_mb {size_mb} exceeds the transcode.cache_size_max_mb ceiling of {} MB",
                self.limit_mb()
            )));
        }
        self.budget_bytes.store(requested, Ordering::Relaxed);
        self.evict(None).await.map_err(ResizeError::Internal)
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

    /// Empties the cache: every **unpinned** artifact and, optionally, the
    /// recorded verdicts. Pinned rows survive by definition — they are the
    /// share-link guarantee, and an admin clear is not a revocation.
    /// Returns how many artifacts were removed.
    pub(crate) async fn clear(&self, include_failures: bool) -> Result<usize> {
        let rows = sqlx::query("SELECT key, file_name FROM artifacts WHERE pinned = 0")
            .fetch_all(&self.pool)
            .await
            .context("failed to list the artifacts to clear")?;
        let mut removed = 0usize;
        for row in rows {
            let key: String = row.get(0);
            let file_name: String = row.get(1);
            // File first, then the row: an orphan file is swept, an orphan
            // row would 404 every request for it.
            let _ = tokio::fs::remove_file(self.dir.join(&file_name)).await;
            self.delete_row(&key).await?;
            removed += 1;
        }
        if include_failures {
            sqlx::query("DELETE FROM transcode_failures")
                .execute(&self.pool)
                .await
                .context("failed to clear the transcode verdicts")?;
        }
        Ok(removed)
    }

    /// Removes a row and everything derived from it. Every removal path — the
    /// eviction pass, [`clear`](Self::clear), the row-without-file repair in
    /// [`lookup`](Self::lookup), reconciliation's orphan rows — funnels here,
    /// so the share entries are dropped in one place instead of four.
    async fn delete_row(&self, key: &str) -> Result<()> {
        self.remove_share_dir(key).await;
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
        let indexed: Vec<(String, String)> = rows
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();

        let dir = self.dir.clone();
        let scan = tokio::task::spawn_blocking(move || scan_for_orphans(&dir, indexed))
            .await
            .context("the transcode cache reconciliation scan failed")?;

        for key in &scan.orphan_rows {
            self.delete_row(key).await?;
        }
        if !scan.orphan_rows.is_empty() {
            tracing::info!(
                rows = scan.orphan_rows.len(),
                "dropped transcode cache rows whose artifact file was gone"
            );
        }
        if scan.orphan_files > 0 {
            tracing::info!(
                files = scan.orphan_files,
                "removed transcode cache files that no row claimed"
            );
        }
        if scan.orphan_shares > 0 {
            tracing::info!(
                shares = scan.orphan_shares,
                "removed transcode share directories that no row claimed"
            );
        }

        self.evict(None).await
    }

    /// Removes `.tmp-*` leftovers from crashed encodes. Runs at open and,
    /// opportunistically, after every finished job: a gateway that stays up
    /// for weeks would otherwise never sweep at all.
    pub(crate) async fn sweep_stale_temp_files(&self) {
        let dir = self.dir.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || sweep_stale_temps(&dir)).await {
            tracing::warn!(error = %err, "the transcode cache temporary sweep failed");
        }
    }

    /// Byte-budgeted LRU with the MFU nudge. `protect` is the key of an
    /// artifact that must survive this pass. It is the entry just committed,
    /// which recency alone would already spare — the case this excludes is the
    /// artifact that is on its own bigger than the whole budget, where the
    /// pass must warn rather than delete the bytes the caller is about to
    /// serve.
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

/// What one reconciliation pass found and fixed.
#[derive(Debug, Default)]
struct OrphanScan {
    /// Keys whose artifact file is gone; the caller deletes the rows.
    orphan_rows: Vec<String>,
    /// Artifact files removed because no row claimed them.
    orphan_files: usize,
    /// `share/<key>` directories removed because no row claimed the key.
    orphan_shares: usize,
}

/// The filesystem half of [`TranscodeCache::reconcile`], as one blocking pass
/// over the directory: stats every indexed file, removes the files no row
/// claims, then does the same for the share directories.
fn scan_for_orphans(dir: &Path, indexed: Vec<(String, String)>) -> OrphanScan {
    let mut claimed: HashSet<String> = HashSet::new();
    let mut live_keys: HashSet<String> = HashSet::new();
    let mut scan = OrphanScan::default();
    for (key, file_name) in indexed {
        if dir.join(&file_name).is_file() {
            claimed.insert(file_name);
            live_keys.insert(key);
        } else {
            scan.orphan_rows.push(key);
        }
    }

    let Ok(entries) = std::fs::read_dir(dir) else {
        return scan;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        // `.tmp-*` belongs to the sweeper (it may be a live encode);
        // cache.db and its WAL sidecars are the index itself.
        if name.starts_with(TEMP_PREFIX) || name.starts_with(DB_FILE_NAME) {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        // Directories are never artifacts. `share/` is the one this cache
        // creates itself, and it is swept below by key rather than by name —
        // deleting it here would destroy live entries on every startup.
        if !meta.is_file() || claimed.contains(&name) {
            continue;
        }
        // Another process publishes by renaming into place and only then
        // inserting the row; a young rowless file may be inside that window.
        let settled = meta
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age >= UNCLAIMED_ARTIFACT_MIN_AGE);
        if settled && std::fs::remove_file(entry.path()).is_ok() {
            scan.orphan_files += 1;
        }
    }

    scan.orphan_shares = sweep_orphan_shares(&dir.join(SHARE_DIR_NAME), &live_keys);
    scan
}

/// Removes `share/<key>` directories whose key is not in the index, and
/// reclaims crashed copy-fallback temporaries from the ones that stay.
///
/// The age guard on removal is the same rule the rowless artifact files get,
/// for the same reason: `live_keys` is a snapshot of *this* process's index
/// read, and a second process sharing the cache directory (the Desktop sidecar
/// alongside a separately started server) can commit a key and materialize
/// `share/<key>/...` between that read and this sweep. Deleting it would hand
/// the other process's clipboard a path that no longer exists. A rowless share
/// directory older than the publish window is what it looks like — debris from
/// an eviction that could not finish.
///
/// The temporary sweep runs over every directory, including the live ones: the
/// root `.tmp-*` sweep does not recurse, so a copy fallback that died mid-write
/// would otherwise leak forever on exactly the filesystems that need the
/// fallback, and those bytes are outside the cache budget.
fn sweep_orphan_shares(share_dir: &Path, live_keys: &HashSet<String>) -> usize {
    let Ok(entries) = std::fs::read_dir(share_dir) else {
        return 0;
    };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        let is_dir = entry.file_type().is_ok_and(|kind| kind.is_dir());
        let settled = !live_keys.contains(&name)
            && entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age >= UNCLAIMED_ARTIFACT_MIN_AGE);
        if !settled {
            if is_dir {
                sweep_stale_temps(&path);
            }
            continue;
        }
        let outcome = if is_dir {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        if outcome.is_ok() {
            removed += 1;
        }
    }
    removed
}

/// Writes `from` to `to` through a temporary in `share_dir`, so a crash
/// mid-copy can never leave a short file under the final name — the name a
/// caller is about to hand to the clipboard.
///
/// Extracted from [`TranscodeCache::materialize_share`] because it is
/// otherwise untestable: hardlinking succeeds on every filesystem a test can
/// create, so the fallback is dead code on the machines that run the suite and
/// live code exactly where nobody is watching (exFAT, SMB without unix
/// extensions).
async fn copy_into_place(from: &Path, to: &Path, share_dir: &Path) -> Result<()> {
    let temp = share_dir.join(format!(
        "{TEMP_PREFIX}{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let copied: Result<()> = async {
        tokio::fs::copy(from, &temp)
            .await
            .with_context(|| format!("failed to copy the artifact {} for sharing", from.display()))?;
        tokio::fs::rename(&temp, to)
            .await
            .with_context(|| format!("failed to publish the share entry {}", to.display()))
    }
    .await;
    if let Err(err) = copied {
        // Best effort, and not the only line of defence: this process may not
        // survive the failure that produced the temporary. Reconciliation
        // sweeps aged `.tmp-*` out of every share directory for that case.
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(err);
    }
    Ok(())
}

/// Per-component byte ceiling of every filesystem this cache can land on.
/// Windows, ext4, APFS and ZFS all stop at 255.
const MAX_SHARE_NAME_BYTES: usize = 255;

/// `MAX_PATH` (260) minus the terminating NUL: the longest path a
/// non-long-path-aware consumer can open.
const MAX_SHARE_PATH_BYTES: usize = 259;

/// Below this there is no name left worth pasting, only a truncated stem and
/// an extension, so the ceiling logic gives up and takes the artifact's own
/// `<key>.<ext>` name instead.
const MIN_SHARE_NAME_BYTES: usize = 16;

/// The longest name a share entry may carry *in this directory*.
///
/// `share/<key>/` alone is ~97 characters on top of the cache directory, so a
/// 255-byte name under a normal Windows profile path crosses `MAX_PATH`. Rust's
/// std reaches past that transparently (it prefixes `\\?\`), which is exactly
/// why the cap has to be applied deliberately here: the whole purpose of this
/// path is to be handed to something *else* — Explorer, a file dialog, a drag
/// payload, a `cmd.exe` invocation — and most of those are still `MAX_PATH`
/// bound. The `+ 1` is the separator between the directory and the name.
///
/// This budget applies to this one materialization; [`MAX_SHARE_NAME_BYTES`]
/// remains the sanitizer's own ceiling everywhere else, including the mirror
/// in `share_cache.rs`.
fn share_name_ceiling(dir: &Path) -> usize {
    MAX_SHARE_NAME_BYTES.min(MAX_SHARE_PATH_BYTES.saturating_sub(dir.as_os_str().len() + 1))
}

/// Characters that cannot appear in a Windows path component, plus the control
/// range (which no platform wants in a filename).
const INVALID_FILENAME_CHARS: &[char] = &['/', '\\', ':', '*', '?', '"', '<', '>', '|'];

/// Windows device names. Reserved with or without an extension, in any case.
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Reduces a download name to one safe path component for `share/<key>/`.
///
/// **This mirrors `panoptikon-desktop/src-tauri/src/share_cache.rs`'s
/// `sanitize_filename` and must keep the same observable behavior** — the two
/// sides name the same file for the same clipboard, and a name that differs
/// between them turns a paste into a file the user did not expect. It is
/// duplicated rather than shared because the two crates have no dependency
/// edge and one three-rule sanitizer does not justify creating one.
///
/// The rules, all of them load-bearing: path separators and the other Windows
/// invalid characters are dropped (never substituted, which could collide two
/// distinct names into one), control characters go with them, `%` becomes `_`
/// because a `cmd.exe` clipboard invocation expands `%NAME%` even inside a
/// quoted region, trailing dots and spaces are trimmed (Windows strips them
/// silently, which would desynchronize the stored name from the looked-up
/// one), and Windows device names are prefixed rather than rejected.
///
/// `None` means nothing usable survived; the caller falls back to the
/// artifact's own `<key>.<ext>` name, which is always addressable. That is the
/// local equivalent of share_cache's hash-prefix fallback — this side has a
/// real on-disk name to fall back to, so it does not synthesize one.
///
/// `ceiling` is the length budget, a parameter only because a share entry has
/// a tighter one than the filesystem's: its directory has already eaten part
/// of a `MAX_PATH` budget (see [`share_name_ceiling`]). Every other rule is
/// unconditional, and the mirror in `share_cache.rs` corresponds to this
/// function at `ceiling == MAX_SHARE_NAME_BYTES`.
fn sanitize_share_name_within(name: &str, ceiling: usize) -> Option<String> {
    let cleaned = strip_unsafe(name);
    if cleaned.is_empty() {
        return None;
    }
    let stem = cleaned.split('.').next().unwrap_or_default();
    let escaped = if RESERVED_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
    {
        // Escaped before truncation, so the underscore is inside the ceiling
        // rather than pushing the result past it.
        format!("_{cleaned}")
    } else {
        cleaned
    };
    truncate_share_name(escaped, ceiling)
}

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

/// Caps one path component at `ceiling` bytes, cutting the stem at a UTF-8
/// boundary and keeping the extension — which is what decides how the pasted
/// file is treated by whatever receives it. `ceiling` is
/// [`MAX_SHARE_NAME_BYTES`] except where the containing directory shortens it
/// (see [`share_name_ceiling`]); it is a parameter rather than a constant so
/// the cut can be exercised without building a path of a platform-dependent
/// length.
fn truncate_share_name(name: String, ceiling: usize) -> Option<String> {
    if name.len() <= ceiling {
        return Some(name);
    }
    let (stem, extension) = match name.rfind('.') {
        // A leading dot is part of the stem, not an extension separator.
        Some(index) if index > 0 => (&name[..index], &name[index..]),
        _ => (name.as_str(), ""),
    };
    // An "extension" that alone fills the budget is not one worth keeping.
    let extension = if extension.len() > ceiling / 2 {
        ""
    } else {
        extension
    };
    let mut end = stem.len().min(ceiling - extension.len());
    while end > 0 && !stem.is_char_boundary(end) {
        end -= 1;
    }
    let truncated = format!("{}{extension}", &stem[..end]);
    // Every byte of the stem was a multi-byte character that did not fit.
    (!truncated.is_empty() && truncated != extension).then_some(truncated)
}

/// Streams a file into a sha256, returning the lowercase hex digest.
fn hash_file(path: &Path) -> std::io::Result<String> {
    use sha2::Digest as _;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Removes `.tmp-*` leftovers from one directory: crashed encodes in the cache
/// root, crashed copy fallbacks in a `share/<key>` directory
/// ([`copy_into_place`]). Age is the whole test: this process's own pid is no
/// evidence of a live write, because a crashed run's pid can be handed back
/// out. Non-recursive by design — every level that has temporaries is walked
/// explicitly, so the sweep can never descend into a live share entry.
fn sweep_stale_temps(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(TEMP_PREFIX) {
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

    /// Writes `bytes` into `temp` and commits it under `key`, carrying the
    /// name a download of it would use.
    async fn commit_named(
        cache: &TranscodeCache,
        key: &str,
        bytes: &[u8],
        temp: &Path,
        download_name: &str,
    ) -> Result<CachedArtifact> {
        std::fs::write(temp, bytes).unwrap();
        cache
            .commit(
                NewArtifact {
                    key,
                    source_sha256: "sha",
                    params_hash: "hash",
                    preset: "clip",
                    file_name: &format!("{key}.mp4"),
                    download_name,
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                temp,
            )
            .await
    }

    /// Writes `bytes` into `temp` and commits it under `key`.
    async fn commit_temp(
        cache: &TranscodeCache,
        key: &str,
        bytes: &[u8],
        temp: &Path,
    ) -> Result<CachedArtifact> {
        commit_named(cache, key, bytes, temp, &format!("{key}-clip.mp4")).await
    }

    /// Writes `bytes` through the real temp-file protocol and commits it.
    async fn commit(cache: &TranscodeCache, key: &str, bytes: &[u8]) -> CachedArtifact {
        let temp = cache.temp_path("mp4");
        commit_temp(cache, key, bytes, &temp).await.unwrap()
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

        let temp = cache.temp_path("mp4");
        let artifact = commit_temp(&cache, "k1", b"0123456789", &temp)
            .await
            .unwrap();
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

    /// A commit that cannot publish takes its temporary with it: nothing else
    /// would remove it for a day, and the encode that wrote it is over.
    #[tokio::test]
    async fn a_failed_commit_removes_its_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        // A directory where the artifact should land: the rename cannot win.
        std::fs::create_dir(dir.path().join("blocked.mp4")).unwrap();
        let temp = cache.temp_path("mp4");
        let err = commit_temp(&cache, "blocked", &[0u8; 10], &temp)
            .await
            .expect_err("publishing over a directory fails");
        assert!(format!("{err:#}").contains("failed to publish"), "{err:#}");
        assert!(!temp.exists(), "the temporary does not outlive the commit");
        assert!(keys(&cache).await.is_empty(), "and no row was written");
    }

    /// Two encodes of the same key never share a temporary, so a retry cannot
    /// overwrite the file another attempt is still writing.
    #[tokio::test]
    async fn temp_paths_are_unique_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;
        let first = cache.temp_path("mp4");
        let second = cache.temp_path("mp4");
        assert_ne!(first, second);
        for path in [&first, &second] {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            assert!(name.starts_with(TEMP_PREFIX), "{name}");
            assert!(name.ends_with(".mp4"), "{name}");
        }
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

        // The hot entry is the *older* of the two, so plain LRU would take it
        // and leave "cold": only the nudge produces the opposite verdict.
        commit(&cache, "hot", &[0u8; 10]).await;
        touch(
            &cache,
            "hot",
            &iso_timestamp_ago(Duration::from_secs(3 * 24 * 60 * 60)),
            HOT_HIT_COUNT,
        )
        .await;
        commit(&cache, "cold", &[0u8; 10]).await;
        touch(&cache, "cold", &current_iso_timestamp(), 0).await;

        // 30 bytes against 25: exactly one entry goes.
        commit(&cache, "new", &[0u8; 10]).await;
        assert_eq!(
            keys(&cache).await,
            ["hot", "new"],
            "the hot entry outranks a colder-but-newer one; LRU alone would \
             have evicted it instead"
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

    /// Backdates a file's mtime by `age`, so an age guard sees it as settled.
    fn backdate(path: &Path, age: Duration) {
        let when = std::time::SystemTime::now() - age;
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    /// [`backdate`] for a *directory*, which cannot simply be opened for
    /// writing: Windows needs the backup-semantics flag before `CreateFile`
    /// will accept one at all, and attribute-write access is all `SetFileTime`
    /// wants; Unix opens it read-only, where `futimens` does not need more.
    fn backdate_dir(path: &Path, age: Duration) {
        let when = std::time::SystemTime::now() - age;
        let mut options = std::fs::File::options();
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
            options
                .access_mode(FILE_WRITE_ATTRIBUTES)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
        }
        #[cfg(not(windows))]
        {
            options.read(true);
        }
        options
            .open(path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    /// Reconciliation closes both halves of a crash: a row whose file never
    /// landed, and a file whose row never did — the latter only once the file
    /// is too old to be another process's publish still on its way to a row.
    #[tokio::test]
    async fn reconciliation_drops_orphan_rows_and_settled_orphan_files() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cache = cache_with_budget(dir.path(), 1024).await;
            let kept = commit(&cache, "kept", &[0u8; 10]).await;
            let lost = commit(&cache, "lost", &[0u8; 10]).await;
            std::fs::remove_file(&lost.path).unwrap();
            assert!(kept.path.is_file());
        }
        // Two files nobody claims — one from a crash, one that may be mid
        // publish right now — and a temporary, which is the sweeper's.
        let abandoned = dir.path().join("unclaimed-old.mp4");
        let publishing = dir.path().join("unclaimed-new.mp4");
        for path in [&abandoned, &publishing] {
            std::fs::write(path, b"junk").unwrap();
        }
        backdate(&abandoned, UNCLAIMED_ARTIFACT_MIN_AGE + Duration::from_secs(60));
        let temp = dir.path().join(format!(".tmp-{}-live.mp4", std::process::id()));
        std::fs::write(&temp, b"encoding").unwrap();

        let cache = cache_with_budget(dir.path(), 1024).await;
        assert_eq!(keys(&cache).await, ["kept"]);
        assert!(!abandoned.exists(), "a settled rowless file is an orphan");
        assert!(
            publishing.is_file(),
            "a fresh rowless file may be a publish that has not inserted yet"
        );
        assert!(dir.path().join("kept.mp4").is_file());
        assert!(temp.is_file(), "an in-progress encode is not an orphan file");
    }

    /// The sweeper's only test is age: a pid is reused after a crash, so even
    /// this process's own prefix proves nothing about a running encode.
    #[tokio::test]
    async fn sweeper_takes_old_temporaries_whatever_the_pid() {
        let dir = tempfile::tempdir().unwrap();

        let own_old = dir.path().join(format!(".tmp-{}-old.mp4", std::process::id()));
        let own_young = dir
            .path()
            .join(format!(".tmp-{}-young.mp4", std::process::id()));
        let other_old = dir.path().join(".tmp-999999-old.mp4");
        let artifact = dir.path().join("artifact.mp4");
        for path in [&own_old, &own_young, &other_old, &artifact] {
            std::fs::write(path, b"x").unwrap();
        }
        for path in [&own_old, &other_old] {
            backdate(path, STALE_TEMP_MAX_AGE + Duration::from_secs(60));
        }

        sweep_stale_temps(dir.path());
        assert!(!own_old.exists(), "our own pid does not spare an old one");
        assert!(!other_old.exists(), "an abandoned temporary is swept");
        assert!(own_young.is_file(), "a young temporary may still be written");
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

    /// The budget is runtime-resizable, shrinking it evicts immediately, and
    /// the configured ceiling is a rejection rather than a silent clamp.
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

        // Over the ceiling is refused, and refused *distinguishably*: the
        // route answers 422 only for this case, never for a failed eviction.
        let err = cache
            .set_budget_mb(64)
            .await
            .expect_err("the ceiling is enforced, not clamped to");
        let ResizeError::AboveCeiling(detail) = &err else {
            panic!("expected a ceiling rejection, got {err:?}");
        };
        assert!(detail.contains("cache_size_max_mb"), "{detail}");
        assert_eq!(cache.budget_bytes(), 0, "a refused resize changes nothing");

        cache.set_budget_mb(2).await.unwrap();
        assert_eq!(cache.budget_bytes(), 2 * 1024 * 1024);
    }

    /// sha256 of the ASCII bytes `0123456789`, so the recorded digest is
    /// checked against an independently known value rather than against a
    /// second run of the same hasher.
    const DIGEST_0123456789: &str =
        "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882";

    fn share_entries(cache: &TranscodeCache, key: &str) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(cache.share_dir(key)) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// The digest describes the *artifact's* bytes, not the source's, and the
    /// download name survives the round trip. A row from before the columns
    /// existed reads back as two `None`s rather than failing the query.
    #[tokio::test]
    async fn commit_records_the_artifacts_own_hash_and_download_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        let temp = cache.temp_path("mp4");
        let artifact = commit_named(&cache, "k1", b"0123456789", &temp, "holiday-clip.mp4")
            .await
            .unwrap();
        assert_eq!(artifact.sha256.as_deref(), Some(DIGEST_0123456789));
        assert_eq!(artifact.download_name.as_deref(), Some("holiday-clip.mp4"));

        let hit = cache.lookup("k1").await.unwrap();
        assert_eq!(hit, artifact, "lookup reads back exactly what commit wrote");

        // Shaped like a row an older build committed: both columns are simply
        // absent from the INSERT, which the migration's nullability allows.
        std::fs::write(dir.path().join("legacy.mp4"), b"old").unwrap();
        let now = current_iso_timestamp();
        sqlx::query(
            "INSERT INTO artifacts (key, source_sha256, params_hash, preset, file_name, \
             mime_type, size_bytes, transcoder_version, created_at, last_access) \
             VALUES ('legacy', 'sha', 'hash', 'clip', 'legacy.mp4', 'video/mp4', 3, 1, ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(&cache.pool)
        .await
        .unwrap();
        let legacy = cache.lookup("legacy").await.expect("a pre-column row still hits");
        assert_eq!(legacy.sha256, None);
        assert_eq!(legacy.download_name, None);

        // Re-committing an existing key must refresh both columns, which is
        // the only way a pre-column row (or one whose hash failed) can ever
        // heal. Without the two SET clauses in the ON CONFLICT arm this row
        // would keep answering with the old digest and the old name, and
        // nothing else in the suite would notice.
        std::fs::write(dir.path().join("legacy.mp4"), b"old").unwrap();
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        let healed = cache
            .commit(
                NewArtifact {
                    key: "legacy",
                    source_sha256: "sha",
                    params_hash: "hash",
                    preset: "clip",
                    file_name: "legacy.mp4",
                    download_name: "renamed-clip.mp4",
                    mime_type: "video/mp4",
                    transcoder_version: 1,
                },
                &temp,
            )
            .await
            .unwrap();
        assert_eq!(healed.sha256.as_deref(), Some(DIGEST_0123456789));
        let healed = cache.lookup("legacy").await.expect("the row is still there");
        assert_eq!(
            healed.sha256.as_deref(),
            Some(DIGEST_0123456789),
            "the conflicting commit refreshed the stored digest"
        );
        assert_eq!(
            healed.download_name.as_deref(),
            Some("renamed-clip.mp4"),
            "and the stored name with it"
        );
    }

    /// A share entry is the artifact's bytes under a name a human (and the
    /// clipboard) can use: sanitized, idempotent, and repaired if a previous
    /// attempt left something short behind.
    #[tokio::test]
    async fn materialize_share_names_the_bytes_and_sanitizes_the_name() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        let temp = cache.temp_path("mp4");
        let artifact = commit_named(&cache, "k1", b"share me", &temp, "CON.mp4")
            .await
            .unwrap();

        let shared = cache.materialize_share(&artifact, None).await.unwrap();
        assert_eq!(shared.parent().unwrap(), cache.share_dir("k1"));
        assert_eq!(shared.file_name().unwrap().to_string_lossy(), "_CON.mp4");
        assert_eq!(std::fs::read(&shared).unwrap(), b"share me");

        // Idempotent: the same path, and nothing accumulates beside it.
        assert_eq!(cache.materialize_share(&artifact, None).await.unwrap(), shared);
        assert_eq!(share_entries(&cache, "k1"), ["_CON.mp4"]);

        // A short leftover — the only thing a crashed copy fallback can leave
        // under the final name — is replaced, not handed out. Unlinked first
        // so the truncation cannot reach the artifact through the hardlink.
        std::fs::remove_file(&shared).unwrap();
        std::fs::write(&shared, b"tru").unwrap();
        assert_eq!(cache.materialize_share(&artifact, None).await.unwrap(), shared);
        assert_eq!(std::fs::read(&shared).unwrap(), b"share me");

        // A name that sanitizes away entirely falls back to the on-disk one.
        let temp = cache.temp_path("mp4");
        let nameless = commit_named(&cache, "k2", b"unnamed", &temp, "///.")
            .await
            .unwrap();
        let shared = cache.materialize_share(&nameless, None).await.unwrap();
        assert_eq!(shared.file_name().unwrap().to_string_lossy(), "k2.mp4");
        assert_eq!(std::fs::read(&shared).unwrap(), b"unnamed");

        // A separator-laden, control-character-laden, over-long multi-byte
        // name still produces something the filesystem accepts.
        let temp = cache.temp_path("mp4");
        let nasty = commit_named(
            &cache,
            "k3",
            b"wide",
            &temp,
            &format!("a/b\\c\"d\u{7}{}.mp4", "録画".repeat(200)),
        )
        .await
        .unwrap();
        let shared = cache.materialize_share(&nasty, None).await.unwrap();
        let name = shared.file_name().unwrap().to_string_lossy().into_owned();
        assert!(shared.is_file(), "{name}");
        assert!(name.len() <= MAX_SHARE_NAME_BYTES, "{}", name.len());
        assert!(name.starts_with("abcd録画"), "{name}");
        assert!(name.ends_with(".mp4"), "{name}");
        assert_eq!(std::fs::read(&shared).unwrap(), b"wide");
    }

    /// The name the *caller* is naming this file wins over the stored one.
    /// The stored column is whichever submitter encoded first, and two
    /// byte-identical sources with the same settings share one cache key — so
    /// deferring to it would paste another file's name onto this one.
    #[tokio::test]
    async fn materialize_share_prefers_the_callers_name_over_the_stored_one() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 1024).await;

        let temp = cache.temp_path("mp4");
        let artifact = commit_named(&cache, "k1", b"share me", &temp, "first-submitter.mp4")
            .await
            .unwrap();

        let shared = cache
            .materialize_share(&artifact, Some("this-request.mp4"))
            .await
            .unwrap();
        assert_eq!(
            shared.file_name().unwrap().to_string_lossy(),
            "this-request.mp4"
        );
        assert_eq!(std::fs::read(&shared).unwrap(), b"share me");

        // The caller's name is sanitized on the same terms as the stored one:
        // it arrives from a request and is no more trusted for it.
        let escaped = cache
            .materialize_share(&artifact, Some("../sub/dir/100%.mp4"))
            .await
            .unwrap();
        assert_eq!(escaped.file_name().unwrap().to_string_lossy(), "..subdir100_.mp4");

        // A caller with no request to name the file from still gets the
        // stored name, and the names simply coexist under the one key.
        let stored = cache.materialize_share(&artifact, None).await.unwrap();
        assert_eq!(
            stored.file_name().unwrap().to_string_lossy(),
            "first-submitter.mp4"
        );
        assert_eq!(
            share_entries(&cache, "k1"),
            ["..subdir100_.mp4", "first-submitter.mp4", "this-request.mp4"]
        );

        // A caller name that sanitizes away does not fall back to the stored
        // one — it falls all the way through to the on-disk name, because a
        // request that named the file at all was not asking for the first
        // submitter's stem.
        let nameless = cache.materialize_share(&artifact, Some("///.")).await.unwrap();
        assert_eq!(nameless.file_name().unwrap().to_string_lossy(), "k1.mp4");
    }

    /// The copy fallback, which no test filesystem ever reaches through
    /// `materialize_share` (hardlinking works everywhere the suite runs):
    /// the bytes land under the final name and the temporary is gone.
    #[tokio::test]
    async fn the_copy_fallback_leaves_no_temporary_behind() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("artifact.mp4");
        std::fs::write(&source, b"copied bytes").unwrap();
        let share = dir.path().join("share").join("k1");
        std::fs::create_dir_all(&share).unwrap();

        let target = share.join("clip.mp4");
        copy_into_place(&source, &target, &share).await.unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"copied bytes");

        let leftovers: Vec<String> = std::fs::read_dir(&share)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(TEMP_PREFIX))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    /// Share entries are derived state: every path that removes a row removes
    /// them too, or the cache directory would grow copies of artifacts that no
    /// longer exist.
    #[tokio::test]
    async fn removing_an_artifact_takes_its_share_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = cache_with_budget(dir.path(), 25).await;

        let old = commit(&cache, "old", &[0u8; 10]).await;
        cache.materialize_share(&old, None).await.unwrap();
        touch(&cache, "old", "2020-01-01T00:00:00", 0).await;
        let mid = commit(&cache, "mid", &[0u8; 10]).await;
        cache.materialize_share(&mid, None).await.unwrap();

        // 30 bytes against 25: the eviction pass takes "old" and its share.
        commit(&cache, "new", &[0u8; 10]).await;
        assert_eq!(keys(&cache).await, ["mid", "new"]);
        assert!(!cache.share_dir("old").exists(), "eviction takes the share");
        assert!(cache.share_dir("mid").is_dir());

        // The row-without-file repair inside lookup() is a removal too.
        std::fs::remove_file(&mid.path).unwrap();
        assert!(cache.lookup("mid").await.is_none());
        assert!(!cache.share_dir("mid").exists());

        let new = cache.lookup("new").await.unwrap();
        cache.materialize_share(&new, None).await.unwrap();
        assert!(cache.share_dir("new").is_dir());
        cache.clear(false).await.unwrap();
        assert!(!cache.share_dir("new").exists(), "clear takes them as well");
    }

    /// Reconciliation is the backstop for a removal that died between its two
    /// halves — and it must not mistake the share directory itself for an
    /// unclaimed artifact file, nor a share another process published a moment
    /// ago for debris. The index snapshot this sweep works from was read
    /// before it started, so a second process's commit is *always* missing
    /// from it; only age separates that case from an eviction that died.
    #[tokio::test]
    async fn reconciliation_sweeps_settled_orphan_share_dirs_and_spares_live_ones() {
        let dir = tempfile::tempdir().unwrap();
        let live;
        {
            let cache = cache_with_budget(dir.path(), 1024).await;
            let kept = commit(&cache, "kept", &[0u8; 10]).await;
            live = cache.materialize_share(&kept, None).await.unwrap();
            for key in ["ghost", "publishing"] {
                let share = cache.share_dir(key);
                std::fs::create_dir_all(&share).unwrap();
                std::fs::write(share.join("junk.mp4"), b"junk").unwrap();
            }
            // Only the settled one is debris; "publishing" stands in for the
            // other process that committed its key seconds ago.
            backdate_dir(
                &cache.share_dir("ghost"),
                UNCLAIMED_ARTIFACT_MIN_AGE + Duration::from_secs(60),
            );
        }

        let cache = cache_with_budget(dir.path(), 1024).await;
        assert_eq!(keys(&cache).await, ["kept"]);
        assert!(live.is_file(), "a live share entry survives reconciliation");
        assert!(
            !cache.share_dir("ghost").exists(),
            "a settled rowless share is debris"
        );
        assert!(
            cache.share_dir("publishing").is_dir(),
            "a young rowless share may be another process's publish in flight"
        );
        assert!(
            dir.path().join(SHARE_DIR_NAME).is_dir(),
            "the share directory is not an unclaimed artifact file"
        );
        assert!(dir.path().join("kept.mp4").is_file());
    }

    /// A copy fallback that died mid-write leaves a `.tmp-*` inside a share
    /// directory whose key is very much alive, so the orphan pass above skips
    /// it and the root sweep never descends to it. Reconciliation reclaims it
    /// on age, exactly as it does an abandoned encode.
    #[tokio::test]
    async fn reconciliation_reclaims_aged_copy_temporaries_from_live_shares() {
        let dir = tempfile::tempdir().unwrap();
        let (stale, fresh, live);
        {
            let cache = cache_with_budget(dir.path(), 1024).await;
            let kept = commit(&cache, "kept", &[0u8; 10]).await;
            live = cache.materialize_share(&kept, None).await.unwrap();
            let share = cache.share_dir("kept");
            stale = share.join(format!(".tmp-{}-crashed", std::process::id()));
            fresh = share.join(".tmp-999999-inflight");
            for path in [&stale, &fresh] {
                std::fs::write(path, b"half a copy").unwrap();
            }
            backdate(&stale, STALE_TEMP_MAX_AGE + Duration::from_secs(60));
        }

        let cache = cache_with_budget(dir.path(), 1024).await;
        assert_eq!(keys(&cache).await, ["kept"]);
        assert!(!stale.exists(), "an aged partial copy is reclaimed");
        assert!(
            fresh.is_file(),
            "a young one may be a copy running right now, here or elsewhere"
        );
        assert!(live.is_file(), "and the entry itself is untouched");
    }

    /// The sanitizer table, kept in step with `share_cache.rs`'s: separators
    /// and control characters vanish, `%` is neutralized, Windows trailing
    /// punctuation is trimmed, device names are escaped, and a name that
    /// reduces to nothing says so instead of returning an empty component.
    #[test]
    fn share_name_sanitizer_table() {
        for (input, expected) in [
            ("clip.mp4", Some("clip.mp4")),
            ("sub/dir/clip.mp4", Some("subdirclip.mp4")),
            ("..\\..\\evil.exe", Some("....evil.exe")),
            ("a\0b\tc.mp4", Some("abc.mp4")),
            ("trailing.  ", Some("trailing")),
            ("CON", Some("_CON")),
            ("con.txt", Some("_con.txt")),
            ("Com1.png", Some("_Com1.png")),
            ("COM10.png", Some("COM10.png")),
            ("nul", Some("_nul")),
            (
                "a%PROCESSOR_ARCHITECTURE%.mp4",
                Some("a_PROCESSOR_ARCHITECTURE_.mp4"),
            ),
            ("100%.mp4", Some("100_.mp4")),
            ("///.mp4", Some(".mp4")),
            ("/*?<>|\":.mp4", Some(".mp4")),
            ("..", None),
            ("   ", None),
            ("", None),
            ("///.", None),
        ] {
            assert_eq!(
                sanitize_share_name_within(input, MAX_SHARE_NAME_BYTES).as_deref(),
                expected,
                "input {input:?}"
            );
        }
    }

    /// An over-long name is cut to the per-component ceiling with its
    /// extension intact, on boundaries a multi-byte name respects.
    #[test]
    fn over_long_share_names_are_truncated_to_the_component_ceiling() {
        let sanitize = |name: &str| sanitize_share_name_within(name, MAX_SHARE_NAME_BYTES);

        let long = sanitize(&format!("{}.mp4", "a".repeat(400))).unwrap();
        assert_eq!(long.len(), MAX_SHARE_NAME_BYTES);
        assert!(long.ends_with(".mp4"));

        // The reserved-name underscore is added before truncation, so it sits
        // inside the budget rather than pushing the result past it.
        let reserved = sanitize(&format!("con.{}.mp4", "b".repeat(400))).unwrap();
        assert_eq!(reserved.len(), MAX_SHARE_NAME_BYTES);
        assert!(reserved.starts_with("_con."));
        assert!(reserved.ends_with(".mp4"));

        let wide = sanitize(&format!("{}.mp4", "録画".repeat(200))).unwrap();
        assert!(wide.len() <= MAX_SHARE_NAME_BYTES, "{}", wide.len());
        assert!(wide.ends_with(".mp4"));
        assert!(wide.starts_with("録画"));

        // A name that is one enormous "extension" keeps the ceiling, not the
        // extension.
        let silly = sanitize(&format!("a.{}", "c".repeat(400))).unwrap();
        assert!(silly.len() <= MAX_SHARE_NAME_BYTES, "{}", silly.len());
    }

    /// The `MAX_PATH` budget a share entry actually gets: the component limit
    /// only wins for a cache directory nobody has, and a deep one eats into
    /// the name until there is nothing left to give.
    ///
    /// Asserted arithmetically rather than by building a real directory of a
    /// given depth, which is exactly the platform-fragile thing this
    /// calculation exists to survive.
    #[test]
    fn the_share_name_ceiling_shrinks_with_the_directory() {
        // 3 characters plus the separator is the only case where the
        // filesystem's per-component limit is the binding constraint.
        assert_eq!(share_name_ceiling(Path::new("Q:\\")), MAX_SHARE_NAME_BYTES);
        assert_eq!(share_name_ceiling(Path::new("Q:\\a")), MAX_SHARE_NAME_BYTES - 1);

        // A realistic one: a profile path plus `share/<64-hex key>/`.
        let deep = "x".repeat(200);
        assert_eq!(share_name_ceiling(Path::new(&deep)), 58);

        // Past the budget entirely — no negative wraparound, and below the
        // floor, where `materialize_share` stops sanitizing and takes the
        // artifact's own name instead.
        let deeper = "y".repeat(300);
        assert_eq!(share_name_ceiling(Path::new(&deeper)), 0);
        assert!(share_name_ceiling(Path::new(&"z".repeat(250))) < MIN_SHARE_NAME_BYTES);
    }

    /// The ceiling threaded into the sanitizer cuts the name the same way the
    /// component limit does — extension kept, UTF-8 boundaries respected —
    /// just sooner.
    #[test]
    fn a_tighter_ceiling_cuts_the_name_the_same_way() {
        let cut = sanitize_share_name_within(&format!("{}.mp4", "a".repeat(400)), 40).unwrap();
        assert_eq!(cut.len(), 40);
        assert!(cut.ends_with(".mp4"));

        let wide = sanitize_share_name_within(&format!("{}.mp4", "録画".repeat(200)), 40).unwrap();
        assert!(wide.len() <= 40, "{}", wide.len());
        assert!(wide.ends_with(".mp4"));
        assert!(wide.starts_with("録画"));

        // Under a tight ceiling a long "extension" is still dropped rather
        // than allowed to consume the whole budget — leaving only the stem,
        // short as it is.
        let silly = sanitize_share_name_within(&format!("a.{}", "c".repeat(400)), 40).unwrap();
        assert_eq!(silly, "a");

        // A ceiling this name cannot fit into at all reports that, rather
        // than returning an empty component.
        assert_eq!(sanitize_share_name_within("録.mp4", 1), None);
    }
}
