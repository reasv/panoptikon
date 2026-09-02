//! The one-time "batch size becomes auto" configuration migration.
//!
//! Batch size stopped being a target and became an optional cap
//! (docs/batch-calibration-design.md, "Batch size UX"): every number stored
//! before the upgrade — `job_settings[].default_batch_size` (a last-used
//! default) and `cron_jobs[].batch_size` (what actually runs unattended) —
//! must be cleared once so existing users land on auto like new ones.
//!
//! The state that decides this lives in two places, which is what makes the
//! migration awkward enough to deserve a module: the values are in the index
//! database's sibling `config.toml`, while the "already done" stamp has to be
//! durable and per-database, i.e. in the index database itself. The rules:
//!
//! - **Stamp last, but stamp.** The config rewrite is attempted first and the
//!   stamp row is written after it. The order matters only for the *crash*
//!   window: a process that dies between the two re-runs the rewrite on the
//!   next startup, which is harmless because nothing can have entered a new
//!   cap before that restart completed, while the opposite order could lose
//!   the migration outright. A *failed* rewrite is different — see below.
//! - **A failed rewrite stamps anyway, loudly.** An unreadable, unparseable
//!   or unwritable config would otherwise leave the database unstamped and
//!   retry on every boot, and a retry that lands after the user has entered a
//!   new cap would delete it. Running once, possibly incompletely, with a
//!   warning that names the file and says what to do about it beats a silent
//!   retry loop that can destroy user input.
//! - **Fresh databases are stamped, not migrated.** A database created after
//!   the upgrade has no legacy caps to clear, so nulling its (default or
//!   absent) config would be a no-op — but stamping it at creation is what
//!   stops a *later* startup sweep from wiping a cap the user entered in the
//!   meantime.
//! - **A missing `config.toml` is skipped, never seeded.** There is nothing
//!   to null, and `SystemConfigStore::load` would create the file — which is
//!   why the decision is made from a direct read instead.
//! - **Read-only mode does nothing at all.** Neither half of the migration
//!   may write, and a read-only process must not consume the one-shot: the
//!   whole hook returns early, so the next writable startup still runs it.
//! - **Nothing degrades into a startup failure.** Only the stamp's own DB
//!   read/write can abort `migrate_path`; everything about the config file is
//!   a warning.
//!
//! One cosmetic consequence of removing keys from a hand-edited file: the
//! rewrite goes through `toml_edit`, where a comment sitting directly above a
//! removed key is that key's decor and is removed with it. Comments elsewhere
//! in the file, including on surviving keys, are preserved.

use std::fs;
use std::io;
use std::path::Path;

use anyhow::{Context, Result, anyhow};

use crate::db::system_config::{SystemConfig, SystemConfigStore};

/// Row presence is the signal; `COUNT` keeps it a one-row read either way.
const STAMP_PRESENT_SQL: &str = "SELECT COUNT(*) FROM batch_auto_migration WHERE id = 1";

const INSERT_STAMP_SQL: &str = "INSERT OR IGNORE INTO batch_auto_migration (id) VALUES (1)";

/// Runs the migration for one index database, if it has not run there yet.
///
/// `path` is the `index.db` file; the config it rewrites is its sibling
/// `config.toml`. `fresh` means the database had no user tables before the
/// SQL migrations ran (see `migrations::migrate_path`).
pub(crate) async fn apply_batch_auto_migration(
    conn: &mut sqlx::SqliteConnection,
    path: &Path,
    fresh: bool,
) -> Result<()> {
    if crate::db::readonly_mode() {
        // Neither the config rewrite nor the stamp may be written here, and
        // stamping without rewriting is worse than doing nothing: it would
        // leave the pre-upgrade numbers in place while recording that they
        // were cleared. (Untested: the runtime config is installed once per
        // process, so there is no per-test read-only mode to flip.)
        return Ok(());
    }
    if is_stamped(conn).await? {
        return Ok(());
    }

    if !fresh {
        // Deliberately not fallible: see the module doc. Whatever happened to
        // the config file, this database is stamped below and never retried.
        clear_stored_batch_sizes(path);
    }

    sqlx::query(INSERT_STAMP_SQL)
        .execute(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "failed to stamp the batch-auto migration in {}",
                path.display()
            )
        })?;
    Ok(())
}

async fn is_stamped(conn: &mut sqlx::SqliteConnection) -> Result<bool> {
    let stamps: i64 = sqlx::query_scalar(STAMP_PRESENT_SQL)
        .fetch_one(&mut *conn)
        .await
        .context("failed to read the batch-auto migration stamp")?;
    Ok(stamps > 0)
}

/// Clears the stored batch sizes for one index database, reporting whatever
/// went wrong to the log instead of to the caller: the caller stamps either
/// way, so the warning is the whole user-facing outcome of a failure.
fn clear_stored_batch_sizes(index_db_file: &Path) {
    let Some((store, index_db)) = store_for_index_db(index_db_file) else {
        // Not a path this server could ever have written a config for — an
        // unexpected directory layout, or a database directory whose name is
        // not UTF-8 and therefore cannot be an `index_db` value. Nothing to
        // null, so this is a skip, not a failure to retry forever.
        tracing::debug!(
            path = %index_db_file.display(),
            "no config location can be derived for this index database; nothing to migrate"
        );
        return;
    };
    if let Err(error) = clear_config_batch_sizes(&store, &index_db) {
        tracing::warn!(
            error = %format!("{error:#}"),
            "pre-upgrade batch sizes in {} could not be cleared automatically; existing \
             values will now act as max-batch caps — clear them in the Scan page or delete \
             the lines by hand",
            store.config_path(&index_db).display()
        );
    }
}

/// Nulls `job_settings[].default_batch_size` and `cron_jobs[].batch_size` in
/// one database's `config.toml`.
///
/// The decision is made from a direct read rather than through
/// [`SystemConfigStore::load`]: load *seeds* a default file when none exists,
/// and "no file" is exactly the case with nothing to null (checking
/// `exists()` first would only narrow that race, not close it). The value
/// parsed here is then handed straight to `save`, which normalizes its clone
/// exactly as `load` would and diffs it against its own normalized parse of
/// the same file — so the patch written back touches the batch-size keys and
/// nothing else, folder lists included.
fn clear_config_batch_sizes(store: &SystemConfigStore, index_db: &str) -> Result<()> {
    let config_path = store.config_path(index_db);
    let raw = match fs::read_to_string(&config_path) {
        Ok(raw) => raw,
        // Nothing to null, and nothing to seed.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", config_path.display()));
        }
    };
    let mut config: SystemConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;

    let mut changed = false;
    for setting in &mut config.job_settings {
        changed |= setting.default_batch_size.take().is_some();
    }
    for job in &mut config.cron_jobs {
        changed |= job.batch_size.take().is_some();
    }
    if !changed {
        return Ok(());
    }

    // `ApiError` is not a `std::error::Error`; the store has already logged
    // the underlying cause, so the context line carries the file name.
    store
        .save(index_db, &config)
        .map_err(|error| anyhow!("{error:?}"))
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    tracing::info!(
        index_db,
        "batch sizes in the stored job settings and cron schedule were reset to auto"
    );
    Ok(())
}

/// Recovers the `(store, index_db)` pair from an `index.db` path, i.e.
/// `<data_folder>/index/<index_db>/index.db`. Deriving it beats plumbing the
/// name down: the sweep walks directories and only ever has paths.
fn store_for_index_db(index_db_file: &Path) -> Option<(SystemConfigStore, String)> {
    let db_dir = index_db_file.parent()?;
    let index_db = db_dir.file_name()?.to_str()?.to_string();
    let data_dir = db_dir.parent()?.parent()?.to_path_buf();
    Some((SystemConfigStore::new(data_dir), index_db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::{migrate_index_db_file, migrate_storage_db_file};
    use crate::db::system_config::SystemConfigStore;
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn index_db_file(data_dir: &Path, index_db: &str) -> PathBuf {
        let dir = data_dir.join("index").join(index_db);
        fs::create_dir_all(&dir).unwrap();
        dir.join("index.db")
    }

    async fn open(path: &Path) -> SqliteConnection {
        SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path))
            .await
            .unwrap()
    }

    async fn stamped(path: &Path) -> bool {
        let mut conn = open(path).await;
        is_stamped(&mut conn).await.unwrap()
    }

    /// Removes the stamp so the next `migrate_path` behaves like the first
    /// startup after the upgrade on a database that predates it.
    async fn unstamp(path: &Path) {
        let mut conn = open(path).await;
        sqlx::query("DELETE FROM batch_auto_migration")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    const CONFIG_WITH_CAPS: &str = concat!(
        "# hand-written database settings\n",
        "scan_images = true\n",
        "\n",
        "[[job_settings]]\n",
        "group_name = \"clip\"\n",
        "# the cap the user picked back when it was a target\n",
        "default_batch_size = 8\n",
        "default_threshold = 0.5\n",
        "\n",
        "[[cron_jobs]]\n",
        "inference_id = \"clip/ViT-H-14\"\n",
        "batch_size = 4\n",
        "threshold = 0.2\n",
    );

    // A database created after the upgrade is stamped at creation and its
    // config is never touched -- in particular, never seeded.
    #[tokio::test]
    async fn fresh_database_is_stamped_without_seeding_a_config() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "fresh");
        migrate_index_db_file(&path).await.unwrap();

        assert!(stamped(&path).await);
        let config_path = SystemConfigStore::new(tmp.path().to_path_buf()).config_path("fresh");
        assert!(
            !config_path.exists(),
            "the migration must not create a config file"
        );
    }

    // The load-bearing case: an existing database with stored caps loses both
    // of them, physically, while the rest of the file survives verbatim.
    #[tokio::test]
    async fn existing_caps_are_removed_from_the_config_and_the_database_is_stamped() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(tmp.path().to_path_buf()).config_path("default");
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();

        migrate_index_db_file(&path).await.unwrap();

        let rewritten = fs::read_to_string(&config_path).unwrap();
        assert!(
            !rewritten.contains("default_batch_size"),
            "the stored default must be gone from the file, not set to something: {rewritten}"
        );
        assert!(
            !rewritten.contains("batch_size = 4"),
            "the cron cap must be gone from the file: {rewritten}"
        );
        assert!(
            rewritten.contains("# hand-written database settings")
                && rewritten.contains("inference_id = \"clip/ViT-H-14\"")
                && rewritten.contains("default_threshold = 0.5")
                && rewritten.contains("threshold = 0.2"),
            "comments, layout and every other value must survive: {rewritten}"
        );
        assert!(stamped(&path).await);
    }

    // Once stamped, a later sweep must not touch a cap entered afterwards.
    #[tokio::test]
    async fn a_second_run_leaves_a_newly_entered_cap_alone() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(tmp.path().to_path_buf()).config_path("default");
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();
        migrate_index_db_file(&path).await.unwrap();

        // The user re-enters a cap after the upgrade.
        let after_upgrade = fs::read_to_string(&config_path)
            .unwrap()
            .replace("default_threshold = 0.5", "default_batch_size = 16");
        fs::write(&config_path, &after_upgrade).unwrap();

        migrate_index_db_file(&path).await.unwrap();

        assert_eq!(
            fs::read_to_string(&config_path).unwrap(),
            after_upgrade,
            "a stamped database must not be migrated again"
        );
    }

    // Crash between the rewrite and the stamp: the re-run finds nothing to
    // null and simply stamps.
    #[tokio::test]
    async fn a_crash_before_the_stamp_is_harmless_on_re_run() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(tmp.path().to_path_buf()).config_path("default");
        let already_nulled = CONFIG_WITH_CAPS
            .replace("default_batch_size = 8\n", "")
            .replace("batch_size = 4\n", "");
        fs::write(&config_path, &already_nulled).unwrap();

        migrate_index_db_file(&path).await.unwrap();

        assert_eq!(fs::read_to_string(&config_path).unwrap(), already_nulled);
        assert!(stamped(&path).await);
    }

    // An existing database that never got a config file is stamped, and the
    // file stays absent (loading it would have seeded one).
    #[tokio::test]
    async fn a_database_without_a_config_is_stamped_and_no_file_appears() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;

        migrate_index_db_file(&path).await.unwrap();

        assert!(stamped(&path).await);
        assert!(
            !SystemConfigStore::new(tmp.path().to_path_buf())
                .config_path("default")
                .exists()
        );
    }

    // An unparseable config cannot be migrated, so the database is stamped
    // anyway (with a warning) rather than retried forever: a retry landing
    // after the user entered a new cap would delete it. The broken file is
    // left exactly as it was, and startup survives.
    #[tokio::test]
    async fn an_unreadable_config_warns_stamps_and_is_left_untouched() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let store = SystemConfigStore::new(tmp.path().to_path_buf());
        let config_path = store.config_path("default");
        let broken = "[[job_settings]]\ngroup_name = \"clip\"\ndefault_batch_size = ";
        fs::write(&config_path, broken).unwrap();

        // The failure the warning is made of.
        assert!(clear_config_batch_sizes(&store, "default").is_err());

        migrate_index_db_file(&path)
            .await
            .expect("a broken config must not fail startup");

        assert_eq!(fs::read_to_string(&config_path).unwrap(), broken);
        assert!(stamped(&path).await);
    }

    // The same for a config that reads fine but cannot be written back.
    #[tokio::test]
    async fn an_unwritable_config_warns_stamps_and_is_left_intact() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let store = SystemConfigStore::new(tmp.path().to_path_buf());
        let config_path = store.config_path("default");
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();
        let writable = fs::metadata(&config_path).unwrap().permissions();
        let mut readonly = writable.clone();
        readonly.set_readonly(true);
        fs::set_permissions(&config_path, readonly).unwrap();

        assert!(clear_config_batch_sizes(&store, "default").is_err());

        migrate_index_db_file(&path)
            .await
            .expect("an unwritable config must not fail startup");

        assert_eq!(fs::read_to_string(&config_path).unwrap(), CONFIG_WITH_CAPS);
        assert!(stamped(&path).await);

        // Restore write access (and drop any read-only temp file the failed
        // atomic write could not clean up) so the TempDir can be removed.
        fs::set_permissions(&config_path, writable.clone()).unwrap();
        for entry in fs::read_dir(config_path.parent().unwrap())
            .unwrap()
            .flatten()
        {
            let _ = fs::set_permissions(entry.path(), writable.clone());
        }
    }

    // The rewrite must not reformat anything it is not there for. Folder
    // lists are the sharp case: `load`/`save` normalize them, so an
    // un-normalized stored path would be rewritten if the migration diffed a
    // normalized value against the raw file.
    #[tokio::test]
    async fn folder_lists_survive_the_rewrite_byte_for_byte() {
        let tmp = TempDir::new().unwrap();
        let path = index_db_file(tmp.path(), "default");
        migrate_index_db_file(&path).await.unwrap();
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(tmp.path().to_path_buf()).config_path("default");
        let folders = concat!(
            "included_folders = [\"/data/pictures\", \"/data/videos/\"]\n",
            "excluded_folders = [\"/data/pictures/thumbs\"]\n",
        );
        let raw = format!("{folders}{CONFIG_WITH_CAPS}");
        fs::write(&config_path, &raw).unwrap();

        migrate_index_db_file(&path).await.unwrap();

        let rewritten = fs::read_to_string(&config_path).unwrap();
        assert!(
            rewritten.starts_with(folders),
            "folder lists must round-trip byte-identically: {rewritten}"
        );
        assert!(!rewritten.contains("default_batch_size") && !rewritten.contains("batch_size = 4"));
    }

    // A database directory whose name is not UTF-8 can never be an
    // `index_db` value, so no config the server could have written exists for
    // it: skip and stamp, rather than fail and retry on every startup.
    #[test]
    fn a_non_utf8_database_directory_is_skipped_not_retried() {
        #[cfg(windows)]
        let name: std::ffi::OsString = {
            use std::os::windows::ffi::OsStringExt;
            // A lone surrogate: valid UTF-16, not representable as UTF-8.
            std::ffi::OsString::from_wide(&[0x0064, 0xD800])
        };
        #[cfg(not(windows))]
        let name: std::ffi::OsString = {
            use std::os::unix::ffi::OsStringExt;
            std::ffi::OsString::from_vec(vec![0x64, 0x80])
        };
        let path = Path::new("data").join("index").join(name).join("index.db");
        assert!(store_for_index_db(&path).is_none());
        // The skip path: no panic, no error, nothing written.
        clear_stored_batch_sizes(&path);
    }

    // The production callers, not just the test hook: a custom-named index DB
    // created through `migrate_databases_on_disk` has its caps cleared, which
    // proves the `<data>/index/<name>/index.db` derivation against a real
    // data folder.
    #[tokio::test]
    async fn the_named_migrator_clears_caps_for_a_custom_index_db() {
        let test_env = crate::test_utils::test_data_dir();
        let root = test_env.path().to_path_buf();
        let name = "batch-auto-named";
        crate::db::migrations::migrate_databases_on_disk(Some(name), None)
            .await
            .expect("create the custom index db");
        let path = root.join("index").join(name).join("index.db");
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(root.clone()).config_path(name);
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();

        crate::db::migrations::migrate_databases_on_disk(Some(name), None)
            .await
            .expect("re-open the custom index db");

        let rewritten = fs::read_to_string(&config_path).unwrap();
        assert!(
            !rewritten.contains("default_batch_size") && !rewritten.contains("batch_size = 4"),
            "{rewritten}"
        );
        assert!(stamped(&path).await);
    }

    // The other production caller: the startup sweep reaches a database
    // nobody selected, which is the only thing that gets to a DB the user
    // never opens after upgrading.
    #[tokio::test]
    async fn the_startup_sweep_clears_caps_for_a_never_opened_index_db() {
        let test_env = crate::test_utils::test_data_dir();
        let root = test_env.path().to_path_buf();
        let name = "batch-auto-swept";
        crate::db::migrations::migrate_databases_on_disk(Some(name), None)
            .await
            .expect("create the swept index db");
        let path = root.join("index").join(name).join("index.db");
        unstamp(&path).await;
        let config_path = SystemConfigStore::new(root.clone()).config_path(name);
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();

        crate::db::migrations::migrate_all_databases_on_disk()
            .await
            .expect("the startup sweep must succeed");

        let rewritten = fs::read_to_string(&config_path).unwrap();
        assert!(
            !rewritten.contains("default_batch_size") && !rewritten.contains("batch_size = 4"),
            "{rewritten}"
        );
        assert!(stamped(&path).await);
    }

    // Only the index migrator carries the hook: the sibling databases in the
    // same directory neither get the table nor rewrite the shared config.
    #[tokio::test]
    async fn sibling_databases_do_not_run_the_migration() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("index").join("default");
        fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.toml");
        fs::write(&config_path, CONFIG_WITH_CAPS).unwrap();
        let storage = dir.join("storage.db");

        migrate_storage_db_file(&storage).await.unwrap();

        assert_eq!(fs::read_to_string(&config_path).unwrap(), CONFIG_WITH_CAPS);
        let mut conn = open(&storage).await;
        let tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'batch_auto_migration'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(tables, 0);
    }
}
