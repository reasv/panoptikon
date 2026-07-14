use std::{fs, path::Path};

use serde::Serialize;
use sqlx::SqliteConnection;
use utoipa::ToSchema;

use crate::{api_error::ApiError, db::system_config::normalize_folder_list};

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
pub(crate) struct FolderValidationIssue {
    pub path: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
pub(crate) struct FolderValidation {
    pub included_folders: Vec<String>,
    pub excluded_folders: Vec<String>,
    pub errors: Vec<FolderValidationIssue>,
}

#[derive(Debug)]
struct PathInspection {
    path: String,
    error: Option<String>,
    empty: bool,
}

fn inspect_directory(path: String) -> PathInspection {
    let directory = Path::new(&path);
    let metadata = match fs::metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) => {
            return PathInspection {
                path,
                error: Some(format!("Cannot access this path: {error}")),
                empty: false,
            };
        }
    };
    if !metadata.is_dir() {
        return PathInspection {
            path,
            error: Some("This path is not a directory.".into()),
            empty: false,
        };
    }
    match fs::read_dir(directory) {
        Ok(mut entries) => PathInspection {
            path,
            error: None,
            empty: entries.next().is_none(),
        },
        Err(error) => PathInspection {
            path,
            error: Some(format!("Cannot read this directory: {error}")),
            empty: false,
        },
    }
}

pub(crate) async fn has_indexed_files_under(
    conn: &mut SqliteConnection,
    folder: &str,
) -> Result<bool, ApiError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM files WHERE substr(path, 1, length(?1)) = ?1)",
    )
    .bind(folder)
    .fetch_one(conn)
    .await
    .map_err(|error| {
        tracing::error!(%error, folder, "failed to check indexed files under folder");
        ApiError::internal("Failed to validate folder safety")
    })
}

/// Normalizes and validates staged wizard folders without changing config or
/// starting work. Empty included folders are safe only when the target index
/// has no file rows beneath that path; this distinguishes a legitimate new
/// watch target from a drive/share that has become momentarily empty.
pub(crate) async fn validate_folders(
    mut conn: Option<&mut SqliteConnection>,
    included_folders: &[String],
    excluded_folders: &[String],
) -> Result<FolderValidation, ApiError> {
    let included_folders = normalize_folder_list(included_folders);
    let excluded_folders = normalize_folder_list(excluded_folders);
    let paths = included_folders
        .iter()
        .chain(excluded_folders.iter())
        .cloned()
        .collect::<Vec<_>>();
    let inspections = tokio::task::spawn_blocking(move || {
        paths.into_iter().map(inspect_directory).collect::<Vec<_>>()
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "folder validation worker failed");
        ApiError::internal("Failed to validate folders")
    })?;

    let mut errors = Vec::new();
    for inspection in inspections {
        if let Some(error) = inspection.error {
            errors.push(FolderValidationIssue {
                path: inspection.path,
                error,
            });
            continue;
        }
        if inspection.empty
            && included_folders.contains(&inspection.path)
            && let Some(database) = conn.as_deref_mut()
            && has_indexed_files_under(database, &inspection.path).await?
        {
            errors.push(FolderValidationIssue {
                path: inspection.path,
                error: "This directory is empty, but the database still contains indexed files beneath it. It was not accepted to protect those entries in case the drive or share is temporarily unavailable.".into(),
            });
        }
    }

    let included_paths = included_folders.iter().map(Path::new).collect::<Vec<_>>();
    for excluded in &excluded_folders {
        let path = Path::new(excluded);
        if !included_paths
            .iter()
            .any(|included| path.starts_with(included))
        {
            errors.push(FolderValidationIssue {
                path: excluded.clone(),
                error: "Excluded directories must be inside an included directory.".into(),
            });
        }
    }

    Ok(FolderValidation {
        included_folders,
        excluded_folders,
        errors,
    })
}

/// Validates the optional continuous-scan whitelist against both the
/// filesystem and the staged full-scan roots. An empty whitelist means all
/// full-scan included folders and is therefore valid.
pub(crate) async fn validate_continuous_folders(
    conn: Option<&mut SqliteConnection>,
    included_folders: &[String],
    excluded_folders: &[String],
    continuous_folders: &[String],
) -> Result<FolderValidation, ApiError> {
    let included_folders = normalize_folder_list(included_folders);
    let excluded_folders = normalize_folder_list(excluded_folders);
    let mut validation = validate_folders(conn, continuous_folders, &[]).await?;
    let included_paths = included_folders.iter().map(Path::new).collect::<Vec<_>>();
    let excluded_paths = excluded_folders.iter().map(Path::new).collect::<Vec<_>>();

    for folder in &validation.included_folders {
        let path = Path::new(folder);
        if !included_paths.iter().any(|root| path.starts_with(root)) {
            validation.errors.push(FolderValidationIssue {
                path: folder.clone(),
                error: "Continuously watched directories must be inside a full-scan included directory.".into(),
            });
        } else if excluded_paths.iter().any(|root| path.starts_with(root)) {
            validation.errors.push(FolderValidationIssue {
                path: folder.clone(),
                error: "Continuously watched directories cannot be inside an excluded directory."
                    .into(),
            });
        }
    }

    Ok(validation)
}

/// A database is ready for Desktop use once at least one currently included
/// folder has reached the filescan pipeline. `file_scans` rows are inserted
/// when scanning actually starts, so queued or failed-before-start jobs do not
/// produce a false positive. Historical scans for folders that were later
/// removed do not count because the join is against the current folder list.
pub(crate) async fn is_ready_for_desktop(conn: &mut SqliteConnection) -> Result<bool, ApiError> {
    sqlx::query_scalar::<_, bool>(
        r#"
SELECT EXISTS (
    SELECT 1
    FROM folders AS folder
    INNER JOIN file_scans AS scan ON scan.path = folder.path
    WHERE folder.included = 1
)
        "#,
    )
    .fetch_one(conn)
    .await
    .map_err(|error| {
        tracing::error!(%error, "failed to determine Desktop database readiness");
        ApiError::internal("Failed to determine database readiness")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrations::migrate_in_memory;

    /// Readiness requires a scan row for a folder that remains included; an
    /// included-only folder, scan-only path, or scan of a removed path is not
    /// enough.
    #[tokio::test]
    async fn readiness_joins_current_included_folders_to_scan_history() {
        let mut dbs = migrate_in_memory("desktop-ready-index".into(), "desktop-ready-user".into())
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query(
            "INSERT INTO folders (time_added, path, included) VALUES ('now', '/current', 1)",
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("INSERT INTO file_scans (start_time, path) VALUES ('now', '/removed')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("INSERT INTO file_scans (start_time, path) VALUES ('now', '/current')")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());

        sqlx::query("DELETE FROM folders WHERE path = '/current'")
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        assert!(!is_ready_for_desktop(&mut dbs.index_conn).await.unwrap());
    }

    #[tokio::test]
    async fn folder_validation_accepts_new_empty_paths_and_checks_exclusion_scope() {
        let mut dbs = migrate_in_memory("validate-index".into(), "validate-user".into())
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let included = root.path().join("included");
        let excluded = included.join("excluded");
        let outside = root.path().join("outside");
        fs::create_dir_all(&excluded).unwrap();
        fs::create_dir_all(&outside).unwrap();

        let valid = validate_folders(
            Some(&mut dbs.index_conn),
            &[included.to_string_lossy().into_owned()],
            &[excluded.to_string_lossy().into_owned()],
        )
        .await
        .unwrap();
        assert!(valid.errors.is_empty());
        assert!(valid.included_folders[0].ends_with(['/', '\\']));

        let invalid = validate_folders(
            Some(&mut dbs.index_conn),
            &[included.to_string_lossy().into_owned()],
            &[outside.to_string_lossy().into_owned()],
        )
        .await
        .unwrap();
        assert!(
            invalid
                .errors
                .iter()
                .any(|issue| issue.error.contains("inside an included"))
        );

        let protected = root.path().join("protected");
        fs::create_dir_all(&protected).unwrap();
        let protected_normalized =
            normalize_folder_list(&[protected.to_string_lossy().into_owned()]).remove(0);
        sqlx::query(
            "INSERT INTO items (id, sha256, md5, type, time_added) VALUES (1, 'sha', 'md5', 'image/png', 'now')",
        )
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO file_scans (id, start_time, path) VALUES (1, 'now', ?1)")
            .bind(&protected_normalized)
            .execute(&mut dbs.index_conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO files (sha256, item_id, path, filename, last_modified, scan_id, available) VALUES ('sha', 1, ?1, 'ghost.png', 'now', 1, 1)",
        )
        .bind(format!("{protected_normalized}ghost.png"))
        .execute(&mut dbs.index_conn)
        .await
        .unwrap();

        let protected_result = validate_folders(
            Some(&mut dbs.index_conn),
            &[protected.to_string_lossy().into_owned()],
            &[],
        )
        .await
        .unwrap();
        assert!(
            protected_result
                .errors
                .iter()
                .any(|issue| issue.error.contains("protect those entries"))
        );
    }

    #[tokio::test]
    async fn continuous_folder_validation_enforces_the_staged_scan_scope() {
        let mut dbs = migrate_in_memory("continuous-index".into(), "continuous-user".into())
            .await
            .unwrap();
        let root = tempfile::tempdir().unwrap();
        let included = root.path().join("included");
        let watched = included.join("watched");
        let excluded = included.join("excluded");
        let outside = root.path().join("outside");
        fs::create_dir_all(&watched).unwrap();
        fs::create_dir_all(&excluded).unwrap();
        fs::create_dir_all(&outside).unwrap();

        let valid = validate_continuous_folders(
            Some(&mut dbs.index_conn),
            &[included.to_string_lossy().into_owned()],
            &[excluded.to_string_lossy().into_owned()],
            &[watched.to_string_lossy().into_owned()],
        )
        .await
        .unwrap();
        assert!(valid.errors.is_empty());

        let invalid = validate_continuous_folders(
            Some(&mut dbs.index_conn),
            &[included.to_string_lossy().into_owned()],
            &[excluded.to_string_lossy().into_owned()],
            &[
                outside.to_string_lossy().into_owned(),
                excluded.to_string_lossy().into_owned(),
            ],
        )
        .await
        .unwrap();
        assert!(
            invalid
                .errors
                .iter()
                .any(|issue| issue.error.contains("full-scan included"))
        );
        assert!(
            invalid
                .errors
                .iter()
                .any(|issue| issue.error.contains("excluded directory"))
        );

        let unrestricted = validate_continuous_folders(
            Some(&mut dbs.index_conn),
            &[included.to_string_lossy().into_owned()],
            &[],
            &[],
        )
        .await
        .unwrap();
        assert!(unrestricted.errors.is_empty());
        assert!(unrestricted.included_folders.is_empty());
    }
}
