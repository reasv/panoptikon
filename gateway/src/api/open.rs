use axum::{Json, extract::Path};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use std::{env, path::Path as FsPath};
use tokio::process::Command;
use utoipa::{IntoParams, ToSchema};

use crate::api::db_params::DbQueryParams;
use crate::api_error::ApiError;
use crate::db::items::get_existing_files_for_sha256;
use crate::db::{DbConnection, ReadOnly};

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct OpenQuery {
    path: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct OpenResponse {
    path: String,
    message: String,
}

fn format_custom_command(command_template: &str, path: &FsPath) -> String {
    let directory = path
        .parent()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let filename = path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let replacements = [
        ("{path}", format!("\"{}\"", path.display())),
        ("{folder}", format!("\"{directory}\"")),
        ("{filename}", format!("\"{filename}\"")),
    ];

    let mut command = command_template.to_string();
    for (placeholder, replacement) in replacements {
        command = command.replace(placeholder, &replacement);
    }
    command
}

/// Executes the custom command by replacing placeholders with actual values.
///
/// `command_template`: The command template with placeholders.
/// `path`: The full path to the file.
async fn execute_custom_command(
    command_name: &str,
    command_template: &str,
    path: &FsPath,
) -> ApiResult<()> {
    let command = format_custom_command(command_template, path);
    if command.trim().is_empty() {
        return Ok(());
    }

    if cfg!(windows) {
        Command::new("cmd")
            .args(["/C", &command])
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!(
                    "Failed to execute custom {command_name} for path '{}': {err}",
                    path.display()
                ))
            })?;
        return Ok(());
    }

    let args = shell_words::split(&command).map_err(|err| {
        ApiError::internal(format!(
            "Failed to execute custom {command_name} for path '{}': {err}",
            path.display()
        ))
    })?;
    if args.is_empty() {
        return Ok(());
    }

    Command::new(&args[0])
        .args(&args[1..])
        .status()
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "Failed to execute custom {command_name} for path '{}': {err}",
                path.display()
            ))
        })?;

    Ok(())
}

/// Open the specified file using the default application.
///
/// `path`: The path to the file to be opened.
async fn open_file(path: &FsPath) -> ApiResult<()> {
    if let Ok(custom_cmd) = env::var("OPEN_FILE_COMMAND") {
        execute_custom_command("OPEN_FILE_COMMAND", &custom_cmd, path).await?;
        return Ok(());
    }

    if !path.exists() {
        return Err(ApiError::internal(format!(
            "File '{}' not found",
            path.display()
        )));
    }

    if cfg!(windows) {
        Command::new("cmd")
            .args(["/C", "start", ""])
            .arg(path)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!("Failed to open file '{}': {err}", path.display()))
            })?;
        return Ok(());
    }

    if cfg!(target_os = "macos") {
        Command::new("open")
            .arg(path)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!("Failed to open file '{}': {err}", path.display()))
            })?;
        return Ok(());
    }

    if cfg!(target_os = "linux") {
        Command::new("xdg-open")
            .arg(path)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!("Failed to open file '{}': {err}", path.display()))
            })?;
        return Ok(());
    }

    Err(ApiError::internal(format!(
        "Unsupported operating system: {}",
        std::env::consts::OS
    )))
}

/// Open the given path in the file explorer and select the file, works on Windows, macOS, and Linux.
///
/// `path`: The path to the file to be shown in the file explorer.
async fn show_in_fm(path: &FsPath) -> ApiResult<()> {
    if !path.exists() {
        return Err(ApiError::internal(format!(
            "Path '{}' does not exist",
            path.display()
        )));
    }

    if let Ok(custom_cmd) = env::var("SHOW_IN_FM_COMMAND") {
        execute_custom_command("SHOW_IN_FM_COMMAND", &custom_cmd, path).await?;
        return Ok(());
    }

    if cfg!(windows) {
        Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!(
                    "Failed to open path '{}' in file explorer: {err}",
                    path.display()
                ))
            })?;
        return Ok(());
    }

    if cfg!(target_os = "macos") {
        Command::new("open")
            .args(["-R"])
            .arg(path)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!(
                    "Failed to open path '{}' in file explorer: {err}",
                    path.display()
                ))
            })?;
        return Ok(());
    }

    if cfg!(target_os = "linux") {
        async fn try_file_manager(name: &str, path: &FsPath) -> bool {
            match Command::new("which").arg(name).status().await {
                Ok(status) if status.success() => {
                    let _ = Command::new(name).arg("--select").arg(path).status().await;
                    true
                }
                _ => false,
            }
        }

        if try_file_manager("dolphin", path).await {
            return Ok(());
        }
        if try_file_manager("nautilus", path).await {
            return Ok(());
        }
        if try_file_manager("thunar", path).await {
            return Ok(());
        }

        match Command::new("which").arg("nemo").status().await {
            Ok(status) if status.success() => {
                let _ = Command::new("nemo").arg(path).status().await;
                return Ok(());
            }
            _ => {}
        }

        let directory = path.parent().unwrap_or_else(|| FsPath::new(""));
        Command::new("xdg-open")
            .arg(directory)
            .status()
            .await
            .map_err(|err| {
                ApiError::internal(format!(
                    "Failed to open path '{}' in file explorer: {err}",
                    path.display()
                ))
            })?;
        return Ok(());
    }

    Err(ApiError::internal(format!(
        "Unsupported operating system: {}",
        std::env::consts::OS
    )))
}

async fn get_correct_path(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    path: Option<String>,
) -> ApiResult<String> {
    let trimmed_path = path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(path) = trimmed_path {
        let files = get_existing_files_for_sha256(conn, sha256).await?;
        if files.is_empty() || !files.iter().any(|file| file.path == path) {
            let available = files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>()
                .join(", ");
            tracing::debug!(
                sha256 = %sha256,
                path = %path,
                available = %available,
                "open path not found"
            );
            return Err(ApiError::internal(format!(
                "404: File {path} not found in {available}"
            )));
        }
        return Ok(path.to_string());
    }

    let files = get_existing_files_for_sha256(conn, sha256).await?;
    if let Some(file) = files.first() {
        return Ok(file.path.clone());
    }

    Err(ApiError::internal("404: File not found"))
}

#[utoipa::path(
    post,
    path = "/api/open/file/{sha256}",
    tag = "open",
    summary = "Open a file in the default application",
    description = "Open a file in the default application on the host system.\nThis is done using os.startfile on Windows and xdg-open on Linux.\nThis is a potentially dangerous operation, as it can execute arbitrary code.",
    params(
        DbQueryParams,
        ("sha256" = String, Path),
        OpenQuery
    ),
    responses(
        (status = 200, description = "File open request issued", body = OpenResponse)
    )
)]
pub async fn open_file_on_host(
    Path(sha256): Path<String>,
    Query(query): Query<OpenQuery>,
    mut db: DbConnection<ReadOnly>,
) -> ApiResult<Json<OpenResponse>> {
    let path = get_correct_path(&mut db.conn, &sha256, query.path).await?;
    open_file(FsPath::new(&path)).await?;
    Ok(Json(OpenResponse {
        path: path.clone(),
        message: format!("Attempting to open: {path}"),
    }))
}

#[utoipa::path(
    post,
    path = "/api/open/folder/{sha256}",
    tag = "open",
    summary = "Show a file in the host system's file manager",
    description = "Show a file in the host system's file manager.\nThis is done using the appropriate command for the host system.\nOn Windows, the file is highlighted in the Windows Explorer.\nOn macOS, the file is revealed in the Finder.\nThis is a potentially dangerous operation.",
    params(
        DbQueryParams,
        ("sha256" = String, Path),
        OpenQuery
    ),
    responses(
        (status = 200, description = "File explorer request issued", body = OpenResponse)
    )
)]
pub async fn show_in_file_manager(
    Path(sha256): Path<String>,
    Query(query): Query<OpenQuery>,
    mut db: DbConnection<ReadOnly>,
) -> ApiResult<Json<OpenResponse>> {
    let path = get_correct_path(&mut db.conn, &sha256, query.path).await?;
    show_in_fm(FsPath::new(&path)).await?;
    Ok(Json(OpenResponse {
        path: path.clone(),
        message: format!("Attempting to open: {path}"),
    }))
}
