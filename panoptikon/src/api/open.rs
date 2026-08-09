use axum::{Json, extract::Path};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use std::path::Path as FsPath;
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

async fn execute_direct_command(program: &str, args: &[String], path: &FsPath) -> ApiResult<()> {
    let folder = path.parent().unwrap_or(path).to_string_lossy();
    let filename = path.file_name().unwrap_or_default().to_string_lossy();
    let expand = |value: &str| {
        value
            .replace("{path}", &path.to_string_lossy())
            .replace("{folder}", &folder)
            .replace("{filename}", &filename)
    };
    Command::new(expand(program))
        .args(args.iter().map(|arg| expand(arg)))
        .spawn()
        .map_err(|error| {
            ApiError::internal(format!("Failed to start custom file action: {error}"))
        })?;
    Ok(())
}

/// Open the specified file using the default application.
///
/// `path`: The path to the file to be opened.
async fn open_file(path: &FsPath) -> ApiResult<()> {
    if let Some(program) = crate::config::runtime()
        .open
        .file_program
        .clone()
        .filter(|value| !value.trim().is_empty())
    {
        execute_direct_command(&program, &crate::config::runtime().open.file_args, path).await?;
        return Ok(());
    }
    if let Some(custom_cmd) = crate::config::runtime().open.file_command.clone() {
        execute_custom_command("open.file_command", &custom_cmd, path).await?;
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

/// Whether the clipboard write is known to have finished by the time
/// [`copy_to_clipboard`] returns, which is what the response message may
/// claim.
enum ClipboardWrite {
    /// The native crate wrote the clipboard before returning.
    Completed,
    /// A custom program or command was handed the path; its own outcome is
    /// not observed here (a direct program is only spawned, and an empty
    /// template is a deliberate no-op).
    Attempted,
}

/// Place an OS-native reference to the file on the host's clipboard, so that
/// pasting it elsewhere attaches the file itself.
///
/// Precedence mirrors [`open_file`]: `open.clipboard_program` (direct exec,
/// no shell) beats `open.clipboard_command` (shell template), and both beat
/// the built-in native write. Both custom forms expand the same `{path}`,
/// `{folder}` and `{filename}` placeholders as the other verbs.
async fn copy_to_clipboard(path: &FsPath) -> ApiResult<ClipboardWrite> {
    if let Some(program) = crate::config::runtime()
        .open
        .clipboard_program
        .clone()
        .filter(|value| !value.trim().is_empty())
    {
        execute_direct_command(
            &program,
            &crate::config::runtime().open.clipboard_args,
            path,
        )
        .await?;
        return Ok(ClipboardWrite::Attempted);
    }
    if let Some(custom_cmd) = crate::config::runtime().open.clipboard_command.clone() {
        execute_custom_command("open.clipboard_command", &custom_cmd, path).await?;
        return Ok(ClipboardWrite::Attempted);
    }

    // The native write is blocking (and on Windows pumps a message loop), so
    // it goes to a blocking thread rather than the async worker. macOS
    // pasteboard writes normally belong on the main thread, but this process
    // has no AppKit run loop to dispatch to — headless when run bare, and a
    // separate process from Tauri when run under the Desktop app — so there is
    // no UI thread here to race.
    let owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || panoptikon_clipboard::copy_files_to_clipboard(&[owned]))
        .await
        .map_err(|err| ApiError::internal(format!("clipboard task failed: {err}")))?
        .map_err(|err| ApiError::internal(format!("{err:#}")))?;
    Ok(ClipboardWrite::Completed)
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

    if let Some(program) = crate::config::runtime()
        .open
        .folder_program
        .clone()
        .filter(|value| !value.trim().is_empty())
    {
        execute_direct_command(&program, &crate::config::runtime().open.folder_args, path).await?;
        return Ok(());
    }
    if let Some(custom_cmd) = crate::config::runtime().open.folder_command.clone() {
        execute_custom_command("open.folder_command", &custom_cmd, path).await?;
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

/// Resolves the on-disk path for a hash, honouring an optional path hint when
/// the same content is indexed under several paths.
///
/// The inner `Err` is "no such file", kept apart from the outer one (a failed
/// database read) because the handler families report it with different
/// status codes.
async fn resolve_path(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    path: Option<String>,
) -> ApiResult<Result<String, String>> {
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
            return Ok(Err(format!("File {path} not found in {available}")));
        }
        return Ok(Ok(path.to_string()));
    }

    let files = get_existing_files_for_sha256(conn, sha256).await?;
    if let Some(file) = files.first() {
        return Ok(Ok(file.path.clone()));
    }

    Ok(Err("File not found".to_string()))
}

/// The `open`/`folder` handlers' historical error shape: a 500 whose body
/// merely *says* 404. Preserved verbatim for them; new endpoints use the real
/// status instead.
async fn get_correct_path(
    conn: &mut sqlx::SqliteConnection,
    sha256: &str,
    path: Option<String>,
) -> ApiResult<String> {
    resolve_path(conn, sha256, path)
        .await?
        .map_err(|reason| ApiError::internal(format!("404: {reason}")))
}

#[utoipa::path(
    post,
    operation_id = "open_file_on_host",
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
    operation_id = "show_in_file_manager",
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

#[utoipa::path(
    post,
    operation_id = "copy_file_to_clipboard_on_host",
    path = "/api/open/clipboard/{sha256}",
    tag = "open",
    summary = "Copy a file to the host system's clipboard",
    description = "Place an OS-native reference to the file on the clipboard of the machine running the server, so that pasting it into a file manager, a chat client or an upload form attaches the original file.\nOnly the path travels; the file's contents are never read.\nThe write targets the *server's* clipboard, so this is only useful when the server and the browser share a machine (or when a custom open.clipboard_command forwards it elsewhere).\nThis is a potentially dangerous operation, as a custom command can execute arbitrary code.",
    params(
        DbQueryParams,
        ("sha256" = String, Path),
        OpenQuery
    ),
    responses(
        (status = 200, description = "File copied to the host clipboard", body = OpenResponse)
    )
)]
pub async fn copy_file_to_clipboard_on_host(
    Path(sha256): Path<String>,
    Query(query): Query<OpenQuery>,
    mut db: DbConnection<ReadOnly>,
) -> ApiResult<Json<OpenResponse>> {
    let path = resolve_path(&mut db.conn, &sha256, query.path)
        .await?
        .map_err(ApiError::not_found)?;
    let message = match copy_to_clipboard(FsPath::new(&path)).await? {
        ClipboardWrite::Completed => format!("Copied to clipboard: {path}"),
        // Hedged like the sibling verbs: a custom command owns the outcome.
        ClipboardWrite::Attempted => format!("Attempting to copy to clipboard: {path}"),
    };
    Ok(Json(OpenResponse {
        path: path.clone(),
        message,
    }))
}
