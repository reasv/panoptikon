use axum::{Json, extract::Path};
use axum_extra::extract::Query;
use serde::{Deserialize, Serialize};
use std::path::{Path as FsPath, PathBuf};
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

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ClipboardArtifactQuery {
    /// The artifact cache key, exactly as `ArtifactRef.key` carries it.
    key: String,
    /// The download name the client was handed on `ArtifactRef.filename`.
    ///
    /// Optional: without it the artifact's stored name is used. It is
    /// re-sanitized inside `materialize_share` (single path component,
    /// length-capped), so a hostile value degrades to a safe name rather
    /// than escaping the share directory.
    name: Option<String>,
}

/// The shell that re-reads a substituted command line, and therefore the
/// quoting rules its values must obey.
///
/// It is a parameter rather than a `cfg!` buried inside the quoting helpers so
/// that both rule sets stay compiled — and testable — on either host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shell {
    /// `cmd.exe`, invoked as `cmd /C "<line>"`.
    Cmd,
    /// POSIX word rules, as re-tokenized by [`shell_words::split`].
    Posix,
}

/// The shell this build's [`execute_custom_command`] actually runs.
const HOST_SHELL: Shell = if cfg!(windows) {
    Shell::Cmd
} else {
    Shell::Posix
};

/// How a placeholder's value is rendered into the text that replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Substitution {
    /// Direct (non-shell) execution: every value becomes an argv entry of its
    /// own, so no shell ever re-reads it and nothing needs quoting.
    Argv,
    /// The location verbs (`open.file_command`, `open.folder_command`): the
    /// executor wraps the value in `"…"`, which is what those templates have
    /// always assumed (`file_command = "mpv {path}"` carries no quotes of its
    /// own). Kept, because changing it would rewrite every existing
    /// configuration.
    LocationShell,
    /// The clipboard verb (`open.clipboard_command`): the value is fully
    /// quoted for the target shell. This is the Desktop relay's convention for
    /// the same verb, so the one shared editor's template behaves identically
    /// on both engines — and it means such a template must *not* add quotes of
    /// its own.
    ClipboardShell,
}

/// POSIX single-quoting: everything inside `'…'` is literal, and the only
/// character that cannot appear there is `'` itself, which is closed, escaped
/// and reopened.
fn quote_posix(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// `cmd.exe` quoting.
///
/// Inside a double-quoted region cmd stops treating `& | < > ^ ( )` as syntax,
/// so wrapping is enough for all of them. `"` would close the region and hand
/// the rest of the value to the parser, and cmd offers no way to escape it, so
/// it is dropped — no Windows path may contain one, which makes this a
/// backstop rather than a lossy transform. `%` still expands `%NAME%` inside
/// quotes and cannot be escaped there either, so it is left alone: every value
/// here comes from a path this server indexed on this machine.
fn quote_cmd(value: &str) -> String {
    format!("\"{}\"", value.replace('"', ""))
}

/// The location verbs' historical `"…"` wrapping, with the value made unable
/// to end that region early.
///
/// On POSIX the wrapped line is re-tokenized by [`shell_words::split`], where
/// `\` and `"` are both meaningful inside `"…"`. Filenames may legally contain
/// either, and unescaped they would inject extra argv entries into the user's
/// own tool (or, at an odd count, fail the split and 500 the request). Order
/// matters: backslashes are doubled first, and that pass emits no `"` for the
/// second one to see. `cmd.exe` has no backslash escape and no path can hold a
/// `"`, so the Windows form stays byte-for-byte what it always was.
fn wrap_location_value(value: &str, shell: Shell) -> String {
    match shell {
        Shell::Cmd => format!("\"{value}\""),
        Shell::Posix => format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")),
    }
}

fn render_value(value: &str, substitution: Substitution, shell: Shell) -> String {
    match substitution {
        Substitution::Argv => value.to_string(),
        Substitution::LocationShell => wrap_location_value(value, shell),
        Substitution::ClipboardShell => match shell {
            Shell::Cmd => quote_cmd(value),
            Shell::Posix => quote_posix(value),
        },
    }
}

const PLACEHOLDERS: [&str; 3] = ["{path}", "{folder}", "{filename}"];

/// Expands `{path}`, `{folder}` and `{filename}` in `template`.
///
/// Single pass, by construction: the template is scanned once and replacement
/// text is only ever *appended* to the output, never rescanned. A chain of
/// `str::replace` calls instead re-reads what the previous call emitted, so a
/// filename containing the literal text `{filename}` (`{`, `}` and `&` are all
/// legal in a filename) would be substituted a second time — splicing a fresh
/// quote pair into the middle of the already-quoted value and leaving the rest
/// of the name outside the quotes, where `cmd.exe` reads it as syntax. That is
/// remote-triggerable command execution, so this must stay single-pass.
fn substitute_placeholders(
    template: &str,
    path: &FsPath,
    substitution: Substitution,
    shell: Shell,
) -> String {
    let full = path.to_string_lossy().into_owned();
    let folder = path.parent().unwrap_or(path).to_string_lossy().into_owned();
    let filename = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let values = [full.as_str(), folder.as_str(), filename.as_str()];

    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    'scan: loop {
        let Some(brace) = rest.find('{') else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..brace]);
        let tail = &rest[brace..];
        for (placeholder, value) in PLACEHOLDERS.iter().zip(values) {
            if let Some(remainder) = tail.strip_prefix(placeholder) {
                out.push_str(&render_value(value, substitution, shell));
                // Resume *after* the replacement: emitted text is never
                // re-examined, so a value that looks like a placeholder is
                // inert data.
                rest = remainder;
                continue 'scan;
            }
        }
        out.push('{');
        rest = &tail[1..];
    }
    out
}

/// `tokio::process::Command::args` escapes for `CommandLineToArgvW`, which
/// cmd.exe does not implement: it turns a template's `"` into `\"`, which cmd
/// passes to the child as a literal quote and then splits the value on its
/// spaces anyway — so a documented `mytool "{path}"` breaks for any path with
/// a space, and a quoted *program* fails to launch at all. `raw_arg` writes the
/// command line verbatim instead, and the extra outer pair is exactly what
/// cmd's documented `/C` rule strips back off. This mirrors the Desktop's
/// `spawn_shell`, which is important: the Desktop's shared "File opening on
/// this computer" editor writes the same template string into these `[open]`
/// keys, so the two engines must read it the same way.
#[cfg(windows)]
async fn run_shell_command(command: &str) -> std::io::Result<()> {
    Command::new("cmd")
        .raw_arg("/C")
        .raw_arg(format!("\"{command}\""))
        .status()
        .await?;
    Ok(())
}

/// No shell is spawned: the line is tokenized here and the program executed
/// directly, so the values substituted into it only need to survive
/// [`shell_words::split`].
#[cfg(not(windows))]
async fn run_shell_command(command: &str) -> std::io::Result<()> {
    let args = shell_words::split(command)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    if args.is_empty() {
        return Ok(());
    }
    Command::new(&args[0]).args(&args[1..]).status().await?;
    Ok(())
}

/// Executes the custom command by replacing placeholders with actual values.
///
/// `command_template`: The command template with placeholders.
/// `path`: The full path to the file.
/// `substitution`: how the values are quoted — see [`Substitution`].
async fn execute_custom_command(
    command_name: &str,
    command_template: &str,
    path: &FsPath,
    substitution: Substitution,
) -> ApiResult<()> {
    let command = substitute_placeholders(command_template, path, substitution, HOST_SHELL);
    if command.trim().is_empty() {
        return Ok(());
    }

    run_shell_command(&command).await.map_err(|err| {
        ApiError::internal(format!(
            "Failed to execute custom {command_name} for path '{}': {err}",
            path.display()
        ))
    })
}

async fn execute_direct_command(program: &str, args: &[String], path: &FsPath) -> ApiResult<()> {
    let expand = |value: &str| substitute_placeholders(value, path, Substitution::Argv, HOST_SHELL);
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
        execute_custom_command(
            "open.file_command",
            &custom_cmd,
            path,
            Substitution::LocationShell,
        )
        .await?;
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
/// [`copy_to_clipboard`] returns, which is exactly what the response message
/// is allowed to claim. The two are worded to be read verbatim as a toast, so
/// a client that shows `message` never over-promises on a custom
/// configuration.
enum ClipboardWrite {
    /// The native crate wrote the clipboard before returning: "Copied…".
    Completed,
    /// A custom program or command was handed the path; its own outcome is
    /// not observed here (a direct program is only spawned, a shell command's
    /// exit status is not inspected, and an empty template is a deliberate
    /// no-op). Hence "Attempting…" rather than "Copied…".
    Attempted,
}

/// Which mechanism a configuration selects for the clipboard verb.
///
/// Precedence mirrors [`open_file`]: `open.clipboard_program` (direct exec, no
/// shell) beats `open.clipboard_command` (shell template), and both beat the
/// built-in native write. Split out from [`copy_to_clipboard`] so the
/// three-way choice can be tested without a database or a live clipboard.
#[derive(Debug, PartialEq, Eq)]
enum ClipboardAction<'a> {
    /// `clipboard_program` (+ `clipboard_args`), executed without a shell.
    Direct {
        program: &'a str,
        args: &'a [String],
    },
    /// `clipboard_command`, a shell template. An empty template is a
    /// deliberate no-op and deliberately does *not* fall through to the native
    /// write.
    Shell(&'a str),
    /// The built-in `panoptikon-clipboard` write.
    Native,
}

fn select_clipboard_action(open: &crate::config::OpenConfig) -> ClipboardAction<'_> {
    if let Some(program) = open
        .clipboard_program
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return ClipboardAction::Direct {
            program,
            args: &open.clipboard_args,
        };
    }
    if let Some(command) = open.clipboard_command.as_deref() {
        return ClipboardAction::Shell(command);
    }
    ClipboardAction::Native
}

/// Place an OS-native reference to the file on the host's clipboard, so that
/// pasting it elsewhere attaches the file itself.
///
/// Both custom forms expand the same `{path}`, `{folder}` and `{filename}`
/// placeholders as the other verbs, but the shell form quotes them
/// ([`Substitution::ClipboardShell`]) where the location verbs do not.
async fn copy_to_clipboard(path: &FsPath) -> ApiResult<ClipboardWrite> {
    match select_clipboard_action(&crate::config::runtime().open) {
        ClipboardAction::Direct { program, args } => {
            execute_direct_command(program, args, path).await?;
            return Ok(ClipboardWrite::Attempted);
        }
        ClipboardAction::Shell(command) => {
            execute_custom_command(
                "open.clipboard_command",
                command,
                path,
                Substitution::ClipboardShell,
            )
            .await?;
            return Ok(ClipboardWrite::Attempted);
        }
        ClipboardAction::Native => {}
    }

    // The native write is blocking — the Windows backend retries
    // `OpenClipboard` with sleeps in between while another process owns it —
    // so it goes to a blocking thread rather than the async worker. macOS
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
        execute_custom_command(
            "open.folder_command",
            &custom_cmd,
            path,
            Substitution::LocationShell,
        )
        .await?;
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

/// Resolve a cached transcode artifact to a path that carries a *human* file
/// name, materializing that name if it does not exist yet.
///
/// Split out of the handler below so the half that can fail on this machine's
/// filesystem is testable without writing to a real clipboard: the clipboard
/// half reads `config::runtime()`, a process-global `OnceLock` that no test
/// can point at a benign program for its own duration.
///
/// Takes the whole query rather than two positional `&str`s: `key` and `name`
/// are the same type, and a swapped call site would 404 its way past every
/// test (an empty-name lookup misses with the very detail the miss test pins).
async fn artifact_share_path(query: &ClipboardArtifactQuery) -> ApiResult<PathBuf> {
    let key = query.key.as_str();
    // A present-but-blank `name=` means "no name", exactly as `resolve_path`
    // treats its `path` — otherwise `Some("")` would shadow the stored
    // download name and the paste would carry the content-addressed one.
    let download_name = query
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let cache = crate::media_tools::transcode::pool::transcode_cache().await?;
    // Also counts as a hit, which is wanted: an artifact the user is actively
    // handing to another application is not a good eviction victim.
    let Some(artifact) = cache.lookup(key).await else {
        // Same wording as the artifact route's miss, so a client that sees
        // one recognizes the other.
        return Err(ApiError::not_found("No cached artifact for this key"));
    };
    cache
        .materialize_share(&artifact, download_name)
        .await
        .map_err(|err| {
            // The chain names the cache directory, so it is logged and not
            // returned — the same rule `ResizeError` documents in cache.rs.
            tracing::error!(error = ?err, key, "failed to materialize a share entry");
            ApiError::internal("Failed to prepare the file for the clipboard")
        })
}

#[utoipa::path(
    post,
    operation_id = "copy_artifact_to_clipboard_on_host",
    path = "/api/open/clipboard/artifact",
    tag = "open",
    summary = "Copy a cached transcode artifact to the host system's clipboard",
    description = "Place an OS-native reference to a finished rendition (a clip, a converted \
video, a mosaic) on the clipboard of the machine running the server, so that pasting it into a \
file manager, a chat client or an upload form attaches that file.\nThe artifact is stored under \
its content-addressed `<key>.<ext>` name, which is useless to paste, so the path handed to the \
clipboard is a hardlinked view of the same bytes under a human file name; it is created on \
demand and removed with the artifact.\nThe write targets the *server's* clipboard, so this is \
only useful when the server and the browser share a machine (or when a custom \
open.clipboard_command forwards it elsewhere).\nThis is a potentially dangerous operation, as a \
custom command can execute arbitrary code.",
    params(ClipboardArtifactQuery),
    responses(
        (status = 200, description = "Artifact copied to the host clipboard", body = OpenResponse),
        (status = 404, description = "No cached artifact for this key", body = crate::api_error::ErrorBody)
    )
)]
pub async fn copy_artifact_to_clipboard_on_host(
    Query(query): Query<ClipboardArtifactQuery>,
) -> ApiResult<Json<OpenResponse>> {
    // No `DbConnection`: the transcode cache is process-global and keyed by
    // content, so nothing here reads an index.
    let share_path = artifact_share_path(&query).await?;
    let path = share_path.to_string_lossy().into_owned();
    let message = match copy_to_clipboard(&share_path).await? {
        ClipboardWrite::Completed => format!("Copied to clipboard: {path}"),
        // Hedged like the sibling verbs: a custom command owns the outcome.
        ClipboardWrite::Attempted => format!("Attempting to copy to clipboard: {path}"),
    };
    Ok(Json(OpenResponse {
        path: path.clone(),
        message,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OpenConfig;

    /// Both shells' rules are exercised on every host, so a Windows-only
    /// quoting regression cannot hide from a Linux CI run and vice versa.
    const SHELLS: [Shell; 2] = [Shell::Cmd, Shell::Posix];

    /// `{`, `}` and `&` are all legal in a filename on every platform this
    /// runs on, so a name that spells out a placeholder costs an attacker
    /// nothing to create — no `"` required.
    const HOSTILE: &str = "/media/ab/a{filename}{folder}&calc&b.png";

    fn open_config(program: Option<&str>, command: Option<&str>) -> OpenConfig {
        OpenConfig {
            clipboard_program: program.map(str::to_string),
            clipboard_args: vec!["--file".to_string(), "{path}".to_string()],
            clipboard_command: command.map(str::to_string),
            ..OpenConfig::default()
        }
    }

    /// The substitution must not re-read its own output. A filename holding
    /// the literal text `{filename}` was, under the old `str::replace` chain,
    /// substituted a second time: the `{path}` pass spliced the name into the
    /// quoted value, and the later `{filename}` pass then replaced the token
    /// *inside* it with another quoted copy — which closes the quoted region
    /// mid-value and leaves the rest of the name (`&calc&`) outside it, where
    /// `cmd /C` reads `&` as a command separator. Remote-triggerable code
    /// execution, so this is asserted structurally: exactly one quote pair,
    /// and the placeholder text survives only as inert data.
    #[test]
    fn placeholder_text_inside_a_filename_is_never_re_substituted() {
        let path = FsPath::new(HOSTILE);
        for shell in SHELLS {
            let command =
                substitute_placeholders("myclip {path}", path, Substitution::LocationShell, shell);
            assert_eq!(
                command,
                format!("myclip \"{HOSTILE}\""),
                "the name is one quoted value and nothing else ({shell:?})"
            );
            assert_eq!(
                command.matches('"').count(),
                2,
                "exactly one quote pair, so nothing escaped it ({shell:?}): {command}"
            );
            assert_eq!(
                command.matches("{filename}").count(),
                1,
                "the token stayed literal data ({shell:?}): {command}"
            );
            assert_eq!(
                command.matches("&calc&").count(),
                1,
                "the payload appears once, inside the quotes ({shell:?}): {command}"
            );
        }

        // The clipboard verb quotes rather than wraps, but the same rule
        // holds: POSIX single quotes leave no `"` at all.
        let cmd_form = substitute_placeholders(
            "myclip {path}",
            path,
            Substitution::ClipboardShell,
            Shell::Cmd,
        );
        assert_eq!(cmd_form, format!("myclip \"{HOSTILE}\""));
        assert_eq!(cmd_form.matches('"').count(), 2);
        let posix_form = substitute_placeholders(
            "myclip {path}",
            path,
            Substitution::ClipboardShell,
            Shell::Posix,
        );
        assert_eq!(posix_form, format!("myclip '{HOSTILE}'"));
        assert_eq!(posix_form.matches('"').count(), 0);
        assert_eq!(posix_form.matches('\'').count(), 2);
    }

    /// The other two placeholders read from the same hostile name, and a
    /// template using them must not gain a second substitution round either.
    #[test]
    fn every_placeholder_is_expanded_exactly_once() {
        let path = FsPath::new(HOSTILE);
        let command = substitute_placeholders(
            "tool {folder} {filename} {path}",
            path,
            Substitution::LocationShell,
            Shell::Posix,
        );
        assert_eq!(
            command,
            "tool \"/media/ab\" \"a{filename}{folder}&calc&b.png\" \"/media/ab/a{filename}{folder}&calc&b.png\""
        );
        assert_eq!(command.matches('"').count(), 6);
    }

    /// A brace that starts no known placeholder is copied through, and the
    /// scan resumes one character later rather than skipping to the next
    /// placeholder.
    #[test]
    fn unknown_braces_pass_through_unchanged() {
        let path = FsPath::new("/media/x.png");
        assert_eq!(
            substitute_placeholders(
                "echo {nope} { {pathx} {path}",
                path,
                Substitution::Argv,
                Shell::Posix
            ),
            "echo {nope} { {pathx} /media/x.png"
        );
    }

    /// Direct execution hands each value to the OS as its own argv entry, so
    /// it must not acquire quotes of any kind.
    #[test]
    fn argv_substitution_adds_no_quoting() {
        let path = FsPath::new("/media/a'b\"c.png");
        for shell in SHELLS {
            assert_eq!(
                substitute_placeholders("{path}", path, Substitution::Argv, shell),
                "/media/a'b\"c.png"
            );
        }
    }

    /// A `"` in a filename used to end the location verbs' quoted region
    /// early: the rest of the name became extra argv *flags* for the user's
    /// own tool, and an odd number of quotes made `shell_words::split` fail —
    /// a hard 500 from a filename. Escaped, the value is one argument again.
    #[test]
    fn a_quote_in_a_filename_cannot_add_arguments_on_posix() {
        let hostile = "/media/a\" --output /tmp/x \"b.png";
        let command = substitute_placeholders(
            "mytool {path}",
            FsPath::new(hostile),
            Substitution::LocationShell,
            Shell::Posix,
        );
        let args = shell_words::split(&command).expect("the line still tokenizes");
        assert_eq!(args, vec!["mytool".to_string(), hostile.to_string()]);
    }

    /// The odd-quote case specifically: it must tokenize at all.
    #[test]
    fn an_odd_quote_count_no_longer_fails_the_split() {
        for template in ["mytool {path}", "mytool {filename}"] {
            for substitution in [Substitution::LocationShell, Substitution::ClipboardShell] {
                let command = substitute_placeholders(
                    template,
                    FsPath::new("/media/a\"b.png"),
                    substitution,
                    Shell::Posix,
                );
                let args = shell_words::split(&command)
                    .unwrap_or_else(|err| panic!("{template} / {substitution:?}: {err}"));
                assert_eq!(args.len(), 2, "{command}");
                assert!(args[1].contains('"'), "the quote survives: {command}");
            }
        }
    }

    /// Backslashes are meaningful inside a POSIX double-quoted region, so a
    /// name containing one must not lose it (or eat the closing quote).
    #[test]
    fn backslashes_survive_the_location_wrapping_on_posix() {
        let hostile = "/media/a\\b\\.png";
        let command = substitute_placeholders(
            "mytool {path}",
            FsPath::new(hostile),
            Substitution::LocationShell,
            Shell::Posix,
        );
        assert_eq!(command, "mytool \"/media/a\\\\b\\\\.png\"");
        let args = shell_words::split(&command).expect("the line still tokenizes");
        assert_eq!(args, vec!["mytool".to_string(), hostile.to_string()]);
    }

    /// Windows keeps the historical wrapping byte for byte: `cmd.exe` has no
    /// backslash escape, and every indexed path is full of backslashes.
    #[test]
    fn the_windows_location_wrapping_is_unchanged() {
        assert_eq!(
            substitute_placeholders(
                "explorer /select,{path}",
                FsPath::new("C:/media/my file.png"),
                Substitution::LocationShell,
                Shell::Cmd,
            ),
            "explorer /select,\"C:/media/my file.png\""
        );
    }

    /// POSIX single-quoting has to close, escape and reopen for a `'`; the
    /// point is that the shell (here its tokenizer) reads back the exact name,
    /// with the backticks and `$(…)` inert.
    #[test]
    fn clipboard_quoting_survives_a_single_quote_on_posix() {
        let hostile = "/media/it's $(id) `whoami`.png";
        let command = substitute_placeholders(
            "myclip {path}",
            FsPath::new(hostile),
            Substitution::ClipboardShell,
            Shell::Posix,
        );
        let args = shell_words::split(&command).expect("the line still tokenizes");
        assert_eq!(args, vec!["myclip".to_string(), hostile.to_string()]);
    }

    /// `cmd.exe` cannot escape a `"`, so the clipboard verb drops it rather
    /// than emit a line whose quoting is unbalanced. No Windows path can
    /// contain one, so this is a backstop.
    #[test]
    fn clipboard_quoting_drops_an_unescapable_quote_on_cmd() {
        let command = substitute_placeholders(
            "myclip {path}",
            FsPath::new("/media/a\"&calc&b.png"),
            Substitution::ClipboardShell,
            Shell::Cmd,
        );
        assert_eq!(command, "myclip \"/media/a&calc&b.png\"");
        assert_eq!(command.matches('"').count(), 2);
    }

    /// An empty template is a no-op for every verb, which is what the config
    /// documents (`""` disables the endpoint).
    #[test]
    fn an_empty_template_substitutes_to_nothing() {
        assert!(
            substitute_placeholders(
                "",
                FsPath::new(HOSTILE),
                Substitution::ClipboardShell,
                Shell::Posix
            )
            .trim()
            .is_empty()
        );
    }

    /// The three-way precedence, which nothing exercised before: a serde test
    /// proving the keys parse passes even if the handler ignores them all.
    #[test]
    fn the_direct_clipboard_program_wins_over_the_shell_template() {
        let config = open_config(Some("my-clipboard-tool"), Some("my-clipboard-shell {path}"));
        assert_eq!(
            select_clipboard_action(&config),
            ClipboardAction::Direct {
                program: "my-clipboard-tool",
                args: &config.clipboard_args,
            }
        );
    }

    #[test]
    fn the_shell_template_wins_over_the_native_write() {
        let config = open_config(None, Some("my-clipboard-shell {path}"));
        assert_eq!(
            select_clipboard_action(&config),
            ClipboardAction::Shell("my-clipboard-shell {path}")
        );
    }

    /// A blank program is treated as unset — the same `trim`-based rule the
    /// other verbs use — so it does not shadow the shell template.
    #[test]
    fn a_blank_clipboard_program_falls_through_to_the_shell_template() {
        let config = open_config(Some("   "), Some("my-clipboard-shell {path}"));
        assert_eq!(
            select_clipboard_action(&config),
            ClipboardAction::Shell("my-clipboard-shell {path}")
        );
    }

    /// With nothing configured the built-in native write runs.
    #[test]
    fn an_unconfigured_clipboard_uses_the_native_write() {
        assert_eq!(
            select_clipboard_action(&open_config(None, None)),
            ClipboardAction::Native
        );
    }

    /// An explicitly empty template is a *silent no-op*, not a fall-through:
    /// a user who disables the verb must not get the native write instead.
    #[test]
    fn an_empty_clipboard_command_is_a_silent_no_op() {
        let config = open_config(None, Some(""));
        let action = select_clipboard_action(&config);
        assert_eq!(action, ClipboardAction::Shell(""));
        let ClipboardAction::Shell(template) = action else {
            unreachable!()
        };
        assert!(
            substitute_placeholders(
                template,
                FsPath::new(HOSTILE),
                Substitution::ClipboardShell,
                HOST_SHELL
            )
            .trim()
            .is_empty(),
            "and it expands to nothing, so no process is spawned"
        );
    }

    // --- the artifact clipboard verb ----------------------------------------
    //
    // Only the resolution half is exercised end-to-end. The write itself
    // reads `config::runtime()`, a process-global `OnceLock` installed by
    // whichever test runs first, so no test can point `clipboard_program` at
    // a benign command for its own duration — a success test through the
    // handler would write to the developer's real clipboard. Hence
    // [`artifact_share_path`]: everything that can fail on this machine lives
    // there, and the handler adds only the (already covered) clipboard call.

    use axum::Router;
    use axum::body::Body as AxumBody;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use tower::ServiceExt;

    use crate::media_tools::transcode::cache::NewArtifact;
    use crate::media_tools::transcode::pool;

    /// Commits ten known bytes under `key` and hands back the cache directory
    /// they landed in.
    async fn commit_artifact(key: &str, download_name: &str) -> PathBuf {
        let cache = pool::transcode_cache().await.expect("the cache opens");
        let temp = cache.temp_path("mp4");
        std::fs::write(&temp, b"0123456789").unwrap();
        let artifact = cache
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
                &temp,
            )
            .await
            .unwrap();
        artifact.path.parent().unwrap().to_path_buf()
    }

    /// The query as the handler would have parsed it, so the seam tests speak
    /// the same named-field language and cannot swap `key` for `name`.
    fn share_query(key: &str, name: Option<&str>) -> ClipboardArtifactQuery {
        ClipboardArtifactQuery {
            key: key.to_string(),
            name: name.map(str::to_string),
        }
    }

    /// A key nothing has encoded is a 404 — and the literal `artifact`
    /// segment reaches this handler rather than being read as a file hash by
    /// the `{sha256}` sibling registered next to it (matchit prefers the
    /// literal, whatever the registration order). A misrouted request would
    /// land on the sibling, which answers 400 for the missing db params.
    #[tokio::test]
    async fn the_artifact_clipboard_verb_404s_on_an_unknown_key() {
        let _env = crate::test_utils::test_data_dir();
        let app = Router::new()
            .route(
                "/api/open/clipboard/{sha256}",
                post(copy_file_to_clipboard_on_host),
            )
            .route(
                "/api/open/clipboard/artifact",
                post(copy_artifact_to_clipboard_on_host),
            );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/open/clipboard/artifact?key=nothing-here")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            json["detail"]
                .as_str()
                .unwrap()
                .contains("No cached artifact"),
            "the miss wording mirrors the artifact route: {json}"
        );
    }

    /// The resolution half: the caller's name wins over the stored one, the
    /// entry carries the artifact's bytes, and a name that spells a traversal
    /// stays a single component inside `share/<key>/`.
    #[tokio::test]
    async fn the_artifact_clipboard_verb_materializes_a_named_view() {
        let _env = crate::test_utils::test_data_dir();
        let key = "open-clipboard-artifact-named";
        commit_artifact(key, "stored-name.mp4").await;

        let path = artifact_share_path(&share_query(key, Some("My Clip.mp4")))
            .await
            .expect("a cached artifact materializes");
        assert_eq!(path.file_name().unwrap(), "My Clip.mp4");
        assert_eq!(path.parent().unwrap().file_name().unwrap(), key);
        assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");

        // Present-but-blank is "no name": the stored download name must win,
        // not the content-addressed on-disk fallback a `Some("")` would buy.
        let blank = artifact_share_path(&share_query(key, Some("   ")))
            .await
            .expect("a blank name falls back to the stored one");
        assert_eq!(blank.file_name().unwrap(), "stored-name.mp4");

        let hostile = artifact_share_path(&share_query(key, Some("../../evil.mp4")))
            .await
            .expect("a hostile name degrades rather than failing");
        assert_eq!(
            hostile.parent().unwrap(),
            path.parent().unwrap(),
            "still one component inside share/<key>: {}",
            hostile.display()
        );
    }

    /// The failure body must not name the cache. `materialize_share`'s
    /// `anyhow` chain embeds absolute paths (it is written for the log), so
    /// the handler answers with a fixed sentence instead — the same rule
    /// `ResizeError` documents in cache.rs.
    #[tokio::test]
    async fn a_share_failure_never_names_the_cache_directory() {
        let _env = crate::test_utils::test_data_dir();
        let key = "open-clipboard-artifact-blocked";
        let cache_dir = commit_artifact(key, "stored-name.mp4").await;

        // A *file* where the entry's directory has to go: `create_dir_all`
        // fails on it deterministically, on every platform.
        let blocker = cache_dir.join("share").join(key);
        std::fs::create_dir_all(blocker.parent().unwrap()).unwrap();
        std::fs::write(&blocker, b"not a directory").unwrap();

        let error = artifact_share_path(&share_query(key, Some("My Clip.mp4")))
            .await
            .expect_err("the share entry cannot be created");
        let detail = error.detail().to_string();
        assert!(
            !detail.contains(&*cache_dir.to_string_lossy()),
            "the cache path leaked into the body: {detail}"
        );
        assert!(
            !detail.contains(key) && !detail.contains("share"),
            "nothing about the store's layout belongs in the body: {detail}"
        );
        assert_eq!(
            error.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );

        let _ = std::fs::remove_file(&blocker);
    }
}
