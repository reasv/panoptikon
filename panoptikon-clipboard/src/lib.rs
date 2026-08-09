//! OS-native *file reference* clipboard writes.
//!
//! Places references to files on the system clipboard — `CF_HDROP` on Windows,
//! file `NSURL`s on macOS, `text/uri-list` through `wl-copy`/`xclip` elsewhere —
//! so that pasting into a file manager, a chat client or a web upload form
//! attaches the original file instead of a browser re-encode of its pixels.
//!
//! Nothing here reads or copies file *contents*: only paths travel.
//!
//! The crate is thread-agnostic. `NSPasteboard` is main-thread hostile, so on
//! macOS the caller owns the dispatch (e.g. Tauri's
//! `AppHandle::run_on_main_thread`); this crate does not dispatch for you.
//!
//! Every error message is user-presentable: they surface verbatim in UI toasts
//! and in HTTP 500 bodies.

use std::path::Path;

use anyhow::bail;

#[cfg(windows)]
#[path = "windows.rs"]
mod imp;

#[cfg(target_os = "macos")]
#[path = "macos.rs"]
mod imp;

#[cfg(all(unix, not(target_os = "macos")))]
#[path = "linux.rs"]
mod imp;

#[cfg(not(any(windows, unix)))]
mod imp {
    pub(crate) fn copy_files(_paths: &[&std::path::Path]) -> anyhow::Result<()> {
        anyhow::bail!("{}", unsupported());
    }

    pub(crate) fn available() -> Result<(), String> {
        Err(unsupported())
    }

    fn unsupported() -> String {
        format!(
            "Copying files to the clipboard is not supported on {}.",
            std::env::consts::OS
        )
    }
}

#[cfg(any(all(unix, not(target_os = "macos")), test))]
mod payload;
#[cfg(any(all(unix, not(target_os = "macos")), test))]
mod uri;

/// Places OS-native file *references* (not contents) on the system clipboard.
///
/// Every path must be absolute and must exist on this machine; the whole call
/// fails if any of them does not, so a partially populated clipboard is never
/// left behind. That guarantee is about *this* write only: a failure after
/// validation may still leave the clipboard empty, because `CF_HDROP` and
/// `NSPasteboard` writes both clear the previous contents before placing the
/// new ones. Callers must not assume the prior clipboard survives an error.
pub fn copy_files_to_clipboard<P: AsRef<Path>>(paths: &[P]) -> anyhow::Result<()> {
    let paths: Vec<&Path> = paths.iter().map(AsRef::as_ref).collect();

    if paths.is_empty() {
        bail!("No files were given to copy to the clipboard.");
    }
    for path in &paths {
        if !path.is_absolute() {
            bail!(
                "Cannot copy '{}' to the clipboard: only absolute paths can be copied.",
                path.display()
            );
        }
        match path.try_exists() {
            Ok(true) => {}
            Ok(false) => bail!(
                "Cannot copy '{}' to the clipboard: the file does not exist on this machine.",
                path.display()
            ),
            Err(err) => bail!(
                "Cannot copy '{}' to the clipboard: it could not be read ({err}).",
                path.display()
            ),
        }
    }

    imp::copy_files(&paths)
}

/// Availability probe: `Ok(())`, or a human-readable reason why a clipboard
/// write would fail here (headless session, missing helper tool).
///
/// A successful probe is not a guarantee, and it never reads or writes clipboard
/// contents (on Windows it does briefly open and close the clipboard itself).
pub fn clipboard_available() -> Result<(), String> {
    imp::available()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_path_list() {
        let empty: [&Path; 0] = [];
        let err = copy_files_to_clipboard(&empty).unwrap_err().to_string();
        assert!(err.contains("No files"), "{err}");
    }

    #[test]
    fn rejects_relative_paths() {
        let err = copy_files_to_clipboard(&[Path::new("relative/file.png")])
            .unwrap_err()
            .to_string();
        assert!(err.contains("absolute"), "{err}");
    }

    #[test]
    fn rejects_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.bin");
        let err = copy_files_to_clipboard(&[&missing])
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"), "{err}");
    }

    /// Clobbers the real clipboard, so it never runs unattended:
    /// `cargo test -p panoptikon-clipboard -- --ignored`.
    #[test]
    #[ignore = "writes to the real system clipboard"]
    fn smoke_writes_a_real_file_reference() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("panoptikon clipboard smoke.txt");
        std::fs::write(&file, b"panoptikon").unwrap();

        if let Err(reason) = clipboard_available() {
            panic!("clipboard unavailable: {reason}");
        }
        copy_files_to_clipboard(&[&file]).unwrap();

        #[cfg(windows)]
        {
            let pasted: Vec<String> =
                clipboard_win::get_clipboard(clipboard_win::formats::FileList)
                    .expect("CF_HDROP should be readable back");
            assert_eq!(pasted, vec![file.to_str().unwrap().to_owned()]);
        }
    }
}
