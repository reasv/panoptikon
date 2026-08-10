//! macOS: file `NSURL`s written to the general `NSPasteboard`.
//!
//! `NSPasteboard` is main-thread hostile; dispatching onto the main thread is
//! the caller's job (see the crate docs). Because that dispatch runs outside
//! the caller's unwind guard, nothing here may panic — including the AppKit
//! call that fetches the pasteboard.

use std::path::Path;

use anyhow::{anyhow, bail};
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2::{ClassType, msg_send};
use objc2_app_kit::{NSPasteboard, NSPasteboardWriting};
use objc2_foundation::{NSArray, NSString, NSURL};

pub(crate) fn copy_files(paths: &[&Path]) -> anyhow::Result<()> {
    let mut urls: Vec<Retained<ProtocolObject<dyn NSPasteboardWriting>>> =
        Vec::with_capacity(paths.len());
    for path in paths {
        let Some(text) = path.to_str() else {
            bail!(
                "Cannot copy '{}' to the clipboard: the path is not valid UTF-8.",
                path.display()
            );
        };
        // Passing the directory flag skips the `stat` that
        // `fileURLWithPath:` would do internally; we already have the
        // metadata. A stale answer (path replaced between the two) only
        // affects the URL's trailing slash, so a race is harmless.
        let is_directory = std::fs::metadata(path)
            .map(|meta| meta.is_dir())
            .unwrap_or(false);
        let url = NSURL::fileURLWithPath_isDirectory(&NSString::from_str(text), is_directory);
        urls.push(ProtocolObject::from_retained(url));
    }

    let pasteboard = general_pasteboard()?;
    pasteboard.clearContents();
    if !pasteboard.writeObjects(&NSArray::from_retained_slice(&urls)) {
        bail!("The macOS pasteboard rejected the file reference.");
    }
    Ok(())
}

pub(crate) fn available() -> Result<(), String> {
    general_pasteboard()
        .map(drop)
        .map_err(|err| err.to_string())
}

/// The general pasteboard, or an error when AppKit has none to give.
///
/// The generated `NSPasteboard::generalPasteboard()` binding is typed as
/// returning a non-optional `Retained`, which objc2 produces by unwrapping the
/// message result — it *panics* on the NULL a session-less process gets back.
/// Sending the message with an optional return type is the only way to see
/// that as a value.
fn general_pasteboard() -> anyhow::Result<Retained<NSPasteboard>> {
    // SAFETY: `generalPasteboard` is a nullary class method of `NSPasteboard`
    // returning `NSPasteboard *`, and belongs to no selector family, so
    // `Option<Retained<NSPasteboard>>` is its correct Rust signature.
    let pasteboard: Option<Retained<NSPasteboard>> =
        unsafe { msg_send![NSPasteboard::class(), generalPasteboard] };

    pasteboard.ok_or_else(|| {
        anyhow!("No macOS window session is available, so there is no clipboard to copy to.")
    })
}
