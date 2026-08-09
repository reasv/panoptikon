//! macOS: file `NSURL`s written to the general `NSPasteboard`.
//!
//! `NSPasteboard` is main-thread hostile; dispatching onto the main thread is
//! the caller's job (see the crate docs).

use std::path::Path;

use anyhow::bail;
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
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

    let pasteboard = NSPasteboard::generalPasteboard();
    pasteboard.clearContents();
    if !pasteboard.writeObjects(&NSArray::from_retained_slice(&urls)) {
        bail!("The macOS pasteboard rejected the file reference.");
    }
    Ok(())
}

pub(crate) fn available() -> Result<(), String> {
    Ok(())
}
