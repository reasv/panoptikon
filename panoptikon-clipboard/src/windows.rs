//! Windows: `CF_HDROP` file references through `clipboard-win`.

use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, bail};
use clipboard_win::Clipboard;
use clipboard_win::options::DoClear;
use clipboard_win::raw::set_file_list_with;

/// The clipboard is a single global lock; another process holding it while it
/// paints a paste menu is routine, so retry before giving up.
const OPEN_ATTEMPTS: u32 = 10;
const OPEN_RETRY_DELAY: Duration = Duration::from_millis(30);
/// `Clipboard::new_attempts(n)` makes one attempt plus `n` retries, so every
/// outer attempt is really six calls to `OpenClipboard`.
const OPEN_INNER_RETRIES: usize = 5;

pub(crate) fn copy_files(paths: &[&Path]) -> anyhow::Result<()> {
    let mut list = Vec::with_capacity(paths.len());
    for path in paths {
        match path.to_str() {
            Some(text) => list.push(text),
            None => bail!(
                "Cannot copy '{}' to the clipboard: the path is not valid Unicode.",
                path.display()
            ),
        }
    }

    let _clipboard = open()?;
    set_file_list_with(&list, DoClear).map_err(|err| {
        anyhow!("Windows rejected the clipboard write ({err}). Please try again.")
    })?;
    Ok(())
}

pub(crate) fn available() -> Result<(), String> {
    open().map(drop).map_err(|err| err.to_string())
}

fn open() -> anyhow::Result<Clipboard> {
    let mut last = None;
    for attempt in 1..=OPEN_ATTEMPTS {
        match Clipboard::new_attempts(OPEN_INNER_RETRIES) {
            Ok(clipboard) => return Ok(clipboard),
            Err(err) => {
                last = Some(err);
                if attempt < OPEN_ATTEMPTS {
                    sleep(OPEN_RETRY_DELAY);
                }
            }
        }
    }

    let total = OPEN_ATTEMPTS * (OPEN_INNER_RETRIES as u32 + 1);
    // `ErrorCode` renders whatever `GetLastError()` last returned, which is
    // "The operation completed successfully." when the failure left it at 0 —
    // worse than saying nothing, so drop the detail in that case.
    let detail = last
        .filter(|err| err.raw_code() != 0)
        .map(|err| format!(" ({err})"))
        .unwrap_or_default();
    bail!(
        "Could not open the Windows clipboard after {total} attempts{detail}; \
         another application may be holding it open. Please try again."
    )
}
