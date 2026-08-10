//! Payload framing for the Linux/BSD external-tool path.
//!
//! `wl-copy` and `xclip` advertise exactly one MIME type per invocation
//! (`wl-copy --type` / `xclip -t` take a single value), and no single type is
//! universal: GNOME-family file managers (Nautilus, Nemo, Caja) paste files
//! only from `x-special/gnome-copied-files`, while browsers, chat clients and
//! every other file manager (Dolphin, Thunar, PCManFM) read `text/uri-list`.
//!
//! Panoptikon always publishes `text/uri-list`. Pasting a file into a chat
//! client, a browser upload form or a non-GNOME file manager is the point of
//! the feature; a GNOME-only flavour would make those silently paste nothing.

/// The single MIME type this crate advertises on Linux and the BSDs.
pub(crate) const URI_LIST_MIME: &str = "text/uri-list";

/// Frames already-encoded `file://` URIs as `text/uri-list`: every URI
/// terminated by CRLF, per RFC 2483.
pub(crate) fn uri_list_payload(uris: &[String]) -> String {
    let mut out = String::new();
    for uri in uris {
        out.push_str(uri);
        out.push_str("\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{URI_LIST_MIME, uri_list_payload};

    fn uris() -> Vec<String> {
        vec![
            "file:///home/u/a%20b.png".to_owned(),
            "file:///home/u/c.png".to_owned(),
        ]
    }

    #[test]
    fn payload_terminates_every_uri_with_crlf() {
        assert_eq!(
            uri_list_payload(&uris()),
            "file:///home/u/a%20b.png\r\nfile:///home/u/c.png\r\n"
        );
    }

    #[test]
    fn payload_of_one_file_ends_in_a_single_crlf() {
        assert_eq!(
            uri_list_payload(&["file:///tmp/x.bin".to_owned()]),
            "file:///tmp/x.bin\r\n"
        );
    }

    #[test]
    fn payload_of_nothing_is_empty() {
        assert_eq!(uri_list_payload(&[]), "");
    }

    #[test]
    fn the_advertised_mime_type_is_uri_list() {
        assert_eq!(URI_LIST_MIME, "text/uri-list");
    }
}
