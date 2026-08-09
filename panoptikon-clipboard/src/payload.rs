//! Clipboard flavour selection and payload framing for the Linux/BSD
//! external-tool path.
//!
//! `wl-copy` and `xclip` each offer exactly one MIME type per invocation, and
//! no single type is universally understood: GNOME-family file managers paste
//! files only from `x-special/gnome-copied-files`, while everything else
//! (Dolphin, Thunar, PCManFM, browsers, chat clients) reads `text/uri-list`.
//! The type is therefore picked from `XDG_CURRENT_DESKTOP`.

/// A clipboard flavour: one MIME type together with its payload framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flavour {
    /// `copy` verb followed by LF-separated URIs and *no* trailing newline.
    /// Nautilus, Nemo and Caja paste files only from this type.
    GnomeCopiedFiles,
    /// CRLF-terminated URIs, per RFC 2483.
    UriList,
}

impl Flavour {
    pub(crate) fn mime(self) -> &'static str {
        match self {
            Flavour::GnomeCopiedFiles => "x-special/gnome-copied-files",
            Flavour::UriList => "text/uri-list",
        }
    }

    /// Frames already-encoded `file://` URIs for this flavour.
    pub(crate) fn payload(self, uris: &[String]) -> String {
        match self {
            Flavour::GnomeCopiedFiles => {
                let mut out = String::from("copy");
                for uri in uris {
                    out.push('\n');
                    out.push_str(uri);
                }
                out
            }
            Flavour::UriList => {
                let mut out = String::new();
                for uri in uris {
                    out.push_str(uri);
                    out.push_str("\r\n");
                }
                out
            }
        }
    }
}

/// Desktop identifiers whose file managers only paste
/// `x-special/gnome-copied-files`. Matched case-insensitively as substrings,
/// so composite values like `ubuntu:GNOME` or `GNOME-Flashback:GNOME` hit.
const GNOME_FAMILY: &[&str] = &["gnome", "cinnamon", "mate", "x-cinnamon", "unity"];

/// Picks the flavour for a `XDG_CURRENT_DESKTOP` value; unset or unknown
/// desktops get `text/uri-list`, the broader of the two.
pub(crate) fn flavour_for_desktop(current_desktop: Option<&str>) -> Flavour {
    let Some(desktop) = current_desktop else {
        return Flavour::UriList;
    };
    let desktop = desktop.to_ascii_lowercase();
    if GNOME_FAMILY.iter().any(|name| desktop.contains(name)) {
        Flavour::GnomeCopiedFiles
    } else {
        Flavour::UriList
    }
}

#[cfg(test)]
mod tests {
    use super::{Flavour, flavour_for_desktop};

    fn uris() -> Vec<String> {
        vec![
            "file:///home/u/a%20b.png".to_owned(),
            "file:///home/u/c.png".to_owned(),
        ]
    }

    #[test]
    fn gnome_payload_is_copy_then_lf_separated_uris_without_trailing_newline() {
        assert_eq!(
            Flavour::GnomeCopiedFiles.payload(&uris()),
            "copy\nfile:///home/u/a%20b.png\nfile:///home/u/c.png"
        );
    }

    #[test]
    fn gnome_payload_of_one_file_has_a_single_separator() {
        assert_eq!(
            Flavour::GnomeCopiedFiles.payload(&["file:///tmp/x.bin".to_owned()]),
            "copy\nfile:///tmp/x.bin"
        );
    }

    #[test]
    fn uri_list_payload_terminates_every_uri_with_crlf() {
        assert_eq!(
            Flavour::UriList.payload(&uris()),
            "file:///home/u/a%20b.png\r\nfile:///home/u/c.png\r\n"
        );
    }

    #[test]
    fn mime_types_match_the_flavours() {
        assert_eq!(
            Flavour::GnomeCopiedFiles.mime(),
            "x-special/gnome-copied-files"
        );
        assert_eq!(Flavour::UriList.mime(), "text/uri-list");
    }

    #[test]
    fn gnome_family_desktops_select_gnome_copied_files() {
        for value in [
            "GNOME",
            "gnome",
            "ubuntu:GNOME",
            "GNOME-Flashback:GNOME",
            "X-Cinnamon",
            "Cinnamon",
            "MATE",
            "Unity:Unity7:ubuntu",
        ] {
            assert_eq!(
                flavour_for_desktop(Some(value)),
                Flavour::GnomeCopiedFiles,
                "{value}"
            );
        }
    }

    #[test]
    fn other_and_absent_desktops_select_uri_list() {
        for value in ["KDE", "XFCE", "LXQt", "sway", "Hyprland", ""] {
            assert_eq!(
                flavour_for_desktop(Some(value)),
                Flavour::UriList,
                "{value}"
            );
        }
        assert_eq!(flavour_for_desktop(None), Flavour::UriList);
    }
}
