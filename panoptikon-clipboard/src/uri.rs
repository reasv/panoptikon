//! `file://` URI encoding for the `text/uri-list` clipboard flavour.

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encodes raw path bytes into a `file://` URI with an empty authority.
///
/// Only the RFC 3986 unreserved set and the path separator survive unescaped,
/// which keeps the result free of the spaces and newlines that would otherwise
/// break `text/uri-list` framing.
pub(crate) fn file_uri_from_bytes(path: &[u8]) -> String {
    let mut out = String::with_capacity(path.len() + 7);
    out.push_str("file://");
    for &byte in path {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn file_uri(path: &std::path::Path) -> String {
    use std::os::unix::ffi::OsStrExt;

    file_uri_from_bytes(path.as_os_str().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::file_uri_from_bytes;

    #[test]
    fn plain_ascii_path_is_untouched() {
        assert_eq!(
            file_uri_from_bytes(b"/home/u/pictures/cat.png"),
            "file:///home/u/pictures/cat.png"
        );
    }

    #[test]
    fn spaces_and_punctuation_are_escaped() {
        assert_eq!(
            file_uri_from_bytes(b"/home/u/my files/a b&c(1)[2].png"),
            "file:///home/u/my%20files/a%20b%26c%281%29%5B2%5D.png"
        );
    }

    #[test]
    fn backslashes_are_escaped_not_treated_as_separators() {
        assert_eq!(
            file_uri_from_bytes(br"/home/u/back\slash.txt"),
            "file:///home/u/back%5Cslash.txt"
        );
    }

    #[test]
    fn non_ascii_is_percent_encoded_per_utf8_byte() {
        assert_eq!(
            file_uri_from_bytes("/home/u/日本語 é.png".as_bytes()),
            "file:///home/u/%E6%97%A5%E6%9C%AC%E8%AA%9E%20%C3%A9.png"
        );
    }

    #[test]
    fn framing_and_percent_characters_cannot_leak_through() {
        assert_eq!(
            file_uri_from_bytes(b"/tmp/a\r\nb\t100%.bin"),
            "file:///tmp/a%0D%0Ab%09100%25.bin"
        );
    }

    #[test]
    fn invalid_utf8_bytes_survive_as_escapes() {
        assert_eq!(file_uri_from_bytes(b"/tmp/\xff\xfe"), "file:///tmp/%FF%FE");
    }
}
