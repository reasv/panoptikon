//! Byte-serving machinery shared by every endpoint that hands a file to a
//! browser: Range, If-Range, conditional GET, and the response assembly.
//!
//! What is deliberately *not* here is validator policy. Which ETag a resource
//! has, and whether it may claim immutability, is knowledge only the endpoint
//! owning the resource has (the item file weighs the recorded mtime against
//! the one on disk; a transcode artifact is content-addressed on both halves
//! of its key and needs no such caveat). Callers decide that, then hand over a
//! [`ServeSpec`] and the bytes to go with it.
//!
//! Where the bytes come from is the *only* difference between serving a file
//! and serving a stored rendition, so it is the only thing [`ServeBody`]
//! carries. Range, `If-Range`, `If-None-Match`, `If-Modified-Since` and the
//! response assembly are one implementation for both: a database-backed mp4
//! that answered `Range` differently from a file-backed one would be a
//! `<video>` element that seeks in one case and not the other.

use axum::{
    body::Body,
    http::{HeaderMap, Response, StatusCode, header},
};
use std::io::SeekFrom;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::api::utils::content_disposition_value;
use crate::api_error::ApiError;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Ceiling on opening/statting a file before giving up on it. Indexed files
/// can live on network shares; a hung mount must not stall requests (or, with
/// no timeout, tokio's blocking pool) indefinitely.
pub(crate) const FILE_IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RangeOutcome {
    /// No usable Range header: serve the whole file with 200.
    Full,
    /// A single satisfiable byte range (inclusive): serve 206.
    Partial { start: u64, end: u64 },
    /// Range header was valid but no requested range overlaps the file: 416.
    Unsatisfiable,
}

/// Everything [`serve`] needs once the caller has decided the resource's
/// identity: the validators and headers that describe it.
pub(crate) struct ServeSpec {
    pub(crate) mime_type: String,
    pub(crate) etag: String,
    pub(crate) cache_control: &'static str,
    /// `None` where the resource has no such date — a stored rendition is
    /// derived from content, not from a file, so its ETag is its only
    /// validator and its only `If-Range` key.
    pub(crate) last_modified: Option<String>,
    pub(crate) content_disposition_type: &'static str,
    pub(crate) filename: String,
}

/// Where the bytes of a response come from.
pub(crate) enum ServeBody {
    /// An open handle, streamed. `size` is authoritative for range math: the
    /// caller stats the handle rather than trusting any recorded size.
    File { file: tokio::fs::File, size: u64 },
    /// Bytes already in memory — a stored rendition, a placeholder.
    Bytes(Vec<u8>),
}

impl ServeBody {
    fn size(&self) -> u64 {
        match self {
            Self::File { size, .. } => *size,
            Self::Bytes(bytes) => bytes.len() as u64,
        }
    }
}

pub(crate) fn if_none_match_matches(header_value: &str, etag: &str) -> bool {
    header_value.trim() == "*"
        || header_value
            .split(',')
            .map(|candidate| candidate.trim().trim_start_matches("W/"))
            .any(|candidate| candidate == etag)
}

pub(crate) fn not_modified_response(
    etag: &str,
    cache_control: &str,
    last_modified: Option<&str>,
) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_MODIFIED;
    let headers = response.headers_mut();
    if let Ok(value) = header::HeaderValue::from_str(etag) {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    if let Some(last_modified) = last_modified
        && let Ok(value) = header::HeaderValue::from_str(last_modified)
    {
        headers.insert(header::LAST_MODIFIED, value);
    }
    response
}

/// Opens a file for serving, bounded by [`FILE_IO_TIMEOUT`]. `not_found` is
/// the caller's 404 detail: a file that is not there means something
/// endpoint-specific (a stale file row, an evicted artifact), while a timeout
/// is always this machine's problem and stays a 500.
pub(crate) async fn open_file_with_timeout(
    path: &str,
    not_found: &'static str,
) -> ApiResult<tokio::fs::File> {
    match tokio::time::timeout(FILE_IO_TIMEOUT, tokio::fs::File::open(path)).await {
        Ok(Ok(file)) => Ok(file),
        Ok(Err(err)) => {
            tracing::error!(error = %err, "failed to open file");
            Err(ApiError::not_found(not_found))
        }
        Err(_) => {
            tracing::error!(path = %path, "timed out opening file");
            Err(ApiError::internal("Timed out opening file"))
        }
    }
}

/// Parses a `Range` request header against a resource of `size` bytes.
///
/// Handles all RFC 9110 byte-range forms: `start-end`, `start-`, and the
/// suffix form `-N`. Out-of-bounds ends are clamped. Multiple ranges are
/// accepted syntactically, but if more than one is satisfiable the header is
/// ignored (full 200) rather than answered with multipart/byteranges, which
/// RFC 9110 permits. Malformed headers are ignored entirely.
pub(crate) fn parse_range_header(value: &str, size: u64) -> RangeOutcome {
    let trimmed = value.trim();
    let Some(specs) = trimmed
        .get(..6)
        .filter(|prefix| prefix.eq_ignore_ascii_case("bytes="))
        .map(|_| &trimmed[6..])
    else {
        return RangeOutcome::Full;
    };

    let mut satisfiable = Vec::new();
    let mut any_valid = false;
    for spec in specs.split(',') {
        let spec = spec.trim();
        if spec.is_empty() {
            continue;
        }
        let Some((start_str, end_str)) = spec.split_once('-') else {
            return RangeOutcome::Full;
        };
        let start_str = start_str.trim();
        let end_str = end_str.trim();
        if start_str.is_empty() {
            // Suffix form: last N bytes.
            let Ok(suffix) = end_str.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            any_valid = true;
            if suffix == 0 || size == 0 {
                continue;
            }
            satisfiable.push((size.saturating_sub(suffix), size - 1));
        } else {
            let Ok(start) = start_str.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            let end = if end_str.is_empty() {
                size.checked_sub(1)
            } else {
                let Ok(end) = end_str.parse::<u64>() else {
                    return RangeOutcome::Full;
                };
                if end < start {
                    return RangeOutcome::Full;
                }
                size.checked_sub(1).map(|last| end.min(last))
            };
            any_valid = true;
            match end {
                Some(end) if start <= end => satisfiable.push((start, end)),
                _ => {}
            }
        }
    }

    match satisfiable.as_slice() {
        [(start, end)] => RangeOutcome::Partial {
            start: *start,
            end: *end,
        },
        [] if any_valid => RangeOutcome::Unsatisfiable,
        _ => RangeOutcome::Full,
    }
}

/// Serves an open file: conditional GET first, then the range decision, then
/// the body and headers.
pub(crate) async fn serve(
    spec: ServeSpec,
    body: ServeBody,
    request_headers: &HeaderMap,
) -> ApiResult<Response<Body>> {
    let ServeSpec {
        mime_type,
        etag,
        cache_control,
        last_modified,
        content_disposition_type,
        filename,
    } = spec;
    let size = body.size();

    // Conditional GET: If-None-Match wins over If-Modified-Since (RFC 9110).
    if let Some(if_none_match) = request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        if if_none_match_matches(if_none_match, &etag) {
            return Ok(not_modified_response(
                &etag,
                cache_control,
                last_modified.as_deref(),
            ));
        }
    } else if let Some(if_modified_since) = request_headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|value| value.to_str().ok())
        && last_modified.as_deref() == Some(if_modified_since.trim())
    {
        return Ok(not_modified_response(
            &etag,
            cache_control,
            last_modified.as_deref(),
        ));
    }

    let mut range = request_headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .map(|value| parse_range_header(value, size))
        .unwrap_or(RangeOutcome::Full);

    // If-Range: only honor the range when the validator still matches;
    // otherwise the client's partial state is stale and it needs the full
    // body. The validator may be either our ETag or the Last-Modified date.
    if range != RangeOutcome::Full
        && let Some(if_range) = request_headers
            .get(header::IF_RANGE)
            .and_then(|value| value.to_str().ok())
    {
        let if_range = if_range.trim();
        let matches = if if_range.starts_with('"') || if_range.starts_with("W/") {
            if_range == etag
        } else {
            last_modified.as_deref() == Some(if_range)
        };
        if !matches {
            range = RangeOutcome::Full;
        }
    }

    let (status, body, content_length, content_range) = match (range, body) {
        (RangeOutcome::Full, ServeBody::File { file, .. }) => (
            StatusCode::OK,
            Body::from_stream(ReaderStream::new(file)),
            size,
            None,
        ),
        (RangeOutcome::Full, ServeBody::Bytes(bytes)) => {
            (StatusCode::OK, Body::from(bytes), size, None)
        }
        (RangeOutcome::Partial { start, end }, ServeBody::File { mut file, .. }) => {
            file.seek(SeekFrom::Start(start)).await.map_err(|err| {
                tracing::error!(error = %err, "failed to seek file");
                ApiError::internal("Failed to read file")
            })?;
            let length = end - start + 1;
            (
                StatusCode::PARTIAL_CONTENT,
                Body::from_stream(ReaderStream::new(file.take(length))),
                length,
                Some(format!("bytes {start}-{end}/{size}")),
            )
        }
        (RangeOutcome::Partial { start, end }, ServeBody::Bytes(bytes)) => {
            let slice = bytes[start as usize..=end as usize].to_vec();
            (
                StatusCode::PARTIAL_CONTENT,
                Body::from(slice),
                end - start + 1,
                Some(format!("bytes {start}-{end}/{size}")),
            )
        }
        (RangeOutcome::Unsatisfiable, _) => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            Body::empty(),
            0,
            Some(format!("bytes */{size}")),
        ),
    };

    let mut response = Response::new(body);
    *response.status_mut() = status;
    let headers = response.headers_mut();

    headers.insert(
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    );
    if let Ok(value) = header::HeaderValue::from_str(&mime_type) {
        headers.insert(header::CONTENT_TYPE, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&content_length.to_string()) {
        headers.insert(header::CONTENT_LENGTH, value);
    }
    if let Some(content_range) = content_range
        && let Ok(value) = header::HeaderValue::from_str(&content_range)
    {
        headers.insert(header::CONTENT_RANGE, value);
    }
    if let Some(last_modified) = &last_modified
        && let Ok(value) = header::HeaderValue::from_str(last_modified)
    {
        headers.insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(&etag) {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = header::HeaderValue::from_str(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }

    if let Some(value) = content_disposition_value(content_disposition_type, &filename) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_none_match_matching() {
        assert!(if_none_match_matches("\"a\"", "\"a\""));
        assert!(if_none_match_matches("\"x\", \"a\"", "\"a\""));
        assert!(if_none_match_matches("W/\"a\"", "\"a\""));
        assert!(if_none_match_matches("*", "\"a\""));
        assert!(!if_none_match_matches("\"b\"", "\"a\""));
    }

    #[test]
    fn parse_range_header_cases() {
        use RangeOutcome::*;

        // Basic forms.
        assert_eq!(
            parse_range_header("bytes=0-499", 1000),
            Partial { start: 0, end: 499 }
        );
        assert_eq!(
            parse_range_header("bytes=500-", 1000),
            Partial {
                start: 500,
                end: 999
            }
        );
        assert_eq!(
            parse_range_header("bytes=-300", 1000),
            Partial {
                start: 700,
                end: 999
            }
        );
        // End clamped to the last byte; suffix longer than the file covers it all.
        assert_eq!(
            parse_range_header("bytes=990-2000", 1000),
            Partial {
                start: 990,
                end: 999
            }
        );
        assert_eq!(
            parse_range_header("bytes=-5000", 1000),
            Partial { start: 0, end: 999 }
        );
        // Whitespace and case tolerance.
        assert_eq!(
            parse_range_header(" BYTES= 0 - 4 ", 1000),
            Partial { start: 0, end: 4 }
        );
        // Unsatisfiable: beyond EOF, zero suffix, empty file.
        assert_eq!(parse_range_header("bytes=1000-", 1000), Unsatisfiable);
        assert_eq!(parse_range_header("bytes=-0", 1000), Unsatisfiable);
        assert_eq!(parse_range_header("bytes=0-", 0), Unsatisfiable);
        // Ignored: other units, malformed specs, inverted ranges,
        // multiple satisfiable ranges (no multipart support).
        assert_eq!(parse_range_header("items=0-4", 1000), Full);
        assert_eq!(parse_range_header("bytes=abc", 1000), Full);
        assert_eq!(parse_range_header("bytes=5-2", 1000), Full);
        assert_eq!(parse_range_header("bytes=0-4,10-14", 1000), Full);
        // One satisfiable range among unsatisfiable ones is still served.
        assert_eq!(
            parse_range_header("bytes=2000-,0-4", 1000),
            Partial { start: 0, end: 4 }
        );
    }
}
