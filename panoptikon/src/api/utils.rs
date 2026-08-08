use axum::http::header;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::api_error::ApiError;
use crate::db::system_config::SystemConfigStore;

/// Whether the request's index database may serve the outro playback
/// metadata (`content_end_ms`, `outro_kind`).
///
/// `detect_outros` off means the whole outro feature is off for that
/// database — including boundaries that were already detected while it was
/// on. Serving them as null is what turns the feature off for every client
/// at once, with no config plumbing in the player
/// (`docs/video-outro-skip-design.md` §6). Deliberately keyed on
/// `detect_outros` alone: the folded `scan_video && detect_outros` is a
/// scan-side concern.
///
/// `carries_metadata` says whether the response actually has one of the two
/// values in it. When it does not, the answer cannot change what is
/// serialized, so the config read is skipped — nulling an already-null field
/// is a no-op, making this exactly equivalent to an unconditional read while
/// keeping the filesystem off the hot metadata and search paths.
pub(crate) fn serve_outro_metadata(
    index_db: &str,
    carries_metadata: bool,
) -> Result<bool, ApiError> {
    if !carries_metadata {
        return Ok(true);
    }
    Ok(SystemConfigStore::from_env().load(index_db)?.detect_outros)
}

pub(crate) fn content_disposition_value(kind: &str, filename: &str) -> Option<header::HeaderValue> {
    let mut value = Vec::new();
    value.extend_from_slice(kind.as_bytes());
    value.extend_from_slice(b"; filename=\"");
    value.extend_from_slice(&latin1_bytes(filename));
    value.extend_from_slice(b"\"");
    header::HeaderValue::from_bytes(&value).ok()
}

pub(crate) fn strip_non_latin1_chars(input: &str) -> String {
    input.chars().filter(|ch| (*ch as u32) <= 0xFF).collect()
}

pub(crate) fn iso_to_system_time(value: &str) -> Option<SystemTime> {
    let trimmed = value.trim_end_matches('Z');
    let mut parts = trimmed.split('T');
    let date = parts.next()?;
    let time = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut date_parts = date.split('-');
    let year: i32 = date_parts.next()?.parse().ok()?;
    let month: u32 = date_parts.next()?.parse().ok()?;
    let day: u32 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour: u32 = time_parts.next()?.parse().ok()?;
    let minute: u32 = time_parts.next()?.parse().ok()?;
    let second: u32 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600)?
        .checked_add(i64::from(minute) * 60)?
        .checked_add(i64::from(second))?;

    if seconds < 0 {
        return None;
    }

    Some(UNIX_EPOCH + Duration::from_secs(seconds as u64))
}

fn latin1_bytes(value: &str) -> Vec<u8> {
    value
        .chars()
        .filter_map(|ch| {
            if (ch as u32) <= 0xFF {
                Some(ch as u8)
            } else {
                None
            }
        })
        .collect()
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut y = year;
    let m = month as i32;
    let d = day as i32;
    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146097 + doe - 719468) as i64)
}
