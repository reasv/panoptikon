//! Hardware H.264 encoder selection for the fast channel
//! (docs/video-transcoding-design.md §4).
//!
//! Listing is worthless on its own — drivers advertise encoders they cannot
//! run (the dev box lists `h264_amf` and `h264_qsv` with neither an AMD nor an
//! Intel GPU present), so the winner is confirmed with a real one-frame
//! encode. There is always a silent fallback to `libx264`.

use std::ffi::OsStr;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Probe candidates, in preference order. Vendor encoders first, then
/// Windows' vendor-neutral MediaFoundation path.
///
// VAAPI: deliberately absent. `h264_vaapi` is the one candidate that cannot be
// swapped into the command line by name — it needs `-vaapi_device` plus a
// `format=nv12,hwupload` filter-chain restructuring — so it is deferred until
// the encoder abstraction earns that shape. Linux Intel gets QSV; everything
// else falls back to `libx264 -preset veryfast`.
pub(crate) const CANDIDATES: [&str; 5] = [
    "h264_nvenc",
    "h264_amf",
    "h264_qsv",
    "h264_videotoolbox",
    "h264_mf",
];

/// Ceiling on the validation encode. A wedged driver blocks on the encode
/// session rather than failing, and this probe runs on the first fast-channel
/// job: without a deadline that job would never start.
const VALIDATE_TIMEOUT: Duration = Duration::from_secs(15);
const VALIDATE_POLL: Duration = Duration::from_millis(50);

/// `[transcode] hwaccel`, parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hwaccel {
    /// Probe the candidates and take the first that validates.
    Auto,
    /// Never use a hardware encoder.
    Off,
    /// Use exactly this encoder, if it validates.
    Named(&'static str),
}

/// Parses the config value. Both the bare vendor spelling (`nvenc`) and the
/// full encoder name (`h264_nvenc`) name the same candidate — the short form
/// is what users type, the long one is what `ffmpeg -encoders` prints.
/// `None` means the value names nothing this build can use, which is a config
/// error rather than a silent fallback.
pub(crate) fn parse_hwaccel(value: &str) -> Option<Hwaccel> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("auto") {
        return Some(Hwaccel::Auto);
    }
    if value.eq_ignore_ascii_case("off") {
        return Some(Hwaccel::Off);
    }
    CANDIDATES
        .iter()
        .find(|candidate| {
            value.eq_ignore_ascii_case(candidate)
                || candidate
                    .strip_prefix("h264_")
                    .is_some_and(|vendor| value.eq_ignore_ascii_case(vendor))
        })
        .map(|candidate| Hwaccel::Named(candidate))
}

/// The accepted `hwaccel` values, for config error messages.
pub(crate) fn hwaccel_values() -> String {
    let named = CANDIDATES.join("\", \"");
    format!("\"auto\", \"off\", \"{named}\"")
}

/// The hardware encoder the fast channel uses, or `None` for `libx264`.
/// Probed once per process: both the listing and the validation encode spawn
/// ffmpeg, and the answer cannot change while the process runs.
pub(crate) fn fast_h264_encoder() -> Option<&'static str> {
    static ENCODER: OnceLock<Option<&'static str>> = OnceLock::new();
    *ENCODER.get_or_init(|| {
        let configured = crate::config::runtime().transcode.hwaccel.clone();
        // Config load rejected anything else, so an unparseable value here can
        // only come from a RuntimeConfig built outside Settings::validate.
        let setting = parse_hwaccel(&configured).unwrap_or(Hwaccel::Auto);
        // `off` must answer without touching the toolchain: the listing below
        // spawns ffmpeg, which is the cost the setting exists to avoid.
        let chosen = if setting == Hwaccel::Off {
            None
        } else {
            select_encoder(setting, &listed_encoders(&encoder_listing()?), validate_encoder)
        };
        match chosen {
            Some(encoder) => tracing::info!(encoder, "hardware H.264 encoder validated"),
            None => tracing::info!(
                hwaccel = configured.as_str(),
                "no hardware H.264 encoder; the fast channel uses libx264"
            ),
        }
        chosen
    })
}

/// The selection policy, separated from the two ffmpeg spawns so it can be
/// tested without a toolchain.
fn select_encoder(
    setting: Hwaccel,
    listed: &[String],
    validate: impl Fn(&str) -> bool,
) -> Option<&'static str> {
    let wanted: &[&'static str] = match setting {
        Hwaccel::Off => return None,
        Hwaccel::Auto => &CANDIDATES,
        // A name outside the candidate list is unreachable through
        // `parse_hwaccel`; a value built any other way falls back to libx264.
        Hwaccel::Named(name) => {
            std::slice::from_ref(CANDIDATES.iter().find(|candidate| **candidate == name)?)
        }
    };
    wanted
        .iter()
        .copied()
        .find(|candidate| listed.iter().any(|name| name == candidate) && validate(candidate))
}

/// `ffmpeg -encoders` stdout, or `None` when the toolchain will not run.
fn encoder_listing() -> Option<String> {
    let output = Command::new(crate::media_tools::ffmpeg())
        .args(["-hide_banner", "-encoders"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|err| {
            tracing::debug!(error = %err, "ffmpeg -encoders did not run; assuming no hardware encoder");
            err
        })
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Encoder names out of `ffmpeg -encoders` output.
///
/// The listing is a legend followed by a `------` rule and then one row per
/// encoder: six capability flag characters, the name, and a description. Rows
/// are recognized by that flag column, so nothing in the legend (whose lines
/// are `X..... = ...`) or in a description can be mistaken for a name.
pub(crate) fn listed_encoders(listing: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut past_rule = false;
    for line in listing.lines() {
        let trimmed = line.trim();
        if !past_rule {
            past_rule = trimmed.starts_with("------");
            continue;
        }
        let mut fields = trimmed.split_whitespace();
        let (Some(flags), Some(name)) = (fields.next(), fields.next()) else {
            continue;
        };
        if flags.len() != 6 || !flags.chars().all(|flag| flag == '.' || flag.is_ascii_uppercase()) {
            continue;
        }
        names.push(name.to_string());
    }
    names
}

/// Encodes one black frame with `encoder`. This is the whole point of the
/// probe: a listed encoder whose driver is absent fails here, and nothing
/// short of a real encode session finds that out.
pub(crate) fn validate_encoder(encoder: &str) -> bool {
    let mut child = match Command::new(crate::media_tools::ffmpeg())
        .args([
            OsStr::new("-nostdin"),
            OsStr::new("-hide_banner"),
            OsStr::new("-nostats"),
            OsStr::new("-v"),
            OsStr::new("error"),
            OsStr::new("-f"),
            OsStr::new("lavfi"),
            OsStr::new("-i"),
            OsStr::new("color=c=black:s=128x128:d=0.04"),
            OsStr::new("-frames:v"),
            OsStr::new("1"),
            OsStr::new("-pix_fmt"),
            OsStr::new("yuv420p"),
            OsStr::new("-c:v"),
            OsStr::new(encoder),
            OsStr::new("-f"),
            OsStr::new("null"),
            OsStr::new("-"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            tracing::debug!(encoder, error = %err, "encoder probe failed to start");
            return false;
        }
    };

    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) => {}
            Err(err) => {
                tracing::debug!(encoder, error = %err, "encoder probe could not be reaped");
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
        if started.elapsed() >= VALIDATE_TIMEOUT {
            tracing::warn!(encoder, "encoder probe timed out; treating it as unusable");
            let _ = child.kill();
            let _ = child.wait();
            return false;
        }
        std::thread::sleep(VALIDATE_POLL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed but otherwise verbatim `ffmpeg -encoders` output.
    const LISTING: &str = "\
Encoders:
 V..... = Video
 A..... = Audio
 S..... = Subtitle
 .F.... = Frame-level multithreading
 ..S... = Slice-level multithreading
 ...X.. = Codec is experimental
 ....B. = Supports draw_horiz_band
 .....D = Supports direct rendering method 1
 ------
 V....D libx264              libx264 H.264 / AVC / MPEG-4 AVC (codec h264)
 V....D h264_amf             AMD AMF H.264 Encoder (codec h264)
 V..... h264_mf              H264 via MediaFoundation (codec h264)
 V....D h264_nvenc           NVIDIA NVENC H.264 encoder (codec h264)
 V..... h264_qsv             H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10 (codec h264)
 V....D libvpx-vp9           libvpx VP9 (codec vp9)
 V..... libwebp_anim         libwebp WebP image (codec webp)
 A....D aac                  AAC (Advanced Audio Coding)
 A..... libopus              libopus Opus (codec opus)
";

    #[test]
    fn encoder_listing_is_parsed_by_its_flag_column() {
        let names = listed_encoders(LISTING);
        assert_eq!(
            names,
            [
                "libx264",
                "h264_amf",
                "h264_mf",
                "h264_nvenc",
                "h264_qsv",
                "libvpx-vp9",
                "libwebp_anim",
                "aac",
                "libopus",
            ]
        );
        // The legend sits above the rule and must contribute nothing, or
        // "=" would be read as an encoder name.
        assert!(!names.iter().any(|name| name == "="));
        // Empty and rule-less input is not a parse error, just no encoders.
        assert!(listed_encoders("").is_empty());
        assert!(listed_encoders("Encoders:\n V..... = Video\n").is_empty());
    }

    /// Both spellings name the same candidate, the two keywords parse
    /// case-insensitively, and anything else is a config error.
    #[test]
    fn hwaccel_values_parse() {
        assert_eq!(parse_hwaccel("auto"), Some(Hwaccel::Auto));
        assert_eq!(parse_hwaccel(" AUTO "), Some(Hwaccel::Auto));
        assert_eq!(parse_hwaccel("off"), Some(Hwaccel::Off));
        assert_eq!(parse_hwaccel("nvenc"), Some(Hwaccel::Named("h264_nvenc")));
        assert_eq!(
            parse_hwaccel("h264_nvenc"),
            Some(Hwaccel::Named("h264_nvenc"))
        );
        assert_eq!(parse_hwaccel("qsv"), Some(Hwaccel::Named("h264_qsv")));
        assert_eq!(parse_hwaccel("h264_mf"), Some(Hwaccel::Named("h264_mf")));
        assert_eq!(parse_hwaccel("libx264"), None);
        assert_eq!(parse_hwaccel(""), None);
        assert!(hwaccel_values().contains("h264_videotoolbox"));
    }

    /// Selection: listing alone never wins, `off` refuses even a working
    /// encoder, an explicit name never falls through to another vendor's, and
    /// `auto` walks the candidates in preference order.
    #[test]
    fn selection_requires_both_listing_and_validation() {
        let listed: Vec<String> = ["libx264", "h264_amf", "h264_nvenc"]
            .iter()
            .map(|name| (*name).to_string())
            .collect();

        assert_eq!(
            select_encoder(Hwaccel::Auto, &listed, |_| true),
            Some("h264_nvenc"),
            "preference order, not listing order"
        );
        // The dev box's case: amf is listed but has no hardware behind it.
        assert_eq!(
            select_encoder(Hwaccel::Auto, &listed, |name| name == "h264_nvenc"),
            Some("h264_nvenc")
        );
        assert_eq!(select_encoder(Hwaccel::Auto, &listed, |_| false), None);
        assert_eq!(select_encoder(Hwaccel::Off, &listed, |_| true), None);
        assert_eq!(
            select_encoder(Hwaccel::Named("h264_amf"), &listed, |_| true),
            Some("h264_amf")
        );
        assert_eq!(
            select_encoder(Hwaccel::Named("h264_amf"), &listed, |name| name
                == "h264_nvenc"),
            None,
            "an explicit choice never silently becomes another encoder"
        );
        assert_eq!(
            select_encoder(Hwaccel::Named("h264_qsv"), &listed, |_| true),
            None,
            "an unlisted encoder is not tried"
        );
        assert_eq!(
            select_encoder(Hwaccel::Named("h264_vaapi"), &listed, |_| true),
            None,
            "a name outside the candidate list is libx264, not a panic"
        );
    }

    /// The probe against the real toolchain: libx264 is in every bundle the
    /// project ships, so it must validate. Skips (not fails) where there is no
    /// ffmpeg, per the media-test convention.
    #[test]
    fn libx264_validates_against_the_real_toolchain() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        assert!(validate_encoder("libx264"));
        assert!(
            !validate_encoder("definitely_not_an_encoder"),
            "a nonexistent encoder must fail rather than hang"
        );
        assert!(listed_encoders(&encoder_listing().expect("ffmpeg lists its encoders"))
            .iter()
            .any(|name| name == "libx264"));
    }

    /// The end-to-end probe: whatever this host has, the answer is either
    /// `None` (libx264) or one of the candidates, and it is stable across
    /// calls (the OnceLock).
    #[test]
    fn fast_encoder_probe_answers_with_a_candidate_or_nothing() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let chosen = fast_h264_encoder();
        if let Some(encoder) = chosen {
            assert!(CANDIDATES.contains(&encoder), "unexpected encoder {encoder}");
        }
        assert_eq!(chosen, fast_h264_encoder());
    }
}
