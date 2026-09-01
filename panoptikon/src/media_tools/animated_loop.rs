//! The animated grid rendition: one H.264 loop per animated item
//! (docs/grid-scroll-performance-implementation.md §2, step B2).
//!
//! A GIF in a grid cell is the worst case the whole tier ladder exists for —
//! an uncompressed-ish palette animation decoded at full resolution, every
//! frame, in every visible cell. The rendition that replaces it is a plain
//! progressive mp4: libx264, yuv420p, CRF [`LOOP_CRF`], `+faststart`, source
//! frame timing and duration preserved, geometry from the *same*
//! [`crate::visual_tiers`] crop rule every still tier uses. Not AV1: hardware
//! decode sessions are finite at 20-30 streams and software AV1 is expensive,
//! while software H.264 is nearly free (settled, §2).
//!
//! Two ways in, and only one of them is ffmpeg's:
//!
//! * **GIF** goes straight to the demuxer.
//! * **Animated WebP** does not. ffmpeg's native webp decoder is still-image
//!   only through 8.0.1 — it answers an animated file with zero frames — so
//!   the file is decoded in Rust and substituted as an ffconcat script of PNG
//!   frames, exactly as the compose path does
//!   ([`crate::media_tools::transcode::webp_bridge`], reused verbatim rather
//!   than reimplemented). A toolchain that *can* decode it (a future ffmpeg,
//!   a user's `ffmpeg =` override) is preferred automatically and takes the
//!   direct path.
//!
//! The bridge's traps carry over and are honoured here: no input-side time
//! option ever rides on a concat input (`-ss` lands on entry boundaries or on
//! nothing at all), the script's TempDir outlives the ffmpeg run *including*
//! its `cache:` retry, and the concat input is never `cache:`-wrapped — which
//! [`crate::media_tools::cache_wrapped_args`] already knows.

use std::ffi::OsString;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use super::transcode::compose::{ItemTime, Transform, orientation_filters};
use crate::visual_tiers::TierRender;

/// x264's rate factor for a loop. Visually transparent for the material —
/// flat-colour animations and short clips — at a fraction of a GIF's bytes.
/// A change here needs a `TIER_PROCESS_VERSION` bump: the stored geometry
/// cannot see it.
const LOOP_CRF: &str = "18";

/// x264's speed/size tradeoff. `medium` is the default and is where the curve
/// flattens; a scan encodes one of these per animated item, so a slower
/// preset would buy single-digit percent for a multiple of the scan time.
const LOOP_PRESET: &str = "medium";

/// The whole animation, expressed in the bridge's window vocabulary: from
/// zero to a bound no real file reaches. The bridge's own frame budget is
/// what actually terminates a hostile input, and a span already inside its
/// window truncates rather than failing — which for a loop is the right
/// degradation (a shorter loop, never a wrong one).
const WHOLE_ANIMATION: ItemTime = ItemTime::Span {
    start_cs: 0,
    end_cs: i64::MAX,
};

/// Encodes one item's loop and returns the mp4 bytes.
///
/// `plan` is [`crate::visual_tiers::loop_render`]'s geometry in **display**
/// space, and `normalize` is the transform that takes the file's stored
/// pixels there (an EXIF-oriented WebP; GIF carries no orientation). The
/// scale filter is unconditional and exact, so the encoded stream always has
/// the dimensions the dispatcher predicted — an output that silently differed
/// would re-dispatch the item on every scan forever.
///
/// `Err` is a string for the log and a retry next scan: a loop is not a
/// thumbnail, so it has no negative-cache kind of its own, and a transient
/// ffmpeg failure must never freeze into a stored verdict.
pub(crate) fn encode_loop(
    path: &Path,
    mime_type: &str,
    plan: &TierRender,
    normalize: Transform,
) -> Result<Vec<u8>, String> {
    let dir = tempfile::tempdir()
        .map_err(|err| format!("could not create a loop directory: {err}"))?;
    let output = dir.path().join("loop.mp4");

    // Held across the ffmpeg run — including the `cache:` retry inside
    // `ffmpeg_output_with_input_retry` — because dropping it deletes the
    // frames the concat script names.
    let bridge = bridge_input(path, mime_type);
    let (input_args, input): (&[&str], &Path) = match &bridge {
        Some(bridge) => (&["-f", "concat"], bridge.script.as_path()),
        None => (&[], path),
    };

    let mut args: Vec<OsString> = Vec::new();
    for arg in ["-nostdin", "-hide_banner", "-nostats", "-v", "error"] {
        args.push(arg.into());
    }
    for arg in input_args {
        args.push((*arg).into());
    }
    args.push("-i".into());
    args.push(input.into());
    // No audio, subtitle or data stream can survive into a grid cell's loop,
    // and an animated container has none to begin with.
    for arg in ["-an", "-sn", "-dn"] {
        args.push(arg.into());
    }
    args.push("-vf".into());
    args.push(loop_filters(plan, normalize).into());
    for arg in [
        "-c:v",
        "libx264",
        "-preset",
        LOOP_PRESET,
        "-crf",
        LOOP_CRF,
        "-pix_fmt",
        "yuv420p",
        // Source timing, verbatim: the decoded frames' own timestamps become
        // the stream's, so a GIF's variable per-frame delays and its total
        // duration both survive. Constant-rate output would resample an
        // animation whose demuxed rate is a least common multiple of its
        // delays into a far longer frame list for the same seconds.
        "-fps_mode",
        "passthrough",
        // The one thing a <video> in a grid cell cannot do without: the moov
        // atom at the front, so playback starts on the first range.
        "-movflags",
        "+faststart",
        "-f",
        "mp4",
        "-y",
    ] {
        args.push(arg.into());
    }
    args.push(output.clone().into());

    let result = crate::media_tools::ffmpeg_output_with_input_retry(&args, |command| {
        command.stdout(std::process::Stdio::null());
    });
    // Only now are the bridged frames done with: the retry inside the helper
    // reads them a second time.
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(err) => return Err(format!("ffmpeg did not start: {err}")),
    };
    drop(bridge);
    if !outcome.status.success() {
        return Err(format!(
            "ffmpeg failed: {}",
            crate::jobs::files::stderr_tail(&outcome.stderr)
        ));
    }
    let bytes =
        std::fs::read(&output).map_err(|err| format!("the encoded loop did not read back: {err}"))?;
    if bytes.is_empty() {
        return Err("ffmpeg produced an empty loop".to_string());
    }
    Ok(bytes)
}

/// The filter chain, as a pure function so the geometry has a test that
/// spawns no toolchain.
///
/// Order is not negotiable: the source is normalized into **display** space
/// first, because that is the space the item's indexed dimensions — and
/// therefore `plan`'s crop rectangle — are expressed in. Cropping stored
/// pixels with a display-space rectangle would cut the wrong band out of
/// every rotated file.
///
/// Both halves are always emitted, even where either is the identity, and
/// that is deliberate: together they are the *only* thing guaranteeing the
/// encoded stream has the geometry the dispatcher predicted, and a stream
/// that silently differed would re-dispatch the item on every scan forever.
/// A whole-frame `crop` is a slice adjustment ffmpeg does not pay for, and
/// omitting it conditionally would need the source dimensions here to know
/// which case it is — a second source of truth for the one number that must
/// not have one.
fn loop_filters(plan: &TierRender, normalize: Transform) -> String {
    let mut filters: Vec<String> = orientation_filters(normalize)
        .into_iter()
        .map(str::to_string)
        .collect();
    filters.push(format!(
        "crop={}:{}:{}:{}",
        plan.crop_width, plan.crop_height, plan.crop_x, plan.crop_y
    ));
    // `accurate_rnd` and `full_chroma_int` are the two swscale flags that
    // matter on the material this sees: flat colour and hard edges, where
    // 4:2:0's chroma downsample is what produces visible fringing. They cost
    // a few percent of a scale that is already cheap.
    filters.push(format!(
        "scale={}:{}:flags=lanczos+accurate_rnd+full_chroma_int",
        plan.width, plan.height
    ));
    filters.join(",")
}

/// The bridged input for an animated WebP, or `None` for everything that goes
/// straight to ffmpeg.
///
/// Cheapest check first, exactly as the compose path orders them: the mime
/// gate, then a 12-byte magic read, then the native-decode bypass, then the
/// whole-file structure sniff. An extraction failure is *not* fatal — the
/// original file is passed through and ffmpeg fails fast on it, which is the
/// pre-bridge behaviour and an error rather than an invented success.
fn bridge_input(
    path: &Path,
    mime_type: &str,
) -> Option<super::transcode::webp_bridge::BridgedInput> {
    if !mime_type.starts_with("image/webp") {
        return None;
    }
    if !super::transcode::webp_bridge::has_webp_magic(path)
        || super::transcode::hw::animated_webp_decodable()
    {
        return None;
    }
    let bytes = super::transcode::webp_bridge::sniff_animated_webp(path)?;
    match super::transcode::webp_bridge::extract(&bytes, WHOLE_ANIMATION, &AtomicBool::new(false)) {
        Ok(bridge) => Some(bridge),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error,
                "could not bridge an animated WebP for its loop; letting ffmpeg try the file"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visual_tiers::loop_render;

    /// A normal-aspect source: the crop is the whole frame, and the scale
    /// lands on exactly the geometry the dispatcher predicted.
    #[test]
    fn an_ordinary_loop_keeps_the_whole_frame() {
        let plan = loop_render(1500, 2000);
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=1500:2000:0:0,scale=1024:1364:flags=lanczos+accurate_rnd+full_chroma_int"
        );
    }

    /// A strip crops first and scales second, and the crop is the top band
    /// `object-position: 50% 0%` paints.
    #[test]
    fn a_strip_crops_before_it_scales() {
        let plan = loop_render(800, 20000);
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=800:2048:0:0,scale=800:2048:flags=lanczos+accurate_rnd+full_chroma_int"
        );

        // A wide strip keeps the horizontally centered band.
        let plan = loop_render(20000, 800);
        assert!(
            loop_filters(&plan, Transform::default())
                .starts_with(&format!("crop=2048:800:{}:0,", (20000 - 2048) / 2)),
            "{}",
            loop_filters(&plan, Transform::default())
        );
    }

    /// The source is normalized into display space BEFORE the crop, because
    /// the crop rectangle is expressed in display space.
    #[test]
    fn orientation_is_applied_before_the_crop() {
        let plan = loop_render(800, 20000);
        let chain = loop_filters(
            &plan,
            Transform {
                quarter_turns: 1,
                flip_h: false,
            },
        );
        assert_eq!(
            chain,
            "transpose=1,crop=800:2048:0:0,\
             scale=800:2048:flags=lanczos+accurate_rnd+full_chroma_int"
        );
        let rotate = chain.find("transpose=1").expect("the source is normalized");
        let crop = chain.find("crop=").expect("the strip is cropped");
        assert!(rotate < crop);
    }

    /// Both halves are unconditional: together they are what guarantees the
    /// encoded stream has the dimensions the backfill dispatcher predicted,
    /// even where the render is the identity in both.
    #[test]
    fn the_exact_geometry_is_never_omitted() {
        let plan = loop_render(300, 300);
        assert_eq!((plan.width, plan.height), (300, 300));
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=300:300:0:0,scale=300:300:flags=lanczos+accurate_rnd+full_chroma_int"
        );
    }

    /// The whole-animation window is a span from zero that no real file
    /// reaches the end of, so the bridge writes every frame it decodes.
    #[test]
    fn the_bridge_window_is_the_whole_animation() {
        assert_eq!(
            WHOLE_ANIMATION,
            ItemTime::Span {
                start_cs: 0,
                end_cs: i64::MAX
            }
        );
    }
}
