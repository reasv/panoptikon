//! The animated-WebP compose bridge (docs/animated-webp-bridge-design.md):
//! Rust-side frame extraction for the one animated container no mainline
//! ffmpeg decodes.
//!
//! ffmpeg's native webp decoder is still-image-only through 8.0.1 — it
//! answers an animated file with "image data not found" and produces **zero**
//! frames, which fails the whole composition graph (even the frozen `Image`
//! chain needs one frame to freeze). The `image` crate decodes the format
//! completely, so at compose *execution* time (never admission, which stays
//! probe-free arithmetic) such an input is bridged: frames decoded one at a
//! time, written as lossless RGBA PNGs into a job-scoped temp directory
//! alongside an `ffconcat` script carrying each frame's duration, and the
//! script substituted as the ffmpeg input (`-f concat`). PNG decode plus the
//! concat demuxer exist in every ffmpeg, which is why
//! [`super::hw::span_capable_image_mimes`] lists `image/webp`
//! unconditionally.
//!
//! Hostile input is assumed. The scanner's structure walk
//! ([`crate::media_tools::animation`]) bounds the *sniff*; the decode has its
//! own budget: only the frames the item shows, a hard [`FRAME_BUDGET`] cap on
//! top, zero frame durations floored to 1 ms so accounting always progresses,
//! and cancellation checked between frames. Every name near the script is
//! generated (`frame-%05d.png`, [`SCRIPT_NAME`]) — no user-controlled string
//! reaches it, so the concat demuxer's default "safe" mode is satisfied and
//! there are no quoting hazards.

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use image::AnimationDecoder;

use super::compose::{ItemTime, StreamInfo, orientation_transform};

/// Hard ceiling on decoded frames (≈ 60 s × 60 fps), whatever the item's
/// bounds claim: the bounds are client-supplied and the decode cost is paid
/// per frame. A span already inside its window truncates like the
/// documented under-run — `overlay eof_action=repeat` holds the last
/// written frame, output length unaffected — while a budget that runs out
/// before the item's timestamp is even reached is an extraction *failure*
/// (fall back, ffmpeg fails fast), never a nearby-wrong frame.
const FRAME_BUDGET: usize = 3600;

/// The script's fixed name inside its directory. Generated, like the frame
/// names, and referenced by relative paths only: the concat demuxer resolves
/// entries against the script's own location, so the ffmpeg process's working
/// directory never matters.
const SCRIPT_NAME: &str = "frames.ffconcat";

/// One bridged input: the substitute script and the probe it makes redundant.
pub(crate) struct BridgedInput {
    /// Owns the frames and the script. Held (never read) so the files
    /// survive until the ffmpeg run — **including its `cache:` retry** — has
    /// finished; dropping it is what deletes them.
    _dir: tempfile::TempDir,
    /// The ffconcat script, which stands where the source path stood.
    pub(crate) script: PathBuf,
    /// Synthesized from the decode itself, in place of `probe_source`:
    /// canvas geometry from the header, no audio (WebP carries none), and
    /// the extracted total as the duration. Strictly more reliable than
    /// ffprobe on a concat script of images.
    pub(crate) info: StreamInfo,
}

/// Whether the file even *begins* as a WebP, from its first 12 bytes: the
/// cheap gate that keeps the native-decode probe and the whole-file sniff
/// off every source that is something else entirely (a compose source is
/// usually a video, and reading gigabytes to learn it is not RIFF would be
/// absurd).
pub(crate) fn has_webp_magic(path: &Path) -> bool {
    let mut magic = [0u8; 12];
    if std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .is_err()
    {
        return false;
    }
    &magic[..4] == b"RIFF" && &magic[8..12] == b"WEBP"
}

/// The file's bytes when — and only when — it is an animated WebP.
///
/// A content sniff, not an index lookup, so it cannot drift from the file:
/// the magic gate first, then the same structure walk the scanner measures
/// durations with. `None` covers every way of not being bridgeable —
/// another container, a still WebP, an unreadable file — all of which
/// simply pass through to ffmpeg untouched.
pub(crate) fn sniff_animated_webp(path: &Path) -> Option<Vec<u8>> {
    if !has_webp_magic(path) {
        return None;
    }
    let bytes = std::fs::read(path).ok()?;
    (crate::media_tools::animation::webp_animation_seconds(&bytes) > 0.0).then_some(bytes)
}

/// Decodes the frames `time` shows into a fresh temp directory and writes
/// the script that plays them.
///
/// Streaming, one frame in memory at a time: decode, write the PNG, drop.
/// `Err` is the extraction-failure contract (design §2): the caller logs it
/// and passes the ORIGINAL file through unbridged, so ffmpeg fails fast —
/// the pre-bridge behavior, an error, never a hang and never a silently
/// dropped item. Cancellation surfaces as an `Err` too; the run refuses to
/// start afterwards either way.
///
/// The item's timestamps are honoured HERE, never by an input seek: `-ss`
/// (and `-to`) on a concat script of image entries land on entry boundaries
/// — the seek snaps to the *next* entry, or to nothing at all past the last
/// one, which fails the whole graph (determined empirically on both 7.1 and
/// 8.0.1). So a span's script holds exactly the window `[start_cs, end_cs)`
/// with the first and last entries' durations trimmed to it, a still's
/// script holds only its covering frame, and the bridged input carries no
/// time options at all. The frames *before* a window are still decoded — a
/// WebP frame may be a delta over the previous canvas, so the compositor
/// cannot skip them — just never written.
pub(crate) fn extract(
    bytes: &[u8],
    time: ItemTime,
    cancel: &AtomicBool,
) -> Result<BridgedInput, String> {
    extract_with_budget(bytes, time, cancel, FRAME_BUDGET, u64::MAX)
}

/// [`extract`] with an aggregate ceiling on what the written frames occupy on
/// disk, for callers that extract a *whole* animation rather than a
/// composition's bounded window.
///
/// [`FRAME_BUDGET`] bounds the wrong axis on its own: 3600 canvas-sized
/// lossless PNGs of a 1080p animation is several gigabytes of temp files for
/// a single item. Exhausting this budget truncates exactly like exhausting
/// the frame budget does — the documented under-run — so the caller gets a
/// shorter animation rather than a failure.
pub(crate) fn extract_within(
    bytes: &[u8],
    time: ItemTime,
    cancel: &AtomicBool,
    byte_budget: u64,
) -> Result<BridgedInput, String> {
    extract_with_budget(bytes, time, cancel, FRAME_BUDGET, byte_budget)
}

/// [`extract`] with the decode budgets as parameters, so the budget rules
/// can be pinned against the two-frame fixture instead of a 3600-frame
/// monster nobody wants committed.
fn extract_with_budget(
    bytes: &[u8],
    time: ItemTime,
    cancel: &AtomicBool,
    budget: usize,
    byte_budget: u64,
) -> Result<BridgedInput, String> {
    let dir =
        tempfile::tempdir().map_err(|err| format!("could not create a frame directory: {err}"))?;
    let mut decoder = image::codecs::webp::WebPDecoder::new(Cursor::new(bytes))
        .map_err(|err| format!("the WebP structure did not decode: {err}"))?;
    // The ceiling is checked against the header's declared canvas before
    // anything is allocated: a hostile VP8X can claim a canvas of ~16M
    // pixels a side, and this loop would otherwise write thousands of
    // canvas-sized PNGs of it. Refusal takes the ordinary fallback below.
    image::ImageDecoder::set_limits(&mut decoder, decode_limits())
        .map_err(|err| format!("the decode limits refused this file: {err}"))?;
    let (width, height) = image::ImageDecoder::dimensions(&decoder);
    // ffmpeg does not apply a WebP's EXIF orientation and the frames written
    // below are canvas-space PNGs, so the graph has to close the gap — the
    // same `normalize` an unbridged EXIF-oriented WebP gets
    // (docs/display-dimensions-design.md §1.3).
    //
    // Read on a decoder of its own: `orientation()` seeks to the EXIF chunk,
    // and nothing about this extraction's frame-by-frame position may depend
    // on a metadata seek having left the reader where it found it.
    let normalize = image::codecs::webp::WebPDecoder::new(Cursor::new(bytes))
        .ok()
        .and_then(|mut probe| image::ImageDecoder::orientation(&mut probe).ok())
        .map(orientation_transform)
        .unwrap_or_default();
    let (display_width, display_height) = if normalize.quarter_turns % 2 == 1 {
        (height, width)
    } else {
        (width, height)
    };

    let hold_covering_frame_only = matches!(time, ItemTime::Still { .. });
    let mut script = String::from("ffconcat version 1.0\n");
    let mut decoded = 0usize;
    let mut written = 0usize;
    // The written entries' total: the script's own length, which is what
    // the synthesized duration must report.
    let mut written_ms: u64 = 0;
    // The decoded timeline's position — where the *next* frame starts.
    let mut total_ms: u64 = 0;
    let mut covering: Option<(image::RgbaImage, u64)> = None;
    // What the written PNGs occupy on disk so far, against `byte_budget`.
    let mut written_bytes: u64 = 0;
    let mut frames = decoder.into_frames();
    loop {
        if !wants_more(time, decoded, total_ms) {
            break;
        }
        // The byte ceiling, checked before the next decode for the same
        // reason the frame budget is, and with the same two outcomes: a span
        // already inside its window truncates (the documented under-run),
        // while a bound that was never reached is a refusal rather than a
        // nearby-wrong frame.
        if written_bytes >= byte_budget {
            if matches!(time, ItemTime::Span { .. }) && written > 0 {
                break;
            }
            return Err(format!(
                "the {byte_budget}-byte frame budget ran out before the item's \
                 timestamp was reached"
            ));
        }
        if decoded >= budget {
            // The budget cut extraction short of the item's own bound. A
            // span already inside its window truncates like the documented
            // under-run (`overlay eof_action=repeat` holds the last written
            // frame, output length unaffected); anything else — a still
            // whose covering frame was never reached, a span whose window
            // was never entered — would ship a NEARBY frame as if it were
            // the right one, and wrong-but-plausible is exactly what the
            // bridge must never invent. Error, so the caller falls back and
            // ffmpeg fails fast on the original file.
            if matches!(time, ItemTime::Span { .. }) && written > 0 {
                break;
            }
            return Err(format!(
                "the {budget}-frame budget ran out before the item's timestamp was reached"
            ));
        }
        if cancel.load(Ordering::Relaxed) {
            return Err("the job was cancelled mid-extraction".to_string());
        }
        let Some(frame) = frames.next() else {
            break;
        };
        let frame = frame.map_err(|err| format!("frame {} did not decode: {err}", decoded + 1))?;
        let ms = frame_ms(frame.delay());
        let (frame_start, frame_end) = (total_ms, total_ms.saturating_add(ms));
        decoded += 1;
        total_ms = frame_end;
        if hold_covering_frame_only {
            covering = Some((frame.into_buffer(), ms));
            continue;
        }
        // How much of this frame the script plays: a span writes only the
        // slice inside its window (`[start, end)` — a frame ending exactly
        // at the window's start is before it), an image its whole first
        // frame.
        let slice = match time {
            ItemTime::Span { start_cs, end_cs } => {
                let (start_ms, end_ms) = (cs_ms(start_cs), cs_ms(end_cs));
                (frame_end > start_ms).then(|| frame_end.min(end_ms) - frame_start.max(start_ms))
            }
            _ => Some(ms),
        };
        if let Some(slice) = slice {
            written += 1;
            let name = frame_name(written);
            let frame_path = dir.path().join(&name);
            frame
                .buffer()
                .save(&frame_path)
                .map_err(|err| format!("frame {written} did not write: {err}"))?;
            written_bytes = written_bytes.saturating_add(
                std::fs::metadata(&frame_path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
            );
            script.push_str(&format!("file {name}\nduration {}\n", ms_seconds(slice)));
            written_ms += slice;
        }
    }
    if let Some((buffer, ms)) = covering {
        written = 1;
        let name = frame_name(written);
        buffer
            .save(dir.path().join(&name))
            .map_err(|err| format!("frame {written} did not write: {err}"))?;
        script.push_str(&format!("file {name}\nduration {}\n", ms_seconds(ms)));
        // The one frame is the whole extraction; the frames decoded past on
        // the way to it are not part of what the script plays.
        written_ms = ms;
    }
    if written == 0 {
        // Either image-webp's verdict differs from the structure walk's
        // (parsed animated, decoded nothing) or the whole animation ended
        // before a span's window began; both leave no frame to build an
        // input from.
        return Err("no frames fell inside the item's bounds".to_string());
    }

    let script_path = dir.path().join(SCRIPT_NAME);
    std::fs::write(&script_path, script)
        .map_err(|err| format!("the concat script did not write: {err}"))?;
    Ok(BridgedInput {
        _dir: dir,
        script: script_path,
        info: StreamInfo {
            width: i64::from(display_width),
            height: i64::from(display_height),
            normalize,
            video_index: 0,
            has_audio: false,
            duration_s: Some(written_ms as f64 / 1000.0),
        },
    })
}

fn frame_name(index: usize) -> String {
    format!("frame-{index:05}.png")
}

/// Widest canvas side the bridge will decode. Comfortably past every real
/// animation while capping the worst transient frame buffer at a known
/// figure (16384² RGBA ≈ 1 GiB, one frame at a time); a maliciously *deep*
/// file inside this square is bounded by [`FRAME_BUDGET`], not by memory.
const MAX_DECODE_SIDE: u32 = 16_384;

/// The decode ceiling: a dimension bound, and deliberately nothing else.
/// `WebPDecoder` has no `set_limits` override, so only the default
/// `check_dimensions` pass applies — a `max_alloc` set here would be
/// silently discarded, a claim of protection rather than protection. The
/// decoder's own constructor already refuses any canvas whose *area*
/// overflows `u32`; this bound is what covers the band under that (sides
/// past 16384 whose area still fits), which matters because the bridge's
/// output is not one decode but up to [`FRAME_BUDGET`] canvas-sized PNGs
/// on disk.
fn decode_limits() -> image::Limits {
    let mut limits = image::Limits::no_limits();
    limits.max_image_width = Some(MAX_DECODE_SIDE);
    limits.max_image_height = Some(MAX_DECODE_SIDE);
    limits
}

/// Whether another frame should be decoded, as a pure function of what the
/// loop has already accounted for: `decoded` frames whose floored durations
/// reach `total_ms` on the decoded timeline. Time bounds ONLY — the decode
/// budget is the loop's own check, because running out of budget and
/// satisfying a bound have different consequences (an error versus a normal
/// stop).
///
/// - `Image` shows the first frame only.
/// - `Span` needs the decode to reach `end_cs` (frames before `start_cs`
///   are decoded for the compositor's sake and never written).
/// - `Still` needs the frame covering `at_cs`: decode while the *next*
///   frame would still start at or before it, and the last frame decoded is
///   the covering one (or the animation's last, for a timestamp past the
///   end — the same answer the probe clamp gives an ordinary video).
/// - At least one frame is always wanted: a bridge with no frames is not an
///   input.
fn wants_more(time: ItemTime, decoded: usize, total_ms: u64) -> bool {
    if decoded == 0 {
        return true;
    }
    match time {
        ItemTime::Image => false,
        ItemTime::Span { end_cs, .. } => total_ms < cs_ms(end_cs),
        ItemTime::Still { at_cs } => total_ms <= cs_ms(at_cs),
    }
}

/// Centiseconds to milliseconds, clamped and saturating: item bounds are
/// attacker-supplied `i64`s, and a wrapped budget would decode forever.
fn cs_ms(cs: i64) -> u64 {
    u64::try_from(cs.max(0)).unwrap_or(0).saturating_mul(10)
}

/// One frame's duration in whole milliseconds, floored to 1: WebP legally
/// stores zero-length frames, and both the script and the cumulative
/// accounting must always move forward. The drift a floor introduces against
/// the scanned total is absorbed by the same under-run tolerance a
/// mismeasured GIF already relies on.
fn frame_ms(delay: image::Delay) -> u64 {
    let (numer, denom) = delay.numer_denom_ms();
    let ms = (f64::from(numer) / f64::from(denom.max(1))).round() as u64;
    ms.max(1)
}

/// Milliseconds as the script's `duration` value. Fixed-point, never a
/// float: the same "printing `0.1 * cs` puts rounding error into a cut
/// boundary" rule as [`super::run::seconds`].
fn ms_seconds(ms: u64) -> String {
    format!("{}.{:03}", ms / 1000, ms % 1000)
}

#[cfg(test)]
mod tests {
    use super::super::compose::Transform;
    use super::*;

    /// The committed two-frame fixture: 16x16, red then blue, 500 ms each.
    const FIXTURE: &[u8] = include_bytes!("fixtures/two-frame.webp");

    fn extract_fixture(time: ItemTime) -> BridgedInput {
        extract(FIXTURE, time, &AtomicBool::new(false)).expect("the fixture bridges")
    }

    fn script_of(bridge: &BridgedInput) -> String {
        std::fs::read_to_string(&bridge.script).expect("the script is readable")
    }

    /// Which primary a frame PNG's first pixel is, by a wide margin.
    fn frame_primary(bridge: &BridgedInput, index: usize) -> char {
        let path = bridge.script.parent().unwrap().join(frame_name(index));
        let image = image::open(path).expect("the frame decodes");
        let pixel = image.to_rgba8().get_pixel(0, 0).0;
        assert_eq!(image.width(), 16, "frames are canvas-sized");
        assert_eq!(image.height(), 16);
        let (red, blue) = (i32::from(pixel[0]), i32::from(pixel[2]));
        if red > blue + 50 {
            'r'
        } else if blue > red + 50 {
            'b'
        } else {
            panic!("no primary dominates: {pixel:?}")
        }
    }

    /// The span extraction, pinned to the byte: both frames as PNGs, the
    /// exact script text, and the synthesized probe the graph builder will
    /// see instead of an ffprobe.
    #[test]
    fn a_span_extracts_every_frame_and_writes_the_exact_script() {
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 0,
            end_cs: 100,
        });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\n\
             file frame-00001.png\nduration 0.500\n\
             file frame-00002.png\nduration 0.500\n"
        );
        assert_eq!(frame_primary(&bridge, 1), 'r');
        assert_eq!(frame_primary(&bridge, 2), 'b');
        assert_eq!(
            bridge.info,
            StreamInfo {
                width: 16,
                height: 16,
                normalize: Transform::default(),
                video_index: 0,
                has_audio: false,
                duration_s: Some(1.0),
            }
        );

        // A span past the animation's end simply gets everything there is;
        // the shortfall is the documented under-run, held by the overlay.
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 0,
            end_cs: 500,
        });
        assert_eq!(bridge.info.duration_s, Some(1.0));
        assert_eq!(frame_primary(&bridge, 2), 'b');
        // And one that ends inside the first frame stops there, with the
        // entry's duration trimmed to the window.
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 0,
            end_cs: 30,
        });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.300\n"
        );
        assert_eq!(bridge.info.duration_s, Some(0.3));
    }

    /// The window of a nonzero-start span, honoured by EXTRACTION rather
    /// than by a seek: `-ss`/`-to` on a concat script of image entries land
    /// on entry boundaries (or on nothing at all), empirically on both
    /// toolchains, so the script itself must hold exactly
    /// `[start_cs, end_cs)` — first and last entries' durations trimmed,
    /// total equal to the span arithmetic the chain's loop math uses.
    #[test]
    fn a_nonzero_start_span_is_windowed_by_extraction_not_by_a_seek() {
        // Starting inside the first frame: its remaining 0.25 s plays, then
        // the whole second frame.
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 25,
            end_cs: 100,
        });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\n\
             file frame-00001.png\nduration 0.250\n\
             file frame-00002.png\nduration 0.500\n"
        );
        assert_eq!(frame_primary(&bridge, 1), 'r');
        assert_eq!(frame_primary(&bridge, 2), 'b');
        assert_eq!(bridge.info.duration_s, Some(0.75));

        // Trimmed at both ends.
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 25,
            end_cs: 75,
        });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\n\
             file frame-00001.png\nduration 0.250\n\
             file frame-00002.png\nduration 0.250\n"
        );
        assert_eq!(bridge.info.duration_s, Some(0.5));

        // A start exactly on a frame boundary excludes the frame before it
        // (`[start, end)` semantics): only blue plays, red was decoded for
        // the compositor's sake and never written.
        let bridge = extract_fixture(ItemTime::Span {
            start_cs: 50,
            end_cs: 100,
        });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.500\n"
        );
        assert_eq!(frame_primary(&bridge, 1), 'b');
        assert_eq!(bridge.info.duration_s, Some(0.5));

        // A window that starts past the whole animation has nothing to
        // play, and an empty stream into the graph is exactly the silent
        // wrongness the bridge must not invent: error, fall back, let
        // ffmpeg fail fast.
        assert!(
            extract(
                FIXTURE,
                ItemTime::Span {
                    start_cs: 150,
                    end_cs: 200
                },
                &AtomicBool::new(false)
            )
            .is_err()
        );
    }

    /// An image shows its first frame and nothing else is decoded.
    #[test]
    fn an_image_extracts_the_first_frame_only() {
        let bridge = extract_fixture(ItemTime::Image);
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.500\n"
        );
        assert_eq!(frame_primary(&bridge, 1), 'r');
        assert_eq!(bridge.info.duration_s, Some(0.5));
    }

    /// A still bridges to exactly its covering frame, because there is no
    /// seek that could select it: `-ss` on a concat script of image entries
    /// lands on the NEXT entry, or on nothing at all past the last one
    /// (empirical, 7.1 and 8.0.1 both). The earlier frames are decoded and
    /// discarded on the way.
    #[test]
    fn a_still_extracts_its_covering_frame_alone() {
        // 0.75 s is inside the second frame's [0.5, 1.0).
        let bridge = extract_fixture(ItemTime::Still { at_cs: 75 });
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.500\n"
        );
        assert_eq!(frame_primary(&bridge, 1), 'b');
        assert_eq!(bridge.info.duration_s, Some(0.5));

        // Time zero is the first frame.
        let bridge = extract_fixture(ItemTime::Still { at_cs: 0 });
        assert_eq!(frame_primary(&bridge, 1), 'r');

        // Past the end holds the last frame — the same answer the probe
        // clamp gives an ordinary video's overlong seek.
        let bridge = extract_fixture(ItemTime::Still { at_cs: 10_000 });
        assert_eq!(frame_primary(&bridge, 1), 'b');
    }

    /// The time bounds, as the pure function the loop consults before every
    /// decode (the budget is the loop's own check — see the test below).
    #[test]
    fn the_time_bounds_are_enforced_before_each_decode() {
        let span = ItemTime::Span {
            start_cs: 0,
            end_cs: 100,
        };
        // The first frame is always wanted, whatever the bounds say.
        assert!(wants_more(span, 0, 0));
        assert!(wants_more(ItemTime::Image, 0, 0));
        // A span decodes until the timeline reaches end_cs.
        assert!(wants_more(span, 1, 999));
        assert!(!wants_more(span, 1, 1000));
        // An image never wants a second frame.
        assert!(!wants_more(ItemTime::Image, 1, 500));
        // A still decodes while the next frame would still start at or
        // before at_cs — the last frame decoded is the covering one.
        let still = ItemTime::Still { at_cs: 75 };
        assert!(wants_more(still, 1, 500));
        assert!(wants_more(still, 1, 750));
        assert!(!wants_more(still, 2, 1000));
        // Saturating conversion: no wrap can reopen a bound. i64::MAX
        // centiseconds does not even fit u64 milliseconds, which is the
        // saturation this asserts.
        assert_eq!(cs_ms(i64::MAX), u64::MAX);
        assert_eq!(cs_ms(100), 1000);
        assert_eq!(cs_ms(-5), 0);
    }

    /// The decode budget's two outcomes, pinned with tiny budgets against
    /// the real fixture: a span already inside its window truncates (the
    /// documented under-run degradation), while a timestamp the budget
    /// never reached is an ERROR — shipping the nearest frame instead would
    /// be wrong-but-plausible, the one failure mode the bridge must never
    /// invent.
    #[test]
    fn an_exhausted_budget_truncates_a_started_span_and_refuses_everything_else() {
        let cancel = AtomicBool::new(false);
        let with_budget =
            |time, budget| extract_with_budget(FIXTURE, time, &cancel, budget, u64::MAX);

        // A still whose covering frame lies past the budget: error.
        assert!(with_budget(ItemTime::Still { at_cs: 75 }, 1).is_err());
        // A budget that exactly reaches the covering frame succeeds.
        let bridge = with_budget(ItemTime::Still { at_cs: 75 }, 2).expect("frame 2 covers 0.75 s");
        assert_eq!(frame_primary(&bridge, 1), 'b');
        let bridge = with_budget(ItemTime::Still { at_cs: 30 }, 1).expect("frame 1 covers 0.30 s");
        assert_eq!(frame_primary(&bridge, 1), 'r');

        // A span whose window was never entered: error, not an empty script.
        assert!(
            with_budget(
                ItemTime::Span {
                    start_cs: 50,
                    end_cs: 100
                },
                1
            )
            .is_err()
        );
        // A span already inside its window truncates instead — the last
        // written frame is held by the overlay, output length unaffected.
        let bridge = with_budget(
            ItemTime::Span {
                start_cs: 0,
                end_cs: 100,
            },
            1,
        )
        .expect("a truncated tail is a degradation, not a failure");
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.500\n"
        );
        assert_eq!(bridge.info.duration_s, Some(0.5));

        // And the real ceiling is what `extract` passes down.
        assert_eq!(FRAME_BUDGET, 3600);
    }

    /// The aggregate byte ceiling, which bounds the axis the frame count
    /// cannot: a span already inside its window truncates, and `extract`
    /// itself is unbounded so the compose path is untouched.
    #[test]
    fn an_exhausted_byte_budget_truncates_a_started_span() {
        let cancel = AtomicBool::new(false);
        let span = ItemTime::Span {
            start_cs: 0,
            end_cs: 100,
        };
        // One byte of budget: the first frame is always written (the check
        // runs before the *next* decode), then the span truncates.
        let bridge = extract_within(FIXTURE, span, &cancel, 1)
            .expect("a truncated tail is a degradation, not a failure");
        assert_eq!(
            script_of(&bridge),
            "ffconcat version 1.0\nfile frame-00001.png\nduration 0.500\n"
        );

        // A budget exhausted before anything was written refuses rather than
        // shipping a nearby frame, exactly as the frame budget does.
        assert!(extract_within(FIXTURE, span, &cancel, 0).is_err());

        // Frames the window excludes are decoded but never written, so they
        // do not spend the budget: a span starting at the second frame still
        // gets that frame under a one-byte ceiling.
        let bridge = extract_within(
            FIXTURE,
            ItemTime::Span {
                start_cs: 50,
                end_cs: 100,
            },
            &cancel,
            1,
        )
        .expect("only written frames are charged");
        assert_eq!(frame_primary(&bridge, 1), 'b');

        // A budget no real animation reaches changes nothing.
        let bridge = extract_within(FIXTURE, span, &cancel, u64::MAX).expect("unbounded");
        assert_eq!(bridge.info.duration_s, Some(1.0));
    }

    /// The decode ceiling: a structurally-valid animated WebP whose VP8X
    /// declares an absurd canvas (the format allows ~16M px a side) is
    /// refused by the limits check before any pixel allocation, taking the
    /// same fallback as any other extraction failure.
    #[test]
    fn an_absurd_declared_canvas_is_refused_before_decoding() {
        // The real fixture with only its VP8X canvas fields rewritten to
        // 20000² — deliberately in the one band the bridge's own bound must
        // catch: sides past MAX_DECODE_SIDE whose *area* still fits in u32.
        // A larger canvas is refused by image-webp's constructor before the
        // limits are even consulted, and a from-scratch stub fails structure
        // parsing there too — either way the wrong code path gets exercised.
        let mut huge = FIXTURE.to_vec();
        let vp8x = huge
            .windows(4)
            .position(|w| w == b"VP8X")
            .expect("the fixture is animated, so it carries a VP8X");
        // Chunk body starts past the fourcc + size; the 24-bit little-endian
        // minus-one canvas fields sit at body offsets 4..7 and 7..10.
        // 19999 = 0x004E1F.
        huge[vp8x + 8 + 4..vp8x + 8 + 10].copy_from_slice(&[0x1F, 0x4E, 0x00, 0x1F, 0x4E, 0x00]);

        assert!(
            crate::media_tools::animation::webp_animation_seconds(&huge) > 0.0,
            "the sniff would bridge this file, so extraction must be what refuses it"
        );
        let outcome = extract(&huge, ItemTime::Image, &AtomicBool::new(false));
        let Err(error) = outcome else {
            panic!("refused before decoding, not decoded");
        };
        assert!(
            error.contains("the decode limits refused this file"),
            "the bridge's own bound must be what refuses it, not an \
             earlier structure error: {error}"
        );
    }

    /// Zero and fractional delays: floored to 1 ms in the script AND the
    /// accounting, so a file of spec-legal zero-duration frames still makes
    /// progress toward every bound.
    #[test]
    fn frame_durations_floor_to_one_millisecond() {
        // `Delay` itself refuses a zero denominator, so the floor inside
        // `frame_ms` only ever has the zero *numerator* to correct — but it
        // guards the ratio anyway, being one division away from a panic.
        let ms = |numer, denom| frame_ms(image::Delay::from_numer_denom_ms(numer, denom));
        assert_eq!(ms(500, 1), 500);
        assert_eq!(ms(0, 1), 1);
        assert_eq!(ms(100, 3), 33);
        assert_eq!(ms_seconds(500), "0.500");
        assert_eq!(ms_seconds(1), "0.001");
        assert_eq!(ms_seconds(1250), "1.250");
    }

    /// The sniff: only a real animated WebP answers with bytes. A still
    /// WebP, a GIF, garbage, and a missing file all pass through unbridged.
    #[test]
    fn only_animated_webps_sniff_as_bridgeable() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let path = dir.path().join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        };

        let animated = write("animated.webp", FIXTURE);
        assert_eq!(sniff_animated_webp(&animated).as_deref(), Some(FIXTURE));
        assert!(has_webp_magic(&animated));

        // A plain still WebP: RIFF/WEBP magic but no animation.
        let mut still = Vec::new();
        still.extend_from_slice(b"RIFF");
        still.extend_from_slice(&12u32.to_le_bytes());
        still.extend_from_slice(b"WEBP");
        still.extend_from_slice(b"VP8 ");
        still.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(sniff_animated_webp(&write("still.webp", &still)), None);

        // The magic gate — what keeps whole-file reads (and the native-
        // decode probe) off sources that are something else entirely.
        let gif = write("actually.gif", b"GIF89a not a webp");
        assert!(!has_webp_magic(&gif));
        assert_eq!(sniff_animated_webp(&gif), None);
        assert_eq!(sniff_animated_webp(&write("tiny", b"RIFF")), None);
        assert!(!has_webp_magic(&dir.path().join("missing.webp")));
        assert_eq!(sniff_animated_webp(&dir.path().join("missing.webp")), None);
    }

    /// Extraction failure is an error, never a panic and never a silent
    /// success: bytes that sniff as animated but do not decode (here,
    /// truncated mid-structure) report why, and the caller falls back to
    /// the unbridged file.
    #[test]
    fn undecodable_bytes_refuse_to_bridge_with_a_reason() {
        let cancel = AtomicBool::new(false);
        assert!(extract(b"RIFF\x04\x00\x00\x00WEBP", ItemTime::Image, &cancel).is_err());
        // A structurally-plausible header whose frames are cut off.
        assert!(extract(&FIXTURE[..FIXTURE.len() / 2], ItemTime::Image, &cancel).is_err());
        // Cancellation is checked before every frame, including the first.
        let cancelled = AtomicBool::new(true);
        assert!(extract(FIXTURE, ItemTime::Image, &cancelled).is_err());
    }
}
