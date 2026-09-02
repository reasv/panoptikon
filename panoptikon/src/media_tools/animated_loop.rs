//! The animated grid rendition: one H.264 loop per animated item
//! (docs/grid-scroll-performance-implementation.md §2, step B2).
//!
//! A GIF in a grid cell is the worst case the whole tier ladder exists for —
//! an uncompressed-ish palette animation decoded at full resolution, every
//! frame, in every visible cell. The rendition that replaces it is a plain
//! progressive mp4: libx264, yuv420p, a CRF chosen by the rendition's rung,
//! `+faststart`, source
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
use crate::visual_tiers::{RenditionRung, TierPlan};

/// x264's rate factor for a **grid** loop. Visually transparent for the
/// material — flat-colour animations and short clips — at a fraction of a
/// GIF's bytes. A change here needs a `LOOP_PROCESS_VERSION` bump: the stored
/// geometry cannot see it.
const LOOP_CRF: u32 = 18;

/// The same for a **display** loop, which is watched at full size rather than
/// in a grid cell and gets the two steps of quality that costs.
const LOOP_DISPLAY_CRF: u32 = 16;

/// The rate factor one rung's loop is encoded at.
///
/// Looked up from the rung rather than passed in beside it, exactly as the
/// still encoders look up their qualities: a caller that carries the number
/// is a caller that can hand a grid loop the display one's.
fn crf(rung: RenditionRung) -> u32 {
    match rung {
        RenditionRung::Grid => LOOP_CRF,
        RenditionRung::Display => LOOP_DISPLAY_CRF,
    }
}

/// The longest loop this encoder will produce, in seconds
/// (docs/thumbnail-format-implementation.md §4).
///
/// A cap rather than a fidelity choice: a grid cell is not a video player, and
/// an unbounded loop lets one 30-minute animation cost more scan time and more
/// stored bytes than the rest of a library's animations together. Applied on
/// the **output** side so it reaches both input paths, and mirrored in
/// [`WHOLE_ANIMATION`] so the WebP bridge stops decoding frames at the same
/// boundary rather than decoding an hour of them for ffmpeg to throw away.
pub(crate) const LOOP_MAX_SECONDS: u32 = 60;

/// x264's speed/size tradeoff. `medium` is the default and is where the curve
/// flattens; a scan encodes one of these per animated item, so a slower
/// preset would buy single-digit percent for a multiple of the scan time.
const LOOP_PRESET: &str = "medium";

/// The animation a loop is made of, expressed in the bridge's window
/// vocabulary: from zero to [`LOOP_MAX_SECONDS`]. A span already inside the
/// window truncates rather than failing — which for a loop is the right
/// degradation (a shorter loop, never a wrong one) and is exactly how the cap
/// takes effect on the bridged path.
const WHOLE_ANIMATION: ItemTime = ItemTime::Span {
    start_cs: 0,
    end_cs: LOOP_MAX_SECONDS as i64 * 100,
};

/// Why a loop could not be produced.
///
/// Four outcomes, because the ledger owes different verdicts for each and
/// only the caller can write them (`jobs::files::build_animated_tiers` maps
/// them onto the same negative-cache machinery the image pass uses). The
/// distinction that matters most is **who failed**: exactly one of these is a
/// statement about the file.
#[derive(Debug)]
pub(crate) enum LoopError {
    /// ffmpeg could not be *started*. Never a verdict on the media: a missing
    /// toolchain is `blocked` and self-heals when it appears, and anything
    /// else about this host stays transient.
    Spawn(std::io::Error),
    /// This machine could not hold the encode — a scratch directory that
    /// would not open, an output that would not read back.
    ///
    /// Kept apart from [`Self::Failed`] precisely because the two are
    /// indistinguishable at the call site otherwise, and the consequences are
    /// not: a disk that fills during a library-wide backfill would spend a
    /// strike against every animated item in the library and retire the
    /// feature wholesale. Host trouble is transient, always.
    Host(String),
    /// ffmpeg ran and did not produce a usable loop. **The one verdict about
    /// the content** — and still only half of one, because ffmpeg did its own
    /// file I/O, so a broken file and a mount hiccup exit identically and
    /// this needs a second failure before it settles anything.
    Failed(String),
    /// This toolchain has no decoder for the container at all.
    ///
    /// **Defensive, and unreachable from a scan**: the dispatcher does not
    /// classify an item as animated when its container needs a decoder this
    /// build lacks — `jobs::files::grid_ladder` asks the same probe — so
    /// nothing reaches the encoder to fail this way. Kept because the encoder
    /// is callable outside that gate, and treated as **transient**: a
    /// permanent verdict here is exactly the trap R2-A found, because no
    /// ledger heal path could ever notice a capable ffmpeg being installed
    /// later. The gate re-evaluates on process restart instead.
    Unsupported(String),
}

impl std::fmt::Display for LoopError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(err) => write!(formatter, "ffmpeg did not start: {err}"),
            Self::Host(detail) | Self::Failed(detail) | Self::Unsupported(detail) => {
                formatter.write_str(detail)
            }
        }
    }
}

/// Encodes one item's loop and returns the mp4 bytes.
///
/// `plan` is [`crate::visual_tiers::loop_plan`]'s geometry in **display**
/// space, and `normalize` is the transform that takes the file's stored
/// pixels there (an EXIF-oriented WebP; GIF carries no orientation). The
/// scale filter is unconditional and exact, so the encoded stream always has
/// the dimensions the dispatcher predicted — an output that silently differed
/// would re-dispatch the item on every scan forever.
pub(crate) fn encode_loop(
    path: &Path,
    mime_type: &str,
    plan: &TierPlan,
    normalize: Transform,
    rung: RenditionRung,
) -> Result<Vec<u8>, LoopError> {
    let dir = tempfile::tempdir()
        .map_err(|err| LoopError::Host(format!("could not create a loop directory: {err}")))?;
    let output = dir.path().join("loop.mp4");

    // Held across the ffmpeg run — including the `cache:` retry inside
    // `ffmpeg_output_with_input_retry` — because dropping it deletes the
    // frames the concat script names.
    let bridge = prepare_input(path, mime_type)?;
    let (input_args, input): (&[&str], &Path) = match &bridge {
        Some(bridge) => (&["-f", "concat"], bridge.script.as_path()),
        None => (&[], path),
    };

    let mut args: Vec<OsString> = Vec::new();
    for arg in ["-nostdin", "-hide_banner", "-nostats", "-v", "error"] {
        args.push(arg.into());
    }
    // The pathological-GIF-delay floor (§2, B2 round 2). Measured, not
    // assumed: ffmpeg's gif demuxer substitutes its `default_delay` for a
    // 0 cs frame but leaves **1 cs** exactly as written, and its documented
    // `-min_delay` / `-default_delay` demuxer options have no effect on that
    // case at all (verified on 7.1 against a real 1 cs file). Browsers and
    // `media_tools::animation::gif_animation_seconds` both floor 0 and 1 cs
    // to 10 cs, so without this the stored loop plays ~10x too fast and its
    // duration disagrees with `items.duration` and with the display path.
    //
    // Applied as an input frame rate — which restamps every frame onto a
    // uniform grid while keeping all of them — and *only* when every delay in
    // the file is pathological, which is how the tools that write them write
    // them. A file with any real delay keeps its exact source timing (see
    // `gif_uniform_pathological_rate`).
    if let Some(rate) = gif_forced_input_rate(path, mime_type) {
        args.push("-r".into());
        args.push(rate.to_string().into());
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
    // Output-side, never input-side: an input time option on a concat script
    // lands on entry boundaries or on nothing at all (the bridge's documented
    // trap), and here it would also have to disagree with the bridge's own
    // window. On the output it truncates both paths identically.
    args.push("-t".into());
    args.push(LOOP_MAX_SECONDS.to_string().into());
    let crf = crf(rung).to_string();
    for arg in [
        "-c:v",
        "libx264",
        "-preset",
        LOOP_PRESET,
        "-crf",
        crf.as_str(),
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
        Err(err) => return Err(LoopError::Spawn(err)),
    };
    drop(bridge);
    if !outcome.status.success() {
        return Err(LoopError::Failed(format!(
            "ffmpeg failed: {}",
            crate::media_tools::stderr_tail(&outcome.stderr)
        )));
    }
    // ffmpeg reported success, so the bytes not coming back is this machine's
    // problem and not the file's — a full disk, a scratch directory swept out
    // from under the run.
    let bytes = std::fs::read(&output)
        .map_err(|err| LoopError::Host(format!("the encoded loop did not read back: {err}")))?;
    if bytes.is_empty() {
        return Err(LoopError::Failed(
            "ffmpeg produced an empty loop".to_string(),
        ));
    }
    Ok(bytes)
}

/// The input frame rate that floors a GIF's pathological frame delays, or
/// `None` for every file whose timing must be left exactly as written.
///
/// Reads the file — the same whole-file read the scan's animation question
/// already performs for this mime — and answers only for `image/gif`.
fn gif_forced_input_rate(path: &Path, mime_type: &str) -> Option<u32> {
    if mime_type != "image/gif" {
        return None;
    }
    let bytes = std::fs::read(path).ok()?;
    crate::media_tools::animation::gif_uniform_pathological_rate(&bytes)
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
fn loop_filters(plan: &TierPlan, normalize: Transform) -> String {
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

/// Aggregate byte ceiling on the bridge's decoded frames, on top of its frame
/// count budget.
///
/// The frame budget alone bounds the wrong axis. 3600 canvas-sized lossless
/// PNGs of a 1920x1080 animation is 3.6-7 GB of temp files for one item —
/// enough to fill a scratch volume mid-scan on a library with a handful of
/// long animated WebPs. This bounds what actually lands on disk, and it
/// degrades through the bridge's existing truncation semantics: a shorter
/// loop, with the stored geometry still exactly what the dispatcher
/// predicted, rather than a failure.
const BRIDGE_BYTE_BUDGET: u64 = 2 * 1024 * 1024 * 1024;

/// The bridged input for an animated WebP, `Ok(None)` for everything that
/// goes straight to ffmpeg, and `Err` for a container this toolchain has no
/// decoder for at all.
///
/// Cheapest check first, exactly as the compose path orders them: the mime
/// gate, then a 12-byte magic read, then the native-decode bypass, then the
/// whole-file structure sniff. A WebP *extraction* failure is not fatal — the
/// original file is passed through and ffmpeg fails fast on it, which is the
/// pre-bridge behaviour and an error rather than an invented success.
///
/// AVIF is the one container with no fallback of either kind: the bridge
/// decodes WebP only, so when this toolchain cannot demux animated AVIF
/// either (`hw::animated_avif_decodable`, the same probe the compose path's
/// capability list is built from) there is nothing left to try. That is a
/// permanent nothing on this host, not a failure to retry — the caller turns
/// it into a ledger verdict so the next scan does not repeat a decode and a
/// process spawn to reach the same conclusion.
fn prepare_input(
    path: &Path,
    mime_type: &str,
) -> Result<Option<super::transcode::webp_bridge::BridgedInput>, LoopError> {
    let support = super::animated_container_support(mime_type);
    if support.loop_is_undecodable() {
        return Err(LoopError::Unsupported(
            "this ffmpeg cannot decode animated AVIF, and the frame bridge covers WebP only"
                .to_string(),
        ));
    }
    if support != super::AnimatedContainer::Bridge {
        return Ok(None);
    }
    if !super::transcode::webp_bridge::has_webp_magic(path)
        || super::transcode::hw::animated_webp_decodable()
    {
        return Ok(None);
    }
    let Some(bytes) = super::transcode::webp_bridge::sniff_animated_webp(path) else {
        return Ok(None);
    };
    // No cancellation flag, and deliberately not a pretend one: the scan
    // cancels by aborting the *async* job task (`JobRunnerMessage::
    // CancelRunning` calls `JoinHandle::abort`), and this whole visuals pass
    // runs inside a `spawn_blocking` closure, which tokio cannot abort and
    // which is therefore always run to completion. Nothing to thread until
    // the scan grows a real cooperative cancellation token; the budgets below
    // are what bound a hostile file's cost in the meantime.
    let cancel = AtomicBool::new(false);
    match super::transcode::webp_bridge::extract_within(
        &bytes,
        WHOLE_ANIMATION,
        &cancel,
        BRIDGE_BYTE_BUDGET,
    ) {
        Ok(bridge) => Ok(Some(bridge)),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                error,
                "could not bridge an animated WebP for its loop; letting ffmpeg try the file"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visual_tiers::loop_plan;

    /// A normal-aspect source: the crop is the whole frame, and the scale
    /// lands on exactly the geometry the dispatcher predicted.
    #[test]
    fn an_ordinary_loop_keeps_the_whole_frame() {
        let plan = loop_plan(1500, 2000);
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=1500:2000:0:0,scale=1024:1364:flags=lanczos+accurate_rnd+full_chroma_int"
        );
    }

    /// A strip crops first and scales second, and the crop is the top band
    /// `object-position: 50% 0%` paints.
    #[test]
    fn a_strip_crops_before_it_scales() {
        let plan = loop_plan(800, 20000);
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=800:2048:0:0,scale=800:2048:flags=lanczos+accurate_rnd+full_chroma_int"
        );

        // A wide strip keeps the horizontally centered band.
        let plan = loop_plan(20000, 800);
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
        let plan = loop_plan(800, 20000);
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
        let plan = loop_plan(300, 300);
        assert_eq!((plan.width, plan.height), (300, 300));
        assert_eq!(
            loop_filters(&plan, Transform::default()),
            "crop=300:300:0:0,scale=300:300:flags=lanczos+accurate_rnd+full_chroma_int"
        );
    }

    /// The bridge's window is the same 60 second cap the output-side `-t`
    /// applies, so a bridged animation and a demuxed one truncate alike.
    #[test]
    fn the_bridge_window_is_the_whole_animation() {
        assert_eq!(
            WHOLE_ANIMATION,
            ItemTime::Span {
                start_cs: 0,
                end_cs: 6000
            }
        );
    }

    /// The two-frame animated WebP the compose bridge is pinned against,
    /// reused here so the loop's bridge path has a real file to walk.
    const WEBP_FIXTURE: &[u8] = include_bytes!("transcode/fixtures/two-frame.webp");

    /// A real animated GIF with the delays this test wants, written by the
    /// `image` crate's encoder so the LZW data is valid and ffmpeg will
    /// actually decode it. Frames differ from one another, so nothing about
    /// the timing can be an artefact of a degenerate encode.
    fn write_gif(path: &Path, side: u32, delays_ms: &[u32]) {
        let file = std::fs::File::create(path).expect("the fixture is writable");
        let mut encoder = image::codecs::gif::GifEncoder::new(file);
        encoder
            .set_repeat(image::codecs::gif::Repeat::Infinite)
            .expect("the repeat header is written");
        let frames: Vec<image::Frame> = delays_ms
            .iter()
            .enumerate()
            .map(|(index, ms)| {
                let shade = (index as u8).wrapping_mul(31);
                let buffer = image::RgbaImage::from_fn(side, side, |x, _| {
                    if (x / 32) % 2 == 0 {
                        image::Rgba([shade, 40, 200, 255])
                    } else {
                        image::Rgba([200, shade, 40, 255])
                    }
                });
                image::Frame::from_parts(
                    buffer,
                    0,
                    0,
                    image::Delay::from_numer_denom_ms(*ms, 1),
                )
            })
            .collect();
        encoder.encode_frames(frames).expect("the fixture encodes");
    }

    /// `(frame count, container duration, every frame's presentation time)`
    /// of an encoded loop, straight from ffprobe.
    fn probe_loop(bytes: &[u8]) -> (usize, f64, Vec<f64>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("loop.mp4");
        std::fs::write(&path, bytes).unwrap();
        let run = |args: &[&str]| {
            let output = std::process::Command::new(crate::media_tools::ffprobe())
                .args(args)
                .arg(&path)
                .output()
                .expect("ffprobe runs");
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };
        let duration: f64 = run(&[
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
        ])
        .parse()
        .expect("a duration");
        let pts: Vec<f64> = run(&[
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "frame=pts_time",
            "-of",
            "csv=p=0",
        ])
        .lines()
        .filter_map(|line| line.trim().trim_end_matches(',').parse().ok())
        .collect();
        (pts.len(), duration, pts)
    }

    /// The 60 second cap, on the input path that has no window of its own.
    ///
    /// A grid cell is not a video player: an unbounded loop lets one long
    /// animation cost more scan time and more stored bytes than every other
    /// animation in a library put together. The bridge truncates at the same
    /// boundary through [`WHOLE_ANIMATION`], so this exercises the half that
    /// only the output-side `-t` reaches.
    #[test]
    fn a_loop_is_capped_at_sixty_seconds() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ninety.gif");
        // 90 frames of one second each: 90 s of animation, of which 60 may
        // be stored.
        write_gif(&path, 64, &[1000; 90]);

        let (frames, duration, _) = probe_loop(&encoded(&path, "image/gif", 64));
        assert!(
            (59.0..=61.0).contains(&duration),
            "a 90 second animation must be truncated to the cap, not stored whole: {duration}"
        );
        assert!(
            (58..=61).contains(&frames),
            "and the frames stop with it: {frames}"
        );
    }

    fn encoded(path: &Path, mime: &str, side: u32) -> Vec<u8> {
        encode_loop(
            path,
            mime,
            &loop_plan(side, side),
            Transform::default(),
            RenditionRung::Grid,
        )
        .expect("the fixture encodes")
    }

    /// The pathological-delay floor, end to end on a real GIF.
    ///
    /// A file whose every frame claims 1 cs is written by ad tools meaning
    /// "as fast as possible", and everything that shows it to a human — the
    /// browsers, and `gif_animation_seconds`, hence `items.duration` and the
    /// display path — plays it at 10 cs a frame. ffmpeg does not: it
    /// substitutes for 0 cs and leaves 1 cs alone, and its `-min_delay` /
    /// `-default_delay` demuxer options do not change that. Without the
    /// floor the stored loop runs 10x fast and a tenth as long as the item it
    /// belongs to.
    #[test]
    fn a_uniformly_pathological_gif_loops_at_the_length_it_plays_at() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("onecs.gif");
        write_gif(&path, 640, &[10; 8]);

        // The premise, from the same parser `items.duration` comes from.
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            crate::media_tools::animation::gif_animation_seconds(&bytes),
            0.8,
            "eight frames floored to 10 cs each"
        );
        assert_eq!(
            crate::media_tools::animation::gif_uniform_pathological_rate(&bytes),
            Some(10)
        );

        let (frames, duration, pts) = probe_loop(&encoded(&path, "image/gif", 640));
        assert_eq!(frames, 8, "every frame survives the retiming");
        assert!(
            (duration - 0.8).abs() < 0.001,
            "the loop must run as long as the item says it does: {duration}"
        );
        for (index, time) in pts.iter().enumerate() {
            assert!(
                (time - index as f64 * 0.1).abs() < 0.001,
                "frame {index} at {time}"
            );
        }
    }

    /// The other half of the same rule: a file with real delays keeps its
    /// exact source timing, variable frame durations and all. This is what
    /// the floor must not touch, and why it engages only when *every* delay
    /// is pathological.
    #[test]
    fn a_mixed_delay_gif_keeps_its_exact_source_timing() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.gif");
        write_gif(&path, 320, &[100, 500, 100, 900]);

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(
            crate::media_tools::animation::gif_animation_seconds(&bytes),
            1.6
        );
        assert_eq!(
            crate::media_tools::animation::gif_uniform_pathological_rate(&bytes),
            None,
            "a real delay anywhere means the source timing is authoritative"
        );

        let (frames, duration, pts) = probe_loop(&encoded(&path, "image/gif", 320));
        assert_eq!(frames, 4);
        assert!((duration - 1.6).abs() < 0.001, "{duration}");
        for (index, expected) in [0.0, 0.1, 0.6, 0.7].iter().enumerate() {
            assert!(
                (pts[index] - expected).abs() < 0.001,
                "frame {index} at {} wanted {expected}",
                pts[index]
            );
        }
    }

    /// The animated-WebP path, which no mainline ffmpeg can demux: the file
    /// is decoded in Rust and substituted as an ffconcat script of PNG
    /// frames. Walked from the magic read through the sniff to a real
    /// encode, against the same committed fixture the compose bridge is
    /// pinned to.
    #[test]
    fn an_animated_webp_loops_through_the_frame_bridge() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("two-frame.webp");
        std::fs::write(&path, WEBP_FIXTURE).unwrap();

        assert!(super::super::transcode::webp_bridge::has_webp_magic(&path));
        assert!(super::super::transcode::webp_bridge::sniff_animated_webp(&path).is_some());
        let bridged = prepare_input(&path, "image/webp").expect("no unsupported container");
        assert_eq!(
            bridged.is_some(),
            !super::super::transcode::hw::animated_webp_decodable(),
            "the bridge engages exactly when this toolchain cannot demux the file"
        );
        if let Some(bridge) = &bridged {
            let script = std::fs::read_to_string(&bridge.script).unwrap();
            assert_eq!(script.matches("file frame-").count(), 2);
        }
        drop(bridged);

        // 16x16, so the loop is the source size rounded to even: unchanged.
        let plan = loop_plan(16, 16);
        assert_eq!((plan.width, plan.height), (16, 16));
        let (frames, duration, _) = probe_loop(
            &encode_loop(&path, "image/webp", &plan, Transform::default(), RenditionRung::Grid)
                .expect("the bridged fixture encodes"),
        );
        assert_eq!(frames, 2, "both frames of the fixture");
        assert!(
            duration > 0.5,
            "the two 500 ms frames must not collapse: {duration}"
        );
    }

    /// AVIF has no fallback of either kind — the frame bridge decodes WebP
    /// only — so a toolchain that cannot demux animated AVIF has nothing left
    /// to try. That is a permanent nothing on this host, reported as such so
    /// the caller can record it instead of paying a decode and a process
    /// spawn to rediscover it on every scan.
    #[test]
    fn an_undecodable_animated_avif_reports_unsupported() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("clip.avif");
        std::fs::write(&path, b"not really an avif").unwrap();

        let outcome = prepare_input(&path, "image/avif");
        if super::super::transcode::hw::animated_avif_decodable() {
            assert!(
                matches!(outcome, Ok(None)),
                "a capable toolchain takes the ordinary path"
            );
        } else {
            assert!(
                matches!(outcome, Err(LoopError::Unsupported(_))),
                "an incapable one says so rather than failing per scan"
            );
        }
    }
}
