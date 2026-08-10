//! Composition documents: the N-input mosaic request, the validation that
//! admits it, and the filtergraph it resolves to
//! (docs/video-transcoding-implementation.md §4 C1/C2).
//!
//! A composition is described entirely by the client: a canvas, a frame rate,
//! an output length policy, and a list of items each carrying a source
//! rectangle (in the source's own pixels, *before* its display orientation is
//! applied), a display transform, a destination rectangle on the canvas, and
//! what the item is showing — a playing span, a frozen frame, or a still
//! image. The server never re-derives any of that geometry: the pinboard's own
//! layout solve is the authority, and reproducing it here would guarantee a
//! drift nobody could see until an export looked wrong.
//!
//! What the server *does* own is admission. Every limit below bounds work that
//! has already been paid for by the time ffmpeg reports it — the loop-memory
//! guard in particular, which is the difference between a refused request and
//! a host that runs out of memory halfway through an encode.
//!
//! The resolved document ([`ResolvedCompose`]) is the cache key's input, so it
//! carries every value the filtergraph reads: normalization (the background
//! colour, the frame rate against the preset's cap, the target length against
//! the container's) happens *before* hashing, never inside the builder. A
//! config edit that changes a cap therefore mints a new key rather than
//! serving bytes produced under the old one.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::presets::{Container, ResolvedPreset};
use super::run::seconds;
use super::{PARAMS_HASH_HEX_LEN, TRANSCODER_VERSION};

/// First half of a composition's cache key, where a single-file artifact
/// carries its source hash. A composition has no single source, so the key is
/// the composition hash alone — and the prefix is what tells the serving path
/// which kind of artifact a bare key names.
pub(crate) const COMPOSE_KEY_PREFIX: &str = "compose";

/// Smallest canvas that is still a picture. Even, like every canvas dimension:
/// the pipeline ends in a 4:2:0 chroma format, which has no odd sizes.
const MIN_CANVAS_SIDE: i64 = 16;
/// Widest canvas side, and the area behind it. Both are code constants rather
/// than config: they bound a *client-authored* geometry, and every deployment
/// pays the same decode cost for it.
const MAX_CANVAS_SIDE: i64 = 4096;
const MAX_CANVAS_AREA: i64 = 3840 * 2160;

/// Frame-rate ceiling for a composition, before the preset's own cap. Beyond
/// this the loop buffers grow without any visible gain.
const MAX_COMPOSE_FPS: u32 = 60;

/// What a composition of nothing but stills runs for when the length policy is
/// `longest_loop_once`: no item contributes a duration, and a frozen mosaic is
/// a legitimate thing to ask for (the server-side equivalent of the still
/// export), so the answer is a short fixed clip rather than a rejection.
const STILLS_ONLY_TARGET_CS: i64 = 100;

/// Sample rate the audio loop buffer is sized against. `aloop`'s `size` is a
/// *maximum*, so over-estimating costs nothing (the segment ends first) while
/// under-estimating would loop a fraction of the segment. No consumer format
/// exceeds this.
const LOOP_AUDIO_RATE: i64 = 192_000;

/// Bytes per pixel of a buffered 4:2:0 frame, as the admission guard counts
/// them: one luma sample plus two quarter-resolution chroma samples.
const LOOP_BYTES_PER_PIXEL_NUM: u128 = 3;
const LOOP_BYTES_PER_PIXEL_DEN: u128 = 2;

const BYTES_PER_MB: u128 = 1024 * 1024;

// --- wire shapes -----------------------------------------------------------

/// A rectangle. Source rectangles are in the source's own pixels *before* its
/// display orientation is applied; destination rectangles are in output pixels
/// on the canvas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub(crate) struct Rect {
    pub(crate) x: i64,
    pub(crate) y: i64,
    pub(crate) w: i64,
    pub(crate) h: i64,
}

/// The display transform of the dihedral group of order 8: `flip_h` applied
/// after `quarter_turns` clockwise rotations. The same decomposition the
/// pinboard stores per pin, passed through verbatim.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub(crate) struct Transform {
    /// 0-3 clockwise quarter turns.
    #[serde(default)]
    pub(crate) quarter_turns: u8,
    #[serde(default)]
    pub(crate) flip_h: bool,
}

/// What an item is showing. Replaces the design's separate "playing" and
/// "muted" flags (§0.5): a span *is* playing, a still and an image are
/// stopped, so no combination of fields can contradict another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ItemTime {
    /// A playing range. `end_cs` is **required**: the client always knows the
    /// duration, and with both bounds the target length is arithmetic rather
    /// than a probe of every input before the job can even be keyed.
    Span { start_cs: i64, end_cs: i64 },
    /// One frame of a video, held for the whole output.
    Still { at_cs: i64 },
    /// A still image file, held for the whole output.
    Image,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub(crate) struct ComposeItem {
    /// Item content hash; resolved against the request's index database.
    pub(crate) sha256: String,
    pub(crate) src: Rect,
    #[serde(default)]
    pub(crate) transform: Transform,
    pub(crate) dest: Rect,
    pub(crate) time: ItemTime,
    /// Whether this item's audio is mixed in. The client sets it to
    /// `playing && !muted`; it is forced off for a still, an image, or a
    /// container that carries no audio at all.
    #[serde(default)]
    pub(crate) audio: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub(crate) struct Canvas {
    pub(crate) w: i64,
    pub(crate) h: i64,
    /// `#RRGGBB`, `#RRGGBBAA` or the `0x` spelling of either. Normalized
    /// before it reaches a filtergraph, which is not decoration: the value is
    /// interpolated into a filter argument, where an unvalidated string could
    /// spell further filters.
    #[serde(default = "default_background")]
    pub(crate) background: String,
}

fn default_background() -> String {
    "#000000".to_string()
}

/// How long the output runs.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum ComposeLength {
    /// The longest span plays exactly once; everything shorter loops to fill.
    LongestLoopOnce,
    /// A fixed length, whatever the items are.
    Cap { seconds: f64 },
}

#[derive(Debug, Clone, PartialEq, Deserialize, ToSchema)]
pub(crate) struct ComposeOutput {
    /// Preset id from `GET /api/video/presets`.
    pub(crate) preset: String,
    pub(crate) length: ComposeLength,
}

/// The composition document as it arrives.
#[derive(Debug, Clone, PartialEq, Deserialize, ToSchema)]
pub(crate) struct ComposeRequest {
    pub(crate) canvas: Canvas,
    /// Output frame rate, 1-60, then capped by the preset.
    pub(crate) fps: u32,
    pub(crate) output: ComposeOutput,
    pub(crate) items: Vec<ComposeItem>,
}

// --- resolved (hashed) shape ----------------------------------------------

/// The composition with every value the filtergraph reads already decided.
///
/// This *is* the cache key's input, so — exactly as for
/// [`super::TranscodeParams`] — its serialization is a contract: fields hash
/// in declaration order and every normalization happens before it is built.
/// A pinned fixture test guards the hash; changing the shape requires a
/// [`TRANSCODER_VERSION`] bump.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResolvedCompose {
    pub(crate) canvas_w: i64,
    pub(crate) canvas_h: i64,
    /// Normalized to ffmpeg's `0xRRGGBB[AA]`.
    pub(crate) background: String,
    pub(crate) fps: u32,
    /// Output length in centiseconds, already clamped to the container's cap.
    /// Resolved here rather than left as the request's policy so that a config
    /// edit which moves a cap mints a new key instead of re-serving bytes
    /// produced under the old one.
    pub(crate) target_cs: i64,
    pub(crate) items: Vec<ComposeItem>,
}

/// Everything that decides a composition's bytes, hashed exactly like
/// [`super::TranscodeParams`].
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ComposeParams {
    pub(crate) doc: ResolvedCompose,
    pub(crate) preset: ResolvedPreset,
    pub(crate) encoder: String,
    pub(crate) transcoder_version: i64,
}

impl ComposeParams {
    pub(crate) fn new(doc: ResolvedCompose, preset: ResolvedPreset, encoder: String) -> Self {
        Self {
            doc,
            preset,
            encoder,
            transcoder_version: TRANSCODER_VERSION,
        }
    }

    /// [`Self::new`] with the encoder resolved against this host's hardware
    /// probe. **Blocking**, like its single-file twin.
    pub(crate) fn resolve(doc: ResolvedCompose, preset: ResolvedPreset) -> Self {
        let encoder = super::run::resolve_encoder(&preset, super::hw::fast_h264_encoder());
        Self::new(doc, preset, encoder)
    }

    pub(crate) fn params_hash(&self) -> String {
        let canonical =
            serde_json::to_string(self).expect("ComposeParams is a plain serializable struct");
        let mut digest = hex::encode(Sha256::digest(canonical.as_bytes()));
        digest.truncate(PARAMS_HASH_HEX_LEN);
        digest
    }

    /// `compose-<params hash>`. The prefix stands where a single-file key
    /// carries its source hash: a composition has many sources and no one of
    /// them names it.
    pub(crate) fn cache_key(&self) -> String {
        format!("{COMPOSE_KEY_PREFIX}-{}", self.params_hash())
    }

    pub(crate) fn artifact_file_name(&self) -> String {
        format!("{}.{}", self.cache_key(), self.preset.container.ext())
    }

    pub(crate) fn mime_type(&self) -> &'static str {
        self.preset.container.mime_type()
    }

    /// The name a download of this composition gets. There is no source stem
    /// to build one from — a mosaic has as many stems as it has items, and the
    /// single-item save's stem would make two documents that render the same
    /// canvas download under different names — so the scheme is fixed and
    /// derived from the item count alone.
    pub(crate) fn download_file_name(&self) -> String {
        compose_file_name(Some(self.doc.items.len()), self.preset.container.ext())
    }

    /// Output length in seconds, which is also the progress denominator.
    pub(crate) fn target_seconds(&self) -> f64 {
        self.doc.target_cs as f64 / 100.0
    }
}

/// Whether a bare cache key names a composition rather than a single-file
/// rendition. The serving path has nothing else to go on: a `key=` request
/// carries no document.
pub(crate) fn is_compose_key(key: &str) -> bool {
    key.strip_prefix(COMPOSE_KEY_PREFIX)
        .is_some_and(|rest| rest.starts_with('-'))
}

/// The download name for a composition. `None` items — the serving path, which
/// knows only the key — gets the bare form.
pub(crate) fn compose_file_name(items: Option<usize>, ext: &str) -> String {
    match items {
        Some(items) if items > 1 => format!("mosaic-{items}items.{ext}"),
        _ => format!("mosaic.{ext}"),
    }
}

// --- validation ------------------------------------------------------------

/// A refused composition. `reason` is a stable name for the rule that refused
/// it (asserted by the tests, logged by the handler); `detail` is the message
/// the client shows, and carries every number the user needs to fix the
/// request — the memory guard's estimate above all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComposeRejection {
    pub(crate) reason: &'static str,
    pub(crate) detail: String,
}

impl ComposeRejection {
    fn new(reason: &'static str, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

/// The admission limits, read off `[transcode]` by the caller so this module
/// stays a pure function of its inputs (and testable at limits no config would
/// ever be set to).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ComposeLimits {
    pub(crate) max_mosaic_inputs: usize,
    pub(crate) max_mosaic_loop_mb: u64,
    pub(crate) max_animated_image_seconds: u64,
    pub(crate) max_output_seconds: u64,
}

impl ComposeLimits {
    pub(crate) fn from_config() -> Self {
        let transcode = &crate::config::runtime().transcode;
        Self {
            max_mosaic_inputs: transcode.max_mosaic_inputs,
            max_mosaic_loop_mb: transcode.max_mosaic_loop_mb,
            max_animated_image_seconds: transcode.max_animated_image_seconds,
            max_output_seconds: transcode.max_output_seconds,
        }
    }
}

type Resolved<T> = Result<T, ComposeRejection>;

/// Validates a composition and resolves everything the filtergraph reads.
///
/// Source rectangles are deliberately *not* rejected against the real stream
/// bounds here: the stream's dimensions are not known until the job runs (a
/// probe of every input on the request path would be paid even by a cache
/// hit), so an out-of-bounds source rectangle is clamped at graph-build time
/// instead — see [`build_filtergraph`]. Everything else is decided here,
/// before a job exists.
pub(crate) fn resolve_compose(
    request: &ComposeRequest,
    preset: &ResolvedPreset,
    limits: ComposeLimits,
) -> Resolved<ResolvedCompose> {
    if request.items.is_empty() {
        return Err(ComposeRejection::new(
            "no_items",
            "a composition needs at least one item",
        ));
    }
    if request.items.len() > limits.max_mosaic_inputs {
        return Err(ComposeRejection::new(
            "too_many_items",
            format!(
                "a composition may hold at most {} items; this one has {}",
                limits.max_mosaic_inputs,
                request.items.len()
            ),
        ));
    }

    let canvas = &request.canvas;
    validate_canvas(canvas, preset)?;
    let background = normalize_color(&canvas.background).ok_or_else(|| {
        ComposeRejection::new(
            "bad_background",
            "background must be a hex colour: #RRGGBB, #RRGGBBAA, or the 0x spelling of either",
        )
    })?;

    let fps = resolve_fps(request.fps, preset)?;
    let items = resolve_items(&request.items, canvas, preset)?;
    let target_cs = resolve_target_cs(&request.output.length, &items, preset, limits)?;

    let doc = ResolvedCompose {
        canvas_w: canvas.w,
        canvas_h: canvas.h,
        background,
        fps,
        target_cs,
        items,
    };
    check_loop_memory(&doc, limits)?;
    Ok(doc)
}

fn validate_canvas(canvas: &Canvas, preset: &ResolvedPreset) -> Resolved<()> {
    if canvas.w < MIN_CANVAS_SIDE || canvas.h < MIN_CANVAS_SIDE {
        return Err(ComposeRejection::new(
            "canvas_too_small",
            format!("the canvas must be at least {MIN_CANVAS_SIDE}x{MIN_CANVAS_SIDE}"),
        ));
    }
    if canvas.w % 2 != 0 || canvas.h % 2 != 0 {
        // Both output formats are 4:2:0, which has no odd sizes; rounding one
        // here would move every destination rectangle by half a pixel.
        return Err(ComposeRejection::new(
            "canvas_odd",
            "the canvas width and height must both be even",
        ));
    }
    if canvas.w > MAX_CANVAS_SIDE || canvas.h > MAX_CANVAS_SIDE {
        return Err(ComposeRejection::new(
            "canvas_too_large",
            format!("neither canvas side may exceed {MAX_CANVAS_SIDE} px"),
        ));
    }
    if canvas.w.saturating_mul(canvas.h) > MAX_CANVAS_AREA {
        return Err(ComposeRejection::new(
            "canvas_too_large",
            format!(
                "the canvas may cover at most {MAX_CANVAS_AREA} px; this one covers {}",
                canvas.w.saturating_mul(canvas.h)
            ),
        ));
    }
    // The preset's own height cap is a property of the rendition, so a canvas
    // over it is refused rather than silently rescaled: rescaling would move
    // every destination rectangle the client placed to the pixel.
    if let Some(max_height) = preset.max_height
        && canvas.h > max_height
    {
        return Err(ComposeRejection::new(
            "canvas_over_preset_height",
            format!(
                "preset '{}' renders at most {max_height} px tall; this canvas is {} px",
                preset.id, canvas.h
            ),
        ));
    }
    Ok(())
}

fn resolve_fps(fps: u32, preset: &ResolvedPreset) -> Resolved<u32> {
    if fps == 0 || fps > MAX_COMPOSE_FPS {
        return Err(ComposeRejection::new(
            "fps_out_of_range",
            format!("fps must be between 1 and {MAX_COMPOSE_FPS}"),
        ));
    }
    // The preset's cap is applied silently: it is not the client's number, and
    // the resolved value rides in the cache key either way.
    let capped = match preset.fps_max {
        Some(max) if max.is_finite() && max >= 1.0 => fps.min(max.floor() as u32),
        _ => fps,
    };
    Ok(capped.max(1))
}

fn resolve_items(
    items: &[ComposeItem],
    canvas: &Canvas,
    preset: &ResolvedPreset,
) -> Resolved<Vec<ComposeItem>> {
    let carries_audio = preset.acodec.is_some();
    items
        .iter()
        .map(|item| {
            validate_item(item, canvas)?;
            let audio = item.audio && carries_audio && matches!(item.time, ItemTime::Span { .. });
            Ok(ComposeItem {
                audio,
                ..item.clone()
            })
        })
        .collect()
}

fn validate_item(item: &ComposeItem, canvas: &Canvas) -> Resolved<()> {
    if item.transform.quarter_turns > 3 {
        return Err(ComposeRejection::new(
            "bad_transform",
            "quarter_turns must be 0, 1, 2 or 3",
        ));
    }
    if item.src.w <= 0 || item.src.h <= 0 || item.src.x < 0 || item.src.y < 0 {
        return Err(ComposeRejection::new(
            "src_empty",
            "every source rectangle must have a positive size and a non-negative origin",
        ));
    }
    let dest = item.dest;
    if dest.w <= 0 || dest.h <= 0 {
        return Err(ComposeRejection::new(
            "dest_empty",
            "every destination rectangle must have a positive size",
        ));
    }
    if dest.x < 0
        || dest.y < 0
        || dest.x.saturating_add(dest.w) > canvas.w
        || dest.y.saturating_add(dest.h) > canvas.h
    {
        return Err(ComposeRejection::new(
            "dest_outside_canvas",
            format!(
                "a destination rectangle ({}x{} at {},{}) falls outside the {}x{} canvas",
                dest.w, dest.h, dest.x, dest.y, canvas.w, canvas.h
            ),
        ));
    }
    if dest.x % 2 != 0 || dest.y % 2 != 0 || dest.w % 2 != 0 || dest.h % 2 != 0 {
        // `overlay` refuses odd offsets in 4:2:0, and an odd-sized item has no
        // chroma plane to place: both are the same restriction the even canvas
        // carries, one level down.
        return Err(ComposeRejection::new(
            "dest_not_even",
            format!(
                "destination rectangles must be even in both position and size; \
                 got {}x{} at {},{}",
                dest.w, dest.h, dest.x, dest.y
            ),
        ));
    }
    match item.time {
        ItemTime::Span { start_cs, end_cs } => {
            if start_cs < 0 || end_cs < 0 {
                return Err(ComposeRejection::new(
                    "span_negative",
                    "span bounds must not be negative",
                ));
            }
            if end_cs <= start_cs {
                // Equal bounds are a still spelled as a range, which the
                // client normalizes before sending: reaching here is a client
                // bug, and rendering it as a zero-length span would produce an
                // empty file rather than the frozen frame that was meant.
                return Err(ComposeRejection::new(
                    "span_not_a_clip",
                    "a span needs end_cs strictly after start_cs; send a still for a frozen frame",
                ));
            }
        }
        ItemTime::Still { at_cs } => {
            if at_cs < 0 {
                return Err(ComposeRejection::new(
                    "still_negative",
                    "a still's timestamp must not be negative",
                ));
            }
        }
        ItemTime::Image => {}
    }
    Ok(())
}

/// The output length, in centiseconds, clamped to the container's cap.
fn resolve_target_cs(
    length: &ComposeLength,
    items: &[ComposeItem],
    preset: &ResolvedPreset,
    limits: ComposeLimits,
) -> Resolved<i64> {
    let requested = match *length {
        ComposeLength::Cap { seconds } => {
            if !seconds.is_finite() || seconds <= 0.0 {
                return Err(ComposeRejection::new(
                    "cap_invalid",
                    "the length cap must be a positive number of seconds",
                ));
            }
            // Bounded before the cast: an `f64` past `i64::MAX` saturates in
            // Rust, but the multiplication below would still be nonsense.
            (seconds.min(limits.max_output_seconds as f64) * 100.0).round() as i64
        }
        ComposeLength::LongestLoopOnce => longest_span_cs(items).unwrap_or(STILLS_ONLY_TARGET_CS),
    };
    let cap_seconds = match preset.container {
        Container::Webp => limits.max_animated_image_seconds,
        Container::Mp4 | Container::Webm => limits.max_output_seconds,
    };
    let cap_cs = (cap_seconds as i64).saturating_mul(100);
    Ok(requested.clamp(1, cap_cs.max(1)))
}

/// The longest playing span, or `None` when nothing plays.
fn longest_span_cs(items: &[ComposeItem]) -> Option<i64> {
    items
        .iter()
        .filter_map(|item| match item.time {
            ItemTime::Span { start_cs, end_cs } => Some(end_cs - start_cs),
            _ => None,
        })
        .max()
}

/// Frames of `cs` centiseconds at `fps`, rounded up: a loop buffer must cover
/// the whole segment.
///
/// Saturating, because a span bound is an attacker-supplied `i64`: an `end_cs`
/// near the top of the range times a frame rate overflows, which in a release
/// build wraps to a negative frame count and waves the memory guard through.
fn frames_for(cs: i64, fps: u32) -> i64 {
    let fps = i64::from(fps.max(1));
    cs.max(0).saturating_mul(fps).saturating_add(99) / 100
}

/// How many *extra* passes an item needs to fill the target, and how many
/// frames each pass buffers. `None` means the item plays through without a
/// loop filter at all — the longest span, and anything already at least as
/// long as the target.
fn span_loop(span_cs: i64, doc: &ResolvedCompose) -> Option<(i64, i64)> {
    let segment = frames_for(span_cs, doc.fps).max(1);
    let target = frames_for(doc.target_cs, doc.fps).max(1);
    if segment >= target {
        return None;
    }
    let passes = (target + segment - 1) / segment;
    Some((passes - 1, segment))
}

/// The admission-time memory guard: every loop buffer holds decoded frames at
/// *destination* resolution (the loop filter sits after the scale, which is
/// what makes this computable at all), so the whole composition's buffered
/// footprint is arithmetic over the document.
fn check_loop_memory(doc: &ResolvedCompose, limits: ComposeLimits) -> Resolved<()> {
    let mut bytes: u128 = 0;
    for item in &doc.items {
        let frames: u128 = match item.time {
            ItemTime::Span { start_cs, end_cs } => match span_loop(end_cs - start_cs, doc) {
                Some((_, segment)) => segment.max(0) as u128,
                None => 0,
            },
            // One buffered frame, held by the infinite loop that freezes it.
            ItemTime::Still { .. } => 1,
            // Looped by the demuxer (`-loop 1`), which buffers nothing.
            ItemTime::Image => 0,
        };
        let pixels = (item.dest.w.max(0) as u128) * (item.dest.h.max(0) as u128);
        bytes = bytes.saturating_add(
            frames
                .saturating_mul(pixels)
                .saturating_mul(LOOP_BYTES_PER_PIXEL_NUM)
                / LOOP_BYTES_PER_PIXEL_DEN,
        );
    }
    let budget = (limits.max_mosaic_loop_mb as u128).saturating_mul(BYTES_PER_MB);
    if bytes > budget {
        return Err(ComposeRejection::new(
            "loop_memory",
            format!(
                "this composition would buffer about {} MB of looped frames, over the \
                 {} MB limit: shorten the output, drop an item, or make the items smaller",
                bytes / BYTES_PER_MB,
                limits.max_mosaic_loop_mb
            ),
        ));
    }
    Ok(())
}

/// `#RRGGBB` / `#RRGGBBAA` / `0x...` to ffmpeg's own `0xRRGGBB[AA]`.
///
/// A whitelist, not a sanitizer: the value is interpolated into a
/// `filter_complex` argument, where `:` and `,` are structure. Named colours
/// are deliberately not accepted — the client sends hex, and every name ffmpeg
/// knows has a hex spelling.
fn normalize_color(value: &str) -> Option<String> {
    let digits = value
        .strip_prefix('#')
        .or_else(|| value.strip_prefix("0x"))
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    if !matches!(digits.len(), 6 | 8) || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("0x{}", digits.to_ascii_lowercase()))
}

// --- source probing --------------------------------------------------------

/// What one input's streams look like, as the graph builder needs them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamInfo {
    /// Display dimensions: the rotation metadata is applied, because ffmpeg
    /// auto-rotates on decode and the client's source rectangle is measured
    /// against the browser's (equally auto-rotated) natural size.
    pub(crate) width: i64,
    pub(crate) height: i64,
    /// Index *among video streams* of the one that carries pictures. A file
    /// whose first video stream is cover art would otherwise compose its album
    /// art (the Phase 2 `content_video_stream` rule, one layer down).
    pub(crate) video_index: usize,
    pub(crate) has_audio: bool,
}

/// One input of a composition: the file, and what a probe found in it.
#[derive(Debug, Clone)]
pub(crate) struct ComposeSource {
    pub(crate) path: PathBuf,
    /// `None` when the probe failed or was skipped. The graph is still built:
    /// the probe only clamps a source rectangle and drops audio that does not
    /// exist, and ffmpeg's own verdict is the backstop for everything else.
    pub(crate) probe: Option<StreamInfo>,
}

/// ffprobe, for the two things a composition cannot ask the index database
/// for: the exact stream geometry a crop must fit inside, and whether an item
/// the client marked audible has an audio stream at all (a `[i:a:0]` label
/// that matches nothing fails the whole graph).
///
/// Blocking, and best-effort: a failure returns `None` rather than an error,
/// because none of what it learns is required to build a graph.
pub(crate) fn probe_source(path: &Path) -> Option<StreamInfo> {
    let output = Command::new(crate::media_tools::ffprobe())
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(path)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_probe(&output.stdout)
}

/// Pure half of [`probe_source`], so the stream selection has tests that need
/// no toolchain.
fn parse_probe(stdout: &[u8]) -> Option<StreamInfo> {
    let data: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    let streams = data.get("streams")?.as_array()?;
    let kind = |stream: &serde_json::Value| {
        stream
            .get("codec_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let has_audio = streams.iter().any(|stream| kind(stream) == "audio");
    let video: Vec<&serde_json::Value> = streams
        .iter()
        .filter(|stream| kind(stream) == "video")
        .collect();
    // The first stream carrying pictures, which is not always the first video
    // stream: an audio file's cover art is one too.
    let (video_index, stream) = video
        .iter()
        .enumerate()
        .find(|(_, stream)| !is_cover(stream))
        .or_else(|| video.iter().enumerate().next())
        .map(|(index, stream)| (index, *stream))?;
    let width = stream.get("width").and_then(serde_json::Value::as_i64)?;
    let height = stream.get("height").and_then(serde_json::Value::as_i64)?;
    let quarter = stream
        .get("side_data_list")
        .and_then(serde_json::Value::as_array)
        .and_then(|list| {
            list.iter()
                .find_map(|entry| entry.get("rotation").and_then(serde_json::Value::as_f64))
        })
        .map(|rotation| (rotation.abs().round() as i64).rem_euclid(360))
        .unwrap_or(0);
    let (width, height) = if quarter == 90 || quarter == 270 {
        (height, width)
    } else {
        (width, height)
    };
    Some(StreamInfo {
        width,
        height,
        video_index,
        has_audio,
    })
}

fn is_cover(stream: &serde_json::Value) -> bool {
    stream
        .get("disposition")
        .and_then(|disposition| disposition.get("attached_pic"))
        .and_then(serde_json::Value::as_i64)
        == Some(1)
}

// --- filtergraph -----------------------------------------------------------

/// One `-i` of a composition, with the input options that belong to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputSpec {
    /// Options that must precede this `-i` (`-ss`, `-to`, `-loop`, ...).
    pub(crate) args: Vec<String>,
    pub(crate) path: PathBuf,
}

/// A composition's whole ffmpeg shape, minus the encoder settings the preset
/// decides. Pure output of [`build_filtergraph`], so it is golden-tested as a
/// string rather than inferred from an encode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilterPlan {
    pub(crate) inputs: Vec<InputSpec>,
    pub(crate) filter_complex: String,
    /// Stream mapping and the pixel format, in the order they are passed.
    pub(crate) output_args: Vec<String>,
    /// Whether the graph produced an audio stream to encode.
    pub(crate) has_audio: bool,
}

/// Builds the filtergraph for a resolved composition.
///
/// `sources` is parallel to `params.doc.items`; a shorter list is a caller bug
/// and is treated as "no probe" for the missing entries rather than panicking
/// inside a job.
pub(crate) fn build_filtergraph(params: &ComposeParams, sources: &[ComposeSource]) -> FilterPlan {
    let doc = &params.doc;
    let container = params.preset.container;
    let target = seconds(doc.target_cs);

    let mut inputs = Vec::with_capacity(doc.items.len());
    let mut chains: Vec<String> = Vec::new();
    let mut audio_labels: Vec<String> = Vec::new();

    for (index, item) in doc.items.iter().enumerate() {
        let source = sources.get(index);
        let probe = source.and_then(|source| source.probe);
        inputs.push(InputSpec {
            args: input_args(item, doc.fps),
            path: source
                .map(|source| source.path.clone())
                .unwrap_or_default(),
        });
        chains.push(video_chain(index, item, doc, probe, &target));
        if item.audio && probe.is_none_or(|probe| probe.has_audio) {
            let label = format!("a{index}");
            chains.push(audio_chain(index, item, doc, &target, &label));
            audio_labels.push(label);
        }
    }

    let format = match container {
        // Animated WebP keeps an alpha plane: the canvas background may be
        // translucent, and a yuv420p output would bake it onto black.
        Container::Webp => "yuva420p",
        Container::Mp4 | Container::Webm => "yuv420p",
    };
    let mut tail = Vec::new();
    if let Some(max_height) = params.preset.max_height {
        // The quotes are ffmpeg's escaping, not a shell's: the comma inside
        // `min()` would otherwise separate two filters.
        tail.push(format!("scale=-2:'min(ih,{max_height})'"));
    }
    tail.push(format!("format={format}"));

    // A single item covering the whole canvas has nothing to overlay onto:
    // its own chain *is* the output, which saves a full-canvas source and a
    // compositing pass on the commonest save of all.
    let full_canvas = doc.items.len() == 1
        && doc.items[0].dest
            == Rect {
                x: 0,
                y: 0,
                w: doc.canvas_w,
                h: doc.canvas_h,
            };
    let mut graph = String::new();
    if full_canvas {
        let chain = chains.remove(0);
        let chain = chain
            .strip_suffix("[v0]")
            .expect("the video chain ends with its label")
            .to_string();
        graph.push_str(&format!("{chain},{}[vout]", tail.join(",")));
        for extra in &chains {
            graph.push(';');
            graph.push_str(extra);
        }
    } else {
        for chain in &chains {
            graph.push_str(chain);
            graph.push(';');
        }
        graph.push_str(&format!(
            "color=c={}:s={}x{}:r={}:d={target}[base]",
            doc.background, doc.canvas_w, doc.canvas_h, doc.fps
        ));
        let mut current = "base".to_string();
        for (index, item) in doc.items.iter().enumerate() {
            let last = index + 1 == doc.items.len();
            let next = if last {
                String::new()
            } else {
                format!("o{index}")
            };
            graph.push_str(&format!(
                ";[{current}][v{index}]overlay={}:{}",
                item.dest.x, item.dest.y
            ));
            if last {
                graph.push_str(&format!(",{}[vout]", tail.join(",")));
            } else {
                graph.push_str(&format!("[{next}]"));
            }
            current = next;
        }
    }

    let has_audio = !audio_labels.is_empty();
    if has_audio {
        graph.push(';');
        for label in &audio_labels {
            graph.push_str(&format!("[{label}]"));
        }
        graph.push_str(&format!(
            "amix=inputs={}:duration=longest:dropout_transition=0[aout]",
            audio_labels.len()
        ));
    }

    let mut output_args = vec!["-map".to_string(), "[vout]".to_string()];
    if has_audio {
        output_args.extend(["-map".to_string(), "[aout]".to_string()]);
    } else {
        output_args.push("-an".to_string());
    }
    output_args.extend([
        "-sn".to_string(),
        "-dn".to_string(),
        "-pix_fmt".to_string(),
        format.to_string(),
    ]);

    FilterPlan {
        inputs,
        filter_complex: graph,
        output_args,
        has_audio,
    }
}

/// The input options an item needs before its `-i`.
fn input_args(item: &ComposeItem, fps: u32) -> Vec<String> {
    match item.time {
        // Input seeking, and `-to` in the input's own timeline: the decoder
        // skips everything outside the span instead of decoding and throwing
        // it away, and we always re-encode, so the fast seek is exact.
        ItemTime::Span { start_cs, end_cs } => vec![
            "-ss".to_string(),
            seconds(start_cs),
            "-to".to_string(),
            seconds(end_cs),
        ],
        ItemTime::Still { at_cs } => vec!["-ss".to_string(), seconds(at_cs)],
        // A still image has one frame and no rate; the demuxer supplies both.
        ItemTime::Image => vec![
            "-loop".to_string(),
            "1".to_string(),
            "-framerate".to_string(),
            fps.to_string(),
        ],
    }
}

fn video_chain(
    index: usize,
    item: &ComposeItem,
    doc: &ResolvedCompose,
    probe: Option<StreamInfo>,
    target: &str,
) -> String {
    let video_index = probe.map(|probe| probe.video_index).unwrap_or(0);
    let src = clamped_src(item.src, probe);
    let geometry = {
        let mut filters = vec![format!("crop={}:{}:{}:{}", src.w, src.h, src.x, src.y)];
        filters.extend(orientation_filters(item.transform).into_iter().map(str::to_string));
        filters.push(format!(
            "scale={}:{}:flags=lanczos",
            item.dest.w, item.dest.h
        ));
        filters
    };

    let mut filters: Vec<String> = Vec::new();
    match item.time {
        ItemTime::Span { start_cs, end_cs } => {
            filters.push("setpts=PTS-STARTPTS".to_string());
            filters.extend(geometry);
            filters.push(format!("fps={}", doc.fps));
            if let Some((loops, size)) = span_loop(end_cs - start_cs, doc) {
                filters.push(format!("loop={loops}:size={size}"));
            }
            filters.push(format!("trim=end={target}"));
            filters.push("setpts=PTS-STARTPTS".to_string());
        }
        ItemTime::Still { .. } => {
            // The seek landed on the frame; one frame is taken, frozen by an
            // infinite loop, and given timestamps at the output rate.
            filters.push("trim=end_frame=1".to_string());
            filters.extend(geometry);
            filters.push("loop=-1:size=1".to_string());
            filters.push(format!("setpts=N/({}*TB)", doc.fps));
            filters.push(format!("trim=end={target}"));
        }
        ItemTime::Image => {
            filters.extend(geometry);
            filters.push(format!("trim=end={target}"));
            filters.push("setpts=PTS-STARTPTS".to_string());
        }
    }
    format!(
        "[{index}:v:{video_index}]{}[v{index}]",
        filters.join(",")
    )
}

fn audio_chain(
    index: usize,
    item: &ComposeItem,
    doc: &ResolvedCompose,
    target: &str,
    label: &str,
) -> String {
    let mut filters = vec!["asetpts=PTS-STARTPTS".to_string()];
    if let ItemTime::Span { start_cs, end_cs } = item.time
        && let Some((loops, _)) = span_loop(end_cs - start_cs, doc)
    {
        // `size` is a maximum, so the segment's own end is what bounds the
        // buffer; the rate only has to be one nothing exceeds.
        let samples = (end_cs - start_cs).max(0).saturating_mul(LOOP_AUDIO_RATE) / 100;
        filters.push(format!("aloop={loops}:size={samples}"));
    }
    filters.push(format!("atrim=end={target}"));
    filters.push("aresample=async=1".to_string());
    format!("[{index}:a:0]{}[{label}]", filters.join(","))
}

/// The crop, held inside the stream it crops. The client computes this against
/// the natural size the browser reported, so a mismatch means the file behind
/// the hash was replaced or the browser and ffmpeg disagree about rotation —
/// either way a crop past the edge fails the encode, and a clamp does not.
fn clamped_src(src: Rect, probe: Option<StreamInfo>) -> Rect {
    let Some(probe) = probe else {
        return src;
    };
    if probe.width <= 0 || probe.height <= 0 {
        return src;
    }
    let x = src.x.clamp(0, probe.width - 1);
    let y = src.y.clamp(0, probe.height - 1);
    Rect {
        x,
        y,
        w: src.w.clamp(1, probe.width - x),
        h: src.h.clamp(1, probe.height - y),
    }
}

/// The D4 table (display = flipH^f ∘ rotCW^q), applied to a frame that is
/// already cropped in source space. Deliberately not collapsed into fewer
/// filters: `transpose` plus an explicit `hflip` is what each of the eight
/// cases means, and a clever identity here is a bug nobody can read.
fn orientation_filters(transform: Transform) -> Vec<&'static str> {
    let mut filters = Vec::new();
    match transform.quarter_turns {
        1 => filters.push("transpose=1"),
        2 => filters.extend(["hflip", "vflip"]),
        3 => filters.push("transpose=2"),
        _ => {}
    }
    if transform.flip_h {
        filters.push("hflip");
    }
    filters
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_tools::transcode::presets::{builtin_presets, find_preset};

    /// See [`compose_params_hash_is_stable`].
    const PINNED_COMPOSE_PARAMS_HASH: &str = "935d70a87811b1031c8df4434fd46be4";

    fn preset(id: &str) -> ResolvedPreset {
        find_preset(&builtin_presets(), id)
            .unwrap_or_else(|| panic!("{id} ships"))
            .clone()
    }

    fn limits() -> ComposeLimits {
        ComposeLimits {
            max_mosaic_inputs: 12,
            max_mosaic_loop_mb: 512,
            max_animated_image_seconds: 30,
            max_output_seconds: 300,
        }
    }

    fn rect(x: i64, y: i64, w: i64, h: i64) -> Rect {
        Rect { x, y, w, h }
    }

    fn item(time: ItemTime) -> ComposeItem {
        ComposeItem {
            sha256: "a".repeat(64),
            src: rect(0, 0, 640, 480),
            transform: Transform::default(),
            dest: rect(0, 0, 320, 240),
            time,
            audio: false,
        }
    }

    fn request(items: Vec<ComposeItem>, length: ComposeLength) -> ComposeRequest {
        ComposeRequest {
            canvas: Canvas {
                w: 640,
                h: 480,
                background: "#101820".to_string(),
            },
            fps: 25,
            output: ComposeOutput {
                preset: "mosaic-mp4".to_string(),
                length,
            },
            items,
        }
    }

    fn resolved(request: &ComposeRequest, preset_id: &str) -> ResolvedCompose {
        resolve_compose(request, &preset(preset_id), limits()).expect("a valid document")
    }

    fn params_for(request: &ComposeRequest, preset_id: &str) -> ComposeParams {
        ComposeParams::new(
            resolved(request, preset_id),
            preset(preset_id),
            super::super::run::ENCODER_X264_QUALITY.to_string(),
        )
    }

    fn sources(count: usize) -> Vec<ComposeSource> {
        (0..count)
            .map(|index| ComposeSource {
                path: PathBuf::from(format!("item{index}.mp4")),
                probe: None,
            })
            .collect()
    }

    /// The two items of the worked example in
    /// `docs/video-transcoding-implementation.md` §4 C2: a rotated, cropped,
    /// audible span on the left, a frozen frame on the right.
    fn worked_example() -> ComposeRequest {
        let span = ComposeItem {
            sha256: "a".repeat(64),
            src: rect(0, 0, 1080, 1920),
            transform: Transform {
                quarter_turns: 1,
                flip_h: false,
            },
            dest: rect(0, 0, 320, 480),
            time: ItemTime::Span {
                start_cs: 200,
                end_cs: 1000,
            },
            audio: true,
        };
        let still = ComposeItem {
            sha256: "b".repeat(64),
            src: rect(100, 50, 400, 300),
            transform: Transform::default(),
            dest: rect(320, 120, 320, 240),
            time: ItemTime::Still { at_cs: 150 },
            audio: false,
        };
        request(vec![span, still], ComposeLength::LongestLoopOnce)
    }

    /// PINNED FIXTURE, for the same reason as its single-file twin: this hash
    /// is the identity of every composition on every user's disk. A failure
    /// here means the document shape or its serialization moved, which is
    /// allowed — and is exactly the change that requires bumping
    /// `TRANSCODER_VERSION` and re-pinning this constant.
    #[test]
    fn compose_params_hash_is_stable() {
        assert_eq!(TRANSCODER_VERSION, 1, "re-pin the fixture below on a bump");
        let params = params_for(&worked_example(), "mosaic-mp4");
        assert_eq!(
            serde_json::to_string(&params.doc).unwrap(),
            r#"{"canvas_w":640,"canvas_h":480,"background":"0x101820","fps":25,"target_cs":800,"items":[{"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","src":{"x":0,"y":0,"w":1080,"h":1920},"transform":{"quarter_turns":1,"flip_h":false},"dest":{"x":0,"y":0,"w":320,"h":480},"time":{"kind":"span","start_cs":200,"end_cs":1000},"audio":true},{"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","src":{"x":100,"y":50,"w":400,"h":300},"transform":{"quarter_turns":0,"flip_h":false},"dest":{"x":320,"y":120,"w":320,"h":240},"time":{"kind":"still","at_cs":150},"audio":false}]}"#
        );
        assert_eq!(params.params_hash(), PINNED_COMPOSE_PARAMS_HASH);
        assert_eq!(
            params.cache_key(),
            format!("compose-{PINNED_COMPOSE_PARAMS_HASH}")
        );
        assert!(is_compose_key(&params.cache_key()));
        assert!(!is_compose_key("abcdef-0011"));
        assert_eq!(
            params.artifact_file_name(),
            format!("{}.mp4", params.cache_key())
        );
        assert_eq!(params.mime_type(), "video/mp4");
        assert_eq!(params.download_file_name(), "mosaic-2items.mp4");
        assert_eq!(params.target_seconds(), 8.0);
    }

    /// Everything that decides the output bytes re-keys, and everything the
    /// resolution *normalized away* does not: two documents that differ only
    /// by a spelling of the same colour are one artifact.
    #[test]
    fn the_key_covers_the_resolved_document() {
        let baseline = params_for(&worked_example(), "mosaic-mp4").cache_key();

        let mut same = worked_example();
        same.canvas.background = "0X101820".to_string();
        assert_eq!(params_for(&same, "mosaic-mp4").cache_key(), baseline);

        let mut moved = worked_example();
        moved.items[1].dest.x = 318;
        assert_ne!(params_for(&moved, "mosaic-mp4").cache_key(), baseline);

        let mut slower = worked_example();
        slower.fps = 24;
        assert_ne!(params_for(&slower, "mosaic-mp4").cache_key(), baseline);

        let mut capped = worked_example();
        capped.output.length = ComposeLength::Cap { seconds: 8.0 };
        assert_eq!(
            resolved(&capped, "mosaic-mp4").target_cs,
            resolved(&worked_example(), "mosaic-mp4").target_cs,
            "the two ways of asking for eight seconds resolve identically"
        );
        assert_eq!(params_for(&capped, "mosaic-mp4").cache_key(), baseline);

        // A different preset is a different artifact, and so is a different
        // encoder on the same preset.
        assert_ne!(
            params_for(&worked_example(), "mosaic-mp4-fast").cache_key(),
            baseline
        );
        let hardware = ComposeParams::new(
            resolved(&worked_example(), "mosaic-mp4"),
            preset("mosaic-mp4"),
            "h264_nvenc".to_string(),
        );
        assert_ne!(hardware.cache_key(), baseline);
    }

    /// The admission table: every rule that refuses a document has a name, and
    /// the message says what to change.
    #[test]
    fn every_refused_document_is_refused_by_name() {
        let reason = |request: &ComposeRequest, preset_id: &str| {
            resolve_compose(request, &preset(preset_id), limits())
                .expect_err("refused")
                .reason
        };

        assert_eq!(
            reason(&request(Vec::new(), ComposeLength::LongestLoopOnce), "mosaic-mp4"),
            "no_items"
        );
        let many: Vec<ComposeItem> = (0..13).map(|_| item(ItemTime::Image)).collect();
        assert_eq!(
            reason(&request(many, ComposeLength::LongestLoopOnce), "mosaic-mp4"),
            "too_many_items"
        );

        let canvas = |w: i64, h: i64| {
            let mut request = request(vec![item(ItemTime::Image)], ComposeLength::LongestLoopOnce);
            request.canvas.w = w;
            request.canvas.h = h;
            request
        };
        assert_eq!(reason(&canvas(8, 480), "mosaic-mp4"), "canvas_too_small");
        assert_eq!(reason(&canvas(641, 480), "mosaic-mp4"), "canvas_odd");
        assert_eq!(reason(&canvas(4098, 480), "mosaic-mp4"), "canvas_too_large");
        assert_eq!(reason(&canvas(4096, 4096), "mosaic-mp4"), "canvas_too_large");
        // The preset's own height cap is refused rather than silently
        // rescaled: rescaling would move every rectangle the client placed.
        assert_eq!(
            reason(&canvas(640, 800), "webp-anim"),
            "canvas_over_preset_height"
        );

        let mut bad_colour = request(vec![item(ItemTime::Image)], ComposeLength::LongestLoopOnce);
        // Not a sanitizer's near-miss: the value is interpolated into a filter
        // argument, where `:` and `,` are structure.
        bad_colour.canvas.background = "black:s=1x1,nullsrc".to_string();
        assert_eq!(reason(&bad_colour, "mosaic-mp4"), "bad_background");

        let with_item = |item: ComposeItem| request(vec![item], ComposeLength::LongestLoopOnce);
        let mut turned = item(ItemTime::Image);
        turned.transform.quarter_turns = 4;
        assert_eq!(reason(&with_item(turned), "mosaic-mp4"), "bad_transform");

        let mut empty_src = item(ItemTime::Image);
        empty_src.src.w = 0;
        assert_eq!(reason(&with_item(empty_src), "mosaic-mp4"), "src_empty");

        let mut empty_dest = item(ItemTime::Image);
        empty_dest.dest.h = 0;
        assert_eq!(reason(&with_item(empty_dest), "mosaic-mp4"), "dest_empty");

        let mut outside = item(ItemTime::Image);
        outside.dest = rect(400, 0, 320, 240);
        assert_eq!(
            reason(&with_item(outside), "mosaic-mp4"),
            "dest_outside_canvas"
        );

        let mut odd = item(ItemTime::Image);
        odd.dest = rect(1, 0, 320, 240);
        assert_eq!(reason(&with_item(odd), "mosaic-mp4"), "dest_not_even");

        assert_eq!(
            reason(
                &with_item(item(ItemTime::Span {
                    start_cs: 500,
                    end_cs: 500
                })),
                "mosaic-mp4"
            ),
            "span_not_a_clip",
            "equal bounds are a still spelled as a range: the client normalizes it"
        );
        assert_eq!(
            reason(
                &with_item(item(ItemTime::Span {
                    start_cs: 500,
                    end_cs: 499
                })),
                "mosaic-mp4"
            ),
            "span_not_a_clip"
        );
        assert_eq!(
            reason(
                &with_item(item(ItemTime::Span {
                    start_cs: -1,
                    end_cs: 500
                })),
                "mosaic-mp4"
            ),
            "span_negative"
        );
        assert_eq!(
            reason(&with_item(item(ItemTime::Still { at_cs: -1 })), "mosaic-mp4"),
            "still_negative"
        );

        let mut fast = request(vec![item(ItemTime::Image)], ComposeLength::LongestLoopOnce);
        fast.fps = 61;
        assert_eq!(reason(&fast, "mosaic-mp4"), "fps_out_of_range");
        fast.fps = 0;
        assert_eq!(reason(&fast, "mosaic-mp4"), "fps_out_of_range");

        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                reason(
                    &request(
                        vec![item(ItemTime::Image)],
                        ComposeLength::Cap { seconds }
                    ),
                    "mosaic-mp4"
                ),
                "cap_invalid",
                "{seconds}"
            );
        }

        // And the document the whole table exists to admit.
        assert!(resolve_compose(&worked_example(), &preset("mosaic-mp4"), limits()).is_ok());
    }

    /// The resolved length: the longest span plays once, a cap says so
    /// outright, a document of nothing but stills still runs for something,
    /// and the container's own ceiling is the last word.
    #[test]
    fn the_output_length_resolves_then_clamps_to_the_container() {
        let target = |request: &ComposeRequest, preset_id: &str| {
            resolve_compose(request, &preset(preset_id), limits())
                .expect("valid")
                .target_cs
        };

        assert_eq!(target(&worked_example(), "mosaic-mp4"), 800);

        let spans = request(
            vec![
                item(ItemTime::Span {
                    start_cs: 0,
                    end_cs: 300,
                }),
                item(ItemTime::Span {
                    start_cs: 100,
                    end_cs: 1300,
                }),
            ],
            ComposeLength::LongestLoopOnce,
        );
        assert_eq!(target(&spans, "mosaic-mp4"), 1200, "the longest span");

        // Stills only: a frozen mosaic is a legitimate thing to ask for, so it
        // gets a short fixed clip rather than a rejection or a zero-length
        // file.
        let stills = request(
            vec![item(ItemTime::Still { at_cs: 0 }), item(ItemTime::Image)],
            ComposeLength::LongestLoopOnce,
        );
        assert_eq!(target(&stills, "mosaic-mp4"), STILLS_ONLY_TARGET_CS);

        let capped = request(
            vec![item(ItemTime::Span {
                start_cs: 0,
                end_cs: 6000,
            })],
            ComposeLength::Cap { seconds: 5.0 },
        );
        assert_eq!(target(&capped, "mosaic-mp4"), 500);

        // The container ceilings. An animated image is capped far below a
        // video, and both are silent clamps: the client already knows both
        // numbers from the presets envelope.
        let long = request(
            vec![item(ItemTime::Span {
                start_cs: 0,
                end_cs: 100_000,
            })],
            ComposeLength::LongestLoopOnce,
        );
        assert_eq!(target(&long, "mosaic-mp4"), 300 * 100);
        let mut small_canvas = long.clone();
        small_canvas.canvas.h = 480;
        assert_eq!(
            target(&small_canvas, "webp-anim"),
            30 * 100,
            "the animated-image cap, not the video one"
        );
        assert_eq!(target(&small_canvas, "mosaic-webm"), 300 * 100);
    }

    /// The frame-rate cap, which is the preset's and is applied silently: it
    /// is not the client's number, and the resolved value rides in the key.
    #[test]
    fn the_frame_rate_is_capped_by_the_preset() {
        let mut capped = preset("mosaic-mp4");
        capped.fps_max = Some(12.0);
        assert_eq!(resolve_fps(25, &capped).unwrap(), 12);
        assert_eq!(resolve_fps(10, &capped).unwrap(), 10);
        assert_eq!(resolve_fps(25, &preset("mosaic-mp4")).unwrap(), 25);
        // A cap below one frame per second would produce no frames at all.
        capped.fps_max = Some(0.5);
        assert_eq!(resolve_fps(25, &capped).unwrap(), 25);
    }

    /// The admission-time memory guard. Loop buffers hold decoded frames at
    /// *destination* resolution (the loop filter sits after the scale), so the
    /// footprint is arithmetic over the document — and the estimate is in the
    /// message, because the fix is to change one of the numbers it names.
    #[test]
    fn the_loop_memory_guard_counts_only_what_is_buffered() {
        let short_spans = |count: usize| {
            let items = (0..count)
                .map(|_| {
                    let mut item = item(ItemTime::Span {
                        start_cs: 0,
                        end_cs: 100,
                    });
                    item.dest = rect(0, 0, 640, 480);
                    item
                })
                .chain(std::iter::once({
                    // The longest span: it plays through, so it buffers
                    // nothing at all.
                    let mut item = item(ItemTime::Span {
                        start_cs: 0,
                        end_cs: 3000,
                    });
                    item.dest = rect(0, 0, 640, 480);
                    item
                }))
                .collect();
            request(items, ComposeLength::LongestLoopOnce)
        };

        // One second at 25 fps of 640x480 is 25 * 460 800 bytes ≈ 11 MB per
        // looping item; the longest span adds nothing.
        assert!(resolve_compose(&short_spans(1), &preset("mosaic-mp4"), limits()).is_ok());
        let tight = ComposeLimits {
            max_mosaic_loop_mb: 5,
            ..limits()
        };
        let refused = resolve_compose(&short_spans(1), &preset("mosaic-mp4"), tight)
            .expect_err("over the guard");
        assert_eq!(refused.reason, "loop_memory");
        assert!(refused.detail.contains("10 MB"), "{}", refused.detail);
        assert!(refused.detail.contains("5 MB"), "{}", refused.detail);

        // Stills buffer one frame each, images none at all (the demuxer loops
        // those), so neither can be what exhausts a host.
        let frozen = request(
            (0..12)
                .map(|index| {
                    let mut item = item(if index % 2 == 0 {
                        ItemTime::Still { at_cs: 0 }
                    } else {
                        ItemTime::Image
                    });
                    item.dest = rect(0, 0, 640, 480);
                    item
                })
                .collect(),
            ComposeLength::Cap { seconds: 30.0 },
        );
        assert!(
            resolve_compose(&frozen, &preset("mosaic-mp4"), ComposeLimits {
                max_mosaic_loop_mb: 4,
                ..limits()
            })
            .is_ok(),
            "six stills at 640x480 are under 2 MB together"
        );

        // Span bounds are attacker-supplied i64s: the frame arithmetic
        // saturates, because a wrapped (negative) frame count would wave the
        // whole guard through in a release build.
        assert_eq!(frames_for(i64::MAX, 60), i64::MAX / 100);
        assert_eq!(frames_for(-5, 25), 0);
        assert_eq!(frames_for(100, 25), 25);
        assert_eq!(frames_for(101, 25), 26, "rounded up: the buffer must cover");
        let extreme = request(
            vec![
                item(ItemTime::Span {
                    start_cs: 0,
                    end_cs: i64::MAX,
                }),
                item(ItemTime::Span {
                    start_cs: 0,
                    end_cs: 1,
                }),
            ],
            ComposeLength::LongestLoopOnce,
        );
        let doc = resolve_compose(&extreme, &preset("mosaic-mp4"), limits())
            .expect("clamped to the container cap");
        assert_eq!(doc.target_cs, 300 * 100);
    }

    /// The colour whitelist. Named colours are deliberately not accepted, and
    /// the reason is not tidiness: the value is interpolated into a filter
    /// argument.
    #[test]
    fn only_hex_colours_reach_a_filtergraph() {
        assert_eq!(normalize_color("#101820").as_deref(), Some("0x101820"));
        assert_eq!(normalize_color("0X101820").as_deref(), Some("0x101820"));
        assert_eq!(normalize_color("101820").as_deref(), Some("0x101820"));
        assert_eq!(normalize_color("#101820FF").as_deref(), Some("0x101820ff"));
        for rejected in [
            "black",
            "",
            "#10182",
            "#1018200",
            "#10182g",
            "0x101820:s=2x2",
            "red,nullsrc",
            "#101820[x]",
        ] {
            assert_eq!(normalize_color(rejected), None, "{rejected}");
        }
    }

    /// The D4 table, every one of its eight cases. Deliberately spelled out
    /// rather than collapsed: `transpose` plus an explicit `hflip` is what
    /// each case *means*.
    #[test]
    fn the_orientation_table_is_the_dihedral_group() {
        let filters = |quarter_turns, flip_h| {
            orientation_filters(Transform {
                quarter_turns,
                flip_h,
            })
        };
        assert!(filters(0, false).is_empty());
        assert_eq!(filters(1, false), ["transpose=1"]);
        assert_eq!(filters(2, false), ["hflip", "vflip"]);
        assert_eq!(filters(3, false), ["transpose=2"]);
        assert_eq!(filters(0, true), ["hflip"]);
        assert_eq!(filters(1, true), ["transpose=1", "hflip"]);
        assert_eq!(filters(2, true), ["hflip", "vflip", "hflip"]);
        assert_eq!(filters(3, true), ["transpose=2", "hflip"]);
    }

    /// GOLDEN. The worked example of `docs/video-transcoding-implementation.md`
    /// §4 C2, built from the document that documents it. Every piece of the
    /// design — the D4 chain after the crop, the still's freeze, the base
    /// canvas, the overlay fold in item order, the audio mix — is visible in
    /// one string, so a change to any of them shows up in a diff rather than
    /// in an exported file.
    #[test]
    fn the_worked_example_builds_the_documented_filtergraph() {
        let params = params_for(&worked_example(), "mosaic-mp4");
        let plan = build_filtergraph(&params, &sources(2));
        assert_eq!(
            plan.filter_complex,
            "[0:v:0]setpts=PTS-STARTPTS,crop=1080:1920:0:0,transpose=1,\
             scale=320:480:flags=lanczos,fps=25,trim=end=8.00,setpts=PTS-STARTPTS[v0];\
             [0:a:0]asetpts=PTS-STARTPTS,atrim=end=8.00,aresample=async=1[a0];\
             [1:v:0]trim=end_frame=1,crop=400:300:100:50,scale=320:240:flags=lanczos,\
             loop=-1:size=1,setpts=N/(25*TB),trim=end=8.00[v1];\
             color=c=0x101820:s=640x480:r=25:d=8.00[base];\
             [base][v0]overlay=0:0[o0];[o0][v1]overlay=320:120,format=yuv420p[vout];\
             [a0]amix=inputs=1:duration=longest:dropout_transition=0[aout]"
        );
        assert_eq!(
            plan.inputs[0].args,
            ["-ss", "2.00", "-to", "10.00"],
            "input seeking, and `-to` in the input's own timeline"
        );
        assert_eq!(plan.inputs[1].args, ["-ss", "1.50"]);
        assert_eq!(
            plan.output_args,
            ["-map", "[vout]", "-map", "[aout]", "-sn", "-dn", "-pix_fmt", "yuv420p"]
        );
        assert!(plan.has_audio);
    }

    /// The loop mechanism: a span shorter than the target repeats, and the
    /// buffer it repeats is `size` frames at *destination* resolution — the
    /// same number the admission guard counted. The longest span never loops.
    #[test]
    fn shorter_spans_loop_and_the_longest_one_does_not() {
        let short = ComposeItem {
            time: ItemTime::Span {
                start_cs: 0,
                end_cs: 250,
            },
            dest: rect(320, 0, 320, 240),
            audio: true,
            ..item(ItemTime::Image)
        };
        let long = ComposeItem {
            time: ItemTime::Span {
                start_cs: 0,
                end_cs: 1000,
            },
            audio: true,
            ..item(ItemTime::Image)
        };
        let params = params_for(
            &request(vec![short, long], ComposeLength::LongestLoopOnce),
            "mosaic-mp4",
        );
        let plan = build_filtergraph(&params, &sources(2));
        // 2.5 s at 25 fps is 63 buffered frames (rounded up, so the buffer
        // covers the whole segment); four passes fill the 10 s target, which
        // is three loops on top of the first.
        assert!(
            plan.filter_complex.contains("fps=25,loop=3:size=63,trim=end=10.00"),
            "{}",
            plan.filter_complex
        );
        assert!(
            plan.filter_complex.contains("aloop=3:size=480000"),
            "the audio repeats with its video: {}",
            plan.filter_complex
        );
        assert!(
            plan.filter_complex.contains("fps=25,trim=end=10.00"),
            "the longest span plays through with no loop filter at all: {}",
            plan.filter_complex
        );
        assert!(
            plan.filter_complex
                .contains("amix=inputs=2:duration=longest:dropout_transition=0[aout]")
        );
    }

    /// The single-item save: one item covering the whole canvas has nothing to
    /// composite onto, so it skips the base source and the overlay entirely.
    #[test]
    fn a_single_item_filling_the_canvas_skips_the_overlay() {
        let full = ComposeItem {
            dest: rect(0, 0, 640, 480),
            ..item(ItemTime::Span {
                start_cs: 0,
                end_cs: 500,
            })
        };
        let params = params_for(
            &request(vec![full.clone()], ComposeLength::LongestLoopOnce),
            "mosaic-mp4",
        );
        let plan = build_filtergraph(&params, &sources(1));
        assert_eq!(
            plan.filter_complex,
            "[0:v:0]setpts=PTS-STARTPTS,crop=640:480:0:0,scale=640:480:flags=lanczos,fps=25,\
             trim=end=5.00,setpts=PTS-STARTPTS,format=yuv420p[vout]"
        );
        assert!(!plan.filter_complex.contains("overlay"));
        assert!(!plan.filter_complex.contains("color=c="));
        assert_eq!(plan.output_args[2], "-an", "nothing to mix");

        // One item that does *not* fill the canvas still composites: the
        // background is part of what was asked for.
        let inset = ComposeItem {
            dest: rect(0, 0, 320, 240),
            ..full
        };
        let params = params_for(
            &request(vec![inset], ComposeLength::LongestLoopOnce),
            "mosaic-mp4",
        );
        let plan = build_filtergraph(&params, &sources(1));
        assert!(plan.filter_complex.contains("[base][v0]overlay=0:0,format=yuv420p[vout]"));
    }

    /// The container decides the output format and whether audio exists at
    /// all: an animated WebP keeps its alpha and never carries a sound track,
    /// whatever the document asked for.
    #[test]
    fn the_container_decides_the_pixel_format_and_the_audio() {
        let mut small = worked_example();
        small.canvas = Canvas {
            w: 320,
            h: 240,
            background: "#101820".to_string(),
        };
        small.items[0].dest = rect(0, 0, 160, 240);
        small.items[1].dest = rect(160, 0, 160, 240);

        let params = params_for(&small, "webp-anim");
        assert!(
            !params.doc.items[0].audio,
            "the audio flag is normalized away, so one document keys one artifact"
        );
        let plan = build_filtergraph(&params, &sources(2));
        assert!(!plan.has_audio);
        assert!(!plan.filter_complex.contains("amix"));
        assert!(plan.filter_complex.contains("format=yuva420p[vout]"));
        assert_eq!(
            plan.output_args,
            ["-map", "[vout]", "-an", "-sn", "-dn", "-pix_fmt", "yuva420p"]
        );

        let plan = build_filtergraph(&params_for(&small, "mosaic-webm"), &sources(2));
        assert!(plan.has_audio, "webm carries opus");
        assert!(plan.filter_complex.contains("format=yuv420p[vout]"));
    }

    /// What the probe is for: a crop that would fall outside the stream is
    /// clamped rather than failing the encode, an item marked audible whose
    /// file has no audio loses its chain instead of failing the graph, and a
    /// file whose first video stream is cover art composes the *other* one.
    #[test]
    fn the_probe_clamps_the_crop_and_drops_absent_audio() {
        let mut audible = worked_example();
        audible.items[1].audio = true;
        audible.items[1].time = ItemTime::Span {
            start_cs: 0,
            end_cs: 400,
        };
        let params = params_for(&audible, "mosaic-mp4");
        let probed = vec![
            ComposeSource {
                path: PathBuf::from("a.mp4"),
                probe: Some(StreamInfo {
                    // Half the source the client measured against: every
                    // rectangle is clamped into it.
                    width: 540,
                    height: 960,
                    video_index: 0,
                    has_audio: true,
                }),
            },
            ComposeSource {
                path: PathBuf::from("b.mp4"),
                probe: Some(StreamInfo {
                    width: 1920,
                    height: 1080,
                    video_index: 1,
                    has_audio: false,
                }),
            },
        ];
        let plan = build_filtergraph(&params, &probed);
        assert!(
            plan.filter_complex.contains("crop=540:960:0:0"),
            "{}",
            plan.filter_complex
        );
        assert!(
            plan.filter_complex.contains("[1:v:1]"),
            "the pictures, not the cover art: {}",
            plan.filter_complex
        );
        assert!(
            !plan.filter_complex.contains("[1:a:0]"),
            "a file with no audio contributes none: {}",
            plan.filter_complex
        );
        assert!(plan.filter_complex.contains("amix=inputs=1"));

        // The clamp itself, including an origin past the edge.
        let info = StreamInfo {
            width: 100,
            height: 80,
            video_index: 0,
            has_audio: false,
        };
        assert_eq!(
            clamped_src(rect(10, 10, 200, 200), Some(info)),
            rect(10, 10, 90, 70)
        );
        assert_eq!(
            clamped_src(rect(500, 500, 10, 10), Some(info)),
            rect(99, 79, 1, 1)
        );
        assert_eq!(
            clamped_src(rect(10, 10, 20, 20), None),
            rect(10, 10, 20, 20),
            "no probe, no clamp"
        );
    }

    /// The probe parser: the stream that carries pictures, and the rotation
    /// ffmpeg will have applied by the time the crop runs.
    #[test]
    fn the_probe_parser_picks_the_content_stream_and_applies_rotation() {
        let probe = |json: &str| parse_probe(json.as_bytes());
        assert_eq!(
            probe(
                r#"{"streams":[{"codec_type":"video","width":640,"height":480},
                    {"codec_type":"audio"}]}"#
            ),
            Some(StreamInfo {
                width: 640,
                height: 480,
                video_index: 0,
                has_audio: true
            })
        );
        // Cover art first: the second video stream is the one with pictures.
        assert_eq!(
            probe(
                r#"{"streams":[
                    {"codec_type":"video","width":300,"height":300,"disposition":{"attached_pic":1}},
                    {"codec_type":"video","width":1920,"height":1080}]}"#
            ),
            Some(StreamInfo {
                width: 1920,
                height: 1080,
                video_index: 1,
                has_audio: false
            })
        );
        // A rotated stream decodes to its display size, which is what the
        // client's source rectangle was measured against.
        assert_eq!(
            probe(
                r#"{"streams":[{"codec_type":"video","width":1920,"height":1080,
                    "side_data_list":[{"rotation":-90}]}]}"#
            )
            .map(|info| (info.width, info.height)),
            Some((1080, 1920))
        );
        assert_eq!(
            probe(
                r#"{"streams":[{"codec_type":"video","width":1920,"height":1080,
                    "side_data_list":[{"rotation":180}]}]}"#
            )
            .map(|info| (info.width, info.height)),
            Some((1920, 1080))
        );
        assert_eq!(probe(r#"{"streams":[{"codec_type":"audio"}]}"#), None);
        assert_eq!(probe("not json"), None);
    }

    /// The download name, which has no source stem to be built from.
    #[test]
    fn a_composition_names_its_download_after_itself() {
        assert_eq!(compose_file_name(Some(4), "mp4"), "mosaic-4items.mp4");
        assert_eq!(compose_file_name(Some(1), "webp"), "mosaic.webp");
        assert_eq!(compose_file_name(None, "webm"), "mosaic.webm");
    }

    // --- end to end against the real toolchain ------------------------------

    /// A flat-coloured lavfi clip. `jobs::files::write_clip`'s fixture is a
    /// single grey, and this needs items that can be told apart by eye — and
    /// by the pixel assertions below.
    fn write_color_clip(path: &Path, color: &str, w: i64, h: i64, seconds: f64) -> bool {
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-f", "lavfi", "-i"])
            .arg(format!("color=c={color}:s={w}x{h}:d={seconds}:r=30"))
            .args(["-pix_fmt", "yuv420p", "-crf", "18"])
            .arg(path)
            .stdin(Stdio::null())
            .status();
        matches!(status, Ok(status) if status.success())
    }

    /// The same, in two shades of one colour. The composition must actually
    /// *move*: libwebp's animation encoder collapses a run of identical frames
    /// into one still image, so a fixture of flat colour would produce a
    /// perfectly valid single-frame WebP and prove nothing about animation.
    fn write_two_shade_clip(path: &Path, first: &str, second: &str, w: i64, h: i64) -> bool {
        let source = |color: &str| format!("color=c={color}:s={w}x{h}:d=1.5:r=30");
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-f", "lavfi", "-i"])
            .arg(source(first))
            .args(["-f", "lavfi", "-i"])
            .arg(source(second))
            .args([
                "-filter_complex",
                "[0:v][1:v]concat=n=2:v=1:a=0[out]",
                "-map",
                "[out]",
                "-pix_fmt",
                "yuv420p",
                "-crf",
                "18",
            ])
            .arg(path)
            .stdin(Stdio::null())
            .status();
        matches!(status, Ok(status) if status.success())
    }

    /// `(canvas width, canvas height, animation frames)` of a WebP, read out
    /// of its RIFF chunks.
    ///
    /// ffprobe is no use here: its `webp_pipe` demuxer reads still images
    /// only, and answers an *animated* file with "image data not found" and a
    /// zero-sized stream. The container's own structure is both the more
    /// direct assertion and the only one this toolchain can make.
    fn webp_shape(bytes: &[u8]) -> Option<(i64, i64, usize)> {
        if bytes.get(..4)? != b"RIFF" || bytes.get(8..12)? != b"WEBP" {
            return None;
        }
        let frames = bytes.windows(4).filter(|window| *window == b"ANMF").count();
        let extended = bytes
            .windows(4)
            .position(|window| window == b"VP8X")
            .map(|at| at + 8)?;
        let u24 = |at: usize| -> Option<i64> {
            let bytes = bytes.get(at..at + 3)?;
            Some(i64::from(bytes[0]) | i64::from(bytes[1]) << 8 | i64::from(bytes[2]) << 16)
        };
        Some((u24(extended + 4)? + 1, u24(extended + 7)? + 1, frames))
    }

    /// `width,height,duration,frames` of an artifact. `-count_frames` rather
    /// than the container's own `nb_frames`: an animated WebP records none.
    fn probe_artifact(path: &Path) -> Option<(i64, i64, f64, i64)> {
        let output = Command::new(crate::media_tools::ffprobe())
            .args([
                "-v",
                "error",
                "-count_frames",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,nb_read_frames:format=duration",
                "-of",
                "json",
            ])
            .arg(path)
            .stdin(Stdio::null())
            .output()
            .ok()?;
        let data: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
        let stream = data.get("streams")?.as_array()?.first()?;
        let number = |value: Option<&serde_json::Value>| -> f64 {
            value
                .and_then(|value| match value {
                    serde_json::Value::String(text) => text.parse().ok(),
                    other => other.as_f64(),
                })
                .unwrap_or(0.0)
        };
        Some((
            number(stream.get("width")) as i64,
            number(stream.get("height")) as i64,
            number(data.get("format").and_then(|format| format.get("duration"))),
            number(stream.get("nb_read_frames")) as i64,
        ))
    }

    /// One frame of an artifact, decoded, so the assertions can be about
    /// pixels rather than about ffmpeg's exit code.
    fn frame_of(path: &Path, at: &str, into: &Path) -> Option<image::DynamicImage> {
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-ss", at, "-i"])
            .arg(path)
            .args(["-frames:v", "1"])
            .arg(into)
            .stdin(Stdio::null())
            .status();
        if !matches!(status, Ok(status) if status.success()) {
            return None;
        }
        image::open(into).ok()
    }

    /// Which primary a pixel is, by a margin no colour-space round trip can
    /// close. `None` when nothing dominates, which is itself an assertion
    /// failure worth reading.
    fn primary(image: &image::DynamicImage, x: u32, y: u32) -> Option<char> {
        use image::GenericImageView;
        let pixel = image.get_pixel(x, y);
        let channels = [
            ('r', i32::from(pixel[0])),
            ('g', i32::from(pixel[1])),
            ('b', i32::from(pixel[2])),
        ];
        let (name, value) = channels.iter().copied().max_by_key(|(_, value)| *value)?;
        channels
            .iter()
            .all(|(other, level)| *other == name || value - level > 50)
            .then_some(name)
    }

    /// The canvas the fixtures below compose onto: a red span on the left, a
    /// blue still inset on the right, and green showing through everywhere
    /// neither of them covers.
    fn fixture_document(preset_id: &str) -> ComposeRequest {
        let span = ComposeItem {
            sha256: "a".repeat(64),
            src: rect(0, 0, 400, 300),
            transform: Transform {
                quarter_turns: 1,
                flip_h: false,
            },
            dest: rect(0, 0, 160, 240),
            time: ItemTime::Span {
                start_cs: 0,
                end_cs: 200,
            },
            audio: false,
        };
        let still = ComposeItem {
            sha256: "b".repeat(64),
            src: rect(50, 50, 100, 100),
            transform: Transform::default(),
            dest: rect(180, 20, 120, 120),
            time: ItemTime::Still { at_cs: 50 },
            audio: false,
        };
        ComposeRequest {
            canvas: Canvas {
                w: 320,
                h: 240,
                background: "#00a000".to_string(),
            },
            fps: 25,
            output: ComposeOutput {
                preset: preset_id.to_string(),
                length: ComposeLength::LongestLoopOnce,
            },
            items: vec![span, still],
        }
    }

    /// End to end against the real toolchain, for each container the mosaic
    /// surface offers: two lavfi clips are composed into one artifact, and
    /// what comes out is the canvas that was asked for — the right size, the
    /// right length, and every item's colour inside the rectangle the document
    /// placed it in with the background showing between them.
    ///
    /// This is the only mechanical proof that the golden filtergraph above
    /// means what it says. Skips (never fails) where there is no ffmpeg.
    #[test]
    fn composes_two_fixture_clips_into_every_container() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let red = dir.path().join("red.mp4");
        let blue = dir.path().join("blue.mp4");
        if !write_two_shade_clip(&red, "0xE00000", "0xA00000", 400, 300)
            || !write_color_clip(&blue, "0x0000E0", 200, 200, 1.0)
        {
            return;
        }

        for (preset_id, ext) in [
            ("mosaic-mp4", "mp4"),
            ("webp-anim", "webp"),
            ("mosaic-webm", "webm"),
        ] {
            let document = fixture_document(preset_id);
            let preset = preset(preset_id);
            let encoder = super::super::run::resolve_encoder(&preset, None);
            let doc = resolve_compose(&document, &preset, limits()).expect("a valid document");
            let params = ComposeParams::new(doc, preset, encoder);
            let output = dir.path().join(format!("mosaic.{ext}"));
            let spec = super::super::run::ComposeJobSpec {
                sources: vec![red.clone(), blue.clone()],
                output: output.clone(),
                params,
            };
            let cancel = std::sync::atomic::AtomicBool::new(false);
            let mut seen: Vec<f32> = Vec::new();
            super::super::run::run_compose(&spec, &cancel, &mut |fraction| {
                if let Some(fraction) = fraction {
                    seen.push(fraction);
                }
            })
            .unwrap_or_else(|err| panic!("{preset_id} composes: {err}"));

            assert!(
                seen.windows(2).all(|pair| pair[1] >= pair[0]),
                "{preset_id} reports monotone progress: {seen:?}"
            );

            let frame = if ext == "webp" {
                let bytes = std::fs::read(&output).expect("the artifact is readable");
                let (width, height, frames) =
                    webp_shape(&bytes).unwrap_or_else(|| panic!("{preset_id} is a WebP"));
                assert_eq!((width, height), (320, 240), "{preset_id} renders the canvas");
                assert!(frames > 1, "{preset_id} is animated: {frames} frames");
                image::open(&output).unwrap_or_else(|err| panic!("{preset_id} decodes: {err}"))
            } else {
                let (width, height, duration, frames) =
                    probe_artifact(&output).unwrap_or_else(|| panic!("{preset_id} is probeable"));
                assert_eq!((width, height), (320, 240), "{preset_id} renders the canvas");
                assert!(frames > 1, "{preset_id} is animated: {frames} frames");
                assert!(
                    (duration - 2.0).abs() <= 2.0 / 25.0,
                    "{preset_id} is the resolved target long: {duration}"
                );
                let png = dir.path().join(format!("{preset_id}.png"));
                frame_of(&output, "1.0", &png)
                    .unwrap_or_else(|| panic!("{preset_id} decodes back to a frame"))
            };
            for (x, y, expected, what) in [
                (80, 120, 'r', "the rotated span fills its rectangle"),
                (240, 80, 'b', "the still fills its own"),
                (170, 230, 'g', "the background shows between them"),
                (310, 220, 'g', "and outside them"),
            ] {
                assert_eq!(
                    primary(&frame, x, y),
                    Some(expected),
                    "{preset_id}: {what} ({x},{y})"
                );
            }
        }
    }
}
