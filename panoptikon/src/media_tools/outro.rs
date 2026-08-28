//! Appended-outro detection for videos: does this file end with a known
//! platform end card, and if so where does the real content stop
//! (docs/video-outro-detection-design.md).
//!
//! Two stages, both shelling out to ffmpeg like the rest of the media
//! pipeline. Stage 1 decodes a single final frame squashed to a fixed 32x32
//! and tests its median colour — a *rejector only*, deliberately loose, and
//! cheap enough to run on every video. Stage 2 runs only on what stage 1
//! promotes: it decodes the last 7s at 30fps, finds the terminal run of card
//! frames, and applies four structural rules. Colour alone is not sufficient
//! (a dark-mode screen recording sits inside tolerance); the rules are what
//! produced zero false positives across 1,338 general and adversarial
//! videos.
//!
//! The pixel logic is deliberately kept out of the ffmpeg plumbing: it is the
//! half that must stay bit-equivalent to the Python reference of design §3.3
//! (§12), and it is the half worth testing on synthetic buffers.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::thread::JoinHandle;

/// Detector version. It is stored inside the `items.outro_kind` value as a
/// `/N` suffix (design §6.2), so a future detector can select the rows whose
/// version it does not recognise — negatives included — and re-run only
/// those. **Any change to detection behaviour must bump this.**
///
/// `i64` to match the visuals ledger's version column, which is what the scan
/// integration compares it against; the kind string formats the same either
/// way.
pub(crate) const OUTRO_DETECTOR_VERSION: i64 = 1;

/// Kind name for "examined, no outro found".
const KIND_NONE: &str = "none";
/// Kind name for the TikTok end card, all generations (design §2.1).
const KIND_TIKTOK_CARD: &str = "tiktok_card";

// --- algorithm constants (design §3.3) -------------------------------------
// Every one of these is tuned on a single library; changing any of them is a
// detector-version bump.

/// Frame median colour of every card frame, across all generations.
const CARD_BG: [u8; 3] = [12, 13, 25];
/// Tolerance on the frame median for "this frame sits on the card colour".
const TOL: f64 = 8.0;
/// Tolerance for counting a pixel as background *of its own frame*.
const BGFRAC_TOL: f64 = 12.0;
/// Minimum background fraction for a card frame. Must stay this permissive:
/// in square (576x576) videos the logo and search bar occupy a far larger
/// share of the frame than in 9:16, and 0.80 silently truncated the run and
/// reported K=3.50 instead of 4.00.
const BGFRAC_MIN: f64 = 0.45;
/// Mean of `card` over the terminal suffix, which bridges the animated
/// search-bar sweep that transiently fails `bgfrac`.
const RUN_MEAN_MIN: f64 = 0.90;
/// R0: shorter than this is no card at all.
const MIN_RUN_S: f64 = 1.0;
/// R1: a card is a *transition*, not a state. Without a boundary inside the
/// window there is no card, only a uniformly dark video.
const MIN_LEAD_S: f64 = 0.40;
/// R2: longer than any observed card. Fired zero times in re-validation;
/// retained as pure safety.
const K_CAP_S: f64 = 5.0;
/// R3: per-pixel distance from the card colour that counts as ink.
const INK_DELTA: i32 = 25;
/// R3: the card is a near-empty field with ink in a few central rows; UI
/// chrome spreads ink across nearly every row. A fraction, so aspect-robust.
const INK_ROWS_MAX: f64 = 0.60;

/// Tail length decoded by stage 2, in seconds.
const TAIL_S: u32 = 7;
/// Frame rate stage 2 resamples to. `K` is quantised to this.
const FPS: u32 = 30;
/// Stage 2 output width; the height follows the aspect ratio (`scale=48:-2`).
const W: usize = 48;
/// Stage 1 squashes to a fixed square, ignoring aspect ratio: a median-colour
/// test does not care about geometry, and a fixed size removes the need for
/// an ffprobe call to size the buffer.
const GATE_SIZE: usize = 32;
/// Stage 1 seek. Long enough to contain the final frame of any stream,
/// short enough that the decode is one frame's worth of work.
const GATE_SSEOF: &str = "-0.35";

/// Read granularity for the raw video stream.
const READ_CHUNK: usize = 64 * 1024;
/// How much stage-2 output may accumulate while the frame height is still
/// unknown. Reaching this means ffmpeg never reported its output geometry;
/// the rest of the stream is drained and discarded so the child still exits.
const HEIGHT_WAIT_CAP: usize = 8 * 1024 * 1024;
/// stderr lines kept for diagnostics.
const STDERR_TAIL_LINES: usize = 12;

/// Why a file was not judged to carry an outro. Not stored — `outro_kind`
/// records only the verdict — but each rule earned its rejections in the
/// adversarial set, so which one fired is worth logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RejectReason {
    /// Stage 1: the final frame is not on the card colour.
    Gate,
    /// R0: no terminal run of card frames.
    NoRun,
    /// R1: the whole window is card-coloured, so no boundary was found.
    NoBoundary,
    /// R2: the run is longer than any observed card.
    TooLong,
    /// R3: ink spread across nearly every row — UI chrome, not a card.
    Layout,
}

impl RejectReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RejectReason::Gate => "gate",
            RejectReason::NoRun => "no-run",
            RejectReason::NoBoundary => "no-boundary",
            RejectReason::TooLong => "too-long",
            RejectReason::Layout => "layout",
        }
    }
}

/// The detector's verdict on a file it managed to decode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum OutroVerdict {
    /// Examined, nothing found. Still a verdict, and still stored: it is what
    /// makes "negatives are never re-examined" hold.
    None(RejectReason),
    /// A TikTok end card occupying the last `k_seconds` of the file.
    TiktokCard { k_seconds: f64 },
}

impl OutroVerdict {
    /// The value stored in `items.outro_kind`, version suffix included.
    pub(crate) fn kind_value(&self) -> String {
        let kind = match self {
            OutroVerdict::None(_) => KIND_NONE,
            OutroVerdict::TiktokCard { .. } => KIND_TIKTOK_CARD,
        };
        format!("{kind}/{OUTRO_DETECTOR_VERSION}")
    }

    /// Seconds of outro measured from the end of the file, when there is one.
    pub(crate) fn k_seconds(&self) -> Option<f64> {
        match self {
            OutroVerdict::None(_) => None,
            OutroVerdict::TiktokCard { k_seconds } => Some(*k_seconds),
        }
    }
}

/// Why a probe produced no verdict. The two variants map to different ledger
/// outcomes: a spawn failure is never a verdict on the media (`blocked`,
/// healed when the toolchain binds), while ffmpeg running and failing is
/// ambiguous between a broken file and a transient mount hiccup (`failed`,
/// confirmed by a second attempt).
#[derive(Debug)]
pub(crate) enum OutroProbeError {
    Spawn(std::io::Error),
    Decode(String),
}

impl std::fmt::Display for OutroProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutroProbeError::Spawn(err) => write!(f, "ffmpeg failed to start: {err}"),
            OutroProbeError::Decode(detail) => write!(f, "outro probe failed: {detail}"),
        }
    }
}

impl std::error::Error for OutroProbeError {}

/// Where real content ends, in milliseconds, given the item's duration and
/// the measured outro length. `None` rather than a nonsense value when the
/// duration is missing or the outro would consume the whole file — a card
/// longer than its video is a measurement that must not reach consumers.
pub(crate) fn content_end_ms(duration_s: f64, k_s: f64) -> Option<i64> {
    if !duration_s.is_finite() || duration_s <= 0.0 || !k_s.is_finite() || k_s < 0.0 {
        return None;
    }
    let end = duration_s - k_s;
    if end <= 0.0 {
        return None;
    }
    Some((end * 1000.0).round() as i64)
}

/// The height ffmpeg's `scale=48:-2` derives for a `width`x`height` source,
/// for any ratio that does not scale away entirely.
///
/// ffmpeg rounds the derived height half-*up* and then to a multiple of two.
/// A language-default banker's rounding computes 68 where ffmpeg produces 70
/// (576x828 -> 828*48/576 = 69.0), the raw buffer then fails to divide into
/// frames, and a perfectly good file is recorded as a probe error.
///
/// `None` covers the degenerate ratios where the height rounds away to zero
/// (4000x1 and the like): swscale then falls back to something this helper
/// cannot reproduce, so there is no candidate to offer rather than a wrong
/// one.
pub(crate) fn scaled_height(width: u32, height: u32) -> Option<u32> {
    if width == 0 || height == 0 {
        return None;
    }
    let exact = f64::from(height) * W as f64 / f64::from(width);
    let rounded = (exact / 2.0 + 0.5).floor() * 2.0;
    if rounded < 2.0 {
        return None;
    }
    Some(rounded as u32)
}

/// The scaled height implied by the item's stored dimensions — but only when
/// the byte count the decode actually produced confirms it.
///
/// Both orientations are candidates, and stay so even now that the stored
/// dimensions are the *display* ones and should therefore agree with what the
/// filter graph sees (docs/display-dimensions-design.md): an item indexed
/// before that landed still holds coded dimensions until the backfill reaches
/// it, and this is not the place to depend on which. A candidate is taken only
/// when it is the sole one whose frame length divides the stream cleanly: if
/// both divide, if neither does, or if nothing was received, the honest answer
/// is `None` and the caller turns that into a probe error. Guessing here
/// reshapes the tail into a confidently wrong verdict, which is far worse than
/// a retry.
fn corroborated_height(source_dims: Option<(u32, u32)>, received: usize) -> Option<u32> {
    let (width, height) = source_dims?;
    if received == 0 {
        return None;
    }
    let mut accepted: Option<u32> = None;
    for candidate in [scaled_height(width, height), scaled_height(height, width)]
        .into_iter()
        .flatten()
    {
        let frame_len = candidate as usize * W * 3;
        if frame_len == 0 || received % frame_len != 0 {
            continue;
        }
        match accepted {
            Some(existing) if existing != candidate => return None,
            _ => accepted = Some(candidate),
        }
    }
    accepted
}

/// Runs both stages. `source_dims` are the item's stored dimensions,
/// consulted only if ffmpeg does not report its output geometry — and even
/// then they are not trusted on sight: they are checked against the byte count
/// the decode produced (`corroborated_height`), and a probe error is returned
/// rather than a verdict if they do not single out one orientation.
pub(crate) fn detect_outro(
    path: &Path,
    source_dims: Option<(u32, u32)>,
) -> Result<OutroVerdict, OutroProbeError> {
    if !gate_promotes(path)? {
        return Ok(OutroVerdict::None(RejectReason::Gate));
    }
    scan_tail(path, source_dims)
}

/// Stage 1 (design §3.1): decode the final frame, squash it to 32x32 and test
/// its median against the card colour. A rejector only — stage 2 is the
/// arbiter, and a loose gate is the cheap insurance against a new card
/// generation whose background differs by a few levels.
pub(crate) fn gate_promotes(path: &Path) -> Result<bool, OutroProbeError> {
    let args = vec![
        OsString::from("-nostdin"),
        OsString::from("-hide_banner"),
        OsString::from("-nostats"),
        OsString::from("-v"),
        OsString::from("error"),
        OsString::from("-sseof"),
        OsString::from(GATE_SSEOF),
        OsString::from("-i"),
        path.as_os_str().to_os_string(),
        OsString::from("-vf"),
        OsString::from(format!("scale={GATE_SIZE}:{GATE_SIZE},format=rgb24")),
        OsString::from("-f"),
        OsString::from("rawvideo"),
        OsString::from("-"),
    ];
    let mut proc = spawn_ffmpeg(&args)?;

    let frame_len = GATE_SIZE * GATE_SIZE * 3;
    let mut last = Vec::new();
    let trailing = match read_frames(&mut proc.stdout, frame_len, |frame| {
        last.clear();
        last.extend_from_slice(frame);
    }) {
        Ok(trailing) => trailing,
        Err(err) => {
            // The stream is abandoned mid-read, so the child may still be
            // writing into a pipe nobody drains: kill it rather than wait on
            // it, or the wait never returns.
            proc.abort();
            return Err(OutroProbeError::Decode(format!(
                "reading ffmpeg output: {err}"
            )));
        }
    };
    // The exit status first: it explains an unreadable stream far better than
    // the byte count does. stdout is at EOF here, so the child has exited and
    // the wait cannot block.
    proc.finish()?;
    if trailing != 0 {
        return Err(OutroProbeError::Decode(format!(
            "gate produced a partial frame ({trailing} trailing bytes)"
        )));
    }
    if last.is_empty() {
        // ffmpeg exited cleanly having emitted nothing: a source under ~3fps
        // can legitimately have no frame inside the last 0.35s. Not a probe
        // error — that would burn two ledger retries and a permanent
        // suppression on a healthy file — just nothing to promote.
        return Ok(false);
    }
    Ok(on_background(&frame_median(&last)))
}

/// Stage 2 (design §3.2): decode the last 7s at 30fps scaled to 48px wide and
/// apply the rules to the terminal run.
fn scan_tail(
    path: &Path,
    source_dims: Option<(u32, u32)>,
) -> Result<OutroVerdict, OutroProbeError> {
    // Default log verbosity (not the reference's `-v error`) so ffmpeg reports
    // its output geometry; `-nostats` keeps the progress spew out of it. The
    // log level does not touch a single output byte.
    let args = vec![
        OsString::from("-nostdin"),
        OsString::from("-hide_banner"),
        OsString::from("-nostats"),
        OsString::from("-sseof"),
        OsString::from(format!("-{TAIL_S}")),
        OsString::from("-i"),
        path.as_os_str().to_os_string(),
        OsString::from("-vf"),
        OsString::from(format!("fps={FPS},scale={W}:-2,format=rgb24")),
        OsString::from("-f"),
        OsString::from("rawvideo"),
        OsString::from("-"),
    ];
    let mut proc = spawn_ffmpeg(&args)?;

    let mut chunk = vec![0u8; READ_CHUNK];
    let mut pending: Vec<u8> = Vec::new();
    let mut card: Vec<bool> = Vec::new();
    let mut last_frame: Vec<u8> = Vec::new();
    let mut frame_len = 0usize;
    let mut overflowed = false;
    let mut io_error: Option<std::io::Error> = None;

    // The height is polled rather than waited on: blocking here would let the
    // stdout pipe fill, which stops ffmpeg writing, which stops it logging,
    // which is a deadlock if the geometry line never arrives. Reading first
    // and asking after keeps the child running whatever happens.
    loop {
        let read = match proc.stdout.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                io_error = Some(err);
                break;
            }
        };
        if frame_len == 0
            && let Ok(height) = proc.height.try_recv()
        {
            frame_len = height as usize * W * 3;
        }
        if overflowed {
            continue;
        }
        pending.extend_from_slice(&chunk[..read]);
        if frame_len == 0 {
            // `-sseof` is not guaranteed (design §3.4): a stream that ignores
            // it decodes whole, so nothing here may grow with the file.
            if pending.len() > HEIGHT_WAIT_CAP {
                overflowed = true;
                pending = Vec::new();
            }
            continue;
        }
        consume_frames(&mut pending, frame_len, &mut card, &mut last_frame);
    }

    if let Some(err) = io_error {
        // The stream is abandoned mid-decode: the child may still be writing
        // into a pipe nobody drains, so neither wait on it nor wait on the
        // stderr scanner's report — both would block forever. Kill first.
        proc.abort();
        return Err(OutroProbeError::Decode(format!(
            "reading ffmpeg output: {err}"
        )));
    }

    if frame_len == 0 && !overflowed {
        // stdout is at EOF, so ffmpeg has exited and closed its stderr: the
        // scanner thread has finished, and a late report — or the absence of
        // one, as a dropped sender — is available without any risk of
        // blocking.
        let height = match proc.height.recv().ok() {
            Some(reported) => Some(reported),
            // Nothing was reported, so the stored dims are the only candidate
            // left — and they are used only if the received byte count picks
            // out one orientation for them. Nothing was consumed while the
            // height was unknown, so `pending` still holds the whole stream.
            None => corroborated_height(source_dims, pending.len()),
        };
        if let Some(height) = height {
            frame_len = height as usize * W * 3;
            consume_frames(&mut pending, frame_len, &mut card, &mut last_frame);
        }
    }

    proc.finish()?;
    if overflowed {
        return Err(OutroProbeError::Decode(
            "ffmpeg output ran past the height-detection window".to_string(),
        ));
    }
    if frame_len == 0 {
        return Err(OutroProbeError::Decode(
            match source_dims {
                Some(dims) => format!(
                    "ffmpeg did not report scaled geometry and stored dims are \
                     ambiguous/inconsistent with the {} bytes received ({}x{})",
                    pending.len(),
                    dims.0,
                    dims.1
                ),
                None => "ffmpeg did not report scaled geometry and the item has no stored \
                         dimensions to fall back on"
                    .to_string(),
            },
        ));
    }
    if !pending.is_empty() {
        return Err(OutroProbeError::Decode(format!(
            "ffmpeg produced a partial frame ({} trailing bytes)",
            pending.len()
        )));
    }
    if card.is_empty() {
        return Err(OutroProbeError::Decode(
            "ffmpeg produced no frames".to_string(),
        ));
    }

    Ok(match verdict_from_tail(&card, &last_frame) {
        Ok(k_seconds) => OutroVerdict::TiktokCard { k_seconds },
        Err(reason) => OutroVerdict::None(reason),
    })
}

/// Drains whole frames out of `pending`, scoring each and keeping the last.
/// Only the boolean per frame and the final frame's pixels are retained, so
/// memory stays flat however many frames the decode produces.
fn consume_frames(
    pending: &mut Vec<u8>,
    frame_len: usize,
    card: &mut Vec<bool>,
    last_frame: &mut Vec<u8>,
) {
    let mut offset = 0;
    while pending.len() - offset >= frame_len {
        let frame = &pending[offset..offset + frame_len];
        card.push(frame_is_card(frame));
        last_frame.clear();
        last_frame.extend_from_slice(frame);
        offset += frame_len;
    }
    pending.drain(..offset);
}

// --- pure pixel logic ------------------------------------------------------

/// Median of an unsorted byte slice. An even count averages the two middle
/// values (numpy semantics), and the result stays in f64 so the `.5` is not
/// lost — design §12 names this as one of the two places the Rust port can
/// diverge from the reference.
fn median_u8(values: &mut [u8]) -> f64 {
    let count = values.len();
    if count == 0 {
        return 0.0;
    }
    let mid = count / 2;
    let (lower, upper, _) = values.select_nth_unstable(mid);
    let upper = f64::from(*upper);
    if count % 2 == 1 {
        return upper;
    }
    let lower = f64::from(*lower.iter().max().expect("an even count has a lower half"));
    (lower + upper) / 2.0
}

/// Per-channel median over every pixel of an rgb24 frame.
fn frame_median(frame: &[u8]) -> [f64; 3] {
    let mut channel = Vec::with_capacity(frame.len() / 3);
    let mut median = [0.0; 3];
    for (index, slot) in median.iter_mut().enumerate() {
        channel.clear();
        channel.extend(frame.iter().skip(index).step_by(3).copied());
        *slot = median_u8(&mut channel);
    }
    median
}

/// Whether a frame median sits on the card colour.
fn on_background(median: &[f64; 3]) -> bool {
    (0..3)
        .map(|channel| (median[channel] - f64::from(CARD_BG[channel])).abs())
        .fold(0.0, f64::max)
        <= TOL
}

/// Fraction of pixels within `BGFRAC_TOL` levels (max channel) of the frame's
/// own median — how much of the frame is flat background.
fn background_fraction(frame: &[u8], median: &[f64; 3]) -> f64 {
    let pixels = frame.len() / 3;
    if pixels == 0 {
        return 0.0;
    }
    let near = frame
        .chunks_exact(3)
        .filter(|pixel| {
            (0..3)
                .map(|channel| (f64::from(pixel[channel]) - median[channel]).abs())
                .fold(0.0, f64::max)
                <= BGFRAC_TOL
        })
        .count();
    near as f64 / pixels as f64
}

fn frame_is_card(frame: &[u8]) -> bool {
    let median = frame_median(frame);
    on_background(&median) && background_fraction(frame, &median) >= BGFRAC_MIN
}

/// R3's measurement: the fraction of rows carrying any ink, ink being any
/// pixel further than `INK_DELTA` from the card colour.
fn ink_row_fraction(frame: &[u8]) -> f64 {
    let row_len = W * 3;
    let rows = frame.len() / row_len;
    if rows == 0 {
        return 0.0;
    }
    let inked = frame
        .chunks_exact(row_len)
        .filter(|row| {
            row.chunks_exact(3).any(|pixel| {
                (0..3)
                    .map(|channel| (i32::from(pixel[channel]) - i32::from(CARD_BG[channel])).abs())
                    .max()
                    .unwrap_or(0)
                    > INK_DELTA
            })
        })
        .count();
    inked as f64 / rows as f64
}

/// Start of the terminal, gap-tolerant run: the smallest index that is itself
/// a card frame and from which at least `RUN_MEAN_MIN` of the remainder are.
/// The tolerance bridges the animated search-bar sweep, which transiently
/// fails `bgfrac` mid-card. Returns `card.len()` when there is no run.
fn terminal_run_start(card: &[bool]) -> usize {
    let count = card.len();
    let mut start = count;
    let mut suffix_true = 0usize;
    for index in (0..count).rev() {
        if !card[index] {
            continue;
        }
        suffix_true += 1;
        let suffix_len = count - index;
        if suffix_true as f64 / suffix_len as f64 >= RUN_MEAN_MIN {
            start = index;
        }
    }
    start
}

/// The rules of design §3.2, in order, over a scored tail. `Ok(K)` is
/// seconds of outro measured from the end of the file.
fn verdict_from_tail(card: &[bool], last_frame: &[u8]) -> Result<f64, RejectReason> {
    let count = card.len();
    let start = terminal_run_start(card);
    let run = (count - start) as f64 / f64::from(FPS);
    let lead = start as f64 / f64::from(FPS);
    if run < MIN_RUN_S {
        return Err(RejectReason::NoRun);
    }
    if lead < MIN_LEAD_S {
        return Err(RejectReason::NoBoundary);
    }
    if run > K_CAP_S {
        return Err(RejectReason::TooLong);
    }
    if ink_row_fraction(last_frame) > INK_ROWS_MAX {
        return Err(RejectReason::Layout);
    }
    Ok(run)
}

// --- ffmpeg plumbing -------------------------------------------------------

struct FfmpegProc {
    child: Child,
    stdout: ChildStdout,
    /// The scaled frame height, as soon as the stderr scanner finds it. The
    /// sender is dropped when the scanner gives up, so a receive can never
    /// wait on a report that is not coming.
    height: Receiver<u32>,
    stderr: Option<JoinHandle<String>>,
}

impl FfmpegProc {
    /// Reaps the child and collects its stderr. `Err` on a non-zero exit:
    /// ffmpeg ran and failed, which the ledger treats as ambiguous between a
    /// broken file and a transient I/O hiccup.
    fn finish(&mut self) -> Result<String, OutroProbeError> {
        let status = self
            .child
            .wait()
            .map_err(|err| OutroProbeError::Decode(format!("waiting for ffmpeg: {err}")))?;
        let stderr = self
            .stderr
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        if !status.success() {
            return Err(OutroProbeError::Decode(format!(
                "ffmpeg exited with {status}: {stderr}"
            )));
        }
        Ok(stderr)
    }

    /// Kills the child and reaps it, ignoring every error. For the paths that
    /// abandon the stream: `wait` alone can block forever on a child still
    /// writing into a pipe nobody is draining.
    fn abort(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stderr.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for FfmpegProc {
    /// A panic between spawn and `finish` must not leave an ffmpeg behind.
    /// Both calls are no-ops once the child has been reaped — std makes `kill`
    /// after a successful `wait` a no-op rather than a signal to a recycled
    /// pid — so the happy path is untouched.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The stderr scanner's line filter. Geometry is read only off a `Stream #`
/// line *inside* the `Output #` block: an input path can contain something
/// shaped like `48x70`, and the output block's own Metadata section copies
/// arbitrary input tags verbatim, so a `title` holding `48x64` would otherwise
/// be read as the frame height.
#[derive(Default)]
struct GeometryScanner {
    in_output_block: bool,
}

impl GeometryScanner {
    fn feed(&mut self, line: &str) -> Option<u32> {
        let line = line.trim();
        if !self.in_output_block {
            self.in_output_block = line.starts_with("Output #");
            return None;
        }
        if !line.starts_with("Stream #") {
            return None;
        }
        parse_scaled_height(line)
    }
}

fn spawn_ffmpeg(args: &[OsString]) -> Result<FfmpegProc, OutroProbeError> {
    let mut child = Command::new(crate::media_tools::ffmpeg())
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(OutroProbeError::Spawn)?;
    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    let (sender, height) = channel();
    // stderr is drained on its own thread whatever happens: a pipe left to
    // fill would stall ffmpeg mid-decode.
    let handle = std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut sender = Some(sender);
        let mut scanner = GeometryScanner::default();
        let mut tail: VecDeque<String> = VecDeque::with_capacity(STDERR_TAIL_LINES);
        let mut raw: Vec<u8> = Vec::new();
        loop {
            // Byte-wise and lossy, never by `lines()`: ffmpeg copies container
            // metadata into its log, so a title tag holding raw bytes puts
            // invalid UTF-8 on stderr. A `lines()` loop stops at the first
            // such line — losing the geometry report and, worse, leaving the
            // pipe to fill and the decode to stall. Draining continues to EOF
            // whatever any single line decodes to.
            raw.clear();
            match reader.read_until(b'\n', &mut raw) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            let line = String::from_utf8_lossy(&raw)
                .trim_end_matches(['\n', '\r'])
                .to_string();
            if let Some(pending) = &sender
                && let Some(height) = scanner.feed(&line)
            {
                let _ = pending.send(height);
                sender = None;
            }
            if tail.len() == STDERR_TAIL_LINES {
                tail.pop_front();
            }
            tail.push_back(line);
        }
        tail.into_iter().collect::<Vec<_>>().join("\n")
    });
    Ok(FfmpegProc {
        child,
        stdout,
        height,
        stderr: Some(handle),
    })
}

/// Pulls the scaled frame height out of an ffmpeg stream-description line by
/// its `48x<h>` geometry token. A digit immediately before the `48`
/// disqualifies the match, so a `1648x928` source geometry is not mistaken
/// for the output's.
fn parse_scaled_height(line: &str) -> Option<u32> {
    let needle = format!("{W}x");
    let bytes = line.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = line[from..].find(&needle) {
        let at = from + offset;
        from = at + needle.len();
        if at > 0 && bytes[at - 1].is_ascii_digit() {
            continue;
        }
        let digits: String = line[from..]
            .chars()
            .take_while(|character| character.is_ascii_digit())
            .collect();
        if let Ok(height) = digits.parse::<u32>()
            && height > 0
        {
            return Some(height);
        }
    }
    None
}

/// Reads fixed-size frames off a stream, handing each to `on_frame`. Returns
/// the number of trailing bytes that did not make up a whole frame.
fn read_frames<R: Read>(
    mut reader: R,
    frame_len: usize,
    mut on_frame: impl FnMut(&[u8]),
) -> std::io::Result<usize> {
    let mut buffer = vec![0u8; frame_len];
    let mut filled = 0usize;
    loop {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => return Ok(filled),
            Ok(read) => {
                filled += read;
                if filled == frame_len {
                    on_frame(&buffer);
                    filled = 0;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_H: usize = 86;

    /// A frame filled with the card background.
    fn card_frame() -> Vec<u8> {
        CARD_BG.repeat(W * TEST_H)
    }

    /// The card as it really looks: a near-empty field with ink in a few
    /// central rows.
    fn card_with_logo() -> Vec<u8> {
        let mut frame = card_frame();
        paint(&mut frame, 40..50, 10..38, [255, 255, 255]);
        frame
    }

    /// Content: nothing like the card colour, so `on_bg` fails outright.
    fn content_frame() -> Vec<u8> {
        [128u8, 128, 128].repeat(W * TEST_H)
    }

    /// A frame of `rows` rows in a single colour.
    fn flat_frame(rows: usize, colour: [u8; 3]) -> Vec<u8> {
        colour.repeat(W * rows)
    }

    /// A 48x10 frame — 480 pixels, so twentieths are exact — whose median is
    /// exactly `CARD_BG` and whose background fraction is exactly
    /// `background / 480`. The non-background pixels straddle the card colour
    /// so that the median is not dragged off it, and neither black nor white
    /// is within `BGFRAC_TOL` of it.
    fn frame_with_background(background: usize) -> Vec<u8> {
        const PIXELS: usize = W * 10;
        let rest = PIXELS - background;
        let dark = rest / 2;
        let mut frame = Vec::with_capacity(PIXELS * 3);
        for index in 0..PIXELS {
            let pixel = if index < dark {
                [0, 0, 0]
            } else if index < dark + background {
                CARD_BG
            } else {
                [255, 255, 255]
            };
            frame.extend_from_slice(&pixel);
        }
        frame
    }

    fn paint(
        frame: &mut [u8],
        rows: std::ops::Range<usize>,
        columns: std::ops::Range<usize>,
        colour: [u8; 3],
    ) {
        for row in rows {
            for column in columns.clone() {
                let at = (row * W + column) * 3;
                frame[at..at + 3].copy_from_slice(&colour);
            }
        }
    }

    /// The scoring half of stage 2 over a whole decoded tail, which is what
    /// the streaming loop builds up incrementally.
    fn classify(frames: &[Vec<u8>]) -> Result<f64, RejectReason> {
        let card: Vec<bool> = frames.iter().map(|frame| frame_is_card(frame)).collect();
        let last = frames.last().cloned().unwrap_or_default();
        verdict_from_tail(&card, &last)
    }

    /// 210 frames: `content` of content, then `card` card frames.
    fn tail(content: usize, card: usize) -> Vec<Vec<u8>> {
        let mut frames = vec![content_frame(); content];
        frames.extend(std::iter::repeat_n(card_with_logo(), card));
        frames
    }

    #[test]
    fn a_synthetic_card_tail_is_accepted() {
        let frames = tail(150, 60);
        assert_eq!(classify(&frames), Ok(2.0));
    }

    #[test]
    fn a_transient_dip_inside_the_run_is_bridged() {
        let mut frames = tail(150, 60);
        // The search-bar sweep: four frames that momentarily stop looking like
        // a card. A strict terminal run would report 1.7s instead of 2.0.
        for frame in &mut frames[155..159] {
            *frame = content_frame();
        }
        assert_eq!(classify(&frames), Ok(2.0));
    }

    #[test]
    fn a_uniformly_card_coloured_window_has_no_boundary() {
        // The dark-mode-capture class: a state, not a transition.
        let frames = tail(0, 210);
        assert_eq!(classify(&frames), Err(RejectReason::NoBoundary));
    }

    #[test]
    fn a_run_under_a_second_is_rejected() {
        let frames = tail(190, 20);
        assert_eq!(classify(&frames), Err(RejectReason::NoRun));
    }

    #[test]
    fn a_run_over_the_cap_is_rejected() {
        let frames = tail(50, 160);
        assert_eq!(classify(&frames), Err(RejectReason::TooLong));
    }

    #[test]
    fn ink_on_every_row_is_rejected_as_layout() {
        let mut frames = tail(150, 60);
        // Still a card frame by colour — a thin vertical rule leaves bgfrac
        // near 1 — but ink reaches every row, which is UI chrome.
        let mut chrome = card_with_logo();
        paint(&mut chrome, 0..TEST_H, 0..2, [255, 255, 255]);
        assert!(frame_is_card(&chrome), "the rejection must come from R3");
        *frames.last_mut().expect("the tail is not empty") = chrome;
        assert_eq!(classify(&frames), Err(RejectReason::Layout));
    }

    // --- §3.3 boundary semantics ------------------------------------------
    // Every comparison in §3.3 is inclusive on the accepting side. These pin
    // that: flipping any one of them from `<=` to `<` (or `>=` to `>`) fails
    // a test here rather than silently shifting the detector's verdicts.

    #[test]
    fn the_on_bg_tolerance_admits_exactly_eight_levels() {
        let at = flat_frame(4, [CARD_BG[0] + 8, CARD_BG[1], CARD_BG[2]]);
        assert!(on_background(&frame_median(&at)), "delta 8 is on the card");
        let past = flat_frame(4, [CARD_BG[0] + 9, CARD_BG[1], CARD_BG[2]]);
        assert!(!on_background(&frame_median(&past)), "delta 9 is not");
        // The same on the low side, in another channel: it is a max over the
        // absolute per-channel differences.
        let below = flat_frame(4, [CARD_BG[0], CARD_BG[1], CARD_BG[2] - 8]);
        assert!(on_background(&frame_median(&below)));
        let further = flat_frame(4, [CARD_BG[0], CARD_BG[1], CARD_BG[2] - 9]);
        assert!(!on_background(&frame_median(&further)));
    }

    #[test]
    fn the_background_fraction_floor_admits_exactly_045() {
        let at = frame_with_background(216); // 216/480 == 0.45
        let median = frame_median(&at);
        assert_eq!(median, [12.0, 13.0, 25.0], "the median must be the card");
        assert_eq!(background_fraction(&at, &median), BGFRAC_MIN);
        assert!(frame_is_card(&at));

        let below = frame_with_background(215); // 215/480 == 0.4479…
        let median = frame_median(&below);
        assert_eq!(median, [12.0, 13.0, 25.0]);
        assert!(background_fraction(&below, &median) < BGFRAC_MIN);
        assert!(!frame_is_card(&below), "one pixel short is not a card frame");
    }

    #[test]
    fn the_background_tolerance_admits_exactly_twelve_levels() {
        // The 0.45 frame with its first (black) and last (white) pixel swapped
        // for near-median ones: 12 levels off in a single channel still counts
        // as background, 13 does not. Neither swap moves the median — both
        // land outside the two middle positions of every channel — which the
        // assert below pins, so the distances really are 12 and 13.
        let mut frame = frame_with_background(216);
        paint(&mut frame, 0..1, 0..1, [CARD_BG[0] + 12, CARD_BG[1], CARD_BG[2]]);
        paint(&mut frame, 9..10, 47..48, [CARD_BG[0] + 13, CARD_BG[1], CARD_BG[2]]);
        let median = frame_median(&frame);
        assert_eq!(median, [12.0, 13.0, 25.0], "the median must be the card");
        // The 216 background pixels plus the one at exactly BGFRAC_TOL.
        assert_eq!(background_fraction(&frame, &median), 217.0 / 480.0);
    }

    #[test]
    fn the_run_floor_and_cap_admit_exactly_one_and_five_seconds() {
        let last = card_with_logo();
        // A one-second lead keeps R1 satisfied whatever the run length is.
        let with_run = |run: usize| -> Vec<bool> {
            (0..30 + run).map(|index| index >= 30).collect()
        };
        // R0 rejects only *below* 1.0s: 30 frames is exactly the floor.
        assert_eq!(verdict_from_tail(&with_run(30), &last), Ok(1.0));
        assert_eq!(
            verdict_from_tail(&with_run(29), &last),
            Err(RejectReason::NoRun)
        );
        // R2 rejects only *past* 5.0s: 150 frames is exactly the cap.
        assert_eq!(verdict_from_tail(&with_run(150), &last), Ok(5.0));
        assert_eq!(
            verdict_from_tail(&with_run(151), &last),
            Err(RejectReason::TooLong)
        );
    }

    #[test]
    fn the_run_mean_floor_admits_exactly_27_of_30() {
        // 27 of the last 30 is exactly 0.90, so index 0 opens the run.
        let mut card = vec![true; 30];
        for index in [10, 15, 20] {
            card[index] = false;
        }
        assert_eq!(terminal_run_start(&card), 0);
        // One card frame fewer over the same window is 26/30 = 0.8667: index 0
        // no longer opens a run, and the run starts after the last gap.
        card[25] = false;
        assert_eq!(terminal_run_start(&card), 26);
    }

    #[test]
    fn ink_starts_one_level_past_the_delta_and_the_row_ceiling_admits_exactly_060() {
        // A pixel exactly INK_DELTA from the card colour is not ink.
        let mut frame = flat_frame(10, CARD_BG);
        let ink_at = |offset: i32| {
            [
                (i32::from(CARD_BG[0]) + offset) as u8,
                CARD_BG[1],
                CARD_BG[2],
            ]
        };
        paint(&mut frame, 0..1, 0..1, ink_at(INK_DELTA));
        assert_eq!(ink_row_fraction(&frame), 0.0);
        paint(&mut frame, 0..1, 0..1, ink_at(INK_DELTA + 1));
        assert_eq!(ink_row_fraction(&frame), 0.1);

        // R3 rejects only *past* the ceiling: 6 of 10 rows is exactly 0.60.
        let inked = |rows: usize| {
            let mut frame = flat_frame(10, CARD_BG);
            paint(&mut frame, 0..rows, 0..1, [255, 255, 255]);
            frame
        };
        assert_eq!(ink_row_fraction(&inked(6)), INK_ROWS_MAX);
        let card: Vec<bool> = (0..210).map(|index| index >= 150).collect();
        assert_eq!(verdict_from_tail(&card, &inked(6)), Ok(2.0));
        assert_eq!(
            verdict_from_tail(&card, &inked(7)),
            Err(RejectReason::Layout)
        );
    }

    #[test]
    fn the_lead_floor_admits_exactly_twelve_frames() {
        let last = card_with_logo();
        let with_lead = |lead: usize| -> Vec<bool> {
            (0..lead + 60).map(|index| index >= lead).collect()
        };
        // 12 frames of content ahead of the run is 0.40s exactly.
        assert_eq!(verdict_from_tail(&with_lead(12), &last), Ok(2.0));
        assert_eq!(
            verdict_from_tail(&with_lead(11), &last),
            Err(RejectReason::NoBoundary)
        );
    }

    #[test]
    fn an_even_count_median_averages_the_two_middle_values() {
        assert_eq!(median_u8(&mut [4, 1, 3, 2]), 2.5);
        assert_eq!(median_u8(&mut [3, 1, 2]), 2.0);
        assert_eq!(median_u8(&mut [10, 20]), 15.0);
        assert_eq!(median_u8(&mut []), 0.0);
    }

    #[test]
    fn scaled_height_rounds_the_way_ffmpeg_does() {
        // The case that broke the reference run: banker's rounding gives 68.
        assert_eq!(scaled_height(576, 828), Some(70));
        // 9:16 phone capture, and TikTok's own 576x1024 export geometry.
        assert_eq!(scaled_height(1080, 1920), Some(86));
        assert_eq!(scaled_height(576, 1024), Some(86));
        // Landscape: 27.0 exactly, rounded up to the next even height.
        assert_eq!(scaled_height(1920, 1080), Some(28));
        // Square, and already the target width.
        assert_eq!(scaled_height(576, 576), Some(48));
        assert_eq!(scaled_height(48, 48), Some(48));
        // Degenerate inputs never produce a zero-length frame.
        assert_eq!(scaled_height(0, 100), None);
        assert_eq!(scaled_height(100, 0), None);
        // Rounds away to nothing: swscale falls back to the source height,
        // which this helper cannot reproduce, so it offers no candidate.
        assert_eq!(scaled_height(4000, 1), None);
    }

    #[test]
    fn a_stored_dims_fallback_is_taken_only_when_the_byte_count_confirms_it() {
        let frame = |height: u32| height as usize * W * 3;
        // 1080x1920 scales to 86, 1920x1080 to 28: at 15 frames only the 86
        // frame length divides the stream, so the bytes settle the
        // orientation.
        assert_eq!(
            corroborated_height(Some((1080, 1920)), frame(86) * 15),
            Some(86)
        );
        assert_eq!(
            corroborated_height(Some((1080, 1920)), frame(28) * 210),
            Some(28)
        );
        // A square source: both orientations agree, so there is nothing to be
        // ambiguous about.
        assert_eq!(
            corroborated_height(Some((576, 576)), frame(48) * 30),
            Some(48)
        );
        // Both candidate frame lengths can divide the same stream — a full
        // 210-frame tail of 86-row frames is also a whole number of 28-row
        // frames — and then the byte count is no evidence either way.
        assert_eq!(corroborated_height(Some((1080, 1920)), frame(86) * 210), None);
        // Neither divides, nothing was received, or nothing was stored.
        assert_eq!(
            corroborated_height(Some((1080, 1920)), frame(86) * 3 + 1),
            None
        );
        assert_eq!(corroborated_height(Some((1080, 1920)), 0), None);
        assert_eq!(corroborated_height(None, frame(86)), None);
        // A degenerate stored ratio offers no candidate for that orientation,
        // and the other one does not fit.
        assert_eq!(corroborated_height(Some((4000, 1)), frame(2) * 4), None);
    }

    #[test]
    fn content_end_ms_refuses_nonsense() {
        assert_eq!(content_end_ms(10.0, 4.0), Some(6000));
        assert_eq!(content_end_ms(4.0667, 4.0667), None);
        assert_eq!(content_end_ms(3.0, 4.0), None);
        assert_eq!(content_end_ms(0.0, 4.0), None);
        assert_eq!(content_end_ms(-1.0, 4.0), None);
        assert_eq!(content_end_ms(f64::NAN, 4.0), None);
        assert_eq!(content_end_ms(10.0, f64::INFINITY), None);
    }

    #[test]
    fn kind_values_carry_the_detector_version() {
        assert_eq!(
            OutroVerdict::TiktokCard { k_seconds: 4.0 }.kind_value(),
            "tiktok_card/1"
        );
        assert_eq!(
            OutroVerdict::None(RejectReason::Gate).kind_value(),
            "none/1"
        );
        assert_eq!(OUTRO_DETECTOR_VERSION, 1);
    }

    #[test]
    fn the_geometry_token_is_read_off_a_stream_line() {
        assert_eq!(
            parse_scaled_height(
                "  Stream #0:0: Video: rawvideo (RGB[24] / 0x18424752), rgb24(pc, gbr/unknown/unknown, progressive), 48x70, q=2-31, 30 fps, 30 tbn"
            ),
            Some(70)
        );
        // A source geometry that merely ends in 48 is not the output's.
        assert_eq!(
            parse_scaled_height("Video: h264, yuv420p, 1648x928 [SAR 1:1]"),
            None
        );
        assert_eq!(parse_scaled_height("Press [q] to stop"), None);
    }

    #[test]
    fn the_output_geometry_is_read_off_the_output_stream_line_only() {
        let mut scanner = GeometryScanner::default();
        // Nothing in the input block is the output's geometry, whatever it
        // happens to be shaped like.
        assert_eq!(scanner.feed("Input #0, mov,mp4,m4a, from '/48x64/clip.mp4':"), None);
        assert_eq!(scanner.feed("  Metadata:"), None);
        assert_eq!(
            scanner.feed("  Stream #0:0(und): Video: h264, yuv420p, 1080x1920 [SAR 1:1]"),
            None
        );
        assert_eq!(scanner.feed("Output #0, rawvideo, to 'pipe:':"), None);
        // The output block copies input metadata tags verbatim; a title that
        // contains a geometry token is not a stream description.
        assert_eq!(scanner.feed("  Metadata:"), None);
        assert_eq!(scanner.feed("    title           : my 48x64 remix"), None);
        assert_eq!(scanner.feed("    encoder         : Lavf62.0.102"), None);
        assert_eq!(
            scanner.feed(
                "  Stream #0:0: Video: rawvideo (RGB[24] / 0x18424752), rgb24, 48x86, q=2-31, 30 fps, 30 tbn"
            ),
            Some(86)
        );
    }

    /// The one test that goes through ffmpeg: output-geometry parsing, frame
    /// streaming and exit handling are exactly what the pure tests above
    /// cannot reach, and a wrong filter string would otherwise surface only
    /// against real media.
    ///
    /// Skipped rather than `#[ignore]`d, so it runs by default wherever ffmpeg
    /// and its lavfi input device exist — which is every machine that can
    /// actually index video — instead of only when someone remembers
    /// `--ignored`. A machine without them returns early rather than failing.
    #[test]
    fn a_generated_card_is_detected_end_to_end() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::TempDir::new().expect("a temp dir");
        let clip = dir.path().join("card.mp4");
        let status = Command::new(crate::media_tools::ffmpeg())
            .args([
                "-y",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=0x404040:s=576x1024:d=5:r=30",
                "-f",
                "lavfi",
                "-i",
                "color=c=0x0C0D19:s=576x1024:d=2:r=30",
                "-filter_complex",
                "[1:v]drawbox=x=100:y=480:w=376:h=60:color=white:t=fill[card];\
                 [0:v][card]concat=n=2:v=1:a=0[out]",
                "-map",
                "[out]",
                "-pix_fmt",
                "yuv420p",
                "-crf",
                "18",
            ])
            .arg(&clip)
            .status();
        // No lavfi (or no working encoder) on this build: nothing to test
        // against, and a failure here would be about the fixture, not the
        // detector.
        if !matches!(status, Ok(status) if status.success()) {
            return;
        }

        assert!(gate_promotes(&clip).expect("the gate runs"));
        // 576x1024 is TikTok's own export geometry, so this also pins the
        // scaled height at the value ffmpeg reports (86).
        let verdict = detect_outro(&clip, Some((576, 1024))).expect("the probe runs");
        assert_eq!(verdict, OutroVerdict::TiktokCard { k_seconds: 2.0 });
        assert_eq!(verdict.kind_value(), "tiktok_card/1");
        assert_eq!(content_end_ms(7.0, 2.0), Some(5000));
    }

    #[test]
    fn frames_are_read_whole_and_trailing_bytes_reported() {
        let data = vec![7u8; 10];
        let mut seen = 0;
        let trailing = read_frames(&data[..], 4, |frame| {
            assert_eq!(frame.len(), 4);
            seen += 1;
        })
        .unwrap();
        assert_eq!((seen, trailing), (2, 2));
    }
}
