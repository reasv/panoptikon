//! One ffmpeg invocation: the argument vector an artifact's identity implies,
//! and the blocking child that produces its bytes
//! (docs/video-transcoding-implementation.md §1 run.rs).
//!
//! The argument builder reads **only** [`TranscodeParams`] plus the two paths.
//! That is the cache contract made mechanical: the key hashes the params, so
//! any input to the command line that is not in the params would let one key
//! mean two different files.
//!
//! Child hygiene follows `outro.rs`: stderr drained byte-wise on its own
//! thread (a full pipe stalls the encode mid-frame, and ffmpeg copies raw
//! container metadata into its log, so a `lines()` loop would stop at the
//! first non-UTF-8 byte), kill-and-reap on drop, and — unlike the outro
//! probe, whose children last milliseconds — `process_tree` console
//! detachment plus die-with-parent, because an encode can outlive a gateway
//! crash by minutes. The one addition is a cancellation watchdog: an encode
//! lasts long enough for a client to change its mind, and the reader thread
//! cannot notice that on its own while blocked on a pipe.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use super::TranscodeParams;
use super::compose::{ComposeParams, ComposeSource, FilterPlan};
use super::presets::{Channel, Container, QualityMode, ResolvedPreset};
use crate::process_tree::{JobGuard, detach_from_console, die_with_parent, kill_process_group_pid};

/// The two software-x264 invocations, named as encoder *identities* rather
/// than encoder names: the x264 `-preset` changes the output bytes, so it is
/// part of what the cache key must cover (see [`TranscodeParams::encoder`]).
pub(crate) const ENCODER_X264_QUALITY: &str = "libx264-medium";
pub(crate) const ENCODER_X264_FAST: &str = "libx264-veryfast";

/// stderr lines kept for the failure message.
const STDERR_TAIL_LINES: usize = 20;

/// nvenc's `-cq` is coarser than x264's CRF at the same number; the design's
/// tuning table compensates by asking for `crf + 3`.
const NVENC_CQ_OFFSET: i64 = 3;

/// Worst (highest) value on the h264 CRF scale, for encoders whose own knob
/// runs on a different one.
const H264_CRF_MAX: i64 = 51;

/// How often the cancellation watchdog re-reads the flag. Cancelling is a
/// human action, so a fifth of a second is invisible to the client; the
/// thread exists at all because the progress reader can block indefinitely.
const CANCEL_POLL: Duration = Duration::from_millis(200);

/// Everything one encode needs. `params` is the artifact's identity, and the
/// only thing besides the paths that reaches the command line.
#[derive(Debug, Clone)]
pub(crate) struct EncodeJobSpec {
    pub(crate) input: PathBuf,
    /// The cache's temporary; [`super::cache::TranscodeCache::commit`] renames
    /// it into place afterwards.
    pub(crate) output: PathBuf,
    pub(crate) params: TranscodeParams,
    /// Source duration, for turning `out_time` into a fraction. `None` only
    /// costs the progress percentage — never the encode.
    pub(crate) source_duration_s: Option<f64>,
}

/// The composition equivalent: N inputs and one filtergraph instead of one
/// input and a preset's filters.
///
/// The graph itself is *not* carried here. It depends on each source's real
/// stream geometry, which costs an ffprobe per input to learn — a price this
/// keeps off the request path (and off every cache hit) by paying it once, in
/// [`run_compose`], after the job has actually been dispatched.
#[derive(Debug, Clone)]
pub(crate) struct ComposeJobSpec {
    /// Source paths, parallel to `params.doc.items`.
    pub(crate) sources: Vec<PathBuf>,
    pub(crate) output: PathBuf,
    pub(crate) params: ComposeParams,
}

/// One dispatched job, of either kind. The pool treats the two identically —
/// same queue, same cancellation, same progress channel — so the distinction
/// lives here rather than in the actor.
#[derive(Debug, Clone)]
pub(crate) enum EncodeTask {
    Single(Box<EncodeJobSpec>),
    Compose(Box<ComposeJobSpec>),
}

/// What an *injected* runner can see of a task without knowing its kind. The
/// real runner matches on the enum instead, so these exist only for the pool's
/// actor tests.
#[cfg(test)]
impl EncodeTask {
    pub(crate) fn output(&self) -> &std::path::Path {
        match self {
            EncodeTask::Single(spec) => &spec.output,
            EncodeTask::Compose(spec) => &spec.output,
        }
    }

    pub(crate) fn cache_key(&self) -> String {
        match self {
            EncodeTask::Single(spec) => spec.params.cache_key(),
            EncodeTask::Compose(spec) => spec.params.cache_key(),
        }
    }
}

/// Why an encode produced no artifact. The variants map to different
/// consequences, which is the whole reason they are distinguished: `Spawn` is
/// this machine's missing toolchain (a `Blocker`, never a verdict on the
/// file), `Failed` is ffmpeg's verdict on the input and is negative-cacheable,
/// and `Cancelled` is the client's own doing and is recorded nowhere.
#[derive(Debug)]
pub(crate) enum EncodeError {
    Spawn(std::io::Error),
    Failed(String),
    Cancelled,
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::Spawn(err) => write!(f, "ffmpeg failed to start: {err}"),
            EncodeError::Failed(detail) => write!(f, "{detail}"),
            EncodeError::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::error::Error for EncodeError {}

/// The concrete encoder a preset resolves to on this host, as an identity
/// string (see [`TranscodeParams::encoder`]).
///
/// `hw` is the validated hardware H.264 encoder, if any. It may only ever
/// serve an **h264** preset: the probe validates an h264 encoder, so handing
/// it to a VP9 or WebP profile would produce h264 bytes in a webm/webp
/// container. For those the channel is expressive of nothing this function can
/// act on, so they get their software encoder either way.
pub(crate) fn resolve_encoder(preset: &ResolvedPreset, hw: Option<&str>) -> String {
    if is_h264(&preset.vcodec) {
        return match preset.channel {
            Channel::Quality => ENCODER_X264_QUALITY.to_string(),
            Channel::Fast => hw
                .map(str::to_string)
                .unwrap_or_else(|| ENCODER_X264_FAST.to_string()),
        };
    }
    match preset.vcodec.to_ascii_lowercase().as_str() {
        "vp9" => "libvpx-vp9".to_string(),
        "vp8" => "libvpx".to_string(),
        "hevc" | "h265" => "libx265".to_string(),
        // A profile naming an encoder outright (`libwebp_anim`, `libsvtav1`,
        // ...) is taken at its word: the encoder table below is a convenience
        // for codec *families*, not a whitelist.
        _ => preset.vcodec.clone(),
    }
}

fn is_h264(vcodec: &str) -> bool {
    matches!(
        vcodec.to_ascii_lowercase().as_str(),
        "h264" | "avc" | "libx264"
    )
}

/// Seconds as ffmpeg wants them, from centiseconds. Never a float: printing
/// `0.1 * cs` puts binary rounding error into a cut boundary.
///
/// A negative input cannot arrive today (the API rejects negative bounds and
/// rejects `end <= start`), and is clamped rather than trusted: integer
/// division and remainder would print `-1.-23`, which ffmpeg parses as a seek
/// to somewhere else entirely instead of failing.
pub(super) fn seconds(cs: i64) -> String {
    let cs = cs.max(0);
    format!("{}.{:02}", cs / 100, cs % 100)
}

/// How long the output should be, when that is knowable: the trim window, or
/// what remains of the source after the start bound.
fn expected_output_seconds(spec: &EncodeJobSpec) -> Option<f64> {
    let start = spec.params.start_cs.unwrap_or(0);
    match spec.params.end_cs {
        Some(end) => Some(((end - start).max(0) as f64) / 100.0),
        None => spec
            .source_duration_s
            .map(|duration| (duration - (start as f64) / 100.0).max(0.0)),
    }
}

/// The full argument vector, program excluded.
pub(crate) fn build_args(spec: &EncodeJobSpec) -> Vec<OsString> {
    let preset = &spec.params.preset;
    let mut args: Vec<OsString> = Vec::new();
    macro_rules! push {
        ($($value:expr),+ $(,)?) => {{ $(args.push(OsString::from($value));)+ }};
    }

    push!("-nostdin", "-hide_banner", "-nostats", "-v", "error");

    // Input `-ss` (fast seek) rather than output `-ss`: we always re-encode,
    // so the fast seek is also exact, and the decoder skips the leading part
    // of the file instead of decoding and discarding it.
    if let Some(start_cs) = spec.params.start_cs {
        push!("-ss", seconds(start_cs));
    }
    push!("-i");
    args.push(spec.input.clone().into_os_string());
    // Duration, never an absolute `-to`: an input `-ss` resets the output
    // timestamps to zero, so `-to end` would cut at `end - start`.
    if let Some(end_cs) = spec.params.end_cs {
        push!("-t", seconds(end_cs - spec.params.start_cs.unwrap_or(0)));
    }

    push!("-progress", "pipe:1", "-map", "0:v:0");
    if preset.acodec.is_some() {
        // Trailing `?`: a video with no audio stream must produce a silent
        // artifact, not an error.
        push!("-map", "0:a:0?");
    }
    push!("-sn", "-dn");

    if let Some(max_height) = preset.max_height {
        // The quotes are ffmpeg's own escaping, not a shell's: without them
        // the comma inside `min()` reads as a filter separator.
        push!("-vf", format!("scale=-2:'min(ih,{max_height})'"));
    }
    if let Some(fps_max) = preset.fps_max {
        push!("-fpsmax", format!("{fps_max}"));
    }

    for arg in video_args(&spec.params.encoder, preset.quality) {
        args.push(arg);
    }
    match &preset.acodec {
        Some(acodec) => push!("-c:a", acodec.as_str()),
        None => push!("-an"),
    }

    if preset.container == Container::Mp4 {
        // Playback must not wait for the moov atom at the end of the file,
        // and a trimmed cut can start at a negative timestamp.
        push!("-movflags", "+faststart", "-avoid_negative_ts", "make_zero");
    }

    args.push(OsString::from("-pix_fmt"));
    args.push(OsString::from("yuv420p"));
    args.push(OsString::from("-y"));
    args.push(spec.output.clone().into_os_string());
    args
}

/// The argument vector for a composition: the plan's inputs and graph, then
/// the same encoder settings a single-file job of that preset would get.
///
/// Everything specific to *this* composition is in the plan; everything
/// specific to the preset is shared with [`build_args`], which is what keeps a
/// mosaic and a clip of the same preset from drifting apart in quality.
pub(crate) fn build_compose_args(spec: &ComposeJobSpec, plan: &FilterPlan) -> Vec<OsString> {
    let preset = &spec.params.preset;
    let mut args: Vec<OsString> = Vec::new();
    macro_rules! push {
        ($($value:expr),+ $(,)?) => {{ $(args.push(OsString::from($value));)+ }};
    }

    push!("-nostdin", "-hide_banner", "-nostats", "-v", "error");
    for input in &plan.inputs {
        for arg in &input.args {
            args.push(OsString::from(arg));
        }
        push!("-i");
        args.push(input.path.clone().into_os_string());
    }
    push!("-progress", "pipe:1", "-filter_complex", &plan.filter_complex);
    for arg in &plan.output_args {
        args.push(OsString::from(arg));
    }

    for arg in video_args(&spec.params.encoder, preset.quality) {
        args.push(arg);
    }
    // `-an` is already in the plan's output args when the graph produced no
    // audio, so an audio codec here would contradict it.
    if plan.has_audio && let Some(acodec) = &preset.acodec {
        push!("-c:a", acodec.as_str());
    }
    if preset.container == Container::Mp4 {
        push!("-movflags", "+faststart", "-avoid_negative_ts", "make_zero");
    }
    push!("-y");
    args.push(spec.output.clone().into_os_string());
    args
}

/// Encoder selection and rate control. Every branch must map the preset's
/// quality onto *something* the encoder understands: silently dropping it
/// would make two presets that differ only by CRF produce identical bytes
/// under different cache keys.
fn video_args(encoder: &str, quality: QualityMode) -> Vec<OsString> {
    let mut args: Vec<String> = Vec::new();
    let mut codec = |name: &str| {
        args.push("-c:v".to_string());
        args.push(name.to_string());
    };
    match encoder {
        ENCODER_X264_QUALITY => {
            codec("libx264");
            args.extend(["-preset".to_string(), "medium".to_string()]);
            args.extend(crf_or_bitrate(quality, "-crf", 0));
        }
        ENCODER_X264_FAST => {
            codec("libx264");
            args.extend(["-preset".to_string(), "veryfast".to_string()]);
            args.extend(crf_or_bitrate(quality, "-crf", 0));
        }
        "h264_nvenc" => {
            codec("h264_nvenc");
            args.extend(["-preset".to_string(), "p4".to_string()]);
            args.extend(crf_or_bitrate(quality, "-cq", NVENC_CQ_OFFSET));
        }
        "h264_amf" => {
            codec("h264_amf");
            args.extend(["-quality".to_string(), "speed".to_string()]);
            if let QualityMode::Crf(crf) = quality {
                // AMF has no CRF; constant-QP is the closest thing it offers,
                // and without it the encoder falls back to a fixed bitrate
                // that ignores the preset entirely.
                args.extend(["-rc".to_string(), "cqp".to_string()]);
                for key in ["-qp_i", "-qp_p"] {
                    args.push(key.to_string());
                    args.push(crf.to_string());
                }
            } else {
                args.extend(crf_or_bitrate(quality, "-qp_i", 0));
            }
        }
        "h264_qsv" => {
            codec("h264_qsv");
            args.extend(["-preset".to_string(), "veryfast".to_string()]);
            args.extend(crf_or_bitrate(quality, "-global_quality", 0));
        }
        "h264_mf" => {
            codec("h264_mf");
            match quality {
                // MediaFoundation's quality-targeted VBR mode. Its `-quality`
                // is a 0-100 scale on which *higher* is better, the inverse of
                // x264's 0-51 CRF, so the value is mapped linearly (crf 0 →
                // 100, crf 51 → 0). Without this the encoder runs at its own
                // default and two presets differing only by CRF would produce
                // identical bytes under different cache keys.
                QualityMode::Crf(crf) => args.extend([
                    "-rate_control".to_string(),
                    "quality".to_string(),
                    "-quality".to_string(),
                    mf_quality(crf).to_string(),
                ]),
                QualityMode::BitrateKbps(kbps) => {
                    args.extend(["-b:v".to_string(), format!("{kbps}k")])
                }
            }
        }
        "libwebp_anim" => {
            codec("libwebp_anim");
            // libwebp's knob is `-q:v` on a 0-100 quality scale, which
            // `QualityMode::Crf` carries for this container.
            args.extend(crf_or_bitrate(quality, "-q:v", 0));
            args.extend(["-loop".to_string(), "0".to_string()]);
        }
        "libvpx-vp9" | "libvpx" => {
            codec(encoder);
            match quality {
                // Constant quality in libvpx needs an explicit zero target
                // bitrate; without it `-crf` is only an upper bound.
                QualityMode::Crf(crf) => args.extend([
                    "-crf".to_string(),
                    crf.to_string(),
                    "-b:v".to_string(),
                    "0".to_string(),
                ]),
                QualityMode::BitrateKbps(kbps) => {
                    args.extend(["-b:v".to_string(), format!("{kbps}k")])
                }
            }
        }
        other => {
            codec(other);
            args.extend(crf_or_bitrate(quality, "-crf", 0));
        }
    }
    args.into_iter().map(OsString::from).collect()
}

/// An h264 CRF (0-51, lower is better) on MediaFoundation's `-quality` scale
/// (0-100, higher is better).
fn mf_quality(crf: i64) -> i64 {
    (H264_CRF_MAX - crf.clamp(0, H264_CRF_MAX)) * 100 / H264_CRF_MAX
}

/// `key <value>` for a CRF-style scale (offset applied and clamped to a
/// non-negative number), or `-b:v <n>k` for a bitrate target.
fn crf_or_bitrate(quality: QualityMode, crf_key: &str, offset: i64) -> Vec<String> {
    match quality {
        QualityMode::Crf(crf) => vec![crf_key.to_string(), (crf + offset).max(0).to_string()],
        QualityMode::BitrateKbps(kbps) => vec!["-b:v".to_string(), format!("{kbps}k")],
    }
}

/// One `-progress` update.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ProgressUpdate {
    pub(crate) out_time_s: f64,
    /// `None` when the output length is unknown; the job is still running.
    pub(crate) fraction: Option<f32>,
}

/// Turns `-progress pipe:1` key=value lines into fractions.
///
/// ffmpeg prints the output clock three ways per block. `out_time_ms` is the
/// trap: it has carried **microseconds** since the key was introduced (it is
/// a verbatim copy of `out_time_us`), so reading it as milliseconds reports a
/// 1000x overshoot. `out_time_us` is preferred for that reason, and once one
/// has been seen the `_ms` twin is ignored outright.
#[derive(Debug)]
pub(crate) struct ProgressReader {
    duration_s: Option<f64>,
    saw_us_key: bool,
}

impl ProgressReader {
    pub(crate) fn new(duration_s: Option<f64>) -> Self {
        Self {
            duration_s: duration_s.filter(|value| value.is_finite() && *value > 0.0),
            saw_us_key: false,
        }
    }

    /// `Some` when the line moved the output clock.
    pub(crate) fn feed(&mut self, line: &str) -> Option<ProgressUpdate> {
        let (key, value) = line.trim().split_once('=')?;
        let value = value.trim();
        let out_time_s = match key.trim() {
            "out_time_us" => {
                self.saw_us_key = true;
                micros_to_seconds(value)?
            }
            "out_time_ms" if !self.saw_us_key => micros_to_seconds(value)?,
            "out_time" => parse_clock(value)?,
            _ => return None,
        };
        Some(ProgressUpdate {
            out_time_s,
            fraction: self
                .duration_s
                .map(|duration| ((out_time_s / duration) as f32).clamp(0.0, 1.0)),
        })
    }
}

fn micros_to_seconds(value: &str) -> Option<f64> {
    let micros: i64 = value.parse().ok()?;
    (micros >= 0).then(|| micros as f64 / 1_000_000.0)
}

/// `HH:MM:SS.ffffff`, ffmpeg's human-readable form. `N/A` and anything else
/// unparseable is not an update.
fn parse_clock(value: &str) -> Option<f64> {
    let mut parts = value.split(':');
    let hours: f64 = parts.next()?.trim().parse().ok()?;
    let minutes: f64 = parts.next()?.parse().ok()?;
    let seconds: f64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let total = hours * 3600.0 + minutes * 60.0 + seconds;
    (total.is_finite() && total >= 0.0).then_some(total)
}

/// The running child, shared with the cancellation watchdog. The two threads
/// never contend for it: the reader below hands it over for the whole encode
/// and only touches it again after the watchdog has been joined.
#[derive(Clone)]
struct ChildHandle(Arc<Mutex<Child>>);

impl ChildHandle {
    fn new(child: Child) -> Self {
        Self(Arc::new(Mutex::new(child)))
    }

    fn lock(&self) -> MutexGuard<'_, Child> {
        // A poisoned lock means the other thread panicked mid-kill, which
        // makes reaping the child more necessary, not less.
        self.0.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// The kill ladder, and the only one: the process group first (the spawn
    /// made the child its own group leader, so this reaps ffmpeg's own
    /// descendants), then the child, then the reap. The pid cannot have been
    /// recycled here — an exited child nobody has waited for is a zombie
    /// still holding it.
    fn kill(&self) {
        let mut child = self.lock();
        kill_process_group_pid(Some(child.id()));
        let _ = child.kill();
        let _ = child.wait();
    }

    fn wait(&self) -> std::io::Result<ExitStatus> {
        self.lock().wait()
    }
}

/// The spawned encode, so a panic or an early return cannot leave an ffmpeg
/// running against a temporary file nobody will ever claim.
struct EncodeChild {
    child: ChildHandle,
    stderr: Option<JoinHandle<String>>,
}

impl EncodeChild {
    fn stderr_tail(&mut self) -> String {
        self.stderr
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default()
    }
}

impl Drop for EncodeChild {
    fn drop(&mut self) {
        // Deliberately *not* the kill ladder: by the time this runs the child
        // may already have been reaped, and a process-group kill on a pid the
        // OS has since handed out would hit an unrelated tree. Both calls
        // below are no-ops once std has seen the child exit.
        let mut child = self.child.lock();
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// Runs one single-file encode to completion, blocking the calling thread.
pub(crate) fn run_encode(
    spec: &EncodeJobSpec,
    cancel: &AtomicBool,
    on_progress: &mut dyn FnMut(Option<f32>),
) -> Result<(), EncodeError> {
    run_ffmpeg(
        &build_args(spec),
        expected_output_seconds(spec),
        cancel,
        on_progress,
    )
}

/// One composition, end to end: probe the inputs, build the graph from what
/// they turned out to be, and run it.
///
/// The probe is where a source rectangle is clamped to the stream it crops and
/// where an item marked audible loses its audio if the file has none — both of
/// which would otherwise fail the whole graph rather than one item. It runs
/// here, on the job's own thread, so a cache hit never pays for it.
pub(crate) fn run_compose(
    spec: &ComposeJobSpec,
    cancel: &AtomicBool,
    on_progress: &mut dyn FnMut(Option<f32>),
) -> Result<(), EncodeError> {
    if cancel.load(Ordering::Relaxed) {
        return Err(EncodeError::Cancelled);
    }
    let sources: Vec<ComposeSource> = spec
        .sources
        .iter()
        .map(|path| ComposeSource {
            path: path.clone(),
            // A cancellation mid-probe skips the rest: the run below refuses
            // to start anyway, and each probe is its own child process.
            probe: (!cancel.load(Ordering::Relaxed))
                .then(|| super::compose::probe_source(path))
                .flatten(),
        })
        .collect();
    let plan = super::compose::build_filtergraph(&spec.params, &sources);
    run_ffmpeg(
        &build_compose_args(spec, &plan),
        Some(spec.params.target_seconds()),
        cancel,
        on_progress,
    )
}

/// The pool's entry point: whichever kind of job was dispatched.
pub(crate) fn run_task(
    task: &EncodeTask,
    cancel: &AtomicBool,
    on_progress: &mut dyn FnMut(Option<f32>),
) -> Result<(), EncodeError> {
    match task {
        EncodeTask::Single(spec) => run_encode(spec, cancel, on_progress),
        EncodeTask::Compose(spec) => run_compose(spec, cancel, on_progress),
    }
}

/// One ffmpeg child, whatever produced its argument vector, run to completion
/// on the calling thread. `expected_seconds` is the progress denominator.
///
/// `on_progress` is called at ffmpeg's own update rate (roughly twice a
/// second); throttling for consumers is the caller's business.
///
/// `cancel` is honoured by a watchdog thread rather than by the progress loop
/// alone. The loop's own check is only an early exit: ffmpeg writes no
/// `-progress` line at all until it starts producing output, so a source it
/// stalls on (or an encode that outlives the client) would leave the reader
/// blocked on a pipe forever and the pool slot held with it. The watchdog is
/// what turns the flag into an EOF, and it is joined on every exit path, so
/// nothing here relies on the runtime being torn down to reap a child.
fn run_ffmpeg(
    args: &[OsString],
    expected_seconds: Option<f64>,
    cancel: &AtomicBool,
    on_progress: &mut dyn FnMut(Option<f32>),
) -> Result<(), EncodeError> {
    if cancel.load(Ordering::Relaxed) {
        return Err(EncodeError::Cancelled);
    }

    let mut command = Command::new(crate::media_tools::ffmpeg());
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    detach_from_console(&mut command);
    die_with_parent(&mut command);
    let mut child = command.spawn().map_err(EncodeError::Spawn)?;
    let _job = JobGuard::assign(&child);

    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    let drain = std::thread::spawn(move || drain_stderr(stderr));
    let mut child = EncodeChild {
        child: ChildHandle::new(child),
        stderr: Some(drain),
    };

    let mut progress = ProgressReader::new(expected_seconds);
    let mut reader = BufReader::new(stdout);
    // Scoped so the watchdog can borrow `cancel`, and so the scope's join is
    // what gives the child back to this thread before it is waited on.
    std::thread::scope(|scope| {
        let (stop, stopped) = channel::<()>();
        let watched = child.child.clone();
        scope.spawn(move || {
            loop {
                if cancel.load(Ordering::Relaxed) {
                    watched.kill();
                    return;
                }
                match stopped.recv_timeout(CANCEL_POLL) {
                    Err(RecvTimeoutError::Timeout) => continue,
                    // The reader loop is over. Usually that means the encode
                    // finished on its own and there is nothing left to kill —
                    // but the reader breaks on the cancel flag too, and when it
                    // observes the flag *first* the sender is dropped before
                    // the check above ever reads it. Returning here without
                    // re-reading the flag would leave ffmpeg running and the
                    // `wait()` below blocked until the encode completed, which
                    // is the exact stall this thread exists to prevent.
                    _ => {
                        if cancel.load(Ordering::Relaxed) {
                            watched.kill();
                        }
                        return;
                    }
                }
            }
        });

        let mut raw: Vec<u8> = Vec::new();
        loop {
            raw.clear();
            match reader.read_until(b'\n', &mut raw) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            let line = String::from_utf8_lossy(&raw);
            if let Some(update) = progress.feed(&line) {
                on_progress(update.fraction);
            }
            if cancel.load(Ordering::Relaxed) {
                break;
            }
        }
        // Wakes the watchdog out of its poll; the scope then joins it, which
        // is also what waits for a kill already in flight.
        drop(stop);
    });

    let status = match child.child.wait() {
        Ok(status) => status,
        Err(err) => {
            let tail = child.stderr_tail();
            return Err(EncodeError::Failed(format!(
                "waiting for ffmpeg failed: {err}: {tail}"
            )));
        }
    };
    let tail = child.stderr_tail();
    if cancel.load(Ordering::Relaxed) {
        return Err(EncodeError::Cancelled);
    }
    if !status.success() {
        return Err(EncodeError::Failed(format!(
            "ffmpeg exited with {status}: {tail}"
        )));
    }
    Ok(())
}

/// Byte-wise, lossy, and to EOF whatever any single line decodes to: ffmpeg
/// copies container metadata into its log, so a `lines()` loop stops at the
/// first tag holding raw bytes — leaving the pipe to fill and the encode to
/// stall.
fn drain_stderr(stderr: std::process::ChildStderr) -> String {
    let mut reader = BufReader::new(stderr);
    let mut tail: VecDeque<String> = VecDeque::with_capacity(STDERR_TAIL_LINES);
    let mut raw: Vec<u8> = Vec::new();
    loop {
        raw.clear();
        match reader.read_until(b'\n', &mut raw) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        let line = String::from_utf8_lossy(&raw)
            .trim_end_matches(['\n', '\r'])
            .to_string();
        if line.is_empty() {
            continue;
        }
        if tail.len() == STDERR_TAIL_LINES {
            tail.pop_front();
        }
        tail.push_back(line);
    }
    tail.into_iter().collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media_tools::transcode::presets::{Surface, builtin_presets, find_preset};

    fn preset(id: &str) -> ResolvedPreset {
        find_preset(&builtin_presets(), id)
            .unwrap_or_else(|| panic!("{id} ships"))
            .clone()
    }

    fn spec_for(id: &str, start_cs: Option<i64>, end_cs: Option<i64>) -> EncodeJobSpec {
        let preset = preset(id);
        let encoder = resolve_encoder(&preset, None);
        EncodeJobSpec {
            input: PathBuf::from("in.mp4"),
            output: PathBuf::from("out.tmp"),
            params: TranscodeParams::new("sha", preset, encoder, start_cs, end_cs),
            source_duration_s: Some(60.0),
        }
    }

    fn args_of(spec: &EncodeJobSpec) -> Vec<String> {
        build_args(spec)
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    /// The index of `needle` in `args`, for the ordering assertions.
    fn at(args: &[String], needle: &str) -> usize {
        args.iter()
            .position(|arg| arg == needle)
            .unwrap_or_else(|| panic!("{needle} missing from {args:?}"))
    }

    /// Trim bounds: `-ss` before the input (fast seek, exact because we
    /// re-encode), a *duration* after it (an input seek zeroes the output
    /// timestamps, so an absolute `-to` would cut in the wrong place), and
    /// centiseconds formatted as fixed-point rather than through a float.
    #[test]
    fn trim_bounds_become_ss_before_input_and_t_after() {
        let args = args_of(&spec_for("clip", Some(1234), Some(5678)));
        assert_eq!(args[at(&args, "-ss") + 1], "12.34");
        assert!(at(&args, "-ss") < at(&args, "-i"), "{args:?}");
        assert_eq!(args[at(&args, "-t") + 1], "44.44");
        assert!(at(&args, "-t") > at(&args, "-i"), "{args:?}");
        assert!(!args.contains(&"-to".to_string()));

        // Whole-second and sub-second bounds keep two decimals.
        let args = args_of(&spec_for("clip", Some(500), Some(507)));
        assert_eq!(args[at(&args, "-ss") + 1], "5.00");
        assert_eq!(args[at(&args, "-t") + 1], "0.07");

        // An end bound alone measures from the start of the file.
        let args = args_of(&spec_for("clip", None, Some(250)));
        assert!(!args.contains(&"-ss".to_string()));
        assert_eq!(args[at(&args, "-t") + 1], "2.50");

        // A start bound alone runs to the end: no duration at all.
        let args = args_of(&spec_for("clip", Some(250), None));
        assert!(!args.contains(&"-t".to_string()));
    }

    /// The whole clip command line, pinned. The ordering assertions above say
    /// what each piece must satisfy; this says what ffmpeg is actually handed,
    /// so a reordering that still passes them — or a stray flag between the
    /// input and the trim duration — is visible in the diff rather than in an
    /// exported file.
    #[test]
    fn the_trimmed_clip_command_line_is_pinned() {
        assert_eq!(
            args_of(&spec_for("clip", Some(100), Some(794))),
            [
                "-nostdin", "-hide_banner", "-nostats", "-v", "error",
                "-ss", "1.00",
                "-i", "in.mp4",
                "-t", "6.94",
                "-progress", "pipe:1",
                "-map", "0:v:0",
                "-map", "0:a:0?",
                "-sn", "-dn",
                "-c:v", "libx264", "-preset", "medium", "-crf", "18",
                "-c:a", "aac",
                "-movflags", "+faststart",
                "-avoid_negative_ts", "make_zero",
                "-pix_fmt", "yuv420p",
                "-y", "out.tmp",
            ]
        );

        // A start bound alone: the same shape with no duration in it at all.
        let open_ended = args_of(&spec_for("clip", Some(100), None));
        let mut expected = args_of(&spec_for("clip", Some(100), Some(794)));
        expected.drain(at(&expected, "-t")..at(&expected, "-t") + 2);
        assert_eq!(open_ended, expected);
    }

    /// A bound that somehow went negative is clamped, never printed: the
    /// remainder of a negative divisor would spell `-1.-23`, which ffmpeg
    /// reads as a seek to somewhere else rather than as an error.
    #[test]
    fn seconds_never_prints_a_negative_bound() {
        assert_eq!(seconds(0), "0.00");
        assert_eq!(seconds(1234), "12.34");
        assert_eq!(seconds(-1), "0.00");
    }

    /// The scale cap is inserted only for presets that carry one, and its
    /// expression is escaped for ffmpeg's parser (the comma inside `min()`
    /// would otherwise read as a filter separator).
    #[test]
    fn resolution_and_frame_rate_caps_follow_the_preset() {
        let args = args_of(&spec_for("playback", None, None));
        assert_eq!(args[at(&args, "-vf") + 1], "scale=-2:'min(ih,1080)'");
        assert!(!args.contains(&"-fpsmax".to_string()));

        // `clip` is uncapped, so neither filter appears.
        let args = args_of(&spec_for("clip", None, None));
        assert!(!args.contains(&"-vf".to_string()));

        let mut spec = spec_for("clip", None, None);
        spec.params.preset.fps_max = Some(30.0);
        spec.params.preset.max_height = Some(720);
        let args = args_of(&spec);
        assert_eq!(args[at(&args, "-fpsmax") + 1], "30");
        assert!(at(&args, "-vf") < at(&args, "-fpsmax"), "{args:?}");
    }

    /// The channel decides the x264 preset, and a validated hardware encoder
    /// replaces libx264 only on the fast channel.
    #[test]
    fn channel_and_hardware_probe_select_the_encoder() {
        assert_eq!(resolve_encoder(&preset("clip"), None), ENCODER_X264_QUALITY);
        assert_eq!(
            resolve_encoder(&preset("clip"), Some("h264_nvenc")),
            ENCODER_X264_QUALITY,
            "the quality channel never rides on a hardware encoder"
        );
        assert_eq!(
            resolve_encoder(&preset("clip-fast"), None),
            ENCODER_X264_FAST
        );
        assert_eq!(
            resolve_encoder(&preset("clip-fast"), Some("h264_nvenc")),
            "h264_nvenc"
        );

        let quality = args_of(&spec_for("clip", None, None));
        assert_eq!(quality[at(&quality, "-c:v") + 1], "libx264");
        assert_eq!(quality[at(&quality, "-preset") + 1], "medium");
        assert_eq!(quality[at(&quality, "-crf") + 1], "18");

        let mut fast = spec_for("clip-fast", None, None);
        fast.params.encoder = resolve_encoder(&fast.params.preset, Some("h264_nvenc"));
        let args = args_of(&fast);
        assert_eq!(args[at(&args, "-c:v") + 1], "h264_nvenc");
        assert_eq!(args[at(&args, "-preset") + 1], "p4");
        assert_eq!(
            args[at(&args, "-cq") + 1],
            "26",
            "nvenc's scale is coarser, so the tuning table asks for crf + 3"
        );
        assert!(!args.contains(&"-crf".to_string()));
    }

    /// Every hardware branch maps the preset's quality onto something its
    /// encoder understands — the invariant that keeps two presets differing
    /// only by CRF from producing identical bytes under different keys.
    /// MediaFoundation is the awkward one: its scale is inverted and runs to
    /// 100, so the CRF is remapped rather than passed through.
    #[test]
    fn every_encoder_maps_the_presets_quality() {
        let args_for = |encoder: &str, quality| {
            let mut spec = spec_for("clip-fast", None, None);
            spec.params.encoder = encoder.to_string();
            spec.params.preset.quality = quality;
            args_of(&spec)
        };

        // clip-fast is crf 23, so 51 - 23 = 28 of the scale's 51 steps, which
        // is 54 of MediaFoundation's 100.
        let mf = args_for("h264_mf", QualityMode::Crf(23));
        assert_eq!(mf[at(&mf, "-c:v") + 1], "h264_mf");
        assert_eq!(mf[at(&mf, "-rate_control") + 1], "quality");
        assert_eq!(mf[at(&mf, "-quality") + 1], "54");
        // The ends of the scale, and its direction.
        assert_eq!(mf_quality(0), 100);
        assert_eq!(mf_quality(51), 0);
        assert_eq!(mf_quality(-5), 100, "a bound outside the scale clamps");
        assert_eq!(mf_quality(80), 0);
        assert!(mf_quality(18) > mf_quality(28), "lower crf is higher quality");

        // A bitrate profile still reaches it as a bitrate, with no quality
        // mode to contradict it.
        let mf = args_for("h264_mf", QualityMode::BitrateKbps(2500));
        assert_eq!(mf[at(&mf, "-b:v") + 1], "2500k");
        assert!(!mf.contains(&"-rate_control".to_string()));

        // The other vendor branches, for the same reason.
        let amf = args_for("h264_amf", QualityMode::Crf(23));
        assert_eq!(amf[at(&amf, "-rc") + 1], "cqp");
        assert_eq!(amf[at(&amf, "-qp_i") + 1], "23");
        let qsv = args_for("h264_qsv", QualityMode::Crf(23));
        assert_eq!(qsv[at(&qsv, "-global_quality") + 1], "23");
    }

    /// A hardware H.264 encoder may never serve a non-h264 preset: the probe
    /// validated an h264 session, so a webp or vp9 profile handed one would
    /// write h264 bytes into the wrong container.
    #[test]
    fn non_h264_presets_ignore_the_hardware_encoder() {
        for id in ["webp-anim", "mosaic-webm"] {
            let preset = preset(id);
            let with_hw = resolve_encoder(&preset, Some("h264_nvenc"));
            assert_eq!(with_hw, resolve_encoder(&preset, None));
            assert!(!with_hw.starts_with("h264_"), "{id} resolved to {with_hw}");
        }
        assert_eq!(resolve_encoder(&preset("webp-anim"), None), "libwebp_anim");
        assert_eq!(resolve_encoder(&preset("mosaic-webm"), None), "libvpx-vp9");
        // Even a webp profile on the fast channel (which `webp-anim` is).
        assert_eq!(preset("webp-anim").channel, Channel::Fast);
    }

    /// Per-container spelling: webp's quality knob is `-q:v`, vp9 needs an
    /// explicit zero bitrate for constant quality, and faststart is an mp4
    /// container feature that must not be handed to the others.
    #[test]
    fn container_specific_arguments() {
        let webp = args_of(&spec_for("webp-anim", None, None));
        assert_eq!(webp[at(&webp, "-c:v") + 1], "libwebp_anim");
        assert_eq!(webp[at(&webp, "-q:v") + 1], "75");
        assert_eq!(webp[at(&webp, "-loop") + 1], "0");
        assert!(!webp.contains(&"-crf".to_string()));
        assert!(!webp.contains(&"-movflags".to_string()));
        assert!(webp.contains(&"-an".to_string()), "webp carries no audio");
        assert!(
            !webp.contains(&"0:a:0?".to_string()),
            "and maps no audio stream"
        );

        let webm = args_of(&spec_for("mosaic-webm", None, None));
        assert_eq!(webm[at(&webm, "-c:v") + 1], "libvpx-vp9");
        assert_eq!(webm[at(&webm, "-crf") + 1], "32");
        assert_eq!(webm[at(&webm, "-b:v") + 1], "0");
        assert!(!webm.contains(&"-movflags".to_string()));
        assert_eq!(webm[at(&webm, "-c:a") + 1], "opus");

        let mp4 = args_of(&spec_for("clip", None, None));
        assert_eq!(mp4[at(&mp4, "-movflags") + 1], "+faststart");
        assert_eq!(mp4[at(&mp4, "-avoid_negative_ts") + 1], "make_zero");
        assert_eq!(mp4[at(&mp4, "-c:a") + 1], "aac");
        // Optional audio mapping: a video with no audio track is a silent
        // artifact, not a failure.
        assert_eq!(mp4[at(&mp4, "-map") + 1], "0:v:0");
        assert!(mp4.contains(&"0:a:0?".to_string()));
    }

    /// The composition command line, pinned. Its inputs and graph come from
    /// the plan; everything after them is the *same* encoder settings a
    /// single-file job of that preset would get, which is what keeps a mosaic
    /// and a clip of one preset from drifting apart in quality.
    #[test]
    fn the_composition_command_line_is_pinned() {
        use crate::media_tools::transcode::compose::{FilterPlan, InputSpec};

        let preset = preset("mosaic-mp4");
        let encoder = resolve_encoder(&preset, None);
        let doc = crate::media_tools::transcode::compose::ResolvedCompose {
            canvas_w: 640,
            canvas_h: 480,
            background: "0x101820".to_string(),
            fps: 25,
            target_cs: 800,
            items: Vec::new(),
        };
        let spec = ComposeJobSpec {
            sources: vec![PathBuf::from("a.mp4")],
            output: PathBuf::from("out.tmp"),
            params: crate::media_tools::transcode::compose::ComposeParams::new(
                doc, preset, encoder,
            ),
        };
        let plan = FilterPlan {
            inputs: vec![InputSpec {
                args: vec!["-ss".to_string(), "2.00".to_string()],
                path: PathBuf::from("a.mp4"),
            }],
            filter_complex: "[0:v:0]null[vout]".to_string(),
            output_args: vec![
                "-map".to_string(),
                "[vout]".to_string(),
                "-an".to_string(),
                "-sn".to_string(),
                "-dn".to_string(),
                "-pix_fmt".to_string(),
                "yuv420p".to_string(),
            ],
            has_audio: false,
        };
        let args: Vec<String> = build_compose_args(&spec, &plan)
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            [
                "-nostdin", "-hide_banner", "-nostats", "-v", "error",
                "-ss", "2.00",
                "-i", "a.mp4",
                "-progress", "pipe:1",
                "-filter_complex", "[0:v:0]null[vout]",
                "-map", "[vout]", "-an", "-sn", "-dn", "-pix_fmt", "yuv420p",
                "-c:v", "libx264", "-preset", "medium", "-crf", "18",
                "-movflags", "+faststart",
                "-avoid_negative_ts", "make_zero",
                "-y", "out.tmp",
            ]
        );

        // A graph that produced audio gets the preset's codec; the `-an` above
        // and a `-c:a` would contradict each other, so exactly one appears.
        let plan = FilterPlan {
            output_args: vec![
                "-map".to_string(),
                "[vout]".to_string(),
                "-map".to_string(),
                "[aout]".to_string(),
            ],
            has_audio: true,
            ..plan
        };
        let args: Vec<String> = build_compose_args(&spec, &plan)
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args[at(&args, "-c:a") + 1], "aac");
        assert!(!args.contains(&"-an".to_string()));
    }

    /// A bitrate profile reaches every encoder family as a bitrate, and the
    /// invariant that makes the cache key sound: the command line is a
    /// function of the params and the two paths, nothing else.
    #[test]
    fn quality_mode_and_output_paths_reach_the_command_line() {
        let mut spec = spec_for("clip", None, None);
        spec.params.preset.quality = QualityMode::BitrateKbps(2500);
        let args = args_of(&spec);
        assert_eq!(args[at(&args, "-b:v") + 1], "2500k");
        assert!(!args.contains(&"-crf".to_string()));

        let spec = spec_for("clip", None, None);
        let args = args_of(&spec);
        assert_eq!(args[at(&args, "-y") + 1], "out.tmp");
        assert_eq!(args[at(&args, "-i") + 1], "in.mp4");
        assert_eq!(args[at(&args, "-progress") + 1], "pipe:1");
        assert_eq!(args[at(&args, "-pix_fmt") + 1], "yuv420p");
        assert!(args.contains(&"-sn".to_string()) && args.contains(&"-dn".to_string()));
        assert!(preset("clip").surfaces.contains(&Surface::Clip));
    }

    /// The `-progress` parser, including the `out_time_ms` trap: ffmpeg has
    /// always written microseconds into that key, so reading it as
    /// milliseconds would report 1000x the real position.
    #[test]
    fn progress_lines_become_clamped_fractions() {
        let mut reader = ProgressReader::new(Some(10.0));
        assert_eq!(reader.feed("frame=12"), None);
        assert_eq!(reader.feed("fps=0.0"), None);

        let update = reader.feed("out_time_us=2500000").expect("an update");
        assert_eq!(update.out_time_s, 2.5);
        assert_eq!(update.fraction, Some(0.25));
        // The `_ms` twin of a key already seen as `_us` is ignored rather
        // than re-reported at a different scale.
        assert_eq!(reader.feed("out_time_ms=2500000"), None);
        assert_eq!(
            reader.feed("out_time=00:00:05.000000").map(|u| u.out_time_s),
            Some(5.0)
        );
        // Past the expected length (a container whose duration was a
        // rounding away) clamps rather than reporting 130%.
        assert_eq!(
            reader.feed("out_time_us=13000000").and_then(|u| u.fraction),
            Some(1.0)
        );
        assert_eq!(reader.feed("out_time_us=N/A"), None);
        assert_eq!(reader.feed("progress=continue"), None);

        // A build that only prints `_ms` is read as microseconds too.
        let mut reader = ProgressReader::new(Some(10.0));
        assert_eq!(
            reader.feed("out_time_ms=5000000").map(|u| u.out_time_s),
            Some(5.0)
        );

        // No known duration: still an update, just no percentage.
        let mut reader = ProgressReader::new(None);
        let update = reader.feed("out_time_us=1000000").expect("an update");
        assert_eq!(update.fraction, None);
        // A zero or nonsensical duration is the same as an unknown one.
        assert_eq!(
            ProgressReader::new(Some(0.0))
                .feed("out_time_us=1000000")
                .and_then(|u| u.fraction),
            None
        );
    }

    /// The expected output length, which is what a progress fraction is
    /// measured against.
    #[test]
    fn expected_output_length_follows_the_trim_window() {
        assert_eq!(
            expected_output_seconds(&spec_for("clip", Some(100), Some(400))),
            Some(3.0)
        );
        assert_eq!(
            expected_output_seconds(&spec_for("clip", Some(1000), None)),
            Some(50.0)
        );
        assert_eq!(
            expected_output_seconds(&spec_for("clip", None, None)),
            Some(60.0)
        );
        let mut unknown = spec_for("clip", None, None);
        unknown.source_duration_s = None;
        assert_eq!(expected_output_seconds(&unknown), None);
    }

    /// The container's own duration, for the trim-window assertion below.
    fn probe_duration_seconds(path: &std::path::Path) -> Option<f64> {
        let output = Command::new(crate::media_tools::ffprobe())
            .args(["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0"])
            .arg(path)
            .stdin(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8_lossy(&output.stdout).trim().parse().ok()
    }

    /// End to end against the real toolchain: a lavfi fixture is trimmed to a
    /// playback rendition, progress advances monotonically to completion, and
    /// the output is exactly the requested window long — which is the only
    /// mechanical proof that the `-ss`/`-t` pair means what the argument test
    /// above asserts it spells. Skips (never fails) where there is no ffmpeg.
    #[test]
    fn encodes_a_fixture_clip_with_monotone_progress() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.mp4");
        if !crate::jobs::files::write_clip(&source, None, None) {
            return;
        }

        // A window well clear of a rounding: the fixture is 7s, so 1.0s-4.5s
        // exercises both bounds and leaves 2.5s of source unencoded.
        let (start_cs, end_cs) = (100, 450);
        let preset = preset("playback");
        let encoder = resolve_encoder(&preset, None);
        let output = dir.path().join("out.mp4");
        let spec = EncodeJobSpec {
            input: source,
            output: output.clone(),
            params: TranscodeParams::new("sha", preset, encoder, Some(start_cs), Some(end_cs)),
            source_duration_s: Some(7.0),
        };

        let mut seen: Vec<f32> = Vec::new();
        let cancel = AtomicBool::new(false);
        run_encode(&spec, &cancel, &mut |fraction| {
            if let Some(fraction) = fraction {
                seen.push(fraction);
            }
        })
        .expect("the fixture encodes");

        assert!(output.is_file(), "the encode produced its output file");
        assert!(
            output.metadata().unwrap().len() > 0,
            "and the file is not empty"
        );
        assert!(!seen.is_empty(), "progress was reported");
        assert!(
            seen.windows(2).all(|pair| pair[1] >= pair[0]),
            "progress is monotone: {seen:?}"
        );
        assert!(seen.iter().all(|value| (0.0..=1.0).contains(value)));

        let expected = ((end_cs - start_cs) as f64) / 100.0;
        let duration = probe_duration_seconds(&output).expect("the artifact has a duration");
        // Two frames at the fixture's 30 fps: the boundary frame may land
        // either side of the cut, and the container rounds its duration field.
        assert!(
            (duration - expected).abs() <= 2.0 / 30.0,
            "the artifact is the trim window long: {duration} vs {expected}"
        );
    }

    /// A source whose *encode* lasts minutes but whose *production* costs
    /// nothing: one small clip stream-copied end to end. Building a long source
    /// by generating one would cost the test the very seconds the assertion
    /// below is measured in.
    fn loop_clip(seed: &std::path::Path, output: &std::path::Path, times: u32) -> bool {
        let status = Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error", "-stream_loop"])
            .arg(times.to_string())
            .arg("-i")
            .arg(seed)
            .args(["-c", "copy"])
            .arg(output)
            .stdin(Stdio::null())
            .status();
        matches!(status, Ok(status) if status.success())
    }

    /// The watchdog's raced path. The progress reader breaks on the cancel flag
    /// too, and when it observes the flag *first* it drops the stop sender, so
    /// the watchdog wakes on a disconnect rather than on its own poll. It must
    /// still kill the child there: nothing else will, and the `wait()` after
    /// the scope would then block until the encode finished on its own — for
    /// this fixture, minutes.
    ///
    /// The ordering is forced rather than hoped for. The flag is set from
    /// inside the progress callback, which runs *on the reader thread*, so the
    /// reader's own check of it is nanoseconds away while the watchdog is
    /// parked in a `CANCEL_POLL` wait.
    ///
    /// `run_encode` blocks, so it runs on a thread of its own and the assertion
    /// is a bounded wait: a regression fails in seconds rather than hanging for
    /// the length of the encode. Skips (never fails) where there is no ffmpeg.
    #[test]
    fn a_cancel_the_reader_notices_first_still_kills_ffmpeg() {
        /// Enough copies of a 7s clip that the encode cannot plausibly finish
        /// inside the budget below on any machine.
        const LOOPS: u32 = 400;
        /// Generous next to the sub-second return this is really asserting;
        /// the point is only that it is far short of the encode.
        const BUDGET: Duration = Duration::from_secs(15);

        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let seed = dir.path().join("seed.mp4");
        if !crate::jobs::files::write_clip(&seed, None, None) {
            return;
        }
        let source = dir.path().join("long.mp4");
        if !loop_clip(&seed, &source, LOOPS) {
            return;
        }

        let preset = preset("clip");
        let encoder = resolve_encoder(&preset, None);
        let spec = EncodeJobSpec {
            input: source,
            output: dir.path().join("out.mp4"),
            params: TranscodeParams::new("sha", preset, encoder, None, None),
            source_duration_s: None,
        };

        let (report, finished) = channel();
        std::thread::spawn(move || {
            let cancel = AtomicBool::new(false);
            let started = std::time::Instant::now();
            let outcome = run_encode(&spec, &cancel, &mut |_| {
                cancel.store(true, Ordering::Relaxed);
            });
            let _ = report.send((outcome, started.elapsed()));
        });

        let (outcome, elapsed) = finished.recv_timeout(BUDGET).unwrap_or_else(|_| {
            panic!("run_encode did not return within {BUDGET:?} of the cancellation: the watchdog left ffmpeg running")
        });
        assert!(
            matches!(outcome, Err(EncodeError::Cancelled)),
            "cancelling is the client's own doing, so it is a verdict on nothing: {outcome:?} after {elapsed:?}"
        );
    }

    /// A cancellation set before the spawn never starts ffmpeg at all, and a
    /// nonexistent input is ffmpeg's verdict (`Failed`), not a spawn failure.
    #[test]
    fn cancellation_and_input_failures_are_distinguished() {
        let dir = tempfile::tempdir().unwrap();
        let spec = EncodeJobSpec {
            input: dir.path().join("nothing.mp4"),
            output: dir.path().join("out.mp4"),
            params: TranscodeParams::new("sha", preset("clip"), ENCODER_X264_FAST.to_string(), None, None),
            source_duration_s: None,
        };

        let cancelled = AtomicBool::new(true);
        assert!(matches!(
            run_encode(&spec, &cancelled, &mut |_| {}),
            Err(EncodeError::Cancelled)
        ));

        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let cancel = AtomicBool::new(false);
        match run_encode(&spec, &cancel, &mut |_| {}) {
            Err(EncodeError::Failed(detail)) => {
                assert!(detail.contains("ffmpeg exited"), "{detail}");
                assert!(
                    !detail.trim().ends_with(':'),
                    "the stderr tail is carried into the verdict: {detail}"
                );
            }
            other => panic!("expected a verdict on the input, got {other:?}"),
        }
    }
}
