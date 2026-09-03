//! Encoding presets: the built-in table, the config-side patch type, and the
//! tri-state resolution that merges them
//! (docs/video-transcoding-design.md §5).
//!
//! A *resolved* preset is what the cache key hashes, so anything that changes
//! output bytes belongs on [`ResolvedPreset`] and anything presentational
//! (`label`, `surfaces`) is skipped by its serialization: renaming a preset or
//! moving it between UI surfaces must not orphan the artifacts it produced.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Ceiling on a preset id. Ids travel in request bodies and in the presets
/// DTO; the identifier charset is the same one database and policy names use.
const MAX_PRESET_ID_LEN: usize = 64;

/// The one `vcodec` value that names no codec at all: the video packets are
/// remuxed as they are, with no decode and no encode
/// (docs/video-hover-preview-implementation.md §4, B6). ffmpeg's own spelling,
/// so a profile that writes it reads exactly like the `-c:v copy` it becomes.
pub(crate) const STREAM_COPY_VCODEC: &str = "copy";

/// Ceiling on a profile's frame-rate cap. Well past anything a browser plays;
/// it exists so a typo cannot ask ffmpeg for an absurd `-fpsmax`.
const MAX_FPS_MAX: f64 = 240.0;

/// Output container. Fixes the file extension, the MIME type the artifact is
/// served with, and whether an audio stream is possible at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Container {
    Mp4,
    Webm,
    Webp,
    Avif,
}

impl Container {
    pub(crate) fn ext(self) -> &'static str {
        match self {
            Container::Mp4 => "mp4",
            Container::Webm => "webm",
            Container::Webp => "webp",
            Container::Avif => "avif",
        }
    }

    pub(crate) fn mime_type(self) -> &'static str {
        match self {
            Container::Mp4 => "video/mp4",
            Container::Webm => "video/webm",
            Container::Webp => "image/webp",
            Container::Avif => "image/avif",
        }
    }

    /// Whether the container can carry an audio stream at all. The animated
    /// image containers cannot, so a profile pairing one with an `acodec` is a
    /// config error rather than something ffmpeg silently drops.
    fn carries_audio(self) -> bool {
        !self.is_animated_image()
    }

    /// The all-frames-in-one-image-file containers, which the animated-image
    /// length cap applies to (video containers stream and seek; these decode
    /// whole).
    pub(crate) fn is_animated_image(self) -> bool {
        matches!(self, Container::Webp | Container::Avif)
    }

    /// Widest rate-control value the encoder behind this container accepts:
    /// x264 stops at 51 and vp9 at 63, while libwebp's `-q:v` is a 0-100
    /// quality scale that [`QualityMode::Crf`] carries too. A value past the
    /// scale is not clamped by ffmpeg, it is a different picture than the one
    /// the user asked for.
    fn max_crf(self) -> i64 {
        match self {
            Container::Mp4 => 51,
            Container::Webm | Container::Avif => 63,
            Container::Webp => 100,
        }
    }
}

/// Which encoder family a preset draws from: `Quality` is software x264 at a
/// decent CRF, `Fast` is the validated hardware encoder when there is one
/// (design §5 — hardware encoders are meaningfully worse per bit, so export
/// quality never rides on them).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Quality,
    Fast,
}

/// Where the UI may offer a preset. Presets carry their own surfaces so a
/// user-declared profile appears in the right dropdowns with no client change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Surface {
    Playback,
    Clip,
    Mosaic,
    /// The grid/filmstrip hover preview
    /// (docs/video-hover-preview-implementation.md). Its own surface rather
    /// than a second `Playback` preset: a policy that offers the gallery's
    /// full-size rendition need not also pay for a preview of every cell the
    /// pointer crosses, and the UI never offers this one in a dropdown.
    Preview,
}

/// Rate control. `Crf` is the quality target every built-in uses (and the
/// carrier for libwebp's `-q:v` scale); `BitrateKbps` exists for profiles that
/// must hit a size target instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QualityMode {
    Crf(i64),
    BitrateKbps(i64),
}

/// A preset with every setting decided. The serialization is one half of the
/// cache key, so field order and the two `skip`s are load-bearing: changing
/// either re-keys every artifact and requires a `TRANSCODER_VERSION` bump.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResolvedPreset {
    pub(crate) id: String,
    #[serde(skip)]
    pub(crate) label: String,
    pub(crate) container: Container,
    /// Codec *family* (`h264`, `vp9`, `libwebp_anim`), not the encoder: which
    /// encoder implements it is a host property resolved from the channel and
    /// the hardware probe.
    pub(crate) vcodec: String,
    /// `None` means the output has no audio stream.
    pub(crate) acodec: Option<String>,
    /// `None` only for a stream-copy preset, which has no rate control to
    /// carry. `Some(q)` serializes exactly as the bare `q` did before the
    /// option was introduced, so no existing artifact was re-keyed by it.
    pub(crate) quality: Option<QualityMode>,
    /// Cap on output height in pixels; `None` keeps the source height.
    pub(crate) max_height: Option<i64>,
    pub(crate) fps_max: Option<f64>,
    pub(crate) channel: Channel,
    #[serde(skip)]
    pub(crate) surfaces: Vec<Surface>,
}

impl ResolvedPreset {
    /// Whether this preset remuxes rather than encodes. Load-time validation
    /// guarantees such a preset carries no rate control, no resolution or
    /// frame-rate cap and no audio, so every consumer can read those as
    /// "nothing to apply" rather than re-deriving the rule.
    pub(crate) fn is_stream_copy(&self) -> bool {
        is_stream_copy_vcodec(&self.vcodec)
    }
}

fn is_stream_copy_vcodec(vcodec: &str) -> bool {
    vcodec.trim().eq_ignore_ascii_case(STREAM_COPY_VCODEC)
}

/// `[transcode.profiles.<name>]`: a patch over a built-in, or a whole new
/// preset. Every field is optional so an override can change exactly one
/// setting of a built-in; a name that matches no built-in must supply at least
/// `container`, `vcodec` and a rate control (checked at config load).
#[derive(Debug, Clone, Deserialize)]
pub struct TranscodeProfileConfig {
    #[serde(default)]
    pub(crate) label: Option<String>,
    #[serde(default)]
    pub(crate) container: Option<Container>,
    #[serde(default)]
    pub(crate) vcodec: Option<String>,
    #[serde(default)]
    pub(crate) acodec: Option<String>,
    /// `false` drops the audio stream. Setting it alone is how a patch turns
    /// off a built-in's audio without knowing which codec it used.
    #[serde(default)]
    pub(crate) audio: Option<bool>,
    #[serde(default)]
    pub(crate) crf: Option<i64>,
    #[serde(default)]
    pub(crate) bitrate_kbps: Option<i64>,
    /// `0` means uncapped, which is how a patch clears a built-in's cap.
    #[serde(default)]
    pub(crate) max_height: Option<i64>,
    /// `0` means uncapped.
    #[serde(default)]
    pub(crate) fps_max: Option<f64>,
    #[serde(default)]
    pub(crate) channel: Option<Channel>,
    #[serde(default)]
    pub(crate) surfaces: Option<Vec<Surface>>,
}

/// The shipped preset table (implementation plan §1). Tunable in code and
/// never shipped as live TOML lines, so a future retune reaches existing
/// installs — at the cost of re-keying their artifacts, which is what
/// `TRANSCODER_VERSION` is for.
pub(crate) fn builtin_presets() -> Vec<ResolvedPreset> {
    /// id, label, container, vcodec, acodec, quality, max_height, fps_max,
    /// channel, surfaces.
    type Row = (
        &'static str,
        &'static str,
        Container,
        &'static str,
        Option<&'static str>,
        Option<QualityMode>,
        Option<i64>,
        Option<f64>,
        Channel,
        &'static [Surface],
    );

    const TABLE: [Row; 10] = [
        (
            "playback",
            "Playback",
            Container::Mp4,
            "h264",
            Some("aac"),
            Some(QualityMode::Crf(23)),
            Some(1080),
            None,
            Channel::Fast,
            &[Surface::Playback],
        ),
        (
            "preview",
            "Hover preview",
            Container::Mp4,
            "h264",
            // Silent by design, not by accident: a grid full of cells the
            // pointer skims cannot ask for audio, and `-an` costs nothing to
            // encode. The container carries audio, so this is the one built-in
            // mp4 that deliberately declines it.
            None,
            // A picture at most a few hundred CSS pixels wide, thrown away as
            // soon as the pointer leaves: latency and bandwidth are the whole
            // point, quality is not.
            Some(QualityMode::Crf(26)),
            Some(480),
            Some(30.0),
            Channel::Fast,
            &[Surface::Preview],
        ),
        (
            "preview-trim",
            "Hover preview (trimmed)",
            Container::Mp4,
            // No decode, no encode: the source's own h264 packets remuxed
            // into a short mp4. It exists because rung 0 — playing the
            // original — turned out to cost megabytes a hover (Chromium
            // buffers as far ahead as it likes, measured 9.9 MB of a 15.7 MB
            // file for one three-second dwell), so a file over the byte cap
            // is served its first seconds instead of its whole self.
            STREAM_COPY_VCODEC,
            None,
            // A stream copy has no rate control, no scale and no frame-rate
            // cap: there is nothing to decide, only packets to move.
            None,
            None,
            None,
            // I/O-bound rather than CPU-bound, so the channel is only a
            // statement of intent; `resolve_encoder` never reaches the
            // hardware probe for a copy.
            Channel::Fast,
            &[Surface::Preview],
        ),
        (
            "clip",
            "Clip",
            Container::Mp4,
            "h264",
            Some("aac"),
            Some(QualityMode::Crf(18)),
            None,
            None,
            Channel::Quality,
            &[Surface::Clip],
        ),
        (
            "clip-fast",
            "Clip (fast)",
            Container::Mp4,
            "h264",
            Some("aac"),
            Some(QualityMode::Crf(23)),
            None,
            None,
            Channel::Fast,
            &[Surface::Clip],
        ),
        (
            "webp-anim",
            "Animated WebP",
            Container::Webp,
            "libwebp_anim",
            None,
            // libwebp's `-q:v` 0-100 quality scale rides in `Crf`. 85, not
            // the libwebp default of 75: lossy WebP is intra-only VP8, so on
            // video content 75 blocks visibly, and the gif-substitute use
            // (Discord/Matrix paste) has size headroom to spend on quality.
            Some(QualityMode::Crf(85)),
            Some(720),
            // Halves 60 fps sources cleanly; 24/25/30 pass through untouched
            // (a 24-cap would judder 30 fps content).
            Some(30.0),
            Channel::Fast,
            &[Surface::Clip, Surface::Mosaic],
        ),
        (
            "avif-anim",
            "Animated AVIF",
            Container::Avif,
            "av1",
            None,
            // A real inter-coded video codec in an image container, so it
            // beats webp-anim on both size and quality; second in the lineup
            // only because far fewer destinations animate it.
            Some(QualityMode::Crf(30)),
            Some(720),
            Some(30.0),
            Channel::Fast,
            &[Surface::Clip, Surface::Mosaic],
        ),
        (
            "mosaic-mp4",
            "MP4",
            Container::Mp4,
            "h264",
            Some("aac"),
            Some(QualityMode::Crf(18)),
            None,
            None,
            Channel::Quality,
            &[Surface::Mosaic],
        ),
        (
            "mosaic-mp4-fast",
            "MP4 (fast)",
            Container::Mp4,
            "h264",
            Some("aac"),
            Some(QualityMode::Crf(23)),
            None,
            None,
            Channel::Fast,
            &[Surface::Mosaic],
        ),
        (
            "mosaic-webm",
            "WebM",
            Container::Webm,
            "vp9",
            Some("opus"),
            Some(QualityMode::Crf(32)),
            None,
            None,
            Channel::Quality,
            &[Surface::Mosaic],
        ),
    ];

    TABLE
        .iter()
        .map(
            |(
                id,
                label,
                container,
                vcodec,
                acodec,
                quality,
                max_height,
                fps_max,
                channel,
                surfaces,
            )| {
                ResolvedPreset {
                    id: (*id).to_string(),
                    label: (*label).to_string(),
                    container: *container,
                    vcodec: (*vcodec).to_string(),
                    acodec: acodec.map(str::to_string),
                    quality: *quality,
                    max_height: *max_height,
                    fps_max: *fps_max,
                    channel: *channel,
                    surfaces: surfaces.to_vec(),
                }
            },
        )
        .collect()
}

/// Tri-state resolution, copying `VectorQuantsConfig`: the section absent
/// means the built-ins, an explicit empty table means no presets at all, and
/// entries are merged by name over the built-ins.
///
/// Infallible: `validate_profiles` rejects an unresolvable profile at config
/// load, so a failure here can only be a profile that became invalid later
/// (a built-in it patched was retired). Such a profile is dropped with a
/// warning rather than taking the whole preset list down with it.
///
pub(crate) fn resolve_presets(
    profiles: Option<&BTreeMap<String, TranscodeProfileConfig>>,
) -> Vec<ResolvedPreset> {
    let Some(profiles) = profiles else {
        return builtin_presets();
    };
    if profiles.is_empty() {
        return Vec::new();
    }
    let mut resolved = builtin_presets();
    for (name, patch) in profiles {
        let base = resolved.iter().position(|preset| preset.id == *name);
        match resolve_one(name, patch, base.map(|at| &resolved[at])) {
            Ok(preset) => match base {
                Some(at) => resolved[at] = preset,
                None => resolved.push(preset),
            },
            Err(err) => tracing::warn!(
                profile = name.as_str(),
                error = %err,
                "ignoring an unresolvable transcode profile"
            ),
        }
    }
    resolved
}

pub(crate) fn find_preset<'a>(
    presets: &'a [ResolvedPreset],
    id: &str,
) -> Option<&'a ResolvedPreset> {
    presets.iter().find(|preset| preset.id == id)
}

/// Config-load validation for `[transcode.profiles]`: every entry must resolve
/// into a complete preset, so an incomplete novel profile fails at startup
/// rather than at the first transcode request.
pub(crate) fn validate_profiles(
    profiles: Option<&BTreeMap<String, TranscodeProfileConfig>>,
) -> Result<()> {
    let Some(profiles) = profiles else {
        return Ok(());
    };
    let builtins = builtin_presets();
    for (name, patch) in profiles {
        if !crate::config::is_safe_identifier(name, MAX_PRESET_ID_LEN) {
            bail!(
                "transcode.profiles name '{name}' is invalid: names must be 1-{MAX_PRESET_ID_LEN} \
                 characters from [a-zA-Z0-9._-]"
            );
        }
        resolve_one(name, patch, find_preset(&builtins, name))
            .with_context(|| format!("transcode.profiles.{name} is incomplete"))?;
    }
    Ok(())
}

/// Applies one config patch to its built-in base (`None` for a novel name).
fn resolve_one(
    name: &str,
    patch: &TranscodeProfileConfig,
    base: Option<&ResolvedPreset>,
) -> Result<ResolvedPreset> {
    // The container decides the CRF scale, so it is resolved first and the
    // range is checked against the *resolved* quality: a patch that only
    // changes the container inherits a value from a different scale.
    let container = patch
        .container
        .or_else(|| base.map(|preset| preset.container))
        .context("container is required")?;
    let explicit_quality = match (patch.crf, patch.bitrate_kbps) {
        (Some(_), Some(_)) => bail!("crf and bitrate_kbps are mutually exclusive"),
        (Some(crf), None) => Some(QualityMode::Crf(crf)),
        (None, Some(kbps)) => {
            if kbps <= 0 {
                bail!("bitrate_kbps must be positive");
            }
            Some(QualityMode::BitrateKbps(kbps))
        }
        (None, None) => None,
    };

    let vcodec = patch
        .vcodec
        .clone()
        .or_else(|| base.map(|preset| preset.vcodec.clone()))
        .context("vcodec is required")?;
    if vcodec.trim().is_empty() {
        bail!("vcodec must not be empty");
    }
    // A stream copy has nothing to decide: no rate control, no scale, no
    // frame-rate cap, no audio. Settings it *inherits* from a base are
    // dropped (otherwise `vcodec = "copy"` over a built-in would be
    // unexpressible, there being no way to clear a crf); settings the patch
    // itself names are refused, because they say two contradictory things
    // about the same output in one entry.
    let copies = is_stream_copy_vcodec(&vcodec);
    if copies && container.is_animated_image() {
        bail!(
            "container {} cannot stream-copy: its frames are re-encoded by definition",
            container.ext()
        );
    }

    let quality = if copies {
        if explicit_quality.is_some() {
            bail!("vcodec = \"copy\" re-encodes nothing, so it takes no crf or bitrate_kbps");
        }
        None
    } else {
        let quality = explicit_quality
            .or_else(|| base.and_then(|preset| preset.quality))
            .context("crf or bitrate_kbps is required")?;
        if let QualityMode::Crf(crf) = quality {
            let max = container.max_crf();
            if !(0..=max).contains(&crf) {
                bail!(
                    "crf must be between 0 and {max} for container {}",
                    container.ext()
                );
            }
        }
        Some(quality)
    };

    let acodec = if copies {
        if patch.acodec.is_some() || patch.audio == Some(true) {
            bail!("vcodec = \"copy\" copies the video stream only, so it takes no audio");
        }
        None
    } else {
        resolve_acodec(patch, base)?
    };
    if acodec.is_some() && !container.carries_audio() {
        bail!("container {} cannot carry an audio stream", container.ext());
    }

    let max_height = match patch.max_height {
        Some(0) => None,
        Some(height) if height < 0 => bail!("max_height must not be negative"),
        Some(height) if copies => {
            bail!("vcodec = \"copy\" cannot rescale, so max_height must be 0 or absent ({height})")
        }
        Some(height) => Some(height),
        None if copies => None,
        None => base.and_then(|preset| preset.max_height),
    };
    let fps_max = match patch.fps_max {
        None if copies => None,
        None => base.and_then(|preset| preset.fps_max),
        Some(fps) => {
            if !fps.is_finite() || !(0.0..=MAX_FPS_MAX).contains(&fps) {
                bail!("fps_max must be between 0 and {MAX_FPS_MAX}");
            }
            if copies && fps > 0.0 {
                bail!("vcodec = \"copy\" cannot resample, so fps_max must be 0 or absent");
            }
            (fps > 0.0).then_some(fps)
        }
    };

    Ok(ResolvedPreset {
        id: name.to_string(),
        label: patch
            .label
            .clone()
            .or_else(|| base.map(|preset| preset.label.clone()))
            .unwrap_or_else(|| name.to_string()),
        container,
        vcodec,
        acodec,
        quality,
        max_height,
        fps_max,
        channel: patch
            .channel
            .or_else(|| base.map(|preset| preset.channel))
            .unwrap_or(Channel::Quality),
        surfaces: patch
            .surfaces
            .clone()
            .or_else(|| base.map(|preset| preset.surfaces.clone()))
            .unwrap_or_default(),
    })
}

fn resolve_acodec(
    patch: &TranscodeProfileConfig,
    base: Option<&ResolvedPreset>,
) -> Result<Option<String>> {
    if patch.audio == Some(false) {
        if patch.acodec.is_some() {
            bail!("audio = false contradicts the acodec set in the same profile");
        }
        return Ok(None);
    }
    if let Some(acodec) = patch.acodec.as_deref() {
        if acodec.trim().is_empty() {
            bail!("acodec must not be empty; set audio = false to drop the audio stream");
        }
        return Ok(Some(acodec.to_string()));
    }
    let inherited = base.and_then(|preset| preset.acodec.clone());
    if patch.audio == Some(true) && inherited.is_none() {
        bail!("audio = true needs an acodec");
    }
    Ok(inherited)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profiles(entries: &[(&str, &str)]) -> BTreeMap<String, TranscodeProfileConfig> {
        entries
            .iter()
            .map(|(name, body)| {
                (
                    (*name).to_string(),
                    toml::from_str::<TranscodeProfileConfig>(body)
                        .unwrap_or_else(|err| panic!("profile {name} should parse: {err}")),
                )
            })
            .collect()
    }

    /// The shipped table is what the params hash and every UI dropdown are
    /// built from: ids are unique, and each preset's channel/surfaces match
    /// the plan's table.
    #[test]
    fn builtin_table_is_the_shipped_one() {
        let presets = builtin_presets();
        let ids: Vec<&str> = presets.iter().map(|preset| preset.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "playback",
                "preview",
                "preview-trim",
                "clip",
                "clip-fast",
                "webp-anim",
                "avif-anim",
                "mosaic-mp4",
                "mosaic-mp4-fast",
                "mosaic-webm",
            ]
        );

        let playback = find_preset(&presets, "playback").unwrap();
        assert_eq!(playback.container, Container::Mp4);
        assert_eq!(playback.quality, Some(QualityMode::Crf(23)));
        assert_eq!(playback.max_height, Some(1080));
        assert_eq!(playback.channel, Channel::Fast);
        assert!(playback.surfaces.contains(&Surface::Playback));

        // The hover preview: the one built-in that pairs an audio-capable
        // container with no audio stream, and the only h264 built-in that caps
        // both height and frame rate. Every number here is a client contract
        // in miniature — the 16 s window the UI asks for is trim, not preset,
        // but the 480 and the silence are what make a skimmed grid affordable.
        let preview = find_preset(&presets, "preview").unwrap();
        assert_eq!(preview.container, Container::Mp4);
        assert_eq!(preview.vcodec, "h264");
        assert_eq!(preview.acodec, None);
        assert_eq!(preview.quality, Some(QualityMode::Crf(26)));
        assert_eq!(preview.max_height, Some(480));
        assert_eq!(preview.fps_max, Some(30.0));
        assert_eq!(preview.channel, Channel::Fast);
        assert_eq!(preview.surfaces, vec![Surface::Preview]);
        // Two rungs, two presets, one surface: the remux and the re-encode
        // are both hover previews and neither belongs in a dropdown.
        let preview_trim = find_preset(&presets, "preview-trim").unwrap();
        assert_eq!(preview_trim.container, Container::Mp4);
        assert_eq!(preview_trim.vcodec, STREAM_COPY_VCODEC);
        assert!(preview_trim.is_stream_copy());
        assert_eq!(preview_trim.acodec, None);
        assert_eq!(preview_trim.quality, None, "a copy has no rate control");
        assert_eq!(preview_trim.max_height, None, "and cannot rescale");
        assert_eq!(preview_trim.fps_max, None, "or resample");
        assert_eq!(preview_trim.surfaces, vec![Surface::Preview]);
        assert!(
            !preview.is_stream_copy(),
            "the 480p rung really encodes; only its cheaper sibling copies"
        );

        // `quality` became an `Option` when the copy rung arrived, and that
        // must have re-keyed nothing: `Some(q)` and a bare `q` serialize
        // identically, and the serialization is half the cache key.
        assert_eq!(
            serde_json::to_string(&Some(QualityMode::Crf(23))).unwrap(),
            serde_json::to_string(&QualityMode::Crf(23)).unwrap(),
            "wrapping the quality in an Option must not orphan every artifact"
        );

        let clip = find_preset(&presets, "clip").unwrap();
        assert_eq!(clip.quality, Some(QualityMode::Crf(18)));
        assert_eq!(clip.channel, Channel::Quality);
        assert_eq!(clip.max_height, None);

        // The animated-image presets carry no audio track and are the only
        // frame-rate-capped built-ins; mosaic-webm is the one built-in that is
        // not h264/aac.
        let webp = find_preset(&presets, "webp-anim").unwrap();
        assert_eq!(webp.container, Container::Webp);
        assert_eq!(webp.acodec, None);
        assert_eq!(webp.quality, Some(QualityMode::Crf(85)));
        assert_eq!(webp.fps_max, Some(30.0));
        assert!(webp.surfaces.contains(&Surface::Clip) && webp.surfaces.contains(&Surface::Mosaic));
        let avif = find_preset(&presets, "avif-anim").unwrap();
        assert_eq!(avif.container, Container::Avif);
        assert_eq!(avif.vcodec, "av1");
        assert_eq!(avif.acodec, None);
        assert_eq!(avif.quality, Some(QualityMode::Crf(30)));
        assert_eq!(avif.max_height, Some(720));
        assert_eq!(avif.fps_max, Some(30.0));
        assert_eq!(avif.surfaces, webp.surfaces);
        assert!(
            presets
                .iter()
                .all(|preset| !preset.container.is_animated_image() || preset.fps_max.is_some()),
            "every animated-image preset is fps-capped"
        );
        let capped: Vec<&str> = presets
            .iter()
            .filter(|preset| preset.fps_max.is_some())
            .map(|preset| preset.id.as_str())
            .collect();
        assert_eq!(
            capped,
            ["preview", "webp-anim", "avif-anim"],
            "the fps cap is the animated-image containers plus the hover preview, \
             whose cells are too small for a 60 fps source to be worth decoding"
        );
        let webm = find_preset(&presets, "mosaic-webm").unwrap();
        assert_eq!(webm.vcodec, "vp9");
        assert_eq!(webm.acodec.as_deref(), Some("opus"));
        assert_eq!(webm.quality, Some(QualityMode::Crf(32)));
    }

    /// The tri-state: absent means the built-ins, an explicit empty table
    /// means no presets at all, entries merge by name.
    #[test]
    fn resolution_is_tri_state() {
        assert_eq!(resolve_presets(None), builtin_presets());
        assert!(resolve_presets(Some(&BTreeMap::new())).is_empty());

        let map = profiles(&[(
            "small-share",
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 28",
        )]);
        let resolved = resolve_presets(Some(&map));
        assert_eq!(resolved.len(), builtin_presets().len() + 1);
        let novel = find_preset(&resolved, "small-share").unwrap();
        assert_eq!(novel.quality, Some(QualityMode::Crf(28)));
        // Nothing inherited: a novel profile with no acodec has no audio, and
        // its label defaults to its own name.
        assert_eq!(novel.acodec, None);
        assert_eq!(novel.label, "small-share");
        assert_eq!(novel.channel, Channel::Quality);
        assert!(novel.surfaces.is_empty());
    }

    /// A patch changes exactly the fields it names and inherits the rest, and
    /// it replaces the built-in in place rather than appending a duplicate.
    #[test]
    fn patches_merge_over_built_ins_by_name() {
        let map = profiles(&[("clip", "crf = 14\nlabel = \"Clip (archival)\"")]);
        let resolved = resolve_presets(Some(&map));
        assert_eq!(resolved.len(), builtin_presets().len());
        let clip = find_preset(&resolved, "clip").unwrap();
        assert_eq!(clip.quality, Some(QualityMode::Crf(14)));
        assert_eq!(clip.label, "Clip (archival)");
        assert_eq!(clip.container, Container::Mp4);
        assert_eq!(clip.acodec.as_deref(), Some("aac"));
        assert_eq!(clip.channel, Channel::Quality);
        assert_eq!(clip.surfaces, vec![Surface::Clip]);
        // Built-in ordering survives a patch, so the presets DTO stays stable.
        assert_eq!(resolved[3].id, "clip");

        // The clearing conventions: audio = false drops the stream, a 0 cap
        // means uncapped, and bitrate_kbps replaces an inherited crf.
        let map = profiles(&[(
            "playback",
            "audio = false\nmax_height = 0\nbitrate_kbps = 2500",
        )]);
        let resolved = resolve_presets(Some(&map));
        let playback = find_preset(&resolved, "playback").unwrap();
        assert_eq!(playback.acodec, None);
        assert_eq!(playback.max_height, None);
        assert_eq!(playback.quality, Some(QualityMode::BitrateKbps(2500)));
    }

    /// Surface tags are what the UI filters on, and they are patchable.
    #[test]
    fn surface_filtering_selects_the_offered_presets() {
        let presets = builtin_presets();
        let clip_ids: Vec<&str> = presets
            .iter()
            .filter(|preset| preset.surfaces.contains(&Surface::Clip))
            .map(|preset| preset.id.as_str())
            .collect();
        assert_eq!(clip_ids, ["clip", "clip-fast", "webp-anim", "avif-anim"]);
        let playback_ids: Vec<&str> = presets
            .iter()
            .filter(|preset| preset.surfaces.contains(&Surface::Playback))
            .map(|preset| preset.id.as_str())
            .collect();
        assert_eq!(playback_ids, ["playback"]);
        let preview_ids: Vec<&str> = presets
            .iter()
            .filter(|preset| preset.surfaces.contains(&Surface::Preview))
            .map(|preset| preset.id.as_str())
            .collect();
        assert_eq!(
            preview_ids,
            ["preview", "preview-trim"],
            "the hover surface is disjoint from the dropdown surfaces"
        );

        let map = profiles(&[("mosaic-webm", "surfaces = [\"clip\", \"mosaic\"]")]);
        let resolved = resolve_presets(Some(&map));
        assert!(
            find_preset(&resolved, "mosaic-webm")
                .unwrap()
                .surfaces
                .contains(&Surface::Clip)
        );
    }

    /// The stream-copy rules, all of which exist because a copy has nothing to
    /// apply them to. Settings a patch *names* alongside `vcodec = "copy"` are
    /// refused — one entry saying two contradictory things about one output —
    /// while settings it merely *inherits* are dropped, because there is no
    /// way to clear an inherited crf and `vcodec = "copy"` over a built-in
    /// would otherwise be unexpressible.
    #[test]
    fn a_stream_copy_preset_carries_nothing_to_encode_with() {
        let copy_err = |body: &str, needle: &str| {
            let map = profiles(&[("remux", body)]);
            let err = validate_profiles(Some(&map)).expect_err(needle);
            let text = format!("{err:#}");
            assert!(text.contains(needle), "expected '{needle}' in: {text}");
        };

        // A whole copy preset needs only a container and the codec.
        let map = profiles(&[(
            "remux",
            "container = \"mp4\"
vcodec = \"copy\"",
        )]);
        validate_profiles(Some(&map)).expect("a copy needs no rate control");
        let resolved = resolve_presets(Some(&map));
        let remux = find_preset(&resolved, "remux").unwrap();
        assert!(remux.is_stream_copy());
        assert_eq!(
            (
                remux.quality,
                remux.max_height,
                remux.fps_max,
                &remux.acodec
            ),
            (None, None, None, &None)
        );

        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
crf = 26",
            "takes no crf or bitrate_kbps",
        );
        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
bitrate_kbps = 2500",
            "takes no crf or bitrate_kbps",
        );
        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
max_height = 480",
            "cannot rescale",
        );
        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
fps_max = 30",
            "cannot resample",
        );
        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
acodec = \"aac\"",
            "takes no audio",
        );
        copy_err(
            "container = \"mp4\"
vcodec = \"copy\"
audio = true",
            "takes no audio",
        );
        // An image container re-encodes its frames by definition, so there is
        // nothing for a copy to move into one.
        copy_err(
            "container = \"webp\"
vcodec = \"copy\"",
            "cannot stream-copy",
        );
        // The explicit clearing spellings stay legal: they say the same thing
        // the copy already does.
        validate_profiles(Some(&profiles(&[(
            "remux",
            "container = \"mp4\"
vcodec = \"copy\"
max_height = 0
fps_max = 0
audio = false",
        )])))
        .expect("zeroes and audio = false agree with the copy");

        // Turning a real preset into a copy drops everything it inherited
        // rather than failing on values it never asked for.
        let map = profiles(&[("playback", "vcodec = \"copy\"")]);
        validate_profiles(Some(&map)).expect("a built-in can be patched into a copy");
        let playback = find_preset(&resolve_presets(Some(&map)), "playback")
            .unwrap()
            .clone();
        assert!(playback.is_stream_copy());
        assert_eq!(
            (
                playback.quality,
                playback.max_height,
                playback.fps_max,
                playback.acodec
            ),
            (None, None, None, None),
            "the inherited crf 23, 1080 cap and aac track are all dropped"
        );
    }

    /// Load-time completeness: a novel name must describe a whole preset, and
    /// the contradictory combinations are refused instead of being guessed at.
    #[test]
    fn validation_rejects_incomplete_and_contradictory_profiles() {
        let expect_err_named = |name: &str, body: &str, needle: &str| {
            let map = profiles(&[(name, body)]);
            let err = validate_profiles(Some(&map)).expect_err(needle);
            let text = format!("{err:#}");
            assert!(text.contains(needle), "expected '{needle}' in: {text}");
        };
        let expect_err = |body: &str, needle: &str| expect_err_named("novel", body, needle);

        expect_err("vcodec = \"h264\"\ncrf = 20", "container is required");
        expect_err("container = \"mp4\"\ncrf = 20", "vcodec is required");
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"",
            "crf or bitrate_kbps is required",
        );
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 20\nbitrate_kbps = 900",
            "mutually exclusive",
        );
        // The CRF scale is the container's: x264 stops at 51, vp9 at 63, and
        // libwebp's `-q:v` runs to 100.
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 52",
            "crf must be between 0 and 51 for container mp4",
        );
        expect_err(
            "container = \"webm\"\nvcodec = \"vp9\"\ncrf = 64",
            "crf must be between 0 and 63 for container webm",
        );
        expect_err(
            "container = \"webp\"\nvcodec = \"libwebp_anim\"\ncrf = 101",
            "crf must be between 0 and 100 for container webp",
        );
        expect_err(
            "container = \"avif\"\nvcodec = \"av1\"\ncrf = 64",
            "crf must be between 0 and 63 for container avif",
        );
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = -1",
            "crf must be between 0 and 51 for container mp4",
        );
        validate_profiles(Some(&profiles(&[
            ("a", "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 51"),
            ("b", "container = \"webm\"\nvcodec = \"vp9\"\ncrf = 63"),
            (
                "c",
                "container = \"webp\"\nvcodec = \"libwebp_anim\"\ncrf = 100",
            ),
            ("d", "container = \"avif\"\nvcodec = \"av1\"\ncrf = 63"),
        ])))
        .expect("the top of each scale is in range");
        // Switching a built-in's container revalidates the inherited value
        // against the new scale (webp-anim's 85 is not an x264 CRF).
        expect_err_named(
            "webp-anim",
            "container = \"mp4\"\nvcodec = \"h264\"",
            "crf must be between 0 and 51 for container mp4",
        );

        expect_err(
            "container = \"webp\"\nvcodec = \"libwebp_anim\"\ncrf = 75\nacodec = \"aac\"",
            "cannot carry an audio stream",
        );
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 20\naudio = true",
            "audio = true needs an acodec",
        );
        // The other half of the audio contradiction: dropping the stream and
        // naming a codec for it in the same entry.
        expect_err(
            "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 20\naudio = false\nacodec = \"aac\"",
            "audio = false contradicts",
        );

        // A patch of a built-in needs nothing at all beyond what it changes.
        validate_profiles(Some(&profiles(&[("clip", "crf = 14")]))).unwrap();
        // The section absent is always valid.
        validate_profiles(None).unwrap();

        // Ids travel in request bodies and the presets DTO.
        let mut map = BTreeMap::new();
        map.insert(
            "not a name".to_string(),
            toml::from_str::<TranscodeProfileConfig>(
                "container = \"mp4\"\nvcodec = \"h264\"\ncrf = 20",
            )
            .unwrap(),
        );
        let err = validate_profiles(Some(&map)).expect_err("ids must be identifier-safe");
        assert!(format!("{err:#}").contains("is invalid"), "{err:#}");
    }
}
