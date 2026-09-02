//! Backend video transcoding (docs/video-transcoding-design.md): the artifact
//! identity every other part of the feature is keyed by.
//!
//! One transcode = one plain finished file in a content-addressed disk cache
//! ([`cache`]), described by a preset ([`presets`]) and, for the fast channel,
//! an encoder chosen by the hardware probe ([`hw`]).

pub(crate) mod cache;
pub(crate) mod compose;
pub(crate) mod hw;
pub(crate) mod pool;
pub(crate) mod presets;
pub(crate) mod run;
pub(crate) mod webp_bridge;

use serde::Serialize;
use sha2::{Digest, Sha256};

use presets::ResolvedPreset;

/// Bumped on **any** change that can alter output bytes for the same request:
/// preset defaults, ffmpeg argument construction, the params serialization
/// below. It rides in the hash, so a bump re-keys every artifact — old files
/// simply age out of the cache and old negative-cache rows are orphaned.
///
/// Also bumped when a runner fix can overturn recorded failure *verdicts*
/// (v3: the Windows/SMB input-open retry): a settled two-strike verdict is
/// keyed like any artifact, so orphaning the keys is the designed way to
/// re-open files an older, buggier transcoder gave up on.
///
/// v4: `ComposeItem` gained its `source` field, which serializes into
/// `ResolvedCompose` — every composition's cache key input changed shape.
pub(crate) const TRANSCODER_VERSION: i64 = 4;

/// Hex characters of the params digest kept in the cache key. 128 bits of a
/// SHA-256, which is collision-free at any cache size that fits on a disk.
const PARAMS_HASH_HEX_LEN: usize = 32;

/// Hex characters of the source hash used to name a download that has no path
/// behind it. Enough to identify the source at a glance; the artifact's real
/// identity is the key, not its file name.
const SHA_NAME_PREFIX_LEN: usize = 10;

/// Everything that decides the bytes of an artifact.
///
/// The serialization *is* the cache key input, so this type is a contract, not
/// a convenience: fields hash in declaration order, absent bounds hash as
/// absent (never as `0`/duration, which would collide a trimmed clip with an
/// untrimmed one), and presentation-only preset fields are skipped. A pinned
/// fixture test guards all three; changing any of them requires a
/// [`TRANSCODER_VERSION`] bump.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TranscodeParams {
    pub(crate) source_sha256: String,
    /// The *resolved* preset, so editing a profile re-keys its artifacts.
    pub(crate) preset: ResolvedPreset,
    /// The concrete encoder invocation the preset resolved to on this host
    /// (`libx264-medium`, `h264_nvenc`, `libvpx-vp9`, ...), decided at submit
    /// time by [`run::resolve_encoder`].
    ///
    /// It is in the key because the key's promise is *same key, same bytes*:
    /// the fast channel resolves to a hardware encoder on one machine and to
    /// `libx264 -preset veryfast` on the next, and the same preset at the same
    /// trim would otherwise name two visibly different files. An hwaccel flip
    /// (probe result changing, `[transcode] hwaccel` edited) must re-key for
    /// exactly the same reason — and the x264 `-preset` rides along in the
    /// identity because it, too, changes the output bytes.
    pub(crate) encoder: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_cs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_cs: Option<i64>,
    pub(crate) transcoder_version: i64,
}

impl TranscodeParams {
    pub(crate) fn new(
        source_sha256: impl Into<String>,
        preset: ResolvedPreset,
        encoder: String,
        start_cs: Option<i64>,
        end_cs: Option<i64>,
    ) -> Self {
        Self {
            source_sha256: source_sha256.into(),
            preset,
            encoder,
            start_cs,
            end_cs,
            transcoder_version: TRANSCODER_VERSION,
        }
    }

    /// [`Self::new`] with the encoder resolved against this host's hardware
    /// probe. **Blocking**: the first call may spawn ffmpeg twice (listing
    /// plus a validation encode), so callers on the async runtime go through
    /// `spawn_blocking`.
    pub(crate) fn resolve(
        source_sha256: impl Into<String>,
        preset: ResolvedPreset,
        start_cs: Option<i64>,
        end_cs: Option<i64>,
    ) -> Self {
        let encoder =
            run::resolve_encoder(&preset, hw::fast_h264_encoder(), hw::av1_software_encoder());
        Self::new(source_sha256, preset, encoder, start_cs, end_cs)
    }

    /// Digest of the canonical JSON, hex, truncated.
    pub(crate) fn params_hash(&self) -> String {
        let canonical =
            serde_json::to_string(self).expect("TranscodeParams is a plain serializable struct");
        let mut digest = hex::encode(Sha256::digest(canonical.as_bytes()));
        digest.truncate(PARAMS_HASH_HEX_LEN);
        digest
    }

    /// `<source sha256>-<params hash>`: content-addressed on both halves, so
    /// the artifact URL can be served `immutable` with no mtime caveat.
    pub(crate) fn cache_key(&self) -> String {
        format!("{}-{}", self.source_sha256, self.params_hash())
    }

    pub(crate) fn artifact_file_name(&self) -> String {
        format!("{}.{}", self.cache_key(), self.preset.container.ext())
    }

    pub(crate) fn mime_type(&self) -> &'static str {
        self.preset.container.mime_type()
    }

    /// The name a *download* of this artifact should carry, given the source's
    /// file stem. Everything else the name is built from — the trim, the
    /// preset, the container's extension, the hash to fall back on — is part
    /// of the identity already.
    pub(crate) fn download_file_name(&self, stem: Option<&str>) -> String {
        transcode_file_name(
            stem,
            &self.source_sha256,
            self.start_cs.is_some() || self.end_cs.is_some(),
            &self.preset.id,
            self.preset.container.ext(),
        )
    }
}

/// The name a download gets (design §8 / implementation plan §3 S3): the
/// source's stem plus `-clip` for a trimmed cut or `-<preset>` for a plain
/// re-encode. A request that carried no path (the `key=` form of the artifact
/// route) falls back to a hash prefix, which is still stable and still
/// identifies the source.
pub(crate) fn transcode_file_name(
    stem: Option<&str>,
    sha256: &str,
    trimmed: bool,
    preset_id: &str,
    ext: &str,
) -> String {
    let suffix = if trimmed {
        "-clip".to_string()
    } else {
        format!("-{preset_id}")
    };
    let base = match stem {
        Some(stem) => stem.to_string(),
        None => sha256.chars().take(SHA_NAME_PREFIX_LEN).collect(),
    };
    format!("{base}{suffix}.{ext}")
}

/// The source file's stem, for [`transcode_file_name`]. `None` for a path
/// with no usable name — the caller then names the download after the hash.
pub(crate) fn path_stem(path: &std::path::Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
        .filter(|stem| !stem.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use presets::{builtin_presets, find_preset};
    use run::{ENCODER_X264_FAST, ENCODER_X264_QUALITY};

    /// See [`params_hash_is_stable`]. Nothing has shipped that used a
    /// different value, so folding the encoder into the params re-pinned this
    /// constant without a `TRANSCODER_VERSION` bump; any later change does
    /// need one.
    const PINNED_CLIP_PARAMS_HASH: &str = "5950177ed34a46e140dbebef284b31b5";

    fn clip_params(start_cs: Option<i64>, end_cs: Option<i64>) -> TranscodeParams {
        let presets = builtin_presets();
        let preset = find_preset(&presets, "clip")
            .expect("the clip preset ships")
            .clone();
        TranscodeParams::new(
            "a".repeat(64),
            preset,
            ENCODER_X264_QUALITY.to_string(),
            start_cs,
            end_cs,
        )
    }

    /// PINNED FIXTURE. This hash is the identity of every artifact on every
    /// user's disk: if it changes, previously cached bytes are served for a
    /// different request or re-encoded for no reason.
    ///
    /// A failure here means the `TranscodeParams`/`ResolvedPreset`
    /// serialization or the built-in `clip` preset moved. That is allowed —
    /// but it is exactly the change that requires bumping
    /// `TRANSCODER_VERSION` (and re-pinning this fixture), never a silent
    /// re-record.
    #[test]
    fn params_hash_is_stable() {
        assert_eq!(TRANSCODER_VERSION, 4, "re-pin the fixture below on a bump");
        let params = clip_params(None, None);
        assert_eq!(
            serde_json::to_string(&params).unwrap(),
            r#"{"source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","preset":{"id":"clip","container":"mp4","vcodec":"h264","acodec":"aac","quality":{"crf":18},"max_height":null,"fps_max":null,"channel":"quality"},"encoder":"libx264-medium","transcoder_version":4}"#
        );
        assert_eq!(params.params_hash(), PINNED_CLIP_PARAMS_HASH);
        assert_eq!(
            params.cache_key(),
            format!("{}-{PINNED_CLIP_PARAMS_HASH}", "a".repeat(64))
        );
        assert_eq!(
            params.artifact_file_name(),
            format!("{}.mp4", params.cache_key())
        );
        assert_eq!(params.mime_type(), "video/mp4");
    }

    /// Absent bounds hash as absent. Without the `skip_serializing_if` an
    /// untrimmed clip would serialize `"start_cs":null` — harmless on its own,
    /// but the rule it protects is that no bound is ever normalized to `0` or
    /// to the duration, which would collide a trimmed artifact with an
    /// untrimmed one.
    #[test]
    fn trim_bounds_key_separately_from_their_absence() {
        let untrimmed = clip_params(None, None).cache_key();
        let from_zero = clip_params(Some(0), None).cache_key();
        let bounded = clip_params(Some(0), Some(500)).cache_key();
        let other_end = clip_params(Some(0), Some(501)).cache_key();
        assert_ne!(untrimmed, from_zero);
        assert_ne!(from_zero, bounded);
        assert_ne!(bounded, other_end);

        // Identical bounds are the same artifact, whatever produced them (the
        // outro cut resolves to explicit centiseconds for exactly this).
        assert_eq!(bounded, clip_params(Some(0), Some(500)).cache_key());
    }

    /// The download name: a trimmed artifact is a clip whatever preset made
    /// it, an untrimmed one is named after its preset, and a request with no
    /// usable path behind it falls back to the source hash prefix.
    #[test]
    fn download_names_follow_the_request() {
        assert_eq!(
            transcode_file_name(Some("holiday"), "abc", true, "clip", "mp4"),
            "holiday-clip.mp4"
        );
        assert_eq!(
            transcode_file_name(Some("holiday"), "abc", false, "clip-fast", "mp4"),
            "holiday-clip-fast.mp4"
        );
        assert_eq!(
            transcode_file_name(None, "0123456789abcdef", false, "webp-anim", "webp"),
            "0123456789-webp-anim.webp"
        );

        // Through the params, which is how every caller reaches it: either
        // bound alone is already a clip, and the preset names the rest.
        let sha = "a".repeat(64);
        let playback = |start_cs, end_cs| {
            let presets = builtin_presets();
            let preset = find_preset(&presets, "playback").expect("ships").clone();
            TranscodeParams::new(
                sha.clone(),
                preset,
                ENCODER_X264_FAST.to_string(),
                start_cs,
                end_cs,
            )
        };
        assert_eq!(
            playback(None, None).download_file_name(Some("holiday")),
            "holiday-playback.mp4"
        );
        assert_eq!(
            playback(Some(100), None).download_file_name(Some("holiday")),
            "holiday-clip.mp4"
        );
        assert_eq!(
            playback(None, Some(400)).download_file_name(None),
            format!("{}-clip.mp4", &sha[..10])
        );

        // The stem is whatever this platform's `Path` makes of the string:
        // forward slashes separate everywhere, backslashes only on Windows,
        // where on Unix they would be part of one long file name.
        assert_eq!(
            path_stem(std::path::Path::new("/videos/holiday.mp4")).as_deref(),
            Some("holiday")
        );
        if cfg!(windows) {
            assert_eq!(
                path_stem(std::path::Path::new(r"C:\videos\holiday.mp4")).as_deref(),
                Some("holiday")
            );
        }
        assert_eq!(path_stem(std::path::Path::new("")), None);
    }

    /// The key covers the *resolved* preset, not its id: patching a profile
    /// must not serve the bytes the old settings produced. Presentation-only
    /// fields must not, or renaming a preset would orphan its artifacts.
    #[test]
    fn resolved_settings_key_but_presentation_does_not() {
        let presets = builtin_presets();
        let clip = find_preset(&presets, "clip").unwrap();
        let quality = ENCODER_X264_QUALITY.to_string();
        let baseline =
            TranscodeParams::new("sha", clip.clone(), quality.clone(), None, None).cache_key();

        let mut retuned = clip.clone();
        retuned.quality = presets::QualityMode::Crf(20);
        assert_ne!(
            TranscodeParams::new("sha", retuned, quality.clone(), None, None).cache_key(),
            baseline
        );

        let mut relabelled = clip.clone();
        relabelled.label = "Clip (renamed)".to_string();
        relabelled.surfaces = vec![presets::Surface::Mosaic];
        assert_eq!(
            TranscodeParams::new("sha", relabelled, quality.clone(), None, None).cache_key(),
            baseline
        );

        // The host's encoder decision is part of the identity: the same
        // preset resolved against a validated hardware encoder produces
        // visibly different bytes, so it must not share a key.
        for encoder in [ENCODER_X264_FAST, "h264_nvenc"] {
            assert_ne!(
                TranscodeParams::new("sha", clip.clone(), encoder.to_string(), None, None)
                    .cache_key(),
                baseline,
                "{encoder} must re-key"
            );
        }

        // Two presets that differ only by id are still different artifacts.
        let fast = find_preset(&presets, "clip-fast").unwrap();
        assert_ne!(
            TranscodeParams::new("sha", fast.clone(), quality, None, None).cache_key(),
            baseline
        );
    }
}
