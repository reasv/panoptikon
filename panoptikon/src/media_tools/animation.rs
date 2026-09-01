//! Animation-length measurement for the three animated-image containers
//! (docs/animated-image-spans-design.md §3): GIF, animated WebP, animated
//! AVIF.
//!
//! Header/structure parsing only — no pixel decoding, and deliberately no
//! ffmpeg: the measurement feeds `items.duration`, and index data must not
//! depend on the bundled toolchain. Exactness is not critical either; the
//! compose graph tolerates a measured length that disagrees with ffmpeg's
//! decode timing (design §5, under-run), so a structure walk is all the
//! precision the column needs.
//!
//! The failure vocabulary is the column's sentinel table (design §2). A file
//! that *reads* but does not parse — truncated mid-block, a marker that is
//! not in the grammar, a box that runs past the end — measures `0.0`: a
//! verdict, exactly like a genuinely still image, so the file is never
//! re-probed and composes frozen, which is today's behavior. Only a failure
//! to open or read the file at all answers `None`, leaving the column NULL
//! so a later scan retries (the visuals-backfill philosophy: an offline file
//! is this run's problem, not a fact about the content).

use std::path::Path;

/// Whether [`animation_duration_seconds`] has anything to say about this
/// mime: the gate both the metadata phase and the backfill dispatcher check
/// before spending anything. Every other image mime stays NULL forever.
pub(crate) fn measures_animation(mime_type: &str) -> bool {
    matches!(mime_type, "image/gif" | "image/webp" | "image/avif")
}

/// The measured animation length of one file, in seconds.
///
/// `Some(0.0)` is the "measured: still, or unparseable" verdict, `Some(>0)`
/// a real animation length, and `None` either a mime outside the three or a
/// file that could not be read at all — see the module header for why the
/// last two differ.
pub(crate) fn animation_duration_seconds(path: &Path, mime_type: &str) -> Option<f64> {
    let parse: fn(&[u8]) -> f64 = match mime_type {
        "image/gif" => gif_animation_seconds,
        "image/webp" => webp_animation_seconds,
        "image/avif" => avif_animation_seconds,
        _ => return None,
    };
    match std::fs::read(path) {
        Ok(bytes) => Some(parse(&bytes)),
        // No verdict: nothing was parsed, so nothing was learned about the
        // content. The column stays NULL and the next scan retries.
        Err(err) => {
            tracing::debug!(
                path = %path.display(),
                error = %err,
                "could not read a file to measure its animation length"
            );
            None
        }
    }
}

// ---- GIF ------------------------------------------------------------------

/// The delay at or below which a GIF frame is *pathological* — the values ad
/// tools write meaning "as fast as possible" — and the length it is played
/// as instead. Both in centiseconds, matching the GCE field's own unit.
pub(crate) const GIF_MIN_DELAY_CS: u64 = 1;
pub(crate) const GIF_DEFAULT_DELAY_CS: u64 = 10;

/// GIF (89a §23-26): walk the block structure and sum one Graphic Control
/// Extension delay per rendered frame.
///
/// A delay of [`GIF_MIN_DELAY_CS`] or less is normalized to
/// [`GIF_DEFAULT_DELAY_CS`], because that is what the file *plays* as where
/// a human sees it: browsers substitute ~100 ms for the pathological delays
/// ad tools wrote, and the number recorded here has to be the length the
/// compose span will actually run. A frame with no GCE at all has delay 0 and
/// gets the same treatment.
///
/// **ffmpeg does not agree, and measurably so** (7.1, verified against real
/// files): its gif demuxer substitutes for a 0 cs frame but leaves 1 cs
/// exactly as written, and its documented `-min_delay` / `-default_delay`
/// demuxer options have no effect on that case at all. Anything that hands a
/// GIF to ffmpeg and needs the *played* length therefore has to close the gap
/// itself — see [`gif_uniform_pathological_rate`], which the animated loop
/// encoder (`media_tools::animated_loop`) uses to do exactly that.
pub(crate) fn gif_animation_seconds(bytes: &[u8]) -> f64 {
    parse_gif(bytes).map(|timing| timing.seconds()).unwrap_or(0.0)
}

/// The input frame rate that makes ffmpeg play a **uniformly pathological**
/// GIF at the length [`gif_animation_seconds`] measured, or `None` when the
/// file's own timing must be left exactly as written.
///
/// `Some` only when every rendered frame's delay is pathological, which is
/// how the tools that write them write them — all frames or none. That
/// restriction is what keeps the fix from touching timing it has no business
/// touching: a mixed file (say 100/500/100/900 ms) is already exact through
/// ffmpeg, and re-stamping it onto a uniform grid would destroy that. A file
/// that mixes pathological and real delays keeps ffmpeg's timing and is
/// short by up to 90 ms per pathological frame; it is not a shape anything
/// observed writes, and inventing per-frame surgery for it would cost the
/// exactness of the common case (`setpts` retiming drops the final frame
/// outright — measured).
pub(crate) fn gif_uniform_pathological_rate(bytes: &[u8]) -> Option<u32> {
    let timing = parse_gif(bytes)?;
    (timing.frames >= 2 && timing.pathological_frames == timing.frames)
        .then_some((100 / GIF_DEFAULT_DELAY_CS) as u32)
}

/// One GIF's frame timing, as the structure walk found it.
struct GifTiming {
    frames: u64,
    /// How many of those frames carried a delay at or below
    /// [`GIF_MIN_DELAY_CS`].
    pathological_frames: u64,
    /// The normalized total, in centiseconds.
    total_cs: u64,
}

impl GifTiming {
    /// `0.0` for a well-formed file with fewer than two frames: a single
    /// picture is not an animation, whatever its GCE claims.
    fn seconds(&self) -> f64 {
        if self.frames < 2 {
            return 0.0;
        }
        self.total_cs as f64 / 100.0
    }
}

/// `None` is a structural error (the caller's 0.0 verdict).
fn parse_gif(bytes: &[u8]) -> Option<GifTiming> {
    if bytes.len() < 13 || (&bytes[..6] != b"GIF87a" && &bytes[..6] != b"GIF89a") {
        return None;
    }
    // Logical Screen Descriptor: 7 bytes after the signature, whose packed
    // field says whether a global color table follows (and how large).
    let mut at = 13usize;
    let packed = bytes[10];
    if packed & 0x80 != 0 {
        at += 3usize << ((packed & 0x07) + 1);
    }

    let mut frames = 0u64;
    let mut pathological_frames = 0u64;
    let mut total_cs = 0u64;
    // The delay the *next* image descriptor renders with: a GCE applies to
    // the single rendering block that follows it (89a §23).
    let mut pending_cs = 0u64;
    loop {
        let marker = *bytes.get(at)?;
        at += 1;
        match marker {
            // Trailer: the walk is complete.
            0x3B => break,
            // Extension: a label byte, then sub-blocks. Only the GCE (0xF9)
            // carries anything this measurement reads; everything else
            // (comments, application extensions, plain text) is skipped by
            // the same sub-block chain.
            0x21 => {
                let label = *bytes.get(at)?;
                at += 1;
                if label == 0xF9 {
                    // The GCE's one data sub-block: packed flags, a little-
                    // endian delay in centiseconds, and the transparency
                    // index. Read in place; the generic skip below still
                    // walks the chain, so a nonstandard size is tolerated.
                    let size = *bytes.get(at)? as usize;
                    if size >= 3 {
                        pending_cs =
                            u64::from(u16::from_le_bytes([*bytes.get(at + 2)?, *bytes.get(at + 3)?]));
                    }
                }
                at = skip_gif_sub_blocks(bytes, at)?;
            }
            // Image descriptor: 9 bytes, an optional local color table, the
            // LZW minimum code size, and the compressed data as sub-blocks.
            0x2C => {
                let packed = *bytes.get(at + 8)?;
                at += 9;
                if packed & 0x80 != 0 {
                    at += 3usize << ((packed & 0x07) + 1);
                }
                at += 1; // LZW minimum code size
                at = skip_gif_sub_blocks(bytes, at)?;
                frames += 1;
                if pending_cs <= GIF_MIN_DELAY_CS {
                    pathological_frames += 1;
                    total_cs += GIF_DEFAULT_DELAY_CS;
                } else {
                    total_cs += pending_cs;
                }
                pending_cs = 0;
            }
            _ => return None,
        }
    }
    Some(GifTiming {
        frames,
        pathological_frames,
        total_cs,
    })
}

/// Walks one chain of GIF data sub-blocks (a length byte, that many bytes,
/// until a zero length), answering the offset just past the terminator.
fn skip_gif_sub_blocks(bytes: &[u8], mut at: usize) -> Option<usize> {
    loop {
        let size = *bytes.get(at)? as usize;
        at += 1;
        if size == 0 {
            return Some(at);
        }
        if bytes.len() < at + size {
            return None;
        }
        at += size;
    }
}

// ---- WebP -----------------------------------------------------------------

/// WebP: a RIFF walk. The `VP8X` extension header's animation flag decides
/// whether the file is animated at all; the length is the sum of the `ANMF`
/// frame chunks' 24-bit millisecond durations.
pub(crate) fn webp_animation_seconds(bytes: &[u8]) -> f64 {
    parse_webp(bytes).unwrap_or(0.0)
}

fn parse_webp(bytes: &[u8]) -> Option<f64> {
    if bytes.len() < 12 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WEBP" {
        return None;
    }
    let mut at = 12usize;
    let mut animated = false;
    let mut frames = 0u64;
    let mut total_ms = 0u64;
    while at < bytes.len() {
        let header = bytes.get(at..at + 8)?;
        let size = u32::from_le_bytes(header[4..8].try_into().ok()?) as usize;
        let body = bytes.get(at + 8..at + 8 + size)?;
        match &header[..4] {
            // Flags byte: bit 1 is the animation flag. A still WebP (plain
            // `VP8 `/`VP8L`, no VP8X at all) simply never sets `animated`.
            b"VP8X" => animated = body.first()? & 0x02 != 0,
            // Frame rectangle (12 bytes of x/y/w/h), then the duration.
            b"ANMF" => {
                let duration = body.get(12..15)?;
                total_ms += u64::from(duration[0])
                    | u64::from(duration[1]) << 8
                    | u64::from(duration[2]) << 16;
                frames += 1;
            }
            _ => {}
        }
        // Odd-length payloads carry a pad byte the size field does not count.
        at += 8 + size + (size & 1);
    }
    if !animated || frames < 2 {
        return Some(0.0);
    }
    Some(total_ms as f64 / 1000.0)
}

// ---- AVIF -----------------------------------------------------------------

/// AVIF: an ISOBMFF box walk. Only an image *sequence* (`avis` brand, major
/// or compatible) is animated, and its length is `moov`/`mvhd`'s
/// duration÷timescale — the movie header of the track the sequence plays as.
/// A plain `avif`-brand still, or a sequence with no `moov`, measures 0.0.
pub(crate) fn avif_animation_seconds(bytes: &[u8]) -> f64 {
    parse_avif(bytes).unwrap_or(0.0)
}

fn parse_avif(bytes: &[u8]) -> Option<f64> {
    let mut at = 0usize;
    let mut has_avis = false;
    let mut seconds = None;
    while at < bytes.len() {
        let (body, next) = isobmff_box(bytes, at)?;
        match &bytes[at + 4..at + 8] {
            b"ftyp" => {
                if body.len() < 8 {
                    return None;
                }
                // Major brand first, then minor version, then every
                // compatible brand: `avis` anywhere in that list is the
                // sequence claim.
                has_avis = &body[..4] == b"avis"
                    || body[8..].chunks_exact(4).any(|brand| brand == b"avis");
            }
            b"moov" => seconds = mvhd_seconds(body),
            _ => {}
        }
        at = next;
    }
    if !has_avis {
        return Some(0.0);
    }
    // An `avis` file with no moov (or an unparseable/unknown mvhd) has no
    // knowable length: measured still, per design §3.
    Some(seconds.unwrap_or(0.0))
}

/// One ISOBMFF box at `at`: `(body, offset just past the box)`. Handles the
/// 64-bit `largesize` form and the size-0 "to end of file" form; anything
/// that runs past the buffer is a truncation.
fn isobmff_box(bytes: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let header = bytes.get(at..at + 8)?;
    let size32 = u32::from_be_bytes(header[..4].try_into().ok()?) as u64;
    let (header_len, size) = match size32 {
        0 => (8usize, (bytes.len() - at) as u64),
        1 => {
            let large = bytes.get(at + 8..at + 16)?;
            (16usize, u64::from_be_bytes(large.try_into().ok()?))
        }
        size => (8usize, size),
    };
    if size < header_len as u64 {
        return None;
    }
    let end = at.checked_add(usize::try_from(size).ok()?)?;
    let body = bytes.get(at + header_len..end)?;
    Some((body, end))
}

/// The duration of a `moov` box's movie header, walking its children for
/// `mvhd` and reading duration÷timescale in both full-box versions.
fn mvhd_seconds(moov: &[u8]) -> Option<f64> {
    let mut at = 0usize;
    while at < moov.len() {
        let (body, next) = isobmff_box(moov, at)?;
        if &moov[at + 4..at + 8] == b"mvhd" {
            let version = *body.first()?;
            // Full-box header (version + flags), then creation and
            // modification times — 32-bit each in version 0, 64-bit in
            // version 1 — then the timescale (always 32-bit) and duration.
            let (timescale_at, duration_len) = match version {
                0 => (4 + 8, 4),
                1 => (4 + 16, 8),
                _ => return None,
            };
            let timescale =
                u32::from_be_bytes(body.get(timescale_at..timescale_at + 4)?.try_into().ok()?);
            let duration_at = timescale_at + 4;
            let duration = match duration_len {
                4 => {
                    let raw = u32::from_be_bytes(
                        body.get(duration_at..duration_at + 4)?.try_into().ok()?,
                    );
                    // All-ones is the spec's "unknown duration" sentinel.
                    if raw == u32::MAX {
                        return None;
                    }
                    u64::from(raw)
                }
                _ => {
                    let raw = u64::from_be_bytes(
                        body.get(duration_at..duration_at + 8)?.try_into().ok()?,
                    );
                    if raw == u64::MAX {
                        return None;
                    }
                    raw
                }
            };
            if timescale == 0 {
                return None;
            }
            return Some(duration as f64 / f64::from(timescale));
        }
        at = next;
    }
    None
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A syntactically complete GIF with one 1×1 frame per delay, built the
    /// way the scan tests need it too: the LZW payload is the canonical
    /// minimal one-pixel stream, so the image crate reads the header (and
    /// even decodes it) while the parser under test walks the real block
    /// grammar rather than a caricature of it.
    pub(crate) fn gif_bytes(delays_cs: &[u16]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"GIF89a");
        out.extend_from_slice(&[0x01, 0x00, 0x01, 0x00]); // 1×1 canvas
        out.extend_from_slice(&[0x80, 0x00, 0x00]); // 2-entry GCT, bg, aspect
        out.extend_from_slice(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0xFF]); // red, blue
        for delay in delays_cs {
            out.extend_from_slice(&[0x21, 0xF9, 0x04, 0x04]); // GCE
            out.extend_from_slice(&delay.to_le_bytes());
            out.extend_from_slice(&[0x00, 0x00]); // transparency, terminator
            out.extend_from_slice(&[0x2C, 0, 0, 0, 0, 0x01, 0x00, 0x01, 0x00, 0x00]);
            out.extend_from_slice(&[0x02, 0x02, 0x44, 0x01, 0x00]); // 1 pixel of LZW
        }
        out.push(0x3B);
        out
    }

    /// A minimal animated WebP: VP8X with the animation flag, ANIM, and one
    /// ANMF (with an empty frame payload — the parser never reads pixels)
    /// per duration.
    fn webp_bytes(animated: bool, durations_ms: &[u32]) -> Vec<u8> {
        let mut chunks = Vec::new();
        let mut chunk = |fourcc: &[u8; 4], body: &[u8]| {
            chunks.extend_from_slice(fourcc);
            chunks.extend_from_slice(&(body.len() as u32).to_le_bytes());
            chunks.extend_from_slice(body);
            if body.len() % 2 == 1 {
                chunks.push(0);
            }
        };
        let flags = if animated { 0x02 } else { 0x00 };
        chunk(b"VP8X", &[flags, 0, 0, 0, 15, 0, 0, 15, 0, 0]);
        chunk(b"ANIM", &[0, 0, 0, 0, 0, 0]);
        for duration in durations_ms {
            let mut body = vec![0u8; 12];
            body.extend_from_slice(&duration.to_le_bytes()[..3]);
            body.push(0);
            chunk(b"ANMF", &body);
        }
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&((chunks.len() + 4) as u32).to_le_bytes());
        out.extend_from_slice(b"WEBP");
        out.extend_from_slice(&chunks);
        out
    }

    fn isobmff(fourcc: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
        out.extend_from_slice(fourcc);
        out.extend_from_slice(body);
        out
    }

    /// A minimal AVIF skeleton: an ftyp with the given major brand and a
    /// moov/mvhd claiming `duration` ticks at `timescale`.
    fn avif_bytes(major: &[u8; 4], mvhd: Option<(u8, u32, u64)>) -> Vec<u8> {
        let mut ftyp = Vec::new();
        ftyp.extend_from_slice(major);
        ftyp.extend_from_slice(&[0, 0, 0, 0]); // minor version
        ftyp.extend_from_slice(b"avifmif1");
        let mut out = isobmff(b"ftyp", &ftyp);
        if let Some((version, timescale, duration)) = mvhd {
            let mut body = vec![version, 0, 0, 0];
            match version {
                0 => {
                    body.extend_from_slice(&[0u8; 8]);
                    body.extend_from_slice(&timescale.to_be_bytes());
                    body.extend_from_slice(&(duration as u32).to_be_bytes());
                }
                _ => {
                    body.extend_from_slice(&[0u8; 16]);
                    body.extend_from_slice(&timescale.to_be_bytes());
                    body.extend_from_slice(&duration.to_be_bytes());
                }
            }
            // Trailing mvhd fields (rate, volume, matrix, next track id)
            // exist in real files; the parser must not require them.
            out.extend_from_slice(&isobmff(b"moov", &isobmff(b"mvhd", &body)));
        }
        out
    }

    #[test]
    fn gif_delays_are_summed_per_rendered_frame() {
        assert_eq!(gif_animation_seconds(&gif_bytes(&[25, 50])), 0.75);
        assert_eq!(gif_animation_seconds(&gif_bytes(&[10, 10, 10])), 0.3);
    }

    #[test]
    fn pathological_gif_delays_are_normalized_to_ten_centiseconds() {
        // 0 and 1 cs both play as ~100 ms in ffmpeg and every browser; 2 cs
        // is honored verbatim.
        assert_eq!(gif_animation_seconds(&gif_bytes(&[0, 0])), 0.2);
        assert_eq!(gif_animation_seconds(&gif_bytes(&[1, 25])), 0.35);
        assert_eq!(gif_animation_seconds(&gif_bytes(&[2, 2])), 0.04);
    }

    #[test]
    fn a_single_frame_gif_is_still() {
        assert_eq!(gif_animation_seconds(&gif_bytes(&[25])), 0.0);
        assert_eq!(gif_animation_seconds(&gif_bytes(&[])), 0.0);
    }

    #[test]
    fn a_truncated_or_garbage_gif_is_a_still_verdict() {
        let whole = gif_bytes(&[25, 50]);
        // Every prefix, so the cut lands inside every block kind there is.
        for len in 0..whole.len() {
            assert_eq!(gif_animation_seconds(&whole[..len]), 0.0, "cut at {len}");
        }
        assert_eq!(gif_animation_seconds(b"not a gif at all"), 0.0);
        assert_eq!(gif_animation_seconds(&[0xFF; 64]), 0.0);
    }

    #[test]
    fn webp_anmf_durations_are_summed() {
        assert_eq!(webp_animation_seconds(&webp_bytes(true, &[500, 500])), 1.0);
        assert_eq!(webp_animation_seconds(&webp_bytes(true, &[40, 60, 100])), 0.2);
    }

    #[test]
    fn a_still_webp_measures_zero() {
        // Animation flag unset — even with (nonsense) ANMF chunks present the
        // VP8X header's word is what decides.
        assert_eq!(webp_animation_seconds(&webp_bytes(false, &[500, 500])), 0.0);
        // Flag set but a single frame: still.
        assert_eq!(webp_animation_seconds(&webp_bytes(true, &[500])), 0.0);
        // A plain still (no VP8X at all).
        let mut still = Vec::new();
        still.extend_from_slice(b"RIFF");
        still.extend_from_slice(&12u32.to_le_bytes());
        still.extend_from_slice(b"WEBP");
        still.extend_from_slice(b"VP8 ");
        still.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(webp_animation_seconds(&still), 0.0);
    }

    #[test]
    fn a_truncated_or_garbage_webp_is_a_still_verdict() {
        let whole = webp_bytes(true, &[500, 500]);
        for len in 0..whole.len() {
            assert_eq!(webp_animation_seconds(&whole[..len]), 0.0, "cut at {len}");
        }
        assert_eq!(webp_animation_seconds(&[0xAB; 40]), 0.0);
    }

    #[test]
    fn an_avis_sequence_reads_its_movie_header() {
        assert_eq!(avif_animation_seconds(&avif_bytes(b"avis", Some((0, 1000, 2500)))), 2.5);
        // Version-1 mvhd: 64-bit duration.
        assert_eq!(avif_animation_seconds(&avif_bytes(b"avis", Some((1, 25, 75)))), 3.0);
    }

    #[test]
    fn a_still_avif_measures_zero() {
        // The `avif` brand is a still even when a moov is (bogusly) present.
        assert_eq!(avif_animation_seconds(&avif_bytes(b"avif", Some((0, 1000, 2500)))), 0.0);
        // The sequence brand with no moov has no knowable length.
        assert_eq!(avif_animation_seconds(&avif_bytes(b"avis", None)), 0.0);
        // The spec's "unknown duration" sentinel is unknown, not enormous.
        assert_eq!(
            avif_animation_seconds(&avif_bytes(b"avis", Some((0, 1000, u64::from(u32::MAX))))),
            0.0
        );
    }

    #[test]
    fn a_truncated_or_garbage_avif_is_a_still_verdict() {
        let whole = avif_bytes(b"avis", Some((0, 1000, 2500)));
        for len in 0..whole.len() {
            assert_eq!(avif_animation_seconds(&whole[..len]), 0.0, "cut at {len}");
        }
        assert_eq!(avif_animation_seconds(&[0x00; 32]), 0.0);
        assert_eq!(avif_animation_seconds(b"\xff\xff\xff\xffgarbage bytes here"), 0.0);
    }

    /// The committed decode-probe fixtures (`transcode/fixtures/`) are real
    /// two-frame animations by this module's own reading — which is the
    /// reading that decides whether a user's file composes as a span, so the
    /// probes must be exercising files the classifier would say yes to.
    #[test]
    fn the_probe_fixtures_are_animations_by_this_parsers_reading() {
        let webp = include_bytes!("transcode/fixtures/two-frame.webp");
        assert_eq!(webp_animation_seconds(webp), 1.0);
        let avif = include_bytes!("transcode/fixtures/two-frame.avif");
        assert_eq!(avif_animation_seconds(avif), 1.0);
    }

    #[test]
    fn only_the_three_mimes_are_measured() {
        assert!(measures_animation("image/gif"));
        assert!(measures_animation("image/webp"));
        assert!(measures_animation("image/avif"));
        assert!(!measures_animation("image/png"));
        assert!(!measures_animation("image/jpeg"));
        assert!(!measures_animation("video/mp4"));
        let missing = Path::new("does-not-exist.gif");
        assert_eq!(animation_duration_seconds(missing, "image/png"), None);
        // The three mimes on an unreadable file: no verdict, retried later.
        assert_eq!(animation_duration_seconds(missing, "image/gif"), None);
    }
}
