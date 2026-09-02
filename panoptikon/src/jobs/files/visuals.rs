//! Everything one visuals pass produces, and the rules that decide what it
//! owes.
//!
//! Split out of the scan's parent module for size alone: this is the whole
//! rendition ladder — [`ProducedVisuals`] and its failure vocabulary, the two
//! generation passes and the builders under them, the geometry-prediction
//! helpers the backfill dispatcher compares stored rows against, and the
//! [`GridLadder`] classifier that decides which ladder an item is on.
//!
//! The *questions* stay with the scan: `ScanContext::pending_tier_work` and
//! `ScanContext::image_facts` are index reads on the dispatcher's connection,
//! and they belong beside the other five. What crosses the line is
//! [`PendingBackfillWork`], which carries their answers here.
//!
//! `use super::*` rather than a list of imports, and `pub(super)` on every
//! item rather than a curated surface: every name here was written against the
//! parent module's imports and was private to one file, so the split is a move
//! and nothing about who may call what has changed.

use super::*;

/// The thumbnail/frame half of a generation pass, before the blurhash.
#[derive(Default)]
pub(super) struct ProducedVisuals {
    pub(super) thumbnails: Vec<StoredImage>,
    pub(super) frames: Vec<StoredImage>,
    /// The grid tiers this pass produced
    /// (docs/grid-scroll-performance-implementation.md §2).
    ///
    /// `None` means the pass never considered tiers, and nothing is written.
    /// `Some` — the empty vector included — is the *authoritative* set for
    /// this item and replaces whatever is stored: a partial write would leave
    /// a rendition from an older rule behind, which the backfill's set
    /// comparison would then re-dispatch on every scan forever.
    pub(super) tiers: Option<Vec<StoredTier>>,
    /// This item's stored display renditions must go: the current rule serves
    /// its original file. Only an image pass ever concludes this, and only in
    /// a backfill — a new item has nothing stored to drop.
    pub(super) drop_thumbnails: bool,
    pub(super) blurhash_source: Option<DynamicImage>,
    /// Kinds the pass concluded about with nothing to blame: the generator ran
    /// and this content genuinely has no such visual. Deliberately *not* set
    /// for images served from their original file — the served-directly
    /// predicate is already the cache for those, and marking them would put a
    /// row in the table for the majority of every image library.
    pub(super) nothing: Vec<VisualKind>,
    /// What this pass measured about the item's pixels (R4), or `None` when
    /// it decoded nothing. Written once, guarded on `has_transparency IS
    /// NULL`, by the one pass that holds the decoded image.
    pub(super) transparency: Option<bool>,
    /// Verdicts the **animated ladder** owes, which the pass carries rather
    /// than returning as an error.
    ///
    /// Its own channel because an animated-ladder failure is not a failure of
    /// the pass: the display rendition and the blurhash come out of the same
    /// decode and are still perfectly good, so propagating a `VisualsError`
    /// would throw away work that succeeded. The verdicts still have to reach
    /// the ledger — a loop that cannot be encoded costs a decode plus an
    /// ffmpeg run on every scan until something records that it cannot
    /// (see [`build_animated_tiers`]).
    pub(super) tier_verdicts: Vec<VisualVerdict>,
}

/// A failure that ended a generation pass, carrying the kinds it actually
/// invalidates.
///
/// The kinds cannot be derived from the mime type alone: one video pass
/// extracts frames *and* encodes a thumbnail grid out of them, so an encode
/// that failed on the grid says nothing about the frames it was built from,
/// and an encode that failed on a frame says nothing about the thumbnail that
/// was already produced. Only the site that failed knows — so each `?` names
/// its own scope, and the marker rows stay truthful for the mime/outcome
/// targeted retry directives that read them.
#[derive(Debug)]
pub(super) struct VisualsError {
    pub(super) kinds: &'static [VisualKind],
    pub(super) error: FileProcessError,
    /// The `scan_errors` stage this failure owes an audit row under, or `None`
    /// when it owes none — which is every failure but the image decode itself.
    ///
    /// Named by the *site*, for the same reason `kinds` is: the mime type
    /// cannot tell them apart. An image pass that decoded fine and then failed
    /// to *encode* its thumbnail is a generator problem, not a verdict on the
    /// file's pixels, and stamping it `decode` would put it under a retry
    /// directive that targets decodes and make the audit surface claim the
    /// image is undecodable. Those failures get the `visual_attempts` marker
    /// only, exactly like a PDF or an HTML page that fails to render.
    pub(super) audit_stage: Option<&'static str>,
}

impl VisualsError {
    /// The thumbnail is gone, whatever was or was not stored for frames.
    pub(super) fn thumbnail(error: FileProcessError) -> Self {
        Self {
            kinds: &[VisualKind::Thumbnail],
            error,
            audit_stage: None,
        }
    }

    /// The full decode of an image failed — the one visuals failure that also
    /// owes a `scan_errors` audit row, and the only site allowed to say so.
    /// Same scope as [`Self::thumbnail`]: the thumbnail, the frames and the
    /// blurhash all come out of this decode, and images have no frames.
    pub(super) fn image_decode(error: FileProcessError) -> Self {
        Self {
            kinds: &[VisualKind::Thumbnail],
            error,
            audit_stage: Some(STAGE_DECODE),
        }
    }

    /// Frame encoding failed after the frames themselves were extracted and
    /// the thumbnail grid was built.
    pub(super) fn frame(error: FileProcessError) -> Self {
        Self {
            kinds: &[VisualKind::Frame],
            error,
            audit_stage: None,
        }
    }

    /// Nothing came out of the source both kinds are made from — for a video,
    /// the frame extraction itself.
    pub(super) fn both(error: FileProcessError) -> Self {
        Self {
            kinds: &[VisualKind::Thumbnail, VisualKind::Frame],
            error,
            audit_stage: None,
        }
    }

    /// The animated ladder failed on a file whose pixels are fine.
    ///
    /// Scoped to [`VisualKind::Loop`] and to nothing else, deliberately: both
    /// sites that produce one — a poster encode and the ffmpeg run — are
    /// *past* the decode, on an image the generator has already turned into a
    /// first frame. Claiming the thumbnail kind here would assert the file
    /// cannot be decoded, and would suppress a display rendition this file is
    /// perfectly capable of producing.
    pub(super) fn animated_loop(error: FileProcessError) -> Self {
        Self {
            kinds: &[VisualKind::Loop],
            error,
            audit_stage: None,
        }
    }
}

/// One verdict per kind the failure actually invalidates. A transient failure
/// yields none at all: nothing is known about the content, so the generation is
/// simply retried next scan.
pub(super) fn failure_verdicts(err: &VisualsError) -> Vec<VisualVerdict> {
    let Some(failure) = err.error.visual_failure() else {
        return Vec::new();
    };
    err.kinds
        .iter()
        .map(|kind| VisualVerdict::failed(*kind, failure.clone()))
        .collect()
}

/// The `scan_errors` row a visuals failure owes, or `None` when it owes none.
///
/// Only the image decode does, and only the *site* knows that it was one — see
/// [`VisualsError::audit_stage`], which is what an encode failure on a
/// successfully decoded image is kept out by. Every other visuals failure has
/// always left the item indexed without visuals, so none of them is *new*
/// information for the audit surface; the image decode is the one this step
/// newly admits into the index, and a class of failure that silently stopped
/// being a failure would be exactly the invisibility requirement 4 exists
/// against (docs/failed-media-retry-design.md).
///
/// The mime check is redundant with the stage today (only the image branches
/// name one) and kept because the row *asserts* the mime type: a non-image
/// path stamped `decode` would answer an `image/`-targeted retry directive.
///
/// The row is audit-only: it never suppresses anything (see
/// [`crate::db::scan_errors::stage_blocks_indexing`]) — scheduling is the
/// `visual_attempts` marker's job, and the marker is what the next scan reads.
/// `attempts` tracks the marker's because both decode sites write it (see
/// [`backfill_scan_error`]); `skip_after` travels with it for the retry
/// directives' benefit.
pub(super) fn visuals_audit_failure(mime_type: &str, err: &VisualsError) -> Option<ScanFailure> {
    let stage = err.audit_stage?;
    if !mime_type.starts_with("image") {
        return None;
    }
    let failure = err.error.visual_failure()?;
    Some(ScanFailure {
        stage,
        kind: failure.kind,
        skip_after: failure.skip_after,
        message: failure.message.clone(),
    })
}

pub(super) fn generate_new_item_visuals(
    path: &Path,
    mime_type: &str,
    metadata: &ItemScanMeta,
    detect_outros: bool,
    formats: FormatPolicy,
    timers: &ScanTimers,
) -> Result<GeneratedVisuals, FileProcessError> {
    // Inside the thumbgen span, and first within it: the probe is part of the
    // visuals pass (~85ms of process spawn per video), and leaving it outside
    // every phase would quietly drop that out of the per-scan times. Running
    // it *before* the generation it clamps is design §7's ordering, and being
    // in one function is what makes that structural rather than a convention.
    let thumb_span = timers.thumbgen.start();
    let outro = outro_pass_for(path, mime_type, metadata, detect_outros);
    let attempt =
        build_new_item_renditions(path, mime_type, metadata, outro.content_end_ms(), formats);
    drop(thumb_span);

    let mut visuals = GeneratedVisuals::default();
    visuals.outro = outro.record;
    let blurhash_source = match attempt {
        Ok(produced) => {
            visuals.thumbnails = produced.thumbnails;
            visuals.frames = produced.frames;
            visuals.tiers = produced.tiers.unwrap_or_default();
            visuals.transparency = produced.transparency;
            visuals.verdicts = produced
                .nothing
                .into_iter()
                .map(VisualVerdict::nothing)
                .collect();
            // The animated ladder's own verdicts, on the same terms as the
            // backfill path: the pass succeeded and its loop still may not
            // have, and an unrecorded loop failure is a decode plus an ffmpeg
            // run repeated on every scan.
            visuals.verdicts.extend(produced.tier_verdicts);
            produced.blurhash_source
        }
        Err(err) if mime_type.starts_with("text/html") => {
            return Err(html_visuals_error_blocks_indexing(err));
        }
        Err(err) => {
            // Unchanged behaviour: the item is indexed without visuals. What
            // is new is that a verdict about the *content* is remembered, so
            // the next scan does not repeat the work.
            //
            // Debug, not error: every generator that fails on the file itself
            // (pdfium, the browser, ffmpeg) already logged the classified
            // reason at its own site, and a second copy per file was the whole
            // of what a broken-media library saw in its logs.
            tracing::debug!(error = ?err, path = %path.display(), "failed to generate visuals");
            visuals.verdicts = failure_verdicts(&err);
            visuals.audit = visuals_audit_failure(mime_type, &err);
            None
        }
    };

    // After the generation verdicts, which is where the pass's own list is
    // built: a failed probe owes a marker of its own kind alongside them.
    visuals.verdicts.extend(outro.verdict);

    let blurhash_span = timers.blurhash.start();
    visuals.blurhash = blurhash_source.and_then(|image| compute_blurhash(&image).ok());
    drop(blurhash_span);

    Ok(visuals)
}

pub(super) fn html_visuals_error_blocks_indexing(err: VisualsError) -> FileProcessError {
    match err.error {
        FileProcessError::Visuals(failure) => FileProcessError::Classified(ScanFailure {
            stage: STAGE_METADATA,
            kind: failure.kind,
            skip_after: failure.skip_after,
            message: failure.message,
        }),
        other => other,
    }
}

/// `content_end_ms` is this pass's own outro verdict, which ran before it
/// (design §7): frame sampling is clamped to `[0, content_end_ms)`.
pub(super) fn build_new_item_renditions(
    path: &Path,
    mime_type: &str,
    metadata: &ItemScanMeta,
    content_end_ms: Option<i64>,
    formats: FormatPolicy,
) -> Result<ProducedVisuals, VisualsError> {
    let mut out = ProducedVisuals::default();

    if mime_type.starts_with("video") {
        let duration = metadata.duration.unwrap_or(0.0);
        if metadata.video_tracks.unwrap_or(0) > 0 && duration > 0.0 {
            // The one failure that really does invalidate both kinds: the
            // thumbnail grid is built out of these frames.
            let extracted_frames = extract_video_frames(path, 4, duration, content_end_ms)
                .map_err(VisualsError::both)?;
            if extracted_frames.is_empty() {
                // ffmpeg ran and this file yields no frame to sample. Nothing
                // about that will change until the generator does.
                out.nothing
                    .extend_from_slice(&[VisualKind::Thumbnail, VisualKind::Frame]);
            } else {
                let grid = overlay_mime_label(build_image_grid(&extracted_frames), mime_type);
                out.thumbnails
                    .push(encode_generated_still(0, &grid).map_err(VisualsError::thumbnail)?);
                let labeled_first = overlay_mime_label(extracted_frames[0].clone(), mime_type);
                out.thumbnails.push(
                    encode_generated_still(1, &labeled_first).map_err(VisualsError::thumbnail)?,
                );
                // A 2x2 grid of 1920x1080 frames is a 3840x2160 still, and it
                // is what the grid loads for every video today.
                out.tiers = Some(
                    tiers_of_stored_thumbnails(&[(0, &grid), (1, &labeled_first)])
                        .map_err(VisualsError::thumbnail)?,
                );
                // Past the thumbnail: the frames were extracted and the grid
                // encoded, so an encode failure here is the frames' verdict
                // alone.
                out.frames = extracted_frames
                    .iter()
                    .enumerate()
                    .map(|(idx, frame)| encode_generated_still(idx as i64, frame))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(VisualsError::frame)?;
                out.blurhash_source = Some(grid);
            }
        } else {
            tracing::debug!(
                path = %path.display(),
                "skipping video thumbnail generation due to missing video track"
            );
            out.nothing
                .extend_from_slice(&[VisualKind::Thumbnail, VisualKind::Frame]);
        }
    } else if mime_type.starts_with("audio") {
        store_rendered_still(&mut out, get_audio_thumbnail(path, mime_type))?;
    } else if mime_type.starts_with("image") {
        // The full decode lives here and only here: it is what the thumbnail,
        // the grid tiers and the blurhash are made of, and — since the
        // un-fusing — nothing else depends on it. A file that fails it is
        // indexed from its header with no visuals, exactly like a PDF pdfium
        // cannot parse.
        // Only this `?` is the decode: the encodes below it run on pixels that
        // came out fine, so they owe a marker and no audit row.
        let file_size = image_file_size(path)?;
        let image = open_image_oriented(path).map_err(|(stage, err)| {
            VisualsError::image_decode(FileProcessError::visuals_from_image_error(stage, err))
        })?;
        // A first generation, so the display half is always owed; the grid
        // half is the same pure verdict the dispatcher reaches on a rescan,
        // read here off the decode's own dimensions rather than the index's
        // (they are the same numbers — the index records the *oriented*
        // geometry this decode produces).
        let (width, height) = image.dimensions();
        let work = ImageLadderWork {
            display: true,
            replace_tiers: true,
            tiers: first_pass_ladder(mime_type, metadata.duration, file_size, width, height),
            // Measured by this very pass's metadata phase, so the loop is
            // oriented by exactly the turn the item is about to be indexed
            // with.
            rotation: metadata.rotation,
            formats,
            // A first generation: nothing is indexed yet, so the pass's own
            // measurement is the only answer there is.
            transparency: None,
            // A first generation has nothing stored to reuse, and no loop
            // encode to spare.
            reusable_loops: Vec::new(),
        };
        build_image_renditions(&mut out, path, mime_type, file_size, image, work)
            .map_err(VisualsError::thumbnail)?;
    } else if mime_type.starts_with("application/pdf") {
        // Still renders nothing when pdfium is unavailable or the PDF is
        // broken — the item is indexed without visuals — but the two are no
        // longer the same event: a missing library self-heals, a document
        // pdfium refuses to parse does not.
        let page = render_pdf_first_page(path)
            .map_err(|err| VisualsError::thumbnail(pdf_visuals_failure(err)))?;
        store_rendered_still(&mut out, page)?;
    } else if mime_type.starts_with("text/html") {
        let shot = render_html_screenshot_classified(path)
            .map_err(|err| VisualsError::thumbnail(html_visuals_failure(err)))?;
        store_rendered_still(&mut out, shot)?;
    } else {
        // No generator for this type at all: a correct, permanent nothing.
        out.nothing.push(VisualKind::Thumbnail);
    }

    Ok(out)
}

/// The rows a planned rendition set would store, in the order
/// [`get_thumbnail_tier_geometry`] returns them (index, then tier name).
///
/// The generator version is per row, not per set: a loop carries
/// [`LOOP_PROCESS_VERSION`] and every still [`TIER_PROCESS_VERSION`], which is
/// what lets a still-encoder change regenerate posters without re-running
/// ffmpeg.
pub(super) fn wanted_tier_geometry(
    idx: i64,
    renditions: &[WantedRendition],
) -> Vec<TierGeometry> {
    let mut wanted: Vec<TierGeometry> = renditions
        .iter()
        .map(|rendition| TierGeometry {
            idx,
            tier: rendition.tier.to_string(),
            width: i64::from(rendition.plan.width),
            height: i64::from(rendition.plan.height),
            version: rendition_process_version(rendition.tier),
            media_type: rendition.media_type.to_string(),
        })
        .collect();
    wanted.sort_by(|left, right| (left.idx, &left.tier).cmp(&(right.idx, &right.tier)));
    wanted
}

/// Whether an item's stored tier set is exactly the one the current ladder
/// would produce — same tiers, same indices, same pixel dimensions, and
/// generated by at least the current [`TIER_PROCESS_VERSION`].
///
/// Exact on the geometry, not "at least": a rendition from a superseded rule
/// is as much a mismatch as a missing one, and leaving it would keep serving
/// the geometry the rule change was meant to retire.
///
/// `>=` on the version, matching `has_thumbnail`/`has_frame`: a row a *newer*
/// generator wrote is not stale, so a downgrade does not thrash the whole
/// library back to the older renditions.
pub(super) fn tier_geometry_matches(
    stored: &[TierGeometry],
    wanted: &[TierGeometry],
    item_mime_type: &str,
) -> bool {
    stored.len() == wanted.len()
        && stored
            .iter()
            .zip(wanted)
            .all(|(stored, wanted)| rendition_row_matches(stored, wanted, item_mime_type))
}

/// Whether one stored row is the row the current ladder wants: same
/// discriminator, same geometry, a generator version at least as new, and a
/// media type the current rule would have written.
pub(super) fn rendition_row_matches(
    stored: &TierGeometry,
    wanted: &TierGeometry,
    item_mime_type: &str,
) -> bool {
    (stored.idx, &stored.tier, stored.width, stored.height)
        == (wanted.idx, &wanted.tier, wanted.width, wanted.height)
        && stored.version >= wanted.version
        && rendition_media_type_matches(stored, wanted, item_mime_type)
}

/// The stored H.264 loop rows the current ladder still wants, named by their
/// geometry so nothing has to read a blob.
///
/// This is what `LOOP_PROCESS_VERSION` being separate actually buys: a
/// poster-only staleness — a new grid tier, a still-encoder bump, a
/// transparency measurement — must not re-run ffmpeg over every animation in
/// the library to reproduce loops that are already correct. The generator
/// *names* these rows in the set it writes, and `store_thumbnail_tiers`
/// leaves them exactly where they are.
///
/// Per row, deliberately. All-or-nothing would re-encode a correct grid loop
/// for an item that has newly started owing a `loop-display` — which is every
/// large animation the moment R3 shipped.
pub(super) fn reusable_loop_rows(
    stored: &[TierGeometry],
    set: &[WantedRendition],
    mime_type: &str,
) -> Vec<TierGeometry> {
    set.iter()
        .filter(|rendition| is_loop_tier(rendition.tier))
        .filter_map(|rendition| {
            let wanted = TierGeometry {
                idx: 0,
                tier: rendition.tier.to_string(),
                width: i64::from(rendition.plan.width),
                height: i64::from(rendition.plan.height),
                version: rendition_process_version(rendition.tier),
                media_type: rendition.media_type.to_string(),
            };
            stored
                .iter()
                .find(|row| row.idx == 0 && row.tier == rendition.tier)
                .filter(|row| rendition_row_matches(row, &wanted, mime_type))
                .cloned()
        })
        .collect()
}

/// Whether a stored row's media type is one the current rule would have
/// written.
///
/// Exact, with one settled exception: a **loop** row naming the item's own
/// mime type is the keep-the-original sentinel — "no encode of this source
/// came out smaller" — which is a verdict about the content and as final as a
/// hit. Treating it as a mismatch would re-run ffmpeg over exactly the files
/// ffmpeg cannot improve, on every scan, forever.
///
/// Still tiers get no such exception: their rows are never sentinels, and an
/// item whose own type is `image/jpeg` would otherwise pin a stale JPEG
/// against a WebP verdict.
fn rendition_media_type_matches(
    stored: &TierGeometry,
    wanted: &TierGeometry,
    item_mime_type: &str,
) -> bool {
    stored.media_type == wanted.media_type
        || (is_loop_tier(&stored.tier) && stored.media_type == item_mime_type)
}

/// The size of the file a display rendition is decided against. Its own
/// helper so the two generation passes cannot drift on which error class a
/// failed stat is (transient io, never a verdict on the content).
pub(super) fn image_file_size(path: &Path) -> Result<u64, VisualsError> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|err| VisualsError::thumbnail(FileProcessError::Io(err.to_string())))
}

/// Which halves of an image's rendition ladder a pass is for.
///
/// Both are decided by the dispatcher from indexed metadata; the generator
/// only obeys. A pass with `display: false` still runs the full decode — the
/// grid tiers come from the original pixels — it simply does not re-derive a
/// picture that is already stored and already correct.
///
/// Not `Copy`: it carries the loop rows a `TIER_PROCESS_VERSION` bump must
/// **not** re-encode.
#[derive(Clone)]
pub(super) struct ImageLadderWork {
    /// Produce the display rendition, or the verdict that retires a stale
    /// one. False exactly when the stored rendition already *is* what
    /// [`display_plan`] wants: in a library-wide tier backfill, re-encoding
    /// and re-storing an identical picture (and its blurhash) for every
    /// already-correct item is the bulk of the work and buys nothing.
    pub(super) display: bool,
    /// Produce the grid tier set, or leave the stored one where it is. False
    /// exactly when the ladder question found the stored set already correct
    /// and some *other* question — the transparency measurement — is what
    /// dragged this image through a decode. The set is written whole, so
    /// re-emitting an identical one deletes and re-inserts every rendition
    /// the item has for nothing.
    pub(super) replace_tiers: bool,
    /// Which grid renditions this pass owes — see [`grid_ladder`].
    pub(super) tiers: GridLadder,
    /// The item's orientation in clockwise quarter turns, as the scan measured
    /// and stored it (`items.rotation`); `None` where nothing has examined it.
    /// Only the animated ladder reads it — see [`indexed_display_transform`].
    pub(super) rotation: Option<i64>,
    /// This database's format policy (R5), folded once per scan. It only
    /// *constrains* what the rules below decide.
    pub(super) formats: FormatPolicy,
    /// `items.has_transparency` as the index holds it *now*, and the value
    /// this pass must choose formats by whenever it is `Some`.
    ///
    /// The pass measures its own (R4) and reports it for the write-once
    /// column, but a stored answer outranks it. The two are the same
    /// measurement of the same pixels, so they agree in practice — and where
    /// they could not, the dispatcher predicted with the *index*, so a
    /// generator that followed its own decode instead would write a set the
    /// dispatcher never asked for and re-dispatch the item on every scan
    /// forever. Deferring to the column is what makes that impossible by
    /// construction.
    pub(super) transparency: Option<bool>,
    /// See [`PendingBackfillWork::reusable_loops`]: the stored H.264 rows the
    /// dispatcher verified against this same plan, named rather than encoded.
    pub(super) reusable_loops: Vec<TierGeometry>,
}

/// The display rendition and the grid tiers of one already-decoded image, in
/// one place so the new-item and backfill passes cannot produce different
/// ladders for the same file.
///
/// Tiers come from the **original decode**, never from the display rendition:
/// a megapixel-guarded display tier can be *smaller* than `grid-m` (an
/// 800x60000 strip scales to 653 px wide), so cascading off it would upscale.
/// For an animated item the same decode is the first frame, which is exactly
/// what its posters are made of — so the animated ladder costs one ffmpeg run
/// and no second decode.
pub(super) fn build_image_renditions(
    out: &mut ProducedVisuals,
    path: &Path,
    mime_type: &str,
    file_size: u64,
    image: DynamicImage,
    work: ImageLadderWork,
) -> Result<(), FileProcessError> {
    let (width, height) = image.dimensions();
    // R4, measured here and nowhere else: this is the one place in the scan
    // that holds an item's decoded pixels, and the answer decides the *format*
    // of every rendition below. This pass's measurement outranks the index's —
    // for a pending item the index holds nothing at all — and it is what the
    // dispatcher reads next scan instead of decoding again.
    let measured = Some(has_alpha_pixels(&image));
    out.transparency = measured;
    // The indexed answer decides the format wherever there is one; see
    // [`ImageLadderWork::transparency`].
    let transparency = work.transparency.or(measured);
    let grid_format = tier_format(transparency, work.formats);
    // Alpha only ever survives into WebP; every JPEG here flattens, which is
    // the documented fallback when the policy or the size limit refuses one.
    let keep_alpha = grid_format == RenditionFormat::Webp && transparency == Some(true);
    match work.tiers {
        _ if !work.replace_tiers => {}
        GridLadder::Static => {
            out.tiers = Some(encode_tiers(
                0,
                &image,
                &grid_plans(file_size, width, height),
                grid_format,
                keep_alpha,
            )?);
        }
        GridLadder::Animated => {
            let (tiers, verdicts) = build_animated_tiers(
                path,
                mime_type,
                file_size,
                &image,
                work.rotation,
                grid_format,
                keep_alpha,
                &work.reusable_loops,
            );
            out.tiers = tiers;
            out.tier_verdicts = verdicts;
        }
        // Neither produces a set: an empty wanted set is a *delete*, and the
        // pass that decides one is owed is the ladder question
        // ([`TierWork::Retire`]), not this one. Leaving `out.tiers` at `None`
        // is what keeps a display-only backfill of a raw-floor animated item
        // from writing anything at all.
        GridLadder::Nothing | GridLadder::Unknown => {}
    }
    if !work.display {
        // The stored display rendition is already the one the rule wants, so
        // nothing about it is produced, written or dropped. The original
        // decode is the blurhash source if one is even owed — which is what
        // the served-directly path uses anyway.
        out.blurhash_source = Some(image);
        return Ok(());
    }
    let plan = display_plan(
        mime_type,
        matches!(work.tiers, GridLadder::Animated),
        transparency,
        file_size,
        width,
        height,
        work.formats,
    );
    match plan {
        DisplayPlan::Thumbnail {
            width: target_width,
            height: target_height,
            format,
        } => {
            let thumb = render_display_rendition(&image, target_width, target_height);
            let keep_alpha = format == RenditionFormat::Webp && transparency == Some(true);
            let encoded = encode_image(0, &thumb, format, RenditionScale::Display, keep_alpha)?;
            // The keep-the-original sentinel (§2 R2): a rendition that is not
            // comfortably smaller than the file it stands in for is not worth a
            // second copy of the picture, so the geometry is stored without the
            // bytes and the endpoint serves the original. The geometry is
            // stored all the same, or the dispatcher would ask for this
            // rendition again on every scan forever — the loop row's
            // convention, now on `thumbnails`.
            if rendition_beats_original(encoded.bytes.len() as u64, file_size) {
                out.thumbnails.push(encoded);
            } else {
                // The row names the format that was **attempted**, never the
                // source's own mime type. The verdict "no encode came out
                // comfortably smaller" is a verdict about *this* encoder, so
                // a later format flip — a policy edit, a transparency
                // measurement — has to be able to see that the attempt it is
                // looking at was made with the other one and try again.
                // Naming the source instead froze the sentinel across every
                // format change, and, where the source's type happened to be
                // the rendition's, made a real rendition indistinguishable
                // from a verdict.
                out.thumbnails.push(StoredImage {
                    bytes: Vec::new(),
                    ..encoded
                });
            }
            out.blurhash_source = Some(thumb);
        }
        // Served from the original file, or — for an animated item over the
        // trigger — from a stored loop, which is a `thumbnail_tiers` row and
        // never a picture here. No marker either way: the served-directly
        // predicate already answers this without decoding anything. The drop is
        // what retires a rendition an older rule stored: an 800x20000 webtoon
        // crushed to 163x4096 by the long-side rule, or the static still an
        // animated WebP used to be frozen into.
        DisplayPlan::Original | DisplayPlan::Loop { .. } => {
            out.drop_thumbnails = true;
            out.blurhash_source = Some(image);
        }
    }
    Ok(())
}

/// Resizes one decoded image onto exactly the dimensions [`display_plan`]
/// named.
///
/// `resize_exact`, never `resize`: the stored dimensions have to be *exactly*
/// the ones the plan predicts, or the backfill's "is this the rendition the
/// current rule wants?" comparison never settles. A rendition that kept every
/// pixel (only the byte bound fired) is cloned rather than resampled — a full
/// Lanczos pass onto its own dimensions would cost the same picture, slightly
/// blurrier.
fn render_display_rendition(image: &DynamicImage, width: u32, height: u32) -> DynamicImage {
    if (image.width(), image.height()) == (width, height) {
        return image.clone();
    }
    image.resize_exact(width, height, image::imageops::FilterType::Lanczos3)
}

/// The animated ladder of one item: its static posters and its H.264 loop
/// (docs/grid-scroll-performance-implementation.md §2, step B2).
///
/// `first_frame` is the item's own decode — `image` hands back the first
/// frame of an animated GIF/WebP — so the posters cost no decode of their
/// own, and they are the *same* crop rule the loop uses.
///
/// The set is all-or-nothing for the same reason [`build_stored_thumbnail_tiers`]
/// is: the stored set is replaced wholesale, so a set missing its loop would
/// never match what the dispatcher predicts — re-dispatching the item on
/// every scan forever.
///
/// **Every failure owes the ledger a verdict**, which is the second half of
/// the contract and the expensive one to get wrong. The animated ladder is a
/// full decode of the original plus an ffmpeg run, and the dispatcher
/// consults a marker before dispatching it — so without a written verdict
/// there is nothing for that consult to find, and an item ffmpeg cannot
/// encode pays the whole cost again on every scan, forever.
///
/// Every verdict written here is [`VisualKind::Loop`] and **never**
/// [`VisualKind::Thumbnail`]. Both failing sites are *past* the decode: the
/// posters were built from `first_frame` moments earlier, so the pixels are
/// demonstrably fine. Marking the thumbnail kind would assert the opposite
/// and, worse, suppress a display rendition this file can produce — including
/// later, when the display rule flips and starts wanting one. The outcomes:
///
/// * a poster encode failure is `input` — the generator decided it on pixels
///   it already held, so one attempt settles it;
/// * a failed ffmpeg run is `input`-unconfirmed — ffmpeg did its own file
///   I/O, so a broken file and a mount hiccup are indistinguishable and it
///   takes two;
/// * a failure to *start* ffmpeg is `blocked`, which self-heals the moment
///   the toolchain appears, and host trouble (no scratch space, no read-back)
///   is transient — neither is a verdict on the media, so neither settles
///   anything.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_animated_tiers(
    path: &Path,
    mime_type: &str,
    file_size: u64,
    first_frame: &DynamicImage,
    rotation: Option<i64>,
    format: RenditionFormat,
    keep_alpha: bool,
    reusable_loops: &[TierGeometry],
) -> (Option<Vec<StoredTier>>, Vec<VisualVerdict>) {
    let (width, height) = first_frame.dimensions();
    let mut tiers = match encode_tiers(
        0,
        first_frame,
        &poster_plans(width, height),
        format,
        keep_alpha,
    ) {
        Ok(tiers) => tiers,
        Err(err) => {
            tracing::debug!(error = ?err, path = %path.display(), "failed to encode a loop poster");
            return (None, failure_verdicts(&VisualsError::animated_loop(err)));
        }
    };

    // Which loop rows this item wants: always the grid loop, plus a
    // `loop-display` where the display answer is a loop the grid one cannot
    // stand in for (R3). Read from the same function the dispatcher predicts
    // with, so the two cannot disagree.
    for (tier, plan) in animated_plans(file_size, width, height)
        .into_iter()
        .filter(|(tier, _)| is_loop_tier(tier))
    {
        // The reuse the version split exists for: a poster-only staleness —
        // a still-encoder change, a new grid tier, a transparency measurement
        // — must not re-run ffmpeg over every animation in the library. The
        // dispatcher hands back the geometry of the rows it already verified
        // against this same plan, sentinel rows included; naming one keeps it
        // in the authoritative set without moving a byte.
        if let Some(stored) = reusable_loops
            .iter()
            .find(|stored| stored.idx == 0 && stored.tier == tier)
        {
            tiers.push(StoredTier {
                idx: 0,
                tier,
                media_type: stored.media_type.clone(),
                width: stored.width,
                height: stored.height,
                version: stored.version,
                payload: TierPayload::Retained,
            });
            continue;
        }
        let crf = if tier == LOOP_DISPLAY_TIER {
            crate::media_tools::animated_loop::LOOP_DISPLAY_CRF
        } else {
            crate::media_tools::animated_loop::LOOP_CRF
        };
        let bytes = match crate::media_tools::animated_loop::encode_loop(
            path,
            mime_type,
            &plan,
            indexed_display_transform(rotation),
            crf,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    tier,
                    error = %error,
                    "failed to encode an animated loop"
                );
                return (None, failure_verdicts(&loop_failure(error)));
            }
        };
        // The settled encoded-larger-than-the-source edge (§2): keep the
        // original. The row is still written — with the geometry the
        // dispatcher predicted, or the backfill would ask for this loop again
        // on every scan forever — but carries no bytes, which is how the
        // endpoint learns to serve the file itself. A verdict about the
        // *content*, unlike the failure above, so freezing it is correct: the
        // same file encodes the same way until `LOOP_PROCESS_VERSION` says
        // otherwise.
        let keeps_original = loop_keeps_original(bytes.len() as u64, file_size);
        if keeps_original {
            tracing::debug!(
                path = %path.display(),
                tier,
                encoded = bytes.len(),
                source = file_size,
                "the animated loop was not smaller than its source; keeping the original"
            );
        }
        tiers.push(StoredTier {
            idx: 0,
            tier,
            media_type: if keeps_original {
                mime_type.to_string()
            } else {
                LOOP_MEDIA_TYPE.to_string()
            },
            width: i64::from(plan.width),
            height: i64::from(plan.height),
            version: LOOP_PROCESS_VERSION,
            payload: TierPayload::Encoded(if keeps_original { Vec::new() } else { bytes }),
        });
    }
    (Some(tiers), Vec::new())
}

/// Classifies one [`LoopError`] into the failure vocabulary the ledger
/// already speaks — always as [`VisualKind::Loop`], never as the thumbnail's
/// (see [`build_animated_tiers`]).
///
/// Only one arm can ever suppress anything, and only after two strikes:
/// exactly one of these outcomes is a statement about the *file*.
pub(super) fn loop_failure(error: LoopError) -> VisualsError {
    match error {
        // Failing to *start* ffmpeg is never a verdict on the media: a
        // missing toolchain is `blocked` and self-heals when it appears,
        // anything else about this machine stays transient and retries.
        LoopError::Spawn(err) => VisualsError::animated_loop(
            FileProcessError::visuals_from_api_error(crate::media_tools::spawn_error("ffmpeg", &err)),
        ),
        // ffmpeg did its own file I/O, so a broken file and a transient mount
        // hiccup exit identically: this needs a second failure in a later
        // scan before it suppresses anything.
        LoopError::Failed(detail) => {
            VisualsError::animated_loop(visuals_input_unconfirmed(format!("loop encode: {detail}")))
        }
        // Host trouble — no scratch space, an output that would not read
        // back — and a container this build cannot decode. Neither says
        // anything about the file, so neither may spend a strike: a disk that
        // fills mid-backfill would otherwise retire the loop ladder for the
        // whole library, and a permanent verdict for an undecodable container
        // has no heal path at all (the dispatcher's own probe is what gates
        // that case instead — see [`grid_ladder`]).
        LoopError::Host(detail) | LoopError::Unsupported(detail) => {
            VisualsError::animated_loop(FileProcessError::Io(detail))
        }
    }
}

/// The transform that takes a file's stored pixels into the display space the
/// index records its dimensions in, from the orientation the scan measured
/// (`items.rotation`) rather than from a second read of the file's header.
///
/// Identity for everything without an EXIF orientation, which is every GIF and
/// nearly every animated WebP — but the loop's crop rectangle is expressed in
/// display space, so where one *does* exist the encode has to close the gap
/// itself (ffmpeg does not apply a WebP's EXIF orientation, and the bridge
/// writes canvas-space frames).
///
/// The indexed value is the authoritative one, and it is threaded in the way
/// the dimensions already are: it is the same measurement the item's `width`
/// and `height` were oriented by, so a loop can no longer disagree with the
/// geometry the dispatcher predicted for it or with its own poster. `None` —
/// an item nothing has examined yet — is the identity, which is what an
/// unreadable header used to produce.
///
/// One thing the column cannot carry: `items.rotation` records the turn only,
/// mirroring dropped ([`orientation_quarter_turns`]), so a *mirrored* animated
/// container gets its turn and not its flip. That is the same class of
/// cosmetic gap the header read had (it could be a stale answer instead of a
/// missing one), on a population no smaller: an EXIF orientation on an
/// animated container is vanishingly rare to begin with, and a mirroring one
/// rarer still.
pub(super) fn indexed_display_transform(rotation: Option<i64>) -> Transform {
    Transform {
        // Euclidean on both, so a column outside 0/90/180/270 — which no
        // writer produces — still lands inside the group instead of wrapping
        // past it.
        quarter_turns: rotation.unwrap_or(0).div_euclid(90).rem_euclid(4) as u8,
        flip_h: false,
    }
}

/// The grid tiers of pictures this generator produced, across every display
/// index an item has.
pub(super) fn tiers_of_stored_thumbnails(
    thumbnails: &[(i64, &DynamicImage)],
) -> Result<Vec<StoredTier>, FileProcessError> {
    let mut out = Vec::new();
    for (idx, image) in thumbnails {
        out.extend(encode_stored_thumbnail_tiers(*idx, image)?);
    }
    Ok(out)
}

/// The whole visuals ladder of a type whose generator renders exactly one
/// still: audio (cover art or placeholder), a PDF's first page, an HTML
/// screenshot. Their branches in the two passes were six verbatim copies of
/// these three lines.
///
/// Deliberately does *not* cover the image and video branches, which look
/// similar and are not: an image's ladder is dimension-first and can be
/// animated or absent entirely, and a video's is built from two pictures with
/// its frames' own verdicts attached.
pub(super) fn store_rendered_still(
    out: &mut ProducedVisuals,
    image: DynamicImage,
) -> Result<(), VisualsError> {
    out.thumbnails
        .push(encode_generated_still(0, &image).map_err(VisualsError::thumbnail)?);
    out.tiers =
        Some(tiers_of_stored_thumbnails(&[(0, &image)]).map_err(VisualsError::thumbnail)?);
    out.blurhash_source = Some(image);
    Ok(())
}

/// A missing pdfium is `blocked` and self-heals once the library appears; a
/// document pdfium ran on and rejected is a verdict on the content — but an
/// unconfirmed one, because pdfium read the file itself.
pub(super) fn pdf_visuals_failure(err: PdfRenderError) -> FileProcessError {
    match err {
        PdfRenderError::Unavailable => {
            visuals_blocked(Blocker::Pdfium, "pdfium library not available")
        }
        PdfRenderError::Document(detail) => visuals_input_unconfirmed(detail),
    }
}

/// The same split for HTML: no browser is `blocked`, the gateway's own file
/// handling around the render is transient, and a browser that ran and
/// produced nothing usable is an unconfirmed verdict on the page.
pub(super) fn html_visuals_failure(err: HtmlRenderError) -> FileProcessError {
    match err {
        HtmlRenderError::NoBrowser => {
            visuals_blocked(Blocker::HtmlRenderer, "no headless browser available")
        }
        HtmlRenderError::Io(detail) => FileProcessError::Io(detail),
        HtmlRenderError::Render(detail) => visuals_input_unconfirmed(detail),
    }
}

/// The orientation half of one backfill dispatch: what the dispatcher knows
/// before the file is opened.
///
/// The stored-visual flags travel here because the dispatcher cannot ask the
/// question itself (docs/display-dimensions-design.md §4.1) — the orientation
/// is only known once the worker has read the header, and by then the
/// dispatcher's decisions have already been made.
pub(super) struct RotationBackfill {
    pub(super) thumbnail_stored: bool,
    pub(super) blurhash_stored: bool,
}

/// What the orientation stage of one pass concluded.
pub(super) struct RotationPass {
    /// Clockwise quarter turns from coded pixels to picture, 0 included — a
    /// measured 0 is an answer and stamps the column.
    pub(super) quarter_turns: i64,
    /// The stored thumbnail is of the *unrotated* pixels and must be replaced,
    /// not merely left in place next to corrected dimensions.
    pub(super) stale_thumbnail: bool,
    /// Likewise the blurhash, which is computed from the same decode.
    pub(super) stale_blurhash: bool,
}

/// Whether this pass invalidated a stored thumbnail. Free functions rather
/// than methods so the `Option` handling reads the same at both call sites.
pub(super) fn stale_thumbnail(rotation: Option<&RotationPass>) -> bool {
    rotation.is_some_and(|pass| pass.stale_thumbnail)
}

/// Whether this pass invalidated a stored blurhash.
pub(super) fn stale_blurhash(rotation: Option<&RotationPass>) -> bool {
    rotation.is_some_and(|pass| pass.stale_blurhash)
}

/// Reads one item's orientation, and decides what that invalidates.
///
/// Deliberately cheap in the common case: an image costs a header read and
/// stops there unless the header says the picture is transformed, which is a
/// small minority of any library. Only then does anything decode.
///
/// A failed probe returns `None` and records nothing, exactly like
/// [`codec_pass_for`]: the column stays NULL and the next scan asks again,
/// which matters more for this column than for any other — its write is not
/// idempotent, so a wrong answer would not be self-correcting.
pub(super) fn rotation_pass_for(
    path: &Path,
    mime_type: &str,
    work: &RotationBackfill,
) -> Option<RotationPass> {
    if mime_type.starts_with("image") {
        let (_, orientation) = match image_header_geometry(path) {
            Ok(geometry) => geometry,
            Err((stage, err)) => {
                tracing::debug!(
                    path = %path.display(),
                    ?stage,
                    error = %err,
                    "orientation probe failed"
                );
                // A header this build deterministically cannot parse — a
                // format with no decoder (a legacy-indexed AVIF), a header
                // PIL tolerated and the image crate rejects — is a verdict,
                // not a transient: retrying it would re-dispatch the item on
                // every scan forever, the one non-termination the backfill
                // design forbids. `0` is exactly the column's "examined:
                // none this build can read". An `Open` failure is I/O and a
                // `Limits` failure is this machine's ceiling — both can
                // change, so both stay retries.
                let deterministic = stage == ImageStage::Header
                    && !matches!(err, image::ImageError::Limits(_));
                return deterministic.then_some(RotationPass {
                    quarter_turns: 0,
                    stale_thumbnail: false,
                    stale_blurhash: false,
                });
            }
        };
        // `NoTransforms`, not a zero quarter turn: a mirrored image keeps its
        // dimensions but not its pixels, so its stored visuals are just as
        // stale as a rotated one's even though the columns need no correction.
        let untransformed = orientation == Orientation::NoTransforms;
        return Some(RotationPass {
            quarter_turns: orientation_quarter_turns(orientation),
            stale_thumbnail: !untransformed && work.thumbnail_stored,
            stale_blurhash: !untransformed && work.blurhash_stored,
        });
    }
    if mime_type.starts_with("video") {
        let info = extract_media_info(path)
            .map_err(|err| {
                tracing::debug!(
                    path = %path.display(),
                    error = ?err,
                    "orientation probe failed"
                );
            })
            .ok()?;
        // Nothing to replace: `extract_video_frames_into` decodes with a plain
        // `-i` and ffmpeg autorotates video, so this item's frames and the
        // thumbnail grid built from them have always been the picture. Only
        // the columns were ever wrong (docs/display-dimensions-design.md §1.2).
        return Some(RotationPass {
            quarter_turns: info.video_track.map(|video| video.rotation).unwrap_or(0),
            stale_thumbnail: false,
            stale_blurhash: false,
        });
    }
    None
}

/// The outro half of one backfill dispatch.
/// What the tier question found an item missing
/// (docs/grid-scroll-performance-implementation.md §3, B1).
///
/// Two shapes, because two kinds of item have two different sources for
/// their renditions.
pub(super) enum TierWork {
    /// An image: the whole ladder — display rendition and grid tiers alike —
    /// is rebuilt from one decode of the original. `replace_display` says a
    /// rendition is already stored and this one overwrites it, which is what
    /// lifts the backfill's "never write over a stored visual" guard for
    /// exactly this item.
    Image { replace_display: bool },
    /// Everything else: the grid tiers are derived from the display
    /// renditions already in the database, keyed by their index. Never from
    /// the source file — re-running ffmpeg over a library's videos to
    /// reproduce pictures that are already stored would be the most expensive
    /// possible way to get them.
    Derived(Vec<(i64, Vec<u8>)>),
    /// An animated image above the raw floor: one H.264 loop plus its static
    /// posters, both produced from the item's own file — the loop by ffmpeg,
    /// the posters from the first frame the same decode yields.
    ///
    /// Which loop rows are already correct is *not* part of this verdict:
    /// every animated-ladder path can reach the generator, this one included,
    /// so the reuse travels beside the ladder itself
    /// ([`PendingBackfillWork::reusable_loops`]).
    Animated,
    /// This item wants **no** stored tier at all and carries some: delete the
    /// set. Produced by [`GridLadder::Nothing`], which in practice means one
    /// thing: an animated item at or below the raw floor, served from its own
    /// file at every tier. (`grid_ladder` also answers `Nothing` for an item
    /// with no mime type at all, but that item never reaches the ladder
    /// question — [`mime_can_have_renditions`] turns it away first, before a
    /// single storage read.) It is the one verdict that can turn an item's
    /// wanted set from non-empty to empty after a scan already wrote one.
    /// Needs no decode and no source — the write is the whole work — so it
    /// deliberately survives the negative cache's suppression.
    Retire,
}

/// The ladder question's whole answer: the work an item owes, and — for an
/// animated one — the stored loop rows the current plan still wants.
///
/// The two are separate because they answer to different things. The work is
/// about *this* dispatch; the reuse is a property of the item's stored set
/// that every path into the generator needs, including the ones that carry no
/// [`TierWork`] at all (see [`PendingBackfillWork::reusable_loops`]).
pub(super) struct TierQuestion {
    pub(super) work: Option<TierWork>,
    pub(super) reusable_loops: Vec<TierGeometry>,
}

impl TierQuestion {
    /// Nothing owed and nothing to reuse — every answer but the animated
    /// ladder's, which is the only one that has loops to speak of.
    pub(super) fn none() -> Self {
        Self {
            work: None,
            reusable_loops: Vec::new(),
        }
    }

    pub(super) fn work(work: TierWork) -> Self {
        Self {
            work: Some(work),
            reusable_loops: Vec::new(),
        }
    }
}

/// One image's measurements, gathered once per dispatch and shared by every
/// question that needs them (see `ScanContext::image_facts`).
pub(super) struct ImageFacts {
    /// The byte count on disk *now*, which is what both rules are decided
    /// against.
    pub(super) file_size: u64,
    /// The indexed display dimensions, or `None` when either was never
    /// measured.
    pub(super) dimensions: Option<(i64, i64)>,
    /// `items.duration` — for an image, the animated-spans measurement.
    pub(super) duration: Option<f64>,
    /// `items.rotation` — the turn the indexed dimensions were oriented by,
    /// which is the one the animated ladder's loop is cropped against
    /// ([`indexed_display_transform`]). `None` where nothing has examined it.
    pub(super) rotation: Option<i64>,
    /// `items.has_transparency` — the fact that decides every rendition's
    /// *format* (R4), `None` where nothing has examined the pixels.
    pub(super) has_transparency: Option<bool>,
}

/// The mime families a stored rendition — and therefore a grid tier — can
/// ever exist for. Exactly the branches `build_new_item_renditions` and
/// `build_backfill_renditions` have generators for; everything else records a
/// permanent "nothing".
pub(super) fn mime_can_have_renditions(mime_type: &str) -> bool {
    mime_type.starts_with("image")
        || mime_type.starts_with("video")
        || mime_type.starts_with("audio")
        || mime_type.starts_with("application/pdf")
        || mime_type.starts_with("text/html")
}

/// Which grid ladder an item's stored renditions come from
/// (docs/grid-scroll-performance-implementation.md §3, B1 and B2).
///
/// The whole verdict is a pure function of *indexed metadata* — mime type,
/// `items.duration`, the file's byte count and its display dimensions — and
/// nothing here ever decodes anything. That is the invariant the backfill
/// dispatcher lives or dies by: the question is asked once per file per scan,
/// forever, over SMB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GridLadder {
    /// Static renditions: `grid-m`/`grid-s` JPEGs. Every item that does not
    /// move, and every non-image (a video's tiers come from its stored frame
    /// grid, which is a still by construction).
    ///
    /// Internal to the scan: nothing about this name reaches the wire, where
    /// `still` means the `still=true` variant of a *request* and selects an
    /// animated item's poster.
    Static,
    /// An animated image above the raw floor: one H.264 loop, plus the static
    /// posters `still=true` answers with.
    Animated,
    /// Nothing at all. An animated image at or below the raw floor is served
    /// from its original file at every tier, so its wanted set is *empty* —
    /// which is a verdict, not an absence, and the only thing that can retire
    /// a set an earlier rule wrote.
    Nothing,
    /// Undecidable from the index alone: an animated image whose dimensions
    /// were never measured, so neither the floor nor the loop's geometry can
    /// be evaluated. No work, and no retirement either — deciding either way
    /// would churn the item every scan until the dimensions land.
    Unknown,
}

/// [`grid_ladder`] for the new-item pass, whose measurements come from the
/// decode it just performed rather than from the index.
///
/// The two must agree — the very next scan asks [`grid_ladder`] the same
/// question off the indexed values — which is why this is a thin adapter and
/// not a second rule.
pub(super) fn first_pass_ladder(
    mime_type: &str,
    duration: Option<f64>,
    file_size: u64,
    width: u32,
    height: u32,
) -> GridLadder {
    grid_ladder(
        mime_type,
        Some(&ImageFacts {
            file_size,
            dimensions: Some((i64::from(width), i64::from(height))),
            duration,
            // The ladder classifier has no use for either; the pass carries
            // its own freshly measured turn to the loop encoder and its own
            // freshly measured transparency to the format rules.
            rotation: None,
            has_transparency: None,
        }),
    )
}

/// [`GridLadder`] for one item, from what the dispatcher already gathered.
///
/// `facts` is `None` for everything that is not an image; an image's byte
/// count and dimensions come from the single stat and index read the
/// dispatch already paid for (`ScanContext::image_facts`).
pub(super) fn grid_ladder(mime_type: &str, facts: Option<&ImageFacts>) -> GridLadder {
    // No mime type at all: `mime_can_have_renditions` says no generator will
    // ever produce a picture for it, so its wanted set is empty by rule.
    if mime_type.is_empty() {
        return GridLadder::Nothing;
    }
    let duration = facts.and_then(|facts| facts.duration);
    if !is_animated_image(mime_type, duration) {
        // An animated *container* nothing has measured yet is not static —
        // it is unknown, and the scan is the only side of the ladder that can
        // tell the difference. The animation question
        // (docs/animated-image-spans-design.md §4) runs after this one in the
        // same scan, so a WebP or AVIF indexed before that feature existed
        // reads `duration IS NULL` here. Calling it `Still` would write
        // static tiers for a picture that may well move, and serve them
        // *immutably* to anything that asks in the window before the
        // measurement lands — and the retirement machinery could never reach
        // them, because on the next scan the genuinely still ones answer
        // identically.
        //
        // GIF cannot reach this branch with an unmeasured duration at all —
        // it already defaults the other way (animated unless measured still),
        // so its unknown case is safe and `measures_animation` needs no
        // exception for it.
        //
        // The accepted cost is small and one-sided: a genuinely still WebP
        // from a pre-spans library gets its grid tiers one scan later.
        // Nothing is pinned meanwhile — with no tiers stored the endpoint
        // falls up and revalidates.
        if duration.is_none() && crate::media_tools::animation::measures_animation(mime_type) {
            return GridLadder::Unknown;
        }
        return GridLadder::Static;
    }
    // A container this build has no decoder for is **undecidable**, not a
    // permanent nothing. The frame bridge decodes WebP only, so an animated
    // AVIF needs a toolchain that can demux it; when this one cannot, there
    // is no loop to produce *today*.
    //
    // Answered here rather than by letting the encoder fail and recording it,
    // which is the trap R2-A found: a ledger row would be the item's answer
    // forever. `nothing` markers carry no blocker, so the auto-heal probe
    // cannot clear them; the store that clears markers never runs for an item
    // served directly at the display tier, which animated items usually are;
    // and the only remaining escape is a `THUMBNAIL_PROCESS_VERSION` bump,
    // which §2 forbids for tier work. Installing a capable ffmpeg would then
    // never produce a single loop. A gate re-evaluates instead — the probe is
    // a cached `OnceLock`, so this is one boolean read per file after the
    // first, and a restart with a better toolchain picks every item up.
    //
    // Which containers need the probe, and the cost of asking, are
    // [`crate::media_tools::AnimatedContainer`]'s to say — the same table the
    // loop encoder's own input gate consults.
    if crate::media_tools::animated_container_support(mime_type).loop_is_undecodable() {
        return GridLadder::Unknown;
    }
    let Some(facts) = facts else {
        return GridLadder::Unknown;
    };
    let Some((width, height)) = facts.dimensions else {
        return GridLadder::Unknown;
    };
    let (Ok(width), Ok(height)) = (u32::try_from(width), u32::try_from(height)) else {
        return GridLadder::Unknown;
    };
    if width == 0 || height == 0 {
        return GridLadder::Unknown;
    }
    if animated_serves_original(facts.file_size, width, height) {
        GridLadder::Nothing
    } else {
        GridLadder::Animated
    }
}

pub(super) struct OutroBackfill {
    pub(super) item: PendingOutroItem,
    /// Whether a positive verdict *replaces* visuals rather than informing
    /// their first generation (design §7.1).
    pub(super) replaces_visuals: bool,
}

/// One dispatch's answers: what every question the dispatcher asks about an
/// item found owing, plus the material it already fetched for the worker.
///
/// It exists because both halves used to be loose parameters. The questions
/// were re-asked as a copied `is_none() && !… && is_none()` chain at each of
/// the three early returns — which is exactly where a copy went stale and the
/// orientation question fell out of one of them, leaving `items.rotation` NULL
/// forever for any item that owed nothing else (see
/// [`PendingBackfillWork::any`]). The material was 13 more positional
/// arguments into a worker that already needed
/// `#[allow(clippy::too_many_arguments)]`.
pub(super) struct PendingBackfillWork {
    /// Run the thumbnail half of the pass. Note that this is *not* "no
    /// thumbnail is stored": the image rule, the negative cache and the ladder
    /// question all move it, and the dispatcher keeps the positive cache's own
    /// answer separately (`thumbnail_stored`) for the two rules that need it.
    pub(super) thumbnail: bool,
    /// Run the blurhash half of the pass.
    pub(super) blurhash: bool,
    /// The outro question (docs/video-outro-detection-design.md §7).
    pub(super) outro: Option<OutroBackfill>,
    /// The codec question (docs/video-transcoding-design.md §6).
    pub(super) codec: bool,
    /// The animation-length question (docs/animated-image-spans-design.md §4).
    pub(super) animation: bool,
    /// The orientation question (docs/display-dimensions-design.md §4).
    pub(super) rotation: Option<RotationBackfill>,
    /// The transparency question
    /// (docs/thumbnail-format-implementation.md §2, R4): "is this an image
    /// whose pixels nothing has examined?" Like the four above it is not about
    /// a missing visual, so an image with every rendition it needs is still
    /// work — which is what backfills an existing library with no separate
    /// job. The examination rides the pass that decodes anyway.
    pub(super) transparency: bool,
    /// `items.rotation` as it stands *now*, which is what orients an animated
    /// item's loop when this pass measures no orientation of its own
    /// ([`indexed_display_transform`]). `None` for a non-image, which has no
    /// animated ladder to orient.
    pub(super) indexed_rotation: Option<i64>,
    /// `items.has_transparency` as it stands *now* — what the dispatcher
    /// predicted this item's rendition formats with, and therefore what the
    /// generator must decide them by. See [`ImageLadderWork::transparency`].
    pub(super) indexed_transparency: Option<bool>,
    /// The rendition-ladder question
    /// (docs/grid-scroll-performance-implementation.md §3, B1).
    pub(super) tier: Option<TierWork>,
    /// This database's format policy (R5), folded once per scan and threaded
    /// to the generator so both sides of the comparison read the same one.
    pub(super) formats: FormatPolicy,
    /// Which grid ladder this item's renditions come from ([`grid_ladder`]).
    /// Carried separately from `tier` because it governs a pass the ladder
    /// question found *no* work for: an animated image missing its display
    /// rendition runs the image pass, and that pass must produce the animated
    /// set rather than static tiers.
    pub(super) ladder: GridLadder,
    /// The stored H.264 loop rows the current plan still wants, verified
    /// row by row against the very set the generator will build.
    ///
    /// Beside `ladder` rather than inside `tier` for the same reason `ladder`
    /// is: **every** animated-ladder path ends in `build_animated_tiers`, and
    /// most of them are not [`TierWork::Animated`] at all — a transparency
    /// fold, a stale still to retire, an orientation that invalidated the
    /// visuals. A pass that arrived without this list re-ran ffmpeg over an
    /// animation whose loop was already correct, which on the library-wide
    /// upgrade pass is every animated item there is.
    ///
    /// Per row, not all-or-nothing: an item newly owing `loop-display` must
    /// encode that one and keep the grid loop it already has.
    pub(super) reusable_loops: Vec<TierGeometry>,
    /// Frames already in `storage.frames`, where the thumbnail half fetched
    /// them: a video's thumbnail can be rebuilt from these without ffmpeg.
    pub(super) existing_frames: Vec<Vec<u8>>,
    /// The stored display rendition, where the blurhash half needs one and the
    /// thumbnail half is not about to produce it.
    pub(super) existing_thumb: Option<Vec<u8>>,
    /// Whether `storage.frames` holds anything for this content — the
    /// dispatcher's answer, never derived from `existing_frames`, which the
    /// replace path never fetches.
    pub(super) frames_stored: bool,
    /// The duration frame sampling places its points inside.
    pub(super) video_duration: f64,
    /// The content boundary a *previous* scan stored, which every
    /// regeneration of an already-examined item must sample within.
    pub(super) stored_content_end_ms: Option<i64>,
}

impl PendingBackfillWork {
    /// Whether any of the dispatcher's five *non-visual* questions found work
    /// — the shared guard of every early return, and the reason they cannot
    /// drift apart again.
    ///
    /// The two visual halves are deliberately not part of it: each early
    /// return sits inside the branch that already established them, and one of
    /// them (the track-less video) returns while the blurhash half is still
    /// nominally on.
    pub(super) fn any(&self) -> bool {
        self.outro.is_some()
            || self.codec
            || self.animation
            || self.rotation.is_some()
            || self.transparency
            || self.tier.is_some()
    }
}

/// Regenerates only the visuals a file is missing. Never fails hard: partial
/// or failed generation degrades to empty results, matching the Python
/// behavior of catching thumbnail/blurhash errors per file.
pub(super) fn generate_backfill_visuals(
    path: &Path,
    mime_type: &str,
    sha256: String,
    work: PendingBackfillWork,
    timers: &ScanTimers,
) -> BackfillResult {
    let PendingBackfillWork {
        thumbnail: needs_thumb,
        blurhash: needs_blurhash,
        outro,
        codec: needs_codecs,
        animation: needs_animation,
        rotation: rotation_work,
        transparency: transparency_work,
        indexed_rotation,
        indexed_transparency,
        tier: tier_work,
        formats,
        ladder,
        reusable_loops,
        existing_frames,
        existing_thumb,
        frames_stored,
        video_duration,
        stored_content_end_ms,
    } = work;
    // Independent of everything below — it reads stream headers and produces
    // no visual — so it runs first and under the metadata timer, which is the
    // phase it belongs to and the one the new-item path charges the identical
    // ffprobe run to.
    let codecs = if needs_codecs {
        let metadata_span = timers.metadata.start();
        let codecs = codec_pass_for(path, mime_type);
        drop(metadata_span);
        codecs
    } else {
        None
    };
    // The animation measurement, under the same reasoning and the same timer:
    // scan metadata, not a visual, and the phase the new-item path charges
    // the identical structure walk to. No ffmpeg — see
    // `media_tools::animation` for why the index must not depend on it.
    let animation = if needs_animation {
        let metadata_span = timers.metadata.start();
        let animation =
            crate::media_tools::animation::animation_duration_seconds(path, mime_type);
        drop(metadata_span);
        animation
    } else {
        None
    };
    // The orientation measurement, under the same reasoning and the same
    // timer: scan metadata, not a visual, and — a header read for an image,
    // the same ffprobe run for a video — exactly what the new-item path
    // charges the metadata phase. It runs before the thumbnail span because
    // what it finds can *turn the thumbnail half on* (§4.1).
    let rotation = if let Some(work) = rotation_work {
        let metadata_span = timers.metadata.start();
        let rotation = rotation_pass_for(path, mime_type, &work);
        drop(metadata_span);
        rotation
    } else {
        None
    };
    // The orientation the animated ladder crops against: this pass's own
    // measurement where it made one — strictly newer than the index, which for
    // a pending item holds nothing at all — and the stored answer otherwise.
    let display_rotation = rotation
        .as_ref()
        .map(|pass| pass.quarter_turns)
        .or(indexed_rotation);
    // Inside the thumbgen span, and first within it: everything below samples
    // frames, and design §7 requires detection to have happened by then. The
    // ~85ms of process spawn belongs to the visuals phase it clamps rather
    // than to no phase at all.
    let thumb_span = timers.thumbgen.start();
    let replaces_visuals = outro
        .as_ref()
        .map(|work| work.replaces_visuals)
        .unwrap_or(false);
    let outro = outro.map(|work| {
        run_outro_pass(
            path,
            work.item.duration,
            work.item.video_tracks,
            outro_source_dims(work.item.width, work.item.height),
        )
    });
    // What *this pass* found, which is the only thing §7.1 keys off: an item
    // is newly positive exactly once, and its visuals are replaced exactly
    // then.
    let pass_content_end_ms = outro.as_ref().and_then(|pass| pass.content_end_ms());
    // `frames_stored` is the dispatcher's answer about `storage.frames`, not
    // anything derived from `existing_frames` — which the discard below
    // empties, and which the replace path never fetched in the first place.
    // `build_backfill_renditions` classifies a failed extraction by it: with
    // frames in the database an extraction failure is a thumbnail-only
    // verdict, and calling those frames permanently unobtainable would be a
    // lie.
    //
    // Newly positive: the stored frames were sampled across the whole file,
    // card frames included, so rebuilding a thumbnail out of them would
    // reproduce exactly what the verdict just invalidated.
    let existing_frames = match pass_content_end_ms {
        Some(_) => Vec::new(),
        None => existing_frames,
    };
    // Negatives change nothing, and an item that has no visuals yet only needs
    // the clamp — the replacement is for the item that already had them. The
    // orientation stage joins on the same terms: it only ever reports stale
    // visuals for an item that *has* them, so an image with none is left to
    // the ordinary generation path — a served-directly image, in particular,
    // still gets no stored thumbnail out of this.
    // The ladder question joins on the same terms as the orientation stage:
    // an image whose stored display rendition is not the one the current rule
    // wants is a *replacement*, so it lifts the same store guard. An image
    // with nothing stored is an ordinary first generation.
    let image_ladder =
        matches!(&tier_work, Some(TierWork::Image { .. })) || transparency_work;
    let ladder_replaces_display =
        matches!(&tier_work, Some(TierWork::Image { replace_display: true }));
    let replace_visuals = (pass_content_end_ms.is_some() && replaces_visuals)
        || stale_thumbnail(rotation.as_ref())
        || ladder_replaces_display;
    // Which halves of the image ladder this pass is for.
    let image_work = ImageLadderWork {
        // The display rendition is owed when the thumbnail question wants one,
        // when a verdict invalidated what is stored, or when the ladder
        // question found the stored rendition disagreeing with the plan. A
        // pass dispatched purely for the grid tiers or for the transparency
        // question does none of that, and leaves the stored rendition and its
        // blurhash exactly as they are — across a library-wide backfill that
        // is the difference between re-encoding every stored rendition in the
        // library and touching none of them.
        display: needs_thumb || replace_visuals || ladder_replaces_display,
        // The tier set is owed when the ladder question said so, and when a
        // verdict invalidated the pixels the stored set was made from. A pass
        // that decoded only to answer the transparency question would
        // otherwise re-emit the set the geometry comparison had just called
        // correct — and the write is a whole-set delete and insert.
        replace_tiers: tier_work.is_some() || replace_visuals,
        tiers: ladder,
        rotation: display_rotation,
        formats,
        transparency: indexed_transparency,
        reusable_loops: reusable_loops.clone(),
    };
    // An image owing renditions runs the ordinary image pass: display tier
    // and grid tiers come out of the one decode together, so there is no
    // separate stage for them and no second decode. The transparency question
    // rides the same pass for the same reason — its answer is that decode.
    let needs_thumb = needs_thumb || replace_visuals || image_ladder;
    // A replaced thumbnail leaves the blurhash describing the old one, which
    // is the same class of visual and the only one derived from it. The
    // orientation stage can also invalidate the blurhash *alone*: an image
    // served from its original file has no stored thumbnail to replace, but
    // its blurhash came from the same unrotated decode.
    let needs_blurhash = needs_blurhash || replace_visuals || stale_blurhash(rotation.as_ref());
    // The clamp, and only the clamp, also honours the boundary a *previous*
    // scan stored: any regeneration of an already-examined item must sample
    // the same content range the first one did, or the card comes back.
    let content_end_ms = pass_content_end_ms.or(stored_content_end_ms);

    let mut thumbnails = Vec::new();
    let mut tiers: Option<Vec<StoredTier>> = None;
    // What this pass measured about the item's pixels (R4). `None` means
    // nothing decoded, and the column keeps waiting.
    let mut measured_transparency: Option<bool> = None;
    let mut drop_thumbnails = false;
    let mut extracted_frames = Vec::new();
    let mut blurhash_source: Option<DynamicImage> = None;
    let mut verdicts = Vec::new();

    // Whether the thumbnail half already tried — and failed — to decode this
    // file, so the blurhash fallback below knows not to repeat it.
    let mut decode_failed = false;
    // The audit row this pass owes, if it turns out to be a decode failure on
    // an image. At most one of the two sites below can produce one: the
    // blurhash fallback only runs when the thumbnail half did not fail.
    let mut audit: Option<ScanFailure> = None;
    if needs_thumb {
        match build_backfill_renditions(
            path,
            mime_type,
            &existing_frames,
            frames_stored,
            video_duration,
            content_end_ms,
            image_work,
        ) {
            Ok(produced) => {
                thumbnails = produced.thumbnails;
                tiers = produced.tiers;
                measured_transparency = produced.transparency;
                drop_thumbnails = produced.drop_thumbnails;
                extracted_frames = produced.frames;
                blurhash_source = produced.blurhash_source;
                verdicts = produced
                    .nothing
                    .into_iter()
                    .map(VisualVerdict::nothing)
                    .collect();
                // The animated ladder's own verdicts ride alongside: the pass
                // succeeded, and its loop still may not have.
                verdicts.extend(produced.tier_verdicts);
            }
            Err(err) => {
                // The failing site named the kinds it invalidates — a pass
                // that rebuilt its thumbnail from already-stored frames never
                // reaches a frame-scoped one. Debug for the same reason as the
                // new-item path: the classified log is the generator's.
                tracing::debug!(error = ?err, path = %path.display(), "failed to generate thumbnails");
                verdicts = failure_verdicts(&err);
                audit = visuals_audit_failure(mime_type, &err);
                decode_failed = true;
            }
        }
    } else if let Some(TierWork::Derived(sources)) = &tier_work {
        // The tier-only half: this item's display renditions are already
        // right, and its grid tiers are derived from those stored pictures.
        // Inside the thumbgen span because that is the phase the work belongs
        // to, even though no source file is opened.
        tiers = build_stored_thumbnail_tiers(sources);
    }
    // The animated ladder, outside the chain above for the same reason the
    // retirement below it is: an animated image that *also* owes a display
    // rendition runs the ordinary image pass, and that pass already produced
    // this set out of the decode it performed (`ImageLadderWork::tiers`), so
    // an `else if` would be dead exactly when it mattered.
    //
    // The exclusivity condition is the image pass having *been attempted*,
    // never its having succeeded. A pass that ran and failed — the decode
    // broke, or ffmpeg did — has already paid the full decode and process
    // spawn and already written the verdicts they owe; re-running the
    // identical work here would double both, in the same scan, for exactly
    // the items least able to afford it.
    //
    // Its own decode of the original when it does run, because there is
    // nothing else to make a poster from: an animated item is normally served
    // directly at the display tier, so there is no stored picture of it
    // anywhere. The negative cache was consulted for exactly this decode
    // before the work was dispatched.
    let image_pass_attempted = needs_thumb && mime_type.starts_with("image");
    if matches!(tier_work, Some(TierWork::Animated)) && !image_pass_attempted {
        match image_file_size(path).map_err(|err| err.error).and_then(|file_size| {
            open_image_oriented(path)
                .map(|image| (file_size, image))
                .map_err(|(stage, err)| FileProcessError::visuals_from_image_error(stage, err))
        }) {
            Ok((file_size, image)) => {
                // The posters come out of this decode, so its pixels answer
                // R4 too — the same measurement the image pass makes, from
                // the same place, rather than a second decode for one column.
                let measured = Some(has_alpha_pixels(&image));
                measured_transparency = measured;
                // The index decides where it has an answer, for the same
                // reason the image pass defers to it.
                let transparency = indexed_transparency.or(measured);
                let format = tier_format(transparency, formats);
                let keep_alpha =
                    format == RenditionFormat::Webp && transparency == Some(true);
                let (produced, tier_verdicts) = build_animated_tiers(
                    path,
                    mime_type,
                    file_size,
                    &image,
                    display_rotation,
                    format,
                    keep_alpha,
                    &reusable_loops,
                );
                tiers = produced;
                verdicts.extend(tier_verdicts);
            }
            Err(err) => {
                // The decode this ladder is made of, and the same verdict the
                // image pass would have written for it: an image whose pixels
                // do not decode is markered, or the next scan repeats the
                // whole attempt.
                let err = VisualsError::image_decode(err);
                tracing::debug!(
                    error = ?err,
                    path = %path.display(),
                    "failed to decode an animated image for its loop poster"
                );
                verdicts.extend(failure_verdicts(&err));
                audit = audit.or_else(|| visuals_audit_failure(mime_type, &err));
            }
        }
    }
    // The retirement verdict, outside the chain above rather than another arm
    // of it: it is a *delete*, and the pass that produced no set is exactly
    // the pass it has to survive. An animated image missing its display
    // rendition runs the ordinary image pass, which — correctly — writes no
    // tiers at all when its ladder is [`GridLadder::Nothing`], so an `else
    // if` here would swallow the retirement and freeze the stale set for
    // another scan. Only ever fills a `None`: a pass that did produce a set
    // is the authority on what this item wants.
    if matches!(tier_work, Some(TierWork::Retire)) && tiers.is_none() {
        tiers = Some(Vec::new());
    }
    drop(thumb_span);

    let blurhash_span = timers.blurhash.start();
    let mut blurhash = None;
    if needs_blurhash {
        let source = blurhash_source.or_else(|| {
            existing_thumb
                .as_deref()
                .and_then(|bytes| decode_image_bytes(bytes).ok())
        });
        let source = match source {
            Some(source) => Some(source),
            // The last possible source: a full decode of the original. For an
            // image served from its original file this is the *only* decode
            // the backfill ever performs — no thumbnail is ever stored for it,
            // so nothing else would ever record a verdict — and since a file
            // whose pixels do not decode is now indexed rather than rejected,
            // an unrecorded failure here would re-decode it on every scan
            // forever.
            None if mime_type.starts_with("image") && !decode_failed => {
                match open_image_oriented(path) {
                    Ok(image) => Some(image),
                    Err((stage, err)) => {
                        let err = VisualsError::image_decode(
                            FileProcessError::visuals_from_image_error(stage, err),
                        );
                        tracing::debug!(
                            error = ?err,
                            path = %path.display(),
                            "failed to decode an image for its blurhash"
                        );
                        verdicts.extend(failure_verdicts(&err));
                        audit = audit.or_else(|| visuals_audit_failure(mime_type, &err));
                        None
                    }
                }
            }
            None => None,
        };
        blurhash = source
            .as_ref()
            .and_then(|image| compute_blurhash(image).ok());
    }
    drop(blurhash_span);

    let (outro_record, outro_verdict) = match outro {
        Some(pass) => (pass.record, pass.verdict),
        None => (None, None),
    };
    verdicts.extend(outro_verdict);

    BackfillResult {
        sha256,
        mime_type: mime_type.to_string(),
        thumbnails,
        tiers,
        drop_thumbnails,
        extracted_frames,
        blurhash,
        visual_verdicts: verdicts,
        visuals_scan_error: audit.and_then(|failure| backfill_scan_error(path, mime_type, failure)),
        outro: outro_record,
        rotation: rotation.map(|pass| pass.quarter_turns),
        codecs,
        animation,
        transparency: measured_transparency,
        replace_visuals,
    }
}

/// The audit row a *backfill's* image decode owes.
///
/// Without this the row would be written once, by the new-item path, and never
/// again: every later confirmation of the same file arrives through the mtime
/// shortcut and the backfill, so `attempts` would sit at 1 forever while the
/// `visual_attempts` marker quietly reached its threshold and suppressed —
/// the audit surface reading "1/2 · will retry" about a file nothing will
/// retry. Writing here keeps the two counters in lockstep (1 on the new-item
/// pass, 2 on the backfill that confirms it, nothing after the marker
/// suppresses), and it is also the only writer for the two cases the new-item
/// path cannot cover at all: a file that was indexed *decodable* and rotted in
/// place, and one whose marker a generator-version bump retired.
///
/// The stat is taken here rather than threaded from the walker: the backfill is
/// dispatched for content, not for a stat, and the pair only has to identify
/// the bytes this decode just failed on. A stat that fails now means the file
/// went away mid-scan — transient, and no row is owed.
pub(super) fn backfill_scan_error(
    path: &Path,
    mime_type: &str,
    failure: ScanFailure,
) -> Option<ScanErrorRecord> {
    let (last_modified, file_size) = get_last_modified_time_and_size(path)
        .map_err(|err| {
            tracing::debug!(
                error = %err,
                path = %path.display(),
                "could not stat a file to record its decode failure"
            );
        })
        .ok()?;
    Some(ScanErrorRecord {
        path: path.to_string_lossy().to_string(),
        last_modified,
        file_size,
        stage: failure.stage.to_string(),
        kind: failure.kind,
        mime_type: Some(mime_type.to_string()),
        error: failure.message,
        skip_after: failure.skip_after,
    })
}

/// `content_end_ms` is where the item's content ends — this pass's own outro
/// verdict, or the one a previous scan stored. See
/// [`build_new_item_renditions`].
///
/// `frames_stored` is whether `storage.frames` holds anything for this item.
/// It is *not* `!existing_frames.is_empty()`: the caller empties that list
/// when a newly-positive verdict makes the stored frames wrong to reuse, and
/// classifying an extraction failure from the emptied list would call frames
/// that exist permanently unobtainable.
pub(super) fn build_backfill_renditions(
    path: &Path,
    mime_type: &str,
    existing_frames: &[Vec<u8>],
    frames_stored: bool,
    video_duration: f64,
    content_end_ms: Option<i64>,
    image_work: ImageLadderWork,
) -> Result<ProducedVisuals, VisualsError> {
    let mut out = ProducedVisuals::default();

    if mime_type.starts_with("video") {
        // Reuse frames already stored in the database before re-running ffmpeg.
        let mut frames: Vec<DynamicImage> = existing_frames
            .iter()
            .filter_map(|bytes| decode_image_bytes(bytes).ok())
            .collect();
        let mut fresh = false;
        if frames.is_empty() {
            frames =
                extract_video_frames(path, 4, video_duration, content_end_ms).map_err(|err| {
                    match frames_stored {
                        // Nothing is stored, so this extraction was the frames'
                        // only chance too.
                        false => VisualsError::both(err),
                        // Frames *are* stored — they would not decode, or a
                        // new verdict made them wrong to reuse: the
                        // re-extraction was a thumbnail rescue, and calling the
                        // stored frames a failure would be a lie.
                        true => VisualsError::thumbnail(err),
                    }
                })?;
            fresh = true;
        }
        if frames.is_empty() {
            out.nothing.push(VisualKind::Thumbnail);
            // A frame verdict only when a fresh extraction genuinely had the
            // inputs to run: with frames already stored (they merely failed to
            // decode) or with no usable duration, ffmpeg concluded nothing
            // about this content and a `none` here would suppress the frames
            // of a video that has them.
            if fresh && !frames_stored && video_duration > 0.0 {
                out.nothing.push(VisualKind::Frame);
            }
        } else {
            let grid = overlay_mime_label(build_image_grid(&frames), mime_type);
            out.thumbnails
                .push(encode_generated_still(0, &grid).map_err(VisualsError::thumbnail)?);
            let labeled_first = overlay_mime_label(frames[0].clone(), mime_type);
            out.thumbnails.push(
                encode_generated_still(1, &labeled_first).map_err(VisualsError::thumbnail)?,
            );
            out.tiers = Some(
                tiers_of_stored_thumbnails(&[(0, &grid), (1, &labeled_first)])
                    .map_err(VisualsError::thumbnail)?,
            );
            if fresh {
                // The thumbnail is already built; only the frames are at stake
                // from here.
                out.frames = frames
                    .iter()
                    .enumerate()
                    .map(|(idx, frame)| encode_generated_still(idx as i64, frame))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(VisualsError::frame)?;
            }
            out.blurhash_source = Some(grid);
        }
    } else if mime_type.starts_with("audio") {
        store_rendered_still(&mut out, get_audio_thumbnail(path, mime_type))?;
    } else if mime_type.starts_with("image") {
        // No pre-gate of its own any more: the dispatcher decided this image
        // owes a rendition (a display one, a tier, or both) from the indexed
        // dimensions, which is the whole point of keeping that predicate
        // decode-free. Re-deriving it here from a byte count alone would
        // disagree with it — the ladder is dimension-first now.
        //
        // As in the new-item pass: the decode owes the audit row, the encodes
        // below it do not.
        let file_size = image_file_size(path)?;
        let image = open_image_oriented(path).map_err(|(stage, err)| {
            VisualsError::image_decode(FileProcessError::visuals_from_image_error(stage, err))
        })?;
        build_image_renditions(&mut out, path, mime_type, file_size, image, image_work)
            .map_err(VisualsError::thumbnail)?;
    } else if mime_type.starts_with("application/pdf") {
        let page = render_pdf_first_page(path)
            .map_err(|err| VisualsError::thumbnail(pdf_visuals_failure(err)))?;
        store_rendered_still(&mut out, page)?;
    } else if mime_type.starts_with("text/html") {
        let shot = render_html_screenshot_classified(path)
            .map_err(|err| VisualsError::thumbnail(html_visuals_failure(err)))?;
        store_rendered_still(&mut out, shot)?;
    } else {
        out.nothing.push(VisualKind::Thumbnail);
    }

    Ok(out)
}

/// The grid tiers of an item whose display renditions are already stored and
/// correct — a video, an audio cover, a PDF page — decoded from those stored
/// JPEGs rather than from the source file.
///
/// This is the tier-only half of the backfill, and it never touches the
/// original: re-running ffmpeg over a library's videos to produce pictures
/// that are already in the database would be the most expensive possible way
/// to get them.
/// `None` on any failure, and deliberately all-or-nothing: the stored set is
/// replaced wholesale, so writing a partial one would leave a set that never
/// matches what the dispatcher expects — re-dispatching the item on every
/// scan forever. Writing nothing retries the same work next scan, which for a
/// stored q85 JPEG is cheap, and never stores a wrong answer.
pub(super) fn build_stored_thumbnail_tiers(sources: &[(i64, Vec<u8>)]) -> Option<Vec<StoredTier>> {
    let mut out = Vec::new();
    for (idx, bytes) in sources {
        let image = decode_image_bytes(bytes)
            .inspect_err(|err| {
                tracing::warn!(error = %err, idx, "a stored thumbnail did not decode");
            })
            .ok()?;
        let tiers = encode_stored_thumbnail_tiers(*idx, &image)
            .inspect_err(|err| {
                tracing::warn!(error = ?err, idx, "failed to encode a thumbnail tier");
            })
            .ok()?;
        out.extend(tiers);
    }
    Some(out)
}

/// The blurhash is computed from an image already in memory, so a failure is
/// deterministic. It has no marker kind of its own (the negative cache
/// shadows the two stored caches, and a blurhash is neither), and every caller
/// discards it with `.ok()`, so this classification only ever reaches a log.
pub(super) fn compute_blurhash(image: &DynamicImage) -> Result<String, FileProcessError> {
    let resized = resize_for_blurhash(image);
    let rgba = resized.to_rgba8();
    blurhash_encode(4, 4, rgba.width(), rgba.height(), rgba.as_raw())
        .map_err(|err| visuals_input(err.to_string()))
}

pub(super) fn resize_for_blurhash(image: &DynamicImage) -> DynamicImage {
    let (width, height) = image.dimensions();
    let max_dim = 128u32;
    if width <= max_dim && height <= max_dim {
        return image.clone();
    }
    // The source is often a full-resolution image (small files never get a
    // stored thumbnail), and the result only feeds a 4x4-component blurhash,
    // so use the fast single-pass box filter instead of a quality resampler.
    image.thumbnail(max_dim, max_dim)
}

/// Whether an image is served from its original file at the **display** tier
/// and therefore gets no stored rendition
/// (docs/grid-scroll-performance-implementation.md §2).
///
/// Kept separate from the generator so a rescan can answer the question from
/// indexed metadata instead of decoding the file: nothing is stored for these
/// images, so `has_thumbnail` stays false forever and an unguarded backfill
/// would decode them on every single scan.
pub(super) fn image_is_served_directly(
    mime_type: &str,
    animated: bool,
    facts: &ImageFacts,
    width: i64,
    height: i64,
    policy: FormatPolicy,
) -> bool {
    let (Ok(width), Ok(height)) = (u32::try_from(width), u32::try_from(height)) else {
        // Nonsensical indexed dimensions: let the worker decode and decide.
        return false;
    };
    matches!(
        display_plan(
            mime_type,
            animated,
            facts.has_transparency,
            facts.file_size,
            width,
            height,
            policy,
        ),
        // An animated item over the trigger stores a *loop*, which is a
        // `thumbnail_tiers` row the ladder question owns — nothing this
        // predicate is about. Its `thumbnails` set is empty either way, so the
        // display half of the pass has nothing to produce.
        DisplayPlan::Original | DisplayPlan::Loop { .. }
    )
}

/// Encodes a planned set of grid renditions of one picture.
///
/// `idx` is the display rendition these are tiers *of*, so `big` selects the
/// same picture at every tier of a video.
pub(super) fn encode_tiers(
    idx: i64,
    image: &DynamicImage,
    plans: &[(ThumbnailTier, TierPlan)],
    format: RenditionFormat,
    keep_alpha: bool,
) -> Result<Vec<StoredTier>, FileProcessError> {
    grid_renditions(image, plans)
        .into_iter()
        .map(|(tier, rendition)| {
            let encoded = encode_image(idx, &rendition, format, RenditionScale::Grid, keep_alpha)?;
            Ok(StoredTier {
                idx,
                tier: tier.as_str(),
                media_type: encoded.media_type,
                width: encoded.width,
                height: encoded.height,
                version: TIER_PROCESS_VERSION,
                payload: TierPayload::Encoded(encoded.bytes),
            })
        })
        .collect()
}

/// The grid tiers of a picture the generator itself produced — a video's
/// frame grid, an audio cover, a rendered PDF page, an HTML screenshot.
/// Their source is a q85 JPEG, never a user file, so the byte half of the
/// serve-directly rule does not apply.
pub(super) fn encode_stored_thumbnail_tiers(
    idx: i64,
    image: &DynamicImage,
) -> Result<Vec<StoredTier>, FileProcessError> {
    let (width, height) = image.dimensions();
    encode_tiers(
        idx,
        image,
        &grid_plans_for_stored_thumbnail(width, height),
        RenditionFormat::Jpeg,
        false,
    )
}

/// One encoded rendition, in the format and at the quality its rung asks for
/// (docs/thumbnail-format-implementation.md §2).
///
/// `keep_alpha` only ever reaches WebP: every JPEG here flattens, including
/// the fallback a policy without `webp` or a side past the WebP size limit
/// forces.
pub(super) fn encode_image(
    idx: i64,
    image: &DynamicImage,
    format: RenditionFormat,
    scale: RenditionScale,
    keep_alpha: bool,
) -> Result<StoredImage, FileProcessError> {
    // In-memory, on pixels already decoded: no file I/O to be ambiguous
    // about, so one attempt settles it.
    let bytes = encode_rendition(image, format, scale, keep_alpha).map_err(visuals_input)?;
    Ok(StoredImage {
        idx,
        width: image.width() as i64,
        height: image.height() as i64,
        media_type: format.media_type().to_string(),
        bytes,
    })
}

/// The display rendition of a picture this **generator** produced — a video's
/// frame grid, an audio cover, a rendered PDF page, an HTML screenshot.
///
/// Always JPEG, deliberately outside the format policy: those pictures are
/// opaque by construction and the format rules are written about a *user's*
/// file, so making them follow the policy would regenerate every video
/// rendition in a library for a setting that is about photographs (§4).
pub(super) fn encode_generated_still(
    idx: i64,
    image: &DynamicImage,
) -> Result<StoredImage, FileProcessError> {
    encode_image(
        idx,
        image,
        RenditionFormat::Jpeg,
        RenditionScale::Display,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visual_tiers::LOOP_TIER;
    // The one fixture the two test modules share; it stays with the scan's
    // own tests, which also plant animated GIFs.
    use super::super::tests::write_animated_gif;

    /// The **display** sentinel names the format it attempted, never the
    /// item's own mime type (§2 R2, as adjudicated after the format-flip
    /// review).
    ///
    /// Reached the way a library reaches it: pure noise is what a lossy codec
    /// is worst at, so a WebP of it does not come out comfortably smaller
    /// than a plausible PNG of the same picture, and the pass records the
    /// verdict instead of a second copy. What the row has to carry is *which*
    /// encoder reached that verdict — "no encode was smaller" is a statement
    /// about `image/webp` here, and a later policy edit or transparency
    /// measurement that moves the verdict to JPEG has to be able to see that
    /// the stored answer was about the other one. Naming `image/png` instead
    /// froze the sentinel across every format change there is.
    #[test]
    fn a_display_sentinel_names_the_format_it_attempted() {
        // 2560 is the rendition cap, so the plan keeps every pixel and the
        // encode has no downscale to win with.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let image = DynamicImage::ImageRgb8(image::RgbImage::from_fn(2560, 2560, |_, _| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let bytes = state.to_le_bytes();
            image::Rgb([bytes[0], bytes[1], bytes[2]])
        }));
        // Just past the lossless class's byte bound, which is what makes a
        // rendition owed at all at this geometry.
        let file_size = crate::visual_tiers::DISPLAY_MAX_FILE_SIZE_LOSSLESS + 1;

        let mut out = ProducedVisuals::default();
        build_image_renditions(
            &mut out,
            Path::new("noise.png"),
            "image/png",
            file_size,
            image,
            ImageLadderWork {
                display: true,
                replace_tiers: true,
                // The display half alone: the grid tiers are a different
                // rule and would only cost three more encodes here.
                tiers: GridLadder::Nothing,
                rotation: None,
                formats: FormatPolicy::default(),
                transparency: Some(false),
                reusable_loops: Vec::new(),
            },
        )
        .expect("the pass runs on decoded pixels");

        let [row] = out.thumbnails.as_slice() else {
            panic!("exactly one display row");
        };
        assert!(
            row.bytes.is_empty(),
            "the premise: no WebP of this picture is comfortably smaller than              a lossless original of it"
        );
        assert_eq!(
            row.media_type, "image/webp",
            "the format the encode was attempted with, not the source's own"
        );
        assert_eq!((row.width, row.height), (2560, 2560));
    }

    /// The settled encoded-larger-than-the-source edge (§2), reached the way
    /// a library reaches it rather than by planting a row: a dithered
    /// two-colour pattern is what GIF's palette coding is best at and what
    /// H.264's transform is worst at, so the encode really does come out
    /// larger — by two orders of magnitude here.
    ///
    /// The row is still written, with the geometry the dispatcher predicted,
    /// because the alternative is asking for this loop again on every scan
    /// forever. It carries no bytes, which is how the endpoint learns to
    /// serve the file itself.
    #[test]
    fn a_loop_no_smaller_than_its_source_keeps_the_original() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("dither.gif");
        write_animated_gif(&path, 600, 2, 100, |x, y, index| {
            if (x + y + index as u32).is_multiple_of(3) {
                image::Rgba([0, 0, 0, 255])
            } else {
                image::Rgba([255, 255, 255, 255])
            }
        });
        let file_size = fs::metadata(&path).unwrap().len();
        let image = open_image_oriented(&path).expect("the fixture decodes");

        let (tiers, verdicts) = build_animated_tiers(
            &path,
            "image/gif",
            file_size,
            &image,
            None,
            RenditionFormat::Jpeg,
            false,
            &[],
        );
        let tiers = tiers.expect("the ladder is produced");
        assert!(
            verdicts.is_empty(),
            "keeping the original is a verdict about the content, not a failure"
        );

        let animated = tiers
            .iter()
            .find(|tier| tier.tier == LOOP_TIER)
            .expect("the loop row is written whatever the comparison says");
        assert!(
            animated.encoded().is_some_and(<[u8]>::is_empty),
            "an encode no smaller than its source must not be stored"
        );
        assert_eq!(
            animated.media_type, "image/gif",
            "the row names what the endpoint will actually serve"
        );
        assert_eq!(
            (animated.width, animated.height),
            (600, 600),
            "the geometry is still exactly what the dispatcher predicts"
        );
        // ... and the posters are real pictures either way: `still=true` has
        // to answer with something.
        let posters: Vec<&StoredTier> = tiers
            .iter()
            .filter(|tier| tier.tier != LOOP_TIER)
            .collect();
        assert!(!posters.is_empty());
        assert!(
            posters
                .iter()
                .all(|tier| tier.encoded().is_some_and(|bytes| !bytes.is_empty()))
        );
    }

    /// The animated ladder's failure ledger. Without it the dispatcher's
    /// existing marker consult has nothing to find, and an item whose loop
    /// cannot be encoded pays a full decode plus a process spawn on every
    /// scan, forever.
    #[test]
    fn a_failed_loop_encode_owes_the_ledger_a_verdict() {
        if !crate::media_tools::ffmpeg_available() {
            return;
        }
        let dir = tempfile::TempDir::new().unwrap();
        // Decodable pixels in hand (so the posters encode), behind a path
        // ffmpeg cannot make anything of.
        let path = dir.path().join("broken.gif");
        fs::write(&path, b"GIF89a and then nothing ffmpeg can use").unwrap();
        let image = DynamicImage::ImageRgb8(image::RgbImage::new(1400, 1400));

        let (tiers, verdicts) = build_animated_tiers(
            &path,
            "image/gif",
            4_000_000,
            &image,
            None,
            RenditionFormat::Jpeg,
            false,
            &[],
        );
        assert!(
            tiers.is_none(),
            "a set without its loop must never be stored: it would never \
             match what the dispatcher predicts"
        );
        let kinds: Vec<VisualKind> = verdicts.iter().map(|verdict| verdict.kind).collect();
        assert_eq!(
            kinds,
            vec![VisualKind::Loop],
            "a loop failure is the loop's own verdict: the pixels decoded, and \
             a thumbnail marker would suppress a display rendition this file \
             can still produce"
        );
        let failure = verdicts[0]
            .failure
            .as_ref()
            .expect("a failed run is a failure, not a permanent nothing");
        assert_eq!(
            failure.skip_after, SKIP_AFTER_AMBIGUOUS,
            "ffmpeg did its own file I/O, so one failure does not settle it"
        );
    }

    /// The classification, per outcome: only the caller can write these, and
    /// the three shapes have very different lifetimes.
    #[test]
    fn loop_failures_classify_by_what_actually_went_wrong() {
        // A missing toolchain is `blocked`: never a verdict on the media, and
        // it self-heals the moment ffmpeg appears.
        let missing = LoopError::Spawn(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no ffmpeg",
        ));
        let failure = loop_failure(missing)
            .error
            .visual_failure()
            .expect("a classified failure")
            .clone();
        assert!(matches!(
            failure.kind,
            ApiErrorKind::Blocked {
                blocker: Blocker::Ffmpeg
            }
        ));

        // Any other spawn problem is this machine's, and stays transient —
        // no verdict at all, so the work is simply retried.
        let busy = LoopError::Spawn(std::io::Error::other("resource temporarily unavailable"));
        assert!(
            failure_verdicts(&loop_failure(busy)).is_empty(),
            "a transient failure must not write a marker"
        );

        // A run that failed needs two strikes: ffmpeg did its own file I/O.
        // This is the ONLY outcome that can ever suppress anything.
        let ran = LoopError::Failed("moov atom not found".to_string());
        let failure = loop_failure(ran)
            .error
            .visual_failure()
            .expect("a classified failure")
            .clone();
        assert_eq!(failure.kind, ApiErrorKind::Input);
        assert_eq!(failure.skip_after, SKIP_AFTER_AMBIGUOUS);

        // Host trouble is never the file's fault. A disk that fills during a
        // library-wide backfill must not spend a strike against every
        // animated item in the library and retire the ladder wholesale.
        for host in [
            LoopError::Host("could not create a loop directory: disk full".to_string()),
            LoopError::Host("the encoded loop did not read back: disk full".to_string()),
        ] {
            assert!(
                failure_verdicts(&loop_failure(host)).is_empty(),
                "host trouble writes no marker and is simply retried"
            );
        }

        // A container this build cannot decode is transient here too: the
        // dispatcher's probe is what gates it, and a ledger row would be the
        // item's answer forever with no path back (R2-A).
        let unsupported = LoopError::Unsupported("no animated AVIF decoder".to_string());
        assert!(failure_verdicts(&loop_failure(unsupported)).is_empty());

        // Every arm scopes to the loop kind and only the loop kind.
        for error in [
            LoopError::Failed("x".to_string()),
            LoopError::Host("y".to_string()),
            LoopError::Spawn(std::io::Error::other("z")),
            LoopError::Unsupported("w".to_string()),
        ] {
            assert_eq!(loop_failure(error).kinds, &[VisualKind::Loop]);
        }
    }

    /// R2-A: a container this build cannot decode is **undecidable**, not a
    /// permanent nothing — so the dispatcher answers `Unknown` and writes no
    /// ledger row, and installing a capable ffmpeg later picks the item up on
    /// the next process. A `nothing` marker could never be cleared: it
    /// carries no blocker for the auto-heal to probe, the store that clears
    /// markers never runs for a served-directly item, and the only remaining
    /// escape is a `THUMBNAIL_PROCESS_VERSION` bump that §2 forbids.
    #[test]
    fn an_undecodable_animated_container_is_undecidable_not_a_permanent_nothing() {
        const MB: u64 = 1024 * 1024;
        let animated = facts(6 * MB, 2000, 2000, Some(2.0));
        assert_eq!(
            grid_ladder("image/avif", Some(&animated)),
            if crate::media_tools::transcode::hw::animated_avif_decodable() {
                GridLadder::Animated
            } else {
                GridLadder::Unknown
            },
            "an animated AVIF follows what this toolchain can actually demux"
        );
        // The gate is the container's, not the ladder's: WebP has the frame
        // bridge, so it never depends on an ffmpeg demuxer.
        assert_eq!(
            grid_ladder("image/webp", Some(&animated)),
            GridLadder::Animated
        );
        assert_eq!(
            grid_ladder("image/gif", Some(&animated)),
            GridLadder::Animated
        );
    }

    // What a failed extraction is allowed to conclude, given whether frames
    // are stored — the rule the replace path made load-bearing.
    //
    // On that path `existing_frames` arrives empty for two different reasons:
    // a stored thumbnail meant `needs_thumb` was false at dispatch so nothing
    // ever fetched it, and a newly-positive verdict deliberately discards what
    // was fetched. Neither means the item is frameless, so the fact travels
    // separately. Derive it from the empty list instead and a failed
    // re-extraction writes a `Frame` failure marker over frames that are
    // sitting in `storage.frames` — one that then suppresses the very
    // regeneration that would have worked.
    #[test]
    fn a_failed_extraction_only_condemns_frames_that_are_not_stored() {
        let dir = tempfile::TempDir::new().unwrap();
        let clip = dir.path().join("clip.mp4");
        fs::write(&clip, b"not a container, and never opened").unwrap();

        // The replace path's shape: no frames in hand, but four in the
        // database. The thumbnail is gone and nothing else is.
        let Err(err) = build_backfill_renditions(
            &clip,
            "video/mp4",
            &[],
            true,
            10.0,
            Some(5000),
            ImageLadderWork {
                display: true,
                replace_tiers: true,
                tiers: GridLadder::Static,
                rotation: None,
                formats: FormatPolicy::default(),
                transparency: None,
                reusable_loops: Vec::new(),
            },
        )
        else {
            panic!("unreadable bytes cannot yield frames");
        };
        assert_eq!(
            failure_verdicts(&err)
                .iter()
                .map(|verdict| verdict.kind)
                .collect::<Vec<_>>(),
            vec![VisualKind::Thumbnail],
            "frames are stored, so this extraction was a thumbnail rescue"
        );

        // Nothing stored anywhere: the same failure really is both kinds'
        // only chance, and that has not changed.
        let Err(err) = build_backfill_renditions(
            &clip,
            "video/mp4",
            &[],
            false,
            10.0,
            None,
            ImageLadderWork {
                display: true,
                replace_tiers: true,
                tiers: GridLadder::Static,
                rotation: None,
                formats: FormatPolicy::default(),
                transparency: None,
                reusable_loops: Vec::new(),
            },
        ) else {
            panic!("unreadable bytes cannot yield frames");
        };
        assert_eq!(
            failure_verdicts(&err)
                .iter()
                .map(|verdict| verdict.kind)
                .collect::<Vec<_>>(),
            vec![VisualKind::Thumbnail, VisualKind::Frame]
        );
    }

    // The other termination story: an image this build cannot parse at all.
    // The legacy scanner's PIL was more tolerant than the image crate (and
    // indexed formats this build has no decoder for, had it ever had the
    // plugins), so a legacy item can sit behind a header that deterministically
    // fails here. That is a verdict — "examined: none this build can read",
    // the column's own 0 — not a retry: leaving the column NULL would
    // re-dispatch the item on every scan forever. An I/O failure, by contrast,
    // says nothing about the bytes and must stay a retry.
    #[test]
    fn an_unparseable_header_is_a_zero_verdict_not_a_retry() {
        let dir = tempfile::TempDir::new().unwrap();
        let work = RotationBackfill {
            thumbnail_stored: true,
            blurhash_stored: true,
        };

        let garbage = dir.path().join("legacy.avif");
        fs::write(&garbage, b"definitely not a parseable image").unwrap();
        let pass = rotation_pass_for(&garbage, "image/avif", &work)
            .expect("a deterministic parse failure is an answer");
        assert_eq!(pass.quarter_turns, 0);
        assert!(
            !pass.stale_thumbnail && !pass.stale_blurhash,
            "nothing can be regenerated from a file nothing can decode"
        );

        let missing = dir.path().join("vanished.jpg");
        assert!(
            rotation_pass_for(&missing, "image/jpeg", &work).is_none(),
            "an I/O failure stays a retry — it says nothing about the bytes"
        );
    }

    // The visuals half of the taxonomy, site by site: which failures are the
    // content's fault (marked), which are a missing dependency (marked, and
    // self-healing), which are this machine's (never marked), and which of
    // them settle on a single failure.
    #[test]
    fn visuals_failures_are_classified_by_what_actually_failed() {
        let dir = tempfile::TempDir::new().unwrap();

        let expect = |err: FileProcessError, kind: ApiErrorKind, skip_after: i64| {
            let failure = err
                .visual_failure()
                .unwrap_or_else(|| panic!("must be marked: {err:?}"));
            assert_eq!(failure.kind, kind, "{err:?}");
            assert_eq!(failure.skip_after, skip_after, "{err:?}");
        };

        // A missing backend self-heals; a document or page the backend ran on
        // and rejected does not — and is unconfirmed, because the backend did
        // its own file I/O.
        expect(
            pdf_visuals_failure(PdfRenderError::Unavailable),
            ApiErrorKind::Blocked {
                blocker: Blocker::Pdfium,
            },
            SKIP_AFTER_CONFIRMED,
        );
        expect(
            pdf_visuals_failure(PdfRenderError::Document("broken xref".to_string())),
            ApiErrorKind::Input,
            SKIP_AFTER_AMBIGUOUS,
        );
        expect(
            html_visuals_failure(HtmlRenderError::NoBrowser),
            ApiErrorKind::Blocked {
                blocker: Blocker::HtmlRenderer,
            },
            SKIP_AFTER_CONFIRMED,
        );
        expect(
            html_visuals_failure(HtmlRenderError::Render("exit 1".to_string())),
            ApiErrorKind::Input,
            SKIP_AFTER_AMBIGUOUS,
        );
        // The gateway's own file handling around a render says nothing about
        // the page.
        let io = html_visuals_failure(HtmlRenderError::Io("no temp dir".to_string()));
        assert!(io.visual_failure().is_none(), "{io:?}");

        // The image stages, with the same reasoning as the scan ledger's: an
        // open that never decoded anything is this machine's problem, a decode
        // is a verdict on the content, and the configurable memory ceiling is
        // a budget rather than a property of the file.
        let (stage, err) = open_image_staged(dir.path().join("absent.png"))
            .err()
            .expect("a missing file must fail");
        let missing = FileProcessError::visuals_from_image_error(stage, err);
        assert!(missing.visual_failure().is_none(), "{missing:?}");

        let garbage = dir.path().join("garbage.png");
        fs::write(&garbage, b"definitely not an image").unwrap();
        let (stage, err) = open_image_staged(&garbage).err().expect("must not decode");
        expect(
            FileProcessError::visuals_from_image_error(stage, err),
            ApiErrorKind::Input,
            SKIP_AFTER_AMBIGUOUS,
        );
        expect(
            FileProcessError::visuals_from_image_error(
                ImageStage::Decode,
                image::ImageError::Limits(image::error::LimitError::from_kind(
                    image::error::LimitErrorKind::InsufficientMemory,
                )),
            ),
            ApiErrorKind::Resource,
            SKIP_AFTER_CONFIRMED,
        );

        // Spawning ffmpeg: a missing toolchain is a dependency the auto-heal
        // can clear, anything else about this machine is transient.
        expect(
            FileProcessError::visuals_from_api_error(crate::media_tools::spawn_error(
                "ffmpeg",
                &io::Error::new(io::ErrorKind::NotFound, "no ffmpeg"),
            )),
            ApiErrorKind::Blocked {
                blocker: Blocker::Ffmpeg,
            },
            SKIP_AFTER_CONFIRMED,
        );
        let denied = FileProcessError::visuals_from_api_error(crate::media_tools::spawn_error(
            "ffmpeg",
            &io::Error::new(io::ErrorKind::PermissionDenied, "nope"),
        ));
        assert!(denied.visual_failure().is_none(), "{denied:?}");

        // A failure marks exactly the kinds it invalidates, and a transient one
        // marks none — the difference between "we know" and "we did not get to
        // find out". The scope is the failing *site*'s, not the mime type's: a
        // video pass builds its thumbnail out of the frames it extracted, so
        // the two are only both lost when the extraction itself was.
        let failure = || visuals_input_unconfirmed("ffmpeg failed");
        assert_eq!(failure_verdicts(&VisualsError::both(failure())).len(), 2);
        assert_eq!(
            failure_verdicts(&VisualsError::thumbnail(failure()))
                .into_iter()
                .map(|verdict| verdict.kind)
                .collect::<Vec<_>>(),
            vec![VisualKind::Thumbnail],
            "an encode that failed on the grid says nothing about the frames"
        );
        assert_eq!(
            failure_verdicts(&VisualsError::frame(failure()))
                .into_iter()
                .map(|verdict| verdict.kind)
                .collect::<Vec<_>>(),
            vec![VisualKind::Frame],
            "and an encode that failed on a frame says nothing about the thumbnail"
        );
        assert!(
            failure_verdicts(&VisualsError::both(FileProcessError::Io(
                "mount went away".to_string()
            )))
            .is_empty()
        );

        // The audit row on top of the marker, and only for the one class of
        // visuals failure that used to keep a file out of the index: an image
        // whose pixels do not decode. A PDF or a video failing the same way
        // has always been indexed without visuals, so it is not news.
        let audit = visuals_audit_failure("image/png", &VisualsError::image_decode(failure()))
            .expect("an image decode failure owes an audit row");
        assert_eq!(
            (audit.stage, audit.kind, audit.skip_after),
            (STAGE_DECODE, ApiErrorKind::Input, SKIP_AFTER_AMBIGUOUS)
        );
        assert!(
            visuals_audit_failure("application/pdf", &VisualsError::image_decode(failure()))
                .is_none(),
            "the row asserts a mime type, so a non-image never gets one"
        );
        assert!(
            visuals_audit_failure(
                "image/png",
                &VisualsError::image_decode(FileProcessError::Io("mount went away".to_string()))
            )
            .is_none(),
            "a transient failure is no verdict to audit"
        );
        // And the distinction the *site* carries: an encode that failed on
        // pixels which decoded fine is the generator's problem, not the file's.
        // Sweeping it into a `decode` row would answer a decode-targeted retry
        // directive and tell the audit surface the image is undecodable.
        assert!(
            visuals_audit_failure("image/png", &VisualsError::thumbnail(failure())).is_none(),
            "an encode failure is marker-only, exactly like a PDF's"
        );
        assert_eq!(
            failure_verdicts(&VisualsError::image_decode(failure()))
                .into_iter()
                .map(|verdict| verdict.kind)
                .collect::<Vec<_>>(),
            vec![VisualKind::Thumbnail],
            "a decode failure still marks exactly what it invalidates"
        );
    }

    // The animated ladder's crop rectangle is expressed in display space, so
    // the loop encoder has to be handed the same turn the item's indexed
    // dimensions were oriented by. It comes from `items.rotation` now rather
    // than from a second read of the file's header, so this is the whole
    // mapping — and the flip the column cannot carry is deliberately absent.
    #[test]
    fn a_loop_is_oriented_by_the_indexed_turn() {
        let turns = |transform: Transform| {
            assert!(!transform.flip_h, "the column records no mirroring");
            transform.quarter_turns
        };
        // Never examined: the identity, which is what every GIF and nearly
        // every animated WebP wants anyway.
        assert_eq!(indexed_display_transform(None), Transform::default());
        for (rotation, quarter_turns) in [(0, 0), (90, 1), (180, 2), (270, 3)] {
            assert_eq!(
                turns(indexed_display_transform(Some(rotation))),
                quarter_turns,
                "{rotation}"
            );
        }
        // Values no writer produces, kept inside the group rather than
        // panicking or wrapping past it.
        assert_eq!(turns(indexed_display_transform(Some(360))), 0);
        assert_eq!(turns(indexed_display_transform(Some(-90))), 3);
    }

    // The dispatcher's copy of the display rule must agree with the
    // generator's, because the whole no-redecode invariant rests on the two
    // being the same function of the same indexed numbers.
    #[test]
    fn served_directly_matches_the_thumbnail_decision() {
        const MB: u64 = 1024 * 1024;
        let served = |mime: &str, bytes: u64, width: i64, height: i64| {
            image_is_served_directly(
                mime,
                false,
                &facts(bytes, width, height, None),
                width,
                height,
                FormatPolicy::default(),
            )
        };
        // The dead hole the old rule had: 2.9 MB, 100 MP, served raw to the
        // grid. Under the dimension-first rule it gets a rendition.
        assert!(!served("image/jpeg", 3 * MB, 12000, 8333));
        // Short side over the cap.
        assert!(!served("image/jpeg", MB, 5000, 5000));
        // Bytes over the source class's own bound, dimensions modest.
        assert!(!served("image/jpeg", 4 * MB + 1, 1000, 1000));
        assert!(served("image/jpeg", 4 * MB, 1000, 1000));
        assert!(!served("image/png", 2 * MB + 1, 1000, 1000));
        assert!(
            served("image/webp", 30 * MB, 1000, 1000),
            "a WebP source's bytes never trigger a rendition"
        );
        // The common case that must never be re-decoded on every scan.
        assert!(served("image/jpeg", 3 * MB, 4096, 4096));
        // A webtoon: 16 MP with an 800 px short side. The old long-side rule
        // stored a 163x4096 rendition for this; now the original serves.
        assert!(served("image/jpeg", 3 * MB, 800, 20000));
        // An animated item never stores a still display rendition, whatever
        // its size: its display answer is its file or a stored loop.
        assert!(image_is_served_directly(
            "image/gif",
            true,
            &facts(30 * MB, 2000, 2000, Some(2.0)),
            2000,
            2000,
            FormatPolicy::default(),
        ));
        // Whatever the generator decides, the two agree — including for the
        // shapes above.
        for (mime, bytes, width, height) in [
            ("image/jpeg", 3 * MB, 12000_u32, 8333_u32),
            ("image/jpeg", MB, 5000, 5000),
            ("image/jpeg", 3 * MB, 4096, 4096),
            ("image/jpeg", 3 * MB, 800, 20000),
            ("image/png", 30 * MB, 4000, 3000),
            ("image/webp", 30 * MB, 4000, 3000),
        ] {
            let generator_stores_one = matches!(
                display_plan(
                    mime,
                    false,
                    None,
                    bytes,
                    width,
                    height,
                    FormatPolicy::default()
                ),
                DisplayPlan::Thumbnail { .. }
            );
            assert_eq!(
                generator_stores_one,
                !served(mime, bytes, i64::from(width), i64::from(height)),
                "{mime} {width}x{height} at {bytes} bytes"
            );
        }
    }

    // The tier ladder is derived from the same indexed numbers, so what the
    // dispatcher predicts and what the generator writes have to be the same
    // pixel dimensions. A drift here does not corrupt anything — it makes
    // the backfill re-dispatch the item on every scan, forever.
    #[test]
    fn planned_tier_geometry_matches_what_is_stored() {
        const MB: u64 = 1024 * 1024;
        for format in [RenditionFormat::Jpeg, RenditionFormat::Webp] {
            for (width, height) in [(1500_u32, 2000_u32), (300, 2000), (2000, 300), (60, 60)] {
                let image = DynamicImage::ImageRgb8(image::RgbImage::new(width, height));
                let plans = grid_plans(50 * MB, width, height);
                let stored =
                    encode_tiers(0, &image, &plans, format, false).expect("tiers encode");
                let stored: Vec<TierGeometry> = stored
                    .iter()
                    .map(|tier| TierGeometry {
                        idx: tier.idx,
                        tier: tier.tier.to_string(),
                        width: tier.width,
                        height: tier.height,
                        version: tier.version,
                        media_type: tier.media_type.clone(),
                    })
                    .collect();
                let wanted = wanted_tier_geometry(
                    0,
                    &static_rendition_set(50 * MB, width, height, format),
                );
                assert!(
                    tier_geometry_matches(&stored, &wanted, "image/png"),
                    "{width}x{height}: stored {stored:?}"
                );
                // The media type is part of the comparison, which is what
                // makes a format change visible without a decode.
                let other = if format == RenditionFormat::Jpeg {
                    RenditionFormat::Webp
                } else {
                    RenditionFormat::Jpeg
                };
                let wanted_other = wanted_tier_geometry(
                    0,
                    &static_rendition_set(50 * MB, width, height, other),
                );
                assert!(
                    stored.is_empty()
                        || !tier_geometry_matches(&stored, &wanted_other, "image/png"),
                    "{width}x{height}: a format flip has to be work"
                );
            }
        }
    }

    /// Facts for the ladder classifier, in the shape the dispatcher gathers
    /// them.
    fn facts(file_size: u64, width: i64, height: i64, duration: Option<f64>) -> ImageFacts {
        ImageFacts {
            file_size,
            dimensions: Some((width, height)),
            duration,
            rotation: None,
            has_transparency: None,
        }
    }

    // The classifier every part of the ladder obeys: the dispatch question,
    // the generator, and — through `is_animated_image` and the raw floor —
    // the serving endpoint. All three have to agree on which items move, or
    // the scan writes a set the dispatcher will never reconcile.
    #[test]
    fn the_ladder_splits_static_animated_and_raw_floor_items() {
        const MB: u64 = 1024 * 1024;
        // Stills, whatever their duration says: a video's tiers come from its
        // stored frame grid, which is a still by construction.
        assert_eq!(grid_ladder("image/jpeg", None), GridLadder::Static);
        assert_eq!(grid_ladder("video/mp4", None), GridLadder::Static);
        assert_eq!(grid_ladder("audio/mpeg", None), GridLadder::Static);
        assert_eq!(
            grid_ladder("image/webp", Some(&facts(9 * MB, 2000, 2000, Some(0.0)))),
            GridLadder::Static,
            "a measured *still* WebP is an ordinary image"
        );
        // No mime type: no generator will ever produce a picture, so the
        // wanted set is empty by rule.
        assert_eq!(grid_ladder("", None), GridLadder::Nothing);

        // Animated, above the raw floor on bytes, on dimensions, or on both.
        assert_eq!(
            grid_ladder("image/gif", Some(&facts(4 * MB, 400, 400, None))),
            GridLadder::Animated,
            "a GIF is animated by mime — the duration measurement runs later"
        );
        assert_eq!(
            grid_ladder("image/gif", Some(&facts(200 * 1024, 800, 600, Some(3.0)))),
            GridLadder::Animated
        );
        assert_eq!(
            grid_ladder("image/webp", Some(&facts(6 * MB, 1200, 1200, Some(2.0)))),
            GridLadder::Animated
        );

        // Under the raw floor: served as-is, nothing stored at all.
        assert_eq!(
            grid_ladder("image/gif", Some(&facts(300 * 1024, 320, 240, Some(1.0)))),
            GridLadder::Nothing
        );
        assert_eq!(
            grid_ladder("image/webp", Some(&facts(MB, 512, 512, Some(1.0)))),
            GridLadder::Nothing
        );

        // A measured-still GIF leaves the animated ladder entirely: a single
        // frame is not an animation, and an eternal one-frame mp4 is not a
        // rendition anyone wants.
        assert_eq!(
            grid_ladder("image/gif", Some(&facts(4 * MB, 1400, 1400, Some(0.0)))),
            GridLadder::Static
        );

        // Undecidable: an animated item whose dimensions were never measured.
        // Neither "empty" (which would retire a correct set) nor "animated"
        // (which needs geometry nobody has) is a safe guess.
        assert_eq!(grid_ladder("image/gif", None), GridLadder::Unknown);
        assert_eq!(
            grid_ladder(
                "image/gif",
                Some(&ImageFacts {
                    file_size: 4 * MB,
                    dimensions: None,
                    duration: Some(2.0),
                    rotation: None,
                    has_transparency: None,
                })
            ),
            GridLadder::Unknown
        );

        // The new-item pass asks the same question off its own decode.
        assert_eq!(
            first_pass_ladder("image/gif", Some(2.0), 4 * MB, 400, 400),
            GridLadder::Animated
        );
        assert_eq!(
            first_pass_ladder("image/gif", Some(0.0), 4 * MB, 1400, 1400),
            GridLadder::Static
        );
        assert_eq!(
            first_pass_ladder("image/gif", Some(2.0), 300 * 1024, 320, 240),
            GridLadder::Nothing
        );
        assert_eq!(
            first_pass_ladder("image/png", None, 30 * MB, 8000, 8000),
            GridLadder::Static
        );
    }

    // The pre-spans window, walked as the transition it is. An animated
    // *container* nothing has measured is not static: classifying it `Static`
    // writes static tiers for a picture that may well move, serves them
    // immutably in the window before the measurement lands, and leaves them
    // unreachable — on the next scan a genuinely still file answers
    // identically, so nothing distinguishes the two. Only the scan can see
    // this; `is_animated_image` is shared with the endpoint and must not move.
    #[test]
    fn an_unmeasured_animated_container_is_undecidable_until_it_is_measured() {
        const MB: u64 = 1024 * 1024;
        for mime in ["image/webp", "image/avif"] {
            // Indexed before the animation question existed.
            assert_eq!(
                grid_ladder(mime, Some(&facts(6 * MB, 2000, 2000, None))),
                GridLadder::Unknown,
                "{mime} with no measurement must not be called still"
            );
            // The measurement lands, and the item settles either way.
            assert_eq!(
                grid_ladder(mime, Some(&facts(6 * MB, 2000, 2000, Some(0.0)))),
                GridLadder::Static,
                "{mime} measured still takes the static ladder"
            );
            assert_eq!(
                grid_ladder(mime, Some(&facts(6 * MB, 2000, 2000, Some(2.0)))),
                GridLadder::Animated
            );
            // Below the raw floor it wants nothing at all, measured or not.
            assert_eq!(
                grid_ladder(mime, Some(&facts(300 * 1024, 400, 400, Some(2.0)))),
                GridLadder::Nothing
            );
        }

        // The containers the animation question never measures are decided
        // immediately, exactly as before: their `duration` is NULL forever.
        assert_eq!(
            grid_ladder("image/png", Some(&facts(6 * MB, 2000, 2000, None))),
            GridLadder::Static
        );
        assert_eq!(
            grid_ladder("image/jpeg", Some(&facts(6 * MB, 2000, 2000, None))),
            GridLadder::Static
        );
        // And a video is never in this family at all.
        assert_eq!(grid_ladder("video/mp4", None), GridLadder::Static);
    }

    // The ladder question's mime early-out must cover exactly the types a
    // generator produces a picture for; anything wider costs two storage
    // queries per file per scan, forever, for a structural `None`.
    #[test]
    fn only_types_with_a_generator_reach_the_ladder_question() {
        for mime in [
            "image/png",
            "video/mp4",
            "audio/mpeg",
            "application/pdf",
            "text/html",
        ] {
            assert!(mime_can_have_renditions(mime), "{mime}");
        }
        for mime in [
            "text/plain",
            "application/zip",
            "application/epub+zip",
            "",
        ] {
            assert!(!mime_can_have_renditions(mime), "{mime}");
        }
    }
}
