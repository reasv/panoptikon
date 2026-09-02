//! The stored-rendition ladder every visual surface serves from
//! (`docs/grid-scroll-performance-implementation.md` §2).
//!
//! Four static tiers, all derived from one decode:
//!
//! * `display` — gallery quality and the browser-safety bound. *Whether* one
//!   is stored is a short side over 4096, over 24 MP, or bytes over the source
//!   class's own bound; *what shape* it takes once stored is the whole image
//!   with its short side capped at 2560. Two separate questions, deliberately
//!   (docs/thumbnail-format-implementation.md §2 R2).
//! * `grid-m` (1024), `grid-s` (512) and `grid-xs` (256) — the grid,
//!   filmstrip and small-pin tiers, capped on the **short** side because a
//!   cell that paints with `object-cover` is only ever as crisp as that.
//!   Requesting the smallest tier whose short side covers the cell box is what
//!   keeps decoded megapixels per screenful constant as cells shrink.
//!
//! …and, for an animated item above the raw floor, renditions that are not
//! tiers at all: ONE H.264 `loop` answering every grid tier, a `loop-display`
//! where the display answer is a loop the grid one cannot stand in for, plus
//! the static posters `still=true` selects.
//!
//! **Format is a policy, never a per-image byte contest** — a contest between
//! quality-unequal candidates picked the worse file 82% of the time. Grid
//! tiers are JPEG, because the grid is decode-bound and WebP decodes 2.2–2.7x
//! slower per megapixel; a display rendition follows its source (a lossless
//! source becomes WebP, a JPEG stays JPEG); pixels with transparency go to
//! WebP everywhere; and the per-database [`FormatPolicy`] constrains all of it.
//!
//! The rules here are pure functions of
//! `(mime, bytes, width, height, duration, has_transparency, policy)` —
//! deliberately, because the scan's backfill
//! dispatcher has to answer "does this item already have what the current
//! generator would produce?" from *indexed metadata*, never by decoding the
//! file again (`jobs::files::maybe_dispatch_backfill`). Anything that made a
//! rendition's geometry or format depend on the pixels would re-decode every
//! image in the library on every scan, forever — which is exactly why
//! transparency is a *stored column* and not a question asked at dispatch.
//!
//! # The animated fallback ladder
//!
//! Written out once, here, because it spans three files that each see one end
//! of it: `api::items::animated_tier_response` serves it, the scan's backfill
//! decides what there is to serve, and `ui/lib/thumbnailTier.ts` decides which
//! element asks. Their own docs point here rather than restating it.
//!
//! A grid request for an animated item, `still` unset, is answered:
//!
//! * **the stored loop** where one exists — `video/mp4`, the exact answer at
//!   every grid tier, immutable;
//! * **the original file, revalidating**, where no loop row exists yet: the
//!   backfill has not reached this item, so the answer must not be pinned;
//! * **the original file, immutable**, where the loop row exists and carries
//!   no bytes — the settled keep-the-original verdict (no encode came out
//!   smaller than the source), which is as final as a hit.
//!
//! `still=true` asks for the poster instead, and is deliberately a **no-op at
//! or below the raw floor**: nothing is stored for those items, so both values
//! answer with the original — which animates natively in an `<img>`. That is
//! what lets a client with an incomplete row ask for `still=true` and never
//! put `video/mp4` into an `<img>`. Above the floor it answers the static
//! posters through the ordinary fall-up ladder, never immutably on a fall-up.
//!
//! A poster is never substituted for a missing loop server-side: the grid
//! would then be the one surface where an animated item silently stops moving.
//! The client closes that gap from its own end — a `<video>` that errors
//! latches to the poster permanently — which is what makes the two
//! original-file answers above safe rather than broken.
//!
//! `animated_floor: null` from `/api/client-config` means **no loops at all**:
//! with no floor to evaluate every animated cell asks `still=true`, and
//! nothing mounts a `<video>`.
//!
//! # The keep-the-original sentinel
//!
//! One convention, both rendition tables. A row with an **empty blob** means
//! "no encode of this source came out comfortably smaller, so the original
//! file is the rendition": `thumbnails` for a still ([`still_keeps_original`]),
//! `thumbnail_tiers` for a loop ([`loop_keeps_original`]).
//!
//! The geometry is stored all the same, or the backfill dispatcher would ask
//! for the rendition again on every scan forever — the row *is* the answer,
//! as final as a hit, and the endpoint serves the item's own file for it.
//!
//! Its `media_type` names the format the generator **tried**, never the
//! source's own type. A sentinel is final only while that format is still the
//! verdict: the judgement is about one encoder, so a later format flip — a
//! policy edit, a transparency measurement — has to be able to see which one
//! made it and try the other. Naming the source instead froze the sentinel
//! across every format change and, where the source's type happened to be the
//! rendition's, made a real rendition indistinguishable from a verdict.
//!
//! One consequence worth stating, because it is what the rule buys: comparing
//! a stored row against a wanted one is then plain equality on the media
//! type, with no exception anywhere, for either table.

use image::{DynamicImage, GenericImageView, imageops::FilterType};
use serde::Deserialize;
use std::borrow::Cow;
use utoipa::ToSchema;

/// Display tier: the largest short side served without a stored rendition.
pub(crate) const DISPLAY_MAX_SHORT_SIDE: u32 = 4096;
/// Display tier: the largest total pixel count served from the original.
/// Decimal megapixels, so the bound reads as the "24 MP" it is written as and
/// a 6000x4000 camera JPEG is exactly on it (the comparison is `>`).
///
/// Down from 32 MP, which was 4096 squared and doubled for a 2:1 aspect — a
/// consequence of the old rule's shape rather than a chosen number.
pub(crate) const DISPLAY_MAX_PIXELS: u64 = 24_000_000;

/// Display tier: the short side a **stored** rendition is capped at.
///
/// Distinct from [`DISPLAY_MAX_SHORT_SIDE`], which is the *trigger*: 4096
/// decides whether a rendition exists at all, 2560 is the shape it takes once
/// it does. 2560 fills a 4K monitor 1:1 in either orientation with margin and
/// removes 32% more bytes than 4096 for a mean screen-fit SSIM loss of 0.0025
/// (measured 2026-09-01 over the user's four corpora).
const DISPLAY_RENDITION_SHORT_SIDE: u32 = 2560;

/// Display tier: the byte bound of a **lossless** original (PNG, BMP, TIFF,
/// and every still container that is neither JPEG nor WebP).
///
/// Measured: a WebP rendition of a PNG saves 80–90% of its bytes from the
/// 1–2 MiB bucket up, and only below 1 MiB does the keep-the-original sentinel
/// start firing.
pub(crate) const DISPLAY_MAX_FILE_SIZE_LOSSLESS: u64 = 2 * 1024 * 1024;

/// Display tier: the byte bound of a **JPEG** original.
///
/// Measured 2026-09-02 (Phase E): a JPEG-encoded downscale saves 57–62% of
/// bytes from 2 MiB up and decodes in 0.56–0.68x the original's time. 4 MiB
/// rather than 2 to halve the regeneration footprint on photo libraries.
///
/// Its own constant because bytes mean different things per format: a 5 MiB
/// PNG is a modest picture, a 5 MiB JPEG a large efficient one, and a 600 KiB
/// 2400x3600 JPEG is already what the gallery wants to paint.
const DISPLAY_MAX_FILE_SIZE_JPEG: u64 = 4 * 1024 * 1024;

/// Display tier: the byte bound of an **animated** original, above which its
/// `display` answer becomes a stored H.264 loop (R3).
///
/// The user's judgement call for GIFs rather than a measurement; a bound
/// justified by per-frame render cost would need a decode trace nothing has
/// taken yet (§9).
pub(crate) const DISPLAY_MAX_FILE_SIZE_ANIMATED: u64 = 5 * 1024 * 1024;

/// The largest side libwebp can encode. A rendition past it on either axis
/// falls back to JPEG at the same quality — in practice the tall strips, whose
/// display rendition keeps its 800 px short side and runs to tens of thousands
/// of rows.
const WEBP_MAX_SIDE: u32 = 16383;

/// The largest side a JPEG can encode: its frame header carries the
/// dimensions in 16 bits.
///
/// Four times WebP's, so it is only reached by the shapes that fell back to
/// JPEG *because* of WebP's — a 200x100000 strip keeps its short side under
/// the 2560 cap and its length with it. Past this there is no container left
/// to store the rendition in, which is a verdict about the shape and not an
/// encoder failure to report ([`display_plan`]).
const JPEG_MAX_SIDE: u32 = 65535;

/// Grid tiers: JPEG quality. One step below the display tier's, because a
/// grid cell paints the picture at a fraction of its size and the ladder's
/// whole point is bytes and decode time per cell.
const GRID_JPEG_QUALITY: u8 = 83;
/// Display renditions: JPEG quality — today's number, unchanged, so a
/// re-encode is a format decision and never a quality regression.
const DISPLAY_JPEG_QUALITY: u8 = 85;
/// Grid tiers: WebP quality, for the transparent items and the
/// storage-constrained policy.
const GRID_WEBP_QUALITY: f32 = 85.0;
/// Display renditions: WebP quality. 90 is where a WebP of a lossless source
/// measured 11x smaller at decode parity or better.
const DISPLAY_WEBP_QUALITY: f32 = 90.0;

/// A stored rendition must be at most this fraction of its source's bytes, or
/// the source itself is the better answer and a sentinel row records that.
/// Expressed as a rational so the comparison stays integral.
const KEEP_ORIGINAL_NUMERATOR: u64 = 3;
const KEEP_ORIGINAL_DENOMINATOR: u64 = 4;

/// Grid tiers: the largest original served directly, as a multiple of the
/// tier's short side. 1.25x — a quarter over budget decodes cheaply enough
/// that storing a near-identical rendition would cost more than it saves.
/// Expressed as a ratio so the comparison stays integral. The same slack
/// applies to the **long** side against `2 * tier`, which is the longest a
/// stored tier is ever allowed to be.
const GRID_DIRECT_NUMERATOR: u64 = 5;
const GRID_DIRECT_DENOMINATOR: u64 = 4;
/// Grid tiers: the largest original served directly by byte count.
const GRID_DIRECT_MAX_FILE_SIZE: u64 = 8 * 1024 * 1024;

/// The aspect above which a grid tier becomes a *crop* rather than a
/// whole-image resize. At or below it, `long <= 2 * short`, and the tier is
/// the plain short-side resize of the whole picture. Equivalently: a stored
/// tier's long side never exceeds `2 * tier`, which is what the serve rule's
/// long-side bound is measured against.
const GRID_MAX_WHOLE_ASPECT: u32 = 2;

/// Animated raw floor (§2, step B2): an animated original this small is
/// served as-is at every grid tier, and nothing is stored for it — no loop,
/// no poster. **Both** clauses have to hold; a 900 KB 600x600 GIF is over the
/// floor on dimensions alone.
pub(crate) const ANIMATED_RAW_MAX_FILE_SIZE: u64 = 1024 * 1024;
/// The other half of the animated raw floor: neither side may exceed this.
pub(crate) const ANIMATED_RAW_MAX_SIDE: u32 = 512;

/// The animated loop's short-side cap. ONE loop per item, reused by every
/// grid tier — an H.264 stream is not a ladder: a smaller cell paints the
/// same decode scaled down, and a second encode would double the scan's most
/// expensive visual for no measurable decode saving.
const LOOP_MAX_SHORT_SIDE: u32 = 1024;

/// The stored discriminator of the animated loop, in the same column as the
/// `grid-*` posters and deliberately not a [`ThumbnailTier`]: the loop is a
/// rendition *kind*, not a `size=` value. It answers **every** grid tier.
pub(crate) const LOOP_TIER: &str = "loop";

/// The second loop row, stored only for an animated item whose **display**
/// answer is a loop and whose grid loop is not already the whole picture at
/// native resolution (R3). Capped at [`DISPLAY_RENDITION_SHORT_SIDE`], whole
/// image, no crop — the display shape, not the grid's.
pub(crate) const LOOP_DISPLAY_TIER: &str = "loop-display";

/// The loop's stored media type, and what the endpoint serves it as.
pub(crate) const LOOP_MEDIA_TYPE: &str = "video/mp4";

/// What one row of `storage.thumbnail_tiers` *is*.
///
/// The `tier` column holds one of five strings, and three separate facts hang
/// off which one: the media type the row carries, the generator version it is
/// stamped with, and whether it can stand in the keep-the-original sentinel
/// state. Reading those off the string at each site is how a fourth kind gets
/// added and one of the three is missed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RenditionKind {
    /// A still: a grid tier of a picture, or an animated item's poster.
    Still(ThumbnailTier),
    /// The H.264 loop that answers **every** grid tier of an animated item.
    Loop,
    /// The second loop, for an animated item whose display answer is a loop
    /// the grid one cannot stand in for (R3).
    LoopDisplay,
}

impl RenditionKind {
    /// The stored discriminator. The strings are frozen: they are the `tier`
    /// column and, through it, part of every rendition ETag.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Still(tier) => tier.as_str(),
            Self::Loop => LOOP_TIER,
            Self::LoopDisplay => LOOP_DISPLAY_TIER,
        }
    }

    /// The media type a row of this kind carries. `format` is the still
    /// ladder's verdict (R1/R4/R5) and has no say over a loop, which is
    /// always H.264 in an mp4.
    pub(crate) fn media_type(self, format: RenditionFormat) -> &'static str {
        match self {
            Self::Still(_) => format.media_type(),
            Self::Loop | Self::LoopDisplay => LOOP_MEDIA_TYPE,
        }
    }

    /// Whether this is one of the two H.264 rows — the ones an ffmpeg run
    /// produces, and the ones a still-encoder bump must not touch.
    pub(crate) fn is_loop(self) -> bool {
        matches!(self, Self::Loop | Self::LoopDisplay)
    }

    /// The generator version a row of this kind is stamped with. See the
    /// version table in `crate::jobs::files`.
    pub(crate) fn process_version(self) -> i64 {
        if self.is_loop() {
            crate::jobs::files::LOOP_PROCESS_VERSION
        } else {
            crate::jobs::files::TIER_PROCESS_VERSION
        }
    }
}

/// One rendition of an item's picture.
///
/// The wire values are the frozen `size=` parameter of
/// `GET /api/items/item/thumbnail`; do not rename them.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ThumbnailTier {
    /// The gallery/default rendition. Requesting it is exactly the
    /// pre-ladder behaviour, which is why it is the parameter's default.
    #[default]
    Display,
    GridM,
    GridS,
    GridXs,
}

impl ThumbnailTier {
    /// The stored discriminator, and the wire value. Kept in one place so the
    /// column, the ETag and the query parameter can never disagree.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Display => "display",
            Self::GridM => "grid-m",
            Self::GridS => "grid-s",
            Self::GridXs => "grid-xs",
        }
    }

    /// The short-side cap in pixels, or `None` for the display tier, whose
    /// rule is a different shape entirely (short side *and* total pixels
    /// *and* bytes, see [`display_plan`]).
    pub(crate) fn short_side(self) -> Option<u32> {
        match self {
            Self::Display => None,
            Self::GridM => Some(1024),
            Self::GridS => Some(512),
            Self::GridXs => Some(256),
        }
    }

    /// The grid tiers, largest first — the order generation cascades in.
    pub(crate) const GRID: [Self; 3] = [Self::GridM, Self::GridS, Self::GridXs];
}

/// The container a still rendition is stored in.
///
/// A *policy* per content class, never a per-image byte contest: a contest
/// between quality-unequal candidates picked the worse file 82% of the time in
/// the bake-off, because bytes alone cannot see that the smaller file is also
/// the blurrier one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RenditionFormat {
    Jpeg,
    Webp,
}

impl RenditionFormat {
    /// The stored `media_type`, and what the endpoint serves it as.
    pub(crate) fn media_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
        }
    }

    /// The format a stored `media_type` names, or `None` for a type that is
    /// not a still rendition at all — the loop rows' [`LOOP_MEDIA_TYPE`], and
    /// anything a future version writes.
    pub(crate) fn from_media_type(media_type: &str) -> Option<Self> {
        [Self::Jpeg, Self::Webp]
            .into_iter()
            .find(|format| format.media_type() == media_type)
    }

    /// The filename extension a rendition in this format is offered under.
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Webp => "webp",
        }
    }

    /// Whether alpha survives into this container for a picture whose pixels
    /// carry it (R4's other half).
    ///
    /// Only WebP does. Every JPEG here flattens — including the fallback a
    /// policy without `webp`, or a side past [`WEBP_MAX_SIDE`], forces — so
    /// the question is the format's own and not the caller's to re-derive.
    fn keeps_alpha(self, has_transparency: Option<bool>) -> bool {
        self == Self::Webp && has_transparency == Some(true)
    }
}

/// Whether a rendition of these dimensions fits libwebp ([`WEBP_MAX_SIDE`]).
///
/// Only a **display** rendition can reach the limit, which is why it is asked
/// there and nowhere else: a grid tier's long side is at most `2 * tier` by
/// construction ([`tier_plan`]), and a source too big for that is cropped, not
/// carried.
fn fits_webp(width: u32, height: u32) -> bool {
    width.max(height) <= WEBP_MAX_SIDE
}

/// Whether a rendition of these dimensions fits a JPEG frame header
/// ([`JPEG_MAX_SIDE`]).
fn fits_jpeg(width: u32, height: u32) -> bool {
    width.max(height) <= JPEG_MAX_SIDE
}

/// Which rung of the ladder a rendition belongs to.
///
/// Every per-rung encoder setting is looked up *from this* rather than passed
/// in beside it — the two JPEG and WebP qualities here, the two H.264 rate
/// factors in [`crate::media_tools::animated_loop`] — so a grid tier and a
/// display rendition can never be encoded at each other's settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RenditionRung {
    Grid,
    Display,
}

/// The container every picture this **generator** produced is stored in — a
/// video's frame grid, an audio cover, a rendered PDF page, an HTML
/// screenshot, and every grid tier derived from one of them.
///
/// Deliberately outside the format policy (R5) and outside R4. Those pictures
/// are opaque by construction, and the format rules are written about a
/// *user's* file: making a video's stills follow a `thumbnail_formats` edit
/// would regenerate every video rendition in the library for a setting that
/// is about photographs (§4). Its own constant so the four sites that write
/// or predict one cannot drift.
pub(crate) const GENERATED_STILL_FORMAT: RenditionFormat = RenditionFormat::Jpeg;

/// The per-database format policy (R5), folded from
/// `SystemConfig::thumbnail_formats` once per scan.
///
/// It *constrains* R1–R4 rather than deciding anything itself: with `webp`
/// absent every WebP verdict becomes JPEG (alpha flattened, as today), with
/// `jpeg` absent every JPEG verdict becomes WebP — the storage-constrained
/// deployment, which knowingly pays the measured 2.2–2.7x decode cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FormatPolicy {
    pub jpeg: bool,
    pub webp: bool,
}

impl Default for FormatPolicy {
    fn default() -> Self {
        Self {
            jpeg: true,
            webp: true,
        }
    }
}

impl FormatPolicy {
    /// The policy named by a `thumbnail_formats` list.
    ///
    /// A list naming neither format is treated as the default **with a
    /// warning**, never as a rejection: the settings UI round-trips the whole
    /// config, so a commit-path reject would break every unrelated save
    /// (CLAUDE.md, learned from the int8 quant remap).
    pub(crate) fn from_names(names: &[String]) -> Self {
        let policy = Self {
            jpeg: names.iter().any(|name| name == "jpeg"),
            webp: names.iter().any(|name| name == "webp"),
        };
        if !policy.jpeg && !policy.webp {
            tracing::warn!(
                formats = ?names,
                "thumbnail_formats names no usable format; using the default"
            );
            return Self::default();
        }
        policy
    }

    /// The format actually stored for a verdict of `wanted`, as far as this
    /// database's policy can say.
    ///
    /// Dimensions are deliberately not part of the question. The one size
    /// limit that binds a rendition is libwebp's, it binds only a *display*
    /// rendition ([`fits_webp`]), and folding it in here meant every caller
    /// that had no dimensions to give — the grid tiers, whose limit cannot
    /// bind — passed a `1, 1` that meant nothing.
    fn constrain(self, wanted: RenditionFormat) -> RenditionFormat {
        match wanted {
            RenditionFormat::Jpeg if !self.jpeg => RenditionFormat::Webp,
            RenditionFormat::Webp if !self.webp => RenditionFormat::Jpeg,
            format => format,
        }
    }
}

/// The content class a display rendition's byte bound and format follow (R2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceClass {
    /// PNG, BMP, TIFF, and every other still container that is neither JPEG
    /// nor WebP.
    Lossless,
    Jpeg,
    Webp,
    /// A moving picture, whose display answer is a loop rather than a still
    /// (R3).
    Animated,
}

/// Which class an item's original belongs to. `animated` is
/// [`is_animated_image`]'s verdict, which the callers already hold.
fn source_class(mime_type: &str, animated: bool) -> SourceClass {
    if animated {
        return SourceClass::Animated;
    }
    if mime_type.starts_with("image/jpeg") || mime_type.starts_with("image/jpg") {
        return SourceClass::Jpeg;
    }
    if mime_type.starts_with("image/webp") {
        return SourceClass::Webp;
    }
    SourceClass::Lossless
}

/// The byte count above which a class's original stops being served as-is, or
/// `None` where bytes never trigger a rendition.
///
/// WebP is the `None`: measured, a rendition of a WebP source saved under 50%
/// and tripped the keep-the-original sentinel half the time, so its bytes say
/// nothing worth acting on.
fn display_byte_bound(class: SourceClass) -> Option<u64> {
    match class {
        SourceClass::Lossless => Some(DISPLAY_MAX_FILE_SIZE_LOSSLESS),
        SourceClass::Jpeg => Some(DISPLAY_MAX_FILE_SIZE_JPEG),
        SourceClass::Webp => None,
        SourceClass::Animated => Some(DISPLAY_MAX_FILE_SIZE_ANIMATED),
    }
}

/// What the display rule wants for an item with these measurements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayPlan {
    /// Serve the original file; store nothing.
    Original,
    /// Store a still rendition of exactly this geometry, in this format.
    Thumbnail {
        plan: TierPlan,
        format: RenditionFormat,
    },
    /// Serve a stored H.264 loop (R3), named by the row that holds it:
    /// [`LOOP_TIER`] where the grid loop is already the whole picture at
    /// native resolution, [`LOOP_DISPLAY_TIER`] where a second encode is
    /// owed. The geometry lives with the plan that produces the row
    /// ([`animated_plans`]) and never here — nothing that reads this needs
    /// it, and two copies of one derivation is how they come to disagree.
    Loop { tier: &'static str },
}

/// The display answer's *shape*, before any format has been decided.
///
/// The half of the rule that is pure geometry and triggers, so the half the
/// **serving** side can ask: a stored row carries its own media type, so the
/// endpoint needs no policy, no transparency verdict and no encoder to know
/// whether a `display` request is answered from the item's own file, from a
/// still rendition, or from a loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayShape {
    /// Serve the original file; store nothing.
    Original,
    /// Store a still rendition of exactly this geometry: the whole picture,
    /// resized. A [`TierPlan`] like every other rendition's, so one `render`
    /// serves them all.
    Still { plan: TierPlan },
    /// Serve the stored H.264 loop this row holds (R3).
    Loop { tier: &'static str },
}

/// The display rule (R2 for stills, R3 for moving pictures) — the one
/// function that answers "what does a `display` request for this item get,
/// and what does the scan store for it?".
///
/// **The trigger and the shape are two separate questions.** A rendition is
/// stored iff the short side exceeds [`DISPLAY_MAX_SHORT_SIDE`] (4096), or the
/// pixel count exceeds [`DISPLAY_MAX_PIXELS`] (24 MP), or the bytes exceed the
/// bound of the source's own class ([`display_byte_bound`]) — because bytes
/// mean different things per format, and a 600 KiB 2400x3600 JPEG is already
/// the picture the gallery paints. Once one fires, the shape is the whole
/// image with its short side capped at [`DISPLAY_RENDITION_SHORT_SIDE`] (2560)
/// and then the same 24 MP bound.
///
/// **The format follows the source class.** Lossless sources become WebP,
/// which measured 11x smaller at decode parity or better; a JPEG source stays
/// JPEG, because a WebP of a JPEG decodes 2.33x slower than the JPEG it
/// replaces while a JPEG downscale is both smaller *and* faster. Transparency
/// overrides both (R4), and the policy constrains the result (R5).
///
/// **A moving picture never gets a still.** Its display answer is its own
/// file — which animates natively in an `<img>` — until the same trigger
/// fires on the animated class's bound, and then it is an H.264 loop.
pub(crate) fn display_shape(
    mime_type: &str,
    animated: bool,
    file_size: u64,
    width: u32,
    height: u32,
) -> DisplayShape {
    if width == 0 || height == 0 {
        return DisplayShape::Original;
    }
    let class = source_class(mime_type, animated);
    if !display_trigger_fires(class, file_size, width, height) {
        return DisplayShape::Original;
    }
    if class == SourceClass::Animated {
        return match animated_display_loop(file_size, width, height) {
            Some(tier) => DisplayShape::Loop { tier },
            // Unreachable: the trigger that got us here is the one
            // [`animated_display_loop`] asks. The arm exists so the two
            // cannot be made to disagree silently.
            None => DisplayShape::Original,
        };
    }
    let (out_width, out_height) = display_dimensions(width, height);
    // A shape no container can name. The 2560 cap is on the *short* side, so
    // a 200x100000 strip keeps every one of its rows: too long for WebP,
    // which sends it to JPEG, and too long for JPEG's 16-bit frame header
    // too. [`JPEG_MAX_SIDE`] is the larger of the two limits and the one the
    // WebP fallback lands on, so it bounds every format and belongs to the
    // shape rather than to any of them. It has to be reached *here*, or the
    // plan names a rendition the generator cannot produce and the item is
    // dispatched, made to fail and dispatched again on every scan forever.
    if !fits_jpeg(out_width, out_height) {
        return DisplayShape::Original;
    }
    DisplayShape::Still {
        plan: whole_image_plan(width, height, out_width, out_height),
    }
}

/// The plan that resizes the whole picture onto `(width, height)`.
///
/// A display surface shows all of it, so this never crops — which is the one
/// thing separating a display rendition, still or loop, from every grid one.
fn whole_image_plan(source_width: u32, source_height: u32, width: u32, height: u32) -> TierPlan {
    TierPlan {
        crop_x: 0,
        crop_y: 0,
        crop_width: source_width.max(1),
        crop_height: source_height.max(1),
        width,
        height,
    }
}

/// [`display_shape`] with the format decided: the whole display rule, and
/// what the **scan** asks, because storing a rendition means choosing a
/// container for it (R2's format half, R4 and R5).
pub(crate) fn display_plan(
    mime_type: &str,
    animated: bool,
    has_transparency: Option<bool>,
    file_size: u64,
    width: u32,
    height: u32,
    policy: FormatPolicy,
) -> DisplayPlan {
    match display_shape(mime_type, animated, file_size, width, height) {
        DisplayShape::Original => DisplayPlan::Original,
        DisplayShape::Loop { tier } => DisplayPlan::Loop { tier },
        DisplayShape::Still { plan } => {
            let wanted = policy.constrain(display_format(
                source_class(mime_type, animated),
                has_transparency,
            ));
            // libwebp's own limit, and the only rendition that can reach it:
            // the tall strips, whose display rendition keeps its short side
            // and runs to tens of thousands of rows. JPEG at the same
            // quality, alpha flattened as it always is there.
            let format = match wanted {
                RenditionFormat::Webp if !fits_webp(plan.width, plan.height) => {
                    RenditionFormat::Jpeg
                }
                format => format,
            };
            DisplayPlan::Thumbnail { plan, format }
        }
    }
}

/// Which loop row answers an animated item's display request, or `None` where
/// the answer is the original file (R3).
///
/// The one derivation, read by [`display_shape`] for the serving side and by
/// [`animated_plans`] for the set the scan stores. Two copies of it is how
/// the endpoint comes to look for a row the generator never wrote.
fn animated_display_loop(file_size: u64, width: u32, height: u32) -> Option<&'static str> {
    if !display_trigger_fires(SourceClass::Animated, file_size, width, height) {
        return None;
    }
    Some(if grid_loop_is_the_display_loop(width, height) {
        LOOP_TIER
    } else {
        LOOP_DISPLAY_TIER
    })
}

/// Whether the display trigger fires: dimensions shared across classes, bytes
/// per class.
fn display_trigger_fires(class: SourceClass, file_size: u64, width: u32, height: u32) -> bool {
    let short = width.min(height);
    let pixels = u64::from(width) * u64::from(height);
    short > DISPLAY_MAX_SHORT_SIDE
        || pixels > DISPLAY_MAX_PIXELS
        || bytes_over_bound(class, file_size)
}

/// Whether an item's **bytes** alone put it over the display trigger.
///
/// The one clause of the rule that needs no dimensions, which is what makes
/// it the whole answer for an image whose width and height were never
/// indexed. Exposed as its own question so that caller reads the same
/// statement [`display_trigger_fires`] does rather than reassembling it out
/// of the class table.
pub(crate) fn display_bytes_trigger(mime_type: &str, animated: bool, file_size: u64) -> bool {
    bytes_over_bound(source_class(mime_type, animated), file_size)
}

fn bytes_over_bound(class: SourceClass, file_size: u64) -> bool {
    display_byte_bound(class).is_some_and(|bound| file_size > bound)
}

/// The unconstrained format verdict for a still display rendition, before the
/// policy and the WebP size limit have their say.
fn display_format(class: SourceClass, has_transparency: Option<bool>) -> RenditionFormat {
    // R4: a picture with a non-opaque pixel is WebP whatever its container,
    // because the alternative is flattening it. Measured from pixels and never
    // from the header — only 2.3% of PNGs have one, while 50% carry an alpha
    // *channel*.
    if has_transparency == Some(true) {
        return RenditionFormat::Webp;
    }
    match class {
        SourceClass::Jpeg => RenditionFormat::Jpeg,
        // Animated items store no still rendition at all; the arm exists so
        // the match stays total rather than as a reachable verdict.
        SourceClass::Lossless | SourceClass::Webp | SourceClass::Animated => RenditionFormat::Webp,
    }
}

/// The format of an item's **grid** renditions and, for an animated item, of
/// its posters (R1 + R4 + R5).
///
/// JPEG unless the pixels carry transparency: grid tiers are what a scrolling
/// screenful decodes, and WebP decodes 2.2–2.7x slower per megapixel.
/// The WebP size limit cannot bind here and is deliberately not asked about
/// ([`fits_webp`]): a grid rendition's long side is at most `2 * tier` =
/// 2048 px by construction, so only the policy can overrule the verdict.
pub(crate) fn tier_format(has_transparency: Option<bool>, policy: FormatPolicy) -> RenditionFormat {
    let wanted = if has_transparency == Some(true) {
        RenditionFormat::Webp
    } else {
        RenditionFormat::Jpeg
    };
    policy.constrain(wanted)
}

/// Whether a still rendition is *not* worth storing, so the original is the
/// answer and a sentinel row records that (see this module's docs).
///
/// Not a byte contest between candidates — the format is already decided — but
/// the floor under it: an efficient 6 MiB JPEG whose re-encode saves nothing
/// would otherwise cost a second copy of itself for no gain. The rendition
/// has to come in at three quarters of the source or better.
///
/// Named for its answer rather than against it, matching
/// [`loop_keeps_original`]: the two sentinels are one convention, and reading
/// them at opposite polarities is how a call site comes to invert one.
pub(crate) fn still_keeps_original(encoded_len: u64, source_len: u64) -> bool {
    encoded_len * KEEP_ORIGINAL_DENOMINATOR > source_len * KEEP_ORIGINAL_NUMERATOR
}

/// `min(2560/short, sqrt(24MP/pixels), 1)` applied to both sides.
///
/// The `1` matters: a file that only broke the *byte* bound keeps its pixel
/// dimensions and is simply re-encoded, which is the whole point of that
/// bound.
fn display_dimensions(width: u32, height: u32) -> (u32, u32) {
    let short = f64::from(width.min(height));
    let pixels = f64::from(width) * f64::from(height);
    let scale = (f64::from(DISPLAY_RENDITION_SHORT_SIDE) / short)
        .min((DISPLAY_MAX_PIXELS as f64 / pixels).sqrt())
        .min(1.0);
    (scale_side(width, scale), scale_side(height, scale))
}

fn scale_side(side: u32, scale: f64) -> u32 {
    let scaled = (f64::from(side) * scale).round();
    if scaled < 1.0 { 1 } else { scaled as u32 }
}

/// The geometry of one grid rendition: a crop in source pixels, then an exact
/// resize. Both halves are recorded so the dispatcher can predict the stored
/// dimensions without touching a pixel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TierPlan {
    pub crop_x: u32,
    pub crop_y: u32,
    pub crop_width: u32,
    pub crop_height: u32,
    pub width: u32,
    pub height: u32,
}

/// The grid tier geometry for a picture of `width` x `height` at a tier whose
/// short-side cap is `tier` (§2).
///
/// One formula covers both settled cases:
///
/// * **aspect <= 2** — the crop cap never binds, so this is the plain
///   short-side resize of the whole image.
/// * **aspect > 2** — the long side is cropped to what scales to `2 * tier`,
///   matching what a cover cell actually paints. `object-position: 50% 0%`
///   means a **tall** image keeps its **top** strip (a two-year-old product
///   decision: portraits keep the face, center-cropping showed torsos) and a
///   **wide** image keeps the horizontally **centered** band.
///
/// Never upscales: the short side target is `min(tier, short)`. By
/// construction the result is at most `2 * tier^2` pixels, so no separate
/// megapixel guard exists or is needed.
pub(crate) fn tier_plan(width: u32, height: u32, tier: u32) -> TierPlan {
    debug_assert!(width > 0 && height > 0 && tier > 0);
    let width = width.max(1);
    let height = height.max(1);
    let tall = height >= width;
    let (short, long) = if tall {
        (width, height)
    } else {
        (height, width)
    };

    // The scale is the exact rational `out_short / short`, so the crop cap
    // and the output length are computed from the same two integers and can
    // never drift apart.
    let out_short = short.min(tier);
    let cap = u64::from(GRID_MAX_WHOLE_ASPECT) * u64::from(tier);
    let crop_long_cap = cap * u64::from(short) / u64::from(out_short);
    let crop_long = u64::from(long).min(crop_long_cap).max(1) as u32;
    let out_long = round_div(
        u64::from(crop_long) * u64::from(out_short),
        u64::from(short),
    );
    let out_long = out_long.max(1);

    if tall {
        TierPlan {
            crop_x: 0,
            // Top strip. Shared by every cover-top consumer (grid cells,
            // gallery filmstrip, pinboard history previews), so the stored
            // crop has to preserve it exactly.
            crop_y: 0,
            crop_width: width,
            crop_height: crop_long,
            width: out_short,
            height: out_long,
        }
    } else {
        TierPlan {
            // Horizontally centered band.
            crop_x: (width - crop_long) / 2,
            crop_y: 0,
            crop_width: crop_long,
            crop_height: height,
            width: out_long,
            height: out_short,
        }
    }
}

/// Half-up integer division, matching `f64::round` on the values this sees.
fn round_div(numerator: u64, denominator: u64) -> u32 {
    ((numerator * 2 + denominator) / (denominator * 2)) as u32
}

/// Whether a grid tier serves an item's **original file** rather than a
/// stored rendition (§2, as refined in adjudication): short side within 1.25x
/// the tier, **long side within 1.25x `2 * tier`**, and bytes within
/// [`GRID_DIRECT_MAX_FILE_SIZE`].
///
/// The long-side bound replaces an `aspect <= 2` clause that refused *every*
/// strip however small. That clause was a proxy for the wrong thing: a
/// 1000x2100 image (aspect 2.1) stored a grid-m "crop" of 1000x2048 — the
/// whole picture, 2% smaller — which is pure waste, while what actually
/// matters is the same thing the short-side clause measures, the decoded
/// pixel count of what the cell paints. `2 * tier` is the longest a stored
/// tier is ever allowed to be ([`tier_plan`]), so an original already
/// within a quarter of that is exactly as cheap to decode as the rendition
/// would have been.
pub(crate) fn grid_serves_original(file_size: u64, width: u32, height: u32, tier: u32) -> bool {
    within_grid_dimensions(width, height, tier) && file_size <= GRID_DIRECT_MAX_FILE_SIZE
}

/// The same question for a rendition **derived from a stored thumbnail** (a
/// video's frame grid, an audio cover, a rendered PDF page): the byte clause
/// is dropped because the source is already a q85 JPEG the generator wrote,
/// never a user file that could be arbitrarily large.
fn grid_serves_stored_thumbnail(width: u32, height: u32, tier: u32) -> bool {
    within_grid_dimensions(width, height, tier)
}

/// Both sides against the largest a stored tier could be: the short side
/// against `tier`, the long side against `2 * tier`, each with the same 1.25x
/// slack.
fn within_grid_dimensions(width: u32, height: u32, tier: u32) -> bool {
    if width == 0 || height == 0 {
        return true;
    }
    let short = u64::from(width.min(height));
    let long = u64::from(width.max(height));
    let tier = u64::from(tier);
    short * GRID_DIRECT_DENOMINATOR <= tier * GRID_DIRECT_NUMERATOR
        && long * GRID_DIRECT_DENOMINATOR
            <= u64::from(GRID_MAX_WHOLE_ASPECT) * tier * GRID_DIRECT_NUMERATOR
}

/// One tier's plan for a source of `width` x `height`: `None` when the source
/// itself is what gets served.
fn grid_plan(file_size: u64, width: u32, height: u32, tier: ThumbnailTier) -> Option<TierPlan> {
    let short_side = tier.short_side()?;
    if width == 0 || height == 0 {
        return None;
    }
    if grid_serves_original(file_size, width, height, short_side) {
        return None;
    }
    Some(tier_plan(width, height, short_side))
}

/// [`grid_plan`] for a rendition derived from a stored thumbnail.
fn grid_plan_for_stored_thumbnail(
    width: u32,
    height: u32,
    tier: ThumbnailTier,
) -> Option<TierPlan> {
    let short_side = tier.short_side()?;
    if width == 0 || height == 0 {
        return None;
    }
    if grid_serves_stored_thumbnail(width, height, short_side) {
        return None;
    }
    Some(tier_plan(width, height, short_side))
}

/// Applies a [`TierPlan`]. `crop_imm` is a view copy, so the resize reads
/// only the pixels the crop kept.
/// A plan that neither crops nor resizes borrows: a display rendition of a
/// picture that only broke the *byte* bound keeps every pixel, and a full
/// Lanczos pass onto its own dimensions would cost the same picture slightly
/// blurrier. `resize_exact`, never `resize`, because the stored dimensions
/// have to be exactly the ones the plan predicts or the backfill's "is this
/// the rendition the current rule wants?" comparison never settles.
pub(crate) fn render<'a>(image: &'a DynamicImage, plan: &TierPlan) -> Cow<'a, DynamicImage> {
    let (width, height) = image.dimensions();
    let whole = plan.crop_x == 0
        && plan.crop_y == 0
        && plan.crop_width == width
        && plan.crop_height == height;
    if whole && plan.width == width && plan.height == height {
        return Cow::Borrowed(image);
    }
    let cropped = if whole {
        Cow::Borrowed(image)
    } else {
        Cow::Owned(image.crop_imm(plan.crop_x, plan.crop_y, plan.crop_width, plan.crop_height))
    };
    if cropped.width() == plan.width && cropped.height() == plan.height {
        return cropped;
    }
    Cow::Owned(cropped.resize_exact(plan.width, plan.height, FilterType::Lanczos3))
}

/// The grid renditions to store for one already-decoded picture, largest
/// first.
///
/// Each rung cascades off the one above it where that one exists — the crops
/// cover the identical source region by construction, so the cascade is exact
/// and every rung after the first resizes from a picture already at most a
/// quarter of the source's pixels. It never cascades off the **display**
/// rendition:
/// a megapixel-guarded display tier can be *smaller* than `grid-m` (a
/// 800x60000 strip scales to 653 px wide), and cropping an already-scaled
/// intermediate would upscale.
pub(crate) fn grid_renditions(
    image: &DynamicImage,
    plans: &[(ThumbnailTier, TierPlan)],
) -> Vec<(ThumbnailTier, DynamicImage)> {
    let mut out: Vec<(ThumbnailTier, DynamicImage)> = Vec::with_capacity(plans.len());
    for (tier, plan) in plans {
        let source = out.last().map(|(_, image)| image).unwrap_or(image);
        // Owned, and it has to be: the next rung reads this one out of the
        // vector it is being pushed into.
        out.push((*tier, render(source, plan).into_owned()));
    }
    out
}

/// The plans for one source, in cascade order: each tier after the first is
/// planned against the dimensions of the one before it, exactly as
/// [`grid_renditions`] renders them. The dispatcher runs this on indexed
/// dimensions alone to learn what a stored set should contain — the two must
/// call the *same* function or the backfill never terminates.
pub(crate) fn grid_plans(
    file_size: u64,
    width: u32,
    height: u32,
) -> Vec<(ThumbnailTier, TierPlan)> {
    cascade(Some(file_size), width, height)
}

/// [`grid_plans`] for a source that is itself a stored thumbnail.
pub(crate) fn grid_plans_for_stored_thumbnail(
    width: u32,
    height: u32,
) -> Vec<(ThumbnailTier, TierPlan)> {
    cascade(None, width, height)
}

/// `file_size` is the source's byte count while the source is still the
/// user's own file, and `None` once a rendition has been produced: from then
/// on the source is a q85 JPEG this generator wrote, so only its dimensions
/// can decide.
///
/// A tier whose source is served as-is is simply absent from the result; the
/// serving ladder falls through to the next larger tier, and then to the
/// display path.
fn cascade(file_size: Option<u64>, width: u32, height: u32) -> Vec<(ThumbnailTier, TierPlan)> {
    let mut out: Vec<(ThumbnailTier, TierPlan)> = Vec::with_capacity(ThumbnailTier::GRID.len());
    let mut source = (width, height);
    let mut bytes = file_size;
    for tier in ThumbnailTier::GRID {
        let plan = match bytes {
            Some(size) => grid_plan(size, source.0, source.1, tier),
            None => grid_plan_for_stored_thumbnail(source.0, source.1, tier),
        };
        let Some(rendition) = plan else {
            // The source is served as-is at this tier. The next, smaller tier
            // is planned against the same source, not against a rendition
            // that was never made.
            continue;
        };
        source = (rendition.width, rendition.height);
        bytes = None;
        out.push((tier, rendition));
    }
    out
}

/// Whether an item's picture *moves* — the one fact that decides which of the
/// two grid ladders it belongs to (§2, step B2).
///
/// The two container families get opposite defaults, because their *unknown*
/// cases are opposite:
///
/// * `image/gif` is animated **unless measured still**. The animation
///   question that stamps `items.duration` runs after the ladder question in
///   the same scan, and a GIF indexed before that feature existed reads
///   `duration IS NULL` here — while the overwhelming majority of GIFs move,
///   so an unmeasured one is treated as animated rather than given static
///   tiers for a picture that moves. Only an explicit `Some(0.0)` — a
///   well-formed file with fewer than two frames, or one whose structure did
///   not parse — takes the static ladder, which is what keeps every
///   single-frame GIF from carrying an eternal one-frame mp4.
/// * Every other image container (WebP today, AVIF when importing lands) is
///   animated only when the measurement says so, because for those formats
///   the static case is the common one.
///
/// Shared by the scan and the serving endpoint, which is why the
/// pre-measurement caution that belongs to the **scan alone** — not writing a
/// WebP's tiers until its animation has been measured — lives in
/// `jobs::files::grid_ladder` rather than here. The two sides must answer
/// this question identically, or a stored rendition becomes unreachable.
pub(crate) fn is_animated_image(mime_type: &str, duration: Option<f64>) -> bool {
    if mime_type.starts_with("image/gif") {
        return !duration.is_some_and(|seconds| seconds <= 0.0);
    }
    mime_type.starts_with("image") && duration.is_some_and(|seconds| seconds > 0.0)
}

/// The animated raw floor: whether an animated original is served as-is at
/// the grid tiers, with nothing stored for it at all.
///
/// Deliberately *both* bounds and deliberately not the static rule's shape: a
/// loop costs an ffmpeg encode per item and a stored mp4 per item, so the
/// floor is where that stops paying for itself. A 512-or-smaller animation
/// under a megabyte decodes cheaply enough in the cell that an H.264
/// rendition of it would be pure overhead.
///
/// A picture with no measured dimensions is **not** under the floor: this is
/// asked by the endpoint, where "unknown" must never become "immutable
/// forever".
pub(crate) fn animated_serves_original(file_size: u64, width: u32, height: u32) -> bool {
    width > 0
        && height > 0
        && file_size <= ANIMATED_RAW_MAX_FILE_SIZE
        && width.max(height) <= ANIMATED_RAW_MAX_SIDE
}

/// The loop's geometry: the [`tier_plan`] of the source at
/// [`LOOP_MAX_SHORT_SIDE`], with both output sides rounded **down** to even.
///
/// Same crop rule as every other grid rendition, so a webtoon's loop is the
/// same top strip its poster is (§2: `object-position: 50% 0%`), and one
/// function keeps the dispatcher, the generator and the endpoint from
/// drifting apart.
///
/// The evenness is yuv420p's: chroma is subsampled 2x2, so an odd side has no
/// legal encoding. Down rather than up because upscaling to satisfy a codec
/// constraint invents pixels; the one exception is a side that would round to
/// zero, which is clamped to 2 (a 1-pixel side can only exist on a degenerate
/// source, and a zero-height video is not a rendition).
pub(crate) fn loop_plan(width: u32, height: u32) -> TierPlan {
    let mut plan = tier_plan(width, height, LOOP_MAX_SHORT_SIDE);
    plan.width = even_side(plan.width);
    plan.height = even_side(plan.height);
    plan
}

fn even_side(side: u32) -> u32 {
    if side < 2 { 2 } else { side & !1 }
}

/// The **display** loop's geometry (R3): the whole image, short side capped at
/// [`DISPLAY_RENDITION_SHORT_SIDE`] and then at [`DISPLAY_MAX_PIXELS`], with
/// both sides rounded down to even for yuv420p.
///
/// Deliberately not [`tier_plan`]: a display surface shows the *whole*
/// picture, so this never crops. That is the one thing separating it from the
/// grid loop, and the reason a strip cannot simply reuse one.
fn loop_display_plan(width: u32, height: u32) -> TierPlan {
    let (out_width, out_height) = display_dimensions(width.max(1), height.max(1));
    whole_image_plan(width, height, even_side(out_width), even_side(out_height))
}

/// Whether an animated item's **grid** loop row is also its display loop (R3),
/// so no second encode is stored.
///
/// The plan states this as "the source short side is at most
/// [`LOOP_MAX_SHORT_SIDE`]", on the grounds that such a loop is already
/// native resolution. That holds exactly while the grid loop is the *whole*
/// picture — and for aspect > 2 it is not: [`loop_plan`] stores a top strip,
/// which is the right thing in a cover cell and the wrong thing on a
/// `object-contain` display surface. So the rule is written as what its own
/// justification says: reuse the grid loop when it is the whole picture at
/// native resolution.
fn grid_loop_is_the_display_loop(width: u32, height: u32) -> bool {
    let (width, height) = (width.max(1), height.max(1));
    let grid = loop_plan(width, height);
    let display = loop_display_plan(width, height);
    (grid.crop_width, grid.crop_height) == (width, height)
        && (grid.width, grid.height) == (display.width, display.height)
}

/// The poster plans for an animated item: the static renditions `still=true`
/// answers with, rendered from the item's first frame.
///
/// Unlike [`grid_plans`] there is no "serve the original" escape, and that is
/// the whole difference: the original *moves*, so it is never an acceptable
/// poster, however small it is. `grid-m` is therefore unconditional — every
/// animated item above the raw floor has exactly one poster to fall up to —
/// while `grid-s` is stored only when it is genuinely smaller, and is
/// answered by the fall-up ladder (`grid-s` -> `grid-m`) when it is not.
/// Nothing is ever stored twice.
pub(crate) fn poster_plans(width: u32, height: u32) -> Vec<(ThumbnailTier, TierPlan)> {
    let mut out = Vec::with_capacity(ThumbnailTier::GRID.len());
    let mut source = (width, height);
    for (index, tier) in ThumbnailTier::GRID.into_iter().enumerate() {
        let Some(short_side) = tier.short_side() else {
            continue;
        };
        let plan = tier_plan(source.0, source.1, short_side);
        // The cascade renders each tier from the one before it, exactly as
        // [`grid_renditions`] does, so a skipped identity render leaves the
        // source where it was.
        if index > 0 && (plan.width, plan.height) == source {
            continue;
        }
        source = (plan.width, plan.height);
        out.push((tier, plan));
    }
    out
}

/// The settled encoded-larger-than-the-source edge (§2): whether an item
/// keeps serving its **original** at the grid tiers because no H.264 encode
/// of it came out smaller — the loop half of the sentinel convention this
/// module's docs write out.
///
/// Real for the shape the loop pipeline meets most often at the small end —
/// a few flat-colour frames at large dimensions, where GIF's palette coding
/// beats a codec that has to carry an intra frame. Serving a *larger*
/// rendition than the file it replaces would invert the entire point of the
/// ladder.
///
/// `>=` rather than `>`: a tie is not worth a second decoder in the cell.
pub(crate) fn loop_keeps_original(encoded_len: u64, source_len: u64) -> bool {
    encoded_len >= source_len
}

/// The **whole** stored set of an animated item above the raw floor.
///
/// At most five rows, and never more: the posters `grid-m`, `grid-s` and
/// `grid-xs` (each stored only where it is genuinely smaller than the one
/// above it), the `loop` that answers every grid tier, and — only where the
/// display answer is a loop the grid one cannot stand in for (R3) — a
/// `loop-display`.
///
/// One function for the dispatcher's prediction and the generator's output,
/// for the same reason [`grid_plans`] is one function: the backfill compares
/// the stored geometry against this and never terminates if the two can
/// disagree. Ordered the way `get_thumbnail_tier_geometry` returns rows —
/// posters then loops is already lexicographic — but the comparison sorts
/// anyway.
pub(crate) fn animated_plans(
    file_size: u64,
    width: u32,
    height: u32,
) -> Vec<(RenditionKind, TierPlan)> {
    let mut out: Vec<(RenditionKind, TierPlan)> = poster_plans(width, height)
        .into_iter()
        .map(|(tier, plan)| (RenditionKind::Still(tier), plan))
        .collect();
    out.push((RenditionKind::Loop, loop_plan(width, height)));
    // The second loop row exists only where the display answer is a loop the
    // grid one cannot stand in for (R3) — which is exactly the row
    // [`animated_display_loop`] names.
    if animated_display_loop(file_size, width, height) == Some(LOOP_DISPLAY_TIER) {
        out.push((RenditionKind::LoopDisplay, loop_display_plan(width, height)));
    }
    out
}

/// One row of the set an item's stored renditions are compared against: the
/// discriminator, the geometry, and the media type the row must carry.
///
/// The media type joins the geometry because it is what makes a format change
/// — a policy edit, a transparency measurement, the display switch — visible
/// to a dispatcher that never decodes anything. Without it a WebP verdict over
/// a stored JPEG of identical dimensions would look like a match forever.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WantedRendition {
    pub kind: RenditionKind,
    pub plan: TierPlan,
    pub media_type: &'static str,
}

/// The whole stored tier set of a **still** item, named and typed.
pub(crate) fn static_rendition_set(
    file_size: u64,
    width: u32,
    height: u32,
    format: RenditionFormat,
) -> Vec<WantedRendition> {
    named(grid_plans(file_size, width, height), format)
}

/// [`static_rendition_set`] for a source that is itself a stored thumbnail —
/// a video's frame grid, an audio cover, a rendered page. Always
/// [`GENERATED_STILL_FORMAT`].
pub(crate) fn stored_thumbnail_rendition_set(width: u32, height: u32) -> Vec<WantedRendition> {
    named(
        grid_plans_for_stored_thumbnail(width, height),
        GENERATED_STILL_FORMAT,
    )
}

/// The whole stored set of an animated item above the raw floor: its posters
/// in `format`, then its loop rows as `video/mp4`.
pub(crate) fn animated_rendition_set(
    file_size: u64,
    width: u32,
    height: u32,
    format: RenditionFormat,
) -> Vec<WantedRendition> {
    animated_plans(file_size, width, height)
        .into_iter()
        .map(|(kind, plan)| WantedRendition {
            kind,
            plan,
            media_type: kind.media_type(format),
        })
        .collect()
}

fn named(plans: Vec<(ThumbnailTier, TierPlan)>, format: RenditionFormat) -> Vec<WantedRendition> {
    plans
        .into_iter()
        .map(|(tier, plan)| WantedRendition {
            kind: RenditionKind::Still(tier),
            plan,
            media_type: format.media_type(),
        })
        .collect()
}

/// The transparency verdict for a picture nothing can decode.
///
/// Named rather than spelled `false` where the dispatcher's marker gate
/// writes it, because it is a *verdict* and not a measurement: a picture
/// nobody can decode has no rendition for anything to be transparent in, and
/// the column has to hold something or the pending index never drains and the
/// item is dispatched again on every scan for the rest of its life.
pub(crate) const UNDECODABLE_HAS_TRANSPARENCY: bool = false;

/// Whether a decoded picture has a single non-opaque pixel (R4).
///
/// The header is not the question: half of all PNGs carry an alpha *channel*
/// and only 2.3% of them use it, so trusting the channel would push the whole
/// library onto a codec the grid decodes 2.2–2.7x slower. Cheap because it
/// runs in the one place that already holds the decoded image
/// (`build_image_renditions`), and free for every container without an alpha
/// channel at all.
pub(crate) fn has_alpha_pixels(image: &DynamicImage) -> bool {
    if !image.color().has_alpha() {
        return false;
    }
    match image {
        DynamicImage::ImageLumaA8(buffer) => buffer.pixels().any(|pixel| pixel.0[1] < u8::MAX),
        DynamicImage::ImageRgba8(buffer) => buffer.pixels().any(|pixel| pixel.0[3] < u8::MAX),
        DynamicImage::ImageLumaA16(buffer) => buffer.pixels().any(|pixel| pixel.0[1] < u16::MAX),
        DynamicImage::ImageRgba16(buffer) => buffer.pixels().any(|pixel| pixel.0[3] < u16::MAX),
        DynamicImage::ImageRgba32F(buffer) => buffer.pixels().any(|pixel| pixel.0[3] < 1.0),
        // `DynamicImage` is `#[non_exhaustive]`; `color().has_alpha()` above
        // already excluded every variant this build knows to be opaque, so an
        // unknown one is treated as opaque rather than decoded a second time.
        _ => false,
    }
}

/// Encodes one rendition in the format and at the quality its rung wants.
///
/// `has_transparency` is the item's R4 verdict, not a keep-alpha instruction:
/// whether the channel survives is [`RenditionFormat::keeps_alpha`]'s to say,
/// and folding it in at the call site is how three copies of one condition
/// came to exist.
pub(crate) fn encode_rendition(
    image: &DynamicImage,
    format: RenditionFormat,
    rung: RenditionRung,
    has_transparency: Option<bool>,
) -> Result<Vec<u8>, String> {
    match format {
        RenditionFormat::Jpeg => encode_jpeg(
            image,
            match rung {
                RenditionRung::Grid => GRID_JPEG_QUALITY,
                RenditionRung::Display => DISPLAY_JPEG_QUALITY,
            },
        ),
        RenditionFormat::Webp => encode_webp(
            image,
            match rung {
                RenditionRung::Grid => GRID_WEBP_QUALITY,
                RenditionRung::Display => DISPLAY_WEBP_QUALITY,
            },
            format.keeps_alpha(has_transparency),
        ),
    }
}

/// Baseline JPEG, 4:4:4, optimized Huffman tables, ImageMagick quantization.
///
/// Every one of those is a deliberate departure from a default. The crate
/// subsamples chroma 4:2:0 below quality 90, which against today's encoder —
/// verified 4:4:4 in the `image` crate's source, whose "4:2:2" doc comment is
/// stale — would be a quality *regression* dressed up as a byte saving.
/// Optimized Huffman tables are off by default and cost one extra pass for
/// several percent. The ImageMagick table is mozjpeg's table 3, which is where
/// the measured 91%-of-today's-bytes at higher SSIM comes from. Progressive
/// stays off: the grid decodes these, and a progressive scan is slower to
/// decode for a picture nobody watches load.
fn encode_jpeg(image: &DynamicImage, quality: u8) -> Result<Vec<u8>, String> {
    encode_jpeg_with(
        image,
        quality,
        true,
        jpeg_encoder::QuantizationTableType::ImageMagick,
    )
}

/// [`encode_jpeg`] with its two invisible settings named.
///
/// Everything else — 4:4:4, baseline, the 16-bit frame-header guard — is the
/// same code, and has to be: the test that pins those two settings reads them
/// as the *difference* between two encodes of the same picture, which is only
/// evidence if the rest of the encoder is identical.
fn encode_jpeg_with(
    image: &DynamicImage,
    quality: u8,
    optimized_huffman: bool,
    tables: jpeg_encoder::QuantizationTableType,
) -> Result<Vec<u8>, String> {
    use jpeg_encoder::{ColorType, Encoder, SamplingFactor};

    // Borrowed where the picture is already 8-bit RGB, which every rendition
    // of a decoded photograph is: `to_rgb8` copies the whole buffer even then.
    let rgb = match image.as_rgb8() {
        Some(rgb) => Cow::Borrowed(rgb),
        None => Cow::Owned(image.to_rgb8()),
    };
    let (width, height) = (rgb.width(), rgb.height());
    // The container's own limit, not ours: JPEG's frame header carries the
    // dimensions in 16 bits. Nothing in the ladder reaches it — [`display_shape`]
    // refuses to plan a rendition past [`JPEG_MAX_SIDE`], and a grid tier is at
    // most `2 * tier` on its long side — so this is the encoder's guard
    // against a caller that has not asked the rule.
    let (Ok(width_16), Ok(height_16)) = (u16::try_from(width), u16::try_from(height)) else {
        return Err(format!("{width}x{height} does not fit a JPEG frame header"));
    };
    let mut buffer = Vec::new();
    let mut encoder = Encoder::new(&mut buffer, quality);
    encoder.set_sampling_factor(SamplingFactor::F_1_1);
    encoder.set_progressive(false);
    encoder.set_optimized_huffman_tables(optimized_huffman);
    encoder.set_quantization_tables(tables.clone(), tables);
    encoder
        .encode(&rgb, width_16, height_16, ColorType::Rgb)
        .map_err(|err| err.to_string())?;
    Ok(buffer)
}

/// Lossy WebP through libwebp.
///
/// `image-webp` — already in the tree, and what decodes these blobs back —
/// encodes lossless only, which for a photographic rendition is the wrong
/// half of the codec entirely.
fn encode_webp(image: &DynamicImage, quality: f32, keep_alpha: bool) -> Result<Vec<u8>, String> {
    let memory = if keep_alpha {
        let rgba = match image.as_rgba8() {
            Some(rgba) => Cow::Borrowed(rgba),
            None => Cow::Owned(image.to_rgba8()),
        };
        let (width, height) = (rgba.width(), rgba.height());
        webp::Encoder::from_rgba(&rgba, width, height).encode_simple(false, quality)
    } else {
        let rgb = match image.as_rgb8() {
            Some(rgb) => Cow::Borrowed(rgb),
            None => Cow::Owned(image.to_rgb8()),
        };
        let (width, height) = (rgb.width(), rgb.height());
        webp::Encoder::from_rgb(&rgb, width, height).encode_simple(false, quality)
    };
    memory
        .map(|memory| memory.to_vec())
        .map_err(|err| format!("webp encode failed: {err:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;
    const JPEG: &str = "image/jpeg";
    const PNG: &str = "image/png";
    const WEBP: &str = "image/webp";
    const GIF: &str = "image/gif";

    fn plan_of(mime: &str, bytes: u64, width: u32, height: u32) -> DisplayPlan {
        display_plan(
            mime,
            false,
            None,
            bytes,
            width,
            height,
            FormatPolicy::default(),
        )
    }

    // R2's trigger: dimensions shared across classes, bytes per class. The
    // dead 100 MP hole the old rule had (anything under 5 MB served raw) is
    // still closed, and the pixel bound is now the chosen 24 MP rather than
    // 4096 squared and doubled.
    #[test]
    fn the_display_trigger_is_dimensions_then_per_class_bytes() {
        // A 12 MP photo, comfortably inside every bound.
        assert_eq!(plan_of(JPEG, 3 * MB, 4000, 3000), DisplayPlan::Original);

        // 100 MP under 3 MB: the hole. 12000x8333 = 99.996 MP.
        let plan = plan_of(JPEG, 3 * MB, 12000, 8333);
        let DisplayPlan::Thumbnail { plan, .. } = plan else {
            panic!("a 100 MP original must not be served directly: {plan:?}");
        };
        let (width, height) = (plan.width, plan.height);
        assert!(u64::from(width) * u64::from(height) <= DISPLAY_MAX_PIXELS);
        assert!(width.min(height) <= DISPLAY_RENDITION_SHORT_SIDE);

        // Exactly 24,000,000 pixels serves raw; one more does not. Decimal
        // megapixels with `>`, so a 6000x4000 camera JPEG is on the bound.
        assert_eq!(plan_of(JPEG, MB, 6000, 4000), DisplayPlan::Original);
        assert!(matches!(
            plan_of(JPEG, MB, 6001, 4000),
            DisplayPlan::Thumbnail { .. }
        ));

        // Short side over 4096, total pixels well under the bound.
        assert!(matches!(
            plan_of(JPEG, MB, 4097, 4097),
            DisplayPlan::Thumbnail { .. }
        ));
        assert_eq!(plan_of(JPEG, MB, 4096, 4096), DisplayPlan::Original);

        // A tall webtoon: 16 MP, short side 800, modest bytes. The long side
        // is not a bound at all.
        assert_eq!(plan_of(JPEG, MB, 800, 20000), DisplayPlan::Original);
    }

    // Bytes mean different things per format, which is the whole reason the
    // bound is per class: a 3 MiB PNG is a modest picture worth re-encoding, a
    // 3 MiB JPEG is a large efficient one, and a WebP's bytes never say
    // anything worth acting on.
    #[test]
    fn the_display_byte_bound_is_per_source_class() {
        assert_eq!(
            display_byte_bound(SourceClass::Lossless),
            Some(2 * MB),
            "measured: a WebP rendition of a PNG saves 80-90% from 1-2 MiB up"
        );
        assert_eq!(display_byte_bound(SourceClass::Jpeg), Some(4 * MB));
        assert_eq!(display_byte_bound(SourceClass::Webp), None);
        assert_eq!(display_byte_bound(SourceClass::Animated), Some(5 * MB));

        // The same picture, three containers, three answers.
        assert!(matches!(
            plan_of(PNG, 3 * MB, 2000, 1500),
            DisplayPlan::Thumbnail { .. }
        ));
        assert_eq!(plan_of(JPEG, 3 * MB, 2000, 1500), DisplayPlan::Original);
        assert_eq!(plan_of(WEBP, 30 * MB, 2000, 1500), DisplayPlan::Original);

        // Exactly on a bound is still the original; one byte over is not.
        assert_eq!(plan_of(PNG, 2 * MB, 2000, 1500), DisplayPlan::Original);
        assert!(matches!(
            plan_of(PNG, 2 * MB + 1, 2000, 1500),
            DisplayPlan::Thumbnail { .. }
        ));
        assert_eq!(plan_of(JPEG, 4 * MB, 2000, 1500), DisplayPlan::Original);
        assert!(matches!(
            plan_of(JPEG, 4 * MB + 1, 2000, 1500),
            DisplayPlan::Thumbnail { .. }
        ));

        // An unknown still container is lossless by default: nothing about a
        // format this build cannot name says its bytes are efficient.
        assert_eq!(source_class("image/tiff", false), SourceClass::Lossless);
        assert_eq!(source_class("image/bmp", false), SourceClass::Lossless);
        assert_eq!(source_class("image/heic", false), SourceClass::Lossless);
        assert_eq!(source_class(GIF, true), SourceClass::Animated);
        assert_eq!(
            source_class(WEBP, true),
            SourceClass::Animated,
            "a moving picture is the animated class whatever its container"
        );
    }

    // The rendition's shape once the trigger has fired: short side 2560, then
    // 24 MP, whole image, no crop.
    #[test]
    fn display_scaling_takes_the_binding_bound() {
        // Short side binds: 8192 -> 2560, aspect kept.
        assert_eq!(display_dimensions(8192, 12288), (2560, 3840));
        // Megapixels bind after it: 6000x6000 -> 2560 short is already 6.5 MP.
        let (width, height) = display_dimensions(6000, 6000);
        assert_eq!((width, height), (2560, 2560));
        // A strip: the short side is inside the cap, so the pixel bound is
        // what scales it. The scale is exact and each side then rounds to the
        // nearest pixel, so the product can land a rounding step *over* the
        // bound - which is why the bound is written as a trigger and the
        // rendition's shape as a scale rather than a hard pixel ceiling.
        let (width, height) = display_dimensions(800, 60000);
        let pixels = u64::from(width) * u64::from(height);
        assert!(
            pixels <= DISPLAY_MAX_PIXELS + u64::from(width) + u64::from(height),
            "{width}x{height} = {pixels}"
        );
        assert!(width < 800, "the pixel bound has to bind here: {width}");
        // Neither binds: the clamp at 1 keeps the original geometry, which is
        // the whole point of the byte bound.
        assert_eq!(display_dimensions(1000, 800), (1000, 800));
        assert_eq!(
            plan_of(PNG, 30 * MB, 2000, 1500),
            DisplayPlan::Thumbnail {
                plan: whole_image_plan(2000, 1500, 2000, 1500),
                format: RenditionFormat::Webp,
            },
            "only the byte bound broke, so every pixel is kept"
        );
    }

    // R2's format rule: the rendition follows the source class, because a WebP
    // of a JPEG decodes 2.33x slower than the JPEG it replaces while a JPEG
    // downscale is smaller AND faster.
    #[test]
    fn the_display_format_follows_the_source_class() {
        let format = |mime: &str, bytes: u64| match plan_of(mime, bytes, 3000, 3000) {
            DisplayPlan::Thumbnail { format, .. } => format,
            other => panic!("expected a rendition: {other:?}"),
        };
        assert_eq!(format(PNG, 30 * MB), RenditionFormat::Webp);
        assert_eq!(format("image/tiff", 30 * MB), RenditionFormat::Webp);
        assert_eq!(format(JPEG, 30 * MB), RenditionFormat::Jpeg);
        // A WebP source only ever reaches a rendition on dimensions, so the
        // shape has to be one that trips a dimension bound.
        let plan = plan_of(WEBP, MB, 5000, 5000);
        assert!(
            matches!(
                plan,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Webp,
                    ..
                }
            ),
            "{plan:?}"
        );
    }

    // R4: transparency is decided by pixels and overrides the class, at every
    // rung. Only 2.3% of PNGs have a non-opaque pixel while 50% carry the
    // channel, which is why the header is never the question.
    #[test]
    fn transparency_moves_every_rendition_to_webp() {
        let transparent = display_plan(
            JPEG,
            false,
            Some(true),
            30 * MB,
            3000,
            3000,
            FormatPolicy::default(),
        );
        assert!(
            matches!(
                transparent,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Webp,
                    ..
                }
            ),
            "{transparent:?}"
        );
        assert_eq!(
            tier_format(Some(true), FormatPolicy::default()),
            RenditionFormat::Webp
        );
        // Unexamined and examined-opaque both take the ordinary verdict: the
        // grid is decode-bound, and WebP decodes 2.2-2.7x slower.
        assert_eq!(
            tier_format(None, FormatPolicy::default()),
            RenditionFormat::Jpeg
        );
        assert_eq!(
            tier_format(Some(false), FormatPolicy::default()),
            RenditionFormat::Jpeg
        );
        let policy = FormatPolicy::default();
        // The size limit still wins: a transparent picture too tall for
        // libwebp is flattened rather than left unencodable. Asked of the
        // display rule, which is the only rendition that can reach it.
        let over = display_plan(
            PNG,
            false,
            Some(true),
            4 * MB,
            600,
            WEBP_MAX_SIDE + 1,
            policy,
        );
        assert!(
            matches!(
                over,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Jpeg,
                    ..
                }
            ),
            "{over:?}"
        );
        let under = display_plan(PNG, false, Some(true), 4 * MB, 600, WEBP_MAX_SIDE, policy);
        assert!(
            matches!(
                under,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Webp,
                    ..
                }
            ),
            "{under:?}"
        );
    }

    // The shape that falls off the end of *both* containers. A 200x100000
    // strip is over the lossless class's byte bound, so a rendition is owed;
    // the 2560 cap is on the short side, so it keeps all 100000 rows; WebP
    // refuses them at 16383 and JPEG's 16-bit frame header at 65535. With no
    // container left the original is the answer — and it has to be answered
    // by the *rule*, or the dispatcher plans a rendition the generator cannot
    // make and re-dispatches the item on every scan forever.
    #[test]
    fn a_strip_neither_container_can_name_keeps_its_original() {
        let policy = FormatPolicy::default();
        assert_eq!(
            display_plan(PNG, false, None, 4 * MB, 200, 100_000, policy),
            DisplayPlan::Original
        );
        // The boundary: JPEG's own limit, exactly on it, is still a rendition.
        assert_eq!(
            display_plan(PNG, false, None, 4 * MB, 200, JPEG_MAX_SIDE, policy),
            DisplayPlan::Thumbnail {
                plan: whole_image_plan(200, JPEG_MAX_SIDE, 200, JPEG_MAX_SIDE),
                format: RenditionFormat::Jpeg,
            }
        );
        assert_eq!(
            display_plan(PNG, false, None, 4 * MB, 200, JPEG_MAX_SIDE + 1, policy),
            DisplayPlan::Original
        );
    }

    // R5, in both directions, and the empty list that must never be a
    // rejection.
    #[test]
    fn the_policy_constrains_every_verdict_in_both_directions() {
        let jpeg_only = FormatPolicy::from_names(&["jpeg".to_string()]);
        assert_eq!(
            tier_format(Some(true), jpeg_only),
            RenditionFormat::Jpeg,
            "with webp absent a transparent picture is flattened, as today"
        );
        let plan = display_plan(PNG, false, None, 30 * MB, 3000, 3000, jpeg_only);
        assert!(
            matches!(
                plan,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Jpeg,
                    ..
                }
            ),
            "{plan:?}"
        );

        let webp_only = FormatPolicy::from_names(&["webp".to_string()]);
        assert_eq!(tier_format(None, webp_only), RenditionFormat::Webp);
        let plan = display_plan(JPEG, false, None, 30 * MB, 3000, 3000, webp_only);
        assert!(
            matches!(
                plan,
                DisplayPlan::Thumbnail {
                    format: RenditionFormat::Webp,
                    ..
                }
            ),
            "{plan:?}"
        );

        // A list naming neither is the default with a warning, never a
        // rejection: the settings UI round-trips the whole config, so a
        // commit-path reject would break every unrelated save.
        assert_eq!(FormatPolicy::from_names(&[]), FormatPolicy::default());
        assert_eq!(
            FormatPolicy::from_names(&["avif".to_string()]),
            FormatPolicy::default()
        );
        assert_eq!(
            FormatPolicy::from_names(&["jpeg".to_string(), "webp".to_string()]),
            FormatPolicy::default()
        );
    }

    // The keep-the-original sentinel's arithmetic: a rendition has to be
    // comfortably smaller, not merely smaller, or the second copy of the
    // picture buys nothing.
    #[test]
    fn a_rendition_has_to_be_three_quarters_of_its_source_or_less() {
        assert!(!still_keeps_original(750, 1000));
        assert!(!still_keeps_original(749, 1000));
        assert!(still_keeps_original(751, 1000));
        assert!(still_keeps_original(1000, 1000));
        assert!(still_keeps_original(4000, 1000));
        // The ordinary case by a wide margin: a WebP of a big PNG.
        assert!(!still_keeps_original(400_000, 4 * MB));
    }

    #[test]
    fn a_normal_aspect_tier_is_a_plain_short_side_resize() {
        // 3000x4000, tier 1024: short side to 1024, no crop.
        let plan = tier_plan(3000, 4000, 1024);
        assert_eq!(
            plan,
            TierPlan {
                crop_x: 0,
                crop_y: 0,
                crop_width: 3000,
                crop_height: 4000,
                width: 1024,
                height: 1365,
            }
        );

        // Landscape is the same rule on the other axis.
        let plan = tier_plan(4000, 3000, 512);
        assert_eq!((plan.width, plan.height), (683, 512));
        assert_eq!((plan.crop_width, plan.crop_height), (4000, 3000));

        // And the new smallest rung is the same rule again.
        let plan = tier_plan(4000, 3000, 256);
        assert_eq!((plan.width, plan.height), (341, 256));
    }

    // Aspect exactly 2 is the boundary and stays whole-image: the crop cap
    // is `2 * tier`, which is exactly what a 2:1 image scales to.
    #[test]
    fn aspect_exactly_two_is_not_cropped() {
        for (width, height) in [(1000_u32, 2000_u32), (2000, 1000)] {
            let plan = tier_plan(width, height, 512);
            assert_eq!(
                (plan.crop_width, plan.crop_height),
                (width, height),
                "{width}x{height} is exactly 2:1 and must keep every pixel"
            );
            assert_eq!(plan.width.min(plan.height), 512);
            assert_eq!(plan.width.max(plan.height), 1024);
        }
        // Just past the boundary, the crop engages.
        let plan = tier_plan(1000, 2001, 512);
        assert!(plan.crop_height < 2001, "{plan:?}");
    }

    // `object-position: 50% 0%`: tall keeps the top, wide keeps the center.
    #[test]
    fn extreme_aspect_crops_match_the_css_presentation() {
        // A webtoon: 800x20000. Tier 1024 -> short side stays 800 (no
        // upscale), long side capped at 2048.
        let plan = tier_plan(800, 20000, 1024);
        assert_eq!(plan.crop_x, 0);
        assert_eq!(plan.crop_y, 0, "tall images keep the TOP strip");
        assert_eq!(plan.crop_width, 800);
        assert_eq!((plan.width, plan.height), (800, 2048));
        assert_eq!(plan.crop_height, 2048, "no upscale, so crop == output");

        // A tall strip wide enough to be scaled down as well.
        let plan = tier_plan(2000, 30000, 512);
        assert_eq!(plan.crop_y, 0);
        assert_eq!((plan.width, plan.height), (512, 1024));
        // 1024 output rows at 512/2000 scale = 4000 source rows.
        assert_eq!(plan.crop_height, 4000);

        // A wide strip: 20000x800, tier 1024. The band is centered.
        let plan = tier_plan(20000, 800, 1024);
        assert_eq!(plan.crop_y, 0);
        assert_eq!(plan.crop_height, 800);
        assert_eq!((plan.width, plan.height), (2048, 800));
        assert_eq!(plan.crop_width, 2048);
        assert_eq!(
            plan.crop_x,
            (20000 - 2048) / 2,
            "wide images keep the horizontally CENTERED band"
        );
    }

    // Never upscale, at any tier, in either orientation.
    #[test]
    fn tiny_originals_are_never_upscaled() {
        for (width, height) in [(64_u32, 48_u32), (48, 64), (7, 7), (1, 1)] {
            for tier in [1024_u32, 512, 256] {
                let plan = tier_plan(width, height, tier);
                assert_eq!(
                    (plan.width, plan.height),
                    (width, height),
                    "{width}x{height} at tier {tier} must be untouched"
                );
            }
        }
        // A tiny *strip* still crops (its long side is over 2x the tier only
        // if it is over 2x the tier; below that it is left whole).
        let plan = tier_plan(100, 300, 512);
        assert_eq!((plan.width, plan.height), (100, 300));
        let plan = tier_plan(100, 3000, 512);
        assert_eq!((plan.width, plan.height), (100, 1024));
    }

    // A tier is never more than 2*tier^2 pixels, which is what removes the
    // need for a separate megapixel guard on the grid path.
    #[test]
    fn every_tier_stays_within_its_pixel_budget() {
        let shapes = [
            (12000_u32, 8333_u32),
            (800, 20000),
            (20000, 800),
            (4000, 3000),
            (1, 40000),
            (40000, 1),
            (5000, 5000),
        ];
        for (width, height) in shapes {
            for tier in ThumbnailTier::GRID {
                let side = tier.short_side().expect("a grid tier has a short side");
                let plan = tier_plan(width, height, side);
                let pixels = u64::from(plan.width) * u64::from(plan.height);
                assert!(
                    pixels <= 2 * u64::from(side) * u64::from(side),
                    "{width}x{height} at tier {side} produced {plan:?}"
                );
                assert!(plan.crop_x + plan.crop_width <= width, "{plan:?}");
                assert!(plan.crop_y + plan.crop_height <= height, "{plan:?}");
                assert!(plan.width >= 1 && plan.height >= 1, "{plan:?}");
            }
        }
    }

    #[test]
    fn the_grid_serve_rule_is_short_side_then_long_side_then_bytes() {
        // Inside all three: served directly.
        assert!(grid_serves_original(2 * MB, 1200, 1200, 1024));
        // Exactly 1.25x the tier on the short side is still direct.
        assert!(grid_serves_original(2 * MB, 1280, 1280, 1024));
        // One pixel over is not.
        assert!(!grid_serves_original(2 * MB, 1281, 1281, 1024));
        // The long side has its own bound: 1.25 * (2 * tier) = 2560 at tier
        // 1024. Exactly on it is direct, one pixel over is not.
        assert!(grid_serves_original(MB, 300, 2560, 1024));
        assert!(!grid_serves_original(MB, 300, 2561, 1024));
        // Aspect alone decides nothing any more: a 1:3 image well inside both
        // bounds is served directly, because the tier a crop would store is
        // the whole picture at 2% off (the adjudicated refinement).
        assert!(grid_serves_original(MB, 300, 900, 1024));
        // A real strip still has a stored crop: its long side is far past the
        // bound however narrow it is.
        assert!(!grid_serves_original(MB, 800, 20000, 1024));
        // Exactly 2:1 stays direct, as it always was.
        assert!(grid_serves_original(MB, 300, 600, 1024));
        // The waste case the refinement exists for: 1000x2100 at tier 1024
        // used to store a 1000x2048 "crop" of a 1000x2100 original.
        assert!(grid_serves_original(2 * MB, 1000, 2100, 1024));
        // ... and the same picture is far outside the *smaller* tier, which
        // still stores a crop for it.
        assert!(!grid_serves_original(2 * MB, 1000, 2100, 512));
        // grid-xs serves directly at 320 short / 640 long, the same ratios.
        assert!(grid_serves_original(MB, 320, 640, 256));
        assert!(!grid_serves_original(MB, 321, 640, 256));
        assert!(!grid_serves_original(MB, 320, 641, 256));
        // Bytes over the bound do not.
        assert!(!grid_serves_original(
            GRID_DIRECT_MAX_FILE_SIZE + 1,
            600,
            600,
            1024
        ));
        assert!(grid_serves_original(
            GRID_DIRECT_MAX_FILE_SIZE,
            600,
            600,
            1024
        ));
        // The derived-thumbnail form drops only the byte clause.
        assert!(grid_serves_stored_thumbnail(600, 600, 1024));
        assert!(!grid_serves_stored_thumbnail(800, 20000, 1024));
    }

    // The point of the refinement, stated as the plan does: a 1000x2100 image
    // stores no grid-m at all (the original serves it), while an 800x20000
    // webtoon still stores its top-strip crops at every tier.
    #[test]
    fn the_long_side_bound_skips_near_identical_tiers_but_keeps_strip_crops() {
        let plans = grid_plans(2 * MB, 1000, 2100);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridS, ThumbnailTier::GridXs],
            "grid-m must not store a 2%-smaller copy of the whole picture"
        );

        let plans = grid_plans(6 * MB, 800, 20000);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![
                ThumbnailTier::GridM,
                ThumbnailTier::GridS,
                ThumbnailTier::GridXs
            ]
        );
        // All three are top-strip crops, exactly as before grid-xs existed.
        assert_eq!((plans[0].1.crop_y, plans[0].1.crop_height), (0, 2048));
        assert_eq!((plans[0].1.width, plans[0].1.height), (800, 2048));
        // grid-s cascades off the 800x2048 grid-m: 1600 of its rows scale to
        // the 1024 the tier allows.
        assert_eq!((plans[1].1.crop_y, plans[1].1.crop_height), (0, 1600));
        assert_eq!((plans[1].1.width, plans[1].1.height), (512, 1024));
        // ... and grid-xs cascades off grid-s the same way.
        assert_eq!(plans[2].1.crop_y, 0);
        assert_eq!((plans[2].1.width, plans[2].1.height), (256, 512));
    }

    // The cascade selects the same source region as a direct plan would, and
    // the same short side. The *long* side can differ by a pixel where two
    // roundings compose, which is harmless precisely because the dispatcher
    // predicts with this same function rather than with a direct plan.
    #[test]
    fn the_grid_xs_cascade_agrees_with_a_direct_plan() {
        let shapes = [
            (3000_u32, 4000_u32),
            (4000, 3000),
            (800, 20000),
            (20000, 800),
            (700, 5000),
            (300, 5000),
            (2000, 2000),
            (1281, 1281),
            (12000, 8333),
        ];
        for (width, height) in shapes {
            let cascaded = grid_plans(50 * MB, width, height);
            let direct = tier_plan(width, height, 256);
            let last = cascaded
                .last()
                .unwrap_or_else(|| panic!("{width}x{height} planned nothing"));
            assert_eq!(last.0, ThumbnailTier::GridXs);
            let plan = last.1;
            assert_eq!(
                plan.width.min(plan.height),
                direct.width.min(direct.height),
                "{width}x{height}: cascade {plan:?} vs direct {direct:?}"
            );
            let cascaded_long = i64::from(plan.width.max(plan.height));
            let direct_long = i64::from(direct.width.max(direct.height));
            assert!(
                (cascaded_long - direct_long).abs() <= 1,
                "{width}x{height}: cascade {plan:?} vs direct {direct:?}"
            );
        }
    }

    // Cascading pixels must agree with cascading dimensions, or the
    // dispatcher's prediction and the generator's output part ways and the
    // backfill never terminates.
    #[test]
    fn rendered_tiers_have_exactly_the_planned_dimensions() {
        let shapes = [(1500_u32, 2000_u32), (300, 2000), (2000, 300), (60, 60)];
        for (width, height) in shapes {
            let image = DynamicImage::ImageRgb8(image::RgbImage::new(width, height));
            let plans = grid_plans(50 * MB, width, height);
            for (tier, rendered) in grid_renditions(&image, &plans) {
                let plan = plans
                    .iter()
                    .find(|(candidate, _)| *candidate == tier)
                    .map(|(_, plan)| *plan)
                    .unwrap();
                assert_eq!(
                    (rendered.width(), rendered.height()),
                    (plan.width, plan.height),
                    "{width}x{height} at {}",
                    tier.as_str()
                );
            }
        }
    }

    // A source small enough that a larger tier serves it directly still gets
    // the smaller ones, planned against the source rather than against a
    // rendition that was never made.
    #[test]
    fn a_skipped_tier_does_not_break_the_cascade() {
        let plans = grid_plans(MB, 1200, 1200);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridS, ThumbnailTier::GridXs]
        );
        assert_eq!((plans[0].1.width, plans[0].1.height), (512, 512));
        assert_eq!((plans[1].1.width, plans[1].1.height), (256, 256));

        // Small enough for the two larger rungs: only grid-xs is stored.
        let plans = grid_plans(MB, 600, 600);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridXs]
        );

        // Small enough for every rung: nothing is stored at all.
        assert!(grid_plans(MB, 300, 300).is_empty());
        // A narrow picture that is small on *both* axes is equally free —
        // aspect on its own no longer forces a stored crop.
        assert!(grid_plans(MB, 200, 400).is_empty());
        // ... but a genuine strip stores all three, because its long side is
        // far past what any tier would ever hold.
        assert_eq!(grid_plans(MB, 200, 6000).len(), 3);
    }

    // The geometry above says where the crop is; this says the pixels agree.
    // A tall strip keeps its top band and a wide one its middle band, which
    // is what `object-position: 50% 0%` paints and what every cover-top
    // surface in the app expects.
    #[test]
    fn crops_keep_the_band_the_css_would_show() {
        const MARK: image::Rgb<u8> = image::Rgb([255, 0, 0]);
        const REST: image::Rgb<u8> = image::Rgb([0, 0, 255]);

        // Tall: 100x1000, top 200 rows marked. Tier 100 caps the long side at
        // 200, so the rendition is exactly that band.
        let mut tall = image::RgbImage::from_pixel(100, 1000, REST);
        for y in 0..200 {
            for x in 0..100 {
                tall.put_pixel(x, y, MARK);
            }
        }
        let plan = tier_plan(100, 1000, 100);
        assert_eq!((plan.crop_y, plan.crop_height), (0, 200));
        let rendered = render(&DynamicImage::ImageRgb8(tall), &plan).to_rgb8();
        assert_eq!(rendered.dimensions(), (100, 200));
        assert!(
            rendered.pixels().all(|pixel| *pixel == MARK),
            "a tall crop must be the TOP band"
        );

        // Wide: 1000x100, the centered 200 columns marked.
        let mut wide = image::RgbImage::from_pixel(1000, 100, REST);
        for y in 0..100 {
            for x in 400..600 {
                wide.put_pixel(x, y, MARK);
            }
        }
        let plan = tier_plan(1000, 100, 100);
        assert_eq!((plan.crop_x, plan.crop_width), (400, 200));
        let rendered = render(&DynamicImage::ImageRgb8(wide), &plan).to_rgb8();
        assert_eq!(rendered.dimensions(), (200, 100));
        assert!(
            rendered.pixels().all(|pixel| *pixel == MARK),
            "a wide crop must be the horizontally CENTERED band"
        );
    }

    // The raw floor's truth table (§2, B2): BOTH clauses, and a picture with
    // no measured dimensions is never under it.
    #[test]
    fn the_animated_raw_floor_needs_both_bytes_and_dimensions() {
        // Under both: served raw.
        assert!(animated_serves_original(400 * 1024, 480, 320));
        // Exactly on both bounds is still under the floor.
        assert!(animated_serves_original(
            ANIMATED_RAW_MAX_FILE_SIZE,
            512,
            512
        ));
        // One byte over.
        assert!(!animated_serves_original(
            ANIMATED_RAW_MAX_FILE_SIZE + 1,
            512,
            512
        ));
        // One pixel over, on either axis — a 900 KB 600x600 GIF is over the
        // floor on dimensions alone, which is the case the two-clause rule
        // exists for.
        assert!(!animated_serves_original(900 * 1024, 513, 512));
        assert!(!animated_serves_original(900 * 1024, 512, 513));
        assert!(!animated_serves_original(900 * 1024, 600, 600));
        // Unmeasured dimensions are never "final by rule".
        assert!(!animated_serves_original(1024, 0, 512));
        assert!(!animated_serves_original(1024, 512, 0));
    }

    // The two container families default opposite ways, because their
    // *unknown* cases are opposite: a GIF is animated unless measured still
    // (the measurement runs after the ladder question, so `duration IS NULL`
    // must not read as "still"), every other container only when measured.
    #[test]
    fn a_gif_is_animated_unless_measured_still() {
        assert!(is_animated_image("image/gif", None));
        assert!(
            !is_animated_image("image/gif", Some(0.0)),
            "a single-frame GIF must not carry an eternal one-frame mp4"
        );
        assert!(is_animated_image("image/gif", Some(1.2)));
        assert!(is_animated_image("image/webp", Some(3.5)));
        assert!(!is_animated_image("image/webp", Some(0.0)));
        assert!(!is_animated_image("image/webp", None));
        assert!(!is_animated_image("image/jpeg", None));
        // Not an image at all: a video's grid tiers come from its stored
        // frame grid, which is a still by construction.
        assert!(!is_animated_image("video/mp4", Some(120.0)));
        assert!(!is_animated_image("audio/mpeg", Some(200.0)));
        assert!(!is_animated_image("", None));
    }

    // The loop is the same crop rule as every other grid rendition, with even
    // sides because yuv420p subsamples chroma 2x2.
    #[test]
    fn loop_geometry_is_the_tier_crop_rounded_down_to_even() {
        // A plain photo-shaped animation: short side to 1024, no crop.
        let plan = loop_plan(1500, 2000);
        assert_eq!((plan.crop_width, plan.crop_height), (1500, 2000));
        assert_eq!((plan.width, plan.height), (1024, 1364));
        // The unrounded render would have been 1365 rows; evenness rounds
        // DOWN, never up.
        assert_eq!(tier_plan(1500, 2000, 1024).height, 1365);

        // Never upscaled: a source under the cap keeps its size (rounded).
        let plan = loop_plan(300, 401);
        assert_eq!((plan.width, plan.height), (300, 400));

        // A tall strip: top band, long side capped at 2 * 1024.
        let plan = loop_plan(800, 20000);
        assert_eq!((plan.crop_x, plan.crop_y), (0, 0));
        assert_eq!(plan.crop_height, 2048, "tall loops keep the TOP strip");
        assert_eq!((plan.width, plan.height), (800, 2048));

        // A wide strip: horizontally centered band.
        let plan = loop_plan(20000, 800);
        assert_eq!(plan.crop_x, (20000 - 2048) / 2);
        assert_eq!((plan.width, plan.height), (2048, 800));

        // Every shape comes out even, and never zero.
        for (width, height) in [
            (1_u32, 1_u32),
            (1, 40000),
            (40000, 1),
            (3, 7),
            (1023, 1025),
            (12000, 8333),
        ] {
            let plan = loop_plan(width, height);
            assert_eq!(plan.width % 2, 0, "{width}x{height} -> {plan:?}");
            assert_eq!(plan.height % 2, 0, "{width}x{height} -> {plan:?}");
            assert!(plan.width >= 2 && plan.height >= 2, "{plan:?}");
        }
    }

    // R3: an animated item's display answer, and which loop row serves it.
    #[test]
    fn an_animated_display_answer_is_the_file_until_the_trigger_fires() {
        let animated = |bytes: u64, width: u32, height: u32| {
            display_plan(
                GIF,
                true,
                None,
                bytes,
                width,
                height,
                FormatPolicy::default(),
            )
        };
        // Under every bound: the file, which animates in an `<img>`.
        assert_eq!(animated(4 * MB, 900, 900), DisplayPlan::Original);
        assert_eq!(
            animated(DISPLAY_MAX_FILE_SIZE_ANIMATED, 900, 900),
            DisplayPlan::Original,
            "exactly on the bound is still the original"
        );
        // Over the byte bound, and the grid loop is already the whole picture
        // at native resolution: no second encode.
        assert_eq!(
            animated(DISPLAY_MAX_FILE_SIZE_ANIMATED + 1, 900, 900),
            DisplayPlan::Loop { tier: LOOP_TIER }
        );
        // Over the bound and larger than the grid loop's cap: a second row.
        assert_eq!(
            animated(6 * MB, 1500, 2000),
            DisplayPlan::Loop {
                tier: LOOP_DISPLAY_TIER
            },
            "the grid loop is downscaled here, so it is not the display loop"
        );
        // A strip is never a reuse: the grid loop is a top crop, which cannot
        // answer a display request however small its short side is.
        assert_eq!(
            animated(6 * MB, 800, 20000),
            DisplayPlan::Loop {
                tier: LOOP_DISPLAY_TIER
            }
        );
        // The row the endpoint looks for is the row the set writes.
        assert!(
            animated_plans(6 * MB, 1500, 2000)
                .iter()
                .any(|(kind, _)| *kind == RenditionKind::LoopDisplay)
        );
        assert!(
            !animated_plans(DISPLAY_MAX_FILE_SIZE_ANIMATED + 1, 900, 900)
                .iter()
                .any(|(kind, _)| *kind == RenditionKind::LoopDisplay)
        );
    }

    // The display loop's own geometry: whole image, 2560 short, even sides.
    #[test]
    fn the_display_loop_is_the_whole_picture_capped_at_2560() {
        let plan = loop_display_plan(6000, 4000);
        assert_eq!((plan.crop_width, plan.crop_height), (6000, 4000));
        assert_eq!((plan.width, plan.height), (3840, 2560));
        for (width, height) in [(1_u32, 1_u32), (801, 20001), (12000, 8333)] {
            let plan = loop_display_plan(width, height);
            assert_eq!(plan.width % 2, 0, "{width}x{height} -> {plan:?}");
            assert_eq!(plan.height % 2, 0, "{width}x{height} -> {plan:?}");
            assert!(plan.width >= 2 && plan.height >= 2, "{plan:?}");
            assert_eq!(
                (plan.crop_x, plan.crop_y),
                (0, 0),
                "a display loop never crops"
            );
        }
    }

    // A poster always exists, however small the animation is: the original
    // moves, so it can never be the poster.
    #[test]
    fn posters_always_store_a_grid_m_and_deduplicate_smaller_rungs() {
        // Small enough that `grid-m` and `grid-s` are the identity render:
        // only one of them is stored, and a `grid-s` request falls up to it.
        // `grid-xs` is genuinely smaller, so it is stored.
        let plans = poster_plans(300, 300);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridM, ThumbnailTier::GridXs]
        );
        assert_eq!((plans[0].1.width, plans[0].1.height), (300, 300));
        assert_eq!((plans[1].1.width, plans[1].1.height), (256, 256));

        // Small enough for every rung: exactly one poster.
        let plans = poster_plans(120, 120);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridM]
        );

        // Big enough for all three, cascading exactly like the static ladder.
        let plans = poster_plans(2000, 2000);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![
                ThumbnailTier::GridM,
                ThumbnailTier::GridS,
                ThumbnailTier::GridXs
            ]
        );
        assert_eq!((plans[0].1.width, plans[0].1.height), (1024, 1024));
        assert_eq!((plans[1].1.width, plans[1].1.height), (512, 512));
        assert_eq!((plans[2].1.width, plans[2].1.height), (256, 256));

        // A strip's poster is the same top crop the static ladder stores.
        let plans = poster_plans(800, 20000);
        assert_eq!((plans[0].1.crop_y, plans[0].1.crop_height), (0, 2048));
        assert_eq!((plans[0].1.width, plans[0].1.height), (800, 2048));

        // Unlike the static ladder, no byte or dimension escape can empty the
        // set: `grid-m` is unconditional.
        for (width, height) in [(1_u32, 1_u32), (64, 48), (300, 900), (5000, 5000)] {
            let plans = poster_plans(width, height);
            assert_eq!(plans[0].0, ThumbnailTier::GridM, "{width}x{height}");
        }
    }

    // The whole stored set of an animated item, which is what the backfill
    // dispatcher compares the stored rows against.
    #[test]
    fn the_animated_set_is_its_posters_plus_its_loops() {
        let plans = animated_plans(MB, 2000, 2000);
        assert_eq!(
            plans
                .iter()
                .map(|(kind, _)| kind.as_str())
                .collect::<Vec<_>>(),
            vec!["grid-m", "grid-s", "grid-xs", LOOP_TIER],
            "under the display trigger there is exactly one loop"
        );
        let (_, loop_geometry) = plans.last().unwrap();
        assert_eq!((loop_geometry.width, loop_geometry.height), (1024, 1024));

        // The smallest possible set: one poster and one loop.
        let plans = animated_plans(MB, 120, 120);
        assert_eq!(
            plans
                .iter()
                .map(|(kind, _)| kind.as_str())
                .collect::<Vec<_>>(),
            vec!["grid-m", LOOP_TIER]
        );

        // Over the display trigger, and too big for the grid loop to stand
        // in: the second loop row joins the set.
        let plans = animated_plans(6 * MB, 2000, 2000);
        assert_eq!(
            plans
                .iter()
                .map(|(kind, _)| kind.as_str())
                .collect::<Vec<_>>(),
            vec!["grid-m", "grid-s", "grid-xs", LOOP_TIER, LOOP_DISPLAY_TIER]
        );

        // Over the trigger but small enough that the grid loop *is* the
        // display loop: still one row.
        let plans = animated_plans(6 * MB, 900, 900);
        assert!(
            !plans
                .iter()
                .any(|(kind, _)| *kind == RenditionKind::LoopDisplay),
            "no second encode where the grid loop is already native"
        );
    }

    // The typed set the dispatcher actually compares: every row named, and
    // every row's media type the one the generator will write.
    #[test]
    fn a_rendition_set_names_the_media_type_of_every_row() {
        let set = static_rendition_set(50 * MB, 3000, 3000, RenditionFormat::Webp);
        assert!(set.iter().all(|row| row.media_type == "image/webp"));
        let set = static_rendition_set(50 * MB, 3000, 3000, RenditionFormat::Jpeg);
        assert!(set.iter().all(|row| row.media_type == "image/jpeg"));

        let set = animated_rendition_set(6 * MB, 2000, 2000, RenditionFormat::Jpeg);
        for row in &set {
            let expected = if row.kind.is_loop() {
                LOOP_MEDIA_TYPE
            } else {
                "image/jpeg"
            };
            assert_eq!(row.media_type, expected, "{}", row.kind.as_str());
        }
        // The five discriminators, and which of them an ffmpeg run produces.
        for kind in [
            RenditionKind::Still(ThumbnailTier::GridM),
            RenditionKind::Still(ThumbnailTier::GridS),
            RenditionKind::Still(ThumbnailTier::GridXs),
            RenditionKind::Loop,
            RenditionKind::LoopDisplay,
        ] {
            assert_eq!(kind.is_loop(), kind.as_str().starts_with("loop"));
            assert_eq!(
                kind.media_type(RenditionFormat::Webp),
                if kind.is_loop() {
                    LOOP_MEDIA_TYPE
                } else {
                    "image/webp"
                }
            );
        }

        // A picture derived from a stored thumbnail is JPEG whatever the
        // policy says: those generators are out of the policy's scope.
        let set = stored_thumbnail_rendition_set(3840, 2160);
        assert!(set.iter().all(|row| row.media_type == "image/jpeg"));
    }

    // Serving a rendition *larger* than the file it replaces would invert the
    // whole point of the ladder, so the original wins ties and everything
    // above them.
    #[test]
    fn an_encode_no_smaller_than_its_source_keeps_the_original() {
        assert!(loop_keeps_original(40_000, 12_000));
        assert!(
            loop_keeps_original(12_000, 12_000),
            "a tie keeps the source"
        );
        assert!(!loop_keeps_original(11_999, 12_000));
        // The ordinary case by a wide margin: a GIF against its H.264.
        assert!(!loop_keeps_original(120_000, 6 * MB));
    }

    #[test]
    fn tier_wire_values_are_the_frozen_contract() {
        assert_eq!(ThumbnailTier::Display.as_str(), "display");
        assert_eq!(ThumbnailTier::GridM.as_str(), "grid-m");
        assert_eq!(ThumbnailTier::GridS.as_str(), "grid-s");
        assert_eq!(ThumbnailTier::GridXs.as_str(), "grid-xs");
        assert_eq!(ThumbnailTier::GridXs.short_side(), Some(256));
        assert_eq!(ThumbnailTier::default(), ThumbnailTier::Display);
        for tier in [
            ThumbnailTier::Display,
            ThumbnailTier::GridM,
            ThumbnailTier::GridS,
            ThumbnailTier::GridXs,
        ] {
            let parsed: ThumbnailTier =
                serde_json::from_str(&format!("\"{}\"", tier.as_str())).unwrap();
            assert_eq!(parsed, tier);
        }
    }

    /// The JPEG encoder's settings, read back out of the bytes it wrote.
    ///
    /// Every one is a departure from a default, and every one is the point:
    /// the crate subsamples 4:2:0 below quality 90 (a quality *regression*
    /// against today's verified-4:4:4 encoder), writes a baseline frame the
    /// grid decodes fastest, leaves optimized Huffman tables off, and
    /// quantizes with its own tables rather than mozjpeg's table 3 — which
    /// between them are the measured 91% of today's bytes at higher SSIM
    /// (§1). Nothing else in the tree can see any of them: they leave no trace
    /// but the encoded bytes, so this is where they are pinned.
    #[test]
    fn the_jpeg_encoder_writes_baseline_444_with_tuned_tables() {
        let image = sample_image(64, 48, false);
        let bytes = encode_rendition(
            &image,
            RenditionFormat::Jpeg,
            RenditionRung::Grid,
            Some(false),
        )
        .expect("the encoder runs on decoded pixels");

        let (marker, sampling) = jpeg_frame(&bytes).expect("a JFIF frame header");
        assert_eq!(
            marker, 0xC0,
            "baseline sequential (SOF0), never progressive"
        );
        assert_eq!(
            sampling, 0x11,
            "4:4:4 - the luma component's sampling factors are both 1"
        );

        // The two settings the frame header cannot show, each read against an
        // encode of the same picture at the same quality with that setting
        // left at the crate's default. A differential rather than a table of
        // literal bytes: what has to hold is that these are *departures*, and
        // the crate is free to renumber its own tables.
        let stock = encode_jpeg_with(
            &image,
            GRID_JPEG_QUALITY,
            false,
            jpeg_encoder::QuantizationTableType::Default,
        )
        .expect("the encoder runs on decoded pixels");

        // Optimized Huffman tables: built from this image's own symbol
        // statistics, not the Annex-K tables the crate emits by default. Worth
        // several percent of every grid rendition in the library for one extra
        // pass at encode time.
        let ours = jpeg_tables(&bytes, 0xC4);
        assert!(!ours.is_empty(), "the encoder writes its Huffman tables");
        assert_ne!(
            ours,
            jpeg_tables(&stock, 0xC4),
            "optimized Huffman tables are off by default; these are the \
             standard ones"
        );

        // ImageMagick quantization, which is mozjpeg's table 3 and where the
        // measured 91%-of-today's-bytes at higher SSIM comes from.
        let ours = jpeg_tables(&bytes, 0xDB);
        assert!(
            !ours.is_empty(),
            "the encoder writes its quantization tables"
        );
        assert_ne!(
            ours,
            jpeg_tables(&stock, 0xDB),
            "the quantization tables are the crate's default ones, not \
             QuantizationTableType::ImageMagick"
        );
        assert_eq!(
            ours,
            jpeg_tables(
                &encode_jpeg_with(
                    &image,
                    GRID_JPEG_QUALITY,
                    true,
                    jpeg_encoder::QuantizationTableType::ImageMagick,
                )
                .expect("the encoder runs on decoded pixels"),
                0xDB
            ),
            "and they are exactly the ImageMagick tables at this quality"
        );

        // And it round-trips through the decoder the app actually uses.
        let decoded = image::load_from_memory(&bytes).expect("the JPEG decodes");
        assert_eq!(decoded.dimensions(), (64, 48));
    }

    /// WebP keeps alpha when it is asked to, and drops it when it is not.
    #[test]
    fn webp_round_trips_alpha_when_it_is_kept() {
        let image = sample_image(32, 32, true);
        let bytes = encode_rendition(
            &image,
            RenditionFormat::Webp,
            RenditionRung::Display,
            Some(true),
        )
        .expect("the encoder runs on decoded pixels");
        assert!(
            bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP",
            "a RIFF/WEBP container"
        );
        let decoded = image::load_from_memory(&bytes).expect("the WebP decodes");
        assert_eq!(decoded.dimensions(), (32, 32));
        assert!(
            has_alpha_pixels(&decoded),
            "the transparent corner has to survive the round trip"
        );

        // The flattening path: the same picture without alpha.
        let flattened = encode_rendition(
            &image,
            RenditionFormat::Webp,
            RenditionRung::Display,
            Some(false),
        )
        .expect("the encoder runs");
        let decoded = image::load_from_memory(&flattened).expect("the WebP decodes");
        assert!(!has_alpha_pixels(&decoded));
    }

    /// R4's measurement: pixels, never the header. An RGBA image whose every
    /// pixel is opaque is opaque.
    #[test]
    fn transparency_is_measured_from_pixels_not_from_the_channel() {
        assert!(has_alpha_pixels(&sample_image(8, 8, true)));
        let opaque_rgba = DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
            8,
            8,
            image::Rgba([1, 2, 3, 255]),
        ));
        assert!(
            opaque_rgba.color().has_alpha(),
            "the fixture has to carry the channel for the test to mean anything"
        );
        assert!(!has_alpha_pixels(&opaque_rgba));
        assert!(!has_alpha_pixels(&sample_image(8, 8, false)));
    }

    /// A gradient, so nothing about an encode can be an artefact of a flat
    /// picture. `transparent` marks one corner non-opaque.
    fn sample_image(width: u32, height: u32, transparent: bool) -> DynamicImage {
        let mut buffer = image::RgbaImage::new(width, height);
        for (x, y, pixel) in buffer.enumerate_pixels_mut() {
            let alpha = if transparent && x < width / 4 && y < height / 4 {
                0
            } else {
                255
            };
            *pixel = image::Rgba([(x * 4) as u8, (y * 4) as u8, 128, alpha]);
        }
        DynamicImage::ImageRgba8(buffer)
    }

    /// Every marker segment of a JPEG as `(marker, payload)`, in file order.
    ///
    /// One walker for both questions asked of these bytes — which segments
    /// carry a table, and what the frame header says — because two walkers
    /// over the same byte format is two places to get the start-of-scan
    /// boundary wrong, and past it the entropy-coded data reads as markers.
    fn jpeg_segments(bytes: &[u8]) -> Vec<(u8, &[u8])> {
        let mut segments = Vec::new();
        let mut index = 2; // Past SOI.
        while index + 4 <= bytes.len() {
            if bytes[index] != 0xFF {
                break;
            }
            let marker = bytes[index + 1];
            // Start of scan: everything past it is entropy-coded data.
            if marker == 0xDA {
                break;
            }
            let length = usize::from(u16::from_be_bytes([bytes[index + 2], bytes[index + 3]]));
            let end = (index + 2 + length).min(bytes.len());
            segments.push((marker, &bytes[index + 4..end]));
            index += 2 + length;
        }
        segments
    }

    /// The payloads carrying `marker`. JPEG may split its tables over several
    /// segments, so the answer is a list.
    fn jpeg_tables(bytes: &[u8], marker: u8) -> Vec<Vec<u8>> {
        jpeg_segments(bytes)
            .into_iter()
            .filter(|(found, _)| *found == marker)
            .map(|(_, payload)| payload.to_vec())
            .collect()
    }

    /// The frame marker and the luma component's sampling byte. `None` when
    /// there is no frame header at all.
    fn jpeg_frame(bytes: &[u8]) -> Option<(u8, u8)> {
        jpeg_segments(bytes)
            .into_iter()
            // SOF0..SOF3 and SOF5..SOF15, excluding the non-frame
            // FFC4/FFC8/FFCC.
            .find(|(marker, _)| {
                (0xC0..=0xCF).contains(marker) && !matches!(marker, 0xC4 | 0xC8 | 0xCC)
            })
            .and_then(|(marker, payload)| {
                // precision(1) height(2) width(2) components(1), then per
                // component: id(1) sampling(1) quant table(1).
                Some((marker, *payload.get(6 + 1)?))
            })
    }
}
