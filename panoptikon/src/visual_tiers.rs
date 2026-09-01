//! The stored-rendition ladder every visual surface serves from
//! (`docs/grid-scroll-performance-implementation.md` §2).
//!
//! Three static tiers, all derived from one decode:
//!
//! * `display` — gallery quality and the browser-safety bound. Capped on the
//!   **short** side (4096) and on total pixels (32 MP), because a cell that
//!   paints with `object-cover` is only ever as crisp as the image's short
//!   side, and because decoded megapixels — not bytes — are what stall a
//!   scrolling grid.
//! * `grid-m` (short side 1024) and `grid-s` (short side 512) — the grid,
//!   filmstrip and small-pin tiers. Requesting the smallest tier whose short
//!   side covers the cell box is what keeps decoded megapixels per screenful
//!   constant as cells shrink.
//!
//! …and, for an animated item above the raw floor, a fourth rendition that is
//! not a tier at all: ONE H.264 `loop` answering both grid tiers, plus the
//! static posters `still=true` selects. Its geometry rules live here beside
//! the static ones, and what gets *served* when a loop is missing is the
//! fallback ladder below.
//!
//! The rules here are pure functions of `(file_size, width, height)` —
//! deliberately, because the scan's backfill
//! dispatcher has to answer "does this item already have what the current
//! generator would produce?" from *indexed metadata*, never by decoding the
//! file again (`jobs::files::maybe_dispatch_backfill`). Anything that made a
//! rendition's geometry depend on the pixels would re-decode every image in
//! the library on every scan, forever.
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
//!   both grid tiers, immutable;
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

use image::{DynamicImage, GenericImageView, imageops::FilterType};
use serde::Deserialize;
use utoipa::ToSchema;

/// Display tier: the largest short side served without a stored rendition.
pub(crate) const DISPLAY_MAX_SHORT_SIDE: u32 = 4096;
/// Display tier: the largest total pixel count served from the original.
/// Decimal megapixels, so the bound reads as the "32 MP" it is written as.
pub(crate) const DISPLAY_MAX_PIXELS: u64 = 32_000_000;
/// Display tier: the largest original that is served as-is regardless of how
/// modest its dimensions are. A 30 MB PNG of a 4000x3000 photo is within
/// every pixel bound and still worth re-encoding.
pub(crate) const DISPLAY_MAX_FILE_SIZE: u64 = 24 * 1024 * 1024;

/// Grid tiers: the largest original served directly, as a multiple of the
/// tier's short side. 1.25x — a quarter over budget decodes cheaply enough
/// that storing a near-identical rendition would cost more than it saves.
/// Expressed as a ratio so the comparison stays integral. The same slack
/// applies to the **long** side against `2 * tier`, which is the longest a
/// stored tier is ever allowed to be.
const GRID_DIRECT_NUMERATOR: u64 = 5;
const GRID_DIRECT_DENOMINATOR: u64 = 4;
/// Grid tiers: the largest original served directly by byte count.
pub(crate) const GRID_DIRECT_MAX_FILE_SIZE: u64 = 8 * 1024 * 1024;

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

/// The animated loop's short-side cap. ONE loop per item, reused by both grid
/// tiers — an H.264 stream is not a ladder: `grid-s` cells paint the same
/// decode scaled down, and a second encode would double the scan's most
/// expensive visual for no measurable decode saving.
pub(crate) const LOOP_MAX_SHORT_SIDE: u32 = 1024;

/// The stored discriminator of the animated loop, in the same column as
/// `grid-m`/`grid-s` and deliberately not a [`ThumbnailTier`]: the loop is a
/// rendition *kind*, not a `size=` value. It answers **both** grid tiers.
pub(crate) const LOOP_TIER: &str = "loop";

/// The loop's stored media type, and what the endpoint serves it as.
pub(crate) const LOOP_MEDIA_TYPE: &str = "video/mp4";

/// Every static rendition's media type — the display renditions and both grid
/// tiers are q85 JPEGs.
pub(crate) const TIER_MEDIA_TYPE: &str = "image/jpeg";

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
}

impl ThumbnailTier {
    /// The stored discriminator, and the wire value. Kept in one place so the
    /// column, the ETag and the query parameter can never disagree.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Display => "display",
            Self::GridM => "grid-m",
            Self::GridS => "grid-s",
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
        }
    }

    /// The grid tiers, largest first — the order generation cascades in.
    pub(crate) const GRID: [Self; 2] = [Self::GridM, Self::GridS];
}

/// What the display rule wants for an image with these measurements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DisplayPlan {
    /// Serve the original file; store nothing.
    Original,
    /// Store a rendition of exactly these dimensions.
    Thumbnail { width: u32, height: u32 },
}

/// The display rule, re-based on the short side (§2).
///
/// Serve the original iff its short side is within
/// [`DISPLAY_MAX_SHORT_SIDE`], its total pixels within
/// [`DISPLAY_MAX_PIXELS`] and its bytes within [`DISPLAY_MAX_FILE_SIZE`].
///
/// The old rule let *any* file under 5 MB through at its original
/// resolution, which is how a 2.9 MB 100 MP JPEG reached the grid raw, and it
/// capped stored thumbs on the **long** side — backwards for `object-cover`
/// cells, and the reason an 800x20000 webtoon used to render 163 px wide in
/// the gallery. Under this rule that webtoon's original serves directly (16
/// MP, one image at a time) and the 100 MP file finally gets a rendition.
pub(crate) fn display_plan(file_size: u64, width: u32, height: u32) -> DisplayPlan {
    if width == 0 || height == 0 {
        return DisplayPlan::Original;
    }
    let short = width.min(height);
    let pixels = u64::from(width) * u64::from(height);
    if short <= DISPLAY_MAX_SHORT_SIDE
        && pixels <= DISPLAY_MAX_PIXELS
        && file_size <= DISPLAY_MAX_FILE_SIZE
    {
        return DisplayPlan::Original;
    }
    let (width, height) = display_dimensions(width, height);
    DisplayPlan::Thumbnail { width, height }
}

/// `min(4096/short, sqrt(32MP/pixels), 1)` applied to both sides.
///
/// The `1` matters: a file that only broke the *byte* bound keeps its pixel
/// dimensions and is simply re-encoded, which is the whole point of that
/// bound.
fn display_dimensions(width: u32, height: u32) -> (u32, u32) {
    let short = f64::from(width.min(height));
    let pixels = f64::from(width) * f64::from(height);
    let scale = (f64::from(DISPLAY_MAX_SHORT_SIDE) / short)
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
pub(crate) struct TierRender {
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
pub(crate) fn tier_render(width: u32, height: u32, tier: u32) -> TierRender {
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
    let out_long = round_div(u64::from(crop_long) * u64::from(out_short), u64::from(short));
    let out_long = out_long.max(1);

    if tall {
        TierRender {
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
        TierRender {
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
/// tier is ever allowed to be ([`tier_render`]), so an original already
/// within a quarter of that is exactly as cheap to decode as the rendition
/// would have been.
pub(crate) fn grid_serves_original(file_size: u64, width: u32, height: u32, tier: u32) -> bool {
    within_grid_dimensions(width, height, tier) && file_size <= GRID_DIRECT_MAX_FILE_SIZE
}

/// The same question for a rendition **derived from a stored thumbnail** (a
/// video's frame grid, an audio cover, a rendered PDF page): the byte clause
/// is dropped because the source is already a q85 JPEG the generator wrote,
/// never a user file that could be arbitrarily large.
pub(crate) fn grid_serves_stored_thumbnail(width: u32, height: u32, tier: u32) -> bool {
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
pub(crate) fn grid_plan(
    file_size: u64,
    width: u32,
    height: u32,
    tier: ThumbnailTier,
) -> Option<TierRender> {
    let short_side = tier.short_side()?;
    if width == 0 || height == 0 {
        return None;
    }
    if grid_serves_original(file_size, width, height, short_side) {
        return None;
    }
    Some(tier_render(width, height, short_side))
}

/// [`grid_plan`] for a rendition derived from a stored thumbnail.
pub(crate) fn grid_plan_for_stored_thumbnail(
    width: u32,
    height: u32,
    tier: ThumbnailTier,
) -> Option<TierRender> {
    let short_side = tier.short_side()?;
    if width == 0 || height == 0 {
        return None;
    }
    if grid_serves_stored_thumbnail(width, height, short_side) {
        return None;
    }
    Some(tier_render(width, height, short_side))
}

/// Applies a [`TierRender`]. `crop_imm` is a view copy, so the resize reads
/// only the pixels the crop kept.
pub(crate) fn render(image: &DynamicImage, plan: &TierRender) -> DynamicImage {
    let (width, height) = image.dimensions();
    let cropped = if plan.crop_x == 0
        && plan.crop_y == 0
        && plan.crop_width == width
        && plan.crop_height == height
    {
        image.clone()
    } else {
        image.crop_imm(plan.crop_x, plan.crop_y, plan.crop_width, plan.crop_height)
    };
    if cropped.width() == plan.width && cropped.height() == plan.height {
        return cropped;
    }
    cropped.resize_exact(plan.width, plan.height, FilterType::Lanczos3)
}

/// The grid renditions to store for one already-decoded picture, largest
/// first.
///
/// `grid-s` cascades off `grid-m` when there is one — the two crops cover the
/// identical source region by construction, so the cascade is exact and
/// halves the resize work. It never cascades off the **display** rendition:
/// a megapixel-guarded display tier can be *smaller* than `grid-m` (a
/// 800x60000 strip scales to 653 px wide), and cropping an already-scaled
/// intermediate would upscale.
pub(crate) fn grid_renditions(
    image: &DynamicImage,
    plans: &[(ThumbnailTier, TierRender)],
) -> Vec<(ThumbnailTier, DynamicImage)> {
    let mut out: Vec<(ThumbnailTier, DynamicImage)> = Vec::with_capacity(plans.len());
    for (tier, plan) in plans {
        let source = out.last().map(|(_, image)| image).unwrap_or(image);
        out.push((*tier, render(source, plan)));
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
) -> Vec<(ThumbnailTier, TierRender)> {
    cascade(Some(file_size), width, height)
}

/// [`grid_plans`] for a source that is itself a stored thumbnail.
pub(crate) fn grid_plans_for_stored_thumbnail(
    width: u32,
    height: u32,
) -> Vec<(ThumbnailTier, TierRender)> {
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
fn cascade(
    file_size: Option<u64>,
    width: u32,
    height: u32,
) -> Vec<(ThumbnailTier, TierRender)> {
    let mut out: Vec<(ThumbnailTier, TierRender)> = Vec::with_capacity(ThumbnailTier::GRID.len());
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

/// The loop's geometry: the [`tier_render`] of the source at
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
pub(crate) fn loop_render(width: u32, height: u32) -> TierRender {
    let mut plan = tier_render(width, height, LOOP_MAX_SHORT_SIDE);
    plan.width = even_side(plan.width);
    plan.height = even_side(plan.height);
    plan
}

fn even_side(side: u32) -> u32 {
    if side < 2 { 2 } else { side & !1 }
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
pub(crate) fn poster_plans(width: u32, height: u32) -> Vec<(ThumbnailTier, TierRender)> {
    let mut out = Vec::with_capacity(ThumbnailTier::GRID.len());
    let mut source = (width, height);
    for (index, tier) in ThumbnailTier::GRID.into_iter().enumerate() {
        let Some(short_side) = tier.short_side() else {
            continue;
        };
        let plan = tier_render(source.0, source.1, short_side);
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
/// of it came out smaller.
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

/// The **whole** stored set of an animated item above the raw floor: its
/// posters, then its loop, named by the strings the `tier` column holds.
///
/// One function for the dispatcher's prediction and the generator's output,
/// for the same reason [`grid_plans`] is one function: the backfill compares
/// the stored geometry against this and never terminates if the two can
/// disagree. Ordered the way `get_thumbnail_tier_geometry` returns rows —
/// `grid-m`, `grid-s`, `loop` is already lexicographic — but the comparison
/// sorts anyway.
pub(crate) fn animated_plans(width: u32, height: u32) -> Vec<(&'static str, TierRender)> {
    let mut out: Vec<(&'static str, TierRender)> = poster_plans(width, height)
        .into_iter()
        .map(|(tier, plan)| (tier.as_str(), plan))
        .collect();
    out.push((LOOP_TIER, loop_render(width, height)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    // The dead 100 MP hole: the old rule served anything under 5 MB at its
    // original resolution, so a 2.9 MB 100 MP JPEG reached the grid raw.
    #[test]
    fn the_display_rule_is_dimension_first() {
        // A 12 MP photo, comfortably inside every bound.
        assert_eq!(display_plan(4 * MB, 4000, 3000), DisplayPlan::Original);

        // 100 MP under 3 MB: the hole. 12000x8333 = 99.996 MP.
        let plan = display_plan(3 * MB, 12000, 8333);
        let DisplayPlan::Thumbnail { width, height } = plan else {
            panic!("a 100 MP original must not be served directly: {plan:?}");
        };
        assert!(u64::from(width) * u64::from(height) <= DISPLAY_MAX_PIXELS);
        assert!(width.min(height) <= DISPLAY_MAX_SHORT_SIDE);

        // Short side over the cap, total pixels under it.
        assert!(matches!(
            display_plan(MB, 5000, 5000),
            DisplayPlan::Thumbnail { .. }
        ));

        // A tall webtoon: 16 MP, short side 800. The old rule capped its
        // *long* side and rendered it 163 px wide in the gallery; now the
        // original serves.
        assert_eq!(display_plan(6 * MB, 800, 20000), DisplayPlan::Original);

        // Only the byte bound is broken, so the rendition keeps every pixel
        // and is merely re-encoded.
        assert_eq!(
            display_plan(30 * MB, 4000, 3000),
            DisplayPlan::Thumbnail {
                width: 4000,
                height: 3000
            }
        );
        // Exactly at the byte bound is still the original.
        assert_eq!(
            display_plan(DISPLAY_MAX_FILE_SIZE, 4000, 3000),
            DisplayPlan::Original
        );
    }

    // The scale factor is `min(4096/short, sqrt(32MP/pixels), 1)`, and which
    // of the three binds depends on the shape.
    #[test]
    fn display_scaling_takes_the_binding_bound() {
        // Short side binds: 8192 -> 4096, aspect kept.
        assert_eq!(display_dimensions(8192, 12288), (4096, 6144));
        // Megapixels bind: 6000x6000 = 36 MP -> sqrt(32/36) = 0.9428.
        let (width, height) = display_dimensions(6000, 6000);
        assert!(u64::from(width) * u64::from(height) <= DISPLAY_MAX_PIXELS);
        assert_eq!(width, height);
        // Neither binds: the clamp at 1 keeps the original geometry.
        assert_eq!(display_dimensions(1000, 800), (1000, 800));
    }

    #[test]
    fn a_normal_aspect_tier_is_a_plain_short_side_resize() {
        // 3000x4000, tier 1024: short side to 1024, no crop.
        let plan = tier_render(3000, 4000, 1024);
        assert_eq!(
            plan,
            TierRender {
                crop_x: 0,
                crop_y: 0,
                crop_width: 3000,
                crop_height: 4000,
                width: 1024,
                height: 1365,
            }
        );

        // Landscape is the same rule on the other axis.
        let plan = tier_render(4000, 3000, 512);
        assert_eq!((plan.width, plan.height), (683, 512));
        assert_eq!((plan.crop_width, plan.crop_height), (4000, 3000));
    }

    // Aspect exactly 2 is the boundary and stays whole-image: the crop cap
    // is `2 * tier`, which is exactly what a 2:1 image scales to.
    #[test]
    fn aspect_exactly_two_is_not_cropped() {
        for (width, height) in [(1000_u32, 2000_u32), (2000, 1000)] {
            let plan = tier_render(width, height, 512);
            assert_eq!(
                (plan.crop_width, plan.crop_height),
                (width, height),
                "{width}x{height} is exactly 2:1 and must keep every pixel"
            );
            assert_eq!(plan.width.min(plan.height), 512);
            assert_eq!(plan.width.max(plan.height), 1024);
        }
        // Just past the boundary, the crop engages.
        let plan = tier_render(1000, 2001, 512);
        assert!(plan.crop_height < 2001, "{plan:?}");
    }

    // `object-position: 50% 0%`: tall keeps the top, wide keeps the center.
    #[test]
    fn extreme_aspect_crops_match_the_css_presentation() {
        // A webtoon: 800x20000. Tier 1024 -> short side stays 800 (no
        // upscale), long side capped at 2048.
        let plan = tier_render(800, 20000, 1024);
        assert_eq!(plan.crop_x, 0);
        assert_eq!(plan.crop_y, 0, "tall images keep the TOP strip");
        assert_eq!(plan.crop_width, 800);
        assert_eq!((plan.width, plan.height), (800, 2048));
        assert_eq!(plan.crop_height, 2048, "no upscale, so crop == output");

        // A tall strip wide enough to be scaled down as well.
        let plan = tier_render(2000, 30000, 512);
        assert_eq!(plan.crop_y, 0);
        assert_eq!((plan.width, plan.height), (512, 1024));
        // 1024 output rows at 512/2000 scale = 4000 source rows.
        assert_eq!(plan.crop_height, 4000);

        // A wide strip: 20000x800, tier 1024. The band is centered.
        let plan = tier_render(20000, 800, 1024);
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
            for tier in [1024_u32, 512] {
                let plan = tier_render(width, height, tier);
                assert_eq!(
                    (plan.width, plan.height),
                    (width, height),
                    "{width}x{height} at tier {tier} must be untouched"
                );
            }
        }
        // A tiny *strip* still crops (its long side is over 2x the tier only
        // if it is over 2x the tier; below that it is left whole).
        let plan = tier_render(100, 300, 512);
        assert_eq!((plan.width, plan.height), (100, 300));
        let plan = tier_render(100, 3000, 512);
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
            for tier in [1024_u32, 512] {
                let plan = tier_render(width, height, tier);
                let pixels = u64::from(plan.width) * u64::from(plan.height);
                assert!(
                    pixels <= 2 * u64::from(tier) * u64::from(tier),
                    "{width}x{height} at tier {tier} produced {plan:?}"
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
        // Bytes over the bound do not.
        assert!(!grid_serves_original(
            GRID_DIRECT_MAX_FILE_SIZE + 1,
            600,
            600,
            1024
        ));
        assert!(grid_serves_original(GRID_DIRECT_MAX_FILE_SIZE, 600, 600, 1024));
        // The derived-thumbnail form drops only the byte clause.
        assert!(grid_serves_stored_thumbnail(600, 600, 1024));
        assert!(!grid_serves_stored_thumbnail(800, 20000, 1024));
    }

    // The point of the refinement, stated as the plan does: a 1000x2100 image
    // stores no grid-m at all (the original serves it), while an 800x20000
    // webtoon still stores its top-strip crops at both tiers.
    #[test]
    fn the_long_side_bound_skips_near_identical_tiers_but_keeps_strip_crops() {
        let plans = grid_plans(2 * MB, 1000, 2100);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridS],
            "grid-m must not store a 2%-smaller copy of the whole picture"
        );

        let plans = grid_plans(6 * MB, 800, 20000);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridM, ThumbnailTier::GridS]
        );
        // Both are top-strip crops, exactly as before the refinement.
        assert_eq!((plans[0].1.crop_y, plans[0].1.crop_height), (0, 2048));
        assert_eq!((plans[0].1.width, plans[0].1.height), (800, 2048));
        // grid-s cascades off the 800x2048 grid-m: 1600 of its rows scale to
        // the 1024 the tier allows.
        assert_eq!((plans[1].1.crop_y, plans[1].1.crop_height), (0, 1600));
        assert_eq!((plans[1].1.width, plans[1].1.height), (512, 1024));
    }

    // The cascade selects the same source region as a direct plan would, and
    // the same short side. The *long* side can differ by a pixel where two
    // roundings compose (12000x8333 lands on 738 cascaded, 737 direct), which
    // is harmless precisely because the dispatcher predicts with this same
    // function rather than with a direct plan.
    #[test]
    fn the_grid_s_cascade_agrees_with_a_direct_plan() {
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
            let direct = tier_render(width, height, 512);
            let last = cascaded
                .last()
                .unwrap_or_else(|| panic!("{width}x{height} planned nothing"));
            assert_eq!(last.0, ThumbnailTier::GridS);
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

    // A source small enough that `grid-m` serves it directly still gets a
    // `grid-s` rendition, planned against the source rather than against a
    // rendition that was never made.
    #[test]
    fn a_skipped_tier_does_not_break_the_cascade() {
        let plans = grid_plans(MB, 1200, 1200);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].0, ThumbnailTier::GridS);
        assert_eq!((plans[0].1.width, plans[0].1.height), (512, 512));

        // Small enough for both: nothing is stored at all.
        assert!(grid_plans(MB, 600, 600).is_empty());
        // A narrow picture that is small on *both* axes is equally free —
        // aspect on its own no longer forces a stored crop.
        assert!(grid_plans(MB, 200, 600).is_empty());
        // ... but a genuine strip stores both, because its long side is far
        // past what either tier would ever hold.
        let plans = grid_plans(MB, 200, 6000);
        assert_eq!(plans.len(), 2);
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
        let plan = tier_render(100, 1000, 100);
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
        let plan = tier_render(1000, 100, 100);
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
        assert!(animated_serves_original(ANIMATED_RAW_MAX_FILE_SIZE, 512, 512));
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
        let plan = loop_render(1500, 2000);
        assert_eq!((plan.crop_width, plan.crop_height), (1500, 2000));
        assert_eq!((plan.width, plan.height), (1024, 1364));
        // The unrounded render would have been 1365 rows; evenness rounds
        // DOWN, never up.
        assert_eq!(tier_render(1500, 2000, 1024).height, 1365);

        // Never upscaled: a source under the cap keeps its size (rounded).
        let plan = loop_render(300, 401);
        assert_eq!((plan.width, plan.height), (300, 400));

        // A tall strip: top band, long side capped at 2 * 1024.
        let plan = loop_render(800, 20000);
        assert_eq!((plan.crop_x, plan.crop_y), (0, 0));
        assert_eq!(plan.crop_height, 2048, "tall loops keep the TOP strip");
        assert_eq!((plan.width, plan.height), (800, 2048));

        // A wide strip: horizontally centered band.
        let plan = loop_render(20000, 800);
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
            let plan = loop_render(width, height);
            assert_eq!(plan.width % 2, 0, "{width}x{height} -> {plan:?}");
            assert_eq!(plan.height % 2, 0, "{width}x{height} -> {plan:?}");
            assert!(plan.width >= 2 && plan.height >= 2, "{plan:?}");
        }
    }

    // A poster always exists, however small the animation is: the original
    // moves, so it can never be the poster.
    #[test]
    fn posters_always_store_a_grid_m_and_deduplicate_grid_s() {
        // Small enough that both tiers would be the identity render: only
        // `grid-m` is stored, and a `grid-s` request falls up to it.
        let plans = poster_plans(300, 300);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridM]
        );
        assert_eq!((plans[0].1.width, plans[0].1.height), (300, 300));

        // Big enough for both, cascading exactly like the static ladder.
        let plans = poster_plans(2000, 2000);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec![ThumbnailTier::GridM, ThumbnailTier::GridS]
        );
        assert_eq!((plans[0].1.width, plans[0].1.height), (1024, 1024));
        assert_eq!((plans[1].1.width, plans[1].1.height), (512, 512));

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
    fn the_animated_set_is_its_posters_plus_exactly_one_loop() {
        let plans = animated_plans(2000, 2000);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec!["grid-m", "grid-s", LOOP_TIER]
        );
        assert_eq!(
            plans
                .iter()
                .filter(|(tier, _)| *tier == LOOP_TIER)
                .count(),
            1,
            "one animated rendition per item, reused by both grid tiers"
        );
        let (_, loop_plan) = plans.last().unwrap();
        assert_eq!((loop_plan.width, loop_plan.height), (1024, 1024));

        // The smallest possible set: one poster and one loop.
        let plans = animated_plans(300, 300);
        assert_eq!(
            plans.iter().map(|(tier, _)| *tier).collect::<Vec<_>>(),
            vec!["grid-m", LOOP_TIER]
        );
    }

    // Serving a rendition *larger* than the file it replaces would invert the
    // whole point of the ladder, so the original wins ties and everything
    // above them.
    #[test]
    fn an_encode_no_smaller_than_its_source_keeps_the_original() {
        assert!(loop_keeps_original(40_000, 12_000));
        assert!(loop_keeps_original(12_000, 12_000), "a tie keeps the source");
        assert!(!loop_keeps_original(11_999, 12_000));
        // The ordinary case by a wide margin: a GIF against its H.264.
        assert!(!loop_keeps_original(120_000, 6 * MB));
    }

    #[test]
    fn tier_wire_values_are_the_frozen_contract() {
        assert_eq!(ThumbnailTier::Display.as_str(), "display");
        assert_eq!(ThumbnailTier::GridM.as_str(), "grid-m");
        assert_eq!(ThumbnailTier::GridS.as_str(), "grid-s");
        assert_eq!(ThumbnailTier::default(), ThumbnailTier::Display);
        for tier in [
            ThumbnailTier::Display,
            ThumbnailTier::GridM,
            ThumbnailTier::GridS,
        ] {
            let parsed: ThumbnailTier =
                serde_json::from_str(&format!("\"{}\"", tier.as_str())).unwrap();
            assert_eq!(parsed, tier);
        }
    }
}
