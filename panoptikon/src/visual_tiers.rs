//! The stored-rendition ladder every visual surface serves from
//! (`docs/grid-scroll-performance-implementation.md` §2).
//!
//! Three tiers, all derived from one decode:
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
//! Two rules do all the work here, and both are pure functions of
//! `(file_size, width, height)` — deliberately, because the scan's backfill
//! dispatcher has to answer "does this item already have what the current
//! generator would produce?" from *indexed metadata*, never by decoding the
//! file again (`jobs::files::maybe_dispatch_backfill`). Anything that made a
//! rendition's geometry depend on the pixels would re-decode every image in
//! the library on every scan, forever.

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
/// Expressed as a ratio so the comparison stays integral.
const GRID_DIRECT_NUMERATOR: u64 = 5;
const GRID_DIRECT_DENOMINATOR: u64 = 4;
/// Grid tiers: the largest original served directly by byte count.
pub(crate) const GRID_DIRECT_MAX_FILE_SIZE: u64 = 8 * 1024 * 1024;

/// The aspect above which a grid tier becomes a *crop* rather than a
/// whole-image resize. At or below it, `long <= 2 * short`, and the tier is
/// the plain short-side resize of the whole picture.
const GRID_MAX_WHOLE_ASPECT: u32 = 2;

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
/// stored rendition (§2): short side within 1.25x the tier, aspect within 2
/// (a strip always has a stored crop), and bytes within
/// [`GRID_DIRECT_MAX_FILE_SIZE`].
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

fn within_grid_dimensions(width: u32, height: u32, tier: u32) -> bool {
    if width == 0 || height == 0 {
        return true;
    }
    let short = u64::from(width.min(height));
    let long = u64::from(width.max(height));
    short * GRID_DIRECT_DENOMINATOR <= u64::from(tier) * GRID_DIRECT_NUMERATOR
        && long <= u64::from(GRID_MAX_WHOLE_ASPECT) * short
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
    fn the_grid_serve_rule_is_dimensions_then_aspect_then_bytes() {
        // Inside all three: served directly.
        assert!(grid_serves_original(2 * MB, 1200, 1200, 1024));
        // Exactly 1.25x the tier is still direct.
        assert!(grid_serves_original(2 * MB, 1280, 1280, 1024));
        // One pixel over is not.
        assert!(!grid_serves_original(2 * MB, 1281, 1281, 1024));
        // Aspect over 2 never serves directly, however small.
        assert!(!grid_serves_original(MB, 300, 900, 1024));
        // Exactly 2 does.
        assert!(grid_serves_original(MB, 300, 600, 1024));
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
        assert!(!grid_serves_stored_thumbnail(300, 900, 1024));
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
        // ... and a strip of the same size still stores both, because a
        // strip always has a stored crop.
        let plans = grid_plans(MB, 200, 600);
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
