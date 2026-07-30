//! Cost-dimension metadata: what a model's memory scales with.
//!
//! Calibration learns `memory ≈ base + slope × units`, and *unit* is a
//! per-model property declared in the registry
//! (docs/batch-calibration-design.md, "Cost dimension taxonomy" and "Model
//! metadata additions"):
//!
//! ```toml
//! [group.clip.metadata.cost]
//! unit        = "item"      # item | pixel | token | audio-second | none
//! aggregation = "count"     # count | sum | max-times-count
//! epoch       = 1           # invalidation lever for the calibration store
//! seed_units  = 8           # first-touch batch on unknown hardware
//! ```
//!
//! Two rules govern resolution:
//!
//! - **Per-key overlay.** An inference id's `metadata.cost` overlays its
//!   group's *key by key*, so an id that only deviates in one dimension
//!   declares only that key. (Metadata layering elsewhere replaces a
//!   top-level key wholesale; for a nested table that would silently drop
//!   the group's other cost keys, which is the opposite of what "override
//!   where an id deviates" means.) This is a **deliberate divergence** from
//!   `Registry`'s wholesale `merge_metadata`, and it needs one guard to be
//!   safe: `seed_units` is scale-bound, so it is *not* inherited across a
//!   unit change (see `resolve_seed_units`) — inheriting `8` from an
//!   `item` group into a `pixel` id would seed batches with 8 pixels, i.e.
//!   nothing, and inheriting `2_000_000` the other way would seed two
//!   million items. Every other cost key is scale-free and inherits.
//! - **Degradation, never an error.** A missing or unparseable declaration
//!   yields `(item, count)` with a conservative seed and `degraded = true`
//!   — worse packing, never a crash and never a refused load. Registry
//!   authors get a warning in the log, not a failure.

use serde_json::{Map as JsonMap, Value as JsonValue};

use super::registry::Registry;

/// Seed batch used when a model declares nothing at all: small enough to be
/// safe on any card that can hold the model, and the ramp grows from it
/// geometrically anyway.
pub const FALLBACK_SEED_UNITS: u32 = 4;

/// `metadata.cost.epoch` default (design: "declared in model metadata
/// (`metadata.cost.epoch`, default 1)").
pub const DEFAULT_EPOCH: u32 = 1;

/// What one unit of a model's batch is measured in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostUnit {
    /// No meaningful GPU batch scaling: remote APIs, network lookups, and
    /// sequential engines with no torch allocator. No admission; at most a
    /// `base` footprint is recorded.
    None,
    Item,
    Pixel,
    Token,
    AudioSecond,
}

impl CostUnit {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "item" => Some(Self::Item),
            "pixel" => Some(Self::Pixel),
            "token" => Some(Self::Token),
            "audio-second" | "audio_second" => Some(Self::AudioSecond),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Item => "item",
            Self::Pixel => "pixel",
            Self::Token => "token",
            Self::AudioSecond => "audio-second",
        }
    }

    /// Conservative first-touch batch for a unit class, used when
    /// `seed_units` is absent or invalid. Deliberately per class: 4 items
    /// and 4 megapixels are wildly different numbers for the same intent.
    fn fallback_seed(self) -> Option<u32> {
        match self {
            Self::None => None,
            Self::Item => Some(FALLBACK_SEED_UNITS),
            // ~2 MP: one 1536² frame, the doctr slice target.
            Self::Pixel => Some(2_000_000),
            Self::Token => Some(2_000),
            Self::AudioSecond => Some(60),
        }
    }
}

/// How per-input units combine into batch units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostAggregation {
    /// Batch units = number of items (each item is a fixed size).
    Count,
    /// Batch units = Σ per-item units (e.g. total decoded pixels).
    Sum,
    /// Batch units = largest item's units × item count: padded/uniform
    /// batches, where every slot pays for the largest member.
    MaxTimesCount,
}

impl CostAggregation {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "sum" => Some(Self::Sum),
            "max-times-count" | "max_times_count" => Some(Self::MaxTimesCount),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::MaxTimesCount => "max-times-count",
        }
    }
}

/// One model's resolved cost dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostDimension {
    pub unit: CostUnit,
    /// `None` exactly when `unit` is [`CostUnit::None`] — there is nothing
    /// to aggregate for a model that does not scale.
    pub aggregation: Option<CostAggregation>,
    pub epoch: u32,
    /// `None` exactly when `unit` is [`CostUnit::None`].
    pub seed_units: Option<u32>,
    /// True when the declaration was missing or unparseable and this is the
    /// conservative fallback. The ledger widens margins for these, the same
    /// way it does for a profile it has not confirmed locally.
    pub degraded: bool,
}

impl CostDimension {
    /// The conservative `(item, count)` fallback.
    pub fn fallback() -> Self {
        Self {
            unit: CostUnit::Item,
            aggregation: Some(CostAggregation::Count),
            epoch: DEFAULT_EPOCH,
            seed_units: Some(FALLBACK_SEED_UNITS),
            degraded: true,
        }
    }

    /// Whether batches of this model are worth pricing at all. `false` for
    /// the `none` class, which gets seed-sized fixed batches and the
    /// Package-1 OOM backstop instead of admission.
    pub fn scales(&self) -> bool {
        self.unit != CostUnit::None
    }

    /// Resolve `group/name`'s dimension from registry metadata. Never
    /// fails; see the module docs for the degradation rule.
    pub fn resolve(registry: &Registry, full_inference_id: &str) -> Self {
        let Some((group_name, inference_id)) = full_inference_id.split_once('/') else {
            return Self::fallback();
        };
        let Some(group) = registry.groups.get(group_name) else {
            return Self::fallback();
        };
        let Some(entry) = group.inference_ids.get(inference_id) else {
            return Self::fallback();
        };
        Self::from_tables(
            cost_table(&entry.metadata),
            cost_table(&group.group_metadata),
            full_inference_id,
        )
    }

    fn from_tables(
        id_cost: Option<&JsonMap<String, JsonValue>>,
        group_cost: Option<&JsonMap<String, JsonValue>>,
        full_inference_id: &str,
    ) -> Self {
        let field = |key: &str| -> Option<&JsonValue> {
            id_cost
                .and_then(|table| table.get(key))
                .or_else(|| group_cost.and_then(|table| table.get(key)))
        };
        // epoch is only a lookup key, so a bad value degrades to the
        // default on its own without discarding a good unit declaration.
        let epoch = match field("epoch") {
            None => DEFAULT_EPOCH,
            Some(value) => match value.as_u64().and_then(|epoch| u32::try_from(epoch).ok()) {
                Some(epoch) if epoch >= 1 => epoch,
                _ => {
                    tracing::warn!(
                        inference_id = %full_inference_id,
                        "metadata.cost.epoch {value} is not a positive integer; using {DEFAULT_EPOCH}"
                    );
                    DEFAULT_EPOCH
                }
            },
        };

        let Some(unit) = field("unit") else {
            // Undeclared is the common case until the registry is fully
            // annotated, so it is a debug line, not a warning.
            tracing::debug!(
                inference_id = %full_inference_id,
                "no metadata.cost declaration; using the conservative (item, count) fallback"
            );
            return Self::fallback();
        };
        let Some(unit) = unit.as_str().and_then(CostUnit::parse) else {
            tracing::warn!(
                inference_id = %full_inference_id,
                "metadata.cost.unit {unit} is not a known cost unit; using the \
                 conservative (item, count) fallback"
            );
            return Self::fallback();
        };
        if unit == CostUnit::None {
            return Self {
                unit,
                aggregation: None,
                epoch,
                seed_units: None,
                degraded: false,
            };
        }

        let aggregation = match field("aggregation") {
            Some(value) => match value.as_str().and_then(CostAggregation::parse) {
                Some(aggregation) => aggregation,
                None => {
                    tracing::warn!(
                        inference_id = %full_inference_id,
                        "metadata.cost.aggregation {value} is not a known aggregation; \
                         using the conservative (item, count) fallback"
                    );
                    return Self::fallback();
                }
            },
            None => {
                // A declared unit with no aggregation is an incomplete
                // declaration: defaulting it would invent a pricing rule
                // (`pixel`/`count` is meaningless), so degrade the whole
                // dimension instead.
                tracing::warn!(
                    inference_id = %full_inference_id,
                    "metadata.cost declares unit {} without an aggregation; using the \
                     conservative (item, count) fallback",
                    unit.as_str()
                );
                return Self::fallback();
            }
        };

        let seed_units = resolve_seed_units(id_cost, group_cost, unit, full_inference_id);

        Self {
            unit,
            aggregation: Some(aggregation),
            epoch,
            seed_units,
            degraded: false,
        }
    }
}

fn cost_table(metadata: &JsonMap<String, JsonValue>) -> Option<&JsonMap<String, JsonValue>> {
    metadata.get("cost").and_then(JsonValue::as_object)
}

/// `seed_units` under the per-key overlay, with the one exception the overlay
/// needs: a seed is **scale-bound**. An id that redeclares `unit` (with or
/// without `aggregation`) and nothing else must not inherit the group's seed,
/// which was written for the group's unit — `8` items inherited into a
/// `pixel` id seeds a batch of 8 pixels (zero work per window), and
/// `2_000_000` pixels inherited into an `item` id seeds two million items
/// (an instant OOM the first time it is touched). In that case the
/// unit-class default applies instead. The id's *own* seed always wins: it
/// was written next to the unit it belongs to.
fn resolve_seed_units(
    id_cost: Option<&JsonMap<String, JsonValue>>,
    group_cost: Option<&JsonMap<String, JsonValue>>,
    unit: CostUnit,
    full_inference_id: &str,
) -> Option<u32> {
    let parse = |value: &JsonValue| -> Option<u32> {
        match value.as_u64().and_then(|seed| u32::try_from(seed).ok()) {
            Some(seed) if seed >= 1 => Some(seed),
            _ => {
                tracing::warn!(
                    inference_id = %full_inference_id,
                    "metadata.cost.seed_units {value} is not a positive integer; \
                     using the {} default",
                    unit.as_str()
                );
                unit.fallback_seed()
            }
        }
    };
    if let Some(value) = id_cost.and_then(|table| table.get("seed_units")) {
        return parse(value);
    }
    let Some(value) = group_cost.and_then(|table| table.get("seed_units")) else {
        return unit.fallback_seed();
    };
    // Resolved-vs-resolved, not declared-vs-resolved. A group that declares
    // a seed but no unit — or a unit that does not parse — is itself priced
    // in `item` (that is what `from_tables` degrades to), so `item` is the
    // scale its seed was written on. Testing only for a *declared* unit made
    // the guard blind to exactly that case: an `8`/`2_000_000` seed from an
    // unannotated group flowed into a `pixel`/`token` id, which is the scale
    // mismatch this function exists to stop.
    let group_unit = group_cost
        .and_then(|table| table.get("unit"))
        .and_then(JsonValue::as_str)
        .and_then(CostUnit::parse)
        .unwrap_or(CostUnit::Item);
    if group_unit != unit {
        tracing::debug!(
            inference_id = %full_inference_id,
            "id overrides the group's cost unit, so the group's seed_units \
             (a different scale) is replaced by the {} default",
            unit.as_str()
        );
        return unit.fallback_seed();
    }
    parse(value)
}

#[cfg(test)]
mod tests {
    use super::super::registry::{Registry, RegistryConfig};
    use super::*;
    use std::fs;
    use std::path::Path;

    fn registry_from(toml: &str) -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.toml"), toml).unwrap();
        let registry = Registry::load(&RegistryConfig {
            config_dirs: vec![dir.path().to_path_buf()],
        })
        .expect("fixture registry loads");
        (registry, dir)
    }

    /// Group-level declarations reach every id in the group.
    #[test]
    fn group_declaration_applies_to_every_id() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"
[group.g.metadata.cost]
unit = "item"
aggregation = "count"
epoch = 2
seed_units = 8
[group.g.inference_ids.x]
"#,
        );
        let cost = CostDimension::resolve(&registry, "g/x");
        assert_eq!(cost.unit, CostUnit::Item);
        assert_eq!(cost.aggregation, Some(CostAggregation::Count));
        assert_eq!(cost.epoch, 2);
        assert_eq!(cost.seed_units, Some(8));
        assert!(!cost.degraded);
        assert!(cost.scales());
    }

    /// A per-id block overlays the group's *key by key*: the deviating id
    /// redeclares unit+aggregation+seed and still inherits the group's
    /// epoch, while its sibling keeps the group dimension untouched.
    #[test]
    fn per_id_block_overlays_group_key_by_key() {
        let (registry, _dir) = registry_from(
            r#"
[group.doctr]
config.impl_class = "doctr"
[group.doctr.metadata.cost]
unit = "item"
aggregation = "count"
epoch = 3
seed_units = 8
[group.doctr.inference_ids.plain]
[group.doctr.inference_ids.easyocr]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "max-times-count"
metadata.cost.seed_units = 4000000
"#,
        );
        let deviating = CostDimension::resolve(&registry, "doctr/easyocr");
        assert_eq!(deviating.unit, CostUnit::Pixel);
        assert_eq!(
            deviating.aggregation,
            Some(CostAggregation::MaxTimesCount)
        );
        assert_eq!(deviating.seed_units, Some(4_000_000));
        assert_eq!(deviating.epoch, 3, "epoch still inherited from the group");
        assert!(!deviating.degraded);

        let plain = CostDimension::resolve(&registry, "doctr/plain");
        assert_eq!(plain.unit, CostUnit::Item);
        assert_eq!(plain.aggregation, Some(CostAggregation::Count));
    }

    /// The `none` class needs no aggregation and no seed, and is not
    /// treated as a degraded declaration.
    #[test]
    fn none_class_has_no_aggregation_or_seed() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"
[group.g.metadata.cost]
unit = "item"
aggregation = "count"
seed_units = 8
[group.g.inference_ids.api]
metadata.cost.unit = "none"
"#,
        );
        let cost = CostDimension::resolve(&registry, "g/api");
        assert_eq!(cost.unit, CostUnit::None);
        assert_eq!(cost.aggregation, None);
        assert_eq!(cost.seed_units, None);
        assert_eq!(cost.epoch, DEFAULT_EPOCH);
        assert!(!cost.degraded);
        assert!(!cost.scales(), "none-class models are never priced");
    }

    /// Nothing declared, an unknown group, and an unknown id all degrade to
    /// the conservative fallback rather than erroring.
    #[test]
    fn missing_declaration_degrades() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"
[group.g.inference_ids.x]
"#,
        );
        for id in ["g/x", "g/nope", "nope/x", "malformed"] {
            let cost = CostDimension::resolve(&registry, id);
            assert_eq!(cost, CostDimension::fallback(), "{id} must degrade");
            assert_eq!(cost.unit, CostUnit::Item);
            assert_eq!(cost.aggregation, Some(CostAggregation::Count));
            assert_eq!(cost.seed_units, Some(FALLBACK_SEED_UNITS));
            assert!(cost.degraded);
        }
    }

    /// Invalid declarations degrade the same way: an unknown unit, an
    /// unknown aggregation, and a unit with no aggregation at all.
    #[test]
    fn invalid_declaration_degrades() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"

[group.g.inference_ids.badunit]
metadata.cost.unit = "furlong"
metadata.cost.aggregation = "count"

[group.g.inference_ids.badaggregation]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "average"

[group.g.inference_ids.noaggregation]
metadata.cost.unit = "pixel"

[group.g.inference_ids.nonstring]
metadata.cost.unit = 7
metadata.cost.aggregation = "count"
"#,
        );
        for id in ["badunit", "badaggregation", "noaggregation", "nonstring"] {
            assert_eq!(
                CostDimension::resolve(&registry, &format!("g/{id}")),
                CostDimension::fallback(),
                "g/{id} must degrade to (item, count)"
            );
        }
    }

    /// A bad epoch or seed is repaired in place — the unit declaration is
    /// good, so discarding it would be a worse answer than defaulting the
    /// broken key. Seeds default per unit class.
    #[test]
    fn invalid_epoch_or_seed_falls_back_per_key() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"

[group.g.inference_ids.badepoch]
metadata.cost.unit = "token"
metadata.cost.aggregation = "max-times-count"
metadata.cost.epoch = 0

[group.g.inference_ids.badseed]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"
metadata.cost.seed_units = -5
"#,
        );
        let epoch = CostDimension::resolve(&registry, "g/badepoch");
        assert_eq!(epoch.epoch, DEFAULT_EPOCH);
        assert_eq!(epoch.unit, CostUnit::Token);
        assert_eq!(epoch.seed_units, Some(2_000), "token-class default seed");
        assert!(!epoch.degraded);

        let seed = CostDimension::resolve(&registry, "g/badseed");
        assert_eq!(seed.unit, CostUnit::Pixel);
        assert_eq!(seed.seed_units, Some(2_000_000), "pixel-class default seed");
    }

    /// A per-id block that changes the *unit* must not inherit the group's
    /// `seed_units`: a seed is scale-bound, and inheriting one across a unit
    /// change is silently catastrophic in both directions (8 pixels = no
    /// work; 2M items = instant OOM). The unit-class default applies instead.
    #[test]
    fn seed_units_is_not_inherited_across_a_unit_change() {
        let (registry, _dir) = registry_from(
            r#"
[group.g]
config.impl_class = "cls"
[group.g.metadata.cost]
unit = "item"
aggregation = "count"
seed_units = 8

# Deviates in unit only: the group's 8 would mean 8 pixels.
[group.g.inference_ids.pixels]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"

# Deviates in unit and states its own seed: its own always wins.
[group.g.inference_ids.own_seed]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"
metadata.cost.seed_units = 500000

# Same unit as the group, only the aggregation deviates: still inherits.
[group.g.inference_ids.same_unit]
metadata.cost.aggregation = "max-times-count"
"#,
        );
        let pixels = CostDimension::resolve(&registry, "g/pixels");
        assert_eq!(pixels.unit, CostUnit::Pixel);
        assert_eq!(
            pixels.seed_units,
            Some(2_000_000),
            "the pixel-class default, not the group's 8 items"
        );
        assert!(!pixels.degraded, "this is a valid declaration, not a fallback");

        assert_eq!(
            CostDimension::resolve(&registry, "g/own_seed").seed_units,
            Some(500_000)
        );
        let same_unit = CostDimension::resolve(&registry, "g/same_unit");
        assert_eq!(same_unit.unit, CostUnit::Item);
        assert_eq!(
            same_unit.aggregation,
            Some(CostAggregation::MaxTimesCount),
            "the aggregation override still applies"
        );
        assert_eq!(
            same_unit.seed_units,
            Some(8),
            "same scale, so the group's seed is inherited as before"
        );

        // The reverse direction: a pixel group with an item id.
        let (reverse, _dir) = registry_from(
            r#"
[group.p]
config.impl_class = "cls"
[group.p.metadata.cost]
unit = "pixel"
aggregation = "sum"
seed_units = 2000000
[group.p.inference_ids.text]
metadata.cost.unit = "item"
metadata.cost.aggregation = "count"
"#,
        );
        assert_eq!(
            CostDimension::resolve(&reverse, "p/text").seed_units,
            Some(FALLBACK_SEED_UNITS),
            "two million items would OOM on first touch"
        );

        // A group that declares a seed but no unit (or one that does not
        // parse) is itself priced in `item` — that is what the declaration
        // degrades to — so its seed is on the item scale and must not cross
        // into an id that declares a different one. Comparing only against a
        // *declared* group unit missed this: the seed sailed straight in.
        let (undeclared, _dir) = registry_from(
            r#"
[group.u]
config.impl_class = "cls"
[group.u.metadata.cost]
seed_units = 8

[group.u.inference_ids.pixels]
metadata.cost.unit = "pixel"
metadata.cost.aggregation = "sum"

[group.u.inference_ids.items]
metadata.cost.unit = "item"
metadata.cost.aggregation = "count"

[group.v]
config.impl_class = "cls"
[group.v.metadata.cost]
unit = "megapixel"
seed_units = 2000000
[group.v.inference_ids.tokens]
metadata.cost.unit = "token"
metadata.cost.aggregation = "sum"
"#,
        );
        assert_eq!(
            CostDimension::resolve(&undeclared, "u/pixels").seed_units,
            Some(2_000_000),
            "8 pixels is no batch at all: the pixel-class default applies"
        );
        assert_eq!(
            CostDimension::resolve(&undeclared, "u/items").seed_units,
            Some(8),
            "same (item) scale as the group resolves to, so it inherits"
        );
        assert_eq!(
            CostDimension::resolve(&undeclared, "v/tokens").seed_units,
            Some(2_000),
            "an unparseable group unit is item-priced too, so 2M does not \
             follow the seed into a token id"
        );
    }

    fn shipped_registry() -> Registry {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../python/inferio/config");
        Registry::load(&RegistryConfig {
            config_dirs: vec![dir],
        })
        .expect("built-in registry loads")
    }

    fn input_handler(metadata: &JsonMap<String, JsonValue>) -> Option<&str> {
        metadata.get("input_spec")?.get("handler")?.as_str()
    }

    /// `pixel` prices *decoded pixels*, so it is only meaningful for a model
    /// that is actually handed images. The tclip text tower shipped as
    /// `pixel`/`sum` in the first draft of this metadata: with the
    /// `extracted_text` handler its batches decode zero pixels, so every
    /// batch would have priced at 0 units and admission would have been
    /// degenerate. Guard the taxonomy against that class of mistake.
    #[test]
    fn pixel_pricing_requires_an_image_handler() {
        let registry = shipped_registry();
        let mut checked = 0;
        for (group_name, group) in &registry.groups {
            for (id, entry) in &group.inference_ids {
                let full = format!("{group_name}/{id}");
                if CostDimension::resolve(&registry, &full).unit != CostUnit::Pixel {
                    continue;
                }
                let handler = input_handler(&entry.metadata)
                    .or_else(|| input_handler(&group.group_metadata));
                assert_eq!(
                    handler,
                    Some("image_frames"),
                    "{full} is priced per pixel but its inputs are not images \
                     (handler {handler:?}); a non-image batch decodes zero \
                     pixels and would price at zero units"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "the guard must actually cover some ids");
    }

    /// The shipped registry must classify every group and every deviating
    /// id, with no silent fallbacks. This is the regression guard for the
    /// taxonomy table in docs/batch-calibration-design.md.
    #[test]
    fn shipped_registry_is_fully_classified() {
        let registry = shipped_registry();

        let mut undeclared = Vec::new();
        for (group_name, group) in &registry.groups {
            for id in group.inference_ids.keys() {
                let full = format!("{group_name}/{id}");
                if CostDimension::resolve(&registry, &full).degraded {
                    undeclared.push(full);
                }
            }
        }
        assert!(
            undeclared.is_empty(),
            "shipped inference ids without a valid cost declaration: {undeclared:?}"
        );

        // Spot-check the classifications the design calls out explicitly.
        let expect = |id: &str, unit: CostUnit, aggregation: Option<CostAggregation>| {
            let cost = CostDimension::resolve(&registry, id);
            assert_eq!(cost.unit, unit, "{id} unit");
            assert_eq!(cost.aggregation, aggregation, "{id} aggregation");
        };
        expect(
            "tags/wd-swinv2-tagger-v3",
            CostUnit::Item,
            Some(CostAggregation::Count),
        );
        expect("tagmatch/danbooru", CostUnit::None, None);
        expect(
            "doctr/dots_ocr",
            CostUnit::Pixel,
            Some(CostAggregation::Sum),
        );
        expect(
            "doctr/easyocr_standard_en",
            CostUnit::Pixel,
            Some(CostAggregation::MaxTimesCount),
        );
        expect(
            "doctr/db_resnet50_crnn_mobilenet_v3_small",
            CostUnit::Item,
            Some(CostAggregation::Count),
        );
        expect(
            "textembed/all-mpnet-base-v2",
            CostUnit::Token,
            Some(CostAggregation::MaxTimesCount),
        );
        expect("textembed/jina-embeddings-v3-api", CostUnit::None, None);
        expect("whisper/large-v3", CostUnit::None, None);
        expect("clip/jina-clip-v2-api", CostUnit::None, None);
        expect("tclip/jina-clip-v2-api", CostUnit::None, None);
        expect(
            "clip/ViT-H-14-378-quickgelu_dfn5b",
            CostUnit::Item,
            Some(CostAggregation::Count),
        );
        expect(
            "clip/qwen3-vl-embedding-8b",
            CostUnit::Pixel,
            Some(CostAggregation::Sum),
        );
        // The tclip ids run the same engine's *text* tower: no pixels are
        // decoded, so they deviate from their clip-group twins. Unlike the
        // openclip text towers they have no fixed context — the processor
        // truncates at 8192 tokens and pads each batch to its longest member —
        // so they are the token/max-times-count class, not the group's
        // per-item one (step 5 of the design's rollout).
        expect(
            "tclip/qwen3-vl-embedding-2b",
            CostUnit::Token,
            Some(CostAggregation::MaxTimesCount),
        );
        expect(
            "tclip/qwen3-vl-embedding-8b",
            CostUnit::Token,
            Some(CostAggregation::MaxTimesCount),
        );
        // moondream's predict loops one image at a time, so no batch dimension
        // exists to price: `none`, like faster_whisper (step 5).
        expect("vlm/moondream-2b-25-03-ocr", CostUnit::None, None);
        expect("tags/moondream-2b-25-03", CostUnit::None, None);
        expect(
            "florence2/msft_large-caption",
            CostUnit::Item,
            Some(CostAggregation::Count),
        );
        expect(
            "clap/clap-htsat-unfused",
            CostUnit::Item,
            Some(CostAggregation::Count),
        );
    }
}
