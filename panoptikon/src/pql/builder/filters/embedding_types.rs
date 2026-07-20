use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
pub(crate) enum DistanceAggregation {
    #[serde(rename = "MIN")]
    Min,
    #[serde(rename = "MAX")]
    Max,
    #[serde(rename = "AVG")]
    Avg,
}

impl Default for DistanceAggregation {
    fn default() -> Self {
        Self::Min
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub(crate) enum DistanceFunction {
    #[serde(rename = "L2")]
    L2,
    #[serde(rename = "COSINE")]
    Cosine,
}

impl DistanceFunction {
    pub(crate) fn from_override(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "l2" => Some(Self::L2),
            "cosine" => Some(Self::Cosine),
            _ => None,
        }
    }
}

impl Default for DistanceFunction {
    fn default() -> Self {
        Self::L2
    }
}

/// Index mode for vector filters (docs/vector-index-design.md).
///
/// `auto` resolves to the default quant profile where its coverage is ready
/// for the queried setter(s), else exact. `exact` always brute-forces
/// full-precision vectors. `quant` demands a quant profile (the `variant`
/// or the default) and errors when it isn't ready. `ann` is reserved.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum IndexMode {
    #[default]
    Auto,
    Exact,
    Quant,
    Ann,
}

/// The default exactness horizon: the coarse-top-k candidates re-scored with
/// full-precision distances. A quality floor — page geometry only ever
/// raises it (client policy), never shrinks it.
pub(crate) fn default_k() -> i64 {
    10_000
}

/// Quant resolution computed at preprocess time: the profile to join quants
/// from and (for query-embedding filters) the query vector centered against
/// the pair's artifact and binarized — by the same SQL functions the write
/// path uses, so bit order is definitionally consistent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QuantResolved {
    pub profile_id: i64,
    pub query_quant: Option<Vec<u8>>,
}
