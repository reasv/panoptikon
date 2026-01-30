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
