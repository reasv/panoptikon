pub(crate) mod builder;
pub(crate) mod embedding_utils;
#[cfg(test)]
mod explain_plan;
#[cfg(test)]
mod fts_probe;
pub(crate) mod model;
pub(crate) mod preprocess;
#[cfg(test)]
mod quant_ab;
pub(crate) mod utils;

pub(crate) use builder::{Pagination, PqlBuilderResult, build_query, build_query_preprocessed};
// The item-set build lands with its endpoint (stage 2 of
// docs/pinboard-content-search-design.md); nothing outside the builder's own
// tests reads it yet.
#[allow(unused_imports)]
pub(crate) use builder::{ItemSetBuild, PrimaryOrderKey, build_item_set_preprocessed};
pub(crate) use preprocess::{
    EmbeddingCacheEntry, EmbeddingCacheStats, PqlError, clear_embedding_cache,
    embedding_cache_stats, preprocess_query_async,
};
