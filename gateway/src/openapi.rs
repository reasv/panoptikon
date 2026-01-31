use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::search::search_pql,
        crate::api::search::search_pql_build,
        crate::api::search::get_search_cache,
        crate::api::search::clear_search_cache,
        crate::api::search::get_tags,
        crate::api::search::get_top_tags,
        crate::api::search::get_stats
    ),
    components(
        schemas(
            crate::api::search::SearchMetrics,
            crate::api::search::CompiledQuery,
            crate::api::search::PqlBuildResponse,
            crate::api::search::SearchResult,
            crate::api::search::FileSearchResponse,
            crate::api::search::TagSearchResults,
            crate::api::search::TagFrequency,
            crate::api::search::TagStats,
            crate::api::search::FileStats,
            crate::api::search::ExtractedTextStats,
            crate::api::search::SearchStats,
            crate::pql::EmbeddingCacheEntry,
            crate::pql::EmbeddingCacheStats,
            crate::pql::model::PqlQuery
        )
    ),
    tags(
        (name = "search", description = "Search and PQL endpoints")
    )
)]
#[allow(dead_code)]
pub struct ApiDoc;

