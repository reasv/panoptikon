use utoipa::Modify;
use utoipa::OpenApi;
use utoipa::openapi::schema::{ObjectBuilder, Schema, SchemaType};

struct JsonValueSchema;

impl Modify for JsonValueSchema {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.schemas.insert(
            "Value".to_string(),
            Schema::Object(
                ObjectBuilder::new()
                    .schema_type(SchemaType::AnyValue)
                    .build(),
            )
            .into(),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::search::search_pql,
        crate::api::search::search_pql_build,
        crate::api::search::get_search_cache,
        crate::api::search::clear_search_cache,
        crate::api::search::get_tags,
        crate::api::search::get_top_tags,
        crate::api::search::get_stats,
        crate::api::items::item_meta,
        crate::api::items::item_file,
        crate::api::items::item_thumbnail,
        crate::api::items::item_text,
        crate::api::items::item_tags,
        crate::api::items::texts_any,
        crate::api::open::open_file_on_host,
        crate::api::open::show_in_file_manager,
        crate::api::jobs::queue_status,
        crate::api::jobs::enqueue_data_extraction,
        crate::api::jobs::enqueue_delete_extracted_data,
        crate::api::jobs::enqueue_folder_rescan,
        crate::api::jobs::enqueue_update_folders,
        crate::api::jobs::cancel_queued,
        crate::api::jobs::cancel_current_job,
        crate::api::jobs::get_folders,
        crate::api::jobs::get_scan_history,
        crate::api::jobs::delete_scan_data,
        crate::api::jobs::get_extraction_history,
        crate::api::jobs::update_config,
        crate::api::jobs::get_config,
        crate::api::jobs::get_setter_data_count,
        crate::api::jobs::manual_trigger_cronjob,
        crate::api::jobs::get_cronjob_schedule,
        crate::api::jobs::get_continuous_scan_status,
        crate::api::bookmarks::bookmark_namespaces,
        crate::api::bookmarks::bookmark_users,
        crate::api::bookmarks::bookmarks_by_namespace,
        crate::api::bookmarks::delete_bookmarks_by_namespace,
        crate::api::bookmarks::add_bookmarks_by_namespace,
        crate::api::bookmarks::get_bookmark,
        crate::api::bookmarks::add_bookmark_by_sha256,
        crate::api::bookmarks::delete_bookmark_by_sha256,
        crate::api::bookmarks::bookmarks_item,
        crate::api::pinboards::list_pinboards,
        crate::api::pinboards::create_pinboard,
        crate::api::pinboards::get_pinboard,
        crate::api::pinboards::update_pinboard,
        crate::api::pinboards::delete_pinboard,
        crate::api::pinboards::list_pinboard_versions,
        crate::api::pinboards::save_pinboard_version,
        crate::api::pinboards::delete_pinboard_version,
        crate::api::pinboards::pinboard_version_preview,
        crate::api::db::db_info,
        crate::api::db::db_create,
        crate::api::client_config::client_config,
        crate::api::desktop::setup_status,
        crate::api::desktop::validate_setup_folders,
        crate::api::desktop::validate_setup_continuous_folders,
        crate::api::desktop::preview_setup_schedule,
        crate::api::desktop::complete_setup,
        crate::api::desktop::external_inputs,
        crate::api::desktop::update_external_inputs,
        crate::api::desktop::reveal_external_input,
        crate::api::desktop::update_status,
        crate::api::desktop::open_update_window,
        crate::api::desktop::snooze_update_ribbon,
        crate::api::desktop::dismiss_update_ribbon
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
            crate::api::items::ItemMetadataResponse,
            crate::api::items::ItemRecordResponse,
            crate::api::items::FileRecordResponse,
            crate::api::items::TextResponse,
            crate::api::items::TagResponse,
            crate::api::open::OpenResponse,
            crate::api::jobs::QueueCancelResponse,
            crate::api::jobs::CancelResponse,
            crate::api::jobs::FoldersResponse,
            crate::api::jobs::SetterDataStats,
            crate::api::jobs::CronJobResponse,
            crate::api::jobs::CronScheduleResponse,
            crate::api::jobs::ContinuousScanMode,
            crate::api::jobs::ContinuousScanStatusResponse,
            crate::jobs::queue::JobModel,
            crate::jobs::queue::JobOutcomeModel,
            crate::jobs::queue::JobOutcomeStatus,
            crate::jobs::queue::QueueStatusModel,
            crate::jobs::queue::JobType,
            crate::db::file_scans::FileScanRecord,
            crate::db::extraction_log::LogRecord,
            crate::db::system_config::SystemConfig,
            crate::db::system_config::CronJob,
            crate::db::system_config::JobSettings,
            crate::db::items::ExtractedTextRecord,
            crate::db::items::ItemIdentifierType,
            crate::api::bookmarks::BookmarkNamespaces,
            crate::api::bookmarks::BookmarkUsers,
            crate::api::bookmarks::Results,
            crate::api::bookmarks::FileSearchResult,
            crate::api::bookmarks::ExistingBookmarkMetadata,
            crate::api::bookmarks::ItemBookmarks,
            crate::api::bookmarks::BookmarkMetadata,
            crate::api::bookmarks::MessageResult,
            crate::api::bookmarks::Items,
            crate::api::bookmarks::ItemsMeta,
            crate::api::bookmarks::BookmarkOrderBy,
            crate::api::bookmarks::SortOrder,
            crate::api::pinboards::SaveVersionRequest,
            crate::api::pinboards::CreatePinboardRequest,
            crate::api::pinboards::RenamePinboardRequest,
            crate::api::pinboards::SavePinboardResponse,
            crate::api::pinboards::PinboardSummaryResponse,
            crate::api::pinboards::PinboardVersionResponse,
            crate::api::pinboards::PinboardListResponse,
            crate::api::pinboards::PinboardDetailResponse,
            crate::api::pinboards::PinboardVersionsResponse,
            crate::api::pinboards::PinboardDeleteResponse,
            crate::policy::DbInfo,
            crate::policy::SingleDbInfo,
            crate::api::db::DbCreateResponse,
            crate::api::client_config::ClientConfigResponse,
            crate::api::client_config::ClientCapabilities,
            crate::api::desktop::DesktopSetupStatus,
            crate::api::desktop::DesktopFolderSelection,
            crate::api::desktop::DesktopContinuousScanSelection,
            crate::api::desktop::DesktopSchedulePreviewRequest,
            crate::api::desktop::DesktopSchedulePreviewResponse,
            crate::api::desktop::DesktopSetupCompleteRequest,
            crate::api::desktop::DesktopSetupCompleteResponse,
            crate::api::desktop::DesktopExternalInputUpdate,
            crate::api::desktop::DesktopUpdateDismissRequest,
            crate::api::desktop::DesktopUpdateSnoozeRequest,
            crate::db::setup::FolderValidation,
            crate::db::setup::FolderValidationIssue,
            crate::pql::EmbeddingCacheEntry,
            crate::pql::EmbeddingCacheStats,
            crate::pql::model::PqlQuery,
            crate::pql::model::QueryElement,
            crate::pql::model::AndOperator,
            crate::pql::model::OrOperator,
            crate::pql::model::NotOperator,
            crate::pql::model::EntityType,
            crate::pql::model::Column,
            crate::pql::model::OrderByField,
            crate::pql::model::OrderDirection,
            crate::pql::model::ScalarValue,
            crate::pql::model::SortableOptions,
            crate::pql::model::OrderArgs,
            crate::pql::model::Rrf,
            crate::pql::model::Match,
            crate::pql::model::MatchAnd,
            crate::pql::model::MatchOr,
            crate::pql::model::MatchNot,
            crate::pql::model::MatchOps,
            crate::pql::model::MatchValue,
            crate::pql::model::MatchValues,
            crate::pql::model::Matches,
            crate::pql::model::MatchPath,
            crate::pql::model::MatchPathArgs,
            crate::pql::model::MatchText,
            crate::pql::model::MatchTextArgs,
            crate::pql::model::MatchTags,
            crate::pql::model::TagsArgs,
            crate::pql::model::InBookmarks,
            crate::pql::model::InBookmarksArgs,
            crate::pql::model::ProcessedBy,
            crate::pql::model::HasUnprocessedData,
            crate::pql::model::DerivedDataArgs,
            crate::pql::model::SemanticTextSearch,
            crate::pql::model::SemanticTextArgs,
            crate::pql::model::SemanticImageSearch,
            crate::pql::model::SemanticImageArgs,
            crate::pql::model::SimilarTo,
            crate::pql::model::SimilarityArgs,
            crate::pql::model::SourceArgs,
            crate::pql::model::DistanceAggregation,
            crate::pql::model::DistanceFunction,
            crate::pql::model::EmbedArgs
        )
    ),
    nest(
        (path = "/api/inference", api = crate::inferio::http::InferioApiDoc)
    ),
    tags(
        (name = "search", description = "Search and PQL endpoints"),
        (name = "items"),
        (name = "open"),
        (name = "jobs"),
        (name = "bookmarks"),
        (name = "pinboards", description = "Saved pinboard arrangements with version history"),
        (name = "database"),
        (name = "client", description = "Per-policy client configuration and derived capabilities"),
        (name = "inference", description = "Model inference service (served locally or proxied upstream — same contract either way)")
    ),
    modifiers(&JsonValueSchema)
)]
#[allow(dead_code)]
pub struct ApiDoc;

// The generated spec is a public contract: the UI's `lib/panoptikon.d.ts` is
// generated from it, so any drift is a (potentially breaking) client change.
// These tests pin the contract two ways: structural invariants with useful
// failure messages, and byte-equality against the committed `openapi.json`
// fixture at the crate root (regenerate deliberately with
// `UPDATE_OPENAPI_FIXTURE=1 cargo test openapi`).
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashSet;

    fn spec() -> Value {
        serde_json::to_value(ApiDoc::openapi()).expect("spec serializes")
    }

    const METHODS: [&str; 7] = ["get", "post", "put", "delete", "patch", "head", "options"];

    /// Every operation carries an explicit, unique operationId — generated
    /// clients key their types on these names, so a Rust fn rename must
    /// never silently rename one.
    #[test]
    fn operation_ids_are_present_and_unique() {
        let spec = spec();
        let mut seen: HashSet<String> = HashSet::new();
        for (path, item) in spec["paths"].as_object().expect("paths object") {
            for (method, op) in item.as_object().expect("path item object") {
                if !METHODS.contains(&method.as_str()) {
                    continue;
                }
                let id = op["operationId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} has no operationId"));
                assert!(
                    seen.insert(id.to_string()),
                    "duplicate operationId `{id}` at {method} {path}"
                );
            }
        }
        assert!(!seen.is_empty(), "spec has no operations");
    }

    fn collect_refs(value: &Value, refs: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    if key == "$ref" {
                        if let Some(target) = child.as_str() {
                            refs.push(target.to_string());
                        }
                    }
                    collect_refs(child, refs);
                }
            }
            Value::Array(items) => {
                for child in items {
                    collect_refs(child, refs);
                }
            }
            _ => {}
        }
    }

    /// Every `$ref` in the document resolves to a registered component.
    /// utoipa does NOT verify this: a `body = T` whose `T` was never listed
    /// in `components(schemas(...))` produces a dangling reference, and a
    /// duplicate component name silently overwrites rather than erroring.
    #[test]
    fn all_schema_refs_resolve() {
        let spec = spec();
        let schemas = spec["components"]["schemas"]
            .as_object()
            .expect("components.schemas object");
        let mut refs = Vec::new();
        collect_refs(&spec, &mut refs);
        assert!(!refs.is_empty());
        for target in refs {
            let name = target
                .strip_prefix("#/components/schemas/")
                .unwrap_or_else(|| panic!("non-schema or external $ref: {target}"));
            assert!(
                schemas.contains_key(name),
                "dangling $ref to unregistered schema `{name}`"
            );
        }
    }

    /// The shared DB-selection query params are optional AND nullable
    /// (`type: ["string", "null"]`): clients that model "no selection" as
    /// an explicit null (the UI does) must type-check against the spec.
    #[test]
    fn db_selection_params_are_nullable() {
        let spec = spec();
        let params = spec["paths"]["/api/items/item"]["get"]["parameters"]
            .as_array()
            .expect("item_meta parameters");
        for wanted in ["index_db", "user_data_db"] {
            let param = params
                .iter()
                .find(|param| param["name"] == wanted)
                .unwrap_or_else(|| panic!("{wanted} param missing"));
            assert_eq!(param["required"], Value::Bool(false), "{wanted} required");
            let types = param["schema"]["type"].as_array().unwrap_or_else(|| {
                panic!("{wanted} should have a type array, got {}", param["schema"])
            });
            assert!(
                types.contains(&Value::String("null".into())),
                "{wanted} not nullable: {types:?}"
            );
        }
    }

    /// The inference surface is documented (nested subdocument), including
    /// the gateway-only /health addition.
    #[test]
    fn inference_paths_are_documented() {
        let spec = spec();
        let paths = spec["paths"].as_object().expect("paths object");
        for wanted in [
            "/api/inference/predict/{group}/{inference_id}",
            "/api/inference/load/{group}/{inference_id}",
            "/api/inference/cache/{cache_key}/{group}/{inference_id}",
            "/api/inference/cache/{cache_key}",
            "/api/inference/cache",
            "/api/inference/metadata",
            "/api/inference/external-inputs",
            "/api/inference/health",
        ] {
            assert!(
                paths.contains_key(wanted),
                "missing inference path {wanted}"
            );
        }
    }

    /// Byte-for-byte comparison against the committed fixture, which is
    /// also the input for regenerating the UI's `lib/panoptikon.d.ts`.
    /// On intentional spec changes: `UPDATE_OPENAPI_FIXTURE=1 cargo test
    /// openapi`, commit the new fixture, and regenerate the UI types.
    #[test]
    fn spec_matches_committed_fixture() {
        let rendered = serde_json::to_string_pretty(&spec()).expect("spec serializes") + "\n";
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
        if std::env::var_os("UPDATE_OPENAPI_FIXTURE").is_some() {
            std::fs::write(&path, &rendered).expect("write fixture");
            return;
        }
        let committed = std::fs::read_to_string(&path)
            .expect("openapi.json fixture missing — generate it with UPDATE_OPENAPI_FIXTURE=1 cargo test openapi")
            .replace("\r\n", "\n");
        if committed != rendered {
            let diverged = committed
                .lines()
                .zip(rendered.lines())
                .position(|(a, b)| a != b)
                .unwrap_or_else(|| committed.lines().count().min(rendered.lines().count()));
            let context: Vec<&str> = rendered
                .lines()
                .skip(diverged.saturating_sub(2))
                .take(5)
                .collect();
            panic!(
                "OpenAPI spec drifted from the committed openapi.json fixture \
                 (first difference near line {}):\n{}\n\nIf the change is \
                 intentional, regenerate with UPDATE_OPENAPI_FIXTURE=1 cargo \
                 test openapi, commit the fixture, and regenerate the UI types.",
                diverged + 1,
                context.join("\n")
            );
        }
    }
}
