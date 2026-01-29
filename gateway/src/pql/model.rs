use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EntityType {
    File,
    Text,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Column {
    FileId,
    Sha256,
    Path,
    Filename,
    LastModified,
    ItemId,
    Md5,
    Type,
    Size,
    Width,
    Height,
    Duration,
    TimeAdded,
    AudioTracks,
    VideoTracks,
    SubtitleTracks,
    Blurhash,
    DataId,
    Language,
    LanguageConfidence,
    Text,
    Confidence,
    TextLength,
    JobId,
    SetterId,
    SetterName,
    DataIndex,
    SourceId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OrderByField {
    FileId,
    Sha256,
    Path,
    Filename,
    LastModified,
    ItemId,
    Md5,
    Type,
    Size,
    Width,
    Height,
    Duration,
    TimeAdded,
    AudioTracks,
    VideoTracks,
    SubtitleTracks,
    Blurhash,
    DataId,
    Language,
    LanguageConfidence,
    Text,
    Confidence,
    TextLength,
    JobId,
    SetterId,
    SetterName,
    DataIndex,
    SourceId,
    Random,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum ScalarValue {
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct Rrf {
    pub k: i32,
    pub weight: f64,
}

impl Default for Rrf {
    fn default() -> Self {
        Self { k: 1, weight: 1.0 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SortableOptions {
    #[serde(default)]
    pub order_by: bool,
    #[serde(default = "default_direction")]
    pub direction: OrderDirection,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub row_n: bool,
    #[serde(default = "default_direction")]
    pub row_n_direction: OrderDirection,
    #[serde(default)]
    pub gt: Option<ScalarValue>,
    #[serde(default)]
    pub lt: Option<ScalarValue>,
    #[serde(default)]
    pub select_as: Option<String>,
    #[serde(default)]
    pub rrf: Option<Rrf>,
}

impl Default for SortableOptions {
    fn default() -> Self {
        Self {
            order_by: false,
            direction: OrderDirection::Asc,
            priority: 0,
            row_n: false,
            row_n_direction: OrderDirection::Asc,
            gt: None,
            lt: None,
            select_as: None,
            rrf: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct OrderArgs {
    #[serde(default = "default_order_by_field")]
    pub order_by: OrderByField,
    #[serde(default)]
    pub order: Option<OrderDirection>,
    #[serde(default)]
    pub priority: i32,
}

impl Default for OrderArgs {
    fn default() -> Self {
        Self {
            order_by: OrderByField::LastModified,
            order: None,
            priority: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub(crate) struct PqlQuery {
    pub query: Option<QueryElement>,
    pub order_by: Vec<OrderArgs>,
    pub select: Vec<Column>,
    pub entity: EntityType,
    pub partition_by: Option<Vec<Column>>,
    pub page: i64,
    pub page_size: i64,
    pub count: bool,
    pub results: bool,
    pub check_path: bool,
}

impl Default for PqlQuery {
    fn default() -> Self {
        Self {
            query: None,
            order_by: default_order_args(),
            select: default_select_fields(),
            entity: EntityType::File,
            partition_by: None,
            page: 1,
            page_size: 10,
            count: true,
            results: true,
            check_path: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct AndOperator {
    pub and_: Vec<QueryElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct OrOperator {
    pub or_: Vec<QueryElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct NotOperator {
    pub not_: Box<QueryElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum QueryElement {
    And(AndOperator),
    Or(OrOperator),
    Not(NotOperator),
    Match(Match),
    MatchPath(MatchPath),
    MatchText(MatchText),
    MatchTags(MatchTags),
    InBookmarks(InBookmarks),
    ProcessedBy(ProcessedBy),
    HasUnprocessedData(HasUnprocessedData),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchValues {
    #[serde(default)]
    pub file_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub item_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub path: Option<OneOrMany<String>>,
    #[serde(default)]
    pub filename: Option<OneOrMany<String>>,
    #[serde(default)]
    pub sha256: Option<OneOrMany<String>>,
    #[serde(default)]
    pub last_modified: Option<OneOrMany<String>>,
    #[serde(default)]
    pub r#type: Option<OneOrMany<String>>,
    #[serde(default)]
    pub size: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub width: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub height: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub duration: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub time_added: Option<OneOrMany<String>>,
    #[serde(default)]
    pub md5: Option<OneOrMany<String>>,
    #[serde(default)]
    pub audio_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub video_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub subtitle_tracks: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub blurhash: Option<OneOrMany<String>>,
    #[serde(default)]
    pub data_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub language: Option<OneOrMany<String>>,
    #[serde(default)]
    pub language_confidence: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub text: Option<OneOrMany<String>>,
    #[serde(default)]
    pub confidence: Option<OneOrMany<f64>>,
    #[serde(default)]
    pub text_length: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub job_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub setter_id: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub setter_name: Option<OneOrMany<String>>,
    #[serde(default)]
    pub data_index: Option<OneOrMany<i64>>,
    #[serde(default)]
    pub source_id: Option<OneOrMany<i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchValue {
    #[serde(default)]
    pub file_id: Option<i64>,
    #[serde(default)]
    pub item_id: Option<i64>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub width: Option<i64>,
    #[serde(default)]
    pub height: Option<i64>,
    #[serde(default)]
    pub duration: Option<f64>,
    #[serde(default)]
    pub time_added: Option<String>,
    #[serde(default)]
    pub md5: Option<String>,
    #[serde(default)]
    pub audio_tracks: Option<i64>,
    #[serde(default)]
    pub video_tracks: Option<i64>,
    #[serde(default)]
    pub subtitle_tracks: Option<i64>,
    #[serde(default)]
    pub blurhash: Option<String>,
    #[serde(default)]
    pub data_id: Option<i64>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub language_confidence: Option<f64>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub text_length: Option<i64>,
    #[serde(default)]
    pub job_id: Option<i64>,
    #[serde(default)]
    pub setter_id: Option<i64>,
    #[serde(default)]
    pub setter_name: Option<String>,
    #[serde(default)]
    pub data_index: Option<i64>,
    #[serde(default)]
    pub source_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchOps {
    #[serde(default)]
    pub eq: Option<MatchValue>,
    #[serde(default)]
    pub neq: Option<MatchValue>,
    #[serde(rename = "in_", default)]
    pub in_: Option<MatchValues>,
    #[serde(default)]
    pub nin: Option<MatchValues>,
    #[serde(default)]
    pub gt: Option<MatchValue>,
    #[serde(default)]
    pub gte: Option<MatchValue>,
    #[serde(default)]
    pub lt: Option<MatchValue>,
    #[serde(default)]
    pub lte: Option<MatchValue>,
    #[serde(default)]
    pub startswith: Option<MatchValues>,
    #[serde(default)]
    pub not_startswith: Option<MatchValues>,
    #[serde(default)]
    pub endswith: Option<MatchValues>,
    #[serde(default)]
    pub not_endswith: Option<MatchValues>,
    #[serde(default)]
    pub contains: Option<MatchValues>,
    #[serde(default)]
    pub not_contains: Option<MatchValues>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchAnd {
    pub and_: Vec<MatchOps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchOr {
    pub or_: Vec<MatchOps>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchNot {
    pub not_: MatchOps,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum Matches {
    Ops(MatchOps),
    And(MatchAnd),
    Or(MatchOr),
    Not(MatchNot),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct Match {
    #[serde(rename = "match")]
    pub match_: Matches,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchPathArgs {
    pub r#match: String,
    #[serde(default)]
    pub filename_only: bool,
    #[serde(default = "default_true")]
    pub raw_fts5_match: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchPath {
    #[serde(flatten, default)]
    pub sort: SortableOptions,
    pub match_path: MatchPathArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchTextArgs {
    pub r#match: String,
    #[serde(default)]
    pub filter_only: bool,
    #[serde(default)]
    pub setters: Vec<String>,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub min_language_confidence: Option<f64>,
    #[serde(default)]
    pub min_confidence: Option<f64>,
    #[serde(default = "default_true")]
    pub raw_fts5_match: bool,
    #[serde(default)]
    pub min_length: Option<i64>,
    #[serde(default)]
    pub max_length: Option<i64>,
    #[serde(default)]
    pub select_snippet_as: Option<String>,
    #[serde(default = "default_snippet_max_len")]
    pub s_max_len: i64,
    #[serde(default = "default_snippet_ellipsis")]
    pub s_ellipsis: String,
    #[serde(default = "default_snippet_start_tag")]
    pub s_start_tag: String,
    #[serde(default = "default_snippet_end_tag")]
    pub s_end_tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchText {
    #[serde(flatten, default)]
    pub sort: SortableOptions,
    pub match_text: MatchTextArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct TagsArgs {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub match_any: bool,
    #[serde(default)]
    pub min_confidence: f64,
    #[serde(default)]
    pub setters: Vec<String>,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub all_setters_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct MatchTags {
    #[serde(flatten, default = "default_sort_desc")]
    pub sort: SortableOptions,
    pub match_tags: TagsArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct InBookmarksArgs {
    #[serde(default = "default_true")]
    pub filter: bool,
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default)]
    pub sub_ns: bool,
    #[serde(default = "default_bookmarks_user")]
    pub user: String,
    #[serde(default = "default_true")]
    pub include_wildcard: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct InBookmarks {
    #[serde(flatten, default = "default_sort_desc")]
    pub sort: SortableOptions,
    pub in_bookmarks: InBookmarksArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct ProcessedBy {
    pub processed_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct DerivedDataArgs {
    pub setter_name: String,
    pub data_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct HasUnprocessedData {
    pub has_data_unprocessed: DerivedDataArgs,
}

fn default_direction() -> OrderDirection {
    OrderDirection::Asc
}

fn default_order_by_field() -> OrderByField {
    OrderByField::LastModified
}

fn default_sort_desc() -> SortableOptions {
    let mut options = SortableOptions::default();
    options.direction = OrderDirection::Desc;
    options.row_n_direction = OrderDirection::Desc;
    options
}

fn default_order_args() -> Vec<OrderArgs> {
    vec![OrderArgs {
        order_by: OrderByField::LastModified,
        order: Some(OrderDirection::Desc),
        priority: 0,
    }]
}

fn default_select_fields() -> Vec<Column> {
    vec![
        Column::Sha256,
        Column::Path,
        Column::LastModified,
        Column::Type,
    ]
}

fn default_true() -> bool {
    true
}

fn default_bookmarks_user() -> String {
    "user".to_string()
}

fn default_snippet_max_len() -> i64 {
    30
}

fn default_snippet_ellipsis() -> String {
    "...".to_string()
}

fn default_snippet_start_tag() -> String {
    "<b>".to_string()
}

fn default_snippet_end_tag() -> String {
    "</b>".to_string()
}
