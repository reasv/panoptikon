use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub(crate) use crate::pql::builder::filters::{
    DerivedDataArgs, DistanceAggregation, DistanceFunction, EmbedArgs, HasUnprocessedData,
    InBookmarks, InBookmarksArgs, Match, MatchAnd, MatchNot, MatchOps, MatchOr, MatchPath,
    MatchPathArgs, MatchTags, MatchText, MatchTextArgs, MatchValue, MatchValues, Matches,
    OneOrMany, ProcessedBy, SemanticImageArgs, SemanticImageSearch, SemanticTextArgs,
    SemanticTextSearch, SimilarTo, SimilarityArgs, SourceArgs, TagsArgs,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EntityType {
    File,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
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
    /// Smoothing Constant
    ///
    /// The smoothing constant for the RRF function.
    /// The formula is: 1 / (rank + k).
    ///
    /// Can be 0 for no smoothing.
    ///
    /// Smoothing reduces the impact of "high" ranks (close to 1) on the final rank value.
    pub k: i32,
    /// Weight
    ///
    /// The weight to apply to this filter's rank value in the RRF function.
    /// The formula is: weight * 1 / (rank + k).
    pub weight: f64,
}

impl Default for Rrf {
    fn default() -> Self {
        Self { k: 1, weight: 1.0 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct SortableOptions {
    /// Order by this filter's rank output
    ///
    /// This filter generates a value that can be used for ordering.
    #[serde(default)]
    pub order_by: bool,
    /// Order Direction
    ///
    /// The order direction for this filter.
    /// If not set, the default order direction for this field is used.
    #[serde(default = "default_direction")]
    pub direction: OrderDirection,
    /// Order By Priority
    ///
    /// The priority of this filter in the order by clause.
    /// If there are multiple filters with order_by set to True,
    /// the priority is used to determine the order.
    /// If two filter order bys have the same priority,
    /// their values are coalesced into a single column to order by,
    /// and the order direction is determined by the first filter that we find from this set.
    ///
    /// It's assumed that if the filters have the same priority, and should be coalesced,
    /// they will have the same order direction.
    #[serde(default)]
    pub priority: i32,
    /// Use Row Number for rank column
    ///
    /// Only applied if either order_by is True, or select_as is set.
    ///
    /// If True, internally sorts the filter's output by its rank_order
    /// column and assigns a row number to each row.
    ///
    /// The row number is used to order the final query.
    ///
    /// This is useful for combining multiple filters with different
    /// rank_order types that may not be directly comparable,
    /// such as text search and embeddings search.
    ///
    /// See `RRF` for a way to combine heterogeneous rank_order filters when using row_n = True.
    #[serde(default)]
    pub row_n: bool,
    /// Order Direction For Row Number
    ///
    /// The order direction (asc or desc) for the internal row number calculation.
    /// Only used if `order_by_row_n` is True.
    /// When `order_by_row_n` is True, the filter's output is sorted by its rank_order column
    /// following this direction, and a row number is assigned to each row.
    /// This row number is used to order the final query.
    /// You should generally leave this as the default value.
    #[serde(default = "default_direction")]
    pub row_n_direction: OrderDirection,
    /// Order By Greater Than
    ///
    /// If set, only include items with an order_rank greater than this value.
    /// Can be used for cursor-based pagination.
    /// The type depends on the filter.
    /// Will be ignored in the count query, which is
    /// used to determine the total number of results when count = True.
    /// With cursor-based pagination, you should probably not rely on count = True anyhow.
    #[serde(default)]
    pub gt: Option<ScalarValue>,
    /// Order By Less Than
    ///
    /// If set, only include items with an order_rank less than this value.
    /// Can be used for cursor-based pagination.
    /// The type depends on the filter.
    /// Will be ignored in the count query, which is
    /// used to determine the total number of results when count = True.
    #[serde(default)]
    pub lt: Option<ScalarValue>,
    /// Order By Select As
    ///
    /// If set, the order_rank column will be returned with the results as this alias under the "extra" object.
    #[serde(default)]
    pub select_as: Option<String>,
    /// Reciprocal Ranked Fusion Parameters
    ///
    /// Parameters for the Reciprocal Ranked Fusion.
    /// If set, when coalescing multiple filters with the same priority,
    /// the RRF function will be applied to the rank_order columns.
    ///
    /// If only one filter has RRF set, but multiple filters have the same priority,
    /// RRF will be ignored.
    ///
    /// If using RRF, you should set row_n to True for all the filters involved.
    /// Moreover, the correct direction for RRF is "desc" (higher is better).
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
    /// Order Priority
    ///
    /// The priority of this order by field. If multiple fields are ordered by,
    /// the priority is used to determine the order they are applied in.
    /// The order in the list is used if the priority is the same.
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
    /// Values to order results by
    ///
    /// The order_args field is a list of { order_by: [field name], order: ["asc" or "desc"] }
    /// objects that define how the results should be ordered.
    /// Results can be ordered by multiple fields by adding multiple objects.
    pub order_by: Vec<OrderArgs>,
    /// Data to return
    ///
    /// The columns to return in the query.
    /// The default columns are sha256, path, last_modified, and type.
    /// Columns belonging to text can only be selected if the entity is "text".
    pub select: Vec<Column>,
    /// Target Entity
    ///
    /// The entity to query on.
    /// You can perform the search on either files or text.
    /// This means that intermediate results will be one per file, or one per text-file pair.
    /// There are generally more text-file pairs than files, so this incurs overhead.
    ///
    /// However, "text" queries allow you to include text-specific columns in the select list.
    /// The final results will also be one for each text-file pair.
    ///
    /// Most of the same filters can be used on both.
    /// "text" queries will include "data_id" in each result. "file_id" and "item_id" are always included.
    pub entity: EntityType,
    /// Partition results By
    ///
    /// Group results by the values of the specified column(s) and return the first result
    /// for each group according to all of the order settings of the query.
    ///
    /// For example, if you partition by "item_id", you'll get one result per unique item.
    /// If you partition by "file_id", you'll get one result per unique file.
    /// Multiple columns yield one result for each unique combination of values for those columns.
    ///
    /// You cannot partition by text columns if the entity is "file".
    pub partition_by: Option<Vec<Column>>,
    pub page: i64,
    pub page_size: i64,
    /// Count Results
    ///
    /// If true, the query will return the total number of results that match the query.
    /// This is useful for pagination, but it requires an additional query to be executed.
    pub count: bool,
    /// Return Results
    ///
    /// If true, the query will return the results that match the query.
    /// If false, only the total count will be returned, if requested.
    pub results: bool,
    /// Check Paths Exist
    ///
    /// If true, the query will check if the path exists on disk before returning it.
    ///
    /// For `file` queries with no partition by,
    /// the result will be omitted if the path does not exist.
    /// This is because if another file exists, it will be included later in the results.
    ///
    /// In other cases, the system will try to find another file for the item and substitute it.
    /// If no other working path is found, the result will be omitted.
    ///
    /// This is not reflected in the total count of results.
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
    #[serde(alias = "and")]
    pub and_: Vec<QueryElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct OrOperator {
    #[serde(alias = "or")]
    pub or_: Vec<QueryElement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct NotOperator {
    #[serde(alias = "not")]
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
    SemanticTextSearch(SemanticTextSearch),
    SemanticImageSearch(SemanticImageSearch),
    SimilarTo(SimilarTo),
    MatchTags(MatchTags),
    InBookmarks(InBookmarks),
    ProcessedBy(ProcessedBy),
    HasUnprocessedData(HasUnprocessedData),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct JobFilter {
    #[serde(default)]
    pub setter_names: Vec<String>,
    pub pql_query: QueryElement,
}

fn default_direction() -> OrderDirection {
    OrderDirection::Asc
}

fn default_order_by_field() -> OrderByField {
    OrderByField::LastModified
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
