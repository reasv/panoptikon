use crate::inferio_client::{InferenceApiClient, InferenceInput, PredictOutput};
use crate::pql::embedding_utils::{embedding_from_npy_bytes, extract_embeddings, serialize_f32};
use crate::pql::model::{
    DistanceFunction, EmbedArgs, HasUnprocessedData, InBookmarks, Match, MatchAnd, MatchOps,
    MatchOr, MatchPath, MatchTags, MatchText, MatchValue, MatchValues, Matches, ProcessedBy,
    QueryElement, SemanticImageSearch, SemanticTextSearch, SimilarTo,
};
use crate::pql::utils::parse_and_escape_query;
use base64::{Engine as _, engine::general_purpose};
use hashlink::LruCache;
use serde::Serialize;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use utoipa::ToSchema;

#[derive(Debug)]
pub(crate) struct PqlError {
    pub message: String,
}

impl PqlError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for PqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PqlError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EmbeddingKind {
    Text,
    Image,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EmbeddingCacheKey {
    model: String,
    kind: EmbeddingKind,
    query: String,
}

#[derive(Debug, Clone)]
struct CachedEmbedding {
    value: Vec<u8>,
}

static EMBEDDING_CACHE: OnceLock<Mutex<LruCache<EmbeddingCacheKey, CachedEmbedding>>> =
    OnceLock::new();

fn embedding_cache() -> &'static Mutex<LruCache<EmbeddingCacheKey, CachedEmbedding>> {
    EMBEDDING_CACHE.get_or_init(|| Mutex::new(LruCache::new(1)))
}

fn ensure_cache_capacity(cache: &mut LruCache<EmbeddingCacheKey, CachedEmbedding>, capacity: usize) {
    let target = capacity.max(1);
    if cache.capacity() != target {
        cache.set_capacity(target);
    }
}

async fn get_cached_embedding(key: &EmbeddingCacheKey, capacity: usize) -> Option<Vec<u8>> {
    let cache = embedding_cache();
    let mut guard = cache.lock().await;
    ensure_cache_capacity(&mut guard, capacity);
    if let Some(entry) = guard.get(key) {
        return Some(entry.value.clone());
    }
    guard.remove(key);
    None
}

async fn put_cached_embedding(key: EmbeddingCacheKey, capacity: usize, value: Vec<u8>) {
    let cache = embedding_cache();
    let mut guard = cache.lock().await;
    ensure_cache_capacity(&mut guard, capacity);
    guard.insert(key, CachedEmbedding { value });
}

async fn cached_embedding_or_fetch<F>(
    key: EmbeddingCacheKey,
    cache_size: usize,
    fetch: F,
) -> Result<Vec<u8>, PqlError>
where
    F: Future<Output = Result<Vec<u8>, PqlError>>,
{
    if cache_size == 0 {
        return fetch.await;
    }
    if let Some(value) = get_cached_embedding(&key, cache_size).await {
        return Ok(value);
    }
    let value = fetch.await?;
    put_cached_embedding(key, cache_size, value.clone()).await;
    Ok(value)
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbeddingCacheEntry {
    pub inference_id: String,
    pub kind: String,
    pub size: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbeddingCacheStats {
    pub used_slots: usize,
    pub total_slots: usize,
    pub page: usize,
    pub page_size: usize,
    pub entries: Vec<EmbeddingCacheEntry>,
}

pub(crate) async fn clear_embedding_cache(cache_size: usize) {
    if cache_size == 0 {
        return;
    }
    let cache = embedding_cache();
    let mut guard = cache.lock().await;
    ensure_cache_capacity(&mut guard, cache_size);
    guard.clear();
}

pub(crate) async fn embedding_cache_stats(
    cache_size: usize,
    page: usize,
    page_size: usize,
) -> EmbeddingCacheStats {
    if cache_size == 0 {
        return EmbeddingCacheStats {
            used_slots: 0,
            total_slots: 0,
            page,
            page_size,
            entries: Vec::new(),
        };
    }

    let cache = embedding_cache();
    let mut guard = cache.lock().await;
    ensure_cache_capacity(&mut guard, cache_size);
    let used_slots = guard.len();
    let total_slots = guard.capacity();

    let mut entries: Vec<EmbeddingCacheEntry> = guard
        .iter()
        .map(|(key, value)| EmbeddingCacheEntry {
            inference_id: key.model.clone(),
            kind: match key.kind {
                EmbeddingKind::Text => "text".to_string(),
                EmbeddingKind::Image => "image".to_string(),
            },
            size: value.value.len(),
        })
        .collect();
    entries.reverse();

    let page = page.max(1);
    let page_size = page_size.max(1);
    let start = (page - 1).saturating_mul(page_size);
    let entries = entries
        .into_iter()
        .skip(start)
        .take(page_size)
        .collect();

    EmbeddingCacheStats {
        used_slots,
        total_slots,
        page,
        page_size,
        entries,
    }
}

pub(crate) fn preprocess_query(el: QueryElement) -> Result<Option<QueryElement>, PqlError> {
    match el {
        QueryElement::And(mut op) => {
            let mut cleaned = Vec::new();
            for sub_element in op.and_ {
                if let Some(subquery) = preprocess_query(sub_element)? {
                    cleaned.push(subquery);
                }
            }
            if cleaned.is_empty() {
                Ok(None)
            } else if cleaned.len() == 1 {
                Ok(Some(cleaned.remove(0)))
            } else {
                op.and_ = cleaned;
                Ok(Some(QueryElement::And(op)))
            }
        }
        QueryElement::Or(mut op) => {
            let mut cleaned = Vec::new();
            for sub_element in op.or_ {
                if let Some(subquery) = preprocess_query(sub_element)? {
                    cleaned.push(subquery);
                }
            }
            if cleaned.is_empty() {
                Ok(None)
            } else if cleaned.len() == 1 {
                Ok(Some(cleaned.remove(0)))
            } else {
                op.or_ = cleaned;
                Ok(Some(QueryElement::Or(op)))
            }
        }
        QueryElement::Not(mut op) => {
            if let Some(subquery) = preprocess_query(*op.not_)? {
                op.not_ = Box::new(subquery);
                Ok(Some(QueryElement::Not(op)))
            } else {
                Ok(None)
            }
        }
        QueryElement::Match(filter) => Ok(filter.validate().map(QueryElement::Match)),
        QueryElement::MatchPath(filter) => Ok(filter.validate().map(QueryElement::MatchPath)),
        QueryElement::MatchText(filter) => Ok(filter.validate().map(QueryElement::MatchText)),
        QueryElement::SemanticTextSearch(filter) => filter
            .validate_sync()
            .map(|value| value.map(QueryElement::SemanticTextSearch)),
        QueryElement::SemanticImageSearch(filter) => filter
            .validate_sync()
            .map(|value| value.map(QueryElement::SemanticImageSearch)),
        QueryElement::SimilarTo(filter) => filter
            .validate_sync()
            .map(|value| value.map(QueryElement::SimilarTo)),
        QueryElement::MatchTags(filter) => Ok(filter.validate().map(QueryElement::MatchTags)),
        QueryElement::InBookmarks(filter) => Ok(filter.validate().map(QueryElement::InBookmarks)),
        QueryElement::ProcessedBy(filter) => Ok(filter.validate().map(QueryElement::ProcessedBy)),
        QueryElement::HasUnprocessedData(filter) => {
            Ok(filter.validate().map(QueryElement::HasUnprocessedData))
        }
    }
}

pub(crate) async fn preprocess_query_async(
    el: QueryElement,
    inference: &InferenceApiClient,
    embedding_cache_size: usize,
) -> Result<Option<QueryElement>, PqlError> {
    let mut state = AsyncPreprocessState {
        inference,
        metadata: None,
        embedding_cache_size,
    };
    preprocess_query_async_inner(el, &mut state).await
}

struct AsyncPreprocessState<'a> {
    inference: &'a InferenceApiClient,
    metadata: Option<Value>,
    embedding_cache_size: usize,
}

impl<'a> AsyncPreprocessState<'a> {
    async fn metadata(&mut self) -> Result<&Value, PqlError> {
        if self.metadata.is_none() {
            let value = self
                .inference
                .get_metadata()
                .await
                .map_err(|err| PqlError::invalid(format!("inference metadata error: {err}")))?;
            self.metadata = Some(value);
        }
        Ok(self.metadata.as_ref().expect("metadata cached"))
    }
}

fn preprocess_query_async_inner<'a, 'b>(
    el: QueryElement,
    state: &'b mut AsyncPreprocessState<'a>,
) -> Pin<Box<dyn Future<Output = Result<Option<QueryElement>, PqlError>> + Send + 'b>> {
    Box::pin(async move {
        match el {
            QueryElement::And(mut op) => {
                let mut cleaned = Vec::new();
                for sub_element in op.and_ {
                    if let Some(subquery) = preprocess_query_async_inner(sub_element, state).await?
                    {
                        cleaned.push(subquery);
                    }
                }
                if cleaned.is_empty() {
                    Ok(None)
                } else if cleaned.len() == 1 {
                    Ok(Some(cleaned.remove(0)))
                } else {
                    op.and_ = cleaned;
                    Ok(Some(QueryElement::And(op)))
                }
            }
            QueryElement::Or(mut op) => {
                let mut cleaned = Vec::new();
                for sub_element in op.or_ {
                    if let Some(subquery) = preprocess_query_async_inner(sub_element, state).await?
                    {
                        cleaned.push(subquery);
                    }
                }
                if cleaned.is_empty() {
                    Ok(None)
                } else if cleaned.len() == 1 {
                    Ok(Some(cleaned.remove(0)))
                } else {
                    op.or_ = cleaned;
                    Ok(Some(QueryElement::Or(op)))
                }
            }
            QueryElement::Not(mut op) => {
                if let Some(subquery) = preprocess_query_async_inner(*op.not_, state).await? {
                    op.not_ = Box::new(subquery);
                    Ok(Some(QueryElement::Not(op)))
                } else {
                    Ok(None)
                }
            }
            QueryElement::Match(filter) => Ok(filter.validate().map(QueryElement::Match)),
            QueryElement::MatchPath(filter) => Ok(filter.validate().map(QueryElement::MatchPath)),
            QueryElement::MatchText(filter) => Ok(filter.validate().map(QueryElement::MatchText)),
            QueryElement::SemanticTextSearch(filter) => filter
                .validate_async(state)
                .await
                .map(|value| value.map(QueryElement::SemanticTextSearch)),
            QueryElement::SemanticImageSearch(filter) => filter
                .validate_async(state)
                .await
                .map(|value| value.map(QueryElement::SemanticImageSearch)),
            QueryElement::SimilarTo(filter) => filter
                .validate_async(state)
                .await
                .map(|value| value.map(QueryElement::SimilarTo)),
            QueryElement::MatchTags(filter) => Ok(filter.validate().map(QueryElement::MatchTags)),
            QueryElement::InBookmarks(filter) => {
                Ok(filter.validate().map(QueryElement::InBookmarks))
            }
            QueryElement::ProcessedBy(filter) => {
                Ok(filter.validate().map(QueryElement::ProcessedBy))
            }
            QueryElement::HasUnprocessedData(filter) => {
                Ok(filter.validate().map(QueryElement::HasUnprocessedData))
            }
        }
    })
}

impl Match {
    fn validate(mut self) -> Option<Self> {
        if clean_matches(&mut self.match_) {
            Some(self)
        } else {
            None
        }
    }
}

impl MatchPath {
    fn validate(mut self) -> Option<Self> {
        if self.match_path.r#match.trim().is_empty() {
            return None;
        }
        if !self.match_path.raw_fts5_match {
            self.match_path.r#match = parse_and_escape_query(&self.match_path.r#match);
        }
        Some(self)
    }
}

impl MatchText {
    fn validate(mut self) -> Option<Self> {
        if !self.match_text.filter_only && self.match_text.r#match.trim().is_empty() {
            return None;
        }
        if self.match_text.filter_only {
            self.match_text.select_snippet_as = None;
            self.sort.order_by = false;
            self.sort.select_as = None;
            self.sort.row_n = false;
            self.match_text.r#match.clear();
        }
        if !self.match_text.raw_fts5_match {
            self.match_text.r#match = parse_and_escape_query(&self.match_text.r#match);
        }
        Some(self)
    }
}

impl MatchTags {
    fn validate(mut self) -> Option<Self> {
        if self.match_tags.tags.is_empty() {
            return None;
        }
        if self.match_tags.all_setters_required && self.match_tags.setters.is_empty() {
            self.match_tags.all_setters_required = false;
        }
        Some(self)
    }
}

impl InBookmarks {
    fn validate(self) -> Option<Self> {
        if self.in_bookmarks.filter {
            Some(self)
        } else {
            None
        }
    }
}

impl ProcessedBy {
    fn validate(self) -> Option<Self> {
        if self.processed_by.trim().is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

impl HasUnprocessedData {
    fn validate(self) -> Option<Self> {
        if self.has_data_unprocessed.setter_name.trim().is_empty() {
            return None;
        }
        if self.has_data_unprocessed.data_types.is_empty() {
            return None;
        }
        Some(self)
    }
}

impl SemanticTextSearch {
    fn validate_sync(mut self) -> Result<Option<Self>, PqlError> {
        if self.text_embeddings.query.trim().is_empty() {
            return Ok(None);
        }
        if self.text_embeddings._embedding.is_some() {
            return Ok(Some(self));
        }
        if self.text_embeddings.embed.is_none() {
            let embedding =
                extract_embeddings(&self.text_embeddings.query).map_err(PqlError::invalid)?;
            self.text_embeddings._embedding = Some(embedding);
            return Ok(Some(self));
        }
        Err(PqlError::invalid(
            "text_embeddings requires async preprocessing to embed the query",
        ))
    }

    async fn validate_async(
        mut self,
        state: &mut AsyncPreprocessState<'_>,
    ) -> Result<Option<Self>, PqlError> {
        if self.text_embeddings.query.trim().is_empty() {
            return Ok(None);
        }
        if self.text_embeddings._embedding.is_none() {
            if let Some(embed_args) = &self.text_embeddings.embed {
                let embedding = embed_text_query(
                    state,
                    &self.text_embeddings.query,
                    &self.text_embeddings.model,
                    embed_args,
                )
                .await?;
                self.text_embeddings._embedding = Some(embedding);
            } else {
                let embedding =
                    extract_embeddings(&self.text_embeddings.query).map_err(PqlError::invalid)?;
                self.text_embeddings._embedding = Some(embedding);
            }
        }
        Ok(Some(self))
    }
}

impl SemanticImageSearch {
    fn validate_sync(mut self) -> Result<Option<Self>, PqlError> {
        if self.image_embeddings.query.trim().is_empty() {
            return Ok(None);
        }
        if self.image_embeddings._embedding.is_none() {
            if self.image_embeddings.embed.is_none() {
                let embedding =
                    extract_embeddings(&self.image_embeddings.query).map_err(PqlError::invalid)?;
                self.image_embeddings._embedding = Some(embedding);
            } else {
                return Err(PqlError::invalid(
                    "image_embeddings requires async preprocessing to embed the query",
                ));
            }
        }
        if !self.image_embeddings.clip_xmodal && self.image_embeddings.src_text.is_some() {
            self.image_embeddings.src_text = None;
        }
        Ok(Some(self))
    }

    async fn validate_async(
        mut self,
        state: &mut AsyncPreprocessState<'_>,
    ) -> Result<Option<Self>, PqlError> {
        if self.image_embeddings.query.trim().is_empty() {
            return Ok(None);
        }
        if self.image_embeddings._embedding.is_none() {
            if let Some(embed_args) = &self.image_embeddings.embed {
                let embedding = embed_image_query(
                    state,
                    &self.image_embeddings.query,
                    &self.image_embeddings.model,
                    embed_args,
                )
                .await?;
                self.image_embeddings._embedding = Some(embedding);
            } else {
                let embedding =
                    extract_embeddings(&self.image_embeddings.query).map_err(PqlError::invalid)?;
                self.image_embeddings._embedding = Some(embedding);
            }
        }

        self.image_embeddings._distance_func_override =
            get_distance_func_override(state, &self.image_embeddings.model).await?;

        if !self.image_embeddings.clip_xmodal && self.image_embeddings.src_text.is_some() {
            self.image_embeddings.src_text = None;
        }
        Ok(Some(self))
    }
}

impl SimilarTo {
    fn validate_sync(mut self) -> Result<Option<Self>, PqlError> {
        if self.similar_to.target.trim().is_empty() {
            return Ok(None);
        }
        if self.similar_to.model.trim().is_empty() {
            return Ok(None);
        }
        if self.similar_to.force_distance_function.unwrap_or(false) {
            return Ok(Some(self));
        }
        Err(PqlError::invalid(
            "similar_to requires async preprocessing to apply distance function overrides",
        ))
    }

    async fn validate_async(
        mut self,
        state: &mut AsyncPreprocessState<'_>,
    ) -> Result<Option<Self>, PqlError> {
        if self.similar_to.target.trim().is_empty() {
            return Ok(None);
        }
        if self.similar_to.model.trim().is_empty() {
            return Ok(None);
        }
        if !self.similar_to.force_distance_function.unwrap_or(false) {
            if let Some(override_fn) =
                get_distance_func_override(state, &self.similar_to.model).await?
            {
                self.similar_to.distance_function = override_fn;
            }
        }
        Ok(Some(self))
    }
}

async fn embed_text_query(
    state: &AsyncPreprocessState<'_>,
    query: &str,
    model: &str,
    embed: &EmbedArgs,
) -> Result<Vec<u8>, PqlError> {
    let key = EmbeddingCacheKey {
        model: model.to_string(),
        kind: EmbeddingKind::Text,
        query: query.to_string(),
    };
    cached_embedding_or_fetch(key, state.embedding_cache_size, async {
        let inputs = [InferenceInput::new(
            serde_json::json!({"text": query, "task": "s2s"}),
            None,
        )];
        let output = state
            .inference
            .predict(
                model,
                &embed.cache_key,
                embed.lru_size,
                embed.ttl_seconds,
                &inputs,
            )
            .await
            .map_err(|err| PqlError::invalid(format!("inference embed error: {err}")))?;
        embedding_from_predict(output)
    })
    .await
}

async fn embed_image_query(
    state: &AsyncPreprocessState<'_>,
    query: &str,
    model: &str,
    embed: &EmbedArgs,
) -> Result<Vec<u8>, PqlError> {
    let key = EmbeddingCacheKey {
        model: model.to_string(),
        kind: EmbeddingKind::Image,
        query: query.to_string(),
    };
    cached_embedding_or_fetch(key, state.embedding_cache_size, async {
        let inputs = [InferenceInput::new(
            serde_json::json!({"text": query}),
            None,
        )];
        let output = state
            .inference
            .predict(
                model,
                &embed.cache_key,
                embed.lru_size,
                embed.ttl_seconds,
                &inputs,
            )
            .await
            .map_err(|err| PqlError::invalid(format!("inference embed error: {err}")))?;
        embedding_from_predict(output)
    })
    .await
}

fn embedding_from_predict(output: PredictOutput) -> Result<Vec<u8>, PqlError> {
    match output {
        PredictOutput::Binary(values) => {
            let first = values
                .first()
                .ok_or_else(|| PqlError::invalid("inference output missing"))?;
            embedding_from_npy_bytes(first).map_err(PqlError::invalid)
        }
        PredictOutput::Json(values) => {
            let first = values
                .first()
                .ok_or_else(|| PqlError::invalid("inference output missing"))?;
            embedding_from_json_value(first)
        }
    }
}

fn embedding_from_json_value(value: &Value) -> Result<Vec<u8>, PqlError> {
    if let Some(obj) = value.as_object() {
        if let Some(Value::String(kind)) = obj.get("__type__") {
            if kind == "base64" {
                let content = obj
                    .get("content")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| PqlError::invalid("base64 output missing content"))?;
                let decoded = general_purpose::STANDARD
                    .decode(content.as_bytes())
                    .map_err(|err| PqlError::invalid(format!("invalid base64 output: {err}")))?;
                return embedding_from_npy_bytes(&decoded).map_err(PqlError::invalid);
            }
        }
        if let Some(Value::Array(array)) = obj.get("embedding") {
            return embedding_from_json_array(array);
        }
    }
    if let Some(array) = value.as_array() {
        if array.is_empty() {
            return Err(PqlError::invalid("inference output embedding is empty"));
        }
        if let Some(first) = array.first() {
            if let Some(nested) = first.as_array() {
                return embedding_from_json_array(nested);
            }
        }
        return embedding_from_json_array(array);
    }
    Err(PqlError::invalid(
        "unsupported inference JSON output for embedding",
    ))
}

fn embedding_from_json_array(values: &[Value]) -> Result<Vec<u8>, PqlError> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let num = value
            .as_f64()
            .ok_or_else(|| PqlError::invalid("inference embedding JSON must be numeric"))?;
        out.push(num as f32);
    }
    Ok(serialize_f32(&out))
}

async fn get_distance_func_override(
    state: &mut AsyncPreprocessState<'_>,
    model_name: &str,
) -> Result<Option<DistanceFunction>, PqlError> {
    let metadata = state.metadata().await?;
    parse_distance_func_override(metadata, model_name)
}

fn parse_distance_func_override(
    metadata: &Value,
    model_name: &str,
) -> Result<Option<DistanceFunction>, PqlError> {
    let (group_name, inference_id) = model_name
        .split_once('/')
        .ok_or_else(|| PqlError::invalid(format!("invalid model name: {model_name}")))?;

    let group = metadata
        .get(group_name)
        .ok_or_else(|| PqlError::invalid(format!("group does not exist: {group_name}")))?;
    let inference_ids = group
        .get("inference_ids")
        .and_then(|value| value.as_object())
        .ok_or_else(|| PqlError::invalid("inference metadata missing inference_ids"))?;
    let group_metadata = group
        .get("group_metadata")
        .and_then(|value| value.as_object());
    let inference_metadata = inference_ids
        .get(inference_id)
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            PqlError::invalid(format!(
                "inference id does not exist: {group_name}/{inference_id}"
            ))
        })?;

    let distance_value = inference_metadata
        .get("distance_func")
        .or_else(|| group_metadata.and_then(|value| value.get("distance_func")));

    let value = match distance_value {
        None => return Ok(None),
        Some(Value::Null) => return Ok(None),
        Some(Value::String(value)) => value.as_str(),
        Some(other) => {
            return Err(PqlError::invalid(format!(
                "Invalid `distance_func` value for {model_name}: {other}. Must be one of: null, 'L2', 'cosine'"
            )));
        }
    };

    let override_fn = DistanceFunction::from_override(value).ok_or_else(|| {
        PqlError::invalid(format!(
            "Invalid `distance_func` value for {model_name}: {value}. Must be one of: null, 'L2', 'cosine'"
        ))
    })?;
    Ok(Some(override_fn))
}

fn clean_matches(matches: &mut Matches) -> bool {
    match matches {
        Matches::Ops(ops) => clean_match_ops(ops),
        Matches::And(and_ops) => clean_match_and(and_ops),
        Matches::Or(or_ops) => clean_match_or(or_ops),
        Matches::Not(not_ops) => clean_match_ops(&mut not_ops.not_),
    }
}

fn clean_match_and(and_ops: &mut MatchAnd) -> bool {
    let mut cleaned = Vec::new();
    for mut op in std::mem::take(&mut and_ops.and_) {
        if clean_match_ops(&mut op) {
            cleaned.push(op);
        }
    }
    and_ops.and_ = cleaned;
    !and_ops.and_.is_empty()
}

fn clean_match_or(or_ops: &mut MatchOr) -> bool {
    let mut cleaned = Vec::new();
    for mut op in std::mem::take(&mut or_ops.or_) {
        if clean_match_ops(&mut op) {
            cleaned.push(op);
        }
    }
    or_ops.or_ = cleaned;
    !or_ops.or_.is_empty()
}

fn clean_match_ops(ops: &mut MatchOps) -> bool {
    let mut has_valid = false;

    if let Some(value) = ops.eq.as_ref() {
        if value.is_empty() {
            ops.eq = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.neq.as_ref() {
        if value.is_empty() {
            ops.neq = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.in_.as_ref() {
        if value.is_empty() {
            ops.in_ = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.nin.as_ref() {
        if value.is_empty() {
            ops.nin = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.gt.as_ref() {
        if value.is_empty() {
            ops.gt = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.gte.as_ref() {
        if value.is_empty() {
            ops.gte = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.lt.as_ref() {
        if value.is_empty() {
            ops.lt = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.lte.as_ref() {
        if value.is_empty() {
            ops.lte = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.startswith.as_ref() {
        if value.is_empty() {
            ops.startswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_startswith.as_ref() {
        if value.is_empty() {
            ops.not_startswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.endswith.as_ref() {
        if value.is_empty() {
            ops.endswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_endswith.as_ref() {
        if value.is_empty() {
            ops.not_endswith = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.contains.as_ref() {
        if value.is_empty() {
            ops.contains = None;
        } else {
            has_valid = true;
        }
    }
    if let Some(value) = ops.not_contains.as_ref() {
        if value.is_empty() {
            ops.not_contains = None;
        } else {
            has_valid = true;
        }
    }

    has_valid
}

impl MatchValue {
    fn is_empty(&self) -> bool {
        self.file_id.is_none()
            && self.item_id.is_none()
            && self.path.is_none()
            && self.filename.is_none()
            && self.sha256.is_none()
            && self.last_modified.is_none()
            && self.r#type.is_none()
            && self.size.is_none()
            && self.width.is_none()
            && self.height.is_none()
            && self.duration.is_none()
            && self.time_added.is_none()
            && self.md5.is_none()
            && self.audio_tracks.is_none()
            && self.video_tracks.is_none()
            && self.subtitle_tracks.is_none()
            && self.blurhash.is_none()
            && self.data_id.is_none()
            && self.language.is_none()
            && self.language_confidence.is_none()
            && self.text.is_none()
            && self.confidence.is_none()
            && self.text_length.is_none()
            && self.job_id.is_none()
            && self.setter_id.is_none()
            && self.setter_name.is_none()
            && self.data_index.is_none()
            && self.source_id.is_none()
    }
}

impl MatchValues {
    fn is_empty(&self) -> bool {
        self.file_id.is_none()
            && self.item_id.is_none()
            && self.path.is_none()
            && self.filename.is_none()
            && self.sha256.is_none()
            && self.last_modified.is_none()
            && self.r#type.is_none()
            && self.size.is_none()
            && self.width.is_none()
            && self.height.is_none()
            && self.duration.is_none()
            && self.time_added.is_none()
            && self.md5.is_none()
            && self.audio_tracks.is_none()
            && self.video_tracks.is_none()
            && self.subtitle_tracks.is_none()
            && self.blurhash.is_none()
            && self.data_id.is_none()
            && self.language.is_none()
            && self.language_confidence.is_none()
            && self.text.is_none()
            && self.confidence.is_none()
            && self.text_length.is_none()
            && self.job_id.is_none()
            && self.setter_id.is_none()
            && self.setter_name.is_none()
            && self.data_index.is_none()
            && self.source_id.is_none()
    }
}
