use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::warn;

use crate::config::Settings;

#[derive(Debug, Clone)]
pub(crate) enum InferenceFile {
    Path(PathBuf),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceInput {
    pub data: Value,
    pub file: Option<InferenceFile>,
}

impl InferenceInput {
    pub fn new(data: Value, file: Option<InferenceFile>) -> Self {
        Self { data, file }
    }
}

#[derive(Debug)]
pub(crate) enum PredictOutput {
    Json(Vec<Value>),
    Binary(Vec<Vec<u8>>),
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceApiClient {
    base_url: String,
    client: ClientWithMiddleware,
    raw_client: reqwest::Client,
    cache_metadata: bool,
}

#[derive(Debug, Clone)]
struct CachedMetadata {
    value: Value,
    fetched_at: Instant,
}

static METADATA_CACHE: OnceLock<RwLock<HashMap<String, CachedMetadata>>> = OnceLock::new();
const METADATA_CACHE_TTL: Duration = Duration::from_secs(300);
const PREDICT_MAX_RETRIES: u32 = 3;
const PREDICT_MIN_DELAY: Duration = Duration::from_secs(1);
const PREDICT_MAX_DELAY: Duration = Duration::from_secs(5);

impl InferenceApiClient {
    pub fn new_with_metadata_cache(
        base_url: impl Into<String>,
        cache_metadata: bool,
    ) -> Result<Self> {
        let base_url = normalize_base_url(base_url.into());
        let base = reqwest::Client::builder()
            .build()
            .context("failed to build inference API client")?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(base.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Ok(Self {
            base_url,
            client,
            raw_client: base,
            cache_metadata,
        })
    }

    pub fn from_settings_with_metadata_cache(
        settings: &Settings,
        cache_metadata: bool,
    ) -> Result<Self> {
        let inference = settings
            .upstreams
            .inference
            .first()
            .context("inference upstream missing from settings")?;
        Self::new_with_metadata_cache(inference.base_url.clone(), cache_metadata)
    }

    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        max_batch: Option<u32>,
        prewarm: Option<bool>,
        inputs: &[InferenceInput],
    ) -> Result<PredictOutput> {
        let url = format!("{}/predict/{}", self.base_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        // Per-request cap on server-side batch merging (design doc §6):
        // only sent when the caller has an opinion. Old Python inference
        // servers (FastAPI) ignore unknown query params, so sending this
        // to a pre-max_batch upstream is harmless.
        if let Some(max_batch) = max_batch {
            query.push(("max_batch", max_batch.to_string()));
        }
        // Lazy prewarm hint (design doc §8): absent = true on the server,
        // so only callers with an opinion (extraction jobs: false) send it.
        // Equally ignored by old Python servers.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let mut attempts: u32 = 0;
        loop {
            let form = build_predict_form(inputs).await?;
            let response = self
                .raw_client
                .post(&url)
                .query(&query)
                .multipart(form)
                .send()
                .await;

            match response {
                Ok(response) => {
                    if response.status().is_success() {
                        let content_type = response
                            .headers()
                            .get(CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        let body = response.bytes().await?.to_vec();
                        return parse_predict_response(&content_type, &body);
                    }

                    let status = response.status();
                    if should_retry_status(status) {
                        if let Some(delay) = next_retry_delay(attempts) {
                            attempts += 1;
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }

                    let body = response.text().await.unwrap_or_default();
                    warn!(%url, %status, %body, "inference predict failed");
                    bail!("inference predict failed ({status})");
                }
                Err(err) => {
                    if should_retry_error(&err) {
                        if let Some(delay) = next_retry_delay(attempts) {
                            attempts += 1;
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                    warn!(%url, error = %err, "inference predict request failed");
                    return Err(err).context("inference predict request failed");
                }
            }
        }
    }

    pub async fn load_model(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        prewarm: Option<bool>,
    ) -> Result<Value> {
        let url = format!("{}/load/{}", self.base_url, inference_id);
        let mut query: Vec<(&str, String)> = vec![
            ("cache_key", cache_key.to_string()),
            ("lru_size", lru_size.to_string()),
            ("ttl_seconds", ttl_seconds.to_string()),
        ];
        // Lazy prewarm hint (design doc §8), same absent-means-true rule as
        // on predict; old Python servers ignore it.
        if let Some(prewarm) = prewarm {
            query.push(("prewarm", prewarm.to_string()));
        }
        let response = self
            .client
            .put(url)
            .query(&query)
            .send()
            .await
            .context("inference load request failed")?;
        parse_json_response(response).await
    }

    pub async fn unload_model(&self, inference_id: &str, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}/{}", self.base_url, cache_key, inference_id);
        let response = self
            .client
            .delete(url)
            .send()
            .await
            .context("inference unload request failed")?;
        parse_json_response(response).await
    }

    pub async fn clear_cache(&self, cache_key: &str) -> Result<Value> {
        let url = format!("{}/cache/{}", self.base_url, cache_key);
        let response = self
            .client
            .delete(url)
            .send()
            .await
            .context("inference clear cache request failed")?;
        parse_json_response(response).await
    }

    // Only exercised by the inferio HTTP tests; mirrors the Python client API.
    #[allow(dead_code)]
    pub async fn get_cached_models(&self) -> Result<Value> {
        let url = format!("{}/cache", self.base_url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("inference cache list request failed")?;
        parse_json_response(response).await
    }

    pub async fn get_metadata(&self) -> Result<Value> {
        if !self.cache_metadata {
            return self.fetch_metadata().await;
        }
        let cache = METADATA_CACHE.get_or_init(|| RwLock::new(HashMap::new()));
        {
            let guard = cache.read().await;
            if let Some(entry) = guard.get(&self.base_url) {
                if entry.fetched_at.elapsed() < METADATA_CACHE_TTL {
                    return Ok(entry.value.clone());
                }
            }
        }

        let value = self.fetch_metadata().await?;
        let mut guard = cache.write().await;
        guard.insert(
            self.base_url.clone(),
            CachedMetadata {
                value: value.clone(),
                fetched_at: Instant::now(),
            },
        );
        Ok(value)
    }

    async fn fetch_metadata(&self) -> Result<Value> {
        let url = format!("{}/metadata", self.base_url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("inference metadata request failed")?;
        parse_json_response(response).await
    }

    pub async fn get_external_inputs(&self) -> Result<Value> {
        let url = format!("{}/external-inputs", self.base_url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("inference external-input request failed")?;
        parse_json_response(response).await
    }

    /// Fetch external inputs when the upstream implements the additive
    /// endpoint. Only a genuine 404 means an older unsupported server;
    /// availability, authorization, and decoding failures remain errors.
    pub async fn get_external_inputs_optional(&self) -> Result<Option<Value>> {
        let url = format!("{}/external-inputs", self.base_url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("inference external-input request failed")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        parse_json_response(response).await.map(Some)
    }
}

async fn file_to_part(idx: usize, file: &InferenceFile) -> Result<Part> {
    let name = idx.to_string();
    let part = match file {
        InferenceFile::Path(path) => {
            let bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("failed to read file {}", path.display()))?;
            Part::bytes(bytes)
        }
        InferenceFile::Bytes(bytes) => Part::bytes(bytes.clone()),
    };
    Ok(part.file_name(name).mime_str("application/octet-stream")?)
}

async fn build_predict_form(inputs: &[InferenceInput]) -> Result<Form> {
    let payload = json!({
        "inputs": inputs.iter().map(|item| item.data.clone()).collect::<Vec<_>>(),
    });
    let mut form = Form::new().text("data", serde_json::to_string(&payload)?);
    for (idx, input) in inputs.iter().enumerate() {
        if let Some(file) = &input.file {
            let part = file_to_part(idx, file).await?;
            form = form.part("files", part);
        }
    }
    Ok(form)
}

fn should_retry_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout()
}

fn next_retry_delay(attempts: u32) -> Option<std::time::Duration> {
    if attempts >= PREDICT_MAX_RETRIES {
        return None;
    }
    let multiplier = 1u64 << attempts;
    let min_ms = PREDICT_MIN_DELAY.as_millis() as u64;
    let max_ms = PREDICT_MAX_DELAY.as_millis() as u64;
    let delay_ms = min_ms.saturating_mul(multiplier).min(max_ms);
    Some(Duration::from_millis(delay_ms))
}

fn normalize_base_url(raw: String) -> String {
    let trimmed = raw.trim_end_matches('/');
    if trimmed.ends_with("/api/inference") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/api/inference")
    }
}

/// Also used by the local orchestrator's HTTP tests as the parity oracle:
/// whatever `inferio::http` encodes must be parseable by this exact logic.
pub(crate) fn parse_predict_response(content_type: &str, body: &[u8]) -> Result<PredictOutput> {
    if content_type.contains("application/json") {
        let value: Value = serde_json::from_slice(body)?;
        let outputs = value
            .get("outputs")
            .and_then(|item| item.as_array())
            .context("predict response missing outputs array")?;
        return Ok(PredictOutput::Json(outputs.to_vec()));
    }

    if content_type.contains("multipart/mixed") {
        let boundary =
            extract_boundary(content_type).context("multipart response missing boundary")?;
        let outputs = parse_multipart_outputs(body, &boundary)?;
        return Ok(PredictOutput::Binary(outputs));
    }

    if content_type.contains("application/octet-stream") {
        return Ok(PredictOutput::Binary(vec![body.to_vec()]));
    }

    bail!("unexpected inference response content type: {content_type}");
}

async fn parse_json_response(response: reqwest::Response) -> Result<Value> {
    if response.status().is_success() {
        return response
            .json::<Value>()
            .await
            .context("decode inference response");
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    bail!("inference request failed ({status}): {body}");
}

fn extract_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|segment| {
        let segment = segment.trim();
        segment
            .strip_prefix("boundary=")
            .map(|value| value.trim_matches('"').to_string())
    })
}

fn parse_multipart_outputs(body: &[u8], boundary: &str) -> Result<Vec<Vec<u8>>> {
    let marker = format!("--{boundary}");
    let mut outputs = Vec::new();

    for part in split_by_boundary(body, marker.as_bytes()) {
        if part.is_empty() || part == b"--\r\n" || part == b"--" {
            continue;
        }
        let Some((headers, content)) = split_headers(part) else {
            continue;
        };
        let Some(filename) = extract_filename(headers) else {
            continue;
        };
        let index = filename
            .trim_start_matches("output")
            .trim_end_matches(".bin")
            .parse::<usize>()
            .ok();
        let mut data = content.to_vec();
        while data.ends_with(b"\r\n") {
            data.truncate(data.len().saturating_sub(2));
        }
        match index {
            Some(idx) => {
                if outputs.len() <= idx {
                    outputs.resize(idx + 1, Vec::new());
                }
                outputs[idx] = data;
            }
            None => outputs.push(data),
        }
    }

    Ok(outputs)
}

fn split_by_boundary<'a>(body: &'a [u8], marker: &[u8]) -> Vec<&'a [u8]> {
    if marker.is_empty() {
        return vec![body];
    }
    let mut parts = Vec::new();
    let mut cursor = 0;
    while let Some(pos) = find_subslice(&body[cursor..], marker) {
        let end = cursor + pos;
        parts.push(&body[cursor..end]);
        cursor = end + marker.len();
    }
    parts.push(&body[cursor..]);
    parts
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn split_headers(part: &[u8]) -> Option<(&[u8], &[u8])> {
    let separator = b"\r\n\r\n";
    part.windows(separator.len())
        .position(|window| window == separator)
        .map(|idx| (&part[..idx], &part[idx + separator.len()..]))
}

fn extract_filename(headers: &[u8]) -> Option<String> {
    let header_str = std::str::from_utf8(headers).ok()?;
    for line in header_str.lines() {
        let line = line.trim();
        if !line.to_ascii_lowercase().starts_with("content-disposition") {
            continue;
        }
        for segment in line.split(';') {
            let segment = segment.trim();
            if let Some(value) = segment.strip_prefix("filename=") {
                return Some(value.trim_matches('"').to_string());
            }
        }
    }
    None
}

#[allow(dead_code)]
fn file_input_from_path(path: impl AsRef<Path>) -> InferenceFile {
    InferenceFile::Path(path.as_ref().to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::RawQuery;
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::sync::{Arc, Mutex as StdMutex};

    /// Optional external-input discovery treats only a 404 as an older
    /// unsupported server; other HTTP failures remain visible to callers.
    #[tokio::test]
    async fn optional_external_inputs_only_ignores_not_found() {
        let app = Router::new()
            .route(
                "/missing/api/inference/external-inputs",
                get(|| async { StatusCode::NOT_FOUND }),
            )
            .route(
                "/broken/api/inference/external-inputs",
                get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let missing =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}/missing"), false)
                .unwrap();
        assert_eq!(missing.get_external_inputs_optional().await.unwrap(), None);

        let broken =
            InferenceApiClient::new_with_metadata_cache(format!("http://{addr}/broken"), false)
                .unwrap();
        assert!(broken.get_external_inputs_optional().await.is_err());
    }

    /// The predict request must carry `max_batch` (design §6) and `prewarm`
    /// (design §8) as query params exactly when the caller passes Some, and
    /// omit them entirely when None — so callers with no opinion (PQL
    /// search embeds pass None for both) leave the server defaults in
    /// charge, and the params never appear as spurious empty values. Same
    /// contract on PUT /load for `prewarm` (extraction jobs pass
    /// Some(false); cron preload passes None). Captured off a stub server
    /// because the client builds the URLs internally.
    #[tokio::test]
    async fn urls_carry_max_batch_and_prewarm_only_when_some() {
        let captured: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let predict_sink = Arc::clone(&captured);
        let load_sink = Arc::clone(&captured);
        let app = Router::new()
            .route(
                "/api/inference/predict/{group}/{id}",
                post(move |RawQuery(query): RawQuery| {
                    let sink = Arc::clone(&predict_sink);
                    async move {
                        sink.lock().unwrap().push(query.unwrap_or_default());
                        Json(json!({"outputs": [{"ok": true}]}))
                    }
                }),
            )
            .route(
                "/api/inference/load/{group}/{id}",
                axum::routing::put(move |RawQuery(query): RawQuery| {
                    let sink = Arc::clone(&load_sink);
                    async move {
                        sink.lock().unwrap().push(query.unwrap_or_default());
                        Json(json!({"status": "loaded"}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = InferenceApiClient::new_with_metadata_cache(format!("http://{addr}"), false)
            .expect("client builds");
        let inputs = [InferenceInput::new(json!({"text": "x"}), None)];
        client
            .predict("group/model", "key", 10, -1, Some(7), Some(false), &inputs)
            .await
            .expect("capped no-prewarm predict");
        client
            .predict("group/model", "key", 10, -1, None, None, &inputs)
            .await
            .expect("no-opinion predict");
        client
            .load_model("group/model", "key", 10, -1, Some(false))
            .await
            .expect("no-prewarm load");
        client
            .load_model("group/model", "key", 10, -1, None)
            .await
            .expect("no-opinion load");

        let queries = captured.lock().unwrap().clone();
        assert_eq!(queries.len(), 4, "all four requests reached the stub");
        assert!(
            queries[0].contains("max_batch=7") && queries[0].contains("prewarm=false"),
            "Some values serialize as query params: {}",
            queries[0]
        );
        assert!(
            queries[0].contains("cache_key=key")
                && queries[0].contains("lru_size=10")
                && queries[0].contains("ttl_seconds=-1"),
            "existing params still present alongside the additive ones: {}",
            queries[0]
        );
        assert!(
            !queries[1].contains("max_batch") && !queries[1].contains("prewarm"),
            "None omits both params entirely: {}",
            queries[1]
        );
        assert!(
            queries[2].contains("prewarm=false"),
            "load with Some(false) carries prewarm: {}",
            queries[2]
        );
        assert!(
            !queries[3].contains("prewarm"),
            "load with None omits prewarm: {}",
            queries[3]
        );
    }
}
