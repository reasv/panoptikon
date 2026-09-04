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
use crate::inferio::slot_error::{ProtocolViolation, SlotErrorClass, slot_error_from_json};

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

impl PredictOutput {
    /// How many successful outputs this carries. Only ever zero when every
    /// slot of the response was a typed error.
    pub fn len(&self) -> usize {
        match self {
            PredictOutput::Json(values) => values.len(),
            PredictOutput::Binary(values) => values.len(),
        }
    }

    /// True when nothing succeeded, which is the one case callers must not
    /// merge (an empty `Json` would clash with a `Binary` sibling chunk).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One input's typed failure, carried alongside the surviving outputs
/// (`docs/inferio-worker-protocol.md`, "Per-item error slots"). `index` is the
/// position of the *input* it belongs to, which is not the position of any
/// output: erroring slots are removed from `PredictResponse::outputs`, so the
/// survivors close ranks.
#[derive(Debug, Clone)]
pub(crate) struct PredictSlotError {
    pub index: usize,
    pub class: SlotErrorClass,
    pub message: String,
}

/// A predict response: the outputs of the inputs that succeeded, plus the
/// typed per-slot failures of the ones that did not. `errors` is empty for
/// every response an inference server without per-item error slots can
/// produce, which is what keeps this backward compatible.
#[derive(Debug)]
pub(crate) struct PredictResponse {
    pub outputs: PredictOutput,
    pub errors: Vec<PredictSlotError>,
    /// The orchestrator's desired in-flight figure for this model, in items
    /// ([`DESIRED_IN_FLIGHT_HEADER`]). `None` when the server did not say —
    /// a Python-era inference server, a model that has not dispatched a
    /// window yet, or a value that is unparsable or zero (the orchestrator
    /// never publishes zero, and reading one as a figure would ask a caller
    /// to keep no work in flight at all). Callers treat `None` as "no
    /// opinion" and keep their own floor.
    pub desired_in_flight_items: Option<u64>,
}

/// Response header the local orchestrator publishes the figure on
/// (`inferio::http::DESIRED_IN_FLIGHT_HEADER`; documented in
/// `docs/inferio-worker-protocol.md`).
pub(crate) const DESIRED_IN_FLIGHT_HEADER: &str = "x-panoptikon-desired-in-flight-items";

/// `detail.kind` of a predict that failed because the inference worker
/// process died with the request in flight
/// (`inferio::http::WORKER_DIED_KIND`). The request's items were never
/// attempted, so re-submitting them is correct — see run1 finding F7.
pub(crate) const WORKER_DIED_KIND: &str = "worker_died";

/// `detail.kind` of a request refused because the model is inside its
/// per-model load-failure cooldown. Unlike every other 503 this must **not**
/// be retried: the server is telling the caller when to come back, and a job
/// that keeps asking only burns the cooldown's whole window one request at a
/// time.
pub(crate) const LOAD_COOLDOWN_KIND: &str = "load_cooldown";

/// A request the inference server refused, with the machine-readable half of
/// its `{"detail": …}` body parsed out.
///
/// It is a typed error (attached to the returned `anyhow::Error`, so callers
/// reach it with `downcast_ref`) rather than a string, because two callers
/// act on it: an extraction job re-queues a [`WORKER_DIED_KIND`] request's
/// items once instead of recording them as failures, and aborts outright on
/// [`LOAD_COOLDOWN_KIND`]. Both decisions have to survive the error being
/// wrapped in context on the way up, and neither may depend on prose.
///
/// `kind` is `None` for every failure that answered with the plain string
/// detail — an older server, an unrelated 4xx/5xx — which is the
/// "no machine-readable opinion" case every caller already handled.
#[derive(Debug, Clone)]
pub(crate) struct InferenceFailure {
    /// HTTP status of the refusal.
    pub status: u16,
    /// `detail.kind`, when the body carried a structured detail.
    pub kind: Option<String>,
    /// Human-readable summary: `detail.message` for a structured detail, the
    /// whole `detail` for a plain string one, and the raw body when it is
    /// neither.
    pub message: String,
    /// The model the failure is about, `group/name`.
    pub model: Option<String>,
    /// The last error that put the model in this state (a cooldown), or the
    /// fatal error chain (a worker death).
    pub last_error: Option<String>,
    /// RFC 3339 instant the model may be retried at.
    pub retry_at: Option<String>,
    /// Consecutive load failures counted so far.
    pub failures: Option<u32>,
    /// `Retry-After`, in seconds, when the server sent one.
    pub retry_after_secs: Option<u64>,
}

impl InferenceFailure {
    /// Parse one refused response. Never fails: a body this cannot read is
    /// still a failure, it just carries no machine-readable half.
    fn parse(status: reqwest::StatusCode, retry_after: Option<u64>, body: &str) -> Self {
        let mut failure = Self {
            status: status.as_u16(),
            kind: None,
            message: body.trim().to_owned(),
            model: None,
            last_error: None,
            retry_at: None,
            failures: None,
            retry_after_secs: retry_after,
        };
        let Ok(parsed) = serde_json::from_str::<Value>(body) else {
            return failure;
        };
        let Some(detail) = parsed.get("detail") else {
            return failure;
        };
        if let Some(text) = detail.as_str() {
            failure.message = text.to_owned();
            return failure;
        }
        let Some(object) = detail.as_object() else {
            return failure;
        };
        let text = |key: &str| object.get(key).and_then(Value::as_str).map(str::to_owned);
        failure.kind = text("kind");
        if let Some(message) = text("message") {
            failure.message = message;
        }
        failure.model = text("model");
        failure.last_error = text("last_error");
        failure.retry_at = text("retry_at");
        failure.failures = object
            .get("failures")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
        failure
    }

    /// The worker process died with the request in flight.
    pub fn is_worker_death(&self) -> bool {
        self.kind.as_deref() == Some(WORKER_DIED_KIND)
    }

    /// The model is inside its per-model load-failure cooldown.
    pub fn is_load_cooldown(&self) -> bool {
        self.kind.as_deref() == Some(LOAD_COOLDOWN_KIND)
    }
}

impl std::fmt::Display for InferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "inference request failed ({})", self.status)?;
        if let Some(kind) = &self.kind {
            write!(f, " [{kind}]")?;
        }
        if !self.message.is_empty() {
            write!(f, ": {}", self.message)?;
        }
        if let Some(retry_at) = &self.retry_at {
            write!(f, "; retry at {retry_at}")?;
        }
        if let Some(last_error) = &self.last_error {
            write!(f, "; last error: {last_error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for InferenceFailure {}

/// The typed failure inside an error chain, if there is one. The chain is
/// what callers hold: `InferencePool` wraps, and the job path adds context.
pub(crate) fn inference_failure(err: &anyhow::Error) -> Option<&InferenceFailure> {
    err.downcast_ref::<InferenceFailure>()
}

/// `Retry-After` in seconds, when the header is present and is a plain
/// delta-seconds value (the only form this surface sends).
fn retry_after_secs(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

impl PredictResponse {
    fn plain(outputs: PredictOutput) -> Self {
        Self {
            outputs,
            errors: Vec::new(),
            desired_in_flight_items: None,
        }
    }
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

    #[allow(clippy::too_many_arguments)]
    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        max_batch: Option<u32>,
        prewarm: Option<bool>,
        inputs: &[InferenceInput],
    ) -> Result<PredictResponse> {
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
                        // Additive, optional: absent (or unparsable) leaves
                        // the caller on its own floor. Read before the body
                        // is consumed, which drops the response.
                        let desired = response
                            .headers()
                            .get(DESIRED_IN_FLIGHT_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .and_then(|value| value.trim().parse::<u64>().ok())
                            .filter(|value| *value > 0);
                        let body = response.bytes().await?.to_vec();
                        let mut parsed = parse_predict_response(&content_type, &body)?;
                        parsed.desired_in_flight_items = desired;
                        return Ok(parsed);
                    }

                    let status = response.status();
                    let retry_after = retry_after_secs(response.headers());
                    // Read before deciding: the body is what says whether a
                    // 503 is the transient one this loop retries or the
                    // cooldown that must be handed straight to the caller.
                    let body = response.text().await.unwrap_or_default();
                    let failure = InferenceFailure::parse(status, retry_after, &body);
                    if failure.is_load_cooldown() {
                        warn!(
                            %url,
                            %status,
                            model = failure.model.as_deref().unwrap_or("?"),
                            retry_at = failure.retry_at.as_deref().unwrap_or("?"),
                            "inference predict refused: the model is in its load-failure cooldown"
                        );
                        return Err(anyhow::Error::new(failure));
                    }
                    if should_retry_status(status)
                        && let Some(delay) = next_retry_delay(attempts)
                    {
                        attempts += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    warn!(%url, %status, %body, "inference predict failed");
                    return Err(anyhow::Error::new(failure));
                }
                Err(err) => {
                    if should_retry_error(&err)
                        && let Some(delay) = next_retry_delay(attempts)
                    {
                        attempts += 1;
                        tokio::time::sleep(delay).await;
                        continue;
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
            if let Some(entry) = guard.get(&self.base_url)
                && entry.fetched_at.elapsed() < METADATA_CACHE_TTL
            {
                return Ok(entry.value.clone());
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
///
/// The binary encodings cannot carry a typed error slot, so only the JSON
/// envelope is inspected for them; a malformed one is an error rather than a
/// payload, mirroring the orchestrator's strictness on the msgpack hop.
pub(crate) fn parse_predict_response(content_type: &str, body: &[u8]) -> Result<PredictResponse> {
    if content_type.contains("application/json") {
        let value: Value = serde_json::from_slice(body)?;
        let outputs = value
            .get("outputs")
            .and_then(|item| item.as_array())
            .context("predict response missing outputs array")?;
        return parse_json_outputs(outputs);
    }

    if content_type.contains("multipart/mixed") {
        let boundary =
            extract_boundary(content_type).context("multipart response missing boundary")?;
        let outputs = parse_multipart_outputs(body, &boundary)?;
        return Ok(PredictResponse::plain(PredictOutput::Binary(outputs)));
    }

    if content_type.contains("application/octet-stream") {
        return Ok(PredictResponse::plain(PredictOutput::Binary(vec![
            body.to_vec(),
        ])));
    }

    bail!("unexpected inference response content type: {content_type}");
}

/// Splits a JSON `outputs` array into surviving payloads and typed slot
/// errors.
///
/// The base64 unwrapping only happens when the batch *did* carry an error
/// slot: without error slots an all-binary batch is encoded as
/// `multipart/mixed`, never as this envelope, so the rule can only ever fire
/// on the new shape and every response an older server can produce is passed
/// through byte-identically to before.
///
/// Whether a survivor is binary is decided *per slot* — the encoder wraps
/// every binary output and leaves every JSON output alone, so wrappedness is
/// the per-slot record of what the model returned for that input. Since
/// `PredictOutput` is one type for the whole response, a batch mixing the two
/// is a response this client cannot represent, and it is reported as such
/// rather than handed on: passed through as JSON, the wrapper map reaches an
/// output handler that reads no `transcription` from it and silently drops
/// the payload.
fn parse_json_outputs(outputs: &[Value]) -> Result<PredictResponse> {
    let mut errors = Vec::new();
    let mut survivors: Vec<&Value> = Vec::with_capacity(outputs.len());
    for (index, value) in outputs.iter().enumerate() {
        match slot_error_from_json(value) {
            Some(Ok(error)) => errors.push(PredictSlotError {
                index,
                class: error.class,
                message: error.message,
            }),
            // Typed, because it is deterministic: callers must not spend an
            // isolation pass re-asking a server that will answer identically.
            Some(Err(reason)) => {
                return Err(anyhow::Error::new(ProtocolViolation::new(format!(
                    "predict output {index} is a malformed error slot: {reason}"
                ))));
            }
            None => survivors.push(value),
        }
    }
    if errors.is_empty() {
        return Ok(PredictResponse::plain(PredictOutput::Json(
            survivors.into_iter().cloned().collect(),
        )));
    }
    let wrapped = survivors.iter().filter(|v| is_base64_wrapper(v)).count();
    if wrapped == 0 {
        return Ok(PredictResponse {
            outputs: PredictOutput::Json(survivors.into_iter().cloned().collect()),
            errors,
            desired_in_flight_items: None,
        });
    }
    if wrapped != survivors.len() {
        return Err(anyhow::Error::new(ProtocolViolation::new(format!(
            "predict response mixes {wrapped} binary and {} JSON outputs, \
             which have no common representation",
            survivors.len() - wrapped
        ))));
    }
    let mut decoded = Vec::with_capacity(survivors.len());
    for value in survivors {
        decoded.push(decode_base64_wrapper(value)?);
    }
    Ok(PredictResponse {
        outputs: PredictOutput::Binary(decoded),
        errors,
        desired_in_flight_items: None,
    })
}

fn is_base64_wrapper(value: &Value) -> bool {
    value
        .get("__type__")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "base64")
}

fn decode_base64_wrapper(value: &Value) -> Result<Vec<u8>> {
    use base64::Engine as _;
    let content = value
        .get("content")
        .and_then(Value::as_str)
        .context("base64 output missing content")?;
    base64::engine::general_purpose::STANDARD
        .decode(content.as_bytes())
        .context("invalid base64 output")
}

async fn parse_json_response(response: reqwest::Response) -> Result<Value> {
    if response.status().is_success() {
        return response
            .json::<Value>()
            .await
            .context("decode inference response");
    }
    let status = response.status();
    let retry_after = retry_after_secs(response.headers());
    let body = response.text().await.unwrap_or_default();
    // Typed here too: a load that hits the per-model cooldown must reach the
    // job with its kind intact, exactly like a predict that does.
    Err(anyhow::Error::new(InferenceFailure::parse(
        status,
        retry_after,
        &body,
    )))
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

    fn envelope(outputs: Vec<Value>) -> (String, Vec<u8>) {
        (
            "application/json".to_string(),
            serde_json::to_vec(&json!({ "outputs": outputs })).unwrap(),
        )
    }

    /// Wrappedness is decided per slot, not all-or-nothing: the encoder wraps
    /// every binary output and leaves every JSON output alone, so a batch
    /// with both is a response `PredictOutput` — one type for the whole
    /// response — cannot represent. It is reported instead of handed on:
    /// passed through as JSON, the wrapper map reaches an output handler that
    /// finds no `transcription` in it and drops the payload silently.
    #[test]
    fn a_mixed_binary_and_json_batch_is_reported_not_guessed() {
        let (content_type, body) = envelope(vec![
            json!({"__error__": {"class": "input", "message": "Unreadable image"}}),
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"transcription": "hello"}),
        ]);
        let err = parse_predict_response(&content_type, &body)
            .expect_err("a response with no common representation must not be guessed at");
        assert!(
            is_protocol_violation(&err),
            "and it is deterministic, so isolation must not retry it: {err:#}"
        );
        assert!(
            format!("{err:#}").contains("mixes 1 binary and 1 JSON"),
            "{err:#}"
        );
    }

    /// The two unmixed shapes still round-trip: all survivors wrapped is a
    /// binary batch (an embedding model whose item had one bad frame), none
    /// wrapped is a JSON batch. Both keep the slot error at its *input's*
    /// index while the survivors close ranks.
    #[test]
    fn unmixed_survivors_round_trip_beside_an_error_slot() {
        let (content_type, body) = envelope(vec![
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"__error__": {"class": "input", "message": "Unreadable image"}}),
            json!({"__type__": "base64", "content": "QkI="}),
        ]);
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors.len(), 1);
        assert_eq!(parsed.errors[0].index, 1);
        match parsed.outputs {
            PredictOutput::Binary(outputs) => {
                assert_eq!(outputs, vec![b"AAA".to_vec(), b"BB".to_vec()]);
            }
            other => panic!("client parsed {other:?}"),
        }

        let (content_type, body) = envelope(vec![
            json!({"__error__": {"class": "transient", "message": "try again"}}),
            json!({"transcription": "hello"}),
        ]);
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert_eq!(parsed.errors[0].class, SlotErrorClass::Transient);
        match parsed.outputs {
            PredictOutput::Json(values) => {
                assert_eq!(values, vec![json!({"transcription": "hello"})]);
            }
            other => panic!("client parsed {other:?}"),
        }
    }

    /// The legacy no-slot path is untouched: a JSON envelope without error
    /// slots is passed through verbatim, wrapper maps and all. (The encoder
    /// never produces that shape — an all-binary batch goes out as
    /// `multipart/mixed` — so this is only pinning the byte-identical
    /// behaviour of every response an older server can send.)
    #[test]
    fn the_legacy_envelope_is_passed_through_verbatim() {
        let values = vec![
            json!({"__type__": "base64", "content": "QUFB"}),
            json!({"transcription": "hello"}),
        ];
        let (content_type, body) = envelope(values.clone());
        let parsed = parse_predict_response(&content_type, &body).unwrap();
        assert!(parsed.errors.is_empty());
        match parsed.outputs {
            PredictOutput::Json(parsed) => assert_eq!(parsed, values),
            other => panic!("client parsed {other:?}"),
        }
    }

    /// A malformed error slot is a protocol violation rather than a payload
    /// or a guessed class, and it is *typed* so the extraction job can skip
    /// the isolation pass that would only ask the same broken server again.
    #[test]
    fn a_malformed_error_slot_is_a_typed_protocol_violation() {
        let (content_type, body) = envelope(vec![
            json!({"transcription": "hello"}),
            json!({"__error__": {"class": "blocked", "message": "not ours"}}),
        ]);
        let err = parse_predict_response(&content_type, &body).expect_err("malformed");
        assert!(is_protocol_violation(&err), "{err:#}");
        assert!(format!("{err:#}").contains("predict output 1"), "{err:#}");
    }

    fn is_protocol_violation(err: &anyhow::Error) -> bool {
        err.downcast_ref::<ProtocolViolation>().is_some()
    }
}
