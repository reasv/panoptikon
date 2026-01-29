use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

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
}

impl InferenceApiClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = normalize_base_url(base_url.into());
        let base = reqwest::Client::builder()
            .build()
            .context("failed to build inference API client")?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(base)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        Ok(Self { base_url, client })
    }

    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let inference = settings
            .upstreams
            .inference
            .as_ref()
            .context("inference upstream missing from settings")?;
        Self::new(inference.base_url.clone())
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        inputs: &[InferenceInput],
    ) -> Result<PredictOutput> {
        let url = format!("{}/predict/{}", self.base_url, inference_id);
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

        let response = self
            .client
            .post(url)
            .query(&[
                ("cache_key", cache_key),
                ("lru_size", &lru_size.to_string()),
                ("ttl_seconds", &ttl_seconds.to_string()),
            ])
            .multipart(form)
            .send()
            .await
            .context("inference predict request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("inference predict failed ({status}): {body}");
        }

        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = response.bytes().await?.to_vec();
        parse_predict_response(&content_type, &body)
    }

    pub async fn load_model(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
    ) -> Result<Value> {
        let url = format!("{}/load/{}", self.base_url, inference_id);
        let response = self
            .client
            .put(url)
            .query(&[
                ("cache_key", cache_key),
                ("lru_size", &lru_size.to_string()),
                ("ttl_seconds", &ttl_seconds.to_string()),
            ])
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
        let url = format!("{}/metadata", self.base_url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("inference metadata request failed")?;
        parse_json_response(response).await
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
    Ok(part
        .file_name(name)
        .mime_str("application/octet-stream")?)
}

fn normalize_base_url(raw: String) -> String {
    let trimmed = raw.trim_end_matches('/');
    if trimmed.ends_with("/api/inference") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/api/inference")
    }
}

fn parse_predict_response(content_type: &str, body: &[u8]) -> Result<PredictOutput> {
    if content_type.contains("application/json") {
        let value: Value = serde_json::from_slice(body)?;
        let outputs = value
            .get("outputs")
            .and_then(|item| item.as_array())
            .context("predict response missing outputs array")?;
        return Ok(PredictOutput::Json(outputs.to_vec()));
    }

    if content_type.contains("multipart/mixed") {
        let boundary = extract_boundary(content_type)
            .context("multipart response missing boundary")?;
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
        return response.json::<Value>().await.context("decode inference response");
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
