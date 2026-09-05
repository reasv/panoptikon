use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::Mutex;

use crate::config::InferenceEndpointConfig;
use crate::inferio_client::{
    InferenceApiClient, InferenceFailure, InferenceInput, PredictResponse, inference_failure,
};

#[derive(Clone)]
pub(crate) struct InferencePool {
    state: Arc<Mutex<PoolState>>,
}

struct PoolState {
    endpoints: Vec<EndpointState>,
}

struct EndpointState {
    client: InferenceApiClient,
    weight: f64,
    current_weight: f64,
}

impl InferencePool {
    pub fn new(endpoints: Vec<InferenceEndpointConfig>) -> Result<Self> {
        let mut states = Vec::new();
        for endpoint in endpoints {
            let client = InferenceApiClient::new_with_metadata_cache(endpoint.base_url, false)
                .context("failed to create inference API client")?;
            states.push(EndpointState {
                client,
                weight: endpoint.weight,
                current_weight: 0.0,
            });
        }
        Ok(Self {
            state: Arc::new(Mutex::new(PoolState { endpoints: states })),
        })
    }

    /// Whether every endpoint this pool would use is known to multiplex its
    /// requests over a shared connection pool (HTTP/2 cleartext). Conservative:
    /// an unknown transport, and a pool with no enabled endpoint, both read as
    /// "not multiplexed", since the answer sizes a *descriptor* budget.
    pub async fn requests_are_multiplexed(&self) -> bool {
        let guard = self.state.lock().await;
        let mut enabled = 0usize;
        for endpoint in guard.endpoints.iter().filter(|e| e.weight > 0.0) {
            enabled += 1;
            match endpoint.client.known_transport() {
                Some(transport) if transport.is_multiplexed() => {}
                // One HTTP/1.1 endpoint is enough to put the per-request
                // socket cost back: the window is one budget across all of
                // them, so it is sized for the most expensive.
                _ => return false,
            }
        }
        enabled > 0
    }

    pub async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard
            .endpoints
            .iter()
            .all(|endpoint| endpoint.weight <= 0.0)
    }

    /// Weighted round-robin with failover: when the selected endpoint fails
    /// (after the client's own HTTP retries), the request is retried on each
    /// remaining endpoint before giving up, so one endpoint being down costs
    /// latency on its share of requests, not failed items.
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
        let mut tried = Vec::new();
        let mut last_err: Option<anyhow::Error> = None;
        loop {
            let selected = {
                let mut guard = self.state.lock().await;
                guard.select_client(&tried)
            };
            let Some((idx, client)) = selected else {
                return Err(last_err.unwrap_or_else(|| anyhow!("no inference endpoints available")));
            };
            match client
                .predict(
                    inference_id,
                    cache_key,
                    lru_size,
                    ttl_seconds,
                    max_batch,
                    prewarm,
                    inputs,
                )
                .await
            {
                Ok(output) => {
                    // Applied per endpoint: the endpoint that published the
                    // figure is the endpoint it is about. This is the client's
                    // transport gate; the job's `UnitBudget` reads the same
                    // header for the work budget, and both have to move
                    // together (docs/batch-calibration-design.md).
                    if let Some(items) = output.desired_in_flight_items {
                        client.observe_desired_in_flight(items);
                    }
                    return Ok(output);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        endpoint = idx,
                        "inference endpoint failed, trying another"
                    );
                    tried.push(idx);
                    last_err = Some(err);
                }
            }
        }
    }

    pub async fn load_model_all(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        prewarm: Option<bool>,
    ) -> Result<()> {
        let clients = {
            let guard = self.state.lock().await;
            guard
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.weight > 0.0)
                .map(|endpoint| endpoint.client.clone())
                .collect::<Vec<_>>()
        };
        if clients.is_empty() {
            bail!("no inference endpoints available");
        }
        // Partial availability is fine: endpoints that failed the explicit
        // load lazy-load on predict, which fails over past dead ones.
        let total = clients.len();
        // The error kept for the caller is the *most informative* one, not
        // the last: a load-failure cooldown is a typed verdict carrying the
        // model, the retry instant and the error that armed it, which a plain
        // 500 from another endpoint must not overwrite. Ties go to the last.
        let mut kept: Option<anyhow::Error> = None;
        let mut kept_is_cooldown = false;
        let mut failed = 0usize;
        for (idx, client) in clients.into_iter().enumerate() {
            if let Err(err) = client
                .load_model(inference_id, cache_key, lru_size, ttl_seconds, prewarm)
                .await
            {
                tracing::error!(
                    error = %err,
                    endpoint = idx,
                    inference_id,
                    "failed to load model on inference endpoint"
                );
                failed += 1;
                let is_cooldown =
                    inference_failure(&err).is_some_and(InferenceFailure::is_load_cooldown);
                if is_cooldown || !kept_is_cooldown {
                    kept_is_cooldown = is_cooldown;
                    kept = Some(err);
                }
            }
        }
        if failed == total {
            return Err(kept
                .unwrap_or_else(|| anyhow!("model load failed on all inference endpoints"))
                .context(format!(
                    "model {inference_id} failed to load on all {total} inference endpoints"
                )));
        }
        Ok(())
    }

    pub async fn unload_model_all(&self, inference_id: &str, cache_key: &str) -> Result<()> {
        let clients = {
            let guard = self.state.lock().await;
            guard
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.weight > 0.0)
                .map(|endpoint| endpoint.client.clone())
                .collect::<Vec<_>>()
        };
        for client in clients {
            let _ = client.unload_model(inference_id, cache_key).await;
        }
        Ok(())
    }
}

impl PoolState {
    /// Smooth weighted round-robin, skipping `exclude`d endpoints (already
    /// tried in this failover round).
    fn select_client(&mut self, exclude: &[usize]) -> Option<(usize, InferenceApiClient)> {
        let mut total_weight = 0.0;
        let mut best_idx: Option<usize> = None;
        let mut best_weight = f64::MIN;

        for (idx, endpoint) in self.endpoints.iter_mut().enumerate() {
            if endpoint.weight <= 0.0 || exclude.contains(&idx) {
                continue;
            }
            endpoint.current_weight += endpoint.weight;
            total_weight += endpoint.weight;
            if endpoint.current_weight > best_weight {
                best_weight = endpoint.current_weight;
                best_idx = Some(idx);
            }
        }

        let idx = best_idx?;
        let endpoint = &mut self.endpoints[idx];
        endpoint.current_weight -= total_weight;
        Some((idx, endpoint.client.clone()))
    }
}

#[derive(Clone)]
pub(crate) struct JobInferenceContext {
    pub primary: InferenceApiClient,
    pub pool: InferencePool,
    pub embedding_cache_size: usize,
    /// Concurrent extraction input loaders (from the gateway's `[jobs]`
    /// config).
    pub loader_concurrency: usize,
    /// Intermediate-data budget for in-flight extraction items, in KiB.
    pub intermediate_budget_kib: u32,
}

static JOB_INFERENCE_CONTEXT: OnceLock<JobInferenceContext> = OnceLock::new();

pub(crate) fn set_job_inference_context(context: JobInferenceContext) -> Result<()> {
    JOB_INFERENCE_CONTEXT
        .set(context)
        .map_err(|_| anyhow::anyhow!("job inference context already set"))?;
    Ok(())
}

pub(crate) fn job_inference_context() -> &'static JobInferenceContext {
    try_job_inference_context().expect("job inference context not initialized")
}

/// For callers that must tolerate an uninitialized context (background tasks
/// spawned by the job queue, which also runs in tests where no inference
/// endpoint exists).
pub(crate) fn try_job_inference_context() -> Option<&'static JobInferenceContext> {
    JOB_INFERENCE_CONTEXT.get()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::InferenceEndpointConfig;
    use axum::http::StatusCode;
    use axum::routing::put;
    use axum::{Json, Router};

    /// **C4: when every endpoint fails, the error the job keeps is the most
    /// informative one, not the last one.**
    ///
    /// A load-failure cooldown is a typed verdict carrying the model, the
    /// consecutive-failure count, the retry instant and the error that armed
    /// the window (R9). A plain 500 from another endpoint carries none of
    /// that. Keeping the last error — which is what this did — is how a job
    /// ends up telling the user nothing but "model load failed on all N
    /// inference endpoints".
    ///
    /// The cooldown is answered by the endpoint asked **first** here, so
    /// "keep the last" and "keep the most informative" give different answers
    /// and the test can tell them apart.
    #[tokio::test]
    async fn a_cooldown_survives_a_plainer_failure_on_another_endpoint() {
        async fn spawn(handler: Router) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, handler).await.unwrap();
            });
            format!("http://{addr}")
        }

        let cooling = spawn(Router::new().route(
            "/api/inference/load/{group}/{id}",
            put(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"detail": {
                        "kind": "load_cooldown",
                        "message": "model is in a load-failure cooldown",
                        "model": "group/model-a",
                        "failures": 3,
                        "retry_at": "2026-09-04T12:00:00Z",
                        "last_error": "CUDA out of memory",
                    }})),
                )
            }),
        ))
        .await;
        let broken = spawn(Router::new().route(
            "/api/inference/load/{group}/{id}",
            put(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        ))
        .await;

        let pool = InferencePool::new(vec![
            InferenceEndpointConfig {
                base_url: cooling,
                weight: 1.0,
                use_for_jobs: true,
            },
            InferenceEndpointConfig {
                base_url: broken,
                weight: 1.0,
                use_for_jobs: true,
            },
        ])
        .expect("pool builds");

        let err = pool
            .load_model_all("group/model-a", "key", 10, -1, None)
            .await
            .expect_err("both endpoints refuse the load");
        let failure = inference_failure(&err).expect("the typed cooldown survives the pool");
        assert!(failure.is_load_cooldown());
        assert_eq!(failure.model.as_deref(), Some("group/model-a"));
        assert_eq!(failure.failures, Some(3));
        assert_eq!(failure.retry_at.as_deref(), Some("2026-09-04T12:00:00Z"));
        assert_eq!(failure.last_error.as_deref(), Some("CUDA out of memory"));
        assert!(
            format!("{err}").contains("group/model-a"),
            "and the context names the model: {err}"
        );
    }
}
