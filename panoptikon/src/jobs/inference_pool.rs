use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::Mutex;

use crate::config::InferenceEndpointConfig;
use crate::inferio_client::{InferenceApiClient, InferenceInput, PredictResponse};

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
    /// requests over a shared connection pool (HTTP/2 cleartext).
    ///
    /// Conservative by construction: an endpoint nothing has talked to yet
    /// has no known transport, and unknown reads as "not multiplexed". The
    /// answer sizes a *descriptor* budget, and being wrong in the optimistic
    /// direction is what run1 blocker F6 measured — `EMFILE`, a job that
    /// could not finish, and SQLite unable to open its own files.
    ///
    /// A pool with no enabled endpoint answers `false` for the same reason.
    pub async fn requests_are_multiplexed(&self) -> bool {
        let guard = self.state.lock().await;
        let mut enabled = 0usize;
        for endpoint in guard.endpoints.iter().filter(|e| e.weight > 0.0) {
            enabled += 1;
            match endpoint.client.known_transport() {
                Some(transport) if transport.is_multiplexed() => {}
                // One HTTP/1.1 endpoint is enough to put the per-request
                // socket cost back: the window is one budget across all of
                // them, so it has to be sized for the most expensive.
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
    /// remaining endpoint before giving up — one endpoint being down costs
    /// latency on its share of requests, not failed items (matching the
    /// Python distributed client's shard retry).
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
                    // The endpoint that published the figure is the endpoint
                    // the figure is about, so it is applied here rather than
                    // across the pool: a second endpoint serving a different
                    // model has its own opinion and its own gate.
                    //
                    // The job's `UnitBudget` reads the same header for the
                    // *work* budget; this is the client's transport gate,
                    // which the header must also be able to move or the work
                    // budget is asking for concurrency the transport will not
                    // carry (run2 S1: 1 632 items asked for, 200 delivered).
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
        // Partial availability is fine (like Python's _all_or_ignore):
        // endpoints that failed the explicit load lazy-load on predict, and
        // predict fails over past endpoints that are down entirely.
        let total = clients.len();
        let mut last_err = None;
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
                last_err = Some(err);
            }
        }
        if failed == total {
            return Err(last_err
                .unwrap_or_else(|| anyhow!("model load failed on all inference endpoints"))
                .context(format!(
                    "model load failed on all {total} inference endpoints"
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
