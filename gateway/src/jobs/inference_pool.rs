use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::Mutex;

use crate::config::InferenceEndpointConfig;
use crate::inferio_client::{InferenceApiClient, InferenceInput, PredictOutput};

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

    pub async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard
            .endpoints
            .iter()
            .all(|endpoint| endpoint.weight <= 0.0)
    }

    pub async fn predict(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
        inputs: &[InferenceInput],
    ) -> Result<PredictOutput> {
        let client = {
            let mut guard = self.state.lock().await;
            guard
                .select_client()
                .ok_or_else(|| anyhow!("no inference endpoints available"))?
        };
        client
            .predict(inference_id, cache_key, lru_size, ttl_seconds, inputs)
            .await
    }

    pub async fn load_model_all(
        &self,
        inference_id: &str,
        cache_key: &str,
        lru_size: i64,
        ttl_seconds: i64,
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
        for client in clients {
            client
                .load_model(inference_id, cache_key, lru_size, ttl_seconds)
                .await?;
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
    fn select_client(&mut self) -> Option<InferenceApiClient> {
        let mut total_weight = 0.0;
        let mut best_idx: Option<usize> = None;
        let mut best_weight = f64::MIN;

        for (idx, endpoint) in self.endpoints.iter_mut().enumerate() {
            if endpoint.weight <= 0.0 {
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
        Some(endpoint.client.clone())
    }
}

#[derive(Clone)]
pub(crate) struct JobInferenceContext {
    pub primary: InferenceApiClient,
    pub pool: InferencePool,
    pub embedding_cache_size: usize,
}

static JOB_INFERENCE_CONTEXT: OnceLock<JobInferenceContext> = OnceLock::new();

pub(crate) fn set_job_inference_context(context: JobInferenceContext) -> Result<()> {
    JOB_INFERENCE_CONTEXT
        .set(context)
        .map_err(|_| anyhow::anyhow!("job inference context already set"))?;
    Ok(())
}

pub(crate) fn job_inference_context() -> &'static JobInferenceContext {
    JOB_INFERENCE_CONTEXT
        .get()
        .expect("job inference context not initialized")
}
