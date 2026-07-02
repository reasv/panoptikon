use crate::api_error::ApiError;
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

mod clip;
mod embeddings;
mod tags;
mod text;
mod text_embedding;

#[derive(Debug)]
pub(super) enum OutputDisposition {
    Written,
    Skipped,
}

pub(super) fn empty_outputs_for(output_type: &str) -> ApiResult<PredictOutput> {
    match output_type {
        "tags" | "text" => Ok(PredictOutput::Json(Vec::new())),
        "clip" | "text-embedding" => Ok(PredictOutput::Binary(Vec::new())),
        other => Err(ApiError::bad_request(format!(
            "Unsupported output type: {other}"
        ))),
    }
}

impl PredictOutput {
    pub(super) fn into_json(self, label: &str) -> ApiResult<Vec<serde_json::Value>> {
        match self {
            PredictOutput::Json(values) => Ok(values),
            PredictOutput::Binary(_) => Err(ApiError::internal(format!(
                "Expected JSON output for {label}"
            ))),
        }
    }

    pub(super) fn into_binary(self, label: &str) -> ApiResult<Vec<Vec<u8>>> {
        match self {
            PredictOutput::Binary(values) => Ok(values),
            PredictOutput::Json(_) => Err(ApiError::internal(format!(
                "Expected binary output for {label}"
            ))),
        }
    }
}

pub(super) async fn handle_outputs(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    match model.output_type.as_str() {
        "tags" => tags::handle_tags_output(index_db, model, job_id, &item, outputs).await,
        "text" => text::handle_text_output(index_db, model, job_id, &item, outputs).await,
        "clip" => clip::handle_clip_output(index_db, model, job_id, &item, outputs).await,
        "text-embedding" => {
            text_embedding::handle_text_embedding_output(index_db, model, job_id, &item, outputs)
                .await
        }
        other => Err(ApiError::bad_request(format!(
            "Unsupported output type: {other}"
        ))),
    }
}
