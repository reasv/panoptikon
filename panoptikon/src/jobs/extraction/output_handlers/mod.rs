use crate::api_error::ApiError;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
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

/// Writes the placeholder row for an item that produced zero inputs, marking
/// it processed. Only for the zero-input case: an *inference* response with
/// missing outputs must go through `handle_outputs`, where it can be
/// distinguished from this and treated as a failure.
pub(super) async fn write_placeholder(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
) -> ApiResult<OutputDisposition> {
    let setter_name = &model.setter_name;
    let item_sha256 = &item.sha256;
    match model.output_type.as_str() {
        "tags" => {
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTagsOutput {
                job_id,
                setter_name: setter_name.clone(),
                item_sha256: item_sha256.clone(),
                tags: Vec::new(),
                text_entries: Vec::new(),
                reply,
            })
            .await?;
        }
        "text" => {
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTextOutput {
                job_id,
                setter_name: setter_name.clone(),
                item_sha256: item_sha256.clone(),
                entries: Vec::new(),
                reply,
            })
            .await?;
        }
        "clip" => {
            call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteClipOutput {
                job_id,
                setter_name: setter_name.clone(),
                item_sha256: item_sha256.clone(),
                entries: Vec::new(),
                reply,
            })
            .await?;
        }
        "text-embedding" => {
            call_index_db_writer(index_db, |reply| {
                IndexDbWriterMessage::WriteTextEmbeddingOutput {
                    job_id,
                    setter_name: setter_name.clone(),
                    item_sha256: item_sha256.clone(),
                    source_data_id: item.data_id,
                    entries: Vec::new(),
                    reply,
                }
            })
            .await?;
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "Unsupported output type: {other}"
            )));
        }
    }
    Ok(OutputDisposition::Written)
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

/// Maps the position of a surviving output back onto the input it came from.
///
/// `survivors` is the map built by `surviving_input_indices`: absent means no
/// slot errored and the mapping is the identity, which is exactly what every
/// pre-error-slot response gets. Present, it is the original input positions
/// in output order, so a partial item's stored `index` stays the page/frame
/// number it always was instead of being renumbered by the drop.
///
/// A position past the end of the map means the peer returned more surviving
/// outputs than inputs survived — a protocol impossibility that the local
/// orchestrator's count check already rules out, but a remote server can
/// produce. It surfaces as an error (transient, never persisted) rather than
/// falling back to the raw position, which would silently attribute the
/// output to whatever frame happens to live at that index.
pub(super) fn input_index(survivors: Option<&[usize]>, position: usize) -> ApiResult<i64> {
    match survivors {
        Some(map) => map
            .get(position)
            .copied()
            .map(|index| index as i64)
            .ok_or_else(|| {
                ApiError::internal(format!(
                    "output position {position} exceeds the {} surviving inputs",
                    map.len()
                ))
            }),
        None => Ok(position as i64),
    }
}

pub(super) async fn handle_outputs(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: JobInputData,
    outputs: PredictOutput,
    survivors: Option<&[usize]>,
) -> ApiResult<OutputDisposition> {
    match model.output_type.as_str() {
        // Tags aggregate across the whole item and text embeddings are
        // single-input by construction, so neither stores a per-input index.
        "tags" => tags::handle_tags_output(index_db, model, job_id, &item, outputs).await,
        "text" => {
            text::handle_text_output(index_db, model, job_id, &item, outputs, survivors).await
        }
        "clip" => {
            clip::handle_clip_output(index_db, model, job_id, &item, outputs, survivors).await
        }
        "text-embedding" => {
            text_embedding::handle_text_embedding_output(index_db, model, job_id, &item, outputs)
                .await
        }
        other => Err(ApiError::bad_request(format!(
            "Unsupported output type: {other}"
        ))),
    }
}
