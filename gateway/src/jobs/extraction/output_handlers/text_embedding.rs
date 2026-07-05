use crate::api_error::ApiError;
use crate::db::extraction_write::EmbeddingEntry;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::OutputDisposition;
use super::embeddings::{parse_npy_to_f32_rows, serialize_f32};

pub(super) async fn handle_text_embedding_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let source_data_id = item.data_id;
    let mut entries = Vec::new();
    let buffers = outputs.into_binary("text-embedding")?;
    // The zero-input placeholder never reaches this handler, so anything
    // other than exactly one npy for the single text input is an inference
    // anomaly: fail the item so it stays retryable instead of writing a
    // placeholder that would permanently mark it processed.
    if buffers.len() != 1 {
        return Err(ApiError::internal(format!(
            "Text embedding output mismatch: expected 1 buffer, got {}",
            buffers.len()
        )));
    }
    let embedding_rows = parse_npy_to_f32_rows(&buffers[0])?;
    for (idx, embedding) in embedding_rows.into_iter().enumerate() {
        entries.push(EmbeddingEntry {
            index: idx as i64,
            embedding: serialize_f32(&embedding),
        });
    }

    call_index_db_writer(index_db, |reply| {
        IndexDbWriterMessage::WriteTextEmbeddingOutput {
            job_id,
            setter_name: model.setter_name.clone(),
            item_sha256: item.sha256.clone(),
            source_data_id,
            entries: entries.clone(),
            reply,
        }
    })
    .await?;
    Ok(OutputDisposition::Written)
}
