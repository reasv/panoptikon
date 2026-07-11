use crate::db::extraction_write::EmbeddingEntry;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::OutputDisposition;
use super::embeddings::{parse_embedding_json, parse_npy_to_f32, serialize_f32};

pub(super) async fn handle_clip_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let mut entries = Vec::new();
    match outputs {
        PredictOutput::Binary(buffers) => {
            for (idx, buffer) in buffers.iter().enumerate() {
                let embedding = parse_npy_to_f32(buffer)?;
                entries.push(EmbeddingEntry {
                    index: idx as i64,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
        PredictOutput::Json(values) => {
            for (idx, value) in values.iter().enumerate() {
                let embedding = parse_embedding_json(value)?;
                entries.push(EmbeddingEntry {
                    index: idx as i64,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
    }

    call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteClipOutput {
        job_id,
        setter_name: model.setter_name.clone(),
        item_sha256: item.sha256.clone(),
        entries: entries.clone(),
        reply,
    })
    .await?;
    Ok(OutputDisposition::Written)
}
