use crate::db::extraction_write::EmbeddingEntry;
use crate::db::index_writer::OutputWritePayload;
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::embeddings::{parse_embedding_json, parse_npy_to_f32, serialize_f32};
use super::{OutputDisposition, input_index, submit_output};

/// The stored `index` is the *input's* position — a video frame or PDF page
/// number — so it comes from the survivor map, never from the enumeration of
/// the outputs that came back (an item whose second frame was rejected still
/// stores frames 0, 2, 3).
pub(super) async fn handle_clip_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
    survivors: Option<&[usize]>,
) -> ApiResult<OutputDisposition> {
    let mut entries = Vec::new();
    match outputs {
        PredictOutput::Binary(buffers) => {
            for (position, buffer) in buffers.iter().enumerate() {
                let embedding = parse_npy_to_f32(buffer)?;
                entries.push(EmbeddingEntry {
                    index: input_index(survivors, position)?,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
        PredictOutput::Json(values) => {
            for (position, value) in values.iter().enumerate() {
                let embedding = parse_embedding_json(value)?;
                entries.push(EmbeddingEntry {
                    index: input_index(survivors, position)?,
                    embedding: serialize_f32(&embedding),
                });
            }
        }
    }

    submit_output(
        index_db,
        model,
        job_id,
        &item.sha256,
        OutputWritePayload::Clip { entries },
    )
    .await?;
    Ok(OutputDisposition::Written)
}
