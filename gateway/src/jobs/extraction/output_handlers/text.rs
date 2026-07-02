use std::collections::HashSet;

use serde_json::Value;

use crate::db::extraction_write::TextEntry;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::OutputDisposition;

pub(super) async fn handle_text_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
) -> ApiResult<OutputDisposition> {
    let values = outputs.into_json("text")?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for (idx, value) in values.iter().enumerate() {
        let transcription = value
            .get("transcription")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if transcription.len() < 3 {
            continue;
        }
        let key = transcription.to_lowercase();
        if !seen.insert(key) {
            continue;
        }
        let confidence = value.get("confidence").and_then(Value::as_f64);
        let language = value
            .get("language")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let language_confidence = value.get("language_confidence").and_then(Value::as_f64);
        entries.push(TextEntry {
            index: idx as i64,
            text: transcription,
            language,
            language_confidence,
            confidence,
        });
    }

    call_index_db_writer(index_db, |reply| IndexDbWriterMessage::WriteTextOutput {
        job_id,
        setter_name: model.setter_name.clone(),
        item_sha256: item.sha256.clone(),
        entries: entries.clone(),
        reply,
    })
    .await?;
    Ok(OutputDisposition::Written)
}
