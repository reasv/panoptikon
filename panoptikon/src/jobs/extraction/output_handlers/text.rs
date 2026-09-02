use std::collections::HashSet;

use serde_json::Value;

use crate::db::extraction_write::TextEntry;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::inferio_client::PredictOutput;
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

use super::{OutputDisposition, input_index};

/// The stored `index` is the *input's* position (the frame/page the text was
/// read from), taken from the survivor map rather than the enumeration of the
/// outputs: a rejected input must leave a gap, not shift its successors. The
/// dedup/length filters below already produce gaps, so downstream consumers
/// tolerate them by construction.
pub(super) async fn handle_text_output(
    index_db: &str,
    model: &ModelMetadata,
    job_id: i64,
    item: &JobInputData,
    outputs: PredictOutput,
    survivors: Option<&[usize]>,
) -> ApiResult<OutputDisposition> {
    let values = outputs.into_json("text")?;
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for (position, value) in values.iter().enumerate() {
        let transcription = value
            .get("transcription")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        // Deliberately byte length, not chars (Python counted chars): the
        // filter exists to drop junk like "a" or "ok", and a 1-2 character
        // CJK result is a real word worth keeping.
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
            index: input_index(survivors, position)?,
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
