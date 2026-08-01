use serde_json::Value;

use crate::api_error::ApiError;
use crate::inferio::worker::FRAME_INPUT_BYTES_BUDGET;
use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata, PreparedItem};

mod audio;
mod extracted_text;
mod image_frames;
mod md5;
mod md5_image;
mod sha256_md5_path;

pub(super) async fn prepare_item(
    index_db: &str,
    model: &ModelMetadata,
    item: JobInputData,
) -> ApiResult<PreparedItem> {
    let inputs = match model.input_handler.as_str() {
        "image_frames" => image_frames::build_image_frames_inputs(index_db, &item, model).await?,
        "audio_tracks" => audio::build_audio_tracks_inputs(&item, model).await?,
        "audio_files" => audio::build_audio_files_inputs(&item, model).await?,
        "extracted_text" => extracted_text::build_extracted_text_inputs(&item)?,
        "md5" => md5::build_md5_inputs(&item)?,
        "md5_image" => md5_image::build_md5_image_inputs(index_db, &item).await?,
        "sha256_md5_path" => sha256_md5_path::build_sha256_md5_path_inputs(&item)?,
        handler => {
            return Err(ApiError::bad_request(format!(
                "Unknown input handler: {handler}"
            )));
        }
    };
    check_frame_budget(&inputs, FRAME_INPUT_BYTES_BUDGET).await?;
    Ok(PreparedItem { item, inputs })
}

/// One input is the smallest unit a predict request can be split into, so a
/// single input whose wire size exceeds the transport's frame budget can
/// never be inferred on this machine no matter how it is batched. That is a
/// verdict about this machine's limits, not about the media, and it is
/// settled here — before any predict is attempted — so it lands in the
/// ledger as `resource` (skip_after 1, clearable by a retry directive if
/// the frame limit ever moves) instead of failing the job transiently on
/// every run.
async fn check_frame_budget(inputs: &[InferenceInput], budget: usize) -> ApiResult<()> {
    for input in inputs {
        let file_bytes = match &input.file {
            Some(InferenceFile::Bytes(bytes)) => bytes.len(),
            // Path inputs are read at request time; size them from metadata.
            // A stat failure is never turned into a persisted verdict — the
            // predict call will fail transiently on its own if the file is
            // really unreadable.
            Some(InferenceFile::Path(path)) => match tokio::fs::metadata(path).await {
                Ok(meta) => usize::try_from(meta.len()).unwrap_or(usize::MAX),
                Err(_) => 0,
            },
            None => 0,
        };
        let data_bytes = input.data.to_string().len();
        if file_bytes.saturating_add(data_bytes) > budget {
            const MIB: usize = 1024 * 1024;
            return Err(ApiError::resource(format!(
                "a single inference input (~{} MiB) exceeds the worker transport frame budget \
                 of {} MiB and cannot be sent",
                (file_bytes + data_bytes) / MIB,
                budget / MIB
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The pre-send budget check is what turns "this input can never fit
    /// one worker frame" into a persisted `resource` verdict instead of an
    /// eternally-retried transient predict failure (the CLAP whole-track
    /// case). Inputs under the budget pass untouched; the file bytes and
    /// the JSON data both count against it.
    #[tokio::test]
    async fn over_budget_single_input_is_a_resource_verdict() {
        let input = |bytes: usize| {
            InferenceInput::new(json!({}), Some(InferenceFile::Bytes(vec![0u8; bytes])))
        };
        assert!(check_frame_budget(&[input(1024)], 4096).await.is_ok());

        let err = check_frame_budget(&[input(8192)], 4096)
            .await
            .expect_err("over-budget input must be refused");
        assert_eq!(err.persisted_class(), Some("resource"));
        assert_eq!(err.skip_after(), 1);

        // The budget is per input, not per item: two inputs that fit
        // individually pass even when their sum does not (the transport
        // splits them across frames).
        assert!(
            check_frame_budget(&[input(3000), input(3000)], 4096)
                .await
                .is_ok()
        );
    }
}

pub(super) fn apply_threshold(
    mut inputs: Vec<InferenceInput>,
    threshold: Option<f64>,
) -> Vec<InferenceInput> {
    let Some(threshold) = threshold else {
        return inputs;
    };
    for input in &mut inputs {
        if let Value::Object(map) = &mut input.data {
            map.insert("threshold".to_string(), Value::from(threshold));
        } else {
            input.data = Value::Object(serde_json::Map::from_iter([(
                "threshold".to_string(),
                Value::from(threshold),
            )]));
        }
    }
    inputs
}
