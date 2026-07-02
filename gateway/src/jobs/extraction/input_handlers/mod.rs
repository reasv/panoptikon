use serde_json::Value;

use crate::api_error::ApiError;
use crate::inferio_client::InferenceInput;
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
    Ok(PreparedItem { item, inputs })
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
