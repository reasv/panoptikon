use serde_json::json;

use crate::api_error::ApiError;
use crate::inferio_client::InferenceInput;
use crate::jobs::extraction::{ApiResult, JobInputData};

pub(super) fn build_extracted_text_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    let Some(text) = item.text.clone() else {
        return Err(ApiError::bad_request("Text input missing text field"));
    };
    Ok(vec![InferenceInput::new(json!({"text": text}), None)])
}
