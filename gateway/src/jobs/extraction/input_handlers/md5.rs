use serde_json::json;

use crate::inferio_client::InferenceInput;
use crate::jobs::extraction::{ApiResult, JobInputData};

pub(super) fn build_md5_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    Ok(vec![InferenceInput::new(json!({"md5": item.md5}), None)])
}
