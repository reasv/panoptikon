use serde_json::json;

use crate::inferio_client::InferenceInput;
use crate::jobs::extraction::{ApiResult, JobInputData};

pub(super) fn build_sha256_md5_path_inputs(item: &JobInputData) -> ApiResult<Vec<InferenceInput>> {
    Ok(vec![InferenceInput::new(
        json!({"sha256": item.sha256, "md5": item.md5, "path": item.path}),
        None,
    )])
}
