use serde_json::json;

use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData};

use super::image_frames::load_base_frames;

pub(super) async fn build_md5_image_inputs(
    index_db: &str,
    item: &JobInputData,
) -> ApiResult<Vec<InferenceInput>> {
    let frames = load_base_frames(index_db, item).await?;
    let frame = frames.into_iter().next();

    Ok(vec![InferenceInput::new(
        json!({"md5": item.md5}),
        frame.map(InferenceFile::Bytes),
    )])
}
