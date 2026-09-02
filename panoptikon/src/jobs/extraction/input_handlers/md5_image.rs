use serde_json::json;

use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData};

use super::image_frames::load_base_frames;

/// `detect_outros` reaches the shared frame loader, which samples video the
/// same way `image_frames` does and is clamped the same way.
pub(super) async fn build_md5_image_inputs(
    index_db: &str,
    item: &JobInputData,
    detect_outros: bool,
) -> ApiResult<Vec<InferenceInput>> {
    let frames = load_base_frames(index_db, item, detect_outros).await?;
    let frame = frames.into_iter().next().map(|frame| frame.bytes);

    Ok(vec![InferenceInput::new(
        json!({"md5": item.md5}),
        frame.map(InferenceFile::Bytes),
    )])
}
