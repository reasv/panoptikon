use std::path::PathBuf;

use image::AnimationDecoder;
use image::codecs::gif::GifDecoder;
use image::{DynamicImage, GenericImageView};
use serde_json::{Value, json};

use crate::api_error::ApiError;
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::open_index_db_read_no_user_data;
use crate::db::storage::{StoredImage, get_frames_bytes};
use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};
use crate::jobs::files::{FRAME_PROCESS_VERSION, stderr_tail};

/// A frame ready to be sent to inference. PDF pages and HTML screenshots
/// carry their own pixel dimensions (each page differs from the item's stored
/// size); frames without dimensions are sliced using the item's stored
/// width/height, mirroring the Python loader.
pub(super) struct BaseFrame {
    pub bytes: Vec<u8>,
    pub width: Option<i64>,
    pub height: Option<i64>,
}

impl BaseFrame {
    fn sized_by_item(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            width: None,
            height: None,
        }
    }
}

pub(super) async fn build_image_frames_inputs(
    index_db: &str,
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    let opts = &model.input_handler_opts;
    let max_frames = opts.get("max_frames").and_then(Value::as_i64).unwrap_or(4) as usize;
    let slice_frames = opts
        .get("slice_frames")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let slice_settings = if slice_frames {
        // An absent slice_settings key means full defaults (like Python's
        // from_dict({})), not "slicing disabled" — slice_frames alone turns
        // slicing on.
        let value = opts
            .get("slice_settings")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        Some(ImageSliceSettings::from_value(&value)?)
    } else {
        None
    };

    let frames = load_base_frames(index_db, item).await?;
    if frames.is_empty() {
        return Ok(Vec::new());
    }

    let mut sliced: Vec<Vec<u8>> = Vec::new();
    for frame in frames {
        let (width, height) = match (frame.width, frame.height) {
            (Some(width), Some(height)) => (Some(width), Some(height)),
            _ => (item.width, item.height),
        };
        sliced.extend(slice_target_size(
            vec![frame.bytes],
            width,
            height,
            slice_settings.as_ref(),
        )?);
    }
    let mut outputs = Vec::new();
    for frame in sliced.into_iter().take(max_frames) {
        outputs.push(InferenceInput::new(
            json!({}),
            Some(InferenceFile::Bytes(frame)),
        ));
    }
    Ok(outputs)
}

pub(super) async fn load_base_frames(
    index_db: &str,
    item: &JobInputData,
) -> ApiResult<Vec<BaseFrame>> {
    // Mirrors the Python image_loader guard: absurdly small images are
    // skipped outright (placeholder written) for every media type.
    if let (Some(width), Some(height)) = (item.width, item.height) {
        if width < 3 || height < 3 {
            tracing::warn!(
                path = %item.path,
                sha256 = %item.sha256,
                width,
                height,
                "image too small, skipping"
            );
            return Ok(Vec::new());
        }
    }
    if item.item_type.starts_with("image/gif") {
        return gif_to_frames(&item.path);
    }
    if item.item_type.starts_with("image") {
        let buffer = tokio::fs::read(&item.path).await.map_err(|err| {
            tracing::error!(error = %err, path = %item.path, "failed to read image");
            ApiError::internal("Failed to read image")
        })?;
        ensure_image_readable(&buffer, &item.path)?;
        return Ok(vec![BaseFrame::sized_by_item(buffer)]);
    }
    if item.item_type.starts_with("video") {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        let cached = get_frames_bytes(&mut conn, &item.sha256)
            .await
            .unwrap_or_default();
        if !cached.is_empty() {
            return Ok(cached.into_iter().map(BaseFrame::sized_by_item).collect());
        }
        if item.duration.unwrap_or(0.0) > 0.0 && item.video_tracks.unwrap_or(0) > 0 {
            let extracted = tokio::task::spawn_blocking({
                let path = item.path.clone();
                move || extract_video_frames(&path, 4)
            })
            .await
            .map_err(|_| ApiError::internal("Failed to extract frames"))??;
            let frames = extracted
                .iter()
                .map(encode_jpeg)
                .collect::<Result<Vec<_>, ApiError>>()?;

            let mut stored = Vec::with_capacity(extracted.len());
            for (idx, img) in extracted.iter().enumerate() {
                stored.push(StoredImage {
                    idx: idx as i64,
                    width: img.width() as i64,
                    height: img.height() as i64,
                    bytes: encode_jpeg(img)?,
                });
            }
            let _ = call_index_db_writer(index_db, |reply| IndexDbWriterMessage::StoreFrames {
                sha256: item.sha256.clone(),
                mime_type: item.item_type.clone(),
                process_version: FRAME_PROCESS_VERSION,
                frames: stored.clone(),
                reply,
            })
            .await;
            return Ok(frames.into_iter().map(BaseFrame::sized_by_item).collect());
        }
        return Ok(Vec::new());
    }
    if item.item_type.starts_with("application/pdf") {
        return render_pdf_frames(&item.path).await;
    }
    if item.item_type.starts_with("text/html") {
        return render_html_frames(&item.path).await;
    }
    Ok(Vec::new())
}

/// Header-level readability check mirroring Python's `is_image_readable`
/// (PIL `verify()` with truncated images accepted): rejects files whose
/// header cannot even be parsed, without decoding pixel data. Without this,
/// a corrupt file reaches the inference server where it can fail an entire
/// coalesced GPU batch instead of just this item.
fn ensure_image_readable(buffer: &[u8], path: &str) -> ApiResult<()> {
    image::ImageReader::new(std::io::Cursor::new(buffer))
        .with_guessed_format()
        .map_err(|err| {
            tracing::error!(error = %err, path, "image format detection failed");
            ApiError::internal(format!("Image {path} is not readable"))
        })?
        .into_dimensions()
        .map_err(|err| {
            tracing::error!(error = %err, path, "image is not readable");
            ApiError::internal(format!("Image {path} is not readable"))
        })?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ImageSliceSettings {
    mode: String,
    ratio_larger: f64,
    ratio_smaller: f64,
    max_multiplier: f64,
    target_multiplier: f64,
    minimum_size: f64,
    pixel_target_size: f64,
    pixel_max_size: f64,
}

impl ImageSliceSettings {
    fn from_value(value: &Value) -> ApiResult<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| ApiError::bad_request("slice_settings must be an object"))?;
        Ok(Self {
            mode: obj
                .get("mode")
                .and_then(Value::as_str)
                .unwrap_or("aspect-ratio")
                .to_string(),
            ratio_larger: obj
                .get("ratio_larger")
                .and_then(Value::as_f64)
                .unwrap_or(16.0),
            ratio_smaller: obj
                .get("ratio_smaller")
                .and_then(Value::as_f64)
                .unwrap_or(9.0),
            max_multiplier: obj
                .get("max_multiplier")
                .and_then(Value::as_f64)
                .unwrap_or(2.0),
            target_multiplier: obj
                .get("target_multiplier")
                .and_then(Value::as_f64)
                .unwrap_or(1.5),
            minimum_size: obj
                .get("minimum_size")
                .and_then(Value::as_f64)
                .unwrap_or(1024.0),
            pixel_target_size: obj
                .get("pixel_target_size")
                .and_then(Value::as_f64)
                .unwrap_or(1024.0),
            pixel_max_size: obj
                .get("pixel_max_size")
                .and_then(Value::as_f64)
                .unwrap_or(4096.0),
        })
    }
}

fn slice_target_size(
    input_images: Vec<Vec<u8>>,
    width: Option<i64>,
    height: Option<i64>,
    settings: Option<&ImageSliceSettings>,
) -> ApiResult<Vec<Vec<u8>>> {
    let (Some(width), Some(height), Some(settings)) = (width, height, settings) else {
        return Ok(input_images);
    };
    let width = width as f64;
    let height = height as f64;
    match settings.mode.as_str() {
        "aspect-ratio" => {
            if width.max(height) <= settings.minimum_size
                || !is_excessive_ratio(width, height, settings)
            {
                return Ok(input_images);
            }
            let slices = calculate_slices_needed(width, height, settings);
            let mut output = Vec::new();
            for image in input_images {
                output.extend(slice_image(&image, slices)?);
            }
            Ok(output)
        }
        "pixels" => {
            if width.max(height) <= settings.pixel_max_size {
                return Ok(input_images);
            }
            let (rows, cols) = grid_for_pixels(width, height, settings);
            let mut output = Vec::new();
            for image in input_images {
                output.extend(slice_image_grid(&image, rows, cols)?);
            }
            Ok(output)
        }
        _ => Ok(input_images),
    }
}

fn is_excessive_ratio(width: f64, height: f64, settings: &ImageSliceSettings) -> bool {
    let image_ratio = if width >= height {
        width / height
    } else {
        height / width
    };
    let target_ratio = settings.ratio_larger / settings.ratio_smaller;
    image_ratio > (target_ratio * settings.max_multiplier)
}

fn calculate_slices_needed(width: f64, height: f64, settings: &ImageSliceSettings) -> usize {
    let is_landscape = width >= height;
    let image_ratio = if is_landscape {
        width / height
    } else {
        height / width
    };
    let base_ratio = settings.ratio_larger / settings.ratio_smaller;
    let max_ratio = base_ratio * settings.max_multiplier;
    let target_ratio = base_ratio * settings.target_multiplier;
    if image_ratio <= max_ratio {
        return 1;
    }
    ((image_ratio / target_ratio).ceil() as usize).max(1)
}

fn slice_image(image_bytes: &[u8], num_slices: usize) -> ApiResult<Vec<Vec<u8>>> {
    let format = slice_output_format(image_bytes);
    let image = load_dynamic_image(image_bytes)?;
    let (width, height) = image.dimensions();
    let mut output = Vec::new();
    if width >= height {
        let slice_width = width / num_slices as u32;
        for idx in 0..num_slices {
            let start = idx as u32 * slice_width;
            let end = if idx == num_slices - 1 {
                width
            } else {
                start + slice_width
            };
            let cropped = image.crop_imm(start, 0, end - start, height);
            output.push(encode_slice(&cropped, format)?);
        }
    } else {
        let slice_height = height / num_slices as u32;
        for idx in 0..num_slices {
            let start = idx as u32 * slice_height;
            let end = if idx == num_slices - 1 {
                height
            } else {
                start + slice_height
            };
            let cropped = image.crop_imm(0, start, width, end - start);
            output.push(encode_slice(&cropped, format)?);
        }
    }
    Ok(output)
}

/// Slices are re-encoded in the source format like the Python loader
/// (`img.save(..., format=img.format)`), so a sliced PNG keeps its alpha
/// channel instead of being flattened into a JPEG. Unknown formats default
/// to PNG, matching Python's fallback.
fn slice_output_format(image_bytes: &[u8]) -> image::ImageFormat {
    image::guess_format(image_bytes).unwrap_or(image::ImageFormat::Png)
}

fn encode_slice(image: &DynamicImage, format: image::ImageFormat) -> ApiResult<Vec<u8>> {
    if format == image::ImageFormat::Jpeg {
        return encode_jpeg(image);
    }
    let mut buffer = std::io::Cursor::new(Vec::new());
    if image.write_to(&mut buffer, format).is_ok() {
        return Ok(buffer.into_inner());
    }
    // Formats without encoder support (or whose encoder rejects this color
    // type) fall back to PNG rather than dropping image data.
    let mut buffer = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut buffer, image::ImageFormat::Png)
        .map_err(|err| {
            tracing::error!(error = %err, "failed to encode image slice");
            ApiError::internal("Failed to encode image slice")
        })?;
    Ok(buffer.into_inner())
}

fn grid_for_pixels(width: f64, height: f64, settings: &ImageSliceSettings) -> (usize, usize) {
    let rows = (height / settings.pixel_target_size).ceil().max(1.0) as usize;
    let cols = (width / settings.pixel_target_size).ceil().max(1.0) as usize;
    (rows, cols)
}

fn slice_image_grid(image_bytes: &[u8], rows: usize, cols: usize) -> ApiResult<Vec<Vec<u8>>> {
    let format = slice_output_format(image_bytes);
    let image = load_dynamic_image(image_bytes)?;
    let (width, height) = image.dimensions();
    let tile_w = width as f64 / cols as f64;
    let tile_h = height as f64 / rows as f64;
    let mut output = Vec::new();
    for row in 0..rows {
        for col in 0..cols {
            let left = (col as f64 * tile_w).round() as u32;
            let top = (row as f64 * tile_h).round() as u32;
            let right = ((col + 1) as f64 * tile_w).round() as u32;
            let bottom = ((row + 1) as f64 * tile_h).round() as u32;
            let cropped = image.crop_imm(left, top, right - left, bottom - top);
            output.push(encode_slice(&cropped, format)?);
        }
    }
    Ok(output)
}

fn load_dynamic_image(buffer: &[u8]) -> ApiResult<DynamicImage> {
    image::load_from_memory(buffer).map_err(|err| {
        tracing::error!(error = %err, "failed to decode image");
        ApiError::internal("Failed to decode image")
    })
}

fn encode_jpeg(image: &DynamicImage) -> ApiResult<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buffer, 85);
    let rgb = image.to_rgb8();
    encoder
        .encode(
            &rgb,
            rgb.width(),
            rgb.height(),
            image::ColorType::Rgb8.into(),
        )
        .map_err(|err| {
            tracing::error!(error = %err, "failed to encode image");
            ApiError::internal("Failed to encode image")
        })?;
    Ok(buffer)
}

fn gif_to_frames(path: &str) -> ApiResult<Vec<BaseFrame>> {
    let file = std::fs::File::open(path).map_err(|err| {
        tracing::error!(error = %err, "failed to open gif");
        ApiError::internal("Failed to open gif")
    })?;
    let decoder = GifDecoder::new(std::io::BufReader::new(file)).map_err(|err| {
        tracing::error!(error = %err, "failed to decode gif");
        ApiError::internal("Failed to decode gif")
    })?;
    let frames = decoder.into_frames().collect_frames().map_err(|err| {
        tracing::error!(error = %err, "failed to collect gif frames");
        ApiError::internal("Failed to decode gif")
    })?;
    if frames.is_empty() {
        return Ok(Vec::new());
    }
    let total_frames = frames.len();
    let step = std::cmp::max(total_frames / 4, 1);
    let mut output = Vec::new();
    for (idx, frame) in frames.into_iter().enumerate() {
        if idx % step == 0 {
            let image: image::RgbaImage = frame.into_buffer();
            let image = DynamicImage::ImageRgba8(image);
            output.push(BaseFrame::sized_by_item(encode_jpeg(&image)?));
        }
        if output.len() >= 4 {
            break;
        }
    }
    Ok(output)
}

fn extract_video_frames(path: &str, num_frames: usize) -> ApiResult<Vec<DynamicImage>> {
    let duration = probe_duration(path)?;
    // The caller already checked the DB-recorded duration; a zero here means
    // the file on disk disagrees (truncated or corrupt). Fail the item so it
    // is retried instead of being permanently marked processed as "no data".
    if duration <= 0.0 {
        return Err(ApiError::internal("Video has no probeable duration"));
    }
    let interval = duration / num_frames as f64;
    let temp_dir = temp_dir_path();
    std::fs::create_dir_all(&temp_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create temp dir");
        ApiError::internal("Failed to extract frames")
    })?;
    let result = extract_video_frames_into(path, num_frames, interval, &temp_dir);
    if let Err(err) = std::fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp frame dir");
    }
    result
}

fn extract_video_frames_into(
    path: &str,
    num_frames: usize,
    interval: f64,
    temp_dir: &std::path::Path,
) -> ApiResult<Vec<DynamicImage>> {
    let output_pattern = temp_dir.join("frame_%04d.png");
    // stdout is silenced, but stderr is captured so a failure can say why
    // (corrupt file, missing codec, disk full); it is only surfaced on a
    // non-zero exit.
    let output = std::process::Command::new("ffmpeg")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{interval}"))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .stdout(std::process::Stdio::null())
        .output()
        .map_err(|err| {
            tracing::error!(error = %err, "ffmpeg failed");
            ApiError::internal("Failed to extract frames")
        })?;
    if !output.status.success() {
        tracing::error!(
            path,
            stderr = %stderr_tail(&output.stderr),
            "ffmpeg failed to extract frames"
        );
        return Err(ApiError::internal("ffmpeg failed to extract frames"));
    }
    let mut paths = std::fs::read_dir(temp_dir)
        .map_err(|err| ApiError::internal(format!("Failed to read frames: {err}")))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("png"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut frames = Vec::new();
    for frame_path in paths.into_iter().take(num_frames) {
        // These PNGs were just written by our own ffmpeg run; one being
        // unreadable means something is broken (disk full, races), so fail
        // the item rather than silently tagging it from fewer frames.
        let image = image::open(&frame_path)
            .map_err(|err| ApiError::internal(format!("Failed to read extracted frame: {err}")))?;
        frames.push(image);
    }
    Ok(frames)
}

fn probe_duration(path: &str) -> ApiResult<f64> {
    let output = std::process::Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=duration")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg(path)
        .output()
        .map_err(|err| {
            tracing::error!(error = %err, "ffprobe failed");
            ApiError::internal("Failed to probe video")
        })?;
    if !output.status.success() {
        return Err(ApiError::internal("ffprobe failed"));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse::<f64>().map_err(|err| {
        tracing::error!(error = %err, "failed to parse duration");
        ApiError::internal("Failed to probe video")
    })
}

fn temp_dir_path() -> PathBuf {
    // PID plus a process-local counter rules out collisions between
    // concurrent extractions and between gateway instances; the timestamp
    // alone could repeat across calls or processes, and a collision means
    // one call's cleanup deletes the other's frames mid-extraction.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let base = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    base.join(format!(
        "panoptikon-extract-{}-{nanos:x}-{unique}",
        std::process::id()
    ))
}

/// Renders every PDF page natively via the shared pdfium binding (same
/// library the scan pipeline uses for thumbnails). Any failure — including
/// pdfium not being installed — is an error so the item is recorded as
/// failed and retried on the next run, never silently marked processed.
async fn render_pdf_frames(path: &str) -> ApiResult<Vec<BaseFrame>> {
    let owned = path.to_string();
    let pages = tokio::task::spawn_blocking(move || {
        crate::jobs::files::render_pdf_pages(std::path::Path::new(&owned))
    })
    .await
    .map_err(|_| ApiError::internal("PDF render task failed"))?
    .map_err(|err| {
        tracing::error!(error = %err, path, "failed to render PDF");
        ApiError::internal("Failed to render PDF")
    })?;
    let mut frames = Vec::with_capacity(pages.len());
    for page in pages {
        frames.push(BaseFrame {
            width: Some(page.width() as i64),
            height: Some(page.height() as i64),
            bytes: encode_jpeg(&page)?,
        });
    }
    Ok(frames)
}

/// Renders an HTML file via the shared headless-browser screenshot path used
/// by the scan pipeline (replacing the Python weasyprint HTML->PDF chain).
/// Failure — including no browser being installed — is an error so the item
/// is recorded as failed and retried, never silently marked processed.
async fn render_html_frames(path: &str) -> ApiResult<Vec<BaseFrame>> {
    let owned = path.to_string();
    let shot = tokio::task::spawn_blocking(move || {
        crate::jobs::files::render_html_screenshot(std::path::Path::new(&owned))
    })
    .await
    .map_err(|_| ApiError::internal("HTML render task failed"))?
    .ok_or_else(|| {
        tracing::error!(path, "failed to render HTML page");
        ApiError::internal("Failed to render HTML page")
    })?;
    Ok(vec![BaseFrame {
        width: Some(shot.width() as i64),
        height: Some(shot.height() as i64),
        bytes: encode_jpeg(&shot)?,
    }])
}
