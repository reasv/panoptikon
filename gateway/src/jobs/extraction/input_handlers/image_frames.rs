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
        opts.get("slice_settings")
            .map(ImageSliceSettings::from_value)
            .transpose()?
    } else {
        None
    };

    if let (Some(width), Some(height)) = (item.width, item.height) {
        if width < 3 || height < 3 {
            return Ok(Vec::new());
        }
    }

    let frames = load_base_frames(index_db, item).await?;
    if frames.is_empty() {
        return Ok(Vec::new());
    }

    let sliced = slice_target_size(frames, item.width, item.height, slice_settings.as_ref())?;
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
) -> ApiResult<Vec<Vec<u8>>> {
    if item.item_type.starts_with("image/gif") {
        return gif_to_frames(&item.path);
    }
    if item.item_type.starts_with("image") {
        let buffer = tokio::fs::read(&item.path).await.map_err(|err| {
            tracing::error!(error = %err, path = %item.path, "failed to read image");
            ApiError::internal("Failed to read image")
        })?;
        return Ok(vec![buffer]);
    }
    if item.item_type.starts_with("video") {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        let cached = get_frames_bytes(&mut conn, &item.sha256)
            .await
            .unwrap_or_default();
        if !cached.is_empty() {
            return Ok(cached);
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
                process_version: 1,
                frames: stored.clone(),
                reply,
            })
            .await;
            return Ok(frames);
        }
        return Ok(Vec::new());
    }
    if item.item_type.starts_with("application/pdf") {
        return render_pdf_frames(&item.path);
    }
    if item.item_type.starts_with("text/html") {
        return render_html_frames(&item.path);
    }
    Ok(Vec::new())
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
            output.push(encode_jpeg(&cropped)?);
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
            output.push(encode_jpeg(&cropped)?);
        }
    }
    Ok(output)
}

fn grid_for_pixels(width: f64, height: f64, settings: &ImageSliceSettings) -> (usize, usize) {
    let rows = (height / settings.pixel_target_size).ceil().max(1.0) as usize;
    let cols = (width / settings.pixel_target_size).ceil().max(1.0) as usize;
    (rows, cols)
}

fn slice_image_grid(image_bytes: &[u8], rows: usize, cols: usize) -> ApiResult<Vec<Vec<u8>>> {
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
            output.push(encode_jpeg(&cropped)?);
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

fn gif_to_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
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
            output.push(encode_jpeg(&image)?);
        }
        if output.len() >= 4 {
            break;
        }
    }
    Ok(output)
}

fn extract_video_frames(path: &str, num_frames: usize) -> ApiResult<Vec<DynamicImage>> {
    let duration = probe_duration(path)?;
    if duration <= 0.0 {
        return Ok(Vec::new());
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
    let status = std::process::Command::new("ffmpeg")
        .arg("-i")
        .arg(path)
        .arg("-vf")
        .arg(format!("fps=1/{interval}"))
        .arg("-vsync")
        .arg("vfr")
        .arg(&output_pattern)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|err| {
            tracing::error!(error = %err, "ffmpeg failed");
            ApiError::internal("Failed to extract frames")
        })?;
    if !status.success() {
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
        if let Ok(image) = image::open(&frame_path) {
            frames.push(image);
        }
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
    let base = std::env::temp_dir();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    base.join(format!("panoptikon-extract-{unique}"))
}

fn render_pdf_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
    render_pdf_or_html_frames(path, None)
}

fn render_html_frames(path: &str) -> ApiResult<Vec<Vec<u8>>> {
    render_pdf_or_html_frames(path, Some("html"))
}

fn render_pdf_or_html_frames(path: &str, kind: Option<&str>) -> ApiResult<Vec<Vec<u8>>> {
    let temp_dir = temp_dir_path();
    std::fs::create_dir_all(&temp_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create temp dir");
        ApiError::internal("Failed to render document")
    })?;
    let script = r#"
import sys
from pathlib import Path
try:
    import pypdfium2 as pdfium
except Exception as e:
    print(f"pypdfium2 unavailable: {e}", file=sys.stderr)
    sys.exit(2)

kind = sys.argv[1]
src = sys.argv[2]
out_dir = Path(sys.argv[3])

if kind == "html":
    try:
        from weasyprint import HTML
    except Exception as e:
        print(f"weasyprint unavailable: {e}", file=sys.stderr)
        sys.exit(2)
    pdf_bytes = HTML(src).write_pdf()
    doc = pdfium.PdfDocument(pdf_bytes)
else:
    doc = pdfium.PdfDocument(src)

for idx, page in enumerate(doc):
    image = page.render(scale=2, rev_byteorder=True).to_pil()
    image.save(out_dir / f"page_{idx:04d}.jpg", format="JPEG")
    image.close()
doc.close()
"#;
    let kind_arg = kind.unwrap_or("pdf");
    let output = std::process::Command::new("python")
        .arg("-c")
        .arg(script)
        .arg(kind_arg)
        .arg(path)
        .arg(temp_dir.to_string_lossy().to_string())
        .output()
        .map_err(|err| {
            tracing::error!(error = %err, "failed to render document");
            ApiError::internal("Failed to render document")
        })?;
    if !output.status.success() {
        tracing::warn!(
            stderr = %String::from_utf8_lossy(&output.stderr),
            "document render failed"
        );
        return Ok(Vec::new());
    }
    let mut paths = std::fs::read_dir(&temp_dir)
        .map_err(|err| ApiError::internal(format!("Failed to read rendered pages: {err}")))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jpg"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut pages = Vec::new();
    for page_path in paths {
        if let Ok(bytes) = std::fs::read(&page_path) {
            pages.push(bytes);
        }
        let _ = std::fs::remove_file(&page_path);
    }
    Ok(pages)
}
