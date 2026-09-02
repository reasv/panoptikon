use std::path::PathBuf;

use image::AnimationDecoder;
use image::codecs::gif::GifDecoder;
use image::metadata::Orientation;
use image::{DynamicImage, GenericImageView, ImageDecoder};
use serde_json::{Value, json};

use crate::api_error::{ApiError, Blocker};
use crate::db::index_writer::{IndexDbWriterMessage, call_index_db_writer};
use crate::db::open_index_db_read_no_user_data;
use crate::db::storage::{StoredImage, get_frames_bytes};
use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};
use crate::jobs::files::FRAME_PROCESS_VERSION;
use crate::media_tools::stderr_tail;

/// A frame ready to be sent to inference. PDF pages and HTML screenshots
/// carry their own pixel dimensions (each page differs from the item's stored
/// size); frames without dimensions are sliced using the item's stored
/// width/height, mirroring the Python loader.
///
/// Dimensions are *display* dimensions throughout, because so are the pixels
/// the models end up seeing (docs/display-dimensions-design.md §5): the
/// slicer orients what it decodes before cutting, and a whole-file send keeps
/// its EXIF for inferio's loader to apply.
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

    fn sized(bytes: Vec<u8>, (width, height): (u32, u32)) -> Self {
        Self {
            bytes,
            width: Some(i64::from(width)),
            height: Some(i64::from(height)),
        }
    }
}

/// `detect_outros` is the job's folded `scan_video && detect_outros` config
/// pair. It gates the outro clamp below, per design §8's "consumers ignore the
/// metadata".
///
/// Note what that does *not* buy on this side: `load_base_frames` returns
/// `storage.frames` before it ever reaches the clamp, and §7.1's recovery path
/// (erase a setter's `item_data`, re-run its extraction) does not touch
/// `storage.frames`. So turning detection off and re-running extraction reuses
/// whatever frames are cached, trimmed ones included. Undoing a false positive
/// takes a scan-side regeneration — that is the path that actually replaces
/// `storage.frames`; this gate only decides how frames are sampled when there
/// is no cache to reuse.
pub(super) async fn build_image_frames_inputs(
    index_db: &str,
    item: &JobInputData,
    model: &ModelMetadata,
    detect_outros: bool,
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

    let frames = load_base_frames(index_db, item, detect_outros).await?;
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

/// `detect_outros` gates the outro clamp on video frame sampling; see
/// [`build_image_frames_inputs`].
pub(super) async fn load_base_frames(
    index_db: &str,
    item: &JobInputData,
    detect_outros: bool,
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
        // The header's own dimensions, not the item's — read from the same
        // bytes the models will see, so a file that changed on disk after
        // indexing can never be sliced against a stale shape. Display
        // dimensions, like everything downstream: the slicer orients this
        // buffer before cutting, and when no slicing happens the bytes go out
        // whole with their EXIF intact, which inferio's loader applies
        // (docs/display-dimensions-design.md §5).
        let dimensions = ensure_image_readable(&buffer, &item.path)?;
        return Ok(vec![BaseFrame::sized(buffer, dimensions)]);
    }
    if item.item_type.starts_with("video") {
        let mut conn = open_index_db_read_no_user_data(index_db).await?;
        let cached = get_frames_bytes(&mut conn, &item.sha256)
            .await
            .unwrap_or_default();
        if !cached.is_empty() {
            return Ok(cached.into_iter().map(BaseFrame::sized_by_item).collect());
        }
        let duration = item.duration.unwrap_or(0.0);
        if duration > 0.0 && item.video_tracks.unwrap_or(0) > 0 {
            // Where the item's real content ends, when the scan's outro
            // detector found a boundary (docs/video-outro-detection-design.md
            // §7). Selected by the work query alongside `duration`, so it is
            // a chunk-boundary snapshot: a verdict written after this item's
            // work chunk was fetched is not seen, and the frames cached below
            // are then unclamped until a scan-side replacement rewrites them.
            // `None` — never examined, examined and negative, or detection
            // switched off — clamps nothing.
            let content_end_ms = if detect_outros {
                item.content_end_ms
            } else {
                None
            };
            let extracted = tokio::task::spawn_blocking({
                let path = item.path.clone();
                move || extract_video_frames(&path, 4, duration, content_end_ms)
            })
            .await
            .map_err(|_| ApiError::internal("Failed to extract frames"))??;
            // No frames to store: skip straight to the empty-inputs
            // placeholder rather than calling the writer. Storing an empty set
            // is not *wrong* (`store_frames` deletes and inserts nothing, and
            // the read side treats zero rows as "not cached"), but it costs a
            // writer transaction that unconditionally bumps the search-cache
            // epoch, and its DELETE (`WHERE item_sha256 = ? AND version <= ?`)
            // could race a concurrent scan that just stored real frames for
            // this item.
            if extracted.is_empty() {
                return Ok(Vec::new());
            }
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
                    // `storage.frames` has no media type column of its own;
                    // these are the JPEGs `encode_jpeg` just wrote.
                    media_type: "image/jpeg".to_string(),
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
///
/// The bytes were read successfully by the gateway, so a parse failure here
/// is unambiguously a verdict on the payload (`input`, confirmed at one
/// attempt). Deliberately still *only* a header parse: fully decoding a still
/// image would make the gateway stricter than the PIL consumer that actually
/// arbitrates it (docs/failed-media-retry-design.md, arbiter principle).
/// Returns the header's **display** dimensions — the coded numbers with the
/// EXIF orientation applied — because that is the space every consumer of the
/// returned frame works in: the slice grid is computed from them, and the
/// pixels they describe are oriented wherever they are actually decoded (the
/// slicer here, inferio's loader for a whole-file send).
fn ensure_image_readable(buffer: &[u8], path: &str) -> ApiResult<(u32, u32)> {
    let mut decoder = image::ImageReader::new(std::io::Cursor::new(buffer))
        .with_guessed_format()
        .map_err(|err| {
            tracing::error!(error = %err, path, "image format detection failed");
            ApiError::input(format!("Image {path} has an unrecognizable format: {err}"))
        })?
        .into_decoder()
        .map_err(|err| {
            tracing::error!(error = %err, path, "image is not readable");
            classify_image_error(err, format!("Image {path} has an unreadable header"))
        })?;
    let coded = decoder.dimensions();
    // An unreadable orientation is never a verdict on the pixels; the same
    // degradation as the scan's `open_image_oriented`.
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    Ok(crate::jobs::files::oriented_dimensions(coded, orientation))
}

/// The scan-side rule ([`crate::jobs::files`]'s image classifier), applied to
/// the extraction-side image-crate stages: `Limits` is a verdict on this
/// machine's decode budget, never on the file, so it is `resource` — settled
/// at one attempt and clearable by a retry directive after the ceiling is
/// raised. Everything else these stages produce is a verdict on bytes already
/// in memory, so it stays `input` confirmed at one attempt. (The header parse
/// runs under the image crate's default 512 MiB caps; the full decodes under
/// the configurable `image_decode_memory_limit_mb` ceiling — filing either as
/// `input` would mark a perfectly good file corrupt forever.)
fn classify_image_error(err: image::ImageError, context: String) -> ApiError {
    match err {
        image::ImageError::Limits(_) => ApiError::resource(format!("{context}: {err}")),
        err => ApiError::input(format!("{context}: {err}")),
    }
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

/// Decode for slicing: oriented, because the slice grid was computed from
/// display dimensions and the cuts must land in the same space
/// (docs/display-dimensions-design.md §5). Slices are re-encoded without
/// EXIF, so orienting here is also what keeps them from reaching the models
/// sideways — a whole-file send keeps its EXIF and inferio's loader applies
/// it instead.
fn load_dynamic_image(buffer: &[u8]) -> ApiResult<DynamicImage> {
    let mut image = crate::jobs::files::decode_image_bytes(buffer).map_err(|err| {
        tracing::error!(error = %err, "failed to decode image");
        ApiError::internal("Failed to decode image")
    })?;
    image.apply_orientation(buffer_orientation(buffer));
    Ok(image)
}

/// The EXIF orientation of an in-memory image, header-only, degrading to
/// `NoTransforms` on any failure: a missing or unreadable orientation is
/// never a verdict on pixels that decoded fine (the scan's
/// `open_image_oriented` rule).
fn buffer_orientation(buffer: &[u8]) -> Orientation {
    image::ImageReader::new(std::io::Cursor::new(buffer))
        .with_guessed_format()
        .ok()
        .and_then(|reader| reader.into_decoder().ok())
        .and_then(|mut decoder| decoder.orientation().ok())
        .unwrap_or(Orientation::NoTransforms)
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
    // The read itself is the gateway's own I/O: a vanished file or an SMB
    // hiccup says nothing about the payload, so it stays transient.
    let buffer = std::fs::read(path).map_err(|err| {
        tracing::error!(error = %err, path, "failed to open gif");
        ApiError::internal(format!("Failed to open gif {path}: {err}"))
    })?;
    // Everything below decodes bytes already in memory, so every failure is a
    // confirmed verdict on the payload.
    //
    // Files are routed here by extension-derived mime type; a mis-named
    // non-GIF (which PIL decoded regardless of extension) is handled as a
    // single still frame instead of failing the item.
    if !matches!(image::guess_format(&buffer), Ok(image::ImageFormat::Gif)) {
        let mut image = crate::jobs::files::decode_image_bytes(&buffer).map_err(|err| {
            tracing::error!(error = %err, path, "failed to decode mis-named gif");
            classify_image_error(err, format!("Failed to decode mis-named gif {path}"))
        })?;
        // Oriented like every other decode headed for the models: the JPEG
        // re-encode below drops the EXIF, and the item dims this frame falls
        // back to are display dims (the scan content-sniffs, so a mis-named
        // still was indexed with its orientation applied).
        image.apply_orientation(buffer_orientation(&buffer));
        return Ok(vec![BaseFrame::sized_by_item(encode_jpeg(&image)?)]);
    }
    let decoder = GifDecoder::new(std::io::Cursor::new(&buffer)).map_err(|err| {
        tracing::error!(error = %err, path, "failed to decode gif");
        classify_image_error(err, format!("Failed to decode gif {path}"))
    })?;
    let frames = decoder.into_frames().collect_frames().map_err(|err| {
        tracing::error!(error = %err, path, "failed to collect gif frames");
        classify_image_error(err, format!("Failed to collect gif frames of {path}"))
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

/// `duration` comes from the item's own `items` row, exactly like the
/// scan-side extractor (`jobs::files::extract_video_frames`) takes it. It
/// cannot be stale: any change to the file's content yields a new sha256 and
/// therefore a new item, so re-probing it here would only buy an extra ffprobe
/// spawn per uncached video. The caller already gates on a positive duration;
/// the guard below mirrors the scan side rather than relying on that.
fn extract_video_frames(
    path: &str,
    num_frames: usize,
    duration: f64,
    content_end_ms: Option<i64>,
) -> ApiResult<Vec<DynamicImage>> {
    if duration <= 0.0 {
        return Ok(Vec::new());
    }
    // The same window the scan side samples, computed by the same helper: the
    // interval spreads the N frames across the content, and the decode bound
    // is what keeps `fps=1/interval` from emitting a card frame past it.
    let (window, bounded) = crate::jobs::files::frame_sampling_window(duration, content_end_ms);
    let interval = window / num_frames as f64;
    let temp_dir = temp_dir_path();
    std::fs::create_dir_all(&temp_dir).map_err(|err| {
        tracing::error!(error = %err, "failed to create temp dir");
        ApiError::internal("Failed to extract frames")
    })?;
    let result = extract_video_frames_into(
        path,
        num_frames,
        interval,
        bounded.then_some(window),
        &temp_dir,
    );
    if let Err(err) = std::fs::remove_dir_all(&temp_dir) {
        tracing::debug!(error = %err, path = %temp_dir.display(), "failed to remove temp frame dir");
    }
    result
}

/// `decode_limit` is the outro clamp, in seconds, passed as an *input* option
/// (`-t` before `-i`) so it bounds the decode rather than only what the muxer
/// writes. The twin of the scan side's.
fn extract_video_frames_into(
    path: &str,
    num_frames: usize,
    interval: f64,
    decode_limit: Option<f64>,
    temp_dir: &std::path::Path,
) -> ApiResult<Vec<DynamicImage>> {
    let output_pattern = temp_dir.join("frame_%04d.png");
    // stdout is silenced, but stderr is captured so a failure can say why
    // (corrupt file, missing codec, disk full); it is only surfaced on a
    // non-zero exit.
    let mut args: Vec<std::ffi::OsString> = Vec::new();
    if let Some(limit) = decode_limit {
        args.push("-t".into());
        args.push(format!("{limit}").into());
    }
    args.push("-i".into());
    args.push(path.into());
    args.push("-vf".into());
    args.push(format!("fps=1/{interval}").into());
    args.push("-vsync".into());
    args.push("vfr".into());
    args.push(output_pattern.clone().into());
    // The retry wrapper, not a bare spawn: the decode clamp above is an
    // *input* `-t`, which is one of the triggers of the Windows/SMB
    // input-open bug (see `media_tools::cache_wrapped_args`) — so without it
    // a clamped video on a network mount fails to open at all.
    let output = crate::media_tools::ffmpeg_output_with_input_retry(&args, |command| {
        command.stdout(std::process::Stdio::null());
    })
    .map_err(|err| {
        tracing::error!(error = %err, path, "ffmpeg failed to start");
        crate::media_tools::spawn_error("ffmpeg", &err)
    })?;
    if !output.status.success() {
        let stderr = stderr_tail(&output.stderr);
        tracing::error!(path, stderr = %stderr, "ffmpeg failed to extract frames");
        // ffmpeg opened the file itself, so a corrupt video and a transient
        // mount hiccup look identical here; the ambiguous threshold is what
        // keeps a single NAS blip from suppressing a healthy file.
        return Err(ApiError::input_unconfirmed(format!(
            "ffmpeg failed to extract frames from {path}: {stderr}"
        )));
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
        let image = crate::jobs::files::open_image(&frame_path)
            .map_err(|err| ApiError::internal(format!("Failed to read extracted frame: {err}")))?;
        frames.push(image);
    }
    Ok(frames)
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
/// library the scan pipeline uses for thumbnails). Both failure modes are
/// classified: a missing pdfium blocks the item until the library appears,
/// and a document pdfium rejects is a payload verdict — unconfirmed, because
/// pdfium read the file itself.
async fn render_pdf_frames(path: &str) -> ApiResult<Vec<BaseFrame>> {
    let owned = path.to_string();
    let pages = tokio::task::spawn_blocking(move || {
        crate::jobs::files::render_pdf_pages(std::path::Path::new(&owned))
    })
    .await
    .map_err(|_| ApiError::internal("PDF render task failed"))?
    .map_err(|err| {
        tracing::error!(error = %err, path, "failed to render PDF");
        match err {
            crate::jobs::files::PdfRenderError::Unavailable => ApiError::blocked(
                Blocker::Pdfium,
                format!("pdfium is not available to render {path}"),
            ),
            crate::jobs::files::PdfRenderError::Document(detail) => {
                ApiError::input_unconfirmed(format!("Failed to render PDF {path}: {detail}"))
            }
        }
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
/// Uses the classified variant: no browser blocks the item until one is
/// installed, a render failure is an unconfirmed payload verdict, and the
/// gateway's own I/O around the render stays transient.
async fn render_html_frames(path: &str) -> ApiResult<Vec<BaseFrame>> {
    let owned = path.to_string();
    let shot = tokio::task::spawn_blocking(move || {
        crate::jobs::files::render_html_screenshot_classified(std::path::Path::new(&owned))
    })
    .await
    .map_err(|_| ApiError::internal("HTML render task failed"))?
    .map_err(|err| {
        tracing::error!(error = %err, path, "failed to render HTML page");
        match err {
            crate::jobs::files::HtmlRenderError::NoBrowser => ApiError::blocked(
                Blocker::HtmlRenderer,
                format!("no headless browser is available to render {path}"),
            ),
            crate::jobs::files::HtmlRenderError::Io(detail) => {
                ApiError::internal(format!("Failed to render HTML page {path}: {detail}"))
            }
            crate::jobs::files::HtmlRenderError::Render(detail) => {
                ApiError::input_unconfirmed(format!("Failed to render HTML page {path}: {detail}"))
            }
        }
    })?;
    Ok(vec![BaseFrame {
        width: Some(shot.width() as i64),
        height: Some(shot.height() as i64),
        bytes: encode_jpeg(&shot)?,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::files::{corner_is_card, jpeg_with_exif_orientation, write_clip};

    /// The extraction side has its own ffmpeg invocation, so the scan side's
    /// clamp test proves nothing about it. Both values of the gate are here:
    /// `Some` is what `detect_outros` on produces, `None` is both "never
    /// examined" and — per design §8 — "detection switched off, so the
    /// consumer ignores the metadata".
    ///
    /// The `None` case is narrower than an undo, though: it only governs what
    /// a *fresh* extraction samples. `load_base_frames` serves
    /// `storage.frames` before reaching this call, so re-running extraction
    /// with detection off reuses trimmed cached frames. See that function's
    /// note.
    #[test]
    fn sampling_is_clamped_only_when_a_boundary_is_supplied() {
        let dir = tempfile::TempDir::new().expect("a temp dir");
        let clip = dir.path().join("card.mp4");
        if !write_clip(&clip, Some(2), None) {
            return;
        }
        let path = clip.to_string_lossy().to_string();

        let clamped = extract_video_frames(&path, 4, 7.0, Some(5000)).expect("ffmpeg runs");
        assert_eq!(
            clamped.len(),
            4,
            "the interval shrinks with the window, so the count is unchanged"
        );
        assert!(
            !clamped.iter().any(corner_is_card),
            "no frame sent to inference may come from the card"
        );

        let unclamped = extract_video_frames(&path, 4, 7.0, None).expect("ffmpeg runs");
        assert_eq!(unclamped.len(), 4);
        assert!(
            unclamped.iter().any(corner_is_card),
            "unclamped sampling lands in the card — that is what the gate turns back on"
        );
    }

    // What the models see is the picture, in the same space the slice grid is
    // computed in (docs/display-dimensions-design.md §5). The gate reports
    // display dimensions and the slicer's decode orients — one disagreeing
    // with the other would tile an EXIF-rotated photo with its rows and
    // columns swapped, which is exactly the coded/display split this closes.
    #[test]
    fn the_slicer_and_its_grid_agree_on_the_picture() {
        // Orientation 6: coded 64x32, painted 32x64.
        let rotated = jpeg_with_exif_orientation(64, 32, 6);

        assert_eq!(
            ensure_image_readable(&rotated, "portrait.jpg").expect("the fixture parses"),
            (32, 64),
            "the gate reports what a browser paints, not the header's numbers"
        );
        assert_eq!(
            load_dynamic_image(&rotated)
                .expect("the fixture decodes")
                .dimensions(),
            (32, 64),
            "the slicer cuts the oriented pixels those dimensions describe"
        );

        // And a missing orientation stays a no-op, coded == display.
        let plain = {
            let mut bytes = Vec::new();
            image::DynamicImage::ImageRgb8(image::RgbImage::new(9, 4))
                .write_to(&mut std::io::Cursor::new(&mut bytes), image::ImageFormat::Png)
                .expect("the fixture encodes");
            bytes
        };
        assert_eq!(
            ensure_image_readable(&plain, "plain.png").expect("the fixture parses"),
            (9, 4)
        );
        assert_eq!(
            load_dynamic_image(&plain)
                .expect("the fixture decodes")
                .dimensions(),
            (9, 4)
        );
    }
}
