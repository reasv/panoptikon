use serde_json::{Value, json};

use crate::api_error::ApiError;
use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};
use crate::media_tools::stderr_tail;

/// Audio tracks decoded per item when the registry sets no `max_tracks`.
/// Further tracks are almost always the same audio dubbed in another
/// language, or a commentary.
const DEFAULT_MAX_TRACKS: usize = 1;

/// The `max_tracks` handler opt; anything below 1 falls back to the default.
fn max_tracks(opts: &serde_json::Map<String, Value>) -> usize {
    opts.get("max_tracks")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_TRACKS)
}

pub(super) async fn build_audio_tracks_inputs(
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    if !item.item_type.starts_with("video") && !item.item_type.starts_with("audio") {
        return Ok(Vec::new());
    }
    let opts = &model.input_handler_opts;
    let sample_rate = opts
        .get("sample_rate")
        .and_then(Value::as_i64)
        .unwrap_or(16000) as u32;
    let max_duration = opts.get("max_duration").and_then(Value::as_f64);

    let audio = load_audio_tracks(
        &item.path,
        sample_rate,
        max_tracks(opts),
        item.audio_tracks,
        max_duration,
    )?;
    let mut outputs = Vec::new();
    for track in audio {
        let bytes = serialize_npy_f32(&track);
        outputs.push(InferenceInput::new(
            json!({}),
            Some(InferenceFile::Bytes(bytes)),
        ));
    }
    Ok(outputs)
}

pub(super) async fn build_audio_files_inputs(
    item: &JobInputData,
    model: &ModelMetadata,
) -> ApiResult<Vec<InferenceInput>> {
    if !item.item_type.starts_with("video") && !item.item_type.starts_with("audio") {
        return Ok(Vec::new());
    }
    let opts = &model.input_handler_opts;
    let sample_rate = opts
        .get("sample_rate")
        .and_then(Value::as_i64)
        .unwrap_or(48000) as u32;
    let max_duration = opts.get("max_duration").and_then(Value::as_f64);

    let audio = load_audio_tracks(
        &item.path,
        sample_rate,
        max_tracks(opts),
        item.audio_tracks,
        max_duration,
    )?;
    let mut outputs = Vec::new();
    for track in audio {
        let wav_bytes = audio_to_wav_bytes(&track, sample_rate);
        outputs.push(InferenceInput::new(
            json!({"type": "audio"}),
            Some(InferenceFile::Bytes(wav_bytes)),
        ));
    }
    Ok(outputs)
}

fn serialize_npy_f32(values: &[f32]) -> Vec<u8> {
    let shape = format!("({},)", values.len());
    let mut header = format!("{{'descr': '<f4', 'fortran_order': False, 'shape': {shape}, }}");
    while (10 + header.len() + 1) % 16 != 0 {
        header.push(' ');
    }
    header.push('\n');
    let header_len = header.len() as u16;
    let mut out = Vec::with_capacity(10 + header.len() + values.len() * 4);
    out.extend_from_slice(b"\x93NUMPY");
    out.extend_from_slice(&[1, 0]);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(header.as_bytes());
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

/// Decode up to `max_tracks` audio tracks to mono PCM at `sample_rate`, in
/// the order of `audio_streams`, optionally capped to the first
/// `max_duration` seconds (`-t` as an ffmpeg output option, so decoding
/// stops at the cap instead of decoding everything and trimming).
/// The cap is a per-model registry opt: embedding models whose receptive
/// field is seconds long gain nothing past it, while transcription models
/// must keep the whole track and simply do not set it.
///
/// `scanned_tracks` is the item's audio stream count stored by the scan. With
/// 0 or 1 there is nothing to order, so the probe is skipped: 0 decodes
/// nothing, and 1 decodes the only stream without `-map`.
fn load_audio_tracks(
    path: &str,
    sample_rate: u32,
    max_tracks: usize,
    scanned_tracks: Option<i64>,
    max_duration: Option<f64>,
) -> ApiResult<Vec<Vec<f32>>> {
    let streams: Vec<Option<u64>> = match scanned_tracks {
        Some(0) => Vec::new(),
        Some(1) => vec![None],
        _ => audio_streams(path)?.into_iter().map(Some).collect(),
    };
    streams
        .into_iter()
        .take(max_tracks)
        .map(|stream| decode_audio_stream(path, stream, sample_rate, max_duration))
        .collect()
}

/// `stream` is a container stream index; `None` lets ffmpeg pick the stream.
fn decode_audio_stream(
    path: &str,
    stream: Option<u64>,
    sample_rate: u32,
    max_duration: Option<f64>,
) -> ApiResult<Vec<f32>> {
    let mut command = std::process::Command::new(crate::media_tools::ffmpeg());
    command
        .arg("-nostdin")
        .arg("-threads")
        .arg("0")
        .arg("-i")
        .arg(path);
    if let Some(stream) = stream {
        command.arg("-map").arg(format!("0:{stream}"));
    }
    command
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg("1")
        .arg("-acodec")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg(sample_rate.to_string());
    if let Some(seconds) = max_duration.filter(|seconds| *seconds > 0.0) {
        command.arg("-t").arg(seconds.to_string());
    }
    match command.arg("-").output() {
        Ok(output) if output.status.success() => Ok(s16le_to_f32(&output.stdout)),
        // The stream is known to exist, so a corrupt track and a
        // transient mount hiccup are indistinguishable: an unconfirmed
        // payload verdict, which needs a second failing run to settle.
        Ok(output) => {
            let stderr = stderr_tail(&output.stderr);
            Err(ApiError::input_unconfirmed(format!(
                "ffmpeg failed to decode audio from {path}: {stderr}"
            )))
        }
        // A spawn failure is never a verdict on the media.
        Err(err) => Err(crate::media_tools::spawn_error("ffmpeg", &err)),
    }
}

/// The file's audio stream indices, main track first: streams flagged
/// default, then more channels, then file order. This is ffmpeg's own rule
/// for picking an audio stream when no `-map` is given, except that ffmpeg
/// ranks a stream with packets in its probe window above the default flag:
/// a default track whose first packet lies past that window is first here
/// but not in ffmpeg.
fn audio_streams(path: &str) -> ApiResult<Vec<u64>> {
    let output = std::process::Command::new(crate::media_tools::ffprobe())
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a")
        .arg("-show_entries")
        .arg("stream=index,channels:stream_disposition=default")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|err| crate::media_tools::spawn_error("ffprobe", &err))?;
    // ffprobe failing is not the same as "no audio stream": a corrupt file or
    // a transient read error (e.g. an SMB hiccup) must fail the item so it is
    // retried, not permanently marked processed with a placeholder. Which of
    // the two it was cannot be told apart here, hence the unconfirmed
    // threshold rather than a class.
    if !output.status.success() {
        let stderr = stderr_tail(&output.stderr);
        return Err(ApiError::input_unconfirmed(format!(
            "ffprobe failed on {path}: {stderr}"
        )));
    }
    // Exit 0 with stdout we cannot parse is not a verdict about the file:
    // ffprobe read it fine and something on *our* side of the pipe went wrong
    // (a truncated read, a version whose `-of json` shape we do not
    // understand). Transient, so the item is retried rather than suppressed.
    let value: Value = serde_json::from_slice(&output.stdout).map_err(|err| {
        ApiError::internal(format!("ffprobe output for {path} is unparseable: {err}"))
    })?;
    Ok(order_audio_streams(&value))
}

fn order_audio_streams(probe: &Value) -> Vec<u64> {
    let mut streams: Vec<(bool, u64, u64)> = probe
        .get("streams")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|stream| {
            let index = stream.get("index")?.as_u64()?;
            let channels = stream.get("channels").and_then(Value::as_u64).unwrap_or(0);
            let default = stream
                .pointer("/disposition/default")
                .and_then(Value::as_i64)
                == Some(1);
            Some((default, channels, index))
        })
        .collect();
    streams
        .sort_by_key(|&(default, channels, index)| (!default, std::cmp::Reverse(channels), index));
    streams.into_iter().map(|(_, _, index)| index).collect()
}

fn s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let value = i16::from_le_bytes([chunk[0], chunk[1]]);
        out.push(value as f32 / 32768.0);
    }
    out
}

fn audio_to_wav_bytes(samples: &[f32], sample_rate: u32) -> Vec<u8> {
    let mut pcm_bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        let value = (clamped * 32768.0) as i16;
        pcm_bytes.extend_from_slice(&value.to_le_bytes());
    }

    let data_size = pcm_bytes.len() as u32;
    let mut out = Vec::with_capacity(44 + pcm_bytes.len());
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_size).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    let byte_rate = sample_rate * 2;
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_size.to_le_bytes());
    out.extend_from_slice(&pcm_bytes);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(value: Value) -> serde_json::Map<String, Value> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn max_tracks_defaults_to_one() {
        assert_eq!(max_tracks(&opts(json!({}))), 1);
        assert_eq!(max_tracks(&opts(json!({"max_tracks": 0}))), 1);
        assert_eq!(max_tracks(&opts(json!({"max_tracks": -2}))), 1);
        assert_eq!(max_tracks(&opts(json!({"max_tracks": 3}))), 3);
    }

    fn probe(streams: &[(u64, u64, i64)]) -> Value {
        let streams: Vec<Value> = streams
            .iter()
            .map(|&(index, channels, default)| {
                json!({"index": index, "channels": channels, "disposition": {"default": default}})
            })
            .collect();
        json!({ "streams": streams })
    }

    #[test]
    fn the_default_track_comes_first_then_more_channels_then_file_order() {
        // (stream index, channels, default flag)
        assert_eq!(
            order_audio_streams(&probe(&[(1, 6, 0), (2, 2, 1), (3, 2, 0)])),
            vec![2, 1, 3]
        );
        assert_eq!(
            order_audio_streams(&probe(&[(1, 2, 0), (2, 6, 0), (3, 2, 0)])),
            vec![2, 1, 3]
        );
        assert_eq!(
            order_audio_streams(&probe(&[(1, 2, 1), (2, 2, 1), (3, 2, 1)])),
            vec![1, 2, 3]
        );
        assert_eq!(order_audio_streams(&json!({})), Vec::<u64>::new());
    }

    /// Writes a file with three mono/stereo tone tracks of 1, 2 and 3
    /// seconds, where only the 2 s track is flagged default. Returns `false`
    /// where this machine cannot, so the test skips rather than fails.
    fn write_three_tracks(path: &std::path::Path) -> bool {
        let status = std::process::Command::new(crate::media_tools::ffmpeg())
            .args(["-y", "-v", "error"])
            .args(["-f", "lavfi", "-i", "sine=frequency=440:duration=1"])
            .args(["-f", "lavfi", "-i", "sine=frequency=550:duration=2"])
            .args(["-f", "lavfi", "-i", "sine=frequency=660:duration=3"])
            .args(["-filter_complex", "[2]pan=stereo|c0=c0|c1=c0[stereo]"])
            .args(["-map", "0", "-map", "1", "-map", "[stereo]"])
            .args(["-c:a", "pcm_s16le"])
            .args(["-disposition:a:0", "0", "-disposition:a:1", "default"])
            .args(["-disposition:a:2", "0"])
            .arg(path)
            .stdin(std::process::Stdio::null())
            .status();
        matches!(status, Ok(status) if status.success())
    }

    /// Seconds of audio in each decoded track, at the 16 kHz used below.
    fn seconds(tracks: &[Vec<f32>]) -> Vec<usize> {
        tracks.iter().map(|track| track.len() / 16000).collect()
    }

    /// The generated three-track file, or `None` where this machine cannot
    /// build it.
    fn three_track_file(dir: &tempfile::TempDir) -> Option<String> {
        if !crate::media_tools::ffmpeg_available() {
            return None;
        }
        let file = dir.path().join("tracks.mka");
        write_three_tracks(&file).then(|| file.to_str().unwrap().to_string())
    }

    #[test]
    fn max_tracks_decodes_that_many_tracks_main_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let Some(path) = three_track_file(&dir) else {
            return;
        };
        let path = path.as_str();

        let default = max_tracks(&opts(json!({})));
        let one = load_audio_tracks(path, 16000, default, None, None).unwrap();
        assert_eq!(seconds(&one), vec![2]);
        let two = load_audio_tracks(path, 16000, 2, None, None).unwrap();
        assert_eq!(seconds(&two), vec![2, 3]);
        let all = load_audio_tracks(path, 16000, 5, None, None).unwrap();
        assert_eq!(seconds(&all), vec![2, 3, 1]);

        // A scanned count of 1 skips the probe and lets ffmpeg pick the
        // stream, which is the same track the probe puts first.
        let unmapped = load_audio_tracks(path, 16000, 5, Some(1), None).unwrap();
        assert_eq!(unmapped, one);
        // A scanned count of 0 decodes nothing.
        assert!(
            load_audio_tracks(path, 16000, 5, Some(0), None)
                .unwrap()
                .is_empty()
        );
    }

    fn audio_item(path: &str) -> JobInputData {
        JobInputData {
            file_id: 1,
            item_id: 1,
            path: path.to_string(),
            sha256: "sha".to_string(),
            md5: "md5".to_string(),
            last_modified: "2026-01-01T00:00:00".to_string(),
            item_type: "audio/x-matroska".to_string(),
            duration: None,
            content_end_ms: None,
            audio_tracks: Some(3),
            video_tracks: Some(0),
            subtitle_tracks: Some(0),
            width: None,
            height: None,
            data_id: None,
            text: None,
        }
    }

    fn audio_model(handler: &str, handler_opts: Value) -> ModelMetadata {
        ModelMetadata {
            group: "audio".to_string(),
            inference_id: "test".to_string(),
            setter_name: "audio/test".to_string(),
            input_handler: handler.to_string(),
            input_handler_opts: opts(handler_opts),
            target_entities: vec!["items".to_string()],
            output_type: "text".to_string(),
            default_batch_size: 1,
            default_threshold: None,
            input_mime_types: vec!["audio/".to_string()],
            skip_processed_items: true,
            unavailable_reason: None,
            name: None,
            description: None,
            link: None,
        }
    }

    #[tokio::test]
    async fn the_handlers_read_max_tracks_from_their_opts() {
        let dir = tempfile::TempDir::new().unwrap();
        let Some(path) = three_track_file(&dir) else {
            return;
        };
        let item = audio_item(&path);
        let inputs = |handler: &'static str, opts: Value| {
            let model = audio_model(handler, opts);
            let item = item.clone();
            async move {
                let inputs = match handler {
                    "audio_tracks" => build_audio_tracks_inputs(&item, &model).await,
                    _ => build_audio_files_inputs(&item, &model).await,
                };
                inputs.unwrap().len()
            }
        };
        assert_eq!(inputs("audio_tracks", json!({})).await, 1);
        assert_eq!(inputs("audio_tracks", json!({"max_tracks": 2})).await, 2);
        assert_eq!(inputs("audio_files", json!({})).await, 1);
        assert_eq!(inputs("audio_files", json!({"max_tracks": 3})).await, 3);
    }
}
