use serde_json::{Value, json};

use crate::api_error::ApiError;
use crate::inferio_client::{InferenceFile, InferenceInput};
use crate::jobs::extraction::{ApiResult, JobInputData, ModelMetadata};

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
    let max_tracks = opts.get("max_tracks").and_then(Value::as_i64).unwrap_or(4) as usize;

    let audio = load_audio_single(&item.path, sample_rate)?;
    let mut outputs = Vec::new();
    for track in audio.into_iter().take(max_tracks) {
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
    let max_tracks = opts.get("max_tracks").and_then(Value::as_i64).unwrap_or(4) as usize;

    let audio = load_audio_single(&item.path, sample_rate)?;
    let mut outputs = Vec::new();
    for track in audio.into_iter().take(max_tracks) {
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

fn load_audio_single(path: &str, sample_rate: u32) -> ApiResult<Vec<Vec<f32>>> {
    let output = std::process::Command::new(crate::media_tools::ffmpeg())
        .arg("-nostdin")
        .arg("-threads")
        .arg("0")
        .arg("-i")
        .arg(path)
        .arg("-f")
        .arg("s16le")
        .arg("-ac")
        .arg("1")
        .arg("-acodec")
        .arg("pcm_s16le")
        .arg("-ar")
        .arg(sample_rate.to_string())
        .arg("-")
        .output();

    match output {
        Ok(output) => {
            if output.status.success() {
                let audio = s16le_to_f32(&output.stdout);
                return Ok(vec![audio]);
            }
            if !has_audio_stream(path)? {
                return Ok(Vec::new());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(ApiError::internal(format!("ffmpeg failed: {stderr}")))
        }
        Err(err) => Err(ApiError::internal(format!("ffmpeg failed: {err}"))),
    }
}

fn has_audio_stream(path: &str) -> ApiResult<bool> {
    let output = std::process::Command::new(crate::media_tools::ffprobe())
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("stream=codec_type")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|err| ApiError::internal(format!("ffprobe failed: {err}")))?;
    // ffprobe failing is not the same as "no audio stream": a corrupt file or
    // a transient read error (e.g. an SMB hiccup) must fail the item so it is
    // retried, not permanently marked processed with a placeholder.
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ApiError::internal(format!("ffprobe failed: {stderr}")));
    }
    let value: Value = serde_json::from_slice(&output.stdout)
        .map_err(|err| ApiError::internal(format!("ffprobe output unparseable: {err}")))?;
    let streams = value
        .get("streams")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for stream in streams {
        if stream.get("codec_type").and_then(Value::as_str) == Some("audio") {
            return Ok(true);
        }
    }
    Ok(false)
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
