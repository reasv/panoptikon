use serde_json::Value;

use crate::api_error::ApiError;
use crate::jobs::extraction::ApiResult;

pub(super) fn parse_embedding_json(value: &Value) -> ApiResult<Vec<f32>> {
    let arr = value
        .as_array()
        .ok_or_else(|| ApiError::internal("Embedding output must be an array"))?;
    let mut embedding = Vec::with_capacity(arr.len());
    for v in arr {
        if let Some(value) = v.as_f64() {
            embedding.push(value as f32);
        }
    }
    Ok(embedding)
}

pub(super) fn serialize_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

pub(super) fn parse_npy_to_f32(buffer: &[u8]) -> ApiResult<Vec<f32>> {
    let (shape, data) = parse_npy(buffer)?;
    if shape.len() != 1 {
        return Err(ApiError::internal("Expected 1D embedding"));
    }
    Ok(data)
}

pub(super) fn parse_npy_to_f32_rows(buffer: &[u8]) -> ApiResult<Vec<Vec<f32>>> {
    let (shape, data) = parse_npy(buffer)?;
    if shape.len() == 1 {
        return Ok(vec![data]);
    }
    if shape.len() != 2 {
        return Err(ApiError::internal("Expected 1D or 2D embedding"));
    }
    let rows = shape[0];
    let cols = shape[1];
    if rows * cols != data.len() {
        return Err(ApiError::internal("Embedding shape mismatch"));
    }
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let start = row * cols;
        out.push(data[start..start + cols].to_vec());
    }
    Ok(out)
}

fn parse_npy(buffer: &[u8]) -> ApiResult<(Vec<usize>, Vec<f32>)> {
    const MAGIC: &[u8] = b"\x93NUMPY";
    if buffer.len() < 10 || &buffer[..6] != MAGIC {
        return Err(ApiError::internal("Invalid NPY buffer"));
    }
    let major = buffer[6];
    let header_len = match major {
        1 => u16::from_le_bytes([buffer[8], buffer[9]]) as usize,
        2 | 3 => u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize,
        _ => return Err(ApiError::internal("Unsupported NPY version")),
    };
    let header_start = if major == 1 { 10 } else { 12 };
    let header_end = header_start + header_len;
    if buffer.len() < header_end {
        return Err(ApiError::internal("Invalid NPY header"));
    }
    let header = std::str::from_utf8(&buffer[header_start..header_end])
        .map_err(|_| ApiError::internal("Invalid NPY header"))?;
    let descr =
        parse_npy_field(header, "descr").ok_or_else(|| ApiError::internal("NPY descr missing"))?;
    if descr != "<f4" {
        return Err(ApiError::internal("Unsupported NPY dtype"));
    }
    let fortran = parse_npy_field(header, "fortran_order").unwrap_or("False".to_string());
    if fortran.trim() != "False" {
        return Err(ApiError::internal("Fortran order not supported"));
    }
    let shape_str =
        parse_npy_field(header, "shape").ok_or_else(|| ApiError::internal("NPY shape missing"))?;
    let shape = parse_shape(&shape_str)?;
    let data_start = header_end;
    let expected = shape.iter().product::<usize>() * 4;
    if buffer.len() < data_start + expected {
        return Err(ApiError::internal("NPY data truncated"));
    }
    let mut values = Vec::with_capacity(expected / 4);
    for chunk in buffer[data_start..data_start + expected].chunks_exact(4) {
        values.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok((shape, values))
}

fn parse_npy_field(header: &str, key: &str) -> Option<String> {
    let needle = format!("'{}':", key);
    let idx = header.find(&needle)?;
    let value_start = idx + needle.len();
    let rest = header[value_start..].trim_start();
    let mut depth: i32 = 0;
    let mut end = 0;
    for (i, ch) in rest.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    if end == 0 {
        end = rest.len();
    }
    Some(rest[..end].trim().trim_matches('\'').to_string())
}

fn parse_shape(shape_str: &str) -> ApiResult<Vec<usize>> {
    let trimmed = shape_str.trim().trim_matches(|c| c == '(' || c == ')');
    if trimmed.is_empty() {
        return Err(ApiError::internal("Invalid NPY shape"));
    }
    let mut dims = Vec::new();
    for part in trimmed.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let value = part
            .parse::<usize>()
            .map_err(|_| ApiError::internal("Invalid NPY shape"))?;
        dims.push(value);
    }
    if dims.is_empty() {
        return Err(ApiError::internal("Invalid NPY shape"));
    }
    Ok(dims)
}
