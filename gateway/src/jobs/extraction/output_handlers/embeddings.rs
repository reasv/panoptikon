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
    // Any little-endian float dtype is accepted and stored as f32, matching
    // Python's np.load + struct.pack('%sf') behavior; models configured with
    // e.g. torch_dtype=float16 serialize <f2 arrays.
    let (elem_size, decode): (usize, fn(&[u8]) -> f32) = match descr.as_str() {
        "<f2" => (2, |b: &[u8]| f16_to_f32(u16::from_le_bytes([b[0], b[1]]))),
        "<f4" => (4, |b: &[u8]| {
            f32::from_le_bytes([b[0], b[1], b[2], b[3]])
        }),
        "<f8" => (8, |b: &[u8]| {
            f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]) as f32
        }),
        other => {
            return Err(ApiError::internal(format!(
                "Unsupported NPY dtype: {other}"
            )));
        }
    };
    let fortran = parse_npy_field(header, "fortran_order").unwrap_or("False".to_string());
    if fortran.trim() != "False" {
        return Err(ApiError::internal("Fortran order not supported"));
    }
    let shape_str =
        parse_npy_field(header, "shape").ok_or_else(|| ApiError::internal("NPY shape missing"))?;
    let shape = parse_shape(&shape_str)?;
    let data_start = header_end;
    let expected = shape.iter().product::<usize>() * elem_size;
    if buffer.len() < data_start + expected {
        return Err(ApiError::internal("NPY data truncated"));
    }
    let mut values = Vec::with_capacity(expected / elem_size);
    for chunk in buffer[data_start..data_start + expected].chunks_exact(elem_size) {
        values.push(decode(chunk));
    }
    Ok((shape, values))
}

/// IEEE 754 half-precision to single-precision, covering subnormals, ±inf,
/// and NaN.
fn f16_to_f32(bits: u16) -> f32 {
    let sign = (bits as u32 >> 15) << 31;
    let exp = (bits >> 10) & 0x1f;
    let frac = (bits & 0x3ff) as u32;
    let out = match exp {
        0 => {
            if frac == 0 {
                sign // ±0
            } else {
                // Subnormal (value = frac × 2^-24): shift the mantissa up
                // until the implicit bit appears, tracking the exponent.
                let mut exp: i32 = -14;
                let mut frac = frac;
                while frac & 0x400 == 0 {
                    frac <<= 1;
                    exp -= 1;
                }
                sign | (((exp + 127) as u32) << 23) | ((frac & 0x3ff) << 13)
            }
        }
        0x1f => sign | (0xff << 23) | (frac << 13), // ±inf / NaN
        _ => sign | ((exp as u32 + 127 - 15) << 23) | (frac << 13),
    };
    f32::from_bits(out)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn npy(descr: &str, shape: &str, data: &[u8]) -> Vec<u8> {
        let header = format!(
            "{{'descr': '{descr}', 'fortran_order': False, 'shape': {shape}, }}"
        );
        let mut padded = header.into_bytes();
        // Pad with spaces so magic+len+header is 16-byte aligned, per spec.
        while (10 + padded.len() + 1) % 16 != 0 {
            padded.push(b' ');
        }
        padded.push(b'\n');
        let mut out = Vec::new();
        out.extend_from_slice(b"\x93NUMPY\x01\x00");
        out.extend_from_slice(&(padded.len() as u16).to_le_bytes());
        out.extend_from_slice(&padded);
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn f16_to_f32_covers_all_classes() {
        assert_eq!(f16_to_f32(0x0000), 0.0);
        assert!(f16_to_f32(0x8000) == 0.0 && f16_to_f32(0x8000).is_sign_negative());
        assert_eq!(f16_to_f32(0x3c00), 1.0);
        assert_eq!(f16_to_f32(0xc000), -2.0);
        assert_eq!(f16_to_f32(0x3555), 0.333251953125); // 1/3 rounded to f16
        assert_eq!(f16_to_f32(0x7bff), 65504.0); // max finite
        assert_eq!(f16_to_f32(0x0400), 6.103515625e-5); // min normal
        assert_eq!(f16_to_f32(0x0200), 2.0f32.powi(-15)); // subnormal
        assert_eq!(f16_to_f32(0x0001), 2.0f32.powi(-24)); // min subnormal
        assert_eq!(f16_to_f32(0x7c00), f32::INFINITY);
        assert_eq!(f16_to_f32(0xfc00), f32::NEG_INFINITY);
        assert!(f16_to_f32(0x7e00).is_nan());
    }

    #[test]
    fn parse_npy_accepts_all_float_dtypes() {
        let f4 = npy("<f4", "(2,)", &[1.5f32.to_le_bytes(), (-2.0f32).to_le_bytes()].concat());
        assert_eq!(parse_npy_to_f32(&f4).unwrap(), vec![1.5, -2.0]);

        let f8 = npy("<f8", "(2,)", &[1.5f64.to_le_bytes(), (-2.0f64).to_le_bytes()].concat());
        assert_eq!(parse_npy_to_f32(&f8).unwrap(), vec![1.5, -2.0]);

        let f2 = npy("<f2", "(2,)", &[0x3c00u16.to_le_bytes(), 0xc000u16.to_le_bytes()].concat());
        assert_eq!(parse_npy_to_f32(&f2).unwrap(), vec![1.0, -2.0]);

        let bad = npy("<i4", "(1,)", &1i32.to_le_bytes());
        assert!(parse_npy_to_f32(&bad).is_err());
    }

    #[test]
    fn parse_npy_rows_reshapes_2d() {
        let data: Vec<u8> = [1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let buf = npy("<f4", "(2, 3)", &data);
        let rows = parse_npy_to_f32_rows(&buf).unwrap();
        assert_eq!(rows, vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]]);
    }
}
