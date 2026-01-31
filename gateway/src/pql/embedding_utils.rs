use base64::{Engine as _, engine::general_purpose};

pub(crate) fn extract_embeddings(encoded: &str) -> Result<Vec<u8>, String> {
    let decoded = general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .map_err(|err| format!("Invalid base64 embeddings: {err}"))?;
    embedding_from_npy_bytes(&decoded)
}

pub(crate) fn embedding_from_npy_bytes(buffer: &[u8]) -> Result<Vec<u8>, String> {
    let values = parse_npy_f32(buffer)?;
    Ok(serialize_f32(&values))
}

pub(crate) fn serialize_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_ne_bytes());
    }
    out
}

#[derive(Clone, Copy, Debug)]
enum NpyKind {
    Float,
    Int,
    UInt,
    Bool,
}

#[derive(Clone, Copy, Debug)]
struct NpyDtype {
    kind: NpyKind,
    size: usize,
}

fn parse_npy_f32(buffer: &[u8]) -> Result<Vec<f32>, String> {
    let (dtype, little_endian, fortran, shape, data_offset) = parse_npy_header(buffer)?;
    if shape.is_empty() {
        return Err("Numpy array has empty shape".to_string());
    }
    if shape.len() > 2 {
        return Err("Only 1D or 2D embeddings are supported".to_string());
    }
    let elem_size = dtype.size;
    let total_len = shape.iter().product::<usize>();
    let total_bytes = total_len
        .checked_mul(elem_size)
        .ok_or_else(|| "Embedding size overflow".to_string())?;
    let end = data_offset
        .checked_add(total_bytes)
        .ok_or_else(|| "Embedding data overflow".to_string())?;
    let data = buffer
        .get(data_offset..end)
        .ok_or_else(|| "Numpy data truncated".to_string())?;

    let row_len = if shape.len() == 1 { shape[0] } else { shape[1] };
    let mut values = Vec::with_capacity(row_len);
    for idx in 0..row_len {
        let elem_index = if shape.len() == 1 {
            idx
        } else if !fortran {
            idx
        } else {
            idx
                .checked_mul(shape[0])
                .ok_or_else(|| "Embedding index overflow".to_string())?
        };
        let start = elem_index * elem_size;
        let slice = data
            .get(start..start + elem_size)
            .ok_or_else(|| "Numpy data truncated".to_string())?;
        let value = parse_scalar(slice, dtype, little_endian)?;
        values.push(value);
    }
    Ok(values)
}

fn parse_npy_header(
    buffer: &[u8],
) -> Result<(NpyDtype, bool, bool, Vec<usize>, usize), String> {
    if buffer.len() < 10 {
        return Err("Numpy buffer too small".to_string());
    }
    if &buffer[..6] != b"\x93NUMPY" {
        return Err("Invalid numpy magic header".to_string());
    }
    let major = buffer[6];
    let minor = buffer[7];
    let (header_len, header_start): (usize, usize) = match major {
        1 => {
            let len = u16::from_le_bytes([buffer[8], buffer[9]]) as usize;
            (len, 10usize)
        }
        2 | 3 => {
            if buffer.len() < 12 {
                return Err("Numpy buffer too small".to_string());
            }
            let len = u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize;
            (len, 12usize)
        }
        _ => {
            return Err(format!("Unsupported numpy version {major}.{minor}"));
        }
    };
    let header_end = header_start
        .checked_add(header_len)
        .ok_or_else(|| "Numpy header overflow".to_string())?;
    let header_bytes = buffer
        .get(header_start..header_end)
        .ok_or_else(|| "Numpy header truncated".to_string())?;
    let header =
        std::str::from_utf8(header_bytes).map_err(|err| format!("Invalid numpy header: {err}"))?;

    let descr =
        parse_str_value(header, "descr").ok_or_else(|| "Numpy header missing descr".to_string())?;
    let fortran = parse_bool_value(header, "fortran_order")
        .ok_or_else(|| "Numpy header missing fortran_order".to_string())?;
    let shape = parse_shape(header).ok_or_else(|| "Numpy header missing shape".to_string())?;

    let (dtype, little_endian) = parse_descr(&descr)?;
    Ok((dtype, little_endian, fortran, shape, header_end))
}

fn parse_descr(descr: &str) -> Result<(NpyDtype, bool), String> {
    if descr == "?" {
        return Ok((
            NpyDtype {
                kind: NpyKind::Bool,
                size: 1,
            },
            true,
        ));
    }
    let bytes = descr.as_bytes();
    if bytes.len() < 2 {
        return Err("Invalid numpy descr".to_string());
    }
    let (endian_char, type_str) = bytes
        .split_first()
        .ok_or_else(|| "Invalid numpy descr".to_string())?;
    let little_endian = match *endian_char as char {
        '<' | '|' => true,
        '>' => false,
        '=' => cfg!(target_endian = "little"),
        _ => {
            return Err(format!("Unsupported numpy endian in descr: {descr}"));
        }
    };
    let type_str = std::str::from_utf8(type_str).map_err(|_| "Invalid numpy descr".to_string())?;
    if type_str.is_empty() {
        return Err("Invalid numpy descr".to_string());
    }
    let mut chars = type_str.chars();
    let kind_char = chars
        .next()
        .ok_or_else(|| "Invalid numpy descr".to_string())?;
    let size: usize = chars
        .as_str()
        .parse()
        .map_err(|_| format!("Unsupported numpy dtype: {descr}"))?;
    let kind = match kind_char {
        'f' => NpyKind::Float,
        'i' => NpyKind::Int,
        'u' => NpyKind::UInt,
        'b' => NpyKind::Bool,
        _ => return Err(format!("Unsupported numpy dtype: {descr}")),
    };
    Ok((NpyDtype { kind, size }, little_endian))
}

fn parse_str_value(header: &str, key: &str) -> Option<String> {
    let key_single = format!("'{key}'");
    let key_double = format!("\"{key}\"");
    let key_pos = header
        .find(&key_single)
        .or_else(|| header.find(&key_double))?;
    let after_key = &header[key_pos + key_single.len()..];
    let colon_pos = after_key.find(':')?;
    let mut value = after_key[colon_pos + 1..].trim_start();
    let quote = value.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    value = &value[1..];
    let end = value.find(quote)?;
    Some(value[..end].to_string())
}

fn parse_bool_value(header: &str, key: &str) -> Option<bool> {
    let key_single = format!("'{key}'");
    let key_double = format!("\"{key}\"");
    let key_pos = header
        .find(&key_single)
        .or_else(|| header.find(&key_double))?;
    let after_key = &header[key_pos + key_single.len()..];
    let colon_pos = after_key.find(':')?;
    let value = after_key[colon_pos + 1..].trim_start();
    if value.starts_with("True") {
        Some(true)
    } else if value.starts_with("False") {
        Some(false)
    } else {
        None
    }
}

fn parse_shape(header: &str) -> Option<Vec<usize>> {
    let key_single = "'shape'";
    let key_double = "\"shape\"";
    let key_pos = header
        .find(key_single)
        .or_else(|| header.find(key_double))?;
    let after_key = &header[key_pos + key_single.len()..];
    let colon_pos = after_key.find(':')?;
    let value = after_key[colon_pos + 1..].trim_start();
    let start = value.find('(')?;
    let end = value.find(')')?;
    let shape_str = &value[start + 1..end];
    let mut shape = Vec::new();
    for part in shape_str.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed.parse::<usize>().ok()?;
        shape.push(value);
    }
    Some(shape)
}

fn parse_scalar(slice: &[u8], dtype: NpyDtype, little_endian: bool) -> Result<f32, String> {
    match dtype.kind {
        NpyKind::Float => match dtype.size {
            2 => {
                let bits = read_u16(slice, little_endian)?;
                Ok(f16_to_f32(bits))
            }
            4 => {
                let bits = read_u32(slice, little_endian)?;
                Ok(f32::from_bits(bits))
            }
            8 => {
                let bits = read_u64(slice, little_endian)?;
                Ok(f64::from_bits(bits) as f32)
            }
            _ => Err(format!("Unsupported float size: {}", dtype.size)),
        },
        NpyKind::Int => match dtype.size {
            1 => Ok(read_i8(slice)? as f32),
            2 => Ok(read_i16(slice, little_endian)? as f32),
            4 => Ok(read_i32(slice, little_endian)? as f32),
            8 => Ok(read_i64(slice, little_endian)? as f32),
            _ => Err(format!("Unsupported int size: {}", dtype.size)),
        },
        NpyKind::UInt => match dtype.size {
            1 => Ok(read_u8(slice)? as f32),
            2 => Ok(read_u16(slice, little_endian)? as f32),
            4 => Ok(read_u32(slice, little_endian)? as f32),
            8 => Ok(read_u64(slice, little_endian)? as f32),
            _ => Err(format!("Unsupported uint size: {}", dtype.size)),
        },
        NpyKind::Bool => match dtype.size {
            1 => Ok(if read_u8(slice)? == 0 { 0.0 } else { 1.0 }),
            _ => Err(format!("Unsupported bool size: {}", dtype.size)),
        },
    }
}

fn read_u8(slice: &[u8]) -> Result<u8, String> {
    slice.get(0).copied().ok_or_else(|| "Numpy data truncated".to_string())
}

fn read_i8(slice: &[u8]) -> Result<i8, String> {
    Ok(read_u8(slice)? as i8)
}

fn read_u16(slice: &[u8], little_endian: bool) -> Result<u16, String> {
    let bytes: [u8; 2] = slice.try_into().map_err(|_| "Numpy data truncated".to_string())?;
    Ok(if little_endian {
        u16::from_le_bytes(bytes)
    } else {
        u16::from_be_bytes(bytes)
    })
}

fn read_i16(slice: &[u8], little_endian: bool) -> Result<i16, String> {
    Ok(read_u16(slice, little_endian)? as i16)
}

fn read_u32(slice: &[u8], little_endian: bool) -> Result<u32, String> {
    let bytes: [u8; 4] = slice.try_into().map_err(|_| "Numpy data truncated".to_string())?;
    Ok(if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}

fn read_i32(slice: &[u8], little_endian: bool) -> Result<i32, String> {
    Ok(read_u32(slice, little_endian)? as i32)
}

fn read_u64(slice: &[u8], little_endian: bool) -> Result<u64, String> {
    let bytes: [u8; 8] = slice.try_into().map_err(|_| "Numpy data truncated".to_string())?;
    Ok(if little_endian {
        u64::from_le_bytes(bytes)
    } else {
        u64::from_be_bytes(bytes)
    })
}

fn read_i64(slice: &[u8], little_endian: bool) -> Result<i64, String> {
    Ok(read_u64(slice, little_endian)? as i64)
}

fn f16_to_f32(bits: u16) -> f32 {
    let sign = ((bits >> 15) & 0x1) as u32;
    let exp = ((bits >> 10) & 0x1f) as u32;
    let mant = (bits & 0x03ff) as u32;

    let f32_bits = if exp == 0 {
        if mant == 0 {
            sign << 31
        } else {
            let mut mant = mant;
            let mut exp = -1i32;
            while (mant & 0x0400) == 0 {
                mant <<= 1;
                exp -= 1;
            }
            mant &= 0x03ff;
            let exp32 = (exp + 1 + 127 - 15) as u32;
            (sign << 31) | (exp32 << 23) | (mant << 13)
        }
    } else if exp == 0x1f {
        let mant32 = if mant == 0 { 0 } else { mant << 13 };
        (sign << 31) | (0xff << 23) | mant32
    } else {
        let exp32 = exp + (127 - 15);
        (sign << 31) | (exp32 << 23) | (mant << 13)
    };
    f32::from_bits(f32_bits)
}

#[cfg(test)]
mod tests {
    use super::{embedding_from_npy_bytes, parse_npy_f32};

    const F32_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/f32_1d.npy");
    const F16_2D_C: &[u8] = include_bytes!("../../tests/fixtures/npy/f16_2d_c.npy");
    const F16_2D_F: &[u8] = include_bytes!("../../tests/fixtures/npy/f16_2d_f.npy");
    const F64_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/f64_1d.npy");
    const I16_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/i16_1d.npy");
    const U8_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/u8_1d.npy");
    const U16_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/u16_1d.npy");
    const BOOL_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/bool_1d.npy");
    const BE_F32_1D: &[u8] = include_bytes!("../../tests/fixtures/npy/be_f32_1d.npy");

    fn assert_slice_approx(actual: &[f32], expected: &[f32], tol: f32) {
        assert_eq!(actual.len(), expected.len(), "length mismatch");
        for (idx, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
            let delta = (*actual - *expected).abs();
            assert!(
                delta <= tol,
                "index {idx}: expected {expected}, got {actual} (delta {delta})"
            );
        }
    }

    #[test]
    fn parses_f32_1d() {
        let values = parse_npy_f32(F32_1D).expect("parse f32 1d");
        assert_slice_approx(&values, &[0.0, 1.5, -2.25, 3.0], 1e-6);
    }

    #[test]
    fn parses_f16_2d_c_order_first_row() {
        let values = parse_npy_f32(F16_2D_C).expect("parse f16 2d c");
        assert_slice_approx(&values, &[1.0, 2.0, 3.0], 1e-3);
    }

    #[test]
    fn parses_f16_2d_fortran_order_first_row() {
        let values = parse_npy_f32(F16_2D_F).expect("parse f16 2d f");
        assert_slice_approx(&values, &[1.0, 2.0, 3.0], 1e-3);
    }

    #[test]
    fn parses_f64_1d() {
        let values = parse_npy_f32(F64_1D).expect("parse f64 1d");
        assert_slice_approx(&values, &[1e-3, -1e3, 42.125], 1e-3);
    }

    #[test]
    fn parses_int_1d() {
        let values = parse_npy_f32(I16_1D).expect("parse i16 1d");
        assert_slice_approx(&values, &[-2.0, 0.0, 2.0, 1234.0], 0.0);
    }

    #[test]
    fn parses_uint_1d() {
        let values = parse_npy_f32(U8_1D).expect("parse u8 1d");
        assert_slice_approx(&values, &[0.0, 200.0, 255.0], 0.0);
        let values = parse_npy_f32(U16_1D).expect("parse u16 1d");
        assert_slice_approx(&values, &[0.0, 65535.0], 0.0);
    }

    #[test]
    fn parses_bool_1d() {
        let values = parse_npy_f32(BOOL_1D).expect("parse bool 1d");
        assert_slice_approx(&values, &[1.0, 0.0, 1.0], 0.0);
    }

    #[test]
    fn parses_big_endian_f32() {
        let values = parse_npy_f32(BE_F32_1D).expect("parse be f32 1d");
        assert_slice_approx(&values, &[1.0, -2.5, 100.25], 1e-6);
    }

    #[test]
    fn serializes_embedding_bytes() {
        let bytes = embedding_from_npy_bytes(F32_1D).expect("serialize f32 1d");
        let expected_values = [0.0f32, 1.5, -2.25, 3.0];
        let mut expected = Vec::with_capacity(expected_values.len() * 4);
        for value in expected_values {
            expected.extend_from_slice(&value.to_ne_bytes());
        }
        assert_eq!(bytes, expected);
    }
}
