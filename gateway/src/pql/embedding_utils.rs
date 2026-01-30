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
enum NpyDtype {
    F32,
    F64,
}

impl NpyDtype {
    fn size(self) -> usize {
        match self {
            Self::F32 => 4,
            Self::F64 => 8,
        }
    }
}

fn parse_npy_f32(buffer: &[u8]) -> Result<Vec<f32>, String> {
    let (dtype, little_endian, shape, data_offset) = parse_npy_header(buffer)?;
    if shape.is_empty() {
        return Err("Numpy array has empty shape".to_string());
    }
    if shape.len() > 2 {
        return Err("Only 1D or 2D embeddings are supported".to_string());
    }
    let elem_size = dtype.size();
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
        let start = idx * elem_size;
        let slice = data
            .get(start..start + elem_size)
            .ok_or_else(|| "Numpy data truncated".to_string())?;
        let value = match dtype {
            NpyDtype::F32 => parse_f32(slice, little_endian),
            NpyDtype::F64 => parse_f64(slice, little_endian) as f32,
        };
        values.push(value);
    }
    Ok(values)
}

fn parse_npy_header(buffer: &[u8]) -> Result<(NpyDtype, bool, Vec<usize>, usize), String> {
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
    if fortran {
        return Err("Fortran-order embeddings are not supported".to_string());
    }
    let shape = parse_shape(header).ok_or_else(|| "Numpy header missing shape".to_string())?;

    let (dtype, little_endian) = parse_descr(&descr)?;
    Ok((dtype, little_endian, shape, header_end))
}

fn parse_descr(descr: &str) -> Result<(NpyDtype, bool), String> {
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
        _ => {
            return Err(format!("Unsupported numpy endian in descr: {descr}"));
        }
    };
    let type_str = std::str::from_utf8(type_str).map_err(|_| "Invalid numpy descr".to_string())?;
    let dtype = match type_str {
        "f4" => NpyDtype::F32,
        "f8" => NpyDtype::F64,
        _ => return Err(format!("Unsupported numpy dtype: {descr}")),
    };
    Ok((dtype, little_endian))
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

fn parse_f32(slice: &[u8], little_endian: bool) -> f32 {
    let bytes: [u8; 4] = slice.try_into().unwrap_or([0, 0, 0, 0]);
    let raw = if little_endian {
        bytes
    } else {
        [bytes[3], bytes[2], bytes[1], bytes[0]]
    };
    f32::from_ne_bytes(raw)
}

fn parse_f64(slice: &[u8], little_endian: bool) -> f64 {
    let bytes: [u8; 8] = slice.try_into().unwrap_or([0, 0, 0, 0, 0, 0, 0, 0]);
    let raw = if little_endian {
        bytes
    } else {
        [
            bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0],
        ]
    };
    f64::from_ne_bytes(raw)
}
