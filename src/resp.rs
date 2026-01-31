use std::str;

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
}

impl RespValue {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => {
                let mut out = Vec::with_capacity(1 + s.len() + 2);
                out.extend_from_slice(b"+");
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
                out
            }
            RespValue::Error(s) => {
                let mut out = Vec::with_capacity(1 + s.len() + 2);
                out.extend_from_slice(b"-");
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
                out
            }
            RespValue::Integer(v) => format!(":{}\r\n", v).into_bytes(),
            RespValue::BulkString(Some(bytes)) => {
                let mut out = Vec::with_capacity(1 + 20 + bytes.len() + 4);
                out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
                out
            }
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
        }
    }
}

#[derive(Debug)]
pub enum RespError {
    InvalidFormat,
    InvalidUtf8,
}

fn read_line(buf: &[u8]) -> Option<(usize, &str)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            let line = &buf[..i];
            if let Ok(s) = str::from_utf8(line) {
                return Some((i + 2, s));
            }
            return None;
        }
        i += 1;
    }
    None
}

fn parse_int(s: &str) -> Result<isize, RespError> {
    s.parse::<isize>().map_err(|_| RespError::InvalidFormat)
}

/// Parse a RESP array of bulk strings into Vec<String>.
/// Returns Ok(None) if more data is needed.
pub fn parse_command(buf: &[u8]) -> Result<Option<(Vec<String>, usize)>, RespError> {
    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] != b'*' {
        return Err(RespError::InvalidFormat);
    }

    let (consumed, line) = match read_line(&buf[1..]) {
        Some(v) => v,
        None => return Ok(None),
    };
    let array_len = parse_int(line)? as usize;
    let mut offset = 1 + consumed;

    let mut parts = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        if offset >= buf.len() {
            return Ok(None);
        }
        if buf[offset] != b'$' {
            return Err(RespError::InvalidFormat);
        }

        let (len_consumed, len_line) = match read_line(&buf[offset + 1..]) {
            Some(v) => v,
            None => return Ok(None),
        };
        let bulk_len = parse_int(len_line)? as isize;
        if bulk_len < 0 {
            parts.push(String::new());
            offset = offset + 1 + len_consumed;
            continue;
        }

        let bulk_len = bulk_len as usize;
        offset = offset + 1 + len_consumed;
        if offset + bulk_len + 2 > buf.len() {
            return Ok(None);
        }

        let data = &buf[offset..offset + bulk_len];
        let s = str::from_utf8(data).map_err(|_| RespError::InvalidUtf8)?;
        parts.push(s.to_string());
        offset += bulk_len + 2;
    }

    Ok(Some((parts, offset)))
}
