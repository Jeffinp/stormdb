use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use stormdb_common::{MAX_FRAME_SIZE, ProtocolError};

/// Representação de um frame RESP2.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    /// Verifica se um frame completo está disponível no buffer sem alocar.
    /// Retorna Ok(()) se completo, Err(Incomplete) se precisa mais dados.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
        match get_u8(src)? {
            b'+' | b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_line(src)?;
                Ok(())
            }
            b'$' => {
                let len = get_decimal(src)?;
                if len == -1 {
                    return Ok(());
                }
                if len < 0 {
                    return Err(ProtocolError::InvalidBulkLength(len));
                }
                let len = len as usize;
                if len > MAX_FRAME_SIZE {
                    return Err(ProtocolError::FrameTooLarge(len));
                }
                skip(src, len + 2)?; // data + \r\n
                Ok(())
            }
            b'*' => {
                let count = get_decimal(src)?;
                if count == -1 {
                    return Ok(());
                }
                if count < 0 {
                    return Err(ProtocolError::InvalidBulkLength(count));
                }
                for _ in 0..count {
                    Frame::check(src)?;
                }
                Ok(())
            }
            byte => Err(ProtocolError::InvalidFrameType(byte)),
        }
    }

    /// Faz o parse de um frame completo a partir do cursor.
    /// Deve ser chamado apenas após `check()` retornar Ok.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, ProtocolError> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?;
                let s = String::from_utf8(line.to_vec())
                    .map_err(|e| ProtocolError::InvalidEncoding(e.to_string()))?;
                Ok(Frame::Simple(s))
            }
            b'-' => {
                let line = get_line(src)?;
                let s = String::from_utf8(line.to_vec())
                    .map_err(|e| ProtocolError::InvalidEncoding(e.to_string()))?;
                Ok(Frame::Error(s))
            }
            b':' => {
                let n = get_decimal(src)?;
                Ok(Frame::Integer(n))
            }
            b'$' => {
                let len = get_decimal(src)?;
                if len == -1 {
                    return Ok(Frame::Null);
                }
                let len = len as usize;
                if src.remaining() < len + 2 {
                    return Err(ProtocolError::Incomplete);
                }
                let data = Bytes::copy_from_slice(&src.get_ref()[src.position() as usize..][..len]);
                src.set_position(src.position() + len as u64 + 2);
                Ok(Frame::Bulk(data))
            }
            b'*' => {
                let count = get_decimal(src)?;
                if count == -1 {
                    return Ok(Frame::Null);
                }
                let count = count as usize;
                let mut frames = Vec::with_capacity(count);
                for _ in 0..count {
                    frames.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(frames))
            }
            byte => Err(ProtocolError::InvalidFrameType(byte)),
        }
    }

    /// Encoda o frame no buffer de saída em formato RESP2.
    pub fn encode(&self, dst: &mut BytesMut) {
        match self {
            Frame::Simple(s) => {
                dst.put_u8(b'+');
                dst.put(s.as_bytes());
                dst.put(&b"\r\n"[..]);
            }
            Frame::Error(s) => {
                dst.put_u8(b'-');
                dst.put(s.as_bytes());
                dst.put(&b"\r\n"[..]);
            }
            Frame::Integer(n) => {
                dst.put_u8(b':');
                dst.put(n.to_string().as_bytes());
                dst.put(&b"\r\n"[..]);
            }
            Frame::Bulk(data) => {
                dst.put_u8(b'$');
                dst.put(data.len().to_string().as_bytes());
                dst.put(&b"\r\n"[..]);
                dst.put(data.as_ref());
                dst.put(&b"\r\n"[..]);
            }
            Frame::Null => {
                dst.put(&b"$-1\r\n"[..]);
            }
            Frame::Array(frames) => {
                dst.put_u8(b'*');
                dst.put(frames.len().to_string().as_bytes());
                dst.put(&b"\r\n"[..]);
                for frame in frames {
                    frame.encode(dst);
                }
            }
        }
    }

    /// Helper: cria um Frame::Bulk a partir de &str.
    pub fn bulk(s: &str) -> Frame {
        Frame::Bulk(Bytes::from(s.to_string()))
    }

    /// Helper: cria um Array de Bulk strings a partir de &[&str].
    pub fn array_from_strs(strs: &[&str]) -> Frame {
        Frame::Array(strs.iter().map(|s| Frame::bulk(s)).collect())
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, ProtocolError> {
    if !src.has_remaining() {
        return Err(ProtocolError::Incomplete);
    }
    Ok(src.get_u8())
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ProtocolError> {
    let start = src.position() as usize;
    let end = src.get_ref().len();

    for i in start..end.saturating_sub(1) {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(ProtocolError::Incomplete)
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, ProtocolError> {
    let line = get_line(src)?;
    let s = std::str::from_utf8(line).map_err(|e| ProtocolError::InvalidInteger(e.to_string()))?;
    s.parse::<i64>()
        .map_err(|e| ProtocolError::InvalidInteger(e.to_string()))
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), ProtocolError> {
    if src.remaining() < n {
        return Err(ProtocolError::Incomplete);
    }
    src.set_position(src.position() + n as u64);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(frame: &Frame) {
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        let bytes = buf.freeze();
        let mut cursor = Cursor::new(bytes.as_ref());
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let parsed = Frame::parse(&mut cursor).unwrap();
        assert_eq!(&parsed, frame);
    }

    #[test]
    fn roundtrip_simple_string() {
        roundtrip(&Frame::Simple("OK".into()));
    }

    #[test]
    fn roundtrip_error() {
        roundtrip(&Frame::Error("ERR unknown command".into()));
    }

    #[test]
    fn roundtrip_integer() {
        roundtrip(&Frame::Integer(42));
        roundtrip(&Frame::Integer(-1));
        roundtrip(&Frame::Integer(0));
    }

    #[test]
    fn roundtrip_bulk() {
        roundtrip(&Frame::Bulk(Bytes::from("hello world")));
        roundtrip(&Frame::Bulk(Bytes::new())); // empty bulk
    }

    #[test]
    fn roundtrip_null() {
        roundtrip(&Frame::Null);
    }

    #[test]
    fn roundtrip_array() {
        let frame = Frame::Array(vec![
            Frame::Simple("OK".into()),
            Frame::Integer(42),
            Frame::Bulk(Bytes::from("data")),
            Frame::Null,
        ]);
        roundtrip(&frame);
    }

    #[test]
    fn roundtrip_nested_array() {
        let frame = Frame::Array(vec![
            Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
            Frame::Bulk(Bytes::from("test")),
        ]);
        roundtrip(&frame);
    }

    #[test]
    fn incomplete_frame() {
        let data = b"+OK\r"; // missing \n
        let mut cursor = Cursor::new(&data[..]);
        assert!(matches!(
            Frame::check(&mut cursor),
            Err(ProtocolError::Incomplete)
        ));
    }

    #[test]
    fn incomplete_bulk() {
        let data = b"$5\r\nhel"; // missing data
        let mut cursor = Cursor::new(&data[..]);
        assert!(matches!(
            Frame::check(&mut cursor),
            Err(ProtocolError::Incomplete)
        ));
    }

    #[test]
    fn invalid_frame_type() {
        let data = b"?invalid\r\n";
        let mut cursor = Cursor::new(&data[..]);
        assert!(matches!(
            Frame::check(&mut cursor),
            Err(ProtocolError::InvalidFrameType(b'?'))
        ));
    }

    #[test]
    fn roundtrip_set_command() {
        let frame = Frame::array_from_strs(&["SET", "key", "value", "EX", "10"]);
        roundtrip(&frame);
    }

    #[test]
    fn encode_bulk_1kb() {
        let data = vec![b'x'; 1024];
        let frame = Frame::Bulk(Bytes::from(data));
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        assert!(buf.len() > 1024);
        roundtrip(&frame);
    }
}
