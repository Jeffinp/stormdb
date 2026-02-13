use bytes::BytesMut;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use stormdb_common::{ConnectionError, INITIAL_BUFFER_CAPACITY};
use stormdb_protocol::Frame;

/// Wrapper sobre TcpStream com buffer para leitura/escrita de frames RESP.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(INITIAL_BUFFER_CAPACITY),
        }
    }

    /// Lê um frame completo do stream. Retorna None no EOF.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err(ConnectionError::ConnectionReset);
            }
        }
    }

    /// Escreve um frame no stream.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        let mut cursor = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut cursor) {
            Ok(()) => {
                let len = cursor.position() as usize;
                cursor.set_position(0);
                let frame = Frame::parse(&mut cursor).map_err(|e| {
                    ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))
                })?;
                self.buffer = self.buffer.split_off(len);
                Ok(Some(frame))
            }
            Err(stormdb_common::ProtocolError::Incomplete) => Ok(None),
            Err(e) => Err(ConnectionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            ))),
        }
    }
}
