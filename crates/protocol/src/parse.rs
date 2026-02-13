use bytes::Bytes;
use stormdb_common::CommandError;

use crate::Frame;

/// Cursor sobre um Frame::Array para extrair argumentos sequencialmente.
pub struct Parse {
    parts: Vec<Frame>,
    pos: usize,
}

impl Parse {
    /// Cria um Parse a partir de um Frame. O frame deve ser Array.
    pub fn new(frame: Frame) -> Result<Parse, CommandError> {
        match frame {
            Frame::Array(parts) => Ok(Parse { parts, pos: 0 }),
            _ => Err(CommandError::InvalidArgument("esperado array".into())),
        }
    }

    /// Retorna o próximo elemento como String (de Bulk ou Simple).
    pub fn next_string(&mut self) -> Result<String, CommandError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => String::from_utf8(data.to_vec())
                .map_err(|_| CommandError::InvalidArgument("string UTF-8 inválida".into())),
            _ => Err(CommandError::InvalidArgument(
                "esperado string ou bulk".into(),
            )),
        }
    }

    /// Retorna o próximo elemento como Bytes (de Bulk).
    pub fn next_bytes(&mut self) -> Result<Bytes, CommandError> {
        match self.next()? {
            Frame::Bulk(data) => Ok(data),
            Frame::Simple(s) => Ok(Bytes::from(s)),
            _ => Err(CommandError::InvalidArgument("esperado bulk".into())),
        }
    }

    /// Retorna o próximo elemento como i64.
    pub fn next_int(&mut self) -> Result<i64, CommandError> {
        match self.next()? {
            Frame::Integer(n) => Ok(n),
            Frame::Bulk(data) => {
                let s = std::str::from_utf8(&data)
                    .map_err(|_| CommandError::InvalidArgument("inteiro inválido".into()))?;
                s.parse::<i64>()
                    .map_err(|_| CommandError::InvalidArgument(format!("'{s}' não é um inteiro")))
            }
            Frame::Simple(s) => s
                .parse::<i64>()
                .map_err(|_| CommandError::InvalidArgument(format!("'{s}' não é um inteiro"))),
            _ => Err(CommandError::InvalidArgument("esperado inteiro".into())),
        }
    }

    /// Verifica se todos os argumentos foram consumidos.
    pub fn finish(&self) -> Result<(), CommandError> {
        if self.pos < self.parts.len() {
            Err(CommandError::InvalidArgument(
                "argumentos extras não esperados".into(),
            ))
        } else {
            Ok(())
        }
    }

    /// Verifica se ainda há argumentos restantes.
    pub fn has_remaining(&self) -> bool {
        self.pos < self.parts.len()
    }

    /// Retorna o número de argumentos restantes.
    pub fn remaining(&self) -> usize {
        self.parts.len() - self.pos
    }

    fn next(&mut self) -> Result<Frame, CommandError> {
        if self.pos >= self.parts.len() {
            return Err(CommandError::InvalidArgument(
                "argumentos insuficientes".into(),
            ));
        }
        let frame = self.parts[self.pos].clone();
        self.pos += 1;
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_extracts_strings() {
        let frame = Frame::array_from_strs(&["SET", "key", "value"]);
        let mut parse = Parse::new(frame).unwrap();
        assert_eq!(parse.next_string().unwrap(), "SET");
        assert_eq!(parse.next_string().unwrap(), "key");
        assert_eq!(parse.next_string().unwrap(), "value");
        parse.finish().unwrap();
    }

    #[test]
    fn parse_extracts_int_from_bulk() {
        let frame = Frame::array_from_strs(&["INCR", "counter"]);
        let mut parse = Parse::new(frame).unwrap();
        assert_eq!(parse.next_string().unwrap(), "INCR");
        assert_eq!(parse.next_string().unwrap(), "counter");
        parse.finish().unwrap();
    }

    #[test]
    fn parse_not_array_fails() {
        let frame = Frame::Simple("OK".into());
        assert!(Parse::new(frame).is_err());
    }

    #[test]
    fn parse_extra_args_fails_finish() {
        let frame = Frame::array_from_strs(&["PING", "extra"]);
        let mut parse = Parse::new(frame).unwrap();
        parse.next_string().unwrap();
        assert!(parse.finish().is_err());
    }

    #[test]
    fn parse_insufficient_args() {
        let frame = Frame::array_from_strs(&["GET"]);
        let mut parse = Parse::new(frame).unwrap();
        parse.next_string().unwrap();
        assert!(parse.next_string().is_err());
    }
}
