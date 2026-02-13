/// Erros de parsing do protocolo RESP.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame incompleto")]
    Incomplete,
    #[error("byte de tipo inválido: {0:#x}")]
    InvalidFrameType(u8),
    #[error("inteiro inválido: {0}")]
    InvalidInteger(String),
    #[error("comprimento de bulk inválido: {0}")]
    InvalidBulkLength(i64),
    #[error("frame excede tamanho máximo ({0} bytes)")]
    FrameTooLarge(usize),
    #[error("encoding inválido: {0}")]
    InvalidEncoding(String),
}

/// Erros de armazenamento/engine de dados.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("operação contra chave com tipo errado")]
    WrongType,
    #[error("valor não é um inteiro válido ou está fora do intervalo")]
    NotAnInteger,
    #[error("chave não encontrada")]
    KeyNotFound,
}

/// Erros de conexão TCP.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("conexão resetada pelo peer")]
    ConnectionReset,
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("servidor em shutdown")]
    Shutdown,
}

/// Erros de parsing/validação de comandos.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("comando desconhecido: {0}")]
    Unknown(String),
    #[error("número errado de argumentos para '{0}'")]
    WrongArity(String),
    #[error("opção inválida para SET: {0}")]
    InvalidSetOption(String),
    #[error("argumento inválido: {0}")]
    InvalidArgument(String),
}

/// Erro top-level do StormDB.
#[derive(Debug, thiserror::Error)]
pub enum StormError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Command(#[from] CommandError),
}

/// Result type alias.
pub type StormResult<T> = Result<T, StormError>;

// Conversão implícita de io::Error → StormError (via ConnectionError)
impl From<std::io::Error> for StormError {
    fn from(e: std::io::Error) -> Self {
        StormError::Connection(ConnectionError::Io(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_error_display() {
        let err = ProtocolError::Incomplete;
        assert_eq!(err.to_string(), "frame incompleto");
    }

    #[test]
    fn storage_error_display() {
        let err = StorageError::WrongType;
        assert_eq!(err.to_string(), "operação contra chave com tipo errado");
    }

    #[test]
    fn storm_error_from_protocol() {
        let err: StormError = ProtocolError::Incomplete.into();
        assert!(matches!(
            err,
            StormError::Protocol(ProtocolError::Incomplete)
        ));
    }

    #[test]
    fn storm_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err: StormError = io_err.into();
        assert!(matches!(
            err,
            StormError::Connection(ConnectionError::Io(_))
        ));
    }

    #[test]
    fn command_error_display() {
        let err = CommandError::WrongArity("GET".into());
        assert_eq!(err.to_string(), "número errado de argumentos para 'GET'");
    }
}
