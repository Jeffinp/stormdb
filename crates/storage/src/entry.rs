use bytes::Bytes;
use std::collections::VecDeque;
use tokio::time::Instant;

/// Tipo do valor armazenado.
#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
}

/// Entrada no store: valor + TTL opcional.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Value, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|t| Instant::now() >= t)
            .unwrap_or(false)
    }
}
