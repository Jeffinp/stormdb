use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 128;

/// Gerenciador de canais pub/sub.
#[derive(Debug)]
pub struct PubSub {
    channels: HashMap<String, broadcast::Sender<Bytes>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    /// Publica uma mensagem no canal. Retorna o número de subscribers que receberam.
    pub fn publish(&self, channel: &str, message: Bytes) -> usize {
        match self.channels.get(channel) {
            Some(tx) => tx.send(message).unwrap_or(0),
            None => 0,
        }
    }

    /// Inscreve-se em um canal. Retorna um Receiver para ouvir mensagens.
    pub fn subscribe(&mut self, channel: &str) -> broadcast::Receiver<Bytes> {
        let tx = self
            .channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        tx.subscribe()
    }

    /// Remove um canal se não tem mais subscribers.
    pub fn cleanup_channel(&mut self, channel: &str) {
        if let Some(tx) = self.channels.get(channel)
            && tx.receiver_count() == 0
        {
            self.channels.remove(channel);
        }
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}
