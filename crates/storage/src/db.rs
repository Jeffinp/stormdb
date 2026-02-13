use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, broadcast};
use tokio::time::{Duration, Instant};
use tracing::debug;

use stormdb_common::StorageError;
use stormdb_protocol::{SetCondition, SetOptions};

use crate::entry::{Entry, Value};
use crate::pubsub::PubSub;

/// Item no BTreeSet de expiração: (instante, chave).
/// Ordenado por instante para purga eficiente.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ExpiryEntry(Instant, String);

/// Estado compartilhado entre todas as conexões.
struct SharedState {
    data: DashMap<String, Entry>,
    expiry: Mutex<BTreeSet<ExpiryEntry>>,
    pubsub: Mutex<PubSub>,
    notify_expiry: Notify,
}

/// Handle para o banco de dados in-memory.
#[derive(Clone)]
pub struct Db {
    shared: Arc<SharedState>,
}

impl Db {
    pub fn new() -> Self {
        let db = Db {
            shared: Arc::new(SharedState {
                data: DashMap::new(),
                expiry: Mutex::new(BTreeSet::new()),
                pubsub: Mutex::new(PubSub::new()),
                notify_expiry: Notify::new(),
            }),
        };

        // Spawn background task para purgar keys expiradas
        let shared = db.shared.clone();
        tokio::spawn(async move {
            purge_expired_keys(shared).await;
        });

        db
    }

    // --- String operations ---

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let entry = self.shared.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.shared.data.remove(key);
            return None;
        }
        match &entry.value {
            Value::String(data) => Some(data.clone()),
            Value::List(_) => None,
        }
    }

    pub fn set(
        &self,
        key: String,
        value: Bytes,
        options: &SetOptions,
    ) -> Result<bool, StorageError> {
        let expires_at = options
            .expire_ms
            .map(|ms| Instant::now() + Duration::from_millis(ms));

        // Verificar condição NX/XX
        if let Some(ref cond) = options.condition {
            let exists = self.shared.data.contains_key(&key);
            match cond {
                SetCondition::Nx if exists => return Ok(false),
                SetCondition::Xx if !exists => return Ok(false),
                _ => {}
            }
        }

        // Remover entrada expirada se existir
        if let Some(entry) = self.shared.data.get(&key)
            && entry.is_expired()
        {
            drop(entry);
            self.shared.data.remove(&key);
        }

        let entry = Entry::new(Value::String(value), expires_at);
        self.shared.data.insert(key.clone(), entry);

        if expires_at.is_some() {
            let shared = self.shared.clone();
            let key_clone = key;
            tokio::spawn(async move {
                let mut expiry = shared.expiry.lock().await;
                expiry.insert(ExpiryEntry(expires_at.unwrap(), key_clone));
                drop(expiry);
                shared.notify_expiry.notify_one();
            });
        }

        Ok(true)
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut count = 0;
        for key in keys {
            if self.shared.data.remove(key).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        let mut count = 0;
        for key in keys {
            if let Some(entry) = self.shared.data.get(key)
                && !entry.is_expired()
            {
                count += 1;
            }
        }
        count
    }

    pub fn incr(&self, key: &str) -> Result<i64, StorageError> {
        self.incr_by(key, 1)
    }

    pub fn decr(&self, key: &str) -> Result<i64, StorageError> {
        self.incr_by(key, -1)
    }

    fn incr_by(&self, key: &str, delta: i64) -> Result<i64, StorageError> {
        // Usar entry API do DashMap para atomicidade
        let mut entry = self
            .shared
            .data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::String(Bytes::from("0")), None));

        if entry.is_expired() {
            entry.value = Value::String(Bytes::from("0"));
            entry.expires_at = None;
        }

        match &entry.value {
            Value::String(data) => {
                let s = std::str::from_utf8(data).map_err(|_| StorageError::NotAnInteger)?;
                let n: i64 = s.parse().map_err(|_| StorageError::NotAnInteger)?;
                let new_val = n.checked_add(delta).ok_or(StorageError::NotAnInteger)?;
                entry.value = Value::String(Bytes::from(new_val.to_string()));
                Ok(new_val)
            }
            Value::List(_) => Err(StorageError::WrongType),
        }
    }

    // --- List operations ---

    pub fn lpush(&self, key: &str, values: &[Bytes]) -> Result<usize, StorageError> {
        let mut entry =
            self.shared.data.entry(key.to_string()).or_insert_with(|| {
                Entry::new(Value::List(std::collections::VecDeque::new()), None)
            });

        if entry.is_expired() {
            entry.value = Value::List(std::collections::VecDeque::new());
            entry.expires_at = None;
        }

        match &mut entry.value {
            Value::List(list) => {
                for v in values {
                    list.push_front(v.clone());
                }
                Ok(list.len())
            }
            Value::String(_) => Err(StorageError::WrongType),
        }
    }

    pub fn rpush(&self, key: &str, values: &[Bytes]) -> Result<usize, StorageError> {
        let mut entry =
            self.shared.data.entry(key.to_string()).or_insert_with(|| {
                Entry::new(Value::List(std::collections::VecDeque::new()), None)
            });

        if entry.is_expired() {
            entry.value = Value::List(std::collections::VecDeque::new());
            entry.expires_at = None;
        }

        match &mut entry.value {
            Value::List(list) => {
                for v in values {
                    list.push_back(v.clone());
                }
                Ok(list.len())
            }
            Value::String(_) => Err(StorageError::WrongType),
        }
    }

    pub fn lpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Bytes>, StorageError> {
        self.list_pop(key, count, true)
    }

    pub fn rpop(&self, key: &str, count: Option<usize>) -> Result<Vec<Bytes>, StorageError> {
        self.list_pop(key, count, false)
    }

    fn list_pop(
        &self,
        key: &str,
        count: Option<usize>,
        from_left: bool,
    ) -> Result<Vec<Bytes>, StorageError> {
        let mut entry = match self.shared.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(vec![]),
        };

        if entry.is_expired() {
            drop(entry);
            self.shared.data.remove(key);
            return Ok(vec![]);
        }

        match &mut entry.value {
            Value::List(list) => {
                let n = count.unwrap_or(1).min(list.len());
                let mut result = Vec::with_capacity(n);
                for _ in 0..n {
                    let item = if from_left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    if let Some(v) = item {
                        result.push(v);
                    }
                }
                // Limpar chave se lista ficou vazia
                if list.is_empty() {
                    drop(entry);
                    self.shared.data.remove(key);
                }
                Ok(result)
            }
            Value::String(_) => Err(StorageError::WrongType),
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, StorageError> {
        let entry = match self.shared.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![]),
        };

        if entry.is_expired() {
            drop(entry);
            self.shared.data.remove(key);
            return Ok(vec![]);
        }

        match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                // Normalizar índices negativos (estilo Redis)
                let s = if start < 0 {
                    (len + start).max(0) as usize
                } else {
                    start.min(len) as usize
                };
                let e = if stop < 0 {
                    (len + stop).max(-1) as usize
                } else {
                    stop.min(len - 1) as usize
                };

                if s > e || s >= list.len() {
                    return Ok(vec![]);
                }

                Ok(list.range(s..=e).cloned().collect())
            }
            Value::String(_) => Err(StorageError::WrongType),
        }
    }

    // --- Pub/Sub ---

    pub async fn publish(&self, channel: &str, message: Bytes) -> usize {
        let pubsub = self.shared.pubsub.lock().await;
        pubsub.publish(channel, message)
    }

    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<Bytes> {
        let mut pubsub = self.shared.pubsub.lock().await;
        pubsub.subscribe(channel)
    }

    pub async fn unsubscribe(&self, channel: &str) {
        let mut pubsub = self.shared.pubsub.lock().await;
        pubsub.cleanup_channel(channel);
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

/// Background task que purga chaves expiradas.
async fn purge_expired_keys(shared: Arc<SharedState>) {
    loop {
        let next_expiry = {
            let expiry = shared.expiry.lock().await;
            expiry.iter().next().map(|e| e.0)
        };

        match next_expiry {
            Some(when) => {
                tokio::select! {
                    _ = tokio::time::sleep_until(when) => {}
                    _ = shared.notify_expiry.notified() => { continue; }
                }
            }
            None => {
                shared.notify_expiry.notified().await;
                continue;
            }
        }

        // Purgar todas as chaves que expiraram
        let now = Instant::now();
        let mut expiry = shared.expiry.lock().await;
        let mut to_remove = Vec::new();

        for entry in expiry.iter() {
            if entry.0 <= now {
                to_remove.push(entry.clone());
            } else {
                break; // BTreeSet é ordenado, os próximos são todos futuros
            }
        }

        for entry in &to_remove {
            expiry.remove(entry);
            // Só remove se realmente expirou (pode ter sido re-setado)
            if let Some(e) = shared.data.get(&entry.1)
                && e.is_expired()
            {
                drop(e);
                shared.data.remove(&entry.1);
                debug!("key expirada removida: {}", entry.1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_set_basic() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("key".into(), Bytes::from("value"), &opts).unwrap();
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
    }

    #[tokio::test]
    async fn get_nonexistent() {
        let db = Db::new();
        assert_eq!(db.get("missing"), None);
    }

    #[tokio::test]
    async fn set_nx_key_exists() {
        let db = Db::new();
        let opts_none = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("key".into(), Bytes::from("v1"), &opts_none).unwrap();

        let opts_nx = SetOptions {
            expire_ms: None,
            condition: Some(SetCondition::Nx),
        };
        let result = db.set("key".into(), Bytes::from("v2"), &opts_nx).unwrap();
        assert!(!result); // não deve sobrescrever
        assert_eq!(db.get("key"), Some(Bytes::from("v1")));
    }

    #[tokio::test]
    async fn set_nx_key_not_exists() {
        let db = Db::new();
        let opts_nx = SetOptions {
            expire_ms: None,
            condition: Some(SetCondition::Nx),
        };
        let result = db.set("key".into(), Bytes::from("v1"), &opts_nx).unwrap();
        assert!(result);
        assert_eq!(db.get("key"), Some(Bytes::from("v1")));
    }

    #[tokio::test]
    async fn set_xx_key_exists() {
        let db = Db::new();
        let opts_none = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("key".into(), Bytes::from("v1"), &opts_none).unwrap();

        let opts_xx = SetOptions {
            expire_ms: None,
            condition: Some(SetCondition::Xx),
        };
        let result = db.set("key".into(), Bytes::from("v2"), &opts_xx).unwrap();
        assert!(result);
        assert_eq!(db.get("key"), Some(Bytes::from("v2")));
    }

    #[tokio::test]
    async fn set_xx_key_not_exists() {
        let db = Db::new();
        let opts_xx = SetOptions {
            expire_ms: None,
            condition: Some(SetCondition::Xx),
        };
        let result = db.set("key".into(), Bytes::from("v1"), &opts_xx).unwrap();
        assert!(!result);
        assert_eq!(db.get("key"), None);
    }

    #[tokio::test]
    async fn set_with_expiry() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: Some(50), // 50ms
            condition: None,
        };
        db.set("key".into(), Bytes::from("value"), &opts).unwrap();
        assert_eq!(db.get("key"), Some(Bytes::from("value")));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(db.get("key"), None);
    }

    #[tokio::test]
    async fn del_keys() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("a".into(), Bytes::from("1"), &opts).unwrap();
        db.set("b".into(), Bytes::from("2"), &opts).unwrap();

        let deleted = db.del(&["a".into(), "b".into(), "c".into()]);
        assert_eq!(deleted, 2);
        assert_eq!(db.get("a"), None);
    }

    #[tokio::test]
    async fn exists_keys() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("a".into(), Bytes::from("1"), &opts).unwrap();

        assert_eq!(db.exists(&["a".into(), "b".into()]), 1);
    }

    #[tokio::test]
    async fn incr_decr_basic() {
        let db = Db::new();
        // INCR em chave inexistente deve criar com 0+1=1
        assert_eq!(db.incr("counter").unwrap(), 1);
        assert_eq!(db.incr("counter").unwrap(), 2);
        assert_eq!(db.decr("counter").unwrap(), 1);
        assert_eq!(db.decr("counter").unwrap(), 0);
        assert_eq!(db.decr("counter").unwrap(), -1);
    }

    #[tokio::test]
    async fn incr_not_integer() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("key".into(), Bytes::from("not_a_number"), &opts)
            .unwrap();
        assert!(matches!(db.incr("key"), Err(StorageError::NotAnInteger)));
    }

    #[tokio::test]
    async fn incr_wrong_type() {
        let db = Db::new();
        db.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(matches!(db.incr("list"), Err(StorageError::WrongType)));
    }

    #[tokio::test]
    async fn lpush_rpush() {
        let db = Db::new();
        assert_eq!(
            db.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
                .unwrap(),
            2
        );
        assert_eq!(db.lpush("list", &[Bytes::from("c")]).unwrap(), 3);
        // list = [c, a, b]
        let range = db.lrange("list", 0, -1).unwrap();
        assert_eq!(
            range,
            vec![Bytes::from("c"), Bytes::from("a"), Bytes::from("b")]
        );
    }

    #[tokio::test]
    async fn lpop_rpop() {
        let db = Db::new();
        db.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();

        let popped = db.lpop("list", None).unwrap();
        assert_eq!(popped, vec![Bytes::from("a")]);

        let popped = db.rpop("list", Some(2)).unwrap();
        assert_eq!(popped, vec![Bytes::from("c"), Bytes::from("b")]);

        // Lista deve estar vazia e a chave removida
        assert_eq!(db.lrange("list", 0, -1).unwrap(), Vec::<Bytes>::new());
    }

    #[tokio::test]
    async fn lrange_negative_indices() {
        let db = Db::new();
        db.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d"),
            ],
        )
        .unwrap();

        // Últimos 2 elementos
        let range = db.lrange("list", -2, -1).unwrap();
        assert_eq!(range, vec![Bytes::from("c"), Bytes::from("d")]);

        // Primeiro ao penúltimo
        let range = db.lrange("list", 0, -2).unwrap();
        assert_eq!(
            range,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[tokio::test]
    async fn lrange_out_of_bounds() {
        let db = Db::new();
        db.rpush("list", &[Bytes::from("a")]).unwrap();

        let range = db.lrange("list", 0, 100).unwrap();
        assert_eq!(range, vec![Bytes::from("a")]);

        let range = db.lrange("list", 5, 10).unwrap();
        assert!(range.is_empty());
    }

    #[tokio::test]
    async fn wrong_type_list_on_string() {
        let db = Db::new();
        let opts = SetOptions {
            expire_ms: None,
            condition: None,
        };
        db.set("key".into(), Bytes::from("value"), &opts).unwrap();
        assert!(matches!(
            db.lpush("key", &[Bytes::from("a")]),
            Err(StorageError::WrongType)
        ));
    }

    #[tokio::test]
    async fn wrong_type_string_on_list() {
        let db = Db::new();
        db.rpush("list", &[Bytes::from("a")]).unwrap();
        // GET em lista deve retornar None (não erro, similar ao Redis)
        assert_eq!(db.get("list"), None);
    }

    #[tokio::test]
    async fn pubsub_basic() {
        let db = Db::new();
        let mut rx = db.subscribe("ch1").await;
        let count = db.publish("ch1", Bytes::from("hello")).await;
        assert_eq!(count, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn pubsub_no_subscribers() {
        let db = Db::new();
        let count = db.publish("ch1", Bytes::from("hello")).await;
        assert_eq!(count, 0);
    }
}
