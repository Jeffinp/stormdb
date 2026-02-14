use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};
use tracing::debug;

use stormdb_common::{ConnectionError, StorageError};
use stormdb_protocol::{Command, Frame};
use stormdb_storage::{Db, is_write_command};

use crate::Connection;

use crate::replication::handle_replica_stream;

/// Loop principal de tratamento de uma conexão.
pub async fn handle_connection(
    mut conn: Connection,
    db: Db,
    shutdown: &mut broadcast::Receiver<()>,
    aof_tx: Option<mpsc::Sender<Command>>,
    replication_tx: broadcast::Sender<Command>,
) -> Result<(), ConnectionError> {
    loop {
        let frame = tokio::select! {
            result = conn.read_frame() => result?,
            _ = shutdown.recv() => {
                return Ok(());
            }
        };

        let frame = match frame {
            Some(f) => f,
            None => return Ok(()), // EOF
        };

        let cmd = match Command::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(e) => {
                let response = Frame::Error(format!("ERR {e}"));
                conn.write_frame(&response).await?;
                continue;
            }
        };

        debug!("comando recebido: {cmd:?}");

        // Verificar Handshake de Réplica
        if let Command::Ping(Some(ref msg)) = cmd
            && msg.as_ref() == b"REPLICA_HANDSHAKE" {
                // Upgrade para conexão de réplica
                let rx = replication_tx.subscribe();
                handle_replica_stream(conn, rx).await?;
                return Ok(());
            }

        match cmd {
            Command::Subscribe(channels) => {
                handle_subscribe(&mut conn, &db, channels, shutdown).await?;
                return Ok(());
            }
            _ => {
                let response = execute_command(&cmd, &db).await;

                // Se é comando de escrita e foi bem-sucedido:
                // 1. Persistir no AOF
                // 2. Enviar para Replicação
                if is_write_command(&cmd) && !matches!(response, Frame::Error(_)) {
                    if let Some(ref tx) = aof_tx {
                        let _ = tx.send(cmd.clone()).await;
                    }
                    // Broadcast para réplicas (não bloqueante se buffer cheio)
                    let _ = replication_tx.send(cmd.clone());
                }

                conn.write_frame(&response).await?;
            }
        }
    }
}

/// Executa um comando e retorna o Frame de resposta.
async fn execute_command(cmd: &Command, db: &Db) -> Frame {
    match cmd {
        Command::Ping(msg) => match msg {
            Some(m) => Frame::Bulk(m.clone()),
            None => Frame::Simple("PONG".into()),
        },
        Command::Echo(msg) => Frame::Bulk(msg.clone()),
        Command::Get(key) => match db.get(key) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
        },
        Command::Set {
            key,
            value,
            options,
        } => match db.set(key.clone(), value.clone(), options) {
            Ok(true) => Frame::Simple("OK".into()),
            Ok(false) => Frame::Null, // NX/XX condition not met
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::Del(keys) => {
            let count = db.del(keys);
            Frame::Integer(count as i64)
        }
        Command::Exists(keys) => {
            let count = db.exists(keys);
            Frame::Integer(count as i64)
        }
        Command::Incr(key) => match db.incr(key) {
            Ok(n) => Frame::Integer(n),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(StorageError::NotAnInteger) => {
                Frame::Error("ERR value is not an integer or out of range".into())
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::Decr(key) => match db.decr(key) {
            Ok(n) => Frame::Integer(n),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(StorageError::NotAnInteger) => {
                Frame::Error("ERR value is not an integer or out of range".into())
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::LPush { key, values } => match db.lpush(key, values) {
            Ok(len) => Frame::Integer(len as i64),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::RPush { key, values } => match db.rpush(key, values) {
            Ok(len) => Frame::Integer(len as i64),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::LPop { key, count } => match db.lpop(key, *count) {
            Ok(items) if items.is_empty() => Frame::Null,
            Ok(items) if count.is_none() => Frame::Bulk(items.into_iter().next().unwrap()),
            Ok(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::RPop { key, count } => match db.rpop(key, *count) {
            Ok(items) if items.is_empty() => Frame::Null,
            Ok(items) if count.is_none() => Frame::Bulk(items.into_iter().next().unwrap()),
            Ok(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::LRange { key, start, stop } => match db.lrange(key, *start, *stop) {
            Ok(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
            Err(StorageError::WrongType) => Frame::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            ),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },
        Command::Publish { channel, message } => {
            let count = db.publish(channel, message.clone()).await;
            Frame::Integer(count as i64)
        }
        Command::DbSize => {
            let len = db.len();
            Frame::Integer(len as i64)
        }
        Command::Subscribe(_) => unreachable!("handled above"),
        Command::Unsubscribe(_) => Frame::Simple("OK".into()),
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),
    }
}

/// Handler dedicado para modo subscribe.
async fn handle_subscribe(
    conn: &mut Connection,
    db: &Db,
    channels: Vec<String>,
    shutdown: &mut broadcast::Receiver<()>,
) -> Result<(), ConnectionError> {
    let mut receivers = StreamMap::new();

    for (i, channel) in channels.iter().enumerate() {
        let rx = db.subscribe(channel).await;
        receivers.insert(channel.clone(), BroadcastStream::new(rx));

        let confirm = Frame::Array(vec![
            Frame::bulk("subscribe"),
            Frame::bulk(channel),
            Frame::Integer((i + 1) as i64),
        ]);
        conn.write_frame(&confirm).await?;
    }

    loop {
        tokio::select! {
            Some((channel, result)) = receivers.next() => {
                match result {
                    Ok(message) => {
                        let msg_frame = Frame::Array(vec![
                            Frame::bulk("message"),
                            Frame::bulk(&channel),
                            Frame::Bulk(message),
                        ]);
                        conn.write_frame(&msg_frame).await?;
                    }
                    Err(e) => {
                        debug!("erro no stream do canal {channel}: {e}");
                        receivers.remove(&channel);
                        if receivers.is_empty() {
                            return Ok(());
                        }
                    }
                }
            }
            result = conn.read_frame() => {
                match result? {
                    Some(frame) => {
                        if let Ok(cmd) = Command::from_frame(frame) {
                            match cmd {
                                Command::Unsubscribe(unsub_channels) => {
                                    let channels_to_unsub = if unsub_channels.is_empty() {
                                        receivers.keys().cloned().collect::<Vec<_>>()
                                    } else {
                                        unsub_channels
                                    };

                                    for ch in &channels_to_unsub {
                                        receivers.remove(ch);
                                        db.unsubscribe(ch).await;
                                    }

                                    let confirm = Frame::Array(vec![
                                        Frame::bulk("unsubscribe"),
                                        Frame::bulk(channels_to_unsub.first().map(|s| s.as_str()).unwrap_or("")),
                                        Frame::Integer(receivers.len() as i64),
                                    ]);
                                    conn.write_frame(&confirm).await?;

                                    if receivers.is_empty() {
                                        return Ok(());
                                    }
                                }
                                Command::Subscribe(new_channels) => {
                                    let current_count = receivers.len();
                                    for (i, channel) in new_channels.iter().enumerate() {
                                        let rx = db.subscribe(channel).await;
                                        receivers.insert(channel.clone(), BroadcastStream::new(rx));

                                        let confirm = Frame::Array(vec![
                                            Frame::bulk("subscribe"),
                                            Frame::bulk(channel),
                                            Frame::Integer((current_count + i + 1) as i64),
                                        ]);
                                        conn.write_frame(&confirm).await?;
                                    }
                                }
                                _ => {
                                    let err = Frame::Error("ERR only SUBSCRIBE / UNSUBSCRIBE are allowed in subscribe mode".into());
                                    conn.write_frame(&err).await?;
                                }
                            }
                        }
                    }
                    None => return Ok(()),
                }
            }
            _ = shutdown.recv() => {
                return Ok(());
            }
        }
    }
}
