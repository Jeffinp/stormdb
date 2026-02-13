use std::io::Cursor;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tracing::{debug, info, warn};

use stormdb_protocol::{Command, Frame};

use crate::Db;

/// Política de fsync.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    /// Fsync após cada write.
    Always,
    /// Fsync a cada segundo.
    #[default]
    EverySec,
    /// Sem fsync explícito (deixa pro OS).
    No,
}

/// Writer que recebe comandos via channel e faz append no arquivo AOF.
pub struct AofWriter {
    rx: mpsc::Receiver<Command>,
    path: PathBuf,
    policy: FsyncPolicy,
}

impl AofWriter {
    pub fn new(rx: mpsc::Receiver<Command>, path: PathBuf, policy: FsyncPolicy) -> Self {
        Self { rx, path, policy }
    }

    /// Loop principal: recebe comandos e escreve no arquivo.
    pub async fn run(mut self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        let mut writer = BufWriter::new(file);
        let mut tick = interval(Duration::from_secs(1));

        info!("AOF writer iniciado: {:?}", self.path);

        loop {
            tokio::select! {
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            let frame = cmd.to_frame();
                            let mut buf = BytesMut::new();
                            frame.encode(&mut buf);
                            writer.write_all(&buf).await?;

                            if self.policy == FsyncPolicy::Always {
                                writer.flush().await?;
                                writer.get_ref().sync_data().await?;
                            }
                        }
                        None => {
                            // Channel fechado — flush final
                            writer.flush().await?;
                            writer.get_ref().sync_data().await?;
                            info!("AOF writer encerrado");
                            return Ok(());
                        }
                    }
                }
                _ = tick.tick(), if self.policy == FsyncPolicy::EverySec => {
                    writer.flush().await?;
                    writer.get_ref().sync_data().await?;
                }
            }
        }
    }
}

/// Lê o arquivo AOF e re-executa os comandos no Db para reconstruir estado.
pub async fn replay_aof(path: &Path, db: &Db) -> std::io::Result<usize> {
    if !path.exists() {
        info!("arquivo AOF não encontrado, iniciando sem dados");
        return Ok(0);
    }

    let mut file = File::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    let mut cursor = Cursor::new(&data[..]);
    let mut count = 0;

    loop {
        if cursor.position() as usize >= data.len() {
            break;
        }

        // Tentar fazer check do frame
        let check_pos = cursor.position();
        match Frame::check(&mut cursor) {
            Ok(()) => {
                cursor.set_position(check_pos);
                match Frame::parse(&mut cursor) {
                    Ok(frame) => match Command::from_frame(frame) {
                        Ok(cmd) => {
                            apply_command(&cmd, db).await;
                            count += 1;
                        }
                        Err(e) => {
                            warn!("AOF: comando inválido ignorado: {e}");
                        }
                    },
                    Err(e) => {
                        warn!("AOF: frame corrompido, parando replay: {e}");
                        break;
                    }
                }
            }
            Err(stormdb_common::ProtocolError::Incomplete) => {
                warn!("AOF: frame incompleto no final do arquivo, parando replay");
                break;
            }
            Err(e) => {
                warn!("AOF: erro no frame, parando replay: {e}");
                break;
            }
        }
    }

    info!("AOF replay completo: {count} comandos restaurados");
    Ok(count)
}

/// Aplica um comando ao Db (replay do AOF).
async fn apply_command(cmd: &Command, db: &Db) {
    match cmd {
        Command::Set {
            key,
            value,
            options,
        } => {
            let _ = db.set(key.clone(), value.clone(), options);
        }
        Command::Del(keys) => {
            db.del(keys);
        }
        Command::Incr(key) => {
            let _ = db.incr(key);
        }
        Command::Decr(key) => {
            let _ = db.decr(key);
        }
        Command::LPush { key, values } => {
            let _ = db.lpush(key, values);
        }
        Command::RPush { key, values } => {
            let _ = db.rpush(key, values);
        }
        Command::LPop { key, count } => {
            let _ = db.lpop(key, *count);
        }
        Command::RPop { key, count } => {
            let _ = db.rpop(key, *count);
        }
        _ => {
            debug!("AOF: comando {cmd:?} ignorado no replay (read-only/pubsub)");
        }
    }
}

/// Cria um par (sender, AofWriter) para uso no servidor.
pub fn create_aof(
    path: PathBuf,
    policy: FsyncPolicy,
    buffer_size: usize,
) -> (mpsc::Sender<Command>, AofWriter) {
    let (tx, rx) = mpsc::channel(buffer_size);
    let writer = AofWriter::new(rx, path, policy);
    (tx, writer)
}

/// Determina se um comando deve ser persistido no AOF.
pub fn is_write_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Set { .. }
            | Command::Del(_)
            | Command::Incr(_)
            | Command::Decr(_)
            | Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use stormdb_protocol::SetOptions;
    use tempfile::tempdir;

    #[tokio::test]
    async fn aof_write_and_replay() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let db = Db::new();

        // Escrever comandos no AOF
        let (tx, writer) = create_aof(aof_path.clone(), FsyncPolicy::Always, 100);

        let writer_handle = tokio::spawn(async move {
            writer.run().await.unwrap();
        });

        // SET key1 value1
        let cmd = Command::Set {
            key: "key1".into(),
            value: Bytes::from("value1"),
            options: SetOptions {
                expire_ms: None,
                condition: None,
            },
        };
        tx.send(cmd.clone()).await.unwrap();
        apply_command(&cmd, &db).await;

        // INCR counter (3 vezes)
        for _ in 0..3 {
            let cmd = Command::Incr("counter".into());
            tx.send(cmd.clone()).await.unwrap();
            apply_command(&cmd, &db).await;
        }

        // RPUSH list a b
        let cmd = Command::RPush {
            key: "list".into(),
            values: vec![Bytes::from("a"), Bytes::from("b")],
        };
        tx.send(cmd.clone()).await.unwrap();
        apply_command(&cmd, &db).await;

        // Drop sender para fechar o writer
        drop(tx);
        writer_handle.await.unwrap();

        // Verificar estado original
        assert_eq!(db.get("key1"), Some(Bytes::from("value1")));
        assert_eq!(db.get("counter"), Some(Bytes::from("3")));
        assert_eq!(
            db.lrange("list", 0, -1).unwrap(),
            vec![Bytes::from("a"), Bytes::from("b")]
        );

        // Novo Db — replay
        let db2 = Db::new();
        let count = replay_aof(&aof_path, &db2).await.unwrap();
        assert_eq!(count, 5); // SET + 3x INCR + RPUSH

        assert_eq!(db2.get("key1"), Some(Bytes::from("value1")));
        assert_eq!(db2.get("counter"), Some(Bytes::from("3")));
        assert_eq!(
            db2.lrange("list", 0, -1).unwrap(),
            vec![Bytes::from("a"), Bytes::from("b")]
        );
    }

    #[tokio::test]
    async fn aof_replay_corrupted() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("corrupted.aof");

        // Escrever um frame válido + lixo no final
        let cmd = Command::Set {
            key: "key1".into(),
            value: Bytes::from("val"),
            options: SetOptions {
                expire_ms: None,
                condition: None,
            },
        };
        let frame = cmd.to_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        buf.extend_from_slice(b"$5\r\nhel"); // frame incompleto

        tokio::fs::write(&aof_path, &buf).await.unwrap();

        let db = Db::new();
        let count = replay_aof(&aof_path, &db).await.unwrap();
        assert_eq!(count, 1); // Apenas o primeiro comando válido
        assert_eq!(db.get("key1"), Some(Bytes::from("val")));
    }

    #[tokio::test]
    async fn aof_replay_nonexistent() {
        let db = Db::new();
        let count = replay_aof(Path::new("/tmp/nonexistent_stormdb.aof"), &db)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn is_write_command_check() {
        assert!(is_write_command(&Command::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            options: SetOptions {
                expire_ms: None,
                condition: None,
            },
        }));
        assert!(is_write_command(&Command::Del(vec!["k".into()])));
        assert!(is_write_command(&Command::Incr("k".into())));
        assert!(!is_write_command(&Command::Ping(None)));
        assert!(!is_write_command(&Command::Get("k".into())));
    }
}
