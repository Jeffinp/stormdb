use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use stormdb_common::ConnectionError;
use stormdb_protocol::{Command, Frame};
use stormdb_storage::Db;

use crate::Connection;

/// Tarefa de fundo que mantém a conexão com o Master.
pub async fn replica_task(
    master_host: String,
    master_port: u16,
    db: Db,
    mut shutdown: broadcast::Receiver<()>,
) {
    let addr = format!("{}:{}", master_host, master_port);
    info!("Iniciando replicação de {}", addr);

    loop {
        // Tentar conectar
        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "Falha ao conectar no Master {}: {}. Tentando em 1s...",
                    addr, e
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    _ = shutdown.recv() => return,
                }
            }
        };

        info!("Conectado ao Master {}!", addr);
        let mut conn = Connection::new(stream);

        // Handshake simples (PSYNC ou similar - por enquanto enviamos um PING para testar)
        // Num futuro, enviaríamos "PSYNC ? -1" para pedir sincronização total.
        if let Err(e) = conn
            .write_frame(&Frame::array_from_strs(&["PING", "REPLICA_HANDSHAKE"]))
            .await
        {
            error!("Erro no handshake com Master: {}", e);
            continue;
        }

        // Loop de processamento de comandos vindos do Master
        // Reutilizamos o handle_connection mas sem responder nada (réplica é passiva na rede)
        // PORÉM, o handle_connection atual tenta escrever na socket.
        // Precisamos de uma versão que APENAS aplique a escrita no DB local.

        loop {
            tokio::select! {
                result = conn.read_frame() => {
                    match result {
                        Ok(Some(frame)) => {
                            match Command::from_frame(frame) {
                                Ok(cmd) => {
                                    // Executar comando localmente (blindly apply)
                                    // Réplicas aplicam tudo o que o master manda.
                                    apply_replica_command(&cmd, &db).await;
                                }
                                Err(e) => error!("Erro ao parsear comando do Master: {}", e),
                            }
                        }
                        Ok(None) => {
                            warn!("Conexão com Master fechada. Reconectando...");
                            break;
                        }
                        Err(e) => {
                            error!("Erro de leitura do Master: {}", e);
                            break;
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Encerrando tarefa de replicação.");
                    return;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn apply_replica_command(cmd: &Command, db: &Db) {
    // Aqui executamos o comando direto no DB.
    // Como é réplica, ignoramos comandos de leitura (GET) vindos do master (não devem vir, mas ok)
    // E executamos os de escrita.

    // NOTA: Precisamos expor o `execute_command` do handler ou duplicar a lógica mínima.
    // Para simplificar, vou fazer um match manual nos comandos de escrita suportados.

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
        Command::Publish { channel, message } => {
            db.publish(channel, message.clone()).await;
        }
        // Ping e outros comandos de controle podem ser ignorados na replicação passiva por enquanto
        _ => {}
    }
}

/// Handler para o lado do MASTER: envia comandos para a réplica conectada.
pub async fn handle_replica_stream(
    mut conn: Connection,
    mut replication_rx: broadcast::Receiver<Command>,
) -> Result<(), ConnectionError> {
    info!("Iniciando stream de replicação para cliente.");
    // conn.write_frame(&Frame::Simple("OK".into())).await?; // Removido: causava erro no parser da réplica

    loop {
        match replication_rx.recv().await {
            Ok(cmd) => {
                // Converter comando para Frame e enviar
                let frame = cmd.to_frame();
                conn.write_frame(&frame).await?;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("Réplica atrasada: perdeu {} comandos.", n);
                // Em um sistema real, aqui fecharíamos a conexão para forçar full-resync
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Ok(());
            }
        }
    }
}
