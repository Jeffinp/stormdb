use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info};

use stormdb_common::{DEFAULT_HOST, DEFAULT_PORT, MAX_CONNECTIONS};
use stormdb_server::{Connection, handle_connection, replication};
use stormdb_storage::{Db, FsyncPolicy, create_aof, replay_aof};

#[derive(Parser, Debug)]
#[command(name = "stormdb-server", about = "StormDB — in-memory data store")]
struct Args {
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(long, default_value_t = MAX_CONNECTIONS)]
    max_connections: usize,
    #[arg(long, value_name = "FILE")]
    aof: Option<PathBuf>,
    #[arg(long, default_value = "everysec", value_parser = parse_fsync)]
    fsync: FsyncPolicy,
    #[arg(long, num_args = 2, value_names = ["HOST", "PORT"])]
    replicaof: Option<Vec<String>>,
}

fn parse_fsync(s: &str) -> Result<FsyncPolicy, String> {
    match s.to_lowercase().as_str() {
        "always" => Ok(FsyncPolicy::Always),
        "everysec" => Ok(FsyncPolicy::EverySec),
        "no" => Ok(FsyncPolicy::No),
        _ => Err(format!("valor inválido: '{s}'. Use: always, everysec, no")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stormdb_server=info".into()),
        )
        .init();

    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    let db = Db::new();

    // Replay AOF se configurado
    let aof_tx = if let Some(ref aof_path) = args.aof {
        let count = replay_aof(aof_path, &db).await?;
        if count > 0 {
            info!("{count} comandos restaurados do AOF");
        }

        let (tx, writer) = create_aof(aof_path.clone(), args.fsync, 10_000);
        tokio::spawn(async move {
            if let Err(e) = writer.run().await {
                error!("AOF writer erro: {e}");
            }
        });
        Some(tx)
    } else {
        None
    };

    let listener = TcpListener::bind(&addr).await?;
    info!("StormDB escutando em {addr}");

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(args.max_connections));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Iniciar Replicação se configurado
    if let Some(replica_args) = args.replicaof {
        if replica_args.len() == 2 {
            let master_host = replica_args[0].clone();
            let master_port = replica_args[1].parse::<u16>().unwrap_or(6379);
            
            let db_replica = db.clone();
            let shutdown_replica = shutdown_tx.subscribe();
            
            tokio::spawn(async move {
                replication::replica_task(master_host, master_port, db_replica, shutdown_replica).await;
            });
        }
    }

    loop {
        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => permit.unwrap(),
            _ = signal::ctrl_c() => {
                info!("shutdown signal recebido");
                drop(shutdown_tx);
                break;
            }
        };

        let (socket, addr) = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok(v) => v,
                    Err(e) => {
                        error!("erro ao aceitar conexão: {e}");
                        continue;
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("shutdown signal recebido");
                drop(shutdown_tx);
                break;
            }
        };

        info!("nova conexão: {addr}");
        let db = db.clone();
        let aof_tx = aof_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        tokio::spawn(async move {
            let conn = Connection::new(socket);
            if let Err(e) = handle_connection(conn, db, &mut shutdown_rx, aof_tx).await {
                error!("erro na conexão {addr}: {e}");
            }
            info!("conexão encerrada: {addr}");
            drop(permit);
        });
    }

    // Drop aof_tx para fechar o writer
    drop(aof_tx);

    Ok(())
}
