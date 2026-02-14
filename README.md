# StormDB

StormDB é um banco de dados in-memory de alta performance, compatível com o protocolo Redis (RESP2), implementado em Rust.

O projeto demonstra a aplicação de conceitos de sistemas distribuídos, concorrência segura (thread-safety) e arquitetura de software modular.

## Funcionalidades

- **Alta Concorrência:** Utiliza `DashMap` para sharding automático e acesso lock-free em operações de leitura.
- **Async I/O:** Baseado no runtime `Tokio` para gerenciar milhares de conexões simultâneas de forma eficiente.
- **Persistência AOF:** Implementação de Append-Only File para durabilidade de dados, com política de `fsync` configurável.
- **Pub/Sub:** Sistema de mensageria em tempo real utilizando broadcast assíncrono otimizado com `tokio-stream`.
- **Replicação Master-Slave:** Suporte a clusters para alta disponibilidade e distribuição de leitura.
- **Monitoramento:** Ferramenta TUI (Terminal User Interface) integrada para visualização de métricas em tempo real.
- **Infraestrutura:** Configuração completa via Docker e Docker Compose.

## Arquitetura

O projeto segue a estrutura de Cargo Workspace para modularização:

- `crates/common`: Tipos compartilhados, constantes e definições de erro.
- `crates/protocol`: Parser e Encoder do protocolo RESP2, focado em alocação zero (Zero-Copy).
- `crates/storage`: Engine de dados, incluindo controle de expiração (TTL) e persistência AOF.
- `crates/server`: Camada de rede TCP, gerenciamento de conexões e lógica de replicação.
- `crates/cli`: Cliente de linha de comando para interação direta.
- `crates/monitor`: Dashboard de monitoramento via terminal.

## Executando com Docker

Para iniciar o cluster completo (Master e Réplica) utilizando Docker Compose:

```bash
docker compose up --build
```

### Monitoramento

Com o cluster em execução, o monitor TUI pode ser iniciado em um container separado:

```bash
docker run -it --rm --network stormdb_stormnet stormdb-master stormdb-monitor --host master --port 6379
```

### Teste de Replicação

Para validar a sincronização de dados entre Master e Réplica:

```bash
# Escrita no Master
docker exec -it stormdb-master stormdb-cli SET chave valor

# Leitura na Réplica
docker exec -it stormdb-replica stormdb-cli GET chave
```

## Desenvolvimento Local

Requisitos: Rust 1.93 ou superior.

1. Iniciar o servidor (porta padrão 6379):
   ```bash
   cargo run -p stormdb-server -- --port 6379
   ```

2. Iniciar o monitor:
   ```bash
   cargo run -p stormdb-monitor -- --port 6379
   ```

3. Executar comandos via CLI:
   ```bash
   cargo run -p stormdb-cli SET framework Rust
   ```

## Comandos Suportados

- **String:** `SET` (com opções EX, PX, NX, XX), `GET`, `INCR`, `DECR`, `ECHO`
- **List:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`
- **Generic:** `DEL`, `EXISTS`, `PING`, `DBSIZE`
- **PubSub:** `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`
- **Replication:** `REPLICAOF`

## Benchmarks

Testes preliminares em ambiente local (Linux, Release build):

| Operação | Latência Média | Throughput |
|----------|----------------|------------|
| PING     | ~30 µs         | 120k ops/s |
| SET      | ~45 µs         | 95k ops/s  |
| GET      | ~35 µs         | 110k ops/s |