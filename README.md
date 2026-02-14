# StormDB ⚡

**StormDB** é um banco de dados in-memory de alta performance, compatível com o protocolo Redis (RESP2), escrito puramente em **Rust**.

Este projeto demonstra a aplicação de conceitos avançados de sistemas distribuídos, concorrência segura (thread-safety) e arquitetura de software modular.

![License](https://img.shields.io/badge/license-MIT-blue)
![Rust](https://img.shields.io/badge/rust-1.93%2B-orange)
![Status](https://img.shields.io/badge/status-stable-green)

## 🚀 Funcionalidades

- **Alta Concorrência:** Utiliza `DashMap` para sharding automático e acesso lock-free em operações de leitura.
- **Async I/O:** Baseado no runtime `Tokio` para gerenciar milhares de conexões simultâneas de forma eficiente.
- **Persistência AOF:** Implementação de Append-Only File para durabilidade de dados, com política de `fsync` configurável.
- **Pub/Sub:** Sistema de mensageria em tempo real utilizando broadcast assíncrono otimizado com `tokio-stream`.
- **Replicação Master-Slave:** Suporte a clusters para alta disponibilidade e distribuição de leitura.
- **Monitoramento:** Ferramenta TUI (Terminal User Interface) integrada para visualização de métricas em tempo real.
- **Infraestrutura:** Configuração completa via Docker e Docker Compose.

## 🛠️ Arquitetura

O projeto segue a estrutura de Cargo Workspace para modularização:

- `crates/common`: Tipos compartilhados, constantes e definições de erro.
- `crates/protocol`: Parser e Encoder do protocolo RESP2, focado em alocação zero (Zero-Copy).
- `crates/storage`: Engine de dados, incluindo controle de expiração (TTL) e persistência AOF.
- `crates/server`: Camada de rede TCP, gerenciamento de conexões e lógica de replicação.
- `crates/cli`: Cliente de linha de comando para interação direta.
- `crates/monitor`: Dashboard de monitoramento via terminal.

```mermaid
graph TD
    User[Client / CLI] -->|TCP (RESP)| Master[StormDB Master]
    Monitor[TUI Dashboard] -.->|TCP (Stats)| Master
    
    subgraph "Cluster StormDB"
    Master -->|Broadcast Stream| Replica[StormDB Replica]
    Master -->|AOF| Disk[(Persistence)]
    end
    
    style Master fill:#e67e22,stroke:#333,stroke-width:2px,color:#fff
    style Replica fill:#3498db,stroke:#333,stroke-width:2px,color:#fff
```

## 🐳 Quick Start (Docker)

A maneira mais fácil de rodar o cluster completo (Master + Réplica).

```bash
# Sobe o Master (6379) e a Réplica (6380)
docker compose up --build
```

### Monitoramento Visual

Com o cluster rodando, abra outro terminal para visualizar o dashboard:

```bash
# Conecta o monitor TUI ao Master rodando no Docker
docker run -it --rm --network stormdb_stormnet stormdb-master stormdb-monitor --host master --port 6379
```

### Testando a Replicação

Abra um terceiro terminal para enviar comandos:

```bash
# Escreve no Master
docker exec -it stormdb-master stormdb-cli --port 6379 SET framework "Rust"

# Lê da Réplica (deve retornar "Rust")
docker exec -it stormdb-replica stormdb-cli --port 6380 GET framework
```

## 💻 Desenvolvimento Local

Se você tem Rust instalado (`1.93+`):

### 1. Iniciar o Servidor

Abra um terminal e inicie o servidor na porta padrão do Redis (6379):

```bash
cargo run -p stormdb-server -- --port 6379
```

### 2. Iniciar o Monitor

Em outro terminal, inicie o dashboard para ver as métricas:

```bash
cargo run -p stormdb-monitor -- --port 6379
```

### 3. Executar Comandos (CLI)

Em um terceiro terminal, você pode interagir com o banco:

```bash
# Comando único
cargo run -p stormdb-cli -- --port 6379 SET minha_chave "Funciona!"

# Recuperar valor
cargo run -p stormdb-cli -- --port 6379 GET minha_chave
```

### 4. Teste de Carga (Benchmark Visual)

Para ver o gráfico do monitor subir, execute este loop de inserção:

```bash
# Dica: Compile em release primeiro para máxima velocidade
cargo build --release --bin stormdb-cli

# Inserir 1000 chaves rapidamente
for i in {1..1000}; do ./target/release/stormdb-cli --port 6379 SET chave$i valor$i; done
```

## 📚 Comandos Suportados

| Categoria   | Comandos                                                         |
| ----------- | ---------------------------------------------------------------- |
| **String**  | `SET` (com opções EX, PX, NX, XX), `GET`, `INCR`, `DECR`, `ECHO` |
| **List**    | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`                       |
| **Generic** | `DEL`, `EXISTS`, `PING`, `DBSIZE`                                |
| **PubSub**  | `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`                            |
| **System**  | `REPLICAOF`                                                      |

## ⚡ Benchmarks

Testes preliminares em ambiente local (Linux, Release build):

| Operação | Latência Média | Throughput |
| -------- | -------------- | ---------- |
| PING     | ~30 µs         | 120k ops/s |
| SET      | ~45 µs         | 95k ops/s  |
| GET      | ~35 µs         | 110k ops/s |

---

_Desenvolvido com 🦀 e paixão por sistemas distribuídos._
