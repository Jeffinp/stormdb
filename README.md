# StormDB

In-memory data store compativel com protocolo Redis (RESP2), implementado em Rust.

## Arquitetura

```
stormdb/
├── crates/
│   ├── common/       # Tipos de erro, constantes
│   ├── protocol/     # Parser/encoder RESP2, Command enum
│   ├── storage/      # Engine in-memory (DashMap), expiry, pub/sub, AOF
│   ├── server/       # Servidor TCP async (Tokio)
│   └── cli/          # Cliente REPL interativo
```

## Comandos Suportados

| Comando                                | Descrição                                       |
| -------------------------------------- | ----------------------------------------------- |
| `PING [msg]`                           | Retorna PONG ou a mensagem                      |
| `ECHO msg`                             | Retorna a mensagem                              |
| `GET key`                              | Retorna o valor da chave                        |
| `SET key value [EX s\|PX ms] [NX\|XX]` | Define valor com opções de expiração e condição |
| `DEL key [key ...]`                    | Remove chaves                                   |
| `EXISTS key [key ...]`                 | Conta chaves existentes                         |
| `INCR key`                             | Incrementa valor inteiro                        |
| `DECR key`                             | Decrementa valor inteiro                        |
| `LPUSH key value [value ...]`          | Insere no início da lista                       |
| `RPUSH key value [value ...]`          | Insere no final da lista                        |
| `LPOP key [count]`                     | Remove do início da lista                       |
| `RPOP key [count]`                     | Remove do final da lista                        |
| `LRANGE key start stop`                | Retorna intervalo da lista                      |
| `SUBSCRIBE channel [channel ...]`      | Inscreve-se em canais                           |
| `PUBLISH channel message`              | Publica mensagem em canal                       |
| `UNSUBSCRIBE [channel ...]`            | Cancela inscrição                               |

## Quick Start

```bash
# Compilar
cargo build --release

# Iniciar servidor (porta 6399)
cargo run -p stormdb-server

# Iniciar servidor com AOF
cargo run -p stormdb-server -- --aof data.aof

# Conectar via CLI
cargo run -p stormdb-cli

# Ou usar redis-cli
redis-cli -p 6399
```

## Testes

```bash
# Todos os testes
cargo test --all

# Testes de um crate específico
cargo test -p stormdb-protocol

# Benchmarks
cargo bench
```

## Benchmarks

| Operação                       | Tempo   |
| ------------------------------ | ------- |
| Parse simple string            | ~33 ns  |
| Encode simple string           | ~27 ns  |
| Parse bulk 1KB                 | ~54 ns  |
| Parse SET command              | ~538 ns |
| SET+GET sequencial 10K         | ~4.1 ms |
| INCR sequencial 10K            | ~787 us |
| INCR concorrente 4 threads 10K | ~1.5 ms |
| RPUSH+LPOP 1K                  | ~216 us |

## Stack

- **Tokio** — runtime async, TCP, channels, signals
- **bytes** — zero-copy buffers
- **DashMap** — concurrent hashmap
- **thiserror** — error derive
- **clap** — CLI args
- **tracing** — structured logging
- **criterion** — benchmarks
