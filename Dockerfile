# Stage 1: Builder
FROM rust:1.93-slim-bookworm as builder

WORKDIR /app

# Instalar dependências de sistema necessárias para compilar (se houver)
# RUN apt-get update && apt-get install -y ...

# Copiar todo o código fonte
COPY . .

# Compilar todos os binários (server, cli, monitor) em modo release
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Instalar dependências mínimas (openssl, ca-certificates se precisar fazer reqs https)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copiar os binários do stage anterior
COPY --from=builder /app/target/release/stormdb-server /usr/local/bin/
COPY --from=builder /app/target/release/stormdb-cli /usr/local/bin/
COPY --from=builder /app/target/release/stormdb-monitor /usr/local/bin/

# Porta padrão do servidor
EXPOSE 6379

# Comando padrão (pode ser sobrescrito pelo docker-compose)
CMD ["stormdb-server", "--host", "0.0.0.0"]
