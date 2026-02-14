use std::io::{self, Write};

use bytes::BytesMut;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use stormdb_common::{DEFAULT_HOST, DEFAULT_PORT};
use stormdb_protocol::Frame;

#[derive(Parser, Debug)]
#[command(name = "stormdb-cli", about = "StormDB CLI client")]
struct Args {
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,
    #[arg(long, short, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Comando para executar diretamente (modo não interativo)
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    let mut stream = TcpStream::connect(&addr).await?;
    
    // Modo comando único (via argumentos)
    if !args.command.is_empty() {
        let frame = Frame::array_from_strs(&args.command.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        execute_request(&mut stream, frame).await?;
        return Ok(());
    }

    println!("Conectado a {addr}");

    let stdin = io::stdin();
    let mut input = String::new();

    loop {
        print!("stormdb> ");
        io::stdout().flush()?;

        input.clear();
        if stdin.read_line(&mut input)? == 0 {
            break; // EOF
        }

        let line = input.trim();
        if line.is_empty() {
            continue;
        }

        if line.eq_ignore_ascii_case("quit") || line.eq_ignore_ascii_case("exit") {
            break;
        }

        let tokens = tokenize(line);
        if tokens.is_empty() {
            continue;
        }

        let frame = Frame::array_from_strs(&tokens.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        if let Err(e) = execute_request(&mut stream, frame).await {
             println!("(error) {}", e);
             // Tentar reconectar ou sair? Por enquanto apenas loga
        }
    }

    Ok(())
}

async fn execute_request(stream: &mut TcpStream, frame: Frame) -> anyhow::Result<()> {
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    stream.write_all(&buf).await?;
    stream.flush().await?;

    // Ler resposta
    let mut response_buf = BytesMut::with_capacity(4096);
    loop {
        let n = stream.read_buf(&mut response_buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("servidor fechou a conexão"));
        }

        let mut cursor = std::io::Cursor::new(&response_buf[..]);
        if Frame::check(&mut cursor).is_ok() {
            cursor.set_position(0);
            let response =
                Frame::parse(&mut cursor).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
            println!("{}", format_frame(&response, 0));
            break;
        }
    }
    Ok(())
}

/// Tokeniza a linha de input com suporte a strings quoted.
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut quote_char = '"';
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quote {
            if c == quote_char {
                in_quote = false;
            } else if c == '\\' {
                if let Some(&next) = chars.peek() {
                    match next {
                        'n' => {
                            current.push('\n');
                            chars.next();
                        }
                        't' => {
                            current.push('\t');
                            chars.next();
                        }
                        '\\' => {
                            current.push('\\');
                            chars.next();
                        }
                        '"' => {
                            current.push('"');
                            chars.next();
                        }
                        '\'' => {
                            current.push('\'');
                            chars.next();
                        }
                        _ => current.push(c),
                    }
                }
            } else {
                current.push(c);
            }
        } else if c == '"' || c == '\'' {
            in_quote = true;
            quote_char = c;
        } else if c.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(c);
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Formata um frame para exibição humana.
fn format_frame(frame: &Frame, indent: usize) -> String {
    let pad = " ".repeat(indent);
    match frame {
        Frame::Simple(s) => format!("{pad}\"{s}\""),
        Frame::Error(s) => format!("{pad}(error) {s}"),
        Frame::Integer(n) => format!("{pad}(integer) {n}"),
        Frame::Bulk(data) => match std::str::from_utf8(data) {
            Ok(s) => format!("{pad}\"{s}\""),
            Err(_) => format!("{pad}(binary) {} bytes", data.len()),
        },
        Frame::Null => format!("{pad}(nil)"),
        Frame::Array(frames) => {
            if frames.is_empty() {
                return format!("{pad}(empty array)");
            }
            let mut lines = Vec::new();
            for (i, f) in frames.iter().enumerate() {
                lines.push(format!("{pad}{}) {}", i + 1, format_frame(f, 0)));
            }
            lines.join("\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("SET key value"), vec!["SET", "key", "value"]);
    }

    #[test]
    fn tokenize_quoted() {
        assert_eq!(
            tokenize(r#"SET key "hello world""#),
            vec!["SET", "key", "hello world"]
        );
    }

    #[test]
    fn tokenize_single_quotes() {
        assert_eq!(
            tokenize("SET key 'hello world'"),
            vec!["SET", "key", "hello world"]
        );
    }

    #[test]
    fn tokenize_escaped() {
        assert_eq!(
            tokenize(r#"SET key "hello\"world""#),
            vec!["SET", "key", r#"hello"world"#]
        );
    }

    #[test]
    fn tokenize_empty() {
        assert_eq!(tokenize(""), Vec::<String>::new());
    }

    #[test]
    fn format_integer() {
        let frame = Frame::Integer(42);
        assert_eq!(format_frame(&frame, 0), "(integer) 42");
    }

    #[test]
    fn format_null() {
        assert_eq!(format_frame(&Frame::Null, 0), "(nil)");
    }

    #[test]
    fn format_error() {
        let frame = Frame::Error("ERR unknown command".into());
        assert_eq!(format_frame(&frame, 0), "(error) ERR unknown command");
    }
}
