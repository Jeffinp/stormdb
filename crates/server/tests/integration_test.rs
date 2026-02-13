use bytes::Bytes;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;

use stormdb_protocol::Frame;

/// Helper: conecta ao servidor e executa um comando, retornando o frame de resposta.
async fn send_command(stream: &mut TcpStream, args: &[&str]) -> Frame {
    let frame = Frame::array_from_strs(args);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // Ler resposta
    let mut response_buf = bytes::BytesMut::with_capacity(4096);
    loop {
        let n = stream.read_buf(&mut response_buf).await.unwrap();
        assert!(n > 0, "server closed connection unexpectedly");

        let mut cursor = Cursor::new(&response_buf[..]);
        if Frame::check(&mut cursor).is_ok() {
            cursor.set_position(0);
            return Frame::parse(&mut cursor).unwrap();
        }
    }
}

async fn start_server(port: u16) -> tokio::task::JoinHandle<()> {
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let db = stormdb_storage::Db::new();
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

        loop {
            let (socket, _) = tokio::select! {
                result = listener.accept() => result.unwrap(),
                _ = tokio::signal::ctrl_c() => break,
            };

            let db = db.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::spawn(async move {
                let conn = stormdb_server::Connection::new(socket);
                let _ = stormdb_server::handle_connection(conn, db, &mut shutdown_rx, None).await;
            });
        }
    });

    // Aguardar servidor estar pronto
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle
}

#[tokio::test]
async fn test_ping_pong() {
    let port = 16400;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["PING"]).await;
    assert_eq!(response, Frame::Simple("PONG".into()));
}

#[tokio::test]
async fn test_ping_with_message() {
    let port = 16401;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["PING", "hello"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("hello")));
}

#[tokio::test]
async fn test_set_get() {
    let port = 16402;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["SET", "mykey", "myvalue"]).await;
    assert_eq!(response, Frame::Simple("OK".into()));

    let response = send_command(&mut stream, &["GET", "mykey"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("myvalue")));
}

#[tokio::test]
async fn test_get_nonexistent() {
    let port = 16403;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["GET", "missing"]).await;
    assert_eq!(response, Frame::Null);
}

#[tokio::test]
async fn test_incr_decr() {
    let port = 16404;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["INCR", "counter"]).await;
    assert_eq!(response, Frame::Integer(1));

    let response = send_command(&mut stream, &["INCR", "counter"]).await;
    assert_eq!(response, Frame::Integer(2));

    let response = send_command(&mut stream, &["DECR", "counter"]).await;
    assert_eq!(response, Frame::Integer(1));
}

#[tokio::test]
async fn test_del_exists() {
    let port = 16405;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    send_command(&mut stream, &["SET", "a", "1"]).await;
    send_command(&mut stream, &["SET", "b", "2"]).await;

    let response = send_command(&mut stream, &["EXISTS", "a", "b", "c"]).await;
    assert_eq!(response, Frame::Integer(2));

    let response = send_command(&mut stream, &["DEL", "a"]).await;
    assert_eq!(response, Frame::Integer(1));

    let response = send_command(&mut stream, &["EXISTS", "a"]).await;
    assert_eq!(response, Frame::Integer(0));
}

#[tokio::test]
async fn test_list_operations() {
    let port = 16406;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["RPUSH", "list", "a", "b", "c"]).await;
    assert_eq!(response, Frame::Integer(3));

    let response = send_command(&mut stream, &["LRANGE", "list", "0", "-1"]).await;
    assert_eq!(
        response,
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("a")),
            Frame::Bulk(Bytes::from("b")),
            Frame::Bulk(Bytes::from("c")),
        ])
    );

    let response = send_command(&mut stream, &["LPUSH", "list", "z"]).await;
    assert_eq!(response, Frame::Integer(4));

    let response = send_command(&mut stream, &["LPOP", "list"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("z")));

    let response = send_command(&mut stream, &["RPOP", "list"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("c")));
}

#[tokio::test]
async fn test_set_with_ex() {
    let port = 16407;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["SET", "temp", "val", "PX", "100"]).await;
    assert_eq!(response, Frame::Simple("OK".into()));

    let response = send_command(&mut stream, &["GET", "temp"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("val")));

    tokio::time::sleep(Duration::from_millis(150)).await;

    let response = send_command(&mut stream, &["GET", "temp"]).await;
    assert_eq!(response, Frame::Null);
}

#[tokio::test]
async fn test_set_nx_xx() {
    let port = 16408;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // SET NX quando key não existe → OK
    let response = send_command(&mut stream, &["SET", "key", "v1", "NX"]).await;
    assert_eq!(response, Frame::Simple("OK".into()));

    // SET NX quando key existe → Null
    let response = send_command(&mut stream, &["SET", "key", "v2", "NX"]).await;
    assert_eq!(response, Frame::Null);

    // Valor deve ser v1
    let response = send_command(&mut stream, &["GET", "key"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("v1")));

    // SET XX quando key existe → OK
    let response = send_command(&mut stream, &["SET", "key", "v3", "XX"]).await;
    assert_eq!(response, Frame::Simple("OK".into()));

    // Valor deve ser v3
    let response = send_command(&mut stream, &["GET", "key"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("v3")));
}

#[tokio::test]
async fn test_echo() {
    let port = 16409;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["ECHO", "Hello, StormDB!"]).await;
    assert_eq!(response, Frame::Bulk(Bytes::from("Hello, StormDB!")));
}

#[tokio::test]
async fn test_unknown_command() {
    let port = 16410;
    let _server = start_server(port).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    let response = send_command(&mut stream, &["FOOBAR"]).await;
    match response {
        Frame::Error(msg) => assert!(msg.contains("unknown command")),
        _ => panic!("expected error frame"),
    }
}

/// Helper: envia um comando raw (sem ler resposta).
async fn send_raw(stream: &mut TcpStream, args: &[&str]) {
    let frame = Frame::array_from_strs(args);
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf);
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();
}

/// Helper: lê um frame do stream.
async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut response_buf = bytes::BytesMut::with_capacity(4096);
    loop {
        let n = stream.read_buf(&mut response_buf).await.unwrap();
        assert!(n > 0, "server closed connection unexpectedly");

        let mut cursor = Cursor::new(&response_buf[..]);
        if Frame::check(&mut cursor).is_ok() {
            cursor.set_position(0);
            return Frame::parse(&mut cursor).unwrap();
        }
    }
}

#[tokio::test]
async fn test_pubsub() {
    let port = 16411;
    let _server = start_server(port).await;

    // Client A: subscriber
    let mut sub_stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Enviar SUBSCRIBE
    send_raw(&mut sub_stream, &["SUBSCRIBE", "news"]).await;

    // Ler confirmação: ["subscribe", "news", 1]
    let confirm = read_frame(&mut sub_stream).await;
    match &confirm {
        Frame::Array(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Frame::Bulk(Bytes::from("subscribe")));
            assert_eq!(parts[1], Frame::Bulk(Bytes::from("news")));
            assert_eq!(parts[2], Frame::Integer(1));
        }
        _ => panic!("expected array for subscribe confirmation"),
    }

    // Client B: publisher
    let mut pub_stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Publicar mensagem
    let response = send_command(&mut pub_stream, &["PUBLISH", "news", "breaking!"]).await;
    assert_eq!(response, Frame::Integer(1)); // 1 subscriber recebeu

    // Client A deve receber: ["message", "news", "breaking!"]
    let msg = read_frame(&mut sub_stream).await;
    match &msg {
        Frame::Array(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Frame::Bulk(Bytes::from("message")));
            assert_eq!(parts[1], Frame::Bulk(Bytes::from("news")));
            assert_eq!(parts[2], Frame::Bulk(Bytes::from("breaking!")));
        }
        _ => panic!("expected array for message"),
    }

    // Publicar em canal sem subscribers
    let response = send_command(&mut pub_stream, &["PUBLISH", "empty", "hello"]).await;
    assert_eq!(response, Frame::Integer(0));
}
