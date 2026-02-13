use bytes::Bytes;
use stormdb_common::CommandError;

use crate::{Frame, Parse};

/// Condição para SET (NX ou XX).
#[derive(Debug, Clone, PartialEq)]
pub enum SetCondition {
    /// Só seta se a chave não existir.
    Nx,
    /// Só seta se a chave já existir.
    Xx,
}

/// Opções do comando SET.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOptions {
    pub expire_ms: Option<u64>,
    pub condition: Option<SetCondition>,
}

/// Enum com todos os comandos suportados.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(String),
    Set {
        key: String,
        value: Bytes,
        options: SetOptions,
    },
    Del(Vec<String>),
    Exists(Vec<String>),
    Incr(String),
    Decr(String),
    LPush {
        key: String,
        values: Vec<Bytes>,
    },
    RPush {
        key: String,
        values: Vec<Bytes>,
    },
    LPop {
        key: String,
        count: Option<usize>,
    },
    RPop {
        key: String,
        count: Option<usize>,
    },
    LRange {
        key: String,
        start: i64,
        stop: i64,
    },
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Publish {
        channel: String,
        message: Bytes,
    },
    Unknown(String),
}

impl Command {
    /// Faz o parse de um Frame em um Command.
    pub fn from_frame(frame: Frame) -> Result<Command, CommandError> {
        let mut parse = Parse::new(frame)?;
        let cmd_name = parse.next_string()?.to_uppercase();

        let cmd = match cmd_name.as_str() {
            "PING" => {
                let msg = if parse.has_remaining() {
                    Some(parse.next_bytes()?)
                } else {
                    None
                };
                parse.finish()?;
                Command::Ping(msg)
            }
            "ECHO" => {
                let msg = parse.next_bytes()?;
                parse.finish()?;
                Command::Echo(msg)
            }
            "GET" => {
                let key = parse.next_string()?;
                parse.finish()?;
                Command::Get(key)
            }
            "SET" => parse_set(&mut parse)?,
            "DEL" => {
                if !parse.has_remaining() {
                    return Err(CommandError::WrongArity("DEL".into()));
                }
                let mut keys = Vec::new();
                while parse.has_remaining() {
                    keys.push(parse.next_string()?);
                }
                Command::Del(keys)
            }
            "EXISTS" => {
                if !parse.has_remaining() {
                    return Err(CommandError::WrongArity("EXISTS".into()));
                }
                let mut keys = Vec::new();
                while parse.has_remaining() {
                    keys.push(parse.next_string()?);
                }
                Command::Exists(keys)
            }
            "INCR" => {
                let key = parse.next_string()?;
                parse.finish()?;
                Command::Incr(key)
            }
            "DECR" => {
                let key = parse.next_string()?;
                parse.finish()?;
                Command::Decr(key)
            }
            "LPUSH" => {
                let key = parse.next_string()?;
                if !parse.has_remaining() {
                    return Err(CommandError::WrongArity("LPUSH".into()));
                }
                let mut values = Vec::new();
                while parse.has_remaining() {
                    values.push(parse.next_bytes()?);
                }
                Command::LPush { key, values }
            }
            "RPUSH" => {
                let key = parse.next_string()?;
                if !parse.has_remaining() {
                    return Err(CommandError::WrongArity("RPUSH".into()));
                }
                let mut values = Vec::new();
                while parse.has_remaining() {
                    values.push(parse.next_bytes()?);
                }
                Command::RPush { key, values }
            }
            "LPOP" => {
                let key = parse.next_string()?;
                let count = if parse.has_remaining() {
                    Some(parse.next_int()? as usize)
                } else {
                    None
                };
                parse.finish()?;
                Command::LPop { key, count }
            }
            "RPOP" => {
                let key = parse.next_string()?;
                let count = if parse.has_remaining() {
                    Some(parse.next_int()? as usize)
                } else {
                    None
                };
                parse.finish()?;
                Command::RPop { key, count }
            }
            "LRANGE" => {
                let key = parse.next_string()?;
                let start = parse.next_int()?;
                let stop = parse.next_int()?;
                parse.finish()?;
                Command::LRange { key, start, stop }
            }
            "SUBSCRIBE" => {
                if !parse.has_remaining() {
                    return Err(CommandError::WrongArity("SUBSCRIBE".into()));
                }
                let mut channels = Vec::new();
                while parse.has_remaining() {
                    channels.push(parse.next_string()?);
                }
                Command::Subscribe(channels)
            }
            "UNSUBSCRIBE" => {
                let mut channels = Vec::new();
                while parse.has_remaining() {
                    channels.push(parse.next_string()?);
                }
                Command::Unsubscribe(channels)
            }
            "PUBLISH" => {
                let channel = parse.next_string()?;
                let message = parse.next_bytes()?;
                parse.finish()?;
                Command::Publish { channel, message }
            }
            _ => Command::Unknown(cmd_name),
        };

        Ok(cmd)
    }

    /// Encoda o comando como Frame para envio via RESP.
    pub fn to_frame(&self) -> Frame {
        match self {
            Command::Ping(None) => Frame::Array(vec![Frame::bulk("PING")]),
            Command::Ping(Some(msg)) => {
                Frame::Array(vec![Frame::bulk("PING"), Frame::Bulk(msg.clone())])
            }
            Command::Echo(msg) => Frame::Array(vec![Frame::bulk("ECHO"), Frame::Bulk(msg.clone())]),
            Command::Get(key) => Frame::Array(vec![Frame::bulk("GET"), Frame::bulk(key)]),
            Command::Set {
                key,
                value,
                options,
            } => {
                let mut parts = vec![
                    Frame::bulk("SET"),
                    Frame::bulk(key),
                    Frame::Bulk(value.clone()),
                ];
                if let Some(ms) = options.expire_ms {
                    parts.push(Frame::bulk("PX"));
                    parts.push(Frame::bulk(&ms.to_string()));
                }
                if let Some(ref cond) = options.condition {
                    match cond {
                        SetCondition::Nx => parts.push(Frame::bulk("NX")),
                        SetCondition::Xx => parts.push(Frame::bulk("XX")),
                    }
                }
                Frame::Array(parts)
            }
            Command::Del(keys) => {
                let mut parts = vec![Frame::bulk("DEL")];
                parts.extend(keys.iter().map(|k| Frame::bulk(k)));
                Frame::Array(parts)
            }
            Command::Exists(keys) => {
                let mut parts = vec![Frame::bulk("EXISTS")];
                parts.extend(keys.iter().map(|k| Frame::bulk(k)));
                Frame::Array(parts)
            }
            Command::Incr(key) => Frame::Array(vec![Frame::bulk("INCR"), Frame::bulk(key)]),
            Command::Decr(key) => Frame::Array(vec![Frame::bulk("DECR"), Frame::bulk(key)]),
            Command::LPush { key, values } => {
                let mut parts = vec![Frame::bulk("LPUSH"), Frame::bulk(key)];
                parts.extend(values.iter().map(|v| Frame::Bulk(v.clone())));
                Frame::Array(parts)
            }
            Command::RPush { key, values } => {
                let mut parts = vec![Frame::bulk("RPUSH"), Frame::bulk(key)];
                parts.extend(values.iter().map(|v| Frame::Bulk(v.clone())));
                Frame::Array(parts)
            }
            Command::LPop { key, count } => {
                let mut parts = vec![Frame::bulk("LPOP"), Frame::bulk(key)];
                if let Some(c) = count {
                    parts.push(Frame::bulk(&c.to_string()));
                }
                Frame::Array(parts)
            }
            Command::RPop { key, count } => {
                let mut parts = vec![Frame::bulk("RPOP"), Frame::bulk(key)];
                if let Some(c) = count {
                    parts.push(Frame::bulk(&c.to_string()));
                }
                Frame::Array(parts)
            }
            Command::LRange { key, start, stop } => Frame::Array(vec![
                Frame::bulk("LRANGE"),
                Frame::bulk(key),
                Frame::bulk(&start.to_string()),
                Frame::bulk(&stop.to_string()),
            ]),
            Command::Subscribe(channels) => {
                let mut parts = vec![Frame::bulk("SUBSCRIBE")];
                parts.extend(channels.iter().map(|c| Frame::bulk(c)));
                Frame::Array(parts)
            }
            Command::Unsubscribe(channels) => {
                let mut parts = vec![Frame::bulk("UNSUBSCRIBE")];
                parts.extend(channels.iter().map(|c| Frame::bulk(c)));
                Frame::Array(parts)
            }
            Command::Publish { channel, message } => Frame::Array(vec![
                Frame::bulk("PUBLISH"),
                Frame::bulk(channel),
                Frame::Bulk(message.clone()),
            ]),
            Command::Unknown(name) => Frame::Array(vec![Frame::bulk(name)]),
        }
    }
}

fn parse_set(parse: &mut Parse) -> Result<Command, CommandError> {
    let key = parse.next_string()?;
    let value = parse.next_bytes()?;

    let mut options = SetOptions {
        expire_ms: None,
        condition: None,
    };

    while parse.has_remaining() {
        let opt = parse.next_string()?.to_uppercase();
        match opt.as_str() {
            "EX" => {
                let secs = parse.next_int()?;
                if secs <= 0 {
                    return Err(CommandError::InvalidSetOption(
                        "EX deve ser positivo".into(),
                    ));
                }
                options.expire_ms = Some(secs as u64 * 1000);
            }
            "PX" => {
                let ms = parse.next_int()?;
                if ms <= 0 {
                    return Err(CommandError::InvalidSetOption(
                        "PX deve ser positivo".into(),
                    ));
                }
                options.expire_ms = Some(ms as u64);
            }
            "NX" => {
                options.condition = Some(SetCondition::Nx);
            }
            "XX" => {
                options.condition = Some(SetCondition::Xx);
            }
            other => {
                return Err(CommandError::InvalidSetOption(other.to_string()));
            }
        }
    }

    Ok(Command::Set {
        key,
        value,
        options,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ping() {
        let frame = Frame::array_from_strs(&["PING"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Ping(None));
    }

    #[test]
    fn parse_ping_with_message() {
        let frame = Frame::array_from_strs(&["PING", "hello"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Ping(Some(Bytes::from("hello"))));
    }

    #[test]
    fn parse_echo() {
        let frame = Frame::array_from_strs(&["ECHO", "hello world"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Echo(Bytes::from("hello world")));
    }

    #[test]
    fn parse_get() {
        let frame = Frame::array_from_strs(&["GET", "mykey"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Get("mykey".into()));
    }

    #[test]
    fn parse_set_simple() {
        let frame = Frame::array_from_strs(&["SET", "key", "value"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: "key".into(),
                value: Bytes::from("value"),
                options: SetOptions {
                    expire_ms: None,
                    condition: None,
                },
            }
        );
    }

    #[test]
    fn parse_set_with_ex() {
        let frame = Frame::array_from_strs(&["SET", "key", "value", "EX", "10"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set { options, .. } => {
                assert_eq!(options.expire_ms, Some(10_000));
                assert_eq!(options.condition, None);
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn parse_set_with_px_nx() {
        let frame = Frame::array_from_strs(&["SET", "key", "value", "PX", "5000", "NX"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set { options, .. } => {
                assert_eq!(options.expire_ms, Some(5000));
                assert_eq!(options.condition, Some(SetCondition::Nx));
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn parse_set_xx() {
        let frame = Frame::array_from_strs(&["SET", "key", "value", "XX"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set { options, .. } => {
                assert_eq!(options.condition, Some(SetCondition::Xx));
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn parse_del_multiple() {
        let frame = Frame::array_from_strs(&["DEL", "a", "b", "c"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Del(vec!["a".into(), "b".into(), "c".into()]));
    }

    #[test]
    fn parse_exists() {
        let frame = Frame::array_from_strs(&["EXISTS", "key1"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Exists(vec!["key1".into()]));
    }

    #[test]
    fn parse_incr_decr() {
        let frame = Frame::array_from_strs(&["INCR", "counter"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::Incr("counter".into())
        );

        let frame = Frame::array_from_strs(&["DECR", "counter"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::Decr("counter".into())
        );
    }

    #[test]
    fn parse_lpush_rpush() {
        let frame = Frame::array_from_strs(&["LPUSH", "list", "a", "b"]);
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::LPush {
                key: "list".into(),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            }
        );
    }

    #[test]
    fn parse_lpop_rpop() {
        let frame = Frame::array_from_strs(&["LPOP", "list"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::LPop {
                key: "list".into(),
                count: None,
            }
        );

        let frame = Frame::array_from_strs(&["RPOP", "list", "3"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::RPop {
                key: "list".into(),
                count: Some(3),
            }
        );
    }

    #[test]
    fn parse_lrange() {
        let frame = Frame::array_from_strs(&["LRANGE", "list", "0", "-1"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::LRange {
                key: "list".into(),
                start: 0,
                stop: -1,
            }
        );
    }

    #[test]
    fn parse_subscribe() {
        let frame = Frame::array_from_strs(&["SUBSCRIBE", "ch1", "ch2"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::Subscribe(vec!["ch1".into(), "ch2".into()])
        );
    }

    #[test]
    fn parse_publish() {
        let frame = Frame::array_from_strs(&["PUBLISH", "ch1", "hello"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::Publish {
                channel: "ch1".into(),
                message: Bytes::from("hello"),
            }
        );
    }

    #[test]
    fn parse_unknown_command() {
        let frame = Frame::array_from_strs(&["FOOBAR"]);
        assert_eq!(
            Command::from_frame(frame).unwrap(),
            Command::Unknown("FOOBAR".into())
        );
    }

    #[test]
    fn case_insensitive_commands() {
        let frame = Frame::array_from_strs(&["ping"]);
        assert_eq!(Command::from_frame(frame).unwrap(), Command::Ping(None));

        let frame = Frame::array_from_strs(&["set", "k", "v", "ex", "5"]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set { options, .. } => assert_eq!(options.expire_ms, Some(5000)),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn wrong_arity_del() {
        let frame = Frame::array_from_strs(&["DEL"]);
        assert!(Command::from_frame(frame).is_err());
    }

    #[test]
    fn invalid_set_option() {
        let frame = Frame::array_from_strs(&["SET", "k", "v", "INVALID"]);
        assert!(Command::from_frame(frame).is_err());
    }
}
