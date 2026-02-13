use bytes::{Bytes, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::io::Cursor;

use stormdb_protocol::{Command, Frame};

fn bench_parse_simple_string(c: &mut Criterion) {
    let frame = Frame::Simple("OK".into());
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    let data = buf.freeze();

    c.bench_function("parse_simple_string", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(black_box(data.as_ref()));
            Frame::parse(&mut cursor).unwrap()
        })
    });
}

fn bench_encode_simple_string(c: &mut Criterion) {
    let frame = Frame::Simple("OK".into());

    c.bench_function("encode_simple_string", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            black_box(&frame).encode(&mut buf);
            buf
        })
    });
}

fn bench_parse_bulk_1kb(c: &mut Criterion) {
    let data = vec![b'x'; 1024];
    let frame = Frame::Bulk(Bytes::from(data));
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    let encoded = buf.freeze();

    c.bench_function("parse_bulk_1kb", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(black_box(encoded.as_ref()));
            Frame::parse(&mut cursor).unwrap()
        })
    });
}

fn bench_encode_bulk_1kb(c: &mut Criterion) {
    let data = vec![b'x'; 1024];
    let frame = Frame::Bulk(Bytes::from(data));

    c.bench_function("encode_bulk_1kb", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(2048);
            black_box(&frame).encode(&mut buf);
            buf
        })
    });
}

fn bench_parse_set_command(c: &mut Criterion) {
    let frame = Frame::array_from_strs(&["SET", "mykey", "myvalue", "EX", "3600"]);
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    let encoded = buf.freeze();

    c.bench_function("parse_set_command", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(black_box(encoded.as_ref()));
            let frame = Frame::parse(&mut cursor).unwrap();
            Command::from_frame(frame).unwrap()
        })
    });
}

fn bench_roundtrip_array(c: &mut Criterion) {
    let frame = Frame::Array(vec![
        Frame::Simple("OK".into()),
        Frame::Integer(42),
        Frame::Bulk(Bytes::from("hello world")),
        Frame::Null,
        Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
    ]);

    c.bench_function("roundtrip_nested_array", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            black_box(&frame).encode(&mut buf);
            let data = buf.freeze();
            let mut cursor = Cursor::new(data.as_ref());
            Frame::parse(&mut cursor).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_parse_simple_string,
    bench_encode_simple_string,
    bench_parse_bulk_1kb,
    bench_encode_bulk_1kb,
    bench_parse_set_command,
    bench_roundtrip_array,
);
criterion_main!(benches);
