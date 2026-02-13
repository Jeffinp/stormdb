use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use stormdb_protocol::SetOptions;
use stormdb_storage::Db;

fn bench_set_get_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("set_get_sequential_10k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::new();
                let opts = SetOptions {
                    expire_ms: None,
                    condition: None,
                };
                for i in 0..10_000 {
                    let key = format!("key:{i}");
                    let value = Bytes::from(format!("value:{i}"));
                    db.set(key.clone(), value, &opts).unwrap();
                    black_box(db.get(&key));
                }
            });
        })
    });
}

fn bench_incr_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("incr_sequential_10k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::new();
                for _ in 0..10_000 {
                    black_box(db.incr("counter").unwrap());
                }
            });
        })
    });
}

fn bench_incr_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("incr_concurrent_4_threads_10k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::new();
                let mut handles = Vec::new();

                for _ in 0..4 {
                    let db = db.clone();
                    handles.push(tokio::spawn(async move {
                        for _ in 0..2_500 {
                            black_box(db.incr("counter").unwrap());
                        }
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
            });
        })
    });
}

fn bench_list_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("rpush_lpop_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::new();
                for i in 0..1_000 {
                    db.rpush("list", &[Bytes::from(format!("item:{i}"))])
                        .unwrap();
                }
                for _ in 0..1_000 {
                    black_box(db.lpop("list", None).unwrap());
                }
            });
        })
    });
}

criterion_group!(
    benches,
    bench_set_get_sequential,
    bench_incr_sequential,
    bench_incr_concurrent,
    bench_list_operations,
);
criterion_main!(benches);
