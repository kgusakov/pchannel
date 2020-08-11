use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pchannel::persist_channel::*;
use tokio::sync::mpsc::*;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;


fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n-1) + fibonacci(n-2),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let data_path = NamedTempFile::new().unwrap();
    let ack_path = NamedTempFile::new().unwrap();
    let (tx, _) =
        persistent_channel::<i32, i32>(data_path.path().to_path_buf(), ack_path.path().to_path_buf(), 10000).unwrap();

    let (tokio_tx, _) = unbounded_channel::<(i32, i32)>();

    c.bench_function("send_tokio_channel", |b| b.iter(|| tokio_tx.send((black_box(20), black_box(20)))));
    c.bench_function("send_pchannel_wrapper_no_fsync", |b| b.iter(|| tx.send((black_box(20), black_box(20)), false)));
    c.bench_function("send_pchannel_wrapper_fsync", |b| b.iter(|| tx.send((black_box(20), black_box(20)), true)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);