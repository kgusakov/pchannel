use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use pchannel::persist_channel::*;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::*;

fn send_benchmark(c: &mut Criterion) {
    let data_path = NamedTempFile::new().unwrap();
    let ack_path = NamedTempFile::new().unwrap();
    let (tx, _) = persistent_channel::<i32, i32>(
        data_path.path().to_path_buf(),
        ack_path.path().to_path_buf(),
        10000,
    )
    .unwrap();

    let (tokio_tx, _) = unbounded_channel::<(i32, i32)>();

    let mut group = c.benchmark_group("tokio channel vs pchannel on send");

    group.throughput(Throughput::Elements(1 as u64));

    group.bench_function("send_tokio_channel", |b| {
        b.iter(|| tokio_tx.send((black_box(20), black_box(20))))
    });
    group.bench_function("send_pchannel_wrapper_no_fsync", |b| {
        b.iter(|| tx.send((black_box(20), black_box(20))))
    });
    group.bench_function("send_pchannel_wrapper_fsync", |b| {
        b.iter(|| tx.send_fsync((black_box(20), black_box(20)), true))
    });
    group.finish();
}

fn send_receive_ack_benchmark(c: &mut Criterion) {
    let data_path = NamedTempFile::new().unwrap();
    let ack_path = NamedTempFile::new().unwrap();

    let (tx, mut rx) = persistent_channel::<i32, i32>(
        data_path.path().to_path_buf(),
        ack_path.path().to_path_buf(),
        u64::MAX,
    )
    .unwrap();

    let (tokio_tx, mut tokio_rx) = unbounded_channel::<(i32, i32)>();

    let mut group = c.benchmark_group("tokio channel vs pchannel on receive");
    group.throughput(Throughput::Elements(1 as u64));

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_tokio_channel", move |b| {
            b.iter(|| {
                tokio_tx.send((black_box(20), black_box(20))).unwrap();
                r.block_on(tokio_rx.recv()).unwrap();
            })
        });
    }

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_pchannel_wrapper_no_fsync", |b| {
            b.iter(|| {
                tx.send((black_box(20), black_box(20))).unwrap();
                let message = r.block_on(rx.recv()).unwrap();
                let f = async move {
                    message.ack().await.unwrap();
                };
                r.block_on(f);
            })
        });
    }

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_pchannel_wrapper_fsync", |b| {
            b.iter(|| {
                tx.send_fsync((black_box(20), black_box(20)), true).unwrap();
                let message = r.block_on(rx.recv()).unwrap();
                let f = async move {
                    message.ack_fsync(true).await.unwrap();
                };
                r.block_on(f);
            })
        });
    }

    group.finish();
}

criterion_group!(benches, send_benchmark, send_receive_ack_benchmark);
criterion_main!(benches);
