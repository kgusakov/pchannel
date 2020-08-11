use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pchannel::persist_channel::*;
use tokio::sync::mpsc::*;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;
use std::io::{Read, Write};

fn send_benchmark(c: &mut Criterion) {
    let data_path = NamedTempFile::new().unwrap();
    let ack_path = NamedTempFile::new().unwrap();
    let (tx, _) =
        persistent_channel::<i32, i32>(data_path.path().to_path_buf(), ack_path.path().to_path_buf(), 10000).unwrap();

    let (tokio_tx, _) = unbounded_channel::<(i32, i32)>();

    let mut group = c.benchmark_group("tokio channel vs pchannel on send");

    group.bench_function("send_tokio_channel", |b| b.iter(|| tokio_tx.send((black_box(20), black_box(20)))));
    group.bench_function("send_pchannel_wrapper_no_fsync", |b| b.iter(|| tx.send((black_box(20), black_box(20)), false)));
    group.bench_function("send_pchannel_wrapper_fsync", |b| b.iter(|| tx.send((black_box(20), black_box(20)), true)));
    group.finish();
}

fn send_receive_ack_benchmark(c: &mut Criterion) {
    let data_path = NamedTempFile::new().unwrap();
    let ack_path = NamedTempFile::new().unwrap();

    let (tx, mut rx) =
        persistent_channel::<i32, i32>(data_path.path().to_path_buf(), ack_path.path().to_path_buf(), u64::MAX).unwrap();

    let (tokio_tx, mut tokio_rx) = unbounded_channel::<(i32, i32)>();

    let mut group = c.benchmark_group("tokio channel vs pchannel on receive");

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_tokio_channel", move |b| b.iter(|| { 
            tokio_tx.send((black_box(20), black_box(20))).unwrap();
            r.block_on(tokio_rx.recv()).unwrap();
        }));
    }

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_pchannel_wrapper_no_fsync", |b| b.iter(|| {
            tx.send((black_box(20), black_box(20)), false).unwrap();
            let message = r.block_on(rx.recv()).unwrap();
            let f = async move {
                message.ack(false).await.unwrap();
            };
            r.block_on(f);
        }));
    }

    {
        let mut r = Runtime::new().unwrap();

        group.bench_function("send_receive_pchannel_wrapper_fsync", |b| b.iter(|| {
            tx.send((black_box(20), black_box(20)), true).unwrap();
            let message = r.block_on(rx.recv()).unwrap();
            let f = async move {
                message.ack(true).await.unwrap();
            };
            r.block_on(f);
        }));
    }

    
    group.finish();
}

fn fsync_benchmark(c: &mut Criterion) {
    let named_file = NamedTempFile::new().unwrap();
    let mut f = named_file.into_file();
    let mut group = c.benchmark_group("check fsync");
    let mut count = 0;
    group.bench_function("no write and fsync", |b| b.iter(|| {
        f.sync_data().unwrap();
    }));
    group.bench_function("write and fsync", |b| b.iter(|| {
        f.write(&[1u8]).unwrap();
        f.sync_data().unwrap();
    }));
    group.bench_function("no fsync", |b| b.iter(|| {
        f.write(&[1u8]).unwrap();
    }));
    group.bench_function("counter loop - no file io at all", |b| b.iter(|| {
        count += 1;
    }));
    group.finish();
}

criterion_group!(benches, send_benchmark, send_receive_ack_benchmark, fsync_benchmark);
criterion_main!(benches);