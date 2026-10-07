// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_base64::{base64_decode_stream, base64_encode_stream};
use datastream_core::{create_readable_stream, stream_to_buffer, stream_to_string};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn config<'a>(
    c: &'a mut Criterion,
    name: &str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut group = c.benchmark_group(name);
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    group
}

// 1MB of repeating ASCII, like the JS bench generator.
fn generate_string(size: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 \n";
    (0..size).map(|i| CHARS[i % CHARS.len()] as char).collect()
}

// Split into 16KB chunks, like a node stream.
fn chunks(data: &[u8]) -> Vec<Vec<u8>> {
    data.chunks(16 * 1024).map(<[u8]>::to_vec).collect()
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    let small = generate_string(1024).into_bytes();
    let large = generate_string(1024 * 1024).into_bytes();
    let encode = |data: Vec<Vec<u8>>| async move {
        stream_to_string(base64_encode_stream(create_readable_stream(data)), None)
            .await
            .unwrap()
    };

    let mut group = config(c, "base64_encode_stream");
    group.bench_function("1KB string", |b| {
        b.to_async(&rt).iter(|| encode(chunks(&small)))
    });
    group.bench_function("1MB string", |b| {
        b.to_async(&rt).iter(|| encode(chunks(&large)))
    });
    group.finish();

    let small_encoded = rt.block_on(encode(chunks(&small))).into_bytes();
    let large_encoded = rt.block_on(encode(chunks(&large))).into_bytes();
    let decode = |data: Vec<Vec<u8>>| async move {
        stream_to_buffer(base64_decode_stream(create_readable_stream(data)), None)
            .await
            .unwrap()
    };
    let mut group = config(c, "base64_decode_stream");
    group.bench_function("1KB encoded", |b| {
        b.to_async(&rt).iter(|| decode(chunks(&small_encoded)))
    });
    group.bench_function("1MB encoded", |b| {
        b.to_async(&rt).iter(|| decode(chunks(&large_encoded)))
    });
    group.finish();

    let mut group = config(c, "base64 roundtrip");
    group.bench_function("1MB encode -> decode", |b| {
        b.to_async(&rt).iter(|| async {
            let encoded = base64_encode_stream(create_readable_stream(chunks(&large)));
            stream_to_buffer(base64_decode_stream(encoded), None)
                .await
                .unwrap()
        })
    });
    group.finish();
}

criterion_group!(index, benches);
criterion_main!(index);
