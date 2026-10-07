// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, pipeline};
use datastream_digest::{digest_stream, DigestOptions};

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

async fn digest(data: Vec<Vec<u8>>, algorithm: &str) {
    let options = DigestOptions {
        algorithm: algorithm.into(),
        result_key: None,
    };
    let (stream, result) = digest_stream(create_readable_stream(data), options).unwrap();
    pipeline(stream, &[&result]).await.unwrap();
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    let large = generate_string(1024 * 1024).into_bytes();

    for algorithm in ["SHA256", "SHA384", "SHA512"] {
        let mut group = config(c, &format!("digest_stream {algorithm}"));
        group.bench_function("1MB string", |b| {
            b.to_async(&rt).iter(|| digest(chunks(&large), algorithm))
        });
        group.finish();
    }

    let mut group = config(c, "digest_stream comparison");
    for algorithm in [
        "SHA2-256", "SHA2-384", "SHA2-512", "SHA3-256", "SHA3-384", "SHA3-512",
    ] {
        group.bench_function(format!("1MB {algorithm}"), |b| {
            b.to_async(&rt).iter(|| digest(chunks(&large), algorithm))
        });
    }
    group.finish();
}

criterion_group!(index, benches);
criterion_main!(index);
