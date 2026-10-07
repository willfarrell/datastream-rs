// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, stream_to_buffer};
use datastream_encrypt::{
    decrypt_stream, encrypt_stream, Algorithm, DecryptOptions, EncryptOptions,
};

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

// Split into 16KB chunks, like a node stream.
fn chunks(data: &[u8]) -> Vec<Vec<u8>> {
    data.chunks(16 * 1024).map(<[u8]>::to_vec).collect()
}

fn key(algorithm: Algorithm) -> Vec<u8> {
    match algorithm {
        Algorithm::Aes128Gcm | Algorithm::Aes128Ctr => vec![7; 16],
        _ => vec![7; 32],
    }
}

async fn encrypt(algorithm: Algorithm, data: Vec<Vec<u8>>) -> (Vec<u8>, serde_json::Value) {
    let options = EncryptOptions {
        algorithm,
        key: key(algorithm),
        ..Default::default()
    };
    let (stream, result) = encrypt_stream(create_readable_stream(data), options).unwrap();
    (stream_to_buffer(stream, None).await.unwrap(), result.get())
}

fn bytes(value: &serde_json::Value) -> Option<Vec<u8>> {
    value
        .as_array()
        .map(|a| a.iter().map(|b| b.as_u64().unwrap() as u8).collect())
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    let small = vec![0xab; 1024];
    let large = vec![0xab; 1024 * 1024];

    for algorithm in [
        Algorithm::Aes256Gcm,
        Algorithm::Aes256Ctr,
        Algorithm::Chacha20Poly1305,
    ] {
        let mut group = config(c, &format!("encrypt_stream {}", algorithm.name()));
        group.bench_function("1KB", |b| {
            b.to_async(&rt).iter(|| encrypt(algorithm, chunks(&small)))
        });
        group.bench_function("1MB", |b| {
            b.to_async(&rt).iter(|| encrypt(algorithm, chunks(&large)))
        });
        group.finish();
    }

    let mut group = config(c, "roundtrip comparison 1MB");
    for algorithm in [
        Algorithm::Aes128Gcm,
        Algorithm::Aes256Gcm,
        Algorithm::Aes128Ctr,
        Algorithm::Aes256Ctr,
        Algorithm::Chacha20Poly1305,
    ] {
        group.bench_function(algorithm.name(), |b| {
            b.to_async(&rt).iter(|| async {
                let (ciphertext, result) = encrypt(algorithm, chunks(&large)).await;
                let options = DecryptOptions {
                    algorithm,
                    key: key(algorithm),
                    iv: bytes(&result["iv"]).unwrap(),
                    auth_tag: bytes(&result["authTag"]),
                    ..Default::default()
                };
                let stream =
                    decrypt_stream(create_readable_stream(chunks(&ciphertext)), options).unwrap();
                stream_to_buffer(stream, None).await.unwrap()
            })
        });
    }
    group.finish();
}

criterion_group!(index, benches);
criterion_main!(index);
