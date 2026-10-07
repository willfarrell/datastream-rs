// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_charset::{
    charset_decode_stream, charset_detect_stream, charset_encode_stream, CharsetDetectOptions,
    CharsetOptions,
};
use datastream_core::{create_readable_stream, pipeline, stream_to_buffer, stream_to_string};

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

fn options(charset: &str) -> CharsetOptions {
    CharsetOptions {
        charset: Some(charset.into()),
    }
}

// String chunks of ~16KB on char boundaries (the data is ASCII).
fn string_chunks(data: &str) -> Vec<String> {
    chunks(data.as_bytes())
        .into_iter()
        .map(|c| String::from_utf8(c).unwrap())
        .collect()
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    let text = generate_string(1024 * 1024);

    let mut group = config(c, "charset_detect_stream");
    group.bench_function("1MB UTF-8 string", |b| {
        b.to_async(&rt).iter(|| async {
            let input = create_readable_stream(chunks(text.as_bytes()));
            let (stream, result) = charset_detect_stream(input, CharsetDetectOptions::default());
            pipeline(stream, &[&result]).await.unwrap()
        })
    });
    group.finish();

    let mut group = config(c, "charset_encode_stream");
    for charset in ["UTF-8", "ISO-8859-1"] {
        group.bench_function(format!("1MB {charset}"), |b| {
            b.to_async(&rt).iter(|| async {
                let input = create_readable_stream(string_chunks(&text));
                stream_to_buffer(charset_encode_stream(input, options(charset)), None)
                    .await
                    .unwrap()
            })
        });
    }
    group.finish();

    let mut group = config(c, "charset_decode_stream");
    group.bench_function("1MB UTF-8", |b| {
        b.to_async(&rt).iter(|| async {
            let input = create_readable_stream(chunks(text.as_bytes()));
            stream_to_string(charset_decode_stream(input, options("UTF-8")), None)
                .await
                .unwrap()
        })
    });
    group.finish();

    let mut group = config(c, "charset roundtrip");
    group.bench_function("1MB UTF-8 encode -> decode", |b| {
        b.to_async(&rt).iter(|| async {
            let input = create_readable_stream(string_chunks(&text));
            let encoded = charset_encode_stream(input, options("UTF-8"));
            stream_to_string(charset_decode_stream(encoded, options("UTF-8")), None)
                .await
                .unwrap()
        })
    });
    group.finish();
}

criterion_group!(index, benches);
criterion_main!(index);
