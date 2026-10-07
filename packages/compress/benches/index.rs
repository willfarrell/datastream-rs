// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_compress::*;
use datastream_core::{create_readable_stream, stream_to_buffer, DataStream};

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

type Compress = fn(DataStream<Vec<u8>>, CompressOptions) -> DataStream<Vec<u8>>;
type Decompress = fn(DataStream<Vec<u8>>, DecompressOptions) -> DataStream<Vec<u8>>;

// ~1MB of JSON-like content, as in the JS bench (values deterministic here).
fn compressible_body() -> Vec<u8> {
    let items: Vec<String> = (0..10_000)
        .map(|i| {
            format!(
                r#"{{"id":{i},"name":"item_{i}","value":0.{:06}}}"#,
                (i * 7919) % 1_000_000
            )
        })
        .collect();
    format!("[{}]", items.join(",")).into_bytes()
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    let body = compressible_body();
    let label = format!("{} bytes", body.len());
    let codecs: [(&str, Compress, Decompress, [i32; 2]); 4] = [
        ("gzip", gzip_compress_stream, gzip_decompress_stream, [1, 9]),
        (
            "deflate",
            deflate_compress_stream,
            deflate_decompress_stream,
            [1, 9],
        ),
        (
            "brotli",
            brotli_compress_stream,
            brotli_decompress_stream,
            [1, 11],
        ),
        (
            "zstd",
            zstd_compress_stream,
            zstd_decompress_stream,
            [1, 19],
        ),
    ];

    for (name, compress, decompress, qualities) in codecs {
        let mut group = config(c, &format!("{name}_compress_stream"));
        for quality in [None, Some(qualities[0]), Some(qualities[1])] {
            let id = match quality {
                None => label.clone(),
                Some(q) => format!("{label}, quality {q}"),
            };
            group.bench_function(id, |b| {
                b.to_async(&rt).iter(|| async {
                    let options = CompressOptions {
                        quality,
                        ..Default::default()
                    };
                    stream_to_buffer(
                        compress(create_readable_stream(chunks(&body)), options),
                        None,
                    )
                    .await
                    .unwrap()
                })
            });
        }
        group.finish();

        let mut group = config(c, &format!("{name} roundtrip"));
        group.bench_function(label.as_str(), |b| {
            b.to_async(&rt).iter(|| async {
                let compressed = compress(
                    create_readable_stream(chunks(&body)),
                    CompressOptions::default(),
                );
                stream_to_buffer(decompress(compressed, DecompressOptions::default()), None)
                    .await
                    .unwrap()
            })
        });
        group.finish();
    }
}

criterion_group!(index, benches);
criterion_main!(index);
