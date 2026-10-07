// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, pipeline, stream_to_array, stream_to_string};
use datastream_string::{
    string_count_stream, string_length_stream, string_minimum_chunk_size,
    string_minimum_first_chunk_size, string_readable_stream, string_replace_stream,
    string_skip_consecutive_duplicates, string_split_stream, StringCountOptions,
    StringLengthOptions, StringMinimumChunkSizeOptions, StringReplaceOptions, StringSplitOptions,
};
use regex::Regex;

fn generate_string(size: usize) -> String {
    let chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\n";
    (0..size).map(|i| chars[i % chars.len()] as char).collect()
}

fn benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let big_string = generate_string(1_024 * 1_024); // 1MB

    c.bench_function("stringLengthStream 1MB", |b| {
        b.to_async(&runtime).iter(|| async {
            let (stream, result) = string_length_stream(
                string_readable_stream(big_string.clone()),
                StringLengthOptions::default(),
            );
            pipeline(stream, &[&result]).await.unwrap()
        })
    });

    for substr in ["\n", "ABCDE"] {
        c.bench_function(&format!("stringCountStream 1MB, count {substr:?}"), |b| {
            b.to_async(&runtime).iter(|| async {
                let (stream, result) = string_count_stream(
                    string_readable_stream(big_string.clone()),
                    StringCountOptions {
                        substr: substr.into(),
                        ..Default::default()
                    },
                )
                .unwrap();
                pipeline(stream, &[&result]).await.unwrap()
            })
        });
    }

    c.bench_function("stringSplitStream 1MB, split by newline", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = string_split_stream(
                string_readable_stream(big_string.clone()),
                StringSplitOptions {
                    separator: "\n".into(),
                    ..Default::default()
                },
            )
            .unwrap();
            stream_to_array(stream, None).await.unwrap()
        })
    });

    let pattern = Regex::new("A").unwrap();
    c.bench_function("stringReplaceStream 1MB, replace char", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = string_replace_stream(
                string_readable_stream(big_string.clone()),
                StringReplaceOptions {
                    pattern: pattern.clone().into(),
                    replacement: "X".into(),
                    ..Default::default()
                },
            );
            stream_to_string(stream, None).await.unwrap()
        })
    });

    let min = StringMinimumChunkSizeOptions {
        chunk_size: Some(64 * 1024),
    };
    c.bench_function("stringMinimumFirstChunkSizeStream 1MB, 64KB", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = string_minimum_first_chunk_size(
                string_readable_stream(big_string.clone()),
                min.clone(),
            );
            stream_to_string(stream, None).await.unwrap()
        })
    });
    c.bench_function("stringMinimumChunkSizeStream 1MB, 64KB", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream =
                string_minimum_chunk_size(string_readable_stream(big_string.clone()), min.clone());
            stream_to_string(stream, None).await.unwrap()
        })
    });

    let duplicates: Vec<String> = (0..10_000)
        .map(|i| if i % 2 == 0 { "aaa" } else { "bbb" }.to_string())
        .collect();
    let unique: Vec<String> = (0..10_000).map(|i| format!("chunk_{i}")).collect();
    for (name, chunks) in [("50% duplicates", duplicates), ("all unique", unique)] {
        c.bench_function(
            &format!("stringSkipConsecutiveDuplicatesStream 10K, {name}"),
            |b| {
                b.to_async(&runtime).iter(|| async {
                    let stream =
                        string_skip_consecutive_duplicates(create_readable_stream(chunks.clone()));
                    stream_to_array(stream, None).await.unwrap()
                })
            },
        );
    }
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = benches
}
criterion_main!(index);
