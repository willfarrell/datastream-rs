// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::StreamExt;
use datastream_file::{file_read_stream, file_write_stream, FileOptions};

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = std::env::temp_dir().join(format!("datastream-file-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("test.csv");
    let out = dir.join("test-out.csv");
    let rows: Vec<String> = (0..10_000)
        .map(|i| format!("{i},item_{i},{}", i as f64 / 7.0))
        .collect();
    std::fs::write(&file, rows.join("\n")).unwrap();
    let read = |path: &std::path::Path| {
        file_read_stream(FileOptions {
            path: path.to_path_buf(),
            ..Default::default()
        })
        .unwrap()
    };

    c.bench_function("fileReadStream/10K row CSV file", |b| {
        b.to_async(&rt).iter(|| async {
            let mut stream = read(&file);
            while let Some(chunk) = stream.next().await {
                chunk.unwrap();
            }
        })
    });
    c.bench_function("fileWriteStream/10K row CSV read → write", |b| {
        b.to_async(&rt).iter(|| async {
            let options = FileOptions {
                path: out.clone(),
                ..Default::default()
            };
            file_write_stream(read(&file), options).await.unwrap();
        })
    });

    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(index, benches);
criterion_main!(index);
