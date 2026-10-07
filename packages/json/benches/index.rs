// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{
    create_readable_stream, create_readable_stream_from_string, pipeline, stream_to_array, Value,
};
use datastream_json::{
    json_format_stream, json_parse_stream, ndjson_format_stream, ndjson_parse_stream,
};
use serde_json::json;

// The JS bench uses 100K; 10K keeps `cargo bench` to a couple of minutes.
const ROWS: usize = 10_000;

fn generate_objects(rows: usize) -> Vec<Value> {
    // Deterministic stand-in for the JS Math.random() value column.
    (0..rows)
        .map(|i| json!({"id": i, "name": format!("item_{i}"), "value": (i * 7919 % 10_007) as f64 / 10_007.0}))
        .collect()
}

fn benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let objects = generate_objects(ROWS);
    let lines: Vec<String> = objects.iter().map(Value::to_string).collect();
    let ndjson = lines.join("\n") + "\n";
    let json_array = Value::from(objects.clone()).to_string();

    c.bench_function("ndjsonParseStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let input = create_readable_stream_from_string(ndjson.clone(), None).unwrap();
            let (stream, _) = ndjson_parse_stream(input, Default::default());
            stream_to_array(stream, None).await.unwrap()
        })
    });

    c.bench_function("ndjsonFormatStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream =
                ndjson_format_stream(create_readable_stream(objects.clone()), Default::default());
            pipeline(stream, &[]).await.unwrap()
        })
    });

    c.bench_function("jsonParseStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let input = create_readable_stream_from_string(json_array.clone(), None).unwrap();
            let (stream, _) = json_parse_stream(input, Default::default());
            stream_to_array(stream, None).await.unwrap()
        })
    });

    c.bench_function("jsonFormatStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream =
                json_format_stream(create_readable_stream(objects.clone()), Default::default());
            pipeline(stream, &[]).await.unwrap()
        })
    });

    c.bench_function("ndjson roundtrip", |b| {
        b.to_async(&runtime).iter(|| async {
            let text =
                ndjson_format_stream(create_readable_stream(objects.clone()), Default::default());
            let (stream, _) = ndjson_parse_stream(text, Default::default());
            pipeline(stream, &[]).await.unwrap()
        })
    });
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
