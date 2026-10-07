// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{create_readable_stream, pipeline, stream_to_array, Value};
use datastream_validate::{
    transpile_schema, validate_stream, Schema, TranspileOptions, ValidateOptions,
};
use serde_json::json;
use tokio::runtime::Runtime;

const ITEMS: usize = 100_000;

fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "id": { "type": "integer" },
            "name": { "type": "string" },
            "email": { "type": "string" },
            "age": { "type": "integer", "minimum": 0, "maximum": 150 },
            "active": { "type": "boolean" },
        },
        "required": ["id", "name", "email"],
        "additionalProperties": false,
    })
}

async fn validate_all(input: Vec<Value>, schema: Schema) -> Vec<Value> {
    let options = ValidateOptions {
        schema: Some(schema),
        ..Default::default()
    };
    let (stream, _) = validate_stream(create_readable_stream(input), options).unwrap();
    stream_to_array(stream, None).await.unwrap()
}

fn benches(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let schema = schema();
    let compiled = transpile_schema(&schema, TranspileOptions::default()).unwrap();
    let valid: Vec<Value> = (0..ITEMS)
        .map(|i| {
            json!({
                "id": i,
                "name": format!("user_{i}"),
                "email": format!("user_{i}@example.com"),
                "age": i % 100 + 18,
                "active": i % 2 == 0,
            })
        })
        .collect();
    let mixed: Vec<Value> = valid
        .iter()
        .enumerate()
        .map(|(i, row)| {
            if i % 100 == 0 {
                json!({"id": format!("not_a_number_{i}"), "name": row["name"], "email": row["email"], "age": -1})
            } else {
                row.clone()
            }
        })
        .collect();

    c.bench_function("transpileSchema/compile schema", |b| {
        b.iter(|| transpile_schema(&schema, TranspileOptions::default()).unwrap())
    });
    c.bench_function("validateStream (all valid)/100000 objects", |b| {
        b.to_async(&rt)
            .iter(|| validate_all(valid.clone(), compiled.clone().into()))
    });
    c.bench_function("validateStream (1% invalid)/100000 objects", |b| {
        b.to_async(&rt).iter(|| async {
            let options = ValidateOptions {
                schema: Some(compiled.clone().into()),
                ..Default::default()
            };
            let (stream, result) =
                validate_stream(create_readable_stream(mixed.clone()), options).unwrap();
            pipeline(stream, &[&result]).await.unwrap()
        })
    });
    let mut group = c.benchmark_group("validateStream precompiled vs inline");
    group.bench_function("100000 objects, precompiled", |b| {
        b.to_async(&rt)
            .iter(|| validate_all(valid.clone(), compiled.clone().into()))
    });
    group.bench_function("100000 objects, inline schema", |b| {
        b.to_async(&rt)
            .iter(|| validate_all(valid.clone(), schema.clone().into()))
    });
    group.finish();
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = benches
}
criterion_main!(index);
